//! One browser session relayed to Felix. The gateway keeps no canvas state:
//! payloads pass through as opaque bytes, and order and offsets are the
//! broker's.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use felix_client::{CacheChange, CacheWatchItem, CursorErrorReason, SubscribeCursorError};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::felix::Felix;
use crate::metrics::Metrics;
use crate::protocol::{
    ClientMessage, CounterName, ErrorCode, MemberEntry, ServerMessage, StartAt, StreamName,
};
use crate::transport::{BrowserConnection, Incoming};

const PING_INTERVAL: Duration = Duration::from_secs(5);

/// Events waiting for a slow browser. When this fills, the subscription stops
/// reading and Felix's own per-subscriber queue decides what to drop, so a
/// loss shows up as an offset gap instead of happening silently here.
const EVENT_QUEUE: usize = 1024;

/// Writes waiting for Felix. A browser that outruns this stops being read.
const WRITE_QUEUE: usize = 256;

/// A write that must reach Felix in the order the browser sent it.
enum Write {
    Publish {
        stream: StreamName,
        payload: Vec<u8>,
        ack: bool,
        id: u64,
    },
    /// Set (`Some`) or delete (`None`) a member entry.
    Member {
        key: String,
        payload: Option<Vec<u8>>,
    },
}

/// Relay one browser connection until it closes.
pub(crate) async fn run<C: BrowserConnection>(
    mut conn: C,
    felix: Arc<Felix>,
    metrics: Arc<Metrics>,
) {
    let (events_tx, mut events_rx) = mpsc::channel(EVENT_QUEUE);
    // Unbounded so the publish task never waits on the browser, which is what
    // lets the loop below block on a full publish queue without deadlocking.
    // Its length is bounded by WRITE_QUEUE anyway: one reply per write.
    let (replies_tx, mut replies_rx) = mpsc::unbounded_channel();
    let (write_tx, write_rx) = mpsc::channel(WRITE_QUEUE);
    let writer = tokio::spawn(write_in_order(
        write_rx,
        Arc::clone(&felix),
        Arc::clone(&metrics),
        replies_tx.clone(),
    ));
    let mut subscriptions: HashMap<StreamName, JoinHandle<()>> = HashMap::new();
    let mut members_watch: Option<JoinHandle<()>> = None;
    let mut ping = tokio::time::interval(PING_INTERVAL);

    let hello = ServerMessage::Hello {
        namespace: felix.namespace().to_string(),
        room: felix.room().to_string(),
        member_ttl_ms: u64::try_from(felix.member_ttl().as_millis()).unwrap_or(u64::MAX),
    };
    let hello = serde_json::to_string(&hello).expect("server messages serialize");
    if conn.send(hello).await.is_err() {
        return;
    }

    loop {
        let outbound = tokio::select! {
            incoming = conn.recv() => match incoming {
                None => break,
                Some(Incoming::RoundTrip(elapsed)) => {
                    metrics.record_browser_rtt(elapsed);
                    continue;
                }
                Some(Incoming::Message(text)) => match serde_json::from_str(&text) {
                    Ok(ClientMessage::Subscribe { stream, from }) => {
                        let task = tokio::spawn(relay_subscription(
                            stream,
                            from,
                            Arc::clone(&felix),
                            events_tx.clone(),
                        ));
                        if let Some(previous) = subscriptions.insert(stream, task) {
                            previous.abort();
                        }
                        continue;
                    }
                    Ok(ClientMessage::Publish { stream, payload, ack, id }) => {
                        match BASE64.decode(payload) {
                            Ok(payload) => {
                                let publish = Write::Publish { stream, payload, ack, id };
                                if write_tx.send(publish).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                            Err(err) => error(Some(id), Some(stream), ErrorCode::BadRequest, err),
                        }
                    }
                    Ok(ClientMessage::SetMember { key, payload }) => {
                        match BASE64.decode(payload) {
                            Ok(_) if !valid_key(&key) => {
                                error(None, None, ErrorCode::BadRequest, BAD_KEY)
                            }
                            Ok(payload) => {
                                let write = Write::Member { key, payload: Some(payload) };
                                if write_tx.send(write).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                            Err(err) => error(None, None, ErrorCode::BadRequest, err),
                        }
                    }
                    Ok(ClientMessage::RemoveMember { key }) => {
                        if valid_key(&key) {
                            if write_tx.send(Write::Member { key, payload: None }).await.is_err() {
                                break;
                            }
                            continue;
                        }
                        error(None, None, ErrorCode::BadRequest, BAD_KEY)
                    }
                    Ok(ClientMessage::WatchMembers) => {
                        let task = tokio::spawn(relay_members(Arc::clone(&felix), events_tx.clone()));
                        if let Some(previous) = members_watch.replace(task) {
                            previous.abort();
                        }
                        continue;
                    }
                    Ok(ClientMessage::CounterAdd { counter, key, delta, id }) => {
                        if !valid_key(&key) {
                            error(Some(id), None, ErrorCode::BadRequest, BAD_KEY)
                        } else {
                            tokio::spawn(add_to_counter(
                                counter,
                                key,
                                delta,
                                id,
                                Arc::clone(&felix),
                                replies_tx.clone(),
                            ));
                            continue;
                        }
                    }
                    Ok(ClientMessage::SnapshotGet { id }) => {
                        tokio::spawn(get_snapshot(id, Arc::clone(&felix), replies_tx.clone()));
                        continue;
                    }
                    Err(err) => error(None, None, ErrorCode::BadRequest, err),
                },
            },
            Some(event) = events_rx.recv() => event,
            Some(reply) = replies_rx.recv() => reply,
            _ = ping.tick() => {
                if conn.ping().await.is_err() {
                    break;
                }
                continue;
            }
        };
        let text = serde_json::to_string(&outbound).expect("server messages serialize");
        if conn.send(text).await.is_err() {
            break;
        }
    }

    for task in subscriptions.into_values().chain(members_watch) {
        task.abort();
    }
    // Writes already handed over still complete, so a closing session's
    // member delete lands; only their replies are lost.
    drop(write_tx);
    let _ = writer.await;
}

/// Write one at a time, in the order the browser sent them, so a session's
/// ops reach the log in its own `seq` order and a member delete is never
/// overtaken by the refresh before it.
async fn write_in_order(
    mut queue: mpsc::Receiver<Write>,
    felix: Arc<Felix>,
    metrics: Arc<Metrics>,
    replies: mpsc::UnboundedSender<ServerMessage>,
) {
    while let Some(write) = queue.recv().await {
        let reply = match write {
            Write::Publish {
                stream,
                payload,
                ack,
                id,
            } => {
                let started = Instant::now();
                match felix.publish(stream, payload, ack).await {
                    Ok(offset) if ack => {
                        metrics.record_felix_ack(stream, started.elapsed());
                        ServerMessage::Ack { id, offset }
                    }
                    Ok(_) => continue,
                    Err(err) => error(Some(id), Some(stream), ErrorCode::PublishFailed, err),
                }
            }
            Write::Member { key, payload } => {
                let written = match payload {
                    Some(payload) => felix.set_member(&key, payload).await,
                    None => felix.remove_member(&key).await,
                };
                match written {
                    Ok(()) => continue,
                    Err(err) => error(None, None, ErrorCode::MemberFailed, err),
                }
            }
        };
        let _ = replies.send(reply);
    }
}

pub(crate) const BAD_KEY: &str = "a key is 1 to 64 ASCII letters, digits, '-' or '_'";

// Keys become part of a Felix cache key shared by every connection, so they
// are kept to a plain alphabet rather than passed through.
pub(crate) fn valid_key(key: &str) -> bool {
    (1..=64).contains(&key.len())
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

async fn add_to_counter(
    counter: CounterName,
    key: String,
    delta: i64,
    id: u64,
    felix: Arc<Felix>,
    replies: mpsc::UnboundedSender<ServerMessage>,
) {
    let reply = match felix.counter_add(counter, &key, delta).await {
        Ok(value) => ServerMessage::Counter { id, value },
        Err(err) => error(Some(id), None, ErrorCode::CounterFailed, err),
    };
    let _ = replies.send(reply);
}

async fn get_snapshot(id: u64, felix: Arc<Felix>, replies: mpsc::UnboundedSender<ServerMessage>) {
    let reply = match felix.snapshot().await {
        Ok(payload) => ServerMessage::Snapshot {
            id,
            payload: payload.map(|bytes| BASE64.encode(bytes)),
        },
        Err(err) => error(Some(id), None, ErrorCode::SnapshotFailed, err),
    };
    let _ = replies.send(reply);
}

async fn relay_subscription(
    stream: StreamName,
    from: StartAt,
    felix: Arc<Felix>,
    events: mpsc::Sender<ServerMessage>,
) {
    let mut subscription = match felix.subscribe(stream, from).await {
        Ok(subscription) => subscription,
        Err(err) => {
            let _ = events
                .send(subscription_error(stream, ErrorCode::SubscribeFailed, err))
                .await;
            return;
        }
    };
    let subscribed = ServerMessage::Subscribed {
        stream,
        start_offset: subscription.start_offset(),
        live_offset: subscription.live_offset(),
    };
    if events.send(subscribed).await.is_err() {
        return;
    }
    let ended = loop {
        match subscription.next_event().await {
            Ok(Some(event)) => {
                let message = ServerMessage::Event {
                    stream,
                    offset: event.offset,
                    skipped_before: event.skipped_before,
                    payload: BASE64.encode(&event.payload),
                };
                if events.send(message).await.is_err() {
                    return;
                }
            }
            Ok(None) => break anyhow::anyhow!("the broker closed the subscription"),
            Err(err) => break err,
        }
    };
    let _ = events
        .send(subscription_error(
            stream,
            ErrorCode::SubscriptionEnded,
            ended,
        ))
        .await;
}

/// `trimmed`, naming the oldest offset left, when retention has passed the
/// offset the subscription asked for; `code` otherwise.
fn subscription_error(stream: StreamName, code: ErrorCode, err: anyhow::Error) -> ServerMessage {
    let trimmed = err
        .chain()
        .filter_map(|cause| cause.downcast_ref::<SubscribeCursorError>())
        .find(|cursor| cursor.reason == CursorErrorReason::TooOld);
    match trimmed {
        Some(cursor) => ServerMessage::Error {
            id: None,
            stream: Some(stream),
            code: ErrorCode::Trimmed,
            oldest: Some(cursor.available),
            message: format!("{err:#}"),
        },
        None => error(None, Some(stream), code, err),
    }
}

/// Relay the room's member entries: the current set as one message, then
/// each change.
async fn relay_members(felix: Arc<Felix>, events: mpsc::Sender<ServerMessage>) {
    let mut watch = match felix.watch_members().await {
        Ok(watch) => watch,
        Err(err) => {
            let _ = events
                .send(error(None, None, ErrorCode::WatchFailed, err))
                .await;
            return;
        }
    };
    let prefix = felix.member_prefix();
    let mut retained = watch.retained_count().unwrap_or(0);
    let mut initial = Some(Vec::new());
    let ended = loop {
        if retained == 0
            && let Some(members) = initial.take()
            && events
                .send(ServerMessage::Members { members })
                .await
                .is_err()
        {
            return;
        }
        match watch.recv().await {
            Some(CacheWatchItem::Change(change)) => {
                let entry = member_entry(&prefix, change);
                match initial.as_mut() {
                    Some(members) => {
                        retained -= 1;
                        if entry.payload.is_some() {
                            members.push(entry);
                        }
                    }
                    None => {
                        if events.send(ServerMessage::Member(entry)).await.is_err() {
                            return;
                        }
                    }
                }
            }
            Some(CacheWatchItem::Lagged { .. }) => break "the member watch fell behind",
            // The watch follows a moved shard on the next recv.
            Some(CacheWatchItem::ShardMoved(_)) => {}
            None => break "the member watch ended",
        }
    };
    let _ = events
        .send(error(None, None, ErrorCode::WatchFailed, ended))
        .await;
}

fn member_entry(prefix: &str, change: CacheChange) -> MemberEntry {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        });
    MemberEntry {
        key: change
            .key
            .strip_prefix(prefix)
            .unwrap_or(&change.key)
            .to_string(),
        payload: change.value.map(|value| BASE64.encode(value)),
        expires_in_ms: (change.expires_at_millis != 0)
            .then(|| change.expires_at_millis.saturating_sub(now)),
    }
}

fn error(
    id: Option<u64>,
    stream: Option<StreamName>,
    code: ErrorCode,
    err: impl std::fmt::Display,
) -> ServerMessage {
    ServerMessage::Error {
        id,
        stream,
        code,
        oldest: None,
        message: format!("{err:#}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trimmed_offset_is_reported_with_the_oldest_left() {
        let err = anyhow::Error::new(SubscribeCursorError {
            reason: CursorErrorReason::TooOld,
            requested: 3,
            available: 120,
        })
        .context("subscribe to canvas.ops.lobby");
        let message = subscription_error(StreamName::Ops, ErrorCode::SubscribeFailed, err);
        let ServerMessage::Error { code, oldest, .. } = message else {
            panic!("{message:?}");
        };
        assert_eq!((code, oldest), (ErrorCode::Trimmed, Some(120)));

        let future = anyhow::Error::new(SubscribeCursorError {
            reason: CursorErrorReason::InFuture,
            requested: 900,
            available: 10,
        });
        let message = subscription_error(StreamName::Ops, ErrorCode::SubscribeFailed, future);
        let ServerMessage::Error { code, oldest, .. } = message else {
            panic!("{message:?}");
        };
        assert_eq!((code, oldest), (ErrorCode::SubscribeFailed, None));
    }
}
