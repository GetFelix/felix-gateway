//! One browser session relayed to Felix. The gateway keeps no canvas state:
//! payloads pass through as opaque bytes, and order and offsets are the
//! broker's.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::felix::Felix;
use crate::metrics::Metrics;
use crate::protocol::{ClientMessage, ErrorCode, ServerMessage, StartAt, StreamName};
use crate::transport::{BrowserConnection, Incoming};

const PING_INTERVAL: Duration = Duration::from_secs(5);

/// Events waiting for a slow browser. When this fills, the subscription stops
/// reading and Felix's own per-subscriber queue decides what to drop, so a
/// loss shows up as an offset gap instead of happening silently here.
const EVENT_QUEUE: usize = 1024;

/// Publishes waiting for Felix. A browser that outruns this stops being read.
const PUBLISH_QUEUE: usize = 256;

struct Publish {
    stream: StreamName,
    payload: Vec<u8>,
    ack: bool,
    id: u64,
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
    // Its length is bounded by PUBLISH_QUEUE anyway: one reply per publish.
    let (replies_tx, mut replies_rx) = mpsc::unbounded_channel();
    let (publish_tx, publish_rx) = mpsc::channel(PUBLISH_QUEUE);
    let publisher = tokio::spawn(publish_in_order(
        publish_rx,
        Arc::clone(&felix),
        Arc::clone(&metrics),
        replies_tx.clone(),
    ));
    let mut subscriptions: HashMap<StreamName, JoinHandle<()>> = HashMap::new();
    let mut ping = tokio::time::interval(PING_INTERVAL);

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
                                let publish = Publish { stream, payload, ack, id };
                                if publish_tx.send(publish).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                            Err(err) => error(Some(id), Some(stream), ErrorCode::BadRequest, err),
                        }
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

    for task in subscriptions.into_values() {
        task.abort();
    }
    // Publishes already handed over still complete; only their replies are lost.
    drop(publish_tx);
    let _ = publisher.await;
}

/// Publish one at a time, in the order the browser sent them, so a session's
/// ops reach the log in its own `seq` order.
async fn publish_in_order(
    mut queue: mpsc::Receiver<Publish>,
    felix: Arc<Felix>,
    metrics: Arc<Metrics>,
    replies: mpsc::UnboundedSender<ServerMessage>,
) {
    while let Some(Publish {
        stream,
        payload,
        ack,
        id,
    }) = queue.recv().await
    {
        let started = Instant::now();
        let reply = match felix.publish(stream, payload, ack).await {
            Ok(offset) if ack => {
                metrics.record_felix_ack(stream, started.elapsed());
                ServerMessage::Ack { id, offset }
            }
            Ok(_) => continue,
            Err(err) => error(Some(id), Some(stream), ErrorCode::PublishFailed, err),
        };
        let _ = replies.send(reply);
    }
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
                .send(error(None, Some(stream), ErrorCode::SubscribeFailed, err))
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
            Ok(None) => break "the broker closed the subscription".to_string(),
            Err(err) => break format!("{err:#}"),
        }
    };
    let _ = events
        .send(error(
            None,
            Some(stream),
            ErrorCode::SubscriptionEnded,
            ended,
        ))
        .await;
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
        message: format!("{err:#}"),
    }
}
