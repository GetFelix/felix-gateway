//! One browser session relayed to Felix. The gateway keeps no application
//! state: payloads pass through as opaque bytes, and order and offsets are
//! the broker's.

use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use felix_client::{CacheChange, CacheWatchItem, CursorErrorReason, SubscribeCursorError};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::Gateway;
use crate::access::{Grant, Refused};
use crate::felix::Felix;
use crate::limits::{self, SessionLimits, WriteCost};
use crate::metrics::{Metrics, Refusal};
use crate::protocol::{
    CacheEntry, ClientMessage, ErrorCode, FEATURES, Join, PROTOCOL, RATE_LIMITED, ServerMessage,
    StartAt,
};
use crate::scope::{
    Action, BAD_NAME, Bound, CacheAction, CounterAction, Scope, StreamAction, valid_name,
};
use crate::throttle::Throttle;
use crate::transport::{BrowserConnection, Incoming};

const PING_INTERVAL: Duration = Duration::from_secs(5);

/// How long a new connection may take to send its `join`.
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Events waiting for a slow browser. When this fills, the subscription stops
/// reading and Felix's own per-subscriber queue decides what to drop. On a
/// durable stream the subscription replays those drops from the log.
const EVENT_QUEUE: usize = 1024;

/// Writes waiting for Felix. A browser that outruns this stops being read.
const WRITE_QUEUE: usize = 256;

/// A write that must reach Felix in the order the browser sent it. Aliases
/// are for replies; names are Felix's.
enum Write {
    Publish {
        stream: String,
        name: String,
        payload: Vec<u8>,
        ack: bool,
        id: u64,
    },
    /// Put (`Some`) or delete (`None`) a cache entry.
    Cache {
        cache: String,
        name: String,
        key: String,
        payload: Option<Vec<u8>>,
        ttl: Option<Duration>,
    },
}

/// One joined connection's tasks and queues.
struct Session {
    felix: Arc<Felix>,
    limits: SessionLimits,
    /// The session asked for [`RATE_LIMITED`].
    rate_limited: bool,
    throttle: Arc<Throttle>,
    events: mpsc::Sender<ServerMessage>,
    replies: mpsc::UnboundedSender<ServerMessage>,
    writes: mpsc::Sender<Write>,
    /// Keyed by stream alias.
    subscriptions: HashMap<String, JoinHandle<()>>,
    /// Keyed by cache alias.
    watches: HashMap<String, JoinHandle<()>>,
}

/// What to do after handling one browser message.
enum Next {
    Continue,
    Reply(ServerMessage),
    /// The writer is gone, so the session is over.
    Stop,
}

/// Relay one browser connection from `client` until it closes.
pub(crate) async fn run<C: BrowserConnection>(
    mut conn: C,
    gateway: Gateway,
    client: Option<IpAddr>,
) {
    let Some(Joined {
        felix,
        hello,
        limits,
        rate_limited,
    }) = join(&mut conn, &gateway, client).await
    else {
        return;
    };
    let felix = Arc::new(felix);
    let (events_tx, mut events_rx) = mpsc::channel(EVENT_QUEUE);
    // Unbounded so the writer never waits on the browser, which is what lets
    // the loop below block on a full write queue without deadlocking. Its
    // length is bounded by WRITE_QUEUE anyway: one reply per write.
    let (replies_tx, mut replies_rx) = mpsc::unbounded_channel();
    let (write_tx, write_rx) = mpsc::channel(WRITE_QUEUE);
    let writer = tokio::spawn(write_in_order(
        write_rx,
        Arc::clone(&felix),
        Arc::clone(&gateway.metrics),
        replies_tx.clone(),
    ));
    let mut session = Session {
        felix,
        limits,
        rate_limited,
        throttle: Arc::new(Throttle::default()),
        events: events_tx,
        replies: replies_tx,
        writes: write_tx,
        subscriptions: HashMap::new(),
        watches: HashMap::new(),
    };
    let mut ping = tokio::time::interval(PING_INTERVAL);
    if send(&mut conn, &hello).await.is_err() {
        return;
    }

    loop {
        let outbound = tokio::select! {
            incoming = conn.recv() => match incoming {
                None => break,
                Some(Incoming::RoundTrip(elapsed)) => {
                    gateway.metrics.record_browser_rtt(elapsed);
                    continue;
                }
                Some(Incoming::Message(text)) => match session.handle(&text).await {
                    Next::Continue => continue,
                    Next::Reply(reply) => reply,
                    Next::Stop => break,
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
        if send(&mut conn, &outbound).await.is_err() {
            break;
        }
    }

    let Session {
        subscriptions,
        watches,
        writes,
        ..
    } = session;
    for task in subscriptions.into_values().chain(watches.into_values()) {
        task.abort();
    }
    // Writes already handed over still complete, so a closing session's
    // cache delete lands; only their replies are lost.
    drop(writes);
    let _ = writer.await;
}

impl Session {
    async fn handle(&mut self, text: &str) -> Next {
        let message = match serde_json::from_str(text) {
            Ok(message) => message,
            Err(err) => return Next::Reply(error(ErrorCode::BadRequest, err)),
        };
        let felix = Arc::clone(&self.felix);
        let scope = &felix.scope;
        match message {
            ClientMessage::Subscribe { stream, from } => {
                let name = match scope.stream(&stream, StreamAction::Subscribe) {
                    Ok(bound) => bound.name,
                    Err(refusal) => {
                        return Next::Reply(stream_error(stream, ErrorCode::BadRequest, refusal));
                    }
                };
                let task = tokio::spawn(relay_subscription(
                    stream.clone(),
                    name,
                    from,
                    Arc::clone(&self.felix),
                    Arc::clone(&self.throttle),
                    self.events.clone(),
                ));
                if let Some(previous) = self.subscriptions.insert(stream, task) {
                    previous.abort();
                }
                Next::Continue
            }
            ClientMessage::Publish {
                stream,
                payload,
                ack,
                id,
            } => {
                let bound = match scope.stream(&stream, StreamAction::Publish) {
                    Ok(bound) => bound,
                    Err(refusal) => {
                        return Next::Reply(with_id(
                            id,
                            stream_error(stream, ErrorCode::BadRequest, refusal),
                        ));
                    }
                };
                let mut payload = match BASE64.decode(payload) {
                    Ok(payload) => payload,
                    Err(err) => {
                        return Next::Reply(with_id(
                            id,
                            stream_error(stream, ErrorCode::BadRequest, err),
                        ));
                    }
                };
                if let Err(refused) = self.charge(&bound, &stream, payload.len()) {
                    let code = self.refusal_code(refused, ErrorCode::PublishFailed);
                    return Next::Reply(with_id(
                        id,
                        limited(stream_error(stream, code, ""), refused),
                    ));
                }
                if bound.resource.stamp_sender {
                    payload = stamped(&self.felix.principal, &payload);
                }
                let name = bound.name;
                self.write(Write::Publish {
                    stream,
                    name,
                    payload,
                    ack,
                    id,
                })
                .await
            }
            ClientMessage::CounterAdd {
                counter,
                key,
                delta,
                id,
            } => {
                let bound = match scope.counter(&counter, CounterAction::Add) {
                    Ok(bound) => bound,
                    Err(refusal) => {
                        return Next::Reply(with_id(id, error(ErrorCode::BadRequest, refusal)));
                    }
                };
                if !valid_name(&key) {
                    return Next::Reply(with_id(id, error(ErrorCode::BadRequest, BAD_NAME)));
                }
                if let Err(refused) = self.charge(&bound, &counter, 0) {
                    let code = self.refusal_code(refused, ErrorCode::CounterFailed);
                    return Next::Reply(with_id(id, limited(error(code, ""), refused)));
                }
                let name = bound.name;
                let felix = Arc::clone(&self.felix);
                let replies = self.replies.clone();
                tokio::spawn(async move {
                    let reply = match felix.counter_add(&name, &key, delta).await {
                        Ok(value) => ServerMessage::Counter { id, value },
                        Err(err) => with_id(id, error(ErrorCode::CounterFailed, err)),
                    };
                    let _ = replies.send(reply);
                });
                Next::Continue
            }
            ClientMessage::CacheGet { cache, key, id } => {
                let name = match scope.cache(&cache, CacheAction::Read) {
                    Ok(_) if !valid_name(&key) => {
                        return Next::Reply(with_id(
                            id,
                            cache_error(cache, ErrorCode::BadRequest, BAD_NAME),
                        ));
                    }
                    Ok(bound) => bound.name,
                    Err(refusal) => {
                        return Next::Reply(with_id(
                            id,
                            cache_error(cache, ErrorCode::BadRequest, refusal),
                        ));
                    }
                };
                let felix = Arc::clone(&self.felix);
                let replies = self.replies.clone();
                tokio::spawn(async move {
                    let reply = match felix.cache_get(&name, &key).await {
                        Ok(payload) => ServerMessage::CacheValue {
                            id,
                            payload: payload.map(|bytes| BASE64.encode(bytes)),
                        },
                        Err(err) => with_id(id, cache_error(cache, ErrorCode::CacheFailed, err)),
                    };
                    let _ = replies.send(reply);
                });
                Next::Continue
            }
            ClientMessage::CachePut {
                cache,
                key,
                payload,
            } => match BASE64.decode(payload) {
                Ok(payload) => self.write_cache(cache, key, Some(payload)).await,
                Err(err) => Next::Reply(cache_error(cache, ErrorCode::BadRequest, err)),
            },
            ClientMessage::CacheDelete { cache, key } => self.write_cache(cache, key, None).await,
            ClientMessage::CacheWatch { cache } => {
                let name = match scope.cache(&cache, CacheAction::Watch) {
                    Ok(bound) => bound.name,
                    Err(refusal) => {
                        return Next::Reply(cache_error(cache, ErrorCode::BadRequest, refusal));
                    }
                };
                let task = tokio::spawn(relay_cache(
                    cache.clone(),
                    name,
                    Arc::clone(&self.felix),
                    self.events.clone(),
                ));
                if let Some(previous) = self.watches.insert(cache, task) {
                    previous.abort();
                }
                Next::Continue
            }
            ClientMessage::Throttle { bits_per_second } => {
                if !scope.config().allow_throttle {
                    return Next::Reply(error(
                        ErrorCode::Unsupported,
                        "this gateway does not throttle",
                    ));
                }
                self.throttle.set(bits_per_second);
                Next::Continue
            }
            ClientMessage::Join {} => Next::Reply(error(ErrorCode::BadRequest, "already joined")),
            ClientMessage::Unsupported => Next::Reply(error(
                ErrorCode::Unsupported,
                "this gateway does not know that message type",
            )),
        }
    }

    async fn write_cache(&mut self, cache: String, key: String, payload: Option<Vec<u8>>) -> Next {
        let felix = Arc::clone(&self.felix);
        let bound = match felix.scope.cache(&cache, CacheAction::Write) {
            Ok(_) if !valid_name(&key) => {
                return Next::Reply(cache_error(cache, ErrorCode::BadRequest, BAD_NAME));
            }
            Ok(bound) => bound,
            Err(refusal) => return Next::Reply(cache_error(cache, ErrorCode::BadRequest, refusal)),
        };
        if let Err(refused) = self.charge(&bound, &cache, payload.as_ref().map_or(0, Vec::len)) {
            let code = self.refusal_code(refused, ErrorCode::CacheFailed);
            return Next::Reply(limited(cache_error(cache, code, ""), refused));
        }
        let ttl = bound.resource.ttl();
        let name = bound.name;
        self.write(Write::Cache {
            cache,
            name,
            key,
            payload,
            ttl,
        })
        .await
    }

    /// Check one write of `bytes` to `bound` against the payload limit and
    /// spend it from the session's rates.
    fn charge<A: Action>(
        &mut self,
        bound: &Bound<'_, A>,
        alias: &str,
        bytes: usize,
    ) -> Result<(), limits::Refused> {
        let config = &self.felix.scope.config().limits;
        if bytes > bound.resource.max_payload(config) {
            self.limits.metrics().record_refusal(Refusal::MessageSize);
            return Err(limits::Refused {
                limit: Refusal::MessageSize,
                retry_after: None,
            });
        }
        let cost = WriteCost {
            kind: A::KIND,
            alias,
            alias_rate: bound.resource.write_rate(),
            bytes,
        };
        self.limits.charge(&cost, std::time::Instant::now())
    }

    /// The code a refused write is answered with: `bad_request` for one too
    /// large, which retrying cannot fix, and otherwise `rate_limited`, or
    /// `fallback` for a session that did not ask for it.
    fn refusal_code(&self, refused: limits::Refused, fallback: ErrorCode) -> ErrorCode {
        match refused.limit {
            Refusal::MessageSize => ErrorCode::BadRequest,
            _ if self.rate_limited => ErrorCode::RateLimited,
            _ => fallback,
        }
    }

    async fn write(&self, write: Write) -> Next {
        match self.writes.send(write).await {
            Ok(()) => Next::Continue,
            Err(_) => Next::Stop,
        }
    }
}

/// `payload` prefixed with `principal` and its length as two big-endian
/// bytes, so a reader knows who published it.
fn stamped(principal: &str, payload: &[u8]) -> Vec<u8> {
    let principal = &principal.as_bytes()[..principal.len().min(usize::from(u16::MAX))];
    let length = u16::try_from(principal.len()).unwrap_or(u16::MAX);
    let mut stamped = Vec::with_capacity(2 + principal.len() + payload.len());
    stamped.extend_from_slice(&length.to_be_bytes());
    stamped.extend_from_slice(principal);
    stamped.extend_from_slice(payload);
    stamped
}

/// Write one at a time, in the order the browser sent them, so a session's
/// records reach the log in the order it sent them and a cache delete is
/// never overtaken by the write before it.
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
                name,
                payload,
                ack,
                id,
            } => {
                let started = Instant::now();
                match felix.publish(&name, payload, ack).await {
                    Ok(offset) if ack => {
                        metrics.record_felix_ack(&stream, started.elapsed());
                        ServerMessage::Ack { id, offset }
                    }
                    Ok(_) => continue,
                    Err(err) => with_id(id, stream_error(stream, ErrorCode::PublishFailed, err)),
                }
            }
            Write::Cache {
                cache,
                name,
                key,
                payload,
                ttl,
            } => {
                let written = match payload {
                    Some(payload) => felix.cache_put(&name, &key, payload, ttl).await,
                    None => felix.cache_delete(&name, &key).await,
                };
                match written {
                    Ok(()) => continue,
                    Err(err) => cache_error(cache, ErrorCode::CacheFailed, err),
                }
            }
        };
        let _ = replies.send(reply);
    }
}

/// A session the gateway has opened, ready to say `hello`.
struct Joined {
    felix: Felix,
    hello: ServerMessage,
    limits: SessionLimits,
    rate_limited: bool,
}

/// Wait for the browser's `join`, exchange its sign-in for a token that
/// reaches only that scope, and connect to Felix with it. Anything else first,
/// or a refusal, is answered with an error and ends the session.
async fn join<C: BrowserConnection>(
    conn: &mut C,
    gateway: &Gateway,
    client: Option<IpAddr>,
) -> Option<Joined> {
    let first = tokio::time::timeout(JOIN_TIMEOUT, async {
        loop {
            match conn.recv().await? {
                Incoming::Message(text) => return Some(text),
                Incoming::RoundTrip(_) => {}
            }
        }
    })
    .await
    .ok()??;
    let refusal = match serde_json::from_str(&first) {
        Ok(ClientMessage::Join {}) => match serde_json::from_str(&first) {
            Ok(join) => match accept(join, gateway, client).await {
                Ok(joined) => return Some(joined),
                Err(refusal) => refusal,
            },
            Err(err) => error(ErrorCode::BadRequest, err),
        },
        Ok(_) => error(ErrorCode::BadRequest, "join first"),
        Err(err) => error(ErrorCode::BadRequest, err),
    };
    let _ = send(conn, &refusal).await;
    None
}

/// Open the session `join` asks for, and the `hello` that answers it.
async fn accept(
    join: Join,
    gateway: &Gateway,
    client: Option<IpAddr>,
) -> Result<Joined, ServerMessage> {
    if join.protocol != PROTOCOL {
        return Err(error(
            ErrorCode::Unsupported,
            format!("this gateway speaks protocol {PROTOCOL}"),
        ));
    }
    let field = &gateway.scope.scope.field;
    let scope = join
        .fields
        .get(field)
        .and_then(|value| value.as_str())
        .and_then(|value| Scope::parse(&gateway.scope, value))
        .ok_or_else(|| error(ErrorCode::BadRequest, format!("{field}: {BAD_NAME}")))?;
    let rate_limited = join.features.iter().any(|feature| feature == RATE_LIMITED);
    // The address cap is checked before the token exchange, and the principal
    // cap, which needs the exchange's answer, before the Felix connection.
    let session_refused = |refused: limits::Refused| {
        let code = if rate_limited {
            ErrorCode::RateLimited
        } else {
            ErrorCode::Unavailable
        };
        limited(error(code, ""), refused)
    };
    let ip = gateway.limiter.admit_ip(client).map_err(session_refused)?;
    let grant = exchange(&scope, &join.token, gateway).await?;
    let principal = gateway
        .limiter
        .admit_principal(&grant.principal, std::time::Instant::now())
        .map_err(session_refused)?;
    let missing = grant.missing.clone();
    let felix = connect(scope, grant, gateway).await?;
    let hello = ServerMessage::Hello {
        protocol: PROTOCOL,
        features: join
            .features
            .into_iter()
            .filter(|feature| FEATURES.contains(&feature.as_str()))
            .collect(),
        namespace: gateway.brokers.namespace().to_string(),
        scope: BTreeMap::from([(field.clone(), felix.scope.value().to_string())]),
        cache_ttl_ms: gateway.scope.cache_ttl_ms(),
        missing,
    };
    Ok(Joined {
        felix,
        hello,
        limits: SessionLimits::new(principal, ip, std::time::Instant::now()),
        rate_limited,
    })
}

/// Exchange `token` for a grant that reaches `scope`. The error is the
/// message to answer the browser with.
pub(crate) async fn exchange(
    scope: &Scope,
    token: &str,
    gateway: &Gateway,
) -> Result<Grant, ServerMessage> {
    gateway
        .control_plane
        .exchange(token, scope)
        .await
        .map_err(|refusal| match refusal {
            Refused::SignedOut => error(ErrorCode::SignedOut, "sign in again"),
            Refused::Forbidden => error(ErrorCode::Forbidden, "not allowed in this scope"),
            Refused::Unavailable(err) => error(ErrorCode::Unavailable, err),
        })
}

/// Connect to Felix with `grant`. The error is the message to answer the
/// browser with.
pub(crate) async fn connect(
    scope: Scope,
    grant: Grant,
    gateway: &Gateway,
) -> Result<Felix, ServerMessage> {
    let principal = grant.principal.clone();
    match &gateway.shared {
        None => gateway
            .brokers
            .connect(gateway.control_plane.tokens(grant))
            .await
            .map(|client| Felix::new(client, &gateway.brokers, scope, principal)),
        Some(shared) => {
            let delegated =
                shared.actor.delegate(&grant.felix_token).await.map_err(
                    |refusal| match refusal {
                        Refused::Unavailable(err) => error(ErrorCode::Unavailable, err),
                        _ => error(ErrorCode::Unavailable, "the gateway cannot act for you"),
                    },
                )?;
            let tokens =
                gateway
                    .control_plane
                    .delegated_tokens(grant, delegated, Arc::clone(&shared.actor));
            Felix::shared(&shared.pool, tokens, &gateway.brokers, scope, principal).await
        }
    }
    .map_err(|err| error(ErrorCode::Unavailable, err))
}

async fn send<C: BrowserConnection>(conn: &mut C, message: &ServerMessage) -> anyhow::Result<()> {
    conn.send(serde_json::to_string(message).expect("server messages serialize"))
        .await
}

async fn relay_subscription(
    stream: String,
    name: String,
    from: StartAt,
    felix: Arc<Felix>,
    throttle: Arc<Throttle>,
    events: mpsc::Sender<ServerMessage>,
) {
    let mut subscription = match felix.subscribe(&name, from).await {
        Ok(subscription) => subscription,
        Err(err) => {
            let _ = events
                .send(subscription_error(stream, ErrorCode::SubscribeFailed, err))
                .await;
            return;
        }
    };
    let subscribed = ServerMessage::Subscribed {
        stream: stream.clone(),
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
                    stream: stream.clone(),
                    offset: event.offset,
                    skipped_before: event.skipped_before,
                    payload: BASE64.encode(&event.payload),
                };
                throttle.pass(&message).await;
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
fn subscription_error(stream: String, code: ErrorCode, err: anyhow::Error) -> ServerMessage {
    let trimmed = err
        .chain()
        .filter_map(|cause| cause.downcast_ref::<SubscribeCursorError>())
        .find(|cursor| cursor.reason == CursorErrorReason::TooOld);
    let mut message = stream_error(stream, code, &err);
    if let (Some(cursor), ServerMessage::Error { code, oldest, .. }) = (trimmed, &mut message) {
        *code = ErrorCode::Trimmed;
        *oldest = Some(cursor.available);
    }
    message
}

/// Relay a cache's entries: the current set as one message, then each change.
async fn relay_cache(
    cache: String,
    name: String,
    felix: Arc<Felix>,
    events: mpsc::Sender<ServerMessage>,
) {
    let mut watch = match felix.watch_cache(&name).await {
        Ok(watch) => watch,
        Err(err) => {
            let _ = events
                .send(cache_error(cache, ErrorCode::WatchFailed, err))
                .await;
            return;
        }
    };
    let mut retained = watch.retained_count().unwrap_or(0);
    let mut initial = Some(Vec::new());
    let ended = loop {
        if retained == 0
            && let Some(entries) = initial.take()
        {
            let entries = ServerMessage::CacheEntries {
                cache: cache.clone(),
                entries,
            };
            if events.send(entries).await.is_err() {
                return;
            }
        }
        match watch.recv().await {
            Some(CacheWatchItem::Change(change)) => {
                let entry = cache_entry(change);
                match initial.as_mut() {
                    Some(entries) => {
                        retained -= 1;
                        if entry.payload.is_some() {
                            entries.push(entry);
                        }
                    }
                    None => {
                        let change = ServerMessage::CacheChange {
                            cache: cache.clone(),
                            entry,
                        };
                        if events.send(change).await.is_err() {
                            return;
                        }
                    }
                }
            }
            Some(CacheWatchItem::Lagged { .. }) => break "the cache watch fell behind",
            // The watch follows a moved shard on the next recv.
            Some(CacheWatchItem::ShardMoved(_)) => {}
            None => break "the cache watch ended",
        }
    };
    let _ = events
        .send(cache_error(cache, ErrorCode::WatchFailed, ended))
        .await;
}

fn cache_entry(change: CacheChange) -> CacheEntry {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        });
    CacheEntry {
        key: change.key,
        payload: change.value.map(|value| BASE64.encode(value)),
        expires_in_ms: (change.expires_at_millis != 0)
            .then(|| change.expires_at_millis.saturating_sub(now)),
    }
}

fn error(code: ErrorCode, err: impl std::fmt::Display) -> ServerMessage {
    ServerMessage::Error {
        id: None,
        stream: None,
        cache: None,
        code,
        oldest: None,
        retry_after_ms: None,
        message: format!("{err:#}"),
    }
}

/// `message`, an error, saying which limit refused and when to retry.
pub(crate) fn limited(mut message: ServerMessage, refused: limits::Refused) -> ServerMessage {
    if let ServerMessage::Error {
        code,
        retry_after_ms,
        message: text,
        ..
    } = &mut message
    {
        *text = refused.message().to_string();
        if *code == ErrorCode::RateLimited {
            *retry_after_ms = refused
                .retry_after
                .map(|wait| u64::try_from(wait.as_micros().div_ceil(1000)).unwrap_or(u64::MAX));
        }
    }
    message
}

fn stream_error(stream: String, code: ErrorCode, err: impl std::fmt::Display) -> ServerMessage {
    let mut message = error(code, err);
    if let ServerMessage::Error { stream: about, .. } = &mut message {
        *about = Some(stream);
    }
    message
}

fn cache_error(cache: String, code: ErrorCode, err: impl std::fmt::Display) -> ServerMessage {
    let mut message = error(code, err);
    if let ServerMessage::Error { cache: about, .. } = &mut message {
        *about = Some(cache);
    }
    message
}

/// `message`, an error, as the answer to request `id`.
fn with_id(id: u64, mut message: ServerMessage) -> ServerMessage {
    if let ServerMessage::Error { id: about, .. } = &mut message {
        *about = Some(id);
    }
    message
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
        .context("subscribe to app.ops.lobby");
        let message = subscription_error("ops".into(), ErrorCode::SubscribeFailed, err);
        let ServerMessage::Error {
            code,
            oldest,
            stream,
            ..
        } = message
        else {
            panic!("{message:?}");
        };
        assert_eq!(
            (code, oldest, stream.as_deref()),
            (ErrorCode::Trimmed, Some(120), Some("ops"))
        );

        let future = anyhow::Error::new(SubscribeCursorError {
            reason: CursorErrorReason::InFuture,
            requested: 900,
            available: 10,
        });
        let message = subscription_error("ops".into(), ErrorCode::SubscribeFailed, future);
        let ServerMessage::Error { code, oldest, .. } = message else {
            panic!("{message:?}");
        };
        assert_eq!((code, oldest), (ErrorCode::SubscribeFailed, None));
    }

    #[test]
    fn a_limited_refusal_says_when_to_retry_only_under_its_own_code() {
        let refused = limits::Refused {
            limit: Refusal::SessionRate,
            retry_after: Some(Duration::from_micros(2_500)),
        };
        let new = limited(
            stream_error("ops".into(), ErrorCode::RateLimited, ""),
            refused,
        );
        let ServerMessage::Error {
            retry_after_ms,
            message,
            ..
        } = &new
        else {
            panic!("{new:?}");
        };
        assert_eq!(*retry_after_ms, Some(3), "rounded up, never early");
        assert_eq!(message, refused.message());

        let old = limited(
            stream_error("ops".into(), ErrorCode::PublishFailed, ""),
            refused,
        );
        let ServerMessage::Error { retry_after_ms, .. } = old else {
            panic!("{old:?}");
        };
        assert_eq!(retry_after_ms, None, "an old browser sees no new field");
    }

    #[test]
    fn a_stamp_carries_the_principal_and_its_length() {
        assert_eq!(stamped("ana", b"\x01\x02"), b"\x00\x03ana\x01\x02");
    }
}
