//! The JSON messages a browser and the gateway exchange. `docs/protocol.md`
//! is the reference for what each one means on the wire.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The protocol version this gateway speaks. A `join` without one means 1.
pub const PROTOCOL: u32 = 1;

/// Optional capabilities a `join` may ask for in `features`; `hello` answers
/// with the ones asked for that are listed here.
pub const FEATURES: &[&str] = &[RATE_LIMITED, BINARY_EVENTS, SLOTS, STATE, READ, SHARDS];

/// The feature under which a write or join refused by a limit is answered
/// with [`ErrorCode::RateLimited`] and `retry_after_ms`. Without it the
/// refusal uses the request's own failure code.
pub const RATE_LIMITED: &str = "rate_limited";

/// The feature under which events arrive as binary WebSocket messages laid
/// out as [`EventFrame`], instead of JSON `event` messages.
pub const BINARY_EVENTS: &str = "binary_events";

/// The feature under which a `subscribe` may name a `slot`, so one stream can
/// hold several subscriptions on one connection.
pub const SLOTS: &str = "slots";

/// The feature under which a session may read a stream's state with
/// `state_get`: the keys an atomic commit writes alongside its event.
pub const STATE: &str = "state";

/// The feature under which a session may read a range of a durable stream
/// with `read`, without subscribing.
pub const READ: &str = "read";

/// The feature under which a `subscribe` may name the `shard` of a stream to
/// follow.
pub const SHARDS: &str = "shards";

/// Where a subscription starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum StartAt {
    /// The first log offset to deliver: the last one the browser handled, plus one.
    Offset(u64),
    /// Only what is published from now on.
    Live(Live),
}

/// The `"live"` literal of [`StartAt`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Live {
    Live,
}

/// The first message on every connection. The scope's value is under the key
/// the scope file names, so it is collected with every other key in `fields`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Join {
    /// The browser's OpenID Connect ID token.
    pub token: String,
    #[serde(default = "first_protocol")]
    pub protocol: u32,
    #[serde(default)]
    pub features: Vec<String>,
    #[serde(flatten)]
    pub fields: Map<String, Value>,
}

fn first_protocol() -> u32 {
    1
}

/// A message from the browser. Streams, caches and counters are named by
/// their alias in the scope file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// See [`Join`]. Answered with [`ServerMessage::Hello`], or with an error
    /// before the gateway closes.
    Join {},
    /// Start relaying a stream's events. Replaces any earlier subscription to
    /// the same stream and slot on this connection.
    Subscribe {
        stream: String,
        from: StartAt,
        /// Leaving it out names the stream's one default slot.
        #[serde(default)]
        slot: Option<String>,
        /// With [`SHARDS`]: the shard whose offsets `from` is. Leaving it out
        /// means shard 0.
        #[serde(default)]
        shard: Option<u32>,
    },
    /// Publish one record. `payload` is base64. With `ack`, the gateway
    /// answers with an [`ServerMessage::Ack`] carrying the same `id`.
    Publish {
        stream: String,
        payload: String,
        ack: bool,
        id: u64,
    },
    /// Add `delta` to a counter. The gateway answers with a
    /// [`ServerMessage::Counter`] carrying the same `id` and the new sum.
    CounterAdd {
        counter: String,
        key: String,
        delta: i64,
        id: u64,
    },
    /// Read one cache entry. Answered with a [`ServerMessage::CacheValue`]
    /// carrying the same `id`.
    CacheGet { cache: String, key: String, id: u64 },
    /// With [`STATE`]: read one key of a stream's state. Answered with a
    /// [`ServerMessage::StateValue`] carrying the same `id`.
    StateGet {
        stream: String,
        key: String,
        /// Picks the shard, as a commit's entity key does. Leaving it out
        /// means the empty key, which is enough for a stream of one shard.
        #[serde(default)]
        entity: String,
        id: u64,
    },
    /// With [`READ`]: one page of a durable stream's records from `from`,
    /// stopping before `end`. Answered with a [`ServerMessage::Records`]
    /// carrying the same `id`.
    Read {
        stream: String,
        from: u64,
        /// Leaving it out reads up to what is committed.
        #[serde(default)]
        end: Option<u64>,
        /// The most records to answer with; leaving it out or 0 takes the
        /// broker's cap.
        #[serde(default)]
        max: u32,
        /// The shard whose offsets `from` and `end` are. Leaving it out means
        /// shard 0, the only one of a stream of one shard.
        #[serde(default)]
        shard: u32,
        id: u64,
    },
    /// Write one cache entry, expiring after the cache's TTL when it has one.
    CachePut {
        cache: String,
        key: String,
        payload: String,
    },
    /// Delete one cache entry.
    CacheDelete { cache: String, key: String },
    /// Send the cache's entries as one [`ServerMessage::CacheEntries`], then
    /// every change as a [`ServerMessage::CacheChange`]. Replaces an earlier
    /// watch of the same cache.
    CacheWatch { cache: String },
    /// Read this connection's subscriptions no faster than `bits_per_second`
    /// would carry them, or at full speed again with `null` or 0. It stands
    /// in for a slow link.
    Throttle { bits_per_second: Option<u64> },
    /// A type this gateway does not know, answered with
    /// [`ErrorCode::Unsupported`].
    #[serde(other)]
    Unsupported,
}

/// A message to the browser.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// The answer to a join: the session is open on this scope.
    Hello {
        protocol: u32,
        /// The features of the join this gateway accepted.
        features: Vec<String>,
        namespace: String,
        /// The scope's value, under the scope file's `field`.
        #[serde(flatten)]
        scope: BTreeMap<String, String>,
        /// How long an entry lasts without a write, for each cache with a TTL.
        cache_ttl_ms: BTreeMap<String, u64>,
        /// Optional resources this session's sign-in does not reach.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        missing: Vec<String>,
    },
    /// The subscription is registered with the broker: anything published
    /// from here on will be delivered.
    Subscribed {
        stream: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        slot: Option<String>,
        /// The shard, when the `subscribe` named one.
        #[serde(skip_serializing_if = "Option::is_none")]
        shard: Option<u32>,
        /// With [`BINARY_EVENTS`]: the number this subscription's event
        /// frames carry.
        #[serde(skip_serializing_if = "Option::is_none")]
        sub: Option<u32>,
        /// The first offset this subscription delivers, when the stream has a log.
        start_offset: Option<u64>,
        /// The stream's tail when the subscription was registered.
        live_offset: Option<u64>,
    },
    /// One record, in the order the broker delivered it.
    Event {
        stream: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        slot: Option<String>,
        /// Log offset; `null` on a stream with no log.
        offset: Option<u64>,
        /// Offsets just before this one that hold no event. A gap in offsets
        /// larger than this is a drop.
        #[serde(skip_serializing_if = "is_zero")]
        skipped_before: u64,
        payload: String,
    },
    /// The broker accepted the publish with this `id`.
    Ack {
        id: u64,
        /// The record's log offset, when the broker acknowledged after writing it.
        offset: Option<u64>,
    },
    /// The sum after the counter add with this `id`.
    Counter { id: u64, value: i64 },
    /// The entry for the `cache_get` with this `id`, base64, or `null` when
    /// there is none.
    CacheValue { id: u64, payload: Option<String> },
    /// The key for the `state_get` with this `id`.
    StateValue {
        id: u64,
        /// The value, base64, or `null` when the key was never written or
        /// was deleted.
        payload: Option<String>,
        /// The offset of the commit that wrote the value.
        version: Option<u64>,
        /// The offset of the last commit the answer reflects.
        as_of: Option<u64>,
    },
    /// The page for the `read` with this `id`.
    Records {
        id: u64,
        /// In offset order.
        records: Vec<Record>,
        /// Where the next page starts. At or past the request's `end` once
        /// the range is done; not always the last offset plus one, since
        /// offsets that hold no record are passed over.
        next_offset: u64,
    },
    /// Every entry in the cache when the watch started.
    CacheEntries {
        cache: String,
        entries: Vec<CacheEntry>,
    },
    /// One cache entry was written or deleted.
    CacheChange {
        cache: String,
        #[serde(flatten)]
        entry: CacheEntry,
    },
    /// Something the browser asked for failed.
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        stream: Option<String>,
        /// The subscription's slot, on an error about a subscription that
        /// named one.
        #[serde(skip_serializing_if = "Option::is_none")]
        slot: Option<String>,
        /// The subscription's shard, on an error about a subscription that
        /// named one.
        #[serde(skip_serializing_if = "Option::is_none")]
        shard: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache: Option<String>,
        code: ErrorCode,
        /// With [`ErrorCode::Trimmed`]: the oldest offset the log still holds.
        #[serde(skip_serializing_if = "Option::is_none")]
        oldest: Option<u64>,
        /// With [`ErrorCode::RateLimited`]: milliseconds until the write
        /// would fit.
        #[serde(skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
        message: String,
    },
}

/// What kind of failure a [`ServerMessage::Error`] reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The message did not parse, named something the scope does not allow,
    /// or its payload was not base64.
    BadRequest,
    /// The message type, protocol version or request is not one this
    /// gateway serves.
    Unsupported,
    /// Felix refused or lost the publish. It may or may not have landed.
    PublishFailed,
    /// Felix refused the subscribe, for example an offset already trimmed.
    SubscribeFailed,
    /// A subscription stopped delivering. Subscribe again from the last
    /// offset handled plus one.
    SubscriptionEnded,
    /// Felix refused or lost a counter add. It may have been counted.
    CounterFailed,
    /// Felix refused or lost a cache read, write or delete.
    CacheFailed,
    /// Felix refused or could not answer a state read.
    StateFailed,
    /// Felix refused or could not answer a range read.
    ReadFailed,
    /// The subscription asked for an offset the log no longer holds.
    Trimmed,
    /// The join carried no usable sign-in. Sign in again.
    SignedOut,
    /// The signed-in user may not open this scope.
    Forbidden,
    /// The join could not be completed for now. Try again.
    Unavailable,
    /// A cache watch was refused or stopped. Watch again.
    WatchFailed,
    /// A gateway limit refused the write or the join; nothing reached Felix.
    /// Only for sessions that asked for [`RATE_LIMITED`].
    RateLimited,
}

/// One cache entry, keyed without the cache's name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheEntry {
    pub key: String,
    /// The entry's value, base64; `null` when it was deleted.
    pub payload: Option<String>,
    /// Milliseconds until the entry expires, by the gateway's clock; `null`
    /// for an entry that never does.
    pub expires_in_ms: Option<u64>,
}

/// One record of a [`ServerMessage::Records`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Record {
    pub offset: u64,
    /// The record's bytes, base64.
    pub payload: String,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// One event as a binary WebSocket message, for sessions with
/// [`BINARY_EVENTS`]. Integers are little-endian:
///
/// | Bytes | Field |
/// |---|---|
/// | 0 | Frame kind, 1 for an event |
/// | 1 | Header length, [`EventFrame::HEADER_LEN`]; the payload starts there |
/// | 2..4 | Reserved, zero |
/// | 4..8 | `sub`, from the subscription's `subscribed` |
/// | 8..16 | Log offset, or `u64::MAX` on a stream with no log |
/// | 16..20 | `skipped_before`, saturating |
/// | 20..24 | Reserved, zero |
/// | 24..32 | When the gateway received the event from Felix |
/// | 32..40 | When the gateway handed the frame to the WebSocket |
///
/// Both times are microseconds since the Unix epoch on the gateway's clock.
/// A reader must skip the header by its length byte, so later versions can
/// append fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventFrame {
    pub sub: u32,
    pub offset: Option<u64>,
    pub skipped_before: u64,
    pub received_us: u64,
    pub sent_us: u64,
}

impl EventFrame {
    /// The frame kind of an event.
    pub const KIND: u8 = 1;
    /// The length of the header this gateway writes.
    pub const HEADER_LEN: usize = 40;

    /// The header followed by `payload`.
    pub fn encode(&self, payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(Self::HEADER_LEN + payload.len());
        frame.extend_from_slice(&[Self::KIND, Self::HEADER_LEN as u8, 0, 0]);
        frame.extend_from_slice(&self.sub.to_le_bytes());
        frame.extend_from_slice(&self.offset.unwrap_or(u64::MAX).to_le_bytes());
        let skipped = u32::try_from(self.skipped_before).unwrap_or(u32::MAX);
        frame.extend_from_slice(&skipped.to_le_bytes());
        frame.extend_from_slice(&[0; 4]);
        frame.extend_from_slice(&self.received_us.to_le_bytes());
        frame.extend_from_slice(&self.sent_us.to_le_bytes());
        frame.extend_from_slice(payload);
        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(message: Value) -> ClientMessage {
        serde_json::from_value(message).unwrap()
    }

    #[test]
    fn parses_subscribe_from_an_offset_and_from_live() {
        assert_eq!(
            parse(json!({"type": "subscribe", "stream": "ops", "from": 42})),
            ClientMessage::Subscribe {
                stream: "ops".into(),
                from: StartAt::Offset(42),
                slot: None,
                shard: None,
            }
        );
        assert_eq!(
            parse(json!({"type": "subscribe", "stream": "presence", "from": "live"})),
            ClientMessage::Subscribe {
                stream: "presence".into(),
                from: StartAt::Live(Live::Live),
                slot: None,
                shard: None,
            }
        );
        assert_eq!(
            parse(json!({"type": "subscribe", "stream": "ops", "from": 7, "slot": "replay"})),
            ClientMessage::Subscribe {
                stream: "ops".into(),
                from: StartAt::Offset(7),
                slot: Some("replay".into()),
                shard: None,
            }
        );
    }

    #[test]
    fn a_join_without_a_protocol_is_version_1() {
        let message = json!({"type": "join", "room": "lobby", "token": "eyJ"});
        assert_eq!(parse(message.clone()), ClientMessage::Join {});
        let join: Join = serde_json::from_value(message).unwrap();
        assert_eq!((join.protocol, join.features.len()), (1, 0));
        assert_eq!(join.token, "eyJ");
        assert_eq!(join.fields["room"], "lobby");
    }

    #[test]
    fn unknown_types_parse_as_unsupported() {
        assert_eq!(
            parse(json!({"type": "x.teleport", "to": 3})),
            ClientMessage::Unsupported
        );
    }

    #[test]
    fn parses_counter_and_cache_requests() {
        assert_eq!(
            parse(json!({
                "type": "counter_add", "counter": "seq", "key": "00ff", "delta": 256, "id": 4
            })),
            ClientMessage::CounterAdd {
                counter: "seq".into(),
                key: "00ff".into(),
                delta: 256,
                id: 4
            }
        );
        assert_eq!(
            parse(json!({"type": "cache_get", "cache": "snap", "key": "latest", "id": 5})),
            ClientMessage::CacheGet {
                cache: "snap".into(),
                key: "latest".into(),
                id: 5
            }
        );
        assert_eq!(
            parse(json!({"type": "cache_put", "cache": "members", "key": "a", "payload": "AQI="})),
            ClientMessage::CachePut {
                cache: "members".into(),
                key: "a".into(),
                payload: "AQI=".into()
            }
        );
        assert_eq!(
            parse(json!({"type": "cache_watch", "cache": "members"})),
            ClientMessage::CacheWatch {
                cache: "members".into()
            }
        );
    }

    #[test]
    fn parses_a_state_read_and_serializes_its_answer() {
        assert_eq!(
            parse(json!({"type": "state_get", "stream": "ops", "key": "match/3", "id": 6})),
            ClientMessage::StateGet {
                stream: "ops".into(),
                key: "match/3".into(),
                entity: String::new(),
                id: 6
            }
        );
        assert_eq!(
            parse(
                json!({"type": "state_get", "stream": "ops", "key": "k", "entity": "e", "id": 7})
            ),
            ClientMessage::StateGet {
                stream: "ops".into(),
                key: "k".into(),
                entity: "e".into(),
                id: 7
            }
        );
        let value = ServerMessage::StateValue {
            id: 6,
            payload: Some("AQI=".into()),
            version: Some(40),
            as_of: Some(42),
        };
        assert_eq!(
            serde_json::to_value(&value).unwrap(),
            json!({"type": "state_value", "id": 6, "payload": "AQI=", "version": 40, "as_of": 42})
        );
    }

    #[test]
    fn parses_a_range_read_and_serializes_its_answer() {
        assert_eq!(
            parse(json!({"type": "read", "stream": "ops", "from": 40, "id": 8})),
            ClientMessage::Read {
                stream: "ops".into(),
                from: 40,
                end: None,
                max: 0,
                shard: 0,
                id: 8
            }
        );
        assert_eq!(
            parse(
                json!({"type": "read", "stream": "ops", "from": 40, "end": 52, "max": 10, "shard": 1, "id": 9})
            ),
            ClientMessage::Read {
                stream: "ops".into(),
                from: 40,
                end: Some(52),
                max: 10,
                shard: 1,
                id: 9
            }
        );
        let page = ServerMessage::Records {
            id: 9,
            records: vec![Record {
                offset: 40,
                payload: "AQI=".into(),
            }],
            next_offset: 41,
        };
        assert_eq!(
            serde_json::to_value(&page).unwrap(),
            json!({"type": "records", "id": 9, "records": [{"offset": 40, "payload": "AQI="}], "next_offset": 41})
        );
    }

    #[test]
    fn parses_a_throttle_and_its_release() {
        assert_eq!(
            parse(json!({"type": "throttle", "bits_per_second": 100_000})),
            ClientMessage::Throttle {
                bits_per_second: Some(100_000)
            }
        );
        assert_eq!(
            parse(json!({"type": "throttle", "bits_per_second": null})),
            ClientMessage::Throttle {
                bits_per_second: None
            }
        );
    }

    #[test]
    fn serializes_hello_with_the_scope_under_its_field() {
        let hello = ServerMessage::Hello {
            protocol: 1,
            features: vec![],
            namespace: "default".into(),
            scope: BTreeMap::from([("room".into(), "lobby".into())]),
            cache_ttl_ms: BTreeMap::from([("members".into(), 30_000)]),
            missing: vec![],
        };
        assert_eq!(
            serde_json::to_value(&hello).unwrap(),
            json!({
                "type": "hello", "protocol": 1, "features": [], "namespace": "default",
                "room": "lobby", "cache_ttl_ms": {"members": 30000}
            })
        );
    }

    #[test]
    fn serializes_cache_answers_flat() {
        let none = ServerMessage::CacheValue {
            id: 5,
            payload: None,
        };
        assert_eq!(
            serde_json::to_value(&none).unwrap(),
            json!({"type": "cache_value", "id": 5, "payload": null})
        );
        let left = ServerMessage::CacheChange {
            cache: "members".into(),
            entry: CacheEntry {
                key: "00ff".into(),
                payload: None,
                expires_in_ms: None,
            },
        };
        assert_eq!(
            serde_json::to_value(&left).unwrap(),
            json!({
                "type": "cache_change", "cache": "members", "key": "00ff",
                "payload": null, "expires_in_ms": null
            })
        );
    }

    #[test]
    fn rejects_known_types_with_bad_fields() {
        for bad in [
            json!({"type": "subscribe", "stream": "ops", "from": "earliest"}),
            json!({"type": "subscribe", "stream": "ops", "from": -1}),
            json!({"type": "publish", "stream": "ops", "payload": "", "ack": true}),
            json!({"type": "cache_get", "cache": "snap", "id": 1}),
            json!({"stream": "ops"}),
        ] {
            assert!(
                serde_json::from_value::<ClientMessage>(bad.clone()).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn serializes_a_rate_limited_refusal_with_when_to_retry() {
        let refusal = ServerMessage::Error {
            id: Some(7),
            stream: Some("ops".into()),
            slot: None,
            shard: None,
            cache: None,
            code: ErrorCode::RateLimited,
            oldest: None,
            retry_after_ms: Some(120),
            message: "slow down".into(),
        };
        assert_eq!(
            serde_json::to_value(&refusal).unwrap(),
            json!({
                "type": "error", "id": 7, "stream": "ops", "code": "rate_limited",
                "retry_after_ms": 120, "message": "slow down"
            })
        );
    }

    #[test]
    fn serializes_events_without_a_zero_skip_count() {
        let event = ServerMessage::Event {
            stream: "ops".into(),
            slot: None,
            offset: Some(7),
            skipped_before: 0,
            payload: "AQI=".into(),
        };
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            json!({"type": "event", "stream": "ops", "offset": 7, "payload": "AQI="})
        );
        let ack = ServerMessage::Ack {
            id: 3,
            offset: None,
        };
        assert_eq!(
            serde_json::to_value(&ack).unwrap(),
            json!({"type": "ack", "id": 3, "offset": null})
        );
    }

    #[test]
    fn an_event_frame_is_its_header_then_the_payload() {
        let frame = EventFrame {
            sub: 3,
            offset: Some(0x0102),
            skipped_before: 1,
            received_us: 10,
            sent_us: 11,
        }
        .encode(b"tick");
        assert_eq!(frame.len(), EventFrame::HEADER_LEN + 4);
        assert_eq!(&frame[..4], [1, 40, 0, 0]);
        assert_eq!(&frame[4..8], 3u32.to_le_bytes());
        assert_eq!(&frame[8..16], 0x0102u64.to_le_bytes());
        assert_eq!(&frame[16..20], 1u32.to_le_bytes());
        assert_eq!(&frame[24..32], 10u64.to_le_bytes());
        assert_eq!(&frame[32..40], 11u64.to_le_bytes());
        assert_eq!(&frame[40..], b"tick");

        let unlogged = EventFrame {
            sub: 0,
            offset: None,
            skipped_before: u64::MAX,
            received_us: 0,
            sent_us: 0,
        }
        .encode(b"");
        assert_eq!(&unlogged[8..16], u64::MAX.to_le_bytes());
        assert_eq!(&unlogged[16..20], u32::MAX.to_le_bytes());
    }

    #[test]
    fn a_subscription_in_a_slot_is_named_in_its_answers() {
        let subscribed = ServerMessage::Subscribed {
            stream: "ticks".into(),
            slot: Some("replay".into()),
            shard: None,
            sub: Some(2),
            start_offset: Some(5),
            live_offset: Some(9),
        };
        assert_eq!(
            serde_json::to_value(&subscribed).unwrap(),
            json!({
                "type": "subscribed", "stream": "ticks", "slot": "replay", "sub": 2,
                "start_offset": 5, "live_offset": 9
            })
        );
    }

    #[test]
    fn a_subscribe_names_its_shard_only_when_the_browser_does() {
        let named = parse(json!({"type": "subscribe", "stream": "ops", "from": 3, "shard": 2}));
        assert!(matches!(
            named,
            ClientMessage::Subscribe { shard: Some(2), .. }
        ));
        let subscribed = |shard| ServerMessage::Subscribed {
            stream: "ops".into(),
            slot: None,
            shard,
            sub: None,
            start_offset: Some(3),
            live_offset: Some(9),
        };
        assert_eq!(
            serde_json::to_value(subscribed(Some(2))).unwrap(),
            json!({
                "type": "subscribed", "stream": "ops", "shard": 2,
                "start_offset": 3, "live_offset": 9
            })
        );
        assert_eq!(
            serde_json::to_value(subscribed(None)).unwrap(),
            json!({"type": "subscribed", "stream": "ops", "start_offset": 3, "live_offset": 9})
        );
    }
}
