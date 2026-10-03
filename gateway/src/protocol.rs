//! The JSON messages a browser and the gateway exchange. `docs/protocol.md`
//! is the reference for what each one means on the wire.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The protocol version this gateway speaks. A `join` without one means 1.
pub const PROTOCOL: u32 = 1;

/// Optional capabilities a `join` may ask for in `features`. None exist yet;
/// `hello` answers with the ones asked for that are listed here.
pub const FEATURES: &[&str] = &[];

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
    /// the same stream on this connection.
    Subscribe { stream: String, from: StartAt },
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
        /// The first offset this subscription delivers, when the stream has a log.
        start_offset: Option<u64>,
        /// The stream's tail when the subscription was registered.
        live_offset: Option<u64>,
    },
    /// One record, in the order the broker delivered it.
    Event {
        stream: String,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        cache: Option<String>,
        code: ErrorCode,
        /// With [`ErrorCode::Trimmed`]: the oldest offset the log still holds.
        #[serde(skip_serializing_if = "Option::is_none")]
        oldest: Option<u64>,
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

fn is_zero(value: &u64) -> bool {
    *value == 0
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
                from: StartAt::Offset(42)
            }
        );
        assert_eq!(
            parse(json!({"type": "subscribe", "stream": "presence", "from": "live"})),
            ClientMessage::Subscribe {
                stream: "presence".into(),
                from: StartAt::Live(Live::Live)
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
    fn serializes_events_without_a_zero_skip_count() {
        let event = ServerMessage::Event {
            stream: "ops".into(),
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
}
