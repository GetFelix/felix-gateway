//! The JSON messages a browser and the gateway exchange. `docs/protocol.md`
//! is the reference for what each one means on the wire.

use serde::{Deserialize, Serialize};

/// Which of the room's two streams a message is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamName {
    /// The durable edit log, `canvas.ops.<room>`.
    Ops,
    /// The in-memory cursor and presence feed, `canvas.presence.<room>`.
    Presence,
}

/// A counter the browser may add to. Each maps to one of the room's Felix
/// counter caches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CounterName {
    /// Per-session op sequence numbers, `canvas.seq.<room>/<key>`.
    Seq,
}

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

/// A message from the browser.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// The first message on every connection: which room to open, and the
    /// browser's OpenID Connect ID token. Answered with
    /// [`ServerMessage::Hello`], or with an error before the gateway closes.
    Join { room: String, token: String },
    /// Start relaying a stream's events. Replaces any earlier subscription to
    /// the same stream on this connection.
    Subscribe { stream: StreamName, from: StartAt },
    /// Publish one record. `payload` is base64. With `ack`, the gateway
    /// answers with an [`ServerMessage::Ack`] carrying the same `id`.
    Publish {
        stream: StreamName,
        payload: String,
        ack: bool,
        id: u64,
    },
    /// Add `delta` to a counter. The gateway answers with a
    /// [`ServerMessage::Counter`] carrying the same `id` and the new sum.
    CounterAdd {
        counter: CounterName,
        key: String,
        delta: i64,
        id: u64,
    },
    /// Read the room's snapshot. The gateway answers with a
    /// [`ServerMessage::Snapshot`] carrying the same `id`.
    SnapshotGet { id: u64 },
    /// Write this session's member entry, `canvas.members.<room>/<key>`,
    /// expiring after the gateway's member TTL unless written again.
    SetMember { key: String, payload: String },
    /// Delete a member entry, for a session that is closing.
    RemoveMember { key: String },
    /// Send the room's members as one [`ServerMessage::Members`], then every
    /// change as a [`ServerMessage::Member`]. Replaces an earlier watch.
    WatchMembers,
    /// Read this connection's subscriptions no faster than `bits_per_second`
    /// would carry them, or at full speed again with `null` or 0. It stands
    /// in for a slow link.
    Throttle { bits_per_second: Option<u64> },
}

/// A message to the browser.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// The answer to [`ClientMessage::Join`]: the session is open on this room.
    Hello {
        namespace: String,
        room: String,
        /// How long a member entry lasts without a refresh.
        member_ttl_ms: u64,
    },
    /// The subscription is registered with the broker: anything published
    /// from here on will be delivered.
    Subscribed {
        stream: StreamName,
        /// The first offset this subscription delivers, when the stream has a log.
        start_offset: Option<u64>,
        /// The stream's tail when the subscription was registered.
        live_offset: Option<u64>,
    },
    /// One record, in the order the broker delivered it.
    Event {
        stream: StreamName,
        /// Log offset; `null` on the presence stream, which has no log.
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
    /// The room's snapshot for the `snapshot_get` with this `id`: base64 bytes
    /// as the snapshotter wrote them, or `null` when it has written none.
    Snapshot { id: u64, payload: Option<String> },
    /// Every member entry in the room when the watch started.
    Members { members: Vec<MemberEntry> },
    /// One member entry was written or deleted.
    Member(MemberEntry),
    /// Something the browser asked for failed.
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        stream: Option<StreamName>,
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
    /// The message did not parse, or its payload was not base64.
    BadRequest,
    /// Felix refused or lost the publish. It may or may not have landed.
    PublishFailed,
    /// Felix refused the subscribe, for example an offset already trimmed.
    SubscribeFailed,
    /// A subscription stopped delivering. Subscribe again from the last
    /// offset handled plus one.
    SubscriptionEnded,
    /// Felix refused or lost a counter add. It may have been counted.
    CounterFailed,
    /// The snapshot could not be read.
    SnapshotFailed,
    /// The subscription asked for an offset the log no longer holds. Start
    /// again from the snapshot.
    Trimmed,
    /// The join carried no usable sign-in. Sign in again.
    SignedOut,
    /// The signed-in user may not open this room.
    Forbidden,
    /// The join could not be completed for now. Try again.
    Unavailable,
    /// Felix refused or lost a member write or delete.
    MemberFailed,
    /// The member watch was refused or stopped. Watch again.
    WatchFailed,
}

/// One member entry, keyed without the room prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MemberEntry {
    pub key: String,
    /// The entry's value, base64; `null` when it was deleted.
    pub payload: Option<String>,
    /// Milliseconds until the entry expires, by the gateway's clock; `null`
    /// for an entry that never does. Felix sends nothing when an entry
    /// expires, so the browser drops it itself at this deadline.
    pub expires_in_ms: Option<u64>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_subscribe_from_an_offset_and_from_live() {
        let at: ClientMessage =
            serde_json::from_value(json!({"type": "subscribe", "stream": "ops", "from": 42}))
                .unwrap();
        assert_eq!(
            at,
            ClientMessage::Subscribe {
                stream: StreamName::Ops,
                from: StartAt::Offset(42)
            }
        );
        let live: ClientMessage = serde_json::from_value(
            json!({"type": "subscribe", "stream": "presence", "from": "live"}),
        )
        .unwrap();
        assert_eq!(
            live,
            ClientMessage::Subscribe {
                stream: StreamName::Presence,
                from: StartAt::Live(Live::Live)
            }
        );
    }

    #[test]
    fn parses_a_join() {
        let join: ClientMessage =
            serde_json::from_value(json!({"type": "join", "room": "lobby", "token": "eyJ"}))
                .unwrap();
        assert_eq!(
            join,
            ClientMessage::Join {
                room: "lobby".into(),
                token: "eyJ".into()
            }
        );
    }

    #[test]
    fn parses_a_counter_add() {
        let add: ClientMessage = serde_json::from_value(json!({
            "type": "counter_add", "counter": "seq", "key": "00ff", "delta": 256, "id": 4
        }))
        .unwrap();
        assert_eq!(
            add,
            ClientMessage::CounterAdd {
                counter: CounterName::Seq,
                key: "00ff".into(),
                delta: 256,
                id: 4
            }
        );
    }

    #[test]
    fn parses_a_snapshot_get_and_serializes_the_answer() {
        let get: ClientMessage =
            serde_json::from_value(json!({"type": "snapshot_get", "id": 5})).unwrap();
        assert_eq!(get, ClientMessage::SnapshotGet { id: 5 });
        let none = ServerMessage::Snapshot {
            id: 5,
            payload: None,
        };
        assert_eq!(
            serde_json::to_value(&none).unwrap(),
            json!({"type": "snapshot", "id": 5, "payload": null})
        );
    }

    #[test]
    fn parses_member_messages() {
        let set: ClientMessage =
            serde_json::from_value(json!({"type": "set_member", "key": "00ff", "payload": "AQI="}))
                .unwrap();
        assert_eq!(
            set,
            ClientMessage::SetMember {
                key: "00ff".into(),
                payload: "AQI=".into()
            }
        );
        let watch: ClientMessage =
            serde_json::from_value(json!({"type": "watch_members"})).unwrap();
        assert_eq!(watch, ClientMessage::WatchMembers);
    }

    #[test]
    fn parses_a_throttle_and_its_release() {
        let on: ClientMessage =
            serde_json::from_value(json!({"type": "throttle", "bits_per_second": 100_000}))
                .unwrap();
        assert_eq!(
            on,
            ClientMessage::Throttle {
                bits_per_second: Some(100_000)
            }
        );
        let off: ClientMessage =
            serde_json::from_value(json!({"type": "throttle", "bits_per_second": null})).unwrap();
        assert_eq!(
            off,
            ClientMessage::Throttle {
                bits_per_second: None
            }
        );
    }

    #[test]
    fn serializes_a_member_change_flat() {
        let left = ServerMessage::Member(MemberEntry {
            key: "00ff".into(),
            payload: None,
            expires_in_ms: None,
        });
        assert_eq!(
            serde_json::to_value(&left).unwrap(),
            json!({"type": "member", "key": "00ff", "payload": null, "expires_in_ms": null})
        );
    }

    #[test]
    fn rejects_unknown_streams_and_positions() {
        for bad in [
            json!({"type": "subscribe", "stream": "chat", "from": "live"}),
            json!({"type": "subscribe", "stream": "ops", "from": "earliest"}),
            json!({"type": "subscribe", "stream": "ops", "from": -1}),
            json!({"type": "publish", "stream": "ops", "payload": "", "ack": true}),
            json!({"type": "counter_add", "counter": "views", "key": "a", "delta": 1, "id": 1}),
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
            stream: StreamName::Ops,
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
