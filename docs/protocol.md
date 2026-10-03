# Browser to gateway protocol

A browser opens one WebSocket to the gateway at `/ws` and exchanges JSON text
frames over it. The gateway relays each request to Felix and each Felix event
back, without decoding payloads. One gateway serves one room, named by
`CANVAS_ROOM`.

The protocol has two streams:

| Name | Felix stream | Kind | Offsets |
|---|---|---|---|
| `ops` | `canvas.ops.<room>` | Durable, retained 30 days | Every event has one |
| `presence` | `canvas.presence.<room>` | In memory, at most once | Always `null` |

## Browser to gateway

### `subscribe`

```json
{"type": "subscribe", "stream": "ops", "from": 42}
{"type": "subscribe", "stream": "presence", "from": "live"}
```

| Field | Meaning |
|---|---|
| `stream` | `"ops"` or `"presence"` |
| `from` | `"live"` for only what is published from now on, or a log offset: the first record wanted, which is the last offset handled plus one |

Subscribing to a stream the connection is already subscribed to replaces the
earlier subscription. That is the recovery path: after a gap or an ended
subscription, subscribe again from the last offset handled plus one.

### `publish`

```json
{"type": "publish", "stream": "ops", "payload": "haNzaWTP...", "ack": true, "id": 7}
```

| Field | Meaning |
|---|---|
| `stream` | `"ops"` or `"presence"` |
| `payload` | The record, base64. The gateway passes the bytes through unread. |
| `ack` | `true` to be told when Felix has the record. `false` publishes fire-and-forget, which is what cursors want. |
| `id` | An integer chosen by the browser. It comes back on the `ack` or `error` for this publish. |

A connection's publishes reach Felix one at a time, in the order the browser
sent them, so one session's ops land in the log in its own `seq` order. The
gateway never resends a publish. When one fails the browser decides whether to
retry, because only the op's `(sid, seq)` makes a retry safe to deduplicate.

## Gateway to browser

### `subscribed`

```json
{"type": "subscribed", "stream": "ops", "start_offset": 42, "live_offset": 57}
```

The subscription is registered with the broker. Anything published from this
point on will be delivered, which is what the join path's subscribe-before-read
rule waits for. `start_offset` is the first offset this subscription delivers
and `live_offset` is the stream's tail when it was registered; both are `null`
on `presence`.

### `event`

```json
{"type": "event", "stream": "ops", "offset": 42, "payload": "haNzaWTP..."}
```

One record, in the order the broker delivered it. On `ops`, events arrive in
increasing offset order. `skipped_before` appears only when it is not zero: it
counts the offsets just before this one that hold no event (a new leader's
generation-start record), so the number of records dropped before this event is
`offset - previous_offset - 1 - skipped_before`. Anything above zero is a drop
the browser must recover from by subscribing again.

Offsets are JSON numbers. They are exact in JavaScript up to 2^53, far beyond
what one room's log reaches.

### `ack`

```json
{"type": "ack", "id": 7, "offset": 42}
```

Felix accepted publish `id`. `offset` is the record's log offset when the broker
had one to give, and `null` when:

- the stream is `presence`, which has no log;
- the broker acknowledged when it queued the record rather than after writing
  it. That is Felix's default; the development stack in `dev/` sets
  `FELIX_ACK_ON_COMMIT=true` so that ops acks carry offsets.

A publish with `"ack": false` gets no `ack`, only an `error` if it fails.

### `error`

```json
{"type": "error", "id": 7, "stream": "ops", "code": "publish_failed", "message": "..."}
```

`id` and `stream` are present when the error is about one request or one
stream. The connection stays open after any error.

| Code | Meaning |
|---|---|
| `bad_request` | The message did not parse, or its payload was not base64 |
| `publish_failed` | Felix refused or lost the publish. It may have landed: retry with the same `(sid, seq)` |
| `subscribe_failed` | Felix refused the subscription, for example an offset already trimmed |
| `subscription_ended` | A subscription stopped delivering. Subscribe again from the last offset handled plus one |

## Slow browsers

The gateway holds up to 1,024 events for a browser that is not reading. Past
that it stops reading the Felix subscription, and Felix's bounded
per-subscriber queue drops new events. The gateway never drops on its own, so a
loss always shows up as a gap in offsets.

## Latency

`GET /metrics` returns the two legs of the gateway separately, in microseconds:

```json
{
  "browser_rtt": {"count": 12, "p50_us": 210, "p90_us": 380, "p99_us": 900, "max_us": 1200},
  "felix_publish_ack_ops": {"count": 40, "p50_us": 650, "p90_us": 900, "p99_us": 1500, "max_us": 2100},
  "felix_publish_ack_presence": {"count": 0, "p50_us": 0, "p90_us": 0, "p99_us": 0, "max_us": 0}
}
```

| Histogram | What it times |
|---|---|
| `browser_rtt` | Browser to gateway and back: a WebSocket ping every 5 seconds per connection, answered by the browser itself |
| `felix_publish_ack_ops` | Gateway to Felix and back: from handing an acknowledged publish to Felix until its ack, on `ops` |
| `felix_publish_ack_presence` | The same on `presence` |

## Op payload

Payloads are opaque to the gateway. The canvas encodes ops on `ops` with the
`model/` package: a MessagePack map with these keys.

| Key | MessagePack type | Meaning |
|---|---|---|
| `sid` | uint | The authoring session, a u64 |
| `seq` | uint | The session's op counter, a u32 |
| `shape` | bin 16 | The target shape id, a u128, big-endian |
| `kind` | uint | 0 create, 1 patch, 2 delete |
| `fields` | map | Only the fields this op changes |

A move (a patch of `x` and `y`) encodes in about 80 bytes.
