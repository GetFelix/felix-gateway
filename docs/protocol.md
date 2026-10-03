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

An offset the log no longer holds is answered with a `trimmed` error naming the
oldest offset left. The browser then joins again from the snapshot, as below.

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

### `counter_add`

```json
{"type": "counter_add", "counter": "seq", "key": "3f9a0c12d4e5b6a7", "delta": 1024, "id": 8}
```

| Field | Meaning |
|---|---|
| `counter` | `"seq"`, the only counter: per-session op sequence numbers, the Felix counter `canvas.seq/<room>:<key>` |
| `key` | 1 to 64 ASCII letters, digits, `-` or `_`. The gateway prefixes the room, so a key never reaches another room's counter |
| `delta` | A signed 64-bit amount to add |
| `id` | As for `publish`; it comes back on the `counter` or `error` |

The gateway answers with a `counter` message carrying the sum after the add.
The canvas reserves its sequence numbers this way: it adds 1,024 to its session's
counter and uses the 1,024 numbers below the sum, fetching another block when
half are gone. Felix counts an add retried after a lost answer twice, which
only wastes numbers.

### `snapshot_get`

```json
{"type": "snapshot_get", "id": 9}
```

Read the room's snapshot, the Felix cache key `canvas.snap/<room>`. The gateway
answers with a `snapshot` message carrying the same `id`.

### Joining

A browser joins a room in this order:

1. `subscribe` to `ops` from `"live"` and wait for `subscribed`. Its
   `live_offset` is the tail `L`, and every op from `L` on will arrive as an
   event, including any published while the next step is in flight.
2. `snapshot_get`. Decode the snapshot with `decodeSnapshot` from `model/`; it
   holds the room through offset `N`.
3. Drop buffered events at or below `N`. If `N + 1 < L`, `subscribe` to `ops`
   from `N + 1` to read the ops in between. With no snapshot, subscribe from 0.

Reading the snapshot before subscribing would lose the ops published between
the two. [design.md](design.md#join-and-snapshot) explains the rule.

## Gateway to browser

### `hello`

```json
{"type": "hello", "namespace": "default", "room": "lobby"}
```

The first message on every connection: the room this gateway serves.

### `subscribed`

```json
{"type": "subscribed", "stream": "ops", "start_offset": 42, "live_offset": 57}
```

The subscription is registered with the broker. Anything published from this
point on will be delivered, which is what the join path's subscribe-before-read
rule waits for. `start_offset` is the first offset this subscription delivers
and `live_offset` is the stream's tail when it was registered; both are `null`
on `presence`. From `"live"` the two are equal.

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

### `counter`

```json
{"type": "counter", "id": 8, "value": 2048}
```

The counter's sum after the `counter_add` with this `id`.

### `snapshot`

```json
{"type": "snapshot", "id": 9, "payload": "hKF2AaZvZmZzZXTN..."}
```

The answer to `snapshot_get`: the snapshot's bytes in base64, as the
snapshotter wrote them, or `null` when the room has none yet. The gateway does
not decode them. [Snapshot payload](#snapshot-payload) describes the format.

### `error`

```json
{"type": "error", "id": 7, "stream": "ops", "code": "publish_failed", "message": "..."}
```

`id` and `stream` are present when the error is about one request or one
stream. `oldest` is present only on `trimmed`. The connection stays open after
any error.

| Code | Meaning |
|---|---|
| `bad_request` | The message did not parse, or its payload was not base64 |
| `publish_failed` | Felix refused or lost the publish. It may have landed: retry with the same `(sid, seq)` |
| `subscribe_failed` | Felix refused the subscription, for example an offset already trimmed |
| `subscription_ended` | A subscription stopped delivering. Subscribe again from the last offset handled plus one |
| `counter_failed` | Felix refused or lost a counter add. It may have been counted |
| `snapshot_failed` | Felix could not read the snapshot. Ask again |
| `trimmed` | The subscription asked for an offset retention has discarded. `oldest` is the oldest offset left. Join again from the snapshot |

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

### Shape fields

A `create` carries every field of the new shape; a `patch` carries only those it
changes. The fold in `model/` applies them last-writer-wins per field on log
offset, and ignores a patch to a shape that does not exist, so a delete is final.

| Field | Shapes | Meaning |
|---|---|---|
| `type` | all | `rect`, `ellipse`, `line` or `stroke`. A create with any other type is ignored. Never changes |
| `x`, `y` | all | The top-left corner, or a line's start, in canvas units |
| `w`, `h` | all | Size; for a line, the offset from start to end, which may be negative |
| `z` | all | A fractional-index key: shapes stack in key order, then by id. A value that is not a key is ignored |
| `points` | `stroke` | Pairs of coordinates relative to `x, y`. Never changes once created |

## Presence payload

Records on `presence` are MessagePack maps, published fire-and-forget at most
once a frame while the pointer or selection moves, and every 3 seconds
otherwise. A session not heard from for 10 seconds is treated as gone.

| Key | MessagePack type | Meaning |
|---|---|---|
| `sid` | uint | The session, as in ops |
| `n` | uint | Increases with every message, so a session can time its own echo |
| `name` | str | Display name |
| `color` | uint | Index into the eight-colour presence palette |
| `x`, `y` | float or nil | The pointer in canvas units, nil when it left the canvas |
| `sel` | array of bin 16 | Ids of the selected shapes |
| `gone` | bool | Present and true on a session's last message |

## Snapshot payload

The snapshotter writes `canvas.snap/<room>` with `encodeSnapshot` from
`model/`: a MessagePack map with these keys.

| Key | MessagePack type | Meaning |
|---|---|---|
| `v` | uint | Format version, 1 |
| `offset` | uint | The last log offset folded in. Continue from `offset + 1` |
| `shapes` | array | One `[id, fields, written]` per shape: the id as bin 16, the fields as in ops, and a map from field name to the offset that last wrote it |
| `seqs` | array | One `[sid, seq]` per session: the highest `seq` folded in, so a retried op that lands later is still recognised as a repeat |
