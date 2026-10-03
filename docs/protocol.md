# Browser to gateway protocol

A browser opens one WebSocket to the gateway at `/ws` and exchanges JSON text
frames over it. The gateway relays each request to Felix and each Felix event
back, without decoding payloads. One gateway serves every room: a connection
names its room and signs in with its first message, `join`, and from then on
reaches that room and nothing else.

Each room has two streams and one member list:

| Name | Felix stream | Kind | Offsets |
|---|---|---|---|
| `ops` | `canvas.ops.<room>` | Durable, kept until the broker's retention trims it | Every event has one |
| `presence` | `canvas.presence.<room>` | In memory, at most once | Always `null` |

The member list is the Felix cache `canvas.members.<room>`, one key per
session, each expiring unless its session writes it again. The room's other
caches are `canvas.seq.<room>` for sequence numbers and `canvas.snap.<room>`
for the snapshot. Every room has its own streams and caches because Felix
authorizes a cache as a whole, never one key of it.

## Signing in

Before it connects, the page asks the gateway how to sign in:

```http
GET /oidc
```

```json
{"issuer": "https://login.example.com", "client_id": "felix-canvas", "scopes": "openid profile"}
```

It signs in with that OpenID Connect provider using the authorization code
flow with PKCE (S256), as a public client with the redirect URI
`<page origin>/`, asking for `scopes`, and keeps the ID token it gets for that
tab only, so two windows of one browser can be signed in to different
accounts. A new tab signs in again; with a provider session that is usually a
redirect with nothing to type. [design.md](design.md#authorization)
describes what the gateway does with it.

## Browser to gateway

### `join`

```json
{"type": "join", "room": "lobby", "token": "eyJhbGciOiJFUzI1NiIs..."}
```

| Field | Meaning |
|---|---|
| `room` | The room to open: 1 to 64 ASCII letters, digits, `-` or `_` |
| `token` | The ID token from signing in |

The first message on every connection, and the only one allowed before
`hello`. The gateway exchanges the ID token at the Felix control plane for a
Felix token narrowed to this room and opens its Felix connection with it. It
answers `hello`, or one `error` and then closes the connection:

| Code | Meaning | What the canvas does |
|---|---|---|
| `forbidden` | The signed-in person is not a member of the room, or the room does not exist | Shows "You don't have access to this canvas" and stops |
| `signed_out` | The ID token is missing, expired or from a provider the deployment does not trust | Offers to sign in again |
| `unavailable` | The control plane or the brokers could not be reached | Reconnects as after any drop |
| `bad_request` | The first message was not a `join`, or the room name is not allowed | |

Messages sent after `join` and before `hello` wait for the answer, so a page
can send its first `subscribe` right after `join`. A connection that sends
nothing for 10 seconds is closed.

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
| `counter` | `"seq"`, the only counter: per-session op sequence numbers, key `key` in the Felix counter cache `canvas.seq.<room>` |
| `key` | 1 to 64 ASCII letters, digits, `-` or `_` |
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

Read the room's snapshot, key `latest` in the Felix cache `canvas.snap.<room>`. The gateway
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

### Reading history

A page may open more than one connection to the same room; each joins and is
narrowed on its own. The canvas reads a room's history over a second one, so
its live subscription is never replaced:

1. `join` the room with the same ID token, then `subscribe` to `ops` from 0.
   `subscribed` names the tail `L`; history is loaded once every offset below
   `L` has arrived, and later events extend it.
2. A jump in offsets is a drop, as on any subscription: `subscribe` again from
   the first offset missing.
3. A `trimmed` answer means retention has discarded the start of the log. Read
   the snapshot with `snapshot_get` and `subscribe` from its offset plus one;
   history starts there.

Closing the connection ends the read. The gateway keeps nothing about it, and
reading again later subscribes from the first offset the page does not hold.
[design.md](design.md#history-and-the-time-scrubber) describes what the page
does with it.
### `set_member`

```json
{"type": "set_member", "key": "3f9a0c12d4e5b6a7", "payload": "gqRuYW1lo0FuYaVjb2xvcgI="}
```

Writes this session's member entry, key `key` in `canvas.members.<room>`, with the
TTL the gateway announced in `hello`. `key` follows the rule for `counter_add`.
The canvas writes its entry on connecting and again every third of the TTL, so
a session that stops writing drops out of everyone's list within one TTL. There
is no reply; a failed write is an `error` with code `member_failed`.

### `remove_member`

```json
{"type": "remove_member", "key": "3f9a0c12d4e5b6a7"}
```

Deletes the entry at once, for a tab that is closing. A connection's member
writes and publishes reach Felix one at a time in the order sent, so a delete is
never overtaken by the refresh before it.

A message sent while a page unloads may never leave it, so a closing tab also
sends `POST /members/leave` with `navigator.sendBeacon`, which the browser
delivers after the page is gone. The body is
`{"room": "lobby", "token": "<ID token>", "key": "3f9a0c12d4e5b6a7"}`, and the
gateway exchanges the token exactly as for `join` before it deletes anything.
It answers 204, 400 for a bad room or key, or 403 with the refusal as an
`error` message.

### `watch_members`

```json
{"type": "watch_members"}
```

Asks for the room's member list: one `members` message with every entry, then a
`member` message for each later write or delete. Watching again replaces the
earlier watch and starts with a fresh `members`.

### `throttle`

```json
{"type": "throttle", "bits_per_second": 100000}
{"type": "throttle", "bits_per_second": null}
```

Reads this connection's subscriptions no faster than a link of
`bits_per_second` would carry them, counting each event's JSON text. `null` or
0 lifts the limit. It stands in for a slow network, so the slow-client
demonstration runs anywhere: Felix keeps delivering at full speed, the
subscription's bounded queue fills and drops new events, and the browser sees a
gap. Other connections are not affected. There is no reply.

## Gateway to browser

### `hello`

```json
{"type": "hello", "namespace": "default", "room": "lobby", "member_ttl_ms": 30000}
```

The answer to `join`: the session is open on this room, and this is how long
a member entry lasts without a write. `CANVAS_MEMBER_TTL_SECONDS` sets
the TTL; it defaults to 30 seconds.

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
### `members` and `member`

```json
{"type": "members", "members": [{"key": "3f9a0c12d4e5b6a7", "payload": "gqRu...", "expires_in_ms": 21500}]}
{"type": "member", "key": "3f9a0c12d4e5b6a7", "payload": null, "expires_in_ms": null}
```

| Field | Meaning |
|---|---|
| `key` | The session, without the room prefix |
| `payload` | The entry, base64, or `null` when it was deleted |
| `expires_in_ms` | Milliseconds until the entry expires, measured on the gateway's clock when it relayed the change; `null` for an entry with no TTL |

Felix expires entries lazily and sends nothing when one lapses
([felix#960](https://github.com/gabloe/felix/issues/960)), so the browser
drops an entry itself once `expires_in_ms` has passed without a newer write.
A relative time keeps the browser's own clock out of it.

### `error`

```json
{"type": "error", "id": 7, "stream": "ops", "code": "publish_failed", "message": "..."}
```

`id` and `stream` are present when the error is about one request or one
stream. `oldest` is present only on `trimmed`. The connection stays open after
any error except an answer to `join`.

| Code | Meaning |
|---|---|
| `bad_request` | The message did not parse, or its payload was not base64 |
| `publish_failed` | Felix refused or lost the publish. It may have landed: retry with the same `(sid, seq)` |
| `subscribe_failed` | Felix refused the subscription, for example an offset already trimmed |
| `subscription_ended` | A subscription stopped delivering. Subscribe again from the last offset handled plus one |
| `counter_failed` | Felix refused or lost a counter add. It may have been counted |
| `snapshot_failed` | Felix could not read the snapshot. Ask again |
| `trimmed` | The subscription asked for an offset retention has discarded. `oldest` is the oldest offset left. Join again from the snapshot |
| `member_failed` | Felix refused or lost a member write or delete. The next refresh writes it again |
| `watch_failed` | The member watch was refused or stopped. Send `watch_members` again |
| `forbidden`, `signed_out`, `unavailable` | Only in answer to `join`; see [`join`](#join) |

## Slow browsers

The gateway holds up to 1,024 events for a browser that is not reading. Past
that it stops reading the Felix subscription, and Felix's bounded
per-subscriber queue drops new events. The gateway never drops on its own, so a
loss always shows up as a gap in offsets.

The browser finds a loss in two ways:

1. An `ops` event whose offset is past the one its subscription should deliver
   next, after allowing for `skipped_before`. It subscribes again from the last
   offset it applied plus one.
2. A peer's presence `at` (see [Presence payload](#presence-payload)) still
   above its own applied count two seconds after it arrived. Felix drops the
   newest records, so when the last records of a burst are the ones dropped,
   nothing arrives after them to show the gap. Every session publishes presence
   at least every 3 seconds, so this finds the loss within a few seconds. The
   grace period covers the normal case where a cursor message overtakes the
   change it reports.

Either way the browser shows that it is catching up, reads the missing records
from the log, and ends with the same state hash as everyone else.

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
| `kind` | uint | 0 create, 1 patch, 2 delete, 3 text |
| `fields` | map | Only the fields this op changes; for `text`, the one field `y` |
| `t` | uint, optional | When the author made the edit, in milliseconds since 1970 by its own clock. Only history shows it; the fold ignores it, and a value that is not a time is dropped |

A move (a patch of `x` and `y`) encodes in about 90 bytes, 11 of them the time.

### Shape fields

A `create` carries every field of the new shape; a `patch` carries only those it
changes. The fold in `model/` applies them last-writer-wins per field on log
offset, and ignores a patch to a shape that does not exist, so a delete is final.

| Field | Shapes | Meaning |
|---|---|---|
| `type` | all | `rect`, `ellipse`, `line`, `stroke` or `text`. A create with any other type is ignored. Never changes |
| `x`, `y` | all | The top-left corner, or a line's start, in canvas units |
| `w`, `h` | all | Size; for a line, the offset from start to end, which may be negative. A `text` shape's height follows its text, so its `h` is unused, and a `w` of 0 makes it grow with its longest line |
| `z` | all | A fractional-index key: shapes stack in key order, then by id. A value that is not a key is ignored |
| `points` | `stroke` | Pairs of coordinates relative to `x, y`. Never changes once created |

### Text

A `text` op changes the rich text of a `text` shape, a rectangle or an
ellipse. Its one field, `y`, is a [Yjs](https://docs.yjs.dev) update in Yjs's
version 2 encoding, to a document with one root XML fragment named `body`.
The fold applies it to that shape's document, so concurrent typing merges
instead of one person's text replacing another's. A shape's first `text` op
creates its body, and a delete removes it.

The fold ignores a `text` op on a shape that does not exist, on a line or a
stroke, or whose update does not decode or is over 64 KB. A session's Yjs
client id is the low 32 bits of its `sid` XORed with the high 32.

The fragment holds what the editor writes through `y-prosemirror`:

| Element | Attributes | Holds |
|---|---|---|
| `p` | none | Text |
| `h` | `level`: 1, 2 or 3 | Text |
| `ul`, `ol` | none | `li` elements |
| `li` | none | A `p`, then any blocks, including lists nested up to three deep |

Text carries marks as Yjs formatting attributes, each a map:

| Mark | Value | Meaning |
|---|---|---|
| `b`, `i`, `u` | `{}` | Bold, italic, underline |
| `a` | `{ href }` | A link; only `http:`, `https:` and `mailto:` addresses count |
| `size` | `{ step }` | `small`, `large` or `huge`: 12, 20 or 28 canvas units against Medium's 16 |
| `color` | `{ name }` | `muted`, `coral`, `orange`, `amber`, `green`, `blue`, `violet`, `magenta` or `rose`; Ink when absent |

Anything else in the document is kept but never shown, and the state hash
leaves it out.

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
| `at` | uint | How many changes the session has applied: the next offset it needs. Optional |
| `txt` | array, optional | While the session edits text: `[shape, anchor, head]`, the shape id as bin 16 and the selection's two ends as encoded Yjs relative positions (`Y.encodeRelativePosition`), so a caret stays on its character while others type before it |

The canvas samples the pointer once per animation frame and sends at most one
message per 16 ms, so a 120 Hz screen still sends 60 a second. Cursors on other
screens are eased toward each new position with a critically damped spring.

## Member payload

A member entry is a MessagePack map, encoded by `encodeMember` in `model/`.

| Key | MessagePack type | Meaning |
|---|---|---|
| `name` | str | Display name, at most 24 characters |
| `color` | uint | Index into the presence palette |
| `person` | uint | A u64 hash of the signed-in account: the 64-bit FNV-1a of the JSON array `[iss, sub]` from the ID token |

The key is the session, which is new on every page load; `person` stays the
same for every tab and every visit signed in to one account, so the canvas
uses it for anything that should outlast a reload. Two windows signed in to
different accounts are two people, even in one browser. A
person's colour is their `person` modulo eight, moved on to the next free
colour while someone with a smaller `person` holds it. Everyone sees the same
member list, so everyone settles on the same colours, and a reload keeps them.
The people list shows each person once, however many tabs they have open or
however many entries an unclean reload left behind.

## Snapshot payload

The snapshotter writes key `latest` of `canvas.snap.<room>` with `encodeSnapshot` from
`model/`: a MessagePack map with these keys.

| Key | MessagePack type | Meaning |
|---|---|---|
| `v` | uint | Format version, 2. Version 1 had no `texts` and still decodes |
| `offset` | uint | The last log offset folded in. Continue from `offset + 1` |
| `shapes` | array | One `[id, fields, written]` per shape: the id as bin 16, the fields as in ops, and a map from field name to the offset that last wrote it |
| `seqs` | array | One `[sid, seq]` per session: the highest `seq` folded in, so a retried op that lands later is still recognised as a repeat |
| `texts` | array | One `[id, state]` per body: the shape id as bin 16 and `Y.encodeStateAsUpdateV2` of its document |
