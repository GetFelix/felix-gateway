# Browser to gateway protocol

A browser opens one WebSocket to the gateway at `/ws` and exchanges JSON text
frames over it. The gateway relays each request to Felix and each Felix event
back, without decoding payloads. One gateway serves every scope: a connection
names its scope and signs in with its first message, `join`,
and from then on reaches that scope and nothing else.

The [scope file](configuration.md#the-scope-file) names the key a `join`
carries the scope under and the Felix streams, caches and counters each scope
owns. Messages name those by alias. The examples below use the test scope file,
`dev/scope.toml`, which has scope field `room` and these aliases:

| Alias | Kind | Felix name | Notes |
|---|---|---|---|
| `ops` | Stream | `demo.ops.<room>` | Durable. Every event has an offset |
| `presence` | Stream | `demo.presence.<room>` | In memory, at most once. Offsets are always `null` |
| `snap` | Cache | `demo.snap.<room>` | Read only |
| `members` | Cache | `demo.members.<room>` | Entries expire 30 seconds after their last write |
| `seq` | Counter | `demo.seq.<room>` | |

The `felix-gateway-client` npm package in `packages/gateway-client/` speaks
this protocol for a browser.

## Versions and features

The protocol has a version and a list of features, the way Felix's own wire
protocol negotiates capabilities instead of bumping versions. A `join` names
the version it speaks in `protocol` and the optional features it wants in
`features`; `hello` answers with the version and the subset of those features
the gateway accepted. A browser uses a feature only if `hello` lists it.

- A `join` without `protocol` is version 1, and one without `features` asks
  for none. Version 1 is frozen: a change that an older browser would misread
  is a new feature or a new version, never an edit to version 1.
- This gateway speaks version 1 and has no features yet.
- A `join` asking for another version is refused with `unsupported`.
- A message type the gateway does not know is answered with an `unsupported`
  error, and the connection stays open.
- A field added to a message later is optional, and leaving it out means what
  the message meant before the field existed.

## Signing in

Before it connects, the page asks the gateway how to sign in:

```http
GET /oidc
```

```json
{"issuer": "https://login.example.com", "client_id": "my-app", "scopes": "openid profile"}
```

It signs in with that OpenID Connect provider using the authorization code
flow with PKCE (S256), as a public client, asking for `scopes`, and sends the
ID token it gets in `join`. The gateway exchanges it at the Felix control
plane for a Felix token narrowed to the one scope the connection opens: only
the resources the scope file names for that scope, and only the actions it
lists. A token for one scope cannot reach another at the broker, even for
someone allowed in both.

The tenant must trust the provider, with the `client_id` as audience, and
grant each person the permissions for the scopes they may open. The gateway
never creates resources or grants; `dev/seed.mjs` shows what a deployment sets
up.

## Browser to gateway

### `join`

```json
{"type": "join", "protocol": 1, "features": [], "room": "lobby", "token": "eyJhbGciOiJFUzI1NiIs..."}
```

| Field | Meaning |
|---|---|
| `protocol` | The protocol version. Optional; leaving it out means 1 |
| `features` | Optional features to turn on. Optional; leaving it out asks for none |
| `room` | The scope's value, under the scope file's `field`: 1 to 64 ASCII letters, digits, `-` or `_` |
| `token` | The ID token from signing in |

The first message on every connection, and the only one allowed before
`hello`. The gateway exchanges the ID token for a narrowed Felix token and
opens its Felix connection with it. It answers `hello`, or one `error` and
then closes the connection:

| Code | Meaning |
|---|---|
| `forbidden` | The signed-in person may not open this scope, or it does not exist |
| `signed_out` | The ID token is missing, expired or from a provider the tenant does not trust |
| `unavailable` | The control plane or the brokers could not be reached. Reconnect as after any drop |
| `unsupported` | The `join` asked for a protocol version this gateway does not speak |
| `bad_request` | The first message was not a `join`, or the scope's value is not allowed |

A resource marked `optional` in the scope file may be missing from the
person's grants without refusing the join; `hello` lists it under `missing`.

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
| `stream` | A stream alias whose actions include `subscribe` |
| `from` | `"live"` for only what is published from now on, or a log offset: the first record wanted, which is the last offset handled plus one |

Subscribing to a stream the connection is already subscribed to replaces the
earlier subscription. That is the recovery path: after a gap or an ended
subscription, subscribe again from the last offset handled plus one.

An offset the log no longer holds is answered with a `trimmed` error naming the
oldest offset left.

### `publish`

```json
{"type": "publish", "stream": "ops", "payload": "haNzaWTP...", "ack": true, "id": 7}
```

| Field | Meaning |
|---|---|
| `stream` | A stream alias whose actions include `publish` |
| `payload` | The record, base64. The gateway passes the bytes through unread, after the sender's stamp on a `stamp_sender` stream |
| `ack` | `true` to be told when Felix has the record. `false` publishes fire-and-forget, which suits cursors |
| `id` | An integer chosen by the browser. It comes back on the `ack` or `error` for this publish |

A connection's publishes and cache writes reach Felix one at a time, in the
order the browser sent them. The gateway never resends a publish. When one
fails the browser decides whether to retry, since only the application knows
whether a record is safe to deduplicate.

On a stream with `stamp_sender`, each payload reaches Felix prefixed with the
publisher's Felix principal: a 2-byte big-endian length, then the principal's
UTF-8 bytes. Readers learn who sent each record, and a browser cannot speak
for someone else.

### `counter_add`

```json
{"type": "counter_add", "counter": "seq", "key": "3f9a0c12d4e5b6a7", "delta": 1024, "id": 8}
```

| Field | Meaning |
|---|---|
| `counter` | A counter alias whose actions include `add` |
| `key` | 1 to 64 ASCII letters, digits, `-` or `_` |
| `delta` | A signed 64-bit amount to add |
| `id` | As for `publish`; it comes back on the `counter` or `error` |

The gateway answers with a `counter` message carrying the sum after the add.
Felix counts an add retried after a lost answer twice.

### `cache_get`

```json
{"type": "cache_get", "cache": "snap", "key": "latest", "id": 9}
```

Reads one entry of a cache whose actions include `read`. `key` follows the
rule for `counter_add`. The gateway answers with a `cache_value` message
carrying the same `id`.

### `cache_put`

```json
{"type": "cache_put", "cache": "members", "key": "3f9a0c12d4e5b6a7", "payload": "gqRuYW1lo0FuYaVjb2xvcgI="}
```

Writes one entry of a cache whose actions include `write`, with the cache's
`ttl_s` when it has one; `hello` gives each TTL in milliseconds. `key` follows
the rule for `counter_add`. There is no reply; a failed write is an `error`
with code `cache_failed` and the cache's alias.

A presence list fits this: each session writes its entry, keyed by session, on
connecting and again every third of the TTL, so a session that stops writing
drops out of everyone's list within one TTL.

### `cache_delete`

```json
{"type": "cache_delete", "cache": "members", "key": "3f9a0c12d4e5b6a7"}
```

Deletes one entry at once, needing `write` as `cache_put` does. A delete is
never overtaken by the write sent before it.

A message sent while a page unloads may never leave it, so a closing tab can
send `POST /members/leave` with `navigator.sendBeacon`, which the browser
delivers after the page is gone. The body is
`{"room": "lobby", "token": "<ID token>", "cache": "members", "key": "3f9a0c12d4e5b6a7"}`,
with the scope under the scope file's `field`. The gateway exchanges the token
exactly as for `join` before it deletes anything, and answers 204, 400 for a
bad scope or key or a cache that does not allow `write`, or 403 with the
refusal as an `error` message.

### `cache_watch`

```json
{"type": "cache_watch", "cache": "members"}
```

Asks for a cache's entries, needing `watch`: one `cache_entries` message with
every entry, then a `cache_change` message for each later write or delete.
Watching the same cache again replaces the earlier watch and starts with a
fresh `cache_entries`.

### `throttle`

```json
{"type": "throttle", "bits_per_second": 100000}
{"type": "throttle", "bits_per_second": null}
```

Reads this connection's subscriptions no faster than a link of
`bits_per_second` would carry them, counting each event's JSON text. `null` or
0 lifts the limit. It stands in for a slow network: Felix keeps delivering at
full speed, the subscription's bounded queue fills and drops new events, and
the browser sees a gap. Other connections are not affected. There is no reply.
A gateway whose scope file does not set `allow_throttle` answers
`unsupported`.

## Patterns

### Subscribe before reading

When a cache holds a fold of a stream up to some offset, as a snapshot would,
read it after subscribing, never before:

1. `subscribe` to the stream from `"live"` and wait for `subscribed`. Its
   `live_offset` is the tail `L`, and every record from `L` on will arrive as
   an event, including any published while the next step is in flight.
2. `cache_get` the fold. Say it covers the stream through offset `N`.
3. Drop buffered events at or below `N`. If `N + 1 < L`, `subscribe` again
   from `N + 1` to read the records in between. With no fold, subscribe from 0.

Reading first would lose the records published between the read and the
subscribe.

### More than one connection

A page may open more than one connection to the same scope; each joins and is
narrowed on its own. A second connection can read a stream from offset 0 for
history while the first keeps its live subscription. Closing it ends the read;
the gateway keeps nothing about it.

## Gateway to browser

### `hello`

```json
{"type": "hello", "protocol": 1, "features": [], "namespace": "default", "room": "lobby", "cache_ttl_ms": {"members": 30000}}
```

The answer to `join`: the session is open on this scope.

| Field | Meaning |
|---|---|
| `protocol` | The version the session speaks |
| `features` | The features of the `join` the gateway accepted |
| `namespace` | The Felix namespace the scope's resources live in |
| `room` | The scope's value, under the scope file's `field` |
| `cache_ttl_ms` | How long an entry lasts without a write, for each cache with a `ttl_s` |
| `missing` | Present only when not empty: aliases of `optional` resources this sign-in does not reach |

### `subscribed`

```json
{"type": "subscribed", "stream": "ops", "start_offset": 42, "live_offset": 57}
```

The subscription is registered with the broker. Anything published from this
point on will be delivered. `start_offset` is the first offset this
subscription delivers and `live_offset` is the stream's tail when it was
registered; both are `null` on a stream with no log, such as `presence`. From
`"live"` the two are equal.

### `event`

```json
{"type": "event", "stream": "ops", "offset": 42, "payload": "haNzaWTP..."}
```

One record, in the order the broker delivered it. On a durable stream, events
arrive in increasing offset order. `skipped_before` appears only when it is not
zero: it counts the offsets just before this one that hold no event (a new
leader's generation-start record), so the number of records dropped before this
event is `offset - previous_offset - 1 - skipped_before`. Anything above zero
is a drop the browser recovers from by subscribing again.

Offsets are JSON numbers, exact in JavaScript up to 2^53.

### `ack`

```json
{"type": "ack", "id": 7, "offset": 42}
```

Felix accepted publish `id`. `offset` is the record's log offset when the broker
had one to give, and `null` when:

- the stream has no log;
- the broker acknowledged when it queued the record rather than after writing
  it. That is Felix's default; set `FELIX_ACK_ON_COMMIT=true` on the broker, as
  `dev/` does, for acks with offsets.

A publish with `"ack": false` gets no `ack`, only an `error` if it fails.

### `counter`

```json
{"type": "counter", "id": 8, "value": 2048}
```

The counter's sum after the `counter_add` with this `id`.

### `cache_value`

```json
{"type": "cache_value", "id": 9, "payload": "hKF2AaZvZmZzZXTN..."}
```

The answer to `cache_get`: the entry's bytes in base64, or `null` when there
is none.

### `cache_entries` and `cache_change`

```json
{"type": "cache_entries", "cache": "members", "entries": [{"key": "3f9a0c12d4e5b6a7", "payload": "gqRu...", "expires_in_ms": 21500}]}
{"type": "cache_change", "cache": "members", "key": "3f9a0c12d4e5b6a7", "payload": null, "expires_in_ms": null}
```

| Field | Meaning |
|---|---|
| `cache` | The cache's alias |
| `key` | The entry's key |
| `payload` | The entry, base64, or `null` when it was deleted |
| `expires_in_ms` | Milliseconds until the entry expires, measured on the gateway's clock when it relayed the change; `null` for an entry with no TTL |

Felix 0.6.0-preview expires entries lazily and sends nothing when one lapses
([felix#960](https://github.com/GetFelix/felix/issues/960)), so the browser
drops an entry itself once `expires_in_ms` has passed without a newer write. A
relative time keeps the browser's own clock out of it.

### `error`

```json
{"type": "error", "id": 7, "stream": "ops", "code": "publish_failed", "message": "..."}
```

`id` is present when the error answers one request, and `stream` or `cache`
when it is about one stream or cache. `oldest` is present only on `trimmed`.
The connection stays open after any error except an answer to `join`.

| Code | Meaning |
|---|---|
| `bad_request` | The message did not parse, its payload was not base64, a key or scope was not allowed, or it named an alias the scope file does not have or an action that alias does not allow |
| `unsupported` | The message type is not one this gateway knows, a `join` asked for another protocol version, or `throttle` is not allowed |
| `publish_failed` | Felix refused or lost the publish. It may have landed |
| `subscribe_failed` | Felix refused the subscription |
| `subscription_ended` | A subscription stopped delivering. Subscribe again from the last offset handled plus one |
| `counter_failed` | Felix refused or lost a counter add. It may have been counted |
| `cache_failed` | Felix refused or lost a cache read, write or delete |
| `trimmed` | The subscription asked for an offset retention has discarded. `oldest` is the oldest offset left |
| `watch_failed` | A cache watch was refused or stopped. Send `cache_watch` again |
| `forbidden`, `signed_out`, `unavailable` | Only in answer to `join`; see [`join`](#join) |

## Slow browsers

The gateway holds up to 1,024 events for a browser that is not reading. Past
that it stops reading the Felix subscription, and Felix's bounded
per-subscriber queue drops new events. The gateway never drops on its own, so a
loss on a durable stream always shows up as a gap in offsets.

Felix drops the newest records, so when the last records of a burst are the
ones dropped, nothing arrives after them to show the gap. An application that
needs to notice that promptly can have each session publish its position on a
second stream every few seconds, so a peer that sees a position ahead of its
own knows it is behind.

## HTTP endpoints

| Path | What |
|---|---|
| `GET /ws` | The WebSocket |
| `GET /oidc` | How a browser signs in: `issuer`, `client_id`, `scopes` |
| `POST /members/leave` | A closing tab's cache delete, described under [`cache_delete`](#cache_delete) |
| `GET /metrics` | Latency, below |
| anything else | A file of the web bundle in `GATEWAY_WEB_DIR`, when set |

`GET /metrics` returns the two legs of the gateway separately, in microseconds:

```json
{
  "browser_rtt": {"count": 12, "p50_us": 210, "p90_us": 380, "p99_us": 900, "max_us": 1200},
  "felix_publish_ack": {
    "ops": {"count": 40, "p50_us": 650, "p90_us": 900, "p99_us": 1500, "max_us": 2100},
    "presence": {"count": 0, "p50_us": 0, "p90_us": 0, "p99_us": 0, "max_us": 0}
  }
}
```

| Histogram | What it times |
|---|---|
| `browser_rtt` | Browser to gateway and back: a WebSocket ping every 5 seconds per connection, answered by the browser itself |
| `felix_publish_ack` | Gateway to Felix and back, one histogram per stream alias in the scope file: from handing an acknowledged publish to Felix until its ack |
