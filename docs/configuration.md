# Configuration

The gateway reads its settings from `GATEWAY_*` environment variables and the
scope from a TOML file. It never creates Felix resources or grants: the tenant,
its trust in the identity provider, every scope's streams and caches, and the
roles that let people open them are set up before the gateway starts.
`dev/seed.mjs` does that for the development stack.

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_SCOPE_FILE` | required | The scope file, below |
| `GATEWAY_TENANT` | required | The Felix tenant that trusts the identity provider |
| `GATEWAY_OIDC_CLIENT_ID` | required | The client registered for the browser app at the identity provider. Browsers' ID tokens carry it as audience |
| `GATEWAY_OIDC_ISSUER` | `http://127.0.0.1:9400` | The identity provider browsers sign in with, as its OpenID Connect issuer URL |
| `GATEWAY_OIDC_SCOPES` | `openid profile` | The scopes a browser asks the provider for |
| `GATEWAY_NAMESPACE` | `default` | The Felix namespace the scope's resources live in |
| `GATEWAY_LISTEN` | `127.0.0.1:8787` | Where browsers connect. The image sets `0.0.0.0:8787` |
| `GATEWAY_FELIX_BROKERS` | `127.0.0.1:5000` | Comma-separated broker addresses as `host:port`. Names are resolved at each connection, so a broker that comes back on a new address is found again |
| `GATEWAY_FELIX_SERVER_NAME` | `localhost` | The name the broker's certificate is checked against |
| `GATEWAY_FELIX_CA_FILE` | unset | PEM certificates to trust for the broker. Unset means the platform trust store |
| `GATEWAY_FELIX_CONTROL_PLANE` | `http://127.0.0.1:8443` | The Felix control plane's base URL, where each browser's sign-in is exchanged |
| `GATEWAY_WEB_DIR` | unset | A built web app to serve on every path the gateway does not route itself |
| `RUST_LOG` | `info` | Log filter, as `tracing-subscriber` reads it |

`/oidc` returns the issuer, client ID and scopes, so the browser app needs no
configuration of its own to sign in.

## Shared connections

By default every browser session opens its own Felix connection with its own
scope token. That needs no setup beyond the variables above, but a broker
accepts a bounded number of connections, so it limits how many sessions one
gateway can serve.

With shared connections, the gateway opens a few connections as itself and
each session acts as its user on one of them. They are on when all three of
these are set, and off when none is. Setting only some of them stops the
gateway at startup, naming the ones missing.

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_FELIX_CREDENTIAL_FILE` | unset | A file holding an ID token for the gateway's own principal, from an identity provider the tenant trusts, such as a Kubernetes projected service account token. Read again at each exchange, so a token rotated in place is used |
| `GATEWAY_FELIX_CLIENT_CERT` | unset | PEM certificate chain, leaf first, issued to the gateway's principal. Read at each new connection |
| `GATEWAY_FELIX_CLIENT_KEY` | unset | The PEM private key of that certificate |
| `GATEWAY_FELIX_SHARED_CONNECTIONS` | `4` | How many connections sessions are spread over. Each session goes to the one with the fewest |

They need Felix 0.6.0-preview.4 or later, on the control plane and the
brokers. A join works like this:

1. The browser's ID token is exchanged for a scope token, as without shared
   connections.
2. The gateway exchanges its own credential for a `felix-controlplane` token
   narrowed to `token.delegate`, kept until a minute before it expires.
3. It calls `POST /v1/tenants/{tenant}/token/delegate` with the scope token.
   The control plane reissues it with the same user and grants and with
   `act: {"sub": "<gateway principal>"}`.
4. The session attaches to a shared connection as the user, with the delegated
   token (`Client::with_identity` in felix-client).

A delegated token has no refresh token. Before it expires, the gateway
refreshes the user's scope token, which re-runs RBAC, and delegates the new
one. A person removed from the scope loses access at that refresh. A session's
identity is closed when the browser disconnects, and the connection stays with
everyone else.

The broker checks each request against the user's delegated token only, and
keeps a subscription cap (`FELIX_MAX_SUBSCRIPTIONS_PER_CONN`) and a publish
byte budget (`FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES`) per user on each
connection, under connection-wide ceilings
(`FELIX_MAX_SUBSCRIPTIONS_PER_CONN_TOTAL`,
`FELIX_BROKER_PUBLISH_CONN_TOTAL_INFLIGHT_BYTES`). A user at the subscription
cap gets `subscribe_failed`, and one at the publish budget is slowed or gets
`publish_failed`; others on the connection are not held back by them until the
ceilings are reached. The defaults for those ceilings
are four users' worth, so a busy gateway may want more connections or higher
ceilings.

The Felix side needs:

- A principal for the gateway. Felix names a signed-in principal by the
  SHA-256 of `<issuer>|<subject>` in hex; that is the id the certificate and
  the grants below use.
- `token.delegate` over `tenant:{tenant}` for that principal. `tenant.manage`
  does not include it.
- One broker grant for that principal, any grant, because the control plane
  mints no token without a permission and the shared connections open with the
  gateway's own broker token. The gateway sends no requests of its own;
  `dev/seed.mjs` grants `cache.read` on a cache that does not exist.
- A client certificate whose subject alternative names include the URI
  `felix:principal:<id>`, from a CA the brokers trust (`FELIX_TLS_CLIENT_CA`).
  With `FELIX_TLS_CLIENT_CERT_BIND_SUBJECT=true` the brokers accept a token on
  that connection only when its `act.sub`, or its `sub`, is that principal.

`dev/up.sh` sets all of this up against a second broker on port 5001 and
prints the variables to use.

Each shared connection goes to one broker. Felix's cluster client, which
follows a shard to its new owner when it moves, does not carry identities yet,
so on a multi-broker cluster a subscription whose shard moves ends with
`subscription_ended` and the browser subscribes again. A subscription that
falls behind on a durable stream still resumes from the log, as it does on a
connection of its own.

## The scope file

The gateway reads it when it starts and refuses to start if it breaks any rule
below.

```toml
# Whether a browser may slow its own connection with `throttle`. Default false.
# A demo switch: leave it off on a public gateway.
allow_throttle = false

[scope]
# The key `join` carries the scope's value under.
field = "room"

[[scope.streams]]
alias = "ops"
name = "app.ops.{scope}"
actions = ["publish", "subscribe"]

[[scope.streams]]
alias = "input"
name = "app.input.{scope}"
actions = ["publish"]
stamp_sender = true
optional = true

[[scope.caches]]
alias = "members"
name = "app.members.{scope}"
actions = ["read", "write", "watch"]
ttl_s = 30

[[scope.counters]]
alias = "seq"
name = "app.seq.{scope}"
actions = ["add"]

# Optional; every key has a default. See "Write limits" below.
[limits]
max_payload_bytes = 65536
```

| Key | Meaning |
|---|---|
| `allow_throttle` | Top level, default `false`. Whether `throttle` is allowed. It exists to demonstrate a slow browser, and it lets a browser hold its subscriptions' queues full, so keep it `false` on a public gateway |
| `field` | 1 to 64 ASCII letters, digits, `-` or `_`, and not `type`, `token`, `protocol` or `features` |
| `alias` | What messages call the resource. The same rule as `field`, unique within its kind |
| `name` | The Felix name. `{scope}` is replaced by the scope's value, and must appear, or every scope would share the resource |
| `actions` | What a session may do. Streams: `publish`, `subscribe`. Caches: `read` (`cache_get`), `write` (`cache_put`, `cache_delete`), `watch` (`cache_watch`). Counters: `add` |
| `optional` | Default `false`. When `true`, a sign-in that does not reach this resource still joins, and `hello` lists the alias under `missing`. A spectator without publish rights on a player's input stream is the usual case |
| `stamp_sender` | Streams only, default `false`. Each publish is prefixed with the publisher's Felix principal (the `sub` of its scope token) as a 2-byte big-endian length and the UTF-8 bytes, so a reader knows who sent it and a browser cannot speak for someone else |
| `ttl_s` | Caches only. How long an entry written with `cache_put` lasts without another write, in seconds, above 0. Without it entries never expire |
| `max_payload_bytes` | Streams and caches. The largest payload, decoded, that one `publish` or `cache_put` may carry, in place of `limits.max_payload_bytes` |
| `writes_per_s` | Writes a second one session may make to this resource, on top of the session and principal rates below. Unset or 0 is no limit of its own |
| `write_burst` | The burst for `writes_per_s`, above 0. Default: one second's worth |

Unknown keys are an error, so a typo fails at startup instead of being ignored.

## Write limits

The `[limits]` table bounds what browsers can write through the gateway. It
covers every write the gateway relays: `publish`, `cache_put`, `cache_delete`,
`counter_add`, and the `POST /members/leave` beacon. Every key is optional,
and the defaults below apply when the table is left out.

```toml
[limits]
max_payload_bytes = 65536      # largest payload of one write, decoded
sessions_per_principal = 8     # sessions one person may hold, across scopes
sessions_per_ip = 32           # sessions one client address may hold
trusted_proxies = 0            # proxies whose X-Forwarded-For entries to trust

[limits.session]               # each session on its own
writes_per_s = 50
write_burst = 100
bytes_per_s = 262144           # 256 KiB
byte_burst = 1048576           # 1 MiB

[limits.principal]             # every session of one person together
writes_per_s = 100
write_burst = 200
bytes_per_s = 524288           # 512 KiB
byte_burst = 2097152           # 2 MiB
```

| Key | Default | Meaning |
|---|---|---|
| `max_payload_bytes` | `65536` | The largest payload, decoded from base64, that one `publish` or `cache_put` may carry, above 0. A stream or cache can set its own. A larger write is answered `bad_request` |
| `sessions_per_principal` | `8` | Sessions one Felix principal may hold at once on this gateway, in any scope. 0 is no limit |
| `sessions_per_ip` | `32` | Sessions one client address may hold at once. 0 is no limit |
| `trusted_proxies` | `0` | How many proxies in front of the gateway append to `X-Forwarded-For`. 0 ignores the header |
| `writes_per_s`, `write_burst` | per table | Writes a second, and how many may come at once. Every write counts as one |
| `bytes_per_s`, `byte_burst` | per table | Payload bytes a second, and how many may come at once. A cache delete and a counter add count no bytes |

Each rate is a token bucket that starts full: a session may write
`write_burst` writes at once, then `writes_per_s` a second. A write must fit
the session's rates, the principal's, and its resource's `writes_per_s` when
it has one. One that does not is refused at once, spends nothing, and never
reaches Felix; the gateway does not queue it. A rate of 0 turns that bucket
off. `byte_burst` must be at least the largest `max_payload_bytes`, or a write
that size could never pass, and the gateway refuses to start otherwise.

A principal's rate is shared by all its sessions, and it outlasts them until
it has refilled, so reconnecting does not buy a fresh burst. The leave beacon has
no session and counts against its principal's rates only.

How a browser learns of a refusal is in
[docs/protocol.md](protocol.md#limits). In short: a browser that asks for the
`rate_limited` feature gets `rate_limited` with `retry_after_ms`, and one that
does not gets the failure code it already knows for that request.

The gateway does not read a WebSocket message larger than the largest
`max_payload_bytes` in base64 plus 16 KiB; such a message closes the
connection.

### Client addresses

`sessions_per_ip` counts sessions by the address the TCP connection comes
from. Behind a load balancer or reverse proxy that is the proxy's address, so
set `trusted_proxies` to the number of proxies that append to
`X-Forwarded-For`. The gateway then takes the entry that many places from the
right, which the nearest of those proxies wrote; entries further left could
have come from the browser. A header with fewer entries, or an entry that is
not an address, falls back to the socket's address. Leave `trusted_proxies`
at 0 unless every connection comes through such proxies, since a browser
reaching the gateway directly could otherwise choose its own address.

The binary always knows the socket's address. A program that embeds the
library must serve `Gateway::router()` with
`into_make_service_with_connect_info::<SocketAddr>()`; without that, and
without `trusted_proxies`, the address cap is not applied.

### Felix limits underneath

The gateway's limits come first. Felix still applies its own: with shared
connections, the broker keeps each user's subscription cap and publish byte
budget on every connection (see [Shared connections](#shared-connections)),
so a person within the gateway's rates can still get `publish_failed` or
`subscribe_failed` from the broker. Without shared connections each session's
own connection has the broker's per-connection limits.

## Felix permissions

Each action needs one Felix permission on the resource:

| Action | Permission |
|---|---|
| stream `publish` | `stream.publish` |
| stream `subscribe` | `stream.subscribe` |
| cache `read`, `watch` | `cache.read` |
| cache `write` | `cache.write` |
| counter `add` | `cache.write`, since Felix keeps counters in caches |

A join asks the token exchange for exactly those on the scope's resources, so
a person needs a role that grants them, and the session's token can do nothing
else. Felix grants a cache as a whole, never one key of it, which is why each
scope has its own caches.

## The image

`ghcr.io/getfelix/felix-gateway` runs the binary as uid 65532, listening on
port 8787, and reads the scope file from `/etc/felix-gateway/scope.toml`:

```sh
docker run -p 8787:8787 \
  -e GATEWAY_TENANT=my-tenant \
  -e GATEWAY_OIDC_CLIENT_ID=my-app \
  -e GATEWAY_OIDC_ISSUER=https://login.example.com \
  -e GATEWAY_FELIX_BROKERS=felix-broker:5000 \
  -e GATEWAY_FELIX_CONTROL_PLANE=https://felix-controlplane:8443 \
  -v "$PWD/scope.toml:/etc/felix-gateway/scope.toml:ro" \
  ghcr.io/getfelix/felix-gateway:0.2.0
```

For shared connections, mount the credential, certificate and key too, and set
the three variables to their paths in the container.

`podman run` takes the same arguments. On SELinux hosts, mount the scope file
with `:ro,Z`.

Its health check fetches `/oidc`, which answers as soon as the gateway is
listening.
