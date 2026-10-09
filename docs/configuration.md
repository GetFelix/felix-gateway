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

## The scope file

The gateway reads it when it starts and refuses to start if it breaks any rule
below.

```toml
# Whether a browser may slow its own connection with `throttle`. Default false.
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
```

| Key | Meaning |
|---|---|
| `allow_throttle` | Top level, default `false`. Whether `throttle` is allowed |
| `field` | 1 to 64 ASCII letters, digits, `-` or `_`, and not `type`, `token`, `protocol` or `features` |
| `alias` | What messages call the resource. The same rule as `field`, unique within its kind |
| `name` | The Felix name. `{scope}` is replaced by the scope's value, and must appear, or every scope would share the resource |
| `actions` | What a session may do. Streams: `publish`, `subscribe`. Caches: `read` (`cache_get`), `write` (`cache_put`, `cache_delete`), `watch` (`cache_watch`). Counters: `add` |
| `optional` | Default `false`. When `true`, a sign-in that does not reach this resource still joins, and `hello` lists the alias under `missing`. A spectator without publish rights on a player's input stream is the usual case |
| `stamp_sender` | Streams only, default `false`. Each publish is prefixed with the publisher's Felix principal (the `sub` of its scope token) as a 2-byte big-endian length and the UTF-8 bytes, so a reader knows who sent it and a browser cannot speak for someone else |
| `ttl_s` | Caches only. How long an entry written with `cache_put` lasts without another write, in seconds, above 0. Without it entries never expire |

Unknown keys are an error, so a typo fails at startup instead of being ignored.

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

`podman run` takes the same arguments. On SELinux hosts, mount the scope file
with `:ro,Z`.

Its health check fetches `/oidc`, which answers as soon as the gateway is
listening.
