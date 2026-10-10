# felix-gateway

[![CI](https://github.com/GetFelix/felix-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/GetFelix/felix-gateway/actions/workflows/ci.yml)
[![MIT license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

A WebSocket gateway between browsers and [Felix](https://github.com/GetFelix/felix).
Each connection signs in to one scope and gets a Felix token that reaches only
that scope's resources.

## What it is

Browsers cannot speak Felix's QUIC protocol, and a browser should never hold a
token that reaches more than the one scope it has open. The gateway sits between
them. A browser opens a WebSocket, names its scope and signs in with an OpenID
Connect ID token. The gateway exchanges that token at the
Felix control plane for a Felix token narrowed to the scope's streams, caches
and counters, opens a Felix connection with it, and relays JSON messages both
ways without decoding payloads.

The gateway holds no application state and knows no application names. A
scope file says what a scope is called and which Felix resources each one
owns, so one binary serves a drawing canvas, a game, or anything else built
from streams, caches and counters. The broker enforces the narrowing, so a bug
in the gateway cannot reach another scope.

It comes as a Rust library, a binary, a container image, and a TypeScript
browser client.

## Features

- Publish and subscribe on Felix streams, from a log offset or from live, with
  each event's offset so a browser can see exactly what it missed.
- Cache reads, writes, deletes and watches, with a per-cache TTL for entries
  such as a presence list.
- Counter adds.
- Sign-in through any OpenID Connect provider. Each connection's Felix token
  covers one scope and only the actions the scope file lists.
- Optional resources, for people who may join a scope without every grant,
  such as a spectator who cannot publish a player's input.
- Sender stamping: on a stream marked `stamp_sender`, every record carries its
  publisher's principal, so a browser cannot speak for someone else.
- A protocol version and negotiated features, so old and new browsers work
  against one gateway.
- Per-connection ordering of publishes and cache writes, and a bounded buffer
  per browser that never drops on its own, so a slow browser on a durable
  stream falls behind and catches up from the log instead of losing records.
- Write limits for a public deployment: write and byte rates per session and
  per person with a burst allowance, a payload size limit per alias, and
  session caps per person and per client address. A write over a rate is
  refused with a retryable error instead of queued, and `/metrics` counts the
  refusals. Felix's own per-user limits still apply underneath.
- A slow-link throttle for demonstrating slow-consumer behaviour, off unless
  the scope file allows it. Keep it off on a public gateway.
- Shared connections: with a credential and a client certificate of its own,
  the gateway carries every session over a few Felix connections, each session
  acting as its own user with a token delegated to the gateway. The broker
  limits each user separately on a shared connection.
- Latency metrics for the browser leg and the Felix leg, apart.
- `felix-gateway-client`, the browser client, with an in-memory fake for tests.

## Quick start

You need Docker or Podman, Rust 1.97 and Node 24. `dev/up.sh` uses Docker when
its daemon is running and Podman otherwise (`podman machine start` first on
macOS); set `CONTAINER_ENGINE=podman` to choose. See [Docker or
Podman](https://docs.getfelix.dev/getting-started/containers/).

```sh
dev/up.sh                          # Felix, a stand-in IdP, and seeded rooms
export GATEWAY_FELIX_CA_FILE=dev/state/broker-cert.pem
export GATEWAY_SCOPE_FILE=dev/scope.toml
export GATEWAY_TENANT=demo GATEWAY_OIDC_CLIENT_ID=felix-gateway
cargo run -p felix-gateway         # listens on 127.0.0.1:8787
```

From a browser app:

```sh
npm install felix-gateway-client
```

```ts
import { GatewayClient } from "felix-gateway-client";

const client = await GatewayClient.connect("ws://127.0.0.1:8787/ws", { room: "lobby", token });
client.onEvent = (event) => console.log(event.stream, event.offset, event.payload);
client.subscribe("ops", "live");
await client.publish("ops", new TextEncoder().encode("hello"));
```

`token` is an ID token from the provider `/oidc` names. The dev IdP mints one
for anyone: `curl "http://127.0.0.1:9400/token?sub=ana&aud=felix-gateway"`.

The image is `ghcr.io/getfelix/felix-gateway`;
[docs/configuration.md](docs/configuration.md#the-image) shows how to run it.

## Configuration

Settings come from `GATEWAY_*` environment variables. Three are required:
`GATEWAY_SCOPE_FILE`, `GATEWAY_TENANT` and `GATEWAY_OIDC_CLIENT_ID`.

The scope file describes one scope:

```toml
[scope]
field = "room"                     # join carries {"room": "lobby", ...}

[[scope.streams]]
alias = "ops"                      # what messages call it
name = "app.ops.{scope}"           # the Felix stream for scope {scope}
actions = ["publish", "subscribe"]

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

Write limits are on by default; the `[limits]` table changes them.
The gateway never creates resources or grants; set those up in Felix first.
[docs/configuration.md](docs/configuration.md) lists every variable, every
scope file key and the Felix permissions each action needs.

Each session opens its own Felix connection by default. Setting
`GATEWAY_FELIX_CREDENTIAL_FILE`, `GATEWAY_FELIX_CLIENT_CERT` and
`GATEWAY_FELIX_CLIENT_KEY` turns on [shared
connections](docs/configuration.md#shared-connections) instead. They need
Felix 0.6.0-preview.4 or later and more setup in Felix: the gateway's principal
needs `token.delegate` on the tenant, and its certificate must carry the URI
`felix:principal:<id>`. Per-session connections stay the default for that
reason.

## Protocol

JSON text frames over one WebSocket at `/ws`: `join` and `hello`, `subscribe`,
`publish`, `counter_add`, `cache_get`, `cache_put`, `cache_delete`,
`cache_watch` and `throttle`, answered by `subscribed`, `event`, `ack`,
`counter`, `cache_value`, `cache_entries`, `cache_change` and `error`.
Payloads are base64 and opaque to the gateway. [docs/protocol.md](docs/protocol.md)
is the reference.

## Status

Pre-1.0. The protocol, the scope file format and the library API may change
between minor versions; version 1 of the wire protocol is frozen, and later
changes arrive as negotiated features. Tested against Felix 0.6.0-preview.4,
which shared connections require.

Known limits, all on the Felix side:

- Without shared connections each session opens its own Felix client, and a
  broker accepts a bounded number of connections.
- A shared connection goes to one broker, because Felix's cluster client does
  not carry identities yet. A subscription whose shard moves to another broker
  ends and is subscribed again by the browser.
- Felix events carry no publisher, which is why sender stamping lives in the
  gateway.

## Docs

- [docs/protocol.md](docs/protocol.md): the browser protocol.
- [docs/configuration.md](docs/configuration.md): environment, the scope file,
  Felix permissions and the image.
- [packages/gateway-client/README.md](packages/gateway-client/README.md): the
  browser client.
- [Browsers](https://docs.getfelix.dev/clients/browsers/) in the Felix docs:
  how the gateway fits next to the other Felix clients.
- [CHANGELOG.md](CHANGELOG.md): what changed in each release.

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) covers the layout, how to run the tests and
how pull requests should read.

## License

MIT. See [LICENSE](LICENSE).
