# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

felix-gateway is pre-1.0: the scope file format and the library API may change
between minor versions. Version 1 of the browser protocol is frozen. Each
release notes the Felix version it was tested against.

## [Unreleased]

### Added

- Shared connections: when `GATEWAY_FELIX_CREDENTIAL_FILE`,
  `GATEWAY_FELIX_CLIENT_CERT` and `GATEWAY_FELIX_CLIENT_KEY` are set, sessions
  share a few Felix connections (`GATEWAY_FELIX_SHARED_CONNECTIONS`, default
  4), each acting as its user with a token delegated to the gateway, and
  re-delegated at each refresh. Needs Felix 0.6.0-preview.4, `token.delegate`
  for the gateway's principal, and a client certificate issued to it. Setting
  only some of the three stops the gateway at startup. Per-session connections
  remain the default (#10).
- `Gateway::attach_shared`, which attaches an identity to a shared connection
  with given tokens (#10).
- `dev/up.sh` starts a second broker on port 5001 that asks for client
  certificates and binds tokens to them, with certificates for it and the
  gateway (#10).
- Write limits, set in the scope file's `[limits]` table: writes and bytes a
  second per session and per principal, each with a burst, a payload size
  limit per stream and cache, session caps per principal and per client
  address, and an optional per-alias write rate. They cover publishes, cache
  writes and deletes, counter adds and the leave beacon. A write over a rate
  is refused at once, never queued. `X-Forwarded-For` is used for the client
  address only with `trusted_proxies` set (#9).
- The `rate_limited` protocol feature: a session that asks for it gets the
  error code `rate_limited` with `retry_after_ms` for a refused write or join.
  Other sessions get the code they already know for that request (#9).
- `limits_refused` in `GET /metrics`, counting refusals by limit (#9).
- `felix-gateway-client`: `GatewayError.retryAfterMs` (#9).

### Changed

- The write limits are on by default: 50 writes and 256 KiB a second per
  session (bursts of 100 and 1 MiB), 100 writes and 512 KiB a second per
  principal (bursts of 200 and 2 MiB), payloads of at most 64 KiB, 8 sessions
  per principal and 32 per client address. A program embedding the library
  serves `Gateway::router()` with `into_make_service_with_connect_info` for
  the address cap to apply (#9).
- Built on felix-client and felix-wire 0.6.0-preview.4, and the dev stack runs
  the 0.6.0-preview.4 images (#10).
- `dev/up.sh` runs on Docker or Podman, and the docs show the Podman form.

## [0.2.0] - 2026-10-04

Moves to Felix 0.6.0-preview.2, which this release is tested against. Browsers
see two changes: a slow browser on a durable stream now gets every record,
late and in order, instead of a gap in offsets, and an expired cache entry
reaches watchers as a delete.

### Changed

- The write limits are on by default: 50 writes and 256 KiB a second per
  session (bursts of 100 and 1 MiB), 100 writes and 512 KiB a second per
  principal (bursts of 200 and 2 MiB), payloads of at most 64 KiB, 8 sessions
  per principal and 32 per client address. A program embedding the library
  serves `Gateway::router()` with `into_make_service_with_connect_info` for
  the address cap to apply (#9).
- Built on felix-client and felix-wire 0.6.0-preview.2, and the dev stack runs
  the `ghcr.io/getfelix` 0.6.0-preview.2 images.
- A browser that falls behind on a durable stream gets every record, late and
  in order, instead of a gap in offsets: Felix reports the drop and the
  gateway's subscription replays it from the log. If retention has passed the
  resume point the browser gets `trimmed`. In-memory streams still drop.
- An expired cache entry reaches watchers as a `cache_change` with a `null`
  payload, within about a second of its TTL, because Felix now writes a delete
  for it. `expires_in_ms` is still sent.

## [0.1.0] - 2026-10-03

The first release, extracted from Felix Canvas with its history. Tested
against Felix 0.6.0-preview.

### Added

- A WebSocket gateway that relays each browser session to Felix over QUIC,
  with a Felix connection per session authenticated by that session's own
  token.
- Sign-in through any OpenID Connect provider. The gateway exchanges each
  browser's ID token for a Felix token narrowed to one scope's resources and
  actions, so the broker refuses anything outside it.
- A scope file that names the scope's field and its streams, caches and
  counters by alias, with `optional` resources and `stamp_sender` streams.
- Stream publish and subscribe from an offset or from live, cache get, put,
  delete and watch with per-cache TTLs, counter adds, a slow-link throttle,
  and a beacon endpoint for a closing tab's cache delete.
- Protocol version 1 with negotiated features.
- Latency metrics for the browser and Felix legs at `/metrics`.
- The `felix-gateway` crate, with the library and the `felix-gateway` binary.
- The `ghcr.io/getfelix/felix-gateway` image for amd64 and arm64.
- The `felix-gateway-client` npm package: the browser client, and an in-memory
  fake of the gateway for tests.
