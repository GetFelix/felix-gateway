# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

felix-gateway is pre-1.0: the scope file format and the library API may change
between minor versions. Version 1 of the browser protocol is frozen. Each
release notes the Felix version it was tested against.

## [Unreleased]

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
