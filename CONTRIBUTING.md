# Contributing

## Layout

| Path | What |
|---|---|
| `crates/felix-gateway/` | The Rust crate: the library and the `felix-gateway` binary. `tests/relay.rs` runs against a real Felix; `examples/viewers.rs` holds the viewers for fanout measurements |
| `packages/gateway-client/` | The `felix-gateway-client` npm package: the browser client and its in-memory fake |
| `dev/` | Felix for local runs and CI: Docker Compose with two brokers (the second requires client certificates), a stand-in IdP, the seed script, and the test scope file |
| `docker/Dockerfile` | The image |
| `docs/` | The protocol and configuration references |

## Running the checks

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked                # unit tests; the integration tests are ignored

dev/up.sh                          # Felix for the integration tests
export GATEWAY_FELIX_CA_FILE="$PWD/dev/state/broker-cert.pem"
export GATEWAY_SCOPE_FILE="$PWD/dev/scope.toml"
export GATEWAY_TENANT=demo GATEWAY_OIDC_CLIENT_ID=felix-gateway
cargo test --locked --test relay -- --include-ignored
# CI runs the suite again over shared connections; see .github/workflows/ci.yml.

npm ci
npm run format:check
npm run build
npm run typecheck
npm test
```

CI runs all of these, and builds the image. Every `package-lock.json` entry
must come from `https://registry.npmjs.org/` with a `sha512` hash; CI checks
that too.

## Code

- Keep the gateway a relay. It never decodes payloads and holds no application
  state or names; those belong in the scope file or in the application.
- Add capabilities as negotiated features, never as edits to protocol
  version 1. A new optional field must mean, when absent, what the message
  meant before the field existed.
- Format with `cargo fmt` and prettier.

## Comments and docs

- Comment only where the code is unclear: a sentence or two on a constraint the
  code cannot show, such as an ordering requirement or a failure mode.
- Document public items briefly (`///` and TSDoc): what it does and what it
  guarantees.
- Write docs in plain sentences. No em-dashes, no filler words, no hedging.
- Docs change with the code. A change to a message belongs in
  [docs/protocol.md](docs/protocol.md), and a change to a setting or the scope
  file in [docs/configuration.md](docs/configuration.md).

## Pull requests

- CI must pass before review.
- Add a line under `## [Unreleased]` in [CHANGELOG.md](CHANGELOG.md) for
  anything a user or an operator would notice, under `Added`, `Changed`,
  `Fixed` or `Removed`, ending with the pull request number. That section
  becomes the release notes.
- No AI attribution in commits or pull request descriptions.
- List design calls under "Review notes" in the description.

## Releasing

A release is a `v*` tag on `main`, such as `v0.2.0`. To cut one:

1. In one pull request, set the version in `Cargo.toml` (`workspace.package`)
   and `packages/gateway-client/package.json`, run `cargo check` and
   `npm install` to update both lockfiles, and move the `Unreleased` notes in
   `CHANGELOG.md` under the new version with the date.
2. Run the Release workflow on the branch: without a tag it is a dry run that
   checks the versions and builds everything without publishing.
3. After merging, tag `main` and push the tag. The workflow publishes the
   image, the crate and the npm package, and creates the GitHub release from
   the changelog.
