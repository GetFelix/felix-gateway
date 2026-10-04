# syntax=docker/dockerfile:1.7
# The gateway. Mount a scope file at /etc/felix-gateway/scope.toml, or point
# GATEWAY_SCOPE_FILE elsewhere, and set GATEWAY_TENANT and
# GATEWAY_OIDC_CLIENT_ID. Mount a built web app and set GATEWAY_WEB_DIR to
# serve it from the same origin as /ws.
# Build from the repository root: docker build -f docker/Dockerfile .

FROM rust:1.97-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml README.md ./
COPY crates crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p felix-gateway --bin felix-gateway \
    && cp target/release/felix-gateway /usr/local/bin/

FROM debian:bookworm-slim
# tini so SIGTERM reaches the gateway; wget for the healthcheck.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini wget \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/felix-gateway /usr/local/bin/
ENV GATEWAY_LISTEN=0.0.0.0:8787 \
    GATEWAY_SCOPE_FILE=/etc/felix-gateway/scope.toml
USER 65532:65532
EXPOSE 8787/tcp
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s --retries=6 \
    CMD wget -qO- http://127.0.0.1:8787/oidc >/dev/null 2>&1 || exit 1
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/felix-gateway"]
