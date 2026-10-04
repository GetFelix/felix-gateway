# syntax=docker/dockerfile:1.7
# The gateway, serving the built web page from the same origin as /ws.
# Build from the repository root: docker build -f docker/gateway.Dockerfile .

FROM node:24-trixie-slim AS web
WORKDIR /src
# Every workspace's manifest, or npm ci refuses the lockfile.
COPY package.json package-lock.json .npmrc tsconfig.base.json ./
COPY packages/gateway-client/package.json packages/gateway-client/
COPY model/package.json model/
COPY web/package.json web/
COPY snapshotter/package.json snapshotter/
RUN --mount=type=cache,target=/root/.npm \
    npm ci --include-workspace-root -w felix-gateway-client -w @felix-canvas/model -w @felix-canvas/web
COPY packages/gateway-client packages/gateway-client
COPY model model
COPY web web
RUN npm run build -w felix-gateway-client -w @felix-canvas/model && npm run build -w @felix-canvas/web

FROM rust:1.97-bookworm AS gateway
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY gateway gateway
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p felix-canvas-gateway \
    && cp target/release/felix-canvas-gateway /usr/local/bin/

FROM debian:bookworm-slim
# tini so SIGTERM reaches the gateway; wget for the healthcheck; openssl so
# the compose install can make the broker a certificate with this image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssl tini wget \
    && rm -rf /var/lib/apt/lists/*
COPY --from=gateway /usr/local/bin/felix-canvas-gateway /usr/local/bin/
COPY --from=web /src/web/dist /usr/share/felix-canvas/web
COPY deploy/scope.toml /usr/share/felix-canvas/scope.toml
ENV CANVAS_LISTEN=0.0.0.0:8787 \
    CANVAS_WEB_DIR=/usr/share/felix-canvas/web \
    CANVAS_SCOPE_FILE=/usr/share/felix-canvas/scope.toml
USER 65532:65532
EXPOSE 8787/tcp
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s --retries=6 \
    CMD wget -qO- http://127.0.0.1:8787/oidc >/dev/null 2>&1 || exit 1
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/felix-canvas-gateway"]
