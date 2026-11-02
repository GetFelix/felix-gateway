#!/usr/bin/env bash
# The nightly run against Felix main: starts the dev stack from the Felix
# images tagged $FELIX_IMAGE_TAG (default: nightly) and runs the integration
# tests that CI runs against it. The felix-nightly workflow calls this after
# pointing Cargo.toml at the same Felix commit.
#
# It rewrites the image tags in the compose files, so run it from a throwaway
# checkout.
set -euo pipefail
cd "$(dirname "$0")/.."

tag=${FELIX_IMAGE_TAG:-nightly}
images=(dev/docker-compose.yml)
TAG=$tag perl -pi -e 's{(ghcr\.io/getfelix/felix-(?:broker|controlplane)):[^\s"\x27]+}{$1:$ENV{TAG}}' "${images[@]}"
# A pattern that stopped matching would leave the pinned release running.
if ! grep -hq "ghcr\.io/getfelix/felix-broker:$tag" "${images[@]}" ||
  grep -h 'ghcr\.io/getfelix/felix-\(broker\|controlplane\)' "${images[@]}" | grep -qv ":${tag}[[:space:]]*\$"; then
  echo "the compose files do not all use the $tag images" >&2
  exit 1
fi

logs() { [ $? -eq 0 ] || docker compose -f dev/docker-compose.yml logs --no-color; }
trap logs EXIT

cargo test --no-run
dev/up.sh

export GATEWAY_TENANT=demo GATEWAY_OIDC_CLIENT_ID=felix-gateway
export GATEWAY_SCOPE_FILE="$PWD/dev/scope.toml"

GATEWAY_FELIX_CA_FILE="$PWD/dev/state/broker-cert.pem" \
  cargo test --test relay -- --include-ignored --nocapture

# The same suite over shared connections, against the broker that binds tokens
# to client certificates. The skipped tests connect without a certificate on
# purpose, or need a stream of several shards, which only the first broker
# serves.
GATEWAY_FELIX_BROKERS=127.0.0.1:5001 \
GATEWAY_FELIX_CA_FILE="$PWD/dev/state/ca.pem" \
GATEWAY_FELIX_CREDENTIAL_FILE="$PWD/dev/state/gateway.token" \
GATEWAY_FELIX_CLIENT_CERT="$PWD/dev/state/gateway-cert.pem" \
GATEWAY_FELIX_CLIENT_KEY="$PWD/dev/state/gateway-key.pem" \
  cargo test --test relay -- --include-ignored --nocapture \
    --skip a_token_for_one_room_cannot_reach_another_at_the_broker \
    --skip subscribe_follows_the_shard_it_names
