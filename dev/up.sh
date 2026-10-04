#!/usr/bin/env bash
# Start the development stack from scratch and wait for the broker. The
# control plane keeps its state in memory, so the broker's log is reset with
# it: a log that outlived its control plane would no longer match it.

set -euo pipefail
cd "$(dirname "$0")"

files=(-f docker-compose.yml)
health=(8080)

docker compose "${files[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
rm -f state/*.pem
mkdir -p state
if ! docker compose "${files[@]}" up --detach; then
  docker compose "${files[@]}" logs >&2
  exit 1
fi

ready() {
  for port in "${health[@]}"; do
    curl -fsS "http://127.0.0.1:$port/ready" >/dev/null 2>&1 || return 1
  done
}

for _ in $(seq 1 90); do
  if ready; then
    echo "Felix is ready. For the gateway:"
    echo "  export GATEWAY_FELIX_CA_FILE=dev/state/broker-cert.pem"
    echo "  export GATEWAY_SCOPE_FILE=dev/scope.toml"
    echo "  export GATEWAY_TENANT=demo GATEWAY_OIDC_CLIENT_ID=felix-gateway"
    exit 0
  fi
  sleep 2
done

echo "the broker did not become ready" >&2
docker compose "${files[@]}" ps --all >&2
docker compose "${files[@]}" logs >&2
exit 1
