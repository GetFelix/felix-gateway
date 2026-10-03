#!/usr/bin/env bash
# Start the development stack from scratch and wait for the broker. The
# control plane keeps its state in memory, so the broker's log is reset with
# it: a log that outlived its control plane would no longer match it.
#
# `--cluster` starts three replicating brokers instead of one
# (docker-compose.cluster.yml).
set -euo pipefail
cd "$(dirname "$0")"

files=(-f docker-compose.yml)
health=(8080)
if [[ "${1:-}" == "--cluster" ]]; then
  files+=(-f docker-compose.cluster.yml)
  health=(8080 8081 8082)
fi

docker compose -f docker-compose.yml -f docker-compose.cluster.yml down --volumes --remove-orphans >/dev/null 2>&1 || true
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
    if [[ ${#health[@]} -gt 1 ]]; then
      # Each broker signs its own certificate; trust all three.
      cat state/broker-*-cert.pem >state/broker-cert.pem
    fi
    echo "Felix is ready. For the gateway:"
    echo "  export CANVAS_FELIX_CA_FILE=dev/state/broker-cert.pem"
    if [[ ${#health[@]} -gt 1 ]]; then
      echo "  export CANVAS_FELIX_BROKERS=127.0.0.1:5000,127.0.0.1:5010,127.0.0.1:5020"
    fi
    echo "The snapshotter also takes"
    echo "  export CANVAS_FELIX_TOKEN=\"\$(cat dev/state/snapshotter.token)\""
    exit 0
  fi
  sleep 2
done

echo "the brokers did not become ready" >&2
docker compose "${files[@]}" ps --all >&2
docker compose "${files[@]}" logs >&2
exit 1
