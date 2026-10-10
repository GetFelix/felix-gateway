#!/usr/bin/env bash
# Start the development stack from scratch and wait for the broker. The
# control plane keeps its state in memory, so the broker's log is reset with
# it: a log that outlived its control plane would no longer match it.

set -euo pipefail
cd "$(dirname "$0")"

# Docker or Podman: CONTAINER_ENGINE if set, else Docker when its daemon
# answers, else Podman.
engine="${CONTAINER_ENGINE:-}"
if [[ -z "$engine" ]]; then
  if docker info >/dev/null 2>&1; then
    engine=docker
  elif command -v podman >/dev/null 2>&1; then
    engine=podman
  else
    echo "no container engine: start Docker, or install Podman (and run podman machine start on macOS)" >&2
    exit 1
  fi
fi
compose=("$engine" compose)

files=(-f docker-compose.yml)
health=(8080 8081)

"${compose[@]}" "${files[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
rm -f state/*.pem state/*.token
mkdir -p state

# Certificates for the second broker, which asks clients for one and binds
# tokens to it: a CA, the broker's certificate, and the gateway's, issued to
# its Felix principal (sha256 of the IdP issuer, "|" and its subject).
gateway_principal=$(printf '%s' "http://127.0.0.1:9400|demo-gateway" | openssl dgst -sha256 | awk '{print $NF}')
key() {
  openssl ecparam -name prime256v1 -genkey -noout | openssl pkcs8 -topk8 -nocrypt -out "$1"
}
key state/ca-key.pem
openssl req -x509 -new -key state/ca-key.pem -out state/ca.pem -days 7 \
  -subj "/CN=felix-gateway dev CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
issue() { # name subject-alt-name extended-key-usage
  key "state/$1-key.pem"
  openssl req -new -key "state/$1-key.pem" -subj "/CN=$1" -out "state/$1.csr" 2>/dev/null
  printf 'basicConstraints=CA:FALSE\nsubjectAltName=%s\nextendedKeyUsage=%s\n' "$2" "$3" >"state/$1.ext"
  openssl x509 -req -in "state/$1.csr" -CA state/ca.pem -CAkey state/ca-key.pem -CAcreateserial \
    -days 7 -extfile "state/$1.ext" -out "state/$1-cert.pem" 2>/dev/null
  rm -f "state/$1.csr" "state/$1.ext"
}
issue broker-mtls "DNS:localhost" serverAuth
issue gateway "URI:felix:principal:$gateway_principal" clientAuth
# The broker runs as uid 65532. Development keys only.
chmod 644 state/*.pem
if ! "${compose[@]}" "${files[@]}" up --detach; then
  "${compose[@]}" "${files[@]}" logs >&2
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
    echo "For shared connections, against the second broker:"
    echo "  export GATEWAY_FELIX_BROKERS=127.0.0.1:5001 GATEWAY_FELIX_CA_FILE=dev/state/ca.pem"
    echo "  export GATEWAY_FELIX_CREDENTIAL_FILE=dev/state/gateway.token"
    echo "  export GATEWAY_FELIX_CLIENT_CERT=dev/state/gateway-cert.pem GATEWAY_FELIX_CLIENT_KEY=dev/state/gateway-key.pem"
    exit 0
  fi
  sleep 2
done

echo "the broker did not become ready" >&2
"${compose[@]}" "${files[@]}" ps --all >&2
"${compose[@]}" "${files[@]}" logs >&2
exit 1
