// Seeds the development stack: bootstrap a tenant that trusts the stand-in
// identity provider, create each room's streams, caches and counter as named
// in dev/scope.toml, and give each room a role whose members may open it.
// Writes the brokers' credential and the gateway's to the state directory.
import { createHash } from "node:crypto";
import { chmod, mkdir, writeFile } from "node:fs/promises";

const CONTROL_PLANE = "http://controlplane:8443";
const BOOTSTRAP = "http://controlplane:9095";
const BOOTSTRAP_TOKEN = "dev-bootstrap";
const IDP = "http://idp:9400";
const STATE = "/state";
const TENANT = "demo";
const NAMESPACE = "default";
// The gateway's GATEWAY_OIDC_CLIENT_ID; browsers' ID tokens carry it as audience.
const CLIENT_ID = "felix-gateway";
// Ana is in both rooms, so the tests can show that a session in one room
// cannot reach the other even for someone allowed in both.
const MEMBERS = { lobby: ["ana", "ben", "cleo"], studio: ["ana"] };

async function request(method, url, { token, headers = {}, body } = {}) {
  const response = await fetch(url, {
    method,
    headers: {
      "content-type": "application/json",
      ...(token ? { authorization: `Bearer ${token}` } : {}),
      ...headers,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  if (response.status === 409) return null;
  if (!response.ok) throw new Error(`${method} ${url} -> ${response.status}: ${text}`);
  return text ? JSON.parse(text) : null;
}

// The provider may not be answering yet.
async function discover() {
  for (let attempt = 1; ; attempt++) {
    try {
      return await request("GET", `${IDP}/.well-known/openid-configuration`);
    } catch (err) {
      if (attempt === 30) throw err;
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
}

const { issuer } = await discover();
// Felix keys RBAC on sha256(issuer|subject), not on the subject itself.
const principal = (subject) => createHash("sha256").update(`${issuer}|${subject}`).digest("hex");

async function exchange(subject, body) {
  const { id_token } = await request("GET", `${IDP}/token?sub=${subject}&aud=${CLIENT_ID}`);
  const url = `${CONTROL_PLANE}/v1/tenants/${TENANT}/token/exchange`;
  return (await request("POST", url, { token: id_token, body })).felix_token;
}

const object = (kind, name) => `${kind}:${TENANT}/${NAMESPACE}/${name}`;
const tenant = `${CONTROL_PLANE}/v1/tenants/${TENANT}`;
const namespace = `${tenant}/namespaces/${NAMESPACE}`;

// A room's role holds exactly what a session in it needs; the gateway asks the
// token exchange for no more than the scope file's grants.
function roomPolicies(room) {
  const role = `role:room-${room}`;
  const grant = (kind, name, action) => ({ subject: role, object: object(kind, name), action });
  return [
    ...[`demo.ops.${room}`, `demo.presence.${room}`].flatMap((stream) => [
      grant("stream", stream, "stream.publish"),
      grant("stream", stream, "stream.subscribe"),
    ]),
    // Counters authorize as cache writes.
    grant("cache", `demo.seq.${room}`, "cache.write"),
    grant("cache", `demo.snap.${room}`, "cache.read"),
    grant("cache", `demo.members.${room}`, "cache.read"),
    grant("cache", `demo.members.${room}`, "cache.write"),
  ];
}

console.log(`bootstrap tenant ${TENANT}`);
await request("POST", `${BOOTSTRAP}/internal/bootstrap/tenants/${TENANT}/initialize`, {
  headers: { "x-felix-bootstrap-token": BOOTSTRAP_TOKEN },
  body: {
    display_name: "Felix gateway demo",
    idp_issuers: [
      {
        issuer,
        audiences: [CLIENT_ID],
        jwks_url: `${IDP}/jwks.json`,
        claim_mappings: { subject_claim: "sub" },
      },
    ],
    initial_admin_principals: [principal("demo-admin")],
    policies: [
      // Gateways act for users on shared connections. Their own broker token
      // needs one grant to be minted at all; it is never used.
      { subject: "role:gateway", object: `tenant:${TENANT}`, action: "token.delegate" },
      { subject: "role:gateway", object: object("cache", "demo.gateway"), action: "cache.read" },
      { subject: "role:admin", object: object("stream", "*"), action: "stream.manage" },
      { subject: "role:admin", object: object("cache", "*"), action: "cache.manage" },
      { subject: "role:broker", object: "cluster:*", action: "node.view" },
      { subject: "role:broker", object: "cluster:*", action: "node.manage" },
    ],
    groupings: [
      { user: principal("demo-admin"), role: "role:admin" },
      { user: principal("demo-broker"), role: "role:broker" },
      // The second is for a test: a token delegated to it must not pass on
      // the first's connection.
      { user: principal("demo-gateway"), role: "role:gateway" },
      { user: principal("demo-other-gateway"), role: "role:gateway" },
    ],
  },
});

const admin = await exchange("demo-admin", { audience: "felix-controlplane" });
await request("POST", `${tenant}/namespaces`, {
  token: admin,
  body: { namespace: NAMESPACE, display_name: NAMESPACE },
});
const stream = (name, durable) => ({
  stream: name,
  kind: "Stream",
  shards: 1,
  replication_factor: 1,
  retention: { max_age_seconds: durable ? 24 * 60 * 60 : null, max_size_bytes: null },
  consistency: "Leader",
  delivery: durable ? "AtLeastOnce" : "AtMostOnce",
  durable,
});
for (const [room, members] of Object.entries(MEMBERS)) {
  for (const [name, durable] of [
    [`demo.ops.${room}`, true],
    [`demo.presence.${room}`, false],
  ]) {
    console.log(`stream ${name}`);
    await request("POST", `${namespace}/streams`, { token: admin, body: stream(name, durable) });
  }
  // Caches per room, because Felix grants a cache as a whole. One shard each:
  // a prefix watch reads a single shard.
  for (const cache of [`demo.seq.${room}`, `demo.snap.${room}`, `demo.members.${room}`]) {
    console.log(`cache ${cache}`);
    await request("POST", `${namespace}/caches`, {
      token: admin,
      body: { cache, display_name: cache, shards: 1, replication_factor: 1, consistency: "Leader" },
    });
  }
  console.log(`role:room-${room} for ${members.join(", ")}`);
  for (const policy of roomPolicies(room)) {
    await request("POST", `${tenant}/rbac/policies`, { token: admin, body: policy });
  }
  for (const name of members) {
    await request("POST", `${tenant}/rbac/groupings`, {
      token: admin,
      body: { user: principal(name), role: `role:room-${room}` },
    });
  }
}

// The broker runs as uid 65532 and writes its certificate here too.
await mkdir(STATE, { recursive: true });
await chmod(STATE, 0o777).catch(() => {});
const broker = await exchange("demo-broker", { audience: "felix-controlplane" });
await writeFile(`${STATE}/node.token`, broker, { mode: 0o644 });
console.log(`wrote node.token to ${STATE}`);
// GATEWAY_FELIX_CREDENTIAL_FILE: the gateway's ID token, which it exchanges
// itself. Its certificate, from dev/up.sh, names the same principal.
const gatewaySignIn = `${IDP}/token?sub=demo-gateway&aud=${CLIENT_ID}`;
const { id_token: gateway } = await request("GET", gatewaySignIn);
await writeFile(`${STATE}/gateway.token`, gateway, { mode: 0o644 });
console.log(`wrote gateway.token to ${STATE}`);
