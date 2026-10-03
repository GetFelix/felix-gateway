// Seeds the development stack the way a deployment would: bootstrap a tenant
// that trusts the dev IdP, create each room's streams and caches, and give
// each room a role whose members may open it. Writes the broker's credential
// and the snapshotter's token to the state directory. Safe to run again:
// existing objects are kept.
import { createHash } from "node:crypto";
import { chmod, mkdir, writeFile } from "node:fs/promises";

const CONTROL_PLANE = process.env.CONTROL_PLANE ?? "http://controlplane:8443";
const BOOTSTRAP = process.env.BOOTSTRAP ?? "http://controlplane:9095";
const BOOTSTRAP_TOKEN = process.env.BOOTSTRAP_TOKEN ?? "dev-bootstrap";
// The control plane fetches keys over the compose network, but browsers reach
// the IdP on the host, and the issuer is what they see.
const IDP = process.env.IDP ?? "http://idp:9400";
const ISSUER = process.env.IDP_ISSUER ?? "http://127.0.0.1:9400";
const STATE = process.env.STATE_DIR ?? "/state";
const TENANT = process.env.CANVAS_TENANT ?? "canvas";
const NAMESPACE = process.env.CANVAS_NAMESPACE ?? "default";
const AUDIENCE = "felix-canvas";
const RETENTION_SECONDS = 30 * 24 * 60 * 60;
// 3 in the three-broker stack, so a room survives losing any one broker.
const REPLICAS = Number(process.env.CANVAS_REPLICAS ?? 1);
// Felix only promotes a replica it knows holds every acknowledged record when
// the ack waited for a majority.
const CONSISTENCY = REPLICAS > 1 ? "Quorum" : "Leader";

// Who may open which room. Ana is in both, so the tests can show that a
// session in one room cannot reach the other even for someone allowed in both.
const MEMBERS = {
  lobby: ["ana", "ben"],
  studio: ["ana"],
};

// Felix keys RBAC on sha256(issuer|subject), not on the subject itself.
const principal = (subject) => createHash("sha256").update(`${ISSUER}|${subject}`).digest("hex");

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
  if (response.status === 409) {
    console.log(`  ${method} ${url}: already exists`);
    return null;
  }
  if (!response.ok) {
    throw new Error(`${method} ${url} -> ${response.status}: ${text}`);
  }
  return text ? JSON.parse(text) : null;
}

async function exchange(subject, body) {
  const { id_token } = await request("GET", `${IDP}/token?sub=${subject}&aud=${AUDIENCE}`);
  const url = `${CONTROL_PLANE}/v1/tenants/${TENANT}/token/exchange`;
  return (await request("POST", url, { token: id_token, body })).felix_token;
}

const streams = `stream:${TENANT}/${NAMESPACE}/*`;
const caches = `cache:${TENANT}/${NAMESPACE}/*`;
const object = (kind, name) => `${kind}:${TENANT}/${NAMESPACE}/${name}`;

// A room's role holds exactly what a session in it needs; the gateway asks
// the token exchange for no more than this.
function roomPolicies(room) {
  const role = `role:room-${room}`;
  return [
    ...[`canvas.ops.${room}`, `canvas.presence.${room}`].flatMap((stream) => [
      { subject: role, object: object("stream", stream), action: "stream.publish" },
      { subject: role, object: object("stream", stream), action: "stream.subscribe" },
    ]),
    // Counters authorize as cache writes.
    { subject: role, object: object("cache", `canvas.seq.${room}`), action: "cache.write" },
    { subject: role, object: object("cache", `canvas.snap.${room}`), action: "cache.read" },
    { subject: role, object: object("cache", `canvas.members.${room}`), action: "cache.read" },
    { subject: role, object: object("cache", `canvas.members.${room}`), action: "cache.write" },
  ];
}

const rooms = Object.keys(MEMBERS);
console.log(`bootstrap tenant ${TENANT}`);
await request("POST", `${BOOTSTRAP}/internal/bootstrap/tenants/${TENANT}/initialize`, {
  headers: { "x-felix-bootstrap-token": BOOTSTRAP_TOKEN },
  body: {
    display_name: "Felix Canvas (development)",
    idp_issuers: [
      {
        issuer: ISSUER,
        audiences: [AUDIENCE],
        jwks_url: `${IDP}/jwks.json`,
        claim_mappings: { subject_claim: "sub" },
      },
    ],
    initial_admin_principals: [principal("canvas-admin")],
    policies: [
      { subject: "role:admin", object: streams, action: "stream.manage" },
      { subject: "role:admin", object: caches, action: "cache.manage" },
      { subject: "role:broker", object: "cluster:*", action: "node.view" },
      // A broker in a cluster registers itself.
      { subject: "role:broker", object: "cluster:*", action: "node.manage" },
      // Subscribing also grants polling the snapshotter's consumer group.
      { subject: "role:snapshotter", object: streams, action: "stream.subscribe" },
      { subject: "role:snapshotter", object: caches, action: "cache.read" },
      { subject: "role:snapshotter", object: caches, action: "cache.write" },
      ...rooms.flatMap(roomPolicies),
    ],
    groupings: [
      { user: principal("canvas-admin"), role: "role:admin" },
      { user: principal("canvas-broker"), role: "role:broker" },
      { user: principal("canvas-snapshotter"), role: "role:snapshotter" },
      ...Object.entries(MEMBERS).flatMap(([room, users]) =>
        users.map((user) => ({ user: principal(user), role: `role:room-${room}` })),
      ),
    ],
  },
});

const admin = await exchange("canvas-admin", { audience: "felix-controlplane" });
console.log(`namespace ${NAMESPACE}`);
await request("POST", `${CONTROL_PLANE}/v1/tenants/${TENANT}/namespaces`, {
  token: admin,
  body: { namespace: NAMESPACE, display_name: NAMESPACE },
});
const stream = (name, durable) => ({
  stream: name,
  kind: "Stream",
  shards: 1,
  replication_factor: REPLICAS,
  retention: { max_age_seconds: durable ? RETENTION_SECONDS : null, max_size_bytes: null },
  consistency: durable ? CONSISTENCY : "Leader",
  delivery: durable ? "AtLeastOnce" : "AtMostOnce",
  durable,
});
for (const room of rooms) {
  for (const [name, durable] of [
    [`canvas.ops.${room}`, true],
    [`canvas.presence.${room}`, false],
  ]) {
    console.log(`stream ${name}`);
    await request("POST", `${CONTROL_PLANE}/v1/tenants/${TENANT}/namespaces/${NAMESPACE}/streams`, {
      token: admin,
      body: stream(name, durable),
    });
  }
  // Caches per room, because Felix grants a cache as a whole. One shard
  // each: a prefix watch reads a single shard, and the member list is one.
  for (const [cache, display_name] of [
    [`canvas.seq.${room}`, `Op sequence per session in ${room}`],
    [`canvas.snap.${room}`, `Snapshot of ${room}`],
    [`canvas.members.${room}`, `Members of ${room}`],
  ]) {
    console.log(`cache ${cache}`);
    await request("POST", `${CONTROL_PLANE}/v1/tenants/${TENANT}/namespaces/${NAMESPACE}/caches`, {
      token: admin,
      body: {
        cache,
        display_name,
        shards: 1,
        replication_factor: REPLICAS,
        consistency: CONSISTENCY,
      },
    });
  }
}

// The broker runs as uid 65532 and writes its certificate here too.
await mkdir(STATE, { recursive: true });
await chmod(STATE, 0o777);
const broker = await exchange("canvas-broker", { audience: "felix-controlplane" });
await writeFile(`${STATE}/node.token`, broker, { mode: 0o644 });
const snapshotter = await exchange("canvas-snapshotter", {
  requested: ["stream.subscribe", "cache.read", "cache.write"],
});
await writeFile(`${STATE}/snapshotter.token`, snapshotter, { mode: 0o644 });
console.log(`wrote node.token and snapshotter.token to ${STATE}`);
