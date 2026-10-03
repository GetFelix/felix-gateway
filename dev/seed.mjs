// Seeds a deployment: bootstrap a tenant that trusts the browsers' identity
// provider, create each room's streams and caches, and give each room a role
// whose members may open it. Writes the broker's credential and the
// snapshotter's token to the state directory. Safe to run again: existing
// objects are kept, and rooms or members added to CANVAS_ROOMS are created.
//
// Every setting is optional and defaults to the development stack in dev/.
// docs/self-hosting.md describes each one.
import { createHash } from "node:crypto";
import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";

const env = (name, fallback) => process.env[name] || fallback;

const CONTROL_PLANE = env("CANVAS_FELIX_CONTROL_PLANE", "http://controlplane:8443");
const BOOTSTRAP = env("CANVAS_FELIX_BOOTSTRAP", "http://controlplane:9095");
const BOOTSTRAP_TOKEN = env("CANVAS_FELIX_BOOTSTRAP_TOKEN", "dev-bootstrap");
const STATE = env("CANVAS_STATE_DIR", "/state");
const TENANT = env("CANVAS_TENANT", "canvas");
const NAMESPACE = env("CANVAS_NAMESPACE", "default");
// Felix only issues tokens in exchange for an IdP token, so the service
// accounts (admin, broker, snapshotter) sign in with dev/idp.mjs, which must
// be reachable only from inside the deployment. See felix#954.
const SERVICE_IDP = env("CANVAS_SERVICE_IDP", "http://idp:9400");
const RETENTION_SECONDS = 30 * 24 * 60 * 60;
// 3 in the three-broker stack, so a room survives losing any one broker.
const REPLICAS = Number(process.env.CANVAS_REPLICAS ?? 1);
// Felix only promotes a replica it knows holds every acknowledged record when
// the ack waited for a majority.
const CONSISTENCY = REPLICAS > 1 ? "Quorum" : "Leader";

// `room=member,member`, separated by spaces. A member is the value of the
// browsers' subject claim, or `group:<name>` for everyone in an IdP group.
// Ana is in both rooms, so the tests can show that a session in one room
// cannot reach the other even for someone allowed in both.
const MEMBERS = Object.fromEntries(
  env("CANVAS_ROOMS", "lobby=ana,ben,cleo studio=ana")
    .split(/\s+/)
    .filter(Boolean)
    .map((entry) => {
      const [room, members = ""] = entry.split("=");
      return [room, members.split(",").filter(Boolean)];
    }),
);

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

// A provider started alongside the seed may not be answering yet.
async function discover(base) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await request("GET", `${base.replace(/\/$/, "")}/.well-known/openid-configuration`);
    } catch (err) {
      if (attempt === 30) throw err;
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
}

const service = await discover(SERVICE_IDP);
const ISSUER = env("CANVAS_OIDC_ISSUER", service.issuer);
const AUDIENCE = env("CANVAS_OIDC_AUDIENCE", env("CANVAS_OIDC_CLIENT_ID", "felix-canvas"));
const JWKS_URL =
  process.env.CANVAS_OIDC_JWKS_URL ||
  (ISSUER === service.issuer ? `${SERVICE_IDP}/jwks.json` : (await discover(ISSUER)).jwks_uri);

// Felix keys RBAC on sha256(issuer|subject), not on the subject itself.
const principal = (issuer, subject) =>
  createHash("sha256").update(`${issuer}|${subject}`).digest("hex");
const serviceAccount = (name) => principal(service.issuer, name);
const member = (name) =>
  name.startsWith("group:") ? `group:${ISSUER}#${name.slice(6)}` : principal(ISSUER, name);

async function exchange(subject, body) {
  const url = `${SERVICE_IDP}/token?sub=${subject}&aud=felix-canvas`;
  const { id_token } = await request("GET", url);
  const exchangeUrl = `${CONTROL_PLANE}/v1/tenants/${TENANT}/token/exchange`;
  return (await request("POST", exchangeUrl, { token: id_token, body })).felix_token;
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

const groupsClaim = env("CANVAS_OIDC_GROUPS_CLAIM", "");
const browsers = {
  issuer: ISSUER,
  audiences: [AUDIENCE],
  jwks_url: JWKS_URL,
  claim_mappings: {
    subject_claim: env("CANVAS_OIDC_SUBJECT_CLAIM", "sub"),
    ...(groupsClaim ? { groups_claim: groupsClaim } : {}),
  },
};
const services = {
  issuer: service.issuer,
  audiences: ["felix-canvas"],
  jwks_url: `${SERVICE_IDP}/jwks.json`,
  claim_mappings: { subject_claim: "sub" },
};

console.log(`bootstrap tenant ${TENANT}`);
await request("POST", `${BOOTSTRAP}/internal/bootstrap/tenants/${TENANT}/initialize`, {
  headers: { "x-felix-bootstrap-token": BOOTSTRAP_TOKEN },
  body: {
    display_name: "Felix Canvas",
    idp_issuers: [services],
    initial_admin_principals: [serviceAccount("canvas-admin")],
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
    ],
    groupings: [
      { user: serviceAccount("canvas-admin"), role: "role:admin" },
      { user: serviceAccount("canvas-broker"), role: "role:broker" },
      { user: serviceAccount("canvas-snapshotter"), role: "role:snapshotter" },
    ],
  },
});

const admin = await exchange("canvas-admin", { audience: "felix-controlplane" });
if (ISSUER !== service.issuer) {
  console.log(`trust ${ISSUER} for browsers`);
  await request("POST", `${CONTROL_PLANE}/v1/tenants/${TENANT}/idp-issuers`, {
    token: admin,
    body: browsers,
  });
}

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
for (const [room, members] of Object.entries(MEMBERS)) {
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
  // Adding a rule or an assignment that exists already changes nothing.
  console.log(`role:room-${room} for ${members.join(", ") || "nobody"}`);
  for (const policy of roomPolicies(room)) {
    await request("POST", `${CONTROL_PLANE}/v1/tenants/${TENANT}/rbac/policies`, {
      token: admin,
      body: policy,
    });
  }
  for (const name of members) {
    await request("POST", `${CONTROL_PLANE}/v1/tenants/${TENANT}/rbac/groupings`, {
      token: admin,
      body: { user: member(name), role: `role:room-${room}` },
    });
  }
}

// The dev stack's broker runs as uid 65532 and writes its certificate here
// too. Elsewhere the directory is already the seed's own, or not ours to
// change, as with a Kubernetes emptyDir.
await mkdir(STATE, { recursive: true });
await chmod(STATE, 0o777).catch(() => {});
const broker = await exchange("canvas-broker", { audience: "felix-controlplane" });
await writeFile(`${STATE}/node.token`, broker, { mode: 0o644 });
const snapshotter = await exchange("canvas-snapshotter", {
  requested: ["stream.subscribe", "cache.read", "cache.write"],
});
await writeFile(`${STATE}/snapshotter.token`, snapshotter, { mode: 0o644 });
console.log(`wrote node.token and snapshotter.token to ${STATE}`);

// In Kubernetes the tokens also go to Secrets, which the broker and
// snapshotter pods mount. Needs NODE_EXTRA_CA_CERTS set to the cluster's CA.
async function storeSecret(name, token) {
  const account = "/var/run/secrets/kubernetes.io/serviceaccount";
  const namespace = (await readFile(`${account}/namespace`, "utf8")).trim();
  const auth = { token: (await readFile(`${account}/token`, "utf8")).trim() };
  const secrets = `https://kubernetes.default.svc/api/v1/namespaces/${namespace}/secrets`;
  const body = { apiVersion: "v1", kind: "Secret", metadata: { name }, stringData: { token } };
  if ((await request("POST", secrets, { ...auth, body })) === null) {
    await request("PUT", `${secrets}/${name}`, { ...auth, body });
  }
  console.log(`stored a token in Secret ${name}`);
}
for (const [variable, token] of [
  ["CANVAS_BROKER_SECRET", broker],
  ["CANVAS_SNAPSHOTTER_SECRET", snapshotter],
]) {
  if (process.env[variable]) await storeSecret(process.env[variable], token);
}
