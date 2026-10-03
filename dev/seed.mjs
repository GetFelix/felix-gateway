// Seeds the development stack the way a deployment would: bootstrap a tenant
// that trusts the dev IdP, exchange IdP tokens for Felix tokens, and create
// the room's streams. Writes the broker's credential and the gateway's token
// to the state directory. Safe to run again: existing objects are kept.
import { createHash } from "node:crypto";
import { chmod, mkdir, writeFile } from "node:fs/promises";

const CONTROL_PLANE = process.env.CONTROL_PLANE ?? "http://controlplane:8443";
const BOOTSTRAP = process.env.BOOTSTRAP ?? "http://controlplane:9095";
const BOOTSTRAP_TOKEN = process.env.BOOTSTRAP_TOKEN ?? "dev-bootstrap";
const IDP = process.env.IDP ?? "http://idp:9400";
const STATE = process.env.STATE_DIR ?? "/state";
const TENANT = process.env.CANVAS_TENANT ?? "canvas";
const NAMESPACE = process.env.CANVAS_NAMESPACE ?? "default";
const ROOM = process.env.CANVAS_ROOM ?? "lobby";
const AUDIENCE = "felix-canvas";
const RETENTION_SECONDS = 30 * 24 * 60 * 60;

// Felix keys RBAC on sha256(issuer|subject), not on the subject itself.
const principal = (subject) => createHash("sha256").update(`${IDP}|${subject}`).digest("hex");

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

async function idpToken(subject) {
  const { id_token } = await request("GET", `${IDP}/token?sub=${subject}&aud=${AUDIENCE}`);
  return id_token;
}

async function exchange(subject, body) {
  const token = await idpToken(subject);
  const url = `${CONTROL_PLANE}/v1/tenants/${TENANT}/token/exchange`;
  return (await request("POST", url, { token, body })).felix_token;
}

const streams = `stream:${TENANT}/${NAMESPACE}/*`;
const caches = `cache:${TENANT}/${NAMESPACE}/*`;
console.log(`bootstrap tenant ${TENANT}`);
await request("POST", `${BOOTSTRAP}/internal/bootstrap/tenants/${TENANT}/initialize`, {
  headers: { "x-felix-bootstrap-token": BOOTSTRAP_TOKEN },
  body: {
    display_name: "Felix Canvas (development)",
    idp_issuers: [
      {
        issuer: IDP,
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
      { subject: "role:gateway", object: streams, action: "stream.publish" },
      { subject: "role:gateway", object: streams, action: "stream.subscribe" },
      // Counters authorize as cache writes.
      { subject: "role:gateway", object: caches, action: "cache.write" },
    ],
    groupings: [
      { user: principal("canvas-admin"), role: "role:admin" },
      { user: principal("canvas-broker"), role: "role:broker" },
      { user: principal("canvas-gateway"), role: "role:gateway" },
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
  replication_factor: 1,
  retention: { max_age_seconds: durable ? RETENTION_SECONDS : null, max_size_bytes: null },
  consistency: "Leader",
  delivery: durable ? "AtLeastOnce" : "AtMostOnce",
  durable,
});
for (const [name, durable] of [
  [`canvas.ops.${ROOM}`, true],
  [`canvas.presence.${ROOM}`, false],
]) {
  console.log(`stream ${name}`);
  await request("POST", `${CONTROL_PLANE}/v1/tenants/${TENANT}/namespaces/${NAMESPACE}/streams`, {
    token: admin,
    body: stream(name, durable),
  });
}

console.log("cache canvas.seq");
await request("POST", `${CONTROL_PLANE}/v1/tenants/${TENANT}/namespaces/${NAMESPACE}/caches`, {
  token: admin,
  body: { cache: "canvas.seq", display_name: "Op sequence per session" },
});

// The broker runs as uid 65532 and writes its certificate here too.
await mkdir(STATE, { recursive: true });
await chmod(STATE, 0o777);
const broker = await exchange("canvas-broker", { audience: "felix-controlplane" });
await writeFile(`${STATE}/node.token`, broker, { mode: 0o644 });
const gateway = await exchange("canvas-gateway", {
  requested: ["stream.publish", "stream.subscribe", "cache.write"],
});
await writeFile(`${STATE}/gateway.token`, gateway, { mode: 0o644 });
console.log(`wrote ${STATE}/node.token and ${STATE}/gateway.token`);
