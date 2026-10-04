// A stand-in OpenID Connect provider for the development stack. It runs the
// authorization code flow with PKCE that a browser app uses against a real
// provider, but signs in anyone who picks a name: never expose it.
//
// GET /token?sub=<subject>&aud=<audience> also mints an ID token directly,
// for the seed script and the gateway's integration tests.
import { createServer } from "node:http";
import { createHash, generateKeyPairSync, randomBytes, randomUUID, sign } from "node:crypto";

const PORT = Number(process.env.IDP_PORT ?? 9400);
const ISSUER = process.env.IDP_ISSUER ?? `http://127.0.0.1:${PORT}`;
const USERS = (process.env.IDP_USERS ?? "ana,ben").split(",");
const TOKEN_TTL_SECONDS = 24 * 60 * 60;
const CODE_TTL_MS = 60_000;

// A new key per start. Tokens Felix already issued stay valid, because Felix
// signs its own; only ID tokens from before the restart stop working.
const { privateKey, publicKey } = generateKeyPairSync("ec", { namedCurve: "P-256" });
const kid = randomUUID();
const jwks = {
  keys: [{ ...publicKey.export({ format: "jwk" }), kid, alg: "ES256", use: "sig" }],
};

const base64url = (value) => Buffer.from(value).toString("base64url");

function mint(subject, audience, nonce) {
  const now = Math.floor(Date.now() / 1000);
  const header = base64url(JSON.stringify({ alg: "ES256", typ: "JWT", kid }));
  const claims = base64url(
    JSON.stringify({
      iss: ISSUER,
      sub: subject,
      aud: audience,
      name: subject.charAt(0).toUpperCase() + subject.slice(1),
      iat: now,
      exp: now + TOKEN_TTL_SECONDS,
      ...(nonce ? { nonce } : {}),
    }),
  );
  const signature = sign("sha256", Buffer.from(`${header}.${claims}`), {
    key: privateKey,
    dsaEncoding: "ieee-p1363",
  });
  return `${header}.${claims}.${base64url(signature)}`;
}

/** Codes handed out by /authorize and not yet redeemed at /token. */
const codes = new Map();

const discovery = {
  issuer: ISSUER,
  authorization_endpoint: `${ISSUER}/authorize`,
  token_endpoint: `${ISSUER}/token`,
  jwks_uri: `${ISSUER}/jwks.json`,
  response_types_supported: ["code"],
  grant_types_supported: ["authorization_code"],
  subject_types_supported: ["public"],
  id_token_signing_alg_values_supported: ["ES256"],
  code_challenge_methods_supported: ["S256"],
  scopes_supported: ["openid", "profile"],
};

const escape = (text) => text.replace(/[&<>"']/g, (char) => `&#${char.charCodeAt(0)};`);

function chooser(url) {
  const link = (user) => {
    const next = new URL(url);
    next.searchParams.set("sub", user);
    return `<a class="user" href="${escape(next.pathname + next.search)}">Continue as ${escape(user)}</a>`;
  };
  const hidden = [...url.searchParams]
    .map(([key, value]) => `<input type="hidden" name="${escape(key)}" value="${escape(value)}">`)
    .join("");
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Development sign-in</title>
<style>
  :root { color-scheme: light dark; font: 15px system-ui, sans-serif; }
  body { margin: 0; min-height: 100vh; display: grid; place-items: center; background: Canvas; }
  main { width: min(360px, calc(100vw - 32px)); display: grid; gap: 12px; }
  h1 { font-size: 20px; margin: 0; } p { margin: 0; opacity: .7; }
  .user, button { display: block; padding: 10px 14px; border: 1px solid #8884; border-radius: 8px;
    color: inherit; background: none; font: inherit; text-align: left; text-decoration: none; cursor: pointer; }
  form { display: flex; gap: 8px; }
  input[name=sub] { flex: 1; padding: 10px; border-radius: 8px; border: 1px solid #8884; font: inherit; }
</style></head>
<body><main>
  <h1>Development sign-in</h1>
  <p>This stand-in signs in anyone. Pick a name.</p>
  ${USERS.map(link).join("\n  ")}
  <form method="get" action="/authorize">${hidden}<input name="sub" placeholder="Another name" aria-label="Name" required><button>Continue</button></form>
</main></body></html>`;
}

function authorize(url, reply, redirect) {
  const params = url.searchParams;
  const redirectUri = params.get("redirect_uri");
  const clientId = params.get("client_id");
  const challenge = params.get("code_challenge");
  if (!redirectUri || !clientId || !challenge || params.get("code_challenge_method") !== "S256") {
    return reply(400, {
      error: "invalid_request",
      error_description: "PKCE with S256 is required",
    });
  }
  const subject = params.get("sub");
  if (!subject) return reply(200, chooser(url), "text/html; charset=utf-8");
  const code = base64url(randomBytes(24));
  codes.set(code, {
    subject,
    clientId,
    redirectUri,
    challenge,
    nonce: params.get("nonce"),
    expires: Date.now() + CODE_TTL_MS,
  });
  const back = new URL(redirectUri);
  back.searchParams.set("code", code);
  if (params.has("state")) back.searchParams.set("state", params.get("state"));
  return redirect(back.href);
}

function redeem(form, reply) {
  const code = form.get("code") ?? "";
  const grant = codes.get(code);
  codes.delete(code);
  const challenge = createHash("sha256")
    .update(form.get("code_verifier") ?? "")
    .digest("base64url");
  if (
    form.get("grant_type") !== "authorization_code" ||
    !grant ||
    grant.expires < Date.now() ||
    grant.clientId !== form.get("client_id") ||
    grant.redirectUri !== form.get("redirect_uri") ||
    grant.challenge !== challenge
  ) {
    return reply(400, { error: "invalid_grant" });
  }
  return reply(200, {
    id_token: mint(grant.subject, grant.clientId, grant.nonce),
    token_type: "Bearer",
    expires_in: TOKEN_TTL_SECONDS,
  });
}

createServer((request, response) => {
  const url = new URL(request.url, ISSUER);
  // The browser calls discovery and the token endpoint from the app's origin.
  const reply = (status, body, type = "application/json") => {
    response.writeHead(status, {
      "content-type": type,
      "cache-control": "no-store",
      "access-control-allow-origin": "*",
    });
    response.end(typeof body === "string" ? body : JSON.stringify(body));
  };
  const redirect = (location) => {
    response.writeHead(302, { location });
    response.end();
  };
  if (url.pathname === "/jwks.json") return reply(200, jwks);
  if (url.pathname === "/.well-known/openid-configuration") return reply(200, discovery);
  if (url.pathname === "/authorize") return authorize(url, reply, redirect);
  if (url.pathname === "/token" && request.method === "POST") {
    let body = "";
    request.on("data", (chunk) => (body += chunk));
    request.on("end", () => redeem(new URLSearchParams(body), reply));
    return;
  }
  const subject = url.searchParams.get("sub");
  const audience = url.searchParams.get("aud");
  if (url.pathname === "/token" && subject && audience) {
    return reply(200, { id_token: mint(subject, audience) });
  }
  return reply(404, { error: "not found" });
}).listen(PORT, () => console.log(`dev IdP ${ISSUER} listening on ${PORT}`));
