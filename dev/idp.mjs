// A stand-in OIDC identity provider for the development stack. Felix only
// issues tokens in exchange for an IdP token, so local runs and CI need one.
// It signs an ES256 token for any subject that asks: never expose it.
import { createServer } from "node:http";
import { generateKeyPairSync, randomUUID, sign } from "node:crypto";

const PORT = Number(process.env.IDP_PORT ?? 9400);
const ISSUER = process.env.IDP_ISSUER ?? `http://idp:${PORT}`;
const TOKEN_TTL_SECONDS = 24 * 60 * 60;

// A new key per start. Tokens Felix already issued stay valid, because Felix
// signs its own; only IdP tokens from before the restart stop working.
const { privateKey, publicKey } = generateKeyPairSync("ec", { namedCurve: "P-256" });
const kid = randomUUID();
const jwks = {
  keys: [{ ...publicKey.export({ format: "jwk" }), kid, alg: "ES256", use: "sig" }],
};

const base64url = (value) => Buffer.from(value).toString("base64url");

function mint(subject, audience) {
  const now = Math.floor(Date.now() / 1000);
  const header = base64url(JSON.stringify({ alg: "ES256", typ: "JWT", kid }));
  const claims = base64url(
    JSON.stringify({
      iss: ISSUER,
      sub: subject,
      aud: audience,
      iat: now,
      exp: now + TOKEN_TTL_SECONDS,
    }),
  );
  const signature = sign("sha256", Buffer.from(`${header}.${claims}`), {
    key: privateKey,
    dsaEncoding: "ieee-p1363",
  });
  return `${header}.${claims}.${base64url(signature)}`;
}

createServer((request, response) => {
  const url = new URL(request.url, ISSUER);
  const reply = (status, body) => {
    response.writeHead(status, { "content-type": "application/json" });
    response.end(JSON.stringify(body));
  };
  if (url.pathname === "/jwks.json") {
    return reply(200, jwks);
  }
  const subject = url.searchParams.get("sub");
  const audience = url.searchParams.get("aud");
  if (url.pathname === "/token" && subject && audience) {
    return reply(200, { id_token: mint(subject, audience) });
  }
  return reply(404, { error: "GET /jwks.json, or /token?sub=<subject>&aud=<audience>" });
}).listen(PORT, () => console.log(`dev IdP ${ISSUER} listening on ${PORT}`));
