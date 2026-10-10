//! Per-session Felix credentials. A browser's sign-in is exchanged at the
//! Felix control plane for a token narrowed to one scope, so the broker, not
//! the gateway, is what keeps a session out of every other scope.
//!
//! With shared connections, each scope token is then delegated to the
//! gateway: the control plane reissues it with `act` naming the gateway, so a
//! broker that binds tokens to client certificates accepts it on the
//! gateway's connection, still limited to the user's grants.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use felix_client::{RefreshingToken, TokenProvider};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::scope::{Permission, Scope};

/// Why a sign-in was not exchanged for a scope token.
#[derive(Debug)]
pub enum Refused {
    /// No usable sign-in: missing, expired, or from an identity provider the
    /// tenant does not trust. Signing in again may help.
    SignedOut,
    /// Signed in, but not allowed in the scope, or the scope does not exist.
    Forbidden,
    /// The control plane could not be asked or gave an answer that made no sense.
    Unavailable(anyhow::Error),
}

/// A scope token and the refresh token that replaces it.
pub(crate) struct Grant {
    pub(crate) felix_token: String,
    refresh_token: String,
    /// Who the token is for: its `sub` claim.
    pub(crate) principal: String,
    /// Aliases of the optional resources the token does not reach.
    pub(crate) missing: Vec<String>,
}

#[derive(Deserialize)]
struct TokenResponse {
    felix_token: String,
    refresh_token: String,
}

#[derive(Deserialize)]
struct DelegateResponse {
    access_token: String,
}

/// The RFC 8693 grant `/token/delegate` takes.
const TOKEN_EXCHANGE_GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";

/// The RFC 8693 type of every token the gateway hands the control plane.
const JWT_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:jwt";

/// A gateway token this close to expiry is replaced before use, so a
/// delegation never goes out with one about to lapse.
const RENEW_BEFORE: Duration = Duration::from_secs(60);

/// Why `/token/delegate` refused.
#[derive(Debug)]
enum Delegation {
    /// The gateway's own token was not accepted; a fresh one may be.
    ActorRefused,
    /// The gateway may not act for users, or not for this token.
    Forbidden(String),
    Failed(anyhow::Error),
}

/// The Felix control plane, as the gateway uses it.
#[derive(Clone)]
pub(crate) struct ControlPlane {
    http: reqwest::Client,
    url: String,
    tenant: String,
    namespace: String,
}

impl ControlPlane {
    pub(crate) fn new(url: &str, tenant: &str, namespace: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            url: url.to_string(),
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
        }
    }

    /// Exchange `id_token` for a token that reaches `scope` and nothing else.
    ///
    /// The exchange only narrows what RBAC grants, so membership is decided
    /// by the scope's role in Felix. A sign-in whose token lacks any required
    /// grant is refused here rather than failing piecemeal later.
    ///
    /// With shared connections, `actor` is the gateway: its control-plane
    /// token goes along as the RFC 8693 `actor_token`, because Felix only
    /// delegates a token minted for the gateway that asks.
    pub(crate) async fn exchange(
        &self,
        id_token: &str,
        scope: &Scope,
        actor: Option<&Actor>,
    ) -> Result<Grant, Refused> {
        let permissions = scope.permissions(&self.tenant, &self.namespace);
        let (actions, objects) = split(&permissions);
        let mut request = json!({ "requested": actions, "resources": objects });
        let mut fresh = false;
        let response = loop {
            if let Some(actor) = actor {
                let token = actor
                    .control_plane_token(fresh)
                    .await
                    .map_err(Refused::Unavailable)?;
                request["actor_token"] = token.into();
                request["actor_token_type"] = JWT_TOKEN_TYPE.into();
            }
            let response = self
                .http
                .post(format!(
                    "{}/v1/tenants/{}/token/exchange",
                    self.url, self.tenant
                ))
                .bearer_auth(id_token)
                .json(&request)
                .send()
                .await
                .map_err(|err| Refused::Unavailable(err.into()))?;
            if actor.is_none() || response.status() != StatusCode::FORBIDDEN {
                break response;
            }
            // A 403 is either the user's RBAC or the gateway's own token.
            let body = response.text().await.unwrap_or_default();
            if !refuses_actor(&body) {
                return Err(Refused::Forbidden);
            }
            if !fresh {
                fresh = true;
                continue;
            }
            tracing::error!(
                %body,
                "the control plane refused the gateway's token as the actor of an exchange; \
                 does the gateway's principal hold token.delegate on the tenant?"
            );
            return Err(Refused::Unavailable(anyhow::anyhow!(
                "the gateway cannot act for users"
            )));
        };
        match response.status() {
            StatusCode::UNAUTHORIZED => return Err(Refused::SignedOut),
            StatusCode::FORBIDDEN => return Err(Refused::Forbidden),
            // 409 included: a Raft control plane answers it until every
            // member understands actor tokens.
            status if !status.is_success() => {
                return Err(Refused::Unavailable(anyhow::anyhow!(
                    "the token exchange answered {status}"
                )));
            }
            _ => {}
        }
        let answer: TokenResponse = response
            .json()
            .await
            .map_err(|err| Refused::Unavailable(err.into()))?;
        let claims = claims(&answer.felix_token).ok_or(Refused::Forbidden)?;
        let missing = missing(&claims, &permissions).ok_or(Refused::Forbidden)?;
        Ok(Grant {
            felix_token: answer.felix_token,
            refresh_token: answer.refresh_token,
            principal: claims.sub,
            missing,
        })
    }

    /// Tokens for one session, starting from `grant` and refreshed before
    /// each expires. A refresh re-runs RBAC with the same narrowing, so a
    /// member removed from the scope loses access at the next one.
    pub(crate) fn tokens(&self, grant: Grant) -> Arc<dyn TokenProvider> {
        let control_plane = self.clone();
        let refresh_token = Arc::new(Mutex::new(grant.refresh_token));
        Arc::new(RefreshingToken::with_initial(
            grant.felix_token,
            move || {
                let control_plane = control_plane.clone();
                let refresh_token = Arc::clone(&refresh_token);
                async move { control_plane.refresh(&refresh_token).await }
            },
        ))
    }

    // RefreshingToken runs one fetch at a time, so the lock is never held by
    // two refreshes; it only carries each single-use token to the next call.
    async fn refresh(&self, refresh_token: &Mutex<String>) -> Result<String> {
        let current = refresh_token.lock().expect("refresh token lock").clone();
        let answer: TokenResponse = self
            .http
            .post(format!(
                "{}/v1/tenants/{}/token/refresh",
                self.url, self.tenant
            ))
            .json(&json!({ "refresh_token": current }))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .context("refresh the scope token")?
            .json()
            .await
            .context("read the refreshed scope token")?;
        *refresh_token.lock().expect("refresh token lock") = answer.refresh_token;
        Ok(answer.felix_token)
    }
}

impl ControlPlane {
    /// Exchange the gateway's own `credential` for a Felix token, asking with
    /// `request` for its audience and narrowing.
    async fn own_token(&self, credential: &str, request: Value) -> Result<String> {
        let answer: TokenResponse = self
            .http
            .post(format!(
                "{}/v1/tenants/{}/token/exchange",
                self.url, self.tenant
            ))
            .bearer_auth(credential)
            .json(&request)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .context("exchange the gateway's credential")?
            .json()
            .await
            .context("read the gateway's token")?;
        Ok(answer.felix_token)
    }

    /// Have `subject`, a user's scope token, reissued to name the holder of
    /// `actor` as the one presenting it.
    async fn delegate(&self, actor: &str, subject: &str) -> Result<String, Delegation> {
        let response = self
            .http
            .post(format!(
                "{}/v1/tenants/{}/token/delegate",
                self.url, self.tenant
            ))
            .bearer_auth(actor)
            .json(&json!({
                "grant_type": TOKEN_EXCHANGE_GRANT,
                "subject_token": subject,
                "subject_token_type": JWT_TOKEN_TYPE,
            }))
            .send()
            .await
            .map_err(|err| Delegation::Failed(err.into()))?;
        match response.status() {
            StatusCode::UNAUTHORIZED => return Err(Delegation::ActorRefused),
            StatusCode::FORBIDDEN => {
                let body = response.text().await.unwrap_or_default();
                return Err(Delegation::Forbidden(body));
            }
            status if !status.is_success() => {
                return Err(Delegation::Failed(anyhow::anyhow!(
                    "the token delegation answered {status}"
                )));
            }
            _ => {}
        }
        let answer: DelegateResponse = response
            .json()
            .await
            .map_err(|err| Delegation::Failed(err.into()))?;
        Ok(answer.access_token)
    }

    /// Tokens for one session on a shared connection: `delegated` first, then
    /// on each refresh the user's scope token is refreshed and delegated
    /// again. A delegated token has no refresh token of its own, and its
    /// expiry is never later than the scope token's.
    pub(crate) fn delegated_tokens(
        &self,
        grant: Grant,
        delegated: String,
        actor: Arc<Actor>,
    ) -> Arc<dyn TokenProvider> {
        let control_plane = self.clone();
        let refresh_token = Arc::new(Mutex::new(grant.refresh_token));
        Arc::new(RefreshingToken::with_initial(delegated, move || {
            let control_plane = control_plane.clone();
            let refresh_token = Arc::clone(&refresh_token);
            let actor = Arc::clone(&actor);
            async move {
                let scope_token = control_plane.refresh(&refresh_token).await?;
                actor
                    .delegate(&scope_token)
                    .await
                    .map_err(|refused| match refused {
                        Refused::Unavailable(err) => err,
                        other => anyhow::anyhow!("delegate the refreshed token: {other:?}"),
                    })
            }
        }))
    }
}

/// The gateway's own identity at the control plane: an ID token in a file,
/// exchanged for a `felix-controlplane` token that may delegate users'
/// tokens, and for a broker token for its own connections.
pub(crate) struct Actor {
    control_plane: ControlPlane,
    credential_file: PathBuf,
    /// The control-plane token and its `exp`, kept until close to expiry.
    held: tokio::sync::Mutex<Option<(String, Option<u64>)>>,
}

impl Actor {
    pub(crate) fn new(control_plane: ControlPlane, credential_file: PathBuf) -> Self {
        Self {
            control_plane,
            credential_file,
            held: tokio::sync::Mutex::new(None),
        }
    }

    fn credential(&self) -> Result<String> {
        let token = std::fs::read_to_string(&self.credential_file)
            .with_context(|| format!("read {}", self.credential_file.display()))?;
        Ok(token.trim().to_string())
    }

    /// A broker token for the gateway's own principal, which is what its
    /// shared connections open with. It carries every broker grant the
    /// principal has; the gateway sends no requests of its own on it.
    pub(crate) async fn broker_token(&self) -> Result<String> {
        let credential = self.credential()?;
        self.control_plane
            .own_token(&credential, json!({ "audience": "felix-broker" }))
            .await
    }

    /// The `felix-controlplane` token to delegate with, narrowed to
    /// `token.delegate`.
    async fn control_plane_token(&self, fresh: bool) -> Result<String> {
        let mut held = self.held.lock().await;
        if !fresh
            && let Some((token, expires)) = held.as_ref()
            && expires.is_none_or(|exp| exp > now() + RENEW_BEFORE.as_secs())
        {
            return Ok(token.clone());
        }
        let credential = self.credential()?;
        let token = self
            .control_plane
            .own_token(
                &credential,
                json!({ "audience": "felix-controlplane", "requested": ["token.delegate"] }),
            )
            .await?;
        let expires = claims(&token).and_then(|claims| claims.exp);
        *held = Some((token.clone(), expires));
        Ok(token)
    }

    /// `scope_token` delegated to the gateway. A refusal is the gateway's
    /// setup, not the user's, so it is reported as unavailable and logged in
    /// full for the operator.
    pub(crate) async fn delegate(&self, scope_token: &str) -> Result<String, Refused> {
        let mut fresh = false;
        loop {
            let actor = self
                .control_plane_token(fresh)
                .await
                .map_err(Refused::Unavailable)?;
            match self.control_plane.delegate(&actor, scope_token).await {
                Ok(token) => return Ok(token),
                Err(Delegation::ActorRefused) if !fresh => fresh = true,
                Err(Delegation::ActorRefused) => {
                    tracing::error!("the control plane does not accept the gateway's token");
                    return Err(Refused::Unavailable(anyhow::anyhow!(
                        "the gateway cannot act for users"
                    )));
                }
                Err(Delegation::Forbidden(detail)) => {
                    tracing::error!(
                        %detail,
                        "the control plane refused to delegate; does the gateway's principal \
                         hold token.delegate on the tenant?"
                    );
                    return Err(Refused::Unavailable(anyhow::anyhow!(
                        "the gateway cannot act for users"
                    )));
                }
                Err(Delegation::Failed(err)) => return Err(Refused::Unavailable(err)),
            }
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// The distinct actions and objects in `permissions`, which is how the
/// exchange takes a narrowing.
fn split(permissions: &[Permission]) -> (BTreeSet<&str>, BTreeSet<&str>) {
    permissions
        .iter()
        .filter_map(|permission| permission.grant.split_once(':'))
        .unzip()
}

/// Whether a 403 body from the exchange refuses the gateway's actor token
/// rather than the user.
fn refuses_actor(body: &str) -> bool {
    serde_json::from_str::<Value>(body).is_ok_and(|body| body["code"] == "actor_refused")
}

#[derive(Deserialize)]
struct Claims {
    #[serde(default)]
    sub: String,
    #[serde(default)]
    perms: Vec<String>,
    exp: Option<u64>,
}

/// A Felix token's claims. The token came straight from the control plane;
/// checking its signature is the broker's job.
fn claims(token: &str) -> Option<Claims> {
    let payload = token.split('.').nth(1)?;
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()
}

/// The optional resources `claims` do not reach, or `None` when they miss a
/// required permission.
fn missing(claims: &Claims, permissions: &[Permission]) -> Option<Vec<String>> {
    let mut missing = Vec::new();
    for permission in permissions {
        if claims.perms.contains(&permission.grant) {
            continue;
        }
        let alias = permission.optional.as_ref()?;
        if !missing.contains(alias) {
            missing.push(alias.clone());
        }
    }
    Some(missing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scope::tests::lobby;
    use axum::Json;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::routing::post;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    fn token(perms: &[&str]) -> String {
        let claims = URL_SAFE_NO_PAD.encode(json!({ "sub": "ana", "perms": perms }).to_string());
        format!("e30.{claims}.c2ln")
    }

    #[test]
    fn the_narrowing_names_each_action_and_object_once() {
        let permissions = lobby().permissions("t", "default");
        let (actions, objects) = split(&permissions);
        assert_eq!(
            actions.into_iter().collect::<Vec<_>>(),
            [
                "cache.read",
                "cache.write",
                "stream.publish",
                "stream.subscribe"
            ]
        );
        assert_eq!(
            objects.into_iter().collect::<Vec<_>>(),
            [
                "cache:t/default/app.members.lobby",
                "cache:t/default/app.seq.lobby",
                "stream:t/default/app.input.lobby",
                "stream:t/default/app.ops.lobby",
            ]
        );
    }

    #[test]
    fn a_token_missing_a_required_grant_does_not_cover_the_scope() {
        let permissions = lobby().permissions("t", "default");
        let all: Vec<&str> = permissions.iter().map(|p| p.grant.as_str()).collect();
        let covered = |token: &str| claims(token).and_then(|claims| missing(&claims, &permissions));
        assert_eq!(covered(&token(&all)), Some(vec![]));
        assert_eq!(claims(&token(&all)).unwrap().sub, "ana");
        assert_eq!(covered(&token(&all[1..])), None);
        assert_eq!(covered(&token(&[])), None);
        assert_eq!(covered("not a token"), None);
    }

    #[test]
    fn a_token_without_an_optional_grant_reports_it_missing() {
        let permissions = lobby().permissions("t", "default");
        let without_input: Vec<&str> = permissions
            .iter()
            .filter(|p| p.optional.is_none())
            .map(|p| p.grant.as_str())
            .collect();
        let claims = claims(&token(&without_input)).unwrap();
        assert_eq!(
            missing(&claims, &permissions),
            Some(vec!["input".to_string()])
        );
    }

    fn jwt(claims: Value) -> String {
        format!("e30.{}.c2ln", URL_SAFE_NO_PAD.encode(claims.to_string()))
    }

    /// A stand-in control plane that records each call.
    #[derive(Default)]
    struct Fake {
        /// Path, bearer and body of each call.
        calls: Mutex<Vec<(String, Option<String>, Value)>>,
        /// Statuses `/token/delegate` answers with before it succeeds.
        refusals: Mutex<Vec<StatusCode>>,
        /// Statuses and error codes a user's exchange answers with before it
        /// succeeds.
        user_refusals: Mutex<Vec<(StatusCode, &'static str)>>,
        exchanges: AtomicUsize,
        /// `exp` of the gateway tokens the exchange mints.
        gateway_exp: AtomicU64,
    }

    impl Fake {
        fn record(&self, path: &str, headers: &HeaderMap, body: &Value) {
            let bearer = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
                .map(str::to_string);
            self.calls
                .lock()
                .unwrap()
                .push((path.to_string(), bearer, body.clone()));
        }

        fn calls(&self, path: &str) -> Vec<(Option<String>, Value)> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(called, _, _)| called == path)
                .map(|(_, bearer, body)| (bearer.clone(), body.clone()))
                .collect()
        }
    }

    async fn serve(fake: Arc<Fake>) -> ControlPlane {
        let router = axum::Router::new()
            .route(
                "/v1/tenants/t/token/exchange",
                post(
                    |State(fake): State<Arc<Fake>>, headers: HeaderMap, Json(body): Json<Value>| async move {
                        fake.record("exchange", &headers, &body);
                        if body.get("audience").is_none() {
                            return user_exchange(&fake, &body);
                        }
                        let n = fake.exchanges.fetch_add(1, Ordering::SeqCst);
                        let exp = fake.gateway_exp.load(Ordering::SeqCst);
                        Ok(Json(json!({
                            "felix_token": jwt(json!({ "sub": "gateway", "exp": exp, "n": n })),
                            "refresh_token": "unused",
                        })))
                    },
                ),
            )
            .route(
                "/v1/tenants/t/token/refresh",
                post(
                    |State(fake): State<Arc<Fake>>, headers: HeaderMap, Json(body): Json<Value>| async move {
                        fake.record("refresh", &headers, &body);
                        Json(json!({
                            "felix_token": jwt(json!({ "sub": "ana", "exp": now() + 900, "n": 2 })),
                            "refresh_token": "r2",
                        }))
                    },
                ),
            )
            .route(
                "/v1/tenants/t/token/delegate",
                post(
                    |State(fake): State<Arc<Fake>>, headers: HeaderMap, Json(body): Json<Value>| async move {
                        fake.record("delegate", &headers, &body);
                        if let Some(status) = fake.refusals.lock().unwrap().pop() {
                            return Err(status);
                        }
                        let subject = body["subject_token"].as_str().unwrap_or_default();
                        Ok(Json(json!({
                            "access_token": format!("delegated:{subject}"),
                            "issued_token_type": "urn:ietf:params:oauth:token-type:jwt",
                            "token_type": "Bearer",
                            "expires_in": 900,
                        })))
                    },
                ),
            )
            .with_state(fake);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await });
        ControlPlane::new(&format!("http://{addr}"), "t", "default")
    }

    /// A user's exchange: the next refusal, or a token holding every
    /// permission asked for.
    fn user_exchange(fake: &Fake, body: &Value) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
        if let Some((status, code)) = fake.user_refusals.lock().unwrap().pop() {
            return Err((
                status,
                Json(json!({ "code": code, "message": "refused", "request_id": null })),
            ));
        }
        let names = |key: &str| -> Vec<String> {
            body[key]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        };
        let perms: Vec<String> = names("requested")
            .iter()
            .flat_map(|action| {
                names("resources")
                    .into_iter()
                    .map(move |object| format!("{action}:{object}"))
            })
            .collect();
        Ok(Json(json!({
            "felix_token": jwt(json!({ "sub": "ana", "perms": perms })),
            "refresh_token": "r1",
        })))
    }

    /// A credential file holding `token`, unique to `test`.
    fn credential(test: &str, token: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("felix-gateway-{test}-{}.token", std::process::id()));
        std::fs::write(&path, format!("{token}\n")).unwrap();
        path
    }

    async fn actor(test: &str, gateway_exp: u64) -> (Arc<Fake>, ControlPlane, Arc<Actor>, PathBuf) {
        let fake = Arc::new(Fake::default());
        fake.gateway_exp.store(gateway_exp, Ordering::SeqCst);
        let control_plane = serve(Arc::clone(&fake)).await;
        let file = credential(test, "gateway-id-token");
        let actor = Arc::new(Actor::new(control_plane.clone(), file.clone()));
        (fake, control_plane, actor, file)
    }

    #[tokio::test]
    async fn a_scope_token_is_delegated_with_the_gateways_own_token() {
        let (fake, _, actor, _) = actor("delegate", now() + 3600).await;
        let delegated = actor.delegate("ana-scope-token").await.unwrap();
        assert_eq!(delegated, "delegated:ana-scope-token");

        let exchanges = fake.calls("exchange");
        assert_eq!(exchanges.len(), 1);
        let (bearer, body) = &exchanges[0];
        assert_eq!(bearer.as_deref(), Some("gateway-id-token"));
        assert_eq!(
            body,
            &json!({ "audience": "felix-controlplane", "requested": ["token.delegate"] })
        );
        let delegations = fake.calls("delegate");
        let (bearer, body) = &delegations[0];
        assert_eq!(claims(bearer.as_deref().unwrap()).unwrap().sub, "gateway");
        assert_eq!(body["grant_type"], TOKEN_EXCHANGE_GRANT);
        assert_eq!(body["subject_token"], "ana-scope-token");
        assert!(body.get("permissions").is_none(), "{body}");
    }

    #[tokio::test]
    async fn a_refresh_delegates_the_refreshed_scope_token() {
        let (fake, control_plane, actor, _) = actor("refresh", now() + 3600).await;
        let grant = Grant {
            felix_token: "ana-scope-token".into(),
            refresh_token: "r1".into(),
            principal: "ana".into(),
            missing: vec![],
        };
        // Already expired, so the first use refreshes.
        let initial = jwt(json!({ "sub": "ana", "iat": now() - 900, "exp": now() - 1 }));
        let tokens = control_plane.delegated_tokens(grant, initial, actor);
        let token = tokens.token().await.unwrap();

        let refreshes = fake.calls("refresh");
        assert_eq!(refreshes.len(), 1);
        assert_eq!(refreshes[0].1, json!({ "refresh_token": "r1" }));
        let delegations = fake.calls("delegate");
        assert_eq!(delegations.len(), 1);
        let refreshed = delegations[0].1["subject_token"].as_str().unwrap();
        assert_eq!(claims(refreshed).unwrap().sub, "ana");
        assert_eq!(token, format!("delegated:{refreshed}"));
    }

    #[tokio::test]
    async fn the_gateway_token_is_kept_until_it_nears_expiry() {
        let (fake, _, actor, _) = actor("kept", now() + 3600).await;
        actor.delegate("one").await.unwrap();
        actor.delegate("two").await.unwrap();
        assert_eq!(fake.exchanges.load(Ordering::SeqCst), 1);

        let (fake, _, actor, file) = actor_with_short_tokens().await;
        actor.delegate("one").await.unwrap();
        std::fs::write(&file, "rotated-id-token").unwrap();
        actor.delegate("two").await.unwrap();
        let bearers: Vec<Option<String>> = fake
            .calls("exchange")
            .into_iter()
            .map(|(bearer, _)| bearer)
            .collect();
        assert_eq!(
            bearers,
            [
                Some("gateway-id-token".into()),
                Some("rotated-id-token".into())
            ],
            "a token about to expire is replaced, from the file as it is now"
        );
    }

    async fn actor_with_short_tokens() -> (Arc<Fake>, ControlPlane, Arc<Actor>, PathBuf) {
        actor("short", now() + RENEW_BEFORE.as_secs() / 2).await
    }

    #[tokio::test]
    async fn a_rejected_gateway_token_is_replaced_once() {
        let (fake, _, actor, _) = actor("rejected", now() + 3600).await;
        fake.refusals.lock().unwrap().push(StatusCode::UNAUTHORIZED);
        assert!(actor.delegate("ana").await.is_ok());
        assert_eq!(fake.exchanges.load(Ordering::SeqCst), 2);

        fake.refusals
            .lock()
            .unwrap()
            .extend([StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED]);
        assert!(matches!(
            actor.delegate("ana").await,
            Err(Refused::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn a_refused_delegation_is_the_gateways_problem_and_names_no_user() {
        let (fake, _, actor, _) = actor("forbidden", now() + 3600).await;
        fake.refusals.lock().unwrap().push(StatusCode::FORBIDDEN);
        let Err(Refused::Unavailable(err)) = actor.delegate("ana-scope-token").await else {
            panic!("a 403 from /token/delegate is unavailable");
        };
        assert_eq!(err.to_string(), "the gateway cannot act for users");
    }

    fn user_exchanges(fake: &Fake) -> Vec<Value> {
        fake.calls("exchange")
            .into_iter()
            .map(|(_, body)| body)
            .filter(|body| body.get("audience").is_none())
            .collect()
    }

    #[tokio::test]
    async fn a_shared_gateway_exchanges_with_its_own_token_as_the_actor() {
        let (fake, control_plane, actor, _) = actor("actor-token", now() + 3600).await;
        let grant = control_plane
            .exchange("ana-id-token", &lobby(), Some(&actor))
            .await
            .unwrap();
        assert_eq!(grant.principal, "ana");

        let exchanges = fake.calls("exchange");
        let (gateway_bearer, gateway_body) = &exchanges[0];
        assert_eq!(gateway_bearer.as_deref(), Some("gateway-id-token"));
        assert_eq!(gateway_body["audience"], "felix-controlplane");
        let (bearer, body) = &exchanges[1];
        assert_eq!(bearer.as_deref(), Some("ana-id-token"));
        assert_eq!(
            claims(body["actor_token"].as_str().unwrap()).unwrap().sub,
            "gateway"
        );
        assert_eq!(body["actor_token_type"], JWT_TOKEN_TYPE);
    }

    #[tokio::test]
    async fn a_per_session_exchange_names_no_actor() {
        let (fake, control_plane, _, _) = actor("no-actor", now() + 3600).await;
        control_plane
            .exchange("ana-id-token", &lobby(), None)
            .await
            .unwrap();
        let exchanges = fake.calls("exchange");
        assert_eq!(exchanges.len(), 1, "no gateway token is fetched");
        let (bearer, body) = &exchanges[0];
        assert_eq!(bearer.as_deref(), Some("ana-id-token"));
        assert!(body.get("actor_token").is_none(), "{body}");
        assert!(body.get("actor_token_type").is_none(), "{body}");
    }

    #[tokio::test]
    async fn a_refused_actor_token_is_replaced_once_then_is_the_gateways_problem() {
        let (fake, control_plane, actor, _) = actor("actor-refused", now() + 3600).await;
        fake.user_refusals
            .lock()
            .unwrap()
            .push((StatusCode::FORBIDDEN, "actor_refused"));
        assert!(
            control_plane
                .exchange("ana-id-token", &lobby(), Some(&actor))
                .await
                .is_ok()
        );
        let actors: Vec<Value> = user_exchanges(&fake)
            .into_iter()
            .map(|body| body["actor_token"].clone())
            .collect();
        assert_eq!(actors.len(), 2);
        assert_ne!(actors[0], actors[1], "the retry uses a fresh gateway token");

        fake.user_refusals.lock().unwrap().extend([
            (StatusCode::FORBIDDEN, "actor_refused"),
            (StatusCode::FORBIDDEN, "actor_refused"),
        ]);
        let Err(Refused::Unavailable(err)) = control_plane
            .exchange("ana-id-token", &lobby(), Some(&actor))
            .await
        else {
            panic!("a twice-refused actor token is unavailable");
        };
        assert_eq!(err.to_string(), "the gateway cannot act for users");
        assert_eq!(user_exchanges(&fake).len(), 4);
    }

    #[tokio::test]
    async fn a_user_refused_by_rbac_is_forbidden_without_a_retry() {
        let (fake, control_plane, actor, _) = actor("user-refused", now() + 3600).await;
        fake.user_refusals
            .lock()
            .unwrap()
            .push((StatusCode::FORBIDDEN, "forbidden"));
        assert!(matches!(
            control_plane
                .exchange("ana-id-token", &lobby(), Some(&actor))
                .await,
            Err(Refused::Forbidden)
        ));
        assert_eq!(user_exchanges(&fake).len(), 1);
    }

    #[tokio::test]
    async fn a_control_plane_not_yet_upgraded_is_unavailable() {
        let (fake, control_plane, actor, _) = actor("conflict", now() + 3600).await;
        fake.user_refusals
            .lock()
            .unwrap()
            .push((StatusCode::CONFLICT, "conflict"));
        assert!(matches!(
            control_plane
                .exchange("ana-id-token", &lobby(), Some(&actor))
                .await,
            Err(Refused::Unavailable(_))
        ));
    }
}
