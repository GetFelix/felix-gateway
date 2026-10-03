//! Per-session Felix credentials. A browser's sign-in is exchanged at the
//! Felix control plane for a token narrowed to one scope, so the broker, not
//! the gateway, is what keeps a session out of every other scope.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use felix_client::{RefreshingToken, TokenProvider};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;

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
    pub(crate) async fn exchange(&self, id_token: &str, scope: &Scope) -> Result<Grant, Refused> {
        let permissions = scope.permissions(&self.tenant, &self.namespace);
        let (actions, objects) = split(&permissions);
        let response = self
            .http
            .post(format!(
                "{}/v1/tenants/{}/token/exchange",
                self.url, self.tenant
            ))
            .bearer_auth(id_token)
            .json(&json!({ "requested": actions, "resources": objects }))
            .send()
            .await
            .map_err(|err| Refused::Unavailable(err.into()))?;
        match response.status() {
            StatusCode::UNAUTHORIZED => return Err(Refused::SignedOut),
            StatusCode::FORBIDDEN => return Err(Refused::Forbidden),
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

/// The distinct actions and objects in `permissions`, which is how the
/// exchange takes a narrowing.
fn split(permissions: &[Permission]) -> (BTreeSet<&str>, BTreeSet<&str>) {
    permissions
        .iter()
        .filter_map(|permission| permission.grant.split_once(':'))
        .unzip()
}

#[derive(Deserialize)]
struct Claims {
    #[serde(default)]
    sub: String,
    perms: Vec<String>,
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
}
