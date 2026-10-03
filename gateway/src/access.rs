//! Per-session Felix credentials. A browser's sign-in is exchanged at the
//! Felix control plane for a token narrowed to one room, so the broker, not
//! the gateway, is what keeps a session out of every other room.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use felix_client::{RefreshingToken, TokenProvider};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;

use crate::room::Room;

/// Why a sign-in was not exchanged for a room token.
#[derive(Debug)]
pub enum Refused {
    /// No usable sign-in: missing, expired, or from an identity provider the
    /// tenant does not trust. Signing in again may help.
    SignedOut,
    /// Signed in, but not a member of the room, or the room does not exist.
    Forbidden,
    /// The control plane could not be asked or gave an answer that made no sense.
    Unavailable(anyhow::Error),
}

/// A room token and the refresh token that replaces it.
pub(crate) struct Grant {
    pub(crate) felix_token: String,
    refresh_token: String,
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

    /// Exchange `id_token` for a token that reaches `room` and nothing else.
    ///
    /// The exchange only narrows what RBAC grants, so membership is decided
    /// by the room's role in Felix. A sign-in whose token lacks any of the
    /// room's grants is refused here rather than failing piecemeal later.
    pub(crate) async fn exchange(&self, id_token: &str, room: &Room) -> Result<Grant, Refused> {
        let grants = room.grants(&self.tenant, &self.namespace);
        let (actions, objects) = split(&grants);
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
        if !covers(&answer.felix_token, &grants) {
            return Err(Refused::Forbidden);
        }
        Ok(Grant {
            felix_token: answer.felix_token,
            refresh_token: answer.refresh_token,
        })
    }

    /// Tokens for one session, starting from `grant` and refreshed before
    /// each expires. A refresh re-runs RBAC with the same narrowing, so a
    /// member removed from the room loses access at the next one.
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
            .context("refresh the room token")?
            .json()
            .await
            .context("read the refreshed room token")?;
        *refresh_token.lock().expect("refresh token lock") = answer.refresh_token;
        Ok(answer.felix_token)
    }
}

/// The distinct actions and objects in `grants`, which is how the exchange
/// takes a narrowing.
fn split(grants: &[String]) -> (BTreeSet<&str>, BTreeSet<&str>) {
    grants
        .iter()
        .filter_map(|grant| grant.split_once(':'))
        .unzip()
}

/// Whether a Felix token's `perms` claim holds every one of `grants`. The
/// token came straight from the control plane; checking its signature is the
/// broker's job.
fn covers(token: &str, grants: &[String]) -> bool {
    #[derive(Deserialize)]
    struct Claims {
        perms: Vec<String>,
    }
    let claims = token
        .split('.')
        .nth(1)
        .and_then(|payload| URL_SAFE_NO_PAD.decode(payload).ok())
        .and_then(|bytes| serde_json::from_slice::<Claims>(&bytes).ok());
    claims.is_some_and(|claims| grants.iter().all(|grant| claims.perms.contains(grant)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(perms: &[&str]) -> String {
        let claims = URL_SAFE_NO_PAD.encode(json!({ "perms": perms }).to_string());
        format!("e30.{claims}.c2ln")
    }

    #[test]
    fn the_narrowing_names_each_action_and_object_once() {
        let grants = Room::parse("lobby").unwrap().grants("canvas", "default");
        let (actions, objects) = split(&grants);
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
                "cache:canvas/default/canvas.members.lobby",
                "cache:canvas/default/canvas.seq.lobby",
                "cache:canvas/default/canvas.snap.lobby",
                "stream:canvas/default/canvas.ops.lobby",
                "stream:canvas/default/canvas.presence.lobby",
            ]
        );
    }

    #[test]
    fn a_token_missing_any_grant_does_not_cover_the_room() {
        let grants = Room::parse("lobby").unwrap().grants("canvas", "default");
        let all: Vec<&str> = grants.iter().map(String::as_str).collect();
        assert!(covers(&token(&all), &grants));
        assert!(!covers(&token(&all[1..]), &grants));
        assert!(!covers(&token(&[]), &grants));
        assert!(!covers("not a token", &grants));
    }
}
