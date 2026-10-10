//! An edge gateway: browsers on one side, Felix on the other.
//!
//! It owns sockets and Felix connections and nothing else. Payloads are
//! relayed as opaque bytes, so a scope's truth stays in the broker's log. The
//! scope file names the scope's field and the Felix resources each scope owns.
//!
//! - `config`: [`Config`], read from the environment.
//! - `protocol`: the JSON messages exchanged with a browser.
//! - `transport`: the browser connection seam and its WebSocket implementation.
//! - `relay`: one browser session, relayed to Felix.
//! - `throttle`: a pretend slow link, for the slow-client demonstration.
//! - `access`: each session's sign-in exchanged for a token narrowed to its scope.
//! - `scope`: the scope file, and the Felix resources one scope owns.
//! - `felix`: the brokers, and one session's connection to them, its own or
//!   shared.
//! - `metrics`: latency of the browser leg and the Felix leg, apart.

mod access;
mod config;
mod felix;
mod metrics;
pub mod protocol;
mod relay;
mod scope;
mod throttle;
pub mod transport;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::extract::{State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use felix_client::{ClusterClient, TokenProvider};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tower_http::services::ServeDir;

pub use access::Refused;
pub use config::{Config, SharedConfig, resolve_brokers};
pub use metrics::{Metrics, Snapshot, Summary};
pub use scope::ScopeConfig;

use access::{Actor, ControlPlane};
use felix::{Brokers, Pool};
use scope::{BAD_NAME, CacheAction, Scope, valid_name};
use transport::websocket::WebSocketConnection;

/// A gateway ready to serve browsers. Each session reaches Felix with its own
/// user's credentials, on a connection of its own or, when
/// [`Config::shared`] is set, as its user on a connection the gateway shares.
#[derive(Clone)]
pub struct Gateway {
    brokers: Arc<Brokers>,
    control_plane: ControlPlane,
    shared: Option<Shared>,
    metrics: Arc<Metrics>,
    oidc: Arc<Value>,
    web_dir: Option<PathBuf>,
    scope: Arc<ScopeConfig>,
}

impl Gateway {
    /// A gateway for the brokers, control plane, identity provider and scope
    /// in `config`.
    ///
    /// # Errors
    /// When the CA file, or the client certificate or key of shared
    /// connections, cannot be read.
    pub fn new(config: &Config) -> Result<Self> {
        let brokers = Arc::new(Brokers::new(config)?);
        let control_plane =
            ControlPlane::new(&config.control_plane, &config.tenant, &config.namespace);
        let shared = match &config.shared {
            Some(shared) => {
                let actor = Arc::new(Actor::new(
                    control_plane.clone(),
                    shared.credential_file.clone(),
                ));
                Some(Shared {
                    pool: Arc::new(Pool::new(Arc::clone(&brokers), shared, Arc::clone(&actor))?),
                    actor,
                })
            }
            None => None,
        };
        Ok(Self {
            brokers,
            control_plane,
            shared,
            metrics: Arc::new(Metrics::new(
                config.scope.scope.streams.iter().map(|s| s.alias.as_str()),
            )),
            oidc: Arc::new(json!({
                "issuer": config.oidc_issuer,
                "client_id": config.oidc_client_id,
                "scopes": config.oidc_scopes,
            })),
            web_dir: config.web_dir.clone(),
            scope: Arc::clone(&config.scope),
        })
    }

    /// The HTTP routes: `/ws` for browsers, `/oidc` for how a browser signs
    /// in, `/members/leave` for a closing tab's goodbye, and `/metrics` for
    /// latency. Every other path is a file of the web bundle, when there is one.
    pub fn router(&self) -> Router {
        let router = Router::new()
            .route("/ws", get(websocket))
            .route("/oidc", get(oidc))
            .route("/members/leave", post(leave))
            .route("/metrics", get(metrics))
            .with_state(self.clone());
        match &self.web_dir {
            Some(dir) => router.fallback_service(ServeDir::new(dir)),
            None => router,
        }
    }

    /// Latency recorded since the gateway started.
    pub fn metrics(&self) -> Snapshot {
        self.metrics.snapshot()
    }

    /// The Felix token a session joining `scope` with `id_token` would get.
    ///
    /// # Errors
    /// As for a join: the sign-in is refused, or the scope's value is not valid.
    pub async fn scope_token(&self, id_token: &str, scope: &str) -> Result<String, Refused> {
        let Some(scope) = Scope::parse(&self.scope, scope) else {
            return Err(Refused::Forbidden);
        };
        let grant = self.control_plane.exchange(id_token, &scope).await?;
        Ok(grant.felix_token)
    }

    /// A Felix connection authenticated by `tokens`, made the way a session's is.
    ///
    /// # Errors
    /// When no broker accepts the connection.
    pub async fn connect_felix(&self, tokens: Arc<dyn TokenProvider>) -> Result<ClusterClient> {
        self.brokers.connect(tokens).await
    }

    /// A client acting with `tokens` on one of the gateway's shared
    /// connections, the way a session's identity is attached. The tokens are
    /// presented as they are, without delegation.
    ///
    /// # Errors
    /// When shared connections are not configured, or the broker refuses the
    /// tokens on the gateway's connection.
    pub async fn attach_shared(&self, tokens: Arc<dyn TokenProvider>) -> Result<SharedIdentity> {
        let shared = self
            .shared
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("shared connections are not configured"))?;
        Ok(SharedIdentity(
            felix::attach_identity(&shared.pool, tokens).await?,
        ))
    }
}

/// One identity on a shared connection, from [`Gateway::attach_shared`].
/// Dropping it closes its streams and leaves the connection.
pub struct SharedIdentity(Arc<felix::Identity>);

impl SharedIdentity {
    /// The client that acts as this identity.
    pub fn client(&self) -> &felix_client::Client {
        &self.0.client
    }
}

/// What shared connections need: the pool, and the gateway's own credential
/// to delegate users' tokens with.
#[derive(Clone)]
struct Shared {
    pool: Arc<Pool>,
    actor: Arc<Actor>,
}

async fn websocket(State(gateway): State<Gateway>, upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| relay::run(WebSocketConnection::new(socket), gateway))
}

#[derive(Deserialize)]
struct Leave {
    token: String,
    cache: String,
    key: String,
    /// The scope's value is under the scope file's field.
    #[serde(flatten)]
    fields: Map<String, Value>,
}

/// Delete the cache entry named by the body, `{<field>, "token", "cache",
/// "key"}`. A closing tab sends this with `navigator.sendBeacon`, which
/// outlives the page; a WebSocket message sent while the page unloads may
/// never leave it. The beacon carries the sign-in and is exchanged like a
/// join, so it can only touch a scope its sender may open.
async fn leave(State(gateway): State<Gateway>, body: String) -> impl IntoResponse {
    // sendBeacon cannot set a JSON content type, so the body is parsed here.
    let Ok(leave) = serde_json::from_str::<Leave>(&body) else {
        return (
            StatusCode::BAD_REQUEST,
            format!(
                "expected {}, token, cache and key",
                gateway.scope.scope.field
            ),
        );
    };
    let scope = leave
        .fields
        .get(&gateway.scope.scope.field)
        .and_then(Value::as_str)
        .and_then(|value| Scope::parse(&gateway.scope, value));
    let (Some(scope), true) = (scope, valid_name(&leave.key)) else {
        return (StatusCode::BAD_REQUEST, BAD_NAME.to_string());
    };
    let cache = match scope.cache(&leave.cache, CacheAction::Write) {
        Ok(cache) => cache.name,
        Err(refusal) => return (StatusCode::BAD_REQUEST, refusal),
    };
    let felix = match relay::open(scope, &leave.token, &gateway).await {
        Ok(felix) => felix,
        Err(refusal) => {
            let body = serde_json::to_string(&refusal).expect("server messages serialize");
            return (StatusCode::FORBIDDEN, body);
        }
    };
    match felix.cache_delete(&cache, &leave.key).await {
        Ok(()) => (StatusCode::NO_CONTENT, String::new()),
        Err(err) => (StatusCode::BAD_GATEWAY, format!("{err:#}")),
    }
}

async fn oidc(State(gateway): State<Gateway>) -> Json<Value> {
    Json((*gateway.oidc).clone())
}

async fn metrics(State(gateway): State<Gateway>) -> Json<Snapshot> {
    Json(gateway.metrics())
}
