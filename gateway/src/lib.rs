//! The Felix Canvas edge gateway: browsers on one side, Felix on the other.
//!
//! It owns sockets and Felix connections and nothing else. Payloads are
//! relayed as opaque bytes, so a room's truth stays in the broker's log.
//!
//! - `config`: [`Config`], read from the environment.
//! - `protocol`: the JSON messages exchanged with a browser.
//! - `transport`: the browser connection seam and its WebSocket implementation.
//! - `relay`: one browser session, relayed to Felix.
//! - `access`: each session's sign-in exchanged for a token narrowed to its room.
//! - `room`: room names and the Felix streams and caches each room owns.
//! - `felix`: the brokers, and one session's connection to them.
//! - `metrics`: latency of the browser leg and the Felix leg, apart.

mod access;
mod config;
mod felix;
mod metrics;
pub mod protocol;
mod relay;
mod room;
pub mod transport;

use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::extract::{State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use felix_client::{ClusterClient, TokenProvider};
use serde::Deserialize;
use serde_json::{Value, json};

pub use access::Refused;
pub use config::Config;
pub use metrics::{Metrics, Snapshot, Summary};

use access::ControlPlane;
use felix::Brokers;
use room::{BAD_NAME, Room, valid_name};
use transport::websocket::WebSocketConnection;

/// A gateway ready to serve browsers. It opens a Felix connection per
/// session, with that session's own credentials.
#[derive(Clone)]
pub struct Gateway {
    brokers: Arc<Brokers>,
    control_plane: ControlPlane,
    metrics: Arc<Metrics>,
    oidc: Arc<Value>,
}

impl Gateway {
    /// A gateway for the brokers, control plane and identity provider in `config`.
    ///
    /// # Errors
    /// When the CA file cannot be read.
    pub fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            brokers: Arc::new(Brokers::new(config)?),
            control_plane: ControlPlane::new(
                &config.control_plane,
                &config.tenant,
                &config.namespace,
            ),
            metrics: Arc::default(),
            oidc: Arc::new(json!({
                "issuer": config.oidc_issuer,
                "client_id": config.oidc_client_id,
            })),
        })
    }

    /// The HTTP routes: `/ws` for browsers, `/oidc` for how a browser signs
    /// in, `/members/leave` for a closing tab's goodbye, and `/metrics` for
    /// latency.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/ws", get(websocket))
            .route("/oidc", get(oidc))
            .route("/members/leave", post(leave))
            .route("/metrics", get(metrics))
            .with_state(self.clone())
    }

    /// Latency recorded since the gateway started.
    pub fn metrics(&self) -> Snapshot {
        self.metrics.snapshot()
    }

    /// The Felix token a session joining `room` with `id_token` would get.
    ///
    /// # Errors
    /// As for a join: the sign-in is refused, or the room name is not valid.
    pub async fn room_token(&self, id_token: &str, room: &str) -> Result<String, Refused> {
        let Some(room) = Room::parse(room) else {
            return Err(Refused::Forbidden);
        };
        let grant = self.control_plane.exchange(id_token, &room).await?;
        Ok(grant.felix_token)
    }

    /// A Felix connection authenticated by `tokens`, made the way a session's is.
    ///
    /// # Errors
    /// When no broker accepts the connection.
    pub async fn connect_felix(&self, tokens: Arc<dyn TokenProvider>) -> Result<ClusterClient> {
        self.brokers.connect(tokens).await
    }
}

async fn websocket(State(gateway): State<Gateway>, upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| {
        relay::run(
            WebSocketConnection::new(socket),
            gateway.brokers,
            gateway.control_plane,
            gateway.metrics,
        )
    })
}

#[derive(Deserialize)]
struct Leave {
    room: String,
    token: String,
    key: String,
}

/// Delete the member entry named by the body, `{"room", "token", "key"}`. A
/// closing tab sends this with `navigator.sendBeacon`, which outlives the
/// page; a WebSocket message sent while the page unloads may never leave it.
/// The beacon carries the sign-in and is exchanged like a join, so it can
/// only touch a room its sender may open.
async fn leave(State(gateway): State<Gateway>, body: String) -> impl IntoResponse {
    // sendBeacon cannot set a JSON content type, so the body is parsed here.
    let Ok(Leave { room, token, key }) = serde_json::from_str(&body) else {
        return (
            StatusCode::BAD_REQUEST,
            "expected room, token and key".into(),
        );
    };
    let (Some(room), true) = (Room::parse(&room), valid_name(&key)) else {
        return (StatusCode::BAD_REQUEST, BAD_NAME.to_string());
    };
    let felix = match relay::open(room, &token, &gateway.brokers, &gateway.control_plane).await {
        Ok(felix) => felix,
        Err(refusal) => {
            let body = serde_json::to_string(&refusal).expect("server messages serialize");
            return (StatusCode::FORBIDDEN, body);
        }
    };
    match felix.remove_member(&key).await {
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
