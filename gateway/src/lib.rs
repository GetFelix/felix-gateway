//! The Felix Canvas edge gateway: browsers on one side, Felix on the other.
//!
//! It owns sockets and a Felix connection and nothing else. Payloads are
//! relayed as opaque bytes, so a room's truth stays in the broker's log.
//!
//! - `config`: [`Config`], read from the environment.
//! - `protocol`: the JSON messages exchanged with a browser.
//! - `transport`: the browser connection seam and its WebSocket implementation.
//! - `relay`: one browser session, relayed to Felix.
//! - `felix`: the shared Felix connection and the room's stream names.
//! - `metrics`: latency of the browser leg and the Felix leg, apart.

mod config;
mod felix;
mod metrics;
pub mod protocol;
mod relay;
pub mod transport;

use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::extract::{State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};

pub use config::Config;
pub use metrics::{Metrics, Snapshot, Summary};

use felix::Felix;
use transport::websocket::WebSocketConnection;

/// A gateway connected to Felix and ready to serve browsers.
#[derive(Clone)]
pub struct Gateway {
    felix: Arc<Felix>,
    metrics: Arc<Metrics>,
}

impl Gateway {
    /// Connect to Felix with the token and room in `config`.
    ///
    /// # Errors
    /// When no broker accepts the connection or the CA file cannot be read.
    pub async fn connect(config: &Config) -> Result<Self> {
        Ok(Self {
            felix: Arc::new(Felix::connect(config).await?),
            metrics: Arc::default(),
        })
    }

    /// The HTTP routes: `/ws` for browsers, `/members/leave` for a closing
    /// tab's goodbye, and `/metrics` for latency.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/ws", get(websocket))
            .route("/members/leave", post(leave))
            .route("/metrics", get(metrics))
            .with_state(self.clone())
    }

    /// Latency recorded since the gateway started.
    pub fn metrics(&self) -> Snapshot {
        self.metrics.snapshot()
    }
}

async fn websocket(State(gateway): State<Gateway>, upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| {
        relay::run(
            WebSocketConnection::new(socket),
            gateway.felix,
            gateway.metrics,
        )
    })
}

/// Delete the member entry named by the body. A closing tab sends this with
/// `navigator.sendBeacon`, which outlives the page; a WebSocket message sent
/// while the page unloads may never leave it.
async fn leave(State(gateway): State<Gateway>, key: String) -> impl IntoResponse {
    if !relay::valid_key(&key) {
        return (StatusCode::BAD_REQUEST, relay::BAD_KEY.to_string());
    }
    match gateway.felix.remove_member(&key).await {
        Ok(()) => (StatusCode::NO_CONTENT, String::new()),
        Err(err) => (StatusCode::BAD_GATEWAY, format!("{err:#}")),
    }
}

async fn metrics(State(gateway): State<Gateway>) -> Json<Snapshot> {
    Json(gateway.metrics())
}
