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
use axum::response::{IntoResponse, Json};
use axum::routing::get;

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

    /// The HTTP routes: `/ws` for browsers and `/metrics` for latency.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/ws", get(websocket))
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

async fn metrics(State(gateway): State<Gateway>) -> Json<Snapshot> {
    Json(gateway.metrics())
}
