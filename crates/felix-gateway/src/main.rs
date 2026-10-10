use std::net::SocketAddr;

use anyhow::Result;
use axum::serve::{Listener, ListenerExt};
use felix_gateway::{Config, Gateway};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = Config::from_env()?;
    let gateway = Gateway::new(&config)?;
    // Small frames both ways: without TCP_NODELAY, Nagle and delayed ACKs add
    // up to 40 ms to a cursor or an ack.
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await?
        .tap_io(|tcp| {
            let _ = tcp.set_nodelay(true);
        });
    tracing::info!(
        listen = %listener.local_addr()?,
        tenant = %config.tenant,
        "gateway ready"
    );
    // The client's address feeds the session cap per address.
    let app = gateway
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    axum::serve(listener, app).await?;
    Ok(())
}
