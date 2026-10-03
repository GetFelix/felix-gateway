use anyhow::Result;
use axum::serve::{Listener, ListenerExt};
use felix_canvas_gateway::{Config, Gateway};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = Config::from_env()?;
    let gateway = Gateway::connect(&config).await?;
    // Small frames both ways: without TCP_NODELAY, Nagle and delayed ACKs add
    // up to 40 ms to a cursor or an ack.
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await?
        .tap_io(|tcp| {
            let _ = tcp.set_nodelay(true);
        });
    tracing::info!(
        listen = %listener.local_addr()?,
        room = %config.room,
        "gateway ready"
    );
    axum::serve(listener, gateway.router()).await?;
    Ok(())
}
