use anyhow::Result;
use felix_canvas_gateway::{Config, Gateway};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = Config::from_env()?;
    let gateway = Gateway::connect(&config).await?;
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(
        listen = %listener.local_addr()?,
        room = %config.room,
        "gateway ready"
    );
    axum::serve(listener, gateway.router()).await?;
    Ok(())
}
