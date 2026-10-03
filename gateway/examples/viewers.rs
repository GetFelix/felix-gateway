//! Viewers for the fanout measurement: `count` subscriptions to a room's op
//! stream over one Felix client, each reading every change the way a browser's
//! session does. Prints `ready` once every subscription is registered, and one
//! line of JSON when standard input closes.
//!
//! ```text
//! cargo run --release -p felix-canvas-gateway --example viewers -- <count> <token file>
//! ```
//!
//! It reads the gateway's `CANVAS_*` variables, plus `CANVAS_ROOM` (default
//! `lobby`). The token needs `stream.subscribe` on the room's op stream.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use felix_canvas_gateway::Config;
use felix_client::{ClientConfig, ClusterClient, StartPosition};
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use serde_json::json;
use tokio::io::AsyncReadExt;

/// What one viewer has seen: changes received, and changes Felix dropped for it.
#[derive(Default)]
struct Seen {
    received: AtomicU64,
    dropped: AtomicU64,
    ended: AtomicU64,
}

#[tokio::main]
async fn main() -> Result<()> {
    const USAGE: &str = "usage: viewers <count> <token file>";
    let mut args = std::env::args().skip(1);
    let count: usize = args.next().context(USAGE)?.parse().context(USAGE)?;
    let token = std::fs::read_to_string(args.next().context(USAGE)?)?;
    let room = std::env::var("CANVAS_ROOM").unwrap_or_else(|_| "lobby".to_string());
    let stream = format!("canvas.ops.{room}");

    let config = Config::from_env()?;
    // Felix's default client, as any subscriber would have, not the gateway's
    // trimmed per-session one.
    let roots = match &config.ca_file {
        Some(path) => {
            let mut roots = RootCertStore::empty();
            for cert in CertificateDer::pem_file_iter(path)? {
                roots.add(cert?)?;
            }
            Some(Arc::new(roots))
        }
        None => None,
    };
    let mut client_config =
        ClientConfig::optimized_defaults(felix_client::quic_client_config(roots, true)?);
    client_config.auth_tenant_id = Some(config.tenant.clone());
    client_config.auth_token = Some(token.trim().to_string());
    if std::env::var_os("VIEWERS_BLOCK").is_some() {
        client_config.client_sub_queue_policy = felix_client::ClientSubQueuePolicy::Block;
    }
    let client = Arc::new(
        ClusterClient::connect(&config.brokers, &config.server_name, client_config)
            .await
            .context("connect to Felix")?,
    );

    let seen: Vec<Arc<Seen>> = (0..count).map(|_| Arc::default()).collect();
    for viewer in &seen {
        let mut subscription = client
            .subscribe_from(
                &config.tenant,
                &config.namespace,
                &stream,
                Some(StartPosition::Latest),
            )
            .await
            .context("subscribe")?;
        let viewer = Arc::clone(viewer);
        tokio::spawn(async move {
            let mut previous: Option<u64> = None;
            loop {
                let event = match subscription.next_event().await {
                    Ok(Some(event)) => event,
                    Ok(None) => break eprintln!("a viewer's subscription ended"),
                    Err(err) => break eprintln!("a viewer's subscription failed: {err:#}"),
                };
                viewer.received.fetch_add(1, Ordering::Relaxed);
                if let (Some(previous), Some(offset)) = (previous, event.offset) {
                    let gap = offset.saturating_sub(previous + 1 + event.skipped_before);
                    viewer.dropped.fetch_add(gap, Ordering::Relaxed);
                }
                previous = event.offset.or(previous);
            }
            viewer.ended.store(1, Ordering::Relaxed);
        });
    }
    let connections: usize = client
        .connections_per_node()
        .await
        .iter()
        .map(|(_, count)| count)
        .sum();
    println!("ready");

    let mut stdin = tokio::io::stdin();
    let mut sink = Vec::new();
    let _ = stdin.read_to_end(&mut sink).await;
    let received: Vec<u64> = seen
        .iter()
        .map(|viewer| viewer.received.load(Ordering::Relaxed))
        .collect();
    let dropped: u64 = seen
        .iter()
        .map(|viewer| viewer.dropped.load(Ordering::Relaxed))
        .sum();
    let ended: u64 = seen
        .iter()
        .map(|viewer| viewer.ended.load(Ordering::Relaxed))
        .sum();
    println!(
        "{}",
        json!({
            "viewers": count,
            "connections": connections,
            "received_min": received.iter().min(),
            "received_max": received.iter().max(),
            "dropped": dropped,
            "ended": ended,
        })
    );
    Ok(())
}
