//! What the gateway reads from its environment.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};

/// Gateway settings. Every field has an environment variable; the defaults
/// match the development stack in `dev/`.
#[derive(Debug, Clone)]
pub struct Config {
    /// `CANVAS_LISTEN`: where browsers connect. Default `127.0.0.1:8787`.
    pub listen: SocketAddr,
    /// `CANVAS_FELIX_BROKERS`: comma-separated broker addresses as
    /// `host:port`. Names are resolved at each connection, so a broker that
    /// comes back on a new address is found again. Default `127.0.0.1:5000`.
    pub brokers: Vec<String>,
    /// `CANVAS_FELIX_SERVER_NAME`: the name the broker's certificate is checked
    /// against. Default `localhost`, which is what a development broker's
    /// self-signed certificate names.
    pub server_name: String,
    /// `CANVAS_FELIX_CA_FILE`: PEM certificates to trust for the broker. Unset
    /// means the platform trust store.
    pub ca_file: Option<PathBuf>,
    /// `CANVAS_FELIX_CONTROL_PLANE`: the Felix control plane's base URL, where
    /// each browser's sign-in is exchanged. Default `http://127.0.0.1:8443`.
    pub control_plane: String,
    /// `CANVAS_TENANT`: the Felix tenant that trusts the identity provider.
    /// Default `canvas`.
    pub tenant: String,
    /// `CANVAS_NAMESPACE`. Default `default`.
    pub namespace: String,
    /// `CANVAS_OIDC_ISSUER`: the identity provider browsers sign in with, as
    /// its OpenID Connect issuer URL. Default `http://127.0.0.1:9400`, the
    /// stand-in in `dev/`.
    pub oidc_issuer: String,
    /// `CANVAS_OIDC_CLIENT_ID`: the client registered for the canvas at that
    /// provider. Default `felix-canvas`.
    pub oidc_client_id: String,
    /// `CANVAS_OIDC_SCOPES`: the scopes a browser asks the provider for.
    /// Default `openid profile`.
    pub oidc_scopes: String,
    /// `CANVAS_WEB_DIR`: a built web bundle to serve on every path the
    /// gateway does not route itself. Unset serves no page.
    pub web_dir: Option<PathBuf>,
    /// `CANVAS_MEMBER_TTL_SECONDS`: how long a member entry outlives its last
    /// refresh. Default 30.
    pub member_ttl: Duration,
}

impl Config {
    /// Read the settings from the environment.
    ///
    /// # Errors
    /// When an address or number does not parse.
    pub fn from_env() -> Result<Self> {
        let var = |name: &str, default: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| default.to_string())
        };
        let path = |name: &str| {
            std::env::var_os(name)
                .filter(|path| !path.is_empty())
                .map(PathBuf::from)
        };
        let listen = var("CANVAS_LISTEN", "127.0.0.1:8787")
            .parse()
            .context("parse CANVAS_LISTEN")?;
        let brokers: Vec<String> = var("CANVAS_FELIX_BROKERS", "127.0.0.1:5000")
            .split(',')
            .map(|addr| addr.trim().to_string())
            .collect();
        if let Some(bad) = brokers.iter().find(|addr| {
            !addr
                .rsplit_once(':')
                .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok())
        }) {
            anyhow::bail!("CANVAS_FELIX_BROKERS: {bad:?} is not host:port");
        }
        Ok(Self {
            listen,
            brokers,
            server_name: var("CANVAS_FELIX_SERVER_NAME", "localhost"),
            ca_file: path("CANVAS_FELIX_CA_FILE"),
            control_plane: var("CANVAS_FELIX_CONTROL_PLANE", "http://127.0.0.1:8443")
                .trim_end_matches('/')
                .to_string(),
            tenant: var("CANVAS_TENANT", "canvas"),
            namespace: var("CANVAS_NAMESPACE", "default"),
            oidc_issuer: var("CANVAS_OIDC_ISSUER", "http://127.0.0.1:9400"),
            oidc_client_id: var("CANVAS_OIDC_CLIENT_ID", "felix-canvas"),
            oidc_scopes: var("CANVAS_OIDC_SCOPES", "openid profile"),
            web_dir: path("CANVAS_WEB_DIR"),
            member_ttl: Duration::from_secs(
                var("CANVAS_MEMBER_TTL_SECONDS", "30")
                    .parse()
                    .ok()
                    .filter(|&seconds| seconds > 0)
                    .context(
                        "CANVAS_MEMBER_TTL_SECONDS must be a whole number of seconds above 0",
                    )?,
            ),
        })
    }
}

/// Resolve `CANVAS_FELIX_BROKERS` entries to addresses. A name that does not
/// resolve is skipped with a warning; it is an error only if none does.
pub async fn resolve_brokers(brokers: &[String]) -> Result<Vec<SocketAddr>> {
    let mut seeds = Vec::new();
    for addr in brokers {
        match tokio::net::lookup_host(addr.as_str()).await {
            Ok(found) => seeds.extend(found),
            Err(err) => tracing::warn!(broker = %addr, "cannot resolve: {err}"),
        }
    }
    anyhow::ensure!(!seeds.is_empty(), "no broker address resolves");
    Ok(seeds)
}
