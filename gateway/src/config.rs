//! What the gateway reads from its environment.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};

/// Gateway settings. Every field has an environment variable; the defaults
/// match the development stack in `dev/`.
#[derive(Debug, Clone)]
pub struct Config {
    /// `CANVAS_LISTEN`: where browsers connect. Default `127.0.0.1:8787`.
    pub listen: SocketAddr,
    /// `CANVAS_FELIX_BROKERS`: comma-separated broker addresses. Default `127.0.0.1:5000`.
    pub brokers: Vec<SocketAddr>,
    /// `CANVAS_FELIX_SERVER_NAME`: the name the broker's certificate is checked
    /// against. Default `localhost`, which is what a development broker's
    /// self-signed certificate names.
    pub server_name: String,
    /// `CANVAS_FELIX_CA_FILE`: PEM certificates to trust for the broker. Unset
    /// means the platform trust store.
    pub ca_file: Option<PathBuf>,
    /// `CANVAS_FELIX_TOKEN`: the Felix token the gateway authenticates with. Required.
    pub token: String,
    /// `CANVAS_TENANT`. Default `canvas`.
    pub tenant: String,
    /// `CANVAS_NAMESPACE`. Default `default`.
    pub namespace: String,
    /// `CANVAS_ROOM`: the one room this gateway serves. Default `lobby`.
    pub room: String,
    /// `CANVAS_MEMBER_TTL_SECONDS`: how long a member entry outlives its last
    /// refresh. Default 30.
    pub member_ttl: Duration,
}

impl Config {
    /// Read the settings from the environment.
    ///
    /// # Errors
    /// When `CANVAS_FELIX_TOKEN` is missing or an address or number does not parse.
    pub fn from_env() -> Result<Self> {
        let var = |name: &str, default: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| default.to_string())
        };
        let listen = var("CANVAS_LISTEN", "127.0.0.1:8787")
            .parse()
            .context("parse CANVAS_LISTEN")?;
        let brokers = var("CANVAS_FELIX_BROKERS", "127.0.0.1:5000")
            .split(',')
            .map(|addr| addr.trim().parse())
            .collect::<Result<Vec<_>, _>>()
            .context("parse CANVAS_FELIX_BROKERS")?;
        let token = var("CANVAS_FELIX_TOKEN", "");
        if token.is_empty() {
            bail!("CANVAS_FELIX_TOKEN must be set to a Felix client token");
        }
        Ok(Self {
            listen,
            brokers,
            server_name: var("CANVAS_FELIX_SERVER_NAME", "localhost"),
            ca_file: std::env::var_os("CANVAS_FELIX_CA_FILE")
                .filter(|path| !path.is_empty())
                .map(PathBuf::from),
            token,
            tenant: var("CANVAS_TENANT", "canvas"),
            namespace: var("CANVAS_NAMESPACE", "default"),
            room: var("CANVAS_ROOM", "lobby"),
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
