//! What the gateway reads from its environment.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::scope::ScopeConfig;

/// Gateway settings. Every field has an environment variable. The defaults
/// point at a Felix stack on the local machine, such as the one in `dev/`.
#[derive(Debug, Clone)]
pub struct Config {
    /// `GATEWAY_LISTEN`: where browsers connect. Default `127.0.0.1:8787`.
    pub listen: SocketAddr,
    /// `GATEWAY_FELIX_BROKERS`: comma-separated broker addresses as
    /// `host:port`. Names are resolved at each connection, so a broker that
    /// comes back on a new address is found again. Default `127.0.0.1:5000`.
    pub brokers: Vec<String>,
    /// `GATEWAY_FELIX_SERVER_NAME`: the name the broker's certificate is checked
    /// against. Default `localhost`, which is what a development broker's
    /// self-signed certificate names.
    pub server_name: String,
    /// `GATEWAY_FELIX_CA_FILE`: PEM certificates to trust for the broker. Unset
    /// means the platform trust store.
    pub ca_file: Option<PathBuf>,
    /// `GATEWAY_FELIX_CONTROL_PLANE`: the Felix control plane's base URL, where
    /// each browser's sign-in is exchanged. Default `http://127.0.0.1:8443`.
    pub control_plane: String,
    /// `GATEWAY_TENANT`: the Felix tenant that trusts the identity provider.
    /// Required.
    pub tenant: String,
    /// `GATEWAY_NAMESPACE`. Default `default`.
    pub namespace: String,
    /// `GATEWAY_OIDC_ISSUER`: the identity provider browsers sign in with, as
    /// its OpenID Connect issuer URL. Default `http://127.0.0.1:9400`, the
    /// stand-in in `dev/`.
    pub oidc_issuer: String,
    /// `GATEWAY_OIDC_CLIENT_ID`: the client registered for the browser app at
    /// that provider. Required.
    pub oidc_client_id: String,
    /// `GATEWAY_OIDC_SCOPES`: the scopes a browser asks the provider for.
    /// Default `openid profile`.
    pub oidc_scopes: String,
    /// `GATEWAY_WEB_DIR`: a built web bundle to serve on every path the
    /// gateway does not route itself. Unset serves no page.
    pub web_dir: Option<PathBuf>,
    /// `GATEWAY_SCOPE_FILE`: the scope file, which names the scope and its
    /// streams, caches and counters. Required.
    pub scope: Arc<ScopeConfig>,
    /// Shared connections, set when the gateway has a credential and a client
    /// certificate of its own. `None` gives every session its own connection.
    pub shared: Option<SharedConfig>,
    /// `GATEWAY_PING_INTERVAL_S` and `GATEWAY_PING_TIMEOUT_S`: how often a
    /// browser is pinged, and how long a silent one is kept.
    pub heartbeat: Heartbeat,
}

/// How the gateway notices a browser that has gone without closing, such as
/// one whose network dropped. Every interval it sends a WebSocket ping, which
/// browsers answer without page code; a session that has sent nothing, not
/// even a pong, for `timeout` is closed like any other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Heartbeat {
    /// Time between pings. Zero sends none and turns the timeout off too,
    /// since an idle browser would then have nothing to answer.
    pub interval: Duration,
    /// How long a session may be silent before it is closed. Zero never
    /// closes one for silence.
    pub timeout: Duration,
}

impl Default for Heartbeat {
    /// A ping every 5 seconds, well inside the 60-second idle timeout common
    /// on proxies and load balancers, and closed after 30 silent seconds, six
    /// unanswered pings.
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(30),
        }
    }
}

impl Heartbeat {
    fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let seconds = |name: &str, default: Duration| match var(name) {
            Some(value) => value
                .parse::<u64>()
                .map(Duration::from_secs)
                .with_context(|| format!("{name} must be a whole number of seconds")),
            None => Ok(default),
        };
        let default = Self::default();
        let heartbeat = Self {
            interval: seconds("GATEWAY_PING_INTERVAL_S", default.interval)?,
            timeout: seconds("GATEWAY_PING_TIMEOUT_S", default.timeout)?,
        };
        anyhow::ensure!(
            heartbeat.interval.is_zero()
                || heartbeat.timeout.is_zero()
                || heartbeat.timeout > heartbeat.interval,
            "GATEWAY_PING_TIMEOUT_S must be longer than GATEWAY_PING_INTERVAL_S, or 0"
        );
        Ok(heartbeat)
    }
}

/// What the gateway needs to carry many users over a few Felix connections:
/// its own sign-in, to have users' tokens delegated to it, and a certificate
/// the brokers bind those tokens to.
#[derive(Debug, Clone)]
pub struct SharedConfig {
    /// `GATEWAY_FELIX_CREDENTIAL_FILE`: an ID token for the gateway's own
    /// principal, from an identity provider the tenant trusts. Read again at
    /// each exchange, so a token rotated in place is picked up.
    pub credential_file: PathBuf,
    /// `GATEWAY_FELIX_CLIENT_CERT`: PEM certificate chain, leaf first, issued
    /// to the gateway's principal (`felix:principal:<id>` URI SAN).
    pub client_cert: PathBuf,
    /// `GATEWAY_FELIX_CLIENT_KEY`: the PEM private key of that certificate.
    pub client_key: PathBuf,
    /// `GATEWAY_FELIX_SHARED_CONNECTIONS`: how many connections users are
    /// spread over. Default 4.
    pub connections: usize,
}

const SHARED_VARS: [&str; 3] = [
    "GATEWAY_FELIX_CREDENTIAL_FILE",
    "GATEWAY_FELIX_CLIENT_CERT",
    "GATEWAY_FELIX_CLIENT_KEY",
];

impl SharedConfig {
    /// Shared connections from `var`, which looks a variable up. `None` when
    /// none of the three files is set; an error when only some are, since
    /// falling back to one connection per session would hide the mistake.
    fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Option<Self>> {
        let [credential, cert, key] = SHARED_VARS.map(|name| var(name).map(PathBuf::from));
        let connections = match var("GATEWAY_FELIX_SHARED_CONNECTIONS") {
            Some(value) => value
                .parse::<usize>()
                .ok()
                .filter(|count| *count > 0)
                .context("GATEWAY_FELIX_SHARED_CONNECTIONS must be a whole number above 0")?,
            None => 4,
        };
        match (credential, cert, key) {
            (None, None, None) => Ok(None),
            (Some(credential_file), Some(client_cert), Some(client_key)) => Ok(Some(Self {
                credential_file,
                client_cert,
                client_key,
                connections,
            })),
            _ => {
                let missing: Vec<&str> = SHARED_VARS
                    .into_iter()
                    .filter(|name| var(name).is_none())
                    .collect();
                anyhow::bail!(
                    "shared connections need all of {}: set {} too, or unset the others",
                    SHARED_VARS.join(", "),
                    missing.join(" and ")
                )
            }
        }
    }
}

impl Config {
    /// Read the settings from the environment.
    ///
    /// # Errors
    /// When a required variable is unset, an address does not parse, or the
    /// scope file is missing or invalid.
    pub fn from_env() -> Result<Self> {
        let var = |name: &str, default: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| default.to_string())
        };
        let required = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.is_empty())
                .with_context(|| format!("set {name}"))
        };
        let path = |name: &str| {
            std::env::var_os(name)
                .filter(|path| !path.is_empty())
                .map(PathBuf::from)
        };
        let set = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let listen = var("GATEWAY_LISTEN", "127.0.0.1:8787")
            .parse()
            .context("parse GATEWAY_LISTEN")?;
        let brokers: Vec<String> = var("GATEWAY_FELIX_BROKERS", "127.0.0.1:5000")
            .split(',')
            .map(|addr| addr.trim().to_string())
            .collect();
        if let Some(bad) = brokers.iter().find(|addr| {
            !addr
                .rsplit_once(':')
                .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok())
        }) {
            anyhow::bail!("GATEWAY_FELIX_BROKERS: {bad:?} is not host:port");
        }
        Ok(Self {
            listen,
            brokers,
            server_name: var("GATEWAY_FELIX_SERVER_NAME", "localhost"),
            ca_file: path("GATEWAY_FELIX_CA_FILE"),
            control_plane: var("GATEWAY_FELIX_CONTROL_PLANE", "http://127.0.0.1:8443")
                .trim_end_matches('/')
                .to_string(),
            tenant: required("GATEWAY_TENANT")?,
            namespace: var("GATEWAY_NAMESPACE", "default"),
            oidc_issuer: var("GATEWAY_OIDC_ISSUER", "http://127.0.0.1:9400"),
            oidc_client_id: required("GATEWAY_OIDC_CLIENT_ID")?,
            oidc_scopes: var("GATEWAY_OIDC_SCOPES", "openid profile"),
            web_dir: path("GATEWAY_WEB_DIR"),
            scope: Arc::new(ScopeConfig::load(
                &path("GATEWAY_SCOPE_FILE").context("set GATEWAY_SCOPE_FILE to the scope file")?,
            )?),
            shared: SharedConfig::from_vars(set)?,
            heartbeat: Heartbeat::from_vars(set)?,
        })
    }
}

/// Resolve `GATEWAY_FELIX_BROKERS` entries to addresses. A name that does not
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

#[cfg(test)]
mod tests {
    use super::*;

    fn shared(vars: &[(&str, &str)]) -> Result<Option<SharedConfig>> {
        SharedConfig::from_vars(|name| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        })
    }

    #[test]
    fn without_any_shared_setting_each_session_has_its_own_connection() {
        assert!(shared(&[]).unwrap().is_none());
        assert!(
            shared(&[("GATEWAY_FELIX_SHARED_CONNECTIONS", "2")])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn all_three_files_turn_shared_connections_on() {
        let all = [
            ("GATEWAY_FELIX_CREDENTIAL_FILE", "id.token"),
            ("GATEWAY_FELIX_CLIENT_CERT", "cert.pem"),
            ("GATEWAY_FELIX_CLIENT_KEY", "key.pem"),
        ];
        let config = shared(&all).unwrap().unwrap();
        assert_eq!(config.credential_file, PathBuf::from("id.token"));
        assert_eq!(config.connections, 4);

        let mut two = all.to_vec();
        two.push(("GATEWAY_FELIX_SHARED_CONNECTIONS", "2"));
        assert_eq!(shared(&two).unwrap().unwrap().connections, 2);
        two.pop();
        two.push(("GATEWAY_FELIX_SHARED_CONNECTIONS", "0"));
        assert!(shared(&two).is_err());
    }

    fn heartbeat(vars: &[(&str, &str)]) -> Result<Heartbeat> {
        Heartbeat::from_vars(|name| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        })
    }

    #[test]
    fn the_heartbeat_reads_seconds_and_wants_a_timeout_past_the_interval() {
        assert_eq!(heartbeat(&[]).unwrap(), Heartbeat::default());
        let set = heartbeat(&[
            ("GATEWAY_PING_INTERVAL_S", "20"),
            ("GATEWAY_PING_TIMEOUT_S", "60"),
        ])
        .unwrap();
        assert_eq!(
            (set.interval, set.timeout),
            (Duration::from_secs(20), Duration::from_secs(60))
        );
        assert!(heartbeat(&[("GATEWAY_PING_INTERVAL_S", "0")]).is_ok());
        assert!(heartbeat(&[("GATEWAY_PING_TIMEOUT_S", "0")]).is_ok());
        assert!(heartbeat(&[("GATEWAY_PING_TIMEOUT_S", "5")]).is_err());
        assert!(heartbeat(&[("GATEWAY_PING_INTERVAL_S", "soon")]).is_err());
    }

    #[test]
    fn a_partial_shared_setup_is_refused_naming_what_is_missing() {
        let err = shared(&[("GATEWAY_FELIX_CLIENT_CERT", "cert.pem")])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("GATEWAY_FELIX_CREDENTIAL_FILE and GATEWAY_FELIX_CLIENT_KEY"),
            "{err}"
        );
    }
}
