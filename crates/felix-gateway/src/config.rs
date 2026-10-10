//! What the gateway reads from its environment.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

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
            shared: SharedConfig::from_vars(|name| {
                std::env::var(name).ok().filter(|value| !value.is_empty())
            })?,
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
