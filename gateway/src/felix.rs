//! The gateway's one Felix connection, shared by every browser session.

use std::sync::Arc;

use anyhow::{Context, Result};
use felix_client::{ClientConfig, ClusterClient, ClusterSubscription, StartPosition};
use felix_wire::AckMode;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

use crate::config::Config;
use crate::protocol::{CounterName, StartAt, StreamName};

/// A Felix connection scoped to one room's streams.
pub(crate) struct Felix {
    client: Arc<ClusterClient>,
    tenant: String,
    namespace: String,
    room: String,
    ops: String,
    presence: String,
}

impl Felix {
    pub(crate) async fn connect(config: &Config) -> Result<Self> {
        let roots = match &config.ca_file {
            Some(path) => {
                let mut roots = RootCertStore::empty();
                for cert in CertificateDer::pem_file_iter(path)
                    .with_context(|| format!("read {}", path.display()))?
                {
                    roots.add(cert.context("parse broker CA certificate")?)?;
                }
                Some(Arc::new(roots))
            }
            None => None,
        };
        let quinn = felix_client::quic_client_config(roots, true)?;
        let mut client_config = ClientConfig::optimized_defaults(quinn);
        client_config.auth_tenant_id = Some(config.tenant.clone());
        client_config.auth_token = Some(config.token.clone());
        let client = ClusterClient::connect(&config.brokers, &config.server_name, client_config)
            .await
            .context("connect to Felix")?;
        Ok(Self {
            client: Arc::new(client),
            tenant: config.tenant.clone(),
            namespace: config.namespace.clone(),
            room: config.room.clone(),
            ops: format!("canvas.ops.{}", config.room),
            presence: format!("canvas.presence.{}", config.room),
        })
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn room(&self) -> &str {
        &self.room
    }

    fn stream(&self, stream: StreamName) -> &str {
        match stream {
            StreamName::Ops => &self.ops,
            StreamName::Presence => &self.presence,
        }
    }

    /// Publish once, never resent by the gateway: the browser owns retries,
    /// because only it holds the `(sid, seq)` that makes a retry deduplicable.
    pub(crate) async fn publish(
        &self,
        stream: StreamName,
        payload: Vec<u8>,
        ack: bool,
    ) -> Result<Option<u64>> {
        let ack = if ack {
            AckMode::PerMessage
        } else {
            AckMode::None
        };
        self.client
            .publish(
                &self.tenant,
                &self.namespace,
                self.stream(stream),
                payload,
                ack,
            )
            .await
    }

    pub(crate) async fn subscribe(
        &self,
        stream: StreamName,
        from: StartAt,
    ) -> Result<ClusterSubscription> {
        let start = match from {
            StartAt::Offset(offset) => Some(StartPosition::Offset(offset)),
            StartAt::Live(_) => None,
        };
        self.client
            .subscribe_from(&self.tenant, &self.namespace, self.stream(stream), start)
            .await
    }

    /// Add to a counter under this room, `<cache>/<room>:<key>`. Like a
    /// publish it is sent once; Felix counts a retried add twice.
    pub(crate) async fn counter_add(
        &self,
        counter: CounterName,
        key: &str,
        delta: i64,
    ) -> Result<i64> {
        let cache = match counter {
            CounterName::Seq => "canvas.seq",
        };
        let key = format!("{}:{key}", self.room);
        self.client
            .client()
            .await
            .counter_add(&self.tenant, &self.namespace, cache, &key, delta)
            .await
    }
}
