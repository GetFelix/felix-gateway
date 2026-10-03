//! The gateway's one Felix connection, shared by every browser session.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_client::{
    CacheWatchFilter, ClientConfig, ClusterCacheWatch, ClusterClient, ClusterSubscription,
    StartPosition,
};
use felix_wire::AckMode;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

use crate::config::Config;
use crate::protocol::{CounterName, StartAt, StreamName};

/// The cache holding member entries, keyed `<room>:<session>`.
const MEMBERS: &str = "canvas.presence";

/// A Felix connection scoped to one room's streams.
pub(crate) struct Felix {
    client: Arc<ClusterClient>,
    tenant: String,
    namespace: String,
    room: String,
    ops: String,
    presence: String,
    member_ttl: Duration,
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
            member_ttl: config.member_ttl,
        })
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn room(&self) -> &str {
        &self.room
    }

    pub(crate) fn member_ttl(&self) -> Duration {
        self.member_ttl
    }

    /// The prefix of every member key in this room. Keys are checked to hold
    /// no `:`, so one room's prefix never matches another room's keys.
    pub(crate) fn member_prefix(&self) -> String {
        format!("{}:", self.room)
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
        // `Latest` rather than no position: only then does the broker report
        // the tail it registered at, which a joining browser needs to know
        // what lies between its snapshot and the live events.
        let start = match from {
            StartAt::Offset(offset) => StartPosition::Offset(offset),
            StartAt::Live(_) => StartPosition::Latest,
        };
        self.client
            .subscribe_from(
                &self.tenant,
                &self.namespace,
                self.stream(stream),
                Some(start),
            )
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

    /// The room's snapshot, `canvas.snap/<room>`, as the snapshotter wrote it.
    pub(crate) async fn snapshot(&self) -> Result<Option<Vec<u8>>> {
        let value = self
            .client
            .client()
            .await
            .cache_get(&self.tenant, &self.namespace, "canvas.snap", &self.room)
            .await?;
        Ok(value.map(|bytes| bytes.to_vec()))
    }

    pub(crate) async fn set_member(&self, key: &str, payload: Vec<u8>) -> Result<()> {
        let key = format!("{}{key}", self.member_prefix());
        let ttl = u64::try_from(self.member_ttl.as_millis()).unwrap_or(u64::MAX);
        self.client
            .client()
            .await
            .cache_put(
                &self.tenant,
                &self.namespace,
                MEMBERS,
                &key,
                payload.into(),
                Some(ttl),
            )
            .await
    }

    pub(crate) async fn remove_member(&self, key: &str) -> Result<()> {
        let key = format!("{}{key}", self.member_prefix());
        self.client
            .client()
            .await
            .cache_delete(&self.tenant, &self.namespace, MEMBERS, &key)
            .await
            .map(drop)
    }

    /// Every member entry in the room, then each change to one.
    pub(crate) async fn watch_members(&self) -> Result<ClusterCacheWatch> {
        self.client
            .watch_cache_retained(
                &self.tenant,
                &self.namespace,
                MEMBERS,
                CacheWatchFilter::Prefix(self.member_prefix()),
            )
            .await
    }
}
