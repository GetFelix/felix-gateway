//! Felix connections: one per browser session, each with that session's own
//! room token.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use felix_client::{
    CacheWatchFilter, ClientConfig, ClusterCacheWatch, ClusterClient, ClusterSubscription,
    StartPosition, TokenProvider,
};
use felix_wire::AckMode;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

use crate::config::Config;
use crate::protocol::{CounterName, StartAt, StreamName};
use crate::room::{Room, SNAPSHOT_KEY};

/// Where the brokers are and how to trust them, read once at startup.
pub(crate) struct Brokers {
    addrs: Vec<String>,
    server_name: String,
    roots: Option<Arc<RootCertStore>>,
    tenant: String,
    namespace: String,
    member_ttl: Duration,
    /// Which broker the next connection tries first.
    next: AtomicUsize,
}

impl Brokers {
    pub(crate) fn new(config: &Config) -> Result<Self> {
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
        Ok(Self {
            addrs: config.brokers.clone(),
            server_name: config.server_name.clone(),
            roots,
            tenant: config.tenant.clone(),
            namespace: config.namespace.clone(),
            member_ttl: config.member_ttl,
            next: AtomicUsize::new(0),
        })
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn member_ttl(&self) -> Duration {
        self.member_ttl
    }

    /// Connect with tokens from `tokens`, which decide what this connection
    /// may touch.
    pub(crate) async fn connect(&self, tokens: Arc<dyn TokenProvider>) -> Result<ClusterClient> {
        let quic = felix_client::quic_client_config(self.roots.clone(), true)?;
        let mut config = ClientConfig::optimized_defaults(quic);
        config.auth_tenant_id = Some(self.tenant.clone());
        config.token_provider = Some(tokens);
        // Every session has its own client (felix#969), and the defaults open
        // 20 connections each against a broker limit of 512 per address. One
        // of each kind is plenty for one person's edits.
        config.publish_conn_pool = 1;
        config.event_conn_pool = 1;
        config.cache_conn_pool = 1;
        let mut seeds = crate::resolve_brokers(&self.addrs).await?;
        // Felix tries the addresses in order and waits out a handshake
        // timeout on each that is down, so starting every connection at the
        // same one would make every session pay for that broker's loss.
        let first = self.next.fetch_add(1, Ordering::Relaxed) % seeds.len();
        seeds.rotate_left(first);
        ClusterClient::connect(&seeds, &self.server_name, config)
            .await
            .context("connect to Felix")
    }
}

/// One session's Felix connection, scoped to its room's streams and caches.
pub(crate) struct Felix {
    client: Arc<ClusterClient>,
    tenant: String,
    namespace: String,
    room: Room,
    ops: String,
    presence: String,
    members: String,
    member_ttl: Duration,
}

impl Felix {
    pub(crate) fn new(client: ClusterClient, brokers: &Brokers, room: Room) -> Self {
        Self {
            client: Arc::new(client),
            tenant: brokers.tenant.clone(),
            namespace: brokers.namespace.clone(),
            ops: room.ops(),
            presence: room.presence(),
            members: room.members(),
            member_ttl: brokers.member_ttl,
            room,
        }
    }

    pub(crate) fn room(&self) -> &str {
        self.room.name()
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

    /// Add to one of the room's counters. Like a publish it is sent once;
    /// Felix counts a retried add twice.
    pub(crate) async fn counter_add(
        &self,
        counter: CounterName,
        key: &str,
        delta: i64,
    ) -> Result<i64> {
        let cache = match counter {
            CounterName::Seq => self.room.seq(),
        };
        self.client
            .client()
            .await
            .counter_add(&self.tenant, &self.namespace, &cache, key, delta)
            .await
    }

    /// The room's snapshot, as the snapshotter wrote it.
    pub(crate) async fn snapshot(&self) -> Result<Option<Vec<u8>>> {
        let value = self
            .client
            .client()
            .await
            .cache_get(
                &self.tenant,
                &self.namespace,
                &self.room.snapshots(),
                SNAPSHOT_KEY,
            )
            .await?;
        Ok(value.map(|bytes| bytes.to_vec()))
    }

    pub(crate) async fn set_member(&self, key: &str, payload: Vec<u8>) -> Result<()> {
        let ttl = u64::try_from(self.member_ttl.as_millis()).unwrap_or(u64::MAX);
        self.client
            .client()
            .await
            .cache_put(
                &self.tenant,
                &self.namespace,
                &self.members,
                key,
                payload.into(),
                Some(ttl),
            )
            .await
    }

    pub(crate) async fn remove_member(&self, key: &str) -> Result<()> {
        self.client
            .client()
            .await
            .cache_delete(&self.tenant, &self.namespace, &self.members, key)
            .await
            .map(drop)
    }

    /// Every member entry in the room, then each change to one. The member
    /// cache has one shard, so an empty prefix covers the whole room.
    pub(crate) async fn watch_members(&self) -> Result<ClusterCacheWatch> {
        self.client
            .watch_cache_retained(
                &self.tenant,
                &self.namespace,
                &self.members,
                CacheWatchFilter::Prefix(String::new()),
            )
            .await
    }
}
