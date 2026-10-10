//! Felix connections. Each browser session either has a connection of its
//! own with its own scope token, or acts as its user on a connection the
//! gateway shares, with that user's delegated token (`shared`).

mod shared;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use felix_client::{
    CacheWatch, CacheWatchFilter, CacheWatchItem, ClientConfig, ClusterCacheWatch, ClusterClient,
    ClusterSubscription, Event, StartPosition, TokenProvider,
};
use felix_wire::AckMode;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

use crate::config::Config;
use crate::protocol::StartAt;
use crate::scope::Scope;

use shared::SharedSubscription;
pub(crate) use shared::{Identity, Pool};

/// Where the brokers are and how to trust them, read once at startup.
pub(crate) struct Brokers {
    addrs: Vec<String>,
    server_name: String,
    roots: Option<Arc<RootCertStore>>,
    tenant: String,
    namespace: String,
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
            next: AtomicUsize::new(0),
        })
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Resolved broker addresses, starting at a different one each call.
    /// Felix tries them in order and waits out a handshake timeout on each
    /// that is down, so starting every connection at the same one would make
    /// every session pay for that broker's loss.
    async fn seeds(&self) -> Result<Vec<std::net::SocketAddr>> {
        let mut seeds = crate::resolve_brokers(&self.addrs).await?;
        let first = self.next.fetch_add(1, Ordering::Relaxed) % seeds.len();
        seeds.rotate_left(first);
        Ok(seeds)
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
        let seeds = self.seeds().await?;
        ClusterClient::connect(&seeds, &self.server_name, config)
            .await
            .context("connect to Felix")
    }
}

/// How a session reaches Felix.
enum Link {
    /// A connection of its own.
    Own(Arc<ClusterClient>),
    /// Its user's identity on a shared connection.
    Shared(shared::Session),
}

/// One session's Felix connection, reaching only its scope's resources.
/// Resources are named by their Felix names, resolved from the scope.
pub(crate) struct Felix {
    link: Link,
    tenant: String,
    namespace: String,
    pub(crate) scope: Scope,
    /// Who signed in, for streams that stamp the sender.
    pub(crate) principal: String,
}

/// A stream subscription, whichever way the session reaches Felix.
pub(crate) enum Subscription {
    Own(ClusterSubscription),
    Shared(SharedSubscription),
}

impl Subscription {
    pub(crate) fn start_offset(&self) -> Option<u64> {
        match self {
            Self::Own(subscription) => subscription.start_offset(),
            Self::Shared(subscription) => subscription.start_offset(),
        }
    }

    pub(crate) fn live_offset(&self) -> Option<u64> {
        match self {
            Self::Own(subscription) => subscription.live_offset(),
            Self::Shared(subscription) => subscription.live_offset(),
        }
    }

    pub(crate) async fn next_event(&mut self) -> Result<Option<Event>> {
        match self {
            Self::Own(subscription) => subscription.next_event().await,
            Self::Shared(subscription) => subscription.next_event().await,
        }
    }
}

/// A cache watch, whichever way the session reaches Felix.
pub(crate) enum Watch {
    Own(Box<ClusterCacheWatch>),
    Shared(CacheWatch),
}

impl Watch {
    pub(crate) fn retained_count(&self) -> Option<u64> {
        match self {
            Self::Own(watch) => watch.retained_count(),
            Self::Shared(watch) => watch.retained_count(),
        }
    }

    pub(crate) async fn recv(&mut self) -> Option<CacheWatchItem> {
        match self {
            Self::Own(watch) => watch.recv().await,
            Self::Shared(watch) => watch.recv().await,
        }
    }
}

impl Felix {
    pub(crate) fn new(
        client: ClusterClient,
        brokers: &Brokers,
        scope: Scope,
        principal: String,
    ) -> Self {
        Self::with_link(Link::Own(Arc::new(client)), brokers, scope, principal)
    }

    /// A session acting as its user on one of `pool`'s connections, with
    /// that user's delegated `tokens`.
    pub(crate) async fn shared(
        pool: &Arc<Pool>,
        tokens: Arc<dyn TokenProvider>,
        brokers: &Brokers,
        scope: Scope,
        principal: String,
    ) -> Result<Self> {
        let session = shared::Session::attach(pool, tokens).await?;
        Ok(Self::with_link(
            Link::Shared(session),
            brokers,
            scope,
            principal,
        ))
    }

    fn with_link(link: Link, brokers: &Brokers, scope: Scope, principal: String) -> Self {
        Self {
            link,
            tenant: brokers.tenant.clone(),
            namespace: brokers.namespace.clone(),
            scope,
            principal,
        }
    }

    /// The identity a shared session's request goes out on, and a check that
    /// replaces it when the request failed. The broker ends the publish or
    /// cache stream a refused request came on, and an identity has one of
    /// each, so without a new one the session could not publish again.
    async fn on_identity<T, F, Fut>(&self, session: &shared::Session, request: F) -> Result<T>
    where
        F: FnOnce(Arc<Identity>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let identity = session.identity().await;
        let result = request(Arc::clone(&identity)).await;
        if result.is_err() {
            session.renew(&identity).await;
        }
        result
    }

    /// Publish once, never resent by the gateway: the browser owns retries,
    /// because only it knows what makes a retry safe to deduplicate.
    pub(crate) async fn publish(
        &self,
        stream: &str,
        payload: Vec<u8>,
        ack: bool,
    ) -> Result<Option<u64>> {
        let ack = if ack {
            AckMode::PerMessage
        } else {
            AckMode::None
        };
        match &self.link {
            Link::Own(client) => {
                client
                    .publish(&self.tenant, &self.namespace, stream, payload, ack)
                    .await
            }
            Link::Shared(session) => {
                self.on_identity(session, |identity| async move {
                    identity
                        .client
                        .publisher()
                        .await?
                        .publish(&self.tenant, &self.namespace, stream, payload, ack)
                        .await
                })
                .await
            }
        }
    }

    pub(crate) async fn subscribe(&self, stream: &str, from: StartAt) -> Result<Subscription> {
        // `Latest` rather than no position: only then does the broker report
        // the tail it registered at, which a joining browser needs to know
        // what lies between its snapshot and the live events.
        let start = match from {
            StartAt::Offset(offset) => StartPosition::Offset(offset),
            StartAt::Live(_) => StartPosition::Latest,
        };
        match &self.link {
            Link::Own(client) => client
                .subscribe_from(&self.tenant, &self.namespace, stream, Some(start))
                .await
                .map(Subscription::Own),
            Link::Shared(session) => SharedSubscription::start(
                session.identity().await,
                &self.tenant,
                &self.namespace,
                stream,
                start,
            )
            .await
            .map(Subscription::Shared),
        }
    }

    /// Add to a counter. Like a publish it is sent once; Felix counts a
    /// retried add twice.
    pub(crate) async fn counter_add(&self, counter: &str, key: &str, delta: i64) -> Result<i64> {
        match &self.link {
            Link::Own(client) => {
                client
                    .client()
                    .await
                    .counter_add(&self.tenant, &self.namespace, counter, key, delta)
                    .await
            }
            Link::Shared(session) => {
                self.on_identity(session, |identity| async move {
                    identity
                        .client
                        .counter_add(&self.tenant, &self.namespace, counter, key, delta)
                        .await
                })
                .await
            }
        }
    }

    pub(crate) async fn cache_get(&self, cache: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let value = match &self.link {
            Link::Own(client) => {
                client
                    .client()
                    .await
                    .cache_get(&self.tenant, &self.namespace, cache, key)
                    .await?
            }
            Link::Shared(session) => {
                self.on_identity(session, |identity| async move {
                    identity
                        .client
                        .cache_get(&self.tenant, &self.namespace, cache, key)
                        .await
                })
                .await?
            }
        };
        Ok(value.map(|bytes| bytes.to_vec()))
    }

    pub(crate) async fn cache_put(
        &self,
        cache: &str,
        key: &str,
        payload: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let ttl = ttl.map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX));
        match &self.link {
            Link::Own(client) => {
                client
                    .client()
                    .await
                    .cache_put(
                        &self.tenant,
                        &self.namespace,
                        cache,
                        key,
                        payload.into(),
                        ttl,
                    )
                    .await
            }
            Link::Shared(session) => {
                self.on_identity(session, |identity| async move {
                    identity
                        .client
                        .cache_put(
                            &self.tenant,
                            &self.namespace,
                            cache,
                            key,
                            payload.into(),
                            ttl,
                        )
                        .await
                })
                .await
            }
        }
    }

    pub(crate) async fn cache_delete(&self, cache: &str, key: &str) -> Result<()> {
        match &self.link {
            Link::Own(client) => client
                .client()
                .await
                .cache_delete(&self.tenant, &self.namespace, cache, key)
                .await
                .map(drop),
            Link::Shared(session) => {
                self.on_identity(session, |identity| async move {
                    identity
                        .client
                        .cache_delete(&self.tenant, &self.namespace, cache, key)
                        .await
                        .map(drop)
                })
                .await
            }
        }
    }

    /// Every entry in the cache, then each change to one. The empty prefix
    /// covers the whole cache only when it has one shard.
    pub(crate) async fn watch_cache(&self, cache: &str) -> Result<Watch> {
        let filter = CacheWatchFilter::Prefix(String::new());
        match &self.link {
            Link::Own(client) => client
                .watch_cache_retained(&self.tenant, &self.namespace, cache, filter)
                .await
                .map(|watch| Watch::Own(Box::new(watch))),
            Link::Shared(session) => session
                .identity()
                .await
                .client
                .watch_cache_retained(&self.tenant, &self.namespace, cache, filter)
                .await
                .map(Watch::Shared),
        }
    }
}

/// `tokens` attached as an identity on one of `pool`'s connections.
pub(crate) async fn attach_identity(
    pool: &Arc<Pool>,
    tokens: Arc<dyn TokenProvider>,
) -> Result<Arc<Identity>> {
    Ok(shared::Session::attach(pool, tokens)
        .await?
        .identity()
        .await)
}
