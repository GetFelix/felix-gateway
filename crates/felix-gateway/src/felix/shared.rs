//! Shared connections: a few Felix connections opened as the gateway, with
//! each session acting as its own user on one of them
//! (`Client::with_identity`). The broker authenticates every stream with the
//! user's delegated token and applies its subscription cap and publish budget
//! per user, so a session reaches what its user may reach and no more.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use felix_client::{
    Client, ClientConfig, ClientIdentity, Event, RefreshingToken, StartPosition,
    SubscriptionLagged, TokenProvider,
};
use tokio::sync::{Mutex, RwLock};

use super::Brokers;
use crate::access::Actor;
use crate::config::SharedConfig;

/// The shared connections. Each slot holds one parent client, connected on
/// first use and replaced when it stops accepting identities.
pub(crate) struct Pool {
    brokers: Arc<Brokers>,
    client_cert: PathBuf,
    client_key: PathBuf,
    /// The gateway's own broker tokens, which the parent clients open with.
    tokens: Arc<dyn TokenProvider>,
    slots: Vec<Slot>,
}

#[derive(Default)]
struct Slot {
    parent: Mutex<Option<Arc<Client>>>,
    /// Sessions attached, to put the next one on the least used slot.
    sessions: AtomicUsize,
}

impl Pool {
    /// A pool of `config.connections` slots. Reads the certificate and key
    /// once, so a bad path fails at startup rather than at the first join.
    pub(crate) fn new(
        brokers: Arc<Brokers>,
        config: &SharedConfig,
        actor: Arc<Actor>,
    ) -> Result<Self> {
        ClientIdentity::from_pem_files(&config.client_cert, &config.client_key)?;
        let tokens: Arc<dyn TokenProvider> = Arc::new(RefreshingToken::new(move || {
            let actor = Arc::clone(&actor);
            async move { actor.broker_token().await }
        }));
        Ok(Self {
            brokers,
            client_cert: config.client_cert.clone(),
            client_key: config.client_key.clone(),
            tokens,
            slots: (0..config.connections).map(|_| Slot::default()).collect(),
        })
    }

    /// The least used slot, counted as taken until the lease drops.
    fn lease(self: &Arc<Self>) -> Lease {
        let (index, slot) = self
            .slots
            .iter()
            .enumerate()
            .min_by_key(|(_, slot)| slot.sessions.load(Ordering::Relaxed))
            .expect("a pool has at least one slot");
        slot.sessions.fetch_add(1, Ordering::Relaxed);
        Lease {
            pool: Arc::clone(self),
            index,
        }
    }

    /// The slot's parent client, connecting it if there is none or `stale`
    /// is the one there.
    async fn parent(&self, index: usize, stale: Option<&Arc<Client>>) -> Result<Arc<Client>> {
        let mut parent = self.slots[index].parent.lock().await;
        if let Some(client) = parent.as_ref()
            && !stale.is_some_and(|stale| Arc::ptr_eq(stale, client))
        {
            return Ok(Arc::clone(client));
        }
        let client = Arc::new(self.connect().await?);
        *parent = Some(Arc::clone(&client));
        Ok(client)
    }

    async fn connect(&self) -> Result<Client> {
        // Read at each connection, so a renewed certificate is used from the
        // next one on.
        let identity = ClientIdentity::from_pem_files(&self.client_cert, &self.client_key)?;
        let quic = felix_client::quic_client_config_with_identity(
            self.brokers.roots.clone(),
            identity,
            true,
        )?;
        let mut config = ClientConfig::optimized_defaults(quic);
        config.auth_tenant_id = Some(self.brokers.tenant().to_string());
        config.token_provider = Some(Arc::clone(&self.tokens));
        // One connection of each kind per slot: the broker's per-user limits
        // are per connection, and the slots are what spreads users out.
        config.publish_conn_pool = 1;
        config.event_conn_pool = 1;
        config.cache_conn_pool = 1;
        let seeds = self.brokers.seeds().await?;
        Client::connect_any(&seeds, &self.brokers.server_name, config)
            .await
            .context("connect to Felix as the gateway")
    }

    /// An identity for `tokens` on the slot `lease` holds. A failure may be
    /// a parent whose connection is gone, so it is tried once more on a new
    /// one before giving up.
    async fn attach(&self, lease: Lease, tokens: &Arc<dyn TokenProvider>) -> Result<Identity> {
        let tenant = self.brokers.tenant().to_string();
        let parent = self.parent(lease.index, None).await?;
        let client = match parent
            .with_identity(tenant.clone(), Arc::clone(tokens))
            .await
        {
            Ok(client) => client,
            Err(first) => {
                tracing::debug!("attaching an identity failed, reconnecting: {first:#}");
                self.parent(lease.index, Some(&parent))
                    .await?
                    .with_identity(tenant, Arc::clone(tokens))
                    .await
                    .context("act as the user on a shared connection")?
            }
        };
        Ok(Identity {
            client,
            _lease: lease,
        })
    }
}

/// A slot taken by one identity.
struct Lease {
    pool: Arc<Pool>,
    index: usize,
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.pool.slots[self.index]
            .sessions
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// One user's identity on a shared connection. Dropping it closes its
/// streams and leaves the connection to everyone else.
pub(crate) struct Identity {
    pub(crate) client: Client,
    _lease: Lease,
}

/// A session's identity, replaced after a failed request.
pub(crate) struct Session {
    pool: Arc<Pool>,
    tokens: Arc<dyn TokenProvider>,
    identity: RwLock<Arc<Identity>>,
}

impl Session {
    pub(crate) async fn attach(pool: &Arc<Pool>, tokens: Arc<dyn TokenProvider>) -> Result<Self> {
        let identity = pool.attach(pool.lease(), &tokens).await?;
        Ok(Self {
            pool: Arc::clone(pool),
            tokens,
            identity: RwLock::new(Arc::new(identity)),
        })
    }

    pub(crate) async fn identity(&self) -> Arc<Identity> {
        Arc::clone(&*self.identity.read().await)
    }

    /// Replace `failed` with a new identity, unless another request already
    /// did. If attaching fails the old one stays, and the next request
    /// reports the trouble.
    pub(crate) async fn renew(&self, failed: &Arc<Identity>) {
        let mut identity = self.identity.write().await;
        if !Arc::ptr_eq(&identity, failed) {
            return;
        }
        match self.pool.attach(self.pool.lease(), &self.tokens).await {
            Ok(fresh) => *identity = Arc::new(fresh),
            Err(err) => tracing::warn!("could not renew a session's identity: {err:#}"),
        }
    }
}

/// A subscription on a shared connection. A plain client's subscription ends
/// when it falls behind and records are dropped; this one subscribes again
/// where the drop began, so a slow browser on a durable stream still gets
/// every record, as it does on a connection of its own.
pub(crate) struct SharedSubscription {
    identity: Arc<Identity>,
    tenant: String,
    namespace: String,
    stream: String,
    subscription: felix_client::Subscription,
}

impl SharedSubscription {
    pub(crate) async fn start(
        identity: Arc<Identity>,
        tenant: &str,
        namespace: &str,
        stream: &str,
        start: StartPosition,
    ) -> Result<Self> {
        let subscription = identity
            .client
            .subscribe_from(tenant, namespace, stream, Some(start))
            .await?;
        Ok(Self {
            identity,
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            subscription,
        })
    }

    pub(crate) fn start_offset(&self) -> Option<u64> {
        self.subscription.start_offset()
    }

    pub(crate) fn live_offset(&self) -> Option<u64> {
        self.subscription.live_offset()
    }

    pub(crate) async fn next_event(&mut self) -> Result<Option<Event>> {
        loop {
            let err = match self.subscription.next_event().await {
                Err(err) => err,
                delivered => return delivered,
            };
            let Some(resume_from) = lagged(&err) else {
                return Err(err);
            };
            self.subscription = self
                .identity
                .client
                .subscribe_from(
                    &self.tenant,
                    &self.namespace,
                    &self.stream,
                    Some(StartPosition::Offset(resume_from)),
                )
                .await
                .context("resubscribe after falling behind")?;
        }
    }
}

/// Where to resume, when `err` says the subscription fell behind.
fn lagged(err: &anyhow::Error) -> Option<u64> {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<SubscriptionLagged>())
        .map(|lagged| lagged.resume_from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lag_resumes_at_the_first_dropped_offset() {
        let err = anyhow::Error::new(SubscriptionLagged { resume_from: 42 })
            .context("subscription to app.ops.lobby");
        assert_eq!(lagged(&err), Some(42));
        assert_eq!(lagged(&anyhow::anyhow!("the broker closed it")), None);
    }
}
