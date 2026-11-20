//! Publishes that land once across reconnects. A browser that names a
//! session in its `join` and numbers its publishes can resend after a drop
//! without a second copy reaching the log.
//!
//! Two layers make that true. The gateway remembers, per session and stream,
//! the highest browser number Felix acknowledged, and answers a resend of
//! anything at or below it without publishing. A publish whose answer was
//! lost goes through Felix's idempotent producer, which holds it in doubt and
//! sends the same batch again under the same sequence. Both outlive the
//! WebSocket: they belong to a [`Registry`] entry keyed by who the session is.

use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use anyhow::Result;
use felix_client::PublishRefused;
use tokio::sync::Mutex;

/// How long a session's entry is kept after its last publish. A browser that
/// is back inside this resumes its sequence; one that is not starts over, and
/// a resend of something that landed before may land again.
pub(crate) const IDLE: Duration = Duration::from_secs(600);

/// Entries one principal may hold. A page opens one per tab, so this is far
/// above real use and only bounds what a client that invents session names
/// can pin in memory.
const PER_PRINCIPAL: usize = 32;

/// Failures in a row after which a producer stuck on a batch in doubt is given
/// up for a fresh one. Below it, a batch that failed is re-sent on the same
/// producer, which is what keeps it from landing twice.
const GIVE_UP_AFTER: u32 = 3;

/// Who a session is: the same person, in the same namespace, under the name
/// the browser chose. Another person naming the same session gets another
/// entry, so nobody can resume or read a stranger's acknowledgements.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    pub(crate) principal: String,
    pub(crate) namespace: String,
    pub(crate) session: String,
}

/// What a publish came to.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Appended now, or answered by Felix as already appended, at `offset`.
    Landed(Option<u64>),
    /// Acknowledged earlier under this session, so not sent. The offset is
    /// known only for the latest one.
    Resent(Option<u64>),
}

/// The entries of every session, shared by all connections.
pub(crate) struct Registry<P> {
    entries: StdMutex<HashMap<Key, std::sync::Arc<Entry<P>>>>,
    idle: Duration,
}

impl<P> Registry<P> {
    pub(crate) fn new(idle: Duration) -> Self {
        Self {
            entries: StdMutex::default(),
            idle,
        }
    }

    /// The entry for `key`, made if there is none. Idle entries are dropped
    /// first, and the principal's oldest if it is at its cap.
    pub(crate) fn entry(&self, key: &Key) -> std::sync::Arc<Entry<P>> {
        let now = Instant::now();
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|_, entry| entry.idle_for(now) < self.idle);
        if let Some(entry) = entries.get(key) {
            entry.touch(now);
            return std::sync::Arc::clone(entry);
        }
        let held: Vec<_> = entries
            .iter()
            .filter(|(other, _)| other.principal == key.principal)
            .map(|(other, entry)| (other.clone(), entry.idle_for(now)))
            .collect();
        if held.len() >= PER_PRINCIPAL
            && let Some((oldest, _)) = held.into_iter().max_by_key(|(_, idle)| *idle)
        {
            entries.remove(&oldest);
        }
        let entry = std::sync::Arc::new(Entry::new(now));
        entries.insert(key.clone(), std::sync::Arc::clone(&entry));
        entry
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

/// One session's memory: its producer, and what Felix has acknowledged.
pub(crate) struct Entry<P> {
    inner: Mutex<Inner<P>>,
    last_used: StdMutex<Instant>,
}

struct Inner<P> {
    /// The producer, and the connection (`owner`) it was made on.
    producer: Option<(u64, P)>,
    /// Failed publishes since the last success.
    failures: u32,
    /// Per Felix stream: the highest browser number acknowledged, and its
    /// offset when Felix reported one.
    acked: HashMap<String, (u64, Option<u64>)>,
}

impl<P> Entry<P> {
    fn new(now: Instant) -> Self {
        Self {
            inner: Mutex::new(Inner {
                producer: None,
                failures: 0,
                acked: HashMap::new(),
            }),
            last_used: StdMutex::new(now),
        }
    }

    fn touch(&self, now: Instant) {
        *self.last_used.lock().unwrap_or_else(|e| e.into_inner()) = now;
    }

    fn idle_for(&self, now: Instant) -> Duration {
        let used = *self.last_used.lock().unwrap_or_else(|e| e.into_inner());
        now.saturating_duration_since(used)
    }

    /// Publish number `seq` of `stream`, unless it was already acknowledged.
    ///
    /// Numbers must rise within a session and stream; they may skip. `owner`
    /// names the connection asking, and `make` builds a producer on it. A
    /// producer made on an earlier connection keeps the publish while one is
    /// in doubt, since only it can resend under the same sequence, and is
    /// replaced when it has nothing in doubt or has failed repeatedly.
    ///
    /// Calls on one entry run one at a time, so two connections that name the
    /// same session cannot interleave.
    pub(crate) async fn publish<M, S>(
        &self,
        owner: u64,
        stream: &str,
        seq: u64,
        make: M,
        send: S,
    ) -> Result<Outcome>
    where
        M: AsyncFnOnce() -> Result<P>,
        S: AsyncFnOnce(&P) -> Result<Option<u64>>,
    {
        self.touch(Instant::now());
        let mut inner = self.inner.lock().await;
        if let Some(&(last, offset)) = inner.acked.get(stream)
            && seq <= last
        {
            return Ok(Outcome::Resent((seq == last).then_some(offset).flatten()));
        }
        let stale = match &inner.producer {
            None => true,
            Some((made_on, _)) => {
                *made_on != owner && (inner.failures == 0 || inner.failures >= GIVE_UP_AFTER)
            }
        };
        if stale {
            let producer = make().await?;
            inner.producer = Some((owner, producer));
            inner.failures = 0;
        }
        let (_, producer) = inner.producer.as_ref().expect("a producer was just made");
        match send(producer).await {
            Ok(offset) => {
                inner.failures = 0;
                inner.acked.insert(stream.to_string(), (seq, offset));
                Ok(Outcome::Landed(offset))
            }
            Err(err) => {
                inner.failures += 1;
                // A refusal ends the producer on the stream, and re-sending
                // cannot mend it, so the next publish starts a new one.
                if err.chain().any(|cause| cause.is::<PublishRefused>()) {
                    inner.producer = None;
                }
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    /// A producer that counts what it was asked to send.
    struct Fake {
        id: u32,
        sent: AtomicU32,
    }

    fn key(principal: &str, session: &str) -> Key {
        Key {
            principal: principal.into(),
            namespace: "ns".into(),
            session: session.into(),
        }
    }

    struct Rig {
        made: AtomicU32,
        sent: AtomicU32,
    }

    impl Rig {
        fn new() -> Self {
            Self {
                made: AtomicU32::new(0),
                sent: AtomicU32::new(0),
            }
        }

        async fn publish(
            &self,
            entry: &Entry<Fake>,
            owner: u64,
            seq: u64,
            fail: bool,
        ) -> Result<Outcome> {
            entry
                .publish(
                    owner,
                    "ops",
                    seq,
                    async || {
                        let id = self.made.fetch_add(1, Ordering::SeqCst);
                        Ok(Fake {
                            id,
                            sent: AtomicU32::new(0),
                        })
                    },
                    async |producer: &Fake| {
                        producer.sent.fetch_add(1, Ordering::SeqCst);
                        self.sent.fetch_add(1, Ordering::SeqCst);
                        if fail {
                            anyhow::bail!("connection lost");
                        }
                        Ok(Some(100 + seq))
                    },
                )
                .await
        }
    }

    #[tokio::test]
    async fn a_resend_of_an_acknowledged_publish_is_not_sent_again() {
        let registry = Registry::new(IDLE);
        let entry = registry.entry(&key("ann", "s1"));
        let rig = Rig::new();
        assert_eq!(
            rig.publish(&entry, 1, 7, false).await.unwrap(),
            Outcome::Landed(Some(107))
        );
        // The browser reconnected and resends from where it never saw an ack.
        let entry = registry.entry(&key("ann", "s1"));
        assert_eq!(
            rig.publish(&entry, 2, 7, false).await.unwrap(),
            Outcome::Resent(Some(107))
        );
        assert_eq!(rig.sent.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_older_number_is_answered_without_an_offset() {
        let registry = Registry::new(IDLE);
        let entry = registry.entry(&key("ann", "s1"));
        let rig = Rig::new();
        rig.publish(&entry, 1, 7, false).await.unwrap();
        rig.publish(&entry, 1, 9, false).await.unwrap();
        assert_eq!(
            rig.publish(&entry, 2, 7, false).await.unwrap(),
            Outcome::Resent(None)
        );
        assert_eq!(rig.sent.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_publish_in_doubt_is_resent_on_the_same_producer() {
        let registry = Registry::new(IDLE);
        let entry = registry.entry(&key("ann", "s1"));
        let rig = Rig::new();
        assert!(rig.publish(&entry, 1, 7, true).await.is_err());
        // A new connection of the same session resends the same publish.
        assert_eq!(
            rig.publish(&entry, 2, 7, false).await.unwrap(),
            Outcome::Landed(Some(107))
        );
        assert_eq!(rig.made.load(Ordering::SeqCst), 1);
        assert_eq!(rig.sent.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_producer_that_keeps_failing_is_replaced() {
        let registry = Registry::new(IDLE);
        let entry = registry.entry(&key("ann", "s1"));
        let rig = Rig::new();
        for _ in 0..GIVE_UP_AFTER {
            assert!(rig.publish(&entry, 1, 7, true).await.is_err());
        }
        rig.publish(&entry, 2, 7, false).await.unwrap();
        assert_eq!(rig.made.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_connection_with_nothing_in_doubt_gets_a_producer_of_its_own() {
        let registry = Registry::new(IDLE);
        let entry = registry.entry(&key("ann", "s1"));
        let rig = Rig::new();
        rig.publish(&entry, 1, 7, false).await.unwrap();
        rig.publish(&entry, 2, 8, false).await.unwrap();
        assert_eq!(rig.made.load(Ordering::SeqCst), 2);
        // What the first connection settled is still known.
        assert_eq!(
            rig.publish(&entry, 2, 7, false).await.unwrap(),
            Outcome::Resent(None)
        );
        let inner = entry.inner.lock().await;
        assert_eq!(inner.producer.as_ref().map(|(_, p)| p.id), Some(1));
    }

    #[tokio::test]
    async fn another_person_naming_the_same_session_gets_a_separate_entry() {
        let registry = Registry::new(IDLE);
        let rig = Rig::new();
        let ann = registry.entry(&key("ann", "s1"));
        rig.publish(&ann, 1, 7, false).await.unwrap();
        let bob = registry.entry(&key("bob", "s1"));
        assert_eq!(
            rig.publish(&bob, 2, 7, false).await.unwrap(),
            Outcome::Landed(Some(107))
        );
        assert!(!Arc::ptr_eq(&ann, &bob));
    }

    #[tokio::test]
    async fn idle_entries_are_forgotten_and_a_principal_is_capped() {
        let registry = Registry::<Fake>::new(Duration::ZERO);
        registry.entry(&key("ann", "s1"));
        registry.entry(&key("ann", "s2"));
        assert_eq!(registry.len(), 1);

        let registry = Registry::<Fake>::new(IDLE);
        for n in 0..PER_PRINCIPAL + 5 {
            registry.entry(&key("ann", &n.to_string()));
        }
        registry.entry(&key("bob", "s1"));
        assert_eq!(registry.len(), PER_PRINCIPAL + 1);
    }
}
