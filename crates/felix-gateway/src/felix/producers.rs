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
use std::sync::{Arc, Mutex as StdMutex};
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

/// Entries in all. The oldest idle one goes first when this is reached. Only
/// an entry with an unsettled publish keeps a Felix client past its
/// connection, so this also bounds those.
const TOTAL: usize = 4096;

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
        if entries.len() >= TOTAL
            && let Some(oldest) = entries
                .iter()
                .max_by_key(|(_, entry)| entry.idle_for(now))
                .map(|(key, _)| key.clone())
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
    producer: Option<(u64, Arc<P>)>,
    /// Per Felix stream.
    streams: HashMap<String, Stream>,
}

#[derive(Default)]
struct Stream {
    /// The highest browser number Felix acknowledged, and its offset when
    /// Felix reported one.
    acked: Option<(u64, Option<u64>)>,
    /// A publish that failed without an answer. It may or may not be in the
    /// log, so nothing after it is sent or acknowledged until it settles.
    unsettled: Option<Unsettled>,
}

struct Unsettled {
    seq: u64,
    payload: Vec<u8>,
    /// Failed attempts in a row.
    failures: u32,
}

impl<P> Entry<P> {
    fn new(now: Instant) -> Self {
        Self {
            inner: Mutex::new(Inner {
                producer: None,
                streams: HashMap::new(),
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

    /// Let go of the producer made on connection `owner`, which holds that
    /// connection's Felix client, unless a publish is unsettled and only this
    /// producer can resend it safely.
    pub(crate) async fn release(&self, owner: u64) {
        let mut inner = self.inner.lock().await;
        let unsettled = inner.streams.values().any(|s| s.unsettled.is_some());
        if !unsettled
            && inner
                .producer
                .as_ref()
                .is_some_and(|(made_on, _)| *made_on == owner)
        {
            inner.producer = None;
        }
    }

    /// Publish number `seq` of `stream`, unless it was already acknowledged.
    ///
    /// Numbers must rise within a session and stream; they may skip. A
    /// number is acknowledged only after Felix did, or if it is at or below
    /// one Felix did. A publish that failed stays unsettled: the next call
    /// for the stream sends it again first, and fails if that fails, so a
    /// later number is never acknowledged ahead of an earlier one.
    ///
    /// `owner` names the connection asking, and `make` builds a producer on
    /// it. A producer made on an earlier connection keeps the publish while
    /// anything is unsettled, since only it can resend under the same
    /// sequence, and is replaced when nothing is, or when this stream's
    /// unsettled publish has failed repeatedly.
    ///
    /// Calls on one entry run one at a time, so two connections that name the
    /// same session cannot interleave.
    pub(crate) async fn publish<M, MF, S, SF>(
        &self,
        owner: u64,
        stream: &str,
        seq: u64,
        payload: Vec<u8>,
        make: M,
        send: S,
    ) -> Result<Outcome>
    where
        M: FnOnce() -> MF,
        MF: Future<Output = Result<P>>,
        S: Fn(Arc<P>, Vec<u8>) -> SF,
        SF: Future<Output = Result<Option<u64>>>,
    {
        self.touch(Instant::now());
        let mut guard = self.inner.lock().await;
        let Inner { producer, streams } = &mut *guard;
        let state = streams.entry(stream.to_string()).or_default();
        if let Some((last, offset)) = state.acked
            && seq <= last
        {
            return Ok(Outcome::Resent((seq == last).then_some(offset).flatten()));
        }
        if let Some(unsettled) = &state.unsettled
            && seq < unsettled.seq
        {
            anyhow::bail!(
                "seq {seq} is out of order: seq {} is unsettled and must be resent first",
                unsettled.seq
            );
        }
        let give_up = state
            .unsettled
            .as_ref()
            .is_some_and(|u| u.failures >= GIVE_UP_AFTER);
        let any_unsettled = streams.values().any(|s| s.unsettled.is_some());
        let stale = match producer {
            None => true,
            Some((made_on, _)) => *made_on != owner && (!any_unsettled || give_up),
        };
        if stale {
            *producer = Some((owner, Arc::new(make().await?)));
        }
        let state = streams.get_mut(stream).expect("inserted above");

        // An earlier publish still unsettled goes first, from the gateway's
        // copy, so a connection that went on without it is not left stuck
        // behind it and nothing later is acknowledged before it.
        if let Some(unsettled) = &mut state.unsettled
            && unsettled.seq != seq
        {
            let live = Arc::clone(&producer.as_ref().expect("a producer is held").1);
            match send(live, unsettled.payload.clone()).await {
                Ok(offset) => {
                    state.acked = Some((unsettled.seq, offset));
                    state.unsettled = None;
                }
                Err(err) => {
                    unsettled.failures += 1;
                    let earlier = unsettled.seq;
                    refused(producer, &err);
                    return Err(err.context(format!(
                        "seq {earlier} is still unsettled, so seq {seq} was not sent"
                    )));
                }
            }
        }

        let live = Arc::clone(&producer.as_ref().expect("a producer is held").1);
        match send(live, payload.clone()).await {
            Ok(offset) => {
                state.unsettled = None;
                state.acked = Some((seq, offset));
                Ok(Outcome::Landed(offset))
            }
            Err(err) => {
                let failures = state.unsettled.as_ref().map_or(0, |u| u.failures) + 1;
                state.unsettled = Some(Unsettled {
                    seq,
                    payload,
                    failures,
                });
                refused(producer, &err);
                Err(err)
            }
        }
    }
}

/// A refusal ends the producer on the stream, and re-sending cannot mend it,
/// so the next publish starts a new one.
fn refused<P>(producer: &mut Option<(u64, Arc<P>)>, err: &anyhow::Error) {
    if err.chain().any(|cause| cause.is::<PublishRefused>()) {
        *producer = None;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU32, Ordering};

    use felix_client::PublishRefusalReason;

    use super::*;

    /// A producer that knows its number.
    struct Fake {
        id: u32,
    }

    fn key(principal: &str, session: &str) -> Key {
        Key {
            principal: principal.into(),
            namespace: "ns".into(),
            session: session.into(),
        }
    }

    /// Stands in for Felix. A payload in `failing` fails like a lost answer,
    /// and one in `refusing` like a refusal.
    #[derive(Default)]
    struct Rig {
        made: AtomicU32,
        sent: StdMutex<Vec<(u32, String)>>,
        failing: StdMutex<HashSet<String>>,
        refusing: StdMutex<HashSet<String>>,
    }

    fn name(stream: &str, seq: u64) -> String {
        format!("{stream}/{seq}")
    }

    impl Rig {
        fn fail(&self, stream: &str, seq: u64) {
            self.failing.lock().unwrap().insert(name(stream, seq));
        }

        fn heal(&self, stream: &str, seq: u64) {
            self.failing.lock().unwrap().remove(&name(stream, seq));
            self.refusing.lock().unwrap().remove(&name(stream, seq));
        }

        fn sent(&self) -> Vec<String> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|(_, p)| p.clone())
                .collect()
        }

        async fn publish(
            &self,
            entry: &Entry<Fake>,
            owner: u64,
            stream: &str,
            seq: u64,
        ) -> Result<Outcome> {
            entry
                .publish(
                    owner,
                    stream,
                    seq,
                    name(stream, seq).into_bytes(),
                    || async {
                        Ok(Fake {
                            id: self.made.fetch_add(1, Ordering::SeqCst),
                        })
                    },
                    |producer: Arc<Fake>, payload: Vec<u8>| async move {
                        let payload = String::from_utf8(payload).unwrap();
                        self.sent
                            .lock()
                            .unwrap()
                            .push((producer.id, payload.clone()));
                        if self.refusing.lock().unwrap().contains(&payload) {
                            return Err(PublishRefused {
                                reason: PublishRefusalReason::UnknownProducer,
                                message: "forgotten".into(),
                            }
                            .into());
                        }
                        if self.failing.lock().unwrap().contains(&payload) {
                            anyhow::bail!("connection lost");
                        }
                        let seq: u64 = payload.rsplit('/').next().unwrap().parse().unwrap();
                        Ok(Some(100 + seq))
                    },
                )
                .await
        }
    }

    fn entry() -> Arc<Entry<Fake>> {
        Registry::new(IDLE).entry(&key("ann", "s1"))
    }

    #[tokio::test]
    async fn a_resend_of_an_acknowledged_publish_is_not_sent_again() {
        let entry = entry();
        let rig = Rig::default();
        assert_eq!(
            rig.publish(&entry, 1, "ops", 7).await.unwrap(),
            Outcome::Landed(Some(107))
        );
        assert_eq!(
            rig.publish(&entry, 2, "ops", 7).await.unwrap(),
            Outcome::Resent(Some(107))
        );
        assert_eq!(rig.sent().len(), 1);
    }

    #[tokio::test]
    async fn an_older_number_is_answered_without_an_offset() {
        let entry = entry();
        let rig = Rig::default();
        rig.publish(&entry, 1, "ops", 7).await.unwrap();
        rig.publish(&entry, 1, "ops", 9).await.unwrap();
        assert_eq!(
            rig.publish(&entry, 2, "ops", 7).await.unwrap(),
            Outcome::Resent(None)
        );
        assert_eq!(rig.sent().len(), 2);
    }

    #[tokio::test]
    async fn a_publish_in_doubt_is_resent_on_the_same_producer() {
        let entry = entry();
        let rig = Rig::default();
        rig.fail("ops", 7);
        assert!(rig.publish(&entry, 1, "ops", 7).await.is_err());
        rig.heal("ops", 7);
        assert_eq!(
            rig.publish(&entry, 2, "ops", 7).await.unwrap(),
            Outcome::Landed(Some(107))
        );
        assert_eq!(rig.made.load(Ordering::SeqCst), 1);
        assert_eq!(rig.sent().len(), 2);
    }

    /// The unsettled publish is never skipped: a later number is not
    /// acknowledged while an earlier one is not.
    #[tokio::test]
    async fn a_later_number_is_not_acknowledged_ahead_of_a_failed_one() {
        let entry = entry();
        let rig = Rig::default();
        rig.fail("ops", 7);
        assert!(rig.publish(&entry, 1, "ops", 7).await.is_err());
        // 8 was already on its way. 7 still fails, so 8 is not sent.
        assert!(rig.publish(&entry, 1, "ops", 8).await.is_err());
        assert!(!rig.sent().contains(&"ops/8".to_string()));
        // The browser resends 7 and it is appended, not answered as a resend.
        rig.heal("ops", 7);
        assert_eq!(
            rig.publish(&entry, 2, "ops", 7).await.unwrap(),
            Outcome::Landed(Some(107))
        );
        assert_eq!(
            rig.publish(&entry, 2, "ops", 8).await.unwrap(),
            Outcome::Landed(Some(108))
        );
    }

    #[tokio::test]
    async fn a_refusal_does_not_let_the_next_number_overtake_the_failed_one() {
        let entry = entry();
        let rig = Rig::default();
        rig.refusing.lock().unwrap().insert(name("ops", 7));
        assert!(rig.publish(&entry, 1, "ops", 7).await.is_err());
        // The producer is gone. 8 comes with a new one, and 7 goes first.
        let err = rig.publish(&entry, 1, "ops", 8).await.unwrap_err();
        assert!(err.to_string().contains("unsettled"), "{err:#}");
        rig.heal("ops", 7);
        // 7 is still unsettled, and sending it settles it exactly once.
        assert_eq!(
            rig.publish(&entry, 1, "ops", 7).await.unwrap(),
            Outcome::Landed(Some(107))
        );
        assert_eq!(
            rig.publish(&entry, 1, "ops", 7).await.unwrap(),
            Outcome::Resent(Some(107))
        );
    }

    #[tokio::test]
    async fn a_connection_that_goes_on_settles_the_unsettled_publish_itself() {
        let entry = entry();
        let rig = Rig::default();
        rig.fail("ops", 7);
        assert!(rig.publish(&entry, 1, "ops", 7).await.is_err());
        rig.heal("ops", 7);
        // The same connection sends 8 without resending 7.
        assert_eq!(
            rig.publish(&entry, 1, "ops", 8).await.unwrap(),
            Outcome::Landed(Some(108))
        );
        assert_eq!(rig.sent(), ["ops/7", "ops/7", "ops/8"]);
        assert_eq!(
            rig.publish(&entry, 2, "ops", 7).await.unwrap(),
            Outcome::Resent(None)
        );
    }

    #[tokio::test]
    async fn an_unsettled_publish_survives_another_streams_success() {
        let entry = entry();
        let rig = Rig::default();
        rig.fail("ops", 7);
        assert!(rig.publish(&entry, 1, "ops", 7).await.is_err());
        rig.publish(&entry, 1, "other", 1).await.unwrap();
        rig.heal("ops", 7);
        // A new connection keeps the producer that holds 7 in doubt.
        rig.publish(&entry, 2, "other", 2).await.unwrap();
        rig.publish(&entry, 2, "ops", 7).await.unwrap();
        assert_eq!(rig.made.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_producer_that_keeps_failing_is_replaced() {
        let entry = entry();
        let rig = Rig::default();
        rig.fail("ops", 7);
        for _ in 0..GIVE_UP_AFTER {
            assert!(rig.publish(&entry, 1, "ops", 7).await.is_err());
        }
        rig.heal("ops", 7);
        rig.publish(&entry, 2, "ops", 7).await.unwrap();
        assert_eq!(rig.made.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_connection_with_nothing_in_doubt_gets_a_producer_of_its_own() {
        let entry = entry();
        let rig = Rig::default();
        rig.publish(&entry, 1, "ops", 7).await.unwrap();
        rig.publish(&entry, 2, "ops", 8).await.unwrap();
        assert_eq!(rig.made.load(Ordering::SeqCst), 2);
        assert_eq!(
            rig.publish(&entry, 2, "ops", 7).await.unwrap(),
            Outcome::Resent(None)
        );
    }

    #[tokio::test]
    async fn a_closed_connection_lets_go_of_its_producer_unless_one_is_unsettled() {
        let entry = entry();
        let rig = Rig::default();
        rig.publish(&entry, 1, "ops", 7).await.unwrap();
        entry.release(1).await;
        assert!(entry.inner.lock().await.producer.is_none());

        rig.fail("ops", 8);
        assert!(rig.publish(&entry, 2, "ops", 8).await.is_err());
        entry.release(2).await;
        assert!(entry.inner.lock().await.producer.is_some());
    }

    #[tokio::test]
    async fn another_person_naming_the_same_session_gets_a_separate_entry() {
        let registry = Registry::new(IDLE);
        let rig = Rig::default();
        let ann = registry.entry(&key("ann", "s1"));
        rig.publish(&ann, 1, "ops", 7).await.unwrap();
        let bob = registry.entry(&key("bob", "s1"));
        assert_eq!(
            rig.publish(&bob, 2, "ops", 7).await.unwrap(),
            Outcome::Landed(Some(107))
        );
        assert!(!Arc::ptr_eq(&ann, &bob));
    }

    #[tokio::test]
    async fn idle_entries_are_forgotten_and_the_caps_hold() {
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

        let registry = Registry::<Fake>::new(IDLE);
        for n in 0..TOTAL + 5 {
            registry.entry(&key(&format!("p{n}"), "s"));
        }
        assert_eq!(registry.len(), TOTAL);
    }
}
