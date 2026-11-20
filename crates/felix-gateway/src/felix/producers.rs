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

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use felix_client::{
    AuthError, BrokerError, PublishRefused, RecordHeadersError, RetryClass, outcome_unknown,
};
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

/// Failed attempts in a row after which the gateway stops re-driving a
/// publish and lets it go. See [`Entry::publish`].
const REDRIVE_LIMIT: u32 = 6;

/// How long a publish may stay unsettled before it is let go.
const UNSETTLED_MAX: Duration = Duration::from_secs(120);

/// Publishes of one stream that failed for good and may be sent again.
const FAILED_MAX: usize = 256;

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

/// There is no room for another session.
#[derive(Debug)]
pub(crate) struct Full;

impl std::fmt::Display for Full {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "too many resumable sessions are open or unsettled")
    }
}

impl<P> Registry<P> {
    pub(crate) fn new(idle: Duration) -> Self {
        Self {
            entries: StdMutex::default(),
            idle,
        }
    }

    /// The entry for `key`, made if there is none. Idle entries are dropped
    /// first. At a cap, the oldest idle entry nobody holds and that has
    /// nothing unsettled makes room; if there is none, the new session is
    /// refused rather than breaking one in use.
    ///
    /// # Errors
    /// [`Full`] when a cap is reached and nothing can be dropped.
    pub(crate) fn entry(&self, key: &Key) -> Result<std::sync::Arc<Entry<P>>, Full> {
        let now = Instant::now();
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|_, entry| entry.in_use() || entry.idle_for(now) < self.idle);
        if let Some(entry) = entries.get(key) {
            entry.touch(now);
            return Ok(std::sync::Arc::clone(entry));
        }
        let held = entries
            .keys()
            .filter(|other| other.principal == key.principal)
            .count();
        if held >= PER_PRINCIPAL {
            let victim = Self::victim(&entries, now, Some(&key.principal));
            entries.remove(&victim.ok_or(Full)?);
        }
        if entries.len() >= TOTAL {
            let victim = Self::victim(&entries, now, None);
            entries.remove(&victim.ok_or(Full)?);
        }
        let entry = std::sync::Arc::new(Entry::new(now));
        entries.insert(key.clone(), std::sync::Arc::clone(&entry));
        Ok(entry)
    }

    /// The longest idle entry, of `principal` if given, that no connection
    /// holds and that has nothing unsettled.
    fn victim(
        entries: &HashMap<Key, std::sync::Arc<Entry<P>>>,
        now: Instant,
        principal: Option<&str>,
    ) -> Option<Key> {
        entries
            .iter()
            .filter(|(key, entry)| {
                principal.is_none_or(|p| key.principal == p) && entry.evictable()
            })
            .max_by_key(|(_, entry)| entry.idle_for(now))
            .map(|(key, _)| key.clone())
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

/// One session's memory: per stream, its producer and what Felix has
/// acknowledged.
pub(crate) struct Entry<P> {
    inner: Mutex<Inner<P>>,
    last_used: StdMutex<Instant>,
}

struct Inner<P> {
    streams: HashMap<String, Stream<P>>,
}

/// One Felix stream of a session. Its producer is its own, so letting one go
/// affects this stream alone.
struct Stream<P> {
    /// The producer, and the connection (`owner`) it was made on.
    producer: Option<(u64, Arc<P>)>,
    /// The highest browser number Felix acknowledged, and its offset when
    /// Felix reported one.
    acked: Option<(u64, Option<u64>)>,
    /// A publish that failed without an answer. It may or may not be in the
    /// log, so nothing after it is sent or acknowledged until it settles.
    unsettled: Option<Unsettled>,
    /// Numbers below `acked` that are not known to be in the log, because the
    /// publish failed for good or was let go. A resend of one is sent, not
    /// answered as already acknowledged.
    failed: BTreeSet<u64>,
}

impl<P> Default for Stream<P> {
    fn default() -> Self {
        Self {
            producer: None,
            acked: None,
            unsettled: None,
            failed: BTreeSet::new(),
        }
    }
}

struct Unsettled {
    seq: u64,
    payload: Vec<u8>,
    /// Failed attempts in a row.
    failures: u32,
    since: Instant,
}

/// Whether `err` says the publish was not applied and sending it again fails
/// the same way: a refusal of the request itself, not a lost answer.
fn definite(err: &anyhow::Error) -> bool {
    !outcome_unknown(err)
        && err.chain().any(|cause| {
            cause.is::<AuthError>()
                || cause.is::<RecordHeadersError>()
                || cause
                    .downcast_ref::<BrokerError>()
                    .is_some_and(|broker| broker.retry == RetryClass::Fatal)
        })
}

impl<P> Entry<P> {
    fn new(now: Instant) -> Self {
        Self {
            inner: Mutex::new(Inner {
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

    /// Whether a connection holds this entry: the registry's own handle is
    /// the only other one.
    fn in_use(self: &Arc<Self>) -> bool {
        Arc::strong_count(self) > 1
    }

    /// Whether it can be dropped without breaking a session or losing a
    /// publish in doubt.
    fn evictable(self: &Arc<Self>) -> bool {
        !self.in_use()
            && self
                .inner
                .try_lock()
                .is_ok_and(|inner| inner.streams.values().all(|s| s.unsettled.is_none()))
    }

    /// Let go of the producers made on connection `owner`, which hold that
    /// connection's Felix client, except where a publish is unsettled and
    /// only that producer can resend it safely.
    pub(crate) async fn release(&self, owner: u64) {
        let mut inner = self.inner.lock().await;
        for stream in inner.streams.values_mut() {
            if stream.unsettled.is_none()
                && stream
                    .producer
                    .as_ref()
                    .is_some_and(|(made_on, _)| *made_on == owner)
            {
                stream.producer = None;
            }
        }
    }
}
impl<P> Entry<P> {
    /// Publish number `seq` of `stream`, unless it was already acknowledged.
    ///
    /// Numbers must rise within a session and stream; they may skip. A
    /// number is answered as already done only if Felix acknowledged it or a
    /// later one and it did not fail for good. A publish that failed without
    /// an answer stays unsettled: before a later number is sent, the gateway
    /// sends it again from its own copy, and fails the later one if that
    /// fails, so a later number is never acknowledged ahead of it.
    ///
    /// A publish Felix refused for good (not permitted, no such stream,
    /// too large) is recorded as failed and does not hold up later numbers; a
    /// resend of it is sent again. One unsettled for more than
    /// [`REDRIVE_LIMIT`] attempts or [`UNSETTLED_MAX`] is let go the same way,
    /// and a resend of it may then land twice.
    ///
    /// `owner` names the connection asking, and `make` builds a producer on
    /// it. A stream's producer made on an earlier connection keeps the
    /// publish while it is unsettled, since only it can resend under the same
    /// sequence, and is replaced when it is not, or after repeated failures.
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
        M: Fn() -> MF,
        MF: Future<Output = Result<P>>,
        S: Fn(Arc<P>, Vec<u8>) -> SF,
        SF: Future<Output = Result<Option<u64>>>,
    {
        self.touch(Instant::now());
        let mut guard = self.inner.lock().await;
        let state = guard.streams.entry(stream.to_string()).or_default();

        if let Some((last, offset)) = state.acked
            && seq <= last
            && !state.failed.contains(&seq)
        {
            return Ok(Outcome::Resent((seq == last).then_some(offset).flatten()));
        }
        if let Some(unsettled) = &state.unsettled
            && seq < unsettled.seq
            && !state.failed.contains(&seq)
        {
            anyhow::bail!(
                "seq {seq} is out of order: seq {} is unsettled and must be resent first",
                unsettled.seq
            );
        }
        if state.failed.len() >= FAILED_MAX && !state.failed.contains(&seq) {
            anyhow::bail!("too many publishes on this stream failed; join again");
        }

        // One that has been tried enough is let go, and its producer with it.
        // Whether it landed cannot be told any more, so a resend may land twice.
        if let Some(unsettled) = &state.unsettled
            && unsettled.seq != seq
            && (unsettled.failures >= REDRIVE_LIMIT || unsettled.since.elapsed() > UNSETTLED_MAX)
        {
            let gone = state.unsettled.take().expect("checked above");
            state.failed.insert(gone.seq);
            state.producer = None;
        }

        let give_up = state
            .unsettled
            .as_ref()
            .is_some_and(|u| u.failures >= GIVE_UP_AFTER);
        let stale = match &state.producer {
            None => true,
            Some((made_on, _)) => *made_on != owner && (state.unsettled.is_none() || give_up),
        };
        if stale {
            state.producer = Some((owner, Arc::new(make().await?)));
        }

        // An earlier publish still unsettled goes first, from the gateway's
        // copy, so a connection that went on without it is not left stuck
        // behind it and nothing later is acknowledged before it.
        if let Some(unsettled) = &mut state.unsettled
            && unsettled.seq != seq
        {
            let live = Arc::clone(&state.producer.as_ref().expect("held").1);
            match send(live, unsettled.payload.clone()).await {
                Ok(offset) => {
                    let earlier = unsettled.seq;
                    state.failed.remove(&earlier);
                    state.unsettled = None;
                    settle(&mut state.acked, earlier, offset);
                }
                Err(err) if definite(&err) => {
                    let gone = state.unsettled.take().expect("checked above");
                    state.failed.insert(gone.seq);
                    state.producer = None;
                    state.producer = Some((owner, Arc::new(make().await?)));
                }
                Err(err) => {
                    unsettled.failures += 1;
                    let earlier = unsettled.seq;
                    refused(&mut state.producer, &err);
                    return Err(err.context(format!(
                        "seq {earlier} is still unsettled, so seq {seq} was not sent"
                    )));
                }
            }
        }

        let live = Arc::clone(&state.producer.as_ref().expect("held").1);
        match send(live, payload.clone()).await {
            Ok(offset) => {
                state.failed.remove(&seq);
                state.unsettled = None;
                settle(&mut state.acked, seq, offset);
                Ok(Outcome::Landed(offset))
            }
            Err(err) if definite(&err) => {
                // Nothing was applied and a resend fails the same way, so it
                // is not kept in doubt. Its producer holds it in doubt, though,
                // so that goes too.
                state.unsettled = None;
                state.failed.insert(seq);
                state.producer = None;
                Err(err)
            }
            Err(err) => {
                let failures = state.unsettled.as_ref().map_or(0, |u| u.failures) + 1;
                let since = state
                    .unsettled
                    .as_ref()
                    .map_or_else(Instant::now, |u| u.since);
                state.unsettled = Some(Unsettled {
                    seq,
                    payload,
                    failures,
                    since,
                });
                refused(&mut state.producer, &err);
                Err(err)
            }
        }
    }
}

/// Record that `seq` was acknowledged. A lower number sent again after a
/// failure does not lower what is known.
fn settle(acked: &mut Option<(u64, Option<u64>)>, seq: u64, offset: Option<u64>) {
    if acked.is_none_or(|(last, _)| seq > last) {
        *acked = Some((seq, offset));
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

    use felix_client::{BrokerError, ErrorCode, PublishRefusalReason, RetryClass};

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
        forbidden: StdMutex<HashSet<String>>,
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
            self.forbidden.lock().unwrap().remove(&name(stream, seq));
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
                        if self.forbidden.lock().unwrap().contains(&payload) {
                            return Err(BrokerError::new(
                                ErrorCode::Forbidden,
                                RetryClass::Fatal,
                                "not permitted",
                            )
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
        Registry::new(IDLE).entry(&key("ann", "s1")).unwrap()
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
        let ops: Vec<u32> = rig
            .sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, p)| p == "ops/7")
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(ops[0], ops[1], "the resend went to the producer holding it");
    }

    /// Another stream's producer is not touched when this one is let go.
    #[tokio::test]
    async fn giving_up_on_one_stream_leaves_another_streams_unsettled_publish_alone() {
        let entry = entry();
        let rig = Rig::default();
        rig.fail("other", 1);
        assert!(rig.publish(&entry, 1, "other", 1).await.is_err());
        rig.fail("ops", 7);
        for _ in 0..GIVE_UP_AFTER {
            assert!(rig.publish(&entry, 1, "ops", 7).await.is_err());
        }
        rig.heal("ops", 7);
        rig.heal("other", 1);
        // A new connection replaces ops' producer only.
        rig.publish(&entry, 2, "ops", 7).await.unwrap();
        rig.publish(&entry, 2, "other", 1).await.unwrap();
        let sent = rig.sent.lock().unwrap().clone();
        let other: Vec<u32> = sent
            .iter()
            .filter(|(_, p)| p == "other/1")
            .map(|(i, _)| *i)
            .collect();
        assert_eq!(
            other[0], other[1],
            "other/1 was resent on the producer holding it"
        );
    }

    /// A publish Felix refused for good does not hold up what follows, and a
    /// resend of it is sent again, not answered as done.
    #[tokio::test]
    async fn a_publish_refused_for_good_does_not_wedge_the_session() {
        let entry = entry();
        let rig = Rig::default();
        rig.forbidden.lock().unwrap().insert(name("ops", 7));
        assert!(rig.publish(&entry, 1, "ops", 7).await.is_err());
        assert_eq!(
            rig.publish(&entry, 1, "ops", 8).await.unwrap(),
            Outcome::Landed(Some(108))
        );
        // Still refused: the resend is sent and fails, never answered as done.
        assert!(rig.publish(&entry, 2, "ops", 7).await.is_err());
        rig.heal("ops", 7);
        assert_eq!(
            rig.publish(&entry, 2, "ops", 7).await.unwrap(),
            Outcome::Landed(Some(107))
        );
        assert_eq!(
            rig.publish(&entry, 2, "ops", 7).await.unwrap(),
            Outcome::Resent(None)
        );
    }

    /// A publish that cannot be settled is let go after enough attempts, and a
    /// resend of it is then sent, not answered as done.
    #[tokio::test]
    async fn an_unsettled_publish_is_let_go_after_enough_attempts() {
        let entry = entry();
        let rig = Rig::default();
        rig.fail("ops", 7);
        for _ in 0..REDRIVE_LIMIT {
            assert!(rig.publish(&entry, 1, "ops", 7).await.is_err());
        }
        assert_eq!(
            rig.publish(&entry, 1, "ops", 8).await.unwrap(),
            Outcome::Landed(Some(108))
        );
        rig.heal("ops", 7);
        assert_eq!(
            rig.publish(&entry, 2, "ops", 7).await.unwrap(),
            Outcome::Landed(Some(107))
        );
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
        let held = |entry: &Entry<Fake>| {
            entry.inner.try_lock().unwrap().streams["ops"]
                .producer
                .is_some()
        };
        assert!(!held(&entry));

        rig.fail("ops", 8);
        assert!(rig.publish(&entry, 2, "ops", 8).await.is_err());
        entry.release(2).await;
        assert!(held(&entry));
    }

    #[tokio::test]
    async fn another_person_naming_the_same_session_gets_a_separate_entry() {
        let registry = Registry::new(IDLE);
        let rig = Rig::default();
        let ann = registry.entry(&key("ann", "s1")).unwrap();
        rig.publish(&ann, 1, "ops", 7).await.unwrap();
        let bob = registry.entry(&key("bob", "s1")).unwrap();
        assert_eq!(
            rig.publish(&bob, 2, "ops", 7).await.unwrap(),
            Outcome::Landed(Some(107))
        );
        assert!(!Arc::ptr_eq(&ann, &bob));
    }

    #[tokio::test]
    async fn idle_entries_are_forgotten_and_the_caps_hold() {
        let registry = Registry::<Fake>::new(Duration::ZERO);
        registry.entry(&key("ann", "s1")).unwrap();
        registry.entry(&key("ann", "s2")).unwrap();
        assert_eq!(registry.len(), 1);

        let registry = Registry::<Fake>::new(IDLE);
        for n in 0..PER_PRINCIPAL + 5 {
            registry.entry(&key("ann", &n.to_string())).unwrap();
        }
        registry.entry(&key("bob", "s1")).unwrap();
        assert_eq!(registry.len(), PER_PRINCIPAL + 1);

        let registry = Registry::<Fake>::new(IDLE);
        for n in 0..TOTAL + 5 {
            registry.entry(&key(&format!("p{n}"), "s")).unwrap();
        }
        assert_eq!(registry.len(), TOTAL);
    }

    /// A cap never breaks a session in use or drops a publish in doubt: the
    /// new session is refused instead.
    #[tokio::test]
    async fn at_a_cap_a_session_in_use_or_unsettled_is_never_evicted() {
        let registry = Registry::<Fake>::new(IDLE);
        let rig = Rig::default();
        let mut held = Vec::new();
        for n in 0..PER_PRINCIPAL {
            held.push(registry.entry(&key("ann", &n.to_string())).unwrap());
        }
        assert!(registry.entry(&key("ann", "new")).is_err());
        assert!(registry.entry(&key("bob", "s")).is_ok());

        // One is let go by its connection but has a publish in doubt.
        let unsettled = held.pop().unwrap();
        rig.fail("ops", 1);
        assert!(rig.publish(&unsettled, 1, "ops", 1).await.is_err());
        drop(unsettled);
        held.pop();
        let again = registry.entry(&key("ann", "new")).unwrap();
        assert_eq!(registry.len(), PER_PRINCIPAL + 1, "the free one made room");
        drop(again);
        held.pop();
        assert!(registry.entry(&key("ann", "newer")).is_ok());
        let kept = registry
            .entry(&key("ann", &(PER_PRINCIPAL - 1).to_string()))
            .unwrap();
        assert!(
            kept.inner.try_lock().unwrap().streams.contains_key("ops"),
            "the unsettled one is still there"
        );
    }
}
