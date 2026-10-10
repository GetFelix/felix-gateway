//! Latency of the gateway's two legs, kept apart so a slow request can be
//! blamed on the right hop, and counts of what the write limits refused.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hdrhistogram::Histogram;
use serde::Serialize;

/// Latency histograms in microseconds.
pub struct Metrics {
    browser_rtt: Mutex<Histogram<u64>>,
    /// Keyed by stream alias.
    felix_publish_ack: HashMap<String, Mutex<Histogram<u64>>>,
    /// Indexed by [`Refusal`].
    refused: [AtomicU64; Refusal::ALL.len()],
}

/// Which limit refused a write or a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    MessageSize,
    SessionRate,
    AliasRate,
    PrincipalRate,
    PrincipalSessions,
    IpSessions,
}

impl Refusal {
    const ALL: [Self; 6] = [
        Self::MessageSize,
        Self::SessionRate,
        Self::AliasRate,
        Self::PrincipalRate,
        Self::PrincipalSessions,
        Self::IpSessions,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::MessageSize => "message_size",
            Self::SessionRate => "session_rate",
            Self::AliasRate => "alias_rate",
            Self::PrincipalRate => "principal_rate",
            Self::PrincipalSessions => "principal_sessions",
            Self::IpSessions => "ip_sessions",
        }
    }
}

/// Percentiles of one histogram, in microseconds.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Summary {
    pub count: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

/// What `GET /metrics` returns.
#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    /// Browser to gateway and back: WebSocket ping to pong.
    pub browser_rtt: Summary,
    /// Gateway to Felix and back: an acknowledged publish, for each stream.
    pub felix_publish_ack: BTreeMap<String, Summary>,
    /// Writes and sessions refused by each limit since the gateway started.
    pub limits_refused: BTreeMap<String, u64>,
}

fn histogram() -> Mutex<Histogram<u64>> {
    // 1 µs to 60 s at 3 significant digits.
    Mutex::new(Histogram::new_with_bounds(1, 60_000_000, 3).unwrap())
}

impl Metrics {
    /// Histograms for the browser leg and for publishes on each of `streams`.
    pub(crate) fn new<'a>(streams: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            browser_rtt: histogram(),
            felix_publish_ack: streams
                .into_iter()
                .map(|stream| (stream.to_string(), histogram()))
                .collect(),
            refused: Default::default(),
        }
    }

    pub(crate) fn record_refusal(&self, refusal: Refusal) {
        self.refused[refusal as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_browser_rtt(&self, elapsed: Duration) {
        record(&self.browser_rtt, elapsed);
    }

    pub(crate) fn record_felix_ack(&self, stream: &str, elapsed: Duration) {
        if let Some(histogram) = self.felix_publish_ack.get(stream) {
            record(histogram, elapsed);
        }
    }

    /// The percentiles recorded so far.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            browser_rtt: summarize(&self.browser_rtt),
            felix_publish_ack: self
                .felix_publish_ack
                .iter()
                .map(|(stream, histogram)| (stream.clone(), summarize(histogram)))
                .collect(),
            limits_refused: Refusal::ALL
                .iter()
                .map(|refusal| {
                    let count = self.refused[*refusal as usize].load(Ordering::Relaxed);
                    (refusal.name().to_string(), count)
                })
                .collect(),
        }
    }
}

fn record(histogram: &Mutex<Histogram<u64>>, elapsed: Duration) {
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    histogram.lock().unwrap().saturating_record(micros.max(1));
}

fn summarize(histogram: &Mutex<Histogram<u64>>) -> Summary {
    let histogram = histogram.lock().unwrap();
    Summary {
        count: histogram.len(),
        p50_us: histogram.value_at_quantile(0.5),
        p90_us: histogram.value_at_quantile(0.9),
        p99_us: histogram.value_at_quantile(0.99),
        max_us: histogram.max(),
    }
}
