//! Latency of the gateway's two legs, kept apart so a slow request can be
//! blamed on the right hop.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use hdrhistogram::Histogram;
use serde::Serialize;

/// Latency histograms in microseconds.
pub struct Metrics {
    browser_rtt: Mutex<Histogram<u64>>,
    /// Keyed by stream alias.
    felix_publish_ack: HashMap<String, Mutex<Histogram<u64>>>,
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
        }
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
