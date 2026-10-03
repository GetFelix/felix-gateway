//! Latency of the gateway's two legs, kept apart so a slow edit can be
//! blamed on the right hop.

use std::sync::Mutex;
use std::time::Duration;

use hdrhistogram::Histogram;
use serde::Serialize;

use crate::protocol::StreamName;

/// Latency histograms in microseconds.
pub struct Metrics {
    browser_rtt: Mutex<Histogram<u64>>,
    felix_ack_ops: Mutex<Histogram<u64>>,
    felix_ack_presence: Mutex<Histogram<u64>>,
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
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Snapshot {
    /// Browser to gateway and back: WebSocket ping to pong.
    pub browser_rtt: Summary,
    /// Gateway to Felix and back: an acknowledged publish on the ops stream.
    pub felix_publish_ack_ops: Summary,
    /// The same on the presence stream.
    pub felix_publish_ack_presence: Summary,
}

impl Default for Metrics {
    fn default() -> Self {
        // 1 µs to 60 s at 3 significant digits.
        let histogram = || Mutex::new(Histogram::new_with_bounds(1, 60_000_000, 3).unwrap());
        Self {
            browser_rtt: histogram(),
            felix_ack_ops: histogram(),
            felix_ack_presence: histogram(),
        }
    }
}

impl Metrics {
    pub(crate) fn record_browser_rtt(&self, elapsed: Duration) {
        record(&self.browser_rtt, elapsed);
    }

    pub(crate) fn record_felix_ack(&self, stream: StreamName, elapsed: Duration) {
        let histogram = match stream {
            StreamName::Ops => &self.felix_ack_ops,
            StreamName::Presence => &self.felix_ack_presence,
        };
        record(histogram, elapsed);
    }

    /// The percentiles recorded so far.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            browser_rtt: summarize(&self.browser_rtt),
            felix_publish_ack_ops: summarize(&self.felix_ack_ops),
            felix_publish_ack_presence: summarize(&self.felix_ack_presence),
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
