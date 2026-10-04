//! A deliberately slow link for one browser connection, for the slow-client
//! demonstration.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

use crate::protocol::ServerMessage;

/// Paces how fast one connection reads its subscriptions, as if every event
/// had to cross a link of a set speed. Felix keeps delivering at full rate, so
/// the subscription's bounded queue fills and drops new events, which a
/// durable stream then replays from the log. Nothing is dropped here.
#[derive(Debug, Default)]
pub(crate) struct Throttle {
    /// Zero when off.
    bits_per_second: AtomicU64,
    /// When the pretend link finishes sending what it already has.
    free_at: Mutex<Option<Instant>>,
}

impl Throttle {
    /// Slow the link to `bits_per_second`, or lift the limit with `None` or 0.
    pub(crate) fn set(&self, bits_per_second: Option<u64>) {
        self.bits_per_second
            .store(bits_per_second.unwrap_or(0), Ordering::Relaxed);
    }

    /// Wait until the link is free to carry `message`.
    pub(crate) async fn pass(&self, message: &ServerMessage) {
        let rate = self.bits_per_second.load(Ordering::Relaxed);
        if rate == 0 {
            return;
        }
        let bytes = serde_json::to_string(message).map_or(0, |text| text.len());
        tokio::time::sleep_until(self.reserve(bytes, rate)).await;
    }

    /// Book `bytes` on the link and return when they start to go out.
    fn reserve(&self, bytes: usize, rate: u64) -> Instant {
        let mut free_at = self.free_at.lock().expect("throttle lock");
        let now = Instant::now();
        let start = free_at.map_or(now, |at| at.max(now));
        *free_at = Some(start + Duration::from_secs_f64(bytes as f64 * 8.0 / rate as f64));
        start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn holds_events_to_the_set_rate() {
        let throttle = Throttle::default();
        throttle.set(Some(8_000));
        let start = Instant::now();
        // 1,000 bytes a second: each 500-byte booking takes half a second.
        assert_eq!(throttle.reserve(500, 8_000), start);
        assert_eq!(
            throttle.reserve(500, 8_000),
            start + Duration::from_millis(500)
        );
        assert_eq!(throttle.reserve(500, 8_000), start + Duration::from_secs(1));

        // An idle link does not bank time.
        tokio::time::advance(Duration::from_secs(10)).await;
        let later = Instant::now();
        assert_eq!(throttle.reserve(500, 8_000), later);
    }

    #[tokio::test(start_paused = true)]
    async fn passes_at_once_when_off() {
        let throttle = Throttle::default();
        let message = ServerMessage::Counter { id: 1, value: 2 };
        throttle.set(Some(8));
        throttle.set(None);
        let start = Instant::now();
        for _ in 0..100 {
            throttle.pass(&message).await;
        }
        assert_eq!(Instant::now(), start);
    }
}
