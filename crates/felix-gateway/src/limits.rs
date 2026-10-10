//! Write limits: how fast a session and a principal may write, how large one
//! write may be, and how many sessions a principal and a client address may
//! hold. A write over a limit is refused at once, never queued, so a browser
//! that outruns its budget learns about it instead of building a backlog.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};
use serde::Deserialize;

use crate::metrics::{Metrics, Refusal};

/// Room in a WebSocket frame for everything around the base64 payload.
const FRAME_SLACK: usize = 16 * 1024;

/// How often idle principals are forgotten.
const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// The scope file's `[limits]` table.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct LimitsConfig {
    /// Largest decoded payload one publish or cache write may carry, unless
    /// the alias sets its own.
    pub(crate) max_payload_bytes: usize,
    /// Sessions one principal may hold at once across every scope. 0 is no limit.
    pub(crate) sessions_per_principal: u32,
    /// Sessions one client address may hold at once. 0 is no limit.
    pub(crate) sessions_per_ip: u32,
    /// How many proxies in front of the gateway append to `X-Forwarded-For`.
    /// 0 ignores the header and uses the socket's address.
    pub(crate) trusted_proxies: usize,
    pub(crate) session: RateSpec,
    pub(crate) principal: RateSpec,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: 64 * 1024,
            sessions_per_principal: 8,
            sessions_per_ip: 32,
            trusted_proxies: 0,
            session: RateSpec::default(),
            principal: RateSpec::default(),
        }
    }
}

/// A write rate as the scope file gives it; unset keys take the defaults of
/// the level it belongs to.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RateSpec {
    writes_per_s: Option<u64>,
    write_burst: Option<u64>,
    bytes_per_s: Option<u64>,
    byte_burst: Option<u64>,
}

/// Writes and bytes a second, each with a burst. A rate of 0 is no limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Rates {
    pub(crate) writes_per_s: u64,
    pub(crate) write_burst: u64,
    pub(crate) bytes_per_s: u64,
    pub(crate) byte_burst: u64,
}

const SESSION_DEFAULTS: Rates = Rates {
    writes_per_s: 50,
    write_burst: 100,
    bytes_per_s: 256 * 1024,
    byte_burst: 1024 * 1024,
};

const PRINCIPAL_DEFAULTS: Rates = Rates {
    writes_per_s: 100,
    write_burst: 200,
    bytes_per_s: 512 * 1024,
    byte_burst: 2 * 1024 * 1024,
};

impl RateSpec {
    fn resolve(&self, defaults: Rates) -> Rates {
        Rates {
            writes_per_s: self.writes_per_s.unwrap_or(defaults.writes_per_s),
            write_burst: self.write_burst.unwrap_or(defaults.write_burst),
            bytes_per_s: self.bytes_per_s.unwrap_or(defaults.bytes_per_s),
            byte_burst: self.byte_burst.unwrap_or(defaults.byte_burst),
        }
    }
}

impl LimitsConfig {
    pub(crate) fn session_rates(&self) -> Rates {
        self.session.resolve(SESSION_DEFAULTS)
    }

    pub(crate) fn principal_rates(&self) -> Rates {
        self.principal.resolve(PRINCIPAL_DEFAULTS)
    }

    /// Check the limits against the largest payload any alias allows, so no
    /// write that passes the size limit can be refused forever by a burst.
    pub(crate) fn check(&self, largest_payload: usize) -> Result<()> {
        ensure!(
            self.max_payload_bytes > 0,
            "limits: max_payload_bytes must be above 0"
        );
        for (level, rates) in [
            ("session", self.session_rates()),
            ("principal", self.principal_rates()),
        ] {
            ensure!(
                rates.writes_per_s == 0 || rates.write_burst > 0,
                "limits.{level}: write_burst must be above 0"
            );
            ensure!(
                rates.bytes_per_s == 0 || rates.byte_burst >= largest_payload as u64,
                "limits.{level}: byte_burst must be at least the largest max_payload_bytes \
                 ({largest_payload}), or a write that size could never pass"
            );
        }
        Ok(())
    }
}

/// A token bucket: `rate` tokens a second, holding at most `burst`. It starts
/// full.
#[derive(Debug)]
pub(crate) struct Bucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    at: Instant,
}

impl Bucket {
    /// `None` when `rate` is 0, which is no limit.
    pub(crate) fn new(rate: u64, burst: u64, now: Instant) -> Option<Self> {
        (rate > 0).then_some(Self {
            rate: rate as f64,
            burst: burst as f64,
            tokens: burst as f64,
            at: now,
        })
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.at).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        self.at = self.at.max(now);
    }

    /// How long until `cost` tokens are there; zero when they are now.
    pub(crate) fn wait(&mut self, cost: f64, now: Instant) -> Duration {
        self.refill(now);
        let short = cost - self.tokens;
        if short <= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(short / self.rate)
        }
    }

    /// Spend `cost` tokens, after [`Bucket::wait`] said they are there.
    pub(crate) fn take(&mut self, cost: f64) {
        self.tokens -= cost;
    }

    fn full(&mut self, now: Instant) -> bool {
        self.refill(now);
        self.tokens >= self.burst
    }
}

/// A write bucket and a byte bucket, either of which may be off.
#[derive(Debug)]
pub(crate) struct Rate {
    writes: Option<Bucket>,
    bytes: Option<Bucket>,
}

impl Rate {
    pub(crate) fn new(rates: Rates, now: Instant) -> Self {
        Self {
            writes: Bucket::new(rates.writes_per_s, rates.write_burst, now),
            bytes: Bucket::new(rates.bytes_per_s, rates.byte_burst, now),
        }
    }

    /// How long until one write of `bytes` fits; zero when it does now.
    fn wait(&mut self, bytes: usize, now: Instant) -> Duration {
        let writes = self
            .writes
            .as_mut()
            .map_or(Duration::ZERO, |b| b.wait(1.0, now));
        let bytes = self
            .bytes
            .as_mut()
            .map_or(Duration::ZERO, |b| b.wait(bytes as f64, now));
        writes.max(bytes)
    }

    fn take(&mut self, bytes: usize) {
        if let Some(bucket) = &mut self.writes {
            bucket.take(1.0);
        }
        if let Some(bucket) = &mut self.bytes {
            bucket.take(bytes as f64);
        }
    }

    fn full(&mut self, now: Instant) -> bool {
        self.writes.as_mut().is_none_or(|b| b.full(now))
            && self.bytes.as_mut().is_none_or(|b| b.full(now))
    }
}

/// A refused write or session: which limit, and when a write could fit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Refused {
    pub(crate) limit: Refusal,
    /// `None` for a session cap, which lifts when another session closes.
    pub(crate) retry_after: Option<Duration>,
}

impl Refused {
    pub(crate) fn message(&self) -> &'static str {
        match self.limit {
            Refusal::MessageSize => "the payload is larger than this gateway allows",
            Refusal::SessionRate => "this session is writing faster than the gateway allows",
            Refusal::AliasRate => "this session is writing to this alias faster than allowed",
            Refusal::PrincipalRate => "you are writing faster than the gateway allows",
            Refusal::PrincipalSessions => "you have as many sessions open as the gateway allows",
            Refusal::IpSessions => "this address has as many sessions open as the gateway allows",
        }
    }
}

/// The gateway-wide side of the limits: session counts per principal and
/// per address, and each principal's write rate.
pub(crate) struct Limiter {
    config: LimitsConfig,
    metrics: Arc<Metrics>,
    state: Mutex<Registry>,
    /// The largest WebSocket message worth reading.
    pub(crate) max_frame_bytes: usize,
}

struct Registry {
    principals: HashMap<String, Principal>,
    ips: HashMap<IpAddr, u32>,
    swept: Instant,
}

/// A principal's sessions and shared rate. It outlives its last session
/// until the rate has refilled, so reconnecting does not buy a fresh burst.
struct Principal {
    sessions: u32,
    rate: Arc<Mutex<Rate>>,
}

impl Limiter {
    pub(crate) fn new(config: LimitsConfig, largest_payload: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            config,
            metrics,
            state: Mutex::new(Registry {
                principals: HashMap::new(),
                ips: HashMap::new(),
                swept: Instant::now(),
            }),
            max_frame_bytes: largest_payload.div_ceil(3) * 4 + FRAME_SLACK,
        }
    }

    pub(crate) fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// The address a session counts against: the socket's, or with
    /// `trusted_proxies` set, the one that many proxies recorded in
    /// `X-Forwarded-For`. A header too short to hold it is ignored.
    pub(crate) fn client_ip(
        &self,
        socket: Option<IpAddr>,
        forwarded_for: &[&str],
    ) -> Option<IpAddr> {
        let hops = self.config.trusted_proxies;
        if hops == 0 {
            return socket;
        }
        let entries: Vec<&str> = forwarded_for
            .iter()
            .flat_map(|value| value.split(','))
            .map(str::trim)
            .collect();
        entries
            .len()
            .checked_sub(hops)
            .and_then(|index| parse_ip(entries[index]))
            .or(socket)
    }

    /// Count a session against `ip`. Without an address there is nothing to
    /// count against.
    pub(crate) fn admit_ip(self: &Arc<Self>, ip: Option<IpAddr>) -> Result<IpSlot, Refused> {
        let Some(ip) = ip else {
            return Ok(IpSlot(None));
        };
        let cap = self.config.sessions_per_ip;
        let mut state = self.state.lock().expect("limits lock");
        let count = state.ips.entry(ip).or_default();
        if cap > 0 && *count >= cap {
            self.metrics.record_refusal(Refusal::IpSessions);
            return Err(Refused {
                limit: Refusal::IpSessions,
                retry_after: None,
            });
        }
        *count += 1;
        Ok(IpSlot(Some((Arc::clone(self), ip))))
    }

    /// Count a session against `principal` and hand back its shared rate.
    pub(crate) fn admit_principal(
        self: &Arc<Self>,
        principal: &str,
        now: Instant,
    ) -> Result<PrincipalSlot, Refused> {
        let cap = self.config.sessions_per_principal;
        let mut state = self.state.lock().expect("limits lock");
        state.sweep(now);
        let entry = state.principal(principal, self.config.principal_rates(), now);
        if cap > 0 && entry.sessions >= cap {
            self.metrics.record_refusal(Refusal::PrincipalSessions);
            return Err(Refused {
                limit: Refusal::PrincipalSessions,
                retry_after: None,
            });
        }
        entry.sessions += 1;
        Ok(PrincipalSlot {
            limiter: Arc::clone(self),
            principal: principal.to_string(),
            rate: Arc::clone(&entry.rate),
        })
    }

    /// Charge one write of `bytes` to `principal` alone, for a write that
    /// comes without a session.
    pub(crate) fn charge_principal(
        &self,
        principal: &str,
        bytes: usize,
        now: Instant,
    ) -> Result<(), Refused> {
        let rate = {
            let mut state = self.state.lock().expect("limits lock");
            state.sweep(now);
            Arc::clone(
                &state
                    .principal(principal, self.config.principal_rates(), now)
                    .rate,
            )
        };
        let mut rate = rate.lock().expect("rate lock");
        let wait = rate.wait(bytes, now);
        if wait > Duration::ZERO {
            return Err(self.refuse(Refusal::PrincipalRate, wait));
        }
        rate.take(bytes);
        Ok(())
    }

    fn refuse(&self, limit: Refusal, wait: Duration) -> Refused {
        self.metrics.record_refusal(limit);
        Refused {
            limit,
            retry_after: Some(wait),
        }
    }
}

impl Registry {
    fn principal(&mut self, principal: &str, rates: Rates, now: Instant) -> &mut Principal {
        self.principals
            .entry(principal.to_string())
            .or_insert_with(|| Principal {
                sessions: 0,
                rate: Arc::new(Mutex::new(Rate::new(rates, now))),
            })
    }

    /// Forget principals with no sessions whose rate has refilled; they would
    /// start the same way if they came back.
    fn sweep(&mut self, now: Instant) {
        if now.saturating_duration_since(self.swept) < SWEEP_EVERY {
            return;
        }
        self.swept = now;
        self.principals.retain(|_, entry| {
            entry.sessions > 0 || !entry.rate.lock().expect("rate lock").full(now)
        });
    }
}

fn parse_ip(entry: &str) -> Option<IpAddr> {
    entry
        .parse::<IpAddr>()
        .ok()
        .or_else(|| entry.parse::<SocketAddr>().ok().map(|addr| addr.ip()))
}

/// One session counted against a client address, until dropped.
pub(crate) struct IpSlot(Option<(Arc<Limiter>, IpAddr)>);

impl Drop for IpSlot {
    fn drop(&mut self) {
        if let Some((limiter, ip)) = &self.0 {
            let mut state = limiter.state.lock().expect("limits lock");
            if let Some(count) = state.ips.get_mut(ip) {
                *count -= 1;
                if *count == 0 {
                    state.ips.remove(ip);
                }
            }
        }
    }
}

/// One session counted against a principal, until dropped.
pub(crate) struct PrincipalSlot {
    limiter: Arc<Limiter>,
    principal: String,
    rate: Arc<Mutex<Rate>>,
}

impl Drop for PrincipalSlot {
    fn drop(&mut self) {
        let mut state = self.limiter.state.lock().expect("limits lock");
        if let Some(entry) = state.principals.get_mut(&self.principal) {
            entry.sessions -= 1;
        }
    }
}

/// One session's limits: its own rate, a rate per alias that sets one, and
/// its principal's.
pub(crate) struct SessionLimits {
    rate: Rate,
    /// Keyed by kind and alias, for aliases with `writes_per_s`.
    aliases: HashMap<(&'static str, String), Bucket>,
    principal: PrincipalSlot,
    _ip: IpSlot,
}

/// What one write is checked against.
pub(crate) struct WriteCost<'a> {
    pub(crate) kind: &'static str,
    pub(crate) alias: &'a str,
    /// The alias's own rate, as writes a second and burst.
    pub(crate) alias_rate: Option<(u64, u64)>,
    pub(crate) bytes: usize,
}

impl SessionLimits {
    pub(crate) fn new(principal: PrincipalSlot, ip: IpSlot, now: Instant) -> Self {
        let rates = principal.limiter.config.session_rates();
        Self {
            rate: Rate::new(rates, now),
            aliases: HashMap::new(),
            principal,
            _ip: ip,
        }
    }

    pub(crate) fn metrics(&self) -> &Metrics {
        self.principal.limiter.metrics()
    }

    /// Spend one write against every limit it falls under, or none of them
    /// when any refuses.
    pub(crate) fn charge(&mut self, cost: &WriteCost<'_>, now: Instant) -> Result<(), Refused> {
        let session = self.rate.wait(cost.bytes, now);
        let mut alias = cost.alias_rate.and_then(|(rate, burst)| {
            let key = (cost.kind, cost.alias.to_string());
            if !self.aliases.contains_key(&key) {
                self.aliases
                    .insert(key.clone(), Bucket::new(rate, burst, now)?);
            }
            self.aliases.get_mut(&key)
        });
        let alias_wait = alias.as_mut().map_or(Duration::ZERO, |b| b.wait(1.0, now));
        let mut principal = self.principal.rate.lock().expect("rate lock");
        let principal_wait = principal.wait(cost.bytes, now);

        let limiter = &self.principal.limiter;
        for (limit, wait) in [
            (Refusal::SessionRate, session),
            (Refusal::AliasRate, alias_wait),
            (Refusal::PrincipalRate, principal_wait),
        ] {
            if wait > Duration::ZERO {
                let wait = session.max(alias_wait).max(principal_wait);
                return Err(limiter.refuse(limit, wait));
            }
        }
        self.rate.take(cost.bytes);
        if let Some(bucket) = alias {
            bucket.take(1.0);
        }
        principal.take(cost.bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter(config: LimitsConfig) -> Arc<Limiter> {
        Arc::new(Limiter::new(
            config,
            64 * 1024,
            Arc::new(Metrics::new(std::iter::empty())),
        ))
    }

    fn rates(writes_per_s: u64, write_burst: u64) -> RateSpec {
        RateSpec {
            writes_per_s: Some(writes_per_s),
            write_burst: Some(write_burst),
            bytes_per_s: Some(0),
            byte_burst: None,
        }
    }

    fn write(bytes: usize) -> WriteCost<'static> {
        WriteCost {
            kind: "stream",
            alias: "ops",
            alias_rate: None,
            bytes,
        }
    }

    fn session(limiter: &Arc<Limiter>, principal: &str, now: Instant) -> SessionLimits {
        let slot = limiter.admit_principal(principal, now).unwrap();
        SessionLimits::new(slot, limiter.admit_ip(None).unwrap(), now)
    }

    #[test]
    fn a_bucket_allows_its_burst_then_its_rate() {
        let start = Instant::now();
        let mut bucket = Bucket::new(10, 3, start).unwrap();
        for _ in 0..3 {
            assert_eq!(bucket.wait(1.0, start), Duration::ZERO);
            bucket.take(1.0);
        }
        assert_eq!(bucket.wait(1.0, start), Duration::from_millis(100));
        let later = start + Duration::from_millis(100);
        assert_eq!(bucket.wait(1.0, later), Duration::ZERO);
        bucket.take(1.0);
        assert!(bucket.wait(1.0, later) > Duration::ZERO);

        // Idle time refills it to the burst and no further.
        let idle = later + Duration::from_secs(60);
        for _ in 0..3 {
            assert_eq!(bucket.wait(1.0, idle), Duration::ZERO);
            bucket.take(1.0);
        }
        assert!(bucket.wait(1.0, idle) > Duration::ZERO);
        assert!(
            Bucket::new(0, 5, start).is_none(),
            "a rate of 0 is no limit"
        );
    }

    #[test]
    fn a_byte_bucket_says_how_long_a_large_write_waits() {
        let start = Instant::now();
        let mut bytes = Bucket::new(1000, 2000, start).unwrap();
        bytes.take(2000.0);
        assert_eq!(bytes.wait(500.0, start), Duration::from_millis(500));
        assert_eq!(
            bytes.wait(500.0, start + Duration::from_millis(500)),
            Duration::ZERO
        );
    }

    #[test]
    fn a_session_over_its_rate_is_refused_with_when_to_retry() {
        let limiter = limiter(LimitsConfig {
            session: rates(10, 2),
            principal: rates(0, 0),
            ..LimitsConfig::default()
        });
        let start = Instant::now();
        let mut limits = session(&limiter, "ana", start);
        limits.charge(&write(10), start).unwrap();
        limits.charge(&write(10), start).unwrap();
        let refused = limits.charge(&write(10), start).unwrap_err();
        assert_eq!(refused.limit, Refusal::SessionRate);
        assert_eq!(refused.retry_after, Some(Duration::from_millis(100)));
        limits
            .charge(&write(10), start + Duration::from_millis(100))
            .unwrap();
        assert_eq!(
            limiter.metrics().snapshot().limits_refused["session_rate"],
            1
        );
    }

    #[test]
    fn a_principal_rate_is_shared_by_its_sessions() {
        let limiter = limiter(LimitsConfig {
            session: rates(0, 0),
            principal: rates(1, 3),
            ..LimitsConfig::default()
        });
        let now = Instant::now();
        let mut first = session(&limiter, "ana", now);
        let mut second = session(&limiter, "ana", now);
        let mut other = session(&limiter, "ben", now);
        first.charge(&write(1), now).unwrap();
        second.charge(&write(1), now).unwrap();
        first.charge(&write(1), now).unwrap();
        assert_eq!(
            second.charge(&write(1), now).unwrap_err().limit,
            Refusal::PrincipalRate
        );
        other.charge(&write(1), now).unwrap();

        // Closing every session keeps the spent rate.
        drop((first, second));
        let mut again = session(&limiter, "ana", now);
        assert!(again.charge(&write(1), now).is_err());
    }

    #[test]
    fn a_refused_write_spends_nothing() {
        let limiter = limiter(LimitsConfig {
            session: rates(100, 100),
            principal: RateSpec {
                writes_per_s: Some(0),
                bytes_per_s: Some(100),
                byte_burst: Some(100),
                ..RateSpec::default()
            },
            ..LimitsConfig::default()
        });
        let now = Instant::now();
        let mut limits = session(&limiter, "ana", now);
        let mut cost = write(60);
        cost.alias_rate = Some((1, 1));
        limits.charge(&cost, now).unwrap();
        // The alias bucket is empty and the byte bucket too short: both refuse,
        // the wait is the longer one, and the session bucket is not charged.
        let refused = limits.charge(&cost, now).unwrap_err();
        assert_eq!(refused.limit, Refusal::AliasRate);
        assert_eq!(refused.retry_after, Some(Duration::from_secs(1)));
        let other = WriteCost {
            alias: "presence",
            ..write(40)
        };
        limits.charge(&other, now).unwrap();
    }

    #[test]
    fn sessions_are_capped_per_principal_and_per_address() {
        let limiter = limiter(LimitsConfig {
            sessions_per_principal: 2,
            sessions_per_ip: 2,
            ..LimitsConfig::default()
        });
        let now = Instant::now();
        let first = limiter.admit_principal("ana", now).unwrap();
        let _second = limiter.admit_principal("ana", now).unwrap();
        let refused = limiter.admit_principal("ana", now).err().unwrap();
        assert_eq!(
            (refused.limit, refused.retry_after),
            (Refusal::PrincipalSessions, None)
        );
        let _ben = limiter.admit_principal("ben", now).unwrap();
        drop(first);
        let _third = limiter.admit_principal("ana", now).unwrap();

        let ip: IpAddr = "203.0.113.7".parse().unwrap();
        let one = limiter.admit_ip(Some(ip)).unwrap();
        let _two = limiter.admit_ip(Some(ip)).unwrap();
        assert_eq!(
            limiter.admit_ip(Some(ip)).err().unwrap().limit,
            Refusal::IpSessions
        );
        let _elsewhere = limiter
            .admit_ip(Some("203.0.113.8".parse().unwrap()))
            .unwrap();
        let _unknown = limiter.admit_ip(None).unwrap();
        drop(one);
        let _three = limiter.admit_ip(Some(ip)).unwrap();

        let snapshot = limiter.metrics().snapshot();
        assert_eq!(snapshot.limits_refused["principal_sessions"], 1);
        assert_eq!(snapshot.limits_refused["ip_sessions"], 1);
    }

    #[test]
    fn zero_caps_are_no_limit() {
        let limiter = limiter(LimitsConfig {
            sessions_per_principal: 0,
            sessions_per_ip: 0,
            ..LimitsConfig::default()
        });
        let ip = Some("203.0.113.7".parse().unwrap());
        let slots: Vec<_> = (0..100)
            .map(|_| {
                (
                    limiter.admit_principal("ana", Instant::now()).unwrap(),
                    limiter.admit_ip(ip).unwrap(),
                )
            })
            .collect();
        assert_eq!(slots.len(), 100);
    }

    #[test]
    fn the_forwarded_for_header_is_ignored_unless_proxies_are_trusted() {
        let socket = Some("10.0.0.2".parse().unwrap());
        let header = ["198.51.100.1, 203.0.113.9"];
        let default = limiter(LimitsConfig::default());
        assert_eq!(default.client_ip(socket, &header), socket);

        let one = limiter(LimitsConfig {
            trusted_proxies: 1,
            ..LimitsConfig::default()
        });
        // The proxy appends the address it saw; anything left of it the
        // client could have written.
        assert_eq!(
            one.client_ip(socket, &header),
            Some("203.0.113.9".parse().unwrap())
        );
        assert_eq!(
            one.client_ip(socket, &["198.51.100.1", "[2001:db8::1]:443"]),
            Some("2001:db8::1".parse().unwrap())
        );
        assert_eq!(one.client_ip(socket, &[]), socket);
        assert_eq!(one.client_ip(socket, &["unknown"]), socket);

        let two = limiter(LimitsConfig {
            trusted_proxies: 2,
            ..LimitsConfig::default()
        });
        assert_eq!(
            two.client_ip(socket, &header),
            Some("198.51.100.1".parse().unwrap())
        );
        assert_eq!(two.client_ip(socket, &["203.0.113.9"]), socket);
    }

    #[test]
    fn idle_principals_are_forgotten_once_refilled() {
        let limiter = limiter(LimitsConfig {
            principal: RateSpec {
                writes_per_s: Some(0),
                bytes_per_s: Some(1),
                byte_burst: Some(1000),
                ..RateSpec::default()
            },
            ..LimitsConfig::default()
        });
        let start = Instant::now();
        let mut limits = session(&limiter, "ana", start);
        limits.charge(&write(1000), start).unwrap();
        drop(limits);
        let count = || limiter.state.lock().unwrap().principals.len();
        let _ben = limiter.admit_principal("ben", start + SWEEP_EVERY).unwrap();
        assert_eq!(count(), 2, "ana's rate has not refilled, so she is kept");
        let refilled = start + Duration::from_secs(1000) + SWEEP_EVERY;
        limiter.charge_principal("cy", 0, refilled).unwrap();
        assert_eq!(count(), 2, "ana is gone; ben and cy remain");
    }

    #[test]
    fn a_burst_smaller_than_the_largest_payload_is_refused() {
        let config = LimitsConfig {
            session: RateSpec {
                byte_burst: Some(1000),
                ..RateSpec::default()
            },
            ..LimitsConfig::default()
        };
        assert!(config.check(1000).is_ok());
        assert!(config.check(1001).is_err());
        let off = LimitsConfig {
            session: RateSpec {
                bytes_per_s: Some(0),
                byte_burst: Some(1),
                ..RateSpec::default()
            },
            ..LimitsConfig::default()
        };
        assert!(off.check(1001).is_ok());
        let zero = LimitsConfig {
            max_payload_bytes: 0,
            ..LimitsConfig::default()
        };
        assert!(zero.check(1).is_err());
    }
}
