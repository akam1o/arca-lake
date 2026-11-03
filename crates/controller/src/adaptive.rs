//! Adaptive scrub budget (Phase 3.4 §5, R-59, ADR-0018): AIMD on the byte
//! rate from the load the scrubbed node reports.
//!
//! - Pressure: the device is busy (utilisation or queue depth over its
//!   target) or clients are slow (GET or PUT p99 over its target) for
//!   `decrease_after` consecutive samples: the rate halves (never below
//!   `min`: scrubbing slows, it never stops).
//! - Calm for `increase_after` consecutive samples: the rate grows by
//!   `step` (never above the ceiling). Requiring several samples in a row
//!   either way, and halving but growing linearly, keeps it from
//!   oscillating on noisy shared storage.
//! - Stripe repair moving bytes lowers the ceiling to half of `max`:
//!   repair has priority over verification.
//! - No signal (an older node, an unreachable one): the fixed base rate,
//!   exactly as without adaptation.
//!
//! The state is per pass and in memory: after a leader failover the new
//! leader starts at the base rate from the persisted scrub cursor.

/// Tunables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptivePolicy {
    /// Rate without signals and at the start, bytes/s.
    pub base: u64,
    /// Floor, bytes/s (> 0).
    pub min: u64,
    /// Ceiling, bytes/s.
    pub max: u64,
    /// Additive step per increase, bytes/s.
    pub step: u64,
    /// Device utilisation target, permille.
    pub util_permille: u32,
    /// Device queue depth target, thousandths.
    pub queue_depth_milli: u32,
    /// Client GET p99 target, µs.
    pub get_p99_us: u64,
    /// Client PUT p99 target, µs.
    pub put_p99_us: u64,
    /// Consecutive pressured samples before a decrease.
    pub decrease_after: u32,
    /// Consecutive calm samples before an increase.
    pub increase_after: u32,
}

impl AdaptivePolicy {
    /// Defaults around a base rate.
    #[must_use]
    pub fn around(base: u64) -> Self {
        Self {
            base,
            min: (1 << 20).min(base.max(1)),
            max: base.saturating_mul(4).max(base),
            step: (4 << 20).min(base.max(1)),
            util_permille: 850,
            queue_depth_milli: 4_000,
            get_p99_us: 50_000,
            put_p99_us: 250_000,
            decrease_after: 2,
            increase_after: 3,
        }
    }
}

/// One sample of a node's load (from `Health`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Signals {
    /// Client GET p99, µs (0 = no read in the window).
    pub get_p99_us: u64,
    /// Client PUT p99, µs.
    pub put_p99_us: u64,
    /// Device utilisation and queue depth, when known.
    pub device: Option<(u32, u32)>,
}

impl Signals {
    /// From a `Health` answer; `None` when the node does not report load.
    #[must_use]
    pub fn from_health(h: &arca_lake_protocol::storage::v1::HealthResponse) -> Option<Self> {
        h.load_reported.then_some(Self {
            get_p99_us: h.load_get_p99_us,
            put_p99_us: h.load_put_p99_us,
            device: h
                .load_device
                .then_some((h.load_device_util_permille, h.load_device_queue_depth_milli)),
        })
    }
}

/// The controller.
#[derive(Debug, Clone)]
pub struct Aimd {
    policy: AdaptivePolicy,
    rate: u64,
    pressured: u32,
    calm: u32,
    /// Decreases so far.
    pub throttles: u64,
}

impl Aimd {
    /// Start at the base rate.
    #[must_use]
    pub fn new(policy: AdaptivePolicy) -> Self {
        Self {
            rate: policy.base,
            policy,
            pressured: 0,
            calm: 0,
            throttles: 0,
        }
    }

    /// Current rate, bytes/s.
    #[must_use]
    pub fn rate(&self) -> u64 {
        self.rate
    }

    /// Whether `s` shows pressure.
    #[must_use]
    pub fn pressure(&self, s: &Signals) -> bool {
        let p = &self.policy;
        s.get_p99_us >= p.get_p99_us
            || s.put_p99_us >= p.put_p99_us
            || s.device
                .is_some_and(|(u, q)| u >= p.util_permille || q >= p.queue_depth_milli)
    }

    /// Take one sample (`None` = no signal) and return the new rate.
    pub fn observe(&mut self, s: Option<Signals>, repair_active: bool) -> u64 {
        let p = self.policy;
        let ceiling = if repair_active {
            (p.max / 2).max(p.min)
        } else {
            p.max
        };
        let Some(s) = s else {
            // Nothing to adapt to: the fixed rate, within the ceiling.
            self.pressured = 0;
            self.calm = 0;
            self.rate = p.base.min(ceiling).max(p.min);
            return self.rate;
        };
        if self.pressure(&s) {
            self.calm = 0;
            self.pressured += 1;
            if self.pressured >= p.decrease_after {
                self.pressured = 0;
                let next = (self.rate / 2).max(p.min);
                if next < self.rate {
                    self.throttles += 1;
                }
                self.rate = next;
            }
        } else {
            self.pressured = 0;
            self.calm += 1;
            if self.calm >= p.increase_after {
                self.calm = 0;
                self.rate = self.rate.saturating_add(p.step);
            }
        }
        self.rate = self.rate.min(ceiling).max(p.min);
        self.rate
    }
}

#[cfg(test)]
#[allow(clippy::unnecessary_wraps)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;

    fn busy() -> Option<Signals> {
        Some(Signals {
            get_p99_us: 120_000,
            put_p99_us: 0,
            device: Some((950, 6_000)),
        })
    }

    fn calm() -> Option<Signals> {
        Some(Signals {
            get_p99_us: 5_000,
            put_p99_us: 20_000,
            device: Some((300, 500)),
        })
    }

    #[test]
    fn slows_under_load_recovers_slowly_never_stops_and_respects_bounds() {
        let p = AdaptivePolicy::around(32 * MIB);
        let mut a = Aimd::new(p);
        assert_eq!(a.rate(), 32 * MIB);
        // One busy sample is not enough (hysteresis); two halve the rate.
        assert_eq!(a.observe(busy(), false), 32 * MIB);
        assert_eq!(a.observe(busy(), false), 16 * MIB);
        // Sustained load: halves down to the floor, never to zero.
        for _ in 0..40 {
            a.observe(busy(), false);
        }
        assert_eq!(a.rate(), p.min);
        assert!(a.rate() > 0);
        // Calm: grows by the step every three samples, up to the ceiling.
        let floor = a.rate();
        for _ in 0..3 {
            a.observe(calm(), false);
        }
        assert_eq!(a.rate(), floor + p.step);
        for _ in 0..300 {
            a.observe(calm(), false);
        }
        assert_eq!(a.rate(), p.max);
        // Repair moving bytes: the ceiling halves at once.
        assert_eq!(a.observe(calm(), true), p.max / 2);
        // Alternating busy/calm samples do not move it (no oscillation).
        let r = a.rate();
        for i in 0..20 {
            a.observe(if i % 2 == 0 { busy() } else { calm() }, true);
        }
        assert_eq!(a.rate(), r);
    }

    #[test]
    fn missing_signals_mean_the_fixed_rate() {
        let p = AdaptivePolicy::around(32 * MIB);
        let mut a = Aimd::new(p);
        for _ in 0..10 {
            a.observe(calm(), false);
        }
        assert!(a.rate() > p.base);
        assert_eq!(a.observe(None, false), p.base);
        // A node with only latency signals (no device statistics) still
        // throttles on slow clients.
        let lat_only = Some(Signals {
            get_p99_us: 200_000,
            put_p99_us: 0,
            device: None,
        });
        a.observe(lat_only, false);
        assert_eq!(a.observe(lat_only, false), p.base / 2);
    }
}
