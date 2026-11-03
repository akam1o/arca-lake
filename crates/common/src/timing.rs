//! Fixed-bucket latency histograms for phase timing (lock-free, cheap to
//! observe, rendered in the Prometheus text format by the binaries).

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

/// Upper bounds of the buckets, in microseconds; the last bucket is open.
pub const BUCKET_BOUNDS_US: [u64; 12] = [
    1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000,
    5_000_000, 10_000_000,
];

const BUCKETS: usize = BUCKET_BOUNDS_US.len() + 1;

/// Upper bound of bucket `i` in microseconds (the open last bucket
/// reports twice the largest bound).
#[must_use]
pub fn bucket_upper_us(i: usize) -> u64 {
    BUCKET_BOUNDS_US
        .get(i)
        .copied()
        .unwrap_or(BUCKET_BOUNDS_US[BUCKET_BOUNDS_US.len() - 1] * 2)
}

/// A histogram of durations: cumulative bucket counts plus sum, count and
/// the largest value seen.
#[derive(Debug, Default)]
pub struct Histogram {
    buckets: [AtomicU64; BUCKETS],
    sum_ns: AtomicU64,
    count: AtomicU64,
    max_ns: AtomicU64,
}

/// A copy of a [`Histogram`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HistogramSnapshot {
    /// Count per bucket (not cumulative), last bucket open.
    pub buckets: [u64; BUCKETS],
    /// Sum of observed durations, nanoseconds.
    pub sum_ns: u64,
    /// Number of observations.
    pub count: u64,
    /// Largest observation, nanoseconds.
    pub max_ns: u64,
}

impl Histogram {
    /// Record one duration.
    pub fn observe(&self, d: Duration) {
        let us = u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
        let idx = BUCKET_BOUNDS_US
            .iter()
            .position(|&b| us <= b)
            .unwrap_or(BUCKETS - 1);
        self.buckets[idx].fetch_add(1, Relaxed);
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.sum_ns.fetch_add(ns, Relaxed);
        self.count.fetch_add(1, Relaxed);
        self.max_ns.fetch_max(ns, Relaxed);
    }

    /// Copy the counters.
    #[must_use]
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut buckets = [0u64; BUCKETS];
        for (i, b) in self.buckets.iter().enumerate() {
            buckets[i] = b.load(Relaxed);
        }
        HistogramSnapshot {
            buckets,
            sum_ns: self.sum_ns.load(Relaxed),
            count: self.count.load(Relaxed),
            max_ns: self.max_ns.load(Relaxed),
        }
    }
}

impl HistogramSnapshot {
    /// Append the Prometheus text representation: `<name>_bucket{<labels>,le="…"}`
    /// (cumulative, seconds), `<name>_sum` and `<name>_count`. `labels` is
    /// `key="value"` pairs or empty. The maximum is a separate gauge family
    /// ([`Self::render_max`]): a histogram family has no other series.
    #[allow(clippy::cast_precision_loss)]
    pub fn render_prometheus(&self, out: &mut String, name: &str, labels: &str) {
        use std::fmt::Write as _;
        let sep = if labels.is_empty() { "" } else { "," };
        let mut cumulative = 0u64;
        for (i, count) in self.buckets.iter().enumerate() {
            cumulative += count;
            let le = BUCKET_BOUNDS_US
                .get(i)
                .map_or_else(|| "+Inf".to_string(), |b| format!("{}", *b as f64 / 1e6));
            let _ = writeln!(
                out,
                "{name}_bucket{{{labels}{sep}le=\"{le}\"}} {cumulative}"
            );
        }
        let lab = if labels.is_empty() {
            String::new()
        } else {
            format!("{{{labels}}}")
        };
        let _ = writeln!(out, "{name}_sum{lab} {}", self.sum_ns as f64 / 1e9);
        let _ = writeln!(out, "{name}_count{lab} {}", self.count);
    }

    /// Append `<name>{<labels>} <max seconds>` (a gauge family the caller
    /// declares).
    #[allow(clippy::cast_precision_loss)]
    pub fn render_max(&self, out: &mut String, name: &str, labels: &str) {
        use std::fmt::Write as _;
        let lab = if labels.is_empty() {
            String::new()
        } else {
            format!("{{{labels}}}")
        };
        let _ = writeln!(out, "{name}{lab} {}", self.max_ns as f64 / 1e9);
    }

    /// Mean observation in seconds (0 when empty).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn mean_seconds(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_ns as f64 / 1e9 / self.count as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_sum_count_and_max() {
        let h = Histogram::default();
        h.observe(Duration::from_micros(500));
        h.observe(Duration::from_millis(30));
        h.observe(Duration::from_secs(20));
        let s = h.snapshot();
        assert_eq!(s.count, 3);
        assert_eq!(s.buckets[0], 1, "500 µs is in the first bucket");
        assert_eq!(s.buckets[4], 1, "30 ms is in the 50 ms bucket");
        assert_eq!(s.buckets[BUCKETS - 1], 1);
        assert_eq!(s.max_ns, 20_000_000_000);
        let mut out = String::new();
        s.render_prometheus(&mut out, "x", "phase=\"p\"");
        assert!(out.contains("x_bucket{phase=\"p\",le=\"0.001\"} 1"));
        assert!(out.contains("x_bucket{phase=\"p\",le=\"+Inf\"} 3"));
        assert!(out.contains("x_count{phase=\"p\"} 3"));
    }
}
