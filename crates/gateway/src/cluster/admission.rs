//! Write admission of a gateway (Phase 3.6, R-64): the controller leader's
//! admission record (`arca_lake_metadata::ops::admission`), refreshed with
//! the cluster state, decides what new client data this gateway takes.
//!
//! - `normal`, `warning`: admitted.
//! - `throttled`: client data at the tier's rate split over the gateways
//!   (a token bucket that may go into debt by one request, so that objects
//!   larger than the burst still pass at the average rate).
//! - `write_rejected`, `recovery_only`: new client data refused. Deletes,
//!   aborts, multipart completion and bucket operations add no data and
//!   stay admitted; reads are never gated here.
//!
//! A record that stops changing is trusted less: after its TTL the gateway
//! acts as if at least `throttled`, after [`STALE_REJECT`] as if
//! `write_rejected` (fail closed while no leader evaluates capacity). No
//! record at all (a controller that predates Phase 3.6) admits: the storage
//! nodes' reserve still refuses writes that would cross it.
//!
//! Refusals are `503 SlowDown` with `Retry-After`, the S3 code SDKs retry
//! with backoff; the message says why.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use arca_lake_metadata::ops::admission::AdmissionRecord;
use s3s::S3Error;

/// Admission states (the record's `state` codes).
pub const NORMAL: u8 = 0;
/// See [`NORMAL`].
pub const WARNING: u8 = 1;
/// See [`NORMAL`].
pub const THROTTLED: u8 = 2;
/// See [`NORMAL`].
pub const WRITE_REJECTED: u8 = 3;
/// See [`NORMAL`].
pub const RECOVERY_ONLY: u8 = 4;

/// A record unchanged for this long is treated as `write_rejected`.
pub const STALE_REJECT: Duration = Duration::from_mins(5);

/// Throttled rate of a stale record, per gateway (bytes/s): slow, but
/// enough for clients to make progress until the leader is back.
const STALE_RATE: f64 = 4.0 * 1024.0 * 1024.0;

/// Seconds of rate the bucket holds at most.
const BURST_SECS: f64 = 2.0;

#[derive(Debug, Default)]
struct Seen {
    record: Option<AdmissionRecord>,
    /// When `record.computed_ms` last changed (this gateway's clock).
    changed: Option<Instant>,
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    at: Instant,
}

/// What admission currently applies to this gateway's tier.
#[derive(Debug, Clone, PartialEq)]
pub struct Effective {
    /// State code.
    pub state: u8,
    /// This gateway's share of the throttled rate, bytes/s.
    pub rate_bytes_per_s: f64,
    /// Groups not to place chunks in.
    pub excluded_groups: Vec<u64>,
    /// Why (record reason, or staleness).
    pub reason: String,
}

/// Counters of [`WriteAdmission`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdmissionStatsSnapshot {
    /// State applied now (code).
    pub state: u8,
    /// Seconds since the record last changed (0 = no record).
    pub record_age_secs: u64,
    /// Requests refused by the throttle.
    pub throttled: u64,
    /// Requests refused by `write_rejected` / `recovery_only`.
    pub rejected: u64,
    /// Shard writes the nodes refused for capacity.
    pub node_capacity_refusals: u64,
    /// Chunks that no group could take for capacity (client saw
    /// `SlowDown`).
    pub placement_capacity_failures: u64,
}

/// Write admission state of a gateway.
#[derive(Debug)]
pub struct WriteAdmission {
    tier: String,
    seen: Mutex<Seen>,
    bucket: Mutex<Option<Bucket>>,
    throttled: AtomicU64,
    rejected: AtomicU64,
    /// Shard writes refused by nodes for capacity.
    pub node_capacity_refusals: AtomicU64,
    /// Chunks no group could take for capacity.
    pub placement_capacity_failures: AtomicU64,
}

impl WriteAdmission {
    /// Admission for client writes into `tier`.
    #[must_use]
    pub fn new(tier: impl Into<String>) -> Self {
        Self {
            tier: tier.into(),
            seen: Mutex::new(Seen::default()),
            bucket: Mutex::new(None),
            throttled: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            node_capacity_refusals: AtomicU64::new(0),
            placement_capacity_failures: AtomicU64::new(0),
        }
    }

    /// The record as just read (`None`: none published).
    pub fn observe(&self, record: Option<AdmissionRecord>, now: Instant) {
        let mut s = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let changed = match (&s.record, &record) {
            (Some(a), Some(b)) => a.computed_ms != b.computed_ms,
            (None, None) => false,
            _ => true,
        };
        if changed {
            s.changed = record.as_ref().map(|_| now);
        }
        s.record = record;
    }

    /// What applies now.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn effective(&self, now: Instant) -> Effective {
        let s = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(rec) = s.record.as_ref() else {
            return Effective {
                state: NORMAL,
                rate_bytes_per_s: 0.0,
                excluded_groups: Vec::new(),
                reason: "no admission record".into(),
            };
        };
        let age = s
            .changed
            .map_or(Duration::ZERO, |t| now.saturating_duration_since(t));
        let tier = rec.tier(&self.tier);
        let mut e = Effective {
            state: tier.map_or(NORMAL, |t| t.state),
            rate_bytes_per_s: tier.map_or(0.0, |t| {
                t.rate_bytes_per_s as f64 / f64::from(rec.gateways.max(1))
            }),
            excluded_groups: tier.map(|t| t.excluded_groups.clone()).unwrap_or_default(),
            reason: tier.map_or_else(String::new, |t| t.reason.clone()),
        };
        if age >= STALE_REJECT {
            if e.state < WRITE_REJECTED {
                e.state = WRITE_REJECTED;
                e.reason = format!(
                    "admission record unchanged for {} s (no controller leader evaluates capacity)",
                    age.as_secs()
                );
            }
        } else if age >= Duration::from_millis(rec.ttl_ms.max(1)) && e.state < THROTTLED {
            e.state = THROTTLED;
            e.rate_bytes_per_s = STALE_RATE;
            e.reason = format!("admission record unchanged for {} s", age.as_secs());
        }
        e
    }

    /// Groups of the tier not to place new chunks in.
    #[must_use]
    pub fn excluded_groups(&self) -> Vec<u64> {
        self.effective(Instant::now()).excluded_groups
    }

    /// Admit `bytes` of new client data (PUT, UploadPart, copies).
    ///
    /// # Errors
    ///
    /// `SlowDown` with `Retry-After` when throttled past the rate or when
    /// new data is refused.
    pub fn admit_data(&self, bytes: u64) -> Result<(), S3Error> {
        self.admit_data_at(bytes, Instant::now())
    }

    /// [`Self::admit_data`] at `now`.
    ///
    /// # Errors
    ///
    /// As [`Self::admit_data`].
    #[allow(clippy::cast_precision_loss)]
    pub fn admit_data_at(&self, bytes: u64, now: Instant) -> Result<(), S3Error> {
        let e = self.effective(now);
        match e.state {
            WRITE_REJECTED | RECOVERY_ONLY => {
                self.rejected.fetch_add(1, Relaxed);
                Err(slow_down(
                    &format!(
                        "new data refused: tier {} is {} ({})",
                        self.tier,
                        name(e.state),
                        e.reason
                    ),
                    if e.state == RECOVERY_ONLY { 60 } else { 30 },
                ))
            }
            THROTTLED => {
                let rate = e.rate_bytes_per_s.max(1.0);
                let mut g = self.bucket.lock().unwrap_or_else(PoisonError::into_inner);
                // A throttle starts empty: the first requests after a
                // recovery do not arrive as one burst.
                let b = g.get_or_insert(Bucket {
                    tokens: 0.0,
                    at: now,
                });
                let dt = now.saturating_duration_since(b.at).as_secs_f64();
                b.tokens = (b.tokens + dt * rate).min(rate * BURST_SECS);
                b.at = now;
                if b.tokens <= 0.0 {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let wait = (-b.tokens / rate).ceil().clamp(1.0, 30.0) as u64;
                    drop(g);
                    self.throttled.fetch_add(1, Relaxed);
                    return Err(slow_down(
                        &format!(
                            "write rate limited: tier {} is throttled ({})",
                            self.tier, e.reason
                        ),
                        wait,
                    ));
                }
                b.tokens -= bytes as f64;
                Ok(())
            }
            _ => {
                *self.bucket.lock().unwrap_or_else(PoisonError::into_inner) = None;
                Ok(())
            }
        }
    }

    /// Admit an operation that adds no data but starts new data (multipart
    /// creation): refused when new data is.
    ///
    /// # Errors
    ///
    /// `SlowDown` in `write_rejected` / `recovery_only`.
    pub fn admit_start(&self) -> Result<(), S3Error> {
        let e = self.effective(Instant::now());
        if e.state >= WRITE_REJECTED {
            self.rejected.fetch_add(1, Relaxed);
            return Err(slow_down(
                &format!(
                    "new uploads refused: tier {} is {} ({})",
                    self.tier,
                    name(e.state),
                    e.reason
                ),
                30,
            ));
        }
        Ok(())
    }

    /// Counters.
    #[must_use]
    pub fn snapshot(&self) -> AdmissionStatsSnapshot {
        let now = Instant::now();
        let age = self
            .seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .changed
            .map_or(0, |t| now.saturating_duration_since(t).as_secs());
        AdmissionStatsSnapshot {
            state: self.effective(now).state,
            record_age_secs: age,
            throttled: self.throttled.load(Relaxed),
            rejected: self.rejected.load(Relaxed),
            node_capacity_refusals: self.node_capacity_refusals.load(Relaxed),
            placement_capacity_failures: self.placement_capacity_failures.load(Relaxed),
        }
    }
}

/// State name.
#[must_use]
pub fn name(state: u8) -> &'static str {
    match state {
        NORMAL => "normal",
        WARNING => "warning",
        THROTTLED => "throttled",
        WRITE_REJECTED => "write_rejected",
        _ => "recovery_only",
    }
}

/// `503 SlowDown` with `Retry-After` (seconds, with up to 25 % jitter so
/// that refused clients do not all return at once).
#[must_use]
pub fn slow_down(message: &str, retry_after_secs: u64) -> S3Error {
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()))
        % (retry_after_secs / 4 + 1);
    let mut e = S3Error::with_message(s3s::S3ErrorCode::SlowDown, message.to_string());
    let mut h = http::HeaderMap::new();
    if let Ok(v) = http::HeaderValue::from_str(&(retry_after_secs + jitter).to_string()) {
        h.insert(http::header::RETRY_AFTER, v);
    }
    e.set_headers(h);
    e
}

#[cfg(test)]
mod tests {
    use super::*;
    use arca_lake_metadata::ops::admission::TierAdmission;

    const MIB: u64 = 1 << 20;

    fn record(computed_ms: u64, state: u8, rate: u64, gateways: u32) -> AdmissionRecord {
        AdmissionRecord {
            computed_ms,
            ttl_ms: 30_000,
            gateways,
            tiers: vec![TierAdmission {
                tier: "durable".into(),
                state,
                rate_bytes_per_s: rate,
                excluded_groups: vec![7],
                free_permille: 100,
                reason: "test".into(),
            }],
        }
    }

    fn retry_after(e: &S3Error) -> u64 {
        e.headers()
            .and_then(|h| h.get(http::header::RETRY_AFTER))
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap()
    }

    #[test]
    fn no_record_admits_and_rejected_refuses_data_but_not_its_absence() {
        let a = WriteAdmission::new("durable");
        assert!(a.admit_data(64 * MIB).is_ok());
        let t0 = Instant::now();
        a.observe(Some(record(1, WRITE_REJECTED, 0, 1)), t0);
        let e = a.admit_data_at(1, t0).unwrap_err();
        assert_eq!(*e.code(), s3s::S3ErrorCode::SlowDown);
        assert!(retry_after(&e) >= 30);
        assert!(a.admit_start().is_err());
        assert_eq!(a.excluded_groups(), vec![7]);
        assert_eq!(a.snapshot().rejected, 2);
    }

    #[test]
    fn throttle_holds_the_average_rate_split_over_gateways() {
        let a = WriteAdmission::new("durable");
        let t0 = Instant::now();
        // 8 MiB/s over 2 gateways: 4 MiB/s here.
        a.observe(Some(record(1, THROTTLED, 8 * MIB, 2)), t0);
        let mut admitted = 0u64;
        let mut refused = 0;
        for ms in (0..10_000u64).step_by(10) {
            match a.admit_data_at(MIB, t0 + Duration::from_millis(ms)) {
                Ok(()) => admitted += MIB,
                Err(e) => {
                    refused += 1;
                    assert!(retry_after(&e) >= 1);
                }
            }
        }
        // 10 s at 4 MiB/s, plus at most one request of debt.
        assert!((38 * MIB..=41 * MIB).contains(&admitted), "{admitted}");
        assert!(refused > 0);
        // An object larger than the burst still passes, then waits.
        let t1 = t0 + Duration::from_secs(20);
        assert!(a.admit_data_at(64 * MIB, t1).is_ok());
        let e = a
            .admit_data_at(MIB, t1 + Duration::from_secs(1))
            .unwrap_err();
        assert!(retry_after(&e) >= 10, "debt of ~56 MiB at 4 MiB/s");
    }

    #[test]
    fn a_record_that_stops_changing_throttles_then_rejects() {
        let a = WriteAdmission::new("durable");
        let t0 = Instant::now();
        a.observe(Some(record(1, NORMAL, 0, 1)), t0);
        assert_eq!(a.effective(t0).state, NORMAL);
        // Refreshed but unchanged (the leader stopped publishing).
        a.observe(Some(record(1, NORMAL, 0, 1)), t0 + Duration::from_secs(20));
        assert_eq!(a.effective(t0 + Duration::from_secs(31)).state, THROTTLED);
        assert_eq!(a.effective(t0 + STALE_REJECT).state, WRITE_REJECTED);
        // A new computation: trusted again.
        let t1 = t0 + STALE_REJECT + Duration::from_secs(1);
        a.observe(Some(record(2, NORMAL, 0, 1)), t1);
        assert_eq!(a.effective(t1).state, NORMAL);
    }

    #[test]
    fn other_tiers_are_not_affected() {
        let a = WriteAdmission::new("archive");
        let t0 = Instant::now();
        a.observe(Some(record(1, RECOVERY_ONLY, 0, 1)), t0);
        assert!(a.admit_data_at(MIB, t0).is_ok());
    }
}
