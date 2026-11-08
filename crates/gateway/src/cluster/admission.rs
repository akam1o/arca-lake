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
//! `write_rejected` (fail closed while no leader evaluates capacity).
//! A record already old by this gateway's clock when first seen (beyond
//! its TTL and a skew allowance) starts aged. No
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

/// Longest a chunk waits for its share of a throttled rate; beyond it the
/// request is refused with `SlowDown` (bounded wait, bounded buffering).
pub const MAX_CHUNK_WAIT: Duration = Duration::from_secs(2);

/// Seconds of rate the bucket holds at most.
const BURST_SECS: f64 = 2.0;

/// A record computed longer ago than its TTL plus this (by this gateway's
/// wall clock) is already aged when first seen: a gateway that starts
/// while no controller publishes does not trust an old record afresh.
/// Clock skew beyond it makes a fresh record look old (fail closed).
const SKEW_ALLOWANCE: Duration = Duration::from_secs(60);

#[derive(Debug, Default)]
struct Seen {
    record: Option<AdmissionRecord>,
    /// Leader token of the record taken last (Phase 3.7): a record of a
    /// lower one (a former leader's) is not taken, unless it was computed
    /// clearly later (metadata restored to an older leader counter).
    token: u64,
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
    /// Client bytes charged chunk by chunk.
    pub charged_bytes: u64,
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
    /// Client bytes charged chunk by chunk (accepted for placement).
    pub charged_bytes: AtomicU64,
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
            charged_bytes: AtomicU64::new(0),
        }
    }

    /// The record as just read (`None`: none published).
    pub fn observe(&self, record: Option<AdmissionRecord>, now: Instant) {
        let wall_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        self.observe_at(record, now, wall_ms);
    }

    /// [`Self::observe`] with this gateway's wall clock `wall_ms`.
    pub fn observe_at(&self, record: Option<AdmissionRecord>, now: Instant, wall_ms: u64) {
        let mut s = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(r) = &record {
            let last_ms = s.record.as_ref().map_or(0, |l| l.computed_ms);
            let clearly_later = r.computed_ms
                > last_ms.saturating_add(u64::try_from(SKEW_ALLOWANCE.as_millis()).unwrap_or(0));
            if r.leader_token < s.token && !clearly_later {
                // Metadata fencing makes this impossible for a published
                // record; kept as defence in depth.
                return;
            }
            s.token = r.leader_token;
        }
        let changed = match (&s.record, &record) {
            (Some(a), Some(b)) => a.computed_ms != b.computed_ms,
            (None, None) => false,
            _ => true,
        };
        if changed {
            s.changed = record.as_ref().map(|r| {
                // Age already accrued before this gateway saw it.
                let age = Duration::from_millis(wall_ms.saturating_sub(r.computed_ms));
                let trusted = Duration::from_millis(r.ttl_ms) + SKEW_ALLOWANCE;
                now.checked_sub(age.saturating_sub(trusted)).unwrap_or(now)
            });
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

    /// Room of the tier's groups as last published (Phase 3.7
    /// capacity-aware placement); empty without a record.
    #[must_use]
    pub fn group_room(&self) -> std::collections::HashMap<u64, u64> {
        let s = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        s.record
            .as_ref()
            .and_then(|r| r.tier(&self.tier))
            .map(|t| t.group_room.iter().map(|g| (g.group, g.room)).collect())
            .unwrap_or_default()
    }

    /// Header-time check of a request that will write about `hint` bytes
    /// of new client data (PUT, UploadPart, copies; `hint` is the content
    /// length or one chunk): refused at once when new data is refused, or
    /// when throttled so far into debt that its first chunk could not wait
    /// for its share. Charges nothing: the chunks are charged as they are
    /// placed ([`Self::reserve_chunk`]).
    ///
    /// # Errors
    ///
    /// `SlowDown` with `Retry-After`.
    pub fn admit_data(&self, hint: u64) -> Result<(), S3Error> {
        self.admit_data_at(hint, Instant::now())
    }

    /// [`Self::admit_data`] at `now`.
    ///
    /// # Errors
    ///
    /// As [`Self::admit_data`].
    pub fn admit_data_at(&self, hint: u64, now: Instant) -> Result<(), S3Error> {
        let _ = hint;
        let e = self.effective(now);
        match e.state {
            WRITE_REJECTED | RECOVERY_ONLY => Err(self.refuse_data(&e)),
            THROTTLED => {
                let wait = self.wait_for(&e, now, None);
                if wait > MAX_CHUNK_WAIT {
                    return Err(self.refuse_rate(&e, wait));
                }
                Ok(())
            }
            _ => {
                *self.bucket.lock().unwrap_or_else(PoisonError::into_inner) = None;
                Ok(())
            }
        }
    }

    /// Charge one chunk of `bytes` about to be placed (every client write
    /// path, whatever its length was known to be: the bytes are counted as
    /// they arrive, once per chunk, before its shards are written; a
    /// placement retried on another group is not charged again). Returns
    /// how long to wait before placing it (at most [`MAX_CHUNK_WAIT`]: a
    /// bounded wait, never an unbounded buffer).
    ///
    /// # Errors
    ///
    /// `SlowDown` when new data is refused or the wait would be longer.
    pub fn reserve_chunk(&self, bytes: u64) -> Result<Duration, S3Error> {
        self.reserve_chunk_at(bytes, Instant::now())
    }

    /// [`Self::reserve_chunk`] at `now`.
    ///
    /// # Errors
    ///
    /// As [`Self::reserve_chunk`].
    pub fn reserve_chunk_at(&self, bytes: u64, now: Instant) -> Result<Duration, S3Error> {
        let e = self.effective(now);
        match e.state {
            WRITE_REJECTED | RECOVERY_ONLY => Err(self.refuse_data(&e)),
            THROTTLED => {
                let wait = self.wait_for(&e, now, Some(bytes));
                if wait > MAX_CHUNK_WAIT {
                    return Err(self.refuse_rate(&e, wait));
                }
                self.charged_bytes.fetch_add(bytes, Relaxed);
                Ok(wait)
            }
            _ => {
                *self.bucket.lock().unwrap_or_else(PoisonError::into_inner) = None;
                self.charged_bytes.fetch_add(bytes, Relaxed);
                Ok(Duration::ZERO)
            }
        }
    }

    /// The tier's rate for this gateway.
    fn rate_of(e: &Effective) -> f64 {
        // A record without a rate (older controller): the floor.
        e.rate_bytes_per_s.max(f64::from(1u32 << 20))
    }

    /// Refill the bucket to `now`; the wait until it is out of debt; with
    /// `take`, charge those bytes when the wait is acceptable.
    #[allow(clippy::cast_precision_loss)]
    fn wait_for(&self, e: &Effective, now: Instant, take: Option<u64>) -> Duration {
        let rate = Self::rate_of(e);
        let mut g = self.bucket.lock().unwrap_or_else(PoisonError::into_inner);
        // A throttle starts empty: the first requests after a recovery do
        // not arrive as one burst.
        let b = g.get_or_insert(Bucket {
            tokens: 0.0,
            at: now,
        });
        let dt = now.saturating_duration_since(b.at).as_secs_f64();
        b.tokens = (b.tokens + dt * rate).min(rate * BURST_SECS);
        b.at = now;
        let wait = Duration::from_secs_f64((-b.tokens / rate).max(0.0));
        if let Some(bytes) = take
            && wait <= MAX_CHUNK_WAIT
        {
            // The whole chunk is owed (no cap: a chunk is taken only when
            // the debt is within MAX_CHUNK_WAIT, so the debt stays below
            // that much rate plus one chunk).
            b.tokens -= bytes as f64;
        }
        wait
    }

    fn refuse_data(&self, e: &Effective) -> S3Error {
        self.rejected.fetch_add(1, Relaxed);
        slow_down(
            &format!(
                "new data refused: tier {} is {} ({})",
                self.tier,
                name(e.state),
                e.reason
            ),
            if e.state == RECOVERY_ONLY { 60 } else { 30 },
        )
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn refuse_rate(&self, e: &Effective, wait: Duration) -> S3Error {
        self.throttled.fetch_add(1, Relaxed);
        slow_down(
            &format!(
                "write rate limited: tier {} is throttled ({})",
                self.tier, e.reason
            ),
            wait.as_secs_f64().ceil().clamp(1.0, 30.0) as u64,
        )
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
            charged_bytes: self.charged_bytes.load(Relaxed),
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
                group_room: Vec::new(),
            }],
            ..Default::default()
        }
    }

    /// Seen at `now`, just computed (by the record's own clock).
    fn seen(a: &WriteAdmission, r: AdmissionRecord, now: Instant) {
        let wall = r.computed_ms;
        a.observe_at(Some(r), now, wall);
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
        seen(&a, record(1, WRITE_REJECTED, 0, 1), t0);
        let e = a.admit_data_at(1, t0).unwrap_err();
        assert_eq!(*e.code(), s3s::S3ErrorCode::SlowDown);
        assert!(a.reserve_chunk_at(1, t0).is_err(), "nor any chunk");
        assert!(retry_after(&e) >= 30);
        assert!(a.admit_start().is_err());
        assert_eq!(a.excluded_groups(), vec![7]);
        assert_eq!(a.snapshot().rejected, 3);
    }

    #[test]
    fn throttle_holds_the_average_rate_split_over_gateways() {
        let a = WriteAdmission::new("durable");
        let t0 = Instant::now();
        // 8 MiB/s over 2 gateways: 4 MiB/s here.
        seen(&a, record(1, THROTTLED, 8 * MIB, 2), t0);
        // A writer that waits as told, chunk after chunk, for 10 s.
        let (mut t, mut admitted) = (t0, 0u64);
        while t < t0 + Duration::from_secs(10) {
            let wait = a.reserve_chunk_at(MIB, t).unwrap();
            assert!(wait <= MAX_CHUNK_WAIT);
            admitted += MIB;
            t += wait.max(Duration::from_millis(1));
        }
        // 10 s at 4 MiB/s, plus at most one chunk of debt.
        assert!((38 * MIB..=41 * MIB).contains(&admitted), "{admitted}");
        // An impatient writer (no waiting) is refused once the debt is
        // longer than the bounded wait.
        let t1 = t0 + Duration::from_secs(20);
        assert_eq!(a.reserve_chunk_at(64 * MIB, t1).unwrap(), Duration::ZERO);
        let e = a
            .reserve_chunk_at(MIB, t1 + Duration::from_secs(1))
            .unwrap_err();
        assert!(retry_after(&e) >= 10, "debt of ~60 MiB at 4 MiB/s");
        // And so is a new request at its header.
        assert!(a.admit_data_at(MIB, t1 + Duration::from_secs(1)).is_err());
        assert_eq!(a.snapshot().throttled, 2);
    }

    #[test]
    fn unknown_lengths_are_charged_by_what_arrives() {
        let a = WriteAdmission::new("durable");
        let t0 = Instant::now();
        seen(&a, record(1, NORMAL, 0, 1), t0);
        // The header check charges nothing, whatever the hint.
        a.admit_data_at(1, t0).unwrap();
        assert_eq!(a.snapshot().charged_bytes, 0);
        for _ in 0..5 {
            a.reserve_chunk_at(MIB, t0).unwrap();
        }
        assert_eq!(a.snapshot().charged_bytes, 5 * MIB);
    }

    #[test]
    fn a_record_that_stops_changing_throttles_then_rejects() {
        let a = WriteAdmission::new("durable");
        let t0 = Instant::now();
        seen(&a, record(1, NORMAL, 0, 1), t0);
        assert_eq!(a.effective(t0).state, NORMAL);
        // Refreshed but unchanged (the leader stopped publishing).
        seen(&a, record(1, NORMAL, 0, 1), t0 + Duration::from_secs(20));
        assert_eq!(a.effective(t0 + Duration::from_secs(31)).state, THROTTLED);
        assert_eq!(a.effective(t0 + STALE_REJECT).state, WRITE_REJECTED);
        // A new computation: trusted again.
        let t1 = t0 + STALE_REJECT + Duration::from_secs(1);
        seen(&a, record(2, NORMAL, 0, 1), t1);
        assert_eq!(a.effective(t1).state, NORMAL);
    }

    #[test]
    fn a_record_already_old_when_first_seen_is_not_trusted_afresh() {
        let a = WriteAdmission::new("durable");
        let t0 = Instant::now();
        // Computed an hour ago (the controller has been down since).
        let wall = 10_000_000_000;
        a.observe_at(Some(record(wall - 3_600_000, NORMAL, 0, 1)), t0, wall);
        assert_eq!(a.effective(t0).state, WRITE_REJECTED);
        // Computed a few seconds ago: trusted.
        let b = WriteAdmission::new("durable");
        b.observe_at(Some(record(wall - 5_000, NORMAL, 0, 1)), t0, wall);
        assert_eq!(b.effective(t0).state, NORMAL);
    }

    #[test]
    fn a_former_leaders_record_is_not_taken() {
        let a = WriteAdmission::new("durable");
        let t0 = Instant::now();
        let mut new = record(10, WRITE_REJECTED, 0, 1);
        new.leader_token = 5;
        seen(&a, new, t0);
        let mut old = record(11, NORMAL, 0, 1);
        old.leader_token = 4;
        seen(&a, old, t0);
        assert_eq!(a.effective(t0).state, WRITE_REJECTED);
        // A lower token computed clearly later (a restored metadata store
        // whose leader counter went back): taken, the gateway is not
        // stranded.
        let mut restored = record(10 + 61_000, NORMAL, 0, 1);
        restored.leader_token = 1;
        seen(&a, restored, t0);
        assert_eq!(a.effective(t0).state, NORMAL);
    }

    #[test]
    fn debt_is_bounded() {
        let a = WriteAdmission::new("durable");
        let t0 = Instant::now();
        seen(&a, record(1, THROTTLED, 2 * MIB, 1), t0);
        let t1 = t0 + Duration::from_secs(1);
        assert!(
            a.reserve_chunk_at(64 * MIB, t1).is_ok(),
            "one chunk may go into debt"
        );
        // The whole chunk is owed: 32 s at 2 MiB/s, refused until the
        // debt is within the bounded wait.
        assert!(
            a.reserve_chunk_at(MIB, t1 + Duration::from_secs(20))
                .is_err()
        );
        let t2 = t1 + Duration::from_secs(31);
        assert!(a.reserve_chunk_at(MIB, t2).is_ok());
    }

    #[test]
    fn other_tiers_are_not_affected() {
        let a = WriteAdmission::new("archive");
        let t0 = Instant::now();
        seen(&a, record(1, RECOVERY_ONLY, 0, 1), t0);
        assert!(a.admit_data_at(MIB, t0).is_ok());
    }
}
