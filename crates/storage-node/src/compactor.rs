//! Background segment compaction (RFC-005 §5.6).
//!
//! Deleted and superseded shards leave dead bytes in sealed segments; the
//! storage engine reclaims them by compaction, which is safe to interrupt
//! and safe with concurrent reads and writes. This task schedules it: every
//! `interval` it compacts up to `max_segments` sealed segments whose dead
//! fraction is at least `min_dead_fraction`, most dead first, one at a time
//! on a blocking thread. A failed compaction leaves the store consistent
//! (the engine's guarantee) and is counted; the next pass retries.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arca_lake_storage::Store;

/// Compaction schedule.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactionPolicy {
    /// Time between passes; zero disables background compaction.
    pub interval: Duration,
    /// A sealed segment is compacted once this fraction of it is dead.
    pub min_dead_fraction: f64,
    /// Segments compacted per pass at most.
    pub max_segments: usize,
    /// Live bytes copied per pass at most (bounds the I/O burst; nearly
    /// dead segments cost almost nothing, so many fit). The first segment
    /// of a pass is compacted whatever its size.
    pub max_copy_bytes: u64,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            min_dead_fraction: 0.5,
            max_segments: 64,
            max_copy_bytes: 512 << 20,
        }
    }
}

/// Compaction counters and the space gauges of the last pass.
#[derive(Debug, Default)]
pub struct CompactionStats {
    /// Segments compacted.
    pub compactions: AtomicU64,
    /// Compactions that failed (the store stays consistent).
    pub errors: AtomicU64,
    /// Live payload bytes copied into new segments.
    pub copied_bytes: AtomicU64,
    /// Segment file bytes released (source size minus copied bytes).
    pub reclaimed_bytes: AtomicU64,
    /// Sum of segment file sizes at the last pass.
    pub segment_bytes: AtomicU64,
    /// Bytes referenced by live records at the last pass.
    pub live_bytes: AtomicU64,
}

/// One pass: refresh the gauges, compact the most dead candidates.
pub(crate) fn pass(store: &Store, policy: &CompactionPolicy, stats: &CompactionStats) {
    let segs = match store.segment_stats() {
        Ok(s) => s,
        Err(e) => {
            stats.errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %e, "compaction: cannot read segment sizes");
            return;
        }
    };
    stats
        .segment_bytes
        .store(segs.iter().map(|s| s.file_bytes).sum(), Ordering::Relaxed);
    stats
        .live_bytes
        .store(segs.iter().map(|s| s.live_bytes).sum(), Ordering::Relaxed);
    #[allow(clippy::cast_precision_loss)]
    let dead = |s: &arca_lake_storage::SegmentStats| {
        s.file_bytes.saturating_sub(s.live_bytes) as f64 / s.file_bytes.max(1) as f64
    };
    let mut candidates: Vec<_> = segs
        .iter()
        // A blocked segment would only be rewritten as it is (its
        // tombstones are still needed): never spend the pass on it.
        .filter(|s| {
            !s.active && !s.blocked && s.file_bytes > 0 && dead(s) >= policy.min_dead_fraction
        })
        .collect();
    candidates.sort_by(|a, b| dead(b).total_cmp(&dead(a)));
    let mut budget = policy.max_copy_bytes;
    for (i, s) in candidates.into_iter().take(policy.max_segments).enumerate() {
        if i > 0 && s.live_bytes > budget {
            break;
        }
        budget = budget.saturating_sub(s.live_bytes);
        match store.compact(s.segment_id) {
            Ok(r) if r.no_progress => {
                tracing::debug!(
                    segment = s.segment_id,
                    "nothing to reclaim yet; skipped until that changes"
                );
            }
            Ok(r) => {
                stats.compactions.fetch_add(1, Ordering::Relaxed);
                stats
                    .copied_bytes
                    .fetch_add(r.copied_bytes, Ordering::Relaxed);
                stats.reclaimed_bytes.fetch_add(
                    s.file_bytes.saturating_sub(r.copied_bytes),
                    Ordering::Relaxed,
                );
                tracing::info!(
                    segment = s.segment_id,
                    file_bytes = s.file_bytes,
                    copied_bytes = r.copied_bytes,
                    copied_records = r.copied_records,
                    "segment compacted"
                );
            }
            Err(e) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(segment = s.segment_id, error = %e, "compaction failed; retried next pass");
            }
        }
    }
}

/// Run passes until `stop` fires; returns after the pass in progress.
pub(crate) async fn run(
    store: Arc<Store>,
    policy: CompactionPolicy,
    stats: Arc<CompactionStats>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    if policy.interval.is_zero() {
        return;
    }
    loop {
        tokio::select! {
            () = tokio::time::sleep(policy.interval) => {}
            _ = stop.changed() => return,
        }
        if *stop.borrow() {
            return;
        }
        let (store, stats) = (Arc::clone(&store), Arc::clone(&stats));
        let _ = tokio::task::spawn_blocking(move || pass(&store, &policy, &stats)).await;
    }
}
