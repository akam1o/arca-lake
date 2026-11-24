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
    /// Segments not compacted because the device lacked room for their
    /// live bytes (Phase 3.6): compaction writes the copy before it frees
    /// the source.
    pub skipped_no_space: AtomicU64,
    /// Live payload bytes copied into new segments.
    pub copied_bytes: AtomicU64,
    /// Segment file bytes released (source size minus copied bytes).
    pub reclaimed_bytes: AtomicU64,
    /// Sum of segment file sizes at the last pass.
    pub segment_bytes: AtomicU64,
    /// Bytes referenced by live records at the last pass.
    pub live_bytes: AtomicU64,
    /// Sealed segments at or over the dead-fraction threshold still waiting
    /// for compaction after the last pass, and their dead bytes.
    pub backlog_segments: AtomicU64,
    /// See `backlog_segments`.
    pub backlog_bytes: AtomicU64,
    /// Segments whose tombstones are all still needed (skipped).
    pub blocked_segments: AtomicU64,
    /// Non-live data records still on disk (what keeps tombstones needed).
    pub dead_records: AtomicU64,
    /// The segment being appended to (never compacted).
    pub active_segment: AtomicU64,
    /// In-memory index entries after the last pass: live keys, tombstone
    /// history (only while rebuilding), keys with non-live records on disk,
    /// quarantined keys (R-70).
    pub index_live: AtomicU64,
    /// See `index_live`.
    pub index_tombstone_history: AtomicU64,
    /// See `index_live`.
    pub index_dead_tracked: AtomicU64,
    /// See `index_live`.
    pub index_quarantined: AtomicU64,
}

/// One pass: refresh the gauges, compact the most dead candidates.
/// Room a compaction must leave beyond its output (headers, alignment).
const OUTPUT_MARGIN: u64 = 1 << 20;

/// One pass. `free` is the device's free space now (`None` = unknown,
/// compaction proceeds and an out-of-space error leaves the store as it
/// was).
#[allow(clippy::too_many_lines)]
pub(crate) fn pass(
    store: &Store,
    policy: &CompactionPolicy,
    stats: &CompactionStats,
    free: Option<u64>,
) {
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
    // One rollover of the active segment for the whole pass (RFC-005 §5.6).
    let mut compaction = store.compaction_pass(candidates.len().min(policy.max_segments));
    for (i, s) in candidates.into_iter().take(policy.max_segments).enumerate() {
        if i > 0 && s.live_bytes > budget {
            break;
        }
        let need = s.live_bytes + s.live_bytes / 64 + OUTPUT_MARGIN;
        if free.is_some_and(|f| f < need) {
            stats.skipped_no_space.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                segment = s.segment_id,
                live_bytes = s.live_bytes,
                free = free.unwrap_or(0),
                "compaction skipped: no room for the segment's live bytes"
            );
            continue;
        }
        budget = budget.saturating_sub(s.live_bytes);
        match compaction.compact(s.segment_id) {
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
    // The state after the pass: what is still waiting, and why.
    if let Ok(segs) = store.segment_stats() {
        let waiting: Vec<_> = segs
            .iter()
            .filter(|s| {
                !s.active && !s.blocked && s.file_bytes > 0 && dead(s) >= policy.min_dead_fraction
            })
            .collect();
        stats
            .backlog_segments
            .store(waiting.len() as u64, Ordering::Relaxed);
        stats.backlog_bytes.store(
            waiting
                .iter()
                .map(|s| s.file_bytes.saturating_sub(s.live_bytes))
                .sum(),
            Ordering::Relaxed,
        );
        stats.blocked_segments.store(
            segs.iter().filter(|s| s.blocked).count() as u64,
            Ordering::Relaxed,
        );
        if let Some(a) = segs.iter().find(|s| s.active) {
            stats.active_segment.store(a.segment_id, Ordering::Relaxed);
        }
    }
    stats
        .dead_records
        .store(store.dead_records(), Ordering::Relaxed);
    let sizes = store.index_sizes();
    stats.index_live.store(sizes.live, Ordering::Relaxed);
    stats
        .index_tombstone_history
        .store(sizes.tombstone_history, Ordering::Relaxed);
    stats
        .index_dead_tracked
        .store(sizes.dead_tracked, Ordering::Relaxed);
    stats
        .index_quarantined
        .store(sizes.quarantined, Ordering::Relaxed);
}

/// Run passes until `stop` fires; returns after the pass in progress.
pub(crate) async fn run(
    store: Arc<Store>,
    policy: CompactionPolicy,
    stats: Arc<CompactionStats>,
    free: Arc<dyn Fn() -> Option<u64> + Send + Sync>,
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
        let (store, stats, free) = (Arc::clone(&store), Arc::clone(&stats), Arc::clone(&free));
        let _ = tokio::task::spawn_blocking(move || pass(&store, &policy, &stats, free())).await;
    }
}
