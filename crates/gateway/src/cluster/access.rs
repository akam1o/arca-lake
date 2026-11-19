//! Access records at scale (ADR-0034): a bounded table of the reads this
//! gateway served, coalesced per generation, written in batches by one
//! flusher.
//!
//! The GET path only adds to the table (no task, no metadata wait). The
//! table and the batch being flushed hold at most `capacity` generations
//! together; a read of a new generation beyond that is dropped and
//! counted. The flusher writes up to `batch` generations per transaction
//! (`object::record_accesses`: the generation's header and record read
//! together, nothing written for a generation GC finished, nothing whose
//! oldest read is older than `max_age` at the attempt). A batch that fails
//! is split down to one generation; one generation that fails goes back to
//! the table once, then is dropped. An unknown commit outcome is not
//! retried (the write adds): its reads are counted *uncertain*. Every read
//! this process can account for ends in exactly one counter.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use arca_lake_metadata::ops::object::{self, AccessUpdate, AccessWrite};
use arca_lake_metadata::records::GenId;
use arca_lake_metadata::{KvError, KvStore, MetaError};

/// Settings of the access-record table (ADR-0034 decision 8).
#[derive(Debug, Clone, PartialEq)]
pub struct AccessConfig {
    /// The per-GET write of earlier releases instead of the table (one
    /// release, for A/B and rollback).
    pub legacy: bool,
    /// Experimental (ADR-0035 option B, test clusters only): write the
    /// atomic access layout instead of the record's read-modify-write.
    pub atomic: bool,
    /// Generations pending at most (table and batch in flight together).
    pub capacity: usize,
    /// Generations per flush transaction at most.
    pub batch: usize,
    /// Time between flushes (with ±`jitter` of it at random).
    pub flush_interval: Duration,
    /// Fraction of `capacity` at which a flush starts at once.
    pub high_watermark: f64,
    /// No read is written later than this after it happened (plus one
    /// transaction attempt); older entries are dropped as stale.
    pub max_age: Duration,
    /// Time a graceful stop spends flushing what is pending.
    pub shutdown_flush: Duration,
    /// Flushes a generation singled out by contention is written alone.
    pub isolate_flushes: u32,
    /// Generations isolated at most.
    pub isolate_cap: usize,
    /// Fraction of the interval by which flush times vary.
    pub jitter: f64,
}

impl Default for AccessConfig {
    fn default() -> Self {
        Self {
            legacy: false,
            atomic: false,
            capacity: 65_536,
            // 128, not 512 (measured, Phase 4.1: a second's reads written
            // as back-to-back 512-generation batches raised GET p99 to about
            // twice the base; 128 brought it back, at 0.008 commit per GET).
            batch: 128,
            flush_interval: Duration::from_secs(1),
            high_watermark: 0.5,
            max_age: Duration::from_secs(30),
            shutdown_flush: Duration::from_secs(2),
            isolate_flushes: 8,
            isolate_cap: 64,
            jitter: 0.2,
        }
    }
}

/// Counters of the table (no per-object label: bounded cardinality).
#[derive(Debug, Default)]
pub struct AccessStats {
    /// Reads offered to the table.
    pub reads: AtomicU64,
    /// Reads merged into a pending entry.
    pub coalesced: AtomicU64,
    /// Reads that started a new entry.
    pub inserted: AtomicU64,
    /// Reads dropped: the table full (at insertion or re-insertion).
    pub dropped_full: AtomicU64,
    /// Reads dropped: older than `max_age` when written.
    pub dropped_stale: AtomicU64,
    /// Reads dropped: their generation failed twice.
    pub dropped_error: AtomicU64,
    /// Reads dropped: pending at the end of a graceful stop.
    pub dropped_shutdown: AtomicU64,
    /// Reads of batches whose commit outcome is unknown (written or not).
    pub uncertain: AtomicU64,
    /// Batches whose commit outcome is unknown.
    pub uncertain_batches: AtomicU64,
    /// Flush transactions committed.
    pub flushes: AtomicU64,
    /// Generations written.
    pub flushed_generations: AtomicU64,
    /// Reads of the generations written.
    pub flushed_reads: AtomicU64,
    /// Generations not written: the generation is gone.
    pub skipped_finished: AtomicU64,
    /// Reads of those.
    pub skipped_finished_reads: AtomicU64,
    /// Failed flush attempts: retries exhausted on conflicts.
    pub errors_conflict: AtomicU64,
    /// Failed flush attempts: backend errors.
    pub errors_backend: AtomicU64,
    /// Failed flush attempts: transaction too large.
    pub errors_too_large: AtomicU64,
    /// Failed flush attempts: anything else (cluster state, codec).
    pub errors_other: AtomicU64,
    /// Largest batch written.
    pub batch_max: AtomicU64,
    /// Flush durations (retries included).
    pub flush_time: arca_lake_common::timing::Histogram,
}

/// A copy of [`AccessStats`] (fields of the same names) and the table's
/// state: `entries` pending in the table, its `capacity`, generations
/// `in_flight`, the age of the oldest entry (`oldest_age_ms`) and the
/// generations `isolated` after contention.
#[derive(Debug, Clone, Default)]
#[allow(missing_docs)]
pub struct AccessStatsSnapshot {
    pub entries: u64,
    pub capacity: u64,
    pub in_flight: u64,
    pub oldest_age_ms: u64,
    pub isolated: u64,
    pub reads: u64,
    pub coalesced: u64,
    pub inserted: u64,
    pub dropped_full: u64,
    pub dropped_stale: u64,
    pub dropped_error: u64,
    pub dropped_shutdown: u64,
    pub uncertain: u64,
    pub uncertain_batches: u64,
    pub flushes: u64,
    pub flushed_generations: u64,
    pub flushed_reads: u64,
    pub skipped_finished: u64,
    pub skipped_finished_reads: u64,
    pub errors_conflict: u64,
    pub errors_backend: u64,
    pub errors_too_large: u64,
    pub errors_other: u64,
    pub batch_max: u64,
    pub flush_time: arca_lake_common::timing::HistogramSnapshot,
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    last_ms: u64,
    reads: u64,
    oldest_ms: u64,
    /// Failed once already (a second failure drops it).
    failed: bool,
}

#[derive(Debug, Default)]
struct State {
    map: HashMap<GenId, Entry>,
    /// Generations of `map`, oldest entry first (each once).
    order: VecDeque<GenId>,
    /// Generations and reads of the batch being flushed.
    in_flight: usize,
    in_flight_reads: u64,
    /// Reads in `map` (kept so that no scan of the table runs under the
    /// lock the GET path takes).
    table_reads: u64,
    /// Generations singled out by contention: flushes left alone.
    isolated: HashMap<GenId, u32>,
    stopping: bool,
    /// The stop's wait ended: the flusher takes nothing more and counts
    /// what it still holds as dropped (`shutdown`) when it reaches it.
    abandoned: bool,
    /// A flush failed for a reason a smaller batch cannot avoid: this
    /// flush takes nothing more.
    backing_off: bool,
}

/// The table of one gateway (shared by its clones).
#[derive(Debug)]
pub struct AccessTable {
    cfg: AccessConfig,
    state: Mutex<State>,
    wake: Arc<tokio::sync::Notify>,
    stopped: tokio::sync::Notify,
    stopped_flag: std::sync::atomic::AtomicBool,
    /// A flusher runs for this table.
    flusher: std::sync::atomic::AtomicBool,
    draws: AtomicU64,
    /// Entries (table and in flight) at which a flush starts at once.
    high: usize,
    /// Its counters.
    pub stats: AccessStats,
}

impl AccessTable {
    /// An empty table.
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    pub fn new(cfg: AccessConfig) -> Self {
        let high =
            ((cfg.capacity as f64 * cfg.high_watermark.clamp(0.0, 1.0)).ceil() as usize).max(1);
        Self {
            high,
            cfg,
            state: Mutex::new(State::default()),
            wake: Arc::new(tokio::sync::Notify::new()),
            stopped: tokio::sync::Notify::new(),
            stopped_flag: std::sync::atomic::AtomicBool::new(false),
            flusher: std::sync::atomic::AtomicBool::new(false),
            // Each gateway its own jitter sequence (review: tables seeded
            // alike flushed in step).
            draws: AtomicU64::new({
                use std::hash::{BuildHasher as _, Hasher as _};
                let mut h = std::collections::hash_map::RandomState::new().build_hasher();
                h.write_u64(0);
                h.finish()
            }),
            stats: AccessStats::default(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A read of `g` at `now_ms` (the GET path: no wait, no task).
    pub fn note(&self, g: GenId, now_ms: u64) {
        let s = &self.stats;
        s.reads.fetch_add(1, Relaxed);
        let wake = {
            let mut st = self.lock();
            if st.stopping {
                drop(st);
                s.dropped_shutdown.fetch_add(1, Relaxed);
                return;
            }
            if let Some(e) = st.map.get_mut(&g) {
                e.last_ms = e.last_ms.max(now_ms);
                e.reads += 1;
                st.table_reads += 1;
                s.coalesced.fetch_add(1, Relaxed);
                false
            } else if st.map.len() + st.in_flight < self.cfg.capacity {
                st.map.insert(
                    g,
                    Entry {
                        last_ms: now_ms,
                        reads: 1,
                        oldest_ms: now_ms,
                        failed: false,
                    },
                );
                st.order.push_back(g);
                st.table_reads += 1;
                s.inserted.fetch_add(1, Relaxed);
                st.map.len() + st.in_flight >= self.high
            } else {
                drop(st);
                s.dropped_full.fetch_add(1, Relaxed);
                return;
            }
        };
        if wake {
            self.wake.notify_one();
        }
    }

    /// Take up to `n` generations, oldest first, as batches: the isolated
    /// ones each alone, the others together. Counted in flight until
    /// [`Self::done`].
    fn take(&self, n: usize) -> Vec<Vec<(GenId, Entry)>> {
        let mut st = self.lock();
        let mut shared = Vec::new();
        let mut alone = Vec::new();
        if st.abandoned || st.backing_off {
            return alone;
        }
        let mut taken = 0usize;
        while taken < n.max(1) {
            let Some(g) = st.order.pop_front() else {
                break;
            };
            let Some(e) = st.map.remove(&g) else {
                continue;
            };
            taken += 1;
            st.in_flight += 1;
            st.in_flight_reads += e.reads;
            st.table_reads = st.table_reads.saturating_sub(e.reads);
            if st.isolated.contains_key(&g) {
                alone.push(vec![(g, e)]);
            } else {
                shared.push((g, e));
            }
        }
        if !shared.is_empty() {
            alone.push(shared);
        }
        alone
    }

    /// A taken generation's flush ended (written, dropped or re-inserted).
    fn done(&self, reads: u64) {
        let mut st = self.lock();
        st.in_flight = st.in_flight.saturating_sub(1);
        st.in_flight_reads = st.in_flight_reads.saturating_sub(reads);
    }

    /// One generation's flush failed: back into the table once (merged
    /// with what arrived meanwhile, the mark kept), else dropped.
    fn requeue(&self, g: GenId, e: Entry, contended: bool) {
        let s = &self.stats;
        let mut st = self.lock();
        st.in_flight = st.in_flight.saturating_sub(1);
        st.in_flight_reads = st.in_flight_reads.saturating_sub(e.reads);
        if st.abandoned {
            drop(st);
            s.dropped_shutdown.fetch_add(e.reads, Relaxed);
            return;
        }
        if e.failed || st.stopping {
            // Dropped: its isolation slot goes too (review).
            st.isolated.remove(&g);
            drop(st);
            s.dropped_error.fetch_add(e.reads, Relaxed);
            return;
        }
        if contended && st.isolated.len() < self.cfg.isolate_cap {
            st.isolated.insert(g, self.cfg.isolate_flushes.max(1));
        }
        if let Some(cur) = st.map.get_mut(&g) {
            cur.last_ms = cur.last_ms.max(e.last_ms);
            cur.reads += e.reads;
            cur.oldest_ms = cur.oldest_ms.min(e.oldest_ms);
            cur.failed = true;
            st.table_reads += e.reads;
        } else if st.map.len() + st.in_flight < self.cfg.capacity {
            st.map.insert(g, Entry { failed: true, ..e });
            st.order.push_back(g);
            st.table_reads += e.reads;
        } else {
            st.isolated.remove(&g);
            drop(st);
            s.dropped_full.fetch_add(e.reads, Relaxed);
        }
    }

    /// A taken entry that was not sent goes back as it was (not marked
    /// failed), or is dropped when there is no room.
    fn put_back(&self, g: GenId, e: Entry) {
        let failed = e.failed;
        let mut st = self.lock();
        st.in_flight = st.in_flight.saturating_sub(1);
        st.in_flight_reads = st.in_flight_reads.saturating_sub(e.reads);
        if st.abandoned {
            drop(st);
            self.stats.dropped_shutdown.fetch_add(e.reads, Relaxed);
            return;
        }
        if let Some(cur) = st.map.get_mut(&g) {
            cur.last_ms = cur.last_ms.max(e.last_ms);
            cur.reads += e.reads;
            cur.oldest_ms = cur.oldest_ms.min(e.oldest_ms);
            cur.failed |= failed;
            st.table_reads += e.reads;
        } else if st.map.len() + st.in_flight < self.cfg.capacity {
            st.map.insert(g, e);
            st.order.push_front(g);
            st.table_reads += e.reads;
        } else {
            drop(st);
            self.stats.dropped_full.fetch_add(e.reads, Relaxed);
        }
    }

    /// The counters and the table's state.
    #[must_use]
    pub fn snapshot(&self, now_ms: u64) -> AccessStatsSnapshot {
        let (entries, in_flight, oldest, isolated) = {
            let st = self.lock();
            let oldest = st
                .order
                .front()
                .and_then(|g| st.map.get(g))
                .map_or(0, |e| now_ms.saturating_sub(e.oldest_ms));
            (st.map.len(), st.in_flight, oldest, st.isolated.len())
        };
        let s = &self.stats;
        let g = |a: &AtomicU64| a.load(Relaxed);
        AccessStatsSnapshot {
            entries: entries as u64,
            capacity: self.cfg.capacity as u64,
            in_flight: in_flight as u64,
            oldest_age_ms: oldest,
            isolated: isolated as u64,
            reads: g(&s.reads),
            coalesced: g(&s.coalesced),
            inserted: g(&s.inserted),
            dropped_full: g(&s.dropped_full),
            dropped_stale: g(&s.dropped_stale),
            dropped_error: g(&s.dropped_error),
            dropped_shutdown: g(&s.dropped_shutdown),
            uncertain: g(&s.uncertain),
            uncertain_batches: g(&s.uncertain_batches),
            flushes: g(&s.flushes),
            flushed_generations: g(&s.flushed_generations),
            flushed_reads: g(&s.flushed_reads),
            skipped_finished: g(&s.skipped_finished),
            skipped_finished_reads: g(&s.skipped_finished_reads),
            errors_conflict: g(&s.errors_conflict),
            errors_backend: g(&s.errors_backend),
            errors_too_large: g(&s.errors_too_large),
            errors_other: g(&s.errors_other),
            batch_max: g(&s.batch_max),
            flush_time: s.flush_time.snapshot(),
        }
    }

    /// Pending generations and reads (table and in flight).
    #[must_use]
    pub fn pending(&self) -> (usize, u64) {
        let st = self.lock();
        (
            st.map.len() + st.in_flight,
            st.table_reads + st.in_flight_reads,
        )
    }

    /// The next flush time from now: the interval, ±`jitter` at random.
    fn next_wait(&self) -> Duration {
        let n = self.draws.fetch_add(1, Relaxed);
        let mut x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xA5A5_5A5A_1234_5678;
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 31;
        #[allow(clippy::cast_precision_loss)]
        let u = (x >> 11) as f64 / (1u64 << 53) as f64;
        let j = self.cfg.jitter.clamp(0.0, 0.9);
        self.cfg.flush_interval.mul_f64(1.0 - j + 2.0 * j * u)
    }

    /// Stop gracefully: the flusher writes what is pending for at most
    /// `shutdown_flush`. When the wait ends, what is still in the table is
    /// counted dropped (`shutdown`); a batch the flusher still holds stays
    /// its own to count (written, uncertain, or dropped as `shutdown` at
    /// its next step), so that no read is counted twice (review).
    pub async fn shutdown(&self) {
        self.lock().stopping = true;
        self.wake.notify_one();
        if self.flusher.load(Relaxed) {
            let stopped = self.stopped.notified();
            tokio::pin!(stopped);
            stopped.as_mut().enable();
            if !self.stopped_flag.load(Relaxed) {
                let _ =
                    tokio::time::timeout(self.cfg.shutdown_flush + Duration::from_secs(1), stopped)
                        .await;
            }
        }
        let mut st = self.lock();
        st.abandoned = true;
        let left = std::mem::take(&mut st.table_reads);
        st.map.clear();
        st.order.clear();
        drop(st);
        self.stats.dropped_shutdown.fetch_add(left, Relaxed);
    }

    fn abandoned(&self) -> bool {
        self.lock().abandoned
    }

    fn back_off(&self) {
        self.lock().backing_off = true;
    }

    fn backing_off(&self) -> bool {
        self.lock().backing_off
    }

    /// The start of a flush: isolation counts down by flushes (so that a
    /// generation never read again frees its slot), and a back-off ends.
    fn begin_flush(&self) {
        let mut st = self.lock();
        st.backing_off = false;
        st.isolated.retain(|_, left| {
            *left = left.saturating_sub(1);
            *left > 0
        });
    }
}

use super::backend::now_ms;

/// How a failed flush attempt counts.
fn classify(e: &MetaError) -> Option<&'static str> {
    match e {
        MetaError::Kv(KvError::CommitUnknown(_)) => None,
        MetaError::TooManyRetries => Some("conflict"),
        MetaError::Kv(KvError::Backend(_)) => Some("backend"),
        MetaError::Kv(KvError::TooLarge { .. }) => Some("too_large"),
        _ => Some("other"),
    }
}

/// Write one batch, splitting it on failure down to one generation.
/// Returns, in the order they would have been tried, the entries left
/// untried when a failure no smaller batch avoids stops it (the caller
/// puts them back).
#[allow(clippy::many_single_char_names)]
async fn flush_batch<S: KvStore>(
    meta: &S,
    t: &AccessTable,
    batch: Vec<(GenId, Entry)>,
    clock: &(dyn Fn() -> u64 + Send + Sync),
) -> Vec<(GenId, Entry)> {
    let s = &t.stats;
    let max_age = u64::try_from(t.cfg.max_age.as_millis()).unwrap_or(u64::MAX);
    let mut todo = vec![batch];
    while let Some(b) = todo.pop() {
        if b.is_empty() {
            continue;
        }
        if t.abandoned() {
            // The stop gave up waiting: what is held is dropped, once.
            for (_, e) in &b {
                s.dropped_shutdown.fetch_add(e.reads, Relaxed);
                t.done(e.reads);
            }
            continue;
        }
        let updates: Vec<AccessUpdate> = b
            .iter()
            .map(|(g, e)| AccessUpdate {
                generation: *g,
                last_ms: e.last_ms,
                reads: e.reads,
                oldest_ms: e.oldest_ms,
            })
            .collect();
        let started = std::time::Instant::now();
        let r = if t.cfg.atomic {
            object::record_accesses_atomic(meta, &updates, max_age, clock).await
        } else {
            object::record_accesses(meta, &updates, max_age, clock).await
        };
        s.flush_time.observe(started.elapsed());
        match r {
            Ok(results) => {
                s.flushes.fetch_add(1, Relaxed);
                s.batch_max.fetch_max(b.len() as u64, Relaxed);
                for ((_, e), w) in b.iter().zip(results) {
                    match w {
                        AccessWrite::Written => {
                            s.flushed_generations.fetch_add(1, Relaxed);
                            s.flushed_reads.fetch_add(e.reads, Relaxed);
                        }
                        AccessWrite::Finished => {
                            s.skipped_finished.fetch_add(1, Relaxed);
                            s.skipped_finished_reads.fetch_add(e.reads, Relaxed);
                        }
                        AccessWrite::Stale => {
                            s.dropped_stale.fetch_add(e.reads, Relaxed);
                        }
                    }
                    t.done(e.reads);
                }
            }
            Err(e) => match classify(&e) {
                None => {
                    // Written or not: never sent again (it adds).
                    s.uncertain_batches.fetch_add(1, Relaxed);
                    for (_, en) in &b {
                        s.uncertain.fetch_add(en.reads, Relaxed);
                        t.done(en.reads);
                    }
                }
                Some(kind) => {
                    match kind {
                        "conflict" => &s.errors_conflict,
                        "backend" => &s.errors_backend,
                        "too_large" => &s.errors_too_large,
                        _ => &s.errors_other,
                    }
                    .fetch_add(1, Relaxed);
                    tracing::debug!(error = %e, generations = b.len(), "access-record flush failed");
                    // Only a failure a smaller batch can avoid is split
                    // (contention, size; review: splitting on a refusal or
                    // an unreachable store multiplies failing
                    // transactions). Otherwise every entry goes back once
                    // and the flusher waits for its next interval.
                    let splittable = kind == "conflict" || kind == "too_large";
                    if b.len() > 1 && splittable {
                        let mut b = b;
                        let second = b.split_off(b.len() / 2);
                        todo.push(second);
                        todo.push(b);
                    } else {
                        for (g, en) in &b {
                            t.requeue(*g, *en, kind == "conflict" && b.len() == 1);
                        }
                        if !splittable {
                            // Sub-batches not tried yet go back unmarked
                            // (by the caller; `todo` pops from its end).
                            t.back_off();
                            return todo.drain(..).rev().flatten().collect();
                        }
                    }
                }
            },
        }
    }
    Vec::new()
}

/// The flusher of `table`: every interval (jittered), at the high
/// watermark, or when stopping, it writes what is pending, batch after
/// batch; it ends when the table is dropped or after a graceful stop.
pub fn spawn_flusher<S: KvStore + 'static>(meta: Arc<S>, table: &Arc<AccessTable>) {
    table.flusher.store(true, Relaxed);
    let weak: Weak<AccessTable> = Arc::downgrade(table);
    tokio::spawn(async move {
        loop {
            let Some(t) = weak.upgrade() else {
                return;
            };
            let wait = t.next_wait();
            let (stopping, backing_off) = {
                let st = t.lock();
                (st.stopping, st.backing_off)
            };
            let wake = Arc::clone(&t.wake);
            // No strong reference while sleeping: a dropped backend ends
            // the flusher at its next wake.
            drop(t);
            if !stopping {
                if backing_off {
                    // The whole interval, whatever the table's level; a
                    // stop still ends the wait.
                    let deadline = tokio::time::Instant::now() + wait;
                    loop {
                        tokio::select! {
                            () = tokio::time::sleep_until(deadline) => break,
                            () = wake.notified() => {
                                if weak.upgrade().is_none_or(|t| t.lock().stopping) {
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    tokio::select! {
                        () = tokio::time::sleep(wait) => {}
                        () = wake.notified() => {}
                    }
                }
            }
            let Some(t) = weak.upgrade() else {
                return;
            };
            let stopping = t.lock().stopping;
            let deadline = stopping.then(|| std::time::Instant::now() + t.cfg.shutdown_flush);
            run_flush(meta.as_ref(), &t, deadline, &now_ms).await;
            if stopping {
                t.stopped_flag.store(true, Relaxed);
                t.stopped.notify_waiters();
                return;
            }
        }
    });
}

/// Write what is pending now, batch after batch (until `deadline`).
async fn run_flush<S: KvStore>(
    meta: &S,
    t: &AccessTable,
    deadline: Option<std::time::Instant>,
    clock: &(dyn Fn() -> u64 + Send + Sync),
) {
    // At most what was pending when the flush began, plus what arrives:
    // bounded by the passes below.
    t.begin_flush();
    let (pending, _) = t.pending();
    let mut left = pending;
    while left > 0 {
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return;
        }
        let batches = t.take(t.cfg.batch.min(left));
        if batches.is_empty() {
            return;
        }
        let n: usize = batches.iter().map(Vec::len).sum();
        left = left.saturating_sub(n);
        let mut batches = batches.into_iter();
        let mut untried = Vec::new();
        for b in batches.by_ref() {
            untried = flush_batch(meta, t, b, clock).await;
            if t.backing_off() {
                break;
            }
        }
        // Backing off (review): what was taken but not sent goes back,
        // nothing more is sent before the next interval. Put back newest
        // first, each to the front, so that the table's order (oldest
        // first) is restored.
        untried.extend(batches.flatten());
        for (g, e) in untried.into_iter().rev() {
            t.put_back(g, e);
        }
        if t.backing_off() {
            return;
        }
    }
}

#[cfg(test)]
#[allow(clippy::too_many_lines)]
mod tests {
    use super::*;
    use arca_lake_metadata::fault::{CommitFault, FaultKv};
    use arca_lake_metadata::keys;
    use arca_lake_metadata::kv::Txn;
    use arca_lake_metadata::mem::MemKv;
    use arca_lake_metadata::records::{GenKind, GenState, GenerationHeader, encode};

    fn cfg(capacity: usize, batch: usize) -> AccessConfig {
        AccessConfig {
            capacity,
            batch,
            max_age: Duration::from_secs(30),
            ..AccessConfig::default()
        }
    }

    fn g(i: u8) -> GenId {
        let mut id = [0u8; 16];
        id[0] = 1;
        id[15] = i;
        id
    }

    async fn put_header<S: KvStore>(kv: &S, id: GenId, state: GenState) {
        let h = GenerationHeader {
            state,
            bucket_id: 1,
            key: format!("k{}", id[15]),
            size: 1,
            etag: String::new(),
            content_type: None,
            user_metadata: Vec::new(),
            chunk_count: 0,
            created_ms: 1,
            kind: GenKind::Object,
            checksum: None,
        };
        let mut tx = kv.begin().await.unwrap();
        tx.set(&keys::gen_header(&id), &encode(&h).unwrap());
        tx.commit().await.unwrap();
    }

    async fn record<S: KvStore>(kv: &S, id: GenId) -> Option<object::AccessRecord> {
        object::access_of(kv, &id).await.unwrap()
    }

    /// Every read the table took is in exactly one place.
    fn assert_conserved(t: &AccessTable) {
        let s = t.snapshot(0);
        let (_, pending) = t.pending();
        let accounted = s.flushed_reads
            + s.skipped_finished_reads
            + s.dropped_stale
            + s.dropped_error
            + s.dropped_shutdown
            + s.dropped_full
            + s.uncertain
            + pending;
        assert_eq!(s.reads, accounted, "{s:?} pending {pending}");
    }

    #[test]
    fn the_table_is_bounded_and_coalesces() {
        let t = AccessTable::new(cfg(4, 512));
        for i in 0..10 {
            t.note(g(i), 100 + u64::from(i));
        }
        // The same generation again: merged, never a new entry.
        for _ in 0..5 {
            t.note(g(0), 500);
        }
        let s = t.snapshot(1000);
        assert_eq!((s.entries, s.inserted, s.coalesced), (4, 4, 5));
        assert_eq!(s.dropped_full, 6);
        // A batch in flight still counts against the capacity.
        let batches = t.take(2);
        assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), 2);
        t.note(g(20), 600);
        assert_eq!(t.snapshot(0).dropped_full, 7);
        assert_eq!(t.pending().0, 4);
        let (_, e) = batches[0][0];
        assert_eq!((e.reads, e.last_ms, e.oldest_ms), (6, 500, 100));
    }

    #[tokio::test]
    async fn a_flush_writes_the_sums_and_nothing_for_a_finished_generation() {
        let kv = MemKv::default();
        put_header(&kv, g(1), GenState::Committed).await;
        put_header(&kv, g(2), GenState::Committed).await;
        // g(3) never existed or was finished: no header.
        let t = AccessTable::new(cfg(64, 512));
        for (id, at) in [(1, 10), (1, 30), (2, 20), (3, 40), (1, 25)] {
            t.note(g(id), at);
        }
        run_flush(&kv, &t, None, &|| 50).await;
        assert_eq!(
            record(&kv, g(1)).await,
            Some(object::AccessRecord {
                last_ms: 30,
                count: 3
            })
        );
        assert_eq!(
            record(&kv, g(2)).await,
            Some(object::AccessRecord {
                last_ms: 20,
                count: 1
            })
        );
        assert_eq!(
            record(&kv, g(3)).await,
            None,
            "an orphan record was created"
        );
        let s = t.snapshot(0);
        assert_eq!(
            (s.flushes, s.flushed_generations, s.skipped_finished),
            (1, 2, 1)
        );
        // Another flush adds, and a record never goes back in time.
        t.note(g(1), 5);
        run_flush(&kv, &t, None, &|| 50).await;
        assert_eq!(
            record(&kv, g(1)).await,
            Some(object::AccessRecord {
                last_ms: 30,
                count: 4
            })
        );
        assert_conserved(&t);
    }

    /// The GC races of ADR-0034 §5: read → finish → flush; flush → finish;
    /// a generation replaced (retiring) and then finished.
    #[tokio::test]
    async fn no_record_survives_or_reappears_after_gc_finished_the_generation() {
        let kv = MemKv::default();
        let t = AccessTable::new(cfg(64, 512));
        // Read, then GC finishes, then the flush: nothing written.
        put_header(&kv, g(1), GenState::Retiring { since_ms: 1 }).await;
        t.note(g(1), 10);
        arca_lake_metadata::ops::gc::finish(&kv, &g(1))
            .await
            .unwrap();
        run_flush(&kv, &t, None, &|| 20).await;
        assert_eq!(record(&kv, g(1)).await, None);
        // Flush, then GC: the finish clears the record with the generation.
        put_header(&kv, g(2), GenState::Committed).await;
        t.note(g(2), 10);
        run_flush(&kv, &t, None, &|| 20).await;
        assert!(record(&kv, g(2)).await.is_some());
        put_header(&kv, g(2), GenState::Retiring { since_ms: 2 }).await;
        arca_lake_metadata::ops::gc::finish(&kv, &g(2))
            .await
            .unwrap();
        assert_eq!(record(&kv, g(2)).await, None);
        // A read of it still pending (a GET served before the finish) does
        // not bring it back.
        t.note(g(2), 30);
        run_flush(&kv, &t, None, &|| 40).await;
        assert_eq!(record(&kv, g(2)).await, None);
        // Tiering's reset of a finished generation writes nothing either.
        object::reset_access(&kv, &g(2), 50).await.unwrap();
        assert_eq!(record(&kv, g(2)).await, None);
        assert_eq!(t.snapshot(0).skipped_finished, 2);
        assert_conserved(&t);
    }

    /// An unknown commit outcome is never retried: an applied batch counts
    /// its reads once, an unapplied one loses them; both are counted
    /// uncertain, and the next flush adds only new reads.
    #[tokio::test]
    async fn an_unknown_outcome_is_never_counted_twice() {
        for (fault, applied) in [
            (CommitFault::UnknownAfterApply, true),
            (CommitFault::UnknownWithoutApply, false),
            (CommitFault::TimeoutAfterApply, true),
            (CommitFault::TimeoutWithoutApply, false),
        ] {
            let kv = FaultKv::new(MemKv::default());
            put_header(&kv, g(1), GenState::Committed).await;
            let t = AccessTable::new(cfg(64, 512));
            for _ in 0..3 {
                t.note(g(1), 10);
            }
            kv.inject_on_key(keys::access(&g(1)), fault, 1);
            run_flush(&kv, &t, None, &|| 20).await;
            let s = t.snapshot(0);
            assert_eq!((s.uncertain, s.uncertain_batches), (3, 1), "{fault:?}");
            assert_eq!(t.pending().0, 0, "{fault:?}: an uncertain batch went back");
            let want = applied.then_some(object::AccessRecord {
                last_ms: 10,
                count: 3,
            });
            assert_eq!(record(&kv, g(1)).await, want, "{fault:?}");
            t.note(g(1), 30);
            run_flush(&kv, &t, None, &|| 40).await;
            assert_eq!(
                record(&kv, g(1)).await.map(|r| r.count),
                Some(if applied { 4 } else { 1 }),
                "{fault:?}"
            );
            assert_conserved(&t);
        }
    }

    /// A batch that keeps failing is split; the generation that fails is
    /// retried once (isolated), then dropped; the others are written.
    #[tokio::test]
    async fn a_contended_generation_is_split_out_retried_once_then_dropped() {
        let kv = FaultKv::new(MemKv::default());
        for i in 1..=8 {
            put_header(&kv, g(i), GenState::Committed).await;
        }
        let t = AccessTable::new(cfg(64, 512));
        for i in 1..=8 {
            t.note(g(i), u64::from(i));
        }
        // Every transaction writing g(5)'s record conflicts (more than the
        // retries of one transaction, several times over).
        kv.inject_on_key(keys::access(&g(5)), CommitFault::Conflict, 10_000);
        run_flush(&kv, &t, None, &|| 20).await;
        for i in (1..=8).filter(|i| *i != 5) {
            assert_eq!(record(&kv, g(i)).await.map(|r| r.count), Some(1), "g({i})");
        }
        assert_eq!(record(&kv, g(5)).await, None);
        let s = t.snapshot(0);
        assert!(s.errors_conflict >= 4, "{s:?}");
        assert_eq!((s.entries, s.isolated), (1, 1), "retried once, alone");
        // Its second failure drops it (counted), it does not come back.
        run_flush(&kv, &t, None, &|| 30).await;
        let s = t.snapshot(0);
        assert_eq!((s.entries, s.dropped_error), (0, 1), "{s:?}");
        assert_conserved(&t);
    }

    /// A read older than the age bound at the flush attempt is not
    /// written (ADR-0034: no read written long after it happened).
    #[tokio::test]
    async fn reads_older_than_the_age_bound_are_dropped_as_stale() {
        let kv = MemKv::default();
        put_header(&kv, g(1), GenState::Committed).await;
        put_header(&kv, g(2), GenState::Committed).await;
        let t = AccessTable::new(cfg(64, 512));
        t.note(g(1), 1_000);
        t.note(g(2), 40_000);
        run_flush(&kv, &t, None, &|| 45_000).await;
        assert_eq!(record(&kv, g(1)).await, None);
        assert_eq!(record(&kv, g(2)).await.map(|r| r.count), Some(1));
        assert_eq!(t.snapshot(0).dropped_stale, 1);
        assert_conserved(&t);
    }

    /// Two gateways recording the same generation (each its own table and
    /// clock, one ahead): the counts add, the time is the latest.
    #[tokio::test]
    async fn two_gateways_add_up_and_the_time_never_goes_back() {
        let kv = std::sync::Arc::new(MemKv::default());
        put_header(kv.as_ref(), g(1), GenState::Committed).await;
        let a = Arc::new(AccessTable::new(cfg(64, 512)));
        let b = Arc::new(AccessTable::new(cfg(64, 512)));
        for i in 0..100u64 {
            a.note(g(1), 1_000 + i);
            b.note(g(1), 5_000 + i); // a clock 4 s ahead
        }
        let (ka, kb) = (Arc::clone(&kv), Arc::clone(&kv));
        let (ta, tb) = (Arc::clone(&a), Arc::clone(&b));
        let ha = tokio::spawn(async move { run_flush(ka.as_ref(), &ta, None, &|| 6_000).await });
        let hb = tokio::spawn(async move { run_flush(kb.as_ref(), &tb, None, &|| 6_000).await });
        ha.await.unwrap();
        hb.await.unwrap();
        assert_eq!(
            record(kv.as_ref(), g(1)).await,
            Some(object::AccessRecord {
                last_ms: 5_099,
                count: 200
            })
        );
        // The slower clock's later flush does not move it back.
        a.note(g(1), 2_000);
        run_flush(kv.as_ref(), &a, None, &|| 6_000).await;
        assert_eq!(
            record(kv.as_ref(), g(1)).await.map(|r| r.last_ms),
            Some(5_099)
        );
    }

    /// A graceful stop flushes what is pending and then refuses reads
    /// (counted); the flusher ends.
    #[tokio::test]
    async fn a_graceful_stop_flushes_what_is_pending() {
        let kv = std::sync::Arc::new(MemKv::default());
        put_header(kv.as_ref(), g(1), GenState::Committed).await;
        let mut c = cfg(64, 512);
        c.flush_interval = Duration::from_secs(3600);
        let t = Arc::new(AccessTable::new(c));
        spawn_flusher(Arc::clone(&kv), &t);
        t.note(g(1), now_ms());
        t.note(g(1), now_ms());
        tokio::time::timeout(Duration::from_secs(5), t.shutdown())
            .await
            .unwrap();
        assert_eq!(record(kv.as_ref(), g(1)).await.map(|r| r.count), Some(2));
        t.note(g(1), now_ms());
        let s = t.snapshot(0);
        assert_eq!((s.dropped_shutdown, s.entries), (1, 0));
        assert_conserved(&t);
    }

    /// The flusher runs at the high watermark without waiting for its
    /// interval, and ends once the table is dropped.
    #[tokio::test(start_paused = true)]
    async fn the_high_watermark_flushes_at_once_and_a_dropped_table_ends_the_flusher() {
        let kv = std::sync::Arc::new(MemKv::default());
        for i in 1..=4 {
            put_header(kv.as_ref(), g(i), GenState::Committed).await;
        }
        let mut c = cfg(8, 512);
        c.flush_interval = Duration::from_secs(3600);
        let t = Arc::new(AccessTable::new(c));
        spawn_flusher(Arc::clone(&kv), &t);
        tokio::task::yield_now().await;
        for i in 1..=4 {
            t.note(g(i), now_ms());
        }
        for _ in 0..100 {
            tokio::task::yield_now().await;
            if t.snapshot(0).flushes > 0 {
                break;
            }
        }
        assert_eq!(
            t.snapshot(0).flushed_generations,
            4,
            "flushed before the interval"
        );
        let weak = Arc::downgrade(&t);
        drop(t);
        tokio::time::sleep(Duration::from_secs(7200)).await;
        assert!(weak.upgrade().is_none());
    }

    /// Random reads, flushes, GC finishes, unknown outcomes and conflicts
    /// on two gateways, against a reference: no record of a finished
    /// generation; a record's count never above the reads served; every
    /// read accounted for.
    #[tokio::test]
    async fn random_interleavings_keep_the_records_within_the_reads() {
        for seed in 1..=40u64 {
            let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut rnd = |n: u64| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x % n
            };
            let kv = FaultKv::new(MemKv::default());
            let tables = [AccessTable::new(cfg(6, 4)), AccessTable::new(cfg(6, 4))];
            let mut live = [true; 10];
            let mut served = [0u64; 10];
            for i in 0..10u8 {
                put_header(&kv, g(i), GenState::Retiring { since_ms: 1 }).await;
            }
            for step in 0..200u64 {
                let i = usize::try_from(rnd(10)).unwrap();
                match rnd(10) {
                    0..=5 if live[i] => {
                        let t = &tables[usize::try_from(rnd(2)).unwrap()];
                        t.note(g(u8::try_from(i).unwrap()), 1_000 + step);
                        served[i] += 1;
                    }
                    6 => {
                        let t = &tables[usize::try_from(rnd(2)).unwrap()];
                        match rnd(4) {
                            0 => kv.inject(
                                |w| w.iter().any(|k| k.windows(2).any(|_| true)),
                                CommitFault::UnknownAfterApply,
                                1,
                            ),
                            1 => kv.inject(|_| true, CommitFault::UnknownWithoutApply, 1),
                            2 => kv.inject(|_| true, CommitFault::Conflict, 40),
                            _ => {}
                        }
                        run_flush(&kv, t, None, &move || 1_000 + step).await;
                        kv.clear_rules();
                    }
                    7 if live[i] => {
                        arca_lake_metadata::ops::gc::finish(&kv, &g(u8::try_from(i).unwrap()))
                            .await
                            .unwrap();
                        live[i] = false;
                    }
                    _ => {}
                }
                for (j, l) in live.iter().enumerate() {
                    let r = record(&kv, g(u8::try_from(j).unwrap())).await;
                    if !l {
                        assert_eq!(
                            r, None,
                            "seed {seed} step {step}: record of finished g({j})"
                        );
                    }
                    if let Some(r) = r {
                        assert!(
                            r.count <= served[j],
                            "seed {seed}: g({j}) {} > {}",
                            r.count,
                            served[j]
                        );
                    }
                }
            }
            for t in &tables {
                assert_conserved(t);
                assert!(t.pending().0 <= 6);
            }
        }
    }

    /// Tiering accuracy (ADR-0034 §6), safety aside: two gateways serve a
    /// hot/cold workload for ten simulated minutes, flushing every second;
    /// tiering's rules (promote at `promote` reads with a recent last read,
    /// demote after `idle` without one) are applied to the recorded and to
    /// the true reads, per class. With room for every generation read
    /// within an interval the decisions match exactly; under forced
    /// overflow (64 entries) the records only under-count: no promotion or
    /// recency that did not happen, and the differences are counted.
    #[tokio::test]
    async fn tiering_decisions_from_the_records_match_the_reads_unless_the_table_overflows() {
        const OBJECTS: u64 = 2_000;
        const HOT: u64 = 20;
        const SECONDS: u64 = 600;
        let decide = |count: u64, last: u64, now: u64| -> (bool, bool) {
            // (promote, demote) with promote_reads 10, window 300 s, idle 300 s.
            (
                count >= 10 && now - last <= 300_000,
                last == 0 || now - last > 300_000,
            )
        };
        for (capacity, exact) in [(65_536usize, true), (64, false)] {
            let kv = MemKv::default();
            let id = |i: u64| {
                let mut g = [0u8; 16];
                g[0] = 2;
                g[8..].copy_from_slice(&i.to_be_bytes());
                g
            };
            for i in 0..OBJECTS {
                put_header(&kv, id(i), GenState::Committed).await;
            }
            let tables = [
                AccessTable::new(cfg(capacity, 512)),
                AccessTable::new(cfg(capacity, 512)),
            ];
            let mut truth = vec![(0u64, 0u64); usize::try_from(OBJECTS).unwrap()];
            let mut x = 0x1234_5678_9ABC_DEF1u64;
            let mut rnd = |n: u64| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x % n
            };
            for sec in 0..SECONDS {
                let now = 1_000_000 + sec * 1000;
                for (gw, t) in tables.iter().enumerate() {
                    // Hot: each read about once a second per gateway; cold:
                    // 120 random objects a second.
                    for i in 0..HOT {
                        if rnd(10) < 9 {
                            t.note(id(i), now + gw as u64);
                            let e = &mut truth[usize::try_from(i).unwrap()];
                            *e = (e.0 + 1, e.1.max(now + gw as u64));
                        }
                    }
                    for _ in 0..120 {
                        let i = HOT + rnd(OBJECTS - HOT);
                        t.note(id(i), now + gw as u64);
                        let e = &mut truth[usize::try_from(i).unwrap()];
                        *e = (e.0 + 1, e.1.max(now + gw as u64));
                    }
                }
                for t in &tables {
                    run_flush(&kv, t, None, &move || now + 500).await;
                }
            }
            let now = 1_000_000 + SECONDS * 1000;
            let (mut differ_hot, mut differ_cold, mut over) = (0, 0, 0);
            for i in 0..OBJECTS {
                let (tc, tl) = truth[usize::try_from(i).unwrap()];
                let r = record(&kv, id(i)).await.unwrap_or_default();
                if r.count > tc || r.last_ms > tl {
                    over += 1;
                }
                if decide(r.count, r.last_ms, now) != decide(tc, tl, now) {
                    if i < HOT {
                        differ_hot += 1;
                    } else {
                        differ_cold += 1;
                    }
                }
            }
            let s = [tables[0].snapshot(0), tables[1].snapshot(0)];
            let dropped: u64 = s.iter().map(|s| s.dropped_full).sum();
            eprintln!(
                "capacity {capacity}: decisions differ hot {differ_hot}/{HOT} cold {differ_cold}/{} dropped {dropped} of {} reads",
                OBJECTS - HOT,
                s[0].reads + s[1].reads
            );
            assert_eq!(over, 0, "a record above the truth");
            if exact {
                assert_eq!((differ_hot, differ_cold, dropped), (0, 0, 0));
            } else {
                assert!(dropped > 0, "the overflow case did not overflow");
                // Hot objects stay pending between flushes: their decisions hold.
                assert_eq!(differ_hot, 0);
            }
            for t in &tables {
                assert_conserved(t);
            }
        }
    }

    /// Review: a batch refused for a reason a smaller batch cannot avoid
    /// (the store unavailable, the cluster not writable) is not split: one
    /// failed transaction, its entries back once, then dropped.
    #[tokio::test]
    async fn a_refused_batch_is_not_split_and_returns_once() {
        let kv = FaultKv::new(MemKv::default());
        for i in 1..=8 {
            put_header(&kv, g(i), GenState::Committed).await;
        }
        let t = AccessTable::new(cfg(64, 512));
        for i in 1..=8 {
            t.note(g(i), 10);
        }
        kv.inject(|_| true, CommitFault::Unavailable, 1);
        run_flush(&kv, &t, None, &|| 20).await;
        let s = t.snapshot(0);
        assert_eq!((s.errors_backend, s.entries, s.flushes), (1, 8, 0), "{s:?}");
        // Back and written at the next flush.
        run_flush(&kv, &t, None, &|| 30).await;
        assert_eq!(record(&kv, g(3)).await.map(|r| r.count), Some(1));
        // Twice refused: dropped, counted.
        for i in 1..=8 {
            t.note(g(i), 40);
        }
        kv.inject(|_| true, CommitFault::Unavailable, 2);
        run_flush(&kv, &t, None, &|| 50).await;
        run_flush(&kv, &t, None, &|| 60).await;
        let s = t.snapshot(0);
        assert_eq!(
            (s.errors_backend, s.dropped_error, s.entries),
            (3, 8, 0),
            "{s:?}"
        );
        assert_conserved(&t);
    }

    /// A store whose commits wait for a permit (a stalled backend).
    #[derive(Debug)]
    struct Stalled {
        inner: MemKv,
        gate: Arc<tokio::sync::Semaphore>,
    }
    struct StalledTx {
        inner: <MemKv as KvStore>::Tx,
        gate: Arc<tokio::sync::Semaphore>,
    }
    impl KvStore for Stalled {
        type Tx = StalledTx;
        async fn begin(&self) -> Result<StalledTx, KvError> {
            Ok(StalledTx {
                inner: self.inner.begin().await?,
                gate: Arc::clone(&self.gate),
            })
        }
        fn limits(&self) -> arca_lake_metadata::kv::Limits {
            self.inner.limits()
        }
    }
    impl Txn for StalledTx {
        async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
            self.inner.get(key).await
        }
        async fn get_range(
            &mut self,
            range: &arca_lake_metadata::kv::KeyRange,
            limit: usize,
            reverse: bool,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError> {
            self.inner.get_range(range, limit, reverse).await
        }
        fn set(&mut self, key: &[u8], value: &[u8]) {
            self.inner.set(key, value);
        }
        fn clear(&mut self, key: &[u8]) {
            self.inner.clear(key);
        }
        fn clear_range(&mut self, range: &arca_lake_metadata::kv::KeyRange) {
            self.inner.clear_range(range);
        }
        async fn commit(self) -> Result<u64, KvError> {
            if let Ok(p) = self.gate.acquire().await {
                p.forget();
            }
            self.inner.commit().await
        }
    }

    /// Review: a stop that gives up waiting while a commit is stalled
    /// counts what the table holds; the batch held by the flusher is the
    /// flusher's to count when its commit ends, once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stop_that_gives_up_on_a_stalled_commit_counts_every_read_once() {
        let kv = Arc::new(Stalled {
            inner: MemKv::default(),
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
        });
        for i in 1..=3 {
            let mut tx = kv.inner.begin().await.unwrap();
            let h = GenerationHeader {
                state: GenState::Committed,
                bucket_id: 1,
                key: format!("k{i}"),
                size: 1,
                etag: String::new(),
                content_type: None,
                user_metadata: Vec::new(),
                chunk_count: 0,
                created_ms: 1,
                kind: GenKind::Object,
                checksum: None,
            };
            tx.set(&keys::gen_header(&g(i)), &encode(&h).unwrap());
            tx.commit().await.unwrap();
        }
        let mut c = cfg(64, 2);
        c.flush_interval = Duration::from_millis(20);
        c.shutdown_flush = Duration::from_millis(100);
        let t = Arc::new(AccessTable::new(c));
        spawn_flusher(Arc::clone(&kv), &t);
        for i in 1..=3 {
            t.note(g(i), now_ms());
        }
        // The flusher takes a batch of two and stalls in its commit.
        for _ in 0..100 {
            if t.snapshot(0).in_flight > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(t.snapshot(0).in_flight, 2);
        tokio::time::timeout(Duration::from_secs(5), t.shutdown())
            .await
            .unwrap();
        let s = t.snapshot(0);
        assert_eq!((s.dropped_shutdown, s.uncertain), (1, 0), "{s:?}");
        // The commit goes through: those two reads are counted written,
        // and nothing else.
        kv.gate.add_permits(10);
        for _ in 0..100 {
            if t.snapshot(0).in_flight == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let s = t.snapshot(0);
        assert_eq!(
            (s.flushed_reads, s.dropped_shutdown, s.in_flight),
            (2, 1, 0),
            "{s:?}"
        );
        assert_conserved(&t);
    }

    /// Review: when one batch of a flush is refused, the batches taken with
    /// it are not sent (they go back unmarked) and the flusher waits its
    /// whole interval even if the table reaches its high watermark.
    #[tokio::test(start_paused = true)]
    async fn a_refusal_stops_the_flush_and_the_back_off_waits_the_interval() {
        let kv = Arc::new(FaultKv::new(MemKv::default()));
        for i in 1..=6 {
            put_header(kv.as_ref(), g(i), GenState::Committed).await;
        }
        // Three isolated generations and three others: four batches.
        let mut c = cfg(8, 512);
        c.flush_interval = Duration::from_secs(10);
        c.jitter = 0.0;
        let t = Arc::new(AccessTable::new(c));
        for i in 1..=3 {
            t.lock().isolated.insert(g(i), 8);
        }
        for i in 1..=6 {
            t.note(g(i), now_ms());
        }
        kv.inject(|_| true, CommitFault::Unavailable, 1);
        run_flush(kv.as_ref(), &t, None, &now_ms).await;
        let s = t.snapshot(0);
        assert_eq!(
            (s.errors_backend, s.flushes, s.entries),
            (1, 0, 6),
            "one batch sent: {s:?}"
        );
        // Spawned with the back-off on: a high-watermark wake does not end it.
        spawn_flusher(Arc::clone(&kv), &t);
        tokio::task::yield_now().await;
        for i in 7..=8 {
            t.note(g(i), now_ms());
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(t.snapshot(0).flushes, 0, "flushed during the back-off");
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert!(t.snapshot(0).flushes > 0);
        assert_eq!(record(kv.as_ref(), g(4)).await.map(|r| r.count), Some(1));
        assert_conserved(&t);
    }

    /// Review: entries put back after a refusal keep the table's order,
    /// oldest first (so the oldest-age gauge reads the oldest one).
    #[tokio::test]
    async fn entries_put_back_after_a_refusal_keep_their_order() {
        let kv = FaultKv::new(MemKv::default());
        for i in 1..=6 {
            put_header(&kv, g(i), GenState::Committed).await;
        }
        let t = AccessTable::new(cfg(64, 512));
        // g(1) and g(2) isolated: batches [g1], [g2], [g3..g6].
        for i in 1..=2 {
            t.lock().isolated.insert(g(i), 8);
        }
        for i in 1..=6 {
            t.note(g(i), 100 * u64::from(i));
        }
        kv.inject(|_| true, CommitFault::Unavailable, 1);
        run_flush(&kv, &t, None, &|| 1_000).await;
        let order: Vec<u8> = t.lock().order.iter().map(|id| id[15]).collect();
        // The refused g(1) goes back marked, behind; the others untried,
        // in their order.
        assert_eq!(order, vec![2, 3, 4, 5, 6, 1]);
        assert_eq!(t.snapshot(1_000).oldest_age_ms, 800);
        assert_conserved(&t);
    }

    fn atomic_cfg() -> AccessConfig {
        AccessConfig {
            atomic: true,
            ..cfg(64, 512)
        }
    }

    /// ADR-0035 option B (experimental): the atomic layout sums and keeps
    /// the latest time, writes nothing for a finished generation, and is
    /// read together with a legacy record.
    #[tokio::test]
    async fn the_atomic_layout_sums_and_skips_finished_generations() {
        let kv = MemKv::default();
        put_header(&kv, g(1), GenState::Committed).await;
        object::record_access(&kv, &g(1), 5, 2).await.unwrap();
        let t = AccessTable::new(atomic_cfg());
        for (id, at) in [(1, 10), (1, 30), (3, 40)] {
            t.note(g(id), at);
        }
        run_flush(&kv, &t, None, &|| 50).await;
        assert_eq!(
            record(&kv, g(1)).await,
            Some(object::AccessRecord {
                last_ms: 30,
                count: 4
            })
        );
        assert_eq!(record(&kv, g(3)).await, None, "an orphan was created");
        let mut tx = kv.begin().await.unwrap();
        assert_eq!(tx.get(&keys::access_count(&g(3))).await.unwrap(), None);
        assert_eq!(t.snapshot(0).skipped_finished, 1);
        assert_conserved(&t);
    }

    /// GC's finish clears the atomic keys with the generation; a reset
    /// leaves `{now, 0}` and later flushes add to it.
    #[tokio::test]
    async fn gc_and_reset_cover_the_atomic_layout() {
        let kv = MemKv::default();
        put_header(&kv, g(1), GenState::Committed).await;
        let t = AccessTable::new(atomic_cfg());
        t.note(g(1), 10);
        t.note(g(1), 20);
        run_flush(&kv, &t, None, &|| 30).await;
        object::reset_access(&kv, &g(1), 100).await.unwrap();
        assert_eq!(
            record(&kv, g(1)).await,
            Some(object::AccessRecord {
                last_ms: 100,
                count: 0
            })
        );
        t.note(g(1), 110);
        run_flush(&kv, &t, None, &|| 120).await;
        assert_eq!(
            record(&kv, g(1)).await,
            Some(object::AccessRecord {
                last_ms: 110,
                count: 1
            })
        );
        put_header(&kv, g(1), GenState::Retiring { since_ms: 1 }).await;
        arca_lake_metadata::ops::gc::finish(&kv, &g(1))
            .await
            .unwrap();
        assert_eq!(record(&kv, g(1)).await, None);
        let mut tx = kv.begin().await.unwrap();
        assert_eq!(tx.get(&keys::access_count(&g(1))).await.unwrap(), None);
        assert_eq!(tx.get(&keys::access_last(&g(1))).await.unwrap(), None);
        // A read still pending does not bring it back.
        t.note(g(1), 130);
        run_flush(&kv, &t, None, &|| 140).await;
        assert_eq!(record(&kv, g(1)).await, None);
        assert_conserved(&t);
    }

    /// Unknown outcomes with the atomic layout: counted uncertain, never
    /// resent, so never counted twice.
    #[tokio::test]
    async fn an_unknown_outcome_is_never_counted_twice_in_the_atomic_layout() {
        for (fault, applied) in [
            (CommitFault::UnknownAfterApply, true),
            (CommitFault::UnknownWithoutApply, false),
            (CommitFault::TimeoutAfterApply, true),
            (CommitFault::TimeoutWithoutApply, false),
        ] {
            let kv = FaultKv::new(MemKv::default());
            put_header(&kv, g(1), GenState::Committed).await;
            let t = AccessTable::new(atomic_cfg());
            for _ in 0..3 {
                t.note(g(1), 10);
            }
            kv.inject_on_key(keys::access_count(&g(1)), fault, 1);
            run_flush(&kv, &t, None, &|| 20).await;
            assert_eq!(t.snapshot(0).uncertain, 3, "{fault:?}");
            t.note(g(1), 30);
            run_flush(&kv, &t, None, &|| 40).await;
            assert_eq!(
                record(&kv, g(1)).await.map(|r| r.count),
                Some(if applied { 4 } else { 1 }),
                "{fault:?}"
            );
            assert_conserved(&t);
        }
    }
}
