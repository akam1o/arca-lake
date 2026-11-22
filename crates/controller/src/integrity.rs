//! Background scrubbing, corruption classification and stripe repair
//! (RFC-008 §8, ADR-0015).
//!
//! The scrub pass deep-verifies every shard of a group on its node
//! (`ScrubShards`), classifies each one against the metadata (RFC-008
//! §8.4), walks the merged member listings for missing shards, and
//! records what it finds as repair tasks. The stripe repair worker drives
//! each task through Classify → Plan → Reconstruct → Verify → Commit
//! (RFC-008 §8.5): it writes only bytes that decode to the chunk whose
//! CRC-32C and length match the chunk record (I11), under the epoch of
//! the group record it planned with (I12), and never deletes anything
//! (I16). Scrub progress and task state live in metadata, so a controller
//! failover resumes where the previous leader stopped.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use arca_lake_common::split_chunk_id;
use arca_lake_ec::ErasureCoder;
use arca_lake_ec::StripeLayout;
use arca_lake_metadata::ops::integrity::{
    self, Finding, ManifestFinding, RepairState, RepairTask, ScrubCursor, ScrubPhase,
};
use arca_lake_metadata::ops::{cluster, gc};
use arca_lake_metadata::records::{ChunkRecord, GroupRecord, GroupRole, NodeRecord};
use arca_lake_metadata::{KvStore, MetaError};
use arca_lake_protocol::client::{NodeClients, ShardRef, ShardStatus};
use arca_lake_tiering::durable::{Coders, encode_chunk};
use bytes::Bytes;
use futures::StreamExt as _;
use tracing::{info, warn};

use crate::ControlError;
use crate::metrics::Metrics;
use crate::walk::GroupWalker;

/// Shutdown signal checked between pages and tasks (and inside pacing
/// sleeps): a pass stops promptly when it flips to `true`; the persisted
/// cursor or task state resumes it later.
pub type Cancel<'a> = Option<&'a tokio::sync::watch::Receiver<bool>>;

/// Whether the shutdown signal fired.
#[must_use]
pub fn is_cancelled(cancel: Cancel<'_>) -> bool {
    cancel.is_some_and(|c| *c.borrow())
}

fn cancelled(cancel: Cancel<'_>) -> bool {
    is_cancelled(cancel)
}

/// Scrub scheduler options (RFC-008 §8.3).
#[derive(Debug, Clone)]
pub struct ScrubOptions {
    /// Shards per `ScrubShards` RPC (upper bound).
    pub page: u32,
    /// Payload bytes per `ScrubShards` RPC (the node stops after the shard
    /// that crosses it; 0 = unbounded). This is the burst a scrub puts on
    /// a device between two pacing sleeps, so it stays small.
    pub page_bytes: u64,
    /// Byte budget per second across groups (0 = unpaced).
    pub rate_bytes_per_s: u64,
    /// A group is due when its last pass finished longer ago than this
    /// (milliseconds); 0 disables scheduled scrubbing.
    pub interval_ms: u64,
    /// Groups scrubbed per pass invocation.
    pub concurrency: usize,
    /// Multipart roots examined per pass by the manifest scrub (R-55); 0
    /// disables the background manifest scrub.
    pub manifest_roots_per_pass: usize,
    /// Adapt the byte budget to the scrubbed node's load (ADR-0018);
    /// `None` = the fixed `rate_bytes_per_s`.
    pub adaptive: Option<crate::adaptive::AdaptivePolicy>,
}

impl Default for ScrubOptions {
    fn default() -> Self {
        Self {
            page: 256,
            page_bytes: 8 << 20,
            rate_bytes_per_s: 32 << 20,
            interval_ms: 24 * 3600 * 1000,
            concurrency: 1,
            manifest_roots_per_pass: 256,
            adaptive: None,
        }
    }
}

/// Stripe repair worker options (RFC-008 §8.5).
#[derive(Debug, Clone)]
pub struct RepairWorkerOptions {
    /// Tasks processed per pass.
    pub tasks_per_pass: usize,
    /// Attempts before a task is `Failed`.
    pub max_attempts: u32,
    /// Milliseconds before an `Unrecoverable` task is re-checked.
    pub recheck_ms: u64,
    /// Stripes repaired concurrently (Phase 3.3 §4).
    pub workers: usize,
    /// Byte budget per second over every shard the worker reads and
    /// writes (0 = unpaced). Separate from the scrub budget; the operator
    /// sizes the two so that their sum fits the devices.
    pub rate_bytes_per_s: u64,
    /// Repair shard transfers in flight per node at once.
    pub per_node_in_flight: usize,
    /// A node with more client PUT or GET streams open than this at the
    /// start of a pass is "busy": repair uses one transfer at a time on it
    /// (yielding to clients).
    pub busy_threshold: u64,
    /// Queue entries a pass scans for due tasks (Phase 3.11, M4), from the
    /// persisted scan cursor, in pages of at most
    /// `integrity::TASK_PAGE_MAX`, one transaction each; the cursor wraps
    /// at the end, so that every task is examined within
    /// ceil(queue / window) passes. At least `tasks_per_pass`.
    pub scan_window: usize,
}

impl Default for RepairWorkerOptions {
    fn default() -> Self {
        Self {
            tasks_per_pass: 256,
            max_attempts: 8,
            recheck_ms: 3600 * 1000,
            workers: 4,
            rate_bytes_per_s: 64 << 20,
            per_node_in_flight: 2,
            busy_threshold: 4,
            scan_window: 4096,
        }
    }
}

/// Shared limits of one repair pass: the byte budget and the per-node
/// transfer slots (narrowed to one on nodes that are busy with clients).
pub struct RepairBudget {
    pacer: tokio::sync::Mutex<Pacer>,
    slots: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Semaphore>>>,
    busy: std::collections::HashSet<String>,
    per_node: usize,
}

impl RepairBudget {
    fn new(opts: &RepairWorkerOptions, busy: std::collections::HashSet<String>) -> Self {
        Self {
            pacer: tokio::sync::Mutex::new(Pacer::new(opts.rate_bytes_per_s)),
            slots: std::sync::Mutex::new(HashMap::new()),
            busy,
            per_node: opts.per_node_in_flight.max(1),
        }
    }

    /// A transfer slot on `addr`.
    async fn slot(&self, addr: &str) -> tokio::sync::OwnedSemaphorePermit {
        let sem = {
            let mut m = self
                .slots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Arc::clone(m.entry(addr.to_string()).or_insert_with(|| {
                let n = if self.busy.contains(addr) {
                    1
                } else {
                    self.per_node
                };
                Arc::new(tokio::sync::Semaphore::new(n))
            }))
        };
        sem.acquire_owned().await.expect("semaphore open")
    }

    /// Account transferred bytes against the budget (sleeps when ahead).
    async fn account(&self, bytes: u64, cancel: Cancel<'_>) {
        self.pacer.lock().await.account(bytes, cancel).await;
    }
}

/// What one scrub pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrubReport {
    /// Groups whose pass completed.
    pub groups_completed: usize,
    /// Groups whose pass advanced but did not complete (budget or error).
    pub groups_in_progress: usize,
    /// Shards verified.
    pub verified: u64,
    /// Bytes read on nodes.
    pub bytes: u64,
    /// Findings by class ([`Finding::ALL`] order).
    pub findings: [u64; 8],
    /// Repair tasks created.
    pub enqueued: u64,
    /// Node or metadata errors (the pass continues).
    pub errors: u64,
    /// Multipart roots verified by the manifest scrub of this pass.
    pub manifest_roots: u64,
    /// Manifest findings of this pass (RFC-008 §8.6).
    pub manifest_findings: Vec<(arca_lake_metadata::records::GenId, ManifestFinding)>,
}

impl ScrubReport {
    /// Findings of one class.
    #[must_use]
    pub fn count(&self, f: Finding) -> u64 {
        self.findings[f.index()]
    }
}

/// What one repair worker pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairWorkerReport {
    /// Tasks examined.
    pub examined: usize,
    /// Tasks removed because the stripe was repaired (or already healthy).
    pub succeeded: usize,
    /// Shards rewritten.
    pub shards_rewritten: usize,
    /// Bytes written to nodes.
    pub bytes_written: u64,
    /// Tasks now `Unrecoverable`.
    pub unrecoverable: usize,
    /// Tasks that failed this pass (retried later).
    pub failed: usize,
    /// Tasks removed because their generation is no longer live.
    pub dropped: usize,
    /// Tasks deferred (not due, or epoch changed and re-planned).
    pub deferred: usize,
    /// Queue entries scanned for due tasks.
    pub scanned: usize,
    /// The scan reached the end of the queue and wrapped.
    pub wrapped: bool,
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

fn shard_ref(group: &GroupRecord, chunk_id: [u8; 16], i: usize) -> ShardRef {
    ShardRef {
        group_id: group.id,
        chunk_id,
        shard_index: u16::try_from(i).unwrap_or(u16::MAX),
    }
}

/// Expected shard length of a chunk record in `group`.
fn expected_shard_len(record: &ChunkRecord, group: &GroupRecord, coder: &dyn ErasureCoder) -> u64 {
    let layout = StripeLayout::new(
        usize::try_from(record.len).unwrap_or(0),
        usize::from(group.data_shards),
        coder.shard_alignment(),
    );
    layout.shard_len() as u64
}

fn is_deferrable(e: &MetaError) -> bool {
    matches!(
        e,
        MetaError::Kv(arca_lake_metadata::KvError::CommitUnknown(_)) | MetaError::TooManyRetries
    )
}

// ---- scrub ----------------------------------------------------------------

/// One page of the background manifest scrub from its persisted cursor:
/// a new pass starts when the interval since the last one passed (R-55).
async fn manifest_scrub_step<S: KvStore>(
    meta: &S,
    opts: &ScrubOptions,
    metrics: Option<&Metrics>,
    report: &mut ScrubReport,
    now_ms: u64,
) -> Result<(), ControlError> {
    // Manifest scrub (RFC-008 §8.6, R-55): one page of multipart roots per
    // pass from a persisted cursor, a new pass when the interval passed.
    let mut cur = match integrity::manifest_cursor(meta).await? {
        Some(c) if !c.done => c,
        Some(c) if now_ms.saturating_sub(c.finished_ms) >= opts.interval_ms => {
            integrity::ManifestCursor {
                pass: c.pass + 1,
                after: None,
                done: false,
                started_ms: now_ms,
                finished_ms: c.finished_ms,
                roots: 0,
                findings: 0,
            }
        }
        Some(c) => c,
        None => integrity::ManifestCursor {
            pass: 1,
            after: None,
            done: false,
            started_ms: now_ms,
            finished_ms: 0,
            roots: 0,
            findings: 0,
        },
    };
    if !cur.done {
        let r = integrity::verify_manifests(meta, cur.after, opts.manifest_roots_per_pass).await?;
        cur.roots += r.roots;
        cur.findings += r.findings.len() as u64;
        report.manifest_roots = r.roots;
        for (root, f) in &r.findings {
            warn!(root = %hex(root), finding = ?f, "multipart manifest finding (RFC-008 §8.6)");
        }
        if let Some(m) = metrics {
            m.manifest_roots_verified_total
                .fetch_add(r.roots, Ordering::Relaxed);
            m.manifest_findings_total
                .fetch_add(r.findings.len() as u64, Ordering::Relaxed);
        }
        report.manifest_findings = r.findings;
        if let Some(n) = r.next {
            cur.after = Some(n);
        } else {
            cur.done = true;
            cur.finished_ms = now_ms;
            if let Some(m) = metrics {
                m.manifest_findings.store(cur.findings, Ordering::Relaxed);
            }
        }
        integrity::set_manifest_cursor(meta, &cur).await?;
    }
    Ok(())
}

/// One scrub pass: scrub up to `opts.concurrency` due groups (resuming
/// cursors), paced by the byte budget.
///
/// # Errors
///
/// Metadata errors that are not deferrable (node errors are counted).
pub async fn scrub_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    coders: &Coders,
    opts: &ScrubOptions,
    metrics: Option<&Metrics>,
    cancel: Cancel<'_>,
    now_ms: u64,
) -> Result<ScrubReport, ControlError> {
    let mut report = ScrubReport::default();
    if opts.interval_ms == 0 {
        return Ok(report);
    }
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let groups = cluster::list_groups(meta).await?;
    let cursors: HashMap<u64, ScrubCursor> =
        integrity::scrub_cursors(meta).await?.into_iter().collect();
    // In-progress groups first, then the longest-unscrubbed.
    let mut due: Vec<(u64, &GroupRecord)> = Vec::new();
    for g in groups.iter().filter(|g| g.role == GroupRole::Durable) {
        match cursors.get(&g.id) {
            Some(c) if !c.is_done() => due.push((0, g)),
            Some(c) if now_ms.saturating_sub(c.finished_ms) >= opts.interval_ms => {
                due.push((c.finished_ms, g));
            }
            Some(_) => {}
            None => due.push((0, g)),
        }
    }
    due.sort_by_key(|(t, g)| (*t, g.id));
    if let Some(m) = metrics {
        m.scrub_groups_overdue
            .store(due.len() as u64, Ordering::Relaxed);
    }
    let mut budget = Pacer::new(opts.rate_bytes_per_s).with_adaptive(opts.adaptive);
    for (_, group) in due.into_iter().take(opts.concurrency.max(1)) {
        if cancelled(cancel) {
            break;
        }
        let cursor = match cursors.get(&group.id) {
            Some(c) if !c.is_done() => c.clone(),
            Some(c) => ScrubCursor::start(c.pass + 1, c.finished_ms, now_ms),
            None => ScrubCursor::start(1, 0, now_ms),
        };
        match scrub_group(
            meta,
            clients,
            coders,
            &nodes,
            group,
            cursor,
            opts,
            &mut budget,
            metrics,
            cancel,
            now_ms,
        )
        .await
        {
            Ok((done, part)) => {
                report.verified += part.verified;
                report.bytes += part.bytes;
                for (i, n) in part.findings.iter().enumerate() {
                    report.findings[i] += n;
                }
                report.enqueued += part.enqueued;
                report.errors += part.errors;
                if done {
                    report.groups_completed += 1;
                } else {
                    report.groups_in_progress += 1;
                }
            }
            Err(ControlError::Meta(e)) if is_deferrable(&e) => {
                report.errors += 1;
                report.groups_in_progress += 1;
            }
            Err(e) => return Err(e),
        }
    }
    // Manifest scrub (RFC-008 §8.6, R-55).
    if opts.manifest_roots_per_pass > 0 && !cancelled(cancel) {
        manifest_scrub_step(meta, opts, metrics, &mut report, now_ms).await?;
    }
    if let Some(m) = metrics {
        m.scrub_passes_total.fetch_add(1, Ordering::Relaxed);
    }
    Ok(report)
}

/// Verify every committed multipart root (RFC-008 §8.6); findings are
/// logged with the root generation. Returns `(roots verified, findings)`.
///
/// # Errors
///
/// Metadata errors.
pub async fn verify_manifests<S: KvStore>(
    meta: &S,
) -> Result<
    (
        u64,
        Vec<(arca_lake_metadata::records::GenId, ManifestFinding)>,
    ),
    ControlError,
> {
    let mut roots = 0u64;
    let mut findings = Vec::new();
    let mut after = None;
    loop {
        let r = integrity::verify_manifests(meta, after, 256).await?;
        roots += r.roots;
        for (root, f) in r.findings {
            warn!(root = %hex(&root), finding = ?f, "multipart manifest finding (RFC-008 §8.6)");
            findings.push((root, f));
        }
        match r.next {
            Some(n) => after = Some(n),
            None => return Ok((roots, findings)),
        }
    }
}

/// Byte budget per second: sleeps so that bytes verified so far fit it.
/// Adaptive (ADR-0018): the rate follows the scrubbed node's load.
struct Pacer {
    rate: u64,
    started: std::time::Instant,
    bytes: u64,
    adaptive: Option<Adapt>,
}

struct Adapt {
    aimd: crate::adaptive::Aimd,
    last_sample: std::time::Instant,
    repair_bytes: u64,
}

/// Shortest time between two load samples of an adaptive scrub.
const ADAPT_EVERY: std::time::Duration = std::time::Duration::from_secs(1);

impl Pacer {
    fn new(rate: u64) -> Self {
        Self {
            rate,
            started: std::time::Instant::now(),
            bytes: 0,
            adaptive: None,
        }
    }

    fn with_adaptive(mut self, policy: Option<crate::adaptive::AdaptivePolicy>) -> Self {
        if let Some(p) = policy {
            self.rate = p.base;
            self.adaptive = Some(Adapt {
                aimd: crate::adaptive::Aimd::new(p),
                last_sample: std::time::Instant::now(),
                repair_bytes: 0,
            });
        }
        self
    }

    /// A new rate takes effect from now (no burst to catch up, no debt).
    fn set_rate(&mut self, rate: u64) {
        if rate != self.rate {
            self.rate = rate;
            self.started = std::time::Instant::now();
            self.bytes = 0;
        }
    }

    /// Adaptive scrubbing: sample the node at `addr` (at most once per
    /// `ADAPT_EVERY`) and apply the new rate.
    async fn adapt<S: KvStore>(
        &mut self,
        meta: &S,
        clients: &NodeClients,
        addr: &str,
        metrics: Option<&Metrics>,
    ) {
        let Some(a) = self.adaptive.as_mut() else {
            return;
        };
        if a.last_sample.elapsed() < ADAPT_EVERY {
            return;
        }
        a.last_sample = std::time::Instant::now();
        let signals = clients
            .health(addr)
            .await
            .ok()
            .and_then(|h| crate::adaptive::Signals::from_health(&h));
        // Repair is active when it moved bytes since the last sample (this
        // process's repair worker) or has work queued (any process's).
        let repair_bytes = metrics.map_or(0, |m| m.repair_bytes_total.load(Ordering::Relaxed));
        let queued = integrity::tasks(meta, 256).await.is_ok_and(|t| {
            t.iter().any(|(_, _, t)| {
                matches!(t.state, RepairState::Detected | RepairState::Planned { .. })
            })
        });
        let repair_active = repair_bytes > a.repair_bytes || queued;
        a.repair_bytes = repair_bytes;
        let before = a.aimd.throttles;
        let rate = a.aimd.observe(signals, repair_active);
        let throttled = a.aimd.throttles - before;
        if rate != self.rate {
            tracing::info!(
                rate_mib_s = rate >> 20,
                signals = ?signals,
                repair_active,
                "adaptive scrub rate"
            );
        }
        if let Some(m) = metrics {
            m.scrub_rate_bytes_per_second.store(rate, Ordering::Relaxed);
            m.scrub_throttles_total
                .fetch_add(throttled, Ordering::Relaxed);
        }
        self.set_rate(rate);
    }

    async fn account(&mut self, bytes: u64, cancel: Cancel<'_>) {
        self.bytes += bytes;
        if self.rate == 0 {
            return;
        }
        let allowed = std::time::Duration::from_millis(self.bytes.saturating_mul(1000) / self.rate);
        let elapsed = self.started.elapsed();
        if allowed > elapsed {
            let sleep = tokio::time::sleep(allowed.saturating_sub(elapsed));
            match cancel {
                Some(c) => {
                    let mut c = c.clone();
                    tokio::select! {
                        () = sleep => {}
                        _ = c.changed() => {}
                    }
                }
                None => sleep.await,
            }
        }
    }
}

/// Scrub one group from its cursor until the pass completes or a page
/// fails. Returns `(done, report)`; the cursor is persisted after every
/// page.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn scrub_group<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    coders: &Coders,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    mut cursor: ScrubCursor,
    opts: &ScrubOptions,
    pacer: &mut Pacer,
    metrics: Option<&Metrics>,
    cancel: Cancel<'_>,
    now_ms: u64,
) -> Result<(bool, ScrubReport), ControlError> {
    let mut report = ScrubReport::default();
    let coder = coders
        .for_group(group)
        .map_err(|e| ControlError::Other(e.to_string()))?;
    let k = usize::from(group.data_shards);
    let finding = |cursor: &mut ScrubCursor, report: &mut ScrubReport, f: Finding| {
        cursor.findings[f.index()] += 1;
        report.findings[f.index()] += 1;
        if let Some(m) = metrics {
            m.integrity_findings_total[f.index()].fetch_add(1, Ordering::Relaxed);
        }
    };
    loop {
        match cursor.phase.clone() {
            ScrubPhase::Shards { member, after } => {
                let Some(m) = group.members.get(usize::from(member)) else {
                    cursor.phase = ScrubPhase::Presence { after: None };
                    integrity::set_scrub_cursor(meta, group.id, &cursor).await?;
                    continue;
                };
                let Some(node) = nodes.get(&m.node_id) else {
                    // Unknown node: skip the member this pass.
                    report.errors += 1;
                    cursor.errors += 1;
                    cursor.phase = ScrubPhase::Shards {
                        member: member + 1,
                        after: None,
                    };
                    integrity::set_scrub_cursor(meta, group.id, &cursor).await?;
                    continue;
                };
                let after_ref = after.map(|c| ShardRef {
                    group_id: group.id,
                    chunk_id: c,
                    shard_index: u16::MAX,
                });
                let page = match clients
                    .scrub_shards_page(
                        &node.address,
                        group.id,
                        after_ref,
                        opts.page,
                        opts.page_bytes,
                    )
                    .await
                {
                    Ok(p) => p,
                    Err(status) => {
                        warn!(group = group.id, member, code = ?status.code(), "scrub page failed; member skipped this pass");
                        report.errors += 1;
                        cursor.errors += 1;
                        if let Some(mm) = metrics {
                            mm.scrub_errors_total.fetch_add(1, Ordering::Relaxed);
                        }
                        cursor.phase = ScrubPhase::Shards {
                            member: member + 1,
                            after: None,
                        };
                        integrity::set_scrub_cursor(meta, group.id, &cursor).await?;
                        continue;
                    }
                };
                let mut last = after;
                for sh in &page.shards {
                    last = Some(sh.shard.chunk_id);
                    // Only the member's own shard index is its responsibility;
                    // other indexes on it are unexpected copies.
                    if usize::from(sh.shard.shard_index) != usize::from(member) {
                        finding(&mut cursor, &mut report, Finding::UnexpectedShard);
                        continue;
                    }
                    let status = match sh.status {
                        // A shard quarantined earlier keeps the class of the
                        // failure that quarantined it.
                        ShardStatus::Quarantined if sh.reason.contains("truncated") => {
                            ShardStatus::Truncated
                        }
                        ShardStatus::Quarantined if sh.reason == "payload block crc mismatch" => {
                            ShardStatus::ChecksumMismatch
                        }
                        ShardStatus::Quarantined => ShardStatus::InvalidHeader,
                        other => other,
                    };
                    match status {
                        ShardStatus::Ok => {}
                        ShardStatus::ChecksumMismatch | ShardStatus::Quarantined => {
                            finding(&mut cursor, &mut report, Finding::ChecksumMismatch);
                            if integrity::enqueue(
                                meta,
                                group.id,
                                sh.shard.chunk_id,
                                Finding::ChecksumMismatch,
                                &[member],
                                now_ms,
                            )
                            .await?
                            {
                                report.enqueued += 1;
                            }
                            continue;
                        }
                        ShardStatus::Truncated => {
                            finding(&mut cursor, &mut report, Finding::TruncatedShard);
                            if integrity::enqueue(
                                meta,
                                group.id,
                                sh.shard.chunk_id,
                                Finding::TruncatedShard,
                                &[member],
                                now_ms,
                            )
                            .await?
                            {
                                report.enqueued += 1;
                            }
                            continue;
                        }
                        ShardStatus::InvalidHeader => {
                            finding(&mut cursor, &mut report, Finding::InvalidHeader);
                            if integrity::enqueue(
                                meta,
                                group.id,
                                sh.shard.chunk_id,
                                Finding::InvalidHeader,
                                &[member],
                                now_ms,
                            )
                            .await?
                            {
                                report.enqueued += 1;
                            }
                            continue;
                        }
                        ShardStatus::IoError => {
                            report.errors += 1;
                            cursor.errors += 1;
                            continue;
                        }
                    }
                    cursor.verified += 1;
                    report.verified += 1;
                    // L3: metadata agreement.
                    let (prefix, index) = split_chunk_id(&sh.shard.chunk_id);
                    match gc::attribute_shard(meta, &prefix, index, group.id).await? {
                        gc::ShardAttribution::Referenced => {}
                        gc::ShardAttribution::NoGeneration | gc::ShardAttribution::Unreferenced => {
                            finding(&mut cursor, &mut report, Finding::UnexpectedShard);
                            continue;
                        }
                    }
                    if let Some(record) =
                        gc::chunk_record_in_group(meta, &prefix, index, group.id).await?
                    {
                        if record.placement.epoch > group.epoch {
                            finding(&mut cursor, &mut report, Finding::StalePlacementEpoch);
                            continue;
                        }
                        if sh.payload_len != expected_shard_len(&record, group, coder.as_ref()) {
                            finding(&mut cursor, &mut report, Finding::MetadataDataMismatch);
                            if integrity::enqueue(
                                meta,
                                group.id,
                                sh.shard.chunk_id,
                                Finding::MetadataDataMismatch,
                                &[member],
                                now_ms,
                            )
                            .await?
                            {
                                report.enqueued += 1;
                            }
                        }
                    }
                }
                cursor.bytes += page.bytes_read;
                report.bytes += page.bytes_read;
                if let Some(mm) = metrics {
                    mm.scrub_shards_verified_total
                        .fetch_add(page.shards.len() as u64, Ordering::Relaxed);
                    mm.scrub_bytes_total
                        .fetch_add(page.bytes_read, Ordering::Relaxed);
                }
                cursor.phase = match page.next {
                    Some(_) => ScrubPhase::Shards {
                        member,
                        after: last,
                    },
                    None => ScrubPhase::Shards {
                        member: member + 1,
                        after: None,
                    },
                };
                integrity::set_scrub_cursor(meta, group.id, &cursor).await?;
                pacer.adapt(meta, clients, &node.address, metrics).await;
                pacer.account(page.bytes_read, cancel).await;
                if cancelled(cancel) {
                    return Ok((false, report));
                }
            }
            ScrubPhase::Presence { after } => {
                let mut walker = GroupWalker::new(clients, nodes, group, after, opts.page);
                let mut seen = 0u32;
                let mut last = after;
                while let Some(chunk) = walker.next().await? {
                    last = Some(chunk.chunk_id);
                    seen += 1;
                    let (prefix, index) = split_chunk_id(&chunk.chunk_id);
                    let Some(record) =
                        gc::chunk_record_in_group(meta, &prefix, index, group.id).await?
                    else {
                        // Pending generation (no record yet) or garbage:
                        // reconciliation's domain.
                        continue;
                    };
                    let holders: Vec<usize> = chunk.holders().collect();
                    // A member whose listing failed is unreachable, not
                    // empty: its shards are neither present nor missing.
                    let unreachable = walker.unavailable();
                    let missing: Vec<u16> = (0..group.members.len())
                        .filter(|i| !holders.contains(i) && !unreachable.contains(i))
                        .map(|i| u16::try_from(i).unwrap_or(u16::MAX))
                        .collect();
                    if missing.is_empty() {
                        continue;
                    }
                    let class = if holders.len() + unreachable.len() < k {
                        Finding::UnrecoverableStripe
                    } else {
                        Finding::MissingShard
                    };
                    let _ = record;
                    // A stripe the shard phase already queued (its shard is
                    // quarantined, hence absent here) is not counted twice.
                    if integrity::enqueue(meta, group.id, chunk.chunk_id, class, &missing, now_ms)
                        .await?
                    {
                        finding(&mut cursor, &mut report, class);
                        report.enqueued += 1;
                    }
                    if seen >= opts.page {
                        break;
                    }
                }
                let unavailable = walker.unavailable();
                if !unavailable.is_empty() {
                    report.errors += unavailable.len() as u64;
                    cursor.errors += unavailable.len() as u64;
                }
                if seen >= opts.page {
                    cursor.phase = ScrubPhase::Presence { after: last };
                    integrity::set_scrub_cursor(meta, group.id, &cursor).await?;
                    if cancelled(cancel) {
                        return Ok((false, report));
                    }
                    continue;
                }
                cursor.phase = ScrubPhase::Done;
                cursor.finished_ms = now_ms;
                integrity::set_scrub_cursor(meta, group.id, &cursor).await?;
                info!(
                    group = group.id,
                    pass = cursor.pass,
                    verified = cursor.verified,
                    bytes = cursor.bytes,
                    findings = ?cursor.findings,
                    errors = cursor.errors,
                    "scrub pass complete"
                );
                return Ok((true, report));
            }
            ScrubPhase::Done => return Ok((true, report)),
        }
    }
}

// ---- stripe repair ----------------------------------------------------------

/// One repair worker pass over due tasks (RFC-008 §8.5): up to
/// `opts.workers` stripes at once under the byte budget.
///
/// # Errors
///
/// Metadata errors that are not deferrable.
#[allow(clippy::too_many_lines)]
pub async fn repair_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    coders: &Coders,
    opts: &RepairWorkerOptions,
    metrics: Option<&Metrics>,
    cancel: Cancel<'_>,
    now_ms: u64,
) -> Result<RepairWorkerReport, ControlError> {
    let mut report = RepairWorkerReport::default();
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    // Nodes busy with clients get one repair transfer at a time.
    let mut busy = std::collections::HashSet::new();
    for n in nodes.values() {
        if let Ok(h) = clients.health(&n.address).await
            && h.puts_in_flight + h.gets_in_flight > opts.busy_threshold
        {
            busy.insert(n.address.clone());
        }
    }
    let budget = RepairBudget::new(opts, busy);
    // Due tasks by priority (recoverable damage first, unrecoverable
    // re-checks last, oldest first within a class), among a window of the
    // queue scanned page by page from the persisted cursor (Phase 3.11,
    // M4): never the whole queue in one transaction, at most
    // `tasks_per_pass` kept in memory.
    let tasks = select_due(meta, opts, now_ms, &mut report).await?;
    report.examined = tasks.len();
    let mut stream = futures::stream::iter(tasks.into_iter().map(|(group_id, chunk_id, task)| {
        let budget = &budget;
        let nodes = &nodes;
        async move {
            if cancelled(cancel) {
                return Ok(Outcome::Replanned);
            }
            repair_one(
                meta, clients, coders, nodes, group_id, chunk_id, task, opts, budget, cancel,
                now_ms,
            )
            .await
        }
    }))
    .buffer_unordered(opts.workers.max(1));
    while let Some(outcome) = stream.next().await {
        match outcome {
            Ok(o) => match o {
                Outcome::Repaired { shards, bytes } => {
                    report.succeeded += 1;
                    report.shards_rewritten += shards;
                    report.bytes_written += bytes;
                    if let Some(m) = metrics {
                        m.stripe_repairs_succeeded_total
                            .fetch_add(1, Ordering::Relaxed);
                        m.shards_rewritten_total
                            .fetch_add(shards as u64, Ordering::Relaxed);
                        m.repair_bytes_total.fetch_add(bytes, Ordering::Relaxed);
                    }
                }
                Outcome::Healthy => report.succeeded += 1,
                Outcome::Dropped => report.dropped += 1,
                Outcome::Unrecoverable => report.unrecoverable += 1,
                Outcome::Failed => {
                    report.failed += 1;
                    if let Some(m) = metrics {
                        m.stripe_repairs_failed_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                Outcome::Replanned => report.deferred += 1,
            },
            Err(ControlError::Meta(e)) if is_deferrable(&e) => report.deferred += 1,
            Err(e) => return Err(e),
        }
    }
    drop(stream);
    if let Some(m) = metrics {
        // The whole queue counted (paged): every pass while it fits in one
        // page (one read), otherwise at most every 30 s, where a count per
        // pass would cost more than the pass.
        let last = m.repair_queue_counted_ms.load(Ordering::Relaxed);
        let small = m.repair_queue_depth.load(Ordering::Relaxed) <= integrity::TASK_PAGE_MAX as u64;
        if !small && last != 0 && now_ms.saturating_sub(last) < 30_000 {
            return Ok(report);
        }
        m.repair_queue_counted_ms
            .store(now_ms.max(1), Ordering::Relaxed);
        let st = integrity::queue_stats(meta).await?;
        for (i, n) in st.by_class.iter().enumerate() {
            m.integrity_findings_open[i].store(*n, Ordering::Relaxed);
        }
        let (total, unrecoverable, failed) = (st.total, st.unrecoverable, st.failed);
        m.repair_queue_depth.store(total, Ordering::Relaxed);
        m.repair_tasks_unrecoverable
            .store(unrecoverable, Ordering::Relaxed);
        m.repair_tasks_failed.store(failed, Ordering::Relaxed);
        let status = if unrecoverable > 0 {
            2
        } else {
            u64::from(total > 0)
        };
        m.data_integrity_status.store(status, Ordering::Relaxed);
    }
    Ok(report)
}

/// Priority of a task: recoverable damage first, then unexpected shards
/// and stale placements, unrecoverable re-checks last; within a class the
/// least recently touched first (a task re-planned at once goes behind
/// the others, so that none is starved; review).
fn priority(t: &RepairTask) -> (u8, u64) {
    let class = match t.reason {
        Finding::UnrecoverableStripe => 2,
        Finding::UnexpectedShard | Finding::StalePlacementEpoch => 1,
        _ => 0,
    };
    (class, t.updated_ms.max(t.created_ms))
}

/// One pass's tasks (Phase 3.11, M4): scan up to `opts.scan_window`
/// entries from the persisted cursor (wrapping once at the end), page by
/// page, keeping the `tasks_per_pass` due ones of highest priority; then
/// persist where the next pass starts.
async fn select_due<S: KvStore>(
    meta: &S,
    opts: &RepairWorkerOptions,
    now_ms: u64,
    report: &mut RepairWorkerReport,
) -> Result<Vec<(u64, [u8; 16], RepairTask)>, ControlError> {
    use std::collections::BinaryHeap;
    struct Ranked((u8, u64), u64, [u8; 16], RepairTask);
    impl PartialEq for Ranked {
        fn eq(&self, o: &Self) -> bool {
            (self.0, self.1, self.2) == (o.0, o.1, o.2)
        }
    }
    impl Eq for Ranked {}
    impl PartialOrd for Ranked {
        fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(o))
        }
    }
    impl Ord for Ranked {
        fn cmp(&self, o: &Self) -> std::cmp::Ordering {
            (self.0, self.1, self.2).cmp(&(o.0, o.1, o.2))
        }
    }
    let keep = opts.tasks_per_pass.max(1);
    let window = opts.scan_window.max(keep);
    let start = integrity::scan_cursor(meta).await?;
    // Max-heap of the best `keep` so far: the worst is popped.
    let mut best: BinaryHeap<Ranked> = BinaryHeap::new();
    let mut after = start.clone();
    let mut next_start = None;
    let mut wrapped = false;
    while report.scanned < window {
        let want = (window - report.scanned).min(integrity::TASK_PAGE_MAX);
        let (page, next) = integrity::tasks_page(meta, after.as_deref(), want).await?;
        let mut stop = false;
        for (g, c, t) in page {
            // After the wrap, stop where this pass started.
            if wrapped
                && start
                    .as_deref()
                    .is_some_and(|s| arca_lake_metadata::keys::repair_task(g, &c).as_slice() > s)
            {
                stop = true;
                break;
            }
            report.scanned += 1;
            next_start = Some(arca_lake_metadata::keys::repair_task(g, &c));
            if t.next_attempt_ms > now_ms {
                report.deferred += 1;
                continue;
            }
            best.push(Ranked(priority(&t), g, c, t));
            if best.len() > keep {
                best.pop();
            }
        }
        if stop {
            break;
        }
        match next {
            Some(k) => after = Some(k),
            None if !wrapped && start.is_some() => {
                // The end: wrap once, up to where this pass started.
                wrapped = true;
                report.wrapped = true;
                after = None;
            }
            None => {
                report.wrapped = true;
                next_start = None;
                break;
            }
        }
    }
    integrity::set_scan_cursor(meta, next_start).await?;
    let mut out: Vec<(u64, [u8; 16], RepairTask)> = best
        .into_iter()
        .map(|Ranked(_, g, c, t)| (g, c, t))
        .collect();
    out.sort_by_key(|(g, c, t)| (priority(t), *g, *c));
    Ok(out)
}

enum Outcome {
    Repaired { shards: usize, bytes: u64 },
    Healthy,
    Dropped,
    Unrecoverable,
    Failed,
    Replanned,
}

/// A member's shard as the node reports it at Classify.
#[derive(Debug)]
enum Source {
    Good(Vec<u8>),
    Bad(&'static str),
}

/// Drive one task through the stages of RFC-008 §8.5.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn repair_one<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    coders: &Coders,
    nodes: &HashMap<u64, NodeRecord>,
    group_id: u64,
    chunk_id: [u8; 16],
    mut task: RepairTask,
    opts: &RepairWorkerOptions,
    budget: &RepairBudget,
    cancel: Cancel<'_>,
    now_ms: u64,
) -> Result<Outcome, ControlError> {
    let cid = hex(&chunk_id);
    // ---- Classify: fresh group record, chunk record, every member's shard.
    let group = match cluster::get_group(meta, group_id).await {
        Ok(g) => g,
        Err(MetaError::NoSuchGroup(_)) => {
            integrity::remove_task(meta, group_id, &chunk_id).await?;
            return Ok(Outcome::Dropped);
        }
        Err(e) => return Err(e.into()),
    };
    let (prefix, index) = split_chunk_id(&chunk_id);
    let Some(record) = gc::chunk_record_in_group(meta, &prefix, index, group_id).await? else {
        // Only a pending generation's record may still arrive here. A
        // committed one with its record in another group (a write diverted
        // there while this group's shard was written) leaves nothing of
        // this group referenced: the task is dropped, never retried forever
        // (0.1.0 three-host validation).
        if gc::generation_is_pending(meta, &prefix).await? {
            // Pending generation: its record may still arrive. Try later.
            task.next_attempt_ms = now_ms + 60_000;
            task.updated_ms = now_ms;
            integrity::set_task(meta, group_id, &chunk_id, &task).await?;
            return Ok(Outcome::Replanned);
        }
        integrity::remove_task(meta, group_id, &chunk_id).await?;
        return Ok(Outcome::Dropped);
    };
    if record.placement.epoch > group.epoch {
        // Metadata ahead of the group (RFC-015): never write under a stale
        // epoch; the restore procedure reconciles epochs first.
        return fail(
            meta,
            group_id,
            &chunk_id,
            task,
            opts,
            now_ms,
            "placement epoch ahead of the group epoch",
        )
        .await;
    }
    let coder = coders
        .for_group(&group)
        .map_err(|e| ControlError::Other(e.to_string()))?;
    let k = usize::from(group.data_shards);
    let n = group.members.len();
    let shard_len = expected_shard_len(&record, &group, coder.as_ref());
    let mut sources: Vec<Source> = Vec::with_capacity(n);
    for (i, m) in group.members.iter().enumerate() {
        let Some(node) = nodes.get(&m.node_id) else {
            sources.push(Source::Bad("unknown node"));
            continue;
        };
        let slot = budget.slot(&node.address).await;
        let got = clients
            .get_shard(&node.address, shard_ref(&group, chunk_id, i), 0, 0)
            .await;
        drop(slot);
        if let Ok(b) = &got {
            budget.account(b.len() as u64, cancel).await;
        }
        match got {
            Ok(b) if b.len() as u64 == shard_len => sources.push(Source::Good(b.to_vec())),
            Ok(_) => sources.push(Source::Bad("length mismatch")),
            Err(s) if s.code() == tonic::Code::NotFound => sources.push(Source::Bad("missing")),
            Err(s) if s.code() == tonic::Code::DataLoss => sources.push(Source::Bad("corrupt")),
            Err(_) => sources.push(Source::Bad("unavailable")),
        }
    }
    let good: Vec<usize> = (0..n)
        .filter(|i| matches!(sources[*i], Source::Good(_)))
        .collect();
    // ---- Plan.
    if good.len() < k {
        let unavailable = sources
            .iter()
            .filter(|s| matches!(s, Source::Bad("unavailable" | "unknown node")))
            .count();
        if unavailable > 0 && good.len() + unavailable >= k {
            // Not enough answers, not a verdict: a node is down. Retry.
            return fail(
                meta,
                group_id,
                &chunk_id,
                task,
                opts,
                now_ms,
                "members unavailable",
            )
            .await;
        }
        if moved_away(meta, &prefix, index, group_id, &record).await? {
            integrity::remove_task(meta, group_id, &chunk_id).await?;
            return Ok(Outcome::Dropped);
        }
        return unrecoverable(meta, group_id, &chunk_id, task, good.len(), k, now_ms).await;
    }
    // ---- Reconstruct from a verifying k-subset of the good shards (I11).
    let Some((verified_set, shards)) =
        reconstruct_verified(coder.as_ref(), &sources, &good, k, n, &record)
    else {
        if moved_away(meta, &prefix, index, group_id, &record).await? {
            integrity::remove_task(meta, group_id, &chunk_id).await?;
            return Ok(Outcome::Dropped);
        }
        return unrecoverable(meta, group_id, &chunk_id, task, 0, k, now_ms).await;
    };
    let targets: Vec<usize> = (0..n).filter(|i| !verified_set.contains(i)).collect();
    if targets.is_empty() {
        integrity::remove_task(meta, group_id, &chunk_id).await?;
        return Ok(Outcome::Healthy);
    }
    // Moved (migration, rebalancing) while the members were read: nothing
    // to rebuild here (Phase 3.8 review).
    if moved_away(meta, &prefix, index, group_id, &record).await? {
        integrity::remove_task(meta, group_id, &chunk_id).await?;
        return Ok(Outcome::Dropped);
    }
    task.state = RepairState::Planned {
        targets: targets
            .iter()
            .map(|i| u16::try_from(*i).unwrap_or(u16::MAX))
            .collect(),
        epoch: group.epoch,
    };
    task.updated_ms = now_ms;
    integrity::set_task(meta, group_id, &chunk_id, &task).await?;
    // ---- Write under the planned epoch (I12), verifying the node's CRC.
    let mut bytes_written = 0u64;
    for &i in &targets {
        let Some(node) = nodes.get(&group.members[i].node_id) else {
            return fail(
                meta,
                group_id,
                &chunk_id,
                task,
                opts,
                now_ms,
                "target node unknown",
            )
            .await;
        };
        let expected = arca_lake_storage::format::crc32c(&shards[i]);
        let slot = budget.slot(&node.address).await;
        let put = clients
            .put_shard_as(
                &node.address,
                shard_ref(&group, chunk_id, i),
                group.epoch,
                shards[i].clone(),
                256 << 10,
                arca_lake_protocol::storage::v1::WriteClass::Repair,
            )
            .await;
        drop(slot);
        budget.account(shards[i].len() as u64, cancel).await;
        match put {
            Ok(r) if r.crc32c == expected => bytes_written += shards[i].len() as u64,
            Ok(r) => {
                return fail(
                    meta,
                    group_id,
                    &chunk_id,
                    task,
                    opts,
                    now_ms,
                    &format!("node stored crc {:08x}, expected {expected:08x}", r.crc32c),
                )
                .await;
            }
            Err(s) if NodeClients::is_epoch_rejected(&s) => {
                // Membership changed: re-plan from fresh reads next pass.
                task.state = RepairState::Detected;
                task.next_attempt_ms = now_ms;
                task.updated_ms = now_ms;
                task.last_error = Some("epoch changed during repair; re-planned".into());
                integrity::set_task(meta, group_id, &chunk_id, &task).await?;
                return Ok(Outcome::Replanned);
            }
            Err(s) => {
                return fail(
                    meta,
                    group_id,
                    &chunk_id,
                    task,
                    opts,
                    now_ms,
                    &format!("write failed: {}", s.code()),
                )
                .await;
            }
        }
    }
    // ---- Verify: read back every target (the node verifies block CRCs).
    for &i in &targets {
        let Some(node) = nodes.get(&group.members[i].node_id) else {
            return fail(
                meta,
                group_id,
                &chunk_id,
                task,
                opts,
                now_ms,
                "target node unknown",
            )
            .await;
        };
        let slot = budget.slot(&node.address).await;
        let back = clients
            .get_shard(&node.address, shard_ref(&group, chunk_id, i), 0, 0)
            .await;
        drop(slot);
        budget.account(shards[i].len() as u64, cancel).await;
        match back {
            Ok(b) if b.as_ref() == shards[i].as_ref() => {}
            Ok(_) => {
                return fail(
                    meta,
                    group_id,
                    &chunk_id,
                    task,
                    opts,
                    now_ms,
                    "read-back differs",
                )
                .await;
            }
            Err(s) => {
                return fail(
                    meta,
                    group_id,
                    &chunk_id,
                    task,
                    opts,
                    now_ms,
                    &format!("read-back failed: {}", s.code()),
                )
                .await;
            }
        }
    }
    // ---- Commit.
    integrity::remove_task(meta, group_id, &chunk_id).await?;
    info!(group = group_id, chunk = %cid, targets = ?targets, reason = ?task.reason, "stripe repaired");
    Ok(Outcome::Repaired {
        shards: targets.len(),
        bytes: bytes_written,
    })
}

/// Find a `k`-subset of the good shards from which the chunk decodes to
/// the recorded length and CRC; return it with the full re-encoded shard
/// vector. Tries the first `k` good shards, then (bounded) other subsets.
fn reconstruct_verified(
    coder: &dyn ErasureCoder,
    sources: &[Source],
    good: &[usize],
    k: usize,
    n: usize,
    record: &ChunkRecord,
) -> Option<(Vec<usize>, Vec<Bytes>)> {
    let try_subset = |subset: &[usize]| -> Option<Vec<Bytes>> {
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; n];
        for &i in subset {
            if let Source::Good(b) = &sources[i] {
                shards[i] = Some(b.clone());
            }
        }
        coder.reconstruct(&mut shards).ok()?;
        let layout = StripeLayout::new(
            usize::try_from(record.len).unwrap_or(0),
            k,
            coder.shard_alignment(),
        );
        let data: Vec<&[u8]> = shards[..k]
            .iter()
            .map(|s| s.as_deref().unwrap_or(&[]))
            .collect();
        let joined = layout.join(&data);
        if joined.len() as u64 != u64::from(record.len)
            || arca_lake_storage::format::crc32c(&joined) != record.crc32c
        {
            return None;
        }
        // Re-encode from the verified chunk so that every target gets bytes
        // derived from the verified data, not from a possibly wrong parity.
        let (_, encoded) = encode_chunk(coder, k, &joined).ok()?;
        Some(encoded)
    };
    // Fast path: the first k good shards.
    if let Some(enc) = try_subset(&good[..k]) {
        // Members whose current bytes equal the re-encoded ones need no write.
        let keep: Vec<usize> = (0..n)
            .filter(|&i| matches!(&sources[i], Source::Good(b) if b.as_slice() == enc[i].as_ref()))
            .collect();
        return Some((keep, enc));
    }
    // Some good-looking shard carries wrong bytes: search other subsets
    // (bounded; n ≤ 16 keeps C(n,k) small for realistic profiles).
    if good.len() > 16 {
        return None;
    }
    for subset in combinations(good, k) {
        if let Some(enc) = try_subset(&subset) {
            let keep: Vec<usize> = (0..n)
                .filter(
                    |&i| matches!(&sources[i], Source::Good(b) if b.as_slice() == enc[i].as_ref()),
                )
                .collect();
            return Some((keep, enc));
        }
    }
    None
}

fn combinations(items: &[usize], k: usize) -> Vec<Vec<usize>> {
    let mut out = Vec::new();
    let mut idx: Vec<usize> = (0..k).collect();
    if k > items.len() {
        return out;
    }
    loop {
        out.push(idx.iter().map(|&i| items[i]).collect());
        // Advance.
        let mut i = k;
        loop {
            if i == 0 {
                return out;
            }
            i -= 1;
            if idx[i] != i + items.len() - k {
                break;
            }
            if i == 0 {
                return out;
            }
        }
        idx[i] += 1;
        for j in i + 1..k {
            idx[j] = idx[j - 1] + 1;
        }
        if out.len() > 4096 {
            return out;
        }
    }
}

async fn unrecoverable<S: KvStore>(
    meta: &S,
    group_id: u64,
    chunk_id: &[u8; 16],
    mut task: RepairTask,
    have: usize,
    need: usize,
    now_ms: u64,
) -> Result<Outcome, ControlError> {
    let since = match task.state {
        RepairState::Unrecoverable { since_ms, .. } => since_ms,
        _ => now_ms,
    };
    task.state = RepairState::Unrecoverable {
        have: u16::try_from(have).unwrap_or(u16::MAX),
        need: u16::try_from(need).unwrap_or(u16::MAX),
        since_ms: since,
    };
    task.next_attempt_ms = now_ms + 3600 * 1000;
    task.updated_ms = now_ms;
    integrity::set_task(meta, group_id, chunk_id, &task).await?;
    warn!(group = group_id, chunk = %hex(chunk_id), have, need, "stripe unrecoverable (I15): readers get an integrity error");
    Ok(Outcome::Unrecoverable)
}

#[allow(clippy::too_many_arguments)]
async fn fail<S: KvStore>(
    meta: &S,
    group_id: u64,
    chunk_id: &[u8; 16],
    mut task: RepairTask,
    opts: &RepairWorkerOptions,
    now_ms: u64,
    why: &str,
) -> Result<Outcome, ControlError> {
    task.attempts += 1;
    task.last_error = Some(why.to_string());
    task.updated_ms = now_ms;
    if task.attempts >= opts.max_attempts {
        task.state = RepairState::Failed {
            attempts: task.attempts,
        };
        task.next_attempt_ms = now_ms + opts.recheck_ms;
    } else {
        task.state = RepairState::Detected;
        let backoff = 30_000u64.saturating_mul(1u64 << task.attempts.min(7));
        task.next_attempt_ms = now_ms + backoff.min(3600 * 1000);
    }
    integrity::set_task(meta, group_id, chunk_id, &task).await?;
    warn!(group = group_id, chunk = %hex(chunk_id), attempts = task.attempts, why, "stripe repair attempt failed");
    Ok(Outcome::Failed)
}

/// Reset a group's scrub cursor so that the next pass starts now
/// (`admin scrub run`).
///
/// # Errors
///
/// Metadata errors.
pub async fn request_scrub<S: KvStore>(
    meta: &S,
    group_id: u64,
    now_ms: u64,
) -> Result<(), ControlError> {
    let cursor = match integrity::scrub_cursor(meta, group_id).await? {
        Some(c) => ScrubCursor::start(c.pass + 1, 0, now_ms),
        None => ScrubCursor::start(1, 0, now_ms),
    };
    integrity::set_scrub_cursor(meta, group_id, &cursor).await?;
    Ok(())
}

/// Scrub one group to completion regardless of its interval (tests and
/// `admin scrub run --wait`). Returns the report of the pass.
///
/// # Errors
///
/// Metadata errors.
pub async fn scrub_group_now<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    coders: &Coders,
    group_id: u64,
    opts: &ScrubOptions,
    now_ms: u64,
) -> Result<ScrubReport, ControlError> {
    scrub_group_now_with(meta, clients, coders, group_id, opts, None, now_ms).await
}

/// [`scrub_group_now`], recording into `metrics` (the adaptive rate and
/// throttles among them).
///
/// # Errors
///
/// As [`scrub_group_now`].
pub async fn scrub_group_now_with<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    coders: &Coders,
    group_id: u64,
    opts: &ScrubOptions,
    metrics: Option<&Metrics>,
    now_ms: u64,
) -> Result<ScrubReport, ControlError> {
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let group = cluster::get_group(meta, group_id).await?;
    let cursor = match integrity::scrub_cursor(meta, group_id).await? {
        Some(c) if !c.is_done() => c,
        Some(c) => ScrubCursor::start(c.pass + 1, c.finished_ms, now_ms),
        None => ScrubCursor::start(1, 0, now_ms),
    };
    // The adaptive policy applies here too (it was ignored, so a
    // `scrub --wait --adaptive` ran at the fixed rate).
    let mut pacer = Pacer::new(opts.rate_bytes_per_s).with_adaptive(opts.adaptive);
    let (_, report) = scrub_group(
        meta, clients, coders, &nodes, &group, cursor, opts, &mut pacer, metrics, None, now_ms,
    )
    .await?;
    Ok(report)
}

/// Shared coders for the controller (one cache per process).
#[must_use]
pub fn coders() -> Arc<Coders> {
    Arc::new(Coders::default())
}

/// Whether the chunk's record no longer places it as `record` did in
/// `group_id` (moved since the repair read it).
async fn moved_away<S: KvStore>(
    meta: &S,
    prefix: &[u8; 12],
    index: u32,
    group_id: u64,
    record: &ChunkRecord,
) -> Result<bool, ControlError> {
    Ok(gc::chunk_record_in_group(meta, prefix, index, group_id)
        .await?
        .is_none_or(|r| r.placement != record.placement))
}
