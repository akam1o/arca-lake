//! Garbage collection (RFC-004 §5.10, RFC-008).
//!
//! Three sources of garbage:
//! 1. Pending generations whose lease expired → retired.
//! 2. Retired generations past the grace period → shards deleted on their
//!    members, then metadata removed.
//! 3. Shards on nodes that no committed, pending, or retiring generation
//!    accounts for → deleted by reconciliation.
//!
//! Time alone never decides a deletion. Before any shard of a generation is
//! deleted, one transaction re-reads the generation and verifies that it is
//! `Retiring` and that neither the current pointer nor a version record of
//! its key references it (RFC-008 invariant I2). Reconciliation deletes a
//! shard only when no generation with its prefix exists in metadata, or
//! when the node is no longer a member of the group and the group is not
//! being repaired; the age threshold is an additional guard, not the
//! criterion.

use std::collections::HashMap;

use futures::StreamExt as _;

use arca_lake_common::{split_chunk_id, uuid_v7_unix_ms};
use arca_lake_metadata::ops::{cluster, gc, multipart, object};
use arca_lake_metadata::records::{
    CachePlacement, ChunkRecord, CurrentPointer, GenId, GenState, GenerationHeader, GroupRecord,
    GroupState, MultipartState, NodeRecord, VersionRecord, decode,
};
use arca_lake_metadata::{KvStore, MetaError, Txn, keys, transact};
use arca_lake_protocol::client::{NodeClients, RecordToken, ShardRef};

use crate::ControlError;

/// Timing and volume of GC work by stage (Phase 3.6, R-64): what a pass
/// spends where, to tell metadata cost from node I/O. Closed label set.
#[derive(Debug, Default)]
pub struct GcStats {
    /// Where the next batch reads the retired queue from (Phase 3.10
    /// audit): batches page through it and wrap at its end, so that
    /// generations deferred at its head do not hold up the rest.
    pub cursor: std::sync::Mutex<Option<arca_lake_metadata::kv::Key>>,
    /// Test hook (Phase 3.11): when set, every generation waits here for a
    /// permit between its reference check and its shard deletes, so that a
    /// test can hold a batch whose deletes are decided but not sent.
    pub delete_gate: std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Semaphore>>>,
    /// Generations that reached [`Self::delete_gate`].
    pub gated: std::sync::atomic::AtomicU64,
    /// `expire_pending`: one transaction per expired lease.
    pub expire: arca_lake_common::timing::Histogram,
    /// `collectable`: reading the retired queue (candidate discovery).
    pub discover: arca_lake_common::timing::Histogram,
    /// One `finish_root_step` transaction of a multipart root.
    pub root_step: arca_lake_common::timing::Histogram,
    /// `verify_unreferenced`: the reference check of one generation.
    pub verify: arca_lake_common::timing::Histogram,
    /// One DeleteShard RPC.
    pub delete_rpc: arca_lake_common::timing::Histogram,
    /// Every shard (and cache copy) delete of one generation.
    pub delete_generation: arca_lake_common::timing::Histogram,
    /// `finish`: removing one generation's metadata.
    pub finish: arca_lake_common::timing::Histogram,
    /// One generation end to end (`collect_one`).
    pub collect: arca_lake_common::timing::Histogram,
    /// One `run_once` batch.
    pub batch: arca_lake_common::timing::Histogram,
    /// Generations collected (finished), roots among them.
    pub collected: std::sync::atomic::AtomicU64,
    /// See `collected`.
    pub collected_roots: std::sync::atomic::AtomicU64,
    /// Part generations a root step retired.
    pub parts_retired: std::sync::atomic::AtomicU64,
    /// Physical bytes of the shards GC deleted (or found already gone):
    /// each chunk's shard size times its shard count.
    pub reclaimed_bytes: std::sync::atomic::AtomicU64,
    /// Metadata transactions GC ran, by kind.
    pub txn_verify: std::sync::atomic::AtomicU64,
    /// See `txn_verify`.
    pub txn_finish: std::sync::atomic::AtomicU64,
    /// See `txn_verify`.
    pub txn_root_step: std::sync::atomic::AtomicU64,
}

impl GcStats {
    /// Histograms by stage label (the `stage` label of
    /// `arca_controller_gc_stage_seconds`).
    #[must_use]
    pub fn stages(&self) -> [(&'static str, &arca_lake_common::timing::Histogram); 9] {
        [
            ("expire", &self.expire),
            ("discover", &self.discover),
            ("root_step", &self.root_step),
            ("verify", &self.verify),
            ("delete_rpc", &self.delete_rpc),
            ("delete_generation", &self.delete_generation),
            ("finish", &self.finish),
            ("collect", &self.collect),
            ("batch", &self.batch),
        ]
    }
}

fn bump(a: &std::sync::atomic::AtomicU64, n: u64) {
    a.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}

/// Physical bytes of one chunk's shards (each `ceil(len / k)`, `k + m` of
/// them).
fn chunk_physical_bytes(c: &ChunkRecord) -> u64 {
    let k = u64::from(c.data_shards.max(1));
    u64::from(c.len).div_ceil(k) * (k + u64::from(c.parity_shards))
}

/// What a GC pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Pending generations retired because their lease expired.
    pub expired: usize,
    /// Generations whose shards were deleted and metadata removed.
    pub collected: usize,
    /// Shard deletions issued.
    pub shards_deleted: usize,
    /// Generations in the retired queue that turned out to be referenced or
    /// not retiring; left untouched. Any non-zero value is an invariant
    /// violation worth investigating.
    pub skipped_referenced: usize,
    /// Generations skipped because a member was unreachable or a metadata
    /// outcome was unknown (retried next pass).
    pub deferred: usize,
    /// The batch read a full page of the queue: more follows it.
    pub more: bool,
}

/// What reconciling one node did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Shards whose generation has no metadata at all.
    pub orphans_deleted: usize,
    /// Shards on a node that is no longer a member of the group.
    pub former_member_deleted: usize,
    /// Shards kept because a generation with their prefix exists.
    pub kept_referenced: usize,
    /// Shards kept because they are younger than the minimum age.
    pub kept_young: usize,
    /// Shards whose delete the node refused because the record changed
    /// since the listing (written again under the same key: kept).
    pub kept_changed: usize,
    /// Shards whose delete the delete fence refused (closed, or not yet
    /// adopted by the node, ADR-0023): kept, retried by a later pass.
    pub kept_fenced: usize,
    /// Shards left because their node reports no record sequence (an older
    /// node: a delete there could not be made conditional; reclaimed once
    /// the node is upgraded, Phase 3.5 audit CON-12).
    pub kept_no_sequence: usize,
    /// Retired already under this incarnation: not deleted again (Phase
    /// 3.12).
    pub already_retired: usize,
}

impl ReconcileReport {
    /// Total deletions.
    #[must_use]
    pub fn deleted(&self) -> usize {
        self.orphans_deleted + self.former_member_deleted
    }
}

/// One pass of generation-driven GC.
///
/// # Errors
///
/// Metadata errors. Node errors and unknown outcomes defer the generation
/// instead of failing the pass.
pub async fn run_once<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    now_ms: u64,
    grace_ms: u64,
    limit: usize,
) -> Result<GcReport, ControlError> {
    run_once_with(meta, clients, now_ms, grace_ms, limit, &GcStats::default()).await
}

/// [`run_once`], recording what each stage took into `stats`.
///
/// # Errors
///
/// As [`run_once`].
pub async fn run_once_with<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    now_ms: u64,
    grace_ms: u64,
    limit: usize,
    stats: &GcStats,
) -> Result<GcReport, ControlError> {
    let started = std::time::Instant::now();
    // An unknown commit outcome on a GC transaction is never retried here:
    // every GC step re-reads the state it changes, so the next pass either
    // sees the change applied or does it again.
    let mut report = GcReport::default();
    let t = std::time::Instant::now();
    match gc::expire_pending(meta, now_ms, limit).await {
        Ok(expired) => report.expired = expired.len(),
        Err(e) if is_deferrable(&e) => report.deferred += 1,
        Err(e) => return Err(e.into()),
    }
    stats.expire.observe(t.elapsed());

    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let groups: HashMap<u64, GroupRecord> = cluster::list_groups(meta)
        .await?
        .into_iter()
        .map(|g| (g.id, g))
        .collect();

    // Generations are independent (each is verified, its shards deleted,
    // then finished, every step re-read and idempotent), so several are
    // collected at once: one at a time, every shard delete waited for a
    // device sync and GC fell behind a sustained write rate (soak test).
    let t = std::time::Instant::now();
    let after = stats
        .cursor
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let (gens, next) = gc::collectable_page(meta, now_ms, grace_ms, after.as_ref(), limit).await?;
    report.more = next.is_some();
    *stats
        .cursor
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
    stats.discover.observe(t.elapsed());
    let work: Vec<_> = gens
        .iter()
        .map(|g| {
            Box::pin(collect_one(
                meta, clients, &nodes, &groups, g, now_ms, stats,
            ))
        })
        .collect();
    let outcomes: Vec<Result<Outcome, ControlError>> = futures::stream::iter(work)
        .buffer_unordered(GC_CONCURRENCY)
        .collect()
        .await;
    for o in outcomes {
        match o? {
            Outcome::Collected(n) => {
                report.collected += 1;
                report.shards_deleted += n;
            }
            Outcome::Deleted(n) => {
                report.deferred += 1;
                report.shards_deleted += n;
            }
            Outcome::Deferred => report.deferred += 1,
            Outcome::Referenced => report.skipped_referenced += 1,
        }
    }
    stats.batch.observe(started.elapsed());
    Ok(report)
}

/// Generations collected at once.
const GC_CONCURRENCY: usize = 32;
/// Shard deletes of one generation in flight at once.
const DELETE_CONCURRENCY: usize = 16;

/// What happened to one generation.
enum Outcome {
    /// Shards deleted (this many) and the generation finished.
    Collected(usize),
    /// Shards deleted, finishing deferred.
    Deleted(usize),
    /// Left for the next pass.
    Deferred,
    /// Still referenced (invariant violation: nothing deleted).
    Referenced,
}

async fn collect_one<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    groups: &HashMap<u64, GroupRecord>,
    generation: &GenId,
    now_ms: u64,
    stats: &GcStats,
) -> Result<Outcome, ControlError> {
    let started = std::time::Instant::now();
    let r = collect_one_inner(meta, clients, nodes, groups, generation, now_ms, stats).await;
    stats.collect.observe(started.elapsed());
    r
}

#[allow(clippy::too_many_arguments)]
async fn collect_one_inner<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    groups: &HashMap<u64, GroupRecord>,
    generation: &GenId,
    now_ms: u64,
    stats: &GcStats,
) -> Result<Outcome, ControlError> {
    // A multipart root first retires its parts in bounded steps (I-MP2);
    // its own metadata is finished only once no reference remains.
    let root = match finish_root_parts(meta, generation, now_ms, stats).await {
        Ok(root) => root,
        Err(e) if is_deferrable(&e) => return Ok(Outcome::Deferred),
        Err(e) => return Err(e.into()),
    };
    let t = std::time::Instant::now();
    let verified = verify_unreferenced(meta, generation).await;
    stats.verify.observe(t.elapsed());
    bump(&stats.txn_verify, 1);
    let (chunks, cached) = match verified {
        Ok(Some(found)) => found,
        Ok(None) => return Ok(Outcome::Referenced),
        Err(e) if is_deferrable(&e) => return Ok(Outcome::Deferred),
        Err(e) => return Err(e.into()),
    };
    let gate = stats
        .delete_gate
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if let Some(gate) = gate {
        stats
            .gated
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Ok(p) = gate.acquire().await {
            p.forget();
        }
    }
    let t = std::time::Instant::now();
    let deleted = match delete_shards(clients, nodes, groups, &chunks, stats).await {
        Ok(n) => delete_cache_copies(clients, nodes, groups, generation, &cached)
            .await
            .map(|c| n + c),
        Err(e) => Err(e),
    };
    stats.delete_generation.observe(t.elapsed());
    match deleted {
        Ok(n) => {
            let t = std::time::Instant::now();
            let finished = gc::finish(meta, generation).await;
            stats.finish.observe(t.elapsed());
            bump(&stats.txn_finish, 1);
            match finished {
                Ok(()) => {
                    bump(&stats.collected, 1);
                    if root {
                        bump(&stats.collected_roots, 1);
                    }
                    bump(
                        &stats.reclaimed_bytes,
                        chunks.iter().map(chunk_physical_bytes).sum(),
                    );
                    Ok(Outcome::Collected(n))
                }
                Err(e) if is_deferrable(&e) => Ok(Outcome::Deleted(n)),
                Err(e) => Err(e.into()),
            }
        }
        Err(ControlError::Node { .. }) => Ok(Outcome::Deferred),
        Err(e) => Err(e),
    }
}

/// Part references and records retired per transaction when a multipart
/// root is finished (RFC-007 §5.3.4).
pub const ROOT_FINISH_BATCH: usize = 256;

/// Retire every part of a retiring multipart root, `ROOT_FINISH_BATCH` per
/// transaction. A non-root or a non-retiring generation is a no-op here
/// (`verify_unreferenced` decides what happens to it).
/// Returns whether any step did work (the generation is a multipart root
/// with parts left).
async fn finish_root_parts<S: KvStore>(
    meta: &S,
    generation: &GenId,
    now_ms: u64,
    stats: &GcStats,
) -> Result<bool, MetaError> {
    let mut root = false;
    loop {
        let t = std::time::Instant::now();
        let step =
            multipart::finish_root_step_counted(meta, generation, ROOT_FINISH_BATCH, now_ms).await;
        stats.root_step.observe(t.elapsed());
        bump(&stats.txn_root_step, 1);
        match step {
            Ok((more, retired)) => {
                bump(&stats.parts_retired, retired as u64);
                root |= retired > 0;
                if !more {
                    return Ok(root);
                }
            }
            Err(MetaError::WrongGenerationState(..)) => return Ok(root),
            Err(e) => return Err(e),
        }
    }
}

/// What the multipart sweep did (RFC-007 §5.3.4).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Open uploads older than the expiry that were aborted.
    pub expired: usize,
    /// Part records processed (retired or dropped) across uploads.
    pub parts_processed: usize,
    /// Uploads whose sweep was left for the next pass (metadata error).
    pub deferred: usize,
}

/// Part records processed per sweep transaction.
pub const SWEEP_BATCH: usize = 256;

/// One multipart sweep: finish aborts a gateway left behind, discard parts
/// of completed uploads that were never listed, and abort open uploads
/// initiated before `expire_before_ms`.
///
/// # Errors
///
/// Metadata errors that are not deferrable.
pub async fn sweep_multipart<S: KvStore>(
    meta: &S,
    now_ms: u64,
    expire_before_ms: Option<u64>,
    limit: usize,
) -> Result<SweepReport, ControlError> {
    let mut report = SweepReport::default();
    for upload in multipart::sweep_candidates(meta, expire_before_ms, limit).await? {
        let bucket = match multipart::bucket_of(meta, upload.bucket_id).await {
            Ok(b) => b,
            Err(MetaError::NoSuchBucket) => continue,
            Err(e) if is_deferrable(&e) => {
                report.deferred += 1;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        if upload.state == MultipartState::Open {
            match multipart::abort(meta, &bucket.name, &upload.key, &upload.upload_id).await {
                Ok(_) => report.expired += 1,
                Err(MetaError::NoSuchUpload) => continue,
                Err(e) if is_deferrable(&e) => {
                    report.deferred += 1;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        loop {
            match multipart::discard_parts(
                meta,
                upload.bucket_id,
                &upload.key,
                &upload.upload_id,
                SWEEP_BATCH,
                now_ms,
            )
            .await
            {
                Ok(n) => {
                    report.parts_processed += n;
                    if n < SWEEP_BATCH {
                        break;
                    }
                }
                Err(e) if is_deferrable(&e) => {
                    report.deferred += 1;
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(report)
}

/// Whether a metadata error means "try again next pass" rather than "stop".
fn is_deferrable(e: &MetaError) -> bool {
    matches!(
        e,
        MetaError::Kv(
            arca_lake_metadata::KvError::CommitUnknown(_) | arca_lake_metadata::KvError::Backend(_)
        ) | MetaError::TooManyRetries
    )
}

/// In one transaction: confirm that `generation` is retiring and that no
/// current pointer or version record of its key references it, and return
/// its chunk records. `None` means "do not touch".
///
/// A retiring generation can never become referenced again (no recipe moves
/// a generation out of `Retiring`), so the check does not need to be in the
/// same transaction as the deletions that follow.
type Unreferenced = (Vec<ChunkRecord>, Vec<(u32, CachePlacement)>);

async fn verify_unreferenced<S: KvStore>(
    meta: &S,
    generation: &GenId,
) -> Result<Option<Unreferenced>, MetaError> {
    transact(meta, *generation, |tx, generation| {
        Box::pin(async move {
            let Some(header) = tx.get(&keys::gen_header(generation)).await? else {
                // Already finished (idempotent re-run): nothing to delete.
                return Ok(Some((Vec::new(), Vec::new())));
            };
            let header: GenerationHeader = decode(&header)?;
            if !matches!(header.state, GenState::Retiring { .. }) {
                return Ok(None);
            }
            // I-MP1 (RFC-007 §5.3.4): a part named by an open upload's part
            // record or by its root's reference is never collected.
            if multipart::part_is_referenced(tx, &header, generation).await? {
                return Ok(None);
            }
            if let Some(p) = tx
                .get(&keys::object_current(header.bucket_id, &header.key))
                .await?
            {
                let pointer: CurrentPointer = decode(&p)?;
                if pointer.generation == *generation {
                    return Ok(None);
                }
            }
            for (_, v) in tx
                .get_range(
                    &keys::versions_range(header.bucket_id, &header.key),
                    usize::MAX,
                    false,
                )
                .await?
            {
                let record: VersionRecord = decode(&v)?;
                if record.generation == *generation {
                    return Ok(None);
                }
            }
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::gen_chunks_range(generation), usize::MAX, false)
                .await?
            {
                out.push(decode::<ChunkRecord>(&v)?);
            }
            let cached = arca_lake_metadata::ops::cache::placements_in_tx(tx, generation).await?;
            Ok(Some((out, cached)))
        })
    })
    .await
}

/// Delete the cache copies of a retiring generation (RFC-006 §5.7).
async fn delete_cache_copies(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    groups: &HashMap<u64, GroupRecord>,
    generation: &GenId,
    cached: &[(u32, CachePlacement)],
) -> Result<usize, ControlError> {
    let mut deleted = 0;
    for (index, p) in cached {
        let Some(group) = groups.get(&p.group_id) else {
            continue;
        };
        for (i, member) in group.members.iter().enumerate() {
            let Some(node) = nodes.get(&member.node_id) else {
                continue;
            };
            let shard = ShardRef {
                group_id: group.id,
                chunk_id: arca_lake_common::chunk_id(generation, *index),
                shard_index: u16::try_from(i).unwrap_or(0),
            };
            match clients
                .delete_shard(&node.address, shard, group.epoch)
                .await
            {
                Ok(()) => deleted += 1,
                Err(s) if s.code() == tonic::Code::NotFound => deleted += 1,
                Err(status) => {
                    return Err(ControlError::Node {
                        addr: node.address.clone(),
                        status,
                    });
                }
            }
        }
    }
    Ok(deleted)
}

/// Delete every shard of `chunks` on the current members of their groups.
/// Returns the number of delete RPCs that succeeded (NotFound counts).
async fn delete_gc_shard(
    clients: &NodeClients,
    addr: String,
    shard: ShardRef,
    epoch: u64,
    stats: &GcStats,
) -> Result<(), ControlError> {
    let t = std::time::Instant::now();
    let r = clients.delete_shard(&addr, shard, epoch).await;
    stats.delete_rpc.observe(t.elapsed());
    match r {
        Ok(()) => Ok(()),
        Err(s) if s.code() == tonic::Code::NotFound => Ok(()),
        Err(e) => Err(ControlError::Node { addr, status: e }),
    }
}

async fn delete_shards(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    groups: &HashMap<u64, GroupRecord>,
    chunks: &[ChunkRecord],
    stats: &GcStats,
) -> Result<usize, ControlError> {
    let mut targets = Vec::new();
    for chunk in chunks {
        let Some(group) = groups.get(&chunk.placement.group_id) else {
            continue;
        };
        for (i, member) in group.members.iter().enumerate() {
            let Some(node) = nodes.get(&member.node_id) else {
                continue;
            };
            let shard = ShardRef {
                group_id: group.id,
                chunk_id: chunk.chunk_id,
                shard_index: u16::try_from(i).unwrap_or(0),
            };
            targets.push((node.address.clone(), shard, group.epoch));
        }
    }
    let work: Vec<_> = targets
        .into_iter()
        .map(|(addr, shard, epoch)| Box::pin(delete_gc_shard(clients, addr, shard, epoch, stats)))
        .collect();
    let results: Vec<Result<(), ControlError>> = futures::stream::iter(work)
        .buffer_unordered(DELETE_CONCURRENCY)
        .collect()
        .await;
    let mut deleted = 0;
    for r in results {
        r?;
        deleted += 1;
    }
    Ok(deleted)
}

/// Reconcile one node's shards of `group` against metadata, page by page.
///
/// A shard is kept when a generation with its prefix is pending, or when a
/// generation with its prefix has a chunk record for its index placed in
/// this group. It is deleted when generations exist but none accounts for
/// it here (a write diverted to another group, or a failed attempt); when
/// no generation with the prefix exists at all and the shard is at least
/// `min_age_ms` old by the time encoded in its chunk ID; or when `node_id`
/// is not a current member of `group` and the group is not being repaired
/// (a former member's copies are never read). Every delete carries the
/// group epoch the decision was made under; a node fenced to a newer epoch
/// rejects it (RFC-008 §6.4).
///
/// `min_age_ms` applies only to the no-generation case and must be at
/// least the pending lease: a shard younger than that may belong to a
/// generation whose `begin_put` is being retried. It guards against clock
/// disagreement between the writer that chose the chunk ID and this
/// controller; it is never the criterion for deletion.
///
/// Memory is bounded by the page size; a shard added or removed on the node
/// during the walk may be missed or seen stale, which the next pass fixes.
///
/// # Errors
///
/// Metadata or node errors.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_node<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node_id: u64,
    node_addr: &str,
    group: &GroupRecord,
    now_ms: u64,
    min_age_ms: u64,
    page_size: u32,
) -> Result<ReconcileReport, ControlError> {
    if group.is_cache() {
        // Cache copies are accounted for by cache placements, not chunk
        // records (RFC-006 §5.6).
        let r =
            arca_lake_tiering::cache::reconcile_node(meta, clients, node_addr, group, page_size)
                .await
                .map_err(tier_err)?;
        return Ok(ReconcileReport {
            orphans_deleted: r.orphans_deleted,
            kept_referenced: r.kept,
            ..ReconcileReport::default()
        });
    }
    let mut report = ReconcileReport::default();
    let mut after = None;
    loop {
        // Quarantined shards are listed too (RFC-008 §8.2): one that no
        // generation references is an orphan like any other and its
        // tombstone releases the quarantine entry; a referenced one is
        // kept for stripe repair to rewrite.
        let page = clients
            .list_shards_page_with(node_addr, group.id, after, page_size, true)
            .await
            .map_err(|status| ControlError::Node {
                addr: node_addr.to_string(),
                status,
            })?;
        for (shard, _) in page.entries.iter().chain(&page.quarantined) {
            // Retired already under this incarnation (Phase 3.12 liveness
            // audit): another delete changes nothing; reclamation ends it.
            if page.retired.contains(shard) {
                report.already_retired += 1;
                continue;
            }
            reconcile_shard(
                meta,
                clients,
                node_id,
                node_addr,
                group,
                now_ms,
                min_age_ms,
                *shard,
                page.sequences.get(shard).copied().unwrap_or_default(),
                &mut report,
            )
            .await?;
        }
        match page.next {
            Some(n) => after = Some(n),
            None => return Ok(report),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_shard<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node_id: u64,
    node_addr: &str,
    group: &GroupRecord,
    now_ms: u64,
    min_age_ms: u64,
    shard: ShardRef,
    sequence: RecordToken,
    report: &mut ReconcileReport,
) -> Result<(), ControlError> {
    // Every delete below names the record the listing showed: one written
    // since under the same key (a migration or repair) is not touched
    // (Phase 3.5 audit CON-5/CON-6). An older node reports no sequence and
    // would delete unconditionally: nothing is deleted there (CON-12).
    let mut delete = async |epoch: u64| -> Result<bool, ControlError> {
        if !sequence.is_some() {
            report.kept_no_sequence += 1;
            return Ok(false);
        }
        match delete_one(clients, node_addr, shard, epoch, sequence).await {
            Ok(()) => Ok(true),
            Err(ControlError::Node { status, .. }) if NodeClients::is_changed(&status) => {
                report.kept_changed += 1;
                Ok(false)
            }
            Err(ControlError::Node { status, .. }) if NodeClients::is_fenced(&status) => {
                report.kept_fenced += 1;
                Ok(false)
            }
            Err(e) => Err(e),
        }
    };
    let is_member = group.members.iter().any(|m| m.node_id == node_id);
    if !is_member {
        if group.state == GroupState::Repairing {
            report.kept_referenced += 1;
            return Ok(());
        }
        // Re-read the group: membership may have changed since the caller
        // loaded it (the node could have been re-added). The delete carries
        // the epoch read here; a node re-added since has been fenced to a
        // newer epoch and rejects it.
        let current = cluster::get_group(meta, group.id).await?;
        if current.members.iter().any(|m| m.node_id == node_id)
            || current.state == GroupState::Repairing
        {
            report.kept_referenced += 1;
            return Ok(());
        }
        if delete(current.epoch).await? {
            report.former_member_deleted += 1;
        }
        return Ok(());
    }
    // A member holds exactly the shard index of its position; a copy under
    // another index is a misdirected write that no reader or repair uses
    // (RFC-008 §8.4, Unexpected Shard).
    let position = group.members.iter().position(|m| m.node_id == node_id);
    if position.is_some_and(|p| usize::from(shard.shard_index) != p) {
        // A metadata read before the delete, as for every other kind (the
        // guarded store refuses it once the cluster left `Active`; Phase
        // 3.8 review), and membership as it is now.
        let current = cluster::get_group(meta, group.id).await?;
        if current
            .members
            .get(usize::from(shard.shard_index))
            .map(|m| m.node_id)
            == Some(node_id)
        {
            report.kept_referenced += 1;
            return Ok(());
        }
        if delete(current.epoch).await? {
            report.orphans_deleted += 1;
        }
        return Ok(());
    }
    let (prefix, index) = split_chunk_id(&shard.chunk_id);
    match gc::attribute_shard(meta, &prefix, index, group.id).await? {
        gc::ShardAttribution::Referenced => {
            report.kept_referenced += 1;
            return Ok(());
        }
        gc::ShardAttribution::NoGeneration => {
            let age = now_ms.saturating_sub(uuid_v7_unix_ms(&prefix));
            if age < min_age_ms {
                report.kept_young += 1;
                return Ok(());
            }
        }
        gc::ShardAttribution::Unreferenced => {}
    }
    if delete(group.epoch).await? {
        report.orphans_deleted += 1;
    }
    Ok(())
}

/// What a reclaim pass did (Phase 3.12, ADR-0028).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReclaimReport {
    /// Retired records listed.
    pub listed: u64,
    /// Not yet the node's retention old.
    pub young: u64,
    /// Needed by the current metadata: kept (no reclaim sent).
    pub needed: u64,
    /// Reclaimed.
    pub reclaimed: u64,
    /// Refused by the node (its checks), or fenced.
    pub refused: u64,
    /// Records skipped on an error (metadata or node), retried next pass.
    pub errors: u64,
    /// Nodes that do not defer (an earlier release, or retention 0).
    pub nodes_not_deferring: u64,
}

impl ReclaimReport {
    /// Add `other`'s counts.
    pub fn add(&mut self, other: &Self) {
        self.listed += other.listed;
        self.young += other.young;
        self.needed += other.needed;
        self.reclaimed += other.reclaimed;
        self.refused += other.refused;
        self.errors += other.errors;
        self.nodes_not_deferring += other.nodes_not_deferring;
    }
}

/// Whether the current metadata needs `shard` on `node_id`, `group` being
/// its group's record (`None`: the group is gone, nothing is needed): the
/// checks reconciliation makes before a delete, without its age guard (the
/// retention exceeds it).
///
/// # Errors
///
/// Metadata errors.
pub async fn shard_needed_in<S: KvStore>(
    meta: &S,
    node_id: u64,
    shard: ShardRef,
    group: Option<&GroupRecord>,
) -> Result<bool, ControlError> {
    let Some(group) = group else {
        return Ok(false);
    };
    let position = group.members.iter().position(|m| m.node_id == node_id);
    Ok(match position {
        // A former member: its shards are a repair source while the group
        // rebuilds.
        None => group.state == GroupState::Repairing,
        Some(p) if usize::from(shard.shard_index) != p => false,
        Some(_) => {
            let (prefix, index) = split_chunk_id(&shard.chunk_id);
            matches!(
                gc::attribute_shard(meta, &prefix, index, group.id).await?,
                gc::ShardAttribution::Referenced
            )
        }
    })
}

/// [`shard_needed_in`], reading the group.
///
/// # Errors
///
/// Metadata errors.
pub async fn shard_needed<S: KvStore>(
    meta: &S,
    node_id: u64,
    shard: ShardRef,
) -> Result<(bool, Option<GroupRecord>), ControlError> {
    let group = cluster::list_groups(meta)
        .await?
        .into_iter()
        .find(|g| g.id == shard.group_id);
    Ok((
        shard_needed_in(meta, node_id, shard, group.as_ref()).await?,
        group,
    ))
}

/// Records of a node judged and reclaimed at once.
const RECLAIM_CONCURRENCY: usize = 16;
/// Consecutive node errors after which the rest of a node waits for the
/// next pass.
const RECLAIM_NODE_ERRORS: u64 = 8;

/// Reclaim pass of one node (Phase 3.12, ADR-0028): every retired record
/// older than the node's retention that the current metadata does not need
/// is reclaimed, each with an observation time taken (`now`) before the
/// metadata reads that judged it (one per page, before the page's group and
/// attribution reads). The node decides: it reclaims only a record retired
/// at least its retention before that observation, by a delete of the
/// incarnation the reclaim carries.
///
/// A record whose judgement or reclaim fails is counted in `errors` and
/// skipped (the next pass retries it); the node is left for the next pass
/// only after [`RECLAIM_NODE_ERRORS`] consecutive node errors, or when its
/// listing fails.
///
/// # Errors
///
/// Listing the node's retired records failed.
#[allow(clippy::too_many_lines, clippy::implicit_hasher)]
pub async fn reclaim_node<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node_id: u64,
    node_addr: &str,
    page: u32,
    now: impl Fn() -> u64 + Sync,
) -> Result<ReclaimReport, ControlError> {
    let mut report = ReclaimReport::default();
    if clients.delete_fence().is_none() {
        // This process deletes nothing now: nothing to judge.
        return Ok(report);
    }
    let node_err = |status| ControlError::Node {
        addr: node_addr.to_string(),
        status,
    };
    // Epochs the node holds, for records of groups the metadata no longer
    // has (fetched once, when first needed).
    let epochs: tokio::sync::OnceCell<HashMap<u64, u64>> = tokio::sync::OnceCell::new();
    let mut after = None;
    let mut consecutive = 0u64;
    loop {
        let (entries, next, retention) = match clients.list_pending(node_addr, after, page).await {
            Ok(p) => p,
            Err(s) if s.code() == tonic::Code::Unimplemented => {
                report.nodes_not_deferring += 1;
                return Ok(report);
            }
            Err(s) => return Err(node_err(s)),
        };
        if retention == 0 {
            report.nodes_not_deferring += 1;
        }
        let page_len = entries.len() as u64;
        report.listed += page_len;
        let due: Vec<_> = entries
            .into_iter()
            .filter(|(_, _, retired_ms)| now() >= retired_ms.saturating_add(retention))
            .collect();
        report.young += page_len - due.len() as u64;
        // The observation, then every metadata read that judges this page
        // (ADR-0028: the observation is no later than what it saw). The
        // groups are read after it, once per page (independent review: a
        // map read before the stamp could be of metadata older than the
        // stamp claims).
        let observed = now();
        let groups: HashMap<u64, GroupRecord> = match cluster::list_groups(meta).await {
            Ok(g) => g.into_iter().map(|g| (g.id, g)).collect(),
            Err(e) => {
                tracing::debug!(error = %e, "reclaim: groups not read, retried next pass");
                report.errors += due.len() as u64;
                return Ok(report);
            }
        };
        let groups = &groups;
        let outcomes: Vec<ReclaimOutcome> = futures::stream::iter(due)
            .map(|(shard, token, _)| {
                let epochs = &epochs;
                async move {
                    let group = groups.get(&shard.group_id);
                    match shard_needed_in(meta, node_id, shard, group).await {
                        Ok(true) => {
                            // Needed: the retirement ends (Phase 3.12
                            // review), so that it is not judged again every
                            // pass; a later delete retires it anew. A record
                            // of a missing group is never needed, so the
                            // group is known here.
                            if let Some(g) = group {
                                let _ = clients.keep_shard(node_addr, shard, g.epoch, token).await;
                            }
                            return ReclaimOutcome::Needed;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            tracing::debug!(error = %e, "reclaim: judgement failed, retried next pass");
                            return ReclaimOutcome::MetaError;
                        }
                    }
                    let epoch = match group {
                        Some(g) => g.epoch,
                        None => match epochs
                            .get_or_try_init(|| async {
                                clients.health(node_addr).await.map(|h| {
                                    h.group_epochs
                                        .iter()
                                        .map(|e| (e.group_id, e.epoch))
                                        .collect::<HashMap<u64, u64>>()
                                })
                            })
                            .await
                        {
                            Ok(m) => m.get(&shard.group_id).copied().unwrap_or(0),
                            Err(_) => return ReclaimOutcome::NodeError,
                        },
                    };
                    if group.is_none() {
                        // A group the metadata does not have: reconciliation
                        // never walks it, so a retirement of an earlier
                        // incarnation there would wait forever (Phase 3.12
                        // review). The delete again, under this incarnation,
                        // retires it anew (a no-op when it already is this
                        // incarnation's); the reclaim then waits the
                        // retention from it.
                        let _ = clients.delete_shard_if(node_addr, shard, epoch, token).await;
                    }
                    match clients
                        .reclaim_shard(node_addr, shard, epoch, token, observed)
                        .await
                    {
                        Ok(()) => ReclaimOutcome::Reclaimed,
                        Err(s)
                            if s.code() == tonic::Code::FailedPrecondition
                                || s.code() == tonic::Code::Aborted
                                || NodeClients::is_epoch_rejected(&s) =>
                        {
                            ReclaimOutcome::Refused
                        }
                        Err(s) => {
                            tracing::debug!(code = ?s.code(), "reclaim: node error, retried next pass");
                            ReclaimOutcome::NodeError
                        }
                    }
                }
            })
            .buffered(RECLAIM_CONCURRENCY)
            .collect()
            .await;
        for o in outcomes {
            match o {
                ReclaimOutcome::Needed => report.needed += 1,
                ReclaimOutcome::Reclaimed => report.reclaimed += 1,
                ReclaimOutcome::Refused => report.refused += 1,
                ReclaimOutcome::MetaError => report.errors += 1,
                ReclaimOutcome::NodeError => {
                    report.errors += 1;
                    consecutive += 1;
                    continue;
                }
            }
            consecutive = 0;
        }
        if consecutive >= RECLAIM_NODE_ERRORS {
            tracing::debug!(
                node = node_id,
                "reclaim: node keeps failing, the rest waits for the next pass"
            );
            return Ok(report);
        }
        match next {
            Some(n) => after = Some(n),
            None => return Ok(report),
        }
    }
}

/// How one retired record ended in a reclaim pass.
enum ReclaimOutcome {
    Needed,
    Reclaimed,
    Refused,
    MetaError,
    NodeError,
}

async fn delete_one(
    clients: &NodeClients,
    node_addr: &str,
    shard: ShardRef,
    epoch: u64,
    token: RecordToken,
) -> Result<(), ControlError> {
    match clients
        .delete_shard_if(node_addr, shard, epoch, token)
        .await
    {
        Ok(()) => Ok(()),
        Err(s) if s.code() == tonic::Code::NotFound => Ok(()),
        Err(status) => Err(ControlError::Node {
            addr: node_addr.to_string(),
            status,
        }),
    }
}

/// Convenience for tests: whether a generation still has metadata.
///
/// # Errors
///
/// Metadata errors.
pub async fn generation_exists<S: KvStore>(
    meta: &S,
    generation: &GenId,
) -> Result<bool, ControlError> {
    Ok(object::get_generation(meta, generation).await.is_ok())
}

fn tier_err(e: arca_lake_tiering::TierError) -> ControlError {
    match e {
        arca_lake_tiering::TierError::Meta(m) => ControlError::Meta(m),
        arca_lake_tiering::TierError::Node { addr, status } => ControlError::Node { addr, status },
        other => ControlError::Other(other.to_string()),
    }
}
