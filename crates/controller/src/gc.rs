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

use arca_lake_common::{split_chunk_id, uuid_v7_unix_ms};
use arca_lake_metadata::ops::{cluster, gc, object};
use arca_lake_metadata::records::{
    ChunkRecord, CurrentPointer, GenId, GenState, GenerationHeader, GroupRecord, GroupState,
    NodeRecord, VersionRecord, decode,
};
use arca_lake_metadata::{KvStore, MetaError, Txn, keys, transact};
use arca_lake_protocol::client::{NodeClients, ShardRef};

use crate::ControlError;

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
    // An unknown commit outcome on a GC transaction is never retried here:
    // every GC step re-reads the state it changes, so the next pass either
    // sees the change applied or does it again.
    let mut report = GcReport::default();
    match gc::expire_pending(meta, now_ms, limit).await {
        Ok(expired) => report.expired = expired.len(),
        Err(e) if is_deferrable(&e) => report.deferred += 1,
        Err(e) => return Err(e.into()),
    }

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

    for generation in gc::collectable(meta, now_ms, grace_ms, limit).await? {
        let chunks = match verify_unreferenced(meta, &generation).await {
            Ok(Some(chunks)) => chunks,
            Ok(None) => {
                report.skipped_referenced += 1;
                continue;
            }
            Err(e) if is_deferrable(&e) => {
                report.deferred += 1;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        match delete_shards(clients, &nodes, &groups, &chunks).await {
            Ok(n) => {
                report.shards_deleted += n;
                match gc::finish(meta, &generation).await {
                    Ok(()) => report.collected += 1,
                    Err(e) if is_deferrable(&e) => report.deferred += 1,
                    Err(e) => return Err(e.into()),
                }
            }
            Err(ControlError::Node { .. }) => report.deferred += 1,
            Err(e) => return Err(e),
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
async fn verify_unreferenced<S: KvStore>(
    meta: &S,
    generation: &GenId,
) -> Result<Option<Vec<ChunkRecord>>, MetaError> {
    transact(meta, *generation, |tx, generation| {
        Box::pin(async move {
            let Some(header) = tx.get(&keys::gen_header(generation)).await? else {
                // Already finished (idempotent re-run): nothing to delete.
                return Ok(Some(Vec::new()));
            };
            let header: GenerationHeader = decode(&header)?;
            if !matches!(header.state, GenState::Retiring { .. }) {
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
            Ok(Some(out))
        })
    })
    .await
}

/// Delete every shard of `chunks` on the current members of their groups.
/// Returns the number of delete RPCs that succeeded (NotFound counts).
async fn delete_shards(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    groups: &HashMap<u64, GroupRecord>,
    chunks: &[ChunkRecord],
) -> Result<usize, ControlError> {
    let mut deleted = 0;
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
    let mut report = ReconcileReport::default();
    let mut after = None;
    loop {
        let page = clients
            .list_shards_page(node_addr, group.id, after, page_size)
            .await
            .map_err(|status| ControlError::Node {
                addr: node_addr.to_string(),
                status,
            })?;
        for (shard, _) in &page.entries {
            reconcile_shard(
                meta,
                clients,
                node_id,
                node_addr,
                group,
                now_ms,
                min_age_ms,
                *shard,
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
    report: &mut ReconcileReport,
) -> Result<(), ControlError> {
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
        delete_one(clients, node_addr, shard, current.epoch).await?;
        report.former_member_deleted += 1;
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
    delete_one(clients, node_addr, shard, group.epoch).await?;
    report.orphans_deleted += 1;
    Ok(())
}

async fn delete_one(
    clients: &NodeClients,
    node_addr: &str,
    shard: ShardRef,
    epoch: u64,
) -> Result<(), ControlError> {
    match clients.delete_shard(node_addr, shard, epoch).await {
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
