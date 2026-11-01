//! Garbage collection (RFC-004 §5.10, RFC-008).
//!
//! Three sources of garbage:
//! 1. Pending generations whose lease expired → retired.
//! 2. Retired generations past the grace period → shards deleted on their
//!    members, then metadata removed.
//! 3. Shards on nodes whose generation no longer exists (crash between
//!    shard write and manifest append) → deleted once older than
//!    `lease + grace`, attributed through the generation prefix in the
//!    chunk ID.

use std::collections::HashMap;

use arca_lake_common::{split_chunk_id, uuid_v7_unix_ms};
use arca_lake_metadata::ops::{cluster, gc, object};
use arca_lake_metadata::records::{GenId, GroupRecord, NodeRecord};
use arca_lake_metadata::{KvStore, Txn, keys, transact};
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
    /// Orphan shards (no generation) deleted by reconciliation.
    pub orphans_deleted: usize,
    /// Generations skipped because a member was unreachable (retried later).
    pub deferred: usize,
}

/// One pass of generation-driven GC.
///
/// # Errors
///
/// Metadata errors. Node errors defer the generation instead of failing.
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
        match delete_generation_shards(meta, clients, &nodes, &groups, &generation).await {
            Ok(n) => {
                report.shards_deleted += n;
                match gc::finish(meta, &generation).await {
                    Ok(()) => report.collected += 1,
                    Err(e) if is_deferrable(&e) => report.deferred += 1,
                    Err(e) => return Err(e.into()),
                }
            }
            Err(ControlError::Node { .. }) => report.deferred += 1,
            Err(ControlError::Meta(e)) if is_deferrable(&e) => report.deferred += 1,
            Err(e) => return Err(e),
        }
    }
    Ok(report)
}

/// Whether a metadata error means "try again next pass" rather than "stop".
fn is_deferrable(e: &arca_lake_metadata::MetaError) -> bool {
    matches!(
        e,
        arca_lake_metadata::MetaError::Kv(
            arca_lake_metadata::KvError::CommitUnknown(_) | arca_lake_metadata::KvError::Backend(_)
        ) | arca_lake_metadata::MetaError::TooManyRetries
    )
}

/// Delete every shard of a generation's recorded chunks. Returns the number
/// of delete RPCs that succeeded (NotFound counts as success).
async fn delete_generation_shards<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    groups: &HashMap<u64, GroupRecord>,
    generation: &GenId,
) -> Result<usize, ControlError> {
    let chunks = transact(meta, *generation, |tx, generation| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::gen_chunks_range(generation), usize::MAX, false)
                .await?
            {
                out.push(arca_lake_metadata::records::decode::<
                    arca_lake_metadata::records::ChunkRecord,
                >(&v)?);
            }
            Ok(out)
        })
    })
    .await?;
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

/// Reconcile one node's shards of `group` against metadata: delete shards
/// whose generation does not exist and whose age exceeds `min_age_ms`.
///
/// # Errors
///
/// Metadata or node errors.
pub async fn reconcile_node<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node_addr: &str,
    group: &GroupRecord,
    now_ms: u64,
    min_age_ms: u64,
) -> Result<usize, ControlError> {
    let shards = clients
        .list_shards(node_addr, group.id)
        .await
        .map_err(|status| ControlError::Node {
            addr: node_addr.to_string(),
            status,
        })?;
    let mut deleted = 0;
    for shard in shards {
        let (prefix, _) = split_chunk_id(&shard.chunk_id);
        let age = now_ms.saturating_sub(uuid_v7_unix_ms(&prefix));
        if age < min_age_ms {
            continue;
        }
        let exists = transact(meta, prefix, |tx, prefix| {
            Box::pin(async move {
                let found = tx
                    .get_range(&keys::gen_prefix_range(prefix), 1, false)
                    .await?;
                Ok(!found.is_empty())
            })
        })
        .await?;
        if exists {
            continue;
        }
        match clients.delete_shard(node_addr, shard, group.epoch).await {
            Ok(()) => deleted += 1,
            Err(s) if s.code() == tonic::Code::NotFound => {}
            Err(status) => {
                return Err(ControlError::Node {
                    addr: node_addr.to_string(),
                    status,
                });
            }
        }
    }
    Ok(deleted)
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
