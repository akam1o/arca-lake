//! Read-through cache operations (RFC-006 §5.3–5.7).
//!
//! A cache group has scheme [`CACHE_SCHEME`] and one member that holds the
//! whole chunk as shard 0. The cache placement record is the only thing
//! that makes a copy visible to readers; copies without a record are
//! orphans and are removed by [`reconcile_node`]; records without a copy
//! are removed by [`invalidate`] when a reader finds them wrong.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use arca_lake_common::{chunk_id, split_chunk_id};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::cache as meta_cache;
use arca_lake_metadata::ops::gc as meta_gc;
use arca_lake_metadata::records::{CachePlacement, ChunkRecord, GenId, GroupRecord, GroupState};
use arca_lake_protocol::client::{NodeClients, ShardRef};
use bytes::Bytes;

use crate::TierError;

/// Scheme identifier of cache groups: one member, whole chunk as shard 0.
pub const CACHE_SCHEME: &str = "cache-v1";

/// Configuration of one cache tier as the gateway and controller see it.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheTierConfig {
    /// Tier name (matches `GroupRecord.tier` of its cache groups).
    pub tier: String,
    /// Capacity in bytes (0 = unlimited; eviction never runs).
    pub capacity_bytes: u64,
    /// Eviction starts above this fraction of the capacity.
    pub high_watermark: f64,
    /// Eviction stops at or below this fraction of the capacity.
    pub low_watermark: f64,
    /// Chunks larger than this are never admitted (0 = no limit).
    pub max_cacheable_bytes: u64,
    /// Admit a chunk only after this many misses were observed for it by
    /// this gateway (0 or 1 = admit on the first miss).
    pub admit_after_misses: u32,
}

impl CacheTierConfig {
    /// Usage above which admissions are rejected and eviction runs.
    #[must_use]
    pub fn high_bytes(&self) -> u64 {
        fraction(self.capacity_bytes, self.high_watermark)
    }

    /// Usage eviction aims for.
    #[must_use]
    pub fn low_bytes(&self) -> u64 {
        fraction(self.capacity_bytes, self.low_watermark)
    }
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn fraction(bytes: u64, f: f64) -> u64 {
    if bytes == 0 {
        return 0;
    }
    ((bytes as f64) * f.clamp(0.0, 1.0)).round() as u64
}

/// Choose the cache group for a chunk: active `cache-v1` groups of `tier`,
/// spread by a hash of the chunk ID so that the choice is stable across
/// gateways (duplicate populations then write the same bytes to the same
/// place).
pub fn choose_group<'a>(
    groups: impl Iterator<Item = &'a GroupRecord>,
    tier: &str,
    cid: &[u8; 16],
) -> Option<&'a GroupRecord> {
    let mut candidates: Vec<&GroupRecord> = groups
        .filter(|g| g.scheme == CACHE_SCHEME && g.tier == tier && g.state == GroupState::Active)
        .collect();
    if candidates.is_empty() {
        return None;
    }
    candidates.sort_by_key(|g| g.id);
    let mut h = DefaultHasher::new();
    cid.hash(&mut h);
    let idx = usize::try_from(h.finish() % candidates.len() as u64).unwrap_or(0);
    Some(candidates[idx])
}

/// Result of a population attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PopulateOutcome {
    /// Copy written and recorded.
    Inserted,
    /// A record already existed (another gateway or an earlier attempt).
    AlreadyPresent,
    /// Not recorded: the generation is gone or the tier is over capacity;
    /// the copy was deleted (best effort).
    Rejected(&'static str),
}

/// Write `bytes` (the whole chunk `index` of `generation`) to the cache
/// member of `group`, then record the placement if the generation is still
/// committed and the tier is within its high watermark (RFC-006 §5.3).
///
/// # Errors
///
/// Node errors on the write, metadata errors, or a CRC mismatch reported
/// by the node (the copy is then deleted).
#[allow(clippy::too_many_arguments)]
pub async fn populate<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &CacheTierConfig,
    group: &GroupRecord,
    node_addr: &str,
    generation: &GenId,
    index: u32,
    bytes: Bytes,
    now_ms: u64,
) -> Result<PopulateOutcome, TierError> {
    let cid = chunk_id(generation, index);
    let shard = ShardRef {
        group_id: group.id,
        chunk_id: cid,
        shard_index: 0,
    };
    let expected = arca_lake_storage::format::crc32c(&bytes);
    let len = bytes.len() as u64;
    let resp = clients
        .put_shard(node_addr, shard, group.epoch, bytes, 256 << 10)
        .await
        .map_err(|status| TierError::Node {
            addr: node_addr.to_string(),
            status,
        })?;
    if resp.crc32c != expected {
        let _ = clients.delete_shard(node_addr, shard, group.epoch).await;
        return Err(TierError::Corrupt(format!(
            "cache node {node_addr} stored crc {:08x}, expected {expected:08x}",
            resp.crc32c
        )));
    }
    let placement = CachePlacement {
        tier: cfg.tier.clone(),
        group_id: group.id,
        epoch: group.epoch,
        bytes: len,
        pinned: false,
        last_access_ms: now_ms,
        created_ms: now_ms,
    };
    match meta_cache::insert(meta, generation, index, &placement, cfg.high_bytes()).await? {
        meta_cache::InsertOutcome::Inserted => Ok(PopulateOutcome::Inserted),
        meta_cache::InsertOutcome::AlreadyPresent => Ok(PopulateOutcome::AlreadyPresent),
        meta_cache::InsertOutcome::GenerationGone => {
            let _ = clients.delete_shard(node_addr, shard, group.epoch).await;
            Ok(PopulateOutcome::Rejected("generation gone"))
        }
        meta_cache::InsertOutcome::OverCapacity => {
            let _ = clients.delete_shard(node_addr, shard, group.epoch).await;
            Ok(PopulateOutcome::Rejected("over capacity"))
        }
    }
}

/// Read a cached chunk and verify it against the chunk record (length and
/// CRC-32C). Any mismatch is an error; the caller falls back to the
/// durable read and invalidates.
///
/// # Errors
///
/// Node errors, [`TierError::Corrupt`].
pub async fn read_cached(
    clients: &NodeClients,
    node_addr: &str,
    placement: &CachePlacement,
    generation: &GenId,
    index: u32,
    chunk: &ChunkRecord,
) -> Result<Bytes, TierError> {
    let shard = ShardRef {
        group_id: placement.group_id,
        chunk_id: chunk_id(generation, index),
        shard_index: 0,
    };
    let bytes = clients
        .get_shard(node_addr, shard, 0, 0)
        .await
        .map_err(|status| TierError::Node {
            addr: node_addr.to_string(),
            status,
        })?;
    if bytes.len() != usize::try_from(chunk.len).unwrap_or(usize::MAX)
        || arca_lake_storage::format::crc32c(&bytes) != chunk.crc32c
    {
        return Err(TierError::Corrupt(format!(
            "cached chunk {index} does not match its record"
        )));
    }
    Ok(bytes)
}

/// Remove a cache placement and (best effort) its copy. Idempotent.
///
/// # Errors
///
/// Metadata errors.
pub async fn invalidate<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node_addr: Option<&str>,
    generation: &GenId,
    index: u32,
) -> Result<bool, TierError> {
    let Some(p) = meta_cache::remove(meta, generation, index).await? else {
        return Ok(false);
    };
    if let Some(addr) = node_addr {
        let shard = ShardRef {
            group_id: p.group_id,
            chunk_id: chunk_id(generation, index),
            shard_index: 0,
        };
        let _ = clients.delete_shard(addr, shard, p.epoch).await;
    }
    Ok(true)
}

/// What an eviction pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EvictReport {
    /// Usage before.
    pub usage_before: u64,
    /// Usage after.
    pub usage_after: u64,
    /// Entries evicted.
    pub evicted: usize,
    /// Pinned entries that were skipped.
    pub skipped_pinned: usize,
    /// Usage is still above the low watermark (pinned entries or node
    /// errors prevented reaching it).
    pub over_watermark: bool,
}

/// LRU eviction of `tier` down to its low watermark when usage exceeds the
/// high watermark (RFC-006 §5.4). Pinned entries are never evicted. Each
/// eviction removes the record first, then deletes the copy with the
/// placement's epoch (a failed delete leaves an orphan for reconciliation).
///
/// # Errors
///
/// Metadata errors.
pub async fn evict_tier<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &CacheTierConfig,
    addr_of_group: &(dyn Fn(u64) -> Option<String> + Sync),
) -> Result<EvictReport, TierError> {
    let mut report = EvictReport {
        usage_before: meta_cache::usage(meta, &cfg.tier).await?,
        ..EvictReport::default()
    };
    report.usage_after = report.usage_before;
    if cfg.capacity_bytes == 0 || report.usage_before <= cfg.high_bytes() {
        return Ok(report);
    }
    let target = cfg.low_bytes();
    let mut after_ms = 0u64;
    loop {
        if report.usage_after <= target {
            break;
        }
        let batch = meta_cache::lru_candidates(meta, &cfg.tier, after_ms, 64).await?;
        if batch.is_empty() {
            break;
        }
        for (generation, index, p) in &batch {
            after_ms = p.last_access_ms;
            if report.usage_after <= target {
                break;
            }
            if p.pinned {
                report.skipped_pinned += 1;
                continue;
            }
            if let Some(removed) = meta_cache::remove(meta, generation, *index).await? {
                report.evicted += 1;
                report.usage_after = report.usage_after.saturating_sub(removed.bytes);
                if let Some(addr) = addr_of_group(removed.group_id) {
                    let shard = ShardRef {
                        group_id: removed.group_id,
                        chunk_id: chunk_id(generation, *index),
                        shard_index: 0,
                    };
                    let _ = clients.delete_shard(&addr, shard, removed.epoch).await;
                }
            }
        }
        // Continue after the last recency key seen (entries with the same
        // millisecond are re-read; `remove` makes that harmless).
        after_ms = after_ms.saturating_add(1);
    }
    report.usage_after = meta_cache::usage(meta, &cfg.tier).await?;
    report.over_watermark = report.usage_after > target;
    Ok(report)
}

/// What reconciling one cache node did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CacheReconcileReport {
    /// Copies without a matching placement (deleted).
    pub orphans_deleted: usize,
    /// Copies with a placement (kept).
    pub kept: usize,
}

/// Delete copies on a cache node that no placement of a live generation
/// names (RFC-006 §5.6): crashed populations, failed inserts, lost
/// metadata. Idempotent; paged.
///
/// # Errors
///
/// Metadata or node errors.
pub async fn reconcile_node<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    node_addr: &str,
    group: &GroupRecord,
    page_size: u32,
) -> Result<CacheReconcileReport, TierError> {
    let mut report = CacheReconcileReport::default();
    let mut after = None;
    loop {
        let page = clients
            .list_shards_page(node_addr, group.id, after, page_size)
            .await
            .map_err(|status| TierError::Node {
                addr: node_addr.to_string(),
                status,
            })?;
        for (shard, _) in &page.entries {
            let (prefix, index) = split_chunk_id(&shard.chunk_id);
            if meta_gc::cache_placement_in_group(meta, &prefix, index, group.id).await? {
                report.kept += 1;
                continue;
            }
            match clients.delete_shard(node_addr, *shard, group.epoch).await {
                Ok(()) => report.orphans_deleted += 1,
                Err(s) if s.code() == tonic::Code::NotFound => {}
                Err(status) => {
                    return Err(TierError::Node {
                        addr: node_addr.to_string(),
                        status,
                    });
                }
            }
        }
        match page.next {
            Some(n) => after = Some(n),
            None => return Ok(report),
        }
    }
}
