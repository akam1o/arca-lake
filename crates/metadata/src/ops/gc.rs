//! Garbage collection recipes (DD-7).
//!
//! Metadata GC has two inputs: pending generations whose lease expired
//! (crashed or abandoned writers) and retired generations (displaced by an
//! overwrite, deleted, or aborted). Both end up in the retired queue. After
//! the grace period a collector deletes the shards on the storage nodes and
//! then calls [`finish`] to remove the generation's metadata.
//!
//! Nothing here deletes data; it only drives state. Shard deletion is the
//! caller's job and must complete before [`finish`].

use crate::error::Result;
use crate::kv::{KeyRange, KvStore, Txn};
use crate::ops::{generation_header, write};
use crate::records::{ChunkRecord, GenId, GenKind, GenState, GenerationHeader, decode};
use crate::{MetaError, keys, transact};

/// Retire pending generations whose lease expired at or before `now_ms`.
/// Returns the generations retired. Each generation is handled in its own
/// transaction so that a concurrent commit racing the expiry is resolved by
/// the store: exactly one of them wins.
///
/// # Errors
///
/// Backend errors.
pub async fn expire_pending<S: KvStore>(
    store: &S,
    now_ms: u64,
    limit: usize,
) -> Result<Vec<GenId>> {
    let candidates = transact(store, (now_ms, limit), |tx, env| {
        Box::pin(async move {
            let (now_ms, limit) = *env;
            tx.get_range(&keys::pending_expired_range(now_ms), limit, false)
                .await
                .map_err(Into::into)
        })
    })
    .await?;
    let mut retired = Vec::new();
    for (k, _) in candidates {
        let Some(generation) = keys::gen_from_index_key(&k) else {
            continue;
        };
        let did = transact(store, (k, generation, now_ms), |tx, env| {
            Box::pin(async move {
                let (k, generation, now_ms) = env;
                // The index entry may already be gone (committed or aborted).
                if tx.get(k).await?.is_none() {
                    return Ok(false);
                }
                let header = generation_header(tx, generation).await?;
                let GenState::Pending { lease_until_ms } = header.state else {
                    tx.clear(k);
                    return Ok(false);
                };
                if lease_until_ms > *now_ms {
                    return Ok(false);
                }
                tx.clear(k);
                let retiring = GenerationHeader {
                    state: GenState::Retiring { since_ms: *now_ms },
                    ..header
                };
                write(tx, &keys::gen_header(generation), &retiring)?;
                tx.set(&keys::retired(*now_ms, generation), &[]);
                Ok(true)
            })
        })
        .await?;
        if did {
            retired.push(generation);
        }
    }
    Ok(retired)
}

/// Retired generations whose grace period has elapsed: retired at or before
/// `now_ms - grace_ms`.
///
/// # Errors
///
/// Backend errors.
pub async fn collectable<S: KvStore>(
    store: &S,
    now_ms: u64,
    grace_ms: u64,
    limit: usize,
) -> Result<Vec<GenId>> {
    let cutoff = now_ms.saturating_sub(grace_ms);
    let entries = transact(store, (cutoff, limit), |tx, env| {
        Box::pin(async move {
            let (cutoff, limit) = *env;
            tx.get_range(&keys::retired_until_range(cutoff), limit, false)
                .await
                .map_err(Into::into)
        })
    })
    .await?;
    Ok(entries
        .iter()
        .filter_map(|(k, _)| keys::gen_from_index_key(k))
        .collect())
}

/// The retired queue: entries (counted up to `cap`) and the time the
/// oldest was retired (R-64: the backlog GC works through).
///
/// # Errors
///
/// Backend errors.
pub async fn backlog<S: KvStore>(store: &S, cap: usize) -> Result<(usize, Option<u64>)> {
    transact(store, cap, |tx, cap| {
        Box::pin(async move {
            let entries = tx
                .get_range(&keys::retired_until_range(u64::MAX), *cap, false)
                .await?;
            let oldest = entries
                .first()
                .and_then(|(k, _)| keys::retired_since_from(k));
            Ok((entries.len(), oldest))
        })
    })
    .await
}

/// The retired queue measured (Phase 3.6, R-64).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Backlog {
    /// Entries counted (at most the cap).
    pub entries: usize,
    /// Logical bytes the counted entries carry.
    pub bytes: u64,
    /// Counted entries that carry no size (written before Phase 3.6).
    pub without_size: usize,
    /// When the oldest entry was retired.
    pub oldest_ms: Option<u64>,
    /// The cap was reached: more entries exist.
    pub truncated: bool,
}

/// Measure the retired queue up to `cap` entries, one bounded page per
/// transaction (a single transaction over a large queue would outlast the
/// store's transaction time limit).
///
/// # Errors
///
/// Backend errors.
pub async fn backlog_bytes<S: KvStore>(store: &S, cap: usize) -> Result<Backlog> {
    const PAGE: usize = 5000;
    let full = keys::retired_until_range(u64::MAX);
    let mut begin = full.begin.clone();
    let mut out = Backlog::default();
    while out.entries < cap {
        let range = KeyRange::new(begin.clone(), full.end.clone());
        let want = PAGE.min(cap - out.entries);
        let page = transact(store, (range, want), |tx, env| {
            Box::pin(async move {
                let (range, want) = env;
                tx.get_range(range, *want, false).await.map_err(Into::into)
            })
        })
        .await?;
        if out.oldest_ms.is_none() {
            out.oldest_ms = page.first().and_then(|(k, _)| keys::retired_since_from(k));
        }
        for (_, v) in &page {
            match <[u8; 8]>::try_from(v.as_slice()) {
                Ok(b) => out.bytes = out.bytes.saturating_add(u64::from_le_bytes(b)),
                Err(_) => out.without_size += 1,
            }
        }
        out.entries += page.len();
        if page.len() < want {
            return Ok(out);
        }
        let Some((last, _)) = page.last() else {
            return Ok(out);
        };
        begin.clone_from(last);
        begin.push(0);
    }
    out.truncated = true;
    Ok(out)
}

/// Remove all metadata of a retired generation. Call only after its shards
/// have been deleted from every storage node.
///
/// # Errors
///
/// [`MetaError::WrongGenerationState`] if the generation is not retiring
/// (a committed generation is never removed by GC).
pub async fn finish<S: KvStore>(store: &S, generation: &GenId) -> Result<()> {
    transact(store, *generation, |tx, generation| {
        Box::pin(async move {
            let header = match generation_header(tx, generation).await {
                Ok(h) => h,
                Err(MetaError::NoSuchGeneration) => return Ok(()), // idempotent
                Err(e) => return Err(e),
            };
            let GenState::Retiring { since_ms } = header.state else {
                return Err(MetaError::WrongGenerationState(
                    header.state.name(),
                    "retiring",
                ));
            };
            // A multipart root is finished only after every part reference
            // was processed by `multipart::finish_root_step` (I-MP2):
            // clearing references here would leak their part generations.
            if matches!(header.kind, GenKind::MultipartRoot { .. })
                && !tx
                    .get_range(&keys::part_refs_range(generation), 1, false)
                    .await?
                    .is_empty()
            {
                return Err(MetaError::InvalidArgument(
                    "multipart root still has part references".into(),
                ));
            }
            // Cache placements live in the generation's subspace; their
            // recency entries and usage are outside it.
            for (index, p) in crate::ops::cache::placements_in_tx(tx, generation).await? {
                tx.clear(&keys::cache_recency(
                    &p.tier,
                    p.last_access_ms,
                    generation,
                    index,
                ));
                tx.atomic_add(
                    &keys::cache_usage(&p.tier),
                    -i64::try_from(p.bytes).unwrap_or(i64::MAX),
                )
                .await?;
            }
            tx.clear(&keys::retired(since_ms, generation));
            tx.clear(&keys::access(generation));
            tx.clear_range(&keys::gen_range(generation));
            Ok(())
        })
    })
    .await
}

/// What metadata says about a shard `(generation prefix, chunk index)`
/// stored in a group (RFC-008 §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardAttribution {
    /// A generation with the prefix is pending, or one places this chunk in
    /// this group: the shard must be kept.
    Referenced,
    /// No generation with the prefix exists at all.
    NoGeneration,
    /// Generations with the prefix exist, none is pending, none places this
    /// chunk in this group.
    Unreferenced,
}

/// Every generation whose ID starts with `prefix`, with its header if the
/// header exists, read in one transaction. Prefix collisions are
/// practically nonexistent, so this is almost always one entry.
async fn generations_with_prefix<X: Txn>(
    tx: &mut X,
    prefix: &[u8; 12],
) -> Result<Vec<(GenId, Option<GenerationHeader>)>> {
    let full = keys::gen_prefix_range(prefix);
    let mut begin = full.begin.clone();
    let mut out = Vec::new();
    // The range also covers chunk records; jump over each generation's
    // subspace after reading its header.
    while begin < full.end {
        let first = tx
            .get_range(&KeyRange::new(begin.clone(), full.end.clone()), 1, false)
            .await?;
        let Some((k, _)) = first.into_iter().next() else {
            break;
        };
        let Some(generation) = keys::gen_from_gen_key(&k) else {
            break;
        };
        let header = match tx.get(&keys::gen_header(&generation)).await? {
            Some(h) => Some(decode::<GenerationHeader>(&h)?),
            None => None,
        };
        out.push((generation, header));
        begin = keys::gen_range(&generation).end;
    }
    Ok(out)
}

fn is_pending(header: Option<&GenerationHeader>) -> bool {
    header.is_some_and(|h| matches!(h.state, GenState::Pending { .. }))
}

fn is_live(header: Option<&GenerationHeader>) -> bool {
    header.is_some_and(|h| matches!(h.state, GenState::Pending { .. } | GenState::Committed))
}

/// Classify a shard by metadata reference, in one transaction.
///
/// # Errors
///
/// Backend or codec errors.
pub async fn attribute_shard<S: KvStore>(
    store: &S,
    prefix: &[u8; 12],
    index: u32,
    group_id: u64,
) -> Result<ShardAttribution> {
    transact(store, (*prefix, index, group_id), |tx, env| {
        Box::pin(async move {
            let (prefix, index, group_id) = *env;
            let gens = generations_with_prefix(tx, &prefix).await?;
            if gens.is_empty() {
                return Ok(ShardAttribution::NoGeneration);
            }
            // A tier migration copying this chunk into this group
            // (Phase 3.5 audit CON-1): its target is kept until the switch
            // makes the record name it, or the intent is dropped or stale.
            let mut cid = [0u8; 16];
            cid[..12].copy_from_slice(&prefix);
            cid[12..].copy_from_slice(&index.to_be_bytes());
            let intent: Option<crate::ops::object::MigrationIntent> =
                crate::ops::read(tx, &keys::migration(&cid)).await?;
            if intent.is_some_and(|i| i.target_group == group_id) {
                return Ok(ShardAttribution::Referenced);
            }
            for (generation, header) in &gens {
                if is_pending(header.as_ref()) {
                    return Ok(ShardAttribution::Referenced);
                }
                if let Some(v) = tx.get(&keys::gen_chunk(generation, index)).await? {
                    let record: ChunkRecord = decode(&v)?;
                    if record.placement.group_id == group_id {
                        return Ok(ShardAttribution::Referenced);
                    }
                }
            }
            Ok(ShardAttribution::Unreferenced)
        })
    })
    .await
}

/// Whether a pending or committed generation with `prefix` exists (a
/// retiring or finished generation's chunks are garbage).
///
/// # Errors
///
/// Backend or codec errors.
pub async fn generation_is_live<S: KvStore>(store: &S, prefix: &[u8; 12]) -> Result<bool> {
    transact(store, *prefix, |tx, prefix| {
        Box::pin(async move {
            let gens = generations_with_prefix(tx, prefix).await?;
            Ok(gens.iter().any(|(_, h)| is_live(h.as_ref())))
        })
    })
    .await
}

/// The chunk record of a live generation with `prefix` that places chunk
/// `index` in `group_id`, if any (used by repair to validate reconstructed
/// bytes against the recorded checksum).
///
/// # Errors
///
/// Backend or codec errors.
pub async fn chunk_record_in_group<S: KvStore>(
    store: &S,
    prefix: &[u8; 12],
    index: u32,
    group_id: u64,
) -> Result<Option<ChunkRecord>> {
    transact(store, (*prefix, index, group_id), |tx, env| {
        Box::pin(async move {
            let (prefix, index, group_id) = *env;
            for (generation, header) in generations_with_prefix(tx, &prefix).await? {
                if !is_live(header.as_ref()) {
                    continue;
                }
                if let Some(v) = tx.get(&keys::gen_chunk(&generation, index)).await? {
                    let record: ChunkRecord = decode(&v)?;
                    if record.placement.group_id == group_id {
                        return Ok(Some(record));
                    }
                }
            }
            Ok(None)
        })
    })
    .await
}

/// Whether a cache placement of a live generation with `prefix` names
/// chunk `index` in cache group `group_id` (cache reconciliation,
/// RFC-006 §5.6).
///
/// # Errors
///
/// Backend or codec errors.
pub async fn cache_placement_in_group<S: KvStore>(
    store: &S,
    prefix: &[u8; 12],
    index: u32,
    group_id: u64,
) -> Result<bool> {
    transact(store, (*prefix, index, group_id), |tx, env| {
        Box::pin(async move {
            let (prefix, index, group_id) = *env;
            for (generation, header) in generations_with_prefix(tx, &prefix).await? {
                if !is_live(header.as_ref()) {
                    continue;
                }
                if let Some(v) = tx.get(&keys::cache_placement(&generation, index)).await? {
                    let p: crate::records::CachePlacement = decode(&v)?;
                    if p.group_id == group_id {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        })
    })
    .await
}

/// The chunk record of chunk `index` of the live generation with `prefix`,
/// whatever its durable group (cache repair validates cache shards against
/// it, RFC-006 §5.9).
///
/// # Errors
///
/// Backend or codec errors.
pub async fn chunk_record_of<S: KvStore>(
    store: &S,
    prefix: &[u8; 12],
    index: u32,
) -> Result<Option<ChunkRecord>> {
    transact(store, (*prefix, index), |tx, env| {
        Box::pin(async move {
            let (prefix, index) = *env;
            for (generation, header) in generations_with_prefix(tx, &prefix).await? {
                if !is_live(header.as_ref()) {
                    continue;
                }
                if let Some(v) = tx.get(&keys::gen_chunk(&generation, index)).await? {
                    return Ok(Some(decode::<ChunkRecord>(&v)?));
                }
            }
            Ok(None)
        })
    })
    .await
}

/// The live generation with `prefix`, if any.
///
/// # Errors
///
/// Backend or codec errors.
pub async fn live_generation_with_prefix<S: KvStore>(
    store: &S,
    prefix: &[u8; 12],
) -> Result<Option<GenId>> {
    transact(store, *prefix, |tx, prefix| {
        Box::pin(async move {
            for (generation, header) in generations_with_prefix(tx, prefix).await? {
                if is_live(header.as_ref()) {
                    return Ok(Some(generation));
                }
            }
            Ok(None)
        })
    })
    .await
}

/// One page of live generations with their chunk records, in key order
/// after `after` (the metadata side of restore verification, RFC-015
/// §5.3.2: a chunk whose shards are gone from every node never appears in
/// a node listing). Returns the page and the last generation when more
/// may follow.
///
/// # Errors
///
/// Backend or codec errors.
pub async fn live_chunks_page<S: KvStore>(
    store: &S,
    after: Option<GenId>,
    limit: usize,
) -> Result<(Vec<(GenId, Vec<ChunkRecord>)>, Option<GenId>)> {
    transact(store, (after, limit), |tx, env| {
        Box::pin(async move {
            let (after, limit) = *env;
            let all = keys::all_generations_range();
            let mut begin = after.map_or_else(|| all.begin.clone(), |g| keys::gen_range(&g).end);
            let mut out = Vec::new();
            let mut last = None;
            let mut seen = 0usize;
            while begin < all.end && seen < limit {
                let first = tx
                    .get_range(&KeyRange::new(begin.clone(), all.end.clone()), 1, false)
                    .await?;
                let Some((k, _)) = first.into_iter().next() else {
                    break;
                };
                let Some(generation) = keys::gen_from_gen_key(&k) else {
                    break;
                };
                seen += 1;
                last = Some(generation);
                begin = keys::gen_range(&generation).end;
                let header = match tx.get(&keys::gen_header(&generation)).await? {
                    Some(h) => Some(decode::<GenerationHeader>(&h)?),
                    None => None,
                };
                if !is_live(header.as_ref()) {
                    continue;
                }
                let mut chunks = Vec::new();
                for (_, v) in tx
                    .get_range(&keys::gen_chunks_range(&generation), usize::MAX, false)
                    .await?
                {
                    chunks.push(decode::<ChunkRecord>(&v)?);
                }
                out.push((generation, chunks));
            }
            Ok((out, if seen >= limit { last } else { None }))
        })
    })
    .await
}
