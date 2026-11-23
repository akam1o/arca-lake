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
                // An index entry without its generation (Phase 3.12 liveness
                // audit): nothing to retire; the entry goes (it would
                // otherwise fail every pass at the head of the range).
                let header = match generation_header(tx, generation).await {
                    Ok(h) => h,
                    Err(crate::MetaError::NoSuchGeneration) => {
                        tx.clear(k);
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                };
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
        .await;
        match did {
            Ok(true) => retired.push(generation),
            // Not due, gone, or a header that does not decode: this entry is
            // skipped, the others go on (an undecodable header is reported
            // wherever it is read, e.g. by the object's GET).
            Ok(false) | Err(crate::MetaError::Codec(_)) => {}
            Err(e) => return Err(e),
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

/// [`collectable`] from after `after` (a queue key a previous page ended
/// at; `None`: from the oldest). Returns the generations and, when the page
/// was full, the key to continue after (Phase 3.10 audit: generations that
/// cannot be collected yet stay at the head of the queue, and a GC reading
/// only the head never reached the others).
///
/// # Errors
///
/// Backend errors.
pub async fn collectable_page<S: KvStore>(
    store: &S,
    now_ms: u64,
    grace_ms: u64,
    after: Option<&crate::kv::Key>,
    limit: usize,
) -> Result<(Vec<GenId>, Option<crate::kv::Key>)> {
    let cutoff = now_ms.saturating_sub(grace_ms);
    let full = keys::retired_until_range(cutoff);
    let begin = match after {
        Some(k) if *k >= full.begin => {
            let mut b = k.clone();
            b.push(0);
            b
        }
        _ => full.begin.clone(),
    };
    if begin >= full.end {
        return Ok((Vec::new(), None));
    }
    let range = KeyRange::new(begin, full.end);
    let entries = transact(store, (range, limit), |tx, env| {
        Box::pin(async move {
            let (range, limit) = env;
            tx.get_range(range, *limit, false).await.map_err(Into::into)
        })
    })
    .await?;
    let next = (entries.len() >= limit.max(1))
        .then(|| entries.last().map(|(k, _)| k.clone()))
        .flatten();
    Ok((
        entries
            .iter()
            .filter_map(|(k, _)| keys::gen_from_index_key(k))
            .collect(),
        next,
    ))
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
            attribute_one(tx, &prefix, index, group_id).await
        })
    })
    .await
}

/// One shard to classify: generation prefix, chunk index, group.
pub type ShardQuery = ([u8; 12], u32, u64);

/// [`attribute_shard`] for many shards in one read-only transaction (Phase
/// 4.1, ADR-0033): the same reads, issued together at one read version,
/// and the same rules. A record of one shard that does not decode fails
/// that shard only.
///
/// # Errors
///
/// Backend errors (the whole batch).
pub async fn attribute_shards<S: KvStore>(
    store: &S,
    shards: &[ShardQuery],
) -> Result<Vec<Result<ShardAttribution>>> {
    transact(store, shards, |tx, shards| {
        Box::pin(async move { attribute_in_tx(tx, shards).await })
    })
    .await
}

fn chunk_id_of(prefix: &[u8; 12], index: u32) -> [u8; 16] {
    let mut cid = [0u8; 16];
    cid[..12].copy_from_slice(prefix);
    cid[12..].copy_from_slice(&index.to_be_bytes());
    cid
}

async fn attribute_in_tx<X: Txn>(
    tx: &mut X,
    shards: &[ShardQuery],
) -> Result<Vec<Result<ShardAttribution>>> {
    // The first generation with each prefix (the first step of
    // `generations_with_prefix`).
    let firsts = tx
        .get_first_many(
            &shards
                .iter()
                .map(|(p, _, _)| keys::gen_prefix_range(p))
                .collect::<Vec<_>>(),
        )
        .await?;
    let firsts: Vec<Option<GenId>> = firsts
        .into_iter()
        .map(|f| f.and_then(|(k, _)| keys::gen_from_gen_key(&k)))
        .collect();
    // For a shard with a generation: its header, the migration intent of
    // its chunk, its chunk record, and whether a second generation shares
    // the prefix (the next step of `generations_with_prefix`).
    let mut point = Vec::new();
    let mut after = Vec::new();
    for ((prefix, index, _), first) in shards.iter().zip(&firsts) {
        if let Some(g) = first {
            point.push(keys::gen_header(g));
            point.push(keys::migration(&chunk_id_of(prefix, *index)));
            point.push(keys::gen_chunk(g, *index));
            after.push(KeyRange::new(
                keys::gen_range(g).end,
                keys::gen_prefix_range(prefix).end,
            ));
        }
    }
    let values = tx.get_many(&point).await?;
    let seconds = tx.get_first_many(&after).await?;
    let mut values = values.into_iter();
    let mut seconds = seconds.into_iter();
    let mut out = Vec::with_capacity(shards.len());
    for (&(prefix, index, group_id), first) in shards.iter().zip(&firsts) {
        if first.is_none() {
            out.push(Ok(ShardAttribution::NoGeneration));
            continue;
        }
        let (header, intent, chunk) = (
            values.next().flatten(),
            values.next().flatten(),
            values.next().flatten(),
        );
        if seconds.next().flatten().is_some() {
            // Two generations share the prefix (practically never): the
            // reads of `attribute_shard`, one after another.
            out.push(per_shard(
                attribute_one(tx, &prefix, index, group_id).await,
            )?);
            continue;
        }
        out.push(per_shard(decide_one(header, intent, chunk, group_id))?);
    }
    Ok(out)
}

/// A codec error is the shard's; any other fails the batch.
fn per_shard(r: Result<ShardAttribution>) -> Result<Result<ShardAttribution>> {
    match r {
        Err(MetaError::Codec(e)) => Ok(Err(MetaError::Codec(e))),
        Err(e) => Err(e),
        ok => Ok(ok),
    }
}

/// The rules of [`attribute_shard`] for a prefix with exactly one
/// generation, from the values read.
fn decide_one(
    header: Option<Vec<u8>>,
    intent: Option<Vec<u8>>,
    chunk: Option<Vec<u8>>,
    group_id: u64,
) -> Result<ShardAttribution> {
    let header = header.map(|h| decode::<GenerationHeader>(&h)).transpose()?;
    let intent = intent
        .map(|i| decode::<crate::ops::object::MigrationIntent>(&i))
        .transpose()?;
    if intent.is_some_and(|i| i.target_group == group_id) {
        return Ok(ShardAttribution::Referenced);
    }
    if is_pending(header.as_ref()) {
        return Ok(ShardAttribution::Referenced);
    }
    if let Some(v) = chunk {
        let record: ChunkRecord = decode(&v)?;
        if record.placement.group_id == group_id {
            return Ok(ShardAttribution::Referenced);
        }
    }
    Ok(ShardAttribution::Unreferenced)
}

async fn attribute_one<X: Txn>(
    tx: &mut X,
    prefix: &[u8; 12],
    index: u32,
    group_id: u64,
) -> Result<ShardAttribution> {
    let gens = generations_with_prefix(tx, prefix).await?;
    if gens.is_empty() {
        return Ok(ShardAttribution::NoGeneration);
    }
    // A tier migration copying this chunk into this group
    // (Phase 3.5 audit CON-1): its target is kept until the switch
    // makes the record name it, or the intent is dropped or stale.
    let intent: Option<crate::ops::object::MigrationIntent> =
        crate::ops::read(tx, &keys::migration(&chunk_id_of(prefix, index))).await?;
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
}

/// What metadata says about a shard the scrub found at its member index
/// (RFC-008 §8.4, 0.2.0): finer than [`ShardAttribution`], from one
/// transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScrubAttribution {
    /// A live generation's chunk record places the chunk in this group.
    Placed(ChunkRecord),
    /// Held by an operation in progress, with no live record here: a
    /// migration intent targets this group (a copy before the switch), a
    /// pending generation may still record it here, or a generation being
    /// collected still records it here.
    InFlight {
        /// A migration intent targets this group.
        migration: bool,
    },
    /// Nothing references it here (no generation with the prefix, or none
    /// that places, may place or is copying the chunk here).
    Unreferenced,
}

/// Classify a shard for the scrub, in one transaction.
///
/// # Errors
///
/// Backend or codec errors.
pub async fn scrub_attribution<S: KvStore>(
    store: &S,
    prefix: &[u8; 12],
    index: u32,
    group_id: u64,
) -> Result<ScrubAttribution> {
    transact(store, (*prefix, index, group_id), |tx, env| {
        Box::pin(async move {
            let (prefix, index, group_id) = *env;
            let gens = generations_with_prefix(tx, &prefix).await?;
            if gens.is_empty() {
                return Ok(ScrubAttribution::Unreferenced);
            }
            let mut in_flight = false;
            for (generation, header) in &gens {
                if is_pending(header.as_ref()) {
                    in_flight = true;
                }
                if let Some(v) = tx.get(&keys::gen_chunk(generation, index)).await? {
                    let record: ChunkRecord = decode(&v)?;
                    if record.placement.group_id == group_id {
                        if is_live(header.as_ref()) {
                            return Ok(ScrubAttribution::Placed(record));
                        }
                        in_flight = true;
                    }
                }
            }
            let intent: Option<crate::ops::object::MigrationIntent> =
                crate::ops::read(tx, &keys::migration(&chunk_id_of(&prefix, index))).await?;
            if intent.is_some_and(|i| i.target_group == group_id) {
                return Ok(ScrubAttribution::InFlight { migration: true });
            }
            Ok(if in_flight {
                ScrubAttribution::InFlight { migration: false }
            } else {
                ScrubAttribution::Unreferenced
            })
        })
    })
    .await
}

/// Whether a fresh migration intent (younger than
/// [`crate::ops::object::MIGRATION_INTENT_TTL_MS`] at `now_ms`) copies
/// chunk `chunk_id` into `group_id`: its target shards are not referenced
/// yet, and become the chunk's placement if it switches.
///
/// # Errors
///
/// Backend or codec errors.
pub async fn migrating_into<S: KvStore>(
    store: &S,
    chunk_id: &[u8; 16],
    group_id: u64,
    now_ms: u64,
) -> Result<bool> {
    transact(store, (*chunk_id, group_id, now_ms), |tx, env| {
        Box::pin(async move {
            let (chunk_id, group_id, now_ms) = *env;
            let intent: Option<crate::ops::object::MigrationIntent> =
                crate::ops::read(tx, &keys::migration(&chunk_id)).await?;
            Ok(intent.is_some_and(|i| {
                i.target_group == group_id
                    && now_ms
                        < i.since_ms
                            .saturating_add(crate::ops::object::MIGRATION_INTENT_TTL_MS)
            }))
        })
    })
    .await
}

/// Whether a pending generation with `prefix` exists: its chunk records
/// may still be appended (a committed generation's are all written).
///
/// # Errors
///
/// Backend or codec errors.
pub async fn generation_is_pending<S: KvStore>(store: &S, prefix: &[u8; 12]) -> Result<bool> {
    transact(store, *prefix, |tx, prefix| {
        Box::pin(async move {
            let gens = generations_with_prefix(tx, prefix).await?;
            Ok(gens.iter().any(|(_, h)| is_pending(h.as_ref())))
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

/// Whether chunk `index` of a live generation with `prefix` is, or may yet
/// be, placed in `group_id` (Phase 3.10 review): a chunk record places it
/// there; or a pending generation has no record of it yet (the gateway
/// writes shards before it appends their records, so a missing record of a
/// write in progress is no proof of an orphan). `false` only when every
/// live generation with the prefix is committed without it here, or has
/// recorded it in another group.
///
/// # Errors
///
/// Backend or codec errors.
pub async fn chunk_may_be_in_group<S: KvStore>(
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
                match tx.get(&keys::gen_chunk(&generation, index)).await? {
                    Some(v) => {
                        let record: ChunkRecord = decode(&v)?;
                        if record.placement.group_id == group_id {
                            return Ok(true);
                        }
                    }
                    None => {
                        if header
                            .as_ref()
                            .is_some_and(|h| matches!(h.state, GenState::Pending { .. }))
                        {
                            return Ok(true);
                        }
                    }
                }
            }
            Ok(false)
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
