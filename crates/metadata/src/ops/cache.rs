//! Cache placements (RFC-006 §5.2–5.4): discardable copies of chunks on
//! cache tiers, recorded per generation and chunk index, with a recency
//! index per tier for LRU eviction and a usage counter per tier.
//!
//! None of these records takes part in durability or visibility: deleting
//! every cache key changes no pointer, header, or chunk record (T3). Cache
//! keys live inside the generation's subspace, so `gc::finish` removes them
//! with the generation (after [`finish_cache`] fixed the index and usage).

use crate::error::Result;
use crate::kv::{KvStore, Txn};
use crate::ops::{generation_header, read, write};
use crate::records::{CachePlacement, GenId, GenState, decode};
use crate::{MetaError, keys, transact};

/// Outcome of [`insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// The placement was recorded.
    Inserted,
    /// An entry already existed (identical copy written twice).
    AlreadyPresent,
    /// The generation is no longer committed; the caller must delete the copy.
    GenerationGone,
    /// Admitting `bytes` would exceed `capacity_bytes`; the caller must
    /// delete the copy.
    OverCapacity,
}

fn usage_of(v: Option<Vec<u8>>) -> u64 {
    v.and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
        .map_or(0, u64::from_le_bytes)
}

async fn add_usage<X: Txn>(tx: &mut X, tier: &str, delta: i128) -> Result<u64> {
    let cur = usage_of(tx.get(&keys::cache_usage(tier)).await?);
    let next = u64::try_from((i128::from(cur) + delta).max(0)).unwrap_or(0);
    tx.set(&keys::cache_usage(tier), &next.to_le_bytes());
    Ok(next)
}

/// Record a cache copy of chunk `index` of `generation` as ready, in one
/// transaction that re-checks that the generation is committed and that
/// the tier stays within `capacity_bytes` (0 = unlimited).
///
/// # Errors
///
/// Backend errors.
pub async fn insert<S: KvStore>(
    store: &S,
    generation: &GenId,
    index: u32,
    placement: &CachePlacement,
    capacity_bytes: u64,
) -> Result<InsertOutcome> {
    transact(
        store,
        (*generation, index, placement, capacity_bytes),
        |tx, env| {
            Box::pin(async move {
                let (generation, index, placement, capacity_bytes) = *env;
                let header = match generation_header(tx, &generation).await {
                    Ok(h) => h,
                    Err(MetaError::NoSuchGeneration) => return Ok(InsertOutcome::GenerationGone),
                    Err(e) => return Err(e),
                };
                if header.state != GenState::Committed {
                    return Ok(InsertOutcome::GenerationGone);
                }
                if tx
                    .get(&keys::cache_placement(&generation, index))
                    .await?
                    .is_some()
                {
                    return Ok(InsertOutcome::AlreadyPresent);
                }
                let usage = usage_of(tx.get(&keys::cache_usage(&placement.tier)).await?);
                if capacity_bytes > 0 && usage + placement.bytes > capacity_bytes {
                    return Ok(InsertOutcome::OverCapacity);
                }
                write(tx, &keys::cache_placement(&generation, index), placement)?;
                tx.set(
                    &keys::cache_recency(
                        &placement.tier,
                        placement.last_access_ms,
                        &generation,
                        index,
                    ),
                    &[],
                );
                add_usage(tx, &placement.tier, i128::from(placement.bytes)).await?;
                Ok(InsertOutcome::Inserted)
            })
        },
    )
    .await
}

/// Remove a cache placement (eviction or invalidation). Returns the
/// placement that was removed, if any, so that the caller can delete the
/// copy on the node.
///
/// # Errors
///
/// Backend errors.
pub async fn remove<S: KvStore>(
    store: &S,
    generation: &GenId,
    index: u32,
) -> Result<Option<CachePlacement>> {
    transact(store, (*generation, index), |tx, env| {
        Box::pin(async move {
            let (generation, index) = *env;
            let Some(p) =
                read::<CachePlacement, _>(tx, &keys::cache_placement(&generation, index)).await?
            else {
                return Ok(None);
            };
            tx.clear(&keys::cache_placement(&generation, index));
            tx.clear(&keys::cache_recency(
                &p.tier,
                p.last_access_ms,
                &generation,
                index,
            ));
            add_usage(tx, &p.tier, -i128::from(p.bytes)).await?;
            Ok(Some(p))
        })
    })
    .await
}

/// Record a hit: move the entry to the front of the recency index.
///
/// # Errors
///
/// Backend errors.
pub async fn touch<S: KvStore>(
    store: &S,
    generation: &GenId,
    index: u32,
    now_ms: u64,
) -> Result<()> {
    transact(store, (*generation, index, now_ms), |tx, env| {
        Box::pin(async move {
            let (generation, index, now_ms) = *env;
            let Some(mut p) =
                read::<CachePlacement, _>(tx, &keys::cache_placement(&generation, index)).await?
            else {
                return Ok(());
            };
            if p.last_access_ms >= now_ms {
                return Ok(());
            }
            tx.clear(&keys::cache_recency(
                &p.tier,
                p.last_access_ms,
                &generation,
                index,
            ));
            p.last_access_ms = now_ms;
            tx.set(
                &keys::cache_recency(&p.tier, now_ms, &generation, index),
                &[],
            );
            write(tx, &keys::cache_placement(&generation, index), &p)?;
            Ok(())
        })
    })
    .await
}

/// Pin or unpin every cache placement of `generation`. Returns how many
/// entries were changed.
///
/// # Errors
///
/// Backend errors.
pub async fn set_pinned<S: KvStore>(store: &S, generation: &GenId, pinned: bool) -> Result<usize> {
    transact(store, (*generation, pinned), |tx, env| {
        Box::pin(async move {
            let (generation, pinned) = *env;
            let mut changed = 0;
            for (k, v) in tx
                .get_range(
                    &keys::cache_placements_range(&generation),
                    usize::MAX,
                    false,
                )
                .await?
            {
                let mut p: CachePlacement = decode(&v)?;
                if p.pinned != pinned {
                    p.pinned = pinned;
                    write(tx, &k, &p)?;
                    changed += 1;
                }
            }
            Ok(changed)
        })
    })
    .await
}

/// Cache placements of a generation, in index order.
///
/// # Errors
///
/// Backend errors.
pub async fn placements_of<S: KvStore>(
    store: &S,
    generation: &GenId,
) -> Result<Vec<(u32, CachePlacement)>> {
    transact(store, *generation, |tx, generation| {
        Box::pin(async move { placements_in_tx(tx, generation).await })
    })
    .await
}

/// Cache placements of a generation inside a transaction.
///
/// # Errors
///
/// Backend or codec errors.
pub async fn placements_in_tx<X: Txn>(
    tx: &mut X,
    generation: &GenId,
) -> Result<Vec<(u32, CachePlacement)>> {
    let mut out = Vec::new();
    for (k, v) in tx
        .get_range(&keys::cache_placements_range(generation), usize::MAX, false)
        .await?
    {
        let index = keys::cache_index_from(&k)
            .ok_or_else(|| MetaError::Codec("unexpected key in cache range".into()))?;
        out.push((index, decode::<CachePlacement>(&v)?));
    }
    Ok(out)
}

/// One cache placement.
///
/// # Errors
///
/// Backend errors.
pub async fn get<S: KvStore>(
    store: &S,
    generation: &GenId,
    index: u32,
) -> Result<Option<CachePlacement>> {
    transact(store, (*generation, index), |tx, env| {
        Box::pin(async move {
            let (generation, index) = *env;
            read(tx, &keys::cache_placement(&generation, index)).await
        })
    })
    .await
}

/// Bytes recorded as cached on `tier`.
///
/// # Errors
///
/// Backend errors.
pub async fn usage<S: KvStore>(store: &S, tier: &str) -> Result<u64> {
    transact(store, tier, |tx, tier| {
        Box::pin(async move { Ok(usage_of(tx.get(&keys::cache_usage(tier)).await?)) })
    })
    .await
}

/// Least recently used entries of `tier`: up to `limit` `(generation,
/// index, placement)` in ascending recency, pinned entries included (the
/// caller skips them) so that progress through the index is visible.
///
/// # Errors
///
/// Backend errors.
pub async fn lru_candidates<S: KvStore>(
    store: &S,
    tier: &str,
    after_ms: u64,
    limit: usize,
) -> Result<Vec<(GenId, u32, CachePlacement)>> {
    transact(store, (tier, after_ms, limit), |tx, env| {
        Box::pin(async move {
            let (tier, after_ms, limit) = *env;
            let entries = tx
                .get_range(
                    &keys::cache_recency_range_from(tier, after_ms),
                    limit,
                    false,
                )
                .await?;
            let mut out = Vec::new();
            for (k, _) in entries {
                let Some((generation, index)) = keys::cache_recency_from(&k) else {
                    continue;
                };
                match read::<CachePlacement, _>(tx, &keys::cache_placement(&generation, index))
                    .await?
                {
                    Some(p) => out.push((generation, index, p)),
                    // Dangling index entry (should not happen): drop it.
                    None => tx.clear(&k),
                }
            }
            Ok(out)
        })
    })
    .await
}

/// Before a generation's keys are cleared by GC: drop its recency entries
/// and usage. Returns the placements so that the caller deletes the copies.
///
/// # Errors
///
/// Backend errors.
pub async fn finish_cache<S: KvStore>(
    store: &S,
    generation: &GenId,
) -> Result<Vec<(u32, CachePlacement)>> {
    transact(store, *generation, |tx, generation| {
        Box::pin(async move {
            let entries = placements_in_tx(tx, generation).await?;
            for (index, p) in &entries {
                tx.clear(&keys::cache_recency(
                    &p.tier,
                    p.last_access_ms,
                    generation,
                    *index,
                ));
                tx.clear(&keys::cache_placement(generation, *index));
                add_usage(tx, &p.tier, -i128::from(p.bytes)).await?;
            }
            Ok(entries)
        })
    })
    .await
}
