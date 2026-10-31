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
use crate::kv::{KvStore, Txn};
use crate::ops::{generation_header, write};
use crate::records::{GenId, GenState, GenerationHeader};
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
    let candidates = transact(store, async |tx| {
        tx.get_range(&keys::pending_expired_range(now_ms), limit, false)
            .await
            .map_err(Into::into)
    })
    .await?;
    let mut retired = Vec::new();
    for (k, _) in candidates {
        let Some(generation) = keys::gen_from_index_key(&k) else {
            continue;
        };
        let did = transact(store, async |tx| {
            // The index entry may already be gone (committed or aborted).
            if tx.get(&k).await?.is_none() {
                return Ok(false);
            }
            let header = generation_header(tx, &generation).await?;
            let GenState::Pending { lease_until_ms } = header.state else {
                tx.clear(&k);
                return Ok(false);
            };
            if lease_until_ms > now_ms {
                return Ok(false);
            }
            tx.clear(&k);
            let retiring = GenerationHeader {
                state: GenState::Retiring { since_ms: now_ms },
                ..header
            };
            write(tx, &keys::gen_header(&generation), &retiring)?;
            tx.set(&keys::retired(now_ms, &generation), &[]);
            Ok(true)
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
    let entries = transact(store, async |tx| {
        tx.get_range(&keys::retired_until_range(cutoff), limit, false)
            .await
            .map_err(Into::into)
    })
    .await?;
    Ok(entries
        .iter()
        .filter_map(|(k, _)| keys::gen_from_index_key(k))
        .collect())
}

/// Remove all metadata of a retired generation. Call only after its shards
/// have been deleted from every storage node.
///
/// # Errors
///
/// [`MetaError::WrongGenerationState`] if the generation is not retiring
/// (a committed generation is never removed by GC).
pub async fn finish<S: KvStore>(store: &S, generation: &GenId) -> Result<()> {
    transact(store, async |tx| {
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
        tx.clear(&keys::retired(since_ms, generation));
        tx.clear_range(&keys::gen_range(generation));
        Ok(())
    })
    .await
}
