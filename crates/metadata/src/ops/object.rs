//! Object write protocol (DD-4) and reads.
//!
//! ```text
//! begin_put ─▶ append_chunks* ─▶ commit_put        (success)
//!                              └▶ abort_put         (client abort)
//!              lease expiry ───▶ gc::expire_pending (crash)
//! ```
//!
//! The commit transaction is the linearization point of a PUT. It verifies
//! that the generation is still pending and leased, evaluates conditional
//! headers against the current pointer, publishes the new pointer (and a
//! version record on versioned buckets), and retires the generation it
//! replaces when that generation is not otherwise referenced.

use std::ops::Range;

use crate::error::Result;
use crate::kv::{KvStore, Txn};
use crate::ops::{bucket_by_id, bucket_by_name, current_pointer, generation_header, read, write};
use crate::records::{
    ChunkRecord, CurrentPointer, GenId, GenState, GenerationHeader, NULL_VERSION_ID, VersionRecord,
    Versioning, encode, new_gen_id, new_version_id,
};
use crate::{MetaError, keys, transact, transact_idempotent};

/// Result of [`begin_put`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingGeneration {
    /// The new generation.
    pub generation: GenId,
    /// Bucket the object belongs to.
    pub bucket_id: u64,
    /// Lease expiry, Unix milliseconds.
    pub lease_until_ms: u64,
}

/// Allocate a pending generation for `bucket/key`.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn begin_put<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    now_ms: u64,
    lease_ms: u64,
) -> Result<PendingGeneration> {
    validate_key(key)?;
    let generation = new_gen_id();
    let lease_until_ms = now_ms + lease_ms;
    // Idempotent: the generation ID is chosen once, so re-running after an
    // unknown commit outcome writes the same header again.
    transact_idempotent(
        store,
        (bucket, key, now_ms, lease_until_ms, generation),
        |tx, env| {
            Box::pin(async move {
                let (bucket, key, now_ms, lease_until_ms, generation) = *env;
                let b = bucket_by_name(tx, bucket).await?;
                let header = GenerationHeader {
                    state: GenState::Pending { lease_until_ms },
                    bucket_id: b.id,
                    key: key.to_string(),
                    size: 0,
                    etag: String::new(),
                    content_type: None,
                    user_metadata: Vec::new(),
                    chunk_count: 0,
                    created_ms: now_ms,
                };
                write(tx, &keys::gen_header(&generation), &header)?;
                tx.set(&keys::pending(lease_until_ms, &generation), &[]);
                Ok(PendingGeneration {
                    generation,
                    bucket_id: b.id,
                    lease_until_ms,
                })
            })
        },
    )
    .await
}

/// Record chunk manifest entries for a pending generation. Splits into as
/// many transactions as the backend's size limit requires; each transaction
/// re-checks that the generation is still pending and leased.
///
/// # Errors
///
/// [`MetaError::WrongGenerationState`] or [`MetaError::LeaseExpired`] if the
/// generation can no longer accept chunks.
pub async fn append_chunks<S: KvStore>(
    store: &S,
    generation: &GenId,
    chunks: &[(u32, ChunkRecord)],
    now_ms: u64,
) -> Result<()> {
    // FoundationDB counts more than the raw key and value bytes of a
    // mutation: each `set` also contributes a write conflict range (the key
    // twice) and per-mutation overhead. Charge a conservative estimate per
    // record and stay well below the documented limit; a batch that is too
    // large fails the whole PUT, while a batch that is too small only costs
    // an extra round trip.
    const PER_MUTATION_OVERHEAD: usize = 128;
    let budget = store.limits().max_transaction_bytes / 4;
    let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut batch_bytes = 0;
    for (index, chunk) in chunks {
        let k = keys::gen_chunk(generation, *index);
        let v = encode(chunk)?;
        let bytes = 3 * k.len() + v.len() + PER_MUTATION_OVERHEAD;
        if batch_bytes + bytes > budget && !batch.is_empty() {
            flush_batch(store, generation, now_ms, &batch).await?;
            batch.clear();
            batch_bytes = 0;
        }
        batch_bytes += bytes;
        batch.push((k, v));
    }
    if !batch.is_empty() {
        flush_batch(store, generation, now_ms, &batch).await?;
    }
    Ok(())
}

async fn flush_batch<S: KvStore>(
    store: &S,
    generation: &GenId,
    now_ms: u64,
    batch: &[(Vec<u8>, Vec<u8>)],
) -> Result<()> {
    // Idempotent: chunk records are a pure function of the chunk.
    transact_idempotent(store, (*generation, now_ms, batch), |tx, env| {
        Box::pin(async move {
            let (generation, now_ms, batch) = *env;
            require_pending(tx, &generation, now_ms).await?;
            for (k, v) in batch {
                tx.set(k, v);
            }
            Ok(())
        })
    })
    .await
}

/// Attributes fixed at commit.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommitInfo {
    /// Object size.
    pub size: u64,
    /// ETag.
    pub etag: String,
    /// Content type.
    pub content_type: Option<String>,
    /// User metadata.
    pub user_metadata: Vec<(String, String)>,
    /// Number of chunk records written with [`append_chunks`].
    pub chunk_count: u32,
}

/// Conditional-write preconditions, evaluated inside the commit transaction.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Precondition {
    /// `If-Match`: the current version's ETag must equal this.
    pub if_match: Option<String>,
    /// `If-None-Match: *`: no current (non-delete-marker) version may exist.
    pub if_none_match_any: bool,
}

/// Result of a successful commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    /// Generation now current.
    pub generation: GenId,
    /// Version ID assigned.
    pub version_id: String,
    /// Generation that was displaced and retired, if any.
    pub retired: Option<GenId>,
}

/// Publish a pending generation as the current version of its key.
///
/// # Errors
///
/// [`MetaError::WrongGenerationState`], [`MetaError::LeaseExpired`],
/// [`MetaError::PreconditionFailed`], [`MetaError::NoSuchBucket`].
pub async fn commit_put<S: KvStore>(
    store: &S,
    generation: &GenId,
    info: &CommitInfo,
    pre: &Precondition,
    now_ms: u64,
) -> Result<Committed> {
    // Chosen once so that retries of the same commit publish the same ID.
    let fresh_version_id = new_version_id();
    transact(
        store,
        (*generation, info, pre, now_ms, fresh_version_id.as_str()),
        |tx, env| {
            Box::pin(async move {
                let (generation, info, pre, now_ms, fresh_version_id) = *env;
                let header = require_pending(tx, &generation, now_ms).await?;
                let bucket = bucket_by_id(tx, header.bucket_id).await?;
                let current = current_pointer(tx, bucket.id, &header.key).await?;

                check_preconditions(tx, pre, current.as_ref()).await?;

                let version_id = if bucket.versioning == Versioning::Enabled {
                    fresh_version_id.to_string()
                } else {
                    NULL_VERSION_ID.to_string()
                };
                let ordinal = current.as_ref().map_or(0, |c| c.next_ordinal);

                let committed = GenerationHeader {
                    state: GenState::Committed,
                    size: info.size,
                    etag: info.etag.clone(),
                    content_type: info.content_type.clone(),
                    user_metadata: info.user_metadata.clone(),
                    chunk_count: info.chunk_count,
                    ..header.clone()
                };
                write(tx, &keys::gen_header(&generation), &committed)?;
                if let GenState::Pending { lease_until_ms } = header.state {
                    tx.clear(&keys::pending(lease_until_ms, &generation));
                }

                let pointer = CurrentPointer {
                    generation,
                    version_id: version_id.clone(),
                    delete_marker: false,
                    next_ordinal: ordinal + 1,
                    size: info.size,
                    etag: info.etag.clone(),
                    last_modified_ms: now_ms,
                };
                write(tx, &keys::object_current(bucket.id, &header.key), &pointer)?;
                if bucket.versioning == Versioning::Enabled {
                    let record = VersionRecord {
                        generation,
                        version_id: version_id.clone(),
                        delete_marker: false,
                        committed_ms: now_ms,
                        size: info.size,
                        etag: info.etag.clone(),
                    };
                    write(tx, &keys::version(bucket.id, &header.key, ordinal), &record)?;
                }

                let retired = retire_displaced(tx, current.as_ref(), now_ms).await?;
                Ok(Committed {
                    generation,
                    version_id,
                    retired,
                })
            })
        },
    )
    .await
}

/// Abandon a pending generation. Its chunks become garbage immediately.
///
/// # Errors
///
/// [`MetaError::NoSuchGeneration`]; aborting a non-pending generation is a
/// [`MetaError::WrongGenerationState`] error.
pub async fn abort_put<S: KvStore>(store: &S, generation: &GenId, now_ms: u64) -> Result<()> {
    transact(store, (*generation, now_ms), |tx, env| {
        Box::pin(async move {
            let (generation, now_ms) = *env;
            let header = generation_header(tx, &generation).await?;
            let GenState::Pending { lease_until_ms } = header.state else {
                return Err(MetaError::WrongGenerationState(
                    header.state.name(),
                    "pending",
                ));
            };
            tx.clear(&keys::pending(lease_until_ms, &generation));
            retire(tx, &generation, &header, now_ms)
        })
    })
    .await
}

/// What a generation turned out to be after a commit whose outcome was
/// unknown (RFC-003 §5.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The commit was applied: the generation is current (or a version).
    Committed(GenerationHeader),
    /// Not applied yet and still leased: the commit may be retried.
    Pending,
    /// Not applied and the lease has passed: expiry may retire it at any
    /// moment; the caller must not retry the commit.
    PendingExpired,
    /// The generation was retired (aborted, expired, or displaced).
    Retiring,
    /// No metadata for the generation.
    Missing,
}

/// Resolve the state of `generation` after a commit with an unknown
/// outcome. Reads only; the decision what to do next belongs to the caller
/// ([`abort_put`] to fence a late commit, or retry [`commit_put`]).
///
/// # Errors
///
/// Backend errors.
pub async fn resolve_commit<S: KvStore>(
    store: &S,
    generation: &GenId,
    now_ms: u64,
) -> Result<CommitOutcome> {
    transact(store, (*generation, now_ms), |tx, env| {
        Box::pin(async move {
            let (generation, now_ms) = *env;
            let header = match generation_header(tx, &generation).await {
                Ok(h) => h,
                Err(MetaError::NoSuchGeneration) => return Ok(CommitOutcome::Missing),
                Err(e) => return Err(e),
            };
            Ok(match header.state {
                GenState::Committed => CommitOutcome::Committed(header),
                GenState::Pending { lease_until_ms } if lease_until_ms > now_ms => {
                    CommitOutcome::Pending
                }
                GenState::Pending { .. } => CommitOutcome::PendingExpired,
                GenState::Retiring { .. } => CommitOutcome::Retiring,
            })
        })
    })
    .await
}

/// Current object and its header, or `None` if the key has no current
/// version or the current version is a delete marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    /// Current pointer.
    pub pointer: CurrentPointer,
    /// Header of the current generation.
    pub header: GenerationHeader,
}

/// HEAD semantics.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn head_object<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
) -> Result<Option<ObjectInfo>> {
    transact(store, (bucket, key), |tx, env| {
        Box::pin(async move {
            let (bucket, key) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            let Some(pointer) = current_pointer(tx, b.id, key).await? else {
                return Ok(None);
            };
            if pointer.delete_marker {
                return Ok(None);
            }
            let header = generation_header(tx, &pointer.generation).await?;
            Ok(Some(ObjectInfo { pointer, header }))
        })
    })
    .await
}

/// Everything a GET needs, read in one transaction: the current pointer, the
/// generation header, and the chunk records (optionally only `chunks`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// Current pointer.
    pub pointer: CurrentPointer,
    /// Generation header.
    pub header: GenerationHeader,
    /// Chunk records in index order.
    pub chunks: Vec<(u32, ChunkRecord)>,
    /// Cache placements of the same generation, read in the same
    /// transaction, in index order (RFC-006 §5.3).
    pub cache: Vec<(u32, crate::records::CachePlacement)>,
}

/// Read the manifest of the current version.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`]; a missing key or delete marker yields `Ok(None)`.
pub async fn read_manifest<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    chunks: Option<Range<u32>>,
) -> Result<Option<Manifest>> {
    transact(store, (bucket, key, &chunks), |tx, env| {
        Box::pin(async move {
            let (bucket, key, chunks) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            let Some(pointer) = current_pointer(tx, b.id, key).await? else {
                return Ok(None);
            };
            if pointer.delete_marker {
                return Ok(None);
            }
            let header = generation_header(tx, &pointer.generation).await?;
            let range = match chunks {
                Some(r) => keys::gen_chunks_subrange(&pointer.generation, r.clone()),
                None => keys::gen_chunks_range(&pointer.generation),
            };
            let mut out = Vec::new();
            for (k, v) in tx.get_range(&range, usize::MAX, false).await? {
                let index = keys::chunk_index_from(&k)
                    .ok_or_else(|| MetaError::Codec("unexpected key in chunk range".into()))?;
                out.push((index, crate::records::decode::<ChunkRecord>(&v)?));
            }
            let cache = crate::ops::cache::placements_in_tx(tx, &pointer.generation).await?;
            Ok(Some(Manifest {
                pointer,
                header,
                chunks: out,
                cache,
            }))
        })
    })
    .await
}

/// Outcome of a DELETE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deleted {
    /// Version ID of the delete marker created, if versioning is enabled.
    pub delete_marker_version: Option<String>,
    /// Generation retired by this delete, if any.
    pub retired: Option<GenId>,
}

/// DELETE without a version ID.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`]. Deleting a missing key succeeds (S3 semantics).
pub async fn delete_object<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    now_ms: u64,
) -> Result<Deleted> {
    let fresh_version_id = new_version_id();
    transact(
        store,
        (bucket, key, now_ms, fresh_version_id.as_str()),
        |tx, env| {
            Box::pin(async move {
                let (bucket, key, now_ms, fresh_version_id) = *env;
                let b = bucket_by_name(tx, bucket).await?;
                let current = current_pointer(tx, b.id, key).await?;
                match b.versioning {
                    Versioning::Disabled => {
                        let retired = retire_displaced(tx, current.as_ref(), now_ms).await?;
                        tx.clear(&keys::object_current(b.id, key));
                        Ok(Deleted {
                            delete_marker_version: None,
                            retired,
                        })
                    }
                    Versioning::Enabled | Versioning::Suspended => {
                        let version_id = if b.versioning == Versioning::Enabled {
                            fresh_version_id.to_string()
                        } else {
                            NULL_VERSION_ID.to_string()
                        };
                        let ordinal = current.as_ref().map_or(0, |c| c.next_ordinal);
                        let marker = CurrentPointer {
                            generation: [0; 16],
                            version_id: version_id.clone(),
                            delete_marker: true,
                            next_ordinal: ordinal + 1,
                            size: 0,
                            etag: String::new(),
                            last_modified_ms: now_ms,
                        };
                        write(tx, &keys::object_current(b.id, key), &marker)?;
                        if b.versioning == Versioning::Enabled {
                            let record = VersionRecord {
                                generation: [0; 16],
                                version_id: version_id.clone(),
                                delete_marker: true,
                                committed_ms: now_ms,
                                size: 0,
                                etag: String::new(),
                            };
                            write(tx, &keys::version(b.id, key, ordinal), &record)?;
                        }
                        let retired = retire_displaced(tx, current.as_ref(), now_ms).await?;
                        Ok(Deleted {
                            delete_marker_version: Some(version_id),
                            retired,
                        })
                    }
                }
            })
        },
    )
    .await
}

/// Header of a generation, whatever its state (for tests and tooling).
///
/// # Errors
///
/// [`MetaError::NoSuchGeneration`].
pub async fn get_generation<S: KvStore>(store: &S, generation: &GenId) -> Result<GenerationHeader> {
    transact(store, *generation, |tx, generation| {
        Box::pin(async move { generation_header(tx, generation).await })
    })
    .await
}

// ---- helpers -------------------------------------------------------------

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(MetaError::InvalidArgument("empty object key".into()));
    }
    if key.len() > 1024 {
        return Err(MetaError::InvalidArgument(
            "object key longer than 1024 bytes".into(),
        ));
    }
    Ok(())
}

async fn require_pending<X: Txn>(
    tx: &mut X,
    generation: &GenId,
    now_ms: u64,
) -> Result<GenerationHeader> {
    let header = generation_header(tx, generation).await?;
    match header.state {
        GenState::Pending { lease_until_ms } if lease_until_ms > now_ms => Ok(header),
        GenState::Pending { .. } => Err(MetaError::LeaseExpired),
        other => Err(MetaError::WrongGenerationState(other.name(), "pending")),
    }
}

async fn check_preconditions<X: Txn>(
    tx: &mut X,
    pre: &Precondition,
    current: Option<&CurrentPointer>,
) -> Result<()> {
    let live = current.filter(|c| !c.delete_marker);
    if pre.if_none_match_any && live.is_some() {
        return Err(MetaError::PreconditionFailed);
    }
    if let Some(expected) = &pre.if_match {
        let Some(c) = live else {
            return Err(MetaError::PreconditionFailed);
        };
        // The pointer's ETag is authoritative; re-read the header to make
        // the commit depend on the displaced generation as well.
        let header: Option<GenerationHeader> = read(tx, &keys::gen_header(&c.generation)).await?;
        if c.etag != *expected || header.is_none_or(|h| h.etag != *expected) {
            return Err(MetaError::PreconditionFailed);
        }
    }
    Ok(())
}

/// Retire the generation behind `current` if nothing else references it: a
/// `null` version is referenced only by the pointer, while a real version ID
/// is also referenced by its version record and must stay.
async fn retire_displaced<X: Txn>(
    tx: &mut X,
    current: Option<&CurrentPointer>,
    now_ms: u64,
) -> Result<Option<GenId>> {
    let Some(c) = current else {
        return Ok(None);
    };
    if c.delete_marker || c.version_id != NULL_VERSION_ID {
        return Ok(None);
    }
    let header = generation_header(tx, &c.generation).await?;
    retire(tx, &c.generation, &header, now_ms)?;
    Ok(Some(c.generation))
}

fn retire<X: Txn>(
    tx: &mut X,
    generation: &GenId,
    header: &GenerationHeader,
    now_ms: u64,
) -> Result<()> {
    let retiring = GenerationHeader {
        state: GenState::Retiring { since_ms: now_ms },
        ..header.clone()
    };
    write(tx, &keys::gen_header(generation), &retiring)?;
    tx.set(&keys::retired(now_ms, generation), &[]);
    Ok(())
}

/// Switch the durable placement of chunk `index` of `generation` to
/// `new_record`, only if the generation is committed and the record still
/// has `expected` as its placement (RFC-006 §5.8 step 4). Returns whether
/// the switch happened.
///
/// # Errors
///
/// Backend errors.
pub async fn switch_placement<S: KvStore>(
    store: &S,
    generation: &GenId,
    index: u32,
    expected: &crate::records::DurablePlacement,
    new_record: &ChunkRecord,
) -> Result<bool> {
    transact(
        store,
        (*generation, index, *expected, new_record),
        |tx, env| {
            Box::pin(async move {
                let (generation, index, expected, new_record) = *env;
                let header = match generation_header(tx, &generation).await {
                    Ok(h) => h,
                    Err(MetaError::NoSuchGeneration) => return Ok(false),
                    Err(e) => return Err(e),
                };
                if header.state != GenState::Committed {
                    return Ok(false);
                }
                let Some(current) =
                    read::<ChunkRecord, _>(tx, &keys::gen_chunk(&generation, index)).await?
                else {
                    return Ok(false);
                };
                if current.placement != expected || current.chunk_id != new_record.chunk_id {
                    return Ok(false);
                }
                write(tx, &keys::gen_chunk(&generation, index), new_record)?;
                Ok(true)
            })
        },
    )
    .await
}

/// Access record of a generation for automatic tiering (RFC-006 §5.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct AccessRecord {
    /// Last read, Unix milliseconds.
    pub last_ms: u64,
    /// Reads counted since the record was created or reset.
    pub count: u64,
}

/// Record a read of `generation` (callers coalesce; one write per call).
///
/// # Errors
///
/// Backend errors.
pub async fn record_access<S: KvStore>(
    store: &S,
    generation: &GenId,
    now_ms: u64,
    reads: u64,
) -> Result<()> {
    transact(store, (*generation, now_ms, reads), |tx, env| {
        Box::pin(async move {
            let (generation, now_ms, reads) = *env;
            let mut rec: AccessRecord = read(tx, &keys::access(&generation))
                .await?
                .unwrap_or_default();
            rec.last_ms = rec.last_ms.max(now_ms);
            rec.count = rec.count.saturating_add(reads);
            write(tx, &keys::access(&generation), &rec)
        })
    })
    .await
}

/// The access record of a generation.
///
/// # Errors
///
/// Backend errors.
pub async fn access_of<S: KvStore>(store: &S, generation: &GenId) -> Result<Option<AccessRecord>> {
    transact(store, *generation, |tx, generation| {
        Box::pin(async move { read(tx, &keys::access(generation)).await })
    })
    .await
}

/// Reset the access count of a generation (after a tiering decision).
///
/// # Errors
///
/// Backend errors.
pub async fn reset_access<S: KvStore>(store: &S, generation: &GenId, now_ms: u64) -> Result<()> {
    transact(store, (*generation, now_ms), |tx, env| {
        Box::pin(async move {
            let (generation, now_ms) = *env;
            write(
                tx,
                &keys::access(&generation),
                &AccessRecord {
                    last_ms: now_ms,
                    count: 0,
                },
            )
        })
    })
    .await
}
