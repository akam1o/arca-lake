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
    Checksum, ChunkRecord, CurrentPointer, GenId, GenKind, GenState, GenerationHeader,
    NULL_VERSION_ID, VersionRecord, Versioning, encode, new_gen_id, new_version_id,
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
                    kind: GenKind::Object,
                    checksum: None,
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
    append_chunks_renewing(store, generation, chunks, now_ms, 0).await
}

/// [`append_chunks`] that also renews the pending generation's lease to
/// `now_ms + lease_ms` (when that is later than the current lease): an
/// upload that keeps delivering chunks keeps its generation, however slow
/// the link; one that stops still expires (RFC-003, R-47). `lease_ms == 0`
/// renews nothing.
///
/// # Errors
///
/// As [`append_chunks`].
pub async fn append_chunks_renewing<S: KvStore>(
    store: &S,
    generation: &GenId,
    chunks: &[(u32, ChunkRecord)],
    now_ms: u64,
    lease_ms: u64,
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
            flush_batch(store, generation, now_ms, lease_ms, &batch).await?;
            batch.clear();
            batch_bytes = 0;
        }
        batch_bytes += bytes;
        batch.push((k, v));
    }
    if !batch.is_empty() {
        flush_batch(store, generation, now_ms, lease_ms, &batch).await?;
    }
    Ok(())
}

/// Renew the lease of a pending generation to `now_ms + lease_ms` if that
/// is later than the current expiry (no-op otherwise). Called by a gateway
/// while an upload is still delivering bytes (R-47: progress keeps the
/// generation alive, a stall lets it expire).
///
/// # Errors
///
/// [`MetaError::WrongGenerationState`] or [`MetaError::LeaseExpired`].
pub async fn renew_lease<S: KvStore>(
    store: &S,
    generation: &GenId,
    now_ms: u64,
    lease_ms: u64,
) -> Result<()> {
    transact_idempotent(store, (*generation, now_ms, lease_ms), |tx, env| {
        Box::pin(async move {
            let (generation, now_ms, lease_ms) = *env;
            let mut header = require_pending(tx, &generation, now_ms).await?;
            if let GenState::Pending { lease_until_ms } = header.state
                && now_ms.saturating_add(lease_ms) > lease_until_ms
            {
                let renewed = now_ms.saturating_add(lease_ms);
                tx.clear(&keys::pending(lease_until_ms, &generation));
                tx.set(&keys::pending(renewed, &generation), &[]);
                header.state = GenState::Pending {
                    lease_until_ms: renewed,
                };
                write(tx, &keys::gen_header(&generation), &header)?;
            }
            Ok(())
        })
    })
    .await
}

async fn flush_batch<S: KvStore>(
    store: &S,
    generation: &GenId,
    now_ms: u64,
    lease_ms: u64,
    batch: &[(Vec<u8>, Vec<u8>)],
) -> Result<()> {
    // Idempotent: chunk records are a pure function of the chunk, and the
    // lease renewal only ever moves the expiry later.
    transact_idempotent(store, (*generation, now_ms, lease_ms, batch), |tx, env| {
        Box::pin(async move {
            let (generation, now_ms, lease_ms, batch) = *env;
            let mut header = require_pending(tx, &generation, now_ms).await?;
            for (k, v) in batch {
                tx.set(k, v);
            }
            if lease_ms > 0
                && let GenState::Pending { lease_until_ms } = header.state
                && now_ms.saturating_add(lease_ms) > lease_until_ms
            {
                let renewed = now_ms.saturating_add(lease_ms);
                tx.clear(&keys::pending(lease_until_ms, &generation));
                tx.set(&keys::pending(renewed, &generation), &[]);
                header.state = GenState::Pending {
                    lease_until_ms: renewed,
                };
                write(tx, &keys::gen_header(&generation), &header)?;
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
    /// Client-supplied checksum, verified by the caller before commit.
    pub checksum: Option<Checksum>,
}

/// Conditional-write preconditions, evaluated inside the commit transaction.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Precondition {
    /// `If-Match`: the current version's ETag must equal this. A key with
    /// no current version fails with [`MetaError::NoSuchKey`] (AWS
    /// behaviour for conditional PUT and DELETE).
    pub if_match: Option<String>,
    /// `If-None-Match: *`: no current (non-delete-marker) version may exist.
    pub if_none_match_any: bool,
    /// Internal (no S3 header): the key's current pointer must name this
    /// generation (`Some(None)`: the key has no pointer). Used to re-run a
    /// write whose commit outcome is unknown against the state it was
    /// decided on, so that the re-run cannot act on a version written
    /// since (Phase 3.5 audit CON-1).
    pub if_current: Option<Option<VersionIdentity>>,
}

/// What tells published versions of a key apart: the generation and the
/// publication time (delete markers all have the zero generation, Phase
/// 3.5 audit CON-3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionIdentity {
    /// Generation (zero for a delete marker).
    pub generation: GenId,
    /// When it was published (`last_modified_ms` / `committed_ms`).
    pub published_ms: u64,
    /// Its ordinal: every publication of a key takes the next one, so two
    /// markers published in the same millisecond differ (CON-7).
    pub ordinal: u64,
}

impl VersionIdentity {
    fn of_pointer(c: &CurrentPointer) -> Self {
        Self {
            generation: c.generation,
            published_ms: c.last_modified_ms,
            ordinal: c.next_ordinal.saturating_sub(1),
        }
    }

    fn of_record(ordinal: u64, v: &VersionRecord) -> Self {
        Self {
            generation: v.generation,
            published_ms: v.committed_ms,
            ordinal,
        }
    }
}

/// The identity of the current version: its version record's (every
/// publication writes one at its ordinal; a pointer restored from an older
/// version keeps the newer `next_ordinal`, Phase 3.5 audit CON-11), else
/// derived from the pointer (unversioned buckets keep no records).
fn pointer_identity_in(c: &CurrentPointer, versions: &[(u64, VersionRecord)]) -> VersionIdentity {
    versions
        .iter()
        .find(|(_, v)| {
            v.version_id == c.version_id
                && v.generation == c.generation
                && v.committed_ms == c.last_modified_ms
        })
        .map_or_else(
            || VersionIdentity::of_pointer(c),
            |(o, v)| VersionIdentity::of_record(*o, v),
        )
}

async fn pointer_identity<X: Txn>(
    tx: &mut X,
    bucket_id: u64,
    key: &str,
    c: &CurrentPointer,
) -> Result<VersionIdentity> {
    Ok(pointer_identity_in(
        c,
        &version_records(tx, bucket_id, key).await?,
    ))
}

impl Precondition {
    /// No client condition (`If-Match`, `If-None-Match`).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.if_match.is_none() && !self.if_none_match_any
    }
}

/// The generation the key's current pointer names (a delete marker's too),
/// if any.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn current_generation<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
) -> Result<Option<GenId>> {
    Ok(current_identity(store, bucket, key)
        .await?
        .map(|i| i.generation))
}

/// The identity of the key's current version (a delete marker's too), if
/// any.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn current_identity<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
) -> Result<Option<VersionIdentity>> {
    transact(store, (bucket, key), |tx, env| {
        Box::pin(async move {
            let (bucket, key) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            match current_pointer(tx, b.id, key).await? {
                Some(c) => Ok(Some(pointer_identity(tx, b.id, key, &c).await?)),
                None => Ok(None),
            }
        })
    })
    .await
}

/// Whether a version with this id exists for the key (current or in the
/// history).
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn version_exists<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<bool> {
    transact(store, (bucket, key, version_id), |tx, env| {
        Box::pin(async move {
            let (bucket, key, version_id) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            if current_pointer(tx, b.id, key)
                .await?
                .is_some_and(|c| c.version_id == version_id)
            {
                return Ok(true);
            }
            Ok(version_records(tx, b.id, key)
                .await?
                .iter()
                .any(|(_, v)| v.version_id == version_id))
        })
    })
    .await
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

                check_preconditions(tx, bucket.id, &header.key, pre, current.as_ref()).await?;

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
                    checksum: info.checksum.clone(),
                    ..header.clone()
                };
                write(tx, &keys::gen_header(&generation), &committed)?;
                if let GenState::Pending { lease_until_ms } = header.state {
                    tx.clear(&keys::pending(lease_until_ms, &generation));
                }
                let published = publish_version(
                    tx,
                    &bucket,
                    &header.key,
                    current.as_ref(),
                    Publish {
                        generation,
                        version_id: &version_id,
                        delete_marker: false,
                        ordinal,
                        size: info.size,
                        etag: &info.etag,
                        now_ms,
                    },
                )
                .await?;
                Ok(Committed {
                    generation,
                    version_id,
                    retired: published,
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
            let _ = lease_until_ms;
            retire(tx, &generation, &header, now_ms)
        })
    })
    .await
}

/// What a generation turned out to be after a commit whose outcome was
/// unknown (RFC-003 §5.8).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
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

/// How a read resolves the version of a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup {
    /// The current version.
    Current,
    /// A specific version id (`"null"` selects the null version).
    Version(&'static str),
}

/// What a lookup found.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum Found {
    /// A data version.
    Object(ObjectInfo),
    /// A delete marker (the current one, or the requested version).
    DeleteMarker {
        /// Its version id.
        version_id: String,
        /// Its time.
        last_modified_ms: u64,
    },
    /// No current version (lookup `Current`) or no such version.
    Missing,
}

/// Resolve `version_id` (or the current version) to a pointer-shaped view:
/// for a non-current version the pointer is synthesized from its version
/// record (`next_ordinal` is 0 and meaningless).
async fn resolve_pointer<X: Txn>(
    tx: &mut X,
    bucket_id: u64,
    key: &str,
    version_id: Option<&str>,
) -> Result<Option<(CurrentPointer, bool)>> {
    let current = current_pointer(tx, bucket_id, key).await?;
    let Some(wanted) = version_id else {
        return Ok(current.map(|c| (c, true)));
    };
    if let Some(c) = &current
        && c.version_id == wanted
    {
        return Ok(Some((c.clone(), true)));
    }
    for (_, v) in version_records(tx, bucket_id, key).await? {
        if v.version_id == wanted {
            return Ok(Some((
                CurrentPointer {
                    generation: v.generation,
                    version_id: v.version_id,
                    delete_marker: v.delete_marker,
                    next_ordinal: 0,
                    size: v.size,
                    etag: v.etag,
                    last_modified_ms: v.committed_ms,
                },
                false,
            )));
        }
    }
    Ok(None)
}

/// HEAD semantics for the current version.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn head_object<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
) -> Result<Option<ObjectInfo>> {
    match head_version(store, bucket, key, None).await? {
        Found::Object(info) => Ok(Some(info)),
        Found::DeleteMarker { .. } | Found::Missing => Ok(None),
    }
}

/// The version id under which `generation` was published as a version of
/// `bucket_id`/`key` (`None` if no version record names it: never
/// published, or deleted since). Scans the key's versions newest first: for
/// resolving the answer of a commit whose outcome was unknown, not for a
/// request path (Phase 4.1, R-112).
///
/// # Errors
///
/// Backend errors.
pub async fn version_id_of<S: KvStore>(
    store: &S,
    bucket_id: u64,
    key: &str,
    generation: &GenId,
) -> Result<Option<String>> {
    const PAGE: usize = 256;
    let full = keys::versions_range(bucket_id, key);
    let mut begin = full.begin.clone();
    loop {
        let range = crate::kv::KeyRange::new(begin.clone(), full.end.clone());
        let page = transact(store, range, |tx, range| {
            Box::pin(async move { tx.get_range(range, PAGE, false).await.map_err(Into::into) })
        })
        .await?;
        for (_, v) in &page {
            let rec: VersionRecord = crate::records::decode(v)?;
            if !rec.delete_marker && rec.generation == *generation {
                return Ok(Some(rec.version_id));
            }
        }
        if page.len() < PAGE {
            return Ok(None);
        }
        let Some((last, _)) = page.last() else {
            return Ok(None);
        };
        begin.clone_from(last);
        begin.push(0);
    }
}

/// HEAD semantics for the current version or a specific version id.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn head_version<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<Found> {
    transact(store, (bucket, key, version_id), |tx, env| {
        Box::pin(async move {
            let (bucket, key, version_id) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            let Some((pointer, _)) = resolve_pointer(tx, b.id, key, version_id).await? else {
                return Ok(Found::Missing);
            };
            if pointer.delete_marker {
                return Ok(Found::DeleteMarker {
                    version_id: pointer.version_id,
                    last_modified_ms: pointer.last_modified_ms,
                });
            }
            let header = generation_header(tx, &pointer.generation).await?;
            Ok(Found::Object(ObjectInfo { pointer, header }))
        })
    })
    .await
}

/// Everything a GET needs, read in one transaction: the pointer, the
/// generation header, and either the chunk records (optionally only
/// `chunks`) or, for a multipart root, the part manifest references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// Pointer of the version read (synthesized for a non-current version).
    pub pointer: CurrentPointer,
    /// Whether the version read is the current one.
    pub is_latest: bool,
    /// Generation header.
    pub header: GenerationHeader,
    /// Chunk records in index order (empty for a multipart root).
    pub chunks: Vec<(u32, ChunkRecord)>,
    /// Cache placements of the same generation, read in the same
    /// transaction, in index order (RFC-006 §5.3).
    pub cache: Vec<(u32, crate::records::CachePlacement)>,
    /// Part manifest references in part-number order (multipart roots
    /// only). Chunk records of each part are read with
    /// [`read_part_manifest`].
    pub parts: Vec<crate::records::PartRef>,
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
    match read_manifest_version(store, bucket, key, None, chunks).await? {
        ManifestFound::Manifest(m) => Ok(Some(m)),
        ManifestFound::DeleteMarker { .. } | ManifestFound::Missing => Ok(None),
    }
}

/// Result of [`read_manifest_version`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum ManifestFound {
    /// A data version.
    Manifest(Manifest),
    /// A delete marker.
    DeleteMarker {
        /// Its version id.
        version_id: String,
        /// Its time.
        last_modified_ms: u64,
    },
    /// No current version, or no such version.
    Missing,
}

/// Read the manifest of the current version or of `version_id`.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn read_manifest_version<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    chunks: Option<Range<u32>>,
) -> Result<ManifestFound> {
    transact(store, (bucket, key, version_id, &chunks), |tx, env| {
        Box::pin(async move {
            let (bucket, key, version_id, chunks) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            let Some((pointer, is_latest)) = resolve_pointer(tx, b.id, key, version_id).await?
            else {
                return Ok(ManifestFound::Missing);
            };
            if pointer.delete_marker {
                return Ok(ManifestFound::DeleteMarker {
                    version_id: pointer.version_id,
                    last_modified_ms: pointer.last_modified_ms,
                });
            }
            let header = generation_header(tx, &pointer.generation).await?;
            let mut parts = Vec::new();
            let mut out = Vec::new();
            if matches!(header.kind, GenKind::MultipartRoot { .. }) {
                for (_, v) in tx
                    .get_range(
                        &keys::part_refs_range(&pointer.generation),
                        usize::MAX,
                        false,
                    )
                    .await?
                {
                    parts.push(crate::records::decode::<crate::records::PartRef>(&v)?);
                }
            } else {
                let range = match chunks {
                    Some(r) => keys::gen_chunks_subrange(&pointer.generation, r.clone()),
                    None => keys::gen_chunks_range(&pointer.generation),
                };
                for (k, v) in tx.get_range(&range, usize::MAX, false).await? {
                    let index = keys::chunk_index_from(&k)
                        .ok_or_else(|| MetaError::Codec("unexpected key in chunk range".into()))?;
                    out.push((index, crate::records::decode::<ChunkRecord>(&v)?));
                }
            }
            let cache = crate::ops::cache::placements_in_tx(tx, &pointer.generation).await?;
            Ok(ManifestFound::Manifest(Manifest {
                pointer,
                is_latest,
                header,
                chunks: out,
                cache,
                parts,
            }))
        })
    })
    .await
}

/// The chunk record `index` of `generation` as it is now (a reader whose
/// manifest predates a migration finds the chunk's new placement here).
///
/// # Errors
///
/// Backend or codec errors.
pub async fn read_chunk_record<S: KvStore>(
    store: &S,
    generation: &GenId,
    index: u32,
) -> Result<Option<ChunkRecord>> {
    transact(store, (*generation, index), |tx, (generation, index)| {
        Box::pin(async move { read(tx, &keys::gen_chunk(generation, *index)).await })
    })
    .await
}

/// Chunk records and cache placements of one part generation (immutable
/// once sealed; RFC-007 §5.3). `None` if the generation is gone.
///
/// # Errors
///
/// Backend or codec errors.
pub async fn read_part_manifest<S: KvStore>(
    store: &S,
    generation: &GenId,
) -> Result<
    Option<(
        Vec<(u32, ChunkRecord)>,
        Vec<(u32, crate::records::CachePlacement)>,
    )>,
> {
    transact(store, *generation, |tx, generation| {
        Box::pin(async move {
            if tx.get(&keys::gen_header(generation)).await?.is_none() {
                return Ok(None);
            }
            let mut out = Vec::new();
            for (k, v) in tx
                .get_range(&keys::gen_chunks_range(generation), usize::MAX, false)
                .await?
            {
                let index = keys::chunk_index_from(&k)
                    .ok_or_else(|| MetaError::Codec("unexpected key in chunk range".into()))?;
                out.push((index, crate::records::decode::<ChunkRecord>(&v)?));
            }
            let cache = crate::ops::cache::placements_in_tx(tx, generation).await?;
            Ok(Some((out, cache)))
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
    delete_object_if(store, bucket, key, &Precondition::default(), now_ms).await
}

/// DELETE without a version ID, with `If-Match` evaluated in the same
/// transaction (RFC-007 §5.5).
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`], [`MetaError::PreconditionFailed`],
/// [`MetaError::NoSuchKey`] (`If-Match` on a key without a current version).
pub async fn delete_object_if<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    pre: &Precondition,
    now_ms: u64,
) -> Result<Deleted> {
    delete_object_as(store, bucket, key, pre, &new_version_id(), now_ms).await
}

/// [`delete_object_if`] with the delete marker's version id chosen by the
/// caller (used on a versioning-enabled bucket), so that a re-run after an
/// unknown commit outcome creates the same marker and the caller can tell
/// whether the first attempt was applied ([`version_exists`]).
///
/// # Errors
///
/// As [`delete_object_if`].
pub async fn delete_object_as<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    pre: &Precondition,
    fresh_version_id: &str,
    now_ms: u64,
) -> Result<Deleted> {
    transact(
        store,
        (bucket, key, pre, now_ms, fresh_version_id),
        |tx, env| {
            Box::pin(async move {
                let (bucket, key, pre, now_ms, fresh_version_id) = *env;
                let b = bucket_by_name(tx, bucket).await?;
                let current = current_pointer(tx, b.id, key).await?;
                check_preconditions(tx, b.id, key, pre, current.as_ref()).await?;
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
                        let retired = publish_version(
                            tx,
                            &b,
                            key,
                            current.as_ref(),
                            Publish {
                                generation: [0; 16],
                                version_id: &version_id,
                                delete_marker: true,
                                ordinal,
                                size: 0,
                                etag: "",
                                now_ms,
                            },
                        )
                        .await?;
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

/// Outcome of a version-specific DELETE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedVersion {
    /// Whether the removed version was a delete marker.
    pub was_delete_marker: bool,
    /// Generation retired, if the version held data.
    pub retired: Option<GenId>,
    /// Whether the version existed (a missing version is a successful,
    /// idempotent no-op, RFC-007 §5.4).
    pub existed: bool,
}

/// DELETE with a version ID: removes exactly that version (data version or
/// delete marker). When it was the current version, the newest remaining
/// version becomes current; when none remains the key disappears.
/// Idempotent on the version id.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
#[allow(clippy::too_many_lines)]
pub async fn delete_version<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    version_id: &str,
    now_ms: u64,
) -> Result<DeletedVersion> {
    // The version is pinned to the generation it names now; the deletion
    // then removes the version only while it still names that generation,
    // so that a re-run after an unknown commit outcome (R-72) cannot remove
    // a version written since under a reused id ("null"; Phase 3.5 audit
    // CON-1). A re-run that finds it gone answers as the deletion did.
    let pinned = transact(store, (bucket, key, version_id), |tx, env| {
        Box::pin(async move {
            let (bucket, key, version_id) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            let current = current_pointer(tx, b.id, key).await?;
            if let Some(c) = current.filter(|c| c.version_id == version_id) {
                let id = pointer_identity(tx, b.id, key, &c).await?;
                return Ok(Some((id, c.delete_marker)));
            }
            Ok(version_records(tx, b.id, key)
                .await?
                .into_iter()
                .find(|(_, v)| v.version_id == version_id)
                .map(|(o, v)| (VersionIdentity::of_record(o, &v), v.delete_marker)))
        })
    })
    .await?;
    let Some(pinned) = pinned else {
        return Ok(DeletedVersion {
            was_delete_marker: false,
            retired: None,
            existed: false,
        });
    };
    transact_idempotent(
        store,
        (bucket, key, version_id, now_ms, pinned),
        |tx, env| {
            Box::pin(async move {
                let (bucket, key, version_id, now_ms, pinned) = *env;
                let b = bucket_by_name(tx, bucket).await?;
                let current = current_pointer(tx, b.id, key).await?;
                let versions = version_records(tx, b.id, key).await?;
                let target = versions
                    .iter()
                    .find(|(_, v)| v.version_id == version_id)
                    .cloned();
                let is_current = current.as_ref().is_some_and(|c| c.version_id == version_id);
                if !is_current && target.is_none() {
                    // Gone since it was pinned: deleted (by an earlier attempt).
                    return Ok(DeletedVersion {
                        was_delete_marker: pinned.1,
                        retired: None,
                        existed: true,
                    });
                }
                let (identity, generation, was_delete_marker) = match (&current, &target) {
                    (Some(c), _) if is_current => (
                        pointer_identity_in(c, &versions),
                        c.generation,
                        c.delete_marker,
                    ),
                    (_, Some((o, v))) => (
                        VersionIdentity::of_record(*o, v),
                        v.generation,
                        v.delete_marker,
                    ),
                    _ => {
                        return Ok(DeletedVersion {
                            was_delete_marker: false,
                            retired: None,
                            existed: false,
                        });
                    }
                };
                if identity != pinned.0 {
                    // The id now names another version: the pinned one is gone
                    // (this deletion's earlier attempt, or an overwrite).
                    return Ok(DeletedVersion {
                        was_delete_marker: pinned.1,
                        retired: None,
                        existed: true,
                    });
                }
                if let Some((ordinal, _)) = &target {
                    tx.clear(&keys::version(b.id, key, *ordinal));
                }
                let retired = if was_delete_marker {
                    None
                } else {
                    let header = generation_header(tx, &generation).await?;
                    retire(tx, &generation, &header, now_ms)?;
                    Some(generation)
                };
                if let Some(c) = current.as_ref().filter(|_| is_current) {
                    // Newest remaining version becomes current.
                    let next = versions
                        .iter()
                        .find(|(_, v)| v.version_id != version_id)
                        .map(|(_, v)| v.clone());
                    match next {
                        Some(v) => {
                            let pointer = CurrentPointer {
                                generation: v.generation,
                                version_id: v.version_id,
                                delete_marker: v.delete_marker,
                                next_ordinal: c.next_ordinal,
                                size: v.size,
                                etag: v.etag,
                                last_modified_ms: v.committed_ms,
                            };
                            write(tx, &keys::object_current(b.id, key), &pointer)?;
                        }
                        None => tx.clear(&keys::object_current(b.id, key)),
                    }
                }
                Ok(DeletedVersion {
                    was_delete_marker,
                    retired,
                    existed: true,
                })
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

pub(crate) async fn require_pending<X: Txn>(
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

pub(crate) async fn check_preconditions<X: Txn>(
    tx: &mut X,
    bucket_id: u64,
    key: &str,
    pre: &Precondition,
    current: Option<&CurrentPointer>,
) -> Result<()> {
    if let Some(expected) = &pre.if_current {
        let now = match current {
            Some(c) => Some(pointer_identity(tx, bucket_id, key, c).await?),
            None => None,
        };
        if now != *expected {
            return Err(MetaError::PreconditionFailed);
        }
    }
    let live = current.filter(|c| !c.delete_marker);
    if pre.if_none_match_any && live.is_some() {
        return Err(MetaError::PreconditionFailed);
    }
    if let Some(expected) = &pre.if_match {
        let Some(c) = live else {
            return Err(MetaError::NoSuchKey);
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

/// The version records of a key, newest first, with their ordinals.
pub(crate) async fn version_records<X: Txn>(
    tx: &mut X,
    bucket_id: u64,
    key: &str,
) -> Result<Vec<(u64, VersionRecord)>> {
    let mut out = Vec::new();
    for (k, v) in tx
        .get_range(&keys::versions_range(bucket_id, key), usize::MAX, false)
        .await?
    {
        let ordinal = keys::version_ordinal_from(&k)
            .ok_or_else(|| MetaError::Codec("unexpected key in version range".into()))?;
        out.push((ordinal, crate::records::decode::<VersionRecord>(&v)?));
    }
    Ok(out)
}

/// What [`publish_version`] publishes.
pub(crate) struct Publish<'a> {
    pub generation: GenId,
    pub version_id: &'a str,
    pub delete_marker: bool,
    pub ordinal: u64,
    pub size: u64,
    pub etag: &'a str,
    pub now_ms: u64,
}

/// Make `p` the current version of `key` (RFC-002 rule V1; RFC-007 §5.4):
/// write the pointer; on `Enabled` buckets write a version record and keep
/// a displaced `null` version as a non-current version (AWS semantics when
/// versioning is enabled after writes); on other buckets retire the
/// displaced `null` version, including a non-current `null` version left
/// from an earlier enabled period (`Suspended`). Returns the generation
/// retired, if any.
pub(crate) async fn publish_version<X: Txn>(
    tx: &mut X,
    bucket: &crate::records::BucketRecord,
    key: &str,
    current: Option<&CurrentPointer>,
    p: Publish<'_>,
) -> Result<Option<GenId>> {
    let pointer = CurrentPointer {
        generation: p.generation,
        version_id: p.version_id.to_string(),
        delete_marker: p.delete_marker,
        next_ordinal: p.ordinal + 1,
        size: p.size,
        etag: p.etag.to_string(),
        last_modified_ms: p.now_ms,
    };
    write(tx, &keys::object_current(bucket.id, key), &pointer)?;
    if bucket.versioning == Versioning::Enabled {
        let record = VersionRecord {
            generation: p.generation,
            version_id: p.version_id.to_string(),
            delete_marker: p.delete_marker,
            committed_ms: p.now_ms,
            size: p.size,
            etag: p.etag.to_string(),
        };
        write(tx, &keys::version(bucket.id, key, p.ordinal), &record)?;
        // A displaced null version (written while unversioned or suspended)
        // has no version record; give it one so it stays listable and
        // addressable as version "null" instead of being retired. A null
        // version that became current again through a version delete
        // already has its record.
        if let Some(c) = current
            && c.version_id == NULL_VERSION_ID
            && c.next_ordinal > 0
            && !version_records(tx, bucket.id, key)
                .await?
                .iter()
                .any(|(_, v)| v.version_id == NULL_VERSION_ID)
        {
            let record = VersionRecord {
                generation: c.generation,
                version_id: c.version_id.clone(),
                delete_marker: c.delete_marker,
                committed_ms: c.last_modified_ms,
                size: c.size,
                etag: c.etag.clone(),
            };
            write(
                tx,
                &keys::version(bucket.id, key, c.next_ordinal - 1),
                &record,
            )?;
        }
        return Ok(None);
    }
    let mut retired = retire_displaced(tx, current, p.now_ms).await?;
    if bucket.versioning == Versioning::Suspended {
        // A non-current null version from an earlier enabled period is
        // overwritten by the new null version (AWS behaviour).
        for (ordinal, v) in version_records(tx, bucket.id, key).await? {
            if v.version_id == NULL_VERSION_ID {
                tx.clear(&keys::version(bucket.id, key, ordinal));
                if !v.delete_marker && current.is_none_or(|c| c.generation != v.generation) {
                    let header = generation_header(tx, &v.generation).await?;
                    retire(tx, &v.generation, &header, p.now_ms)?;
                    retired.get_or_insert(v.generation);
                }
            }
        }
    }
    Ok(retired)
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

/// Mark a generation retiring (idempotent: an already retiring generation
/// keeps its original `since_ms` and queue entry).
pub(crate) fn retire<X: Txn>(
    tx: &mut X,
    generation: &GenId,
    header: &GenerationHeader,
    now_ms: u64,
) -> Result<()> {
    if matches!(header.state, GenState::Retiring { .. }) {
        return Ok(());
    }
    if let GenState::Pending { lease_until_ms } = header.state {
        tx.clear(&keys::pending(lease_until_ms, generation));
    }
    let retiring = GenerationHeader {
        state: GenState::Retiring { since_ms: now_ms },
        ..header.clone()
    };
    write(tx, &keys::gen_header(generation), &retiring)?;
    // The entry carries the generation's logical bytes, so that the backlog
    // can be measured in bytes (Phase 3.6). A multipart root holds no
    // chunks of its own: its parts carry theirs when the root retires them.
    // Entries written before carry nothing (counted as unknown).
    let bytes = if matches!(header.kind, crate::records::GenKind::MultipartRoot { .. }) {
        0
    } else {
        header.size
    };
    tx.set(&keys::retired(now_ms, generation), &bytes.to_le_bytes());
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
    attempt: &GenId,
) -> Result<bool> {
    transact(
        store,
        (*generation, index, *expected, new_record, *attempt),
        |tx, env| {
            Box::pin(async move {
                let (generation, index, expected, new_record, attempt) = *env;
                // Only the migration holding the chunk's intent switches,
                // and the intent goes with the switch (the target is then
                // referenced by the record itself).
                let intent: Option<MigrationIntent> =
                    read(tx, &keys::migration(&new_record.chunk_id)).await?;
                if intent.is_none_or(|i| i.attempt != attempt) {
                    return Ok(false);
                }
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
                // The target as the copy wrote it (Phase 3.8 review): a
                // membership change since (repair, replacement) bumped its
                // epoch, and the members that hold the copy may no longer
                // be the group's; the copy is abandoned and retried.
                let target: Option<crate::records::GroupRecord> =
                    read(tx, &keys::group(new_record.placement.group_id)).await?;
                if target.is_none_or(|g| {
                    g.epoch != new_record.placement.epoch
                        || g.state != crate::records::GroupState::Active
                }) {
                    return Ok(false);
                }
                write(tx, &keys::gen_chunk(&generation, index), new_record)?;
                tx.clear(&keys::migration(&new_record.chunk_id));
                Ok(true)
            })
        },
    )
    .await
}

/// A tier migration in flight for one chunk (Phase 3.5 audit CON-1,
/// CON-2): written before any target shard, it makes reconciliation keep
/// the target's shards and lets one migration of the chunk run at a time.
/// It goes with the switch, or after the target was cleaned up; one older
/// than [`MIGRATION_INTENT_TTL_MS`] can be taken over and is removed by
/// [`expire_migrations`] (its migration can no longer switch).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MigrationIntent {
    /// Group the chunk is being copied to.
    pub target_group: u64,
    /// The migration attempt (random).
    pub attempt: GenId,
    /// When it was taken.
    pub since_ms: u64,
}

impl crate::records::Record for MigrationIntent {}

/// Age after which a migration intent is stale.
pub const MIGRATION_INTENT_TTL_MS: u64 = 3_600_000;

/// Take the migration intent of `chunk_id` for `attempt`; `false` when
/// another live migration holds it.
///
/// # Errors
///
/// Backend errors.
pub async fn begin_migration<S: KvStore>(
    store: &S,
    chunk_id: &[u8; 16],
    target_group: u64,
    attempt: &GenId,
    now_ms: u64,
) -> Result<bool> {
    transact(
        store,
        (*chunk_id, target_group, *attempt, now_ms),
        |tx, env| {
            Box::pin(async move {
                let (chunk_id, target_group, attempt, now_ms) = *env;
                let held: Option<MigrationIntent> = read(tx, &keys::migration(&chunk_id)).await?;
                if held.is_some_and(|i| {
                    i.attempt != attempt
                        && now_ms < i.since_ms.saturating_add(MIGRATION_INTENT_TTL_MS)
                }) {
                    return Ok(false);
                }
                write(
                    tx,
                    &keys::migration(&chunk_id),
                    &MigrationIntent {
                        target_group,
                        attempt,
                        since_ms: now_ms,
                    },
                )?;
                Ok(true)
            })
        },
    )
    .await
}

/// Whether `attempt` still holds the migration intent of `chunk_id`.
///
/// # Errors
///
/// Backend errors.
pub async fn holds_migration<S: KvStore>(
    store: &S,
    chunk_id: &[u8; 16],
    attempt: &GenId,
) -> Result<bool> {
    transact(store, (*chunk_id, *attempt), |tx, env| {
        Box::pin(async move {
            let (chunk_id, attempt) = *env;
            let held: Option<MigrationIntent> = read(tx, &keys::migration(&chunk_id)).await?;
            Ok(held.is_some_and(|i| i.attempt == attempt))
        })
    })
    .await
}

/// Whether `attempt` holds the migration intent of `chunk_id` with at
/// least `margin_ms` left before another migration could take it over
/// (Phase 3.8 review: an attempt whose intent expired must not delete the
/// target records, which a successor that took the intent over may have
/// committed; the margin keeps the takeover out of the time between this
/// check and the deletes).
///
/// # Errors
///
/// Backend errors.
pub async fn holds_migration_fresh<S: KvStore>(
    store: &S,
    chunk_id: &[u8; 16],
    attempt: &GenId,
    now_ms: u64,
    margin_ms: u64,
) -> Result<bool> {
    transact(store, (*chunk_id, *attempt), |tx, env| {
        Box::pin(async move {
            let (chunk_id, attempt) = *env;
            let held: Option<MigrationIntent> = read(tx, &keys::migration(&chunk_id)).await?;
            Ok(held.is_some_and(|i| {
                i.attempt == attempt
                    && now_ms.saturating_sub(i.since_ms)
                        < MIGRATION_INTENT_TTL_MS.saturating_sub(margin_ms)
            }))
        })
    })
    .await
}

/// Drop the migration intent of `chunk_id` if `attempt` holds it (after
/// the target was cleaned up).
///
/// # Errors
///
/// Backend errors.
pub async fn end_migration<S: KvStore>(
    store: &S,
    chunk_id: &[u8; 16],
    attempt: &GenId,
) -> Result<()> {
    transact(store, (*chunk_id, *attempt), |tx, env| {
        Box::pin(async move {
            let (chunk_id, attempt) = *env;
            let held: Option<MigrationIntent> = read(tx, &keys::migration(&chunk_id)).await?;
            if held.is_some_and(|i| i.attempt == attempt) {
                tx.clear(&keys::migration(&chunk_id));
            }
            Ok(())
        })
    })
    .await
}

/// Remove migration intents older than [`MIGRATION_INTENT_TTL_MS`] (a
/// migration that crashed): reconciliation then reclaims their targets.
/// Returns how many were removed.
///
/// # Errors
///
/// Backend errors.
pub async fn expire_migrations<S: KvStore>(store: &S, now_ms: u64) -> Result<usize> {
    // Bounded pages over the whole range (Phase 3.5 audit OPS-3: one page
    // of young intents hid every later stale one).
    const PAGE: usize = 1000;
    let full = keys::migrations_range();
    let mut begin = full.begin.clone();
    let mut removed = 0;
    loop {
        let range = crate::kv::KeyRange::new(begin.clone(), full.end.clone());
        let (n, last, more) = transact(store, (range, now_ms), |tx, env| {
            Box::pin(async move {
                let (range, now_ms) = env;
                let entries = tx.get_range(range, PAGE, false).await?;
                let more = entries.len() == PAGE;
                let mut n = 0;
                for (k, v) in &entries {
                    let i: MigrationIntent = crate::records::decode(v)?;
                    if *now_ms >= i.since_ms.saturating_add(MIGRATION_INTENT_TTL_MS) {
                        tx.clear(k);
                        n += 1;
                    }
                }
                Ok((n, entries.last().map(|(k, _)| k.clone()), more))
            })
        })
        .await?;
        removed += n;
        match last {
            Some(mut k) if more => {
                k.push(0);
                begin = k;
            }
            _ => return Ok(removed),
        }
    }
}

/// Access record of a generation for automatic tiering (RFC-006 §5.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct AccessRecord {
    /// Last read, Unix milliseconds.
    pub last_ms: u64,
    /// Reads counted since the record was created or reset.
    pub count: u64,
}

impl crate::records::Record for AccessRecord {}

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

/// One generation's reads to add to its access record (ADR-0034).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessUpdate {
    /// The generation read.
    pub generation: GenId,
    /// Latest read time, Unix milliseconds.
    pub last_ms: u64,
    /// Reads to add.
    pub reads: u64,
    /// Time of the oldest of these reads (the age bound).
    pub oldest_ms: u64,
}

/// What [`record_accesses`] did with one update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessWrite {
    /// Written (`last_ms = max`, `count += reads`).
    Written,
    /// Not written: the generation is gone (GC finished it).
    Finished,
    /// Not written: its oldest read is older than the age bound at the
    /// attempt that committed.
    Stale,
}

/// Add many generations' reads to their access records in one transaction
/// (ADR-0034): for each, the generation's header and its record are read
/// together, and nothing is written for a generation whose header is gone
/// (no record for a generation GC finished: its `finish` clears the record
/// with the header, and conflicts with this read). An update whose oldest
/// read is more than `max_age_ms` older than `now()` at the attempt
/// (`0`: no bound) is left out, so that no read is written long after it
/// happened. Returns what was done with each update, in order.
///
/// Not idempotent (it adds): callers must not retry after an unknown
/// commit outcome.
///
/// # Errors
///
/// Backend and codec errors (the whole batch).
pub async fn record_accesses<S: KvStore>(
    store: &S,
    updates: &[AccessUpdate],
    max_age_ms: u64,
    now: impl Fn() -> u64 + Send + Sync,
) -> Result<Vec<AccessWrite>> {
    transact(store, (updates, max_age_ms, &now), |tx, env| {
        Box::pin(async move {
            let (updates, max_age_ms, now) = *env;
            let at = now();
            let fresh: Vec<&AccessUpdate> = updates
                .iter()
                .filter(|u| max_age_ms == 0 || at.saturating_sub(u.oldest_ms) <= max_age_ms)
                .collect();
            let mut want = Vec::with_capacity(fresh.len() * 2);
            for u in &fresh {
                want.push(keys::gen_header(&u.generation));
                want.push(keys::access(&u.generation));
            }
            let got = tx.get_many(&want).await?;
            let mut out = Vec::with_capacity(updates.len());
            let mut fresh_at = 0usize;
            for u in updates {
                if max_age_ms != 0 && at.saturating_sub(u.oldest_ms) > max_age_ms {
                    out.push(AccessWrite::Stale);
                    continue;
                }
                let (header, record) = (&got[2 * fresh_at], &got[2 * fresh_at + 1]);
                fresh_at += 1;
                if header.is_none() {
                    out.push(AccessWrite::Finished);
                    continue;
                }
                let mut rec: AccessRecord = match record {
                    Some(v) => crate::records::decode(v)?,
                    None => AccessRecord::default(),
                };
                rec.last_ms = rec.last_ms.max(u.last_ms);
                rec.count = rec.count.saturating_add(u.reads);
                write(tx, &keys::access(&u.generation), &rec)?;
                out.push(AccessWrite::Written);
            }
            Ok(out)
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
        Box::pin(async move {
            let got = tx
                .get_many(&[
                    keys::access(generation),
                    keys::access_count(generation),
                    keys::access_last(generation),
                ])
                .await?;
            let legacy: Option<AccessRecord> = match &got[0] {
                Some(v) => Some(crate::records::decode(v)?),
                None => None,
            };
            let le = |v: &Option<Vec<u8>>| {
                v.as_ref()
                    .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
                    .map(u64::from_le_bytes)
            };
            let (n, t) = (le(&got[1]), le(&got[2]));
            if legacy.is_none() && n.is_none() && t.is_none() {
                return Ok(None);
            }
            // Both layouts summed (ADR-0035 option B): each read is
            // written to exactly one of them.
            let l = legacy.unwrap_or_default();
            Ok(Some(AccessRecord {
                last_ms: l.last_ms.max(t.unwrap_or(0)),
                count: l.count.saturating_add(n.unwrap_or(0)),
            }))
        })
    })
    .await
}

/// [`record_accesses`] with the experimental atomic layout (ADR-0035
/// option B): the header is read (the GC check), the record is not; the
/// reads are added (`ADD`) and the time raised (`MAX`) by atomic
/// mutations, which do not conflict between gateways.
///
/// # Errors
///
/// Backend and codec errors (the whole batch).
pub async fn record_accesses_atomic<S: KvStore>(
    store: &S,
    updates: &[AccessUpdate],
    max_age_ms: u64,
    now: impl Fn() -> u64 + Send + Sync,
) -> Result<Vec<AccessWrite>> {
    transact(store, (updates, max_age_ms, &now), |tx, env| {
        Box::pin(async move {
            let (updates, max_age_ms, now) = *env;
            let at = now();
            let stale =
                |u: &AccessUpdate| max_age_ms != 0 && at.saturating_sub(u.oldest_ms) > max_age_ms;
            let want: Vec<crate::kv::Key> = updates
                .iter()
                .filter(|u| !stale(u))
                .map(|u| keys::gen_header(&u.generation))
                .collect();
            let got = tx.get_many(&want).await?;
            let mut out = Vec::with_capacity(updates.len());
            let mut fresh_at = 0usize;
            for u in updates {
                if stale(u) {
                    out.push(AccessWrite::Stale);
                    continue;
                }
                let header = &got[fresh_at];
                fresh_at += 1;
                if header.is_none() {
                    out.push(AccessWrite::Finished);
                    continue;
                }
                tx.atomic_add(
                    &keys::access_count(&u.generation),
                    i64::try_from(u.reads).unwrap_or(i64::MAX),
                )
                .await?;
                tx.atomic_max(&keys::access_last(&u.generation), u.last_ms)
                    .await?;
                out.push(AccessWrite::Written);
            }
            Ok(out)
        })
    })
    .await
}

/// Reset the access count of a generation (after a tiering decision);
/// nothing for a generation that is gone (ADR-0034: a reset must not
/// recreate the record of a generation GC finished).
///
/// # Errors
///
/// Backend errors.
pub async fn reset_access<S: KvStore>(store: &S, generation: &GenId, now_ms: u64) -> Result<()> {
    transact(store, (*generation, now_ms), |tx, env| {
        Box::pin(async move {
            let (generation, now_ms) = *env;
            if tx.get(&keys::gen_header(&generation)).await?.is_none() {
                return Ok(());
            }
            // Both layouts reset (ADR-0035 option B): the atomic keys
            // cleared, so that the legacy record alone holds `{now, 0}`.
            tx.clear(&keys::access_count(&generation));
            tx.clear(&keys::access_last(&generation));
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

/// Generations in ID order after `after`, with their headers, at most
/// `limit` (one transaction; jumps over each generation's subspace).
///
/// # Errors
///
/// Backend errors.
pub async fn list_generations<S: KvStore>(
    store: &S,
    after: Option<GenId>,
    limit: usize,
) -> Result<Vec<(GenId, GenerationHeader)>> {
    transact(store, (after, limit), |tx, env| {
        Box::pin(async move {
            let (after, limit) = *env;
            let full = keys::all_generations_range();
            let mut begin = match after {
                Some(g) => keys::gen_range(&g).end,
                None => full.begin.clone(),
            };
            let mut out = Vec::new();
            while out.len() < limit && begin < full.end {
                let first = tx
                    .get_range(
                        &crate::kv::KeyRange::new(begin.clone(), full.end.clone()),
                        1,
                        false,
                    )
                    .await?;
                let Some((k, _)) = first.into_iter().next() else {
                    break;
                };
                let Some(generation) = keys::gen_from_gen_key(&k) else {
                    break;
                };
                if let Some(h) =
                    read::<GenerationHeader, _>(tx, &keys::gen_header(&generation)).await?
                {
                    out.push((generation, h));
                }
                begin = keys::gen_range(&generation).end;
            }
            Ok(out)
        })
    })
    .await
}

/// Chunk records of a generation by ID (any state), in index order.
///
/// # Errors
///
/// Backend errors.
pub async fn read_manifest_of<S: KvStore>(
    store: &S,
    generation: &GenId,
) -> Result<Option<Vec<(u32, ChunkRecord)>>> {
    transact(store, *generation, |tx, generation| {
        Box::pin(async move {
            if tx.get(&keys::gen_header(generation)).await?.is_none() {
                return Ok(None);
            }
            let mut out = Vec::new();
            for (k, v) in tx
                .get_range(&keys::gen_chunks_range(generation), usize::MAX, false)
                .await?
            {
                let index = keys::chunk_index_from(&k)
                    .ok_or_else(|| MetaError::Codec("unexpected key in chunk range".into()))?;
                out.push((index, crate::records::decode::<ChunkRecord>(&v)?));
            }
            Ok(Some(out))
        })
    })
    .await
}

/// Where a generation sits in its key's history (R-54).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Located {
    /// The object version's generation: the generation itself, or for a
    /// multipart part the root that references it.
    pub root: GenId,
    /// S3 version id (`null` for an unversioned key), when a version of
    /// the key references the root.
    pub version_id: Option<String>,
    /// Whether that version is the current one.
    pub is_current: bool,
    /// Part number, when the generation is a multipart part.
    pub part_number: Option<u32>,
}

/// Find which version of its key a generation belongs to. Reads only the
/// key's current pointer and version records (and, for a part, their
/// generation headers): the cost is bounded by the versions of one key,
/// never a scan of the bucket.
///
/// # Errors
///
/// Backend or codec errors; `Ok(None)` when the header is gone.
pub async fn locate_generation<S: KvStore>(
    store: &S,
    generation: &GenId,
) -> Result<Option<Located>> {
    transact(store, *generation, |tx, generation| {
        Box::pin(async move {
            let header: Option<GenerationHeader> = read(tx, &keys::gen_header(generation)).await?;
            let Some(header) = header else {
                return Ok(None);
            };
            let (part_upload, part_number) = match header.kind {
                GenKind::Part {
                    upload_id,
                    part_number,
                } => (Some(upload_id), Some(part_number)),
                _ => (None, None),
            };
            // Candidate versions: the current pointer first, then the
            // version records newest first.
            let mut candidates: Vec<(GenId, String, bool)> = Vec::new();
            if let Some(p) = current_pointer(tx, header.bucket_id, &header.key).await?
                && !p.delete_marker
            {
                candidates.push((p.generation, p.version_id.clone(), true));
            }
            for (_, v) in tx
                .get_range(
                    &keys::versions_range(header.bucket_id, &header.key),
                    usize::MAX,
                    false,
                )
                .await?
            {
                let r = crate::records::decode::<VersionRecord>(&v)?;
                if !r.delete_marker && !candidates.iter().any(|c| c.0 == r.generation) {
                    candidates.push((r.generation, r.version_id, false));
                }
            }
            for (g, version_id, is_current) in candidates {
                let matches = match part_upload {
                    None => g == *generation,
                    Some(upload) => {
                        let h: Option<GenerationHeader> = read(tx, &keys::gen_header(&g)).await?;
                        h.is_some_and(|h| {
                            matches!(
                                h.kind,
                                GenKind::MultipartRoot { upload_id, .. } if upload_id == upload
                            )
                        })
                    }
                };
                if matches {
                    return Ok(Some(Located {
                        root: g,
                        version_id: Some(version_id),
                        is_current,
                        part_number,
                    }));
                }
            }
            Ok(Some(Located {
                root: *generation,
                version_id: None,
                is_current: false,
                part_number,
            }))
        })
    })
    .await
}
