//! Multipart upload recipes (RFC-007 §5.3): hierarchical manifest.
//!
//! ```text
//! create_upload ─▶ begin_part ─▶ append_chunks* ─▶ seal_part      (per part)
//!               ─▶ complete                                       (one transaction)
//!               ─▶ abort ─▶ abort_step*                           (batched)
//! root Retiring ─▶ gc: finish_root_step* ─▶ gc::finish
//! ```
//!
//! Parts are generations of kind [`GenKind::Part`] with their own immutable
//! chunk manifests. `complete` publishes a root generation of kind
//! [`GenKind::MultipartRoot`] whose part manifest references
//! `("al","g",root,"m",n)` name the part generations: no chunk record is
//! copied and the transaction size is bounded by the number of parts.
//!
//! Invariant I-MP1 (a referenced part is never retired) is kept by making
//! every transition that retires a part re-read the upload record (and the
//! root's reference) in the same transaction; see `retire_part_if_unreferenced`.

use crate::error::Result;
use crate::kv::{KvStore, Txn};
use crate::ops::object::{
    PendingGeneration, Precondition, Publish, check_preconditions, publish_version,
    require_pending, retire, version_records,
};
use crate::ops::{bucket_by_id, bucket_by_name, current_pointer, generation_header, read, write};
use crate::records::{
    BucketRecord, Checksum, ChecksumAlgorithm, ChecksumType, GenId, GenKind, GenState,
    GenerationHeader, MultipartRecord, MultipartState, NULL_VERSION_ID, PartRecord, PartRef,
    UploadId, Versioning, decode, new_gen_id, new_upload_id, new_version_id,
};
use crate::{MetaError, keys, transact, transact_idempotent};

/// Highest part number.
pub const MAX_PART_NUMBER: u32 = 10_000;

/// Attributes fixed at creation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UploadInfo {
    /// Initiating principal.
    pub owner: String,
    /// Content type of the completed object.
    pub content_type: Option<String>,
    /// User metadata of the completed object.
    pub user_metadata: Vec<(String, String)>,
    /// Checksum algorithm announced at creation.
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// Checksum type announced at creation.
    pub checksum_type: Option<ChecksumType>,
}

/// CreateMultipartUpload.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn create_upload<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    info: &UploadInfo,
    now_ms: u64,
) -> Result<MultipartRecord> {
    if key.is_empty() || key.len() > 1024 {
        return Err(MetaError::InvalidArgument("invalid object key".into()));
    }
    let upload_id = new_upload_id();
    transact_idempotent(store, (bucket, key, info, now_ms, upload_id), |tx, env| {
        Box::pin(async move {
            let (bucket, key, info, now_ms, upload_id) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            let record = MultipartRecord {
                upload_id,
                bucket_id: b.id,
                key: key.to_string(),
                initiated_ms: now_ms,
                owner: info.owner.clone(),
                content_type: info.content_type.clone(),
                user_metadata: info.user_metadata.clone(),
                checksum_algorithm: info.checksum_algorithm,
                checksum_type: info.checksum_type,
                state: MultipartState::Open,
            };
            write(tx, &keys::mpu(b.id, key, &upload_id), &record)?;
            Ok(record)
        })
    })
    .await
}

async fn upload_record<X: Txn>(
    tx: &mut X,
    bucket_id: u64,
    key: &str,
    upload_id: &UploadId,
) -> Result<Option<MultipartRecord>> {
    read(tx, &keys::mpu(bucket_id, key, upload_id)).await
}

async fn open_upload<X: Txn>(
    tx: &mut X,
    bucket_id: u64,
    key: &str,
    upload_id: &UploadId,
) -> Result<MultipartRecord> {
    match upload_record(tx, bucket_id, key, upload_id).await? {
        Some(r) if r.state == MultipartState::Open => Ok(r),
        _ => Err(MetaError::NoSuchUpload),
    }
}

/// The upload record, whatever its state.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`]; a missing upload yields `Ok(None)`.
pub async fn get_upload<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    upload_id: &UploadId,
) -> Result<Option<MultipartRecord>> {
    transact(store, (bucket, key, *upload_id), |tx, env| {
        Box::pin(async move {
            let (bucket, key, upload_id) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            upload_record(tx, b.id, key, &upload_id).await
        })
    })
    .await
}

/// Allocate a pending part generation (UploadPart begins).
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`], [`MetaError::NoSuchUpload`] (upload missing
/// or not open), [`MetaError::InvalidArgument`] (part number).
pub async fn begin_part<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    upload_id: &UploadId,
    part_number: u32,
    now_ms: u64,
    lease_ms: u64,
) -> Result<PendingGeneration> {
    if part_number == 0 || part_number > MAX_PART_NUMBER {
        return Err(MetaError::InvalidArgument(format!(
            "part number must be between 1 and {MAX_PART_NUMBER}"
        )));
    }
    let generation = new_gen_id();
    let lease_until_ms = now_ms + lease_ms;
    transact_idempotent(
        store,
        (
            bucket,
            key,
            *upload_id,
            part_number,
            now_ms,
            lease_until_ms,
            generation,
        ),
        |tx, env| {
            Box::pin(async move {
                let (bucket, key, upload_id, part_number, now_ms, lease_until_ms, generation) =
                    *env;
                let b = bucket_by_name(tx, bucket).await?;
                let record = open_upload(tx, b.id, key, &upload_id).await?;
                let header = GenerationHeader {
                    state: GenState::Pending { lease_until_ms },
                    bucket_id: b.id,
                    key: record.key.clone(),
                    size: 0,
                    etag: String::new(),
                    content_type: None,
                    user_metadata: Vec::new(),
                    chunk_count: 0,
                    created_ms: now_ms,
                    kind: GenKind::Part {
                        upload_id,
                        part_number,
                    },
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

/// Attributes fixed when a part is sealed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PartInfo {
    /// Part size.
    pub size: u64,
    /// MD5 ETag.
    pub etag: String,
    /// Checksum, verified by the caller.
    pub checksum: Option<Checksum>,
    /// Chunk records appended.
    pub chunk_count: u32,
}

/// Result of [`seal_part`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sealed {
    /// The part generation.
    pub generation: GenId,
    /// Generation of the previous upload of the same part number, now
    /// retiring, if any.
    pub superseded: Option<GenId>,
}

/// Seal a pending part generation as part `n` of its upload (the
/// linearization point of UploadPart). Replaces and retires a previous part
/// with the same number.
///
/// # Errors
///
/// [`MetaError::WrongGenerationState`], [`MetaError::LeaseExpired`],
/// [`MetaError::NoSuchUpload`] (the upload was completed or aborted
/// meanwhile; the caller retires the generation).
pub async fn seal_part<S: KvStore>(
    store: &S,
    generation: &GenId,
    info: &PartInfo,
    now_ms: u64,
) -> Result<Sealed> {
    transact(store, (*generation, info, now_ms), |tx, env| {
        Box::pin(async move {
            let (generation, info, now_ms) = *env;
            let header = require_pending(tx, &generation, now_ms).await?;
            let GenKind::Part {
                upload_id,
                part_number,
            } = header.kind
            else {
                return Err(MetaError::WrongGenerationState("object", "part"));
            };
            open_upload(tx, header.bucket_id, &header.key, &upload_id).await?;
            let previous: Option<PartRecord> =
                read(tx, &keys::part(&upload_id, part_number)).await?;
            let mut superseded = None;
            if let Some(prev) = previous
                && prev.generation != generation
            {
                let prev_header = generation_header(tx, &prev.generation).await?;
                retire(tx, &prev.generation, &prev_header, now_ms)?;
                superseded = Some(prev.generation);
            }
            let committed = GenerationHeader {
                state: GenState::Committed,
                size: info.size,
                etag: info.etag.clone(),
                chunk_count: info.chunk_count,
                checksum: info.checksum.clone(),
                ..header.clone()
            };
            write(tx, &keys::gen_header(&generation), &committed)?;
            if let GenState::Pending { lease_until_ms } = header.state {
                tx.clear(&keys::pending(lease_until_ms, &generation));
            }
            write(
                tx,
                &keys::part(&upload_id, part_number),
                &PartRecord {
                    generation,
                    size: info.size,
                    etag: info.etag.clone(),
                    checksum: info.checksum.clone(),
                    chunk_count: info.chunk_count,
                    uploaded_ms: now_ms,
                },
            )?;
            Ok(Sealed {
                generation,
                superseded,
            })
        })
    })
    .await
}

/// A page of parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartsPage {
    /// The upload.
    pub upload: MultipartRecord,
    /// Parts in part-number order.
    pub parts: Vec<(u32, PartRecord)>,
    /// Whether more parts follow.
    pub is_truncated: bool,
}

/// ListParts: parts with number greater than `after`, at most `max`.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`], [`MetaError::NoSuchUpload`].
pub async fn list_parts<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    upload_id: &UploadId,
    after: u32,
    max: usize,
) -> Result<PartsPage> {
    transact(store, (bucket, key, *upload_id, after, max), |tx, env| {
        Box::pin(async move {
            let (bucket, key, upload_id, after, max) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            let upload = open_upload(tx, b.id, key, &upload_id).await?;
            let entries = tx
                .get_range(&keys::parts_after(&upload_id, after), max + 1, false)
                .await?;
            let is_truncated = entries.len() > max;
            let mut parts = Vec::new();
            for (k, v) in entries.into_iter().take(max) {
                let n = keys::part_number_from(&k)
                    .ok_or_else(|| MetaError::Codec("unexpected key in parts range".into()))?;
                parts.push((n, decode::<PartRecord>(&v)?));
            }
            Ok(PartsPage {
                upload,
                parts,
                is_truncated,
            })
        })
    })
    .await
}

/// ListMultipartUploads request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListUploadsRequest {
    /// Key prefix.
    pub prefix: String,
    /// Delimiter for common prefixes.
    pub delimiter: Option<String>,
    /// Resume after this key (and upload id, if given).
    pub key_marker: Option<String>,
    /// Resume after this upload id of `key_marker`.
    pub upload_id_marker: Option<UploadId>,
    /// Maximum uploads plus common prefixes.
    pub max_uploads: usize,
}

/// ListMultipartUploads response.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListUploadsResponse {
    /// Open uploads in (key, upload id) order.
    pub uploads: Vec<MultipartRecord>,
    /// Common prefixes.
    pub common_prefixes: Vec<String>,
    /// Whether more exist.
    pub is_truncated: bool,
    /// Markers for the next page (key, upload id).
    pub next_markers: Option<(String, UploadId)>,
}

/// ListMultipartUploads: open uploads only (RFC-007 §5.3.2).
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn list_uploads<S: KvStore>(
    store: &S,
    bucket: &str,
    req: &ListUploadsRequest,
) -> Result<ListUploadsResponse> {
    let max = req.max_uploads.clamp(1, 1000);
    transact(store, (bucket, req, max), |tx, env| {
        Box::pin(async move {
            let (bucket, req, max) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            let full = keys::mpu_prefix_range(b.id, &req.prefix);
            let mut begin = full.begin.clone();
            if let Some(k) = &req.key_marker {
                begin = begin.max(keys::mpu_after(b.id, k, req.upload_id_marker.as_ref()));
            }
            let delimiter = req.delimiter.as_deref().filter(|d| !d.is_empty());
            let mut resp = ListUploadsResponse::default();
            let mut last: Option<(String, UploadId)> = None;
            'outer: while begin < full.end {
                let page = tx
                    .get_range(
                        &crate::kv::KeyRange::new(begin.clone(), full.end.clone()),
                        1000,
                        false,
                    )
                    .await?;
                if page.is_empty() {
                    break;
                }
                for (k, v) in &page {
                    let (key, upload_id) = keys::mpu_from(k)
                        .ok_or_else(|| MetaError::Codec("unexpected key in upload range".into()))?;
                    if resp.uploads.len() + resp.common_prefixes.len() >= max {
                        resp.is_truncated = true;
                        resp.next_markers = last;
                        return Ok(resp);
                    }
                    if let Some(delim) = delimiter {
                        let tail = &key[req.prefix.len()..];
                        if let Some(pos) = tail.find(delim) {
                            let common = key[..req.prefix.len() + pos + delim.len()].to_string();
                            begin = crate::kv::strinc(&keys::mpu_prefix_range(b.id, &common).begin);
                            last = Some((common.clone(), [0xff; 16]));
                            resp.common_prefixes.push(common);
                            continue 'outer;
                        }
                    }
                    last = Some((key.clone(), upload_id));
                    begin = keys::mpu_after(b.id, &key, Some(&upload_id));
                    let record: MultipartRecord = decode(v)?;
                    if record.state == MultipartState::Open {
                        resp.uploads.push(record);
                    }
                }
            }
            Ok(resp)
        })
    })
    .await
}

/// One listed part of a Complete request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletePart {
    /// Part number.
    pub part_number: u32,
    /// ETag the client saw for the part (quotes stripped).
    pub etag: String,
    /// Checksum the client supplies for the part, if any (verified).
    pub checksum: Option<Vec<u8>>,
}

/// How the object checksum is derived from the parts' checksums (RFC-007
/// §5.7). Given `(part size, part checksum value)` pairs, returns the
/// object checksum value. Supplied by the caller so that this crate stays
/// free of hash implementations.
pub type DeriveChecksum<'a> =
    &'a (dyn Fn(ChecksumAlgorithm, ChecksumType, &[(u64, Vec<u8>)]) -> Option<Vec<u8>> + Sync);

/// Complete request.
pub struct CompleteRequest<'a> {
    /// Listed parts, as sent by the client.
    pub parts: &'a [CompletePart],
    /// Root generation id, chosen by the caller before the first attempt
    /// so that a retry after an unknown commit outcome is idempotent.
    pub root: GenId,
    /// Conditional headers.
    pub pre: &'a Precondition,
    /// Minimum size of every part but the last.
    pub min_part_size: u64,
    /// Composite ETag of the listed parts, computed by the caller from the
    /// listed ETags (`hex(md5(md5_1 ‖ … ‖ md5_N))-N`).
    pub etag: &'a str,
    /// Object checksum requested by the client, if any (compared with the
    /// derived value).
    pub expected_checksum: Option<&'a Checksum>,
    /// Checksum derivation.
    pub derive_checksum: DeriveChecksum<'a>,
}

/// Result of a Complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completed {
    /// The root generation now current.
    pub root: GenId,
    /// Version id assigned.
    pub version_id: String,
    /// Composite ETag.
    pub etag: String,
    /// Object size.
    pub size: u64,
    /// Object checksum, if any.
    pub checksum: Option<Checksum>,
    /// Part generations that were uploaded but not listed; the caller
    /// retires them with [`discard_parts`] (not done in the Complete
    /// transaction to keep its size bounded).
    pub unlisted: Vec<u32>,
    /// Whether this call found the upload already completed with the same
    /// parts (idempotent retry).
    pub already_completed: bool,
}

/// CompleteMultipartUpload: one transaction, the linearization point of the
/// upload (RFC-007 §5.3.3).
///
/// # Errors
///
/// [`MetaError::NoSuchUpload`], [`MetaError::InvalidPart`],
/// [`MetaError::InvalidPartOrder`], [`MetaError::EntityTooSmall`],
/// [`MetaError::PreconditionFailed`], [`MetaError::InvalidArgument`]
/// (checksum mismatch, reported as `BadDigest` by the gateway).
#[allow(clippy::too_many_lines)]
pub async fn complete<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    upload_id: &UploadId,
    req: &CompleteRequest<'_>,
    now_ms: u64,
) -> Result<Completed> {
    if req.parts.is_empty() {
        return Err(MetaError::InvalidArgument("no parts listed".into()));
    }
    for w in req.parts.windows(2) {
        if w[1].part_number <= w[0].part_number {
            return Err(MetaError::InvalidPartOrder);
        }
    }
    let fresh_version_id = new_version_id();
    transact(
        store,
        (
            bucket,
            key,
            *upload_id,
            req,
            now_ms,
            fresh_version_id.as_str(),
        ),
        |tx, env| {
            Box::pin(async move {
                let (bucket, key, upload_id, req, now_ms, fresh_version_id) = *env;
                let b = bucket_by_name(tx, bucket).await?;
                let Some(upload) = upload_record(tx, b.id, key, &upload_id).await? else {
                    return Err(MetaError::NoSuchUpload);
                };
                match upload.state {
                    MultipartState::Open => {}
                    MultipartState::Aborting => return Err(MetaError::NoSuchUpload),
                    MultipartState::Completed { root } => {
                        return already_completed(tx, &b, key, root, req).await;
                    }
                }
                // Every sealed part, in one range read (≤ 10 000 records).
                let mut records: Vec<(u32, PartRecord)> = Vec::new();
                for (k, v) in tx
                    .get_range(&keys::parts_range(&upload_id), usize::MAX, false)
                    .await?
                {
                    let n = keys::part_number_from(&k)
                        .ok_or_else(|| MetaError::Codec("unexpected key in parts range".into()))?;
                    records.push((n, decode::<PartRecord>(&v)?));
                }
                let mut refs = Vec::with_capacity(req.parts.len());
                let mut offset = 0u64;
                let mut checksum_inputs: Vec<(u64, Vec<u8>)> = Vec::new();
                let mut listed = std::collections::BTreeSet::new();
                for (i, p) in req.parts.iter().enumerate() {
                    let Some((_, rec)) = records.iter().find(|(n, _)| *n == p.part_number) else {
                        return Err(MetaError::InvalidPart(p.part_number));
                    };
                    if rec.etag != p.etag {
                        return Err(MetaError::InvalidPart(p.part_number));
                    }
                    if let Some(c) = &p.checksum
                        && rec.checksum.as_ref().is_none_or(|rc| rc.value != *c)
                    {
                        return Err(MetaError::InvalidPart(p.part_number));
                    }
                    if i + 1 < req.parts.len() && rec.size < req.min_part_size {
                        return Err(MetaError::EntityTooSmall(p.part_number));
                    }
                    if let Some(c) = &rec.checksum {
                        checksum_inputs.push((rec.size, c.value.clone()));
                    }
                    listed.insert(p.part_number);
                    refs.push(PartRef {
                        generation: rec.generation,
                        part_number: p.part_number,
                        offset,
                        size: rec.size,
                        chunk_count: rec.chunk_count,
                        etag: rec.etag.clone(),
                        checksum: rec.checksum.clone(),
                    });
                    offset += rec.size;
                }
                let size = offset;
                // Object checksum: derived when every listed part carries one.
                let checksum = match (upload.checksum_algorithm, req.expected_checksum) {
                    (Some(algo), expected) if checksum_inputs.len() == req.parts.len() => {
                        let kind = upload.checksum_type.unwrap_or(if algo.combinable() {
                            if algo == ChecksumAlgorithm::Crc64Nvme {
                                ChecksumType::FullObject
                            } else {
                                ChecksumType::Composite
                            }
                        } else {
                            ChecksumType::Composite
                        });
                        let value = (req.derive_checksum)(algo, kind, &checksum_inputs)
                            .ok_or_else(|| {
                                MetaError::InvalidArgument(format!(
                                    "checksum type {} is not available for {}",
                                    kind.name(),
                                    algo.name()
                                ))
                            })?;
                        let derived = Checksum {
                            algorithm: algo,
                            kind,
                            value,
                            parts: (kind == ChecksumType::Composite)
                                .then(|| u32::try_from(req.parts.len()).unwrap_or(u32::MAX)),
                        };
                        if let Some(e) = expected
                            && (e.algorithm != derived.algorithm || e.value != derived.value)
                        {
                            return Err(MetaError::InvalidArgument("checksum mismatch".into()));
                        }
                        Some(derived)
                    }
                    (_, Some(_)) => {
                        return Err(MetaError::InvalidArgument(
                            "object checksum requested but parts carry no checksums".into(),
                        ));
                    }
                    _ => None,
                };
                let unlisted: Vec<u32> = records
                    .iter()
                    .map(|(n, _)| *n)
                    .filter(|n| !listed.contains(n))
                    .collect();

                let current = current_pointer(tx, b.id, key).await?;
                check_preconditions(tx, b.id, key, req.pre, current.as_ref()).await?;
                let version_id = if b.versioning == Versioning::Enabled {
                    fresh_version_id.to_string()
                } else {
                    NULL_VERSION_ID.to_string()
                };
                let ordinal = current.as_ref().map_or(0, |c| c.next_ordinal);
                let root_header = GenerationHeader {
                    state: GenState::Committed,
                    bucket_id: b.id,
                    key: key.to_string(),
                    size,
                    etag: req.etag.to_string(),
                    content_type: upload.content_type.clone(),
                    user_metadata: upload.user_metadata.clone(),
                    chunk_count: 0,
                    created_ms: now_ms,
                    kind: GenKind::MultipartRoot {
                        upload_id,
                        part_count: u32::try_from(refs.len()).unwrap_or(u32::MAX),
                    },
                    checksum: checksum.clone(),
                };
                write(tx, &keys::gen_header(&req.root), &root_header)?;
                for r in &refs {
                    write(tx, &keys::part_ref(&req.root, r.part_number), r)?;
                }
                publish_version(
                    tx,
                    &b,
                    key,
                    current.as_ref(),
                    Publish {
                        generation: req.root,
                        version_id: &version_id,
                        delete_marker: false,
                        ordinal,
                        size,
                        etag: req.etag,
                        now_ms,
                    },
                )
                .await?;
                let completed_record = MultipartRecord {
                    state: MultipartState::Completed { root: req.root },
                    ..upload
                };
                write(tx, &keys::mpu(b.id, key, &upload_id), &completed_record)?;
                Ok(Completed {
                    root: req.root,
                    version_id,
                    etag: req.etag.to_string(),
                    size,
                    checksum,
                    unlisted,
                    already_completed: false,
                })
            })
        },
    )
    .await
}

/// A Complete that finds the upload already completed: the same result for
/// the same part list (lost-reply retry), `NoSuchUpload` otherwise.
async fn already_completed<X: Txn>(
    tx: &mut X,
    b: &BucketRecord,
    key: &str,
    root: GenId,
    req: &CompleteRequest<'_>,
) -> Result<Completed> {
    let header = match generation_header(tx, &root).await {
        Ok(h) => h,
        Err(MetaError::NoSuchGeneration) => return Err(MetaError::NoSuchUpload),
        Err(e) => return Err(e),
    };
    let mut refs = Vec::new();
    for (_, v) in tx
        .get_range(&keys::part_refs_range(&root), usize::MAX, false)
        .await?
    {
        refs.push(decode::<PartRef>(&v)?);
    }
    let same = refs.len() == req.parts.len()
        && refs
            .iter()
            .zip(req.parts)
            .all(|(r, p)| r.part_number == p.part_number && r.etag == p.etag);
    if !same {
        return Err(MetaError::NoSuchUpload);
    }
    // The version id: the pointer if the root is current, else its record.
    let version_id = match current_pointer(tx, b.id, key).await? {
        Some(c) if c.generation == root => c.version_id,
        _ => version_records(tx, b.id, key)
            .await?
            .into_iter()
            .find(|(_, v)| v.generation == root)
            .map(|(_, v)| v.version_id)
            .ok_or(MetaError::NoSuchUpload)?,
    };
    Ok(Completed {
        root,
        version_id,
        etag: header.etag,
        size: header.size,
        checksum: header.checksum,
        unlisted: Vec::new(),
        already_completed: true,
    })
}

/// Retire a part generation unless it is still referenced (I-MP1): by the
/// part record of an open upload, or by its root's reference after
/// completion. Clears the part record when the part is not referenced
/// through it. Returns whether the generation was retired.
async fn retire_part_if_unreferenced<X: Txn>(
    tx: &mut X,
    upload: &MultipartRecord,
    part_number: u32,
    rec: &PartRecord,
    now_ms: u64,
) -> Result<bool> {
    let referenced = match upload.state {
        MultipartState::Open => true,
        MultipartState::Aborting => false,
        MultipartState::Completed { root } => {
            let r: Option<PartRef> = read(tx, &keys::part_ref(&root, part_number)).await?;
            r.is_some_and(|r| r.generation == rec.generation)
                && tx.get(&keys::gen_header(&root)).await?.is_some()
        }
    };
    if referenced && upload.state == MultipartState::Open {
        return Ok(false);
    }
    tx.clear(&keys::part(&upload.upload_id, part_number));
    if referenced {
        // Referenced through the root: the record is redundant, the
        // generation stays.
        return Ok(false);
    }
    match generation_header(tx, &rec.generation).await {
        Ok(h) => {
            retire(tx, &rec.generation, &h, now_ms)?;
            Ok(true)
        }
        Err(MetaError::NoSuchGeneration) => Ok(false),
        Err(e) => Err(e),
    }
}

/// After Complete: retire the parts that were uploaded but not listed and
/// drop the now redundant part records, `batch` records per transaction.
/// Returns the number of part records processed; `0` means nothing is left.
/// Safe to call any number of times, by the completing gateway or by the
/// controller sweep.
///
/// # Errors
///
/// Backend errors. A missing or open upload processes nothing.
pub async fn discard_parts<S: KvStore>(
    store: &S,
    bucket_id: u64,
    key: &str,
    upload_id: &UploadId,
    batch: usize,
    now_ms: u64,
) -> Result<usize> {
    // Re-run after an unknown commit outcome (R-72): a part record is
    // cleared in the transaction that retires its generation, so a re-run
    // after an applied commit finds fewer records and retires nothing twice.
    transact_idempotent(
        store,
        (bucket_id, key, *upload_id, batch.max(1), now_ms),
        |tx, env| {
            Box::pin(async move {
                let (bucket_id, key, upload_id, batch, now_ms) = *env;
                let Some(upload) = upload_record(tx, bucket_id, key, &upload_id).await? else {
                    return Ok(0);
                };
                if upload.state == MultipartState::Open {
                    return Ok(0);
                }
                let entries = tx
                    .get_range(&keys::parts_range(&upload_id), batch, false)
                    .await?;
                let mut n = 0;
                for (k, v) in entries {
                    let part_number = keys::part_number_from(&k)
                        .ok_or_else(|| MetaError::Codec("unexpected key in parts range".into()))?;
                    let rec: PartRecord = decode(&v)?;
                    retire_part_if_unreferenced(tx, &upload, part_number, &rec, now_ms).await?;
                    n += 1;
                }
                if upload.state == MultipartState::Aborting && n < batch {
                    // Nothing left: the abort is complete.
                    tx.clear(&keys::mpu(bucket_id, key, &upload_id));
                }
                Ok(n)
            })
        },
    )
    .await
}

/// AbortMultipartUpload, transaction 1: `Open → Aborting` (the
/// linearization point of Abort). The parts are retired by
/// [`discard_parts`] until it returns `0`.
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`], [`MetaError::NoSuchUpload`] (missing or
/// completed). An upload already `Aborting` succeeds (the caller continues
/// the sweep).
pub async fn abort<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    upload_id: &UploadId,
) -> Result<MultipartRecord> {
    // Re-run after an unknown commit outcome (R-71, R-72): `Aborting` is
    // accepted again, so a re-run after an applied commit succeeds; one
    // after a Complete that won instead answers `NoSuchUpload`. Leaving the
    // outcome to the client parked the parts until the controller sweep.
    transact_idempotent(store, (bucket, key, *upload_id), |tx, env| {
        Box::pin(async move {
            let (bucket, key, upload_id) = *env;
            let b = bucket_by_name(tx, bucket).await?;
            let Some(upload) = upload_record(tx, b.id, key, &upload_id).await? else {
                return Err(MetaError::NoSuchUpload);
            };
            match upload.state {
                MultipartState::Completed { .. } => Err(MetaError::NoSuchUpload),
                MultipartState::Aborting => Ok(upload),
                MultipartState::Open => {
                    let aborting = MultipartRecord {
                        state: MultipartState::Aborting,
                        ..upload
                    };
                    write(tx, &keys::mpu(b.id, key, &upload_id), &aborting)?;
                    Ok(aborting)
                }
            }
        })
    })
    .await
}

/// Abort, then retire every part (convenience for gateways and sweeps).
///
/// # Errors
///
/// As [`abort`] and [`discard_parts`].
pub async fn abort_and_discard<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    upload_id: &UploadId,
    batch: usize,
    now_ms: u64,
) -> Result<()> {
    let upload = abort(store, bucket, key, upload_id).await?;
    while discard_parts(store, upload.bucket_id, key, upload_id, batch, now_ms).await? >= batch {}
    Ok(())
}

/// Uploads the controller sweep should work on (RFC-007 §5.3.4): every
/// upload not `Open`, plus `Open` uploads initiated before `expire_before_ms`.
/// Scans the whole upload key space; at most `limit` records.
///
/// # Errors
///
/// Backend errors.
pub async fn sweep_candidates<S: KvStore>(
    store: &S,
    expire_before_ms: Option<u64>,
    limit: usize,
) -> Result<Vec<MultipartRecord>> {
    // One bounded page per transaction (Phase 3.5 audit OPS-1: the whole
    // upload space was read in one transaction, which outgrows the store's
    // transaction limits and the controller's memory with many uploads).
    const PAGE: usize = 1000;
    let full = crate::kv::KeyRange::prefix(&foundationdb_tuple::pack(&("al", "mpu")));
    let mut begin = full.begin.clone();
    let mut out = Vec::new();
    loop {
        let range = crate::kv::KeyRange::new(begin.clone(), full.end.clone());
        let (page, more) = transact(store, (range, expire_before_ms), |tx, env| {
            Box::pin(async move {
                let (range, expire_before_ms) = env;
                let entries = tx.get_range(range, PAGE, false).await?;
                let more = entries.len() == PAGE;
                let mut page = Vec::new();
                for (k, v) in entries {
                    let r: MultipartRecord = decode(&v)?;
                    let wanted = match r.state {
                        MultipartState::Open => {
                            expire_before_ms.is_some_and(|t| r.initiated_ms < t)
                        }
                        MultipartState::Aborting => true,
                        MultipartState::Completed { .. } => {
                            // Work only if part records remain.
                            !tx.get_range(&keys::parts_range(&r.upload_id), 1, false)
                                .await?
                                .is_empty()
                        }
                    };
                    page.push((k, wanted.then_some(r)));
                }
                Ok((page, more))
            })
        })
        .await?;
        let last = page.last().map(|(k, _)| k.clone());
        for (_, r) in page {
            if let Some(r) = r {
                out.push(r);
                if out.len() >= limit {
                    return Ok(out);
                }
            }
        }
        match last {
            Some(k) if more => {
                begin = k;
                begin.push(0);
            }
            _ => return Ok(out),
        }
    }
}

/// One step of finishing a retiring multipart root (GC; I-MP2): retire up to
/// `batch` referenced part generations and clear their references, then
/// up to `batch` remaining part records of the upload. Returns `true` while
/// work remains. Refuses a root that is not `Retiring`.
///
/// The parts are retired as of the root's retirement, not `now_ms`: a
/// reader reaches a completed upload's parts only through its root, so the
/// grace the root has served covers them (Phase 3.6, R-64; a second grace
/// kept every multipart object's data for two grace periods). Parts never
/// listed in the completion have been unreachable since it.
///
/// # Errors
///
/// [`MetaError::WrongGenerationState`] if the root is not retiring.
pub async fn finish_root_step<S: KvStore>(
    store: &S,
    root: &GenId,
    batch: usize,
    now_ms: u64,
) -> Result<bool> {
    Ok(finish_root_step_counted(store, root, batch, now_ms)
        .await?
        .0)
}

/// [`finish_root_step`], also returning how many part generations the
/// step retired.
///
/// # Errors
///
/// As [`finish_root_step`].
pub async fn finish_root_step_counted<S: KvStore>(
    store: &S,
    root: &GenId,
    batch: usize,
    now_ms: u64,
) -> Result<(bool, usize)> {
    transact(store, (*root, batch.max(1), now_ms), |tx, env| {
        Box::pin(async move {
            let (root, batch, now_ms) = *env;
            let header = match generation_header(tx, &root).await {
                Ok(h) => h,
                Err(MetaError::NoSuchGeneration) => return Ok((false, 0)),
                Err(e) => return Err(e),
            };
            let GenState::Retiring { since_ms } = header.state else {
                return Err(MetaError::WrongGenerationState(
                    header.state.name(),
                    "retiring",
                ));
            };
            let GenKind::MultipartRoot { upload_id, .. } = header.kind else {
                return Ok((false, 0));
            };
            let since = since_ms.min(now_ms);
            let refs = tx
                .get_range(&keys::part_refs_range(&root), batch, false)
                .await?;
            let mut n = 0;
            let mut retired = 0;
            for (k, v) in &refs {
                let r: PartRef = decode(v)?;
                match generation_header(tx, &r.generation).await {
                    Ok(h) => {
                        retire(tx, &r.generation, &h, since)?;
                        retired += 1;
                    }
                    Err(MetaError::NoSuchGeneration) => {}
                    Err(e) => return Err(e),
                }
                tx.clear(k);
                n += 1;
            }
            if n >= batch {
                return Ok((true, retired));
            }
            // Remaining part records (unlisted parts a dead gateway never
            // discarded, or redundant records): all garbage now.
            let parts = tx
                .get_range(&keys::parts_range(&upload_id), batch - n, false)
                .await?;
            for (k, v) in &parts {
                let rec: PartRecord = decode(v)?;
                match generation_header(tx, &rec.generation).await {
                    Ok(h) => {
                        retire(tx, &rec.generation, &h, since)?;
                        retired += 1;
                    }
                    Err(MetaError::NoSuchGeneration) => {}
                    Err(e) => return Err(e),
                }
                tx.clear(k);
                n += 1;
            }
            if n >= batch {
                return Ok((true, retired));
            }
            tx.clear(&keys::mpu(header.bucket_id, &header.key, &upload_id));
            Ok((false, retired))
        })
    })
    .await
}

/// Whether a retiring part generation is still referenced (defence in
/// depth for GC, I-MP1): true if its upload is `Open` and the part record
/// names it, or `Completed { root }` with the root's reference naming it
/// and the root header present.
///
/// # Errors
///
/// Backend errors.
pub async fn part_is_referenced<X: Txn>(
    tx: &mut X,
    header: &GenerationHeader,
    generation: &GenId,
) -> Result<bool> {
    let GenKind::Part {
        upload_id,
        part_number,
    } = header.kind
    else {
        return Ok(false);
    };
    let Some(upload) = upload_record(tx, header.bucket_id, &header.key, &upload_id).await? else {
        return Ok(false);
    };
    match upload.state {
        MultipartState::Open => {
            let rec: Option<PartRecord> = read(tx, &keys::part(&upload_id, part_number)).await?;
            Ok(rec.is_some_and(|r| r.generation == *generation))
        }
        MultipartState::Aborting => Ok(false),
        MultipartState::Completed { root } => {
            let r: Option<PartRef> = read(tx, &keys::part_ref(&root, part_number)).await?;
            Ok(r.is_some_and(|r| r.generation == *generation)
                && tx.get(&keys::gen_header(&root)).await?.is_some())
        }
    }
}

/// Bucket record by id (for sweeps that only hold a record).
///
/// # Errors
///
/// [`MetaError::NoSuchBucket`].
pub async fn bucket_of<S: KvStore>(store: &S, bucket_id: u64) -> Result<BucketRecord> {
    transact(store, bucket_id, |tx, id| {
        Box::pin(async move { bucket_by_id(tx, *id).await })
    })
    .await
}
