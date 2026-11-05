//! Read path (RFC-004 §5.6, RFC-006 §5.3, §5.9, RFC-007 §5.3–5.5):
//! manifest (current or a version; inline chunks or part references) →
//! conditional headers → per chunk, a validated cache hit (possibly
//! degraded, then repaired asynchronously) or a durable read with
//! asynchronous cache population → stream. Part manifests of a multipart
//! object are resolved lazily, part by part, only for the requested range.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::integrity::Finding;
use arca_lake_metadata::ops::object::{self, Manifest, ManifestFound};
use arca_lake_metadata::records::{
    CachePlacement, Checksum, ChecksumAlgorithm, ChunkRecord, GenId, GenKind, GroupRecord, PartRef,
};
use arca_lake_protocol::client::concat_frames;
use arca_lake_tiering::TierError;
use arca_lake_tiering::cache::{self as tcache, CacheTierConfig};
use bytes::Bytes;
use s3s::dto::{
    ETag, ETagCondition, GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput,
    StreamingBlob, Timestamp,
};
use s3s::{S3Error, S3Response, S3Result, s3_error};

use super::backend::{ClusterBackend, meta_err, ms_to_timestamp, now_ms};
use super::map::ClusterMap;
use crate::checksum;

/// An in-flight chunk read.
type ChunkTask = tokio::task::JoinHandle<Result<Vec<Bytes>, S3Error>>;

fn addr_of(map: &ClusterMap) -> impl Fn(u64) -> Option<String> + Sync + '_ {
    move |node_id| map.node_addr(node_id).map(str::to_string)
}

// ---- conditional requests (RFC 9110 §13.1, RFC-007 §5.5) -----------------

/// The four conditional headers of a read.
#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_field_names)]
pub(super) struct Conditions<'a> {
    pub if_match: Option<&'a ETagCondition>,
    pub if_none_match: Option<&'a ETagCondition>,
    pub if_modified_since: Option<&'a Timestamp>,
    pub if_unmodified_since: Option<&'a Timestamp>,
}

/// Unix seconds of a timestamp (HTTP dates have second precision).
pub(super) fn timestamp_secs(t: &Timestamp) -> Option<i64> {
    let mut buf = Vec::new();
    t.format(s3s::dto::TimestampFormat::EpochSeconds, &mut buf)
        .ok()?;
    let s = std::str::from_utf8(&buf).ok()?;
    s.split('.').next()?.parse().ok()
}

/// Whether `cond` matches `etag`. `strong`: strong comparison (If-Match);
/// otherwise weak comparison (If-None-Match), where a weak tag matches by
/// opaque-tag equality.
fn etag_matches(cond: &ETagCondition, etag: &str, strong: bool) -> bool {
    match cond {
        ETagCondition::Any => true,
        ETagCondition::ETag(ETag::Strong(s)) => s == etag,
        ETagCondition::ETag(ETag::Weak(s)) => !strong && s == etag,
    }
}

/// Evaluate the conditions in RFC 9110 §13.2.2 order: `If-Match` (else
/// `If-Unmodified-Since`) → `412`; then `If-None-Match` (else
/// `If-Modified-Since`) → `304`. `last_modified_ms` is truncated to seconds
/// for the date comparisons.
pub(super) fn evaluate_conditions(
    c: &Conditions<'_>,
    etag: &str,
    last_modified_ms: u64,
) -> Result<(), S3Error> {
    let lm_secs = i64::try_from(last_modified_ms / 1000).unwrap_or(i64::MAX);
    if let Some(m) = c.if_match {
        if !etag_matches(m, etag, true) {
            return Err(s3_error!(PreconditionFailed));
        }
    } else if let Some(t) = c.if_unmodified_since
        && let Some(since) = timestamp_secs(t)
        && lm_secs > since
    {
        return Err(s3_error!(PreconditionFailed));
    }
    if let Some(n) = c.if_none_match {
        if etag_matches(n, etag, false) {
            return Err(s3_error!(NotModified));
        }
    } else if let Some(t) = c.if_modified_since
        && let Some(since) = timestamp_secs(t)
        && lm_secs <= since
    {
        return Err(s3_error!(NotModified));
    }
    Ok(())
}

/// An error carrying `x-amz-delete-marker: true` (and the marker's version
/// id): `404 NoSuchKey` for the current version, `405 MethodNotAllowed`
/// for an explicit version (AWS behaviour).
pub(super) fn delete_marker_error(explicit_version: bool, version_id: &str) -> S3Error {
    let mut e = if explicit_version {
        s3_error!(MethodNotAllowed, "the specified version is a delete marker")
    } else {
        s3_error!(NoSuchKey)
    };
    let mut h = hyper::HeaderMap::new();
    h.insert(
        "x-amz-delete-marker",
        hyper::header::HeaderValue::from_static("true"),
    );
    if let Ok(v) = hyper::header::HeaderValue::from_str(version_id) {
        h.insert("x-amz-version-id", v);
    }
    e.set_headers(h);
    e
}

/// The checksum headers of a stored checksum, as `(crc32, crc32c,
/// crc64nvme, sha1, sha256, type)` output fields.
pub(super) type ChecksumFields = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<s3s::dto::ChecksumType>,
);

pub(super) fn checksum_fields(c: Option<&Checksum>) -> ChecksumFields {
    let Some(c) = c else {
        return (None, None, None, None, None, None);
    };
    let v = Some(checksum::header_value(c));
    let kind = Some(s3s::dto::ChecksumType::from(c.kind.name().to_string()));
    match c.algorithm {
        ChecksumAlgorithm::Crc32 => (v, None, None, None, None, kind),
        ChecksumAlgorithm::Crc32c => (None, v, None, None, None, kind),
        ChecksumAlgorithm::Crc64Nvme => (None, None, v, None, None, kind),
        ChecksumAlgorithm::Sha1 => (None, None, None, v, None, kind),
        ChecksumAlgorithm::Sha256 => (None, None, None, None, v, kind),
    }
}

/// What a GET or HEAD describes: the version resolved and, with
/// `partNumber`, the byte range of that part.
pub(super) struct Described {
    pub manifest: Manifest,
    /// `(start, end)` of the selected part, if any.
    pub part: Option<(u64, u64)>,
    /// Number of parts of a multipart object.
    pub parts_count: Option<u32>,
}

impl<S: KvStore + 'static> ClusterBackend<S> {
    /// Resolve the version a GET/HEAD addresses, evaluate the conditional
    /// headers and select the part (RFC-007 §5.4–5.5).
    pub(super) async fn describe(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        conditions: &Conditions<'_>,
        part_number: Option<i32>,
    ) -> Result<Described, S3Error> {
        let found =
            object::read_manifest_version(self.meta.as_ref(), bucket, key, version_id, None)
                .await
                .map_err(meta_err)?;
        let manifest = match found {
            ManifestFound::Manifest(m) => m,
            ManifestFound::DeleteMarker { version_id: v, .. } => {
                return Err(delete_marker_error(version_id.is_some(), &v));
            }
            ManifestFound::Missing => {
                return Err(if version_id.is_some() {
                    s3_error!(NoSuchVersion)
                } else {
                    s3_error!(NoSuchKey)
                });
            }
        };
        evaluate_conditions(
            conditions,
            &manifest.header.etag,
            manifest.pointer.last_modified_ms,
        )?;
        let parts_count = match manifest.header.kind {
            GenKind::MultipartRoot { part_count, .. } => Some(part_count),
            _ => None,
        };
        let part = match part_number {
            None => None,
            Some(n) if n < 1 => {
                return Err(s3_error!(InvalidRange, "part number must be >= 1"));
            }
            Some(n) => {
                let n = usize::try_from(n).unwrap_or(usize::MAX);
                if manifest.parts.is_empty() {
                    if n == 1 {
                        Some((0, manifest.header.size))
                    } else {
                        return Err(s3_error!(
                            InvalidRange,
                            "the requested part number is not satisfiable"
                        ));
                    }
                } else {
                    let Some(p) = manifest.parts.get(n - 1) else {
                        return Err(s3_error!(
                            InvalidRange,
                            "the requested part number is not satisfiable"
                        ));
                    };
                    Some((p.offset, p.offset + p.size))
                }
            }
        };
        Ok(Described {
            manifest,
            part,
            parts_count,
        })
    }

    pub(super) async fn read_object(
        &self,
        input: GetObjectInput,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let conditions = Conditions {
            if_match: input.if_match.as_ref(),
            if_none_match: input.if_none_match.as_ref(),
            if_modified_since: input.if_modified_since.as_ref(),
            if_unmodified_since: input.if_unmodified_since.as_ref(),
        };
        let d = self
            .describe(
                &input.bucket,
                &input.key,
                input.version_id.as_deref(),
                &conditions,
                input.part_number,
            )
            .await?;
        let manifest = d.manifest;
        let total = manifest.header.size;
        let (base_start, base_end) = d.part.unwrap_or((0, total));
        let span = base_end - base_start;
        let (start, end, content_range) = match &input.range {
            None => (
                base_start,
                base_end,
                d.part
                    .map(|_| format!("bytes {base_start}-{}/{total}", base_end.saturating_sub(1))),
            ),
            Some(r) => {
                let r = r.check(span)?;
                (
                    base_start + r.start,
                    base_start + r.end,
                    Some(format!(
                        "bytes {}-{}/{total}",
                        base_start + r.start,
                        (base_start + r.end).saturating_sub(1)
                    )),
                )
            }
        };

        let map = self.map_up_to_date().await.map_err(meta_err)?;
        self.note_access(manifest.pointer.generation);
        let checksum_on = input
            .checksum_mode
            .as_ref()
            .is_some_and(|m| m.as_str().eq_ignore_ascii_case("ENABLED"));
        // Checksums describe whole objects (or, with `partNumber`, one
        // part): a ranged read gets none, as AWS clients validate whatever
        // header they receive against the bytes they got.
        let stored = match (input.part_number, &input.range) {
            (_, Some(_)) => None,
            (Some(n), None) if !manifest.parts.is_empty() => manifest
                .parts
                .get(usize::try_from(n).unwrap_or(usize::MAX).saturating_sub(1))
                .and_then(|p| p.checksum.as_ref()),
            _ => manifest.header.checksum.as_ref(),
        };
        let (crc32, crc32c, crc64nvme, sha1, sha256, checksum_type) = if checksum_on {
            checksum_fields(stored)
        } else {
            (None, None, None, None, None, None)
        };
        let version_id =
            (manifest.pointer.version_id != "null").then(|| manifest.pointer.version_id.clone());
        let body = self.stream_range(Arc::clone(&map), &manifest, start, end);
        let mut resp = S3Response::new(GetObjectOutput {
            body: Some(body),
            content_length: Some(i64::try_from(end - start).unwrap_or(i64::MAX)),
            content_range,
            e_tag: Some(ETag::Strong(manifest.header.etag.clone())),
            last_modified: Some(ms_to_timestamp(manifest.pointer.last_modified_ms)),
            content_type: input
                .response_content_type
                .or_else(|| manifest.header.content_type.clone()),
            metadata: (!manifest.header.user_metadata.is_empty())
                .then(|| manifest.header.user_metadata.iter().cloned().collect()),
            accept_ranges: Some("bytes".into()),
            version_id,
            checksum_crc32: crc32,
            checksum_crc32c: crc32c,
            checksum_crc64nvme: crc64nvme,
            checksum_sha1: sha1,
            checksum_sha256: sha256,
            checksum_type,
            ..Default::default()
        });
        if let Some(n) = d.parts_count.filter(|_| input.part_number.is_some()) {
            resp.headers.insert(
                "x-amz-mp-parts-count",
                hyper::header::HeaderValue::from_str(&n.to_string())
                    .unwrap_or_else(|_| hyper::header::HeaderValue::from_static("0")),
            );
        }
        if d.part.is_some() {
            resp.status = Some(hyper::StatusCode::PARTIAL_CONTENT);
        }
        Ok(resp)
    }

    pub(super) async fn head_object_impl(
        &self,
        input: HeadObjectInput,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let conditions = Conditions {
            if_match: input.if_match.as_ref(),
            if_none_match: input.if_none_match.as_ref(),
            if_modified_since: input.if_modified_since.as_ref(),
            if_unmodified_since: input.if_unmodified_since.as_ref(),
        };
        let d = self
            .describe(
                &input.bucket,
                &input.key,
                input.version_id.as_deref(),
                &conditions,
                input.part_number,
            )
            .await?;
        let manifest = d.manifest;
        let total = manifest.header.size;
        let (start, end) = d.part.unwrap_or((0, total));
        let checksum_on = input
            .checksum_mode
            .as_ref()
            .is_some_and(|m| m.as_str().eq_ignore_ascii_case("ENABLED"));
        let stored = match (input.part_number, &input.range) {
            (_, Some(_)) => None,
            (Some(n), None) if !manifest.parts.is_empty() => manifest
                .parts
                .get(usize::try_from(n).unwrap_or(usize::MAX).saturating_sub(1))
                .and_then(|p| p.checksum.as_ref()),
            _ => manifest.header.checksum.as_ref(),
        };
        let (crc32, crc32c, crc64nvme, sha1, sha256, checksum_type) = if checksum_on {
            checksum_fields(stored)
        } else {
            (None, None, None, None, None, None)
        };
        let version_id =
            (manifest.pointer.version_id != "null").then(|| manifest.pointer.version_id.clone());
        let mut resp = S3Response::new(HeadObjectOutput {
            content_length: Some(i64::try_from(end - start).unwrap_or(i64::MAX)),
            content_range: d
                .part
                .map(|_| format!("bytes {start}-{}/{total}", end.saturating_sub(1))),
            e_tag: Some(ETag::Strong(manifest.header.etag.clone())),
            last_modified: Some(ms_to_timestamp(manifest.pointer.last_modified_ms)),
            content_type: manifest.header.content_type.clone(),
            metadata: (!manifest.header.user_metadata.is_empty())
                .then(|| manifest.header.user_metadata.iter().cloned().collect()),
            accept_ranges: Some("bytes".into()),
            version_id,
            checksum_crc32: crc32,
            checksum_crc32c: crc32c,
            checksum_crc64nvme: crc64nvme,
            checksum_sha1: sha1,
            checksum_sha256: sha256,
            checksum_type,
            ..Default::default()
        });
        if let Some(n) = d.parts_count.filter(|_| input.part_number.is_some()) {
            resp.headers.insert(
                "x-amz-mp-parts-count",
                hyper::header::HeaderValue::from_str(&n.to_string())
                    .unwrap_or_else(|_| hyper::header::HeaderValue::from_static("0")),
            );
        }
        Ok(resp)
    }

    /// Stream bytes `[start, end)` of the object, chunk by chunk, with up
    /// to `read_parallelism` chunks in flight ahead of the one being sent
    /// (RFC-009 §9). Delivery stays in order; memory is bounded by the
    /// in-flight chunks plus the channel buffer; a dropped body (client
    /// cancellation) aborts every in-flight read. Errors after the first
    /// byte terminate the stream with an error (the client observes a
    /// truncated body), never with wrong bytes. For a multipart object the
    /// part manifests are read lazily, in order, for the parts overlapping
    /// the range (RFC-007 §5.3.2).
    #[allow(clippy::too_many_lines)]
    pub(super) fn stream_range(
        &self,
        map: Arc<ClusterMap>,
        manifest: &Manifest,
        start: u64,
        end: u64,
    ) -> StreamingBlob {
        let mut source = ChunkSource {
            inline: std::collections::VecDeque::new(),
            parts: std::collections::VecDeque::new(),
            start,
            end,
        };
        if manifest.parts.is_empty() {
            let generation = manifest.pointer.generation;
            for (i, c) in &manifest.chunks {
                if c.offset < end && c.offset + u64::from(c.len) > start {
                    let cached = manifest
                        .cache
                        .iter()
                        .find(|(ci, _)| ci == i)
                        .map(|(_, p)| p.clone());
                    source.inline.push_back((generation, *i, c.clone(), cached));
                }
            }
        } else {
            // Binary search for the first part overlapping the range.
            let first = manifest
                .parts
                .partition_point(|p| p.offset + p.size <= start);
            for p in &manifest.parts[first..] {
                if p.offset >= end {
                    break;
                }
                source.parts.push_back(p.clone());
            }
        }
        let backend = self.clone();
        let parallelism = self.cfg.read_parallelism.max(1);
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, S3Error>>(1);
        tokio::spawn(async move {
            let mut pending: std::collections::VecDeque<(ChunkRecord, ChunkTask)> =
                std::collections::VecDeque::new();
            let abort_all = |pending: &mut std::collections::VecDeque<(ChunkRecord, ChunkTask)>| {
                for (_, task) in pending.drain(..) {
                    task.abort();
                }
            };
            loop {
                while pending.len() < parallelism {
                    let next = match source.next(&backend).await {
                        Ok(Some(n)) => n,
                        Ok(None) => break,
                        Err(e) => {
                            abort_all(&mut pending);
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    };
                    let (generation, index, chunk, cached) = next;
                    let backend = backend.clone();
                    let map = Arc::clone(&map);
                    let record = chunk.clone();
                    let task = tokio::spawn(async move {
                        backend
                            .read_chunk_bytes(&map, generation, index, &record, cached.as_ref())
                            .await
                    });
                    pending.push_back((chunk, task));
                }
                let Some((chunk, task)) = pending.pop_front() else {
                    return;
                };
                let parts = match task.await {
                    Ok(Ok(p)) => p,
                    Ok(Err(e)) => {
                        abort_all(&mut pending);
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                    Err(join) => {
                        abort_all(&mut pending);
                        let err = s3_error!(InternalError, "chunk read task failed: {join}");
                        let _ = tx.send(Err(err)).await;
                        return;
                    }
                };
                let cs = chunk.offset;
                let ce = chunk.offset + u64::from(chunk.len);
                let from = usize::try_from(start.max(cs) - cs).unwrap_or(0);
                let to = usize::try_from(end.min(ce) - cs).unwrap_or(usize::MAX);
                // Hand the frames through as received, clipped to the
                // requested range; no concatenation.
                let mut pos = 0usize;
                for part in parts {
                    let p_end = pos + part.len();
                    let lo = from.max(pos);
                    let hi = to.min(p_end);
                    if lo < hi {
                        let piece = if lo == pos && hi == p_end {
                            part
                        } else {
                            part.slice(lo - pos..hi - pos)
                        };
                        if tx.send(Ok(piece)).await.is_err() {
                            abort_all(&mut pending);
                            return;
                        }
                    }
                    pos = p_end;
                    if pos >= to {
                        break;
                    }
                }
            }
        });
        StreamingBlob::wrap(tokio_stream::wrappers::ReceiverStream::new(rx))
    }
}

/// Chunks to read, in object order: inline chunk records, then the chunk
/// manifests of the remaining parts, resolved one part at a time. Offsets
/// of part chunks are rebased to the object.
struct ChunkSource {
    inline: std::collections::VecDeque<(GenId, u32, ChunkRecord, Option<CachePlacement>)>,
    parts: std::collections::VecDeque<PartRef>,
    start: u64,
    end: u64,
}

impl ChunkSource {
    async fn next<S: KvStore + 'static>(
        &mut self,
        backend: &ClusterBackend<S>,
    ) -> Result<Option<(GenId, u32, ChunkRecord, Option<CachePlacement>)>, S3Error> {
        loop {
            if let Some(c) = self.inline.pop_front() {
                return Ok(Some(c));
            }
            let Some(part) = self.parts.pop_front() else {
                return Ok(None);
            };
            let Some((chunks, cache)) =
                object::read_part_manifest(backend.meta.as_ref(), &part.generation)
                    .await
                    .map_err(meta_err)?
            else {
                return Err(s3_error!(
                    InternalError,
                    "part {} manifest is missing (data integrity error)",
                    part.part_number
                ));
            };
            for (i, mut c) in chunks {
                c.offset += part.offset;
                if c.offset < self.end && c.offset + u64::from(c.len) > self.start {
                    let cached = cache
                        .iter()
                        .find(|(ci, _)| *ci == i)
                        .map(|(_, p)| p.clone());
                    self.inline.push_back((part.generation, i, c, cached));
                }
            }
        }
    }
}

impl<S: KvStore + 'static> ClusterBackend<S> {
    /// One whole chunk: from the cache when a validated copy exists,
    /// otherwise from the durable placement (then possibly populated).
    pub(super) async fn read_chunk_bytes(
        &self,
        map: &Arc<ClusterMap>,
        generation: GenId,
        index: u32,
        chunk: &ChunkRecord,
        cached: Option<&CachePlacement>,
    ) -> Result<Vec<Bytes>, S3Error> {
        if let (Some(_), Some(p)) = (self.cfg.cache.as_ref(), cached) {
            self.cache_stats.lookup_hits.fetch_add(1, Ordering::Relaxed);
            if let Some(parts) = self.try_cache_hit(map, generation, index, chunk, p).await {
                self.cache_stats.hits.fetch_add(1, Ordering::Relaxed);
                self.touch_cache(generation, index);
                return Ok(parts);
            }
            self.cache_stats.fallbacks.fetch_add(1, Ordering::Relaxed);
        } else if self.cfg.cache.is_some() {
            self.cache_stats.misses.fetch_add(1, Ordering::Relaxed);
        }
        let parts = self.read_durable(map, chunk).await?;
        if self.cfg.cache.is_some() && cached.is_none() {
            // Populating needs the contiguous chunk (one copy, off the
            // GET's path once queued).
            self.maybe_populate(map, generation, index, chunk, concat_frames(parts.clone()));
        }
        Ok(parts)
    }

    /// Durable read of one chunk (RFC-004 §5.6).
    pub(super) async fn read_durable(
        &self,
        map: &ClusterMap,
        chunk: &ChunkRecord,
    ) -> Result<Vec<Bytes>, S3Error> {
        let group = map.groups.get(&chunk.placement.group_id).ok_or_else(|| {
            s3_error!(
                InternalError,
                "unknown stripe group {}",
                chunk.placement.group_id
            )
        })?;
        let coder = self
            .coders
            .get(&chunk.scheme, chunk.data_shards, chunk.parity_shards)
            .map_err(|e| s3_error!(InternalError, "{e}"))?;
        let r = arca_lake_tiering::durable::read_chunk_alternates(
            &self.clients,
            group,
            &addr_of(map),
            map.alternates.get(&group.id).map_or(&[][..], Vec::as_slice),
            chunk,
            coder.as_ref(),
        )
        .await;
        match r {
            Ok(r) => {
                if !r.missing.is_empty() {
                    self.integrity
                        .stats
                        .degraded_reads
                        .fetch_add(1, Ordering::Relaxed);
                    self.report_integrity(chunk, Finding::MissingShard, &r.missing);
                }
                Ok(r.parts)
            }
            Err(TierError::Unreadable { have, need }) => {
                self.integrity
                    .stats
                    .unreadable_reads
                    .fetch_add(1, Ordering::Relaxed);
                self.report_integrity(chunk, Finding::UnrecoverableStripe, &[]);
                Err(s3_error!(
                    InternalError,
                    "chunk unreadable: only {have} of {need} required shards available (data integrity error)"
                ))
            }
            Err(other) => Err(s3_error!(InternalError, "{other}")),
        }
    }

    /// Report a degraded or unreadable chunk to the stripe repair queue
    /// (RFC-008 §8.7), deduplicated per chunk and off the GET's path. A
    /// second detection of a chunk that already has a task is a no-op in
    /// metadata, so this is safe to race with the scrubber.
    fn report_integrity(&self, chunk: &ChunkRecord, reason: Finding, missing: &[usize]) {
        if !self.integrity.should_report(chunk.chunk_id, now_ms()) {
            self.integrity
                .stats
                .integrity_reports_dropped
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let backend = self.clone();
        let group_id = chunk.placement.group_id;
        let chunk_id = chunk.chunk_id;
        let hints: Vec<u16> = missing
            .iter()
            .map(|i| u16::try_from(*i).unwrap_or(u16::MAX))
            .collect();
        tokio::spawn(async move {
            match arca_lake_metadata::ops::integrity::enqueue(
                backend.meta_repair.as_ref(),
                group_id,
                chunk_id,
                reason,
                &hints,
                now_ms(),
            )
            .await
            {
                Ok(_) => {
                    backend
                        .integrity
                        .stats
                        .integrity_reports
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    backend
                        .integrity
                        .stats
                        .integrity_reports_dropped
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    /// A cache hit (clean or degraded), or `None` after invalidating a copy
    /// that could not be read or did not match its record.
    async fn try_cache_hit(
        &self,
        map: &Arc<ClusterMap>,
        generation: GenId,
        index: u32,
        chunk: &ChunkRecord,
        p: &CachePlacement,
    ) -> Option<Vec<Bytes>> {
        let Some(group) = map.groups.get(&p.group_id) else {
            self.cache_stats
                .fallback_unknown_group
                .fetch_add(1, Ordering::Relaxed);
            self.invalidate_cache(Arc::clone(map), None, generation, index);
            return None;
        };
        let r = match tcache::read_cached(
            &self.clients,
            &self.coders,
            group,
            &addr_of(map),
            &generation,
            index,
            chunk,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                let counter = match e {
                    TierError::Unreadable { .. } => &self.cache_stats.fallback_unreadable,
                    TierError::Corrupt(_) => &self.cache_stats.fallback_corrupt,
                    TierError::Node { .. } => &self.cache_stats.fallback_node,
                    TierError::Meta(_) | TierError::Other(_) => {
                        &self.cache_stats.fallback_unknown_group
                    }
                };
                counter.fetch_add(1, Ordering::Relaxed);
                self.invalidate_cache(Arc::clone(map), Some(group.clone()), generation, index);
                return None;
            }
        };
        if !r.missing.is_empty() {
            self.cache_stats
                .degraded_hits
                .fetch_add(1, Ordering::Relaxed);
            self.repair_cache_shards(
                Arc::clone(map),
                group.clone(),
                generation,
                index,
                concat_frames(r.parts.clone()),
                r.missing,
            );
        }
        Some(r.parts)
    }

    fn invalidate_cache(
        &self,
        map: Arc<ClusterMap>,
        group: Option<GroupRecord>,
        generation: GenId,
        index: u32,
    ) {
        self.cache_stats
            .invalidations
            .fetch_add(1, Ordering::Relaxed);
        let backend = self.clone();
        tokio::spawn(async move {
            let _ = tcache::invalidate(
                backend.meta.as_ref(),
                &backend.clients,
                group.as_ref(),
                &addr_of(&map),
                &generation,
                index,
            )
            .await;
        });
    }

    /// Read-triggered cache repair after a degraded hit (RFC-006 §5.9):
    /// rewrite the missing shards, bounded by a per-gateway concurrency
    /// limit; when no permit is free the repair is skipped (the next
    /// degraded hit retries).
    fn repair_cache_shards(
        &self,
        map: Arc<ClusterMap>,
        group: GroupRecord,
        generation: GenId,
        index: u32,
        bytes: Bytes,
        missing: Vec<usize>,
    ) {
        let Ok(permit) = Arc::clone(&self.cache_state.repair_permits).try_acquire_owned() else {
            self.cache_stats
                .shard_repairs_skipped
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        let backend = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let r = tcache::repair_shards(
                &backend.clients,
                &backend.coders,
                &group,
                &addr_of(&map),
                &generation,
                index,
                &bytes,
                &missing,
            )
            .await;
            match r {
                Ok(n) => {
                    backend
                        .cache_stats
                        .shards_repaired
                        .fetch_add(n as u64, Ordering::Relaxed);
                }
                Err(_) => {
                    backend
                        .cache_stats
                        .shard_repair_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    /// Record a hit in the recency index, at most once per entry per
    /// `touch_interval` (per gateway).
    fn touch_cache(&self, generation: GenId, index: u32) {
        let now = now_ms();
        if !self.cache_state.should_touch(generation, index, now) {
            return;
        }
        let backend = self.clone();
        tokio::spawn(async move {
            let _ = arca_lake_metadata::ops::cache::touch(
                backend.meta.as_ref(),
                &generation,
                index,
                now,
            )
            .await;
        });
    }

    /// Admission control, then asynchronous population (never on the GET's
    /// critical path; failures only count, RFC-006 §5.3).
    fn maybe_populate(
        &self,
        map: &Arc<ClusterMap>,
        generation: GenId,
        index: u32,
        chunk: &ChunkRecord,
        bytes: Bytes,
    ) {
        let Some(cfg) = self.cfg.cache.as_ref() else {
            return;
        };
        if cfg.max_cacheable_bytes > 0 && u64::from(chunk.len) > cfg.max_cacheable_bytes {
            self.cache_stats
                .admissions_rejected
                .fetch_add(1, Ordering::Relaxed);
            self.cache_stats
                .admission_rejected_size
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        if !self
            .cache_state
            .admit(generation, index, cfg.admit_after_misses)
        {
            self.cache_stats
                .admission_rejected_threshold
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let cid = arca_lake_common::chunk_id(&generation, index);
        let Some(group) = tcache::choose_group(map.groups.values(), &cfg.tier, &cid).cloned()
        else {
            return;
        };
        if !self.cache_state.begin_populate(generation, index) {
            self.cache_stats
                .populations_deduplicated
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let nodes: Vec<String> = group
            .members
            .iter()
            .filter_map(|m| map.node_addr(m.node_id).map(str::to_string))
            .collect();
        let backend = self.clone();
        let cfg = cfg.clone();
        let map = Arc::clone(map);
        let group = Arc::new(group);
        let attempt: super::popqueue::Attempt = Arc::new(move || {
            let backend = backend.clone();
            let cfg = cfg.clone();
            let map = Arc::clone(&map);
            let group = Arc::clone(&group);
            let bytes = bytes.clone();
            Box::pin(async move {
                tcache::populate(
                    backend.meta.as_ref(),
                    &backend.clients,
                    &backend.coders,
                    &cfg,
                    &group,
                    &addr_of(&map),
                    &generation,
                    index,
                    bytes,
                    now_ms(),
                )
                .await
            })
        });
        self.pop_queue.enqueue(super::popqueue::Job {
            nodes,
            attempt,
            done: self.population_done(generation, index),
        });
    }

    /// The completion callback of a queued population: releases the dedup
    /// marker and counts the outcome.
    fn population_done(&self, generation: GenId, index: u32) -> super::popqueue::Done {
        let done_backend = self.clone();
        Box::new(move |r| {
            done_backend.cache_state.end_populate(generation, index);
            let stats = &done_backend.cache_stats;
            match r {
                None => {
                    stats.populations_skipped.fetch_add(1, Ordering::Relaxed);
                }
                Some(Ok(tcache::PopulateOutcome::Inserted)) => {
                    stats.populated.fetch_add(1, Ordering::Relaxed);
                }
                Some(Ok(tcache::PopulateOutcome::AlreadyPresent)) => {}
                Some(Ok(tcache::PopulateOutcome::Rejected(why))) => {
                    stats.admissions_rejected.fetch_add(1, Ordering::Relaxed);
                    let counter = if why == "over capacity" {
                        &stats.admission_rejected_capacity
                    } else {
                        &stats.admission_rejected_generation
                    };
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                Some(Err(e)) => {
                    tracing::debug!(error = %e, index, "cache population failed");
                    stats.population_failures.fetch_add(1, Ordering::Relaxed);
                    let counter = match e {
                        TierError::Node { .. } => &stats.population_failures_node,
                        TierError::Corrupt(_) => &stats.population_failures_corrupt,
                        _ => &stats.population_failures_meta,
                    };
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    }

    /// Coalesced access record for automatic tiering (RFC-006 §5.8).
    pub(super) fn note_access(&self, generation: GenId) {
        let now = now_ms();
        let Some(reads) = self.cache_state.flush_access(generation, now) else {
            return;
        };
        let backend = self.clone();
        tokio::spawn(async move {
            let _ = object::record_access(backend.meta.as_ref(), &generation, now, reads).await;
        });
    }

    /// Every `(generation, index, chunk record, cache placement)` of the
    /// current version of `bucket/key`, parts resolved (RFC-006 §5.5 tooling).
    async fn all_chunks(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<
        (
            Manifest,
            Vec<(GenId, u32, ChunkRecord, Option<CachePlacement>)>,
        ),
        S3Error,
    > {
        let manifest = object::read_manifest(self.meta.as_ref(), bucket, key, None)
            .await
            .map_err(meta_err)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let mut source = ChunkSource {
            inline: std::collections::VecDeque::new(),
            parts: manifest.parts.iter().cloned().collect(),
            start: 0,
            end: manifest.header.size,
        };
        let g = manifest.pointer.generation;
        for (i, c) in &manifest.chunks {
            let cached = manifest
                .cache
                .iter()
                .find(|(ci, _)| ci == i)
                .map(|(_, p)| p.clone());
            source.inline.push_back((g, *i, c.clone(), cached));
        }
        let mut out = Vec::new();
        while let Some(c) = source.next(self).await? {
            out.push(c);
        }
        Ok((manifest, out))
    }

    /// Populate every chunk of the current version of `bucket/key`
    /// synchronously (RFC-006 §5.5). Returns how many chunks are now
    /// recorded as cached.
    ///
    /// # Errors
    ///
    /// Missing object, no cache tier, or read errors.
    pub async fn prefetch(&self, bucket: &str, key: &str) -> Result<usize, S3Error> {
        let cfg = self
            .cfg
            .cache
            .clone()
            .ok_or_else(|| s3_error!(InternalError, "no cache tier configured"))?;
        let (_, chunks) = self.all_chunks(bucket, key).await?;
        let map = self.map_up_to_date().await.map_err(meta_err)?;
        let mut cached = 0;
        for (generation, index, chunk, placement) in &chunks {
            if placement.is_some() {
                cached += 1;
                continue;
            }
            let bytes = concat_frames(self.read_durable(&map, chunk).await?);
            let cid = arca_lake_common::chunk_id(generation, *index);
            let Some(group) = tcache::choose_group(map.groups.values(), &cfg.tier, &cid) else {
                return Err(s3_error!(
                    InternalError,
                    "no active cache group on tier {}",
                    cfg.tier
                ));
            };
            match tcache::populate(
                self.meta.as_ref(),
                &self.clients,
                &self.coders,
                &cfg,
                group,
                &addr_of(&map),
                generation,
                *index,
                bytes,
                now_ms(),
            )
            .await
            {
                Ok(tcache::PopulateOutcome::Inserted | tcache::PopulateOutcome::AlreadyPresent) => {
                    cached += 1;
                }
                Ok(tcache::PopulateOutcome::Rejected(why)) => {
                    return Err(s3_error!(InternalError, "cache admission rejected: {why}"));
                }
                Err(e) => return Err(s3_error!(InternalError, "cache population failed: {e}")),
            }
        }
        Ok(cached)
    }

    /// Prefetch and pin the current version of `bucket/key`.
    ///
    /// # Errors
    ///
    /// As [`Self::prefetch`].
    pub async fn pin(&self, bucket: &str, key: &str) -> Result<usize, S3Error> {
        let n = self.prefetch(bucket, key).await?;
        self.set_pinned(bucket, key, true).await?;
        Ok(n)
    }

    /// Unpin the current version of `bucket/key` (entries become evictable).
    ///
    /// # Errors
    ///
    /// Missing object or metadata errors.
    pub async fn unpin(&self, bucket: &str, key: &str) -> Result<usize, S3Error> {
        self.set_pinned(bucket, key, false).await
    }

    async fn set_pinned(&self, bucket: &str, key: &str, pinned: bool) -> Result<usize, S3Error> {
        let manifest = object::read_manifest(self.meta.as_ref(), bucket, key, None)
            .await
            .map_err(meta_err)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let mut gens = vec![manifest.pointer.generation];
        gens.extend(manifest.parts.iter().map(|p| p.generation));
        let mut n = 0;
        for g in gens {
            n += arca_lake_metadata::ops::cache::set_pinned(self.meta.as_ref(), &g, pinned)
                .await
                .map_err(meta_err)?;
        }
        Ok(n)
    }

    /// Run LRU eviction of the configured cache tier (RFC-006 §5.4).
    ///
    /// # Errors
    ///
    /// No cache tier, or metadata errors.
    pub async fn evict(&self) -> Result<tcache::EvictReport, S3Error> {
        let cfg = self
            .cfg
            .cache
            .clone()
            .ok_or_else(|| s3_error!(InternalError, "no cache tier configured"))?;
        let map = self.map_up_to_date().await.map_err(meta_err)?;
        tcache::evict_tier(
            self.meta.as_ref(),
            &self.clients,
            &cfg,
            &map.groups,
            &addr_of(&map),
        )
        .await
        .map_err(|e| s3_error!(InternalError, "{e}"))
    }

    /// The cache tier configuration, if any.
    #[must_use]
    pub fn cache_config(&self) -> Option<&CacheTierConfig> {
        self.cfg.cache.as_ref()
    }
}
