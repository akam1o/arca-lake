//! Write path (RFC-003 §5.3, RFC-004 §5.5, RFC-007 §5.3 and §5.7).
//!
//! One streaming pipeline serves PutObject, UploadPart and the destination
//! of CopyObject: the body is chunked, erasure coded and placed with up to
//! `write_pipeline` chunks in flight, chunk records are appended in
//! batches, MD5 and the requested checksum are computed in the same pass,
//! and the generation is then published (object commit) or sealed (part).
//! A checksum mismatch aborts the generation before anything is published.

use std::collections::HashMap;
use std::future::Future;

use arca_lake_common::chunk_id;
use arca_lake_metadata::kv::ConflictBackoff;
use arca_lake_metadata::ops::multipart::{self, PartInfo};
use arca_lake_metadata::ops::object::{self, CommitInfo, CommitOutcome, Precondition};
use arca_lake_metadata::records::{
    Checksum, ChecksumAlgorithm, ChecksumType, ChunkRecord, DurablePlacement, GenId, GroupRecord,
    MultipartState, NULL_VERSION_ID, UploadId, Versioning,
};
use arca_lake_metadata::{KvError, KvStore, MetaError};
use arca_lake_placement::WritableGroup;
use arca_lake_protocol::client::{NodeClients, ShardRef};
use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use s3s::crypto::{Checksum as _, Md5};
use s3s::dto::StreamingBlob;
use s3s::{S3Error, s3_error};

use super::backend::{ClusterBackend, FaultPoint, meta_err, now_ms};
use super::map::ClusterMap;
use crate::checksum::{self, Hasher};

/// Result of a committed write.
#[derive(Debug, Clone)]
pub struct Committed {
    /// Generation.
    pub generation: GenId,
    /// ETag (hex MD5).
    pub etag: String,
    /// Size.
    pub size: u64,
    /// Version id assigned (`null` on unversioned buckets).
    pub version_id: String,
    /// Stored checksum, if one was requested.
    pub checksum: Option<Checksum>,
}

/// Result of a sealed part.
#[derive(Debug, Clone)]
pub struct SealedPart {
    /// Part ETag (hex MD5).
    pub etag: String,
    /// Size.
    pub size: u64,
    /// Stored part checksum, if one was requested.
    pub checksum: Option<Checksum>,
}

/// Checksum the client asked for on an upload (RFC-007 §5.7): the
/// algorithm and, when known before the body is read, the expected value.
/// With `aws-chunked` trailers the value arrives after the body; it is read
/// from `trailing` once the body is consumed.
#[derive(Debug, Clone, Default)]
pub struct RequestChecksum {
    /// Algorithm to compute.
    pub algorithm: Option<ChecksumAlgorithm>,
    /// Expected value from a header, if any.
    pub expected: Option<Vec<u8>>,
    /// Trailing headers of an `aws-chunked` body.
    pub trailing: Option<s3s::TrailingHeaders>,
    /// The `Content-MD5` digest of the (decoded) body, if sent.
    pub content_md5: Option<[u8; 16]>,
}

impl RequestChecksum {
    /// Resolve the expected value after the body was consumed: the header
    /// value, else the trailer.
    fn expected_value(&self) -> Result<Option<Vec<u8>>, S3Error> {
        if let Some(v) = &self.expected {
            return Ok(Some(v.clone()));
        }
        let Some(algo) = self.algorithm else {
            return Ok(None);
        };
        let name = format!("x-amz-checksum-{}", algo.name().to_ascii_lowercase());
        let Some(t) = &self.trailing else {
            return Ok(None);
        };
        let raw = t.read(|h| {
            h.get(&name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        });
        match raw {
            Some(Some(s)) => checksum::decode_b64(algo, &s)
                .map(Some)
                .ok_or_else(|| s3_error!(InvalidRequest, "malformed {name} trailer")),
            _ => Ok(None),
        }
    }
}

/// What the stream produced.
struct Streamed {
    size: u64,
    etag: String,
    chunk_count: u32,
    checksum: Option<(ChecksumAlgorithm, Vec<u8>)>,
}

/// A failed write and whether the generation should be aborted. After a
/// commit whose outcome was decided (committed, or fenced by an abort)
/// nothing must touch the generation again.
struct WriteFailure {
    error: S3Error,
    abort: bool,
}

impl From<S3Error> for WriteFailure {
    fn from(error: S3Error) -> Self {
        Self { error, abort: true }
    }
}

/// One chunk written to a group.
struct Placed {
    index: u32,
    record: ChunkRecord,
}

/// Chunk placements in flight: `(index, started, task)` in chunk order.
/// Dropped (on an early return, or with the request's future), each is
/// aborted, and a cancelled shard stream stores nothing (ADR-0011).
type Pipeline = std::collections::VecDeque<(u32, std::time::Instant, Placement)>;

/// A chunk placement task, aborted when dropped, also while it is being
/// awaited (a dropped `JoinHandle` would detach the task and let it keep
/// writing shards).
struct Placement(tokio::task::JoinHandle<Result<Placed, S3Error>>);

impl Drop for Placement {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Future for Placement {
    type Output = Result<Result<Placed, S3Error>, tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx)
    }
}

/// The longest a failed write waits for its placements in flight before
/// it aborts them and answers (see [`ClusterBackend::stream_generation`]).
pub const FAILED_WRITE_SETTLE: std::time::Duration = std::time::Duration::from_secs(5);

/// Decrements a gauge when dropped (PUTs in flight).
struct InFlightGuard<'a>(&'a std::sync::atomic::AtomicU64);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Verify the streamed `Content-MD5` and checksum against the client's
/// expectation and build the stored record. A mismatch is `BadDigest`;
/// nothing is published.
fn verify_checksum(
    streamed: &Streamed,
    req: &RequestChecksum,
) -> Result<Option<Checksum>, S3Error> {
    checksum::verify_content_md5(
        req.content_md5.as_ref(),
        &checksum::unhex(&streamed.etag).unwrap_or_default(),
    )?;
    let Some((algo, value)) = &streamed.checksum else {
        return Ok(None);
    };
    if let Some(expected) = req.expected_value()?
        && expected != *value
    {
        return Err(s3_error!(
            BadDigest,
            "the {} you specified did not match the calculated checksum",
            algo.name()
        ));
    }
    Ok(Some(Checksum {
        algorithm: *algo,
        kind: ChecksumType::FullObject,
        value: value.clone(),
        parts: None,
    }))
}

impl<S: KvStore + 'static> ClusterBackend<S> {
    /// Full PUT: begin a generation, stream the body into chunks, erasure
    /// code and place each chunk, append manifest entries, commit.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn write_object(
        &self,
        bucket: &str,
        key: &str,
        mut body: StreamingBlob,
        content_type: Option<String>,
        user_metadata: Option<HashMap<String, String>>,
        pre: &Precondition,
        checksum: &RequestChecksum,
    ) -> Result<Committed, S3Error> {
        let now = now_ms();
        let put_started = std::time::Instant::now();
        self.put_stats
            .in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _in_flight = InFlightGuard(&self.put_stats.in_flight);
        let lease_ms = u64::try_from(self.cfg.lease.as_millis()).unwrap_or(u64::MAX);
        let pending = object::begin_put(self.meta.as_ref(), bucket, key, now, lease_ms)
            .await
            .map_err(meta_err)?;
        let generation = pending.generation;

        let result = async {
            let streamed = self
                .stream_generation(generation, &mut body, checksum.algorithm)
                .await?;
            let stored = verify_checksum(&streamed, checksum)?;
            if self.take_fault(FaultPoint::BeforeCommit) {
                return Err(s3_error!(InternalError, "injected crash before commit").into());
            }
            if self.take_fault(FaultPoint::DieBeforeCommit) {
                return Err(WriteFailure {
                    error: s3_error!(InternalError, "injected gateway death before commit"),
                    abort: false,
                });
            }
            let info = CommitInfo {
                size: streamed.size,
                etag: streamed.etag.clone(),
                content_type,
                user_metadata: user_metadata
                    .map(|m| m.into_iter().collect())
                    .unwrap_or_default(),
                chunk_count: streamed.chunk_count,
                checksum: stored.clone(),
            };
            let t = std::time::Instant::now();
            // Held until the outcome is decided; a refusal comes before any
            // commit attempt, and the generation is aborted (ADR-0041).
            let turn = self.commit_turn(bucket, key).await?;
            let version_id = self.commit_resolving(generation, &info, pre).await?;
            drop(turn);
            self.put_stats.commit.observe(t.elapsed());
            if self.take_fault(FaultPoint::AfterCommit) {
                return Err(WriteFailure {
                    error: s3_error!(InternalError, "injected crash after commit"),
                    abort: false,
                });
            }
            Ok(Committed {
                generation,
                etag: streamed.etag,
                size: streamed.size,
                version_id,
                checksum: stored,
            })
        }
        .await;
        match result {
            Ok(c) => {
                self.put_stats.total.observe(put_started.elapsed());
                Ok(c)
            }
            Err(WriteFailure { error, abort }) => {
                // Best effort: mark the generation retiring so GC reclaims
                // its shards sooner than the lease would. If this fails the
                // lease expiry sweep handles it. `abort_put` re-reads the
                // generation, so it can never retire a committed one.
                if abort {
                    let _ = object::abort_put(self.meta.as_ref(), &generation, now_ms()).await;
                }
                Err(error)
            }
        }
    }

    /// UploadPart: a pending part generation, streamed like an object, then
    /// sealed as part `part_number` of the upload (RFC-007 §5.3.2).
    pub(super) async fn write_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &UploadId,
        part_number: u32,
        mut body: StreamingBlob,
        checksum: &RequestChecksum,
    ) -> Result<SealedPart, S3Error> {
        let now = now_ms();
        self.put_stats
            .in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _in_flight = InFlightGuard(&self.put_stats.in_flight);
        let lease_ms = u64::try_from(self.cfg.lease.as_millis()).unwrap_or(u64::MAX);
        let pending = multipart::begin_part(
            self.meta.as_ref(),
            bucket,
            key,
            upload_id,
            part_number,
            now,
            lease_ms,
        )
        .await
        .map_err(meta_err)?;
        let generation = pending.generation;
        let result = async {
            let streamed = self
                .stream_generation(generation, &mut body, checksum.algorithm)
                .await?;
            let stored = verify_checksum(&streamed, checksum)?;
            if self.take_fault(FaultPoint::BeforeCommit) {
                return Err(s3_error!(InternalError, "injected crash before seal").into());
            }
            if self.take_fault(FaultPoint::DieBeforeCommit) {
                return Err(WriteFailure {
                    error: s3_error!(InternalError, "injected gateway death before seal"),
                    abort: false,
                });
            }
            let info = PartInfo {
                size: streamed.size,
                etag: streamed.etag.clone(),
                checksum: stored.clone(),
                chunk_count: streamed.chunk_count,
            };
            let t = std::time::Instant::now();
            self.seal_resolving(generation, bucket, key, upload_id, &info)
                .await?;
            self.put_stats.commit.observe(t.elapsed());
            Ok(SealedPart {
                etag: streamed.etag,
                size: streamed.size,
                checksum: stored,
            })
        }
        .await;
        match result {
            Ok(p) => Ok(p),
            Err(WriteFailure { error, abort }) => {
                if abort {
                    let _ = object::abort_put(self.meta.as_ref(), &generation, now_ms()).await;
                }
                Err(error)
            }
        }
    }

    /// Commit, resolving an unknown outcome (RFC-003 §5.8). A lost reply is
    /// decided by re-reading the generation: committed means success; still
    /// pending means the commit is retried a bounded number of times, after
    /// which the generation is aborted so that a late commit is rejected by
    /// the store. If the abort finds the generation committed, the PUT
    /// succeeded after all. Once an attempt's outcome was unknown, a later
    /// attempt's error is not the answer either (that attempt may still be
    /// applied): the generation is fenced the same way (Phase 4.1, R-112: a
    /// `SlowDown` after an unknown attempt could become visible). Returns
    /// the version id published.
    async fn commit_resolving(
        &self,
        generation: GenId,
        info: &CommitInfo,
        pre: &Precondition,
    ) -> Result<String, WriteFailure> {
        const MAX_COMMIT_ATTEMPTS: usize = 4;
        let meta = self.meta.as_ref();
        let backoff = self.commit_queue.backoff();
        let mut reason = String::new();
        let mut unknown = false;
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            match object::commit_put_with(
                meta,
                &generation,
                info,
                pre,
                now_ms(),
                backoff.as_ref().map(|b| b as &dyn ConflictBackoff),
            )
            .await
            {
                Ok(c) => return Ok(c.version_id),
                Err(MetaError::Kv(KvError::CommitUnknown(r))) => {
                    reason = r;
                    unknown = true;
                }
                Err(e) if !unknown => return Err(meta_err(e).into()),
                Err(e) => {
                    reason = format!("{reason}; then {e}");
                    break;
                }
            }
            match object::resolve_commit(meta, &generation, now_ms()).await {
                Ok(CommitOutcome::Committed(_)) => return self.version_of(generation).await,
                Ok(CommitOutcome::Pending) if attempt < MAX_COMMIT_ATTEMPTS => {}
                Ok(CommitOutcome::Pending | CommitOutcome::PendingExpired) => break,
                Ok(CommitOutcome::Retiring | CommitOutcome::Missing) => {
                    return Err(WriteFailure {
                        error: s3_error!(
                            InternalError,
                            "commit outcome unknown ({reason}) and the generation was retired"
                        ),
                        abort: false,
                    });
                }
                Err(e) => {
                    return Err(WriteFailure {
                        error: s3_error!(
                            ServiceUnavailable,
                            "commit outcome unknown ({reason}) and unresolved ({e}); the write stays pending until its lease expires"
                        ),
                        abort: false,
                    });
                }
            }
        }
        self.fence_unknown(generation, &reason).await?;
        self.version_of(generation).await
    }

    /// Version id of a committed generation: the current pointer's if it
    /// is still current, otherwise its version record's (a later PUT may
    /// have become current before the outcome was resolved). In a bucket
    /// that never had versioning it is `null`, and in a suspended one when
    /// no record names it (a later `null` write replaced it). Anything else (a read that
    /// fails, no version naming it any more) is not answered with an
    /// invented id: the write was applied, its id is unknown, and the answer
    /// says so (503) (Phase 4.1 review of R-112).
    async fn version_of(&self, generation: GenId) -> Result<String, WriteFailure> {
        let unresolved = |why: String| WriteFailure {
            error: s3_error!(
                ServiceUnavailable,
                "the write was applied but its version id could not be resolved ({why})"
            ),
            abort: false,
        };
        let meta = self.meta.as_ref();
        let h = object::get_generation(meta, &generation)
            .await
            .map_err(|e| unresolved(e.to_string()))?;
        let b = multipart::bucket_of(meta, h.bucket_id)
            .await
            .map_err(|e| unresolved(e.to_string()))?;
        if b.versioning == Versioning::Disabled {
            return Ok(NULL_VERSION_ID.into());
        }
        if let object::Found::Object(i) = object::head_version(meta, &b.name, &h.key, None)
            .await
            .map_err(|e| unresolved(e.to_string()))?
            && i.pointer.generation == generation
        {
            return Ok(i.pointer.version_id);
        }
        match object::version_id_of(meta, h.bucket_id, &h.key, &generation)
            .await
            .map_err(|e| unresolved(e.to_string()))?
        {
            Some(v) => Ok(v),
            // Suspended: a write publishes the `null` version, which a later
            // `null` write may have replaced (review).
            None if b.versioning == Versioning::Suspended => Ok(NULL_VERSION_ID.into()),
            None => Err(unresolved("no version record names it any more".into())),
        }
    }

    /// Seal a part, resolving an unknown outcome (RFC-007 §5.3.2): sealed
    /// means success; pending and leased is retried; retiring while the
    /// upload is still open means a concurrent re-upload of the same part
    /// number superseded this one, which is success for the client.
    async fn seal_resolving(
        &self,
        generation: GenId,
        bucket: &str,
        key: &str,
        upload_id: &UploadId,
        info: &PartInfo,
    ) -> Result<(), WriteFailure> {
        const MAX_ATTEMPTS: usize = 4;
        let meta = self.meta.as_ref();
        let mut reason = String::new();
        let mut unknown = false;
        for attempt in 1..=MAX_ATTEMPTS {
            match multipart::seal_part(meta, &generation, info, now_ms()).await {
                Ok(_) => return Ok(()),
                Err(MetaError::Kv(KvError::CommitUnknown(r))) => {
                    reason = r;
                    unknown = true;
                }
                // As in `commit_resolving`: after an unknown attempt, a
                // later error is not a definite answer.
                Err(e) if !unknown => return Err(meta_err(e).into()),
                Err(e) => {
                    reason = format!("{reason}; then {e}");
                    break;
                }
            }
            match object::resolve_commit(meta, &generation, now_ms()).await {
                Ok(CommitOutcome::Committed(_)) => return Ok(()),
                Ok(CommitOutcome::Pending) if attempt < MAX_ATTEMPTS => {}
                Ok(CommitOutcome::Pending | CommitOutcome::PendingExpired) => break,
                Ok(CommitOutcome::Retiring) => {
                    // A read that fails proves nothing: the outcome stays
                    // unknown (Phase 4.1 review).
                    let unknown = |e: MetaError| WriteFailure {
                        error: s3_error!(
                            ServiceUnavailable,
                            "seal outcome unknown ({reason}) and unresolved ({e})"
                        ),
                        abort: false,
                    };
                    let open = multipart::get_upload(meta, bucket, key, upload_id)
                        .await
                        .map_err(unknown)?
                        .is_some_and(|u| u.state == MultipartState::Open);
                    if !open {
                        return Err(WriteFailure {
                            error: s3_error!(NoSuchUpload),
                            abort: false,
                        });
                    }
                    // Superseded by a later upload of the same part only if
                    // it was sealed first: a seal writes the part's ETag into
                    // the header, a pending part has none. An expired pending
                    // part is retiring too, never sealed (Phase 4.1 review).
                    let h = object::get_generation(meta, &generation)
                        .await
                        .map_err(unknown)?;
                    if !h.etag.is_empty() && h.etag == info.etag {
                        return Ok(());
                    }
                    return Err(WriteFailure {
                        error: s3_error!(
                            InternalError,
                            "the part expired before it was sealed ({reason}); upload it again"
                        ),
                        abort: false,
                    });
                }
                Ok(CommitOutcome::Missing) => {
                    return Err(WriteFailure {
                        error: s3_error!(
                            InternalError,
                            "seal outcome unknown ({reason}) and the part is gone"
                        ),
                        abort: false,
                    });
                }
                Err(e) => {
                    return Err(WriteFailure {
                        error: s3_error!(
                            ServiceUnavailable,
                            "seal outcome unknown ({reason}) and unresolved ({e})"
                        ),
                        abort: false,
                    });
                }
            }
        }
        self.fence_unknown(generation, &reason).await
    }

    /// Decide a commit that is still unknown after the retries: abort the
    /// generation. The abort transaction reads the generation header, so it
    /// conflicts with (and therefore excludes) a late commit; if it finds
    /// the generation already committed, the write succeeded.
    async fn fence_unknown(&self, generation: GenId, reason: &str) -> Result<(), WriteFailure> {
        match object::abort_put(self.meta.as_ref(), &generation, now_ms()).await {
            Ok(()) => Err(WriteFailure {
                error: s3_error!(
                    ServiceUnavailable,
                    "commit outcome unknown ({reason}); the write was aborted and is not visible"
                ),
                abort: false,
            }),
            Err(MetaError::WrongGenerationState("committed", _)) => Ok(()),
            Err(MetaError::WrongGenerationState(state, _)) => Err(WriteFailure {
                error: s3_error!(
                    InternalError,
                    "commit outcome unknown ({reason}); the generation is {state} and not visible"
                ),
                abort: false,
            }),
            Err(e) => Err(WriteFailure {
                error: s3_error!(
                    ServiceUnavailable,
                    "commit outcome unknown ({reason}); fencing failed ({e}); the write stays pending until its lease expires"
                ),
                abort: false,
            }),
        }
    }

    /// Stream `body` into chunks of `generation`: encode, place, append
    /// manifest entries; compute MD5 and the requested checksum in the same
    /// pass. Does not commit.
    async fn stream_generation(
        &self,
        generation: GenId,
        body: &mut StreamingBlob,
        algorithm: Option<ChecksumAlgorithm>,
    ) -> Result<Streamed, WriteFailure> {
        let mut pipeline = Pipeline::new();
        let result = self
            .stream_into(generation, body, algorithm, &mut pipeline)
            .await;
        if result.is_err() && !pipeline.is_empty() {
            // A failed write (a refused body, a failed chunk) lets the
            // placements in flight finish before it is answered, as a
            // successful one waits for them: aborted, a shard stream already
            // sent can still be stored after the reply and after the
            // caller's abort and GC, an orphan only node reconciliation
            // finds (RFC-004 §5.10). Bounded: a refusal the client caused
            // is not held by a slow node. The bound is at most a third of
            // the lease, which no renewal extends meanwhile: streaming
            // renews it every third, so it cannot expire while settling.
            let deadline = self.cfg.failed_write_settle.min(self.cfg.lease / 3);
            let settle = async {
                while let Some((_, _, p)) = pipeline.pop_front() {
                    let _ = p.await;
                }
            };
            if tokio::time::timeout(deadline, settle).await.is_err() {
                // What is left is aborted as it is dropped (the old
                // behaviour); reconciliation reclaims what it stored.
                self.put_stats
                    .settle_timeouts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        result
    }

    /// The body of [`Self::stream_generation`], placing into `pipeline`.
    #[allow(clippy::too_many_lines)]
    async fn stream_into(
        &self,
        generation: GenId,
        body: &mut StreamingBlob,
        algorithm: Option<ChecksumAlgorithm>,
        pipeline: &mut Pipeline,
    ) -> Result<Streamed, WriteFailure> {
        let max_chunk = usize::try_from(self.cfg.profile.max_chunk_bytes).unwrap_or(usize::MAX);
        let mut md5 = Md5::new();
        let mut hasher = algorithm.map(Hasher::new);
        let mut size = 0u64;
        let mut index = 0u32;
        let lease_ms = u64::try_from(self.cfg.lease.as_millis()).unwrap_or(u64::MAX);
        let mut batch: Vec<(u32, ChunkRecord)> = Vec::new();
        let mut buf = BytesMut::with_capacity(max_chunk.min(8 << 20));
        let mut finished = false;

        // Read the first chunk to learn whether this is a small object.
        let mut first = true;
        let mut last_renewal = std::time::Instant::now();
        while !finished {
            // Fill `buf` up to max_chunk or end of body.
            let body_started = std::time::Instant::now();
            while buf.len() < max_chunk {
                let next = body
                    .try_next()
                    .await
                    .map_err(|e| crate::body::body_error(&e))?;
                let Some(b) = next else {
                    finished = true;
                    break;
                };
                buf.extend_from_slice(&b);
                // Bytes from the client are progress: keep the generation
                // alive on a slow client link (R-47).
                self.renew_if_due(generation, lease_ms, &mut last_renewal)
                    .await?;
            }
            self.put_stats.body_read.observe(body_started.elapsed());
            let chunk = if buf.len() > max_chunk {
                buf.split_to(max_chunk).freeze()
            } else {
                buf.split().freeze()
            };
            if chunk.is_empty() && !first {
                break;
            }
            let small = first && finished && self.cfg.profile.is_small_object(chunk.len() as u64);
            first = false;
            md5.update(&chunk);
            if let Some(h) = &mut hasher {
                h.update(&chunk);
            }
            let chunk_len = chunk.len();
            // Capacity admission (Phase 3.7, R-81): every client write path
            // (PUT of any length, UploadPart, copies) is charged here, chunk
            // by chunk as the bytes arrive, once per chunk; a throttled
            // chunk waits for its share (bounded), else the write fails
            // before its shards exist and stays invisible.
            let wait = self
                .cluster
                .admission
                .reserve_chunk(chunk_len as u64)
                .map_err(WriteFailure::from)?;
            if !wait.is_zero() {
                self.renewing(
                    generation,
                    lease_ms,
                    &mut last_renewal,
                    tokio::time::sleep(wait),
                )
                .await?;
            }
            // Chunk placements are pipelined: up to `write_pipeline` chunks
            // have their shards in flight while the next chunk's body is
            // read and encoded, so that one slow group commit on a node
            // does not idle the whole PUT (R-48). Records are collected in
            // chunk order; the first failure aborts the rest.
            let placing = {
                let backend = self.clone();
                let offset = size;
                Placement(tokio::spawn(async move {
                    backend
                        .place_chunk(generation, index, chunk, offset, small)
                        .await
                }))
            };
            pipeline.push_back((index, std::time::Instant::now(), placing));
            size += chunk_len as u64;
            index += 1;
            if pipeline.len() >= self.cfg.write_pipeline.max(1) {
                self.collect_placement(
                    pipeline,
                    &mut batch,
                    generation,
                    lease_ms,
                    &mut last_renewal,
                )
                .await?;
            }
            if batch.len() >= self.cfg.chunk_batch {
                let t = std::time::Instant::now();
                object::append_chunks_renewing(
                    self.meta.as_ref(),
                    &generation,
                    &batch,
                    now_ms(),
                    lease_ms,
                )
                .await
                .map_err(meta_err)?;
                self.put_stats.append.observe(t.elapsed());
                batch.clear();
            }
            if chunk_len < max_chunk {
                break;
            }
        }
        while !pipeline.is_empty() {
            self.collect_placement(
                pipeline,
                &mut batch,
                generation,
                lease_ms,
                &mut last_renewal,
            )
            .await?;
        }
        if !batch.is_empty() {
            let t = std::time::Instant::now();
            object::append_chunks_renewing(
                self.meta.as_ref(),
                &generation,
                &batch,
                now_ms(),
                lease_ms,
            )
            .await
            .map_err(meta_err)?;
            self.put_stats.append.observe(t.elapsed());
        }
        Ok(Streamed {
            size,
            etag: checksum::hex(md5.finalize().as_ref()),
            chunk_count: index,
            checksum: hasher.map(|h| (h.algorithm(), h.finalize())),
        })
    }

    /// Renew the pending generation's lease if a third of it has passed
    /// since `last_renewal`.
    async fn renew_if_due(
        &self,
        generation: GenId,
        lease_ms: u64,
        last_renewal: &mut std::time::Instant,
    ) -> Result<(), WriteFailure> {
        if last_renewal.elapsed() >= self.cfg.lease / 3 {
            let t = std::time::Instant::now();
            object::renew_lease(self.meta.as_ref(), &generation, now_ms(), lease_ms)
                .await
                .map_err(meta_err)?;
            self.put_stats.lease_renew.observe(t.elapsed());
            *last_renewal = std::time::Instant::now();
        }
        Ok(())
    }

    /// Drive `fut` to completion, renewing the lease every third of it
    /// meanwhile.
    async fn renewing<T>(
        &self,
        generation: GenId,
        lease_ms: u64,
        last_renewal: &mut std::time::Instant,
        fut: impl Future<Output = T>,
    ) -> Result<T, WriteFailure> {
        tokio::pin!(fut);
        loop {
            let due = *last_renewal + self.cfg.lease / 3;
            tokio::select! {
                out = &mut fut => return Ok(out),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(due)) => {
                    self.renew_if_due(generation, lease_ms, last_renewal).await?;
                }
            }
        }
    }

    /// Await the oldest pipelined placement (renewing the lease meanwhile),
    /// record its chunk, and abort the rest on failure.
    async fn collect_placement(
        &self,
        pipeline: &mut Pipeline,
        batch: &mut Vec<(u32, ChunkRecord)>,
        generation: GenId,
        lease_ms: u64,
        last_renewal: &mut std::time::Instant,
    ) -> Result<(), WriteFailure> {
        let Some((_, started, handle)) = pipeline.pop_front() else {
            return Ok(());
        };
        let joined = self
            .renewing(generation, lease_ms, last_renewal, handle)
            .await?;
        let placed = match joined {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => return Err(e.into()),
            Err(e) => {
                return Err(s3_error!(InternalError, "chunk placement task failed: {e}").into());
            }
        };
        self.put_stats.shard_write.observe(started.elapsed());
        batch.push((placed.index, placed.record));
        if self.take_fault(FaultPoint::AfterShardsBeforeManifest) {
            return Err(s3_error!(InternalError, "injected crash after shards").into());
        }
        Ok(())
    }

    /// Encode one chunk and write its shards to a stripe group, retrying
    /// with a refreshed map on epoch rejection and with another group on
    /// node failures.
    #[allow(clippy::too_many_lines)]
    async fn place_chunk(
        &self,
        generation: GenId,
        index: u32,
        chunk: Bytes,
        offset: u64,
        small: bool,
    ) -> Result<Placed, S3Error> {
        let profile = &self.cfg.profile;
        let (scheme, k, m) = if small {
            let replicas = profile.small_object.as_ref().map_or(1, |s| s.replicas);
            (
                "replication-v1".to_string(),
                1u16,
                replicas.saturating_sub(1).max(1),
            )
        } else {
            (
                profile.scheme.clone(),
                profile.data_shards,
                profile.parity_shards,
            )
        };
        let coder = self
            .coders
            .get(&scheme, k, m)
            .map_err(|e| s3_error!(InternalError, "{e}"))?;
        let encode_started = std::time::Instant::now();
        let (_, shards) =
            arca_lake_tiering::durable::encode_chunk(coder.as_ref(), usize::from(k), &chunk)
                .map_err(|e| s3_error!(InternalError, "{e}"))?;
        let cid = chunk_id(&generation, index);
        let crc = arca_lake_storage::format::crc32c(&chunk);
        self.put_stats.encode.observe(encode_started.elapsed());

        // Groups the controller left out of placement for capacity (Phase
        // 3.6) are not tried.
        let mut tried: Vec<u64> = self.cluster.admission.excluded_groups();
        let excluded = tried.len();
        let mut last_reason: Option<String> = None;
        // Every group that was tried refused for capacity.
        let mut all_capacity = true;
        let mut map = self.map_up_to_date().await.map_err(meta_err)?;
        for attempt in 0..4u32 {
            let Some(group) = self.choose_group(
                &map,
                &scheme,
                k,
                m,
                &tried,
                // Spread by the chunk id (random bits of the generation id),
                // not by the chunk index: index 0 of every object would
                // otherwise always land in the same group.
                placement_salt(&cid).wrapping_add(u64::from(attempt)),
            ) else {
                if all_capacity && (last_reason.is_some() || excluded > 0) {
                    return Err(self.capacity_exhausted(index, last_reason.as_deref()));
                }
                return Err(match last_reason {
                    Some(reason) => s3_error!(
                        ServiceUnavailable,
                        "no active stripe group can accept writes (last group failed: {reason})"
                    ),
                    None => s3_error!(
                        ServiceUnavailable,
                        "no active stripe group can accept writes"
                    ),
                });
            };
            match self.write_shards(&map, &group, cid, &shards).await {
                Ok(()) => {
                    return Ok(Placed {
                        index,
                        record: ChunkRecord {
                            chunk_id: cid,
                            offset,
                            len: u32::try_from(chunk.len()).unwrap_or(u32::MAX),
                            scheme,
                            data_shards: k,
                            parity_shards: m,
                            crc32c: crc,
                            placement: DurablePlacement {
                                group_id: group.id,
                                epoch: group.epoch,
                            },
                        },
                    });
                }
                Err(WriteError::StaleEpoch) => {
                    map = self.refresh_map().await.map_err(meta_err)?;
                    // Same group, fresh epoch: do not mark it as tried.
                }
                Err(WriteError::Capacity(reason)) => {
                    tracing::warn!(group = group.id, index, %reason, "group refused the chunk for capacity");
                    last_reason = Some(reason.clone());
                    tried.push(group.id);
                    if attempt == 3 && all_capacity {
                        return Err(self.capacity_exhausted(index, Some(&reason)));
                    }
                }
                Err(WriteError::Failed(reason)) => {
                    tracing::warn!(group = group.id, index, %reason, "shard write failed");
                    all_capacity = false;
                    last_reason = Some(reason.clone());
                    tried.push(group.id);
                    if attempt == 3 {
                        return Err(s3_error!(
                            ServiceUnavailable,
                            "chunk {index} could not be protected: {reason}"
                        ));
                    }
                }
            }
        }
        Err(s3_error!(
            ServiceUnavailable,
            "chunk {index} could not be placed"
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn choose_group(
        &self,
        map: &ClusterMap,
        scheme: &str,
        k: u16,
        m: u16,
        exclude: &[u64],
        salt: u64,
    ) -> Option<GroupRecord> {
        let mut groups: Vec<&GroupRecord> = map.groups.values().collect();
        // A stable order: the map's iteration order differs per process.
        groups.sort_by_key(|g| g.id);
        // Capacity-aware choice (Phase 3.7, R-82): the groups with the most
        // room left (their fullest member's, as the controller last heard)
        // take new chunks; unknown room counts as plenty.
        let room = self.cluster.admission.group_room();
        let candidates: Vec<WritableGroup> = groups
            .into_iter()
            .filter(|g| {
                matches!(
                    g.state,
                    arca_lake_metadata::records::GroupState::Active
                        | arca_lake_metadata::records::GroupState::Repairing
                )
            })
            .filter(|g| g.scheme == scheme && g.data_shards == k && g.parity_shards == m)
            .filter(|g| g.tier == self.cfg.tier && !exclude.contains(&g.id))
            .map(|g| WritableGroup {
                id: g.id,
                min_free_bytes: room.get(&g.id).copied().unwrap_or(u64::MAX),
            })
            .collect();
        let id = arca_lake_placement::pick_group_for_write(&candidates, salt)?;
        map.groups.get(&id).cloned()
    }

    /// The error of a chunk no group could take for capacity: retryable
    /// `SlowDown` (space comes back as GC and compaction run).
    fn capacity_exhausted(&self, index: u32, reason: Option<&str>) -> S3Error {
        self.cluster
            .admission
            .placement_capacity_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        super::admission::slow_down(
            &format!(
                "chunk {index}: no stripe group has capacity for it ({})",
                reason.unwrap_or("every group is out of placement")
            ),
            30,
        )
    }

    /// Write all shards of a chunk to the group's members in parallel. The
    /// chunk is protected when all data shards and at least `m_ack` parity
    /// shards are acknowledged (Phase 1 requires all: `m_ack = m`).
    async fn write_shards(
        &self,
        map: &ClusterMap,
        group: &GroupRecord,
        cid: [u8; 16],
        shards: &[Bytes],
    ) -> Result<(), WriteError> {
        if group.members.len() != shards.len() {
            return Err(WriteError::Failed(format!(
                "group {} has {} members for {} shards",
                group.id,
                group.members.len(),
                shards.len()
            )));
        }
        let clients: NodeClients = self.clients.clone();
        let frame = self.cfg.shard_frame_bytes;
        let futures = group.members.iter().enumerate().map(|(i, member)| {
            let clients = clients.clone();
            let data = shards[i].clone();
            let addr = map.node_addr(member.node_id).map(str::to_string);
            let shard = ShardRef {
                group_id: group.id,
                chunk_id: cid,
                shard_index: u16::try_from(i).unwrap_or(u16::MAX),
            };
            let epoch = group.epoch;
            async move {
                let Some(addr) = addr else {
                    return Err(WriteError::Failed(format!(
                        "unknown node {}",
                        member.node_id
                    )));
                };
                let expected_crc = arca_lake_storage::format::crc32c(&data);
                // Named for the member: another node at its address
                // refuses it (ADR-0037).
                match clients
                    .put_shard_to(
                        &addr,
                        member.node_id,
                        shard,
                        epoch,
                        data,
                        frame,
                        arca_lake_protocol::storage::v1::WriteClass::Client,
                    )
                    .await
                {
                    Ok(resp) if resp.crc32c == expected_crc => Ok(()),
                    Ok(resp) => Err(WriteError::Failed(format!(
                        "node {addr} stored different bytes (crc {:08x} vs {expected_crc:08x})",
                        resp.crc32c
                    ))),
                    Err(status) if NodeClients::is_epoch_rejected(&status) => {
                        Err(WriteError::StaleEpoch)
                    }
                    // The node is healthy, only full: keep its connection.
                    Err(status) if NodeClients::is_capacity_refused(&status) => Err(
                        WriteError::Capacity(format!("node {addr}: {}", status.message())),
                    ),
                    Err(status) => {
                        clients.forget(&addr);
                        Err(WriteError::Failed(format!("node {addr}: {status}")))
                    }
                }
            }
        });
        let results = futures::future::join_all(futures).await;
        let mut failed = Vec::new();
        let mut capacity = 0u64;
        for r in results {
            match r {
                Ok(()) => {}
                Err(WriteError::StaleEpoch) => return Err(WriteError::StaleEpoch),
                Err(WriteError::Capacity(reason)) => {
                    capacity += 1;
                    failed.push(reason);
                }
                Err(WriteError::Failed(reason)) => failed.push(reason),
            }
        }
        if capacity > 0 {
            self.cluster
                .admission
                .node_capacity_refusals
                .fetch_add(capacity, std::sync::atomic::Ordering::Relaxed);
        }
        // m_ack = m: every shard must be durable. Shards stored before a
        // member refused belong to a pending generation: its GC (or
        // reconciliation) removes them.
        if failed.is_empty() {
            Ok(())
        } else if capacity == failed.len() as u64 {
            Err(WriteError::Capacity(failed.join("; ")))
        } else {
            Err(WriteError::Failed(failed.join("; ")))
        }
    }
}

enum WriteError {
    StaleEpoch,
    /// Every failed member refused for capacity (Phase 3.6).
    Capacity(String),
    Failed(String),
}

/// Group-choice salt of a chunk: a mix of its id's bytes (the generation id
/// carries random bits), so that chunks spread over the eligible groups.
fn placement_salt(cid: &[u8; 16]) -> u64 {
    let a = u64::from_le_bytes(cid[..8].try_into().unwrap_or_default());
    let b = u64::from_le_bytes(cid[8..].try_into().unwrap_or_default());
    // splitmix64 finalizer
    let mut z = a ^ b.rotate_left(29);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
