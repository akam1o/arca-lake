//! Read path (RFC-004 §5.6, RFC-006 §5.3, §5.9): manifest → per chunk, a
//! validated cache hit (possibly degraded, then repaired asynchronously) or
//! a durable read with asynchronous cache population → stream.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::object::{self, Manifest};
use arca_lake_metadata::records::{CachePlacement, ChunkRecord, GenId, GroupRecord};
use arca_lake_tiering::TierError;
use arca_lake_tiering::cache::{self as tcache, CacheTierConfig};
use bytes::Bytes;
use s3s::dto::{ETag, GetObjectInput, GetObjectOutput, StreamingBlob};
use s3s::{S3Error, S3Response, S3Result, s3_error};

use super::backend::{ClusterBackend, meta_err, now_ms};
use super::map::ClusterMap;

fn addr_of(map: &ClusterMap) -> impl Fn(u64) -> Option<String> + Sync + '_ {
    move |node_id| map.node_addr(node_id).map(str::to_string)
}

impl<S: KvStore + 'static> ClusterBackend<S> {
    pub(super) async fn read_object(
        &self,
        input: GetObjectInput,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let manifest = object::read_manifest(self.meta.as_ref(), &input.bucket, &input.key, None)
            .await
            .map_err(meta_err)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let total = manifest.header.size;
        let (start, end, content_range) = match &input.range {
            None => (0, total, None),
            Some(r) => {
                let r = r.check(total)?;
                (
                    r.start,
                    r.end,
                    Some(format!(
                        "bytes {}-{}/{total}",
                        r.start,
                        r.end.saturating_sub(1)
                    )),
                )
            }
        };

        let map = self.map_up_to_date().await.map_err(meta_err)?;
        self.note_access(manifest.pointer.generation);
        let body = self.stream_range(Arc::clone(&map), &manifest, start, end);
        Ok(S3Response::new(GetObjectOutput {
            body: Some(body),
            content_length: Some(i64::try_from(end - start).unwrap_or(i64::MAX)),
            content_range,
            e_tag: Some(ETag::Strong(manifest.header.etag.clone())),
            last_modified: Some(s3s::dto::Timestamp::from(
                std::time::UNIX_EPOCH
                    + std::time::Duration::from_millis(manifest.pointer.last_modified_ms),
            )),
            content_type: manifest.header.content_type.clone(),
            metadata: (!manifest.header.user_metadata.is_empty())
                .then(|| manifest.header.user_metadata.iter().cloned().collect()),
            accept_ranges: Some("bytes".into()),
            ..Default::default()
        }))
    }

    /// Stream bytes `[start, end)` of the object, chunk by chunk. Errors
    /// after the first byte terminate the stream with an error (the client
    /// observes a truncated body), never with wrong bytes.
    fn stream_range(
        &self,
        map: Arc<ClusterMap>,
        manifest: &Manifest,
        start: u64,
        end: u64,
    ) -> StreamingBlob {
        let generation = manifest.pointer.generation;
        let chunks: Vec<(u32, ChunkRecord, Option<CachePlacement>)> = manifest
            .chunks
            .iter()
            .filter(|(_, c)| c.offset < end && c.offset + u64::from(c.len) > start)
            .map(|(i, c)| {
                let cached = manifest
                    .cache
                    .iter()
                    .find(|(ci, _)| ci == i)
                    .map(|(_, p)| p.clone());
                (*i, c.clone(), cached)
            })
            .collect();
        let backend = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, S3Error>>(2);
        tokio::spawn(async move {
            for (index, chunk, cached) in chunks {
                let result = backend
                    .read_chunk_bytes(&map, generation, index, &chunk, cached.as_ref())
                    .await;
                let bytes = match result {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                let cs = chunk.offset;
                let ce = chunk.offset + u64::from(chunk.len);
                let from = usize::try_from(start.max(cs) - cs).unwrap_or(0);
                let to = usize::try_from(end.min(ce) - cs).unwrap_or(bytes.len());
                if tx.send(Ok(bytes.slice(from..to))).await.is_err() {
                    return;
                }
            }
        });
        StreamingBlob::wrap(tokio_stream::wrappers::ReceiverStream::new(rx))
    }

    /// One whole chunk: from the cache when a validated copy exists,
    /// otherwise from the durable placement (then possibly populated).
    pub(super) async fn read_chunk_bytes(
        &self,
        map: &Arc<ClusterMap>,
        generation: GenId,
        index: u32,
        chunk: &ChunkRecord,
        cached: Option<&CachePlacement>,
    ) -> Result<Bytes, S3Error> {
        if let (Some(_), Some(p)) = (self.cfg.cache.as_ref(), cached) {
            if let Some(bytes) = self.try_cache_hit(map, generation, index, chunk, p).await {
                self.cache_stats.hits.fetch_add(1, Ordering::Relaxed);
                self.touch_cache(generation, index);
                return Ok(bytes);
            }
            self.cache_stats.fallbacks.fetch_add(1, Ordering::Relaxed);
        } else if self.cfg.cache.is_some() {
            self.cache_stats.misses.fetch_add(1, Ordering::Relaxed);
        }
        let bytes = self.read_durable(map, chunk).await?;
        if self.cfg.cache.is_some() && cached.is_none() {
            self.maybe_populate(map, generation, index, chunk, bytes.clone());
        }
        Ok(bytes)
    }

    /// Durable read of one chunk (RFC-004 §5.6).
    pub(super) async fn read_durable(
        &self,
        map: &ClusterMap,
        chunk: &ChunkRecord,
    ) -> Result<Bytes, S3Error> {
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
        arca_lake_tiering::durable::read_chunk(
            &self.clients,
            group,
            &addr_of(map),
            chunk,
            coder.as_ref(),
        )
        .await
        .map_err(|e| match e {
            TierError::Unreadable { have, need } => s3_error!(
                InternalError,
                "chunk unreadable: only {have} of {need} required shards available (data integrity error)"
            ),
            other => s3_error!(InternalError, "{other}"),
        })
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
    ) -> Option<Bytes> {
        let Some(group) = map.groups.get(&p.group_id) else {
            self.invalidate_cache(Arc::clone(map), None, generation, index);
            return None;
        };
        let Ok(r) = tcache::read_cached(
            &self.clients,
            &self.coders,
            group,
            &addr_of(map),
            &generation,
            index,
            chunk,
        )
        .await
        else {
            self.invalidate_cache(Arc::clone(map), Some(group.clone()), generation, index);
            return None;
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
                r.bytes.clone(),
                r.missing,
            );
        }
        Some(r.bytes)
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
            return;
        }
        if !self
            .cache_state
            .admit(generation, index, cfg.admit_after_misses)
        {
            return;
        }
        let cid = arca_lake_common::chunk_id(&generation, index);
        let Some(group) = tcache::choose_group(map.groups.values(), &cfg.tier, &cid).cloned()
        else {
            return;
        };
        let Ok(permit) = Arc::clone(&self.cache_state.populate_permits).try_acquire_owned() else {
            self.cache_stats
                .populations_skipped
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        if !self.cache_state.begin_populate(generation, index) {
            self.cache_stats
                .populations_deduplicated
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let backend = self.clone();
        let cfg = cfg.clone();
        let map = Arc::clone(map);
        tokio::spawn(async move {
            let _permit = permit;
            let r = tcache::populate(
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
            .await;
            backend.cache_state.end_populate(generation, index);
            match r {
                Ok(tcache::PopulateOutcome::Inserted) => {
                    backend
                        .cache_stats
                        .populated
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(tcache::PopulateOutcome::AlreadyPresent) => {}
                Ok(tcache::PopulateOutcome::Rejected(_)) => {
                    backend
                        .cache_stats
                        .admissions_rejected
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::debug!(error = %e, index, "cache population failed");
                    backend
                        .cache_stats
                        .population_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    /// Coalesced access record for automatic tiering (RFC-006 §5.8).
    fn note_access(&self, generation: GenId) {
        let now = now_ms();
        let Some(reads) = self.cache_state.flush_access(generation, now) else {
            return;
        };
        let backend = self.clone();
        tokio::spawn(async move {
            let _ = object::record_access(backend.meta.as_ref(), &generation, now, reads).await;
        });
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
        let manifest = object::read_manifest(self.meta.as_ref(), bucket, key, None)
            .await
            .map_err(meta_err)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let map = self.map_up_to_date().await.map_err(meta_err)?;
        let generation = manifest.pointer.generation;
        let mut cached = 0;
        for (index, chunk) in &manifest.chunks {
            if manifest.cache.iter().any(|(i, _)| i == index) {
                cached += 1;
                continue;
            }
            let bytes = self.read_durable(&map, chunk).await?;
            let cid = arca_lake_common::chunk_id(&generation, *index);
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
                &generation,
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
        let manifest = object::read_manifest(self.meta.as_ref(), bucket, key, None)
            .await
            .map_err(meta_err)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        arca_lake_metadata::ops::cache::set_pinned(
            self.meta.as_ref(),
            &manifest.pointer.generation,
            true,
        )
        .await
        .map_err(meta_err)?;
        Ok(n)
    }

    /// Unpin the current version of `bucket/key` (entries become evictable).
    ///
    /// # Errors
    ///
    /// Missing object or metadata errors.
    pub async fn unpin(&self, bucket: &str, key: &str) -> Result<usize, S3Error> {
        let manifest = object::read_manifest(self.meta.as_ref(), bucket, key, None)
            .await
            .map_err(meta_err)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        arca_lake_metadata::ops::cache::set_pinned(
            self.meta.as_ref(),
            &manifest.pointer.generation,
            false,
        )
        .await
        .map_err(meta_err)
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
