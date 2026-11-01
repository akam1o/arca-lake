//! Write path (RFC-003 §5.3, RFC-004 §5.5).

use std::collections::HashMap;

use arca_lake_common::chunk_id;
use arca_lake_ec::{ErasureCoder, StripeLayout};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::object::{self, CommitInfo, Precondition};
use arca_lake_metadata::records::{ChunkRecord, DurablePlacement, GenId, GroupRecord};
use arca_lake_placement::WritableGroup;
use arca_lake_protocol::client::{NodeClients, ShardRef};
use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use s3s::crypto::{Checksum as _, Md5};
use s3s::dto::StreamingBlob;
use s3s::{S3Error, s3_error};

use super::backend::{ClusterBackend, FaultPoint, meta_err, now_ms};
use super::map::ClusterMap;

/// Result of a committed write.
#[derive(Debug, Clone)]
pub struct Committed {
    /// Generation.
    pub generation: GenId,
    /// ETag (hex MD5).
    pub etag: String,
    /// Size.
    pub size: u64,
}

/// One chunk written to a group.
struct Placed {
    index: u32,
    record: ChunkRecord,
}

impl<S: KvStore + 'static> ClusterBackend<S> {
    /// Full PUT: begin a generation, stream the body into chunks, erasure
    /// code and place each chunk, append manifest entries, commit.
    pub(super) async fn write_object(
        &self,
        bucket: &str,
        key: &str,
        mut body: StreamingBlob,
        content_type: Option<String>,
        user_metadata: Option<HashMap<String, String>>,
        pre: &Precondition,
    ) -> Result<Committed, S3Error> {
        let profile = &self.cfg.profile;
        let max_chunk = usize::try_from(profile.max_chunk_bytes).unwrap_or(usize::MAX);
        let now = now_ms();
        let lease_ms = u64::try_from(self.cfg.lease.as_millis()).unwrap_or(u64::MAX);
        let pending = object::begin_put(self.meta.as_ref(), bucket, key, now, lease_ms)
            .await
            .map_err(meta_err)?;
        let generation = pending.generation;

        let result = self
            .write_generation(
                generation,
                &mut body,
                max_chunk,
                content_type,
                user_metadata,
                pre,
            )
            .await;
        match result {
            Ok(c) => Ok(c),
            Err(e) => {
                // Best effort: mark the generation retiring so GC reclaims
                // its shards sooner than the lease would. If this fails the
                // lease expiry sweep handles it.
                let _ = object::abort_put(self.meta.as_ref(), &generation, now_ms()).await;
                Err(e)
            }
        }
    }

    async fn write_generation(
        &self,
        generation: GenId,
        body: &mut StreamingBlob,
        max_chunk: usize,
        content_type: Option<String>,
        user_metadata: Option<HashMap<String, String>>,
        pre: &Precondition,
    ) -> Result<Committed, S3Error> {
        let mut md5 = Md5::new();
        let mut size = 0u64;
        let mut index = 0u32;
        let mut batch: Vec<(u32, ChunkRecord)> = Vec::new();
        let mut buf = BytesMut::with_capacity(max_chunk.min(8 << 20));
        let mut finished = false;

        // Read the first chunk to learn whether this is a small object.
        let mut first = true;
        while !finished {
            // Fill `buf` up to max_chunk or end of body.
            while buf.len() < max_chunk {
                let next = body
                    .try_next()
                    .await
                    .map_err(|e| s3_error!(InternalError, "body read failed: {e}"))?;
                let Some(b) = next else {
                    finished = true;
                    break;
                };
                buf.extend_from_slice(&b);
            }
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
            let placed = self
                .place_chunk(generation, index, chunk.clone(), size, small)
                .await?;
            size += chunk.len() as u64;
            batch.push((placed.index, placed.record));
            index += 1;

            if self.take_fault(FaultPoint::AfterShardsBeforeManifest) {
                return Err(s3_error!(InternalError, "injected crash after shards"));
            }
            if batch.len() >= self.cfg.chunk_batch {
                object::append_chunks(self.meta.as_ref(), &generation, &batch, now_ms())
                    .await
                    .map_err(meta_err)?;
                batch.clear();
            }
            if chunk.len() < max_chunk {
                break;
            }
        }
        if !batch.is_empty() {
            object::append_chunks(self.meta.as_ref(), &generation, &batch, now_ms())
                .await
                .map_err(meta_err)?;
        }
        if self.take_fault(FaultPoint::BeforeCommit) {
            return Err(s3_error!(InternalError, "injected crash before commit"));
        }

        let etag = hex(md5.finalize().as_ref());
        let info = CommitInfo {
            size,
            etag: etag.clone(),
            content_type,
            user_metadata: user_metadata
                .map(|m| m.into_iter().collect())
                .unwrap_or_default(),
            chunk_count: index,
        };
        object::commit_put(self.meta.as_ref(), &generation, &info, pre, now_ms())
            .await
            .map_err(meta_err)?;
        if self.take_fault(FaultPoint::AfterCommit) {
            return Err(s3_error!(InternalError, "injected crash after commit"));
        }
        Ok(Committed {
            generation,
            etag,
            size,
        })
    }

    /// Encode one chunk and write its shards to a stripe group, retrying
    /// with a refreshed map on epoch rejection and with another group on
    /// node failures.
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
        let coder = self.coder(&scheme, k, m)?;
        let layout = StripeLayout::new(chunk.len(), usize::from(k), coder.shard_alignment());
        let shards = encode_chunk(coder.as_ref(), &layout, &chunk)?;
        let cid = chunk_id(&generation, index);
        let crc = arca_lake_storage::format::crc32c(&chunk);

        let mut tried: Vec<u64> = Vec::new();
        let mut map = self.map_up_to_date().await.map_err(meta_err)?;
        for attempt in 0..4u32 {
            let Some(group) = self.choose_group(
                &map,
                &scheme,
                k,
                m,
                &tried,
                u64::from(index) + u64::from(attempt),
            ) else {
                return Err(s3_error!(
                    ServiceUnavailable,
                    "no active stripe group can accept writes"
                ));
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
                Err(WriteError::Failed(reason)) => {
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

    fn choose_group(
        &self,
        map: &ClusterMap,
        scheme: &str,
        k: u16,
        m: u16,
        exclude: &[u64],
        salt: u64,
    ) -> Option<GroupRecord> {
        let candidates: Vec<WritableGroup> = map
            .groups
            .values()
            .filter(|g| g.state == arca_lake_metadata::records::GroupState::Active)
            .filter(|g| g.scheme == scheme && g.data_shards == k && g.parity_shards == m)
            .filter(|g| g.tier == self.cfg.tier && !exclude.contains(&g.id))
            .map(|g| WritableGroup {
                id: g.id,
                min_free_bytes: u64::MAX,
            })
            .collect();
        let id = arca_lake_placement::pick_group_for_write(&candidates, salt)?;
        map.groups.get(&id).cloned()
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
                match clients.put_shard(&addr, shard, epoch, data, frame).await {
                    Ok(resp) if resp.crc32c == expected_crc => Ok(()),
                    Ok(resp) => Err(WriteError::Failed(format!(
                        "node {addr} stored different bytes (crc {:08x} vs {expected_crc:08x})",
                        resp.crc32c
                    ))),
                    Err(status) if NodeClients::is_epoch_rejected(&status) => {
                        Err(WriteError::StaleEpoch)
                    }
                    Err(status) => {
                        clients.forget(&addr);
                        Err(WriteError::Failed(format!("node {addr}: {status}")))
                    }
                }
            }
        });
        let results = futures::future::join_all(futures).await;
        let mut failed = Vec::new();
        for r in results {
            match r {
                Ok(()) => {}
                Err(WriteError::StaleEpoch) => return Err(WriteError::StaleEpoch),
                Err(WriteError::Failed(reason)) => failed.push(reason),
            }
        }
        // m_ack = m: every shard must be durable.
        if failed.is_empty() {
            Ok(())
        } else {
            Err(WriteError::Failed(failed.join("; ")))
        }
    }
}

enum WriteError {
    StaleEpoch,
    Failed(String),
}

fn encode_chunk(
    coder: &dyn ErasureCoder,
    layout: &StripeLayout,
    chunk: &[u8],
) -> Result<Vec<Bytes>, S3Error> {
    let data = layout.split(chunk);
    let refs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
    let mut parity = vec![vec![0u8; layout.shard_len()]; coder.params().parity];
    {
        let mut pr: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
        coder
            .encode(&refs, &mut pr)
            .map_err(|e| s3_error!(InternalError, "encode: {e}"))?;
    }
    Ok(data.into_iter().chain(parity).map(Bytes::from).collect())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}
