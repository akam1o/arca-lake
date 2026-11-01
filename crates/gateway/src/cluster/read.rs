//! Read path (RFC-004 §5.6): manifest → shards → decode → verify → stream.

use std::sync::Arc;

use arca_lake_ec::StripeLayout;
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::object::{self, Manifest};
use arca_lake_metadata::records::ChunkRecord;
use arca_lake_protocol::client::{NodeClients, ShardRef};
use bytes::Bytes;
use s3s::dto::{ETag, GetObjectInput, GetObjectOutput, StreamingBlob};
use s3s::{S3Error, S3Response, S3Result, s3_error};

use super::backend::{ClusterBackend, meta_err};
use super::map::ClusterMap;

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
        let clients = self.clients.clone();
        let coders: Vec<(ChunkRecord, Arc<dyn arca_lake_ec::ErasureCoder>)> = manifest
            .chunks
            .iter()
            .filter(|(_, c)| c.offset < end && c.offset + u64::from(c.len) > start)
            .filter_map(|(_, c)| {
                self.coder(&c.scheme, c.data_shards, c.parity_shards)
                    .ok()
                    .map(|k| (c.clone(), k))
            })
            .collect();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, S3Error>>(2);
        tokio::spawn(async move {
            for (chunk, coder) in coders {
                let result = fetch_chunk(&clients, &map, &chunk, coder.as_ref()).await;
                let bytes = match result {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                // Slice to the requested range.
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
}

/// Fetch and verify one whole chunk: data shards first, parity on demand,
/// decode if needed, then verify the chunk checksum.
async fn fetch_chunk(
    clients: &NodeClients,
    map: &ClusterMap,
    chunk: &ChunkRecord,
    coder: &dyn arca_lake_ec::ErasureCoder,
) -> Result<Bytes, S3Error> {
    let group = map.groups.get(&chunk.placement.group_id).ok_or_else(|| {
        s3_error!(
            InternalError,
            "unknown stripe group {}",
            chunk.placement.group_id
        )
    })?;
    let k = usize::from(chunk.data_shards);
    let m = usize::from(chunk.parity_shards);
    let total = k + m;
    if group.members.len() != total {
        return Err(s3_error!(
            InternalError,
            "group {} membership does not match chunk",
            group.id
        ));
    }
    let layout = StripeLayout::new(
        usize::try_from(chunk.len).unwrap_or(0),
        k,
        coder.shard_alignment(),
    );

    let fetch = |i: usize| {
        let clients = clients.clone();
        let addr = map.node_addr(group.members[i].node_id).map(str::to_string);
        let shard = ShardRef {
            group_id: group.id,
            chunk_id: chunk.chunk_id,
            shard_index: u16::try_from(i).unwrap_or(0),
        };
        async move {
            let Some(addr) = addr else { return (i, None) };
            match clients.get_shard(&addr, shard, 0, 0).await {
                Ok(b) if b.len() == layout.shard_len() => (i, Some(b)),
                Ok(_) | Err(_) => (i, None),
            }
        }
    };

    let mut shards: Vec<Option<Vec<u8>>> = vec![None; total];
    let mut have = 0usize;
    // Round 1: data shards.
    for (i, b) in futures::future::join_all((0..k).map(fetch)).await {
        if let Some(b) = b {
            shards[i] = Some(b.to_vec());
            have += 1;
        }
    }
    // Round 2: parity shards as needed (all of them; missing ones are fine).
    if have < k {
        for (i, b) in futures::future::join_all((k..total).map(fetch)).await {
            if let Some(b) = b {
                shards[i] = Some(b.to_vec());
                have += 1;
            }
        }
    }
    if have < k {
        return Err(s3_error!(
            InternalError,
            "chunk {} unreadable: only {have} of {k} required shards available (data integrity error)",
            hex(&chunk.chunk_id)
        ));
    }
    if shards[..k].iter().any(Option::is_none) {
        coder
            .reconstruct(&mut shards)
            .map_err(|e| s3_error!(InternalError, "decode failed: {e}"))?;
    }
    let data: Vec<Vec<u8>> = shards
        .into_iter()
        .take(k)
        .map(Option::unwrap_or_default)
        .collect();
    let bytes = layout.join(&data);
    if arca_lake_storage::format::crc32c(&bytes) != chunk.crc32c {
        return Err(s3_error!(
            InternalError,
            "chunk {} checksum mismatch after assembly",
            hex(&chunk.chunk_id)
        ));
    }
    Ok(Bytes::from(bytes))
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
