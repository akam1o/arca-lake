//! Reading a whole chunk from its durable placement (RFC-004 §5.6): data
//! shards first, parity on demand, decode, verify the recorded checksum.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use arca_lake_ec::{CodeParams, ErasureCoder, SchemeId, StripeLayout};
use arca_lake_metadata::records::{ChunkRecord, GroupRecord};
use arca_lake_protocol::client::{NodeClients, ShardRef};
use bytes::Bytes;

use crate::TierError;

type CoderMap = HashMap<(String, u16, u16), Arc<dyn ErasureCoder>>;

/// Cache of erasure coders keyed by `(scheme, k, m)`.
#[derive(Default)]
pub struct Coders {
    inner: Mutex<CoderMap>,
}

impl std::fmt::Debug for Coders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coders").finish_non_exhaustive()
    }
}

impl Coders {
    /// Coder for a scheme and parameters.
    ///
    /// # Errors
    ///
    /// Unknown scheme or invalid parameters.
    pub fn get(&self, scheme: &str, k: u16, m: u16) -> Result<Arc<dyn ErasureCoder>, TierError> {
        let key = (scheme.to_string(), k, m);
        if let Some(c) = self
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
        {
            return Ok(Arc::clone(c));
        }
        let id = SchemeId::parse(scheme)
            .ok_or_else(|| TierError::Other(format!("unknown EC scheme {scheme}")))?;
        let params = CodeParams::new(usize::from(k), usize::from(m))
            .map_err(|e| TierError::Other(format!("bad EC params: {e}")))?;
        let coder: Arc<dyn ErasureCoder> = Arc::from(
            arca_lake_ec::new_coder(id, params).map_err(|e| TierError::Other(e.to_string()))?,
        );
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, Arc::clone(&coder));
        Ok(coder)
    }
}

/// Read and verify one whole chunk from `group` (its current members).
/// `addr_of(node_id)` resolves member addresses.
///
/// # Errors
///
/// [`TierError::Unreadable`] with fewer than `k` shards,
/// [`TierError::Corrupt`] on a checksum mismatch after assembly.
pub async fn read_chunk(
    clients: &NodeClients,
    group: &GroupRecord,
    addr_of: &(dyn Fn(u64) -> Option<String> + Sync),
    chunk: &ChunkRecord,
    coder: &dyn ErasureCoder,
) -> Result<Bytes, TierError> {
    let k = usize::from(chunk.data_shards);
    let m = usize::from(chunk.parity_shards);
    let total = k + m;
    if group.members.len() != total {
        return Err(TierError::Other(format!(
            "group {} has {} members for a {k}+{m} chunk",
            group.id,
            group.members.len()
        )));
    }
    let layout = StripeLayout::new(
        usize::try_from(chunk.len).unwrap_or(0),
        k,
        coder.shard_alignment(),
    );
    let fetch = |i: usize| {
        let clients = clients.clone();
        let addr = addr_of(group.members[i].node_id);
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
    for (i, b) in futures::future::join_all((0..k).map(fetch)).await {
        if let Some(b) = b {
            shards[i] = Some(b.to_vec());
            have += 1;
        }
    }
    if have < k {
        for (i, b) in futures::future::join_all((k..total).map(fetch)).await {
            if let Some(b) = b {
                shards[i] = Some(b.to_vec());
                have += 1;
            }
        }
    }
    if have < k {
        return Err(TierError::Unreadable { have, need: k });
    }
    if shards[..k].iter().any(Option::is_none) {
        coder
            .reconstruct(&mut shards)
            .map_err(|e| TierError::Other(format!("decode failed: {e}")))?;
    }
    let data: Vec<Vec<u8>> = shards
        .into_iter()
        .take(k)
        .map(Option::unwrap_or_default)
        .collect();
    let bytes = layout.join(&data);
    if arca_lake_storage::format::crc32c(&bytes) != chunk.crc32c {
        return Err(TierError::Corrupt(
            "chunk checksum mismatch after assembly".into(),
        ));
    }
    Ok(Bytes::from(bytes))
}

/// Encode a chunk into `k + m` shards.
///
/// # Errors
///
/// Encoder errors.
pub fn encode_chunk(
    coder: &dyn ErasureCoder,
    k: usize,
    chunk: &[u8],
) -> Result<(StripeLayout, Vec<Bytes>), TierError> {
    let layout = StripeLayout::new(chunk.len(), k, coder.shard_alignment());
    let data = layout.split(chunk);
    let refs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
    let mut parity = vec![vec![0u8; layout.shard_len()]; coder.params().parity];
    {
        let mut pr: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
        coder
            .encode(&refs, &mut pr)
            .map_err(|e| TierError::Other(format!("encode: {e}")))?;
    }
    Ok((
        layout,
        data.into_iter().chain(parity).map(Bytes::from).collect(),
    ))
}
