//! Reading and writing a whole chunk through a coded group (RFC-004 §5.6,
//! RFC-006 §5.9): data shards first, parity on demand, decode, verify the
//! recorded checksum. Shared by the durable read path, migration, and
//! replicated or erasure-coded cache groups.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use arca_lake_ec::{CodeParams, ErasureCoder, SchemeId, StripeLayout};
use arca_lake_metadata::records::{ChunkRecord, GroupRecord};
use arca_lake_protocol::client::{NodeClients, ShardRef};
use bytes::Bytes;

use crate::TierError;

type CoderMap = HashMap<(String, u16, u16), Arc<dyn ErasureCoder>>;

/// Resolves a node ID to its address.
pub type AddrOf<'a> = &'a (dyn Fn(u64) -> Option<String> + Sync);

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

    /// Coder for a group's scheme and parameters.
    ///
    /// # Errors
    ///
    /// As [`Self::get`].
    pub fn for_group(&self, group: &GroupRecord) -> Result<Arc<dyn ErasureCoder>, TierError> {
        self.get(&group.scheme, group.data_shards, group.parity_shards)
    }
}

/// A chunk assembled from a coded group.
#[derive(Debug, Clone)]
pub struct CodedRead {
    /// The chunk.
    pub bytes: Bytes,
    /// Member indexes whose shard was missing, short, or unreachable
    /// (empty for a clean read). Parity shards that were never requested
    /// are not listed.
    pub missing: Vec<usize>,
}

/// Read and verify one whole chunk of `len` bytes and checksum `crc32c`
/// from `group` (its current members). Data shards are read first; parity
/// is read and decoding happens only when a data shard is unavailable.
///
/// # Errors
///
/// [`TierError::Unreadable`] with fewer than `k` shards,
/// [`TierError::Corrupt`] on a checksum mismatch after assembly.
pub async fn read_coded(
    clients: &NodeClients,
    group: &GroupRecord,
    addr_of: AddrOf<'_>,
    chunk_id: [u8; 16],
    len: u64,
    crc32c: u32,
    coder: &dyn ErasureCoder,
) -> Result<CodedRead, TierError> {
    let k = usize::from(group.data_shards);
    let m = usize::from(group.parity_shards);
    let total = k + m;
    if group.members.len() != total {
        return Err(TierError::Other(format!(
            "group {} has {} members for a {k}+{m} chunk",
            group.id,
            group.members.len()
        )));
    }
    let layout = StripeLayout::new(
        usize::try_from(len).unwrap_or(0),
        k,
        coder.shard_alignment(),
    );
    let fetch = |i: usize| {
        let clients = clients.clone();
        let addr = addr_of(group.members[i].node_id);
        let shard = ShardRef {
            group_id: group.id,
            chunk_id,
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
    let mut missing = Vec::new();
    let mut have = 0usize;
    for (i, b) in futures::future::join_all((0..k).map(fetch)).await {
        match b {
            Some(b) => {
                shards[i] = Some(b.to_vec());
                have += 1;
            }
            None => missing.push(i),
        }
    }
    if have < k {
        for (i, b) in futures::future::join_all((k..total).map(fetch)).await {
            match b {
                Some(b) => {
                    shards[i] = Some(b.to_vec());
                    have += 1;
                }
                None => missing.push(i),
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
    if arca_lake_storage::format::crc32c(&bytes) != crc32c {
        return Err(TierError::Corrupt(
            "chunk checksum mismatch after assembly".into(),
        ));
    }
    missing.sort_unstable();
    Ok(CodedRead {
        bytes: Bytes::from(bytes),
        missing,
    })
}

/// Read and verify one whole chunk from its durable placement.
///
/// # Errors
///
/// As [`read_coded`].
pub async fn read_chunk(
    clients: &NodeClients,
    group: &GroupRecord,
    addr_of: AddrOf<'_>,
    chunk: &ChunkRecord,
    coder: &dyn ErasureCoder,
) -> Result<Bytes, TierError> {
    if usize::from(chunk.data_shards) != usize::from(group.data_shards)
        || usize::from(chunk.parity_shards) != usize::from(group.parity_shards)
    {
        return Err(TierError::Other(format!(
            "chunk recorded as {}+{} but group {} is {}+{}",
            chunk.data_shards,
            chunk.parity_shards,
            group.id,
            group.data_shards,
            group.parity_shards
        )));
    }
    read_coded(
        clients,
        group,
        addr_of,
        chunk.chunk_id,
        u64::from(chunk.len),
        chunk.crc32c,
        coder,
    )
    .await
    .map(|r| r.bytes)
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

/// Write `shards` of `chunk_id` to the members of `group` with the group's
/// epoch (member `i` receives shard `i`), restricted to `only` when given.
/// Every write must be acknowledged with the expected CRC; on any failure
/// the shards written by this call are deleted again and the error is
/// returned, so that a caller never records a partial copy.
///
/// # Errors
///
/// Unknown member, node errors, CRC mismatch.
pub async fn write_coded(
    clients: &NodeClients,
    group: &GroupRecord,
    addr_of: AddrOf<'_>,
    chunk_id: [u8; 16],
    shards: &[Bytes],
    only: Option<&[usize]>,
) -> Result<Vec<(String, ShardRef)>, TierError> {
    if shards.len() != group.members.len() {
        return Err(TierError::Other(format!(
            "group {} has {} members for {} shards",
            group.id,
            group.members.len(),
            shards.len()
        )));
    }
    let mut written: Vec<(String, ShardRef)> = Vec::new();
    for (i, member) in group.members.iter().enumerate() {
        if only.is_some_and(|o| !o.contains(&i)) {
            continue;
        }
        let Some(addr) = addr_of(member.node_id) else {
            delete_written(clients, &written, group.epoch).await;
            return Err(TierError::Other(format!("unknown node {}", member.node_id)));
        };
        let shard = ShardRef {
            group_id: group.id,
            chunk_id,
            shard_index: u16::try_from(i).unwrap_or(0),
        };
        let expected = arca_lake_storage::format::crc32c(&shards[i]);
        let resp = match clients
            .put_shard(&addr, shard, group.epoch, shards[i].clone(), 256 << 10)
            .await
        {
            Ok(r) => r,
            Err(status) => {
                delete_written(clients, &written, group.epoch).await;
                return Err(TierError::Node { addr, status });
            }
        };
        if resp.crc32c != expected {
            written.push((addr.clone(), shard));
            delete_written(clients, &written, group.epoch).await;
            return Err(TierError::Corrupt(format!(
                "node {addr} stored shard {i} with crc {:08x}, expected {expected:08x}",
                resp.crc32c
            )));
        }
        written.push((addr, shard));
    }
    Ok(written)
}

/// Delete shard `i` of `chunk_id` on member `i` of `group` for every
/// member, with `epoch`. Best effort: returns how many deletes failed with
/// something other than `NotFound` (those shards are orphans for
/// reconciliation).
pub async fn delete_shards(
    clients: &NodeClients,
    group: &GroupRecord,
    addr_of: AddrOf<'_>,
    chunk_id: [u8; 16],
    epoch: u64,
) -> usize {
    let mut failed = 0;
    for (i, member) in group.members.iter().enumerate() {
        let Some(addr) = addr_of(member.node_id) else {
            failed += 1;
            continue;
        };
        let shard = ShardRef {
            group_id: group.id,
            chunk_id,
            shard_index: u16::try_from(i).unwrap_or(0),
        };
        match clients.delete_shard(&addr, shard, epoch).await {
            Ok(()) => {}
            Err(s) if s.code() == tonic::Code::NotFound => {}
            Err(_) => failed += 1,
        }
    }
    failed
}

async fn delete_written(clients: &NodeClients, written: &[(String, ShardRef)], epoch: u64) {
    for (addr, shard) in written {
        let _ = clients.delete_shard(addr, *shard, epoch).await;
    }
}
