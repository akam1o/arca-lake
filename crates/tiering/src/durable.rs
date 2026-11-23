//! Reading and writing a whole chunk through a coded group (RFC-004 §5.6,
//! RFC-006 §5.9): data shards first, parity on demand, decode, verify the
//! recorded checksum. Shared by the durable read path, migration, and
//! replicated or erasure-coded cache groups.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use arca_lake_ec::{CodeParams, ErasureCoder, SchemeId, StripeLayout};
use arca_lake_metadata::records::{ChunkRecord, GroupRecord};
use arca_lake_protocol::client::{NodeClients, RecordToken, ShardRef, concat_frames, frames_len};
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
    /// The chunk, as the frames received from the nodes in chunk order
    /// (one part when a shard had to be reconstructed). Concatenate with
    /// [`concat_frames`] when a contiguous buffer is needed.
    pub parts: Vec<Bytes>,
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
    read_coded_with(clients, group, addr_of, &[], chunk_id, len, crc32c, coder).await
}

/// [`read_coded`] with alternate sources: `(shard index, node)` pairs tried
/// for a member whose shard is unavailable. A group whose member is being
/// rebuilt names the node that member left, which still holds its shards
/// until the group is `Active` again, so a move does not lower the read
/// path's tolerance (Phase 3.5, R-65). Whatever the source, the chunk is
/// verified against its length and checksum.
///
/// # Errors
///
/// As [`read_coded`].
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn read_coded_with(
    clients: &NodeClients,
    group: &GroupRecord,
    addr_of: AddrOf<'_>,
    alternates: &[(usize, u64)],
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
        let mut addrs: Vec<String> = addr_of(group.members[i].node_id).into_iter().collect();
        addrs.extend(
            alternates
                .iter()
                .filter(|(idx, _)| *idx == i)
                .filter_map(|(_, node)| addr_of(*node)),
        );
        let shard = ShardRef {
            group_id: group.id,
            chunk_id,
            shard_index: u16::try_from(i).unwrap_or(0),
        };
        async move {
            for addr in addrs {
                if let Ok(f) = clients.get_shard_frames(&addr, shard, 0, 0).await
                    && frames_len(&f) == layout.shard_len()
                {
                    return (i, Some(f));
                }
            }
            (i, None)
        }
    };
    let mut shards: Vec<Option<Vec<Bytes>>> = vec![None; total];
    let mut missing = Vec::new();
    let mut have = 0usize;
    for (i, f) in futures::future::join_all((0..k).map(fetch)).await {
        match f {
            Some(f) => {
                shards[i] = Some(f);
                have += 1;
            }
            None => missing.push(i),
        }
    }
    if have < k {
        for (i, f) in futures::future::join_all((k..total).map(fetch)).await {
            match f {
                Some(f) => {
                    shards[i] = Some(f);
                    have += 1;
                }
                None => missing.push(i),
            }
        }
    }
    if have < k {
        return Err(TierError::Unreadable { have, need: k });
    }
    let chunk_len = usize::try_from(len).unwrap_or(0);
    let parts: Vec<Bytes> = if shards[..k].iter().all(Option::is_some) {
        // Fast path: the data shards are the chunk in order; hand their
        // frames through, trimming the padding of the last shard, with
        // no copy at all.
        let mut parts = Vec::new();
        let mut remaining = chunk_len;
        for s in shards.into_iter().take(k) {
            for f in s.unwrap_or_default() {
                if remaining == 0 {
                    break;
                }
                let take = f.len().min(remaining);
                remaining -= take;
                parts.push(if take == f.len() { f } else { f.slice(..take) });
            }
        }
        parts
    } else {
        // Slow path: a data shard is missing; the decoder needs contiguous
        // shards, so concatenate, reconstruct, and join.
        let mut owned: Vec<Option<Vec<u8>>> = shards
            .into_iter()
            .map(|s| s.map(|f| concat_frames(f).to_vec()))
            .collect();
        coder
            .reconstruct(&mut owned)
            .map_err(|e| TierError::Other(format!("decode failed: {e}")))?;
        let data: Vec<Vec<u8>> = owned
            .into_iter()
            .take(k)
            .map(Option::unwrap_or_default)
            .collect();
        vec![Bytes::from(layout.join(&data))]
    };
    if frames_len(&parts) != chunk_len || arca_lake_storage::format::crc32c_parts(&parts) != crc32c
    {
        return Err(TierError::Corrupt(
            "chunk checksum mismatch after assembly".into(),
        ));
    }
    missing.sort_unstable();
    Ok(CodedRead { parts, missing })
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
) -> Result<Vec<Bytes>, TierError> {
    read_chunk_detailed(clients, group, addr_of, chunk, coder)
        .await
        .map(|r| r.parts)
}

/// [`read_chunk`], also reporting which members' shards were missing or
/// unusable (a degraded read, RFC-008 §8.7).
///
/// # Errors
///
/// As [`read_chunk`].
pub async fn read_chunk_detailed(
    clients: &NodeClients,
    group: &GroupRecord,
    addr_of: AddrOf<'_>,
    chunk: &ChunkRecord,
    coder: &dyn ErasureCoder,
) -> Result<CodedRead, TierError> {
    read_chunk_alternates(clients, group, addr_of, &[], chunk, coder).await
}

/// [`read_chunk_detailed`] with alternate sources ([`read_coded_with`]).
///
/// # Errors
///
/// As [`read_chunk`].
pub async fn read_chunk_alternates(
    clients: &NodeClients,
    group: &GroupRecord,
    addr_of: AddrOf<'_>,
    alternates: &[(usize, u64)],
    chunk: &ChunkRecord,
    coder: &dyn ErasureCoder,
) -> Result<CodedRead, TierError> {
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
    read_coded_with(
        clients,
        group,
        addr_of,
        alternates,
        chunk.chunk_id,
        u64::from(chunk.len),
        chunk.crc32c,
        coder,
    )
    .await
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
/// the shards written by this call are deleted again (only the records it
/// wrote: their tokens) and the error is returned, so that a caller never
/// records a partial copy. Returns each shard written with its node's
/// delete token (R-73).
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
) -> Result<Vec<(String, ShardRef, RecordToken)>, TierError> {
    if shards.len() != group.members.len() {
        return Err(TierError::Other(format!(
            "group {} has {} members for {} shards",
            group.id,
            group.members.len(),
            shards.len()
        )));
    }
    let mut written: Vec<(String, ShardRef, RecordToken)> = Vec::new();
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
            // Only cache copies are written this way (Phase 3.6 class).
            .put_shard_to(
                &addr,
                member.node_id,
                shard,
                group.epoch,
                shards[i].clone(),
                256 << 10,
                arca_lake_protocol::storage::v1::WriteClass::Cache,
            )
            .await
        {
            Ok(r) => r,
            Err(status) => {
                delete_written(clients, &written, group.epoch).await;
                return Err(TierError::Node { addr, status });
            }
        };
        let token = RecordToken::of_put(&resp);
        if resp.crc32c != expected {
            written.push((addr.clone(), shard, token));
            delete_written(clients, &written, group.epoch).await;
            return Err(TierError::Corrupt(format!(
                "node {addr} stored shard {i} with crc {:08x}, expected {expected:08x}",
                resp.crc32c
            )));
        }
        written.push((addr, shard, token));
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

async fn delete_written(
    clients: &NodeClients,
    written: &[(String, ShardRef, RecordToken)],
    epoch: u64,
) {
    // Only records this call wrote (by token): a shard without one (an
    // older node) is left to reconciliation rather than deleted
    // unconditionally over a concurrent writer's copy. A refusal (the
    // node's delete fence or epoch, or an identity the pool does not
    // stamp, R-100) leaves the shard to reconciliation too: counted and
    // logged.
    for (addr, shard, token) in written.iter().filter(|(_, _, t)| t.is_some()) {
        match clients.delete_shard_if(addr, *shard, epoch, *token).await {
            Ok(()) => {}
            Err(s) if s.code() == tonic::Code::NotFound || NodeClients::is_changed(&s) => {}
            Err(s) => {
                clients.note_cleanup_refused();
                tracing::warn!(
                    node = %addr,
                    group = shard.group_id,
                    shard = shard.shard_index,
                    error = %s,
                    "partial copy not deleted: left to reconciliation"
                );
            }
        }
    }
}
