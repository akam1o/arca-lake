//! Member replacement and reconstruction (RFC-004 §5.4, §5.7; RFC-008 §7).
//!
//! Order of operations and the meaning of "complete":
//!
//! 1. **Fence.** Every reachable member of the old and new membership
//!    learns `epoch + 1`. The replacement must acknowledge the fence (it is
//!    about to receive writes under the new epoch, and a pending delete
//!    decided under the old epoch must be rejected by it, RFC-008 §6.4).
//! 2. **Commit membership.** One transaction replaces the member, sets
//!    `epoch + 1`, state `Repairing`, and clears the cursor. It fails if the
//!    group changed since it was read ([`ControlError::EpochChanged`]).
//!    Writers keep using the group (`Repairing` accepts writes); their new
//!    chunks carry the new epoch and include the replacement directly.
//! 3. **Reconstruct.** Chunks are walked in chunk-ID order with bounded
//!    memory ([`crate::walk::GroupWalker`]). For each chunk the replacement
//!    lacks (or holds corrupt, per `VerifyShards`), `k` shards are read, the
//!    missing shard is reconstructed, the whole chunk is checked against the
//!    checksum, scheme, and placement recorded in metadata, and the shard is
//!    written with the new epoch; the node's returned CRC must match. The
//!    cursor `(member, chunk ID)` is persisted after every chunk so that a
//!    crashed controller resumes by re-issuing the same command.
//! 4. **Complete.** The group becomes `Active` only when the completeness
//!    check passes (§ `Completeness`): every member lists every chunk with
//!    the same payload length, and the replacement's shards verify. If the
//!    check fails, the cursor is cleared so that the next run walks the
//!    whole group again (a shard the node quarantined during verification
//!    is then found missing and rebuilt). If another membership change
//!    happened meanwhile, the repair stops with
//!    [`ControlError::EpochChanged`] and must be re-issued; the group stays
//!    `Repairing`.
//!
//! Nothing here deletes anything: shards of the replaced member stay where
//! they are and are reclaimed by reconciliation once the group is `Active`
//! and the node is no longer a member (`gc::reconcile_node`).

use std::collections::{BTreeSet, HashMap};

use arca_lake_common::split_chunk_id;
use arca_lake_ec::{CodeParams, ErasureCoder, SchemeId, StripeLayout};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::{cluster, gc};
use arca_lake_metadata::records::{GroupMember, GroupRecord, GroupState, NodeRecord, RepairCursor};
use arca_lake_protocol::client::{NodeClients, ShardRef};
use bytes::Bytes;

use crate::ControlError;
use crate::walk::{ChunkPresence, GroupWalker};

/// Tunables. Defaults: node default page size, verify only the replacement.
#[derive(Debug, Clone, Default)]
pub struct RepairOptions {
    /// `ListShards`/`VerifyShards` page size (0 = node default).
    pub page_size: u32,
    /// Verify every member's shards (not only the replacement's) before
    /// declaring the group complete. Reads all data of the group.
    pub verify_all_members: bool,
}

/// Result of a repair.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairReport {
    /// Epoch of the group during this repair.
    pub epoch: u64,
    /// Chunks whose shard was reconstructed and written.
    pub reconstructed: usize,
    /// Chunks already present (and verified) on the replacement.
    pub skipped: usize,
    /// Whether this run continued an interrupted repair.
    pub resumed: bool,
    /// Whether the group is `Active` again.
    pub complete: bool,
    /// What the completeness check found.
    pub completeness: Completeness,
}

/// Outcome of the completeness check (RFC-008 §7).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Completeness {
    /// Chunks walked.
    pub chunks: usize,
    /// Chunks missing on at least one member.
    pub missing: usize,
    /// Chunks whose shards have different payload lengths across members.
    pub length_mismatch: usize,
    /// Shards that failed verification, per member index.
    pub corrupt: Vec<(usize, usize)>,
    /// Members that did not answer.
    pub unavailable: Vec<usize>,
}

impl Completeness {
    /// All members answered, nothing missing, nothing corrupt.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing == 0
            && self.length_mismatch == 0
            && self.corrupt.is_empty()
            && self.unavailable.is_empty()
    }
}

/// Points at which a [`replace_member_with`] hook is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairStep {
    /// Reachable members were told the new epoch.
    Fenced,
    /// The new membership is committed in metadata.
    MembershipCommitted,
    /// One chunk's shard was written to the replacement (`index` counts
    /// reconstructed chunks in this run).
    Chunk {
        /// Ordinal within this run.
        index: usize,
        /// Chunk ID.
        chunk_id: [u8; 16],
    },
    /// Reconstruction finished; completeness is about to be checked.
    Reconstructed,
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

/// Replace member `index` of `group_id` with `replacement` (see the module
/// documentation for the protocol). Re-issuing the same call after a crash
/// or an [`ControlError::EpochChanged`] resumes from the persisted cursor.
///
/// # Errors
///
/// Metadata errors, node errors on the replacement, an unrecoverable or
/// corrupt chunk, or [`ControlError::EpochChanged`].
pub async fn replace_member<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    group_id: u64,
    index: usize,
    replacement: GroupMember,
) -> Result<RepairReport, ControlError> {
    replace_member_with(
        meta,
        clients,
        group_id,
        index,
        replacement,
        &RepairOptions::default(),
        async |_| Ok(()),
    )
    .await
}

/// [`replace_member`] with options and a hook called at every
/// [`RepairStep`]; an error from the hook stops the repair at that point
/// (tests use it to emulate a controller crash or to run concurrent
/// operations).
///
/// # Errors
///
/// As [`replace_member`], plus whatever the hook returns.
pub async fn replace_member_with<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    group_id: u64,
    index: usize,
    replacement: GroupMember,
    opts: &RepairOptions,
    mut hook: impl AsyncFnMut(RepairStep) -> Result<(), ControlError>,
) -> Result<RepairReport, ControlError> {
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let group = cluster::get_group(meta, group_id).await?;
    if index >= group.members.len() {
        return Err(ControlError::Other(format!(
            "member index {index} out of range"
        )));
    }

    let (group, resumed) = if group.members[index] == replacement {
        match group.state {
            GroupState::Repairing => (group, true),
            _ => {
                // Already replaced and complete: nothing to do.
                return Ok(RepairReport {
                    epoch: group.epoch,
                    complete: true,
                    ..RepairReport::default()
                });
            }
        }
    } else {
        let new_epoch = group.epoch + 1;
        fence(clients, &nodes, &group, replacement, new_epoch).await?;
        hook(RepairStep::Fenced).await?;
        let expected_epoch = group.epoch;
        let updated = cluster::update_group(meta, group_id, |g| {
            if g.epoch != expected_epoch {
                return false;
            }
            g.members[index] = replacement;
            g.epoch = new_epoch;
            g.state = GroupState::Repairing;
            // The cursor names the member being rebuilt from the start, so
            // that any controller can resume the repair (RFC-011 §5.5).
            g.repair_cursor = Some(RepairCursor {
                member: u16::try_from(index).unwrap_or(u16::MAX),
                chunk_id: [0; 16],
            });
            true
        })
        .await?;
        if updated.epoch != new_epoch || updated.members[index] != replacement {
            return Err(ControlError::EpochChanged {
                group: group_id,
                expected: expected_epoch,
                found: updated.epoch,
            });
        }
        hook(RepairStep::MembershipCommitted).await?;
        (updated, false)
    };

    let mut report =
        reconstruct_member(meta, clients, &nodes, &group, index, opts, &mut hook).await?;
    report.resumed = resumed;
    hook(RepairStep::Reconstructed).await?;

    let completeness = check_completeness(clients, &nodes, &group, index, opts).await?;
    let complete = completeness.is_complete();
    if !complete {
        // The next run must walk from the start again: the cursor only
        // records how far reconstruction got, and the check found something
        // (a corrupt or missing shard, an unreachable member) that a resumed
        // walk from the cursor would never revisit.
        let epoch = group.epoch;
        cluster::update_group(meta, group_id, |g| {
            if g.epoch == epoch && g.state == GroupState::Repairing && g.repair_cursor.is_some() {
                g.repair_cursor = None;
                true
            } else {
                false
            }
        })
        .await?;
    }
    if complete {
        let epoch = group.epoch;
        let after = cluster::update_group(meta, group_id, |g| {
            if g.epoch == epoch && g.state == GroupState::Repairing {
                g.state = GroupState::Active;
                g.repair_cursor = None;
                true
            } else {
                false
            }
        })
        .await?;
        if after.epoch != epoch {
            return Err(ControlError::EpochChanged {
                group: group_id,
                expected: epoch,
                found: after.epoch,
            });
        }
    }
    report.complete = complete;
    report.completeness = completeness;
    Ok(report)
}

/// Tell every reachable member of old ∪ new the new epoch. Unreachable
/// *old* members are the ones being replaced; the replacement must
/// acknowledge, otherwise the change is not started.
async fn fence(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    replacement: GroupMember,
    new_epoch: u64,
) -> Result<(), ControlError> {
    let mut fenced: BTreeSet<u64> = BTreeSet::new();
    for m in &group.members {
        if !fenced.insert(m.node_id) {
            continue;
        }
        if let Some(n) = nodes.get(&m.node_id) {
            let _ = clients.update_epoch(&n.address, group.id, new_epoch).await;
        }
    }
    let n = nodes.get(&replacement.node_id).ok_or_else(|| {
        ControlError::Other(format!("replacement node {} unknown", replacement.node_id))
    })?;
    clients
        .update_epoch(&n.address, group.id, new_epoch)
        .await
        .map_err(|status| ControlError::Node {
            addr: n.address.clone(),
            status,
        })?;
    Ok(())
}

/// Shards on the target that fail verification (walks `VerifyShards`).
async fn corrupt_on_target(
    clients: &NodeClients,
    addr: &str,
    group: &GroupRecord,
    index: usize,
    page_size: u32,
) -> Result<BTreeSet<[u8; 16]>, ControlError> {
    let own = u16::try_from(index).unwrap_or(u16::MAX);
    let mut out = BTreeSet::new();
    let mut after = None;
    loop {
        let page = clients
            .verify_shards_page(addr, group.id, after, page_size)
            .await
            .map_err(|status| ControlError::Node {
                addr: addr.to_string(),
                status,
            })?;
        for v in &page.shards {
            if v.shard.shard_index == own && !v.ok {
                out.insert(v.shard.chunk_id);
            }
        }
        match page.next {
            Some(n) => after = Some(n),
            None => return Ok(out),
        }
    }
}

fn coder_for(group: &GroupRecord) -> Result<(Box<dyn ErasureCoder>, CodeParams), ControlError> {
    let scheme = SchemeId::parse(&group.scheme)
        .ok_or_else(|| ControlError::Other(format!("unknown scheme {}", group.scheme)))?;
    let params = CodeParams::new(
        usize::from(group.data_shards),
        usize::from(group.parity_shards),
    )
    .map_err(|e| ControlError::Other(e.to_string()))?;
    let coder =
        arca_lake_ec::new_coder(scheme, params).map_err(|e| ControlError::Other(e.to_string()))?;
    Ok((coder, params))
}

async fn reconstruct_member<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    index: usize,
    opts: &RepairOptions,
    hook: &mut impl AsyncFnMut(RepairStep) -> Result<(), ControlError>,
) -> Result<RepairReport, ControlError> {
    let target_addr = nodes
        .get(&group.members[index].node_id)
        .map(|n| n.address.clone())
        .ok_or_else(|| {
            ControlError::Other(format!(
                "replacement node {} unknown",
                group.members[index].node_id
            ))
        })?;
    // Shards the target holds but cannot read back are treated as missing.
    let corrupt = corrupt_on_target(clients, &target_addr, group, index, opts.page_size).await?;
    let cursor = group
        .repair_cursor
        .filter(|c| usize::from(c.member) == index && c.chunk_id != [0; 16])
        .map(|c| c.chunk_id);
    let (coder, params) = coder_for(group)?;

    let mut report = RepairReport {
        epoch: group.epoch,
        ..RepairReport::default()
    };
    let mut walker = GroupWalker::new(clients, nodes, group, cursor, opts.page_size);
    while let Some(chunk) = walker.next().await? {
        let cid = chunk.chunk_id;
        if chunk.present[index].is_some() && !corrupt.contains(&cid) {
            report.skipped += 1;
            persist_cursor(meta, group, index, cid).await?;
            continue;
        }
        let Some(bytes) = reconstruct_one(
            meta,
            clients,
            nodes,
            group,
            index,
            &chunk,
            coder.as_ref(),
            params,
        )
        .await?
        else {
            // Deleted meanwhile (no live generation accounts for it).
            report.skipped += 1;
            persist_cursor(meta, group, index, cid).await?;
            continue;
        };
        let expected_crc = arca_lake_storage::format::crc32c(&bytes);
        let shard = ShardRef {
            group_id: group.id,
            chunk_id: cid,
            shard_index: u16::try_from(index).unwrap_or(0),
        };
        match clients
            .put_shard(&target_addr, shard, group.epoch, bytes, 256 << 10)
            .await
        {
            Ok(resp) if resp.crc32c == expected_crc => {}
            Ok(resp) => {
                return Err(ControlError::Corrupt(format!(
                    "replacement {target_addr} stored chunk {} with crc {:08x}, expected {expected_crc:08x}",
                    hex(&cid),
                    resp.crc32c
                )));
            }
            Err(status) if NodeClients::is_epoch_rejected(&status) => {
                return Err(ControlError::EpochChanged {
                    group: group.id,
                    expected: group.epoch,
                    found: 0,
                });
            }
            Err(status) => {
                return Err(ControlError::Node {
                    addr: target_addr.clone(),
                    status,
                });
            }
        }
        persist_cursor(meta, group, index, cid).await?;
        let index_in_run = report.reconstructed;
        report.reconstructed += 1;
        hook(RepairStep::Chunk {
            index: index_in_run,
            chunk_id: cid,
        })
        .await?;
    }
    if walker.unavailable().contains(&index) {
        return Err(ControlError::Other(format!(
            "replacement {target_addr} did not answer ListShards"
        )));
    }
    Ok(report)
}

/// Read any `k` shards of the chunk from the members other than `index`,
/// reconstruct shard `index`, and validate the whole chunk against its
/// metadata record (checksum, scheme, placement) when one exists.
///
/// `None` when fewer than `k` shards exist and metadata has no live
/// generation for the chunk (deleted while the repair ran).
#[allow(clippy::too_many_arguments)]
async fn reconstruct_one<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    index: usize,
    chunk: &ChunkPresence,
    coder: &dyn ErasureCoder,
    params: CodeParams,
) -> Result<Option<Bytes>, ControlError> {
    let cid = chunk.chunk_id;
    let k = params.data;
    let mut shards: Vec<Option<Vec<u8>>> = vec![None; params.total()];
    let mut have = 0;
    // Prefer members that list the shard; try the others too (the listing
    // may be stale).
    let order: Vec<usize> = chunk
        .holders()
        .chain(0..group.members.len())
        .filter(|&i| i != index)
        .collect();
    for i in order {
        if have >= k || shards[i].is_some() {
            continue;
        }
        let Some(n) = nodes.get(&group.members[i].node_id) else {
            continue;
        };
        let shard = ShardRef {
            group_id: group.id,
            chunk_id: cid,
            shard_index: u16::try_from(i).unwrap_or(0),
        };
        if let Ok(b) = clients.get_shard(&n.address, shard, 0, 0).await {
            shards[i] = Some(b.to_vec());
            have += 1;
        }
    }
    let (prefix, chunk_index) = split_chunk_id(&cid);
    if have < k {
        if !gc::generation_is_live(meta, &prefix).await? {
            return Ok(None);
        }
        return Err(ControlError::Unrecoverable {
            group: group.id,
            chunk: hex(&cid),
            have,
            need: k,
        });
    }
    coder
        .reconstruct(&mut shards)
        .map_err(|e| ControlError::Other(format!("reconstruct: {e}")))?;

    // Validate against metadata when the chunk is recorded (a pending
    // generation may not have appended its record yet).
    if let Some(record) = gc::chunk_record_in_group(meta, &prefix, chunk_index, group.id).await? {
        if record.scheme != group.scheme
            || record.data_shards != group.data_shards
            || record.parity_shards != group.parity_shards
        {
            return Err(ControlError::Corrupt(format!(
                "chunk {} recorded as {} {}+{} but group {} is {} {}+{}",
                hex(&cid),
                record.scheme,
                record.data_shards,
                record.parity_shards,
                group.id,
                group.scheme,
                group.data_shards,
                group.parity_shards
            )));
        }
        let layout = StripeLayout::new(
            usize::try_from(record.len).unwrap_or(0),
            k,
            coder.shard_alignment(),
        );
        let data: Vec<&[u8]> = shards[..k]
            .iter()
            .map(|s| s.as_deref().unwrap_or(&[]))
            .collect();
        let joined = layout.join(&data);
        if arca_lake_storage::format::crc32c(&joined) != record.crc32c {
            return Err(ControlError::Corrupt(format!(
                "chunk {} reconstructed from {} shards does not match its recorded checksum",
                hex(&cid),
                have
            )));
        }
    }
    Ok(Some(Bytes::from(shards[index].take().unwrap_or_default())))
}

/// Completeness check: walk every member's listing (same chunk IDs, same
/// payload lengths), verify the replacement's shards (and every member's
/// when `verify_all_members`).
async fn check_completeness(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    index: usize,
    opts: &RepairOptions,
) -> Result<Completeness, ControlError> {
    let mut c = Completeness::default();
    let mut walker = GroupWalker::new(clients, nodes, group, None, opts.page_size);
    while let Some(chunk) = walker.next().await? {
        c.chunks += 1;
        if chunk.present.iter().any(Option::is_none) {
            c.missing += 1;
        }
        let lens: BTreeSet<u64> = chunk.present.iter().flatten().copied().collect();
        if lens.len() > 1 {
            c.length_mismatch += 1;
        }
    }
    c.unavailable = walker.unavailable();
    let to_verify: Vec<usize> = if opts.verify_all_members {
        (0..group.members.len()).collect()
    } else {
        vec![index]
    };
    for i in to_verify {
        let Some(n) = nodes.get(&group.members[i].node_id) else {
            c.unavailable.push(i);
            continue;
        };
        let corrupt = corrupt_on_target(clients, &n.address, group, i, opts.page_size).await?;
        if !corrupt.is_empty() {
            c.corrupt.push((i, corrupt.len()));
        }
    }
    Ok(c)
}

/// Record progress; fails with [`ControlError::EpochChanged`] if the group
/// moved on (another membership change), so that this run stops writing.
async fn persist_cursor<S: KvStore>(
    meta: &S,
    group: &GroupRecord,
    index: usize,
    cid: [u8; 16],
) -> Result<(), ControlError> {
    let epoch = group.epoch;
    let member = u16::try_from(index).unwrap_or(u16::MAX);
    let after = cluster::update_group(meta, group.id, |g| {
        if g.epoch == epoch && g.state == GroupState::Repairing {
            g.repair_cursor = Some(RepairCursor {
                member,
                chunk_id: cid,
            });
            true
        } else {
            false
        }
    })
    .await?;
    if after.epoch != epoch {
        return Err(ControlError::EpochChanged {
            group: group.id,
            expected: epoch,
            found: after.epoch,
        });
    }
    Ok(())
}
