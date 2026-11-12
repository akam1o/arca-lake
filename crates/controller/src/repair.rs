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
use arca_lake_metadata::ops::{cluster, epoch_intent, gc};
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
    /// Reconstruct at most this many chunks per call (0 = unlimited); when
    /// the budget is spent the cursor is persisted, the completeness check
    /// is skipped, and the group stays `Repairing` (RFC-006 §5.9).
    pub max_chunks: usize,
    /// Nodes that held the member being rebuilt and may still answer (the
    /// node a replacement request or a drain moves the member off): a
    /// source of its own shard of each chunk (Phase 3.4 §4, §10).
    pub former: Vec<u64>,
    /// How shards are obtained when a former member answers.
    pub source: RepairSource,
    /// A planned move (the leaving member still answers, so redundancy is
    /// whole): its writes keep the target's client reserve (`Migration`
    /// class at the node's admission), unlike a failure repair, which may
    /// use the space down to the repair floor (Phase 3.11, M3).
    pub keep_client_reserve: bool,
}

/// Where a rebuilt shard comes from (Phase 3.4 §10).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RepairSource {
    /// EC reconstruction from `k` current members, validated against the
    /// chunk's metadata checksum; the former member's shard is used only
    /// when fewer than `k` current members have a good shard (then it is
    /// one of the `k` inputs and the same validation applies).
    #[default]
    Reconstruct,
    /// Copy the former member's shard (verified by its node's block
    /// checksums when read, and by the target's CRC when written); EC
    /// reconstruction when it does not answer.
    Direct,
}

/// Wall clock, ms since the epoch.
fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Result of a repair.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct RepairReport {
    /// Chunks this walk could not rebuild, queued for stripe repair
    /// (Phase 3.12).
    pub unrebuilt: usize,
    /// Epoch of the group during this repair.
    pub epoch: u64,
    /// Chunks whose shard was reconstructed and written.
    pub reconstructed: usize,
    /// Chunks already present (and verified) on the replacement.
    pub skipped: usize,
    /// Whether this run continued an interrupted repair.
    pub resumed: bool,
    /// Whether this run stopped because `max_chunks` was reached.
    pub budget_exhausted: bool,
    /// Shards copied from the former member as they were.
    pub copied_direct: usize,
    /// Reconstructions that needed the former member's shard (fewer than
    /// `k` current members had a good one).
    pub used_former: usize,
    /// Whether the group is `Active` again.
    pub complete: bool,
    /// The completeness check found the rebuilt slot itself whole: every
    /// chunk placed in the group listed on the target, none of its shards
    /// corrupt, none queued for stripe repair, the target answering
    /// (Phase 3.13: only then may a second failed member be replaced; its
    /// shards are no longer needed for any chunk to have `k`).
    pub slot_whole: bool,
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
    /// Of `missing`, chunks missing (or retired) on the member rebuilt.
    pub missing_on_target: usize,
    /// Chunks whose shards have different payload lengths across members.
    pub length_mismatch: usize,
    /// Shards that failed verification, per member index.
    pub corrupt: Vec<(usize, usize)>,
    /// Members that did not answer.
    pub unavailable: Vec<usize>,
    /// Chunks missing on members other than the one rebuilt, with those
    /// members (the move hands them to stripe repair, bounded per check).
    pub missing_elsewhere: Vec<([u8; 16], Vec<u16>)>,
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
#[allow(clippy::too_many_lines)]
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
        let expected_epoch = group.epoch;
        // The intent first (Phase 3.11, M2): a crash once a node holds the
        // new epoch leaves a record any leader resumes or settles.
        let member = u16::try_from(index).unwrap_or(u16::MAX);
        let intent = epoch_intent::EpochIntent {
            group: group_id,
            from_epoch: expected_epoch,
            index: member,
            replacement,
            since_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(0)),
        };
        match epoch_intent::begin(meta, &intent).await? {
            epoch_intent::Begin::Recorded | epoch_intent::Begin::Same => {}
            epoch_intent::Begin::Other(o) => {
                return Err(ControlError::Other(format!(
                    "group {group_id} has an open epoch change (member {} to node {}) from epoch {}: it is resumed or settled first",
                    o.index, o.replacement.node_id, o.from_epoch
                )));
            }
            epoch_intent::Begin::Stale { found } => {
                return Err(ControlError::EpochChanged {
                    group: group_id,
                    expected: expected_epoch,
                    found,
                });
            }
        }
        fence(clients, &nodes, &group, replacement, new_epoch).await?;
        hook(RepairStep::Fenced).await?;
        let updated = epoch_intent::commit(meta, group_id, expected_epoch, |g| {
            g.members[index] = replacement;
            g.state = GroupState::Repairing;
            // The cursor names the member being rebuilt from the start, so
            // that any controller can resume the repair (RFC-011 §5.5).
            g.repair_cursor = Some(RepairCursor {
                member,
                chunk_id: [0; 16],
            });
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

    // An unprotected cache group (one copy, `cache-v1`) has nothing to
    // rebuild from: the new member starts empty, reads invalidate the
    // placements that pointed at the old copy and refill them from the
    // durable tier (RFC-006 §5.9). The membership change is the repair.
    if group.is_cache() && group.parity_shards == 0 && group.members.len() == 1 {
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
        return Ok(RepairReport {
            epoch: after.epoch,
            resumed,
            complete: after.state == GroupState::Active,
            ..RepairReport::default()
        });
    }
    let mut report =
        reconstruct_member(meta, clients, &nodes, &group, index, opts, &mut hook).await?;
    report.resumed = resumed;
    if report.budget_exhausted {
        return Ok(report);
    }
    hook(RepairStep::Reconstructed).await?;

    let completeness = check_completeness(meta, clients, &nodes, &group, index, opts).await?;
    // A chunk a walk could not rebuild for this member is queued for
    // stripe repair (above); while its task is open the member is not
    // whole, whatever the listing shows (Phase 3.12: shards a node
    // quarantined are not listed, so a later walk and this check would not
    // see the chunk at all, and the group would become Active without it).
    let member = u16::try_from(index).unwrap_or(u16::MAX);
    let mut queued_for_member = false;
    if let Some(target) = nodes.get(&group.members[index].node_id) {
        for (cid, t) in arca_lake_metadata::ops::integrity::tasks_of_group(meta, group.id).await? {
            if !t.hint_targets.contains(&member) {
                continue;
            }
            let shard = ShardRef {
                group_id: group.id,
                chunk_id: cid,
                shard_index: member,
            };
            // Still not on the new member (a task of a shard it holds
            // now is stripe repair's to close).
            if clients
                .get_shard(&target.address, shard, 0, 1)
                .await
                .is_err()
            {
                queued_for_member = true;
                break;
            }
        }
    }
    let complete = completeness.is_complete() && !queued_for_member;
    report.slot_whole = completeness.missing_on_target == 0
        && completeness.corrupt.is_empty()
        && !completeness.unavailable.contains(&index)
        && !queued_for_member;
    // Shards missing on the other members (a corrupt one quarantined when
    // the move read it, a member that lost one) are stripe repair's work:
    // queue it, so that the next check can pass (otherwise the group would
    // wait for a repair nobody started).
    for (cid, members) in &completeness.missing_elsewhere {
        arca_lake_metadata::ops::integrity::enqueue_or_wake(
            meta,
            group.id,
            *cid,
            arca_lake_metadata::ops::integrity::Finding::MissingShard,
            members,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(0)),
        )
        .await?;
    }
    if !complete {
        // The next run must walk from the start again: the cursor only
        // records how far reconstruction got, and the check found something
        // (a corrupt or missing shard, an unreachable member) that a resumed
        // walk from the cursor would never revisit.
        // Reset to the start of the same member (`chunk_id` zero), not to
        // no cursor: the resume path needs the member index, and a group
        // Repairing without a cursor waited for an operator forever.
        let epoch = group.epoch;
        let member = u16::try_from(index).unwrap_or(u16::MAX);
        cluster::update_group(meta, group_id, |g| {
            if g.epoch == epoch && g.state == GroupState::Repairing {
                g.repair_cursor = Some(RepairCursor {
                    member,
                    chunk_id: [0; 16],
                });
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

#[allow(clippy::too_many_lines)]
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
    let mut first_unrebuilt: Option<ControlError> = None;
    while let Some(chunk) = walker.next().await? {
        let cid = chunk.chunk_id;
        // The target's record counts only if not retired (ADR-0028 H4,
        // Phase 3.12 review): a retired one is written again.
        if chunk.present[index].is_some() && !chunk.retired[index] && !corrupt.contains(&cid) {
            report.skipped += 1;
            persist_cursor(meta, group, index, cid).await?;
            continue;
        }
        // Members whose listing failed in this walk are not asked for
        // shards either (Phase 3.13: with a member down, every chunk's read
        // of it waited for the RPC retries, and a walk took hours).
        let down = walker.unavailable();
        let rebuilt = match reconstruct_one(
            meta,
            clients,
            nodes,
            group,
            index,
            &chunk,
            coder.as_ref(),
            params,
            opts,
            &down,
        )
        .await
        {
            Ok(r) => r,
            // A chunk this walk cannot rebuild (fewer than `k` shards, or
            // what they give does not match its record) goes to the stripe
            // repair queue, where it is retried with backoff and shown to
            // the operator, and the walk goes on past it (Phase 3.12
            // liveness audit: it was retried first on every pass, and the
            // rest of the group waited behind it). The walk still ends in
            // this error (below): the group stays `Repairing`, whatever
            // completeness sees (shards a node hides are not listed).
            Err(e @ (ControlError::Unrecoverable { .. } | ControlError::Corrupt(_))) => {
                tracing::error!(group = group.id, chunk = %hex(&cid), error = %e, "member repair: chunk not rebuilt, queued for stripe repair");
                arca_lake_metadata::ops::integrity::enqueue(
                    meta,
                    group.id,
                    cid,
                    arca_lake_metadata::ops::integrity::Finding::MissingShard,
                    &[u16::try_from(index).unwrap_or(0)],
                    now_unix_ms(),
                )
                .await?;
                report.unrebuilt += 1;
                first_unrebuilt.get_or_insert(e);
                persist_cursor(meta, group, index, cid).await?;
                continue;
            }
            Err(e) => return Err(e),
        };
        let Some((bytes, how)) = rebuilt else {
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
            .put_shard_as(
                &target_addr,
                shard,
                group.epoch,
                bytes,
                256 << 10,
                // A cache group's copies are written as cache (Phase 3.12
                // review): the node learns its role from the class, and a
                // repair write would make it durable for good, its
                // evictions then retained for the reclaim retention. A
                // discardable copy needs no reserve either.
                if group.is_cache() {
                    arca_lake_protocol::storage::v1::WriteClass::Cache
                } else if opts.keep_client_reserve {
                    arca_lake_protocol::storage::v1::WriteClass::Migration
                } else {
                    arca_lake_protocol::storage::v1::WriteClass::Repair
                },
            )
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
        match how {
            Source::Direct => report.copied_direct += 1,
            Source::WithFormer => report.used_former += 1,
            Source::Members => {}
        }
        let index_in_run = report.reconstructed;
        report.reconstructed += 1;
        hook(RepairStep::Chunk {
            index: index_in_run,
            chunk_id: cid,
        })
        .await?;
        if opts.max_chunks > 0 && report.reconstructed >= opts.max_chunks {
            report.budget_exhausted = true;
            return Ok(report);
        }
    }
    if let Some(e) = first_unrebuilt {
        // Its task stays queued: completeness counts the member incomplete
        // while it is (above), whatever later walks see.
        return Err(e);
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
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn reconstruct_one<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    index: usize,
    chunk: &ChunkPresence,
    coder: &dyn ErasureCoder,
    params: CodeParams,
    opts: &RepairOptions,
    down: &[usize],
) -> Result<Option<(Bytes, Source)>, ControlError> {
    let cid = chunk.chunk_id;
    let k = params.data;
    let own = ShardRef {
        group_id: group.id,
        chunk_id: cid,
        shard_index: u16::try_from(index).unwrap_or(0),
    };
    // The former member's copy of the shard being rebuilt, if it answers
    // (its node verifies the block checksums on the way out).
    let former_shard = async || -> Option<Vec<u8>> {
        for id in &opts.former {
            if group.members.iter().any(|m| m.node_id == *id) {
                continue;
            }
            if let Some(n) = nodes.get(id)
                && let Ok(b) = clients.get_shard(&n.address, own, 0, 0).await
            {
                return Some(b.to_vec());
            }
        }
        None
    };
    if opts.source == RepairSource::Direct && !group.is_cache() {
        let (prefix, chunk_index) = split_chunk_id(&cid);
        // Only for a chunk of a live generation recorded in this group, and
        // only a shard of the length its layout gives.
        if let Some(record) =
            gc::chunk_record_in_group(meta, &prefix, chunk_index, group.id).await?
            && let Some(b) = former_shard().await
        {
            let layout = StripeLayout::new(
                usize::try_from(record.len).unwrap_or(0),
                k,
                coder.shard_alignment(),
            );
            if b.len() == layout.shard_len() {
                return Ok(Some((Bytes::from(b), Source::Direct)));
            }
        }
    }
    let mut shards: Vec<Option<Vec<u8>>> = vec![None; params.total()];
    let mut have = 0;
    // Prefer members that list the shard; try the others too (the listing
    // may be stale).
    let order: Vec<usize> = chunk
        .holders()
        .chain(0..group.members.len())
        .filter(|&i| i != index && !down.contains(&i))
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
    if group.is_cache() {
        // A cache shard is worth rebuilding only while a live generation's
        // cache placement names this group; otherwise it is an orphan that
        // reconciliation deletes. An unreadable cached chunk is dropped
        // (the record is removed; the next read refills it): cache repair
        // never fails on data that the durable tier still has.
        if !gc::cache_placement_in_group(meta, &prefix, chunk_index, group.id).await? {
            return Ok(None);
        }
        if have < k {
            if let Some(generation) = gc::live_generation_with_prefix(meta, &prefix).await? {
                arca_lake_metadata::ops::cache::remove(meta, &generation, chunk_index).await?;
            }
            return Ok(None);
        }
        coder
            .reconstruct(&mut shards)
            .map_err(|e| ControlError::Other(format!("reconstruct: {e}")))?;
        if let Some(record) = gc::chunk_record_of(meta, &prefix, chunk_index).await?
            && !joined_matches(&shards, k, coder, record.len, record.crc32c)
        {
            return Err(ControlError::Corrupt(format!(
                "cached chunk {} reconstructed from {} shards does not match its record",
                hex(&cid),
                have
            )));
        }
        return Ok(Some((
            Bytes::from(shards[index].take().unwrap_or_default()),
            Source::Members,
        )));
    }
    let mut how = Source::Members;
    if have < k
        && let Some(b) = former_shard().await
    {
        // One of the `k` inputs; the reconstruction below is validated
        // against the recorded checksum like any other.
        shards[index] = Some(b);
        have += 1;
        how = Source::WithFormer;
    }
    if have < k {
        // Only a chunk a live generation places in this group needs this
        // shard. Anything else is an orphan reconciliation deletes: a
        // retired generation's, or the leftover of a write that failed in
        // this group and was placed in another one (Phase 3.10 audit: a
        // member move stayed `Repairing` on such a leftover, retrying it as
        // unrecoverable, while the object was whole in the other group).
        if !gc::chunk_may_be_in_group(meta, &prefix, chunk_index, group.id).await? {
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
        if !joined_matches(&shards, k, coder, record.len, record.crc32c) {
            return Err(ControlError::Corrupt(format!(
                "chunk {} reconstructed from {} shards does not match its recorded checksum",
                hex(&cid),
                have
            )));
        }
    } else if how == Source::WithFormer {
        // Without a record there is nothing to validate the former copy
        // against: do not write it.
        return Err(ControlError::Unrecoverable {
            group: group.id,
            chunk: hex(&cid),
            have: have - 1,
            need: k,
        });
    }
    Ok(Some((
        Bytes::from(shards[index].take().unwrap_or_default()),
        how,
    )))
}

/// How a shard was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// Reconstructed from current members.
    Members,
    /// Reconstructed with the former member's shard as one input.
    WithFormer,
    /// Copied from the former member.
    Direct,
}

/// Whether the joined data shards have the recorded length and CRC.
fn joined_matches(
    shards: &[Option<Vec<u8>>],
    k: usize,
    coder: &dyn ErasureCoder,
    len: u32,
    crc32c: u32,
) -> bool {
    let layout = StripeLayout::new(
        usize::try_from(len).unwrap_or(0),
        k,
        coder.shard_alignment(),
    );
    let data: Vec<&[u8]> = shards[..k]
        .iter()
        .map(|s| s.as_deref().unwrap_or(&[]))
        .collect();
    let joined = layout.join(&data);
    arca_lake_storage::format::crc32c(&joined) == crc32c
}

/// Whether every shard of slot `index` that one of `former` (nodes that
/// held the slot, answering) still lists, of a chunk placed in the group,
/// is on the slot's member (Phase 3.13 re-review: a chunk no current
/// member lists is invisible to the members' walk, but a former node may
/// hold it). `false` also when a listing fails.
///
/// # Errors
///
/// Metadata errors.
#[allow(clippy::implicit_hasher)]
pub async fn former_shards_on_target<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    index: usize,
    former: &[u64],
) -> Result<bool, ControlError> {
    let Some(target) = group.members.get(index).and_then(|m| nodes.get(&m.node_id)) else {
        return Ok(false);
    };
    let slot = u16::try_from(index).unwrap_or(u16::MAX);
    let Ok(on_target) = clients.list_shards(&target.address, group.id).await else {
        return Ok(false);
    };
    let on_target: BTreeSet<[u8; 16]> = on_target
        .iter()
        .filter(|s| s.shard_index == slot)
        .map(|s| s.chunk_id)
        .collect();
    for id in former {
        if group.members.iter().any(|m| m.node_id == *id) {
            continue;
        }
        let Some(n) = nodes.get(id) else { continue };
        let Ok(held) = clients.list_shards(&n.address, group.id).await else {
            return Ok(false);
        };
        for s in held.iter().filter(|s| s.shard_index == slot) {
            if !on_target.contains(&s.chunk_id) && placed_here(meta, group, &s.chunk_id).await? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Completeness check: walk every member's listing (same chunk IDs, same
/// payload lengths), verify the replacement's shards (and every member's
/// when `verify_all_members`).
/// Whether a live generation places chunk `cid` in `group`, or may yet
/// (a pending write whose record is not appended yet): its durable chunk
/// record, or for a cache group its cache placement.
async fn placed_here<S: KvStore>(
    meta: &S,
    group: &GroupRecord,
    cid: &[u8; 16],
) -> Result<bool, ControlError> {
    let (prefix, chunk_index) = split_chunk_id(cid);
    Ok(if group.is_cache() {
        gc::cache_placement_in_group(meta, &prefix, chunk_index, group.id).await?
    } else {
        gc::chunk_may_be_in_group(meta, &prefix, chunk_index, group.id).await?
    })
}

async fn check_completeness<S: KvStore>(
    meta: &S,
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
        // A chunk that no live generation places in this group is an
        // orphan (a retired generation's, or the leftover of a write that
        // failed here and was placed elsewhere): reconciliation's, not a
        // gap of the move (Phase 3.10 audit: such a leftover held a move
        // `Repairing` until reconciliation removed it).
        // The target's record counts only if not retired (ADR-0028 H4);
        // the other members' retired records are still theirs (after a
        // restore they are exactly what the restored metadata needs).
        let target_retired = chunk.retired.get(index).copied().unwrap_or(false);
        if (chunk.present.iter().any(Option::is_none) || target_retired)
            && placed_here(meta, group, &chunk.chunk_id).await?
        {
            c.missing += 1;
            if target_retired || chunk.present.get(index).is_none_or(Option::is_none) {
                c.missing_on_target += 1;
            }
            let others: Vec<u16> = chunk
                .present
                .iter()
                .enumerate()
                .filter(|(i, p)| *i != index && p.is_none())
                .map(|(i, _)| u16::try_from(i).unwrap_or(u16::MAX))
                .collect();
            if !others.is_empty() && c.missing_elsewhere.len() < 1000 {
                c.missing_elsewhere.push((chunk.chunk_id, others));
            }
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
