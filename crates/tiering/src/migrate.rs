//! Migration of a chunk's durable placement between tiers (RFC-006 §5.8,
//! DD-6): copy, verify, switch, retire. The source stays referenced until
//! the switch transaction commits; the source shards then become
//! unreferenced (their chunk record is placed elsewhere) and are deleted,
//! eagerly here or later by reconciliation.

use arca_lake_metadata::KvStore;
use arca_lake_metadata::records::{ChunkRecord, GenId, GroupRecord};
use arca_lake_protocol::client::concat_frames;
use arca_lake_protocol::client::{NodeClients, RecordToken, ShardRef};

use crate::TierError;
use crate::durable::{Coders, encode_chunk, read_chunk};

/// Why a migration was skipped when another migration of the chunk holds
/// its intent ([`MigrateOutcome::Skipped`]): nothing was read or written.
pub const IN_FLIGHT: &str = "another migration of the chunk is in flight";

/// Result of one chunk migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrateOutcome {
    /// The chunk record now names the target group.
    Switched {
        /// Old group.
        from: u64,
        /// New group.
        to: u64,
    },
    /// The record was already placed in the target tier.
    AlreadyThere,
    /// The record changed under us (another migration, or the generation
    /// is no longer committed); nothing was switched and the target shards
    /// were deleted.
    Skipped(&'static str),
    /// The tiering policy that decided the move changed or was disabled
    /// (ADR-0035 §10.1); nothing was switched and the target shards were
    /// deleted. Only [`migrate_chunk_for_tiering`] returns it.
    PolicyChanged,
}

impl MigrateOutcome {
    /// Skipped because another migration of the chunk is in flight: this
    /// attempt did nothing (a tiering pass must not reset the generation's
    /// access record on it, ADR-0035 §10.1).
    #[must_use]
    pub fn in_flight(&self) -> bool {
        matches!(self, Self::Skipped(why) if *why == IN_FLIGHT)
    }
}

/// Fault-injection points for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrateFault {
    /// Stop after the target shards are written and verified, before the
    /// switch transaction.
    BeforeSwitch,
    /// Stop after the switch, before the source shards are deleted.
    AfterSwitch,
}

/// Move chunk `index` of `generation` from its current durable group to
/// `target` (a durable group of another tier, same or different profile).
///
/// # Errors
///
/// Read errors from the source, write errors to the target, verification
/// failures (nothing is switched), metadata errors.
#[allow(clippy::too_many_arguments)]
pub async fn migrate_chunk<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    coders: &Coders,
    addr_of: &(dyn Fn(u64) -> Option<String> + Sync),
    source_group: &GroupRecord,
    target: &GroupRecord,
    generation: &GenId,
    index: u32,
    record: &ChunkRecord,
    fault: Option<MigrateFault>,
) -> Result<MigrateOutcome, TierError> {
    migrate_chunk_with(
        meta,
        clients,
        coders,
        addr_of,
        source_group,
        target,
        generation,
        index,
        record,
        fault,
        async || {},
    )
    .await
}

/// [`migrate_chunk`] calling `before_switch` once the target is written
/// and verified, right before the switch (tests interleave other work
/// there).
///
/// # Errors
///
/// As [`migrate_chunk`].
#[allow(clippy::too_many_arguments)]
pub async fn migrate_chunk_with<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    coders: &Coders,
    addr_of: &(dyn Fn(u64) -> Option<String> + Sync),
    source_group: &GroupRecord,
    target: &GroupRecord,
    generation: &GenId,
    index: u32,
    record: &ChunkRecord,
    fault: Option<MigrateFault>,
    before_switch: impl AsyncFnMut(),
) -> Result<MigrateOutcome, TierError> {
    migrate_inner(
        meta,
        clients,
        coders,
        addr_of,
        source_group,
        target,
        generation,
        index,
        record,
        fault,
        before_switch,
        None,
    )
    .await
}

/// [`migrate_chunk_with`] for a move a tiering pass decided under the
/// tiering record's `policy_generation` (ADR-0035 §10.1): the switch is
/// refused ([`MigrateOutcome::PolicyChanged`]) when the record moved on or
/// tiering was disabled.
///
/// # Errors
///
/// As [`migrate_chunk`].
#[allow(clippy::too_many_arguments)]
pub async fn migrate_chunk_for_tiering<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    coders: &Coders,
    addr_of: &(dyn Fn(u64) -> Option<String> + Sync),
    source_group: &GroupRecord,
    target: &GroupRecord,
    generation: &GenId,
    index: u32,
    record: &ChunkRecord,
    policy_generation: u64,
    before_switch: impl AsyncFnMut(),
) -> Result<MigrateOutcome, TierError> {
    migrate_inner(
        meta,
        clients,
        coders,
        addr_of,
        source_group,
        target,
        generation,
        index,
        record,
        None,
        before_switch,
        Some(policy_generation),
    )
    .await
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn migrate_inner<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    coders: &Coders,
    addr_of: &(dyn Fn(u64) -> Option<String> + Sync),
    source_group: &GroupRecord,
    target: &GroupRecord,
    generation: &GenId,
    index: u32,
    record: &ChunkRecord,
    fault: Option<MigrateFault>,
    mut before_switch: impl AsyncFnMut(),
    policy_generation: Option<u64>,
) -> Result<MigrateOutcome, TierError> {
    if record.placement.group_id == target.id {
        return Ok(MigrateOutcome::AlreadyThere);
    }
    // 0. The chunk's migration intent, before any target shard exists:
    //    reconciliation keeps the target's shards while it is held, and no
    //    second migration of the chunk writes the same shard keys (Phase 3.5
    //    audit CON-1, CON-2).
    let attempt = arca_lake_metadata::records::new_gen_id();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    if !arca_lake_metadata::ops::object::begin_migration(
        meta,
        &record.chunk_id,
        target.id,
        &attempt,
        now,
    )
    .await?
    {
        return Ok(MigrateOutcome::Skipped(IN_FLIGHT));
    }
    // 1. Read and verify the chunk from its current placement.
    let read = async {
        let coder = coders.get(&record.scheme, record.data_shards, record.parity_shards)?;
        read_chunk(clients, source_group, addr_of, record, coder.as_ref()).await
    };
    let bytes = match read.await {
        Ok(parts) => concat_frames(parts),
        Err(e) => {
            let _ =
                arca_lake_metadata::ops::object::end_migration(meta, &record.chunk_id, &attempt)
                    .await;
            return Err(e);
        }
    };

    // 2–3. Encode with the target's profile, write every shard, read back.
    let written = match write_and_verify(clients, coders, addr_of, target, record, &bytes).await {
        Ok(w) => w,
        Err((written, e)) => {
            // What was written goes, only while this attempt still owns the
            // intent with a margin (else a successor may have committed
            // these keys; reconciliation then decides).
            if owns(meta, &record.chunk_id, &attempt).await {
                cleanup(clients, &written, target.epoch).await;
            }
            let _ =
                arca_lake_metadata::ops::object::end_migration(meta, &record.chunk_id, &attempt)
                    .await;
            return Err(e);
        }
    };
    if fault == Some(MigrateFault::BeforeSwitch) {
        return Err(TierError::Other("injected stop before switch".into()));
    }
    before_switch().await;

    // 4. Switch: one transaction conditioned on the record we read.
    let expected_placement = record.placement;
    let new_record = ChunkRecord {
        scheme: target.scheme.clone(),
        data_shards: target.data_shards,
        parity_shards: target.parity_shards,
        placement: arca_lake_metadata::records::DurablePlacement {
            group_id: target.id,
            epoch: target.epoch,
        },
        ..record.clone()
    };
    let switched = arca_lake_metadata::ops::object::switch_placement_fenced(
        meta,
        generation,
        index,
        &expected_placement,
        &new_record,
        &attempt,
        policy_generation,
    )
    .await?;
    if switched != arca_lake_metadata::ops::object::Switch::Switched {
        // Delete the target only while this attempt still holds the intent:
        // a migration that lost it (stale) must not delete shards another
        // attempt now owns under the same keys.
        if owns(meta, &record.chunk_id, &attempt).await {
            cleanup(clients, &written, target.epoch).await;
            arca_lake_metadata::ops::object::end_migration(meta, &record.chunk_id, &attempt)
                .await?;
        }
        if switched == arca_lake_metadata::ops::object::Switch::PolicyChanged {
            return Ok(MigrateOutcome::PolicyChanged);
        }
        return Ok(MigrateOutcome::Skipped(
            "record changed, generation not committed, or intent lost",
        ));
    }
    if fault == Some(MigrateFault::AfterSwitch) {
        return Err(TierError::Other("injected stop after switch".into()));
    }

    // 5. The source shards are left to reconciliation, which deletes only
    //    the records it listed: a delete sent from here could arrive after
    //    a later migration put the chunk back into the source group and
    //    remove those shards (Phase 3.5 audit CON-5).
    Ok(MigrateOutcome::Switched {
        from: source_group.id,
        to: target.id,
    })
}

/// What a target write left behind when it failed, and why.
type Failed = (Vec<(String, ShardRef, RecordToken)>, TierError);

/// Encode `bytes` for `target`, write every shard with the target epoch,
/// and read each one back. On failure, the records written so far are
/// returned with the error, for the caller to delete.
async fn write_and_verify(
    clients: &NodeClients,
    coders: &Coders,
    addr_of: &(dyn Fn(u64) -> Option<String> + Sync),
    target: &GroupRecord,
    record: &ChunkRecord,
    bytes: &[u8],
) -> Result<Vec<(String, ShardRef, RecordToken)>, Failed> {
    let encode = || -> Result<Vec<bytes::Bytes>, TierError> {
        let target_coder = coders.get(&target.scheme, target.data_shards, target.parity_shards)?;
        let k = usize::from(target.data_shards);
        let (_, shards) = encode_chunk(target_coder.as_ref(), k, bytes)?;
        if shards.len() != target.members.len() {
            return Err(TierError::Other(format!(
                "target group {} has {} members for {} shards",
                target.id,
                target.members.len(),
                shards.len()
            )));
        }
        Ok(shards)
    };
    let shards = encode().map_err(|e| (Vec::new(), e))?;
    let mut written: Vec<(String, ShardRef, RecordToken)> = Vec::new();
    // Every way out after the first shard returns what was written, for
    // the caller to delete while it owns the intent (Phase 3.8 review: a
    // put or read-back error left them to reconciliation, using room on a
    // node possibly at its reserve).
    match write_all_and_verify(clients, addr_of, target, record, &shards, &mut written).await {
        Ok(()) => Ok(written),
        Err(e) => Err((written, e)),
    }
}

/// Margin before the intent's expiry within which an attempt no longer
/// deletes its target records.
const OWNERSHIP_MARGIN_MS: u64 = 10 * 60_000;

async fn owns<S: KvStore>(
    meta: &S,
    chunk_id: &[u8; 16],
    attempt: &arca_lake_metadata::records::GenId,
) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    arca_lake_metadata::ops::object::holds_migration_fresh(
        meta,
        chunk_id,
        attempt,
        now,
        OWNERSHIP_MARGIN_MS,
    )
    .await
    .unwrap_or(false)
}

async fn write_all_and_verify(
    clients: &NodeClients,
    addr_of: &(dyn Fn(u64) -> Option<String> + Sync),
    target: &GroupRecord,
    record: &ChunkRecord,
    shards: &[bytes::Bytes],
    written: &mut Vec<(String, ShardRef, RecordToken)>,
) -> Result<(), TierError> {
    for (i, member) in target.members.iter().enumerate() {
        let addr = addr_of(member.node_id)
            .ok_or_else(|| TierError::Other(format!("unknown node {}", member.node_id)))?;
        let shard = ShardRef {
            group_id: target.id,
            chunk_id: record.chunk_id,
            shard_index: u16::try_from(i).unwrap_or(0),
        };
        let expected = arca_lake_storage::format::crc32c(&shards[i]);
        let resp = clients
            .put_shard_as(
                &addr,
                shard,
                target.epoch,
                shards[i].clone(),
                256 << 10,
                arca_lake_protocol::storage::v1::WriteClass::Migration,
            )
            .await
            .map_err(|status| TierError::Node {
                addr: addr.clone(),
                status,
            })?;
        if resp.crc32c != expected {
            written.push((addr.clone(), shard, RecordToken::of_put(&resp)));
            return Err(TierError::Corrupt(format!(
                "target {addr} stored shard {i} with a different checksum"
            )));
        }
        written.push((addr, shard, RecordToken::of_put(&resp)));
    }
    for (i, (addr, shard, _)) in written.iter().enumerate() {
        let back = clients
            .get_shard(addr, *shard, 0, 0)
            .await
            .map_err(|status| TierError::Node {
                addr: addr.clone(),
                status,
            })?;
        if back != shards[i] {
            return Err(TierError::Corrupt(format!(
                "target {addr} returned different bytes for shard {i}"
            )));
        }
    }
    Ok(())
}

/// Delete the target shards this attempt wrote, each only while the node
/// still holds the record this attempt wrote (a later write under the same
/// key is left alone, Phase 3.5 audit CON-6).
async fn cleanup(clients: &NodeClients, written: &[(String, ShardRef, RecordToken)], epoch: u64) {
    for (addr, shard, sequence) in written {
        // 0: an older node, where the delete would be unconditional
        // (CON-12): left to reconciliation after the node's upgrade.
        if sequence.is_some() {
            let _ = clients
                .delete_shard_if(addr, *shard, epoch, *sequence)
                .await;
        }
    }
}
