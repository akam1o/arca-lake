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
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
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
    mut before_switch: impl AsyncFnMut(),
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
        return Ok(MigrateOutcome::Skipped(
            "another migration of the chunk is in flight",
        ));
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
        Err(e) => {
            // Whatever was written is deleted by `write_and_verify`.
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
    let switched = arca_lake_metadata::ops::object::switch_placement(
        meta,
        generation,
        index,
        &expected_placement,
        &new_record,
        &attempt,
    )
    .await?;
    if !switched {
        // Delete the target only while this attempt still holds the intent:
        // a migration that lost it (stale) must not delete shards another
        // attempt now owns under the same keys.
        if arca_lake_metadata::ops::object::holds_migration(meta, &record.chunk_id, &attempt)
            .await?
        {
            cleanup(clients, &written, target.epoch).await;
            arca_lake_metadata::ops::object::end_migration(meta, &record.chunk_id, &attempt)
                .await?;
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

/// Encode `bytes` for `target`, write every shard with the target epoch,
/// and read each one back; on any mismatch delete what was written.
async fn write_and_verify(
    clients: &NodeClients,
    coders: &Coders,
    addr_of: &(dyn Fn(u64) -> Option<String> + Sync),
    target: &GroupRecord,
    record: &ChunkRecord,
    bytes: &[u8],
) -> Result<Vec<(String, ShardRef, RecordToken)>, TierError> {
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
    let mut written: Vec<(String, ShardRef, RecordToken)> = Vec::new();
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
            cleanup(clients, &written, target.epoch).await;
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
            cleanup(clients, &written, target.epoch).await;
            return Err(TierError::Corrupt(format!(
                "target {addr} returned different bytes for shard {i}"
            )));
        }
    }
    Ok(written)
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
