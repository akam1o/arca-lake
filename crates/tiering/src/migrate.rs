//! Migration of a chunk's durable placement between tiers (RFC-006 §5.8,
//! DD-6): copy, verify, switch, retire. The source stays referenced until
//! the switch transaction commits; the source shards then become
//! unreferenced (their chunk record is placed elsewhere) and are deleted,
//! eagerly here or later by reconciliation.

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::records::{ChunkRecord, GenId, GroupRecord};
use arca_lake_protocol::client::{NodeClients, ShardRef};

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
    if record.placement.group_id == target.id {
        return Ok(MigrateOutcome::AlreadyThere);
    }
    // 1. Read and verify the chunk from its current placement.
    let coder = coders.get(&record.scheme, record.data_shards, record.parity_shards)?;
    let bytes = read_chunk(clients, source_group, addr_of, record, coder.as_ref()).await?;

    // 2–3. Encode with the target's profile, write every shard, read back.
    let written = write_and_verify(clients, coders, addr_of, target, record, &bytes).await?;
    if fault == Some(MigrateFault::BeforeSwitch) {
        return Err(TierError::Other("injected stop before switch".into()));
    }

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
    )
    .await?;
    if !switched {
        cleanup(clients, &written, target.epoch).await;
        return Ok(MigrateOutcome::Skipped(
            "record changed or generation not committed",
        ));
    }
    if fault == Some(MigrateFault::AfterSwitch) {
        return Err(TierError::Other("injected stop after switch".into()));
    }

    // 5. Retire the source shards (best effort; reconciliation finishes).
    let source = cluster::get_group(meta, source_group.id).await?;
    for (i, member) in source.members.iter().enumerate() {
        if let Some(addr) = addr_of(member.node_id) {
            let shard = ShardRef {
                group_id: source.id,
                chunk_id: record.chunk_id,
                shard_index: u16::try_from(i).unwrap_or(0),
            };
            let _ = clients.delete_shard(&addr, shard, source.epoch).await;
        }
    }
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
) -> Result<Vec<(String, ShardRef)>, TierError> {
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
    let mut written: Vec<(String, ShardRef)> = Vec::new();
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
            .put_shard(&addr, shard, target.epoch, shards[i].clone(), 256 << 10)
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
        written.push((addr, shard));
    }
    for (i, (addr, shard)) in written.iter().enumerate() {
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

async fn cleanup(clients: &NodeClients, written: &[(String, ShardRef)], epoch: u64) {
    for (addr, shard) in written {
        let _ = clients.delete_shard(addr, *shard, epoch).await;
    }
}
