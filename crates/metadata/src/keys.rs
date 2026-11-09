//! Key layout, version 1.
//!
//! All keys are FoundationDB tuples (order-preserving encoding, via the
//! `foundationdb-tuple` crate) rooted at the string `"al"`. Subspaces:
//!
//! | Tuple | Value | Purpose |
//! | --- | --- | --- |
//! | `("al","meta","schema")` | u64 LE | metadata schema version (RFC-014 §5.4) |
//! | `("al","meta","schema_write")` | u64 LE | schema writers may encode (RFC-014 §5.4) |
//! | `("al","meta","format_write")` | u64 LE | storage format nodes may write (RFC-014 §5.3) |
//! | `("al","meta","cluster")` | `ClusterStateRecord` | recovery state and incarnation (RFC-015 §5.3) |
//! | `("al","meta","upgrade")` | `UpgradeCursor` | finalize in progress (RFC-014 §5.5) |
//! | `("al","meta","restore","cursor")` | `RestoreCursor` | restore verification progress (RFC-015 §5.3.2) |
//! | `("al","meta","restore","report",incarnation)` | `RestoreReport` | verification report per incarnation |
//! | `("al","proc",id)` | `ProcRecord` | gateway/controller heartbeat with capabilities (RFC-014 §5.5) |
//! | `("al","ctr","bucket")` | u64 LE | bucket ID allocator |
//! | `("al","b",name)` | `BucketRecord` | bucket by name |
//! | `("al","bi",bucket_id)` | `BucketRecord` | bucket by ID |
//! | `("al","o",bucket_id,key)` | `CurrentPointer` | current version of an object key |
//! | `("al","v",bucket_id,key,ordinal_desc)` | `VersionRecord` | version history, newest first |
//! | `("al","g",gen,"h")` | `GenerationHeader` | one immutable generation |
//! | `("al","g",gen,"c",index)` | `ChunkRecord` | chunk manifest + placement, one key per chunk |
//! | `("al","g",root,"m",part_number)` | `PartRef` | part manifest reference of a multipart root (RFC-007 §5.3) |
//! | `("al","mpu",bucket_id,key,upload_id)` | `MultipartRecord` | multipart upload |
//! | `("al","mpp",upload_id,part_number)` | `PartRecord` | sealed part of an upload |
//! | `("al","scrub",group_id)` | `ScrubCursor` | scrub progress of a group (RFC-008 §8.3) |
//! | `("al","repair",group_id,chunk_id)` | `RepairTask` | stripe repair task (RFC-008 §8.5) |
//! | `("al","pend",lease_until_ms,gen)` | empty | pending generations by lease expiry |
//! | `("al","gc",since_ms,gen)` | empty | retired generations awaiting GC |
//!
//! A generation's header and chunk records share a prefix so that one range
//! read fetches everything a GET needs. Object keys of one bucket share a
//! prefix so that listing is a range read, and the string encoding of the
//! tuple layer lets a key prefix be turned into a key range.

use foundationdb_tuple::{Bytes, pack, unpack};

use crate::kv::{Key, KeyRange, strinc};
use crate::records::{GenId, UploadId};

const ROOT: &str = "al";

/// Metadata schema version of this build (RFC-014 §5.4): 1 = Phase 0–2
/// layout (record version 1); 2 = Phase 3.1 (record version 2, multipart
/// and cluster-state keys); 3 = Phase 3.10, the version-2 layout in a
/// cluster whose deletes are fenced (ADR-0023, ADR-0024): once `upgrade
/// finalize` raised it, no release without the delete fence (protocol
/// below 3) and no release without the rejoin fence starts a controller or
/// gateway on the store. 4 exists only in a `version-bump` build.
pub const SCHEMA_VERSION: u64 = 3 + arca_lake_common::compat::BUMP;

/// Oldest schema version this build serves (`M_cur − 1`, RFC-014 §5.4).
pub const SCHEMA_VERSION_MIN: u64 = SCHEMA_VERSION - 1;

/// Key holding the schema writers may encode (RFC-014 §5.4).
#[must_use]
pub fn schema_write() -> Key {
    pack(&(ROOT, "meta", "schema_write"))
}

/// Key holding the storage format nodes may write (RFC-014 §5.3).
#[must_use]
pub fn format_write() -> Key {
    pack(&(ROOT, "meta", "format_write"))
}

/// The upgrade finalize cursor (RFC-014 §5.5).
#[must_use]
pub fn upgrade_cursor() -> Key {
    pack(&(ROOT, "meta", "upgrade"))
}

/// Audit record of a finalized upgrade (`UpgradeCursor` as it ended).
#[must_use]
pub fn upgrade_audit(finished_ms: u64) -> Key {
    pack(&(ROOT, "meta", "upgrade_log", finished_ms))
}

/// Every upgrade audit record.
#[must_use]
pub fn upgrade_audits_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "meta", "upgrade_log")))
}

/// Restore verification cursor (RFC-015 §5.3.2).
#[must_use]
pub fn restore_cursor() -> Key {
    pack(&(ROOT, "meta", "restore", "cursor"))
}

/// Restore verification report of one incarnation.
#[must_use]
pub fn restore_report(incarnation: &[u8; 16]) -> Key {
    pack(&(
        ROOT,
        "meta",
        "restore",
        "report",
        Bytes::from(&incarnation[..]),
    ))
}

/// Every restore report.
#[must_use]
pub fn restore_reports_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "meta", "restore", "report")))
}

/// Audit record of one `admin recovery finalize` attempt (accepted or
/// refused) for an incarnation; `id` is unique per attempt (attempts in
/// the same millisecond must not overwrite each other).
#[must_use]
pub fn restore_finalize_audit(incarnation: &[u8; 16], at_ms: u64, id: &[u8; 16]) -> Key {
    pack(&(
        ROOT,
        "meta",
        "restore",
        "finalize",
        Bytes::from(&incarnation[..]),
        at_ms,
        Bytes::from(&id[..]),
    ))
}

/// Every finalize audit record.
#[must_use]
pub fn restore_finalize_audits_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "meta", "restore", "finalize")))
}

/// A data loss accepted by a forced finalize: one chunk of one object
/// version that could not be recovered (kept until the operator clears it).
#[must_use]
pub fn data_loss(generation: &GenId, chunk_index: u32) -> Key {
    pack(&(
        ROOT,
        "meta",
        "loss",
        Bytes::from(&generation[..]),
        chunk_index,
    ))
}

/// Every data-loss record.
#[must_use]
pub fn data_losses_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "meta", "loss")))
}

/// Heartbeat of a gateway or controller process (RFC-014 §5.5).
#[must_use]
pub fn proc(id: &str) -> Key {
    pack(&(ROOT, "proc", id))
}

/// Every process heartbeat.
#[must_use]
pub fn procs_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "proc")))
}

/// What a node reported in `Health` (RFC-014 §5.1), written by the
/// controller.
#[must_use]
pub fn node_caps(node_id: u64) -> Key {
    pack(&(ROOT, "ncap", node_id))
}

/// Every node capability record.
#[must_use]
pub fn node_caps_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "ncap")))
}

/// Every bucket-by-name record (existence probe of the gate).
#[must_use]
pub fn buckets_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "b")))
}

/// Every node record.
#[must_use]
pub fn nodes_range() -> KeyRange {
    KeyRange::prefix(&nodes_prefix())
}

/// Key holding the schema version (u64 little-endian).
#[must_use]
pub fn schema_version() -> Key {
    pack(&(ROOT, "meta", "schema"))
}

/// Key holding the cluster recovery state (RFC-015 §5.3).
#[must_use]
pub fn cluster_state() -> Key {
    pack(&(ROOT, "meta", "cluster"))
}

/// Scrub cursor of a group (RFC-008 §8.3).
#[must_use]
pub fn scrub_cursor(group_id: u64) -> Key {
    pack(&(ROOT, "scrub", group_id))
}

/// Every scrub cursor.
#[must_use]
pub fn scrub_cursors_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "scrub")))
}

/// Group id from a scrub cursor key, if it is one.
#[must_use]
pub fn scrub_group_from(key: &[u8]) -> Option<u64> {
    unpack::<(String, String, u64)>(key).ok().map(|t| t.2)
}

/// Lifecycle of a node (draining, decommissioned; Phase 3.3 §6).
#[must_use]
pub fn node_state(node_id: u64) -> Key {
    pack(&(ROOT, "nstate", node_id))
}

/// Every node lifecycle record.
#[must_use]
pub fn node_states_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "nstate")))
}

/// Operator request to replace a member of a group.
#[must_use]
pub fn replace_request(group_id: u64) -> Key {
    pack(&(ROOT, "replace", group_id))
}

/// Every replacement request.
#[must_use]
pub fn replace_requests_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "replace")))
}

/// Cursor of the multipart manifest scrub (RFC-008 §8.6, R-55).
#[must_use]
pub fn manifest_cursor() -> Key {
    pack(&(ROOT, "mscrub"))
}

/// Stripe repair task of `(group, chunk)` (RFC-008 §8.5).
#[must_use]
pub fn repair_task(group_id: u64, chunk_id: &[u8; 16]) -> Key {
    pack(&(ROOT, "repair", group_id, Bytes::from(&chunk_id[..])))
}

/// Every repair task, in `(group, chunk)` order.
#[must_use]
pub fn repair_tasks_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "repair")))
}

/// `(group, chunk)` from a repair task key, if it is one.
#[must_use]
pub fn repair_task_from(key: &[u8]) -> Option<(u64, [u8; 16])> {
    let (_, _, g, c): (String, String, u64, Bytes<'_>) = unpack(key).ok()?;
    Some((g, <[u8; 16]>::try_from(c.as_ref()).ok()?))
}

/// Multipart upload record.
#[must_use]
pub fn mpu(bucket_id: u64, key: &str, upload_id: &UploadId) -> Key {
    pack(&(ROOT, "mpu", bucket_id, key, Bytes::from(&upload_id[..])))
}

/// Upload records of a bucket whose object key starts with `prefix`.
#[must_use]
pub fn mpu_prefix_range(bucket_id: u64, prefix: &str) -> KeyRange {
    let mut begin = pack(&(ROOT, "mpu", bucket_id, prefix));
    begin.pop();
    let end = strinc(&begin);
    KeyRange::new(begin, end)
}

/// First key strictly after the upload record `(key, upload_id)`, or after
/// every upload of `key` when `upload_id` is `None`.
#[must_use]
pub fn mpu_after(bucket_id: u64, key: &str, upload_id: Option<&UploadId>) -> Key {
    match upload_id {
        Some(u) => {
            let mut k = mpu(bucket_id, key, u);
            k.push(0);
            k
        }
        None => KeyRange::prefix(&pack(&(ROOT, "mpu", bucket_id, key))).end,
    }
}

/// `(key, upload_id)` from an upload record key, if it is one.
#[must_use]
pub fn mpu_from(key: &[u8]) -> Option<(String, UploadId)> {
    let (_, _, _, k, u): (String, String, u64, String, Bytes<'_>) = unpack(key).ok()?;
    Some((k, <[u8; 16]>::try_from(u.as_ref()).ok()?))
}

/// Part record `part_number` of an upload.
#[must_use]
pub fn part(upload_id: &UploadId, part_number: u32) -> Key {
    pack(&(
        ROOT,
        "mpp",
        Bytes::from(&upload_id[..]),
        u64::from(part_number),
    ))
}

/// All part records of an upload, in part-number order.
#[must_use]
pub fn parts_range(upload_id: &UploadId) -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "mpp", Bytes::from(&upload_id[..]))))
}

/// Part records with part number strictly greater than `after`.
#[must_use]
pub fn parts_after(upload_id: &UploadId, after: u32) -> KeyRange {
    KeyRange::new(
        part(upload_id, after.saturating_add(1)),
        parts_range(upload_id).end,
    )
}

/// Part number from a part record key, if it is one.
#[must_use]
pub fn part_number_from(key: &[u8]) -> Option<u32> {
    let (_, _, _, n): (String, String, Bytes<'_>, u64) = unpack(key).ok()?;
    u32::try_from(n).ok()
}

/// Part manifest reference `part_number` of a multipart root.
#[must_use]
pub fn part_ref(root: &GenId, part_number: u32) -> Key {
    pack(&(
        ROOT,
        "g",
        Bytes::from(&root[..]),
        "m",
        u64::from(part_number),
    ))
}

/// All part manifest references of a root, in part-number order.
#[must_use]
pub fn part_refs_range(root: &GenId) -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "g", Bytes::from(&root[..]), "m")))
}

/// Part number from a part reference key, if it is one.
#[must_use]
pub fn part_ref_number_from(key: &[u8]) -> Option<u32> {
    let (_, _, _, tag, n): (String, String, Bytes<'_>, String, u64) = unpack(key).ok()?;
    (tag == "m").then(|| u32::try_from(n).ok()).flatten()
}

/// Bucket ID allocator.
#[must_use]
pub fn bucket_counter() -> Key {
    pack(&(ROOT, "ctr", "bucket"))
}

/// Bucket record by name.
#[must_use]
pub fn bucket_by_name(name: &str) -> Key {
    pack(&(ROOT, "b", name))
}

/// Bucket record by ID.
#[must_use]
pub fn bucket_by_id(bucket_id: u64) -> Key {
    pack(&(ROOT, "bi", bucket_id))
}

/// Current pointer of an object key.
#[must_use]
pub fn object_current(bucket_id: u64, key: &str) -> Key {
    pack(&(ROOT, "o", bucket_id, key))
}

/// Range of current pointers whose object key starts with `prefix`.
#[must_use]
pub fn object_prefix_range(bucket_id: u64, prefix: &str) -> KeyRange {
    // A packed string ends with its 0x00 terminator; dropping it yields the
    // encoded prefix of every string that starts with `prefix`.
    let mut begin = pack(&(ROOT, "o", bucket_id, prefix));
    begin.pop();
    let end = strinc(&begin);
    KeyRange::new(begin, end)
}

/// First key strictly after the current pointer of `key`.
#[must_use]
pub fn object_after(bucket_id: u64, key: &str) -> Key {
    let mut k = object_current(bucket_id, key);
    k.push(0);
    k
}

/// Object key from a current-pointer key, if it is one.
#[must_use]
pub fn object_key_from(key: &[u8]) -> Option<String> {
    unpack::<(String, String, u64, String)>(key)
        .ok()
        .map(|t| t.3)
}

/// Version record. `ordinal_desc` is `u64::MAX - ordinal` so that the
/// newest version sorts first.
#[must_use]
pub fn version(bucket_id: u64, key: &str, ordinal: u64) -> Key {
    pack(&(ROOT, "v", bucket_id, key, u64::MAX - ordinal))
}

/// Ordinal from a version record key, if it is one.
#[must_use]
pub fn version_ordinal_from(key: &[u8]) -> Option<u64> {
    unpack::<(String, String, u64, String, u64)>(key)
        .ok()
        .map(|t| u64::MAX - t.4)
}

/// All version records of an object key, newest first.
#[must_use]
pub fn versions_range(bucket_id: u64, key: &str) -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "v", bucket_id, key)))
}

/// Generation header.
#[must_use]
pub fn gen_header(generation: &GenId) -> Key {
    pack(&(ROOT, "g", Bytes::from(&generation[..]), "h"))
}

/// Chunk record `index` of a generation.
#[must_use]
pub fn gen_chunk(generation: &GenId, index: u32) -> Key {
    pack(&(
        ROOT,
        "g",
        Bytes::from(&generation[..]),
        "c",
        u64::from(index),
    ))
}

/// All chunk records of a generation, in index order.
#[must_use]
pub fn gen_chunks_range(generation: &GenId) -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "g", Bytes::from(&generation[..]), "c")))
}

/// Chunk records `indices` of a generation.
#[must_use]
pub fn gen_chunks_subrange(generation: &GenId, indices: std::ops::Range<u32>) -> KeyRange {
    KeyRange::new(
        gen_chunk(generation, indices.start),
        gen_chunk(generation, indices.end),
    )
}

/// Cache placement of chunk `index` of a generation (inside the
/// generation's subspace, so it is cleared with the generation).
#[must_use]
pub fn cache_placement(generation: &GenId, index: u32) -> Key {
    pack(&(
        ROOT,
        "g",
        Bytes::from(&generation[..]),
        "x",
        u64::from(index),
    ))
}

/// Delete tokens of the cache copy of chunk `index` (R-73; inside the
/// generation's subspace, outside the placements' range).
#[must_use]
pub fn cache_tokens(generation: &GenId, index: u32) -> Key {
    pack(&(
        ROOT,
        "g",
        Bytes::from(&generation[..]),
        "xt",
        u64::from(index),
    ))
}

/// All cache placements of a generation, in index order.
#[must_use]
pub fn cache_placements_range(generation: &GenId) -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "g", Bytes::from(&generation[..]), "x")))
}

/// Chunk index from a cache-placement key, if it is one.
#[must_use]
pub fn cache_index_from(key: &[u8]) -> Option<u32> {
    let (_, _, _, tag, index): (String, String, Bytes<'_>, String, u64) = unpack(key).ok()?;
    (tag == "x").then(|| u32::try_from(index).ok()).flatten()
}

/// Recency index entry of a cached chunk on `tier`.
#[must_use]
pub fn cache_recency(tier: &str, last_access_ms: u64, generation: &GenId, index: u32) -> Key {
    pack(&(
        ROOT,
        "cidx",
        tier,
        last_access_ms,
        Bytes::from(&generation[..]),
        u64::from(index),
    ))
}

/// Recency entries of `tier` with `last_access_ms >= from_ms`, oldest first.
#[must_use]
pub fn cache_recency_range_from(tier: &str, from_ms: u64) -> KeyRange {
    let full = KeyRange::prefix(&pack(&(ROOT, "cidx", tier)));
    KeyRange::new(pack(&(ROOT, "cidx", tier, from_ms)), full.end)
}

/// `(generation, index)` from a recency key, if it is one.
#[must_use]
pub fn cache_recency_from(key: &[u8]) -> Option<(GenId, u32)> {
    let (_, _, _, _, generation, index): (String, String, String, u64, Bytes<'_>, u64) =
        unpack(key).ok()?;
    Some((
        <[u8; 16]>::try_from(generation.as_ref()).ok()?,
        u32::try_from(index).ok()?,
    ))
}

/// Bytes cached on `tier` (little-endian u64).
#[must_use]
pub fn cache_usage(tier: &str) -> Key {
    pack(&(ROOT, "cusage", tier))
}

/// Access record of a generation for automatic tiering.
#[must_use]
pub fn access(generation: &GenId) -> Key {
    pack(&(ROOT, "acc", Bytes::from(&generation[..])))
}

/// Every generation subspace (all `("al","g",...)` keys).
#[must_use]
pub fn all_generations_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "g")))
}

/// Every key belonging to a generation.
#[must_use]
pub fn gen_range(generation: &GenId) -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "g", Bytes::from(&generation[..]))))
}

/// Chunk index from a chunk-record key, if it is one.
#[must_use]
pub fn chunk_index_from(key: &[u8]) -> Option<u32> {
    unpack::<(String, String, Bytes<'_>, String, u64)>(key)
        .ok()
        .and_then(|t| u32::try_from(t.4).ok())
}

/// Pending-generation index entry.
#[must_use]
pub fn pending(lease_until_ms: u64, generation: &GenId) -> Key {
    pack(&(ROOT, "pend", lease_until_ms, Bytes::from(&generation[..])))
}

/// Pending-generation entries whose lease expired at or before `now_ms`.
#[must_use]
pub fn pending_expired_range(now_ms: u64) -> KeyRange {
    KeyRange::new(
        pack(&(ROOT, "pend")),
        pack(&(ROOT, "pend", now_ms.saturating_add(1))),
    )
}

/// Retired-generation queue entry.
#[must_use]
pub fn retired(since_ms: u64, generation: &GenId) -> Key {
    pack(&(ROOT, "gc", since_ms, Bytes::from(&generation[..])))
}

/// Migration intent of one chunk (tier migration in flight, Phase 3.5
/// audit CON-1/CON-2).
#[must_use]
pub fn migration(chunk_id: &[u8; 16]) -> Key {
    pack(&(ROOT, "mig", Bytes::from(&chunk_id[..])))
}

/// Every migration intent.
#[must_use]
pub fn migrations_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT, "mig")))
}

/// The retirement time of a retired-queue entry.
#[must_use]
pub fn retired_since_from(key: &[u8]) -> Option<u64> {
    unpack::<(String, String, u64, Bytes<'_>)>(key)
        .ok()
        .map(|t| t.2)
}

/// Retired-generation entries retired at or before `cutoff_ms`.
#[must_use]
pub fn retired_until_range(cutoff_ms: u64) -> KeyRange {
    KeyRange::new(
        pack(&(ROOT, "gc")),
        pack(&(ROOT, "gc", cutoff_ms.saturating_add(1))),
    )
}

/// Controller leader record (holder, fencing token, lease).
#[must_use]
pub fn leader() -> Key {
    pack(&(ROOT, "leader"))
}

/// The write admission record (Phase 3.6).
#[must_use]
pub fn admission() -> Key {
    pack(&(ROOT, "admission"))
}

/// The delete fence (Phase 3.9, ADR-0023).
#[must_use]
pub fn delete_fence() -> Key {
    pack(&(ROOT, "delete_fence"))
}

/// The capacity rebalancing state (Phase 3.8).
#[must_use]
pub fn rebalance() -> Key {
    pack(&(ROOT, "rebalance"))
}

/// Sequence of the current cluster incarnation (Phase 3.10, R-92): grows
/// with every incarnation, never compared with a clock.
#[must_use]
pub fn incarnation_seq() -> Key {
    pack(&(ROOT, "incarnation_seq"))
}

/// Which controller process opened the delete fence's generation (Phase
/// 3.10).
#[must_use]
pub fn delete_fence_opener() -> Key {
    pack(&(ROOT, "delete_fence_opener"))
}

/// The rebalancer's pacing (Phase 3.9).
#[must_use]
pub fn rebalance_pacing() -> Key {
    pack(&(ROOT, "rebalance_pacing"))
}

/// Every arca-lake key (for test setup and teardown).
#[must_use]
pub fn all_range() -> KeyRange {
    KeyRange::prefix(&pack(&(ROOT,)))
}

/// Node record.
#[must_use]
pub fn node(node_id: u64) -> Key {
    pack(&(ROOT, "node", node_id))
}

/// Prefix of all node records.
#[must_use]
pub fn nodes_prefix() -> Key {
    pack(&(ROOT, "node"))
}

/// Node ID allocator.
#[must_use]
pub fn node_counter() -> Key {
    pack(&(ROOT, "ctr", "node"))
}

/// Stripe group record.
#[must_use]
pub fn group(group_id: u64) -> Key {
    pack(&(ROOT, "grp", group_id))
}

/// Prefix of all group records.
#[must_use]
pub fn groups_prefix() -> Key {
    pack(&(ROOT, "grp"))
}

/// Group ID allocator.
#[must_use]
pub fn group_counter() -> Key {
    pack(&(ROOT, "ctr", "group"))
}

/// Cluster-map version, bumped on any node or group change.
#[must_use]
pub fn map_version() -> Key {
    pack(&(ROOT, "map", "version"))
}

/// All generation headers whose ID starts with `prefix` (the first 12
/// bytes of a chunk ID, RFC-004 §5.1).
#[must_use]
pub fn gen_prefix_range(prefix: &[u8; 12]) -> KeyRange {
    // The tuple encoding of a byte string is positional (0x00 is escaped as
    // 0x00 0xFF), so the encoding of a prefix is a prefix of the encoding,
    // once the terminator is removed.
    let mut begin = pack(&(ROOT, "g", Bytes::from(&prefix[..])));
    begin.pop();
    let end = strinc(&begin);
    KeyRange::new(begin, end)
}

/// Generation ID from a generation key (`("al","g",gen,...)`), if it is one.
#[must_use]
pub fn gen_from_gen_key(key: &[u8]) -> Option<GenId> {
    unpack::<(String, String, Bytes<'_>, String)>(key)
        .ok()
        .and_then(|t| <[u8; 16]>::try_from(t.2.as_ref()).ok())
        .or_else(|| {
            unpack::<(String, String, Bytes<'_>, String, u64)>(key)
                .ok()
                .and_then(|t| <[u8; 16]>::try_from(t.2.as_ref()).ok())
        })
}

/// Generation ID from a pending or retired index key.
#[must_use]
pub fn gen_from_index_key(key: &[u8]) -> Option<GenId> {
    unpack::<(String, String, u64, Bytes<'_>)>(key)
        .ok()
        .and_then(|t| <[u8; 16]>::try_from(t.3.as_ref()).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_keys_sort_and_prefix_ranges_cover_exactly_the_prefix() {
        let b = 7;
        let a = object_current(b, "a");
        let ab = object_current(b, "ab");
        let b0 = object_current(b, "b");
        assert!(a < ab && ab < b0);
        let r = object_prefix_range(b, "a");
        assert!(r.contains(&a) && r.contains(&ab) && !r.contains(&b0));
        assert!(!r.contains(&object_current(b + 1, "a")));
        let all = object_prefix_range(b, "");
        assert!(all.contains(&a) && all.contains(&b0));
        assert!(!all.contains(&object_current(b + 1, "a")));
        assert!(object_after(b, "a") > a && object_after(b, "a") < ab);
        assert_eq!(object_key_from(&ab), Some("ab".to_string()));
    }

    #[test]
    fn generation_subspace_is_one_range() {
        let g = [3u8; 16];
        let r = gen_range(&g);
        assert!(r.contains(&gen_header(&g)));
        assert!(r.contains(&gen_chunk(&g, 0)));
        assert!(r.contains(&gen_chunk(&g, u32::MAX)));
        assert!(!r.contains(&gen_header(&[4u8; 16])));
        assert!(gen_chunk(&g, 1) < gen_chunk(&g, 2));
        assert_eq!(chunk_index_from(&gen_chunk(&g, 42)), Some(42));
        let sub = gen_chunks_subrange(&g, 2..5);
        assert!(
            !sub.contains(&gen_chunk(&g, 1))
                && sub.contains(&gen_chunk(&g, 4))
                && !sub.contains(&gen_chunk(&g, 5))
        );
    }

    #[test]
    fn gen_prefix_range_covers_generations_with_that_prefix() {
        let mut g = [0u8; 16];
        g[..12].copy_from_slice(&[0, 1, 2, 0, 4, 5, 6, 7, 8, 9, 10, 11]); // includes a 0x00 inside
        g[12..].copy_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD]);
        let mut p = [0u8; 12];
        p.copy_from_slice(&g[..12]);
        let r = gen_prefix_range(&p);
        assert!(r.contains(&gen_header(&g)));
        assert!(r.contains(&gen_chunk(&g, 3)));
        let mut other = g;
        other[11] ^= 1;
        assert!(!r.contains(&gen_header(&other)));
        assert_eq!(gen_from_gen_key(&gen_header(&g)), Some(g));
        assert_eq!(gen_from_gen_key(&gen_chunk(&g, 9)), Some(g));
    }

    #[test]
    fn multipart_keys_sort_and_parse() {
        let u1 = [1u8; 16];
        let u2 = [2u8; 16];
        let r = mpu_prefix_range(3, "a");
        assert!(r.contains(&mpu(3, "a", &u1)) && r.contains(&mpu(3, "ab", &u2)));
        assert!(!r.contains(&mpu(3, "b", &u1)) && !r.contains(&mpu(4, "a", &u1)));
        assert!(mpu(3, "a", &u1) < mpu(3, "a", &u2) && mpu(3, "a", &u2) < mpu(3, "ab", &u1));
        assert!(mpu_after(3, "a", Some(&u1)) > mpu(3, "a", &u1));
        assert!(mpu_after(3, "a", Some(&u1)) < mpu(3, "a", &u2));
        assert!(mpu_after(3, "a", None) > mpu(3, "a", &u2));
        assert!(mpu_after(3, "a", None) < mpu(3, "ab", &u1));
        assert_eq!(mpu_from(&mpu(3, "k", &u1)), Some(("k".to_string(), u1)));
        assert!(parts_range(&u1).contains(&part(&u1, 1)));
        assert!(!parts_range(&u1).contains(&part(&u2, 1)));
        assert!(part(&u1, 2) < part(&u1, 10));
        assert!(parts_after(&u1, 2).contains(&part(&u1, 3)));
        assert!(!parts_after(&u1, 2).contains(&part(&u1, 2)));
        assert_eq!(part_number_from(&part(&u1, 7)), Some(7));
        let g = [5u8; 16];
        assert!(gen_range(&g).contains(&part_ref(&g, 1)));
        assert!(!gen_chunks_range(&g).contains(&part_ref(&g, 1)));
        assert!(part_refs_range(&g).contains(&part_ref(&g, 10_000)));
        assert!(!part_refs_range(&g).contains(&gen_chunk(&g, 1)));
        assert_eq!(part_ref_number_from(&part_ref(&g, 42)), Some(42));
        assert_eq!(part_ref_number_from(&gen_chunk(&g, 42)), None);
        assert_eq!(gen_from_gen_key(&part_ref(&g, 1)), Some(g));
    }

    #[test]
    fn index_keys_order_by_time() {
        let g = [9u8; 16];
        assert!(pending(5, &g) < pending(6, &g));
        assert!(pending_expired_range(5).contains(&pending(5, &g)));
        assert!(!pending_expired_range(5).contains(&pending(6, &g)));
        assert_eq!(gen_from_index_key(&retired(1, &g)), Some(g));
        assert!(
            version(1, "k", 1) < version(1, "k", 0),
            "newer ordinal sorts first"
        );
    }
}
