//! Key layout, version 1.
//!
//! All keys are FoundationDB tuples (order-preserving encoding, via the
//! `foundationdb-tuple` crate) rooted at the string `"al"`. Subspaces:
//!
//! | Tuple | Value | Purpose |
//! | --- | --- | --- |
//! | `("al","meta","layout")` | layout version | format marker |
//! | `("al","ctr","bucket")` | u64 LE | bucket ID allocator |
//! | `("al","b",name)` | `BucketRecord` | bucket by name |
//! | `("al","bi",bucket_id)` | `BucketRecord` | bucket by ID |
//! | `("al","o",bucket_id,key)` | `CurrentPointer` | current version of an object key |
//! | `("al","v",bucket_id,key,ordinal_desc)` | `VersionRecord` | version history, newest first |
//! | `("al","g",gen,"h")` | `GenerationHeader` | one immutable generation |
//! | `("al","g",gen,"c",index)` | `ChunkRecord` | chunk manifest + placement, one key per chunk |
//! | `("al","pend",lease_until_ms,gen)` | empty | pending generations by lease expiry |
//! | `("al","gc",since_ms,gen)` | empty | retired generations awaiting GC |
//!
//! A generation's header and chunk records share a prefix so that one range
//! read fetches everything a GET needs. Object keys of one bucket share a
//! prefix so that listing is a range read, and the string encoding of the
//! tuple layer lets a key prefix be turned into a key range.

use foundationdb_tuple::{Bytes, pack, unpack};

use crate::kv::{Key, KeyRange, strinc};
use crate::records::GenId;

const ROOT: &str = "al";

/// Layout version written to `("al","meta","layout")`.
pub const LAYOUT_VERSION: u64 = 1;

/// Key holding the layout version.
#[must_use]
pub fn layout_version() -> Key {
    pack(&(ROOT, "meta", "layout"))
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

/// Retired-generation entries retired at or before `cutoff_ms`.
#[must_use]
pub fn retired_until_range(cutoff_ms: u64) -> KeyRange {
    KeyRange::new(
        pack(&(ROOT, "gc")),
        pack(&(ROOT, "gc", cutoff_ms.saturating_add(1))),
    )
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
