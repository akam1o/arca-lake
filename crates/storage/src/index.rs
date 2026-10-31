//! In-memory shard index. Rebuilt from segments on open.

use std::collections::BTreeMap;
use std::ops::Bound;

use crate::format::ShardKey;

/// Where a live record lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    /// Segment ID.
    pub segment_id: u64,
    /// Offset of the record header within the segment file.
    pub offset: u64,
    /// Encoded header length (fixed prefix, CRC table, header CRC).
    pub header_len: u64,
    /// Payload length.
    pub payload_len: u32,
    /// Sequence number of the record.
    pub sequence: u64,
}

impl Location {
    /// Offset of the first payload byte.
    #[must_use]
    pub fn payload_offset(&self) -> u64 {
        self.offset + self.header_len
    }
}

/// Ordered in-memory index.
#[derive(Debug, Default)]
pub struct MemIndex {
    map: BTreeMap<ShardKey, Location>,
    live_bytes: u64,
}

impl MemIndex {
    /// Record `loc` for `key` if it is newer than what is indexed.
    /// Returns the superseded location, if any.
    pub fn insert(&mut self, key: ShardKey, loc: Location) -> Option<Location> {
        if let Some(existing) = self.map.get(&key)
            && existing.sequence >= loc.sequence
        {
            return None;
        }
        self.live_bytes += u64::from(loc.payload_len);
        let old = self.map.insert(key, loc);
        if let Some(old) = old {
            self.live_bytes -= u64::from(old.payload_len);
        }
        old
    }

    /// Remove `key`. Returns the removed location, if any.
    pub fn remove(&mut self, key: &ShardKey) -> Option<Location> {
        let old = self.map.remove(key);
        if let Some(old) = old {
            self.live_bytes -= u64::from(old.payload_len);
        }
        old
    }

    /// Look up `key`.
    #[must_use]
    pub fn get(&self, key: &ShardKey) -> Option<Location> {
        self.map.get(key).copied()
    }

    /// Number of live shards.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Total payload bytes of live shards.
    #[must_use]
    pub fn live_bytes(&self) -> u64 {
        self.live_bytes
    }

    /// All live keys in `group_id`, in key order.
    pub fn keys_in_group(&self, group_id: u64) -> impl Iterator<Item = ShardKey> + '_ {
        let start = ShardKey {
            group_id,
            chunk_id: [0; 16],
            shard_index: 0,
        };
        self.map
            .range((Bound::Included(start), Bound::Unbounded))
            .take_while(move |(k, _)| k.group_id == group_id)
            .map(|(k, _)| *k)
    }

    /// All live entries.
    pub fn iter(&self) -> impl Iterator<Item = (&ShardKey, &Location)> + '_ {
        self.map.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(seq: u64, len: u32) -> Location {
        Location {
            segment_id: 1,
            offset: 0,
            header_len: 64,
            payload_len: len,
            sequence: seq,
        }
    }

    #[test]
    fn newer_sequence_wins_and_live_bytes_track() {
        let mut idx = MemIndex::default();
        let k = ShardKey {
            group_id: 1,
            chunk_id: [0; 16],
            shard_index: 0,
        };
        assert!(idx.insert(k, loc(5, 100)).is_none());
        assert_eq!(idx.live_bytes(), 100);
        assert!(idx.insert(k, loc(3, 50)).is_none(), "older must be ignored");
        assert_eq!(idx.get(&k).unwrap().sequence, 5);
        assert_eq!(idx.insert(k, loc(9, 10)).unwrap().sequence, 5);
        assert_eq!(idx.live_bytes(), 10);
        assert!(idx.remove(&k).is_some());
        assert_eq!(idx.live_bytes(), 0);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn group_iteration_is_bounded() {
        let mut idx = MemIndex::default();
        for g in [1u64, 2, 2, 3] {
            let k = ShardKey {
                group_id: g,
                chunk_id: [u8::try_from(idx.len()).unwrap(); 16],
                shard_index: 0,
            };
            idx.insert(k, loc(idx.len() as u64 + 1, 1));
        }
        assert_eq!(idx.keys_in_group(2).count(), 2);
        assert_eq!(idx.keys_in_group(4).count(), 0);
    }
}
