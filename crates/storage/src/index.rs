//! In-memory shard index. Rebuilt from segments on open.
//!
//! The index is a pure function of the records in the segments: for every
//! key, the record with the highest sequence number wins, whether it is a
//! data record or a tombstone. Because the rule depends only on sequence
//! numbers and not on segment order, duplicated records (left behind by an
//! interrupted compaction) and records copied forward into newer segments
//! cannot change the outcome.

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

/// Per-segment accounting of live data.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentLive {
    /// Live payload bytes.
    pub bytes: u64,
    /// Live records.
    pub records: usize,
}

/// Ordered in-memory index.
#[derive(Debug, Default)]
pub struct MemIndex {
    map: BTreeMap<ShardKey, Location>,
    /// Highest tombstone sequence seen per key. Consulted so that a data
    /// record encountered *after* a newer tombstone (possible when segments
    /// are scanned in ID order and records were copied forward) is ignored.
    tombstones: BTreeMap<ShardKey, u64>,
    live_bytes: u64,
    segment_live: BTreeMap<u64, SegmentLive>,
}

impl MemIndex {
    /// Record `loc` for `key` if it is newer than any record or tombstone
    /// already seen for the key. Returns the superseded location, if any.
    pub fn insert(&mut self, key: ShardKey, loc: Location) -> Option<Location> {
        if let Some(existing) = self.map.get(&key)
            && existing.sequence >= loc.sequence
        {
            return None;
        }
        if let Some(&t) = self.tombstones.get(&key)
            && t >= loc.sequence
        {
            return None;
        }
        self.account_add(&loc);
        let old = self.map.insert(key, loc);
        if let Some(old) = &old {
            self.account_remove(old);
        }
        old
    }

    /// Apply a tombstone with sequence `sequence`: removes the key if its
    /// current record is older. Returns the removed location, if any.
    pub fn tombstone(&mut self, key: ShardKey, sequence: u64) -> Option<Location> {
        let t = self.tombstones.entry(key).or_insert(0);
        *t = (*t).max(sequence);
        match self.map.get(&key) {
            Some(existing) if existing.sequence < sequence => {
                let old = self.map.remove(&key)?;
                self.account_remove(&old);
                Some(old)
            }
            _ => None,
        }
    }

    /// Replace the location of `key` only if it is still exactly `from`.
    /// Used by compaction to retarget a record it has copied.
    pub fn relocate(&mut self, key: &ShardKey, from: &Location, to: Location) -> bool {
        match self.map.get_mut(key) {
            Some(cur) if cur == from => {
                let old = *cur;
                *cur = to;
                self.account_remove(&old);
                self.account_add(&to);
                true
            }
            _ => false,
        }
    }

    /// Forget a record that failed validation. Returns the removed location.
    pub fn quarantine(&mut self, key: &ShardKey) -> Option<Location> {
        let old = self.map.remove(key)?;
        self.account_remove(&old);
        Some(old)
    }

    /// Drop the tombstone history kept for rebuild. Afterwards only live
    /// records and new (strictly increasing) sequences matter.
    pub fn finish_rebuild(&mut self) {
        self.tombstones.clear();
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

    /// Live accounting for one segment.
    #[must_use]
    pub fn segment_live(&self, segment_id: u64) -> SegmentLive {
        self.segment_live
            .get(&segment_id)
            .copied()
            .unwrap_or_default()
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

    /// Live entries in `group_id` strictly after `after` (or from the start
    /// of the group), in key order, at most `limit`.
    pub fn entries_in_group_after(
        &self,
        group_id: u64,
        after: Option<ShardKey>,
        limit: usize,
    ) -> impl Iterator<Item = (ShardKey, u32)> + '_ {
        let start = match after {
            Some(k) => Bound::Excluded(k),
            None => Bound::Included(ShardKey {
                group_id,
                chunk_id: [0; 16],
                shard_index: 0,
            }),
        };
        self.map
            .range((start, Bound::Unbounded))
            .take_while(move |(k, _)| k.group_id == group_id)
            .map(|(k, loc)| (*k, loc.payload_len))
            .take(limit)
    }

    /// All live entries.
    pub fn iter(&self) -> impl Iterator<Item = (&ShardKey, &Location)> + '_ {
        self.map.iter()
    }

    fn account_add(&mut self, loc: &Location) {
        self.live_bytes += u64::from(loc.payload_len);
        let s = self.segment_live.entry(loc.segment_id).or_default();
        s.bytes += u64::from(loc.payload_len);
        s.records += 1;
    }

    fn account_remove(&mut self, loc: &Location) {
        self.live_bytes -= u64::from(loc.payload_len);
        if let Some(s) = self.segment_live.get_mut(&loc.segment_id) {
            s.bytes -= u64::from(loc.payload_len);
            s.records -= 1;
            if s.records == 0 {
                self.segment_live.remove(&loc.segment_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(seg: u64, seq: u64, len: u32) -> Location {
        Location {
            segment_id: seg,
            offset: seq * 4096,
            header_len: 64,
            payload_len: len,
            sequence: seq,
        }
    }

    fn key(i: u8) -> ShardKey {
        ShardKey {
            group_id: 1,
            chunk_id: [i; 16],
            shard_index: 0,
        }
    }

    #[test]
    fn newer_sequence_wins_and_accounting_tracks() {
        let mut idx = MemIndex::default();
        assert!(idx.insert(key(1), loc(1, 5, 100)).is_none());
        assert_eq!(idx.live_bytes(), 100);
        assert!(
            idx.insert(key(1), loc(1, 3, 50)).is_none(),
            "older must be ignored"
        );
        assert_eq!(idx.get(&key(1)).unwrap().sequence, 5);
        assert_eq!(idx.insert(key(1), loc(2, 9, 10)).unwrap().sequence, 5);
        assert_eq!(idx.live_bytes(), 10);
        assert_eq!(idx.segment_live(1), SegmentLive::default());
        assert_eq!(
            idx.segment_live(2),
            SegmentLive {
                bytes: 10,
                records: 1
            }
        );
        assert!(idx.tombstone(key(1), 10).is_some());
        assert_eq!(idx.live_bytes(), 0);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn tombstone_order_independence() {
        // Tombstone (seq 7) scanned before an older data record (seq 5) that
        // was copied into a later segment: the data must stay dead.
        let mut idx = MemIndex::default();
        assert!(idx.tombstone(key(1), 7).is_none());
        assert!(idx.insert(key(1), loc(9, 5, 1)).is_none());
        assert!(idx.get(&key(1)).is_none());
        // A genuinely newer write revives the key.
        assert!(idx.insert(key(1), loc(9, 8, 1)).is_none());
        assert!(idx.get(&key(1)).is_some());
        // An older tombstone does not kill a newer record.
        assert!(idx.tombstone(key(1), 6).is_none());
        assert!(idx.get(&key(1)).is_some());
        // Duplicate of the live record in another segment is ignored.
        assert!(idx.insert(key(1), loc(10, 8, 1)).is_none());
        assert_eq!(idx.get(&key(1)).unwrap().segment_id, 9);
    }

    #[test]
    fn relocate_is_compare_and_set() {
        let mut idx = MemIndex::default();
        idx.insert(key(1), loc(1, 1, 10));
        assert!(!idx.relocate(&key(1), &loc(1, 2, 10), loc(5, 1, 10)));
        assert!(idx.relocate(&key(1), &loc(1, 1, 10), loc(5, 1, 10)));
        assert_eq!(idx.get(&key(1)).unwrap().segment_id, 5);
        assert_eq!(idx.segment_live(1).records, 0);
        assert_eq!(idx.segment_live(5).records, 1);
    }

    #[test]
    fn group_iteration_is_bounded() {
        let mut idx = MemIndex::default();
        for (i, g) in [1u64, 2, 2, 3].into_iter().enumerate() {
            let k = ShardKey {
                group_id: g,
                chunk_id: [u8::try_from(i).unwrap(); 16],
                shard_index: 0,
            };
            idx.insert(k, loc(1, i as u64 + 1, 1));
        }
        assert_eq!(idx.keys_in_group(2).count(), 2);
        assert_eq!(idx.keys_in_group(4).count(), 0);
    }
}
