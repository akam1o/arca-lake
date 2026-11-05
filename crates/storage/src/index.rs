//! In-memory shard index. Rebuilt from segments on open.
//!
//! The index is a pure function of the records in the segments: for every
//! key, the record with the highest sequence number wins, whether it is a
//! data record or a tombstone. Because the rule depends only on sequence
//! numbers and not on segment order, duplicated records (left behind by an
//! interrupted compaction) and records copied forward into newer segments
//! cannot change the outcome.

use std::collections::{BTreeMap, HashMap};
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

/// A shard whose record failed validation (RFC-005 §5.6a): absent from
/// the live index, its bytes kept until compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuarantineEntry {
    /// Where the failed record lives.
    pub location: Location,
    /// Why it failed.
    pub reason: &'static str,
    /// When it was quarantined, Unix milliseconds.
    pub since_unix_ms: u64,
}

/// Why a quarantine entry was released.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseCause {
    /// A newer data record for the key was written (repair).
    Superseded,
    /// A newer tombstone for the key was written (delete).
    Tombstone,
    /// The segment holding the bytes was compacted away.
    Purged,
}

impl ReleaseCause {
    /// Name used in the quarantine log.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Superseded => "superseded",
            Self::Tombstone => "tombstone",
            Self::Purged => "purged",
        }
    }
}

/// Entry counts of the in-memory index.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexSizes {
    /// Live keys.
    pub live: u64,
    /// Tombstone history entries (only while rebuilding).
    pub tombstone_history: u64,
    /// Keys with non-live records still on disk.
    pub dead_tracked: u64,
    /// Quarantined keys.
    pub quarantined: u64,
    /// Segments with per-segment bookkeeping.
    pub segments_tracked: u64,
}

/// Ordered in-memory index.
#[derive(Debug, Default)]
pub struct MemIndex {
    map: BTreeMap<ShardKey, Location>,
    /// Quarantined shards (not live).
    quarantined: BTreeMap<ShardKey, QuarantineEntry>,
    /// Quarantine entries released since the last drain, for the log.
    released: Vec<(ShardKey, QuarantineEntry, ReleaseCause)>,
    /// Highest tombstone sequence seen per key. Consulted so that a data
    /// record encountered *after* a newer tombstone (possible when segments
    /// are scanned in ID order and records were copied forward) is ignored.
    tombstones: BTreeMap<ShardKey, u64>,
    live_bytes: u64,
    segment_live: BTreeMap<u64, SegmentLive>,
    /// Data records still physically present that are not live (superseded,
    /// quarantined, or the source copy of a record compaction relocated),
    /// per segment and key. A tombstone of a key is needed exactly while
    /// such a record of the key exists outside its own segment: it could
    /// otherwise reappear when the store is opened (Phase 3.4: the previous
    /// rule, "while any other segment has a lower sequence", kept every
    /// tombstone behind one long-lived record forever).
    dead_by_segment: HashMap<u64, HashMap<ShardKey, u32>>,
    /// Per key, the sum over segments of `dead_by_segment`.
    dead_total: HashMap<ShardKey, u32>,
    /// The rebuild from the segments is over ([`Self::finish_rebuild`]):
    /// from then on new records carry strictly increasing sequences and the
    /// tombstone history is neither needed nor kept (Phase 3.5, R-70: every
    /// runtime delete added an entry that lived until the next restart,
    /// about 5 MiB/h of growth under the soak's delete rate).
    rebuilt: bool,
}

impl MemIndex {
    /// Record `loc` for `key` if it is newer than any record or tombstone
    /// already seen for the key. Returns the superseded location, if any.
    pub fn insert(&mut self, key: ShardKey, loc: Location) -> Option<Location> {
        if let Some(existing) = self.map.get(&key)
            && existing.sequence >= loc.sequence
        {
            self.dead_add(key, loc.segment_id);
            return None;
        }
        if let Some(&t) = self.tombstones.get(&key)
            && t >= loc.sequence
        {
            self.dead_add(key, loc.segment_id);
            return None;
        }
        if let Some(q) = self.quarantined.get(&key) {
            if q.location.sequence >= loc.sequence {
                // An older record cannot resurrect a key whose newest record
                // is quarantined.
                self.dead_add(key, loc.segment_id);
                return None;
            }
            let q = self.quarantined.remove(&key).expect("present");
            self.released.push((key, q, ReleaseCause::Superseded));
        }
        self.account_add(&loc);
        let old = self.map.insert(key, loc);
        if let Some(old) = &old {
            self.account_remove(old);
            self.dead_add(key, old.segment_id);
        }
        old
    }

    /// Apply a tombstone with sequence `sequence`: removes the key if its
    /// current record is older. Returns the removed location, if any.
    pub fn tombstone(&mut self, key: ShardKey, sequence: u64) -> Option<Location> {
        if !self.rebuilt {
            let t = self.tombstones.entry(key).or_insert(0);
            *t = (*t).max(sequence);
        }
        if let Some(q) = self.quarantined.get(&key)
            && q.location.sequence < sequence
        {
            let q = self.quarantined.remove(&key).expect("present");
            self.released.push((key, q, ReleaseCause::Tombstone));
        }
        match self.map.get(&key) {
            Some(existing) if existing.sequence < sequence => {
                let old = self.map.remove(&key)?;
                self.account_remove(&old);
                self.dead_add(key, old.segment_id);
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
                // The source copy stays on disk until its segment is gone.
                self.dead_add(*key, old.segment_id);
                true
            }
            _ => false,
        }
    }

    /// Quarantine the record at `entry.location` for `key` (RFC-005
    /// §5.6a): the key leaves the live index if its live record is not
    /// newer, and the entry is kept so that older records cannot resurrect
    /// the key. Returns whether the entry was recorded (a newer live
    /// record, tombstone or quarantine entry for the key wins instead).
    pub fn quarantine(&mut self, key: ShardKey, entry: QuarantineEntry) -> bool {
        let seq = entry.location.sequence;
        if self.map.get(&key).is_some_and(|l| l.sequence > seq)
            || self.tombstones.get(&key).is_some_and(|&t| t >= seq)
            || self
                .quarantined
                .get(&key)
                .is_some_and(|q| q.location.sequence >= seq)
        {
            return false;
        }
        if let Some(old) = self.map.remove(&key) {
            self.account_remove(&old);
            if old != entry.location {
                self.dead_add(key, old.segment_id);
            }
        }
        // Quarantined bytes stay on disk until their segment is gone.
        self.dead_add(key, entry.location.segment_id);
        self.quarantined.insert(key, entry);
        true
    }

    pub(crate) fn dead_add(&mut self, key: ShardKey, segment_id: u64) {
        *self
            .dead_by_segment
            .entry(segment_id)
            .or_default()
            .entry(key)
            .or_default() += 1;
        *self.dead_total.entry(key).or_default() += 1;
    }

    /// A segment file is gone (compacted and unlinked): its non-live
    /// records no longer exist anywhere.
    pub fn segment_removed(&mut self, segment_id: u64) {
        let Some(keys) = self.dead_by_segment.remove(&segment_id) else {
            return;
        };
        for (k, n) in keys {
            if let Some(t) = self.dead_total.get_mut(&k) {
                *t = t.saturating_sub(n);
                if *t == 0 {
                    self.dead_total.remove(&k);
                }
            }
        }
    }

    /// Whether a non-live data record of `key` exists in a segment other
    /// than `segment_id` (a tombstone of the key in `segment_id` is then
    /// still needed).
    #[must_use]
    pub fn dead_elsewhere(&self, key: &ShardKey, segment_id: u64) -> bool {
        let total = self.dead_total.get(key).copied().unwrap_or(0);
        let here = self
            .dead_by_segment
            .get(&segment_id)
            .and_then(|m| m.get(key))
            .copied()
            .unwrap_or(0);
        total > here
    }

    /// Non-live data records tracked (for statistics).
    #[must_use]
    pub fn dead_records(&self) -> u64 {
        self.dead_total.values().map(|&n| u64::from(n)).sum()
    }

    /// The quarantine entry of `key`, if any.
    #[must_use]
    pub fn quarantine_entry(&self, key: &ShardKey) -> Option<QuarantineEntry> {
        self.quarantined.get(key).copied()
    }

    /// Release the quarantine entry of `key` (its bytes are gone).
    pub fn purge_quarantine(&mut self, key: &ShardKey, location: &Location) -> bool {
        match self.quarantined.get(key) {
            Some(q) if q.location == *location => {
                let q = self.quarantined.remove(key).expect("present");
                self.released.push((*key, q, ReleaseCause::Purged));
                true
            }
            _ => false,
        }
    }

    /// Quarantine entries released since the last call.
    pub fn drain_released(&mut self) -> Vec<(ShardKey, QuarantineEntry, ReleaseCause)> {
        std::mem::take(&mut self.released)
    }

    /// Quarantined shards in `group_id` strictly after `after`, in key
    /// order, at most `limit`.
    pub fn quarantined_in_group_after(
        &self,
        group_id: u64,
        after: Option<ShardKey>,
        limit: usize,
    ) -> impl Iterator<Item = (ShardKey, QuarantineEntry)> + '_ {
        let start = match after {
            Some(k) => Bound::Excluded(k),
            None => Bound::Included(ShardKey {
                group_id,
                chunk_id: [0; 16],
                shard_index: 0,
            }),
        };
        self.quarantined
            .range((start, Bound::Unbounded))
            .take_while(move |(k, _)| k.group_id == group_id)
            .map(|(k, q)| (*k, *q))
            .take(limit)
    }

    /// Every quarantined shard.
    pub fn quarantined(&self) -> impl Iterator<Item = (&ShardKey, &QuarantineEntry)> + '_ {
        self.quarantined.iter()
    }

    /// Number of quarantined shards and their payload bytes.
    #[must_use]
    pub fn quarantine_stats(&self) -> (usize, u64) {
        (
            self.quarantined.len(),
            self.quarantined
                .values()
                .map(|q| u64::from(q.location.payload_len))
                .sum(),
        )
    }

    /// Drop the tombstone history kept for rebuild. Afterwards only live
    /// records and new (strictly increasing) sequences matter.
    pub fn finish_rebuild(&mut self) {
        self.tombstones.clear();
        self.rebuilt = true;
    }

    /// Quarantine a record read through the live index (runtime): only if
    /// the key still has exactly that live location. A key deleted, replaced
    /// or quarantined since the read is left alone; this is what the
    /// tombstone history protected against before it stopped being kept
    /// after the rebuild.
    pub fn quarantine_live(&mut self, key: ShardKey, entry: QuarantineEntry) -> bool {
        if self.map.get(&key) != Some(&entry.location) {
            return false;
        }
        self.quarantine(key, entry)
    }

    /// Sizes of the in-memory structures (diagnostics and metrics).
    #[must_use]
    pub fn sizes(&self) -> IndexSizes {
        IndexSizes {
            live: self.map.len() as u64,
            tombstone_history: self.tombstones.len() as u64,
            dead_tracked: self.dead_total.len() as u64,
            quarantined: self.quarantined.len() as u64,
            segments_tracked: self.dead_by_segment.len().max(self.segment_live.len()) as u64,
        }
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
    ) -> impl Iterator<Item = (ShardKey, u32, u64)> + '_ {
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
            .map(|(k, loc)| (*k, loc.payload_len, loc.sequence))
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

    /// After the rebuild a runtime quarantine applies only to the location
    /// still live: a key deleted or replaced since the read is left alone,
    /// without any tombstone history (R-70).
    #[test]
    fn runtime_quarantine_needs_the_location_still_live() {
        let mut idx = MemIndex::default();
        idx.finish_rebuild();
        let q = |l: Location| QuarantineEntry {
            location: l,
            reason: "test",
            since_unix_ms: 0,
        };
        idx.insert(key(1), loc(1, 5, 100));
        idx.tombstone(key(1), 6);
        assert!(
            !idx.quarantine_live(key(1), q(loc(1, 5, 100))),
            "deleted since the read"
        );
        idx.insert(key(2), loc(1, 7, 100));
        idx.insert(key(2), loc(1, 8, 100));
        assert!(
            !idx.quarantine_live(key(2), q(loc(1, 7, 100))),
            "replaced since the read"
        );
        assert!(idx.quarantine_live(key(2), q(loc(1, 8, 100))));
        assert_eq!(idx.sizes().tombstone_history, 0);
        assert_eq!(idx.sizes().quarantined, 1);
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
