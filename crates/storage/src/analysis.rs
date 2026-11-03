//! Offline space analysis of a store directory (Phase 3.4 soak
//! convergence): where the bytes of the segment files go. Reads the files
//! only (no index is built, nothing is written, the quarantine log is not
//! touched); run it on a directory no process is serving.
//!
//! Every record is classified against all segments of the directory:
//! - live data: the newest record of its key, and data;
//! - dead data: superseded by a newer record of its key;
//! - tombstones: *needed* while an older data record of the key exists in
//!   another segment (dropping it could resurrect that record at the next
//!   open), otherwise *droppable*; and whether compaction's conservative
//!   rule (keep while any other segment has a lower minimum sequence) keeps
//!   it now;
//! - damaged records, and the rest of each file (segment header, record
//!   headers and alignment padding: slack).

use std::collections::HashMap;
use std::path::Path;

use crate::error::Result;
use crate::format::ShardKey;
use crate::segment;

/// One segment's breakdown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentBreakdown {
    /// Segment id.
    pub segment_id: u64,
    /// File length.
    pub file_bytes: u64,
    /// Payload bytes of live data records.
    pub live_bytes: u64,
    /// Live data records.
    pub live_records: u64,
    /// Payload bytes of superseded data records.
    pub dead_bytes: u64,
    /// Superseded data records.
    pub dead_records: u64,
    /// Tombstones still needed (an older data record exists elsewhere).
    pub tombstones_needed: u64,
    /// Tombstones no longer needed.
    pub tombstones_droppable: u64,
    /// Tombstones compaction's rule keeps today (lower minimum sequence in
    /// another segment).
    pub tombstones_kept_by_rule: u64,
    /// Bytes of tombstone records (their headers).
    pub tombstone_bytes: u64,
    /// Damaged records.
    pub damaged_records: u64,
    /// Everything else: segment header, record headers of data, padding.
    pub slack_bytes: u64,
    /// Lowest record sequence.
    pub min_seq: Option<u64>,
}

/// A directory's breakdown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirBreakdown {
    /// Per segment, in id order.
    pub segments: Vec<SegmentBreakdown>,
}

impl DirBreakdown {
    /// Sum over segments.
    #[must_use]
    pub fn total(&self) -> SegmentBreakdown {
        let mut t = SegmentBreakdown::default();
        for s in &self.segments {
            t.file_bytes += s.file_bytes;
            t.live_bytes += s.live_bytes;
            t.live_records += s.live_records;
            t.dead_bytes += s.dead_bytes;
            t.dead_records += s.dead_records;
            t.tombstones_needed += s.tombstones_needed;
            t.tombstones_droppable += s.tombstones_droppable;
            t.tombstones_kept_by_rule += s.tombstones_kept_by_rule;
            t.tombstone_bytes += s.tombstone_bytes;
            t.damaged_records += s.damaged_records;
            t.slack_bytes += s.slack_bytes;
            t.min_seq = match (t.min_seq, s.min_seq) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
        }
        t
    }
}

struct Rec {
    segment: u64,
    sequence: u64,
    tombstone: bool,
    header_len: u64,
    payload_len: u64,
}

/// Analyse every segment file of `dir`.
///
/// # Errors
///
/// I/O errors reading the directory or a segment that cannot be scanned.
pub fn analyze_dir(dir: &Path) -> Result<DirBreakdown> {
    let mut ids: Vec<u64> = std::fs::read_dir(dir)?
        .filter_map(std::result::Result::ok)
        .filter_map(|e| e.file_name().to_str().and_then(segment::parse_segment_name))
        .collect();
    ids.sort_unstable();
    let mut records: Vec<(ShardKey, Rec)> = Vec::new();
    let mut out = DirBreakdown::default();
    for &id in &ids {
        let scan = segment::scan(&segment::segment_path(dir, id), id)?;
        let mut b = SegmentBreakdown {
            segment_id: id,
            file_bytes: scan.file_len,
            damaged_records: scan.damaged.len() as u64,
            ..SegmentBreakdown::default()
        };
        for r in &scan.records {
            let p = r.header.prefix();
            b.min_seq = Some(b.min_seq.map_or(p.sequence, |m| m.min(p.sequence)));
            records.push((
                p.key,
                Rec {
                    segment: id,
                    sequence: p.sequence,
                    tombstone: p.is_tombstone(),
                    header_len: p.header_len() as u64,
                    payload_len: u64::from(p.payload_len),
                },
            ));
        }
        out.segments.push(b);
    }
    // Newest sequence per key, and per key the data records' sequences and
    // segments (for the tombstone rule).
    let mut newest: HashMap<ShardKey, u64> = HashMap::new();
    let mut data: HashMap<ShardKey, Vec<(u64, u64)>> = HashMap::new();
    for (k, r) in &records {
        let e = newest.entry(*k).or_insert(r.sequence);
        *e = (*e).max(r.sequence);
        if !r.tombstone {
            data.entry(*k).or_default().push((r.sequence, r.segment));
        }
    }
    let mins: Vec<(u64, u64)> = out
        .segments
        .iter()
        .filter_map(|s| s.min_seq.map(|m| (m, s.segment_id)))
        .collect();
    let other_min = |id: u64| mins.iter().filter(|(_, s)| *s != id).map(|(m, _)| *m).min();
    let pos: HashMap<u64, usize> = out
        .segments
        .iter()
        .enumerate()
        .map(|(i, s)| (s.segment_id, i))
        .collect();
    for (k, r) in &records {
        let b = &mut out.segments[pos[&r.segment]];
        if r.tombstone {
            b.tombstone_bytes += r.header_len;
            let older_elsewhere = data.get(k).is_some_and(|v| {
                v.iter()
                    .any(|(seq, seg)| *seq < r.sequence && *seg != r.segment)
            });
            if older_elsewhere {
                b.tombstones_needed += 1;
            } else {
                b.tombstones_droppable += 1;
            }
            if other_min(r.segment).is_some_and(|m| m < r.sequence) {
                b.tombstones_kept_by_rule += 1;
            }
        } else if newest.get(k) == Some(&r.sequence) {
            b.live_bytes += r.payload_len;
            b.live_records += 1;
        } else {
            b.dead_bytes += r.payload_len;
            b.dead_records += 1;
        }
    }
    for b in &mut out.segments {
        b.slack_bytes = b
            .file_bytes
            .saturating_sub(b.live_bytes + b.dead_bytes + b.tombstone_bytes);
    }
    Ok(out)
}
