//! The per-device store: open/recover, put, get, delete, compaction, scrub.

use std::collections::BTreeMap;
use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

use arca_lake_common::timing::{Histogram, HistogramSnapshot};
use std::sync::{Condvar, Mutex, PoisonError, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, StorageError};
use crate::format::{
    FLAG_TOMBSTONE, RecordHeader, SEGMENT_FORMAT_VERSION, SegmentHeader, ShardKey,
    padded_header_len, validate_geometry,
};
use crate::index::{Location, MemIndex, QuarantineEntry, ReleaseCause};
use crate::segment::{self, ScanEnd, SegmentWriter, segment_path, verify_blocks};

/// When `put` and `delete` make data durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// `fdatasync` inside the writer lock before every acknowledgment.
    /// Simple and durable; serializes all writers behind each sync.
    Always,
    /// Group commit: appends happen under the writer lock, then the caller
    /// waits until a sync covering its record has completed. The first
    /// waiter performs one `fdatasync` on behalf of everyone appended so
    /// far. Durable before acknowledgment, like `Always`, with fewer syncs
    /// under concurrency.
    Group,
    /// Never sync. **Tests and measurements only**; acknowledged data may be
    /// lost on crash.
    Never,
}

/// Store configuration.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Device directory.
    pub dir: PathBuf,
    /// Roll to a new segment once the active one reaches this many bytes.
    pub segment_max_bytes: u64,
    /// Record alignment (power of two, ≥ 64). Written into segment headers.
    pub record_alignment: u32,
    /// Payload checksum block size (multiple of 64). Written into segment headers.
    pub block_size: u32,
    /// Durability policy.
    pub sync: SyncPolicy,
}

impl StoreConfig {
    /// Defaults suitable for experiments: 1 GiB segments, 4 KiB alignment,
    /// 64 KiB checksum blocks, group commit. Nothing here is a tuned value.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            segment_max_bytes: 1 << 30,
            record_alignment: 4096,
            block_size: 1 << 16,
            sync: SyncPolicy::Group,
        }
    }
}

/// What `open` found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenReport {
    /// Segments scanned.
    pub segments: usize,
    /// Live shards indexed.
    pub live_shards: usize,
    /// Records that were superseded by a later record or tombstone.
    pub superseded_records: usize,
    /// Bytes discarded as a torn tail (last segment only), including a last
    /// segment whose header was never completely written.
    pub truncated_bytes: u64,
    /// Segments in which corruption (not a torn tail) was detected, with the
    /// offset where valid data ends and the reason.
    pub corrupt_segments: Vec<(u64, u64, &'static str)>,
    /// Records quarantined at open (valid header, corrupt payload), as
    /// `(segment, offset, reason)`.
    pub quarantined: Vec<(u64, u64, &'static str)>,
    /// Regions skipped because their header did not validate, as
    /// `(segment, offset, end, reason)`.
    pub holes: Vec<(u64, u64, u64, &'static str)>,
}

/// Scrub result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrubReport {
    /// Shards verified intact.
    pub verified: usize,
    /// Shards whose on-disk data no longer validates. These are quarantined
    /// so that reads report them as missing.
    pub corrupt: Vec<ShardKey>,
    /// Shards that were already quarantined.
    pub quarantined: usize,
}

/// Result of verifying one shard end to end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verified {
    /// Payload length.
    pub payload_len: u32,
    /// CRC-32C of the whole payload.
    pub crc32c: u32,
    /// Record sequence number.
    pub sequence: u64,
}

/// Append-only, fsynced record of quarantine events (RFC-005 §5.6a).
/// Lines: `quarantine group=.. chunk=<hex> index=.. segment=.. offset=..
/// sequence=.. len=.. reason="..." ms=..` and `release ... cause=..`.
#[derive(Debug)]
struct QuarantineLog {
    file: Mutex<File>,
}

impl QuarantineLog {
    const NAME: &'static str = "quarantine.log";

    fn open(dir: &Path) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(dir.join(Self::NAME))?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    fn write(&self, line: &str) -> Result<()> {
        use std::io::Write as _;
        let mut f = self.file.lock().unwrap_or_else(PoisonError::into_inner);
        f.write_all(line.as_bytes())?;
        f.write_all(b"\n")?;
        f.sync_data()?;
        Ok(())
    }

    fn key_fields(key: &ShardKey) -> String {
        use std::fmt::Write as _;
        let mut hex = String::with_capacity(32);
        for b in key.chunk_id {
            let _ = write!(hex, "{b:02x}");
        }
        format!(
            "group={} chunk={hex} index={}",
            key.group_id, key.shard_index
        )
    }

    fn quarantine(&self, key: &ShardKey, q: &QuarantineEntry) -> Result<()> {
        self.write(&format!(
            "quarantine {} segment={} offset={} sequence={} len={} reason=\"{}\" ms={}",
            Self::key_fields(key),
            q.location.segment_id,
            q.location.offset,
            q.location.sequence,
            q.location.payload_len,
            q.reason,
            q.since_unix_ms
        ))
    }

    fn release(&self, key: &ShardKey, q: &QuarantineEntry, cause: ReleaseCause) -> Result<()> {
        self.write(&format!(
            "release {} segment={} offset={} sequence={} cause={} ms={}",
            Self::key_fields(key),
            q.location.segment_id,
            q.location.offset,
            q.location.sequence,
            cause.name(),
            unix_ms()
        ))
    }

    /// Lines of the log (for operators and tests).
    fn lines(&self) -> Result<Vec<String>> {
        use std::io::{Read as _, Seek as _, SeekFrom};
        let mut f = self.file.lock().unwrap_or_else(PoisonError::into_inner);
        let mut text = String::new();
        f.seek(SeekFrom::Start(0))?;
        f.read_to_string(&mut text)?;
        Ok(text.lines().map(str::to_string).collect())
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

/// Capacity accounting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreStats {
    /// Live shards.
    pub live_shards: usize,
    /// Payload bytes of live shards.
    pub live_bytes: u64,
    /// Total bytes of all segment files (live + dead + headers + padding).
    pub segment_bytes: u64,
    /// Number of segments.
    pub segments: usize,
}

/// Per-segment accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentStats {
    /// Segment ID.
    pub segment_id: u64,
    /// File length.
    pub file_bytes: u64,
    /// Live payload bytes referenced by the index.
    pub live_bytes: u64,
    /// Live records referenced by the index.
    pub live_records: usize,
    /// Whether this is the active (append) segment.
    pub active: bool,
}

/// Points at which compaction can be interrupted by a fault-injection hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStep {
    /// A record was copied to the output segment (count so far).
    Copied(usize),
    /// The output segment is durable.
    Synced,
    /// The index points at the output segment.
    IndexSwapped,
    /// The source segment file has been unlinked.
    Unlinked,
}

/// Compaction result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionReport {
    /// Compacted segment.
    pub segment_id: u64,
    /// Output segment, if any record had to be kept.
    pub output_segment_id: Option<u64>,
    /// Live data records copied.
    pub copied_records: usize,
    /// Payload bytes copied.
    pub copied_bytes: u64,
    /// Dead data records dropped.
    pub dropped_records: usize,
    /// Tombstones copied because an older record for their key may exist.
    pub tombstones_kept: usize,
    /// Tombstones dropped.
    pub tombstones_dropped: usize,
}

#[derive(Debug)]
struct Writer {
    active: SegmentWriter,
    /// Lowest sequence number appended to the active segment.
    active_min_seq: Option<u64>,
    next_sequence: u64,
    next_segment_id: u64,
}

#[derive(Debug)]
struct SegmentInfo {
    file: File,
    header: SegmentHeader,
    /// Lowest sequence number in the segment; `None` while active or empty.
    min_seq: Option<u64>,
}

/// Group-commit coordinator. Positions are `(segment_id, end_offset)` and
/// compare lexicographically: everything in earlier segments plus the prefix
/// of the given segment.
#[derive(Debug, Default)]
struct Durability {
    state: Mutex<DurState>,
    cv: Condvar,
}

#[derive(Debug, Default)]
struct DurState {
    durable: (u64, u64),
    appended: (u64, u64),
    syncing: bool,
}

/// Accumulated state while scanning segments at open.
struct Recovery {
    report: OpenReport,
    index: MemIndex,
    /// Damaged records seen by the scans, quarantined once every segment
    /// has been read.
    damaged: Vec<(ShardKey, QuarantineEntry)>,
    readers: BTreeMap<u64, SegmentInfo>,
    device_id: Option<[u8; 16]>,
    max_sequence: u64,
    /// Header, valid length, and min sequence of the last segment if it may
    /// be appended to.
    appendable: Option<(SegmentHeader, u64, Option<u64>)>,
}

/// A single-device store.
#[derive(Debug)]
pub struct Store {
    cfg: StoreConfig,
    device_id: [u8; 16],
    writer: Mutex<Writer>,
    index: RwLock<MemIndex>,
    readers: RwLock<BTreeMap<u64, SegmentInfo>>,
    durability: Durability,
    timings: StoreTimings,
    quarantine_log: QuarantineLog,
}

/// Where a write spends its time (R-48 PUT profiling).
#[derive(Debug, Default)]
pub struct StoreTimings {
    /// Appending header and payload to the active segment, under the
    /// writer lock (includes a rollover when one happens).
    pub append: Histogram,
    /// Segment rollover (sync old, create and sync new, sync directory).
    pub rollover: Histogram,
    /// Waiting in the group commit until the record is durable.
    pub sync_wait: Histogram,
    /// One `fdatasync` call issued by the group committer.
    pub fsync: Histogram,
}

/// A copy of [`StoreTimings`].
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreTimingsSnapshot {
    /// See [`StoreTimings::append`].
    pub append: HistogramSnapshot,
    /// See [`StoreTimings::rollover`].
    pub rollover: HistogramSnapshot,
    /// See [`StoreTimings::sync_wait`].
    pub sync_wait: HistogramSnapshot,
    /// See [`StoreTimings::fsync`].
    pub fsync: HistogramSnapshot,
}

impl Store {
    /// Open (or create) the store in `cfg.dir`, rebuilding the index.
    ///
    /// # Errors
    ///
    /// Fails on I/O errors, unsupported formats, mixed device IDs, or
    /// geometry that differs from existing segments.
    pub fn open(cfg: StoreConfig) -> Result<(Self, OpenReport)> {
        validate_geometry(cfg.record_alignment, cfg.block_size)?;
        std::fs::create_dir_all(&cfg.dir)?;

        let mut ids: Vec<u64> = std::fs::read_dir(&cfg.dir)?
            .filter_map(std::result::Result::ok)
            .filter_map(|e| e.file_name().to_str().and_then(segment::parse_segment_name))
            .collect();
        ids.sort_unstable();

        let mut rec = Recovery {
            report: OpenReport::default(),
            index: MemIndex::default(),
            damaged: Vec::new(),
            readers: BTreeMap::new(),
            device_id: None,
            max_sequence: 0,
            appendable: None,
        };
        for (i, &id) in ids.iter().enumerate() {
            Self::recover_segment(&cfg, id, i + 1 == ids.len(), &mut rec)?;
        }
        rec.index.finish_rebuild();

        let device_id = rec
            .device_id
            .unwrap_or_else(|| *uuid::Uuid::new_v4().as_bytes());
        let (active, active_min_seq, next_segment_id) =
            if let Some((header, valid_len, min_seq)) = rec.appendable {
                (
                    SegmentWriter::reopen(&cfg.dir, header, valid_len)?,
                    min_seq,
                    header.segment_id + 1,
                )
            } else {
                let id = ids.last().map_or(0, |last| last + 1);
                let w = Self::create_segment(&cfg, device_id, id)?;
                rec.readers.insert(
                    id,
                    SegmentInfo {
                        file: w.reader()?,
                        header: *w.header(),
                        min_seq: None,
                    },
                );
                (w, None, id + 1)
            };

        let start = (active.header().segment_id, active.end());
        rec.report.live_shards = rec.index.len();
        let quarantine_log = QuarantineLog::open(&cfg.dir)?;
        // Damaged records found by the scan are quarantined now that the
        // whole index is known (an older damaged record must not displace a
        // newer live one). Every entry is logged; replay is by re-scan.
        for (key, entry) in std::mem::take(&mut rec.damaged) {
            if rec.index.quarantine(key, entry) {
                quarantine_log.quarantine(&key, &entry)?;
                rec.report.quarantined.push((
                    entry.location.segment_id,
                    entry.location.offset,
                    entry.reason,
                ));
            }
        }
        rec.index.drain_released();
        let store = Self {
            cfg,
            device_id,
            writer: Mutex::new(Writer {
                active,
                active_min_seq,
                next_sequence: rec.max_sequence + 1,
                next_segment_id,
            }),
            index: RwLock::new(rec.index),
            readers: RwLock::new(rec.readers),
            durability: Durability {
                state: Mutex::new(DurState {
                    durable: start,
                    appended: start,
                    syncing: false,
                }),
                cv: Condvar::new(),
            },
            timings: StoreTimings::default(),
            quarantine_log,
        };
        Ok((store, rec.report))
    }

    /// Scan one segment and fold its records into the recovery state.
    fn recover_segment(
        cfg: &StoreConfig,
        id: u64,
        is_last: bool,
        rec: &mut Recovery,
    ) -> Result<()> {
        let path = segment_path(&cfg.dir, id);
        let file_len = std::fs::metadata(&path)?.len();

        // A last segment shorter than its own header is a crash during
        // segment creation: it cannot contain records. Discard it.
        if is_last && file_len < padded_header_len(cfg.record_alignment) {
            std::fs::remove_file(&path)?;
            File::open(&cfg.dir)?.sync_all()?;
            rec.report.truncated_bytes += file_len;
            return Ok(());
        }

        let scan = segment::scan(&path, id)?;
        rec.report.segments += 1;
        match rec.device_id {
            None => rec.device_id = Some(scan.header.device_id),
            Some(d) if d != scan.header.device_id => {
                return Err(StorageError::UnsupportedFormat(
                    "segments from different devices in one directory".into(),
                ));
            }
            Some(_) => {}
        }
        if scan.header.record_alignment != cfg.record_alignment
            || scan.header.block_size != cfg.block_size
        {
            return Err(StorageError::UnsupportedFormat(format!(
                "segment {id} geometry (alignment {}, block {}) differs from configuration",
                scan.header.record_alignment, scan.header.block_size
            )));
        }

        let mut min_seq: Option<u64> = None;
        for r in &scan.records {
            rec.max_sequence = rec.max_sequence.max(r.header.sequence);
            min_seq = Some(min_seq.map_or(r.header.sequence, |m| m.min(r.header.sequence)));
            let p = r.header.prefix();
            let superseded = if p.is_tombstone() {
                rec.index.tombstone(r.header.key, p.sequence).is_some()
            } else {
                let loc = Location {
                    segment_id: id,
                    offset: r.offset,
                    header_len: p.header_len() as u64,
                    payload_len: p.payload_len,
                    sequence: p.sequence,
                };
                rec.index.insert(r.header.key, loc).is_some()
            };
            if superseded {
                rec.report.superseded_records += 1;
            }
        }

        for d in &scan.damaged {
            let p = d.header.prefix();
            rec.max_sequence = rec.max_sequence.max(p.sequence);
            rec.damaged.push((
                d.header.key,
                QuarantineEntry {
                    location: Location {
                        segment_id: id,
                        offset: d.offset,
                        header_len: p.header_len() as u64,
                        payload_len: p.payload_len,
                        sequence: p.sequence,
                    },
                    reason: d.reason,
                    since_unix_ms: unix_ms(),
                },
            ));
        }
        for h in &scan.holes {
            rec.report.holes.push((id, h.offset, h.end, h.reason));
        }
        let mut appendable = is_last;
        match scan.end {
            ScanEnd::Clean => {}
            ScanEnd::TornTail { valid_len, .. } if is_last => {
                rec.report.truncated_bytes += scan.file_len - valid_len;
                segment::truncate(&path, valid_len)?;
            }
            ScanEnd::TornTail { valid_len, reason } | ScanEnd::Corrupt { valid_len, reason } => {
                rec.report.corrupt_segments.push((id, valid_len, reason));
                appendable = false;
            }
        }
        rec.readers.insert(
            id,
            SegmentInfo {
                file: File::open(&path)?,
                header: scan.header,
                min_seq,
            },
        );
        if appendable {
            rec.appendable = Some((scan.header, scan.valid_len, min_seq));
        }
        Ok(())
    }

    fn create_segment(cfg: &StoreConfig, device_id: [u8; 16], id: u64) -> Result<SegmentWriter> {
        let created_unix_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| u64::try_from(d.as_nanos()).ok())
            .unwrap_or(0);
        let header = SegmentHeader {
            format_version: SEGMENT_FORMAT_VERSION,
            record_alignment: cfg.record_alignment,
            block_size: cfg.block_size,
            device_id,
            segment_id: id,
            created_unix_nanos,
        };
        SegmentWriter::create(&cfg.dir, header)
    }

    /// Device UUID.
    #[must_use]
    pub fn device_id(&self) -> [u8; 16] {
        self.device_id
    }

    /// Store directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.cfg.dir
    }

    /// Write a shard. Returns after the record is durable (per [`SyncPolicy`]).
    /// Writing an existing key again supersedes the previous record.
    ///
    /// # Errors
    ///
    /// Fails on I/O errors or if `payload` exceeds `u32::MAX` bytes.
    pub fn put(&self, key: ShardKey, payload: &[u8]) -> Result<()> {
        self.append(key, 0, payload)
    }

    /// Delete a shard by writing a tombstone.
    ///
    /// # Errors
    ///
    /// Fails on I/O errors. Deleting a missing key is a durable no-op.
    pub fn delete(&self, key: ShardKey) -> Result<()> {
        self.append(key, FLAG_TOMBSTONE, &[])
    }

    /// Timing of writes since the store was opened.
    #[must_use]
    pub fn timings(&self) -> StoreTimingsSnapshot {
        StoreTimingsSnapshot {
            append: self.timings.append.snapshot(),
            rollover: self.timings.rollover.snapshot(),
            sync_wait: self.timings.sync_wait.snapshot(),
            fsync: self.timings.fsync.snapshot(),
        }
    }

    fn append(&self, key: ShardKey, flags: u8, payload: &[u8]) -> Result<()> {
        let mut w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let append_started = Instant::now();
        if w.active.end() >= self.cfg.segment_max_bytes {
            let t = Instant::now();
            self.rollover(&mut w)?;
            self.timings.rollover.observe(t.elapsed());
        }
        let sequence = w.next_sequence;
        let header = RecordHeader::for_payload(key, flags, sequence, payload, self.cfg.block_size)?;
        let offset = w.active.append(&header, payload)?;
        let segment_id = w.active.header().segment_id;
        let end = w.active.end();
        w.next_sequence += 1;
        w.active_min_seq.get_or_insert(sequence);
        {
            let mut st = self
                .durability
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            st.appended = (segment_id, end);
            if self.cfg.sync == SyncPolicy::Always {
                w.active.sync()?;
                st.durable = st.durable.max((segment_id, end));
            }
        }
        drop(w);
        self.timings.append.observe(append_started.elapsed());

        if self.cfg.sync == SyncPolicy::Group {
            let t = Instant::now();
            self.wait_durable((segment_id, end))?;
            self.timings.sync_wait.observe(t.elapsed());
        }

        let mut idx = self.index.write().unwrap_or_else(PoisonError::into_inner);
        if flags & FLAG_TOMBSTONE == 0 {
            idx.insert(
                key,
                Location {
                    segment_id,
                    offset,
                    header_len: header.prefix().header_len() as u64,
                    payload_len: header.payload_len,
                    sequence,
                },
            );
        } else {
            idx.tombstone(key, sequence);
        }
        self.log_released(&mut idx);
        Ok(())
    }

    /// Seal the active segment (making it fully durable) and start a new one.
    fn rollover(&self, w: &mut Writer) -> Result<()> {
        w.active.sync()?;
        let old_id = w.active.header().segment_id;
        let id = w.next_segment_id;
        let new = Self::create_segment(&self.cfg, self.device_id, id)?;
        let start = (id, new.end());
        {
            let mut readers = self.readers.write().unwrap_or_else(PoisonError::into_inner);
            if let Some(old) = readers.get_mut(&old_id) {
                old.min_seq = w.active_min_seq;
            }
            readers.insert(
                id,
                SegmentInfo {
                    file: new.reader()?,
                    header: *new.header(),
                    min_seq: None,
                },
            );
        }
        {
            let mut st = self
                .durability
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            st.durable = st.durable.max(start);
            st.appended = st.appended.max(start);
        }
        w.active = new;
        w.active_min_seq = None;
        w.next_segment_id = id + 1;
        Ok(())
    }

    /// Group commit: wait until everything up to `pos` is durable, syncing on
    /// behalf of all waiters if nobody else is.
    fn wait_durable(&self, pos: (u64, u64)) -> Result<()> {
        let mut st = self
            .durability
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        loop {
            if st.durable >= pos {
                return Ok(());
            }
            if st.syncing {
                st = self
                    .durability
                    .cv
                    .wait(st)
                    .unwrap_or_else(PoisonError::into_inner);
                continue;
            }
            st.syncing = true;
            let target = st.appended;
            drop(st);

            let result = (|| -> Result<()> {
                let file = {
                    let readers = self.readers.read().unwrap_or_else(PoisonError::into_inner);
                    readers
                        .get(&target.0)
                        .ok_or(StorageError::NotFound)?
                        .file
                        .try_clone()?
                };
                let t = Instant::now();
                file.sync_data()?;
                self.timings.fsync.observe(t.elapsed());
                Ok(())
            })();

            st = self
                .durability
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            st.syncing = false;
            if result.is_ok() {
                st.durable = st.durable.max(target);
            }
            self.durability.cv.notify_all();
            result?;
        }
    }

    /// Whether `key` is live.
    #[must_use]
    pub fn contains(&self, key: &ShardKey) -> bool {
        self.locate(key).is_some()
    }

    /// Location of a live shard.
    #[must_use]
    pub fn locate(&self, key: &ShardKey) -> Option<Location> {
        self.index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
    }

    /// Read a shard, or a byte range of it, verifying the checksum blocks
    /// that cover the range.
    ///
    /// # Errors
    ///
    /// [`StorageError::NotFound`] if the key is not live;
    /// [`StorageError::Corrupt`] if any covered block fails its CRC (the
    /// shard is then dropped from the index); [`StorageError::InvalidArgument`]
    /// if the range is out of bounds.
    pub fn get(&self, key: &ShardKey, range: Option<Range<u64>>) -> Result<Vec<u8>> {
        let loc = self.locate(key).ok_or(StorageError::NotFound)?;
        let total = u64::from(loc.payload_len);
        let range = range.unwrap_or(0..total);
        if range.start > range.end || range.end > total {
            return Err(StorageError::InvalidArgument(format!(
                "range {range:?} outside 0..{total}"
            )));
        }
        if range.is_empty() {
            return Ok(Vec::new());
        }
        let block = u64::from(self.cfg.block_size);
        let first_block = range.start / block;
        let last_block = (range.end - 1) / block;
        let read_start = first_block * block;
        let read_end = ((last_block + 1) * block).min(total);

        let readers = self.readers.read().unwrap_or_else(PoisonError::into_inner);
        let Some(seg) = readers.get(&loc.segment_id) else {
            // The segment was compacted away after we looked up the index;
            // the index now points elsewhere. Retry once.
            drop(readers);
            return match self.locate(key) {
                Some(l) if l != loc => self.get(key, Some(range)),
                _ => Err(StorageError::NotFound),
            };
        };
        let corrupt = |reason| StorageError::Corrupt {
            segment_id: loc.segment_id,
            offset: loc.offset,
            reason,
        };

        // Re-read and validate the header so that a stale index entry can
        // never return bytes from the wrong record.
        let mut hbuf = vec![0u8; to_usize(loc.header_len)];
        if let Err(e) = seg.file.read_exact_at(&mut hbuf, loc.offset) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Err(self.quarantine(key, loc, "record header truncated"));
            }
            return Err(e.into());
        }
        let header = match RecordHeader::decode(&hbuf, seg.header.block_size) {
            Ok(h) if h.key == *key && h.sequence == loc.sequence => h,
            Ok(_) => {
                return Err(self.quarantine(key, loc, "record does not match index"));
            }
            Err(reason) => return Err(self.quarantine(key, loc, reason)),
        };

        let mut buf = vec![0u8; to_usize(read_end - read_start)];
        if let Err(e) = seg
            .file
            .read_exact_at(&mut buf, loc.payload_offset() + read_start)
        {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                // The record extends past the segment: truncated on disk.
                return Err(self.quarantine(key, loc, "record truncated"));
            }
            return Err(e.into());
        }
        if let Err(reason) = verify_blocks(
            &buf,
            to_usize(first_block),
            &header.block_crcs,
            seg.header.block_size,
        ) {
            return Err(self.quarantine(key, loc, reason));
        }
        let _ = corrupt;
        buf.drain(..to_usize(range.start - read_start));
        buf.truncate(to_usize(range.end - range.start));
        Ok(buf)
    }

    /// Quarantine the record at `loc` for `key` durably (RFC-005 §5.6a):
    /// the event is logged and fsynced, then the key leaves the live index.
    /// Returns the `Corrupt` error to report to the caller.
    fn quarantine(&self, key: &ShardKey, loc: Location, reason: &'static str) -> StorageError {
        let entry = QuarantineEntry {
            location: loc,
            reason,
            since_unix_ms: unix_ms(),
        };
        // A log write failure must not hide the corruption from the caller
        // nor keep serving the bytes: the index change still happens.
        let _ = self.quarantine_log.quarantine(key, &entry);
        self.index
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .quarantine(*key, entry);
        StorageError::Corrupt {
            segment_id: loc.segment_id,
            offset: loc.offset,
            reason,
        }
    }

    /// Verify a shard end to end (every block) without returning the bytes.
    ///
    /// # Errors
    ///
    /// As [`Store::get`]: a `Corrupt` shard is quarantined.
    pub fn verify(&self, key: &ShardKey) -> Result<Verified> {
        let loc = self.locate(key).ok_or(StorageError::NotFound)?;
        let data = self.get(key, None)?;
        Ok(Verified {
            payload_len: loc.payload_len,
            crc32c: crate::format::crc32c(&data),
            sequence: loc.sequence,
        })
    }

    /// The quarantine entry of `key`, if it is quarantined.
    #[must_use]
    pub fn quarantine_entry(&self, key: &ShardKey) -> Option<QuarantineEntry> {
        self.index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .quarantine_entry(key)
    }

    /// Quarantined shards of `group_id` after `after`, in key order, at
    /// most `limit`.
    #[must_use]
    pub fn quarantined_in_group_after(
        &self,
        group_id: u64,
        after: Option<ShardKey>,
        limit: usize,
    ) -> Vec<(ShardKey, QuarantineEntry)> {
        self.index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .quarantined_in_group_after(group_id, after, limit)
            .collect()
    }

    /// Every quarantined shard.
    #[must_use]
    pub fn quarantined(&self) -> Vec<(ShardKey, QuarantineEntry)> {
        self.index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .quarantined()
            .map(|(k, q)| (*k, *q))
            .collect()
    }

    /// Number of quarantined shards and their payload bytes.
    #[must_use]
    pub fn quarantine_stats(&self) -> (usize, u64) {
        self.index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .quarantine_stats()
    }

    /// The quarantine log's lines (operators and tests).
    ///
    /// # Errors
    ///
    /// I/O errors.
    pub fn quarantine_log(&self) -> Result<Vec<String>> {
        self.quarantine_log.lines()
    }

    /// Log the quarantine entries released by the last index change.
    fn log_released(&self, idx: &mut MemIndex) {
        for (key, q, cause) in idx.drain_released() {
            let _ = self.quarantine_log.release(&key, &q, cause);
        }
    }

    /// Live keys in `group_id`.
    #[must_use]
    pub fn keys_in_group(&self, group_id: u64) -> Vec<ShardKey> {
        self.index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys_in_group(group_id)
            .collect()
    }

    /// Live shards of `group_id` after `after`, in key order, with their
    /// payload lengths; at most `limit` (paging for the node API).
    #[must_use]
    pub fn entries_in_group_after(
        &self,
        group_id: u64,
        after: Option<ShardKey>,
        limit: usize,
    ) -> Vec<(ShardKey, u32)> {
        self.index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .entries_in_group_after(group_id, after, limit)
            .collect()
    }

    /// All live keys.
    #[must_use]
    pub fn keys(&self) -> Vec<ShardKey> {
        self.index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(k, _)| *k)
            .collect()
    }

    /// Verify every live shard end to end.
    ///
    /// # Errors
    ///
    /// Fails only on I/O errors; corruption is reported, not returned.
    pub fn scrub(&self) -> Result<ScrubReport> {
        let mut report = ScrubReport::default();
        for key in self.keys() {
            match self.get(&key, None) {
                Ok(_) => report.verified += 1,
                Err(StorageError::Corrupt { .. }) => report.corrupt.push(key),
                Err(StorageError::NotFound) => {}
                Err(e) => return Err(e),
            }
        }
        report.quarantined = self.quarantine_stats().0;
        Ok(report)
    }

    /// Capacity accounting.
    ///
    /// # Errors
    ///
    /// Fails on I/O errors while reading segment sizes.
    pub fn stats(&self) -> Result<StoreStats> {
        let idx = self.index.read().unwrap_or_else(PoisonError::into_inner);
        let readers = self.readers.read().unwrap_or_else(PoisonError::into_inner);
        let mut segment_bytes = 0;
        for info in readers.values() {
            segment_bytes += info.file.metadata()?.len();
        }
        Ok(StoreStats {
            live_shards: idx.len(),
            live_bytes: idx.live_bytes(),
            segment_bytes,
            segments: readers.len(),
        })
    }

    /// Per-segment accounting, in segment order.
    ///
    /// # Errors
    ///
    /// Fails on I/O errors while reading segment sizes.
    pub fn segment_stats(&self) -> Result<Vec<SegmentStats>> {
        let active = self
            .writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .active
            .header()
            .segment_id;
        let idx = self.index.read().unwrap_or_else(PoisonError::into_inner);
        let readers = self.readers.read().unwrap_or_else(PoisonError::into_inner);
        let mut out = Vec::with_capacity(readers.len());
        for (&id, info) in readers.iter() {
            let live = idx.segment_live(id);
            out.push(SegmentStats {
                segment_id: id,
                file_bytes: info.file.metadata()?.len(),
                live_bytes: live.bytes,
                live_records: live.records,
                active: id == active,
            });
        }
        Ok(out)
    }

    /// Sealed segments whose dead fraction (bytes not referenced by live
    /// records, relative to file size) is at least `min_dead_fraction`.
    ///
    /// # Errors
    ///
    /// Fails on I/O errors while reading segment sizes.
    #[allow(clippy::cast_precision_loss)]
    pub fn compaction_candidates(&self, min_dead_fraction: f64) -> Result<Vec<u64>> {
        Ok(self
            .segment_stats()?
            .into_iter()
            .filter(|s| !s.active && s.file_bytes > 0)
            .filter(|s| {
                let dead = s.file_bytes.saturating_sub(s.live_bytes) as f64 / s.file_bytes as f64;
                dead >= min_dead_fraction
            })
            .map(|s| s.segment_id)
            .collect())
    }

    /// Compact a sealed segment: copy its live records (and the tombstones
    /// that may still be needed) into a new segment, retarget the index, and
    /// delete the old file.
    ///
    /// Safe to interrupt at any point: the index rebuild rule makes duplicate
    /// records harmless, and the source file is removed only after the copy
    /// is durable and the index no longer points at it. Re-running
    /// compaction on the same segment completes the work.
    ///
    /// # Errors
    ///
    /// [`StorageError::InvalidArgument`] for the active or an unknown
    /// segment; I/O errors otherwise.
    pub fn compact(&self, segment_id: u64) -> Result<CompactionReport> {
        self.compact_with_hooks(segment_id, &mut |_| Ok(()))
    }

    /// [`Store::compact`] with a fault-injection hook called at each
    /// [`CompactionStep`]. An error returned by the hook aborts compaction
    /// at that point and is returned; the store remains consistent.
    ///
    /// # Errors
    ///
    /// As [`Store::compact`], plus whatever the hook returns.
    #[allow(clippy::too_many_lines)]
    pub fn compact_with_hooks(
        &self,
        segment_id: u64,
        hooks: &mut dyn FnMut(CompactionStep) -> Result<()>,
    ) -> Result<CompactionReport> {
        // Reserve an output ID and learn the active segment's state.
        let (output_id, active_id, active_min_seq) = {
            let mut w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
            if w.active.header().segment_id == segment_id {
                return Err(StorageError::InvalidArgument(
                    "cannot compact the active segment".into(),
                ));
            }
            let id = w.next_segment_id;
            w.next_segment_id += 1;
            (id, w.active.header().segment_id, w.active_min_seq)
        };

        let (source, other_min_seq) = {
            let readers = self.readers.read().unwrap_or_else(PoisonError::into_inner);
            let Some(info) = readers.get(&segment_id) else {
                return Err(StorageError::InvalidArgument(format!(
                    "unknown segment {segment_id}"
                )));
            };
            let other_min = readers
                .iter()
                .filter(|&(&id, _)| id != segment_id)
                .filter_map(|(&id, i)| {
                    if id == active_id {
                        active_min_seq
                    } else {
                        i.min_seq
                    }
                })
                .min();
            (info.file.try_clone()?, other_min)
        };

        let path = segment_path(&self.cfg.dir, segment_id);
        let scan = segment::scan(&path, segment_id)?;
        let mut report = CompactionReport {
            segment_id,
            ..Default::default()
        };
        let mut output: Option<SegmentWriter> = None;
        let mut moved: Vec<(ShardKey, Location, Location)> = Vec::new();
        let mut out_min_seq: Option<u64> = None;

        for r in &scan.records {
            let p = r.header.prefix();
            let (copy, payload) = if p.is_tombstone() {
                // Needed as long as an older record for the key may exist in
                // another segment.
                let keep = other_min_seq.is_some_and(|m| m < p.sequence);
                if keep {
                    report.tombstones_kept += 1;
                } else {
                    report.tombstones_dropped += 1;
                }
                (keep, Vec::new())
            } else {
                let old = Location {
                    segment_id,
                    offset: r.offset,
                    header_len: p.header_len() as u64,
                    payload_len: p.payload_len,
                    sequence: p.sequence,
                };
                let live = self.locate(&r.header.key) == Some(old);
                if !live {
                    // A quarantined record of this segment loses its bytes
                    // with the segment: release the entry.
                    let mut idx = self.index.write().unwrap_or_else(PoisonError::into_inner);
                    if idx.purge_quarantine(&r.header.key, &old) {
                        self.log_released(&mut idx);
                    }
                }
                if live {
                    let mut payload = vec![0u8; to_usize(u64::from(p.payload_len))];
                    source.read_exact_at(&mut payload, old.payload_offset())?;
                    if let Err(reason) =
                        verify_blocks(&payload, 0, &r.header.block_crcs, scan.header.block_size)
                    {
                        return Err(StorageError::Corrupt {
                            segment_id,
                            offset: r.offset,
                            reason,
                        });
                    }
                    (true, payload)
                } else {
                    report.dropped_records += 1;
                    (false, Vec::new())
                }
            };
            if !copy {
                continue;
            }
            let out = match output.as_mut() {
                Some(o) => o,
                None => output.insert(Self::create_segment(&self.cfg, self.device_id, output_id)?),
            };
            let new_offset = out.append(&r.header, &payload)?;
            out_min_seq = Some(out_min_seq.map_or(p.sequence, |m| m.min(p.sequence)));
            if !p.is_tombstone() {
                let old = Location {
                    segment_id,
                    offset: r.offset,
                    header_len: p.header_len() as u64,
                    payload_len: p.payload_len,
                    sequence: p.sequence,
                };
                let new = Location {
                    segment_id: output_id,
                    offset: new_offset,
                    ..old
                };
                moved.push((r.header.key, old, new));
                report.copied_records += 1;
                report.copied_bytes += u64::from(p.payload_len);
            }
            hooks(CompactionStep::Copied(moved.len()))?;
        }

        for d in &scan.damaged {
            let p = d.header.prefix();
            let loc = Location {
                segment_id,
                offset: d.offset,
                header_len: p.header_len() as u64,
                payload_len: p.payload_len,
                sequence: p.sequence,
            };
            let mut idx = self.index.write().unwrap_or_else(PoisonError::into_inner);
            if idx.purge_quarantine(&d.header.key, &loc) {
                self.log_released(&mut idx);
            }
            report.dropped_records += 1;
        }
        if let Some(out) = &output {
            out.sync()?;
            File::open(&self.cfg.dir)?.sync_all()?;
            report.output_segment_id = Some(output_id);
            self.readers
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(
                    output_id,
                    SegmentInfo {
                        file: out.reader()?,
                        header: *out.header(),
                        min_seq: out_min_seq,
                    },
                );
        }
        hooks(CompactionStep::Synced)?;

        {
            let mut idx = self.index.write().unwrap_or_else(PoisonError::into_inner);
            for (key, old, new) in &moved {
                idx.relocate(key, old, *new);
            }
        }
        hooks(CompactionStep::IndexSwapped)?;

        // Readers that already hold the file keep reading it (POSIX unlink
        // semantics); new reads go through the index, which no longer points
        // here.
        self.readers
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&segment_id);
        std::fs::remove_file(&path)?;
        File::open(&self.cfg.dir)?.sync_all()?;
        hooks(CompactionStep::Unlinked)?;
        Ok(report)
    }
}

/// Lengths handled here are bounded by `u32` payloads and small headers, so
/// they always fit in `usize` on supported (64-bit) platforms.
fn to_usize(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}
