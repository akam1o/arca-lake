//! The per-device store: open/recover, put, get, delete, compaction, scrub.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::Instant;

use arca_lake_common::timing::{Histogram, HistogramSnapshot};
use std::sync::{Condvar, Mutex, PoisonError, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, StorageError};
use crate::faults;
use crate::format::{
    FLAG_TOMBSTONE, RecordHeader, SEGMENT_FORMAT_VERSION, SEGMENT_FORMAT_VERSION_MIN,
    SegmentHeader, ShardKey, padded_header_len, validate_geometry,
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
    /// Segment format version stamped on new segments: the cluster's
    /// `format_write` (RFC-014 §5.3), clamped into
    /// `[SEGMENT_FORMAT_VERSION_MIN, SEGMENT_FORMAT_VERSION]`. A node that
    /// never heard a value writes `F_cur − 1` (clamped), so that a
    /// not-yet-upgraded reader can still open what it writes.
    pub format_write: u16,
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
            format_write: default_format_write(),
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
    /// The sequence bound file was damaged and ignored (safe: every open
    /// takes a new store epoch; sequences restart above the surviving
    /// records).
    pub sequence_bound_damaged: bool,
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
    written: std::sync::atomic::AtomicU64,
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
            written: std::sync::atomic::AtomicU64::new(0),
        })
    }

    fn write(&self, line: &str) -> Result<()> {
        let mut f = self.file.lock().unwrap_or_else(PoisonError::into_inner);
        let mut buf = line.as_bytes().to_vec();
        buf.push(b'\n');
        faults::write_all(&mut f, &buf, &faults::IoOp::QuarantineLog)?;
        faults::guard(&faults::IoOp::QuarantineLog)?;
        f.sync_data()?;
        self.written
            .fetch_add(buf.len() as u64, std::sync::atomic::Ordering::Relaxed);
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
    /// A compaction found nothing to reclaim (its tombstones are still
    /// needed) and nothing changed since: compacting it again would only
    /// rewrite the same records. Compaction schedulers skip it.
    pub blocked: bool,
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
    /// Nothing could be reclaimed (no dead record, every tombstone still
    /// needed): the segment was left as it was, nothing was written.
    pub no_progress: bool,
}

#[derive(Debug)]
struct Writer {
    active: SegmentWriter,
    /// Set after a write or sync of the active segment failed: the next
    /// append rolls to a new segment first, so that no later ACK depends on
    /// dirty pages whose fate a failed fsync left unknown (R-53).
    poisoned: bool,
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

/// `F_cur − 1` clamped to the oldest writable format: what a node writes
/// until the controller tells it the cluster's `format_write`.
#[must_use]
pub const fn default_format_write() -> u16 {
    if SEGMENT_FORMAT_VERSION > SEGMENT_FORMAT_VERSION_MIN {
        SEGMENT_FORMAT_VERSION - 1
    } else {
        SEGMENT_FORMAT_VERSION_MIN
    }
}

/// Clamp a cluster `format_write` into what this build can write.
#[must_use]
pub fn clamp_format_write(v: u64) -> u16 {
    u16::try_from(v.clamp(
        u64::from(SEGMENT_FORMAT_VERSION_MIN),
        u64::from(SEGMENT_FORMAT_VERSION),
    ))
    .unwrap_or(SEGMENT_FORMAT_VERSION_MIN)
}

/// Current length of a file (0 when unknown).
fn file_len(f: &File) -> u64 {
    f.metadata().map_or(0, |m| m.len())
}

/// A single-device store.
#[derive(Debug)]
pub struct Store {
    cfg: StoreConfig,
    device_id: [u8; 16],
    /// Store epoch: half of every delete token ([`Store::store_epoch`]).
    epoch: u64,
    /// Exclusive lock on the directory, held while the store is open.
    _lock: File,
    writer: Mutex<Writer>,
    index: RwLock<MemIndex>,
    readers: RwLock<BTreeMap<u64, SegmentInfo>>,
    /// Segments whose last compaction made no progress:
    /// `segment → (keys of the tombstones it had to keep, live records
    /// then)`. Retried once one of those tombstones can go (no other
    /// segment holds a non-live record of its key any more) or its live
    /// records changed.
    compaction_blocked: Mutex<HashMap<u64, (Vec<ShardKey>, usize)>>,
    /// Data records appended and not yet in the index (waiting for group
    /// commit): `key → sequence`. Inserted under the writer lock, removed
    /// after the index took the record, so that [`Self::delete_if`] sees
    /// every record appended before it.
    appending: Mutex<HashMap<ShardKey, u64>>,
    /// Sequences appended and not yet applied to the index (any record):
    /// the index's runtime tombstone history below the lowest is dropped
    /// (CON-9, R-70).
    inflight: Mutex<std::collections::BTreeSet<u64>>,
    durability: Durability,
    timings: StoreTimings,
    quarantine_log: QuarantineLog,
    /// Format stamped on the next new segment (RFC-014 §5.3).
    format_write: std::sync::atomic::AtomicU16,
    /// Bytes written to the device by kind (Phase 3.8 capacity breakdown).
    written: Written,
}

/// Bytes the store wrote since it was opened, by kind.
#[derive(Debug, Default)]
struct Written {
    data: std::sync::atomic::AtomicU64,
    tombstone: std::sync::atomic::AtomicU64,
    rollover: std::sync::atomic::AtomicU64,
    compaction: std::sync::atomic::AtomicU64,
    sidecar: std::sync::atomic::AtomicU64,
}

/// Bytes the store wrote to its device since it was opened, by kind
/// (record padding included: what the file system allocates for them).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WrittenBytes {
    /// Data records (shard writes of every class).
    pub data: u64,
    /// Tombstone records (deletes).
    pub tombstone: u64,
    /// New segment headers written by rollovers.
    pub rollover: u64,
    /// Compaction output (records copied and the output's header).
    pub compaction: u64,
    /// Quarantine log lines.
    pub quarantine_log: u64,
    /// Side files (the sequence bound).
    pub sidecar: u64,
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
    #[allow(clippy::too_many_lines)]
    pub fn open(cfg: StoreConfig) -> Result<(Self, OpenReport)> {
        validate_geometry(cfg.record_alignment, cfg.block_size)?;
        let cfg_format_write = cfg.format_write;
        std::fs::create_dir_all(&cfg.dir)?;
        // One store per directory, for the store's whole life (Phase 3.5
        // audit CON-19: two stores on one directory would write segments
        // and hand out sequences independently).
        let lock = File::create(cfg.dir.join(LOCK_FILE))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!("{} is in use by another store", cfg.dir.display()),
                )
                .into());
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
        }

        // Compaction outputs that never got their segment name: never
        // referenced by an index (their source still holds every record).
        let mut removed_temp = false;
        for e in std::fs::read_dir(&cfg.dir)?.filter_map(std::result::Result::ok) {
            if let Some(id) = e
                .file_name()
                .to_str()
                .and_then(segment::parse_temp_segment_name)
            {
                faults::guard(&faults::IoOp::Remove { segment: id })?;
                std::fs::remove_file(e.path())?;
                removed_temp = true;
            }
        }
        if removed_temp {
            faults::sync_all(&File::open(&cfg.dir)?, "dir")?;
        }
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
                let w = Self::create_segment(
                    &cfg,
                    device_id,
                    id,
                    clamp_format_write(u64::from(cfg_format_write)),
                )?;
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
        // The tombstone history goes only now: the damaged records above
        // were checked against it (Phase 3.5 audit OPS-4: an older damaged
        // record of a deleted key was quarantined).
        rec.index.finish_rebuild();
        // A delete token is (store epoch, sequence), and every open takes a
        // new epoch: no token handed out before can match a record written
        // after, whatever the directory went through in between (compaction
        // dropping the highest sequences, a lost bound, a restored older
        // copy; Phase 3.5 audit CON-10, CON-16, CON-18). A token from
        // before a restart is refused and the caller lists again. The bound
        // only keeps sequences increasing across opens (diagnostics).
        let store_epoch = new_store_epoch();
        let (bound, damaged) = read_sequence_bound(&cfg.dir);
        rec.report.sequence_bound_damaged = damaged;
        let next_sequence = reserve(rec.max_sequence, 1)?.max(bound);
        write_sequence_bound(
            &cfg.dir,
            reserve(next_sequence, sequences_per_segment(&cfg))?,
            store_epoch,
        )?;
        let store = Self {
            cfg,
            device_id,
            epoch: store_epoch,
            _lock: lock,
            writer: Mutex::new(Writer {
                active,
                poisoned: false,
                active_min_seq,
                next_sequence,
                next_segment_id,
            }),
            index: RwLock::new(rec.index),
            readers: RwLock::new(rec.readers),
            compaction_blocked: Mutex::new(HashMap::new()),
            appending: Mutex::new(HashMap::new()),
            inflight: Mutex::new(std::collections::BTreeSet::new()),
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
            format_write: std::sync::atomic::AtomicU16::new(clamp_format_write(u64::from(
                cfg_format_write,
            ))),
            written: Written::default(),
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

        // A segment shorter than its own header is a crash (or a failed
        // write) during segment creation, wherever it sorts: it cannot
        // contain records. Discard it.
        if file_len < padded_header_len(cfg.record_alignment) {
            let _ = is_last;
            faults::guard(&faults::IoOp::Remove { segment: id })?;
            std::fs::remove_file(&path)?;
            faults::sync_all(&File::open(&cfg.dir)?, "dir")?;
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
                segment::truncate(&path, valid_len, id)?;
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

    fn create_segment(
        cfg: &StoreConfig,
        device_id: [u8; 16],
        id: u64,
        format_version: u16,
    ) -> Result<SegmentWriter> {
        SegmentWriter::create(
            &cfg.dir,
            Self::new_segment_header(cfg, device_id, id, format_version),
        )
    }

    fn new_segment_header(
        cfg: &StoreConfig,
        device_id: [u8; 16],
        id: u64,
        format_version: u16,
    ) -> SegmentHeader {
        let created_unix_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| u64::try_from(d.as_nanos()).ok())
            .unwrap_or(0);
        SegmentHeader {
            format_version,
            record_alignment: cfg.record_alignment,
            block_size: cfg.block_size,
            device_id,
            segment_id: id,
            created_unix_nanos,
        }
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
        self.put_sequenced(key, payload).map(|_| ())
    }

    /// [`Self::put`], returning the record's sequence (what a later
    /// [`Self::delete_if`] names).
    ///
    /// # Errors
    ///
    /// As [`Self::put`].
    pub fn put_sequenced(&self, key: ShardKey, payload: &[u8]) -> Result<u64> {
        self.append_if(key, 0, payload, None)
            .map(Option::unwrap_or_default)
    }

    /// Delete `key` only if its newest record (live or quarantined, or
    /// appended and not yet indexed) has sequence `sequence`; `false` when
    /// it has another one or none (Phase 3.5 audit CON-5/CON-6: a delete
    /// decided on a listing must not remove a record written since under
    /// the same key). Checked and written under the writer lock, so no
    /// record can be appended in between.
    ///
    /// # Errors
    ///
    /// Fails on I/O errors.
    pub fn delete_if(&self, key: ShardKey, sequence: u64) -> Result<bool> {
        self.append_if(key, FLAG_TOMBSTONE, &[], Some(sequence))
            .map(|s| s.is_some())
    }

    /// Delete a shard by writing a tombstone.
    ///
    /// # Errors
    ///
    /// Fails on I/O errors. Deleting a missing key is a durable no-op.
    pub fn delete(&self, key: ShardKey) -> Result<()> {
        self.append(key, FLAG_TOMBSTONE, &[])
    }

    /// Bytes written to the device since the store was opened, by kind.
    #[must_use]
    pub fn written(&self) -> WrittenBytes {
        use std::sync::atomic::Ordering::Relaxed;
        WrittenBytes {
            data: self.written.data.load(Relaxed),
            tombstone: self.written.tombstone.load(Relaxed),
            rollover: self.written.rollover.load(Relaxed),
            compaction: self.written.compaction.load(Relaxed),
            quarantine_log: self.quarantine_log.written.load(Relaxed),
            sidecar: self.written.sidecar.load(Relaxed),
        }
    }

    /// Bytes one tombstone takes on the device (its padded record).
    #[must_use]
    pub fn tombstone_bytes(&self) -> u64 {
        crate::format::round_up(
            crate::format::RECORD_FIXED_LEN as u64 + 4,
            u64::from(self.cfg.record_alignment),
        )
    }

    /// Bytes of the store directory's files other than segments
    /// (quarantine log, sequence bound, temporary files).
    #[must_use]
    pub fn side_file_bytes(&self) -> u64 {
        let Ok(rd) = std::fs::read_dir(&self.cfg.dir) else {
            return 0;
        };
        rd.filter_map(std::result::Result::ok)
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_none_or(|n| segment::parse_segment_name(n).is_none())
            })
            .filter_map(|e| e.metadata().ok())
            .filter(std::fs::Metadata::is_file)
            .map(|m| m.len())
            .sum()
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
        self.append_if(key, flags, payload, None).map(|_| ())
    }

    /// The newest sequence of `key` the store knows (appended, live or
    /// quarantined). Call with the writer lock held.
    fn newest_sequence(&self, key: &ShardKey) -> Option<u64> {
        if let Some(&s) = self
            .appending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
        {
            return Some(s);
        }
        let idx = self.index.read().unwrap_or_else(PoisonError::into_inner);
        idx.get(key)
            .map(|l| l.sequence)
            .or_else(|| idx.quarantine_entry(key).map(|q| q.location.sequence))
    }

    /// Append a record; with `if_newest`, only if the key's newest record
    /// has that sequence (`Ok(None)` otherwise). Returns the sequence.
    #[allow(clippy::too_many_lines)]
    fn append_if(
        &self,
        key: ShardKey,
        flags: u8,
        payload: &[u8],
        if_newest: Option<u64>,
    ) -> Result<Option<u64>> {
        let mut w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(expected) = if_newest
            && self.newest_sequence(&key) != Some(expected)
        {
            return Ok(None);
        }
        // A delete of a key with no record (never written, or deleted
        // already): nothing to cover, so no tombstone (Phase 3.8 review:
        // repeated deletes of absent keys spent the delete reserve).
        // Non-live records of the key on disk are covered by the tombstone
        // that made them non-live, which compaction keeps while they exist.
        if flags & FLAG_TOMBSTONE != 0
            && if_newest.is_none()
            && self.newest_sequence(&key).is_none()
        {
            return Ok(Some(0));
        }
        let append_started = Instant::now();
        if w.active.end() >= self.cfg.segment_max_bytes || w.poisoned {
            let t = Instant::now();
            // A poisoned segment is sealed without syncing it again: its
            // last acknowledged position was synced before the failure.
            self.rollover(&mut w)?;
            w.poisoned = false;
            self.timings.rollover.observe(t.elapsed());
        }
        let sequence = w.next_sequence;
        if sequence == u64::MAX {
            // Never wrap (CON-17): a reused sequence could match a token.
            return Err(std::io::Error::other("record sequences exhausted").into());
        }
        let header = RecordHeader::for_payload(key, flags, sequence, payload, self.cfg.block_size)?;
        let before = w.active.end();
        let offset = match w.active.append(&header, payload) {
            Ok(o) => o,
            Err(e) => {
                w.poisoned = true;
                return Err(e);
            }
        };
        let segment_id = w.active.header().segment_id;
        let end = w.active.end();
        let kind = if flags & FLAG_TOMBSTONE == 0 {
            &self.written.data
        } else {
            &self.written.tombstone
        };
        kind.fetch_add(end - before, std::sync::atomic::Ordering::Relaxed);
        w.next_sequence += 1;
        w.active_min_seq.get_or_insert(sequence);
        // Forgets the record in `appending` and `inflight` on every way out,
        // a panic included (Phase 3.5 audit CON-13: a sequence left in
        // `inflight` would stop the tombstone history from being pruned).
        let mut guard = Inflight {
            store: self,
            key,
            sequence,
            applied: false,
        };
        let data = flags & FLAG_TOMBSTONE == 0;
        self.inflight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(sequence);
        {
            let mut a = self
                .appending
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if data {
                a.insert(key, sequence);
            } else {
                a.remove(&key);
            }
        }
        {
            let mut st = self
                .durability
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            st.appended = (segment_id, end);
            if self.cfg.sync == SyncPolicy::Always {
                if let Err(e) = w.active.sync() {
                    w.poisoned = true;
                    drop(st);
                    return Err(e);
                }
                st.durable = st.durable.max((segment_id, end));
            }
        }
        drop(w);
        self.timings.append.observe(append_started.elapsed());

        if self.cfg.sync == SyncPolicy::Group {
            let t = Instant::now();
            if let Err(e) = self.wait_durable((segment_id, end)) {
                self.writer
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .poisoned = true;
                return Err(e);
            }
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
        // Applied: history below every record still on its way can go.
        let lowest = guard.applied();
        idx.prune_history(lowest);
        drop(idx);
        drop(guard);
        Ok(Some(sequence))
    }

    /// Seal the active segment (making it fully durable) and start a new one.
    fn rollover(&self, w: &mut Writer) -> Result<()> {
        w.active.sync()?;
        let old_id = w.active.header().segment_id;
        // Consume the id first: a creation that fails after the file exists
        // (a write or fsync error) must not make every later rollover try
        // the same name again (`EEXIST` forever, found by the soak test).
        let id = w.next_segment_id;
        w.next_segment_id = id + 1;
        let new = Self::create_segment(&self.cfg, self.device_id, id, self.format_write())?;
        self.written
            .rollover
            .fetch_add(new.end(), std::sync::atomic::Ordering::Relaxed);
        // Every sequence the new segment can take stays below the bound.
        write_sequence_bound(
            &self.cfg.dir,
            reserve(w.next_sequence, sequences_per_segment(&self.cfg))?,
            self.epoch,
        )?;
        self.written.sidecar.fetch_add(
            std::fs::metadata(self.cfg.dir.join(SEQUENCE_BOUND_FILE)).map_or(0, |m| m.len()),
            std::sync::atomic::Ordering::Relaxed,
        );
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
            // Gives up the leadership if the sync below panics, so that the
            // waiting writers elect another leader (Phase 3.5 audit CON-15).
            let leader = SyncLeader {
                store: self,
                done: false,
            };

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
                faults::sync_data(&file, target.0)?;
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
            leader.finish();
            result?;
        }
    }

    /// Set the format stamped on new segments (the cluster's `format_write`,
    /// RFC-014 §5.3); takes effect at the next rollover. Returns the value
    /// in force after clamping.
    pub fn set_format_write(&self, v: u64) -> u16 {
        let c = clamp_format_write(v);
        self.format_write
            .store(c, std::sync::atomic::Ordering::Relaxed);
        c
    }

    /// Seal the active segment and start one of `format_write` now, if the
    /// active one is of an older format (Phase 3.10): a raised format then
    /// takes effect at once, so that an earlier release refuses the store
    /// from the moment the raise is acknowledged.
    ///
    /// # Errors
    ///
    /// I/O errors of the rollover.
    pub fn roll_to_format_write(&self) -> Result<()> {
        let mut w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        if w.active.header().format_version >= self.format_write() {
            return Ok(());
        }
        let t = Instant::now();
        self.rollover(&mut w)?;
        self.timings.rollover.observe(t.elapsed());
        Ok(())
    }

    /// The format stamped on new segments.
    #[must_use]
    pub fn format_write(&self) -> u16 {
        self.format_write.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Newest segment format version present on the device (the version a
    /// downgrade target must read, `storage-node --check-downgrade`).
    ///
    /// # Errors
    ///
    /// I/O errors.
    pub fn newest_segment_format(&self) -> Result<u16> {
        Ok(self
            .readers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|s| s.header.format_version)
            .max()
            .unwrap_or(SEGMENT_FORMAT_VERSION_MIN))
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
        self.read(key, range, true)
    }

    /// [`Store::get`] for read-only verification: a corrupt record is
    /// reported (`Corrupt`) but not quarantined; nothing on the device or
    /// in the index changes (`admin integrity verify-object`, Phase 3.4).
    ///
    /// # Errors
    ///
    /// As [`Store::get`], without the quarantine.
    pub fn get_no_quarantine(&self, key: &ShardKey, range: Option<Range<u64>>) -> Result<Vec<u8>> {
        self.read(key, range, false)
    }

    fn read(&self, key: &ShardKey, range: Option<Range<u64>>, quarantine: bool) -> Result<Vec<u8>> {
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
                Some(l) if l != loc => self.read(key, Some(range), quarantine),
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
        if let Err(e) = faults::read_exact_at(&seg.file, &mut hbuf, loc.offset, loc.segment_id) {
            // A short read is corruption only if the file really ends
            // there; otherwise it is a transient I/O error, never quarantine
            // (RFC-008 §8.2).
            if e.kind() == std::io::ErrorKind::UnexpectedEof
                && file_len(&seg.file) < loc.offset + loc.header_len
            {
                return Err(self.on_corrupt(quarantine, key, loc, "record header truncated"));
            }
            return Err(e.into());
        }
        let header = match RecordHeader::decode(&hbuf, seg.header.block_size) {
            Ok(h) if h.key == *key && h.sequence == loc.sequence => h,
            Ok(_) => {
                return Err(self.on_corrupt(quarantine, key, loc, "record does not match index"));
            }
            Err(reason) => return Err(self.on_corrupt(quarantine, key, loc, reason)),
        };

        let mut buf = vec![0u8; to_usize(read_end - read_start)];
        if let Err(e) = faults::read_exact_at(
            &seg.file,
            &mut buf,
            loc.payload_offset() + read_start,
            loc.segment_id,
        ) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof
                && file_len(&seg.file) < loc.payload_offset() + read_end
            {
                // The record extends past the segment: truncated on disk.
                return Err(self.on_corrupt(quarantine, key, loc, "record truncated"));
            }
            return Err(e.into());
        }
        if let Err(reason) = verify_blocks(
            &buf,
            to_usize(first_block),
            &header.block_crcs,
            seg.header.block_size,
        ) {
            return Err(self.on_corrupt(quarantine, key, loc, reason));
        }
        let _ = corrupt;
        buf.drain(..to_usize(range.start - read_start));
        buf.truncate(to_usize(range.end - range.start));
        Ok(buf)
    }

    /// Quarantine (when asked) and return the `Corrupt` error.
    fn on_corrupt(
        &self,
        quarantine: bool,
        key: &ShardKey,
        loc: Location,
        reason: &'static str,
    ) -> StorageError {
        if quarantine {
            self.quarantine(key, loc, reason)
        } else {
            StorageError::Corrupt {
                segment_id: loc.segment_id,
                offset: loc.offset,
                reason,
            }
        }
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
        let current = self
            .index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key);
        if current == Some(loc) {
            // Logged outside the index lock (it fsyncs). If the key changes
            // before the index takes the entry, the log holds one entry the
            // next open overrides with the newer record or tombstone.
            let _ = self.quarantine_log.quarantine(key, &entry);
            self.index
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .quarantine_live(*key, entry);
        }
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

    /// Live entries (with sequences) and quarantined shards of `group_id`
    /// after `after`, each in key order and at most `limit`, from one
    /// snapshot of the index.
    #[must_use]
    pub fn listing_in_group_after(
        &self,
        group_id: u64,
        after: Option<ShardKey>,
        limit: usize,
    ) -> GroupListing {
        // One index snapshot for both (Phase 3.5 audit OPS-6: a key moving
        // between live and quarantined between two reads was skipped or
        // listed twice).
        let idx = self.index.read().unwrap_or_else(PoisonError::into_inner);
        (
            idx.entries_in_group_after(group_id, after, limit).collect(),
            idx.quarantined_in_group_after(group_id, after, limit)
                .collect(),
        )
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
            .map(|(k, len, _)| (k, len))
            .collect()
    }

    /// [`Self::entries_in_group_after`] with each record's sequence.
    #[must_use]
    pub fn entries_in_group_after_sequenced(
        &self,
        group_id: u64,
        after: Option<ShardKey>,
        limit: usize,
    ) -> Vec<(ShardKey, u32, u64)> {
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
        let blocked = self
            .compaction_blocked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut out = Vec::with_capacity(readers.len());
        for (&id, info) in readers.iter() {
            let live = idx.segment_live(id);
            let blocked = blocked.get(&id).is_some_and(|(keys, records)| {
                *records == live.records && keys.iter().all(|k| idx.dead_elsewhere(k, id))
            });
            out.push(SegmentStats {
                segment_id: id,
                file_bytes: info.file.metadata()?.len(),
                live_bytes: live.bytes,
                live_records: live.records,
                active: id == active,
                blocked,
            });
        }
        Ok(out)
    }

    /// A sequence below which every record that exists is in the index
    /// (Phase 3.13+: the next sequence, or the lowest still on its way to
    /// the index if lower). A record written from now on gets a sequence at
    /// or above it, so that a verification of every record listed now
    /// covers every record below it.
    #[must_use]
    pub fn indexed_below(&self) -> u64 {
        let w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let next = w.next_sequence;
        let lowest = self
            .inflight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .first()
            .copied()
            .unwrap_or(u64::MAX);
        drop(w);
        next.min(lowest)
    }

    /// The store epoch, new at every open. A delete token is `(store
    /// epoch, sequence)`; the node refuses one of another epoch.
    #[must_use]
    pub fn store_epoch(&self) -> u64 {
        self.epoch
    }

    /// Entry counts of the in-memory index (R-70 diagnostics).
    #[must_use]
    pub fn index_sizes(&self) -> crate::IndexSizes {
        self.index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .sizes()
    }

    /// Non-live data records still on disk (superseded, quarantined, or
    /// source copies of relocated records): what keeps tombstones needed.
    #[must_use]
    pub fn dead_records(&self) -> u64 {
        self.index
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .dead_records()
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
            .filter(|s| !s.active && !s.blocked && s.file_bytes > 0)
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
    #[allow(clippy::too_many_lines, clippy::missing_panics_doc)]
    pub fn compact_with_hooks(
        &self,
        segment_id: u64,
        hooks: &mut dyn FnMut(CompactionStep) -> Result<()>,
    ) -> Result<CompactionReport> {
        let mut output_id = None;
        let r = self.compact_into(segment_id, hooks, &mut output_id);
        // An output not yet published (renamed) is nobody's: removed now
        // rather than at the next open (Phase 3.9 review), so repeated
        // failures do not pile up temporary files.
        if r.is_err()
            && let Some(id) = output_id
        {
            let tmp = segment::temp_segment_path(&self.cfg.dir, id);
            if std::fs::remove_file(&tmp).is_ok()
                && let Ok(dir) = File::open(&self.cfg.dir)
            {
                let _ = faults::sync_all(&dir, "dir");
            }
        }
        r
    }

    #[allow(clippy::too_many_lines)]
    fn compact_into(
        &self,
        segment_id: u64,
        hooks: &mut dyn FnMut(CompactionStep) -> Result<()>,
        reserved: &mut Option<u64>,
    ) -> Result<CompactionReport> {
        // Reserve an output ID.
        let output_id = {
            let mut w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
            if w.active.header().segment_id == segment_id {
                return Err(StorageError::InvalidArgument(
                    "cannot compact the active segment".into(),
                ));
            }
            let id = w.next_segment_id;
            w.next_segment_id += 1;
            id
        };
        *reserved = Some(output_id);

        let source = {
            let readers = self.readers.read().unwrap_or_else(PoisonError::into_inner);
            let Some(info) = readers.get(&segment_id) else {
                return Err(StorageError::InvalidArgument(format!(
                    "unknown segment {segment_id}"
                )));
            };
            info.file.try_clone()?
        };
        // A tombstone is needed exactly while a non-live data record of its
        // key exists in another segment (it could reappear at the next open
        // otherwise). The tracked set only shrinks when a segment is gone,
        // and new records have higher sequences, so a decision to drop is
        // never invalidated by what happens meanwhile.
        let needed = |key: &ShardKey| {
            self.index
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .dead_elsewhere(key, segment_id)
        };

        let path = segment_path(&self.cfg.dir, segment_id);
        let scan = segment::scan(&path, segment_id)?;
        let mut report = CompactionReport {
            segment_id,
            ..Default::default()
        };
        // Would this reclaim anything? A segment of live records and
        // tombstones that are all still needed would only be rewritten
        // into an identical new segment, every pass (soak test): leave it,
        // and let `segment_stats` mark it blocked until that changes.
        let mut kept_keys = Vec::new();
        let progress = !scan.damaged.is_empty()
            || scan.records.iter().any(|r| {
                let p = r.header.prefix();
                if p.is_tombstone() {
                    let keep = needed(&p.key);
                    if keep {
                        kept_keys.push(p.key);
                    }
                    !keep
                } else {
                    self.locate(&r.header.key)
                        != Some(Location {
                            segment_id,
                            offset: r.offset,
                            header_len: p.header_len() as u64,
                            payload_len: p.payload_len,
                            sequence: p.sequence,
                        })
                }
            });
        if !progress {
            let records = self
                .index
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .segment_live(segment_id)
                .records;
            self.compaction_blocked
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(segment_id, (kept_keys, records));
            report.no_progress = true;
            return Ok(report);
        }
        self.compaction_blocked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&segment_id);
        let mut output: Option<SegmentWriter> = None;
        let mut moved: Vec<(ShardKey, Location, Location)> = Vec::new();
        let mut out_min_seq: Option<u64> = None;

        for r in &scan.records {
            let p = r.header.prefix();
            let (copy, payload) = if p.is_tombstone() {
                let keep = needed(&p.key);
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
                    faults::read_exact_at(&source, &mut payload, old.payload_offset(), segment_id)?;
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
            if output.is_none() {
                let o = SegmentWriter::create_temp(
                    &self.cfg.dir,
                    Self::new_segment_header(
                        &self.cfg,
                        self.device_id,
                        output_id,
                        self.format_write(),
                    ),
                )?;
                self.written
                    .compaction
                    .fetch_add(o.end(), std::sync::atomic::Ordering::Relaxed);
                output = Some(o);
            }
            let out = output.as_mut().expect("created above");
            let before = out.end();
            let new_offset = out.append(&r.header, &payload)?;
            self.written
                .compaction
                .fetch_add(out.end() - before, std::sync::atomic::Ordering::Relaxed);
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
            out.publish(&self.cfg.dir)?;
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
                if !idx.relocate(key, old, *new) {
                    // The key changed meanwhile: the copy is not live.
                    idx.dead_add(*key, new.segment_id);
                }
            }
        }
        hooks(CompactionStep::IndexSwapped)?;

        // Unlink first: if it fails the segment stays known (its records
        // still exist and stay counted) and the next pass retries it. Readers
        // that already hold the file keep reading it (POSIX unlink
        // semantics); new reads go through the index, which no longer points
        // here.
        faults::guard(&faults::IoOp::Remove {
            segment: segment_id,
        })?;
        std::fs::remove_file(&path)?;
        faults::sync_all(&File::open(&self.cfg.dir)?, "dir")?;
        self.readers
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&segment_id);
        // Only now, with the file durably gone, do its records stop
        // counting (a crash before this point reopens with the file).
        self.index
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .segment_removed(segment_id);
        hooks(CompactionStep::Unlinked)?;
        Ok(report)
    }
}

/// Lengths handled here are bounded by `u32` payloads and small headers, so
/// they always fit in `usize` on supported (64-bit) platforms.
fn to_usize(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

/// File holding a bound above every record sequence handed out so far.
const SEQUENCE_BOUND_FILE: &str = "sequence.bound";

/// Records one segment can take at most (each has at least a 64-byte
/// header), plus one for the record that crosses the size limit.
fn sequences_per_segment(cfg: &StoreConfig) -> u64 {
    cfg.segment_max_bytes / 64 + 2
}

/// File locked while a store has the directory open.
const LOCK_FILE: &str = "store.lock";

/// A random, non-zero store epoch: 64 random bits (the two halves of a
/// v4 UUID combined, so that its fixed version and variant bits are not
/// part of it; Phase 3.5 audit CON-20). Two opens collide with
/// probability 2^-64.
fn new_store_epoch() -> u64 {
    let b = *uuid::Uuid::new_v4().as_bytes();
    let (lo, hi) = b.split_at(8);
    let x = u64::from_le_bytes(lo.try_into().unwrap_or_default())
        ^ u64::from_le_bytes(hi.try_into().unwrap_or_default()).rotate_left(29);
    x.max(1)
}

/// `next + n`, refusing to wrap (Phase 3.5 audit CON-17, CON-21).
fn reserve(next: u64, n: u64) -> Result<u64> {
    next.checked_add(n)
        .ok_or_else(|| std::io::Error::other("record sequences exhausted").into())
}

/// The persisted bound (0 when there is none) and whether the file was
/// damaged: the bound only keeps sequences increasing across opens, safety
/// comes from the store epoch, so a damaged file is not a reason to refuse
/// the open.
fn read_sequence_bound(dir: &Path) -> (u64, bool) {
    let text = match std::fs::read_to_string(dir.join(SEQUENCE_BOUND_FILE)) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (0, false),
        // Unreadable or not UTF-8: reported like a damaged one (OPS-9).
        Err(_) => return (0, true),
    };
    let fields: Vec<&str> = text.split_whitespace().collect();
    if let [v, epoch, crc] = fields.as_slice() {
        let body = format!("{v} {epoch}");
        if format!("{:08x}", crate::format::crc32c(body.as_bytes())) == *crc
            && let Ok(v) = v.parse()
        {
            return (v, false);
        }
    }
    (0, true)
}

/// Replace the bound durably (write, sync, rename, sync the directory).
fn write_sequence_bound(dir: &Path, bound: u64, epoch: u64) -> Result<()> {
    let tmp = dir.join(format!("{SEQUENCE_BOUND_FILE}.tmp"));
    {
        let f = File::create(&tmp)?;
        let body = format!("{bound} {epoch}");
        let crc = crate::format::crc32c(body.as_bytes());
        std::io::Write::write_all(&mut &f, format!("{body} {crc:08x}\n").as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(SEQUENCE_BOUND_FILE))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Live entries `(key, payload length, sequence)` and quarantined shards of
/// a listing page ([`Store::listing_in_group_after`]).
pub type GroupListing = (Vec<(ShardKey, u32, u64)>, Vec<(ShardKey, QuarantineEntry)>);

/// A record appended and not yet applied to the index (see `append_if`).
struct Inflight<'a> {
    store: &'a Store,
    key: ShardKey,
    sequence: u64,
    applied: bool,
}

impl Inflight<'_> {
    /// The record reached the index (call with the index lock held): the
    /// lowest sequence still on its way, `u64::MAX` when none is.
    fn applied(&mut self) -> u64 {
        self.applied = true;
        let mut f = self
            .store
            .inflight
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        f.remove(&self.sequence);
        f.first().copied().unwrap_or(u64::MAX)
    }
}

impl Drop for Inflight<'_> {
    fn drop(&mut self) {
        if !self.applied {
            self.store
                .inflight
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&self.sequence);
        }
        let mut a = self
            .store
            .appending
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if a.get(&self.key) == Some(&self.sequence) {
            a.remove(&self.key);
        }
    }
}

/// The group-commit sync leadership of one writer (see `wait_durable`).
struct SyncLeader<'a> {
    store: &'a Store,
    done: bool,
}

impl SyncLeader<'_> {
    fn finish(mut self) {
        self.done = true;
    }
}

impl Drop for SyncLeader<'_> {
    fn drop(&mut self) {
        if !self.done {
            let mut st = self
                .store
                .durability
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            st.syncing = false;
            self.store.durability.cv.notify_all();
        }
    }
}
