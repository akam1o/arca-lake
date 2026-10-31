//! The per-device store: open/recover, put, get, delete, scrub.

use std::collections::BTreeMap;
use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, StorageError};
use crate::format::{
    FLAG_TOMBSTONE, RecordHeader, SEGMENT_FORMAT_VERSION, SegmentHeader, ShardKey,
    padded_header_len, validate_geometry,
};
use crate::index::{Location, MemIndex};
use crate::segment::{self, ScanEnd, SegmentWriter, segment_path, verify_blocks};

/// When `put` and `delete` make data durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// `fdatasync` before every acknowledgment. The only policy that
    /// provides the durability the write protocol relies on.
    Always,
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
    /// 64 KiB checksum blocks, sync always. Nothing here is a tuned value.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            segment_max_bytes: 1 << 30,
            record_alignment: 4096,
            block_size: 1 << 16,
            sync: SyncPolicy::Always,
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
}

/// Scrub result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrubReport {
    /// Shards verified intact.
    pub verified: usize,
    /// Shards whose on-disk data no longer validates. These are removed from
    /// the index so that reads report them as missing.
    pub corrupt: Vec<ShardKey>,
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

#[derive(Debug)]
struct Writer {
    active: SegmentWriter,
    next_sequence: u64,
    next_segment_id: u64,
}

/// Accumulated state while scanning segments at open.
struct Recovery {
    report: OpenReport,
    index: MemIndex,
    readers: BTreeMap<u64, (File, SegmentHeader)>,
    device_id: Option<[u8; 16]>,
    max_sequence: u64,
    /// Header and valid length of the last segment that may be appended to.
    appendable: Option<(SegmentHeader, u64)>,
}

/// A single-device store.
#[derive(Debug)]
pub struct Store {
    cfg: StoreConfig,
    device_id: [u8; 16],
    writer: Mutex<Writer>,
    index: RwLock<MemIndex>,
    /// Read handles per segment.
    readers: RwLock<BTreeMap<u64, (File, SegmentHeader)>>,
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
        let (active, next_segment_id) = if let Some((header, valid_len)) = rec.appendable {
            (
                SegmentWriter::reopen(&cfg.dir, header, valid_len)?,
                header.segment_id + 1,
            )
        } else {
            let id = ids.last().map_or(0, |last| last + 1);
            let w = Self::create_segment(&cfg, device_id, id)?;
            rec.readers.insert(id, (w.reader()?, *w.header()));
            (w, id + 1)
        };

        rec.report.live_shards = rec.index.len();
        let store = Self {
            cfg,
            device_id,
            writer: Mutex::new(Writer {
                active,
                next_sequence: rec.max_sequence + 1,
                next_segment_id,
            }),
            index: RwLock::new(rec.index),
            readers: RwLock::new(rec.readers),
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

        for r in &scan.records {
            rec.max_sequence = rec.max_sequence.max(r.header.sequence);
            let p = r.header.prefix();
            let superseded = if p.is_tombstone() {
                rec.index.remove(&r.header.key).is_some()
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
        rec.readers.insert(id, (File::open(&path)?, scan.header));
        if appendable {
            rec.appendable = Some((scan.header, scan.valid_len));
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

    fn append(&self, key: ShardKey, flags: u8, payload: &[u8]) -> Result<()> {
        let mut w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        if w.active.end() >= self.cfg.segment_max_bytes {
            let id = w.next_segment_id;
            let new = Self::create_segment(&self.cfg, self.device_id, id)?;
            self.readers
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(id, (new.reader()?, *new.header()));
            w.active = new;
            w.next_segment_id = id + 1;
        }
        let sequence = w.next_sequence;
        let header = RecordHeader::for_payload(key, flags, sequence, payload, self.cfg.block_size)?;
        let offset = w.active.append(&header, payload)?;
        if self.cfg.sync == SyncPolicy::Always {
            w.active.sync()?;
        }
        w.next_sequence += 1;
        let segment_id = w.active.header().segment_id;
        drop(w);

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
            idx.remove(&key);
        }
        Ok(())
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
        let (file, seg) = readers.get(&loc.segment_id).ok_or(StorageError::NotFound)?;
        let corrupt = |reason| StorageError::Corrupt {
            segment_id: loc.segment_id,
            offset: loc.offset,
            reason,
        };

        // Re-read and validate the header so that a stale index entry can
        // never return bytes from the wrong record.
        let mut hbuf = vec![0u8; to_usize(loc.header_len)];
        file.read_exact_at(&mut hbuf, loc.offset)?;
        let header = match RecordHeader::decode(&hbuf, seg.block_size) {
            Ok(h) if h.key == *key && h.sequence == loc.sequence => h,
            Ok(_) => return Err(self.quarantine(key, corrupt("record does not match index"))),
            Err(reason) => return Err(self.quarantine(key, corrupt(reason))),
        };

        let mut buf = vec![0u8; to_usize(read_end - read_start)];
        file.read_exact_at(&mut buf, loc.payload_offset() + read_start)?;
        if let Err(reason) = verify_blocks(
            &buf,
            to_usize(first_block),
            &header.block_crcs,
            seg.block_size,
        ) {
            return Err(self.quarantine(key, corrupt(reason)));
        }
        buf.drain(..to_usize(range.start - read_start));
        buf.truncate(to_usize(range.end - range.start));
        Ok(buf)
    }

    /// Drop a shard that failed validation from the index and return `err`.
    fn quarantine(&self, key: &ShardKey, err: StorageError) -> StorageError {
        self.index
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key);
        err
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
        for (file, _) in readers.values() {
            segment_bytes += file.metadata()?.len();
        }
        Ok(StoreStats {
            live_shards: idx.len(),
            live_bytes: idx.live_bytes(),
            segment_bytes,
            segments: readers.len(),
        })
    }
}

/// Lengths handled here are bounded by `u32` payloads and small headers, so
/// they always fit in `usize` on supported (64-bit) platforms.
fn to_usize(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}
