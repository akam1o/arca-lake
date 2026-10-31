//! Segment files: creation, appending, and recovery scanning.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};
use crate::format::{
    RECORD_FIXED_LEN, RecordHeader, RecordPrefix, SEGMENT_HEADER_LEN, SegmentHeader, round_up,
};

/// File name for a segment ID.
#[must_use]
pub fn segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("seg-{segment_id:016x}.arca"))
}

/// Parse a segment ID from a file name produced by [`segment_path`].
#[must_use]
pub fn parse_segment_name(name: &str) -> Option<u64> {
    let hex = name.strip_prefix("seg-")?.strip_suffix(".arca")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// The active segment being appended to.
#[derive(Debug)]
pub struct SegmentWriter {
    file: File,
    header: SegmentHeader,
    /// Next write offset; always a multiple of the record alignment.
    end: u64,
}

impl SegmentWriter {
    /// Create a new segment file, write and sync its header, and sync the directory.
    pub fn create(dir: &Path, header: SegmentHeader) -> Result<Self> {
        let path = segment_path(dir, header.segment_id);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let padded = header.padded_len();
        let mut buf = vec![0u8; usize::try_from(padded).expect("alignment fits usize")];
        buf[..SEGMENT_HEADER_LEN].copy_from_slice(&header.encode());
        file.write_all(&buf)?;
        file.sync_all()?;
        File::open(dir)?.sync_all()?;
        Ok(Self {
            file,
            header,
            end: padded,
        })
    }

    /// Reopen an existing segment for appending at `end` (after a recovery scan).
    pub fn reopen(dir: &Path, header: SegmentHeader, end: u64) -> Result<Self> {
        let path = segment_path(dir, header.segment_id);
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        file.seek(SeekFrom::Start(end))?;
        Ok(Self { file, header, end })
    }

    /// Segment header.
    #[must_use]
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// Current end offset (next record start).
    #[must_use]
    pub fn end(&self) -> u64 {
        self.end
    }

    /// Append one record (header, payload, padding). Returns the record offset.
    /// Does **not** sync; call [`SegmentWriter::sync`].
    pub fn append(&mut self, header: &RecordHeader, payload: &[u8]) -> Result<u64> {
        let offset = self.end;
        let encoded = header.encode();
        let unpadded = encoded.len() as u64 + payload.len() as u64;
        let padded = round_up(unpadded, u64::from(self.header.record_alignment));
        let pad = usize::try_from(padded - unpadded).expect("padding fits usize");
        let mut buf = Vec::with_capacity(usize::try_from(padded).expect("record fits usize"));
        buf.extend_from_slice(&encoded);
        buf.extend_from_slice(payload);
        buf.resize(buf.len() + pad, 0);
        self.file.write_all(&buf)?;
        self.end += padded;
        Ok(offset)
    }

    /// Make appended records durable.
    pub fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Read-only handle to the same file for serving reads.
    pub fn reader(&self) -> Result<File> {
        Ok(self.file.try_clone()?)
    }
}

/// One valid record found by a scan.
#[derive(Debug, Clone)]
pub struct ScannedRecord {
    /// Record offset.
    pub offset: u64,
    /// Decoded header.
    pub header: RecordHeader,
}

/// How a scan ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanEnd {
    /// Every byte up to end of file was valid records (and padding).
    Clean,
    /// Valid records were followed by a partial record and then only zeros or
    /// end of file: a torn tail from a crash. `valid_len` is where it starts.
    TornTail {
        /// Offset at which valid data ends.
        valid_len: u64,
        /// Reason the first invalid record was rejected.
        reason: &'static str,
    },
    /// Valid records were followed by an invalid record and then non-zero
    /// data: corruption, not a crash.
    Corrupt {
        /// Offset at which valid data ends.
        valid_len: u64,
        /// Reason the first invalid record was rejected.
        reason: &'static str,
    },
}

/// Result of scanning one segment.
#[derive(Debug)]
pub struct ScanResult {
    /// Segment header.
    pub header: SegmentHeader,
    /// Valid records in file order.
    pub records: Vec<ScannedRecord>,
    /// Offset at which valid data ends (next append position if reopened).
    pub valid_len: u64,
    /// File length at scan time.
    pub file_len: u64,
    /// How the scan ended.
    pub end: ScanEnd,
}

/// Scan a segment file, validating every record (header CRC and payload
/// block CRCs) and stopping at the first invalid one.
pub fn scan(path: &Path, segment_id: u64) -> Result<ScanResult> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut hbuf = [0u8; SEGMENT_HEADER_LEN];
    file.read_exact(&mut hbuf)?;
    let header = SegmentHeader::decode(&hbuf, segment_id)?;
    if header.segment_id != segment_id {
        return Err(StorageError::Corrupt {
            segment_id,
            offset: 0,
            reason: "segment id mismatch",
        });
    }
    let align = u64::from(header.record_alignment);
    let mut offset = header.padded_len();
    let mut records = Vec::new();
    let mut prefix_buf = [0u8; RECORD_FIXED_LEN];

    let end = loop {
        if offset >= file_len {
            break ScanEnd::Clean;
        }
        match read_record_at(&file, &header, offset, &mut prefix_buf, file_len) {
            Ok(rec) => {
                let len = round_up(rec_len(&rec.header.prefix()), align);
                records.push(rec);
                offset += len;
            }
            Err(reason) => {
                let valid_len = offset;
                if later_valid_record_exists(&file, &header, offset + align, file_len) {
                    break ScanEnd::Corrupt { valid_len, reason };
                }
                break ScanEnd::TornTail { valid_len, reason };
            }
        }
    };
    let valid_len = match &end {
        ScanEnd::Clean => offset,
        ScanEnd::TornTail { valid_len, .. } | ScanEnd::Corrupt { valid_len, .. } => *valid_len,
    };
    Ok(ScanResult {
        header,
        records,
        valid_len,
        file_len,
        end,
    })
}

fn rec_len(p: &RecordPrefix) -> u64 {
    p.unpadded_len()
}

/// Read and fully validate the record at `offset`.
fn read_record_at(
    file: &File,
    seg: &SegmentHeader,
    offset: u64,
    prefix_buf: &mut [u8; RECORD_FIXED_LEN],
    file_len: u64,
) -> std::result::Result<ScannedRecord, &'static str> {
    if offset + RECORD_FIXED_LEN as u64 > file_len {
        return Err("record header truncated");
    }
    file.read_exact_at(prefix_buf, offset)
        .map_err(|_| "read error")?;
    let prefix = RecordHeader::decode_prefix(prefix_buf, seg.block_size)?;
    if offset + prefix.unpadded_len() > file_len {
        return Err("record truncated");
    }
    let mut hbuf = vec![0u8; prefix.header_len()];
    file.read_exact_at(&mut hbuf, offset)
        .map_err(|_| "read error")?;
    let header = RecordHeader::decode(&hbuf, seg.block_size)?;
    let mut payload = vec![0u8; prefix.payload_len as usize];
    file.read_exact_at(&mut payload, offset + prefix.header_len() as u64)
        .map_err(|_| "read error")?;
    verify_blocks(&payload, 0, &header.block_crcs, seg.block_size)?;
    Ok(ScannedRecord { offset, header })
}

/// Verify payload blocks starting at block index `first_block`.
pub fn verify_blocks(
    payload: &[u8],
    first_block: usize,
    crcs: &[u32],
    block_size: u32,
) -> std::result::Result<(), &'static str> {
    for (i, block) in payload.chunks(block_size as usize).enumerate() {
        let expected = crcs
            .get(first_block + i)
            .ok_or("block crc table too short")?;
        if crc32c::crc32c(block) != *expected {
            return Err("payload block crc mismatch");
        }
    }
    Ok(())
}

/// Distinguish a torn tail from a hole: probe every aligned offset after the
/// invalid record for a fully valid record. If one exists, the invalid
/// region is followed by data that was written *after* it, which a crash
/// cannot produce (writes are sequential and a record is only followed by
/// another once it was completely written), so it is corruption.
fn later_valid_record_exists(file: &File, seg: &SegmentHeader, from: u64, file_len: u64) -> bool {
    let align = u64::from(seg.record_alignment);
    let mut prefix_buf = [0u8; RECORD_FIXED_LEN];
    let mut offset = from;
    while offset + RECORD_FIXED_LEN as u64 <= file_len {
        if read_record_at(file, seg, offset, &mut prefix_buf, file_len).is_ok() {
            return true;
        }
        offset += align;
    }
    false
}

/// Truncate a segment to `len` and sync it.
pub fn truncate(path: &Path, len: u64) -> Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_name_roundtrip() {
        let p = segment_path(Path::new("/x"), 0xabc);
        assert_eq!(
            p.file_name().unwrap().to_str().unwrap(),
            "seg-0000000000000abc.arca"
        );
        assert_eq!(parse_segment_name("seg-0000000000000abc.arca"), Some(0xabc));
        assert_eq!(parse_segment_name("seg-abc.arca"), None);
        assert_eq!(parse_segment_name("other"), None);
    }
}
