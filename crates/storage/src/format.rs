//! On-disk format, version 1. See `FORMAT.md`.

use crate::error::{Result, StorageError};

/// Segment file magic.
pub const SEGMENT_MAGIC: [u8; 8] = *b"ARCASEG1";
/// Newest segment format version this build reads and can write (`F_cur`).
/// Version 2 (Phase 3.10, ADR-0024) has the version-1 layout: it marks a
/// store of a cluster that finalized the rejoin fence, which no earlier
/// release opens, so that a node cannot be rolled back past it. Version 3
/// exists only in a `version-bump` build (ADR-0016), with the same layout.
/// New segments are stamped with the store's `format_write` (RFC-014
/// §5.3), never above this.
#[allow(clippy::cast_possible_truncation)]
pub const SEGMENT_FORMAT_VERSION: u16 = 2 + arca_lake_common::compat::BUMP as u16;
/// Oldest segment format version this build reads (all `F ≤ F_cur`).
pub const SEGMENT_FORMAT_VERSION_MIN: u16 = 1;
/// Encoded segment header length in bytes.
pub const SEGMENT_HEADER_LEN: usize = 64;
/// Record magic.
pub const RECORD_MAGIC: [u8; 4] = *b"ARCR";
/// Record version written by this build.
pub const RECORD_VERSION: u8 = 1;
/// Fixed part of the record header, before the block CRC table.
pub const RECORD_FIXED_LEN: usize = 60;
/// Record flag: the record is a tombstone.
pub const FLAG_TOMBSTONE: u8 = 0b0000_0001;
/// Smallest permitted record alignment.
pub const MIN_ALIGNMENT: u32 = 64;

/// Identity of a shard on a device: `(stripe group, chunk, shard index)`.
///
/// Ordering is by group first so that all shards of a group are contiguous
/// in an ordered index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardKey {
    /// Stripe group ID.
    pub group_id: u64,
    /// Chunk ID (128-bit, opaque to the engine).
    pub chunk_id: [u8; 16],
    /// Shard index within the stripe.
    pub shard_index: u16,
}

// Little-endian field readers. Callers guarantee bounds; these never panic
// on well-formed input lengths checked by the decoders.
fn le_u16(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}
fn le_u32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}
fn le_u64(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(a)
}

/// Segment header fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Format version (`SEGMENT_FORMAT_VERSION` when written by this build).
    pub format_version: u16,
    /// Record alignment in bytes (power of two, ≥ `MIN_ALIGNMENT`).
    pub record_alignment: u32,
    /// Payload checksum block size in bytes (multiple of 64).
    pub block_size: u32,
    /// Device UUID.
    pub device_id: [u8; 16],
    /// Segment ID.
    pub segment_id: u64,
    /// Creation time in Unix nanoseconds; informational only.
    pub created_unix_nanos: u64,
}

impl SegmentHeader {
    /// Encode to the fixed 64-byte form.
    #[must_use]
    pub fn encode(&self) -> [u8; SEGMENT_HEADER_LEN] {
        const HEADER_LEN_FIELD: u16 = 64;
        const _: () = assert!(HEADER_LEN_FIELD as usize == SEGMENT_HEADER_LEN);
        let mut b = [0u8; SEGMENT_HEADER_LEN];
        b[0..8].copy_from_slice(&SEGMENT_MAGIC);
        b[8..10].copy_from_slice(&self.format_version.to_le_bytes());
        b[10..12].copy_from_slice(&HEADER_LEN_FIELD.to_le_bytes());
        b[12..16].copy_from_slice(&self.record_alignment.to_le_bytes());
        b[16..20].copy_from_slice(&self.block_size.to_le_bytes());
        b[20..36].copy_from_slice(&self.device_id);
        b[36..44].copy_from_slice(&self.segment_id.to_le_bytes());
        b[44..52].copy_from_slice(&self.created_unix_nanos.to_le_bytes());
        let crc = crc32c::crc32c(&b[0..60]);
        b[60..64].copy_from_slice(&crc.to_le_bytes());
        b
    }

    /// Decode and validate a segment header.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::UnsupportedFormat`] on bad magic, version, or
    /// parameters, and [`StorageError::Corrupt`] on a CRC mismatch.
    pub fn decode(b: &[u8], segment_id_hint: u64) -> Result<Self> {
        if b.len() < SEGMENT_HEADER_LEN {
            return Err(StorageError::UnsupportedFormat(
                "segment header too short".into(),
            ));
        }
        if b[0..8] != SEGMENT_MAGIC {
            return Err(StorageError::UnsupportedFormat("bad segment magic".into()));
        }
        if le_u32(b, 60) != crc32c::crc32c(&b[0..60]) {
            return Err(StorageError::Corrupt {
                segment_id: segment_id_hint,
                offset: 0,
                reason: "segment header crc mismatch",
            });
        }
        let format_version = le_u16(b, 8);
        if !(SEGMENT_FORMAT_VERSION_MIN..=SEGMENT_FORMAT_VERSION).contains(&format_version) {
            return Err(StorageError::UnsupportedFormat(format!(
                "segment format version {format_version}, this build supports {SEGMENT_FORMAT_VERSION_MIN}..={SEGMENT_FORMAT_VERSION}"
            )));
        }
        let header_len = le_u16(b, 10);
        if usize::from(header_len) != SEGMENT_HEADER_LEN {
            return Err(StorageError::UnsupportedFormat(format!(
                "segment header length {header_len}"
            )));
        }
        let record_alignment = le_u32(b, 12);
        let block_size = le_u32(b, 16);
        validate_geometry(record_alignment, block_size)?;
        let mut device_id = [0u8; 16];
        device_id.copy_from_slice(&b[20..36]);
        Ok(Self {
            format_version,
            record_alignment,
            block_size,
            device_id,
            segment_id: le_u64(b, 36),
            created_unix_nanos: le_u64(b, 44),
        })
    }

    /// Length of the header plus padding to the first record.
    #[must_use]
    pub fn padded_len(&self) -> u64 {
        padded_header_len(self.record_alignment)
    }
}

/// Length of the segment header plus padding for a given alignment.
#[must_use]
pub fn padded_header_len(record_alignment: u32) -> u64 {
    round_up(SEGMENT_HEADER_LEN as u64, u64::from(record_alignment))
}

/// Validate alignment and block size parameters.
///
/// # Errors
///
/// Returns [`StorageError::UnsupportedFormat`] when out of range.
pub fn validate_geometry(record_alignment: u32, block_size: u32) -> Result<()> {
    if record_alignment < MIN_ALIGNMENT || !record_alignment.is_power_of_two() {
        return Err(StorageError::UnsupportedFormat(format!(
            "record alignment {record_alignment} must be a power of two >= {MIN_ALIGNMENT}"
        )));
    }
    if block_size == 0 || !block_size.is_multiple_of(64) {
        return Err(StorageError::UnsupportedFormat(format!(
            "block size {block_size} must be a positive multiple of 64"
        )));
    }
    Ok(())
}

/// CRC-32C of `data`, the checksum used throughout the format.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// CRC-32C over the concatenation of `parts`, without concatenating them.
#[must_use]
pub fn crc32c_parts<P: AsRef<[u8]>>(parts: &[P]) -> u32 {
    parts
        .iter()
        .fold(0u32, |crc, p| crc32c::crc32c_append(crc, p.as_ref()))
}

/// Round `n` up to a multiple of `align` (which must be non-zero).
#[must_use]
pub const fn round_up(n: u64, align: u64) -> u64 {
    n.div_ceil(align) * align
}

/// Record header: fixed fields plus the per-block payload CRC table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    /// Flag bits.
    pub flags: u8,
    /// Shard key.
    pub key: ShardKey,
    /// Payload length in bytes.
    pub payload_len: u32,
    /// Per-device monotonic sequence number.
    pub sequence: u64,
    /// CRC-32C of each payload block.
    pub block_crcs: Vec<u32>,
}

/// The fixed prefix of a record header, decoded without CRC validation so
/// that the reader knows how many table bytes follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordPrefix {
    /// Flag bits.
    pub flags: u8,
    /// Shard key.
    pub key: ShardKey,
    /// Payload length in bytes.
    pub payload_len: u32,
    /// Number of CRC table entries that follow the fixed prefix.
    pub block_count: u32,
    /// Sequence number.
    pub sequence: u64,
}

impl RecordPrefix {
    /// Full encoded header length including table and header CRC.
    #[must_use]
    pub fn header_len(&self) -> usize {
        RECORD_FIXED_LEN + 4 * self.block_count as usize + 4
    }

    /// Length of header + payload before padding.
    #[must_use]
    pub fn unpadded_len(&self) -> u64 {
        self.header_len() as u64 + u64::from(self.payload_len)
    }

    /// Whether the tombstone flag is set.
    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        self.flags & FLAG_TOMBSTONE != 0
    }
}

impl RecordHeader {
    /// Build a header for `payload`, computing the block CRC table.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidArgument`] if `payload` is longer than
    /// `u32::MAX` bytes.
    pub fn for_payload(
        key: ShardKey,
        flags: u8,
        sequence: u64,
        payload: &[u8],
        block_size: u32,
    ) -> Result<Self> {
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| StorageError::InvalidArgument("payload exceeds 4 GiB".into()))?;
        let block_crcs = payload
            .chunks(block_size as usize)
            .map(crc32c::crc32c)
            .collect();
        Ok(Self {
            flags,
            key,
            payload_len,
            sequence,
            block_crcs,
        })
    }

    /// The fixed prefix view.
    #[must_use]
    pub fn prefix(&self) -> RecordPrefix {
        RecordPrefix {
            flags: self.flags,
            key: self.key,
            payload_len: self.payload_len,
            // At most ceil(u32::MAX / 64) entries, so this cannot truncate.
            block_count: u32::try_from(self.block_crcs.len()).unwrap_or(u32::MAX),
            sequence: self.sequence,
        }
    }

    /// Encode the full header (fixed prefix, CRC table, header CRC).
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let prefix = self.prefix();
        let mut b = Vec::with_capacity(prefix.header_len());
        b.extend_from_slice(&RECORD_MAGIC);
        b.push(RECORD_VERSION);
        b.push(self.flags);
        b.extend_from_slice(&self.key.shard_index.to_le_bytes());
        b.extend_from_slice(&self.key.group_id.to_le_bytes());
        b.extend_from_slice(&self.key.chunk_id);
        b.extend_from_slice(&self.payload_len.to_le_bytes());
        b.extend_from_slice(&prefix.block_count.to_le_bytes());
        b.extend_from_slice(&self.sequence.to_le_bytes());
        b.extend_from_slice(&[0u8; 12]);
        debug_assert_eq!(b.len(), RECORD_FIXED_LEN);
        for crc in &self.block_crcs {
            b.extend_from_slice(&crc.to_le_bytes());
        }
        let crc = crc32c::crc32c(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        b
    }

    /// Decode the fixed prefix. Performs structural checks only (magic,
    /// version, consistency of lengths); the header CRC is verified by
    /// [`RecordHeader::decode`] once the table has been read.
    ///
    /// # Errors
    ///
    /// Returns `Err(reason)` describing the structural problem.
    pub fn decode_prefix(
        b: &[u8],
        block_size: u32,
    ) -> std::result::Result<RecordPrefix, &'static str> {
        if b.len() < RECORD_FIXED_LEN {
            return Err("record header truncated");
        }
        if b[0..4] != RECORD_MAGIC {
            return Err("bad record magic");
        }
        if b[4] != RECORD_VERSION {
            return Err("unsupported record version");
        }
        let flags = b[5];
        let shard_index = le_u16(b, 6);
        let group_id = le_u64(b, 8);
        let mut chunk_id = [0u8; 16];
        chunk_id.copy_from_slice(&b[16..32]);
        let payload_len = le_u32(b, 32);
        let block_count = le_u32(b, 36);
        let sequence = le_u64(b, 40);
        if block_count != payload_len.div_ceil(block_size) {
            return Err("block count inconsistent with payload length");
        }
        if flags & FLAG_TOMBSTONE != 0 && payload_len != 0 {
            return Err("tombstone with payload");
        }
        if flags & !FLAG_TOMBSTONE != 0 {
            return Err("unknown record flags");
        }
        Ok(RecordPrefix {
            flags,
            key: ShardKey {
                group_id,
                chunk_id,
                shard_index,
            },
            payload_len,
            block_count,
            sequence,
        })
    }

    /// Decode and validate a complete header (`b.len() >= prefix.header_len()`).
    ///
    /// # Errors
    ///
    /// Returns `Err(reason)` on structural problems or CRC mismatch.
    pub fn decode(b: &[u8], block_size: u32) -> std::result::Result<Self, &'static str> {
        let prefix = Self::decode_prefix(b, block_size)?;
        let len = prefix.header_len();
        if b.len() < len {
            return Err("record header truncated");
        }
        if le_u32(b, len - 4) != crc32c::crc32c(&b[..len - 4]) {
            return Err("record header crc mismatch");
        }
        let block_crcs = b[RECORD_FIXED_LEN..len - 4]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|c| u32::from_le_bytes(*c))
            .collect();
        Ok(Self {
            flags: prefix.flags,
            key: prefix.key,
            payload_len: prefix.payload_len,
            sequence: prefix.sequence,
            block_crcs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ShardKey {
        ShardKey {
            group_id: 42,
            chunk_id: [7u8; 16],
            shard_index: 3,
        }
    }

    #[test]
    fn segment_header_roundtrip() {
        let h = SegmentHeader {
            format_version: SEGMENT_FORMAT_VERSION,
            record_alignment: 4096,
            block_size: 65536,
            device_id: [9u8; 16],
            segment_id: 5,
            created_unix_nanos: 123,
        };
        let b = h.encode();
        assert_eq!(SegmentHeader::decode(&b, 5).unwrap(), h);
        assert_eq!(h.padded_len(), 4096);
        let mut bad = b;
        bad[20] ^= 1;
        assert!(matches!(
            SegmentHeader::decode(&bad, 5),
            Err(StorageError::Corrupt { .. })
        ));
    }

    #[test]
    fn record_header_roundtrip_and_crc() {
        let payload = vec![1u8; 100];
        let h = RecordHeader::for_payload(key(), 0, 77, &payload, 64).unwrap();
        assert_eq!(h.block_crcs.len(), 2);
        let b = h.encode();
        assert_eq!(b.len(), h.prefix().header_len());
        assert_eq!(RecordHeader::decode(&b, 64).unwrap(), h);
        for i in 0..b.len() {
            let mut bad = b.clone();
            bad[i] ^= 0x55;
            assert!(
                RecordHeader::decode(&bad, 64).is_err(),
                "flip at {i} undetected"
            );
        }
    }

    #[test]
    fn geometry_validation() {
        assert!(validate_geometry(4096, 65536).is_ok());
        assert!(validate_geometry(100, 64).is_err());
        assert!(validate_geometry(32, 64).is_err());
        assert!(validate_geometry(64, 100).is_err());
        assert_eq!(round_up(0, 64), 0);
        assert_eq!(round_up(1, 64), 64);
        assert_eq!(round_up(64, 64), 64);
    }
}
