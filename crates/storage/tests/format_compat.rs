//! On-disk format compatibility.
//!
//! `tests/golden/segment-v1.arca` is a segment written by format version 1
//! with fixed contents. Every future version of the engine must open it and
//! return the same records. Regenerate only when deliberately creating a new
//! golden file for a new format version:
//! `ARCA_STORAGE_WRITE_GOLDEN=1 cargo test -p arca-lake-storage --test format_compat`.

#![allow(clippy::assert_is_empty)]

use std::path::PathBuf;

use arca_lake_storage::format::{
    FLAG_TOMBSTONE, RECORD_VERSION, RecordHeader, SEGMENT_FORMAT_VERSION, SegmentHeader, ShardKey,
};
use arca_lake_storage::{StorageError, Store, StoreConfig, SyncPolicy};

const ALIGN: u32 = 512;
const BLOCK: u32 = 256;

fn golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden/segment-v1.arca")
}

fn key(i: u8) -> ShardKey {
    ShardKey {
        group_id: u64::from(i),
        chunk_id: [i; 16],
        shard_index: u16::from(i),
    }
}

fn payload(i: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|j| u8::try_from((usize::from(i) * 7 + j) % 251).unwrap())
        .collect()
}

/// Records in the golden segment: (key, sequence, payload or tombstone).
fn expected() -> Vec<(ShardKey, u64, Option<Vec<u8>>)> {
    vec![
        (key(1), 1, Some(payload(1, 100))),
        (key(2), 2, Some(payload(2, 700))), // spans several checksum blocks
        (key(1), 3, Some(payload(1, 10))),  // overwrite
        (key(3), 4, Some(Vec::new())),      // empty payload
        (key(2), 5, None),                  // tombstone
    ]
}

/// Build the golden bytes with the current writer (used only when regenerating).
fn build_golden() -> Vec<u8> {
    let header = SegmentHeader {
        // The golden file is format v1 in every build; a newer build
        // (`version-bump`, format 2) must still open it.
        format_version: 1,
        record_alignment: ALIGN,
        block_size: BLOCK,
        device_id: [0xA5; 16],
        segment_id: 7,
        created_unix_nanos: 1_700_000_000_000_000_000,
    };
    let mut out = header.encode().to_vec();
    out.resize(usize::try_from(header.padded_len()).unwrap(), 0);
    for (k, seq, p) in expected() {
        let (flags, body) = match p {
            Some(b) => (0, b),
            None => (FLAG_TOMBSTONE, Vec::new()),
        };
        let h = RecordHeader::for_payload(k, flags, seq, &body, BLOCK).unwrap();
        let start = out.len();
        out.extend_from_slice(&h.encode());
        out.extend_from_slice(&body);
        let padded = (out.len() - start).div_ceil(ALIGN as usize) * ALIGN as usize;
        out.resize(start + padded, 0);
    }
    out
}

#[test]
fn golden_segment_v1_opens_and_matches() {
    let path = golden_path();
    if std::env::var_os("ARCA_STORAGE_WRITE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, build_golden()).unwrap();
    }
    let bytes = std::fs::read(&path)
        .unwrap_or_else(|e| panic!("missing golden file {}: {e}", path.display()));
    assert_eq!(
        bytes,
        build_golden(),
        "the current writer no longer produces format v1 bytes"
    );

    // Open it as a store directory.
    let dir = tempfile::tempdir().unwrap();
    std::fs::copy(&path, dir.path().join("seg-0000000000000007.arca")).unwrap();
    let cfg = StoreConfig {
        dir: dir.path().to_path_buf(),
        segment_max_bytes: 1 << 20,
        record_alignment: ALIGN,
        block_size: BLOCK,
        sync: SyncPolicy::Never,
        format_write: arca_lake_storage::default_format_write(),
    };
    let (store, report) = Store::open(cfg).unwrap();
    assert_eq!(report.segments, 1);
    assert_eq!(report.truncated_bytes, 0);
    assert!(report.corrupt_segments.is_empty());
    assert_eq!(store.device_id(), [0xA5; 16]);
    assert_eq!(
        store.get(&key(1), None).unwrap(),
        payload(1, 10),
        "overwrite wins"
    );
    assert!(
        matches!(store.get(&key(2), None), Err(StorageError::NotFound)),
        "tombstone wins"
    );
    assert_eq!(store.get(&key(3), None).unwrap(), Vec::<u8>::new());
    assert_eq!(store.keys().len(), 2);
    // Appending after the golden segment continues the sequence.
    store.put(key(9), b"new").unwrap();
    assert_eq!(store.get(&key(9), None).unwrap(), b"new");
}

#[test]
fn unknown_versions_and_flags_are_rejected_explicitly() {
    let mut bytes = build_golden();
    // Bump the segment format version and fix up its CRC.
    bytes[8..10].copy_from_slice(&(SEGMENT_FORMAT_VERSION + 1).to_le_bytes());
    let crc = crc32c::crc32c(&bytes[0..60]);
    bytes[60..64].copy_from_slice(&crc.to_le_bytes());
    let err = SegmentHeader::decode(&bytes[..64], 7).unwrap_err();
    assert!(matches!(err, StorageError::UnsupportedFormat(_)), "{err}");

    // A record with an unknown flag bit must not be accepted as valid.
    let h = RecordHeader::for_payload(key(1), 0b1000_0000, 1, b"x", BLOCK).unwrap();
    let enc = h.encode();
    assert_eq!(enc[4], RECORD_VERSION);
    assert_eq!(
        RecordHeader::decode(&enc, BLOCK).unwrap_err(),
        "unknown record flags"
    );
}

/// The quarantine log's line format (Phase 3.4 §8): operators and tools
/// read it, so its lines are fixed in `tests/golden/quarantine-v1.log`
/// (timestamps normalized). Regenerate with `ARCA_STORAGE_WRITE_GOLDEN=1`.
#[test]
fn quarantine_log_lines_keep_their_format() {
    use std::os::unix::fs::FileExt as _;
    let golden = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden/quarantine-v1.log");
    let dir = tempfile::tempdir().unwrap();
    std::fs::copy(golden_path(), dir.path().join("seg-0000000000000007.arca")).unwrap();
    let cfg = StoreConfig {
        dir: dir.path().to_path_buf(),
        segment_max_bytes: 1 << 20,
        record_alignment: ALIGN,
        block_size: BLOCK,
        sync: SyncPolicy::Never,
        format_write: arca_lake_storage::default_format_write(),
    };
    let (store, _) = Store::open(cfg).unwrap();
    // Corrupt key 1's payload on disk, read it (quarantined), then write a
    // newer record (the quarantine is released as superseded).
    let loc = store.locate(&key(1)).unwrap();
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.path().join(format!("seg-{:016x}.arca", loc.segment_id)))
        .unwrap();
    let mut b = [0u8; 1];
    f.read_exact_at(&mut b, loc.payload_offset()).unwrap();
    f.write_all_at(&[b[0] ^ 0x5a], loc.payload_offset())
        .unwrap();
    assert!(matches!(
        store.get(&key(1), None),
        Err(StorageError::Corrupt { .. })
    ));
    store.put(key(1), b"repaired").unwrap();
    let lines: Vec<String> = store
        .quarantine_log()
        .unwrap()
        .into_iter()
        .map(|l| match l.rsplit_once(" ms=") {
            Some((head, _)) => format!("{head} ms=<ms>"),
            None => l,
        })
        .collect();
    let text = lines.join("\n") + "\n";
    if std::env::var_os("ARCA_STORAGE_WRITE_GOLDEN").is_some() {
        std::fs::write(&golden, &text).unwrap();
    }
    let want = std::fs::read_to_string(&golden)
        .unwrap_or_else(|e| panic!("missing golden file {}: {e}", golden.display()));
    assert_eq!(text, want, "the quarantine log line format changed");
    assert_eq!(lines.len(), 2, "{lines:?}");
}
