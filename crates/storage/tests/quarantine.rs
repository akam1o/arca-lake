//! Durable quarantine and resynchronising recovery (RFC-005 §5.6a,
//! RFC-008 §8.2, ADR-0015).

#![allow(clippy::cast_possible_truncation)]

use std::os::unix::fs::FileExt;
use std::path::Path;

use arca_lake_storage::{ReleaseCause, ShardKey, StorageError, Store, StoreConfig, SyncPolicy};

fn config(dir: &Path) -> StoreConfig {
    StoreConfig {
        dir: dir.to_path_buf(),
        segment_max_bytes: 1 << 20,
        record_alignment: 64,
        block_size: 256,
        sync: SyncPolicy::Group,
    }
}

fn key(i: u8) -> ShardKey {
    ShardKey {
        group_id: 1,
        chunk_id: [i; 16],
        shard_index: 0,
    }
}

fn payload(i: u8, len: usize) -> Vec<u8> {
    (0..len).map(|j| i.wrapping_add(j as u8)).collect()
}

/// Flip one payload byte of `k`'s live record (the store stays open).
fn corrupt_payload(store: &Store, k: &ShardKey) {
    let loc = store.locate(k).expect("live");
    let path = store
        .dir()
        .join(format!("seg-{:016x}.arca", loc.segment_id));
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    let pos = loc.payload_offset() + u64::from(loc.payload_len) / 2;
    let mut b = [0u8; 1];
    std::fs::File::open(
        store
            .dir()
            .join(format!("seg-{:016x}.arca", loc.segment_id)),
    )
    .unwrap()
    .read_exact_at(&mut b, pos)
    .unwrap();
    f.write_all_at(&[b[0] ^ 0x55], pos).unwrap();
}

/// Overwrite the record header of `k`'s live record with garbage.
fn corrupt_header(store: &Store, k: &ShardKey) {
    let loc = store.locate(k).expect("live");
    let path = store
        .dir()
        .join(format!("seg-{:016x}.arca", loc.segment_id));
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.write_all_at(&[0xA5; 16], loc.offset).unwrap();
}

#[test]
fn corrupt_payload_is_quarantined_durably_and_released_by_a_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path())).unwrap();
    for i in 0..5u8 {
        store.put(key(i), &payload(i, 1000)).unwrap();
    }
    corrupt_payload(&store, &key(2));
    // The read reports corruption once, then the key is absent; the entry
    // and the log record it; healthy keys are untouched.
    assert!(matches!(
        store.get(&key(2), None),
        Err(StorageError::Corrupt { .. })
    ));
    assert!(matches!(
        store.get(&key(2), None),
        Err(StorageError::NotFound)
    ));
    let q = store.quarantine_entry(&key(2)).expect("quarantined");
    assert_eq!(q.reason, "payload block crc mismatch");
    assert_eq!(store.quarantine_stats(), (1, 1000));
    assert_eq!(store.keys_in_group(1).len(), 4);
    assert_eq!(store.quarantined_in_group_after(1, None, 10).len(), 1);
    let log = store.quarantine_log().unwrap();
    assert_eq!(log.len(), 1);
    assert!(
        log[0].starts_with("quarantine group=1 chunk=0202"),
        "{}",
        log[0]
    );
    assert!(log[0].contains("reason=\"payload block crc mismatch\""));
    for i in [0u8, 1, 3, 4] {
        assert_eq!(store.get(&key(i), None).unwrap(), payload(i, 1000));
    }
    // Reopen: the damaged record is quarantined again by the scan, the
    // records written after it in the same segment are indexed.
    drop(store);
    let (store, report) = Store::open(config(dir.path())).unwrap();
    assert_eq!(report.live_shards, 4);
    assert_eq!(report.quarantined.len(), 1);
    assert_eq!(
        report.corrupt_segments.len(),
        0,
        "a damaged payload is not a hole"
    );
    assert!(store.quarantine_entry(&key(2)).is_some());
    assert!(matches!(
        store.get(&key(2), None),
        Err(StorageError::NotFound)
    ));
    for i in [0u8, 1, 3, 4] {
        assert_eq!(store.get(&key(i), None).unwrap(), payload(i, 1000));
    }
    // Scrub reports the quarantined count and finds nothing new.
    let scrub = store.scrub().unwrap();
    assert_eq!(
        (scrub.verified, scrub.corrupt.len(), scrub.quarantined),
        (4, 0, 1)
    );
    // A rewrite (repair) releases the entry; the new bytes are served.
    store.put(key(2), &payload(9, 1000)).unwrap();
    assert!(store.quarantine_entry(&key(2)).is_none());
    assert_eq!(store.get(&key(2), None).unwrap(), payload(9, 1000));
    assert_eq!(store.quarantine_stats(), (0, 0));
    let log = store.quarantine_log().unwrap();
    assert!(
        log.iter()
            .any(|l| l.starts_with("release group=1 chunk=0202") && l.contains("cause=superseded")),
        "{log:?}"
    );
    // Reopen after the release: nothing quarantined, the new record wins.
    drop(store);
    let (store, report) = Store::open(config(dir.path())).unwrap();
    assert_eq!(
        report.quarantined.len(),
        0,
        "the damaged record is superseded by a newer one"
    );
    assert_eq!(store.get(&key(2), None).unwrap(), payload(9, 1000));
    assert_eq!(store.quarantine_stats(), (0, 0));
}

#[test]
fn delete_releases_and_compaction_purges_quarantined_records() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path())).unwrap();
    for i in 0..3u8 {
        store.put(key(i), &payload(i, 500)).unwrap();
    }
    corrupt_payload(&store, &key(0));
    corrupt_payload(&store, &key(1));
    assert!(store.get(&key(0), None).is_err());
    assert!(store.get(&key(1), None).is_err());
    assert_eq!(store.quarantine_stats().0, 2);
    // Delete releases (tombstone newer than the quarantined record).
    store.delete(key(0)).unwrap();
    assert!(store.quarantine_entry(&key(0)).is_none());
    assert!(
        store
            .quarantine_log()
            .unwrap()
            .iter()
            .any(|l| l.contains("cause=tombstone"))
    );
    // Compaction of the segment purges the remaining entry and never
    // copies damaged bytes.
    let seg = store.locate(&key(2)).unwrap().segment_id;
    for i in 10..40u8 {
        store.put(key(i), &payload(i, 60_000)).unwrap(); // roll the segment
    }
    assert_ne!(store.locate(&key(39)).map(|l| l.segment_id), Some(seg));
    let live_in_seg = store
        .keys_in_group(1)
        .iter()
        .filter(|k| store.locate(k).map(|l| l.segment_id) == Some(seg))
        .count();
    let report = store.compact(seg).unwrap();
    assert_eq!(
        report.copied_records, live_in_seg,
        "only healthy live records are copied"
    );
    assert!(store.quarantine_entry(&key(1)).is_none());
    assert!(
        store
            .quarantine_log()
            .unwrap()
            .iter()
            .any(|l| l.contains("cause=purged"))
    );
    assert_eq!(store.get(&key(2), None).unwrap(), payload(2, 500));
    assert_eq!(store.quarantine_stats(), (0, 0));
    let _ = ReleaseCause::Purged;
}

#[test]
fn corrupt_header_is_a_hole_and_later_records_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path())).unwrap();
    for i in 0..6u8 {
        store.put(key(i), &payload(i, 700)).unwrap();
    }
    corrupt_header(&store, &key(1));
    assert!(matches!(
        store.get(&key(1), None),
        Err(StorageError::Corrupt { .. })
    ));
    drop(store);
    let (store, report) = Store::open(config(dir.path())).unwrap();
    assert_eq!(report.holes.len(), 1, "{report:?}");
    assert_eq!(
        report.live_shards, 5,
        "every record after the hole is indexed"
    );
    assert!(store.get(&key(1), None).is_err());
    for i in [0u8, 2, 3, 4, 5] {
        assert_eq!(
            store.get(&key(i), None).unwrap(),
            payload(i, 700),
            "key {i}"
        );
    }
    // The segment with the hole is sealed: new writes land elsewhere and
    // the store keeps working.
    let before = store.locate(&key(0)).unwrap().segment_id;
    store.put(key(7), &payload(7, 10)).unwrap();
    assert_ne!(store.locate(&key(7)).unwrap().segment_id, before);
    assert_eq!(store.get(&key(7), None).unwrap(), payload(7, 10));
}

#[test]
fn verify_returns_crc_and_quarantines_like_get() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path())).unwrap();
    store.put(key(1), &payload(1, 3000)).unwrap();
    let v = store.verify(&key(1)).unwrap();
    assert_eq!(v.payload_len, 3000);
    assert_eq!(
        v.crc32c,
        arca_lake_storage::format::crc32c(&payload(1, 3000))
    );
    corrupt_payload(&store, &key(1));
    assert!(matches!(
        store.verify(&key(1)),
        Err(StorageError::Corrupt { .. })
    ));
    assert!(matches!(store.verify(&key(1)), Err(StorageError::NotFound)));
    assert!(store.quarantine_entry(&key(1)).is_some());
}
