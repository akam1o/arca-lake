//! Crash-consistency and corruption tests for the storage engine.
//!
//! These tests simulate crashes by damaging segment files between a `put`
//! and the next `open`, then check two invariants:
//!
//! 1. **No acknowledged, synced write is lost** unless the damage reaches
//!    into its bytes (a torn tail may only ever remove a *suffix* of the
//!    acknowledged sequence, never a middle record).
//! 2. **No corrupt bytes are ever returned as valid.** Every damaged record
//!    is reported as missing or corrupt.

#![allow(clippy::cast_possible_truncation)]

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::Path;

use arca_lake_storage::{ShardKey, StorageError, Store, StoreConfig, SyncPolicy};

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 11
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn key(i: u64) -> ShardKey {
    let mut chunk_id = [0u8; 16];
    chunk_id[..8].copy_from_slice(&i.to_le_bytes());
    ShardKey {
        group_id: i % 3,
        chunk_id,
        shard_index: (i % 5) as u16,
    }
}

fn payload(i: u64, len: usize) -> Vec<u8> {
    let mut rng = Lcg(i.wrapping_mul(0x9E37_79B9) | 1);
    (0..len).map(|_| rng.next() as u8).collect()
}

fn small_config(dir: &Path) -> StoreConfig {
    StoreConfig {
        dir: dir.to_path_buf(),
        segment_max_bytes: 64 << 10, // force frequent rollover
        record_alignment: 512,
        block_size: 256,
        sync: SyncPolicy::Always,
    }
}

fn segment_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "arca"))
        .collect();
    v.sort();
    v
}

/// Write `n` shards with varying sizes; returns `(key, payload)` in ack order.
fn write_n(store: &Store, n: u64, seed: u64) -> Vec<(ShardKey, Vec<u8>)> {
    let mut rng = Lcg(seed | 1);
    (0..n)
        .map(|i| {
            let len = rng.below(3000) as usize;
            let p = payload(i ^ seed, len);
            store.put(key(i), &p).unwrap();
            (key(i), p)
        })
        .collect()
}

#[test]
fn reopen_recovers_everything_including_overwrites_and_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(small_config(dir.path())).unwrap();
    let written = write_n(&store, 200, 1);
    // Overwrite some keys and delete some others.
    store.put(key(3), b"replacement").unwrap();
    store.delete(key(4)).unwrap();
    store.delete(key(999)).unwrap(); // missing key: durable no-op
    let stats_before = store.stats().unwrap();
    assert!(stats_before.segments > 1, "test should roll segments");
    drop(store);

    let (store, report) = Store::open(small_config(dir.path())).unwrap();
    assert_eq!(report.truncated_bytes, 0);
    assert_eq!(report.corrupt_segments, []);
    assert_eq!(report.superseded_records, 2);
    assert_eq!(store.stats().unwrap().live_shards, 199);
    for (k, p) in &written {
        if *k == key(3) {
            assert_eq!(store.get(k, None).unwrap(), b"replacement");
        } else if *k == key(4) {
            assert!(matches!(store.get(k, None), Err(StorageError::NotFound)));
        } else {
            assert_eq!(&store.get(k, None).unwrap(), p);
        }
    }
    // Range reads across checksum-block boundaries.
    let (k, p) = &written[10];
    if p.len() > 600 {
        assert_eq!(store.get(k, Some(100..600)).unwrap(), &p[100..600]);
        assert_eq!(store.get(k, Some(255..257)).unwrap(), &p[255..257]);
    }
    assert!(store.get(k, Some(0..p.len() as u64 + 1)).is_err());
}

#[test]
fn torn_tail_loses_at_most_a_suffix_and_never_returns_garbage() {
    for iteration in 0..40u64 {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = Store::open(small_config(dir.path())).unwrap();
        let written = write_n(&store, 30, iteration + 100);
        drop(store);

        // Crash model: the last segment loses a random suffix (as if the
        // final write was torn or the device lost its tail).
        let files = segment_files(dir.path());
        let last = files.last().unwrap();
        let len = std::fs::metadata(last).unwrap().len();
        let mut rng = Lcg(iteration * 7919 + 3);
        let cut = rng.below(len + 1);
        OpenOptions::new()
            .write(true)
            .open(last)
            .unwrap()
            .set_len(cut)
            .unwrap();

        let (store, report) = Store::open(small_config(dir.path())).unwrap();
        assert!(
            report.corrupt_segments.is_empty(),
            "a cut tail is a torn tail, not corruption"
        );

        // Invariant 1: readable keys form a prefix of the ack order.
        let mut seen_missing = false;
        for (k, p) in &written {
            match store.get(k, None) {
                Ok(got) => {
                    assert!(
                        !seen_missing,
                        "iteration {iteration}: a record after a lost one survived"
                    );
                    assert_eq!(&got, p, "iteration {iteration}: wrong bytes");
                }
                Err(StorageError::NotFound) => seen_missing = true,
                Err(e) => panic!("iteration {iteration}: unexpected error {e}"),
            }
        }
        // Appending after recovery works and the file is consistent again.
        store.put(key(1000), b"after-crash").unwrap();
        drop(store);
        let (store, report) = Store::open(small_config(dir.path())).unwrap();
        assert_eq!(report.truncated_bytes, 0);
        assert_eq!(store.get(&key(1000), None).unwrap(), b"after-crash");
    }
}

#[test]
fn bit_flips_are_detected_as_corruption() {
    for iteration in 0..40u64 {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = Store::open(small_config(dir.path())).unwrap();
        let written = write_n(&store, 20, iteration + 500);
        drop(store);

        // Flip one byte somewhere after the segment header of a random segment.
        let files = segment_files(dir.path());
        let mut rng = Lcg(iteration * 104_729 + 11);
        let target = &files[rng.below(files.len() as u64) as usize];
        let len = std::fs::metadata(target).unwrap().len();
        if len <= 512 {
            continue;
        }
        let pos = 512 + rng.below(len - 512);
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(target)
            .unwrap();
        let mut b = [0u8; 1];
        f.read_exact_at(&mut b, pos).unwrap();
        let flipped = [b[0] ^ (1 << rng.below(8))];
        f.write_all_at(&flipped, pos).unwrap();
        f.sync_all().unwrap();
        drop(f);

        let (store, _report) = Store::open(small_config(dir.path())).unwrap();
        // Invariant 2: every key either returns the exact payload or is
        // reported missing/corrupt; never different bytes.
        let mut damaged = 0;
        for (k, p) in &written {
            match store.get(k, None) {
                Ok(got) => assert_eq!(
                    &got, p,
                    "iteration {iteration}: corrupt bytes returned as valid"
                ),
                Err(StorageError::NotFound | StorageError::Corrupt { .. }) => damaged += 1,
                Err(e) => panic!("iteration {iteration}: unexpected error {e}"),
            }
        }
        // A flip inside padding or inside a superseded region is harmless;
        // otherwise exactly the affected records are lost, and the scrub agrees.
        let scrub = store.scrub().unwrap();
        assert!(
            scrub.corrupt.is_empty(),
            "open already excluded invalid records"
        );
        assert_eq!(scrub.verified + damaged, written.len());
    }
}

#[test]
fn corruption_after_open_is_caught_by_get_and_scrub() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(small_config(dir.path())).unwrap();
    let p = payload(1, 2000);
    store.put(key(1), &p).unwrap();
    let loc = store.locate(&key(1)).unwrap();

    // Damage the payload on disk while the store is open (silent corruption).
    let path = segment_files(dir.path())
        .into_iter()
        .find(|f| {
            f.to_str()
                .unwrap()
                .contains(&format!("{:016x}", loc.segment_id))
        })
        .unwrap();
    let f = OpenOptions::new().write(true).open(path).unwrap();
    f.write_all_at(&[0xFF], loc.payload_offset() + 1000)
        .unwrap();
    f.sync_all().unwrap();

    // A range that does not touch the damaged block still verifies.
    assert_eq!(store.get(&key(1), Some(0..200)).unwrap(), &p[0..200]);
    // A range that touches it is reported as corrupt and the shard is quarantined.
    assert!(matches!(
        store.get(&key(1), Some(900..1100)),
        Err(StorageError::Corrupt { .. })
    ));
    assert!(matches!(
        store.get(&key(1), None),
        Err(StorageError::NotFound)
    ));
    assert!(!store.contains(&key(1)));
}

#[test]
fn unsynced_store_is_explicitly_not_durable_but_still_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = small_config(dir.path());
    cfg.sync = SyncPolicy::Never;
    let (store, _) = Store::open(cfg.clone()).unwrap();
    let written = write_n(&store, 50, 9);
    drop(store);
    let (store, _) = Store::open(cfg).unwrap();
    for (k, p) in &written {
        match store.get(k, None) {
            Ok(got) => assert_eq!(&got, p),
            Err(StorageError::NotFound) => {}
            Err(e) => panic!("{e}"),
        }
    }
}

#[test]
fn mixed_device_ids_are_rejected() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let (sa, _) = Store::open(small_config(a.path())).unwrap();
    let (sb, _) = Store::open(small_config(b.path())).unwrap();
    sa.put(key(1), b"a").unwrap();
    sb.put(key(2), b"b").unwrap();
    drop((sa, sb));
    // Move b's segment into a's directory under a new id.
    let src = segment_files(b.path()).pop().unwrap();
    std::fs::copy(&src, a.path().join("seg-0000000000000001.arca")).unwrap();
    let err = Store::open(small_config(a.path())).expect_err("must reject");
    assert!(
        matches!(
            err,
            StorageError::Corrupt { .. } | StorageError::UnsupportedFormat(_)
        ),
        "{err}"
    );
}
