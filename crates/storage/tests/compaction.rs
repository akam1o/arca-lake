//! Compaction: correctness, interruption at every step, concurrent reads.

#![allow(clippy::assert_is_empty)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arca_lake_storage::{ShardKey, StorageError, Store, StoreConfig, SyncPolicy};

fn key(i: u64) -> ShardKey {
    let mut chunk_id = [0u8; 16];
    chunk_id[..8].copy_from_slice(&i.to_le_bytes());
    ShardKey {
        group_id: 1,
        chunk_id,
        shard_index: 0,
    }
}

fn payload(i: u64) -> Vec<u8> {
    let len = 100 + usize::try_from(i % 1500).unwrap();
    (0..len)
        .map(|j| u8::try_from((i * 31 + j as u64) % 251).unwrap())
        .collect()
}

fn config(dir: &std::path::Path) -> StoreConfig {
    StoreConfig {
        dir: dir.to_path_buf(),
        segment_max_bytes: 32 << 10,
        record_alignment: 512,
        block_size: 256,
        sync: SyncPolicy::Never, // durability is covered elsewhere; keep these fast
    }
}

type Model = BTreeMap<u64, Option<Vec<u8>>>;

/// Write a workload with overwrites and deletes spread across several segments.
fn populate(store: &Store) -> Model {
    let mut model = Model::new();
    for i in 0..200u64 {
        store.put(key(i), &payload(i)).unwrap();
        model.insert(i, Some(payload(i)));
        if i % 3 == 0 && i >= 6 {
            store.delete(key(i - 6)).unwrap();
            model.insert(i - 6, None);
        }
        if i % 5 == 0 && i >= 10 {
            store.put(key(i - 10), &payload(i + 1000)).unwrap();
            model.insert(i - 10, Some(payload(i + 1000)));
        }
    }
    model
}

fn check(store: &Store, model: &Model, ctx: &str) {
    for (k, expected) in model {
        match (expected, store.get(&key(*k), None)) {
            (Some(p), Ok(got)) => assert_eq!(&got, p, "{ctx}: key {k}"),
            (None, Err(StorageError::NotFound)) => {}
            (e, got) => panic!(
                "{ctx}: key {k}: expected {:?}, got {got:?}",
                e.as_ref().map(Vec::len)
            ),
        }
    }
    assert_eq!(
        store.keys().len(),
        model.values().filter(|v| v.is_some()).count(),
        "{ctx}: live count"
    );
}

#[test]
fn compaction_preserves_state_and_reclaims_space() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path())).unwrap();
    let model = populate(&store);
    let before = store.stats().unwrap();
    let candidates = store.compaction_candidates(0.2).unwrap();
    assert!(!candidates.is_empty(), "workload must produce dead space");
    let mut copied = 0;
    for seg in &candidates {
        let r = store.compact(*seg).unwrap();
        copied += r.copied_records;
        assert!(
            matches!(store.compact(*seg), Err(StorageError::InvalidArgument(_))),
            "segment gone"
        );
    }
    assert!(copied > 0);
    check(&store, &model, "after compaction");
    let after = store.stats().unwrap();
    assert!(
        after.segment_bytes < before.segment_bytes,
        "{before:?} -> {after:?}"
    );
    assert_eq!(after.live_bytes, before.live_bytes);
    // Tombstones for keys with no older record anywhere are dropped over time,
    // but never while an older record may exist: reopening must agree.
    drop(store);
    let (store, report) = Store::open(config(dir.path())).unwrap();
    assert!(report.corrupt_segments.is_empty());
    check(&store, &model, "after reopen");
}

#[test]
fn interrupted_compaction_is_safe_at_every_step_and_resumable() {
    // Enumerate interruption points: after each copied record, after sync,
    // after index swap, after unlink.
    let mut step_index = 0usize;
    loop {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = Store::open(config(dir.path())).unwrap();
        let model = populate(&store);
        let seg = *store
            .compaction_candidates(0.2)
            .unwrap()
            .first()
            .expect("candidate");

        let mut seen = 0usize;
        let mut hit = None;
        let res = store.compact_with_hooks(seg, &mut |step| {
            if seen == step_index {
                hit = Some(step);
                return Err(StorageError::InvalidArgument("injected crash".into()));
            }
            seen += 1;
            Ok(())
        });
        let Some(step) = hit else {
            // step_index beyond the last hook: compaction completed; done.
            assert!(res.is_ok());
            break;
        };
        assert!(res.is_err());
        let ctx = format!("interrupted at step {step_index} ({step:?})");

        // The live store stays consistent without reopening.
        check(&store, &model, &format!("{ctx}, before reopen"));
        drop(store);

        // Recovery sees a consistent state and compaction can be re-run.
        let (store, report) = Store::open(config(dir.path())).unwrap();
        assert!(report.corrupt_segments.is_empty(), "{ctx}: {report:?}");
        check(&store, &model, &format!("{ctx}, after reopen"));
        if store
            .segment_stats()
            .unwrap()
            .iter()
            .any(|s| s.segment_id == seg)
        {
            store.compact(seg).unwrap();
            check(&store, &model, &format!("{ctx}, after re-run"));
        }
        // And once more from disk, including every other candidate.
        drop(store);
        let (store, _) = Store::open(config(dir.path())).unwrap();
        for s in store.compaction_candidates(0.0).unwrap() {
            store.compact(s).unwrap();
        }
        check(&store, &model, &format!("{ctx}, after full compaction"));
        drop(store);
        let (store, _) = Store::open(config(dir.path())).unwrap();
        check(&store, &model, &format!("{ctx}, final reopen"));

        step_index += 1;
        assert!(step_index < 10_000, "runaway");
    }
    assert!(
        step_index > 3,
        "must have exercised several steps, got {step_index}"
    );
}

#[test]
fn reads_during_compaction_never_fail_or_see_wrong_data() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path())).unwrap();
    let store = Arc::new(store);
    let model = populate(&store);
    let live: Vec<(u64, Vec<u8>)> = model
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|p| (*k, p.clone())))
        .collect();

    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|t| {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            let live = live.clone();
            std::thread::spawn(move || {
                let mut reads = 0u64;
                let mut i = t;
                while !stop.load(Ordering::Relaxed) {
                    let (k, p) = &live[i % live.len()];
                    let got = store
                        .get(&key(*k), None)
                        .unwrap_or_else(|e| panic!("read of live key {k} failed: {e}"));
                    assert_eq!(&got, p, "key {k}");
                    reads += 1;
                    i += 7;
                }
                reads
            })
        })
        .collect();

    for _ in 0..3 {
        for seg in store.compaction_candidates(0.0).unwrap() {
            store.compact(seg).unwrap();
        }
        // Create more dead space and compact again while readers run.
        for i in 0..50u64 {
            store.put(key(i + 1000), &payload(i)).unwrap();
            store.delete(key(i + 1000)).unwrap();
        }
    }
    stop.store(true, Ordering::Relaxed);
    let total: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(total > 0);
    check(&store, &model, "after concurrent compaction");
}

#[test]
fn tombstones_are_kept_while_older_data_may_exist() {
    // Data for key 1 in segment A; later tombstone in segment B. Compacting
    // B must keep the tombstone (A still has the data), so that a rebuild
    // scanning A then B never resurrects key 1.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.segment_max_bytes = 1024; // header (512) + one record (512): roll before every append
    let (store, _) = Store::open(cfg.clone()).unwrap();
    store.put(key(1), b"data").unwrap();
    store.put(key(2), b"filler").unwrap(); // forces a rollover before the tombstone
    store.delete(key(1)).unwrap();
    store.put(key(3), b"more").unwrap(); // rollover again so the tombstone's segment is sealed
    // Both the data segment and the tombstone segment have no live records;
    // the tombstone's is the newer one.
    let tomb_seg = store
        .segment_stats()
        .unwrap()
        .iter()
        .filter(|s| !s.active && s.live_records == 0)
        .map(|s| s.segment_id)
        .max()
        .expect("a sealed segment holding only the tombstone");
    let r = store.compact(tomb_seg).unwrap();
    assert_eq!(r.tombstones_kept, 1, "{r:?}");
    drop(store);
    let (store, _) = Store::open(cfg).unwrap();
    assert!(
        matches!(store.get(&key(1), None), Err(StorageError::NotFound)),
        "key 1 resurrected"
    );
    assert_eq!(store.get(&key(2), None).unwrap(), b"filler");
}
