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
        format_write: arca_lake_storage::default_format_write(),
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
    // Nothing in it can go: the segment (and its tombstone) is left as it
    // is rather than rewritten.
    let r = store.compact(tomb_seg).unwrap();
    assert!(r.no_progress, "{r:?}");
    assert!(
        store
            .segment_stats()
            .unwrap()
            .iter()
            .any(|s| s.segment_id == tomb_seg && s.blocked),
        "{r:?}"
    );
    drop(store);
    let (store, _) = Store::open(cfg).unwrap();
    assert!(
        matches!(store.get(&key(1), None), Err(StorageError::NotFound)),
        "key 1 resurrected"
    );
    assert_eq!(store.get(&key(2), None).unwrap(), b"filler");
}

/// A segment holding only tombstones that must be kept (an older segment
/// still has records below them) is not rewritten into another such
/// segment pass after pass; it is skipped until the older records are
/// gone, then its tombstones are dropped (found by the Phase 3.4 soak:
/// endless tombstone-segment churn starved real compaction).
#[test]
fn kept_tombstones_are_not_rewritten_until_they_can_be_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path())).unwrap();
    let active = |store: &Store| {
        store
            .segment_stats()
            .unwrap()
            .into_iter()
            .find(|s| s.active)
            .unwrap()
            .segment_id
    };
    for i in 0..200 {
        store.put(key(i), &payload(i)).unwrap();
    }
    let last_data = store.locate(&key(199)).unwrap().segment_id;
    // 200 tombstones of one 512-byte record each: several segments of
    // nothing but tombstones after the last data segment.
    for i in 0..200 {
        store.delete(key(i)).unwrap();
    }
    // Seal them.
    let mut n = 10_000;
    let first_active = active(&store);
    while active(&store) == first_active {
        store.put(key(n), &payload(n)).unwrap();
        n += 1;
    }
    let tomb = last_data + 1;
    assert!(
        tomb < first_active,
        "a sealed tombstone-only segment exists"
    );
    // Its tombstones are needed (the data segments below are still
    // there): compacting it makes no progress and writes nothing ...
    let r = store.compact(tomb).unwrap();
    assert!(r.no_progress, "{r:?}");
    assert_eq!(r.output_segment_id, None);
    assert_eq!(r.tombstones_kept, 0, "nothing was rewritten: {r:?}");
    // ... and it is not offered again while that holds.
    let files = || std::fs::read_dir(dir.path()).unwrap().count();
    let before = files();
    for _ in 0..3 {
        assert!(!store.compaction_candidates(0.0).unwrap().contains(&tomb));
    }
    assert_eq!(files(), before);
    // The old data segments are compacted away (all their records are
    // dead): the tombstones can go now, the segment is offered again and
    // compacting it drops them.
    for s in 0..=last_data {
        if store
            .segment_stats()
            .unwrap()
            .iter()
            .any(|x| x.segment_id == s)
        {
            store.compact(s).unwrap();
        }
    }
    assert!(store.compaction_candidates(0.0).unwrap().contains(&tomb));
    let r = store.compact(tomb).unwrap();
    assert!(!r.no_progress && r.tombstones_dropped > 0, "{r:?}");
    assert_eq!(r.output_segment_id, None, "every tombstone dropped: {r:?}");
    for i in 0..200 {
        assert!(matches!(
            store.get(&key(i), None),
            Err(StorageError::NotFound)
        ));
    }
    drop(store);
    // A reopen agrees: nothing deleted comes back.
    let (store, _) = Store::open(config(dir.path())).unwrap();
    for i in 0..200 {
        assert!(
            matches!(store.get(&key(i), None), Err(StorageError::NotFound)),
            "{i} came back"
        );
    }
}

/// The offline breakdown agrees with the store's index (live bytes and
/// records, file bytes) and classifies tombstones by whether an older
/// record still exists elsewhere.
#[test]
fn offline_breakdown_matches_the_index_and_classifies_tombstones() {
    use arca_lake_storage::analysis::analyze_dir;
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path())).unwrap();
    let model = populate(&store);
    let stats = store.segment_stats().unwrap();
    let live_bytes: u64 = stats.iter().map(|s| s.live_bytes).sum();
    let live_records: usize = stats.iter().map(|s| s.live_records).sum();
    let file_bytes: u64 = stats.iter().map(|s| s.file_bytes).sum();
    let t = analyze_dir(dir.path()).unwrap().total();
    assert_eq!(t.live_bytes, live_bytes);
    assert_eq!(usize::try_from(t.live_records).unwrap(), live_records);
    assert_eq!(t.file_bytes, file_bytes);
    assert_eq!(
        usize::try_from(t.live_records).unwrap(),
        model.values().filter(|v| v.is_some()).count()
    );
    assert!(t.dead_records > 0 && t.tombstones_needed + t.tombstones_droppable > 0);
    // Everything compacted until nothing moves: no needed tombstone can
    // remain without an older record, and no dead data remains.
    for _ in 0..4 {
        for s in store.compaction_candidates(0.0).unwrap() {
            store.compact(s).unwrap();
        }
    }
    // (The active segment is never compacted.)
    let active = store
        .segment_stats()
        .unwrap()
        .into_iter()
        .find(|s| s.active)
        .unwrap()
        .segment_id;
    for s in analyze_dir(dir.path())
        .unwrap()
        .segments
        .iter()
        .filter(|s| s.segment_id != active)
    {
        assert_eq!(s.dead_records, 0, "{s:?}");
        assert_eq!(s.tombstones_needed, 0, "{s:?}");
    }
    check(&store, &model, "after full compaction");
}

fn seal(store: &Store, next: &mut u64) {
    let active = |store: &Store| {
        store
            .segment_stats()
            .unwrap()
            .into_iter()
            .find(|s| s.active)
            .unwrap()
            .segment_id
    };
    let a = active(store);
    while active(store) == a {
        store.put(key(*next), &payload(*next)).unwrap();
        *next += 1;
    }
}

/// A tombstone goes once the old records of its key are gone, even with
/// long-lived data older than it elsewhere (the previous rule, "keep while
/// any other segment has a lower sequence", kept every such tombstone
/// forever: 4 KiB per deleted shard, found by the Phase 3.4 convergence
/// test). An interrupted compaction never lets it go early.
#[test]
fn tombstones_go_when_their_old_records_go_not_when_old_data_exists() {
    for interrupt in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = Store::open(config(dir.path())).unwrap();
        let mut n = 50_000;
        // Long-lived data in the oldest segment.
        store.put(key(1), &payload(1)).unwrap();
        // Key 2 in its own segment, then deleted (tombstone elsewhere).
        seal(&store, &mut n);
        store.put(key(2), &payload(2)).unwrap();
        let old_seg = store.locate(&key(2)).unwrap().segment_id;
        seal(&store, &mut n);
        store.delete(key(2)).unwrap();
        let tomb_seg = store
            .segment_stats()
            .unwrap()
            .into_iter()
            .find(|s| s.active)
            .unwrap()
            .segment_id;
        seal(&store, &mut n);
        // While key 2's old record exists, its tombstone is needed.
        let r = store.compact(tomb_seg).unwrap();
        assert!(r.no_progress || r.tombstones_kept == 1, "{r:?}");
        let tomb_seg = r.output_segment_id.unwrap_or(tomb_seg);
        if interrupt {
            // Compaction of the old record's segment stops before the
            // unlink: the record still exists, the tombstone stays needed.
            let e = store.compact_with_hooks(old_seg, &mut |step| {
                if step == arca_lake_storage::CompactionStep::IndexSwapped {
                    Err(StorageError::InvalidArgument("injected crash".into()))
                } else {
                    Ok(())
                }
            });
            assert!(e.is_err());
            assert!(dir.path().join(format!("seg-{old_seg:016x}.arca")).exists());
            let r = store.compact(tomb_seg).unwrap();
            assert!(r.no_progress, "kept while the old record is on disk: {r:?}");
            // Reopen in that state: nothing resurrected.
            drop(store);
            let (store, _) = Store::open(config(dir.path())).unwrap();
            assert!(matches!(
                store.get(&key(2), None),
                Err(StorageError::NotFound)
            ));
            assert_eq!(store.get(&key(1), None).unwrap(), payload(1));
            continue;
        }
        store.compact(old_seg).unwrap();
        assert!(!dir.path().join(format!("seg-{old_seg:016x}.arca")).exists());
        // Key 1 (sequence below the tombstone) is still in the oldest
        // segment; the tombstone goes all the same.
        let r = store.compact(tomb_seg).unwrap();
        assert!(!r.no_progress && r.tombstones_dropped >= 1, "{r:?}");
        drop(store);
        let (store, _) = Store::open(config(dir.path())).unwrap();
        assert!(
            matches!(store.get(&key(2), None), Err(StorageError::NotFound)),
            "resurrected"
        );
        assert_eq!(store.get(&key(1), None).unwrap(), payload(1));
    }
}

/// Random puts, overwrites, deletes, compactions (some interrupted at a
/// random step) and reopens against a model: nothing deleted ever comes
/// back, nothing acknowledged is lost, and once everything is compacted no
/// sealed segment keeps dead data or a tombstone that could go.
#[test]
fn random_workload_with_compaction_and_reopen_matches_the_model() {
    use arca_lake_storage::CompactionStep;
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(config(dir.path())).unwrap().0;
    let mut model = Model::new();
    let mut rng = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    for round in 0..40u64 {
        for _ in 0..60 {
            let k = next() % 120;
            if next() % 10 < 6 {
                let p = payload(k + round * 7);
                store.put(key(k), &p).unwrap();
                model.insert(k, Some(p));
            } else {
                store.delete(key(k)).unwrap();
                model.insert(k, None);
            }
        }
        for s in store.compaction_candidates(0.3).unwrap() {
            if next() % 5 == 0 {
                let stop = match next() % 3 {
                    0 => CompactionStep::Synced,
                    1 => CompactionStep::IndexSwapped,
                    _ => CompactionStep::Unlinked,
                };
                let _ = store.compact_with_hooks(s, &mut |step| {
                    if step == stop {
                        Err(StorageError::InvalidArgument("injected".into()))
                    } else {
                        Ok(())
                    }
                });
            } else {
                store.compact(s).unwrap();
            }
        }
        check(&store, &model, &format!("round {round}"));
        if next() % 3 == 0 {
            drop(store);
            store = Store::open(config(dir.path())).unwrap().0;
            check(&store, &model, &format!("round {round} after reopen"));
        }
    }
    // Everything compacted until nothing moves.
    for _ in 0..6 {
        for s in store.compaction_candidates(0.0).unwrap() {
            store.compact(s).unwrap();
        }
    }
    let active = store
        .segment_stats()
        .unwrap()
        .into_iter()
        .find(|s| s.active)
        .unwrap()
        .segment_id;
    let report = arca_lake_storage::analysis::analyze_dir(dir.path()).unwrap();
    for s in report.segments.iter().filter(|s| s.segment_id != active) {
        assert_eq!(s.dead_records, 0, "{s:?}");
        assert_eq!(
            s.tombstones_droppable, 0,
            "a tombstone that could go stays: {s:?}"
        );
    }
    check(&store, &model, "final");
    drop(store);
    let store = Store::open(config(dir.path())).unwrap().0;
    check(&store, &model, "final reopen");
}

/// The tombstone history is kept only while the index is rebuilt at open:
/// runtime deletes of distinct keys do not grow it (Phase 3.5, R-70: one
/// entry per delete lived until the next restart). A record read before a
/// concurrent delete is not quarantined afterwards, and nothing deleted
/// comes back at the next open.
#[test]
fn runtime_deletes_keep_no_tombstone_history_and_deleted_keys_stay_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path())).unwrap();
    for i in 0..2_000u64 {
        store.put(key(100_000 + i), &payload(i)).unwrap();
        store.delete(key(100_000 + i)).unwrap();
    }
    let sizes = store.index_sizes();
    assert_eq!(sizes.tombstone_history, 0, "{sizes:?}");
    assert_eq!(sizes.live, 0, "{sizes:?}");
    // Compaction leaves no bookkeeping behind either.
    for _ in 0..4 {
        for s in store.compaction_candidates(0.0).unwrap() {
            store.compact(s).unwrap();
        }
    }
    let sizes = store.index_sizes();
    assert!(sizes.dead_tracked < 2_000, "{sizes:?}");
    drop(store);
    let (store, _) = Store::open(config(dir.path())).unwrap();
    for i in 0..2_000u64 {
        assert!(matches!(
            store.get(&key(100_000 + i), None),
            Err(StorageError::NotFound)
        ));
    }
    assert_eq!(store.index_sizes().tombstone_history, 0);
}

/// Sequences are never handed out twice, even when the records that
/// carried the highest ones are gone (compaction can drop them; simulated
/// here by removing the segment files): a delete conditional on an old
/// sequence cannot match a new record after a reopen (Phase 3.5 audit
/// CON-10).
#[test]
fn sequences_are_not_reused_after_their_records_are_gone() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path())).unwrap();
    let old = store.put_sequenced(key(7), &payload(7)).unwrap();
    drop(store);
    for e in std::fs::read_dir(dir.path()).unwrap() {
        let e = e.unwrap();
        if e.file_name().to_string_lossy().starts_with("seg-") {
            std::fs::remove_file(e.path()).unwrap();
        }
    }
    let (store, _) = Store::open(config(dir.path())).unwrap();
    let new = store.put_sequenced(key(7), &payload(8)).unwrap();
    assert!(new > old, "sequence {new} handed out again (was {old})");
    assert!(!store.delete_if(key(7), old).unwrap());
    assert_eq!(store.get(&key(7), None).unwrap(), payload(8));
}
