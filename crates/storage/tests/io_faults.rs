//! Injected I/O faults (R-53, Phase 3.3 §3): what the engine guarantees
//! when a read, write, sync, quarantine-log append or compaction step
//! fails at the system-call boundary. The claims are about the engine's
//! behaviour on the errors the kernel reports; a power loss or a device
//! that silently drops writes is outside what these tests show (RFC-005
//! §8).

#![allow(clippy::too_many_lines, clippy::cast_possible_truncation)]

use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arca_lake_storage::faults::{self, FaultOutcome, IoOp, fail_times, set_io_hook};
use arca_lake_storage::{ShardKey, StorageError, Store, StoreConfig, SyncPolicy};

fn config(dir: &Path, sync: SyncPolicy) -> StoreConfig {
    StoreConfig {
        dir: dir.to_path_buf(),
        segment_max_bytes: 1 << 20,
        record_alignment: 64,
        block_size: 256,
        sync,
        format_write: arca_lake_storage::default_format_write(),
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

fn is_io(e: &StorageError, kind: ErrorKind) -> bool {
    matches!(e, StorageError::Io(io) if io.kind() == kind)
}

/// Reopen and check that every key in `expect` reads back and no other
/// key of group 1 is live.
fn reopen_and_check(dir: &Path, sync: SyncPolicy, expect: &[(ShardKey, Vec<u8>)]) -> Store {
    let (store, report) = Store::open(config(dir, sync)).unwrap();
    assert_eq!(report.corrupt_segments.len(), 0, "{report:?}");
    for (k, v) in expect {
        assert_eq!(&store.get(k, None).unwrap(), v, "{k:?}");
    }
    assert_eq!(store.keys_in_group(1).len(), expect.len(), "{report:?}");
    store
}

#[test]
fn a_failed_append_is_not_acknowledged_and_the_next_one_lands_in_a_fresh_segment() {
    let _s = faults::serial();
    for sync in [SyncPolicy::Always, SyncPolicy::Group] {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = Store::open(config(dir.path(), sync)).unwrap();
        store.put(key(1), &payload(1, 1000)).unwrap();
        let seg_before = store.locate(&key(1)).unwrap().segment_id;
        // EIO on the write of the next record.
        set_io_hook(Some(fail_times(
            |op| matches!(op, IoOp::Write { .. }),
            ErrorKind::Other,
            1,
        )));
        let e = store.put(key(2), &payload(2, 1000)).unwrap_err();
        assert!(is_io(&e, ErrorKind::Other), "{e}");
        set_io_hook(None);
        assert!(
            store.get(&key(2), None).is_err(),
            "never acknowledged, never visible"
        );
        // The next append works and goes to a new segment (the failed one
        // is sealed, R-53); every acknowledged record survives a reopen.
        store.put(key(3), &payload(3, 1000)).unwrap();
        assert_ne!(store.locate(&key(3)).unwrap().segment_id, seg_before);
        drop(store);
        reopen_and_check(
            dir.path(),
            sync,
            &[(key(1), payload(1, 1000)), (key(3), payload(3, 1000))],
        );
    }
}

#[test]
fn a_partial_write_leaves_a_torn_record_that_recovery_discards() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path(), SyncPolicy::Always)).unwrap();
    store.put(key(1), &payload(1, 1000)).unwrap();
    set_io_hook(Some(Arc::new(|op| match op {
        IoOp::Write { .. } => FaultOutcome::PartialWrite(300),
        _ => FaultOutcome::None,
    })));
    let e = store.put(key(2), &payload(2, 1000)).unwrap_err();
    assert!(is_io(&e, ErrorKind::StorageFull), "{e}");
    set_io_hook(None);
    assert!(matches!(
        store.get(&key(2), None),
        Err(StorageError::NotFound)
    ));
    // Reopen: the torn bytes are a torn tail (no valid record follows),
    // truncated away; nothing else is affected.
    drop(store);
    let (store, report) = Store::open(config(dir.path(), SyncPolicy::Always)).unwrap();
    assert!(
        report.truncated_bytes > 0 || report.corrupt_segments.is_empty(),
        "{report:?}"
    );
    assert_eq!(store.get(&key(1), None).unwrap(), payload(1, 1000));
    assert!(matches!(
        store.get(&key(2), None),
        Err(StorageError::NotFound)
    ));
    assert_eq!(store.keys_in_group(1).len(), 1);
    store.put(key(2), &payload(2, 1000)).unwrap();
    assert_eq!(store.get(&key(2), None).unwrap(), payload(2, 1000));
}

#[test]
fn a_failed_fsync_is_not_acknowledged_and_the_segment_is_sealed() {
    let _s = faults::serial();
    for sync in [SyncPolicy::Always, SyncPolicy::Group] {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = Store::open(config(dir.path(), sync)).unwrap();
        store.put(key(1), &payload(1, 500)).unwrap();
        let seg = store.locate(&key(1)).unwrap().segment_id;
        set_io_hook(Some(fail_times(
            move |op| matches!(op, IoOp::SyncData { segment } if *segment == seg),
            ErrorKind::Other,
            1,
        )));
        let e = store.put(key(2), &payload(2, 500)).unwrap_err();
        assert!(is_io(&e, ErrorKind::Other), "{e}");
        set_io_hook(None);
        // Not visible: the engine does not know whether the bytes reached
        // the device, so it does not claim them.
        assert!(matches!(
            store.get(&key(2), None),
            Err(StorageError::NotFound)
        ));
        // The segment is sealed: the next record goes elsewhere and is
        // synced there; the sealed segment's acknowledged records stand.
        store.put(key(3), &payload(3, 500)).unwrap();
        assert_ne!(store.locate(&key(3)).unwrap().segment_id, seg);
        assert_eq!(store.get(&key(1), None).unwrap(), payload(1, 500));
        drop(store);
        // After a reopen, key 2 may or may not be there (its bytes may have
        // reached the device); what is certain is that key 1 and key 3 are
        // and that nothing is corrupt.
        let (store, report) = Store::open(config(dir.path(), sync)).unwrap();
        assert_eq!(report.corrupt_segments.len(), 0);
        assert_eq!(store.get(&key(1), None).unwrap(), payload(1, 500));
        assert_eq!(store.get(&key(3), None).unwrap(), payload(3, 500));
        if let Ok(v) = store.get(&key(2), None) {
            assert_eq!(v, payload(2, 500), "if present, intact");
        }
    }
}

#[test]
fn disk_full_is_reported_and_service_resumes_when_space_returns() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path(), SyncPolicy::Group)).unwrap();
    store.put(key(1), &payload(1, 2000)).unwrap();
    let full = Arc::new(AtomicUsize::new(1));
    let f = Arc::clone(&full);
    set_io_hook(Some(Arc::new(move |op| {
        if matches!(op, IoOp::Write { .. } | IoOp::Create { .. }) && f.load(Ordering::Relaxed) == 1
        {
            FaultOutcome::Error(std::io::Error::new(ErrorKind::StorageFull, "ENOSPC"))
        } else {
            FaultOutcome::None
        }
    })));
    for i in 2..5u8 {
        let e = store.put(key(i), &payload(i, 2000)).unwrap_err();
        assert!(is_io(&e, ErrorKind::StorageFull), "{e}");
    }
    assert!(store.delete(key(1)).is_err(), "a tombstone needs space too");
    assert_eq!(
        store.get(&key(1), None).unwrap(),
        payload(1, 2000),
        "reads keep working"
    );
    // Space returns.
    full.store(0, Ordering::Relaxed);
    for i in 2..5u8 {
        store.put(key(i), &payload(i, 2000)).unwrap();
    }
    set_io_hook(None);
    drop(store);
    reopen_and_check(
        dir.path(),
        SyncPolicy::Group,
        &(1..5u8)
            .map(|i| (key(i), payload(i, 2000)))
            .collect::<Vec<_>>(),
    );
}

#[test]
fn read_errors_and_short_reads_are_io_errors_not_corruption() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path(), SyncPolicy::Group)).unwrap();
    store.put(key(1), &payload(1, 3000)).unwrap();
    // EIO on the payload read.
    set_io_hook(Some(fail_times(
        |op| matches!(op, IoOp::Read { len, .. } if *len >= 3000),
        ErrorKind::Other,
        1,
    )));
    let e = store.get(&key(1), None).unwrap_err();
    assert!(is_io(&e, ErrorKind::Other), "{e}");
    assert!(
        store.quarantine_entry(&key(1)).is_none(),
        "an I/O error never quarantines"
    );
    assert!(store.contains(&key(1)), "the key stays live");
    // A short read where the file is long enough: transient, not truncation.
    set_io_hook(Some(Arc::new(|op| match op {
        IoOp::Read { len, .. } if *len >= 3000 => FaultOutcome::ShortRead(100),
        _ => FaultOutcome::None,
    })));
    let e = store.get(&key(1), None).unwrap_err();
    assert!(is_io(&e, ErrorKind::UnexpectedEof), "{e}");
    assert!(store.quarantine_entry(&key(1)).is_none());
    set_io_hook(None);
    assert_eq!(store.get(&key(1), None).unwrap(), payload(1, 3000));
    assert_eq!(store.quarantine_log().unwrap().len(), 0);
    // A real truncation (the file is cut) is still corruption.
    let loc = store.locate(&key(1)).unwrap();
    let path = dir.path().join(format!("seg-{:016x}.arca", loc.segment_id));
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(loc.payload_offset() + 1000)
        .unwrap();
    assert!(matches!(
        store.get(&key(1), None),
        Err(StorageError::Corrupt { .. })
    ));
    assert_eq!(
        store.quarantine_entry(&key(1)).unwrap().reason,
        "record truncated"
    );
}

#[test]
fn a_quarantine_log_failure_still_quarantines_in_memory_and_recovery_rederives_it() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path(), SyncPolicy::Group)).unwrap();
    store.put(key(1), &payload(1, 3000)).unwrap();
    store.put(key(2), &payload(2, 3000)).unwrap();
    // Corrupt key 1's payload on disk.
    {
        use std::os::unix::fs::FileExt as _;
        let loc = store.locate(&key(1)).unwrap();
        let path = dir.path().join(format!("seg-{:016x}.arca", loc.segment_id));
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.write_all_at(&[0xFF], loc.payload_offset() + 10).unwrap();
    }
    set_io_hook(Some(Arc::new(|op| match op {
        IoOp::QuarantineLog => FaultOutcome::Error(std::io::Error::other("EIO")),
        _ => FaultOutcome::None,
    })));
    assert!(matches!(
        store.get(&key(1), None),
        Err(StorageError::Corrupt { .. })
    ));
    // The log append failed, the quarantine took effect anyway: the corrupt
    // bytes are never served again.
    assert!(store.quarantine_entry(&key(1)).is_some());
    assert!(matches!(
        store.get(&key(1), None),
        Err(StorageError::NotFound)
    ));
    assert_eq!(store.get(&key(2), None).unwrap(), payload(2, 3000));
    set_io_hook(None);
    assert!(
        store.quarantine_log().unwrap().is_empty(),
        "nothing was logged"
    );
    // A reopen re-derives the quarantine from the segments, not the log.
    drop(store);
    let (store, report) = Store::open(config(dir.path(), SyncPolicy::Group)).unwrap();
    assert_eq!(report.quarantined.len(), 1, "{report:?}");
    assert!(store.quarantine_entry(&key(1)).is_some());
    assert_eq!(store.quarantine_log().unwrap().len(), 1, "logged at open");
    assert_eq!(store.get(&key(2), None).unwrap(), payload(2, 3000));
}

#[test]
fn compaction_failures_leave_the_source_segment_and_the_index_untouched() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), SyncPolicy::Group);
    cfg.segment_max_bytes = 64 << 10;
    let (store, _) = Store::open(cfg.clone()).unwrap();
    for i in 1..6u8 {
        store.put(key(i), &payload(i, 20_000)).unwrap();
    }
    store.delete(key(2)).unwrap();
    for i in 6..12u8 {
        store.put(key(i), &payload(i, 20_000)).unwrap(); // roll
    }
    let seg = store.locate(&key(1)).unwrap().segment_id;
    assert_ne!(store.locate(&key(11)).unwrap().segment_id, seg);
    let locs_before: Vec<_> = (1..12u8).map(|i| store.locate(&key(i))).collect();
    // (a) the output segment's write fails; (b) its sync fails; (c) the
    // source's unlink fails after the index switched.
    for (what, matcher) in [
        (
            "write",
            Box::new(
                |op: &IoOp, seg: u64| matches!(op, IoOp::Write { segment, .. } if *segment != seg),
            ) as Box<dyn Fn(&IoOp, u64) -> bool + Send + Sync>,
        ),
        (
            "sync",
            Box::new(
                |op: &IoOp, seg: u64| matches!(op, IoOp::SyncData { segment } if *segment != seg),
            ),
        ),
    ] {
        set_io_hook(Some(fail_times(
            move |op| matcher(op, seg),
            ErrorKind::Other,
            1,
        )));
        let e = store.compact(seg).unwrap_err();
        assert!(is_io(&e, ErrorKind::Other), "{what}: {e}");
        set_io_hook(None);
        let locs_after: Vec<_> = (1..12u8).map(|i| store.locate(&key(i))).collect();
        assert_eq!(locs_after, locs_before, "{what}: index unchanged");
        for i in (1..12u8).filter(|i| *i != 2) {
            assert_eq!(
                store.get(&key(i), None).unwrap(),
                payload(i, 20_000),
                "{what}: key {i}"
            );
        }
    }
    // (c) unlink fails: the records were already relocated and verified; the
    // leftover file is harmless (recovery re-reads it and the newer records
    // win), and a retry succeeds.
    set_io_hook(Some(fail_times(
        move |op| matches!(op, IoOp::Remove { segment } if *segment == seg),
        ErrorKind::Other,
        1,
    )));
    let e = store.compact(seg).unwrap_err();
    assert!(is_io(&e, ErrorKind::Other), "{e}");
    set_io_hook(None);
    for i in (1..12u8).filter(|i| *i != 2) {
        assert_eq!(
            store.get(&key(i), None).unwrap(),
            payload(i, 20_000),
            "key {i}"
        );
    }
    assert!(
        store.locate(&key(1)).unwrap().segment_id != seg,
        "relocated"
    );
    drop(store);
    let (store, report) = Store::open(cfg).unwrap();
    assert_eq!(report.corrupt_segments.len(), 0, "{report:?}");
    for i in (1..12u8).filter(|i| *i != 2) {
        assert_eq!(
            store.get(&key(i), None).unwrap(),
            payload(i, 20_000),
            "key {i}"
        );
    }
    assert!(matches!(
        store.get(&key(2), None),
        Err(StorageError::NotFound)
    ));
}

#[test]
fn a_failed_segment_creation_is_an_error_and_the_store_keeps_serving() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), SyncPolicy::Group);
    cfg.segment_max_bytes = 8 << 10;
    let (store, _) = Store::open(cfg.clone()).unwrap();
    store.put(key(1), &payload(1, 6000)).unwrap();
    store.put(key(2), &payload(2, 6000)).unwrap(); // the next put rolls
    set_io_hook(Some(fail_times(
        |op| matches!(op, IoOp::Create { .. } | IoOp::SyncAll { .. }),
        ErrorKind::Other,
        2,
    )));
    let e = store.put(key(3), &payload(3, 100)).unwrap_err();
    assert!(is_io(&e, ErrorKind::Other), "{e}");
    set_io_hook(None);
    assert_eq!(store.get(&key(2), None).unwrap(), payload(2, 6000));
    store.put(key(3), &payload(3, 100)).unwrap();
    drop(store);
    reopen_and_check(
        dir.path(),
        SyncPolicy::Group,
        &[
            (key(1), payload(1, 6000)),
            (key(2), payload(2, 6000)),
            (key(3), payload(3, 100)),
        ],
    );
}

#[test]
fn a_segment_creation_failing_after_the_file_exists_does_not_block_later_writes() {
    // The header write or its fsync fails once the new file exists: the
    // write is refused, and the next write rolls to a fresh segment instead
    // of failing forever on the existing name (EEXIST).
    for fail_on_sync in [false, true] {
        let _s = faults::serial();
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(dir.path(), SyncPolicy::Group);
        cfg.segment_max_bytes = 8 << 10;
        let (store, _) = Store::open(cfg.clone()).unwrap();
        store.put(key(1), &payload(1, 6000)).unwrap();
        store.put(key(2), &payload(2, 6000)).unwrap(); // the next put rolls
        let active = store
            .segment_stats()
            .unwrap()
            .iter()
            .find(|s| s.active)
            .unwrap()
            .segment_id;
        set_io_hook(Some(fail_times(
            move |op| {
                if fail_on_sync {
                    matches!(op, IoOp::SyncAll { .. })
                } else {
                    matches!(op, IoOp::Write { segment, .. } if *segment > active)
                }
            },
            ErrorKind::Other,
            1,
        )));
        let e = store.put(key(3), &payload(3, 100)).unwrap_err();
        assert!(is_io(&e, ErrorKind::Other), "{e}");
        set_io_hook(None);
        for i in 3..8 {
            store
                .put(key(i), &payload(i, 3000))
                .unwrap_or_else(|e| panic!("put {i}: {e}"));
        }
        drop(store);
        reopen_and_check(
            dir.path(),
            SyncPolicy::Group,
            &(1..8)
                .map(|i| (key(i), payload(i, if i < 3 { 6000 } else { 3000 })))
                .collect::<Vec<_>>(),
        );
    }
}

/// A damaged record of a key deleted since is not quarantined at open: the
/// tombstone history the rebuild uses is still there when damaged records
/// are applied (Phase 3.5 audit OPS-4).
#[test]
fn a_damaged_record_of_a_deleted_key_is_not_quarantined_at_open() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path(), SyncPolicy::Group)).unwrap();
    store.put(key(1), &payload(1, 3000)).unwrap();
    store.put(key(2), &payload(2, 3000)).unwrap();
    {
        use std::os::unix::fs::FileExt as _;
        let loc = store.locate(&key(1)).unwrap();
        let path = dir.path().join(format!("seg-{:016x}.arca", loc.segment_id));
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.write_all_at(&[0xFF], loc.payload_offset() + 10).unwrap();
    }
    store.delete(key(1)).unwrap();
    drop(store);
    let (store, report) = Store::open(config(dir.path(), SyncPolicy::Group)).unwrap();
    assert!(report.quarantined.is_empty(), "{report:?}");
    assert!(store.quarantine_entry(&key(1)).is_none());
    assert!(matches!(
        store.get(&key(1), None),
        Err(StorageError::NotFound)
    ));
    assert_eq!(store.get(&key(2), None).unwrap(), payload(2, 3000));
}

/// A write that panics between its append and its index update does not
/// stay "in flight" for ever: later deletes' tombstone history is still
/// pruned (Phase 3.5 audit CON-13; R-70 depends on the history staying
/// bounded).
#[test]
fn a_panicking_write_does_not_stop_the_tombstone_history_from_being_pruned() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path(), SyncPolicy::Always)).unwrap();
    store.put(key(1), &payload(1, 100)).unwrap();
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let a = Arc::clone(&armed);
    set_io_hook(Some(Arc::new(move |op| {
        if matches!(op, IoOp::SyncData { .. } | IoOp::SyncAll { .. })
            && a.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("injected panic during the write's sync");
        }
        FaultOutcome::None
    })));
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.put(key(2), &payload(2, 100))
    }));
    set_io_hook(None);
    assert!(r.is_err(), "the injected panic fired");
    for i in 10..210u8 {
        store.put(key(i), &payload(i, 100)).unwrap();
        store.delete(key(i)).unwrap();
    }
    let sizes = store.index_sizes();
    assert_eq!(sizes.tombstone_history, 0, "{sizes:?}");
}

/// A group-commit sync leader that panics gives up the leadership: the
/// next writer syncs and completes instead of waiting for ever (Phase 3.5
/// audit CON-15).
#[test]
fn a_panicking_sync_leader_does_not_stall_later_writers() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path(), SyncPolicy::Group)).unwrap();
    let store = Arc::new(store);
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let a = Arc::clone(&armed);
    set_io_hook(Some(Arc::new(move |op| {
        if matches!(op, IoOp::SyncData { .. } | IoOp::SyncAll { .. })
            && a.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("injected panic in the sync leader");
        }
        FaultOutcome::None
    })));
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.put(key(1), &payload(1, 100))
    }));
    assert!(r.is_err() && !armed.load(std::sync::atomic::Ordering::SeqCst));
    let (tx, rx) = std::sync::mpsc::channel();
    let s2 = Arc::clone(&store);
    std::thread::spawn(move || {
        let _ = tx.send(s2.put(key(2), &payload(2, 100)));
    });
    let done = rx.recv_timeout(std::time::Duration::from_secs(10));
    set_io_hook(None);
    assert!(
        matches!(done, Ok(Ok(()))),
        "a later write stalled behind the panicked leader: {done:?}"
    );
}

/// Phase 3.8 review: deleting a key the store does not hold writes no
/// tombstone (repeated deletes of absent keys spent the delete reserve);
/// a deleted key stays deleted across a reopen.
#[test]
fn deleting_an_absent_key_writes_nothing() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path(), SyncPolicy::Group)).unwrap();
    store.put(key(1), &payload(1, 2000)).unwrap();
    store.delete(key(1)).unwrap();
    let after_first = store.written().tombstone;
    assert!(after_first > 0);
    for _ in 0..100 {
        store.delete(key(1)).unwrap();
        store.delete(key(2)).unwrap();
    }
    assert_eq!(store.written().tombstone, after_first);
    drop(store);
    reopen_and_check(dir.path(), SyncPolicy::Group, &[]);
}

/// A compaction that dies while writing its output (here: a partial write,
/// as a SIGKILL or a full device leaves it) while the active segment has
/// rolled over to a higher id since the output id was reserved. The torn
/// output is not the last segment, so a recovery that took it for a
/// segment reported it corrupt (CI, Phase 3.9: `sigkill.rs` iteration 9).
/// The output is written under a temporary name and becomes a segment only
/// once synced: a reopen finds no corrupt segment, loses nothing, and the
/// leftover is removed.
#[test]
fn a_compaction_output_torn_before_its_sync_is_never_a_corrupt_segment() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let mut expect = Vec::new();
    {
        let (store, _) = Store::open(config(dir.path(), SyncPolicy::Always)).unwrap();
        // Fill a segment and roll over; supersede half of it.
        for i in 0..40u8 {
            store.put(key(i), &payload(i, 30_000)).unwrap();
        }
        let first = store.locate(&key(0)).unwrap().segment_id;
        for i in (0..40u8).step_by(2) {
            store.put(key(i), &payload(i + 1, 30_000)).unwrap();
        }
        // The second record copied into the output is torn.
        let writes = Arc::new(AtomicUsize::new(0));
        let w = Arc::clone(&writes);
        let active = store.locate(&key(38)).unwrap().segment_id;
        set_io_hook(Some(Arc::new(move |op: &IoOp| match op {
            IoOp::Write { segment, .. } if *segment > active => {
                if w.fetch_add(1, Ordering::SeqCst) == 1 {
                    FaultOutcome::PartialWrite(100)
                } else {
                    FaultOutcome::None
                }
            }
            _ => FaultOutcome::None,
        })));
        assert!(store.compact(first).is_err());
        set_io_hook(None);
        // The failed output is removed at once, not left for the next open.
        let tmp = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .count();
        assert_eq!(tmp, 0, "a failed compaction left its output");
        // The active segment rolls over past the reserved output id.
        for i in 100..140u8 {
            store.put(key(i), &payload(i, 30_000)).unwrap();
        }
        for i in 0..40u8 {
            let v = if i % 2 == 0 {
                payload(i + 1, 30_000)
            } else {
                payload(i, 30_000)
            };
            expect.push((key(i), v));
        }
        for i in 100..140u8 {
            expect.push((key(i), payload(i, 30_000)));
        }
    }
    let _store = reopen_and_check(dir.path(), SyncPolicy::Always, &expect);
    let leftovers = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .count();
    assert_eq!(leftovers, 0);
}
