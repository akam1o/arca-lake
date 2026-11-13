//! The differential verification watermark (Phase 3.14): every record that
//! exists below `Store::indexed_below` is in the index. A record appended
//! and waiting for its group commit has its sequence and is not listed
//! yet; the watermark must stay at or below it, or a verification pass
//! that listed the index would take the record as verified.

use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arca_lake_storage::faults::{self, FaultOutcome, IoOp, set_io_hook};
use arca_lake_storage::{ShardKey, Store, StoreConfig, SyncPolicy};

fn config(dir: &Path) -> StoreConfig {
    StoreConfig {
        dir: dir.to_path_buf(),
        segment_max_bytes: 1 << 20,
        record_alignment: 64,
        block_size: 256,
        sync: SyncPolicy::Group,
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

#[test]
fn a_record_waiting_for_its_commit_stays_above_the_watermark() {
    let _s = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = Store::open(config(dir.path())).unwrap();
    let store = Arc::new(store);
    let first = store.put_sequenced(key(1), b"one").unwrap();
    assert_eq!(store.indexed_below(), first + 1);

    // The next group commit's sync waits until released.
    let (entered_tx, entered) = mpsc::channel::<()>();
    let (release, release_rx) = mpsc::channel::<()>();
    let entered_tx = Mutex::new(Some(entered_tx));
    let release_rx = Mutex::new(release_rx);
    set_io_hook(Some(Arc::new(move |op: &IoOp| {
        if matches!(op, IoOp::SyncData { .. })
            && let Some(tx) = entered_tx.lock().unwrap().take()
        {
            tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }
        FaultOutcome::None
    })));
    let writer = {
        let store = Arc::clone(&store);
        std::thread::spawn(move || store.put_sequenced(key(2), b"two").unwrap())
    };
    entered.recv_timeout(Duration::from_secs(10)).unwrap();
    // Appended (it has its sequence), not in the index yet.
    assert!(store.locate(&key(2)).is_none());
    let (wm_tx, wm) = mpsc::channel();
    {
        let store = Arc::clone(&store);
        std::thread::spawn(move || wm_tx.send(store.indexed_below()).unwrap());
    }
    let below = wm.recv_timeout(Duration::from_secs(10));
    release.send(()).unwrap();
    let second = writer.join().unwrap();
    set_io_hook(None);
    let below = below.expect("the watermark waited for the group commit");
    assert!(
        below <= second,
        "watermark {below} above the record {second} not yet in the index"
    );
    assert_eq!(store.locate(&key(2)).unwrap().sequence, second);
    assert_eq!(store.indexed_below(), second + 1);
}
