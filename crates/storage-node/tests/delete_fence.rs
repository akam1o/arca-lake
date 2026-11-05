//! Delete fencing on the storage node (Phase 3.5 audit SEC-5, SEC-6): once
//! the node adopted a cluster incarnation a delete must carry it, and a
//! delete that passed its fence checks finishes before an epoch raise (or a
//! new incarnation) is acknowledged.

// The I/O hook is process-wide: the tests hold its serial lock throughout.
#![allow(clippy::await_holding_lock)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_protocol::storage::v1::HealthRequest;
use arca_lake_storage::faults::{self, FaultOutcome, IoOp};
use arca_lake_storage_node::{NodeConfig, serve};
use bytes::Bytes;

fn shard(i: u8) -> ShardRef {
    ShardRef {
        group_id: 1,
        chunk_id: [i; 16],
        shard_index: 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_without_the_adopted_incarnation_are_refused() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(5));
    let data = Bytes::from(vec![1u8; 4096]);
    c.put_shard(&addr, shard(1), 0, data.clone(), 1024)
        .await
        .unwrap();
    c.put_shard(&addr, shard(2), 0, data.clone(), 1024)
        .await
        .unwrap();
    // Before any incarnation is adopted an unstamped delete is accepted.
    c.delete_shard(&addr, shard(1), 0).await.unwrap();
    c.health_with(
        &addr,
        HealthRequest {
            format_write: 1,
            incarnation: Bytes::from_static(&[7; 16]),
            incarnation_changed_ms: 1,
        },
    )
    .await
    .unwrap();
    let e = c.delete_shard(&addr, shard(2), 0).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e}");
    assert_eq!(c.get_shard(&addr, shard(2), 0, 0).await.unwrap(), data);
    c.set_incarnation(Some([7; 16]));
    c.delete_shard(&addr, shard(2), 0).await.unwrap();
    n.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_epoch_raise_is_acknowledged_only_after_deletes_past_the_old_fence() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(10));
    c.put_shard(&addr, shard(3), 0, Bytes::from(vec![3u8; 4096]), 1024)
        .await
        .unwrap();
    // Hold the delete inside the store: its tombstone write sleeps.
    let armed = Arc::new(AtomicBool::new(true));
    let a = Arc::clone(&armed);
    faults::set_io_hook(Some(Arc::new(move |op: &IoOp| {
        if matches!(op, IoOp::Write { .. }) && a.swap(false, Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(800));
        }
        FaultOutcome::None
    })));
    let (c2, addr2) = (c.clone(), addr.clone());
    let delete = tokio::spawn(async move {
        c2.delete_shard(&addr2, shard(3), 0).await.unwrap();
        Instant::now()
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !armed.load(Ordering::SeqCst),
        "the delete reached the store"
    );
    c.update_epoch(&addr, 1, 5).await.unwrap();
    let raised = Instant::now();
    let deleted = delete.await.unwrap();
    faults::set_io_hook(None);
    assert!(
        deleted <= raised,
        "the epoch raise was acknowledged while a delete past the old fence was still running"
    );
    n.shutdown().await;
}

/// A delete decided on a listing names the record it saw: after the key
/// is written again it is refused (ABORTED) and the new record survives
/// (Phase 3.5 audit CON-5/CON-6).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_decided_on_an_old_record_does_not_remove_a_newer_one() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(5));
    let first = c
        .put_shard(&addr, shard(5), 0, Bytes::from(vec![1u8; 4096]), 1024)
        .await
        .unwrap();
    let page = c
        .list_shards_page_with(&addr, 1, None, 100, true)
        .await
        .unwrap();
    let listed = page.sequences[&shard(5)];
    assert_eq!(listed, first.sequence);
    let again = Bytes::from(vec![2u8; 4096]);
    let second = c
        .put_shard(&addr, shard(5), 0, again.clone(), 1024)
        .await
        .unwrap();
    assert!(second.sequence > first.sequence);
    let e = c
        .delete_shard_if(&addr, shard(5), 0, listed)
        .await
        .unwrap_err();
    assert!(NodeClients::is_changed(&e), "{e}");
    assert_eq!(c.get_shard(&addr, shard(5), 0, 0).await.unwrap(), again);
    c.delete_shard_if(&addr, shard(5), 0, second.sequence)
        .await
        .unwrap();
    assert!(c.get_shard(&addr, shard(5), 0, 0).await.is_err());
    n.shutdown().await;
}

/// Quarantined shards are listed page by page like live ones: with fewer
/// live keys than the page limit and more quarantined keys than it, a walk
/// still reaches every key (Phase 3.5 audit OPS-5: the cursor followed the
/// live keys only, so quarantined keys beyond the first page were never
/// listed and never reconciled).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quarantined_shards_beyond_one_page_are_listed() {
    use arca_lake_storage::{ShardKey, Store, StoreConfig};
    use std::os::unix::fs::FileExt as _;
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let key = |i: u8| ShardKey {
        group_id: 1,
        chunk_id: [i; 16],
        shard_index: 0,
    };
    {
        let (store, _) = Store::open(StoreConfig::new(dir.path())).unwrap();
        for i in 1..=6u8 {
            store.put(key(i), &vec![i; 8192]).unwrap();
        }
        // Damage keys 2..=6 on disk: the next open quarantines them.
        for i in 2..=6u8 {
            let loc = store.locate(&key(i)).unwrap();
            let path = dir.path().join(format!("seg-{:016x}.arca", loc.segment_id));
            let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            f.write_all_at(&[!i], loc.payload_offset() + 10).unwrap();
        }
    }
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(5));
    let (mut live, mut quarantined, mut after) = (Vec::new(), Vec::new(), None);
    for _ in 0..10 {
        let page = c
            .list_shards_page_with(&addr, 1, after, 2, true)
            .await
            .unwrap();
        assert!(page.entries.len() + page.quarantined.len() <= 2);
        live.extend(page.entries.iter().map(|(s, _)| s.chunk_id[0]));
        quarantined.extend(page.quarantined.iter().map(|(s, _)| s.chunk_id[0]));
        after = page.next;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(live, vec![1]);
    assert_eq!(quarantined, vec![2, 3, 4, 5, 6]);
    n.shutdown().await;
}
