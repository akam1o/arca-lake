//! Delete fencing on the storage node (Phase 3.5 audit SEC-5, SEC-6): once
//! the node adopted a cluster incarnation a delete must carry it, and a
//! delete that passed its fence checks finishes before an epoch raise (or a
//! new incarnation) is acknowledged.

// The I/O hook is process-wide: the tests hold its serial lock throughout.
#![allow(clippy::await_holding_lock)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arca_lake_protocol::client::{NodeClients, RecordToken, ShardRef};
use arca_lake_protocol::storage::v1::HealthRequest;
use arca_lake_storage::faults::{self, FaultOutcome, IoOp};
use arca_lake_storage_node::{NodeConfig, serve};
use bytes::Bytes;

/// Open delete fence generation 1 on the node at `addr` and take it for
/// `c` (what a controller leader does, ADR-0023).
async fn open_fence(c: &NodeClients, addr: &str) {
    assert_eq!(c.push_delete_fence(addr, 1, true).await.unwrap(), (1, true));
    c.set_delete_fence(Some(1));
}

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
    open_fence(&c, &addr).await;
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
            ..Default::default()
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
    open_fence(&c, &addr).await;
    c.put_shard(&addr, shard(3), 0, Bytes::from(vec![3u8; 4096]), 1024)
        .await
        .unwrap();
    // Hold the delete inside the store: its tombstone write sleeps, and
    // notes when it is done (the physical delete, not the response, which
    // the node sends after releasing its guard).
    let armed = Arc::new(AtomicBool::new(true));
    let written = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let (a, w) = (Arc::clone(&armed), Arc::clone(&written));
    faults::set_io_hook(Some(Arc::new(move |op: &IoOp| {
        if matches!(op, IoOp::Write { .. }) && a.swap(false, Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(800));
            *w.lock().unwrap() = Some(Instant::now());
        }
        FaultOutcome::None
    })));
    let (c2, addr2) = (c.clone(), addr.clone());
    let delete = tokio::spawn(async move {
        c2.delete_shard(&addr2, shard(3), 0).await.unwrap();
    });
    // Wait (bounded) for the delete to reach the store: a fixed 200 ms
    // was not enough on a loaded CI runner (R-77).
    let waiting = Instant::now();
    while armed.load(Ordering::SeqCst) {
        assert!(
            waiting.elapsed() < Duration::from_secs(10),
            "the delete never reached the store"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    c.update_epoch(&addr, 1, 5).await.unwrap();
    let raised = Instant::now();
    delete.await.unwrap();
    faults::set_io_hook(None);
    let deleted = written.lock().unwrap().expect("the tombstone was written");
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
    open_fence(&c, &addr).await;
    let first = c
        .put_shard(&addr, shard(5), 0, Bytes::from(vec![1u8; 4096]), 1024)
        .await
        .unwrap();
    let page = c
        .list_shards_page_with(&addr, 1, None, 100, true)
        .await
        .unwrap();
    let listed = page.sequences[&shard(5)];
    assert_eq!(listed, RecordToken::of_put(&first));
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
    c.delete_shard_if(&addr, shard(5), 0, RecordToken::of_put(&second))
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
    // A store written without the node has no cluster facts: the node holds
    // its delete fence until an operator releases it (Phase 3.9 review).
    assert_eq!(n.service().delete_fence(), (0, false));
    assert!(n.service().delete_fence_held());
    let release = |token| arca_lake_protocol::client::FencePush {
        release: true,
        release_token: token,
        ..Default::default()
    };
    // A release without the token this start drew (one sent before the
    // facts were lost) does not lift it; one with it does.
    let held = c
        .push_delete_fence_with(&addr, 0, false, release(0))
        .await
        .unwrap();
    assert!(held.2, "{held:?}");
    let token = c.health(&addr).await.unwrap().delete_fence_unknown_token;
    assert_ne!(token, 0);
    let held = c
        .push_delete_fence_with(&addr, 0, false, release(token ^ 1))
        .await
        .unwrap();
    assert!(held.2, "a wrong token: {held:?}");
    let held = c
        .push_delete_fence_with(&addr, 0, false, release(token))
        .await
        .unwrap();
    assert!(!held.2, "{held:?}");
    assert_eq!(c.health(&addr).await.unwrap().delete_fence_unknown_token, 0);
    open_fence(&c, &addr).await;
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

/// A node whose store lost its sequence bound takes a new store epoch:
/// delete tokens handed out before the restart are refused (nothing is
/// deleted), and a new listing hands out tokens that work (Phase 3.5 audit
/// CON-16: without the bound the store could hand an old sequence out
/// again and an old token would match a new record).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tokens_from_before_a_lost_sequence_bound_are_refused() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let c = NodeClients::new(Duration::from_secs(5));
    let addr = n.local_addr.to_string();
    open_fence(&c, &addr).await;
    let put = c
        .put_shard(&addr, shard(6), 0, Bytes::from(vec![6u8; 4096]), 1024)
        .await
        .unwrap();
    let old = RecordToken::of_put(&put);
    n.shutdown().await;
    std::fs::remove_file(dir.path().join("sequence.bound")).unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    // A started node rejoins: the leader's push renews its fence.
    c.push_delete_fence(&addr, 1, true).await.unwrap();
    let e = c
        .delete_shard_if(&addr, shard(6), 0, old)
        .await
        .unwrap_err();
    assert!(NodeClients::is_changed(&e), "{e}");
    assert!(c.get_shard(&addr, shard(6), 0, 0).await.is_ok());
    let page = c
        .list_shards_page_with(&addr, 1, None, 100, false)
        .await
        .unwrap();
    let fresh = page.sequences[&shard(6)];
    assert_ne!(fresh.store_epoch, old.store_epoch);
    c.delete_shard_if(&addr, shard(6), 0, fresh).await.unwrap();
    assert!(c.get_shard(&addr, shard(6), 0, 0).await.is_err());
    n.shutdown().await;
}

/// A node holding a shard, with `c` able to reach it (fence not opened).
async fn node_with_shard(
    dir: &std::path::Path,
) -> (arca_lake_storage_node::NodeHandle, String, NodeClients) {
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(10));
    for i in 1..=4 {
        c.put_shard(&addr, shard(i), 0, Bytes::from(vec![i; 4096]), 1024)
            .await
            .unwrap();
    }
    (n, addr, c)
}

/// Phase 3.9 (ADR-0023): the node is the enforcement point. Without an
/// adopted fence nothing is deleted (fail closed); a delete under another
/// generation, under a closed one, or without one is refused; generations
/// never go back and a closed one never reopens; refusals are counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_need_the_open_fence_generation_the_node_holds() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (n, addr, c) = node_with_shard(dir.path()).await;
    // Nothing adopted: refused whatever the caller believes.
    c.set_delete_fence(Some(1));
    let e = c.delete_shard(&addr, shard(1), 0).await.unwrap_err();
    assert!(NodeClients::is_fenced(&e), "{e}");
    // The client without a fence sends nothing.
    c.set_delete_fence(None);
    let e = c.delete_shard(&addr, shard(1), 0).await.unwrap_err();
    assert!(NodeClients::is_fenced(&e), "{e}");
    // Generation 3 open: only 3 deletes.
    assert_eq!(
        c.push_delete_fence(&addr, 3, true).await.unwrap(),
        (3, true)
    );
    for g in [1, 2, 4] {
        c.set_delete_fence(Some(g));
        let e = c.delete_shard(&addr, shard(1), 0).await.unwrap_err();
        assert!(NodeClients::is_fenced(&e), "generation {g}: {e}");
    }
    c.set_delete_fence(Some(3));
    c.delete_shard(&addr, shard(1), 0).await.unwrap();
    // Lower generations are ignored; closing 3 stops its deletes; 3 never
    // reopens; only a higher generation opens again.
    assert_eq!(
        c.push_delete_fence(&addr, 2, true).await.unwrap(),
        (3, true)
    );
    assert_eq!(
        c.push_delete_fence(&addr, 3, false).await.unwrap(),
        (3, false)
    );
    let e = c.delete_shard(&addr, shard(2), 0).await.unwrap_err();
    assert!(NodeClients::is_fenced(&e), "{e}");
    assert_eq!(
        c.push_delete_fence(&addr, 3, true).await.unwrap(),
        (3, false)
    );
    assert_eq!(
        c.push_delete_fence(&addr, 5, true).await.unwrap(),
        (5, true)
    );
    let e = c.delete_shard(&addr, shard(2), 0).await.unwrap_err();
    assert!(NodeClients::is_fenced(&e), "the old generation 3: {e}");
    c.set_delete_fence(Some(5));
    c.delete_shard(&addr, shard(2), 0).await.unwrap();
    assert!(n.service().fence_refusals() >= 6);
    assert_eq!(
        c.get_shard(&addr, shard(3), 0, 0).await.unwrap(),
        vec![3u8; 4096]
    );
    n.shutdown().await;
}

/// A request below the fenced protocol version (an older controller) is
/// refused even with a generation: no unfenced delete in a mixed cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_older_protocol_delete_is_refused() {
    use arca_lake_protocol::storage::v1 as pb;
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (n, addr, c) = node_with_shard(dir.path()).await;
    c.push_delete_fence(&addr, 1, true).await.unwrap();
    let mut raw = pb::storage_node_client::StorageNodeClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    for (version, fence) in [(2, 1), (2, 0), (3, 0), (pb::FENCED_DELETE_VERSION, 2)] {
        let e = raw
            .delete_shard(pb::DeleteShardRequest {
                protocol_version: version,
                key: Some(pb::ShardKey {
                    group_id: 1,
                    chunk_id: Bytes::from(vec![1u8; 16]),
                    shard_index: 0,
                }),
                delete_fence: fence,
                ..Default::default()
            })
            .await
            .unwrap_err();
        // Refused by the fence, or (an older version than the node
        // accepts, as in a version-bump build) by the version check.
        assert!(
            NodeClients::is_fenced(&e) || e.message().contains("not supported"),
            "v{version} g{fence}: {e}"
        );
    }
    raw.delete_shard(pb::DeleteShardRequest {
        protocol_version: pb::REJOIN_FENCE_VERSION,
        key: Some(pb::ShardKey {
            group_id: 1,
            chunk_id: Bytes::from(vec![1u8; 16]),
            shard_index: 0,
        }),
        delete_fence: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    n.shutdown().await;
}

/// Phase 3.10 downgrade gate (ADR-0024): before the cluster finalized a
/// release with the rejoin fence, the previous release's (protocol 3)
/// deletes are taken, so that deletes go on during a rolling upgrade. Once
/// a leader told the node the finalized schema, only protocol-4 deletes
/// are, also after a restart: a previous release started on metadata
/// restored from before the finalize deletes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_the_finalize_only_releases_with_the_rejoin_fence_delete() {
    use arca_lake_protocol::storage::v1 as pb;
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (n, addr, c) = node_with_shard(dir.path()).await;
    c.push_delete_fence(&addr, 1, true).await.unwrap();
    let delete = async |addr: &str, version: u32, fence: u64, i: u8| {
        let mut raw = pb::storage_node_client::StorageNodeClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        raw.delete_shard(pb::DeleteShardRequest {
            protocol_version: version,
            key: Some(pb::ShardKey {
                group_id: 1,
                chunk_id: Bytes::from(vec![i; 16]),
                shard_index: 0,
            }),
            delete_fence: fence,
            ..Default::default()
        })
        .await
    };
    // A version-bump build no longer speaks protocol 3 at all.
    let speaks_3 = pb::PROTOCOL_VERSION_MIN <= pb::FENCED_DELETE_VERSION;
    if speaks_3 {
        delete(&addr, pb::FENCED_DELETE_VERSION, 1, 1)
            .await
            .unwrap();
    }
    c.health_with(
        &addr,
        HealthRequest {
            schema_floor: pb::REJOIN_FENCE_SCHEMA,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    if speaks_3 {
        let e = delete(&addr, pb::FENCED_DELETE_VERSION, 1, 2)
            .await
            .unwrap_err();
        assert!(NodeClients::is_fenced(&e), "{e}");
    }
    delete(&addr, pb::REJOIN_FENCE_VERSION, 1, 2).await.unwrap();
    n.shutdown().await;
    // The floor survives a restart.
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    assert_eq!(
        c.health(&addr).await.unwrap().schema_floor,
        pb::REJOIN_FENCE_SCHEMA
    );
    c.push_delete_fence(&addr, 2, true).await.unwrap();
    if speaks_3 {
        let e = delete(&addr, pb::FENCED_DELETE_VERSION, 2, 3)
            .await
            .unwrap_err();
        assert!(NodeClients::is_fenced(&e), "{e}");
    }
    delete(&addr, pb::REJOIN_FENCE_VERSION, 2, 3).await.unwrap();
    n.shutdown().await;
}

/// Phase 3.10 review: two clusters (one restored from a backup of the
/// other) opened the same generation number. A push of that open
/// generation by another opener than the one that opened it on the node
/// renews nothing: the node closes it, and deletes under it are refused
/// (the first opener's included); a higher generation opens again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_generation_from_another_opener_closes_it() {
    use arca_lake_protocol::client::FencePush;
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (n, addr, c) = node_with_shard(dir.path()).await;
    let push = async |g: u64, open: bool, opener: u64| {
        c.push_delete_fence_with(
            &addr,
            g,
            open,
            FencePush {
                opener,
                ..FencePush::default()
            },
        )
        .await
        .unwrap()
    };
    assert_eq!(push(3, true, 11).await, (3, true, false));
    c.set_delete_fence(Some(3));
    c.delete_shard(&addr, shard(1), 0).await.unwrap();
    // The same opener renews; another closes.
    assert_eq!(push(3, true, 11).await, (3, true, false));
    assert_eq!(push(3, true, 22).await, (3, false, false));
    let e = c.delete_shard(&addr, shard(2), 0).await.unwrap_err();
    assert!(NodeClients::is_fenced(&e), "{e}");
    // Closed for good, also for the first opener.
    assert_eq!(push(3, true, 11).await, (3, false, false));
    let e = c.delete_shard(&addr, shard(2), 0).await.unwrap_err();
    assert!(NodeClients::is_fenced(&e), "{e}");
    // The opener survives a restart.
    assert_eq!(push(4, true, 22).await, (4, true, false));
    n.shutdown().await;
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let r = c
        .push_delete_fence_with(
            &addr,
            4,
            true,
            FencePush {
                opener: 11,
                ..FencePush::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r, (4, false, false), "another opener after a restart");
    n.shutdown().await;
}

/// Phase 3.10 re-review: once a controller of this release led the node
/// (its push carries an opener), the node takes only protocol-4 deletes,
/// whatever its schema floor (it may have missed the floor, absent or
/// decommissioned at the finalize), also after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_a_leader_of_this_release_only_its_deletes_apply() {
    use arca_lake_protocol::client::FencePush;
    use arca_lake_protocol::storage::v1 as pb;
    if pb::PROTOCOL_VERSION_MIN > pb::FENCED_DELETE_VERSION {
        return; // a version-bump build speaks no protocol 3 at all
    }
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (n, addr, c) = node_with_shard(dir.path()).await;
    let delete = async |addr: &str, version: u32, fence: u64, i: u8| {
        let mut raw = pb::storage_node_client::StorageNodeClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        raw.delete_shard(pb::DeleteShardRequest {
            protocol_version: version,
            key: Some(pb::ShardKey {
                group_id: 1,
                chunk_id: Bytes::from(vec![i; 16]),
                shard_index: 0,
            }),
            delete_fence: fence,
            ..Default::default()
        })
        .await
    };
    // An earlier release's leader (no opener): its deletes apply.
    c.push_delete_fence(&addr, 1, true).await.unwrap();
    delete(&addr, pb::FENCED_DELETE_VERSION, 1, 1)
        .await
        .unwrap();
    // A leader of this release.
    c.push_delete_fence_with(
        &addr,
        2,
        true,
        FencePush {
            opener: 7,
            ..FencePush::default()
        },
    )
    .await
    .unwrap();
    let e = delete(&addr, pb::FENCED_DELETE_VERSION, 2, 2)
        .await
        .unwrap_err();
    assert!(NodeClients::is_fenced(&e), "{e}");
    delete(&addr, pb::REJOIN_FENCE_VERSION, 2, 2).await.unwrap();
    n.shutdown().await;
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    // An earlier release's leader again (opener 0): it opens, but its
    // deletes are refused.
    c.push_delete_fence(&addr, 3, true).await.unwrap();
    let e = delete(&addr, pb::FENCED_DELETE_VERSION, 3, 3)
        .await
        .unwrap_err();
    assert!(NodeClients::is_fenced(&e), "{e}");
    n.shutdown().await;
}

/// A fence change is acknowledged only after every delete that passed the
/// old fence has finished its physical delete, also when the caller gave
/// up on that delete (its RPC cancelled while the store was writing the
/// tombstone): the shared guard lives in the blocking store call, not in
/// the cancelled handler. After the acknowledgement no delete of the old
/// generation completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_the_fence_waits_for_deletes_in_progress_even_cancelled_ones() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (n, addr, c) = node_with_shard(dir.path()).await;
    c.push_delete_fence(&addr, 1, true).await.unwrap();
    c.set_delete_fence(Some(1));
    let mut held = 1;
    for cancel in [false, true] {
        let key = if cancel { shard(3) } else { shard(2) };
        // Hold the next tombstone write 800 ms.
        let armed = Arc::new(AtomicBool::new(true));
        let a = Arc::clone(&armed);
        faults::set_io_hook(Some(Arc::new(move |op: &IoOp| {
            if matches!(op, IoOp::Write { .. }) && a.swap(false, Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(800));
            }
            FaultOutcome::None
        })));
        let (c2, addr2) = (c.clone(), addr.clone());
        let delete = tokio::spawn(async move { c2.delete_shard(&addr2, key, 0).await });
        let waiting = Instant::now();
        while armed.load(Ordering::SeqCst) {
            assert!(waiting.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let in_store = Instant::now();
        if cancel {
            delete.abort();
        }
        let generation = held + 1;
        let ack = c.push_delete_fence(&addr, generation, false).await.unwrap();
        let acked = in_store.elapsed();
        faults::set_io_hook(None);
        assert_eq!(ack, (generation, false));
        assert!(
            acked >= Duration::from_millis(600),
            "cancel={cancel}: the fence change was acknowledged after {acked:?}, while a delete past the old fence was still in the store"
        );
        if !cancel {
            delete.await.unwrap().unwrap();
        }
        // The key is gone either way (the delete passed the fence before
        // it changed); nothing of the old generation goes through now.
        assert_eq!(
            c.get_shard(&addr, key, 0, 0).await.unwrap_err().code(),
            tonic::Code::NotFound
        );
        let e = c.delete_shard(&addr, shard(4), 0).await.unwrap_err();
        assert!(NodeClients::is_fenced(&e), "{e}");
        held = generation + 1;
        c.push_delete_fence(&addr, held, true).await.unwrap();
        c.set_delete_fence(Some(held));
        c.put_shard(&addr, shard(4), 0, Bytes::from(vec![4; 4096]), 1024)
            .await
            .unwrap();
    }
    n.shutdown().await;
}

/// The fence is persisted before it is acknowledged: a restarted node
/// keeps a closed fence closed and an open one open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fence_survives_a_node_restart() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (n, addr, c) = node_with_shard(dir.path()).await;
    c.push_delete_fence(&addr, 7, true).await.unwrap();
    c.push_delete_fence(&addr, 7, false).await.unwrap();
    let listen = n.local_addr;
    n.shutdown().await;
    let n = serve(NodeConfig::new(listen, dir.path())).await.unwrap();
    c.set_delete_fence(Some(7));
    let e = c.delete_shard(&addr, shard(1), 0).await.unwrap_err();
    assert!(NodeClients::is_fenced(&e), "closed before the restart: {e}");
    assert_eq!(n.service().delete_fence(), (7, false));
    c.push_delete_fence(&addr, 8, true).await.unwrap();
    n.shutdown().await;
    let n = serve(NodeConfig::new(listen, dir.path())).await.unwrap();
    c.set_delete_fence(Some(8));
    // The rejoin fence (Phase 3.10): open generation 8 is kept, but a
    // started node admits no delete until the leader pushes it again.
    assert_eq!(n.service().delete_fence(), (8, true));
    let e = c.delete_shard(&addr, shard(1), 0).await.unwrap_err();
    assert!(NodeClients::is_fenced(&e), "before the rejoin push: {e}");
    c.push_delete_fence(&addr, 8, true).await.unwrap();
    c.delete_shard(&addr, shard(1), 0).await.unwrap();
    n.shutdown().await;
}

/// Phase 3.10 rejoin fence: the open fence's lease runs on the node's
/// monotonic clock from the last push of that generation by a caller of
/// the node's incarnation. When pushes stop (the node is cut off, or the
/// leader is gone) deletes are refused after the lease, without the node
/// learning anything about the cluster; the next push reopens. A push
/// from a caller of another incarnation neither opens nor renews.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_open_fence_lapses_without_the_leaders_pushes() {
    use arca_lake_protocol::client::FencePush;
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path());
    cfg.fence_lease = Duration::from_millis(600);
    let n = serve(cfg).await.unwrap();
    let addr = n.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(5));
    for i in 1..=3 {
        c.put_shard(&addr, shard(i), 0, Bytes::from(vec![i; 4096]), 1024)
            .await
            .unwrap();
    }
    c.health_with(
        &addr,
        HealthRequest {
            incarnation: Bytes::from(vec![5; 16]),
            incarnation_changed_ms: 10,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let push = |g, inc: u8| {
        let (c, addr) = (c.clone(), addr.clone());
        async move {
            c.push_delete_fence_with(
                &addr,
                g,
                true,
                FencePush {
                    incarnation: Some([inc; 16]),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        }
    };
    // Another incarnation opens nothing.
    assert_eq!(push(4, 9).await, (0, false, false));
    assert_eq!(push(4, 5).await, (4, true, false));
    c.set_incarnation(Some([5; 16]));
    c.set_delete_fence(Some(4));
    c.delete_shard(&addr, shard(1), 0).await.unwrap();
    assert!(c.health(&addr).await.unwrap().delete_fence_lease_ms > 0);
    // Pushes stop: the lease lapses, deletes are refused.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(c.health(&addr).await.unwrap().delete_fence_lease_ms, 0);
    let e = c.delete_shard(&addr, shard(2), 0).await.unwrap_err();
    assert!(NodeClients::is_fenced(&e), "{e}");
    // A push of another incarnation does not renew it.
    push(4, 9).await;
    let e = c.delete_shard(&addr, shard(2), 0).await.unwrap_err();
    assert!(NodeClients::is_fenced(&e), "{e}");
    // The leader's next push does.
    push(4, 5).await;
    c.delete_shard(&addr, shard(2), 0).await.unwrap();
    assert_eq!(
        c.get_shard(&addr, shard(3), 0, 0).await.unwrap(),
        vec![3u8; 4096]
    );
    n.shutdown().await;
}

/// The operator hold is tied to the node's incarnation when it was set:
/// a caller of an older incarnation (which the node does not adopt) cannot
/// lift it; a caller of a later incarnation the node adopts can; a caller
/// of the held incarnation needs the explicit release.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_a_later_incarnation_or_a_release_lifts_the_hold() {
    use arca_lake_protocol::client::FencePush;
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let (n, addr, c) = node_with_shard(dir.path()).await;
    let facts = |inc: u8, changed_ms: u64| HealthRequest {
        incarnation: Bytes::from(vec![inc; 16]),
        incarnation_changed_ms: changed_ms,
        ..Default::default()
    };
    c.health_with(&addr, facts(2, 20)).await.unwrap();
    let push = |g, open, hold, release, inc: u8| {
        let c = c.clone();
        let addr = addr.clone();
        async move {
            c.push_delete_fence_with(
                &addr,
                g,
                open,
                FencePush {
                    hold,
                    release,
                    incarnation: Some([inc; 16]),
                    release_token: 0,
                    opener: 0,
                },
            )
            .await
            .unwrap()
        }
    };
    assert_eq!(push(4, false, true, false, 2).await, (4, false, true));
    // Older incarnation (not adopted): the hold stays.
    c.health_with(&addr, facts(1, 5)).await.unwrap();
    assert_eq!(push(5, true, false, false, 1).await, (4, false, true));
    // The held incarnation: needs the release.
    assert_eq!(push(5, true, false, false, 2).await, (4, false, true));
    // A later incarnation the node adopts: lifted.
    c.health_with(&addr, facts(3, 30)).await.unwrap();
    assert_eq!(push(5, true, false, false, 3).await, (5, true, false));
    // Held again in 3; a stale caller of 2 cannot set or lift anything
    // beyond what 3 decides; the release of 3 lifts it.
    assert_eq!(push(6, false, true, false, 3).await, (6, false, true));
    assert_eq!(push(7, true, false, false, 2).await, (6, false, true));
    // A release names the generation it releases: below it, nothing.
    assert_eq!(push(5, false, false, true, 3).await, (6, false, true));
    assert_eq!(push(6, false, false, true, 3).await, (6, false, false));
    assert_eq!(push(7, true, false, false, 3).await, (7, true, false));
    // A release delayed past a later close does not lift that close's hold.
    assert_eq!(push(8, false, true, false, 3).await, (8, false, true));
    let delayed_release_of_6 = push(6, false, false, true, 3).await;
    assert_eq!(delayed_release_of_6, (8, false, true));
    // A release never opens; a generation no cluster reaches is ignored.
    assert_eq!(push(9, true, false, true, 3).await, (8, false, true));
    assert_eq!(
        push(u64::MAX, false, false, false, 3).await,
        (8, false, true)
    );
    n.shutdown().await;
}
