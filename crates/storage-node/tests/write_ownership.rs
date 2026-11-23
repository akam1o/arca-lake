//! Shard write ownership (R-117, ADR-0037): a node takes a `PutShard`
//! only from a writer of its own cluster, as far as the incarnation it
//! adopted can tell, over the real RPC.

#![allow(missing_docs)]
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

/// The schema from which nodes are strict (ADR-0037), written out so that
/// the behavioural tests build on a release that does not name it.
const STRICT_SCHEMA: u64 = 5;

fn shard(i: u8) -> ShardRef {
    ShardRef {
        group_id: 1,
        chunk_id: [i; 16],
        shard_index: 0,
    }
}

fn data(i: u8) -> Bytes {
    Bytes::from(vec![i; 4096])
}

/// What a controller's health pass gives node 1: its identity (token
/// [`TOKEN`]), the incarnation and the schema floor.
fn facts(incarnation: u8, changed_ms: u64, schema_floor: u64) -> HealthRequest {
    HealthRequest {
        incarnation: Bytes::copy_from_slice(&[incarnation; 16]),
        incarnation_changed_ms: changed_ms,
        schema_floor,
        node_id: 1,
        node_identity: Bytes::from_static(&TOKEN),
        adopt_identity: true,
        ..Default::default()
    }
}

/// The identity token node 1 acknowledged.
const TOKEN: [u8; 16] = [0x11; 16];

/// A pool that writes for node 1 at `addr` (the placement member).
fn writer(addr: &str) -> NodeClients {
    let c = NodeClients::new(Duration::from_secs(10));
    c.note_node_id(addr, 1);
    c
}

fn refused(e: &tonic::Status) -> bool {
    e.code() == tonic::Code::FailedPrecondition && e.message().starts_with("incarnation rejected")
}

/// A node that has adopted no incarnation (a new disk its controller has
/// not reached yet, possibly at an address another cluster still maps)
/// cannot tell its cluster's writers from another's: it refuses every
/// write, carrying an incarnation or not, and stores nothing. Once its
/// controller gave it the incarnation, that cluster's writes go through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_adopted_no_incarnation_refuses_every_write() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let c = writer(&addr);
    for inc in [Some([9; 16]), None] {
        c.set_incarnation(inc);
        let e = c
            .put_shard(&addr, shard(1), 0, data(1), 1024)
            .await
            .unwrap_err();
        assert!(refused(&e), "{inc:?}: {e}");
    }
    assert_eq!(
        c.get_shard(&addr, shard(1), 0, 0).await.unwrap_err().code(),
        tonic::Code::NotFound,
        "a refused write was stored"
    );
    assert_eq!(c.health(&addr).await.unwrap().live_shards, 0);
    // The cluster's controller reaches it: its writers are taken, another
    // cluster's are not.
    c.health_with(&addr, facts(7, 1, 4)).await.unwrap();
    c.set_incarnation(Some([7; 16]));
    c.put_shard(&addr, shard(1), 0, data(1), 1024)
        .await
        .unwrap();
    c.set_incarnation(Some([9; 16]));
    assert!(refused(
        &c.put_shard(&addr, shard(2), 0, data(2), 1024)
            .await
            .unwrap_err()
    ));
    assert_eq!(c.get_shard(&addr, shard(1), 0, 0).await.unwrap(), data(1));
    n.shutdown().await;
}

/// Told the strict schema, a node refuses a write carrying no incarnation
/// (a tool that holds none, or a process of a release that predates the
/// check) as it refuses another's; its own cluster's writes go through,
/// and a restart keeps both the incarnation and the strict mode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_strict_node_refuses_writes_without_its_incarnation() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let listen = n.local_addr;
    let addr = listen.to_string();
    let c = writer(&addr);
    c.health_with(&addr, facts(7, 1, STRICT_SCHEMA))
        .await
        .unwrap();
    for (inc, ok) in [(None, false), (Some([9; 16]), false), (Some([7; 16]), true)] {
        c.set_incarnation(inc);
        let r = c.put_shard(&addr, shard(3), 0, data(3), 1024).await;
        if ok {
            r.unwrap();
        } else {
            let e = r.unwrap_err();
            assert!(refused(&e), "{inc:?}: {e}");
        }
    }
    // A restart (no controller push yet): the same.
    n.shutdown().await;
    let n = serve(NodeConfig::new(listen, dir.path())).await.unwrap();
    c.forget(&addr);
    c.set_incarnation(None);
    assert!(refused(
        &c.put_shard(&addr, shard(4), 0, data(4), 1024)
            .await
            .unwrap_err()
    ));
    c.set_incarnation(Some([7; 16]));
    c.put_shard(&addr, shard(4), 0, data(4), 1024)
        .await
        .unwrap();
    assert_eq!(c.get_shard(&addr, shard(3), 0, 0).await.unwrap(), data(3));
    n.shutdown().await;
}

/// A new incarnation adopted while a write is in flight: a write whose
/// body arrives after it is refused (checked again once the body exists),
/// and the adoption is acknowledged only once no write that passed the
/// old incarnation can still be stored (the check and the store write
/// hold the node's fence guard shared; the adoption takes it exclusive).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_incarnation_is_acknowledged_only_after_writes_past_the_old_one() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let c = writer(&addr);
    c.health_with(&addr, facts(7, 1, 0)).await.unwrap();
    c.set_incarnation(Some([7; 16]));
    // The first record write the put does sleeps in the store.
    let armed = Arc::new(AtomicBool::new(true));
    let written = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let (arm, wrote) = (Arc::clone(&armed), Arc::clone(&written));
    faults::set_io_hook(Some(Arc::new(move |op: &IoOp| {
        if matches!(op, IoOp::Write { .. }) && arm.swap(false, Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(800));
            *wrote.lock().unwrap() = Some(Instant::now());
        }
        FaultOutcome::None
    })));
    let (c2, addr2) = (c.clone(), addr.clone());
    let put = tokio::spawn(async move { c2.put_shard(&addr2, shard(5), 0, data(5), 1024).await });
    let waiting = Instant::now();
    while armed.load(Ordering::SeqCst) {
        assert!(
            waiting.elapsed() < Duration::from_secs(10),
            "the write never reached the store"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // A recovery's new incarnation.
    c.health_with(&addr, facts(8, 2, 0)).await.unwrap();
    let adopted = Instant::now();
    let r = put.await.unwrap();
    faults::set_io_hook(None);
    let stored = written.lock().unwrap().expect("the record was written");
    assert!(
        stored <= adopted,
        "the new incarnation was acknowledged while a write of the old one was still in the store"
    );
    r.unwrap();
    // The old incarnation's writers are refused from the acknowledgement.
    assert!(refused(
        &c.put_shard(&addr, shard(6), 0, data(6), 1024)
            .await
            .unwrap_err()
    ));

    // A body still in flight when the incarnation changes.
    let mut client =
        arca_lake_protocol::storage::v1::storage_node_client::StorageNodeClient::connect(format!(
            "http://{addr}"
        ))
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(arca_lake_protocol::storage::v1::PutShardRequest {
        body: Some(
            arca_lake_protocol::storage::v1::put_shard_request::Body::Header(
                arca_lake_protocol::storage::v1::PutShardHeader {
                    protocol_version: arca_lake_protocol::storage::v1::PROTOCOL_VERSION,
                    key: Some(arca_lake_protocol::storage::v1::ShardKey {
                        group_id: 1,
                        chunk_id: Bytes::copy_from_slice(&[7; 16]),
                        shard_index: 0,
                    }),
                    group_epoch: 0,
                    payload_len: 4096,
                    write_class: 0,
                    incarnation: Bytes::copy_from_slice(&[8; 16]),
                    ..Default::default()
                },
            ),
        ),
    })
    .await
    .unwrap();
    let call = tokio::spawn(async move {
        client
            .put_shard(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
    });
    // The header passed (the node waits for the body) when the next
    // incarnation is adopted.
    let waiting = Instant::now();
    while n.service().stats().puts_in_flight == 0 {
        assert!(waiting.elapsed() < Duration::from_secs(10), "no write open");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    c.health_with(&addr, facts(9, 3, 0)).await.unwrap();
    tx.send(arca_lake_protocol::storage::v1::PutShardRequest {
        body: Some(arca_lake_protocol::storage::v1::put_shard_request::Body::Data(data(7))),
    })
    .await
    .unwrap();
    drop(tx);
    let e = call.await.unwrap().unwrap_err();
    assert!(refused(&e), "{e}");
    assert_eq!(
        c.get_shard(&addr, shard(7), 0, 0).await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    n.shutdown().await;
}

/// A write names the node it is for (the placement member): another node
/// of the same cluster at the address the writer maps to that member (an
/// address reused within the cluster, a stale map) refuses it, also when
/// only its identity token differs (another disk of that node id), and
/// stores nothing. Strict, a write naming no node is refused too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_for_another_node_is_refused() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let admin = NodeClients::new(Duration::from_secs(5));
    // Node 1 of the cluster of incarnation 7 (as its controller made it).
    admin.health_with(&addr, facts(7, 1, 4)).await.unwrap();
    let for_node = |id: u64, token: Option<[u8; 16]>| {
        let c = NodeClients::new(Duration::from_secs(5));
        c.set_incarnation(Some([7; 16]));
        c.note_node_id(&addr, id);
        c.note_identity(&addr, token.map(|t| (id, t)));
        c
    };
    for (who, c) in [
        ("node 2", for_node(2, None)),
        ("node 2 with a token", for_node(2, Some([0x22; 16]))),
        ("node 1, another disk", for_node(1, Some([0x99; 16]))),
    ] {
        let e = c
            .put_shard(&addr, shard(1), 0, data(1), 1024)
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{who}: {e}");
        assert!(e.message().starts_with("node rejected"), "{who}: {e}");
    }
    assert_eq!(
        admin
            .get_shard(&addr, shard(1), 0, 0)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound,
        "a write for another node was stored"
    );
    // Its own writers: named with or without its token.
    for_node(1, Some(TOKEN))
        .put_shard(&addr, shard(1), 0, data(1), 1024)
        .await
        .unwrap();
    for_node(1, None)
        .put_shard(&addr, shard(2), 0, data(2), 1024)
        .await
        .unwrap();
    // Strict: a write that names no node (an earlier release's writer).
    admin
        .health_with(
            &addr,
            HealthRequest {
                schema_floor: STRICT_SCHEMA,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let unnamed = NodeClients::new(Duration::from_secs(5));
    unnamed.set_incarnation(Some([7; 16]));
    let e = unnamed
        .put_shard(&addr, shard(3), 0, data(3), 1024)
        .await
        .unwrap_err();
    assert!(e.message().starts_with("node rejected"), "{e}");
    for_node(1, None)
        .put_shard(&addr, shard(3), 0, data(3), 1024)
        .await
        .unwrap();
    n.shutdown().await;
}

/// A disk adopting its identity while a write checked without one is in
/// the store (review L3): the adoption is acknowledged only once that
/// write is stored, so no write is taken against the disk's previous
/// (lack of) identity after the node reports the new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_identity_is_acknowledged_only_after_writes_checked_without_it() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let c = writer(&addr);
    // The incarnation only (no identity yet), not strict.
    c.health_with(
        &addr,
        HealthRequest {
            incarnation: Bytes::from_static(&[7; 16]),
            incarnation_changed_ms: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    c.set_incarnation(Some([7; 16]));
    let armed = Arc::new(AtomicBool::new(true));
    let written = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let (arm, wrote) = (Arc::clone(&armed), Arc::clone(&written));
    faults::set_io_hook(Some(Arc::new(move |op: &IoOp| {
        if matches!(op, IoOp::Write { .. }) && arm.swap(false, Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(800));
            *wrote.lock().unwrap() = Some(Instant::now());
        }
        FaultOutcome::None
    })));
    let (c2, addr2) = (c.clone(), addr.clone());
    let put = tokio::spawn(async move { c2.put_shard(&addr2, shard(5), 0, data(5), 1024).await });
    let waiting = Instant::now();
    while armed.load(Ordering::SeqCst) {
        assert!(
            waiting.elapsed() < Duration::from_secs(10),
            "the write never reached the store"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The controller gives the disk its identity: node 2.
    let health = c
        .health_with(
            &addr,
            HealthRequest {
                node_id: 2,
                node_identity: Bytes::from_static(&[0x22; 16]),
                adopt_identity: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let adopted = Instant::now();
    assert_eq!(health.identity_node_id, 2);
    let put = put.await.unwrap();
    faults::set_io_hook(None);
    let stored = written.lock().unwrap().expect("the record was written");
    assert!(
        stored <= adopted,
        "the identity was acknowledged while a write checked without it was still in the store"
    );
    put.unwrap();
    // From the acknowledgement, writes for node 1 are refused.
    let e = c
        .put_shard(&addr, shard(6), 0, data(6), 1024)
        .await
        .unwrap_err();
    assert!(e.message().starts_with("node rejected"), "{e}");
    n.shutdown().await;
}

/// A strict node that holds the cluster's incarnation but no identity
/// cannot check the node a write names, and refuses it (re-review of
/// ADR-0037): a disk whose identity file was removed (it keeps its facts
/// and incarnation; the controller then pushes no adoption), and a
/// plaintext node never given one. Not strict, the same write is taken and
/// counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_strict_node_without_an_identity_refuses_named_writes() {
    let _serial = faults::serial();
    // The identity file removed.
    let dir = tempfile::tempdir().unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let listen = n.local_addr;
    let addr = listen.to_string();
    let c = writer(&addr);
    c.set_incarnation(Some([7; 16]));
    c.health_with(&addr, facts(7, 1, STRICT_SCHEMA))
        .await
        .unwrap();
    c.put_shard(&addr, shard(1), 0, data(1), 1024)
        .await
        .unwrap();
    n.shutdown().await;
    std::fs::remove_file(dir.path().join("node-identity.txt")).unwrap();
    let n = serve(NodeConfig::new(listen, dir.path())).await.unwrap();
    c.forget(&addr);
    let health = c.health(&addr).await.unwrap();
    assert_eq!(
        (health.identity_node_id, health.incarnation.as_ref()),
        (0, &[7u8; 16][..])
    );
    assert!(health.write_ownership_strict);
    let e = c
        .put_shard(&addr, shard(2), 0, data(2), 1024)
        .await
        .unwrap_err();
    assert!(e.message().starts_with("node rejected"), "{e}");
    n.shutdown().await;

    // A plaintext node given the incarnation and the strict schema but no
    // identity; not strict, the same write is taken, counted.
    for (floor, taken) in [(STRICT_SCHEMA, false), (4, true)] {
        let dir = tempfile::tempdir().unwrap();
        let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
            .await
            .unwrap();
        let addr = n.local_addr.to_string();
        let c = writer(&addr);
        c.set_incarnation(Some([7; 16]));
        c.health_with(
            &addr,
            HealthRequest {
                incarnation: Bytes::from_static(&[7; 16]),
                incarnation_changed_ms: 1,
                schema_floor: floor,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let put = c.put_shard(&addr, shard(3), 0, data(3), 1024).await;
        if taken {
            put.unwrap();
            assert_eq!(n.service().writes_unproven(), 1);
        } else {
            let e = put.unwrap_err();
            assert!(
                e.message().starts_with("node rejected"),
                "floor {floor}: {e}"
            );
        }
        n.shutdown().await;
    }
}
