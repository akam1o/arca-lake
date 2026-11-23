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

fn facts(incarnation: u8, changed_ms: u64, schema_floor: u64) -> HealthRequest {
    HealthRequest {
        incarnation: Bytes::copy_from_slice(&[incarnation; 16]),
        incarnation_changed_ms: changed_ms,
        schema_floor,
        ..Default::default()
    }
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
    let c = NodeClients::new(Duration::from_secs(5));
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
    let c = NodeClients::new(Duration::from_secs(5));
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
    let c = NodeClients::new(Duration::from_secs(10));
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
