//! Shard write ownership in the mixed window (R-117, ADR-0037): before
//! the cluster is strict, writes 0.1.0 peers rely on are still taken, and
//! counted and reported.

#![allow(missing_docs)]
// The I/O hook is process-wide: the tests hold its serial lock throughout.
#![allow(clippy::await_holding_lock)]

use std::time::Duration;

use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_protocol::storage::v1::{HealthRequest, WRITE_OWNERSHIP_SCHEMA};
use arca_lake_storage::faults;
use arca_lake_storage_node::{NodeConfig, serve};
use bytes::Bytes;

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

/// Before the strict schema (a cluster that may still run 0.1.0
/// processes), a write carrying no incarnation is accepted, as 0.1.0 does,
/// and counted; the node reports its mode. Strict from the schema on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unproven_writes_are_counted_and_reported_until_strict() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(5));
    let h = c
        .health_with(&addr, facts(7, 1, WRITE_OWNERSHIP_SCHEMA - 1))
        .await
        .unwrap();
    assert!(h.write_ownership_supported);
    assert!(!h.write_ownership_strict);
    assert_eq!(h.writes_unproven, 0);
    c.put_shard(&addr, shard(1), 0, data(1), 1024)
        .await
        .unwrap();
    let h = c.health(&addr).await.unwrap();
    assert_eq!(h.writes_unproven, 1);
    assert_eq!(n.service().writes_unproven(), 1);
    c.set_incarnation(Some([7; 16]));
    c.put_shard(&addr, shard(2), 0, data(2), 1024)
        .await
        .unwrap();
    assert_eq!(c.health(&addr).await.unwrap().writes_unproven, 1);
    let h = c
        .health_with(
            &addr,
            HealthRequest {
                schema_floor: WRITE_OWNERSHIP_SCHEMA,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(h.write_ownership_strict);
    assert!(n.service().write_ownership_strict());
    c.set_incarnation(None);
    assert!(refused(
        &c.put_shard(&addr, shard(3), 0, data(3), 1024)
            .await
            .unwrap_err()
    ));
    assert_eq!(c.health(&addr).await.unwrap().writes_unproven, 1);
    n.shutdown().await;
}

/// A disk that holds data and never adopted an incarnation (an earlier
/// release's node never reached by a controller) keeps taking writes as
/// that release did until the cluster is strict, counted; told the strict
/// schema it takes none before its controller gives it the incarnation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_earlier_disk_without_an_incarnation_takes_writes_only_until_strict() {
    let _serial = faults::serial();
    let dir = tempfile::tempdir().unwrap();
    {
        let cfg = NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path());
        let (store, _) = arca_lake_storage::Store::open(cfg.store).unwrap();
        store
            .put_sequenced(
                arca_lake_storage::ShardKey {
                    group_id: 1,
                    chunk_id: [1; 16],
                    shard_index: 0,
                },
                &data(1),
            )
            .unwrap();
    }
    let n = serve(NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path()))
        .await
        .unwrap();
    let addr = n.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(5));
    c.put_shard(&addr, shard(2), 0, data(2), 1024)
        .await
        .unwrap();
    assert_eq!(n.service().writes_unproven(), 1);
    c.health_with(
        &addr,
        HealthRequest {
            schema_floor: WRITE_OWNERSHIP_SCHEMA,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for inc in [None, Some([7; 16])] {
        c.set_incarnation(inc);
        assert!(refused(
            &c.put_shard(&addr, shard(3), 0, data(3), 1024)
                .await
                .unwrap_err()
        ));
    }
    c.health_with(&addr, facts(7, 1, 0)).await.unwrap();
    c.put_shard(&addr, shard(3), 0, data(3), 1024)
        .await
        .unwrap();
    assert_eq!(n.service().writes_unproven(), 1);
    n.shutdown().await;
}
