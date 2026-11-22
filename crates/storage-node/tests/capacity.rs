//! Capacity admission of a node (Phase 3.6): client writes stop at the
//! reserve, repair goes on to its floor, deletes always pass (a tombstone
//! is what frees space), compaction waits for room for its output, and a
//! write that ends early holds nothing afterwards.

use std::time::{Duration, Instant};

use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_protocol::storage::v1 as pb;
use arca_lake_storage_node::compactor::CompactionPolicy;
use arca_lake_storage_node::{NodeConfig, serve};
use bytes::Bytes;

/// Open delete fence generation 1 on the node at `addr` and take it for
/// `c` (what a controller leader does, ADR-0023).
async fn open_fence(c: &NodeClients, addr: &str) {
    assert_eq!(c.push_delete_fence(addr, 1, true).await.unwrap(), (1, true));
    c.set_delete_fence(Some(1));
}

const MIB: u64 = 1 << 20;

fn shard(i: u32) -> ShardRef {
    let mut chunk_id = [0u8; 16];
    chunk_id[..4].copy_from_slice(&i.to_le_bytes());
    ShardRef {
        group_id: 1,
        chunk_id,
        shard_index: 0,
    }
}

fn data(i: u32) -> Bytes {
    Bytes::from(vec![u8::try_from(i % 251).unwrap(); 256 << 10])
}

fn config(dir: &std::path::Path, capacity: u64, reserve: u64, compaction: Duration) -> NodeConfig {
    let mut cfg = NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir);
    cfg.store.sync = arca_lake_storage::SyncPolicy::Never;
    cfg.store.segment_max_bytes = 2 * MIB;
    cfg.capacity_bytes = Some(capacity);
    cfg.reserve_bytes = Some(reserve);
    cfg.compaction = CompactionPolicy {
        interval: compaction,
        min_dead_fraction: 0.3,
        max_segments: 64,
        max_copy_bytes: 64 << 20,
    };
    cfg
}

/// Write client shards until the node refuses; returns how many it took.
async fn fill(c: &NodeClients, addr: &str, from: u32, class: pb::WriteClass) -> u32 {
    let mut i = from;
    loop {
        match c
            .put_shard_as(addr, shard(i), 0, data(i), 64 << 10, class)
            .await
        {
            Ok(_) => i += 1,
            Err(s) => {
                assert!(NodeClients::is_capacity_refused(&s), "{s}");
                assert!(s.message().contains("insufficient capacity"), "{s}");
                return i - from;
            }
        }
        assert!(i - from < 10_000, "never refused");
    }
}

#[tokio::test]
async fn client_writes_stop_at_the_reserve_repair_at_its_floor_and_deletes_always_pass() {
    let dir = tempfile::tempdir().unwrap();
    let node = serve(config(dir.path(), 48 * MIB, 24 * MIB, Duration::ZERO))
        .await
        .unwrap();
    let addr = node.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(5));
    open_fence(&c, &addr).await;
    let svc = node.service();
    let clients = fill(&c, &addr, 0, pb::WriteClass::Client).await;
    let d = svc.device_capacity().unwrap();
    // Both floors sit on the delete reserve (Phase 3.8): a tombstone for
    // every record held, and some.
    assert!(d.delete_reserve_bytes >= u64::from(clients) * 4096, "{d:?}");
    assert_eq!(d.reserved_bytes, 24 * MIB + d.delete_reserve_bytes);
    assert_eq!(
        d.repair_floor_bytes,
        18 * MIB + d.delete_reserve_bytes,
        "one segment and 16 MiB"
    );
    assert!(
        d.available_bytes >= d.reserved_bytes,
        "client writes crossed the reserve: {d:?}"
    );
    assert!(
        d.available_bytes < d.reserved_bytes + MIB,
        "stopped early: {d:?}"
    );
    assert!(clients > 50, "{clients}");
    // Cache population and migration stop where client writes do.
    assert_eq!(fill(&c, &addr, 20_000, pb::WriteClass::Cache).await, 0);
    assert_eq!(fill(&c, &addr, 20_000, pb::WriteClass::Migration).await, 0);
    // Repair goes on, down to its floor.
    let repairs = fill(&c, &addr, 30_000, pb::WriteClass::Repair).await;
    assert!(repairs > 0);
    let d = svc.device_capacity().unwrap();
    assert!(d.available_bytes >= d.repair_floor_bytes, "{d:?}");
    // Deletes need a tombstone each: they pass however full the node is.
    for i in 0..clients {
        c.delete_shard(&addr, shard(i), 0).await.unwrap();
    }
    // Every write that was acknowledged reads back.
    for i in 30_000..30_000 + repairs {
        assert_eq!(c.get_shard(&addr, shard(i), 0, 0).await.unwrap(), data(i));
    }
    let (admitted, refused) = svc.admission_counts();
    assert_eq!(admitted, u64::from(clients + repairs));
    assert_eq!(refused, [1, 1, 1, 1], "client, repair, migration, cache");
    // The health response carries the device.
    let h = c.health(&addr).await.unwrap();
    assert_eq!(h.devices.len(), 1);
    assert_eq!(
        h.devices[0].reserved_bytes,
        24 * MIB + h.devices[0].delete_reserve_bytes
    );
    assert_eq!(h.devices[0].inflight_bytes, 0);
    node.shutdown().await;
}

#[tokio::test]
async fn compaction_waits_for_room_for_its_output_and_loses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    // No reserve: client writes fill the cap down to the delete reserve, as
    // a misconfigured node (or a node whose disk is shared) could; segments
    // larger than that, so a compaction's output does not fit.
    let mut cfg = config(dir.path(), 24 * MIB, 0, Duration::from_millis(100));
    cfg.store.segment_max_bytes = 8 * MIB;
    let node = serve(cfg).await.unwrap();
    let addr = node.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(5));
    open_fence(&c, &addr).await;
    let n = fill(&c, &addr, 0, pb::WriteClass::Client).await;
    // Half of every sealed segment dead.
    for i in (0..n).filter(|i| i % 2 == 1) {
        c.delete_shard(&addr, shard(i), 0).await.unwrap();
    }
    let stats = std::sync::Arc::clone(node.service().compaction_stats());
    let start = Instant::now();
    while stats
        .skipped_no_space
        .load(std::sync::atomic::Ordering::Relaxed)
        == 0
    {
        assert!(start.elapsed() < Duration::from_secs(10), "never skipped");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(stats.errors.load(std::sync::atomic::Ordering::Relaxed), 0);
    for i in (0..n).step_by(2) {
        assert_eq!(c.get_shard(&addr, shard(i), 0, 0).await.unwrap(), data(i));
    }
    // With room again (a larger cap), compaction proceeds.
    node.shutdown().await;
    let mut cfg = config(dir.path(), 64 * MIB, 0, Duration::from_millis(100));
    cfg.store.segment_max_bytes = 8 * MIB;
    let node = serve(cfg).await.unwrap();
    let addr = node.local_addr.to_string();
    let stats = std::sync::Arc::clone(node.service().compaction_stats());
    let start = Instant::now();
    while stats.compactions.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        assert!(start.elapsed() < Duration::from_secs(10), "never compacted");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for i in 0..n {
        let r = c.get_shard(&addr, shard(i), 0, 0).await;
        if i % 2 == 0 {
            assert_eq!(r.unwrap(), data(i));
        } else {
            assert_eq!(r.unwrap_err().code(), tonic::Code::NotFound);
        }
    }
    node.shutdown().await;
}

#[tokio::test]
async fn a_write_that_ends_early_holds_nothing_and_a_restart_starts_clean() {
    let dir = tempfile::tempdir().unwrap();
    let node = serve(config(dir.path(), 64 * MIB, 16 * MIB, Duration::ZERO))
        .await
        .unwrap();
    let addr = node.local_addr;
    let svc = std::sync::Arc::clone(node.service());
    // A writer that sends its header and one frame, then stalls (a gateway
    // that hangs or crashes mid-write).
    let mut client = pb::storage_node_client::StorageNodeClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(pb::PutShardRequest {
        body: Some(pb::put_shard_request::Body::Header(pb::PutShardHeader {
            protocol_version: pb::PROTOCOL_VERSION,
            key: Some(pb::ShardKey {
                group_id: 1,
                chunk_id: Bytes::copy_from_slice(&shard(1).chunk_id),
                shard_index: 0,
            }),
            group_epoch: 0,
            payload_len: 8 * MIB,
            write_class: pb::WriteClass::Client.into(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    tx.send(pb::PutShardRequest {
        body: Some(pb::put_shard_request::Body::Data(Bytes::from(vec![
            0u8;
            1 << 16
        ]))),
    })
    .await
    .unwrap();
    let call = tokio::spawn(async move {
        client
            .put_shard(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
    });
    let start = Instant::now();
    while svc.device_capacity().unwrap().inflight_bytes == 0 {
        assert!(start.elapsed() < Duration::from_secs(5), "never admitted");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(svc.device_capacity().unwrap().inflight_bytes >= 8 * MIB);
    // The writer goes away: the hold is released with the stream.
    call.abort();
    drop(tx);
    let start = Instant::now();
    while svc.device_capacity().unwrap().inflight_bytes != 0 {
        assert!(start.elapsed() < Duration::from_secs(15), "hold leaked");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Nothing was stored, and a restarted node holds nothing either.
    let c = NodeClients::new(Duration::from_secs(5));
    assert_eq!(
        c.get_shard(&addr.to_string(), shard(1), 0, 0)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    drop(svc);
    node.shutdown().await;
    let node = serve(config(dir.path(), 64 * MIB, 16 * MIB, Duration::ZERO))
        .await
        .unwrap();
    let d = node.service().device_capacity().unwrap();
    assert_eq!(d.inflight_bytes, 0);
    assert_eq!(node.service().admission_counts().0, 0);
    node.shutdown().await;
}
