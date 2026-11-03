//! Background compaction in a running node (RFC-005 §5.6): dead space of
//! deleted shards is reclaimed while the node serves, and every surviving
//! shard reads back unchanged, also after a restart.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_storage_node::compactor::CompactionPolicy;
use arca_lake_storage_node::{NodeConfig, serve};
use bytes::Bytes;

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
    Bytes::from(vec![u8::try_from(i % 251).unwrap(); 64 << 10])
}

fn config(dir: &std::path::Path, listen: std::net::SocketAddr) -> NodeConfig {
    let mut cfg = NodeConfig::new(listen, dir);
    cfg.store.sync = arca_lake_storage::SyncPolicy::Never;
    cfg.store.segment_max_bytes = 1 << 20;
    cfg.compaction = CompactionPolicy {
        interval: Duration::from_millis(100),
        min_dead_fraction: 0.5,
        max_segments: 4,
        max_copy_bytes: 64 << 20,
    };
    cfg
}

#[tokio::test]
async fn deleted_space_is_reclaimed_while_serving_and_survivors_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let node = serve(config(dir.path(), "127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let addr = node.local_addr.to_string();
    let c = NodeClients::new(Duration::from_secs(5));
    for i in 0..120 {
        c.put_shard(&addr, shard(i), 0, data(i), 16 << 10)
            .await
            .unwrap();
    }
    let seg_bytes = |dir: &std::path::Path| -> u64 {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with("seg-"))
            .map(|e| e.metadata().unwrap().len())
            .sum()
    };
    let before = seg_bytes(dir.path());
    // Delete four of every five shards.
    for i in (0..120).filter(|i| i % 5 != 0) {
        c.delete_shard(&addr, shard(i), 0).await.unwrap();
    }
    let stats = std::sync::Arc::clone(node.service().compaction_stats());
    let start = Instant::now();
    while seg_bytes(dir.path()) >= before / 2 {
        assert!(start.elapsed() < Duration::from_secs(20), "no compaction");
        // Reads during compaction never fail or see wrong data.
        for i in (0..120).step_by(5) {
            assert_eq!(c.get_shard(&addr, shard(i), 0, 0).await.unwrap(), data(i));
        }
    }
    assert_eq!(stats.errors.load(Ordering::Relaxed), 0);
    assert!(stats.compactions.load(Ordering::Relaxed) > 0);
    assert!(stats.reclaimed_bytes.load(Ordering::Relaxed) > 0);
    // Settled: idle passes do not keep rewriting segments whose tombstones
    // are still needed (the churn found by the soak test).
    tokio::time::sleep(Duration::from_millis(500)).await;
    let settled = stats.compactions.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        stats.compactions.load(Ordering::Relaxed),
        settled,
        "compaction churns while idle"
    );
    assert_eq!(
        stats.backlog_segments.load(Ordering::Relaxed),
        0,
        "compaction backlog left"
    );
    for i in 0..120 {
        let r = c.get_shard(&addr, shard(i), 0, 0).await;
        if i % 5 == 0 {
            assert_eq!(r.unwrap(), data(i));
        } else {
            assert_eq!(r.unwrap_err().code(), tonic::Code::NotFound, "{i}");
        }
    }
    // A restart rebuilds the same state from the compacted segments.
    let listen = node.local_addr;
    node.shutdown().await;
    let node = serve(config(dir.path(), listen)).await.unwrap();
    let c = NodeClients::new(Duration::from_secs(5));
    for i in 0..120 {
        let r = c.get_shard(&addr, shard(i), 0, 0).await;
        if i % 5 == 0 {
            assert_eq!(r.unwrap(), data(i));
        } else {
            assert_eq!(
                r.unwrap_err().code(),
                tonic::Code::NotFound,
                "deleted {i} resurrected"
            );
        }
    }
    node.shutdown().await;
}
