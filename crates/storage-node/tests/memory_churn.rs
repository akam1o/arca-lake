//! Memory of a storage node under sustained shard churn (Phase 3.4 soak:
//! node RSS grew about 5 MiB/h for a whole process lifetime). Measurement,
//! run explicitly on Linux:
//!
//! `ARCA_CHURN_SECS=1800 cargo test --release -p arca-lake-storage-node
//! --test memory_churn -- --ignored --nocapture`
//!
//! 16 clients put, overwrite and delete shards of 64 KiB to 1 MiB over a
//! bounded key space (the live set stays constant) with compaction every
//! second; the anonymous RSS of the process is printed every minute. A
//! leak keeps growing with the work done; allocator retention levels off
//! (compare with `MALLOC_ARENA_MAX=2`).

#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_storage_node::compactor::CompactionPolicy;
use arca_lake_storage_node::{NodeConfig, serve};
use bytes::Bytes;

fn rss_anon_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("RssAnon:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement"]
async fn rss_under_sustained_churn() {
    let secs: u64 = std::env::var("ARCA_CHURN_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path());
    cfg.store.segment_max_bytes = 256 << 20;
    cfg.compaction = CompactionPolicy {
        interval: Duration::from_secs(1),
        min_dead_fraction: 0.5,
        max_segments: 64,
        max_copy_bytes: 512 << 20,
    };
    let node = serve(cfg).await.unwrap();
    let addr = node.local_addr.to_string();
    let clients = NodeClients::new(Duration::from_secs(30));
    let ops = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut tasks = Vec::new();
    for w in 0..16u64 {
        let (c, addr, ops, bytes) = (
            clients.clone(),
            addr.clone(),
            Arc::clone(&ops),
            Arc::clone(&bytes),
        );
        tasks.push(tokio::spawn(async move {
            let mut x = 0x9E37_79B9_7F4A_7C15_u64 ^ (w << 40) | 1;
            let mut next = move || {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x
            };
            while Instant::now() < deadline {
                let k = next() % 64;
                let mut chunk_id = [0u8; 16];
                chunk_id[..8].copy_from_slice(&(w * 1000 + k).to_le_bytes());
                let shard = ShardRef {
                    group_id: 1,
                    chunk_id,
                    shard_index: 0,
                };
                if next() % 4 == 0 {
                    let _ = c.delete_shard(&addr, shard, 0).await;
                } else {
                    let len =
                        usize::try_from((64 << 10) + next() % ((1 << 20) - (64 << 10))).unwrap();
                    let data = Bytes::from(vec![u8::try_from(k).unwrap(); len]);
                    c.put_shard(&addr, shard, 0, data, 64 << 10).await.unwrap();
                    bytes.fetch_add(len as u64, Ordering::Relaxed);
                    let _ = c.get_shard(&addr, shard, 0, 0).await;
                }
                ops.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    let start = Instant::now();
    let reporter = tokio::spawn({
        let (ops, bytes) = (Arc::clone(&ops), Arc::clone(&bytes));
        let stats = Arc::clone(node.service().compaction_stats());
        async move {
            while Instant::now() < deadline {
                println!(
                    "{{\"t_s\":{},\"rss_anon_kib\":{},\"ops\":{},\"put_gib\":{:.2},\"compactions\":{},\"segment_mib\":{}}}",
                    start.elapsed().as_secs(),
                    rss_anon_kib(),
                    ops.load(Ordering::Relaxed),
                    bytes.load(Ordering::Relaxed) as f64 / f64::from(1u32 << 30),
                    stats.compactions.load(Ordering::Relaxed),
                    stats.segment_bytes.load(Ordering::Relaxed) >> 20,
                );
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
    });
    for t in tasks {
        t.await.unwrap();
    }
    reporter.await.unwrap();
    println!("{{\"final\":true,\"rss_anon_kib\":{}}}", rss_anon_kib());
    node.shutdown().await;
}
