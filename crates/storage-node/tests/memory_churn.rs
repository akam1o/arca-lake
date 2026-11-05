//! Memory of a storage node under sustained load (Phase 3.4 soak: node RSS
//! grew about 5 MiB/h for a whole process lifetime; Phase 3.5, R-70).
//! Measurement, run explicitly on Linux:
//!
//! `ARCA_CHURN_SECS=1800 ARCA_CHURN_MODE=unique cargo test --release
//! -p arca-lake-storage-node --test memory_churn -- --ignored --nocapture`
//!
//! Modes (`ARCA_CHURN_MODE`), 16 clients, shards of 64 KiB to 1 MiB:
//! - `churn` (default): put, overwrite and delete over 1024 keys;
//! - `unique`: every put a new key, deleted 64 operations later (the soak's
//!   pattern: every delete is of a distinct key, the live set is bounded);
//! - `put`: overwrites of 1024 keys, no deletes;
//! - `get`: reads of 1024 keys written first;
//! - `idle`: the node only.
//!
//! `ARCA_CHURN_COMPACTION=0` turns compaction off. Every minute one JSON
//! line: anonymous RSS, the allocator's bytes in use and free (glibc), the
//! index entry counts, CPU seconds, throughput and put/get p95/p99. A leak
//! grows with the work done; allocator retention levels off (compare with
//! `MALLOC_ARENA_MAX=2`).

#![allow(
    clippy::cast_precision_loss,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_storage_node::compactor::CompactionPolicy;
use arca_lake_storage_node::{NodeConfig, serve};
use bytes::Bytes;

fn cpu_seconds() -> f64 {
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            let rest = s.rsplit_once(") ")?.1.to_string();
            let f: Vec<&str> = rest.split_whitespace().collect();
            Some((f.get(11)?.parse::<f64>().ok()? + f.get(12)?.parse::<f64>().ok()?) / 100.0)
        })
        .unwrap_or(0.0)
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn shard(id: u64) -> ShardRef {
    let mut chunk_id = [0u8; 16];
    chunk_id[..8].copy_from_slice(&id.to_le_bytes());
    ShardRef {
        group_id: 1,
        chunk_id,
        shard_index: 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement"]
async fn rss_under_sustained_churn() {
    let env = |k: &str| std::env::var(k).ok();
    let secs: u64 = env("ARCA_CHURN_SECS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);
    let mode = env("ARCA_CHURN_MODE").unwrap_or_else(|| "churn".into());
    let compaction = env("ARCA_CHURN_COMPACTION").as_deref() != Some("0");
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = NodeConfig::new("127.0.0.1:0".parse().unwrap(), dir.path());
    cfg.store.segment_max_bytes = 256 << 20;
    cfg.compaction = CompactionPolicy {
        interval: if compaction {
            Duration::from_secs(1)
        } else {
            Duration::from_secs(1 << 30)
        },
        min_dead_fraction: 0.5,
        max_segments: 64,
        max_copy_bytes: 512 << 20,
    };
    let node = serve(cfg).await.unwrap();
    let addr = node.local_addr.to_string();
    let clients = NodeClients::new(Duration::from_secs(30));
    let ops = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let lat_put = Arc::new(Mutex::new(Vec::<f64>::new()));
    let lat_get = Arc::new(Mutex::new(Vec::<f64>::new()));
    if mode == "get" {
        for k in 0..1024 {
            let data = Bytes::from(vec![1u8; 256 << 10]);
            clients
                .put_shard(&addr, shard(k), 0, data, 64 << 10)
                .await
                .unwrap();
        }
    }
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut tasks = Vec::new();
    let workers = if mode == "idle" { 0 } else { 16u64 };
    for w in 0..workers {
        let (c, addr, ops, bytes, lp, lg, mode) = (
            clients.clone(),
            addr.clone(),
            Arc::clone(&ops),
            Arc::clone(&bytes),
            Arc::clone(&lat_put),
            Arc::clone(&lat_get),
            mode.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let mut x = 0x9E37_79B9_7F4A_7C15_u64 ^ (w << 40) | 1;
            let mut next = move || {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x
            };
            let mut seq = 0u64;
            let mut window = std::collections::VecDeque::new();
            while Instant::now() < deadline {
                let len = usize::try_from((64 << 10) + next() % ((1 << 20) - (64 << 10))).unwrap();
                let put = async |id: u64| {
                    let t = Instant::now();
                    let data = Bytes::from(vec![u8::try_from(id % 251).unwrap(); len]);
                    c.put_shard(&addr, shard(id), 0, data, 64 << 10)
                        .await
                        .unwrap();
                    lp.lock().unwrap().push(t.elapsed().as_secs_f64() * 1e3);
                    bytes.fetch_add(len as u64, Ordering::Relaxed);
                };
                let get = async |id: u64| {
                    let t = Instant::now();
                    let _ = c.get_shard(&addr, shard(id), 0, 0).await;
                    lg.lock().unwrap().push(t.elapsed().as_secs_f64() * 1e3);
                };
                match mode.as_str() {
                    "unique" => {
                        // Distinct ids per worker: w in the high bits.
                        let id = (w << 48) | seq;
                        seq += 1;
                        put(id).await;
                        get(id).await;
                        window.push_back(id);
                        if window.len() > 64 {
                            let old = window.pop_front().unwrap();
                            let _ = c.delete_shard(&addr, shard(old), 0).await;
                        }
                    }
                    "put" => put(w * 64 + next() % 64).await,
                    "get" => get(next() % 1024).await,
                    _ => {
                        let id = w * 64 + next() % 64;
                        if next() % 4 == 0 {
                            let _ = c.delete_shard(&addr, shard(id), 0).await;
                        } else {
                            put(id).await;
                            get(id).await;
                        }
                    }
                }
                ops.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    let start = Instant::now();
    let reporter = tokio::spawn({
        let (ops, bytes) = (Arc::clone(&ops), Arc::clone(&bytes));
        let (lp, lg) = (Arc::clone(&lat_put), Arc::clone(&lat_get));
        let stats = Arc::clone(node.service().compaction_stats());
        let store = Arc::clone(node.service().store());
        let mode = mode.clone();
        async move {
            loop {
                let m = arca_lake_common::process::memory();
                let sizes = store.index_sizes();
                let (mut p, mut g) = (
                    std::mem::take(&mut *lp.lock().unwrap()),
                    std::mem::take(&mut *lg.lock().unwrap()),
                );
                println!(
                    "{}",
                    serde_json::json!({
                        "t_s": start.elapsed().as_secs(), "mode": mode, "compaction": compaction,
                        "anon_mib": m.anon as f64 / 1_048_576.0,
                        "heap_in_use_mib": m.heap_in_use as f64 / 1_048_576.0,
                        "heap_free_mib": m.heap_free as f64 / 1_048_576.0,
                        "index_live": sizes.live, "index_tombstone_history": sizes.tombstone_history,
                        "index_dead_tracked": sizes.dead_tracked,
                        "cpu_s": cpu_seconds(), "ops": ops.load(Ordering::Relaxed),
                        "put_gib": bytes.load(Ordering::Relaxed) as f64 / f64::from(1u32 << 30),
                        "compactions": stats.compactions.load(Ordering::Relaxed),
                        "compaction_copied_mib": stats.copied_bytes.load(Ordering::Relaxed) >> 20,
                        "put_p95_ms": pct(&mut p, 0.95), "put_p99_ms": pct(&mut p, 0.99),
                        "get_p95_ms": pct(&mut g, 0.95), "get_p99_ms": pct(&mut g, 0.99),
                    })
                );
                if Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
    });
    for t in tasks {
        t.await.unwrap();
    }
    reporter.await.unwrap();
    node.shutdown().await;
}
