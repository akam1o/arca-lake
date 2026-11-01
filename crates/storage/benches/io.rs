//! Storage engine I/O micro-benchmarks.
//!
//! Measures the engine's own overhead on top of the file system: append
//! throughput per record size for each durability policy, the effect of
//! group commit under concurrent writers, and random range-read throughput
//! including checksum verification.
//!
//! The device under test is whatever backs `ARCA_STORAGE_BENCH_DIR`
//! (default: the system temp directory). Compare against `fio` on the same
//! device to see how much the engine costs; see `benchmarks/README.md`.

#![allow(clippy::cast_possible_truncation)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arca_lake_storage::{ShardKey, Store, StoreConfig, SyncPolicy};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn bench_dir() -> PathBuf {
    std::env::var_os("ARCA_STORAGE_BENCH_DIR").map_or_else(std::env::temp_dir, PathBuf::from)
}

fn record_sizes() -> Vec<usize> {
    let mut v = vec![64 << 10, 1 << 20];
    if std::env::var_os("ARCA_STORAGE_BENCH_FULL").is_some() {
        v.push(4 << 20);
    }
    v
}

fn key(i: u64) -> ShardKey {
    let mut chunk_id = [0u8; 16];
    chunk_id[..8].copy_from_slice(&i.to_le_bytes());
    ShardKey {
        group_id: 1,
        chunk_id,
        shard_index: 0,
    }
}

fn open(sync: SyncPolicy) -> (tempfile::TempDir, Store) {
    let dir = tempfile::Builder::new()
        .prefix("arca-storage-bench-")
        .tempdir_in(bench_dir())
        .unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.sync = sync;
    let (store, _) = Store::open(cfg).unwrap();
    (dir, store)
}

fn bench_append(c: &mut Criterion) {
    let mut g = c.benchmark_group("append");
    g.sample_size(20);
    for sync in [SyncPolicy::Always, SyncPolicy::Group, SyncPolicy::Never] {
        for len in record_sizes() {
            let (_dir, store) = open(sync);
            let payload = vec![0xA5u8; len];
            let mut i = 0u64;
            g.throughput(Throughput::Bytes(len as u64));
            g.bench_with_input(BenchmarkId::new(format!("{sync:?}"), len), &len, |b, _| {
                b.iter(|| {
                    store.put(key(i), &payload).unwrap();
                    i += 1;
                });
            });
        }
    }
    g.finish();
}

/// Concurrent writers: total throughput of `threads` writers each appending
/// records of `len` bytes, per durability policy. This is where group commit
/// is expected to matter.
fn bench_append_concurrent(c: &mut Criterion) {
    let mut g = c.benchmark_group("append_concurrent");
    g.sample_size(10);
    let len = 64 << 10;
    for sync in [SyncPolicy::Always, SyncPolicy::Group] {
        for threads in [1usize, 4, 16] {
            let (_dir, store) = open(sync);
            let store = Arc::new(store);
            g.throughput(Throughput::Bytes((len * threads) as u64));
            g.bench_with_input(
                BenchmarkId::new(format!("{sync:?}"), threads),
                &threads,
                |b, &threads| {
                    b.iter_custom(|iters| {
                        let payload = Arc::new(vec![0x5Au8; len]);
                        let start = Instant::now();
                        let handles: Vec<_> = (0..threads)
                            .map(|t| {
                                let store = Arc::clone(&store);
                                let payload = Arc::clone(&payload);
                                std::thread::spawn(move || {
                                    for i in 0..iters {
                                        store
                                            .put(key(i * threads as u64 + t as u64), &payload)
                                            .unwrap();
                                    }
                                })
                            })
                            .collect();
                        for h in handles {
                            h.join().unwrap();
                        }
                        // Report per-"iteration" time where one iteration is
                        // one record from each thread.
                        let elapsed = start.elapsed();
                        Duration::from_nanos((elapsed.as_nanos() / u128::from(iters.max(1))) as u64)
                            * iters as u32
                    });
                },
            );
        }
    }
    g.finish();
}

fn bench_read(c: &mut Criterion) {
    let mut g = c.benchmark_group("read");
    g.sample_size(20);
    let (_dir, store) = open(SyncPolicy::Never);
    let len: usize = 1 << 20;
    let count = 64u64;
    let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    for i in 0..count {
        store.put(key(i), &payload).unwrap();
    }
    for range_len in [4u64 << 10, 64 << 10, len as u64] {
        let mut i = 0u64;
        g.throughput(Throughput::Bytes(range_len));
        g.bench_with_input(BenchmarkId::new("range", range_len), &range_len, |b, _| {
            b.iter(|| {
                let k = key(i % count);
                let start = (i * 4096) % (len as u64 - range_len + 1);
                let got = store.get(&k, Some(start..start + range_len)).unwrap();
                i += 1;
                got
            });
        });
    }
    g.finish();
}

criterion_group!(benches, bench_append, bench_append_concurrent, bench_read);
criterion_main!(benches);
