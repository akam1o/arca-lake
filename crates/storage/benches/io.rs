//! Storage engine I/O micro-benchmarks.
//!
//! Measures the engine's own overhead on top of the file system: append
//! throughput per record size with and without `fdatasync`, and random
//! range-read throughput including checksum verification.
//!
//! The device under test is whatever backs `ARCA_STORAGE_BENCH_DIR`
//! (default: the system temp directory). Compare against `fio` on the same
//! device to see how much the engine costs; see `benchmarks/README.md`.

#![allow(clippy::cast_possible_truncation)]

use std::path::PathBuf;

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
    for sync in [SyncPolicy::Always, SyncPolicy::Never] {
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

criterion_group!(benches, bench_append, bench_read);
criterion_main!(benches);
