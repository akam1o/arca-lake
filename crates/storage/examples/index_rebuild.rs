//! Measures how long `Store::open` takes to rebuild the in-memory index
//! from segments, to decide whether the index needs to be persisted
//! (RFC-005 open question; OD-09 neighbourhood).
//!
//! Usage: `cargo run --release -p arca-lake-storage --example index_rebuild
//! -- <dir> <records> <payload_bytes>`
//!
//! Writes `records` shards with `SyncPolicy::Never` (durability is not
//! what is measured), drops the store, then opens it again and reports the
//! wall time of the open and the segment bytes scanned. Numbers depend on
//! the machine and are relative; see `benchmarks/README.md` before quoting
//! them anywhere.

use std::time::Instant;

use arca_lake_storage::{ShardKey, Store, StoreConfig, SyncPolicy};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("dir");
    let records: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let payload: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(4096);
    std::fs::create_dir_all(&dir).expect("create dir");

    let cfg = StoreConfig {
        dir: dir.clone().into(),
        segment_max_bytes: 1 << 30,
        record_alignment: 512,
        block_size: 4096,
        sync: SyncPolicy::Never,
    };
    let data = vec![0xA5u8; payload];
    let t = Instant::now();
    {
        let (store, _) = Store::open(cfg.clone()).expect("open for writing");
        for i in 0..records {
            let mut chunk_id = [0u8; 16];
            chunk_id[8..].copy_from_slice(&i.to_be_bytes());
            let key = ShardKey {
                group_id: 1,
                chunk_id,
                shard_index: 0,
            };
            store.put(key, &data).expect("put");
        }
    }
    let write_s = t.elapsed().as_secs_f64();

    let t = Instant::now();
    let (store, report) = Store::open(cfg).expect("reopen");
    let open_s = t.elapsed().as_secs_f64();
    let stats = store.stats().expect("stats");
    println!(
        "records={records} payload_bytes={payload} segments={} segment_bytes={} \
         write_s={write_s:.2} open_s={open_s:.3} live_shards={} \
         rebuild_rate_records_per_s={:.0}",
        stats.segments,
        stats.segment_bytes,
        report.live_shards,
        f64::from(u32::try_from(records).unwrap_or(u32::MAX)) / open_s
    );
}
