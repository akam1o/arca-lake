//! Measures what compaction costs the writers: sustained group-commit
//! overwrites from several threads while a compactor runs passes, as the
//! storage node's compactor does. Reports put latency, rollovers and the
//! segment count at the end (PR #64 review: one rollover per compaction
//! output versus one per pass).
//!
//! Usage: `cargo run --release -p arca-lake-storage --example
//! compaction_pause -- <dir> [seconds] [writers] [payload_bytes] [mode]`
//! with `mode` `pass` (one `Store::compaction_pass` per pass, the default)
//! or `each` (`Store::compact` per segment).
//!
//! Numbers depend on the machine and are relative; see
//! `benchmarks/README.md` before quoting them anywhere.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arca_lake_storage::{ShardKey, Store, StoreConfig, SyncPolicy};

const KEYS: u64 = 2000;

fn key(i: u64) -> ShardKey {
    let mut chunk_id = [0u8; 16];
    chunk_id[..8].copy_from_slice(&i.to_le_bytes());
    ShardKey {
        group_id: 1,
        chunk_id,
        shard_index: 0,
    }
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i]
}

#[allow(clippy::too_many_lines)]
fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("dir");
    let secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(20);
    let writers: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(4);
    let len: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(16 << 10);
    let mode = args.next().unwrap_or_else(|| "pass".into());
    std::fs::create_dir_all(&dir).expect("create dir");
    let cfg = StoreConfig {
        dir: dir.clone().into(),
        segment_max_bytes: 8 << 20,
        record_alignment: 512,
        block_size: 4096,
        sync: SyncPolicy::Group,
        format_write: arca_lake_storage::default_format_write(),
    };
    let (store, _) = Store::open(cfg).expect("open");
    let store = Arc::new(store);
    let stop = Arc::new(AtomicBool::new(false));
    let outputs = Arc::new(AtomicU64::new(0));
    let passes = Arc::new(AtomicU64::new(0));

    let compactor = {
        let (store, stop, outputs, passes) = (
            Arc::clone(&store),
            Arc::clone(&stop),
            Arc::clone(&outputs),
            Arc::clone(&passes),
        );
        let mode = mode.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(500));
                let candidates = store.compaction_candidates(0.5).expect("candidates");
                if candidates.is_empty() {
                    continue;
                }
                passes.fetch_add(1, Ordering::Relaxed);
                let mut out = 0;
                if mode == "each" {
                    for seg in candidates {
                        let r = store.compact(seg).expect("compact");
                        out += u64::from(r.output_segment_id.is_some());
                    }
                } else {
                    let mut pass = store.compaction_pass(candidates.len());
                    for seg in candidates {
                        let r = pass.compact(seg).expect("compact");
                        out += u64::from(r.output_segment_id.is_some());
                    }
                }
                outputs.fetch_add(out, Ordering::Relaxed);
            }
        })
    };

    let started = Instant::now();
    let handles: Vec<_> = (0..writers)
        .map(|t| {
            let (store, stop) = (Arc::clone(&store), Arc::clone(&stop));
            std::thread::spawn(move || {
                let body = vec![u8::try_from(t).unwrap_or(0); len];
                let mut lat = Vec::new();
                let mut x = t.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                while !stop.load(Ordering::Relaxed) {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let t0 = Instant::now();
                    store.put(key(x % KEYS), &body).expect("put");
                    lat.push(t0.elapsed());
                }
                lat
            })
        })
        .collect();
    std::thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);
    let mut lat: Vec<Duration> = handles
        .into_iter()
        .flat_map(|h| h.join().expect("writer"))
        .collect();
    compactor.join().expect("compactor");
    let elapsed = started.elapsed();
    lat.sort_unstable();
    let t = store.timings();
    let segments = store.segment_stats().expect("stats");
    let small = segments
        .iter()
        .filter(|s| !s.active && s.file_bytes < (8 << 20) / 2)
        .count();
    println!(
        "mode {mode} writers {writers} payload {len} seconds {:.1}",
        elapsed.as_secs_f64()
    );
    #[allow(clippy::cast_precision_loss)]
    let rate = lat.len() as f64 / elapsed.as_secs_f64();
    println!(
        "puts {} ({rate:.0}/s) p50 {:?} p99 {:?} p99.9 {:?} max {:?}",
        lat.len(),
        pct(&lat, 0.5),
        pct(&lat, 0.99),
        pct(&lat, 0.999),
        lat.last().copied().unwrap_or_default()
    );
    println!(
        "passes {} outputs {} rollovers {} (sum {:?}, max {:?}) segments {} (sealed under half full {small})",
        passes.load(Ordering::Relaxed),
        outputs.load(Ordering::Relaxed),
        t.rollover.count,
        Duration::from_nanos(t.rollover.sum_ns),
        Duration::from_nanos(t.rollover.max_ns),
        segments.len(),
    );
}
