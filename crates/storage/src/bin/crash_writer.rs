//! Workload process for the SIGKILL crash-consistency test.
//!
//! Writes a deterministic sequence of operations to a store and prints each
//! operation **after** it was acknowledged (`put`/`del` lines) so that the
//! parent can rebuild the exact set of acknowledged state after killing this
//! process at an arbitrary point. Also runs compaction periodically so that
//! the kill may land inside a compaction.
//!
//! Usage: `crash_writer <dir> <seed> <sync-policy: always|group|never>`
//!
//! Output lines: `put <i>`, `del <i>`, `compact <segment>`.

use std::io::Write;

use arca_lake_storage::{ShardKey, Store, StoreConfig, SyncPolicy};

fn key(i: u64) -> ShardKey {
    let mut chunk_id = [0u8; 16];
    chunk_id[..8].copy_from_slice(&i.to_le_bytes());
    ShardKey {
        group_id: i % 3,
        chunk_id,
        shard_index: u16::try_from(i % 5).unwrap_or(0),
    }
}

/// Deterministic payload for operation `i` under `seed`. Shared with the test.
pub fn payload(i: u64, seed: u64) -> Vec<u8> {
    let mut x = (i ^ seed).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let len = 64 + usize::try_from(x % 6000).unwrap_or(0);
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            u8::try_from(x >> 56).unwrap_or(0)
        })
        .collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: crash_writer <dir> <seed> <always|group|never>");
        std::process::exit(2);
    }
    let seed: u64 = args[2].parse().expect("seed");
    let sync = match args[3].as_str() {
        "always" => SyncPolicy::Always,
        "group" => SyncPolicy::Group,
        "never" => SyncPolicy::Never,
        other => panic!("unknown sync policy {other}"),
    };
    let cfg = StoreConfig {
        dir: args[1].clone().into(),
        segment_max_bytes: 128 << 10,
        record_alignment: 512,
        block_size: 256,
        sync,
        format_write: arca_lake_storage::default_format_write(),
    };
    let (store, _) = Store::open(cfg).expect("open");
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let mut i: u64 = 0;
    loop {
        store.put(key(i), &payload(i, seed)).expect("put");
        writeln!(out, "put {i}").unwrap();
        if i % 7 == 3 && i >= 3 {
            store.delete(key(i - 3)).expect("delete");
            writeln!(out, "del {}", i - 3).unwrap();
        }
        if i % 50 == 25 && i >= 10 {
            // Overwrite an older key with the payload of the current index.
            store
                .put(key(i - 10), &payload(i, seed))
                .expect("overwrite");
            writeln!(out, "over {} {i}", i - 10).unwrap();
        }
        if i % 97 == 96 {
            for seg in store.compaction_candidates(0.3).expect("candidates") {
                store.compact(seg).expect("compact");
                writeln!(out, "compact {seg}").unwrap();
            }
        }
        out.flush().unwrap();
        i += 1;
    }
}
