//! Process-crash consistency: a writer process is killed with SIGKILL at
//! random points while writing, deleting, overwriting, and compacting. After
//! each kill the store is reopened and checked against the operations the
//! process acknowledged (printed) before dying.
//!
//! The writer's operation sequence is deterministic, so the parent knows
//! which operations follow the last acknowledged one. Because every
//! operation is made durable before the next starts, the recovered state
//! must equal the acknowledged state plus *some prefix* of the following,
//! unacknowledged operations (an operation may have completed just before
//! its acknowledgment was printed).
//!
//! Invariants:
//! 1. Every acknowledged operation is visible after recovery.
//! 2. Unacknowledged operations are applied as a prefix, fully or not at
//!    all; payloads are never partial or corrupt.
//! 3. Recovery is idempotent: a second open changes nothing.
//!
//! Scope: SIGKILL tests crash atomicity of the process (torn appends, lost
//! in-memory state, interrupted compaction). It does not model lost page
//! cache on power failure; that case is covered by the truncation tests in
//! `crash.rs`, which model it as an arbitrary cut of the file tail.

#![allow(clippy::assert_is_empty)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arca_lake_storage::{ShardKey, StorageError, Store, StoreConfig, SyncPolicy};

fn key(i: u64) -> ShardKey {
    let mut chunk_id = [0u8; 16];
    chunk_id[..8].copy_from_slice(&i.to_le_bytes());
    ShardKey {
        group_id: i % 3,
        chunk_id,
        shard_index: u16::try_from(i % 5).unwrap_or(0),
    }
}

fn payload(i: u64, seed: u64) -> Vec<u8> {
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

fn config(dir: &std::path::Path) -> StoreConfig {
    StoreConfig {
        dir: dir.to_path_buf(),
        segment_max_bytes: 128 << 10,
        record_alignment: 512,
        block_size: 256,
        sync: SyncPolicy::Group,
    }
}

/// State-changing operations, mirroring `crash_writer`'s loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Put(u64),
    Del(u64),
    Over(u64, u64),
}

impl Op {
    fn parse(line: &str) -> Option<Self> {
        let mut p = line.split_whitespace();
        match (p.next()?, p.next(), p.next(), p.next()) {
            ("put", Some(i), None, None) => Some(Self::Put(i.parse().ok()?)),
            ("del", Some(i), None, None) => Some(Self::Del(i.parse().ok()?)),
            ("over", Some(k), Some(s), None) => Some(Self::Over(k.parse().ok()?, s.parse().ok()?)),
            _ => None,
        }
    }
}

/// The deterministic operation sequence of the writer, for the first `n` ops.
fn sequence(n: usize) -> Vec<Op> {
    let mut ops = Vec::with_capacity(n);
    let mut i = 0u64;
    while ops.len() < n {
        ops.push(Op::Put(i));
        if i % 7 == 3 && i >= 3 {
            ops.push(Op::Del(i - 3));
        }
        if i % 50 == 25 && i >= 10 {
            ops.push(Op::Over(i - 10, i));
        }
        i += 1;
    }
    ops.truncate(n);
    ops
}

/// Expected state: key index -> Some(payload source index) or None (deleted).
type Model = BTreeMap<u64, Option<u64>>;

fn apply(model: &mut Model, op: Op) {
    match op {
        Op::Put(i) => {
            model.insert(i, Some(i));
        }
        Op::Del(i) => {
            model.insert(i, None);
        }
        Op::Over(k, src) => {
            model.insert(k, Some(src));
        }
    }
}

/// Whether the store matches `model` exactly for every key in the model.
/// Returns a description of the first mismatch.
fn mismatch(store: &Store, model: &Model, seed: u64) -> Option<String> {
    for (&k, expected) in model {
        match (expected, store.get(&key(k), None)) {
            (Some(src), Ok(got)) => {
                if got != payload(*src, seed) {
                    return Some(format!("key {k}: wrong payload"));
                }
            }
            (Some(_), Err(StorageError::NotFound)) => return Some(format!("key {k}: missing")),
            (None, Err(StorageError::NotFound)) => {}
            (None, Ok(_)) => return Some(format!("key {k}: present but expected deleted")),
            (_, Err(e)) => return Some(format!("key {k}: {e}")),
        }
    }
    None
}

/// How many unacknowledged operations may have completed before the kill.
const LOOKAHEAD: usize = 4;

fn run_one(iteration: u64, policy: &str) -> (usize, usize) {
    let dir = tempfile::tempdir().unwrap();
    let seed = 1000 + iteration;
    let mut child = Command::new(env!("CARGO_BIN_EXE_crash_writer"))
        .arg(dir.path())
        .arg(seed.to_string())
        .arg(policy)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn crash_writer");

    // Collect acknowledgments as they arrive, wait for the writer to have
    // acknowledged at least one operation (its start-up, including the
    // first group commit, can take longer than the kill delay on a slow
    // disk), then let it run for a pseudo-random time and kill it hard.
    let lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let reader = {
        let lines = Arc::clone(&lines);
        let stdout = child.stdout.take().unwrap();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                lines.lock().unwrap().push(line);
            }
        })
    };
    let started = std::time::Instant::now();
    while lines.lock().unwrap().is_empty() && started.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(5));
    }
    let ms = 30 + (iteration * 7919 + 13) % 400;
    std::thread::sleep(Duration::from_millis(ms));
    child.kill().expect("kill");
    let _ = child.wait();
    reader.join().unwrap();
    let out = lines.lock().unwrap().join("\n");

    // Acknowledged operations, in order. The last line may be cut off by the
    // kill; `Op::parse` rejects such fragments and the loop stops there.
    let mut acked: Vec<Op> = Vec::new();
    for line in out.lines() {
        if line.starts_with("compact ") {
            continue;
        }
        match Op::parse(line) {
            Some(op) => acked.push(op),
            None => break,
        }
    }
    let expected_seq = sequence(acked.len());
    assert_eq!(
        acked, expected_seq,
        "iteration {iteration}: writer deviated from the deterministic sequence"
    );

    let (store, report) = Store::open(config(dir.path())).unwrap();
    assert!(
        report.corrupt_segments.is_empty(),
        "iteration {iteration}: {report:?}"
    );

    // Invariants 1 and 2: the state equals the acknowledged model extended by
    // some prefix of the following operations.
    let future = sequence(acked.len() + LOOKAHEAD);
    let mut model = Model::new();
    for op in &acked {
        apply(&mut model, *op);
    }
    let mut applied_unacked = None;
    let mut last_mismatch = None;
    for extra in 0..=LOOKAHEAD {
        let mut m = model.clone();
        for op in &future[acked.len()..acked.len() + extra] {
            apply(&mut m, *op);
        }
        match mismatch(&store, &m, seed) {
            None => {
                applied_unacked = Some(extra);
                break;
            }
            Some(why) => last_mismatch = Some(why),
        }
    }
    let applied_unacked = applied_unacked.unwrap_or_else(|| {
        panic!(
            "iteration {iteration} ({policy}): recovered state matches no prefix of unacknowledged ops; \
             acked={} last mismatch: {last_mismatch:?}",
            acked.len()
        )
    });
    let scrub = store.scrub().unwrap();
    assert!(
        scrub.corrupt.is_empty(),
        "iteration {iteration}: scrub found corruption"
    );

    // Invariant 3: reopening is a no-op.
    let keys_before = store.keys();
    drop(store);
    let (store, report2) = Store::open(config(dir.path())).unwrap();
    assert_eq!(
        report2.truncated_bytes, 0,
        "iteration {iteration}: second open truncated"
    );
    assert!(report2.corrupt_segments.is_empty());
    assert_eq!(
        store.keys(),
        keys_before,
        "iteration {iteration}: second open changed the index"
    );
    (acked.len(), applied_unacked)
}

#[test]
fn sigkill_during_writes_and_compaction_never_loses_acknowledged_data() {
    let iterations: u64 = std::env::var("ARCA_SIGKILL_ITERATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);
    let mut total_acked = 0;
    let mut total_unacked_applied = 0;
    for it in 0..iterations {
        let policy = if it % 2 == 0 { "group" } else { "always" };
        let (acked, unacked) = run_one(it, policy);
        total_acked += acked;
        total_unacked_applied += unacked;
    }
    eprintln!(
        "sigkill: {iterations} iterations, {total_acked} acknowledged ops verified, \
         {total_unacked_applied} unacknowledged-but-applied ops observed"
    );
    assert!(
        total_acked > 0,
        "the writer never acknowledged anything; check the harness"
    );
}
