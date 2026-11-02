//! Metadata hot-key micro-benchmark (Phase 2.3 §6).
//!
//! Several tasks hammer the cache metadata of one tier the way gateways
//! do: each task owns committed generations and repeatedly records cache
//! placements (`insert`), drops them (`remove`), and refreshes recency
//! (`touch`); an optional evictor task runs the controller's eviction
//! recipe (`lru_candidates` + `remove`) concurrently. Every operation
//! goes through the same transaction loop as production, so the
//! process-wide transaction counters (attempts, conflicts, unknown-outcome
//! retries, exhausted) measure the contention on the tier's usage key,
//! the recency index and the generation headers. Several processes may
//! run at once against one FoundationDB cluster (multi-gateway contention)
//! with the same or different tier names (multiple tiers).
//!
//! Output: one JSON line with throughput, latency percentiles and the
//! transaction counter deltas.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arca_lake_metadata::kv::{Limits, transact_stats};
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::cache as meta_cache;
use arca_lake_metadata::ops::object::{self, CommitInfo, Precondition};
use arca_lake_metadata::records::{CachePlacement, ChunkRecord, DurablePlacement, GenId};
use arca_lake_metadata::{KvStore, ops::bucket};
use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Backend {
    Mem,
    Fdb,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// insert, then remove, alternating per chunk.
    InsertRemove,
    /// insert once per chunk, then touch repeatedly.
    Touch,
    /// insert/remove on half the tasks, touch on the other half.
    Mixed,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, value_enum, default_value_t = Backend::Mem)]
    backend: Backend,
    /// FoundationDB cluster file (`fdb` backend).
    #[arg(long, env = "ARCA_FDB_CLUSTER_FILE")]
    cluster_file: Option<String>,
    /// Concurrent tasks (one gateway's populations ≈ one task each).
    #[arg(long, default_value_t = 8)]
    tasks: usize,
    /// Run time per measurement.
    #[arg(long, default_value_t = 10)]
    seconds: u64,
    #[arg(long, value_enum, default_value_t = Mode::InsertRemove)]
    mode: Mode,
    /// Cache tier name (several processes with distinct tiers = multiple
    /// tiers; the same tier = several gateways on one tier).
    #[arg(long, default_value = "fast")]
    tier: String,
    /// Chunks per task (distinct placements a task cycles over).
    #[arg(long, default_value_t = 64)]
    chunks: u32,
    /// Run an evictor task alongside (lru_candidates + remove, 50/s).
    #[arg(long)]
    evictor: bool,
    /// Capacity passed to insert (0 = unlimited; a small value exercises
    /// the over-capacity path).
    #[arg(long, default_value_t = 0)]
    capacity_bytes: u64,
    /// Label copied into the output.
    #[arg(long, default_value = "")]
    label: String,
    #[arg(long, default_value_t = 4)]
    threads: usize,
}

#[derive(Debug, serde::Serialize)]
struct Report {
    label: String,
    backend: String,
    mode: String,
    tasks: usize,
    tier: String,
    seconds: f64,
    ops: u64,
    ops_per_s: f64,
    errors: u64,
    lat_ms_p50: f64,
    lat_ms_p99: f64,
    lat_ms_max: f64,
    txn_attempts: u64,
    txn_conflicts: u64,
    txn_unknown_retries: u64,
    txn_exhausted: u64,
    conflicts_per_op: f64,
}

fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(u64::MAX)
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

async fn committed_generation<S: KvStore>(
    store: &S,
    bucket: &str,
    key: &str,
    chunks: u32,
) -> GenId {
    let now = now_ms();
    let pending = object::begin_put(store, bucket, key, now, 600_000)
        .await
        .expect("begin_put");
    let records: Vec<(u32, ChunkRecord)> = (0..chunks)
        .map(|i| {
            (
                i,
                ChunkRecord {
                    chunk_id: arca_lake_common_chunk_id(&pending.generation, i),
                    offset: u64::from(i) << 20,
                    len: 1 << 20,
                    scheme: "rs-gf8-v1".into(),
                    data_shards: 2,
                    parity_shards: 1,
                    crc32c: 0,
                    placement: DurablePlacement {
                        group_id: 1,
                        epoch: 1,
                    },
                },
            )
        })
        .collect();
    object::append_chunks(store, &pending.generation, &records, now)
        .await
        .expect("append");
    object::commit_put(
        store,
        &pending.generation,
        &CommitInfo {
            size: u64::from(chunks) << 20,
            etag: "bench".into(),
            content_type: None,
            user_metadata: Vec::new(),
            chunk_count: chunks,
            checksum: None,
        },
        &Precondition::default(),
        now,
    )
    .await
    .expect("commit");
    pending.generation
}

/// Same derivation as `arca_lake_common::chunk_id` without the dependency.
fn arca_lake_common_chunk_id(generation: &GenId, index: u32) -> [u8; 16] {
    let mut id = *generation;
    id[12..].copy_from_slice(&index.to_be_bytes());
    id
}

fn placement(tier: &str, now: u64) -> CachePlacement {
    CachePlacement {
        tier: tier.to_string(),
        group_id: 2,
        epoch: 1,
        bytes: 1 << 20,
        pinned: false,
        last_access_ms: now,
        created_ms: now,
    }
}

#[allow(clippy::too_many_lines)]
async fn run<S: KvStore + 'static>(store: Arc<S>, args: &Args) -> Report {
    bucket::create_bucket(
        store.as_ref(),
        "metabench",
        arca_lake_metadata::records::Versioning::Disabled,
        now_ms(),
    )
    .await
    .ok();
    let mut gens = Vec::new();
    for t in 0..args.tasks {
        gens.push(
            committed_generation(
                store.as_ref(),
                "metabench",
                &format!("{}/{}/{t}", args.label, args.tier),
                args.chunks,
            )
            .await,
        );
    }
    let before = transact_stats();
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let lats: Arc<tokio::sync::Mutex<Vec<f64>>> = Arc::default();
    let started = Instant::now();
    let mut tasks = Vec::new();
    for (t, generation) in gens.iter().copied().enumerate() {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let ops = Arc::clone(&ops);
        let errors = Arc::clone(&errors);
        let lats = Arc::clone(&lats);
        let tier = args.tier.clone();
        let chunks = args.chunks;
        let capacity = args.capacity_bytes;
        let touch_only = match args.mode {
            Mode::InsertRemove => false,
            Mode::Touch => true,
            Mode::Mixed => t % 2 == 1,
        };
        tasks.push(tokio::spawn(async move {
            let mut i = 0u32;
            if touch_only {
                for c in 0..chunks {
                    let _ = meta_cache::insert(
                        store.as_ref(),
                        &generation,
                        c,
                        &placement(&tier, now_ms()),
                        capacity,
                    )
                    .await;
                }
            }
            let mut present = false;
            while !stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                let r = if touch_only {
                    meta_cache::touch(store.as_ref(), &generation, i % chunks, now_ms())
                        .await
                        .map(|_touched| ())
                } else if present {
                    meta_cache::remove(store.as_ref(), &generation, i % chunks)
                        .await
                        .map(|_| ())
                } else {
                    meta_cache::insert(
                        store.as_ref(),
                        &generation,
                        i % chunks,
                        &placement(&tier, now_ms()),
                        capacity,
                    )
                    .await
                    .map(|_| ())
                };
                let ms = t0.elapsed().as_secs_f64() * 1000.0;
                match r {
                    Ok(()) => {
                        ops.fetch_add(1, Ordering::Relaxed);
                        lats.lock().await.push(ms);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if !touch_only && i % chunks == chunks - 1 {
                    present = !present;
                }
                i = i.wrapping_add(1);
            }
        }));
    }
    if args.evictor {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let tier = args.tier.clone();
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(cands) = meta_cache::lru_candidates(store.as_ref(), &tier, 0, 8).await {
                    for (g, i, _) in cands {
                        let _ = meta_cache::remove(store.as_ref(), &g, i).await;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(args.seconds)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }
    let secs = started.elapsed().as_secs_f64();
    let after = transact_stats();
    let mut l = lats.lock().await.clone();
    l.sort_by(f64::total_cmp);
    let ops_n = ops.load(Ordering::Relaxed);
    #[allow(clippy::cast_precision_loss)]
    Report {
        label: args.label.clone(),
        backend: format!("{:?}", args.backend).to_lowercase(),
        mode: format!("{:?}", args.mode).to_lowercase(),
        tasks: args.tasks,
        tier: args.tier.clone(),
        seconds: secs,
        ops: ops_n,
        ops_per_s: ops_n as f64 / secs,
        errors: errors.load(Ordering::Relaxed),
        lat_ms_p50: percentile(&l, 0.5),
        lat_ms_p99: percentile(&l, 0.99),
        lat_ms_max: l.last().copied().unwrap_or(0.0),
        txn_attempts: after.attempts - before.attempts,
        txn_conflicts: after.conflicts - before.conflicts,
        txn_unknown_retries: after.unknown_retries - before.unknown_retries,
        txn_exhausted: after.exhausted - before.exhausted,
        conflicts_per_op: if ops_n == 0 {
            0.0
        } else {
            (after.conflicts - before.conflicts) as f64 / ops_n as f64
        },
    }
}

fn main() {
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads.max(1))
        .enable_all()
        .build()
        .expect("runtime");
    let report = rt.block_on(async {
        match args.backend {
            Backend::Mem => run(Arc::new(MemKv::new(Limits::FOUNDATIONDB)), &args).await,
            Backend::Fdb => {
                #[cfg(feature = "fdb")]
                {
                    let store = arca_lake_metadata::fdb::FdbKv::open(args.cluster_file.as_deref())
                        .expect("open FoundationDB");
                    run(Arc::new(store), &args).await
                }
                #[cfg(not(feature = "fdb"))]
                {
                    eprintln!("built without the fdb feature");
                    std::process::exit(2);
                }
            }
        }
    });
    println!("{}", serde_json::to_string(&report).expect("report"));
}
