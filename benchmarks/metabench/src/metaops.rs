//! `metaops`: object metadata operations straight on the metadata store
//! (Phase 4.0), through the production functions the gateway calls
//! (`begin_put` / `append_chunks` / `commit_put`, `read_manifest`,
//! `head_object`, `delete_object`, `list_objects`, the map version read and
//! the access record of a GET), so that the records written are those of a
//! real PUT of the same shape (no shard is stored: the chunk records name
//! group 1). Each run measures one operation at a fixed concurrency, for a
//! time or over a key range, and counts per operation the transactions,
//! the reads, range reads, rows and bytes read, the writes and bytes
//! written (a counting wrapper of the store) and the conflicts (the
//! production transaction loop's counters).
//!
//! Output: one JSON line.

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arca_lake_metadata::kv::{Key, KeyRange, Limits, Value, transact_stats};
use arca_lake_metadata::ops::list::{ListRequest, list_objects};
use arca_lake_metadata::ops::object::{self, CommitInfo, Precondition};
use arca_lake_metadata::ops::{bucket, cluster};
use arca_lake_metadata::records::{ChunkRecord, DurablePlacement};
use arca_lake_metadata::{KvError, KvStore, Txn};
use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Op {
    /// Create the buckets.
    Buckets,
    /// Create `--objects` keys from `--first` (one pass): the population.
    Populate,
    /// PUT metadata of fresh keys (after the populated range).
    Put,
    /// PUT metadata over and over on `--hot-keys` keys (concurrent
    /// overwrite).
    Overwrite,
    /// `read_manifest` (the GET path's lookup).
    Get,
    /// `head_object`.
    Head,
    /// `delete_object` of populated keys (one pass over `--objects`).
    Delete,
    /// One page of `list_objects` from a random start key.
    ListPage,
    /// The gateway's per-request map version read.
    MapVersion,
    /// The gateway's per-GET access record write.
    Access,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Layout {
    Sequential,
    Hashed,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, env = "ARCA_FDB_CLUSTER_FILE")]
    cluster_file: Option<String>,
    /// In-memory store instead of FoundationDB (development only).
    #[arg(long)]
    mem: bool,
    #[arg(long, value_enum)]
    op: Op,
    #[arg(long, default_value_t = 1)]
    buckets: u64,
    #[arg(long, default_value = "meta")]
    bucket_prefix: String,
    #[arg(long, default_value_t = 100_000)]
    objects: u64,
    #[arg(long, default_value_t = 0)]
    first: u64,
    #[arg(long, value_enum, default_value_t = Layout::Sequential)]
    layout: Layout,
    /// Chunk records per object.
    #[arg(long, default_value_t = 1)]
    chunks: u32,
    #[arg(long, default_value_t = 16)]
    concurrency: usize,
    /// 0: one pass (populate, delete); otherwise the measured time.
    #[arg(long, default_value_t = 20)]
    duration_secs: u64,
    #[arg(long, default_value_t = 3)]
    warmup_secs: u64,
    /// `overwrite`: keys shared by every task.
    #[arg(long, default_value_t = 1)]
    hot_keys: u64,
    #[arg(long, default_value_t = 1000)]
    page: usize,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value = "")]
    label: String,
    #[arg(long, default_value_t = 4)]
    threads: usize,
}

// ---------- counting store

#[derive(Default)]
struct Counts {
    txns: AtomicU64,
    gets: AtomicU64,
    ranges: AtomicU64,
    rows: AtomicU64,
    bytes_read: AtomicU64,
    sets: AtomicU64,
    clears: AtomicU64,
    bytes_written: AtomicU64,
}

struct Counting<S> {
    inner: S,
    c: Arc<Counts>,
}

struct CountingTx<T> {
    inner: T,
    c: Arc<Counts>,
}

impl<S: KvStore> KvStore for Counting<S> {
    type Tx = CountingTx<S::Tx>;

    async fn begin(&self) -> Result<Self::Tx, KvError> {
        self.c.txns.fetch_add(1, Ordering::Relaxed);
        Ok(CountingTx {
            inner: self.inner.begin().await?,
            c: Arc::clone(&self.c),
        })
    }

    fn limits(&self) -> Limits {
        self.inner.limits()
    }
}

impl<T: Txn> Txn for CountingTx<T> {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.c.gets.fetch_add(1, Ordering::Relaxed);
        let v = self.inner.get(key).await?;
        self.c.bytes_read.fetch_add(
            (key.len() + v.as_ref().map_or(0, Vec::len)) as u64,
            Ordering::Relaxed,
        );
        Ok(v)
    }

    async fn get_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Key, Value)>, KvError> {
        self.c.ranges.fetch_add(1, Ordering::Relaxed);
        let rows = self.inner.get_range(range, limit, reverse).await?;
        self.c.rows.fetch_add(rows.len() as u64, Ordering::Relaxed);
        self.c.bytes_read.fetch_add(
            rows.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum(),
            Ordering::Relaxed,
        );
        Ok(rows)
    }

    async fn get_snapshot(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.c.gets.fetch_add(1, Ordering::Relaxed);
        let v = self.inner.get_snapshot(key).await?;
        self.c.bytes_read.fetch_add(
            (key.len() + v.as_ref().map_or(0, Vec::len)) as u64,
            Ordering::Relaxed,
        );
        Ok(v)
    }

    async fn atomic_add(&mut self, key: &[u8], delta: i64) -> Result<(), KvError> {
        self.c.sets.fetch_add(1, Ordering::Relaxed);
        self.c
            .bytes_written
            .fetch_add(key.len() as u64 + 8, Ordering::Relaxed);
        self.inner.atomic_add(key, delta).await
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.c.sets.fetch_add(1, Ordering::Relaxed);
        self.c
            .bytes_written
            .fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
        self.inner.set(key, value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.c.clears.fetch_add(1, Ordering::Relaxed);
        self.inner.clear(key);
    }

    fn clear_range(&mut self, range: &KeyRange) {
        self.c.clears.fetch_add(1, Ordering::Relaxed);
        self.inner.clear_range(range);
    }

    async fn commit(self) -> Result<u64, KvError> {
        self.inner.commit().await
    }
}

// ---------- keys

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn name(args: &Args, i: u64) -> (String, String) {
    let b = format!("{}-{}", args.bucket_prefix, i % args.buckets.max(1));
    let k = match args.layout {
        Layout::Sequential => format!("obj/{i:010}"),
        Layout::Hashed => format!("obj/{:04x}/{i:010}", mix(i) & 0xffff),
    };
    (b, k)
}

fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(u64::MAX)
}

/// One PUT's metadata, as the gateway records it.
async fn put_meta<S: KvStore>(s: &S, bucket: &str, key: &str, chunks: u32) -> Result<(), String> {
    let now = now_ms();
    let p = object::begin_put(s, bucket, key, now, 600_000)
        .await
        .map_err(|e| e.to_string())?;
    let records: Vec<(u32, ChunkRecord)> = (0..chunks)
        .map(|i| {
            let mut id = p.generation;
            id[12..].copy_from_slice(&i.to_be_bytes());
            (
                i,
                ChunkRecord {
                    chunk_id: id,
                    offset: u64::from(i) << 22,
                    len: 4096,
                    scheme: "rs-gf8-v1".into(),
                    data_shards: 2,
                    parity_shards: 1,
                    crc32c: 0x1234_5678,
                    placement: DurablePlacement {
                        group_id: 1,
                        epoch: 1,
                    },
                },
            )
        })
        .collect();
    object::append_chunks_renewing(s, &p.generation, &records, now, 600_000)
        .await
        .map_err(|e| e.to_string())?;
    object::commit_put(
        s,
        &p.generation,
        &CommitInfo {
            size: 4096 * u64::from(chunks),
            etag: "\"0123456789abcdef0123456789abcdef\"".into(),
            content_type: Some("application/octet-stream".into()),
            user_metadata: Vec::new(),
            chunk_count: chunks,
            checksum: None,
        },
        &Precondition::default(),
        now,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

async fn one<S: KvStore>(s: &S, args: &Args, i: u64, rnd: u64) -> Result<(), String> {
    let (b, k) = name(args, i);
    match args.op {
        Op::Populate | Op::Put | Op::Overwrite => put_meta(s, &b, &k, args.chunks).await,
        Op::Get => object::read_manifest(s, &b, &k, None)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Op::Head => object::head_object(s, &b, &k)
            .await
            .map_err(|e| e.to_string())
            .and_then(|o| o.map(|_| ()).ok_or_else(|| "missing".to_string())),
        Op::Delete => object::delete_object(s, &b, &k, now_ms())
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Op::ListPage => {
            let (_, start) = name(args, args.first + rnd % args.objects.max(1));
            list_objects(
                s,
                &b,
                &ListRequest {
                    prefix: "obj/".into(),
                    delimiter: None,
                    start_after: Some(start),
                    continuation: None,
                    max_keys: args.page,
                },
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
        }
        Op::MapVersion => cluster::map_version(s)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Op::Access => {
            let mut g = [0u8; 16];
            g[..8].copy_from_slice(&mix(i).to_be_bytes());
            object::record_access(s, &g, now_ms(), 1)
                .await
                .map_err(|e| e.to_string())
        }
        Op::Buckets => Ok(()),
    }
}

fn pct(v: &[u64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v[((v.len() as f64 - 1.0) * p).round() as usize] as f64 / 1000.0
}

async fn run<S: KvStore + Send + Sync + 'static>(
    store: Counting<S>,
    args: Arc<Args>,
) -> serde_json::Value
where
    S::Tx: Send,
{
    let store = Arc::new(store);
    if args.op == Op::Buckets {
        for b in 0..args.buckets.max(1) {
            let _ = bucket::create_bucket(
                store.as_ref(),
                &format!("{}-{b}", args.bucket_prefix),
                arca_lake_metadata::records::Versioning::Disabled,
                now_ms(),
            )
            .await;
        }
        return serde_json::json!({"op": "buckets", "buckets": args.buckets});
    }
    let one_pass = args.duration_secs == 0 || matches!(args.op, Op::Populate | Op::Delete);
    let measuring = Arc::new(AtomicBool::new(one_pass || args.warmup_secs == 0));
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicU64::new(0));
    let lat = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let errors = Arc::new(AtomicU64::new(0));
    let first_error = Arc::new(std::sync::Mutex::new(None::<String>));
    // Workers start together after the first counter snapshot.
    let go = Arc::new(tokio::sync::Notify::new());
    let started = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for w in 0..args.concurrency.max(1) {
        let (go, started) = (Arc::clone(&go), Arc::clone(&started));
        let (store, args, measuring, stop, next, lat, errors, first_error) = (
            Arc::clone(&store),
            Arc::clone(&args),
            Arc::clone(&measuring),
            Arc::clone(&stop),
            Arc::clone(&next),
            Arc::clone(&lat),
            Arc::clone(&errors),
            Arc::clone(&first_error),
        );
        tasks.push(tokio::spawn(async move {
            let wait = go.notified();
            if !started.load(Ordering::SeqCst) {
                wait.await;
            }
            let mut x = mix(args.seed ^ (w as u64 + 1) << 40);
            while !stop.load(Ordering::Relaxed) {
                let n = next.fetch_add(1, Ordering::Relaxed);
                x = mix(x.wrapping_add(n));
                let i = match args.op {
                    Op::Populate | Op::Delete => {
                        if n >= args.objects {
                            return;
                        }
                        args.first + n
                    }
                    Op::Put => args.first + args.objects + n,
                    Op::Overwrite => args.first + x % args.hot_keys.max(1),
                    _ => args.first + x % args.objects.max(1),
                };
                let t = Instant::now();
                let r = one(store.as_ref(), &args, i, x).await;
                let us = u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX);
                if measuring.load(Ordering::Relaxed) {
                    match r {
                        Ok(()) => lat.lock().unwrap().push(us),
                        Err(e) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            first_error.lock().unwrap().get_or_insert(e);
                        }
                    }
                }
            }
        }));
    }
    let (c0, s0, t0, c1, s1, secs);
    if one_pass {
        c0 = snapshot(&store.c);
        s0 = transact_stats();
        t0 = Instant::now();
        started.store(true, Ordering::SeqCst);
        go.notify_waiters();
        for t in tasks {
            let _ = t.await;
        }
        secs = t0.elapsed().as_secs_f64();
        c1 = snapshot(&store.c);
        s1 = transact_stats();
    } else {
        started.store(true, Ordering::SeqCst);
        go.notify_waiters();
        tokio::time::sleep(Duration::from_secs(args.warmup_secs)).await;
        c0 = snapshot(&store.c);
        s0 = transact_stats();
        t0 = Instant::now();
        measuring.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(args.duration_secs)).await;
        measuring.store(false, Ordering::Relaxed);
        // Counters at the window's end, before the operations in flight
        // finish (those are neither in the latencies nor in the counts,
        // apart from the work they did inside the window).
        secs = t0.elapsed().as_secs_f64();
        c1 = snapshot(&store.c);
        s1 = transact_stats();
        stop.store(true, Ordering::Relaxed);
        for t in tasks {
            let _ = t.await;
        }
    }
    let mut l = lat.lock().unwrap().clone();
    l.sort_unstable();
    let ops = l.len() as u64;
    let per = |a: u64, b: u64| {
        if ops > 0 {
            (b - a) as f64 / ops as f64
        } else {
            0.0
        }
    };
    serde_json::json!({
        "label": args.label, "op": format!("{:?}", args.op).to_lowercase(),
        "layout": format!("{:?}", args.layout).to_lowercase(),
        "buckets": args.buckets, "objects": args.objects, "first": args.first, "chunks": args.chunks,
        "concurrency": args.concurrency, "hot_keys": args.hot_keys, "page": args.page,
        "seconds": secs, "ops": ops, "ops_per_s": ops as f64 / secs,
        "errors": errors.load(Ordering::Relaxed), "first_error": *first_error.lock().unwrap(),
        "lat_ms_p50": pct(&l, 0.5), "lat_ms_p95": pct(&l, 0.95), "lat_ms_p99": pct(&l, 0.99),
        "lat_ms_max": pct(&l, 1.0),
        "per_op": {
            "transaction_attempts": per(c0[0], c1[0]), "gets": per(c0[1], c1[1]), "range_reads": per(c0[2], c1[2]),
            "rows": per(c0[3], c1[3]), "bytes_read": per(c0[4], c1[4]), "sets": per(c0[5], c1[5]),
            "clears": per(c0[6], c1[6]), "bytes_written": per(c0[7], c1[7]),
            "attempts": per(s0.attempts, s1.attempts), "conflicts": per(s0.conflicts, s1.conflicts),
        },
        "conflicts": s1.conflicts - s0.conflicts, "exhausted": s1.exhausted - s0.exhausted,
        "unknown_retries": s1.unknown_retries - s0.unknown_retries,
    })
}

fn snapshot(c: &Counts) -> [u64; 8] {
    [
        c.txns.load(Ordering::Relaxed),
        c.gets.load(Ordering::Relaxed),
        c.ranges.load(Ordering::Relaxed),
        c.rows.load(Ordering::Relaxed),
        c.bytes_read.load(Ordering::Relaxed),
        c.sets.load(Ordering::Relaxed),
        c.clears.load(Ordering::Relaxed),
        c.bytes_written.load(Ordering::Relaxed),
    ]
}

fn main() {
    let args = Arc::new(Args::parse());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads.max(1))
        .enable_all()
        .build()
        .expect("runtime");
    let out = rt.block_on(async {
        let c = Arc::new(Counts::default());
        if args.mem {
            let inner = arca_lake_metadata::mem::MemKv::new(Limits::FOUNDATIONDB);
            run(Counting { inner, c }, Arc::clone(&args)).await
        } else {
            #[cfg(feature = "fdb")]
            {
                let inner = arca_lake_metadata::fdb::FdbKv::open_with_timeout(
                    args.cluster_file.as_deref(),
                    10_000,
                )
                .expect("open FoundationDB");
                run(Counting { inner, c }, Arc::clone(&args)).await
            }
            #[cfg(not(feature = "fdb"))]
            {
                let _ = c;
                panic!("built without the fdb feature: use --mem");
            }
        }
    });
    println!("{out}");
}
