//! `nodebench`: the storage node's shard RPCs without a gateway (Phase
//! 4.0): `PutShard` or `GetShard` of a fixed size at a fixed concurrency
//! against one node, for a time after a warm-up. Shards are written under
//! fresh chunk ids of the given group and epoch (a registered group: the
//! node checks the epoch). The puts cycle over a bounded set of shard
//! keys (each later put of a key replaces its record; the replaced
//! record is dead and compaction reclaims it), so that a sweep of sizes
//! does not fill the node: a storage node refuses deletes that do not
//! come under the leader's delete fence (ADR-0023), so shards a benchmark
//! wrote cannot be deleted by it. Reads hit keys written in the same run
//! (mostly the page cache).
//! The result is the node's own cost (network, RPC, the storage engine),
//! not an S3 figure.
//!
//! Output: one JSON line (ops, MiB/s, latency percentiles, errors).

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_protocol::storage::v1::WriteClass;
use bytes::Bytes;
use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    /// Node address (`host:port`).
    #[arg(long)]
    addr: String,
    #[arg(long, default_value_t = 1)]
    group: u64,
    #[arg(long, default_value_t = 1)]
    epoch: u64,
    /// `put` or `get` (get reads the shards a put phase of the same run
    /// wrote: `put-get` does both).
    #[arg(long, default_value = "put-get")]
    op: String,
    #[arg(long, default_value_t = 1 << 20)]
    size: u64,
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
    #[arg(long, default_value_t = 20)]
    duration_secs: u64,
    #[arg(long, default_value_t = 5)]
    warmup_secs: u64,
    /// Frame size of PutShard streams.
    #[arg(long, default_value_t = 256 << 10)]
    frame_bytes: usize,
    #[arg(long, default_value = "")]
    label: String,
    /// Distinct shard keys the puts cycle over (0: about 256 MiB of them,
    /// at most 8,192 and at least four per connection: every live record
    /// also holds a tombstone's worth of the node's delete reserve).
    #[arg(long, default_value_t = 0)]
    keys: u64,
    #[arg(long, default_value_t = 4)]
    threads: usize,
}

fn pct(v: &mut [u64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_unstable();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i] as f64 / 1000.0
}

struct Phase {
    lat: std::sync::Mutex<Vec<u64>>,
    ops: AtomicU64,
    errors: AtomicU64,
    first_error: std::sync::Mutex<Option<String>>,
    measuring: AtomicBool,
}

async fn phase(
    clients: &NodeClients,
    args: &Arc<Args>,
    name: &str,
    written: &Arc<std::sync::Mutex<Vec<[u8; 16]>>>,
) -> serde_json::Value {
    let ph = Arc::new(Phase {
        lat: std::sync::Mutex::new(Vec::new()),
        ops: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        first_error: std::sync::Mutex::new(None),
        measuring: AtomicBool::new(false),
    });
    let stop = Arc::new(AtomicBool::new(false));
    let seq = Arc::new(AtomicU64::new(0));
    let data = Bytes::from(vec![0xa5u8; usize::try_from(args.size).unwrap_or(0)]);
    let mut tasks = Vec::new();
    for _ in 0..args.concurrency {
        let (clients, args, ph, stop, seq, data, written) = (
            clients.clone(),
            Arc::clone(args),
            Arc::clone(&ph),
            Arc::clone(&stop),
            Arc::clone(&seq),
            data.clone(),
            Arc::clone(written),
        );
        let put = name == "put";
        let keys = if args.keys > 0 {
            args.keys
        } else {
            ((256u64 << 20) / args.size.max(1)).clamp(4 * args.concurrency as u64, 8192)
        };
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let n = seq.fetch_add(1, Ordering::Relaxed);
                let chunk = if put {
                    let mut c = [0u8; 16];
                    c[..8].copy_from_slice(b"nodebnch");
                    c[8..].copy_from_slice(&(n % keys).to_be_bytes());
                    c
                } else {
                    let w = written.lock().unwrap();
                    if w.is_empty() {
                        return;
                    }
                    w[usize::try_from(n).unwrap_or(0) % w.len()]
                };
                let shard = ShardRef {
                    group_id: args.group,
                    chunk_id: chunk,
                    shard_index: 0,
                };
                let t = Instant::now();
                let r: Result<(), String> = if put {
                    clients
                        .put_shard_as(
                            &args.addr,
                            shard,
                            args.epoch,
                            data.clone(),
                            args.frame_bytes,
                            WriteClass::Client,
                        )
                        .await
                        .map(|_| ())
                        .map_err(|e| format!("{:?}: {}", e.code(), e.message()))
                } else {
                    match clients.get_shard(&args.addr, shard, 0, args.size).await {
                        Ok(b) if b.len() as u64 == args.size => Ok(()),
                        Ok(b) => Err(format!("short read: {} bytes", b.len())),
                        Err(e) => Err(format!("{:?}: {}", e.code(), e.message())),
                    }
                };
                let ok = r.is_ok();
                let us = u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX);
                if put && ok && n < keys {
                    written.lock().unwrap().push(chunk);
                }
                if ph.measuring.load(Ordering::Relaxed) {
                    if ok {
                        ph.ops.fetch_add(1, Ordering::Relaxed);
                        ph.lat.lock().unwrap().push(us);
                    } else {
                        ph.errors.fetch_add(1, Ordering::Relaxed);
                        if let Err(e) = r {
                            ph.first_error.lock().unwrap().get_or_insert(e);
                        }
                    }
                }
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(args.warmup_secs)).await;
    ph.measuring.store(true, Ordering::Relaxed);
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs(args.duration_secs)).await;
    ph.measuring.store(false, Ordering::Relaxed);
    let secs = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }
    let ops = ph.ops.load(Ordering::Relaxed);
    let mut lat = ph.lat.lock().unwrap().clone();
    serde_json::json!({
        "label": args.label, "op": name, "size": args.size, "concurrency": args.concurrency,
        "measured_s": secs, "ops": ops, "ops_per_s": ops as f64 / secs,
        "mib_per_s": (ops * args.size) as f64 / 1_048_576.0 / secs,
        "lat_ms_p50": pct(&mut lat, 0.5), "lat_ms_p95": pct(&mut lat, 0.95),
        "lat_ms_p99": pct(&mut lat, 0.99), "lat_ms_max": pct(&mut lat, 1.0),
        "errors": ph.errors.load(Ordering::Relaxed),
        "first_error": ph.first_error.lock().unwrap().clone(),
    })
}

fn main() {
    let args = Arc::new(Args::parse());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads.max(1))
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let clients = NodeClients::new(Duration::from_secs(60));
        // The node takes writes only with the incarnation it adopted
        // (ADR-0037): the benchmark targets this one node by address, so
        // it writes as the node's own cluster.
        let held = clients
            .health(&args.addr)
            .await
            .ok()
            .and_then(|h| <[u8; 16]>::try_from(h.incarnation.as_ref()).ok());
        clients.set_incarnation(held);
        let written = Arc::new(std::sync::Mutex::new(Vec::new()));
        for name in ["put", "get"] {
            if args.op == name || args.op == "put-get" {
                let r = phase(&clients, &args, name, &written).await;
                println!("{r}");
            }
        }
    });
}
