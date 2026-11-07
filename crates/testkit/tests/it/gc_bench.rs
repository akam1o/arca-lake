//! GC throughput by workload (Phase 3.6, R-64). Measurement, run
//! explicitly:
//!
//! `cargo test --release -p arca-lake-testkit --test it gc_bench:: --
//! --ignored --nocapture --test-threads=1`
//!
//! Each workload retires generations, then GC passes run (grace 0) until
//! the retired queue is empty; one JSON line reports the wall time, the
//! generations and physical bytes collected, the metadata transactions,
//! the per-stage mean and maximum of `GcStats`, and the heap grown.
//! `ARCA_GC_BENCH_SCALE` (default 1) multiplies the object counts;
//! `ARCA_GC_BENCH_BG_MIBS` (default 0) runs a background PUT load of that
//! many MiB/s while GC runs, so that shard deletes queue behind client
//! data in the nodes' group commits. On FoundationDB
//! (`gc_bench::fdb_*`, `--features fdb`, `ARCA_FDB_TESTS=1`) the
//! metadata-only workloads show the transaction cost of 100 to 10 000
//! parts.
//!
//! Workloads: objects of 256 KiB, 1 MiB and 16 MiB overwritten; versioned
//! overwrites of one key; multipart uploads of 2 parts of 1, 8 and 64 MiB
//! overwritten; metadata-only uploads of 100, 1000 and 10 000 parts
//! completed then deleted, and aborted (no shard data: the deletes find
//! nothing, which isolates metadata and RPC cost).

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::too_many_lines
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arca_lake_controller::gc::{GcStats, run_once_with};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::multipart::{
    self, CompletePart, CompleteRequest, PartInfo, UploadInfo,
};
use arca_lake_metadata::ops::object::{self, Precondition};
use arca_lake_metadata::ops::{bucket, gc};
use arca_lake_metadata::records::{ChunkRecord, DurablePlacement, Versioning, new_gen_id};
use arca_lake_testkit::s3::{self, PutOptions, request};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use futures::{StreamExt as _, stream};
use s3s::S3;
use s3s::dto::{
    CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadInput, ETag, StreamingBlob, UploadPartInput,
};

const MIB: u64 = 1 << 20;
const BUCKET: &str = "gcbench";

fn scale() -> usize {
    std::env::var("ARCA_GC_BENCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

fn bg_mibs() -> u64 {
    std::env::var("ARCA_GC_BENCH_BG_MIBS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// The soak layout's geometry: 2 + 1, chunks of 1 MiB.
fn opts() -> ClusterOptions {
    ClusterOptions {
        groups: 2,
        multipart_min_part_bytes: MIB,
        ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
    }
}

/// GC until the retired queue is empty; one JSON line.
async fn measure<S: KvStore + 'static>(c: &TestCluster<S>, workload: &str, retired: usize) {
    let stats = GcStats::default();
    let tx0 = arca_lake_metadata::kv::transact_stats();
    let heap0 = arca_lake_common::process::memory().heap_in_use;
    let started = Instant::now();
    let mut passes = 0;
    loop {
        let far = now_ms() + 3_600_000;
        let r = run_once_with(c.meta.as_ref(), &c.clients, far, 0, 1000, &stats)
            .await
            .unwrap();
        passes += 1;
        let (left, _) = gc::backlog(c.meta.as_ref(), 1).await.unwrap();
        if left == 0 && r.collected == 0 && r.deferred == 0 {
            break;
        }
        assert!(passes < 100_000, "{workload}: GC does not finish");
    }
    let secs = started.elapsed().as_secs_f64();
    let tx1 = arca_lake_metadata::kv::transact_stats();
    let heap1 = arca_lake_common::process::memory().heap_in_use;
    let collected = stats.collected.load(Ordering::Relaxed);
    let bytes = stats.reclaimed_bytes.load(Ordering::Relaxed);
    let mut stages = serde_json::Map::new();
    for (name, h) in stats.stages() {
        let s = h.snapshot();
        if s.count == 0 {
            continue;
        }
        stages.insert(
            name.into(),
            serde_json::json!({
                "count": s.count,
                "mean_ms": s.sum_ns as f64 / s.count as f64 / 1e6,
                "max_ms": s.max_ns as f64 / 1e6,
                "total_s": s.sum_ns as f64 / 1e9,
            }),
        );
    }
    println!(
        "{}",
        serde_json::json!({
            "workload": workload,
            "background_mib_s": bg_mibs(),
            "retired_ops": retired,
            "gc_seconds": secs,
            "passes": passes,
            "collected": collected,
            "roots": stats.collected_roots.load(Ordering::Relaxed),
            "parts_retired": stats.parts_retired.load(Ordering::Relaxed),
            "generations_per_s": collected as f64 / secs,
            "reclaimed_mib": bytes as f64 / MIB as f64,
            "reclaimed_mib_per_s": bytes as f64 / MIB as f64 / secs,
            "transactions": tx1.attempts - tx0.attempts,
            "conflicts": tx1.conflicts - tx0.conflicts,
            "transactions_per_generation": (tx1.attempts - tx0.attempts) as f64 / collected.max(1) as f64,
            "heap_grown_mib": (heap1 as f64 - heap0 as f64) / MIB as f64,
            "stages": stages,
        })
    );
}

/// A background writer of `bg_mibs()` MiB/s of 1 MiB objects (stopped by
/// the flag), so that GC competes with client data in the nodes.
fn background_load<S: KvStore + 'static>(
    c: &TestCluster<S>,
    stop: Arc<AtomicBool>,
) -> Option<tokio::task::JoinHandle<()>> {
    let rate = bg_mibs();
    if rate == 0 {
        return None;
    }
    let backend = c.backend.clone();
    Some(tokio::spawn(async move {
        let data = Bytes::from(random_bytes(usize::try_from(MIB).unwrap(), 99));
        let started = Instant::now();
        let mut n: u64 = 0;
        while !stop.load(Ordering::Relaxed) {
            let due = started + Duration::from_secs_f64(n as f64 / rate as f64);
            tokio::time::sleep_until(due.into()).await;
            let _ = s3::put(
                &backend,
                "gcbench-bg",
                &format!("bg/{}", n % 256),
                data.clone(),
                &PutOptions::default(),
            )
            .await;
            n += 1;
        }
    }))
}

async fn overwrite<S: KvStore + 'static>(
    c: &TestCluster<S>,
    size: u64,
    keys: usize,
    rounds: usize,
) -> usize {
    let data = Bytes::from(random_bytes(usize::try_from(size).unwrap(), size));
    let mut ops = 0;
    for r in 0..rounds {
        let jobs: Vec<_> = (0..keys)
            .map(|k| {
                let (b, d) = (c.backend.clone(), data.clone());
                async move {
                    s3::put(
                        &b,
                        BUCKET,
                        &format!("o{size}/{k}"),
                        d,
                        &PutOptions::default(),
                    )
                    .await
                    .unwrap();
                }
            })
            .collect();
        stream::iter(jobs)
            .buffer_unordered(16)
            .collect::<Vec<_>>()
            .await;
        if r > 0 {
            ops += keys;
        }
    }
    ops
}

fn etag_of(e: Option<&ETag>) -> String {
    match e {
        Some(ETag::Strong(s) | ETag::Weak(s)) => s.clone(),
        None => String::new(),
    }
}

/// A multipart upload of `parts` copies of `part`, completed (also used
/// by `gc_safety`).
pub(crate) async fn multipart_upload(
    s3: &impl S3,
    bucket: &str,
    key: &str,
    part: &Bytes,
    parts: i32,
) {
    let id = s3
        .create_multipart_upload(request(CreateMultipartUploadInput {
            bucket: bucket.into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let mut done = Vec::new();
    for n in 1..=parts {
        let out = s3
            .upload_part(request(UploadPartInput {
                bucket: bucket.into(),
                key: key.into(),
                upload_id: id.clone(),
                part_number: n,
                body: Some(StreamingBlob::from_bytes(part.clone())),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        done.push(CompletedPart {
            part_number: Some(n),
            e_tag: Some(ETag::Strong(etag_of(out.e_tag.as_ref()))),
            ..Default::default()
        });
    }
    s3.complete_multipart_upload(request(CompleteMultipartUploadInput {
        bucket: bucket.into(),
        key: key.into(),
        upload_id: id,
        multipart_upload: Some(CompletedMultipartUpload { parts: Some(done) }),
        ..Default::default()
    }))
    .await
    .unwrap();
}

async fn multipart_overwrite<S: KvStore + 'static>(
    c: &TestCluster<S>,
    part_size: u64,
    keys: usize,
    rounds: usize,
) -> usize {
    let part = Bytes::from(random_bytes(
        usize::try_from(part_size).unwrap(),
        part_size + 1,
    ));
    let mut ops = 0;
    for r in 0..rounds {
        let jobs: Vec<_> = (0..keys)
            .map(|k| {
                let (b, p) = (c.backend.clone(), part.clone());
                async move {
                    multipart_upload(&b, BUCKET, &format!("mp{part_size}/{k}"), &p, 2).await;
                }
            })
            .collect();
        stream::iter(jobs)
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await;
        if r > 0 {
            ops += keys;
        }
    }
    ops
}

/// A chunk record placed in a real group (no shard data is written).
fn fake_chunk(group: u64, epoch: u64) -> (u32, ChunkRecord) {
    let mut chunk_id = [0u8; 16];
    chunk_id[..8].copy_from_slice(&rand_u64().to_le_bytes());
    (
        0,
        ChunkRecord {
            chunk_id,
            offset: 0,
            len: u32::try_from(MIB).unwrap(),
            scheme: "rs-gf8-v1".into(),
            data_shards: 2,
            parity_shards: 1,
            crc32c: 0,
            placement: DurablePlacement {
                group_id: group,
                epoch,
            },
        },
    )
}

fn rand_u64() -> u64 {
    u64::from_le_bytes(new_gen_id()[8..].try_into().unwrap())
}

/// Metadata-only upload of `parts` parts; returns its id.
async fn meta_upload<S: KvStore>(
    meta: &S,
    key: &str,
    parts: u32,
    group: (u64, u64),
) -> (arca_lake_metadata::records::UploadId, Vec<CompletePart>) {
    let uid = multipart::create_upload(meta, BUCKET, key, &UploadInfo::default(), now_ms())
        .await
        .unwrap()
        .upload_id;
    let one = |n: u32| async move {
        let now = now_ms();
        let p = multipart::begin_part(meta, BUCKET, key, &uid, n, now, 600_000)
            .await
            .unwrap();
        object::append_chunks(meta, &p.generation, &[fake_chunk(group.0, group.1)], now)
            .await
            .unwrap();
        let etag = format!("{n:032x}");
        multipart::seal_part(
            meta,
            &p.generation,
            &PartInfo {
                size: MIB,
                etag: etag.clone(),
                checksum: None,
                chunk_count: 1,
            },
            now,
        )
        .await
        .unwrap();
        CompletePart {
            part_number: n,
            etag,
            checksum: None,
        }
    };
    let mut done: Vec<CompletePart> = stream::iter(1..=parts)
        .map(one)
        .buffer_unordered(32)
        .collect()
        .await;
    done.sort_by_key(|p| p.part_number);
    (uid, done)
}

async fn meta_multipart<S: KvStore + 'static>(c: &TestCluster<S>, parts: u32, complete: bool) {
    let g = c
        .groups()
        .await
        .into_iter()
        .find(|g| !g.is_cache())
        .unwrap();
    let key = format!("meta/{parts}/{complete}");
    let (uid, done) = meta_upload(c.meta.as_ref(), &key, parts, (g.id, g.epoch)).await;
    let tx0 = arca_lake_metadata::kv::transact_stats();
    let t = Instant::now();
    if complete {
        multipart::complete(
            c.meta.as_ref(),
            BUCKET,
            &key,
            &uid,
            &CompleteRequest {
                parts: &done,
                root: new_gen_id(),
                pre: &Precondition::default(),
                min_part_size: 0,
                etag: "bench",
                expected_checksum: None,
                derive_checksum: &|_, _, _| None,
            },
            now_ms(),
        )
        .await
        .unwrap();
        object::delete_object(c.meta.as_ref(), BUCKET, &key, now_ms())
            .await
            .unwrap();
    } else {
        multipart::abort(c.meta.as_ref(), BUCKET, &key, &uid)
            .await
            .unwrap();
        let b = bucket::get_bucket(c.meta.as_ref(), BUCKET).await.unwrap();
        while multipart::discard_parts(c.meta.as_ref(), b.id, &key, &uid, 256, now_ms())
            .await
            .unwrap()
            > 0
        {}
    }
    let tx1 = arca_lake_metadata::kv::transact_stats();
    eprintln!(
        "{} of {parts} parts: {:?}, {} transactions",
        if complete {
            "complete + delete"
        } else {
            "abort + discard"
        },
        t.elapsed(),
        tx1.attempts - tx0.attempts
    );
    measure(
        c,
        &format!(
            "meta-multipart-{}-{parts}",
            if complete { "complete" } else { "abort" }
        ),
        parts as usize,
    )
    .await;
}

async fn matrix<S: KvStore + 'static>(c: &TestCluster<S>, full: bool) {
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let _ = s3::create_bucket(&c.backend, "gcbench-bg").await;
    let n = scale();
    let stop = Arc::new(AtomicBool::new(false));
    if full {
        for (label, size, keys) in [
            ("object-256k", 256 << 10, 200 * n),
            ("object-1m", MIB, 100 * n),
            ("object-16m", 16 * MIB, 8 * n),
        ] {
            let ops = overwrite(c, size, keys, 2).await;
            let bg = background_load(c, Arc::clone(&stop));
            measure(c, label, ops).await;
            stop.store(true, Ordering::Relaxed);
            if let Some(h) = bg {
                h.await.unwrap();
            }
            stop.store(false, Ordering::Relaxed);
        }
        // Versioned overwrites of a few keys.
        let vb = "gcbench-v";
        s3::create_bucket(&c.backend, vb).await.unwrap();
        bucket::set_versioning(c.meta.as_ref(), vb, Versioning::Enabled)
            .await
            .unwrap();
        let data = Bytes::from(random_bytes(256 << 10, 5));
        // Every version but each key's newest is deleted by version id.
        let mut ops = 0;
        let mut ids: Vec<(String, String)> = Vec::new();
        for i in 0..(100 * n) {
            let key = format!("v{}", i % 4);
            s3::put(&c.backend, vb, &key, data.clone(), &PutOptions::default())
                .await
                .unwrap();
            ids.push((key.clone(), c.manifest(vb, &key).await.pointer.version_id));
        }
        for (key, v) in &ids[..ids.len().saturating_sub(4)] {
            object::delete_version(c.meta.as_ref(), vb, key, v, now_ms())
                .await
                .unwrap();
            ops += 1;
        }
        measure(c, "versioned-256k", ops).await;
        for (label, part, keys) in [
            ("multipart-2x1m", MIB, 50 * n),
            ("multipart-2x8m", 8 * MIB, 8 * n),
            ("multipart-2x64m", 64 * MIB, n),
        ] {
            let ops = multipart_overwrite(c, part, keys, 2).await;
            let bg = background_load(c, Arc::clone(&stop));
            measure(c, label, ops).await;
            stop.store(true, Ordering::Relaxed);
            if let Some(h) = bg {
                h.await.unwrap();
            }
            stop.store(false, Ordering::Relaxed);
        }
    }
    for parts in [100, 1000, 10_000] {
        meta_multipart(c, parts, true).await;
        meta_multipart(c, parts, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement"]
async fn in_memory_metadata() {
    let c = TestCluster::start(opts()).await;
    matrix(&c, true).await;
    c.shutdown().await;
}

#[cfg(feature = "fdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement"]
async fn fdb_metadata() {
    if std::env::var_os("ARCA_FDB_TESTS").is_none() {
        eprintln!("ARCA_FDB_TESTS not set; skipping");
        return;
    }
    let path = std::env::var("ARCA_FDB_CLUSTER_FILE").ok();
    let kv = arca_lake_metadata::fdb::FdbKv::open(path.as_deref()).expect("open FoundationDB");
    kv.clear_range_blocking(&arca_lake_metadata::keys::all_range())
        .await
        .unwrap();
    let c = TestCluster::start_with(Arc::new(kv), opts()).await;
    let full = std::env::var_os("ARCA_GC_BENCH_FULL").is_some();
    matrix(&c, full).await;
    c.shutdown().await;
}
