//! Model-based test of the whole data path (Phase 3.8, H5/H6): random
//! sequences of client operations (PUT, overwrite, DELETE, multipart
//! complete and abort) interleaved with every background process that
//! moves or removes data (GC, multipart sweep, reconciliation, compaction,
//! shard loss with scrub and repair, chunk migration between groups with
//! crashes before and after the switch, cache population and eviction,
//! a former controller leader's late GC), run on an in-memory cluster and
//! on a reference model (key → bytes) at once.
//!
//! After every step every key reads back what the model says (whole
//! bytes, or `NoSuchKey`). At the end, once everything is collected, the
//! cluster holds exactly the shards of the live objects (no referenced
//! shard was deleted, every orphan converged) and no node holds capacity.
//!
//! Reproduction: each run's seed is printed on failure and appended to
//! `$CARGO_TARGET_TMPDIR/system-model-failures.txt`; `ARCA_MODEL_SEED=<n>`
//! replays one, `ARCA_MODEL_RUNS` and `ARCA_MODEL_STEPS` (default 8 and
//! 80) set the volume.

#![allow(clippy::too_many_lines, clippy::cast_possible_truncation)]

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use arca_lake_controller::integrity::{
    RepairWorkerOptions, ScrubOptions, coders, repair_pass, scrub_group_now,
};
use arca_lake_metadata::records::GroupRecord;
use arca_lake_testkit::s3::{self, PutOptions, request};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use arca_lake_tiering::durable::Coders;
use arca_lake_tiering::migrate::{MigrateFault, migrate_chunk};
use bytes::Bytes;
use s3s::dto::{
    AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletedMultipartUpload,
    CompletedPart, CreateMultipartUploadInput, ETag, StreamingBlob, UploadPartInput,
};
use s3s::{S3, S3ErrorCode};

const BUCKET: &str = "model";
const KEYS: usize = 8;
const CHUNK: u64 = 256 << 10;

/// xorshift64*: deterministic from the seed, no dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Put,
    Delete,
    MultipartComplete,
    MultipartAbort,
    Gc,
    Reconcile,
    Compact,
    LoseShardAndRepair,
    Migrate(Option<MigrateFault>),
    CacheReadAndEvict,
    FormerLeaderGc,
}

fn pick(rng: &mut Rng) -> Op {
    match rng.below(20) {
        0..=5 => Op::Put,
        6..=7 => Op::Delete,
        8 => Op::MultipartComplete,
        9 => Op::MultipartAbort,
        10..=11 => Op::Gc,
        12 => Op::Reconcile,
        13 => Op::Compact,
        14 => Op::LoseShardAndRepair,
        15 => Op::Migrate(None),
        16 => Op::Migrate(Some(MigrateFault::BeforeSwitch)),
        17 => Op::Migrate(Some(MigrateFault::AfterSwitch)),
        18 => Op::CacheReadAndEvict,
        _ => Op::FormerLeaderGc,
    }
}

fn etag(e: Option<&ETag>) -> String {
    match e {
        Some(ETag::Strong(s) | ETag::Weak(s)) => s.clone(),
        None => String::new(),
    }
}

async fn multipart(c: &TestCluster, key: &str, parts: &[Bytes], complete: bool) {
    let id = c
        .backend
        .create_multipart_upload(request(CreateMultipartUploadInput {
            bucket: BUCKET.into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let mut done = Vec::new();
    for (i, p) in parts.iter().enumerate() {
        let n = i32::try_from(i + 1).unwrap();
        let out = c
            .backend
            .upload_part(request(UploadPartInput {
                bucket: BUCKET.into(),
                key: key.into(),
                upload_id: id.clone(),
                part_number: n,
                body: Some(StreamingBlob::from_bytes(p.clone())),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        done.push(CompletedPart {
            part_number: Some(n),
            e_tag: Some(ETag::Strong(etag(out.e_tag.as_ref()))),
            ..Default::default()
        });
    }
    if complete {
        c.backend
            .complete_multipart_upload(request(CompleteMultipartUploadInput {
                bucket: BUCKET.into(),
                key: key.into(),
                upload_id: id,
                multipart_upload: Some(CompletedMultipartUpload { parts: Some(done) }),
                ..Default::default()
            }))
            .await
            .unwrap();
    } else {
        c.backend
            .abort_multipart_upload(request(AbortMultipartUploadInput {
                bucket: BUCKET.into(),
                key: key.into(),
                upload_id: id,
                ..Default::default()
            }))
            .await
            .unwrap();
    }
}

fn scrub_opts() -> ScrubOptions {
    ScrubOptions {
        page: 256,
        page_bytes: 0,
        rate_bytes_per_s: 0,
        interval_ms: 1,
        concurrency: 1,
        adaptive: None,
        manifest_roots_per_pass: 0,
    }
}

async fn ec_groups(c: &TestCluster) -> Vec<GroupRecord> {
    let mut g: Vec<GroupRecord> = c
        .groups()
        .await
        .into_iter()
        .filter(|g| !g.is_cache() && g.scheme == "rs-gf8-v1")
        .collect();
    g.sort_by_key(|g| g.id);
    g
}

/// Every chunk of the current version of `key` (a multipart object's are
/// in its parts' generations): `(generation, index, record)`.
async fn object_chunks(
    c: &TestCluster,
    key: &str,
) -> Vec<(
    arca_lake_metadata::records::GenId,
    u32,
    arca_lake_metadata::records::ChunkRecord,
)> {
    let m = c.manifest(BUCKET, key).await;
    if m.parts.is_empty() {
        return m
            .chunks
            .into_iter()
            .map(|(i, r)| (m.pointer.generation, i, r))
            .collect();
    }
    let mut out = Vec::new();
    for p in &m.parts {
        let (chunks, _) =
            arca_lake_metadata::ops::object::read_part_manifest(c.meta.as_ref(), &p.generation)
                .await
                .unwrap()
                .unwrap();
        out.extend(chunks.into_iter().map(|(i, r)| (p.generation, i, r)));
    }
    out
}

/// Every key reads what the model says.
async fn check(c: &TestCluster, model: &BTreeMap<String, Bytes>, seed: u64, step: usize) {
    for i in 0..KEYS {
        let k = format!("k{i}");
        match (model.get(&k), s3::get(&c.backend, BUCKET, &k).await) {
            (Some(v), Ok((got, _))) => {
                assert_eq!(&got, v, "seed {seed} step {step}: {k} reads other bytes");
            }
            (None, Err(e)) if *e.code() == S3ErrorCode::NoSuchKey => {}
            (want, got) => panic!(
                "seed {seed} step {step}: {k} expected {:?}, got {:?}",
                want.map(Bytes::len),
                got.map(|(b, _)| b.len())
            ),
        }
    }
}

async fn run(seed: u64, steps: usize) -> BTreeMap<String, usize> {
    let mut rng = Rng(seed | 1);
    let c = TestCluster::start(ClusterOptions {
        groups: 2,
        multipart_min_part_bytes: CHUNK / 4,
        cache_nodes: 2,
        cache: Some(ClusterOptions::default_cache()),
        ..ClusterOptions::rs(6, 2, 1, CHUNK, 16 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut model: BTreeMap<String, Bytes> = BTreeMap::new();
    let nodes: HashMap<u64, String> = c.nodes.iter().map(|n| (n.id, n.addr.to_string())).collect();
    let addr_of = |id: u64| nodes.get(&id).cloned();
    let mut trace = Vec::new();
    for step in 0..steps {
        let op = pick(&mut rng);
        trace.push(op);
        let key = format!("k{}", rng.below(KEYS as u64));
        let size = match rng.below(4) {
            0 => 1 + rng.below(16 << 10),
            1 => CHUNK,
            _ => 1 + rng.below(3 * CHUNK),
        };
        let data = Bytes::from(random_bytes(size as usize, rng.next()));
        match op {
            Op::Put => {
                s3::put(
                    &c.backend,
                    BUCKET,
                    &key,
                    data.clone(),
                    &PutOptions::default(),
                )
                .await
                .unwrap();
                model.insert(key, data);
            }
            Op::Delete => {
                s3::delete(&c.backend, BUCKET, &key).await.unwrap();
                model.remove(&key);
            }
            Op::MultipartComplete | Op::MultipartAbort => {
                let parts: Vec<Bytes> = (0..=rng.below(3))
                    .map(|_| Bytes::from(random_bytes(CHUNK as usize / 2, rng.next())))
                    .collect();
                let complete = matches!(op, Op::MultipartComplete);
                multipart(&c, &key, &parts, complete).await;
                if complete {
                    model.insert(key, Bytes::from(parts.concat()));
                }
            }
            Op::Gc => {
                // Nothing is in flight between steps: an hour ahead is
                // safe and lets every retired generation go.
                let far = now_ms() + 3_600_000;
                c.gc(far, 0).await;
                arca_lake_controller::gc::sweep_multipart(c.meta.as_ref(), far, Some(far), 1000)
                    .await
                    .unwrap();
            }
            Op::Reconcile => {
                c.reconcile(now_ms() + 3_600_000, 0).await;
            }
            Op::Compact => {
                for i in 0..c.nodes.len() {
                    let s = c.node_store(i);
                    for seg in s.compaction_candidates(0.0).unwrap_or_default() {
                        let _ = s.compact(seg);
                    }
                }
            }
            Op::LoseShardAndRepair => {
                // One shard of a live object's chunk is lost on its node;
                // a scrub finds it and repair rebuilds it.
                if let Some(k) = model.keys().nth(rng.below(model.len() as u64) as usize) {
                    let chunks = object_chunks(&c, k).await;
                    let (_, _, rec) = &chunks[rng.below(chunks.len() as u64) as usize];
                    if rec.scheme == "rs-gf8-v1" {
                        let g = c
                            .groups()
                            .await
                            .into_iter()
                            .find(|g| g.id == rec.placement.group_id)
                            .unwrap();
                        let member = &g.members[rng.below(g.members.len() as u64) as usize];
                        let i = c.node_index(member).unwrap();
                        let store = c.node_store(i);
                        for sk in store.keys_in_group(g.id) {
                            if sk.chunk_id == rec.chunk_id {
                                store.delete(sk).unwrap();
                            }
                        }
                        scrub_group_now(
                            c.meta.as_ref(),
                            &c.clients,
                            &coders(),
                            g.id,
                            &scrub_opts(),
                            now_ms(),
                        )
                        .await
                        .unwrap();
                        repair_pass(
                            c.meta.as_ref(),
                            &c.clients,
                            &coders(),
                            &RepairWorkerOptions::default(),
                            None,
                            None,
                            now_ms(),
                        )
                        .await
                        .unwrap();
                    }
                }
            }
            Op::Migrate(fault) => {
                if let Some(k) = model.keys().nth(rng.below(model.len() as u64) as usize) {
                    let groups = ec_groups(&c).await;
                    let chunks = object_chunks(&c, k).await;
                    for (generation, index, rec) in
                        chunks.iter().filter(|(_, _, r)| r.scheme == "rs-gf8-v1")
                    {
                        let Some(src) = groups.iter().find(|g| g.id == rec.placement.group_id)
                        else {
                            continue;
                        };
                        let dst = groups.iter().find(|g| g.id != src.id).unwrap();
                        // Errors are the injected crashes.
                        let _ = migrate_chunk(
                            c.meta.as_ref(),
                            &c.clients,
                            &Coders::default(),
                            &addr_of,
                            src,
                            dst,
                            generation,
                            *index,
                            rec,
                            fault,
                        )
                        .await;
                    }
                    if fault.is_some() {
                        // The crashed attempt's intent expires later.
                        arca_lake_metadata::ops::object::expire_migrations(
                            c.meta.as_ref(),
                            now_ms() + 2 * arca_lake_metadata::ops::object::MIGRATION_INTENT_TTL_MS,
                        )
                        .await
                        .unwrap();
                    }
                }
            }
            Op::CacheReadAndEvict => {
                if let Some(k) = model.keys().next() {
                    // A miss populates the cache in the background.
                    let _ = s3::get(&c.backend, BUCKET, k).await;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                let cfg = arca_lake_tiering::cache::CacheTierConfig {
                    capacity_bytes: 1,
                    high_watermark: 0.0,
                    low_watermark: 0.0,
                    ..ClusterOptions::default_cache()
                };
                let map = c.backend.map();
                let addr = |node_id: u64| map.node_addr(node_id).map(str::to_string);
                arca_lake_tiering::cache::evict_tier(
                    c.meta.as_ref(),
                    &c.clients,
                    &cfg,
                    &map.groups,
                    &addr,
                )
                .await
                .unwrap();
            }
            Op::FormerLeaderGc => {
                // A leader whose lease lapsed runs a late GC pass: every
                // metadata write it makes is fenced.
                use arca_lake_metadata::fence::FencedKv;
                use arca_lake_metadata::ops::leader;
                let t = now_ms();
                let old = leader::try_acquire(c.meta.as_ref(), "old", t, 1)
                    .await
                    .unwrap();
                if let Some(old) = old {
                    leader::try_acquire(c.meta.as_ref(), "new", t + 10, 1)
                        .await
                        .unwrap();
                    let fenced = FencedKv::new(std::sync::Arc::clone(&c.meta), old);
                    let r = arca_lake_controller::gc::run_once(
                        &fenced,
                        &c.clients,
                        now_ms() + 3_600_000,
                        0,
                        1000,
                    )
                    .await;
                    if let Ok(r) = r {
                        assert_eq!(
                            r.collected, 0,
                            "seed {seed} step {step}: a fenced GC collected"
                        );
                    }
                }
            }
        }
        check(&c, &model, seed, step).await;
    }
    // Converge: collect everything collectable.
    let far = now_ms() + 3_600_000;
    for _ in 0..3 {
        c.gc(far, 0).await;
        arca_lake_controller::gc::sweep_multipart(c.meta.as_ref(), far, Some(far), 1000)
            .await
            .unwrap();
    }
    arca_lake_metadata::ops::object::expire_migrations(
        c.meta.as_ref(),
        far + 2 * arca_lake_metadata::ops::object::MIGRATION_INTENT_TTL_MS,
    )
    .await
    .unwrap();
    c.reconcile(far, 0).await;
    let _ = c.reconcile_cache().await;
    check(&c, &model, seed, steps).await;
    // Exactly the live objects' shards on the durable groups.
    let mut expected = 0usize;
    for k in model.keys() {
        for (_, _, rec) in object_chunks(&c, k).await {
            expected += usize::from(rec.data_shards + rec.parity_shards);
        }
    }
    let durable: usize = {
        let groups: Vec<GroupRecord> = c
            .groups()
            .await
            .into_iter()
            .filter(|g| !g.is_cache())
            .collect();
        let mut n = 0;
        for g in &groups {
            for m in &g.members {
                if let Some(i) = c.node_index(m) {
                    n += c.shard_count(i, g.id);
                }
            }
        }
        n
    };
    assert_eq!(
        durable, expected,
        "seed {seed}: durable shards left {durable}, live objects need {expected}; trace {trace:?}"
    );
    for i in 0..c.nodes.len() {
        assert_eq!(
            c.node_service(i).device_capacity().unwrap().inflight_bytes,
            0,
            "seed {seed}: node {i} holds capacity"
        );
    }
    c.shutdown().await;
    let mut counts = BTreeMap::new();
    for op in trace {
        *counts.entry(format!("{op:?}")).or_insert(0) += 1;
    }
    counts
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_operation_sequences_agree_with_the_model() {
    let steps = env("ARCA_MODEL_STEPS", 80) as usize;
    let seeds: Vec<u64> = if let Ok(s) = std::env::var("ARCA_MODEL_SEED") {
        vec![s.parse().expect("ARCA_MODEL_SEED")]
    } else {
        let base = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos() as u64);
        (0..env("ARCA_MODEL_RUNS", 8))
            .map(|i| base.wrapping_add(i * 7919))
            .collect()
    };
    for seed in seeds {
        let started = Instant::now();
        match tokio::spawn(run(seed, steps)).await {
            Ok(counts) => eprintln!(
                "system model seed {seed}: {steps} steps in {:?}: {counts:?}",
                started.elapsed()
            ),
            Err(e) => {
                let line = format!("{seed} steps {steps}\n");
                let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
                    .join("system-model-failures.txt");
                let _ = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()));
                panic!(
                    "system model failed for seed {seed} (replay: ARCA_MODEL_SEED={seed}; recorded in {}): {e:?}",
                    path.display()
                );
            }
        }
    }
}
