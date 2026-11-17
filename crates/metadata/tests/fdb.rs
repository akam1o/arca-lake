//! The write protocol against a real FoundationDB cluster.
//!
//! Requires `--features fdb`, the FoundationDB client library, and a cluster
//! file in `ARCA_FDB_CLUSTER_FILE` (or the client default). Skips with a
//! message when the environment variable `ARCA_FDB_TESTS` is not set, so
//! that `cargo test --all-features` on a machine without a cluster does not
//! fail. See `deploy/dev/README.md` for a one-command setup.
//!
//! The goals are (1) to confirm that the recipes behave identically on the
//! in-memory emulation and on the real store, including conflict detection
//! and size limits, and (2) to collect first latency numbers for the commit
//! path. Latencies are printed, not asserted, and are "local, relative only".

#![allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::many_single_char_names
)]

use std::future::Future;
use std::time::Instant;

use arca_lake_metadata::fdb::FdbKv;
use arca_lake_metadata::kv::KeyRange;
use arca_lake_metadata::ops::multipart::{
    self, CompletePart, CompleteRequest, PartInfo, UploadInfo,
};
use arca_lake_metadata::ops::object::{
    CommitInfo, Precondition, append_chunks, begin_put, commit_put, delete_object, head_object,
    read_manifest,
};
use arca_lake_metadata::ops::{bucket, gc, list};
use arca_lake_metadata::records::{
    ChecksumAlgorithm, ChecksumType, ChunkRecord, DurablePlacement, GenState, Versioning,
    new_gen_id,
};
use arca_lake_metadata::{KvError, MetaError};
use futures::{StreamExt, stream};

#[path = "support/attribute.rs"]
mod attribute;

fn run<T>(f: impl Future<Output = T>) -> T {
    futures::executor::block_on(f)
}

const LEASE: u64 = 60_000;

fn open() -> Option<FdbKv> {
    if std::env::var_os("ARCA_FDB_TESTS").is_none() {
        eprintln!("ARCA_FDB_TESTS not set; skipping FoundationDB tests");
        return None;
    }
    let path = std::env::var("ARCA_FDB_CLUSTER_FILE").ok();
    let kv = FdbKv::open(path.as_deref()).expect("open FoundationDB");
    // Each test starts from an empty arca-lake keyspace.
    run(kv.clear_range_blocking(&KeyRange::prefix(&foundationdb_tuple::pack(&("al",))))).unwrap();
    Some(kv)
}

fn chunk(i: u32) -> (u32, ChunkRecord) {
    (
        i,
        ChunkRecord {
            chunk_id: [u8::try_from(i % 256).unwrap(); 16],
            offset: u64::from(i) * 4096,
            len: 4096,
            scheme: "rs-gf8-v1".into(),
            data_shards: 8,
            parity_shards: 4,
            crc32c: i,
            placement: DurablePlacement {
                group_id: 1,
                epoch: 1,
            },
        },
    )
}

fn info(etag: &str, chunks: u32) -> CommitInfo {
    CommitInfo {
        size: u64::from(chunks) * 4096,
        etag: etag.into(),
        content_type: None,
        user_metadata: vec![],
        chunk_count: chunks,
        checksum: None,
    }
}

async fn put(
    kv: &FdbKv,
    bucket: &str,
    key: &str,
    etag: &str,
    chunks: u32,
    now: u64,
    pre: &Precondition,
) -> Result<arca_lake_metadata::ops::object::Committed, MetaError> {
    let p = begin_put(kv, bucket, key, now, LEASE).await?;
    let recs: Vec<_> = (0..chunks).map(chunk).collect();
    append_chunks(kv, &p.generation, &recs, now).await?;
    commit_put(kv, &p.generation, &info(etag, chunks), pre, now).await
}

#[test]
fn protocol_on_foundationdb() {
    let Some(kv) = open() else { return };
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();

        // Put, read, overwrite, delete.
        let c1 = put(&kv, "b", "k", "e1", 3, 10, &Precondition::default())
            .await
            .unwrap();
        let m = read_manifest(&kv, "b", "k", None).await.unwrap().unwrap();
        assert_eq!(m.chunks.len(), 3);
        let c2 = put(&kv, "b", "k", "e2", 1, 20, &Precondition::default())
            .await
            .unwrap();
        assert_eq!(c2.retired, Some(c1.generation));
        assert_eq!(
            gc::collectable(&kv, 31, 10, 10).await.unwrap(),
            vec![c1.generation]
        );
        gc::finish(&kv, &c1.generation).await.unwrap();
        let d = delete_object(&kv, "b", "k", 40).await.unwrap();
        assert_eq!(d.retired, Some(c2.generation));
        assert!(head_object(&kv, "b", "k").await.unwrap().is_none());

        // Concurrent conditional creates: exactly one wins, on the real store.
        let pre = Precondition {
            if_none_match_any: true,
            ..Default::default()
        };
        for round in 0..10u64 {
            let key = format!("c{round}");
            let (a, b) = futures::join!(
                put(&kv, "b", &key, "A", 1, 10, &pre),
                put(&kv, "b", &key, "B", 1, 10, &pre),
            );
            assert_eq!(
                usize::from(a.is_ok()) + usize::from(b.is_ok()),
                1,
                "round {round}: {a:?} {b:?}"
            );
        }
        assert_eq!(
            gc::expire_pending(&kv, 10 + LEASE, 100)
                .await
                .unwrap()
                .len(),
            10
        );

        // Real size limits: a value over 100 KB is rejected by the server.
        let p = begin_put(&kv, "b", "huge", 1, LEASE).await.unwrap();
        let huge = CommitInfo {
            user_metadata: vec![("m".into(), "x".repeat(200_000))],
            ..info("e", 0)
        };
        let err = commit_put(&kv, &p.generation, &huge, &Precondition::default(), 2)
            .await
            .unwrap_err();
        assert!(
            matches!(err, MetaError::Kv(KvError::TooLarge { what: "value", .. })),
            "{err}"
        );

        // A manifest larger than one transaction is split and reads back.
        let p = begin_put(&kv, "b", "big", 1, LEASE).await.unwrap();
        let recs: Vec<_> = (0..120_000).map(chunk).collect();
        let t = Instant::now();
        append_chunks(&kv, &p.generation, &recs, 1).await.unwrap();
        let append_elapsed = t.elapsed();
        commit_put(
            &kv,
            &p.generation,
            &info("big", 120_000),
            &Precondition::default(),
            2,
        )
        .await
        .unwrap();
        let t = Instant::now();
        let m = read_manifest(&kv, "b", "big", None).await.unwrap().unwrap();
        let read_elapsed = t.elapsed();
        assert_eq!(m.chunks.len(), 120_000);
        assert!(matches!(m.header.state, GenState::Committed));
        eprintln!(
            "local, relative only: append 120000 chunk records {append_elapsed:?}; read full manifest {read_elapsed:?}"
        );

        // Listing with delimiter.
        for k in ["d/1", "d/2", "x"] {
            put(&kv, "b", k, "e", 1, 1, &Precondition::default())
                .await
                .unwrap();
        }
        let l = list::list_objects(
            &kv,
            "b",
            &list::ListRequest {
                delimiter: Some("/".into()),
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let names: Vec<_> = l.objects.iter().map(|o| o.key.as_str()).collect();
        assert!(names.contains(&"x") && names.contains(&"big"));
        assert_eq!(l.common_prefixes, ["d/"]);
    });
}

#[test]
fn commit_latency_sample() {
    let Some(kv) = open() else { return };
    run(async {
        bucket::create_bucket(&kv, "lat", Versioning::Disabled, 1)
            .await
            .unwrap();
        let n = 200u32;
        let mut begin_us = Vec::with_capacity(n as usize);
        let mut commit_us = Vec::with_capacity(n as usize);
        for i in 0..n {
            let key = format!("k{i}");
            let t = Instant::now();
            let p = begin_put(&kv, "lat", &key, 1, LEASE).await.unwrap();
            begin_us.push(t.elapsed().as_micros());
            append_chunks(&kv, &p.generation, &[chunk(0)], 1)
                .await
                .unwrap();
            let t = Instant::now();
            commit_put(
                &kv,
                &p.generation,
                &info("e", 1),
                &Precondition::default(),
                2,
            )
            .await
            .unwrap();
            commit_us.push(t.elapsed().as_micros());
        }
        begin_us.sort_unstable();
        commit_us.sort_unstable();
        let pct = |v: &[u128], p: usize| v[(v.len() * p / 100).min(v.len() - 1)];
        eprintln!(
            "local, relative only: begin_put p50={}us p99={}us; commit_put p50={}us p99={}us (n={n})",
            pct(&begin_us, 50),
            pct(&begin_us, 99),
            pct(&commit_us, 50),
            pct(&commit_us, 99)
        );
    });
}

/// RFC-007 §5.3: a 10 000-part upload completes in exactly one
/// FoundationDB transaction (the transaction size is bounded by the number
/// of parts, not by chunks or bytes), and finishing the retired root
/// retires every part in bounded steps (I-MP2).
#[test]
fn ten_thousand_part_complete_is_one_transaction_on_foundationdb() {
    let Some(kv) = open() else { return };
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let key = "k".repeat(1024);
        let info = UploadInfo {
            checksum_algorithm: Some(ChecksumAlgorithm::Crc32c),
            checksum_type: Some(ChecksumType::Composite),
            ..Default::default()
        };
        let uid = multipart::create_upload(&kv, "b", &key, &info, 1)
            .await
            .unwrap()
            .upload_id;
        let started = Instant::now();
        // Parts are uploaded 32 at a time, as clients do; each part still
        // runs begin, append, seal in order. The recipes write only
        // per-generation and per-part keys, so the parts do not conflict.
        let upload = |n: u32| {
            let (kv, key, uid) = (&kv, &key, &uid);
            async move {
                let etag = format!("{n:032x}");
                let p = multipart::begin_part(kv, "b", key, uid, n, 2, LEASE)
                    .await
                    .unwrap();
                append_chunks(kv, &p.generation, &[chunk(0)], 2)
                    .await
                    .unwrap();
                multipart::seal_part(
                    kv,
                    &p.generation,
                    &PartInfo {
                        size: 5 << 20,
                        etag: etag.clone(),
                        checksum: Some(arca_lake_metadata::records::Checksum {
                            algorithm: ChecksumAlgorithm::Crc32c,
                            kind: ChecksumType::FullObject,
                            value: n.to_be_bytes().to_vec(),
                            parts: None,
                        }),
                        chunk_count: 1,
                    },
                    2,
                )
                .await
                .unwrap();
                CompletePart {
                    part_number: n,
                    etag,
                    checksum: None,
                }
            }
        };
        let mut parts: Vec<CompletePart> = stream::iter(1..=10_000u32)
            .map(upload)
            .buffer_unordered(32)
            .collect()
            .await;
        parts.sort_by_key(|p| p.part_number);
        eprintln!(
            "uploaded 10 000 parts (32 concurrent) in {:?}",
            started.elapsed()
        );
        let before = arca_lake_metadata::kv::transact_stats();
        let root = new_gen_id();
        let t = Instant::now();
        let done = multipart::complete(
            &kv,
            "b",
            &key,
            &uid,
            &CompleteRequest {
                parts: &parts,
                root,
                pre: &Precondition::default(),
                min_part_size: 5 << 20,
                etag: "composite-10000",
                expected_checksum: None,
                derive_checksum: &|_, kind, inputs| {
                    (kind == ChecksumType::Composite)
                        .then(|| inputs.iter().flat_map(|(_, v)| v.clone()).collect())
                },
            },
            3,
        )
        .await
        .unwrap();
        let after = arca_lake_metadata::kv::transact_stats();
        eprintln!("complete of 10 000 parts: {:?}", t.elapsed());
        assert_eq!(after.attempts - before.attempts, 1, "one transaction");
        assert_eq!(done.size, (5 << 20) * 10_000);
        let m = read_manifest(&kv, "b", &key, None).await.unwrap().unwrap();
        assert_eq!(m.parts.len(), 10_000);
        loop {
            if multipart::discard_parts(&kv, 1, &key, &uid, 512, 4)
                .await
                .unwrap()
                < 512
            {
                break;
            }
        }
        delete_object(&kv, "b", &key, 5).await.unwrap();
        let t = Instant::now();
        let mut steps = 0;
        while multipart::finish_root_step(&kv, &root, 256, 6)
            .await
            .unwrap()
        {
            steps += 1;
        }
        gc::finish(&kv, &root).await.unwrap();
        eprintln!("root finish: {steps} steps in {:?}", t.elapsed());
        assert!(steps >= 39);
        assert_eq!(
            gc::collectable(&kv, 1_000, 0, 20_000).await.unwrap().len(),
            10_000
        );
    });
}

/// Phase 4.1 (ADR-0033): the batched shard attribution, whose reads the
/// FoundationDB backend issues at once (`get_many`, `get_first_many`),
/// answers as `attribute_shard` alone on random metadata, prefixes with
/// zero bytes included; the cases of `attribute_batch.rs`, from fixed
/// seeds.
#[test]
fn batched_attribution_agrees_with_one_shard_at_a_time_on_foundationdb() {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
    let Some(kv) = open() else { return };
    let strategy = (
        proptest::collection::vec(attribute::generation(), 0..8),
        proptest::collection::vec((0..7u8, 0..4u32, 0..3u64), 0..4),
        proptest::collection::vec((0..7u8, 0..4u32, 1..3u64), 1..24),
    );
    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &[41; 32]),
    );
    for case in 0..64 {
        let (gens, intents, queries) = strategy.new_tree(&mut runner).unwrap().current();
        run(kv.clear_range_blocking(&KeyRange::prefix(&foundationdb_tuple::pack(&("al",)))))
            .unwrap();
        run(attribute::load(&kv, &gens, &intents));
        if let Some(diff) = run(attribute::compare(&kv, &queries)) {
            panic!("case {case}: {diff}");
        }
    }
    // One batch at the size reconciliation uses.
    let queries: Vec<(u8, u32, u64)> = (0..512u32)
        .map(|i| ((i % 7) as u8, i % 4, u64::from(i % 2) + 1))
        .collect();
    assert_eq!(run(attribute::compare(&kv, &queries)), None);
}
