//! FoundationDB failover on a multi-process cluster (RFC-011 §6).
//!
//! Requires `--features fdb`, `ARCA_FDB_TESTS=1`, `ARCA_FDB_FAILOVER=1`, a
//! cluster file, and a chaos driver that can kill and restart the
//! FoundationDB processes `arca-fdb-1..3`:
//!
//! - `ARCA_CHAOS_DOCKER=1`: the test runs `docker exec arca-fdb fdbproc
//!   kill|start N` itself (CI, test on the host; the container runs
//!   `deploy/dev/fdb-multi-entrypoint.sh`).
//! - `ARCA_CHAOS_DIR=<dir>`: the test writes `req-N` files and waits for
//!   `ack-N` (local, test inside a container without a docker socket;
//!   `deploy/dev/fdb-failover.sh` answers them).
//!
//! The cluster is configured `double` with three coordinators, so it
//! tolerates one lost process; losing two coordinators loses the quorum and
//! every operation must fail explicitly within the transaction timeout.
//! This describes the test configuration, not a production availability
//! guarantee.

#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arca_lake_controller::ControlError;
use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_controller::repair::RepairStep;
use arca_lake_metadata::Txn;
use arca_lake_metadata::fdb::FdbKv;
use arca_lake_metadata::records::GroupState;
use arca_lake_testkit::chaos::Chaos;
use arca_lake_testkit::s3::{self, PutOptions, request};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::dto::{
    AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletedMultipartUpload,
    CompletedPart, CreateMultipartUploadInput, ETag, ListMultipartUploadsInput, StreamingBlob,
    UploadPartInput,
};
use s3s::{S3, S3Result};

const MIB: u64 = 1 << 20;
const BUCKET: &str = "failover";

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait until the store answers again after processes were restarted: a
/// read succeeds before the transaction subsystem has recovered (R-50: a
/// PUT issued right after a read-only probe timed out on its commit), so
/// the probe is a committed write.
async fn wait_for_store(c: &TestCluster<FdbKv>) {
    wait_until("store available", Duration::from_secs(120), async || {
        arca_lake_metadata::transact(c.meta.as_ref(), (), |tx, ()| {
            Box::pin(async move {
                tx.set(b"zz-failover-probe", b"1");
                Ok(())
            })
        })
        .await
        .is_ok()
    })
    .await;
}

/// Until probe commits have succeeded without a failure for 10 s (at
/// least five of them): a cluster just created or just restarted can still
/// answer 1021 a few seconds after its first good commits (one setup's
/// node registration did, right after five quick successes).
async fn settle(kv: &FdbKv) {
    let start = std::time::Instant::now();
    let mut since: Option<std::time::Instant> = None;
    let mut ok = 0;
    loop {
        assert!(
            start.elapsed() < Duration::from_secs(180),
            "the store did not settle"
        );
        let r = arca_lake_metadata::transact(kv, (), |tx, ()| {
            Box::pin(async move {
                tx.set(b"zz-failover-probe", b"1");
                Ok(())
            })
        })
        .await;
        if r.is_ok() {
            ok += 1;
            let first = *since.get_or_insert_with(std::time::Instant::now);
            if ok >= 5 && first.elapsed() >= Duration::from_secs(10) {
                return;
            }
        } else {
            ok = 0;
            since = None;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Multipart helpers (RFC-007 A10).
async fn create_upload(s3: &impl S3, key: &str) -> String {
    s3.create_multipart_upload(request(CreateMultipartUploadInput {
        bucket: BUCKET.into(),
        key: key.into(),
        ..Default::default()
    }))
    .await
    .unwrap()
    .output
    .upload_id
    .unwrap()
}

async fn upload_part(
    s3: &impl S3,
    key: &str,
    upload_id: &str,
    n: i32,
    data: Bytes,
) -> S3Result<String> {
    let out = s3
        .upload_part(request(UploadPartInput {
            bucket: BUCKET.into(),
            key: key.into(),
            upload_id: upload_id.into(),
            part_number: n,
            body: Some(StreamingBlob::from_bytes(data)),
            ..Default::default()
        }))
        .await?
        .output;
    Ok(match out.e_tag {
        Some(ETag::Strong(s) | ETag::Weak(s)) => s,
        None => panic!("no etag"),
    })
}

async fn complete(
    s3: &impl S3,
    key: &str,
    upload_id: &str,
    parts: &[(i32, String)],
) -> S3Result<s3s::dto::CompleteMultipartUploadOutput> {
    Ok(s3
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: BUCKET.into(),
            key: key.into(),
            upload_id: upload_id.into(),
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(
                    parts
                        .iter()
                        .map(|(n, e)| CompletedPart {
                            part_number: Some(*n),
                            e_tag: Some(ETag::Strong(e.clone())),
                            ..Default::default()
                        })
                        .collect(),
                ),
            }),
            ..Default::default()
        }))
        .await?
        .output)
}

async fn abort(s3: &impl S3, key: &str, upload_id: &str) -> S3Result<()> {
    s3.abort_multipart_upload(request(AbortMultipartUploadInput {
        bucket: BUCKET.into(),
        key: key.into(),
        upload_id: upload_id.into(),
        ..Default::default()
    }))
    .await?;
    Ok(())
}

async fn open_uploads(s3: &impl S3) -> Vec<String> {
    s3.list_multipart_uploads(request(ListMultipartUploadsInput {
        bucket: BUCKET.into(),
        ..Default::default()
    }))
    .await
    .unwrap()
    .output
    .uploads
    .unwrap_or_default()
    .into_iter()
    .filter_map(|u| u.upload_id)
    .collect()
}

#[tokio::test]
async fn foundationdb_process_and_coordinator_failures() {
    if std::env::var_os("ARCA_FDB_TESTS").is_none()
        || std::env::var_os("ARCA_FDB_FAILOVER").is_none()
    {
        eprintln!("ARCA_FDB_FAILOVER not set; skipping");
        return;
    }
    // Nothing in this test may wait forever: a hung step is a failure.
    tokio::time::timeout(Duration::from_mins(15), scenario())
        .await
        .expect("the failover scenario exceeded 15 minutes");
}

async fn scenario() {
    let Some(chaos) = Chaos::from_env() else {
        eprintln!("no chaos driver configured; skipping");
        return;
    };
    let path = std::env::var("ARCA_FDB_CLUSTER_FILE").ok();
    // A short timeout so that a lost quorum fails fast (test configuration).
    let kv = FdbKv::open_with_timeout(path.as_deref(), 5_000).expect("open FoundationDB");
    // An earlier scenario's chaos may have restarted processes moments
    // ago: setup waits until commits go through again (two setups failed
    // with 1021 right after a restart, once in the clear below, once in
    // group creation, which is not idempotent and so is not retried). The
    // clear is idempotent: an unknown outcome is retried.
    settle(&kv).await;
    let mut cleared = false;
    for _ in 0..5 {
        match kv
            .clear_range_blocking(&arca_lake_metadata::keys::all_range())
            .await
        {
            Ok(()) => {
                cleared = true;
                break;
            }
            Err(e) => {
                eprintln!("setup clear: {e}; settling again");
                settle(&kv).await;
            }
        }
    }
    assert!(cleared, "the setup clear never committed");
    settle(&kv).await;
    let mut c = TestCluster::start_with(
        Arc::new(kv),
        ClusterOptions {
            groups: 1,
            ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
        },
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();

    // 1. Baseline.
    let mut committed: Vec<(String, Bytes)> = Vec::new();
    let base = payload(300 << 10, 1);
    s3::put(
        &c.backend,
        BUCKET,
        "base",
        base.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    committed.push(("base".into(), base));

    // 2. One process dies during concurrent PUTs: the cluster (double
    //    redundancy) stays available; every PUT that reported success is
    //    readable afterwards, every failure is explicit.
    let writers: Vec<_> = (0..6u64)
        .map(|i| {
            let backend = c.backend.clone();
            tokio::spawn(async move {
                let data = payload(200 << 10, 10 + i);
                let key = format!("during-kill-{i}");
                let r = s3::put(&backend, BUCKET, &key, data.clone(), &PutOptions::default()).await;
                (key, data, r.map(|_| ()))
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(50)).await;
    chaos.run("kill 2").await;
    let mut failed = 0;
    for w in writers {
        let (key, data, r) = w.await.unwrap();
        match r {
            Ok(()) => committed.push((key, data)),
            Err(e) => {
                failed += 1;
                eprintln!("put {key} failed explicitly during process loss: {e}");
            }
        }
    }
    eprintln!(
        "process loss: {} committed, {failed} failed",
        committed.len() - 1
    );
    wait_for_store(&c).await;
    for (key, data) in &committed {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    // A PUT with one process down succeeds (redundancy double).
    let d2 = payload(200 << 10, 20);
    s3::put(
        &c.backend,
        BUCKET,
        "one-down",
        d2.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    committed.push(("one-down".into(), d2));
    chaos.run("start 2").await;
    wait_for_store(&c).await;

    // 3. The original coordinator dies: two of three coordinators remain.
    chaos.run("kill 1").await;
    wait_for_store(&c).await;
    let d3 = payload(200 << 10, 30);
    s3::put(
        &c.backend,
        BUCKET,
        "coordinator-down",
        d3.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    committed.push(("coordinator-down".into(), d3));

    // 4. A second coordinator dies: quorum lost. Operations fail explicitly
    //    within the transaction timeout; nothing hangs, nothing is lost.
    chaos.run("kill 3").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let started = Instant::now();
    let err = s3::put(
        &c.backend,
        BUCKET,
        "no-quorum",
        payload(64 << 10, 40),
        &PutOptions::default(),
    )
    .await
    .expect_err("PUT without quorum must fail");
    eprintln!("no quorum: PUT failed after {:?}: {err}", started.elapsed());
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "failed fast, not hung"
    );
    let started = Instant::now();
    assert!(
        s3::get(&c.backend, BUCKET, "base").await.is_err(),
        "GET without quorum fails"
    );
    assert!(started.elapsed() < Duration::from_secs(60));
    chaos.run("start 1").await;
    chaos.run("start 3").await;
    wait_for_store(&c).await;
    assert!(
        s3::head(&c.backend, BUCKET, "no-quorum").await.is_err(),
        "the failed PUT did not become visible after recovery"
    );
    for (key, data) in &committed {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    let report = c.gc(now_ms() + 3_600_000, 0).await;
    assert_eq!(report.skipped_referenced, 0);
    // What failed requests above left (a pending generation expired by the
    // pass above and collected by the next; shards without a generation,
    // reclaimed by reconciliation) goes before the baseline is taken: the
    // multipart step's count below covers multipart shards only.
    c.gc(now_ms() + 3_600_000, 0).await;
    c.reconcile(now_ms() + 3_600_000, 0).await;
    for (key, data) in &committed {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }

    // 4b. Multipart during process and coordinator loss (RFC-007 A10).
    //     Every outcome is explicit; a completed object is never partially
    //     visible; referenced parts survive GC; nothing leaks.
    let shards_before_multipart = c.total_shards().await;
    let part = |n: u64| payload(300 << 10, 100 + n);
    // (a) UploadPart under process loss, then Complete.
    let u1 = create_upload(&c.backend, "mp-1").await;
    let uploads: Vec<_> = (1..=2i32)
        .map(|n| {
            let backend = c.backend.clone();
            let u1 = u1.clone();
            tokio::spawn(async move {
                (
                    n,
                    upload_part(&backend, "mp-1", &u1, n, part(u64::try_from(n).unwrap())).await,
                )
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(30)).await;
    chaos.run("kill 2").await;
    let mut etags: Vec<(i32, String)> = Vec::new();
    let mut retry = Vec::new();
    for u in uploads {
        let (n, r) = u.await.unwrap();
        match r {
            Ok(e) => etags.push((n, e)),
            Err(e) => {
                eprintln!("upload part {n} failed explicitly during process loss: {e}");
                retry.push(n);
            }
        }
    }
    chaos.run("start 2").await;
    wait_for_store(&c).await;
    for n in retry {
        etags.push((
            n,
            upload_part(&c.backend, "mp-1", &u1, n, part(u64::try_from(n).unwrap()))
                .await
                .unwrap(),
        ));
    }
    etags.push((
        3,
        upload_part(&c.backend, "mp-1", &u1, 3, part(3))
            .await
            .unwrap(),
    ));
    etags.sort_by_key(|(n, _)| *n);
    complete(&c.backend, "mp-1", &u1, &etags).await.unwrap();
    let mp1: Bytes = Bytes::from([part(1), part(2), part(3)].concat());
    assert_eq!(s3::get(&c.backend, BUCKET, "mp-1").await.unwrap().0, mp1);
    committed.push(("mp-1".into(), mp1));

    // (b) Concurrent Complete and Abort racing while a coordinator dies:
    //     at most one wins; afterwards the object is whole or absent.
    let u2 = create_upload(&c.backend, "mp-2").await;
    let e1 = upload_part(&c.backend, "mp-2", &u2, 1, part(21))
        .await
        .unwrap();
    let e2 = upload_part(&c.backend, "mp-2", &u2, 2, part(22))
        .await
        .unwrap();
    let parts2 = vec![(1, e1), (2, e2)];
    let (bc, bu, bp) = (c.backend.clone(), u2.clone(), parts2.clone());
    let completing = tokio::spawn(async move { complete(&bc, "mp-2", &bu, &bp).await.map(|_| ()) });
    let (bc, bu) = (c.backend.clone(), u2.clone());
    let aborting = tokio::spawn(async move { abort(&bc, "mp-2", &bu).await });
    chaos.run("kill 1").await;
    let (rc, ra) = (completing.await.unwrap(), aborting.await.unwrap());
    eprintln!("complete/abort race under coordinator loss: complete {rc:?}, abort {ra:?}");
    assert!(!(rc.is_ok() && ra.is_ok()), "both cannot win");
    wait_for_store(&c).await;
    let mp2: Bytes = Bytes::from([part(21), part(22)].concat());
    if let Ok((size, _)) = s3::head(&c.backend, BUCKET, "mp-2").await {
        assert!(
            ra.is_err(),
            "the abort cannot have won if the object exists"
        );
        assert_eq!(size, mp2.len() as u64, "whole or nothing");
        assert_eq!(s3::get(&c.backend, BUCKET, "mp-2").await.unwrap().0, mp2);
        committed.push(("mp-2".into(), mp2));
    } else {
        assert!(
            rc.is_err(),
            "the complete cannot have won if the object is absent"
        );
        // An abort answered with an error may still have been applied
        // (`Aborting`, no longer listed as open) or not (still open): like
        // any S3 client, retry it until it is decided. Before the gateway
        // re-ran an abort with an unknown outcome, this case left the parts
        // committed until the controller sweep (R-71).
        if ra.is_err() {
            wait_for_store(&c).await;
            let mut decided = false;
            for attempt in 0..5 {
                match abort(&c.backend, "mp-2", &u2).await {
                    Ok(()) => {
                        decided = true;
                        break;
                    }
                    Err(e) if e.code() == &s3s::S3ErrorCode::NoSuchUpload => {
                        decided = true;
                        break;
                    }
                    Err(e) => {
                        eprintln!("abort retry {attempt}: {e}");
                        wait_for_store(&c).await;
                    }
                }
            }
            assert!(decided, "the abort is decided within five retries");
        }
    }
    chaos.run("start 1").await;
    wait_for_store(&c).await;

    // (c) Two concurrent Completes while a process dies (a commit whose
    //     outcome is unknown to one caller): exactly one object version
    //     results and both callers see success with the same ETag or an
    //     explicit error.
    let u3 = create_upload(&c.backend, "mp-3").await;
    let e1 = upload_part(&c.backend, "mp-3", &u3, 1, part(31))
        .await
        .unwrap();
    let e2 = upload_part(&c.backend, "mp-3", &u3, 2, part(32))
        .await
        .unwrap();
    let parts3 = vec![(1, e1), (2, e2)];
    let racers: Vec<_> = (0..2)
        .map(|_| {
            let (bc, bu, bp) = (c.backend.clone(), u3.clone(), parts3.clone());
            tokio::spawn(async move { complete(&bc, "mp-3", &bu, &bp).await.map(|o| o.e_tag) })
        })
        .collect();
    chaos.run("kill 3").await;
    let mut etags3 = Vec::new();
    for r in racers {
        match r.await.unwrap() {
            Ok(e) => etags3.push(e),
            Err(e) => eprintln!("complete failed explicitly during process loss: {e}"),
        }
    }
    chaos.run("start 3").await;
    wait_for_store(&c).await;
    let mp3: Bytes = Bytes::from([part(31), part(32)].concat());
    if etags3.is_empty() {
        // Both callers failed: the object is whole or absent; if absent and
        // the upload is still open, complete it now.
        match s3::head(&c.backend, BUCKET, "mp-3").await {
            Ok((size, _)) => assert_eq!(size, mp3.len() as u64),
            Err(_) => {
                complete(&c.backend, "mp-3", &u3, &parts3).await.unwrap();
            }
        }
    } else {
        assert!(
            etags3.windows(2).all(|w| w[0] == w[1]),
            "one object, one ETag: {etags3:?}"
        );
    }
    assert_eq!(s3::get(&c.backend, BUCKET, "mp-3").await.unwrap().0, mp3);
    assert!(!open_uploads(&c.backend).await.contains(&u3));
    committed.push(("mp-3".into(), mp3));

    // (d) Orphan GC after the races: a GC pass and the multipart sweep keep
    //     every referenced part (I-MP1) and remove nothing else; the
    //     manifests verify clean.
    c.gc(now_ms() + 3_600_000, 0).await;
    c.gc(now_ms() + 3_600_000, 0).await;
    arca_lake_controller::gc::sweep_multipart(
        c.meta.as_ref(),
        now_ms() + 3_600_000,
        Some(now_ms() + 3_600_000),
        100,
    )
    .await
    .unwrap();
    c.reconcile(now_ms() + 3_600_000, 0).await;
    let (roots, findings) = arca_lake_controller::integrity::verify_manifests(c.meta.as_ref())
        .await
        .unwrap();
    assert_eq!(findings.len(), 0, "{findings:?}");
    let mp_objects = committed
        .iter()
        .filter(|(k, _)| k.starts_with("mp-"))
        .count();
    assert_eq!(usize::try_from(roots).unwrap(), mp_objects);
    let chunks: usize = committed
        .iter()
        .filter(|(k, _)| k.starts_with("mp-"))
        .map(|(_, d)| d.len().div_ceil(300 << 10))
        .sum();
    let total = c.total_shards().await;
    if total != shards_before_multipart + chunks * 3 {
        // Name what is left: every shard whose generation no committed
        // object references.
        let mut live = std::collections::HashSet::new();
        for (key, _) in &committed {
            let m = c.manifest(BUCKET, key).await;
            for (_, ch) in &m.chunks {
                live.insert(ch.chunk_id);
            }
            for p in &m.parts {
                if let Some((chunks, _)) = arca_lake_metadata::ops::object::read_part_manifest(
                    c.meta.as_ref(),
                    &p.generation,
                )
                .await
                .unwrap()
                {
                    for (_, ch) in chunks {
                        live.insert(ch.chunk_id);
                    }
                }
            }
        }
        for g in c.groups().await.iter().filter(|g| !g.is_cache()) {
            for i in 0..c.nodes.len() {
                if !c.node_running(i) {
                    continue;
                }
                for k in c.shard_keys(i, g.id) {
                    if live.contains(&k.chunk_id) {
                        continue;
                    }
                    let (prefix, index) = arca_lake_common::split_chunk_id(&k.chunk_id);
                    let a = arca_lake_metadata::ops::gc::attribute_shard(
                        c.meta.as_ref(),
                        &prefix,
                        index,
                        g.id,
                    )
                    .await
                    .unwrap();
                    eprintln!(
                        "leftover node {i} group {} chunk {:02x?} index {} shard {}: {a:?}",
                        g.id, k.chunk_id, index, k.shard_index
                    );
                }
            }
        }
        let gc_queue = arca_lake_metadata::transact(c.meta.as_ref(), (), |tx, ()| {
            Box::pin(async move {
                Ok(tx
                    .get_range(
                        &arca_lake_metadata::keys::retired_until_range(u64::MAX),
                        1000,
                        false,
                    )
                    .await?
                    .len())
            })
        })
        .await
        .unwrap();
        eprintln!(
            "retired queue entries: {gc_queue}; open uploads: {:?}",
            open_uploads(&c.backend).await
        );
    }
    assert_eq!(total, shards_before_multipart + chunks * 3);
    for (key, data) in committed.iter().filter(|(k, _)| k.starts_with("mp-")) {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }

    // 5. Controller leader failover on the real store.
    let mut cfg = ControllerConfig::new("f1");
    cfg.lease = Duration::from_millis(1_500);
    cfg.gc_interval = Duration::from_millis(500);
    cfg.repair_interval = Duration::from_millis(500);
    cfg.health_interval = Duration::from_millis(500);
    cfg.gc_grace = Duration::ZERO;
    let mut cfg2 = cfg.clone();
    cfg2.id = "f2".into();
    // FoundationDB was just restarting processes: start from a store that
    // commits again, and allow a failover the time of a slow recovery
    // (leases are renewed through it).
    wait_for_store(&c).await;
    let ctl_a = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let ctl_b = spawn(Arc::clone(&c.meta), cfg2).await.unwrap();
    wait_until("leader", Duration::from_secs(20), async || {
        ctl_a.is_leader() || ctl_b.is_leader()
    })
    .await;
    let (leader, follower) = if ctl_a.is_leader() {
        (ctl_a, ctl_b)
    } else {
        (ctl_b, ctl_a)
    };
    leader.abort();
    wait_until("failover", Duration::from_secs(60), async || {
        follower.is_leader()
    })
    .await;
    assert_eq!(follower.token(), 2);
    follower.shutdown().await;

    // 6. Repair while a FoundationDB process dies and comes back: the repair
    //    either completes or stops with an error, and a re-run completes.
    let group = c.groups().await.remove(0);
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;
    let outcome = c
        .repair_with(group.id, 0, new_node, async |step| {
            if let RepairStep::Chunk { index: 0, .. } = step {
                chaos.run("kill 2").await;
            }
            Ok(())
        })
        .await;
    match &outcome {
        Ok(r) => eprintln!("repair during process loss completed: {r:?}"),
        Err(e) => eprintln!("repair during process loss stopped: {e}"),
    }
    chaos.run("start 2").await;
    wait_for_store(&c).await;
    // The re-run completes. FoundationDB may still answer a commit with an
    // unknown outcome while it finishes recovering: like the controller's
    // next pass, retry then (the repair resumes from its cursor); any other
    // error fails the test.
    let mut done = None;
    for attempt in 0..5 {
        match c.repair_with(group.id, 0, new_node, async |_| Ok(())).await {
            Ok(r) => {
                done = Some(r);
                break;
            }
            Err(ControlError::Meta(arca_lake_metadata::MetaError::Kv(
                arca_lake_metadata::KvError::CommitUnknown(_)
                | arca_lake_metadata::KvError::Backend(_),
            ))) => {
                eprintln!("repair re-run {attempt}: transient metadata error, retrying");
                wait_for_store(&c).await;
            }
            Err(e) => panic!("repair re-run failed: {e}"),
        }
    }
    let done = done.expect("repair re-run completes within five attempts");
    assert!(done.complete, "{done:?}");
    assert_eq!(c.groups().await[0].state, GroupState::Active);

    // 7. GC while a process dies: the pass defers or errors, the next pass
    //    collects; totals stay consistent.
    for (key, _) in committed.iter().take(3) {
        s3::delete(&c.backend, BUCKET, key).await.unwrap();
    }
    chaos.run("kill 3").await;
    let gc_result = arca_lake_controller::gc::run_once(
        c.meta.as_ref(),
        &c.clients,
        now_ms() + 3_600_000,
        0,
        100,
    )
    .await;
    match &gc_result {
        Ok(r) => eprintln!("gc during process loss: {r:?}"),
        Err(e) => {
            assert!(matches!(e, ControlError::Meta(_)), "{e}");
            eprintln!("gc during process loss stopped: {e}");
        }
    }
    chaos.run("start 3").await;
    wait_for_store(&c).await;
    let report = c.gc(now_ms() + 3_600_000, 0).await;
    assert_eq!(report.skipped_referenced, 0);
    for (key, data) in committed.iter().skip(3) {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    for (key, _) in committed.iter().take(3) {
        assert!(s3::head(&c.backend, BUCKET, key).await.is_err());
    }
    let other = c.node_index(&group.members[1]).unwrap();
    c.stop_node(other).await;
    for (key, data) in committed.iter().skip(3) {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    c.shutdown().await;
}

/// Cluster recovery under FoundationDB process and coordinator failures
/// (RFC-015 §5.3–§5.6, Phase 3.3 O5/O6/O8/O9): verification survives a
/// lost process, a lost quorum and a controller leader failover and
/// resumes from its cursor without losing or doubling a page; nothing is
/// deleted while the cluster is not Active; a process of the previous
/// incarnation is fenced in metadata and on the nodes throughout.
#[tokio::test]
async fn recovery_survives_foundationdb_failures_and_fences_the_old_incarnation() {
    if std::env::var_os("ARCA_FDB_TESTS").is_none()
        || std::env::var_os("ARCA_FDB_FAILOVER").is_none()
    {
        eprintln!("ARCA_FDB_FAILOVER not set; skipping");
        return;
    }
    tokio::time::timeout(Duration::from_mins(15), recovery_scenario())
        .await
        .expect("the recovery scenario exceeded 15 minutes");
}

/// Live shards on every node, asked of the nodes alone (works while the
/// metadata store has no quorum).
async fn node_shards(c: &TestCluster<FdbKv>) -> u64 {
    let mut n = 0;
    for node in &c.nodes {
        n += c
            .clients
            .health(&node.addr.to_string())
            .await
            .unwrap()
            .live_shards;
    }
    n
}

async fn recovery_scenario() {
    use arca_lake_controller::recovery::RecoveryOptions;
    use arca_lake_metadata::guard::{GuardedKv, OpClass};
    use arca_lake_metadata::ops::{compat, recovery};
    use arca_lake_metadata::records::ClusterPhase;
    use arca_lake_metadata::{KvError, KvStore};
    use arca_lake_protocol::client::{NodeClients, ShardRef};
    use arca_lake_storage_node::ShardKey;

    let Some(chaos) = Chaos::from_env() else {
        eprintln!("no chaos driver configured; skipping");
        return;
    };
    let path = std::env::var("ARCA_FDB_CLUSTER_FILE").ok();
    let kv = FdbKv::open_with_timeout(path.as_deref(), 5_000).expect("open FoundationDB");
    // An earlier scenario's chaos may have restarted processes moments
    // ago: setup waits until commits go through again (two setups failed
    // with 1021 right after a restart, once in the clear below, once in
    // group creation, which is not idempotent and so is not retried). The
    // clear is idempotent: an unknown outcome is retried.
    settle(&kv).await;
    let mut cleared = false;
    for _ in 0..5 {
        match kv
            .clear_range_blocking(&arca_lake_metadata::keys::all_range())
            .await
        {
            Ok(()) => {
                cleared = true;
                break;
            }
            Err(e) => {
                eprintln!("setup clear: {e}; settling again");
                settle(&kv).await;
            }
        }
    }
    assert!(cleared, "the setup clear never committed");
    settle(&kv).await;
    let mut c = TestCluster::start_with(
        Arc::new(kv),
        ClusterOptions {
            groups: 1,
            ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
        },
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut objects = Vec::new();
    for i in 0..12u64 {
        let d = payload(512 << 10, 200 + i);
        s3::put(
            &c.backend,
            BUCKET,
            &format!("r{i}"),
            d.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        objects.push((format!("r{i}"), d));
    }
    let group = c.groups().await.remove(0);
    let chunk_of =
        async |c: &TestCluster<FdbKv>, k: &str| c.manifest(BUCKET, k).await.chunks[0].1.clone();
    let shard = |ch: &arca_lake_metadata::records::ChunkRecord, i: u16| ShardKey {
        group_id: group.id,
        chunk_id: ch.chunk_id,
        shard_index: i,
    };
    // The divergence a restore leaves: r0 lost every shard (unreadable),
    // r1 one (degraded), and an orphan written after the backup point.
    let r0 = chunk_of(&c, "r0").await;
    let r1 = chunk_of(&c, "r1").await;
    for i in 0..3u16 {
        let n = c.node_index(&group.members[usize::from(i)]).unwrap();
        c.node_store(n).delete(shard(&r0, i)).unwrap();
    }
    let n2 = c.node_index(&group.members[2]).unwrap();
    c.node_store(n2).delete(shard(&r1, 2)).unwrap();
    let backup_point = now_ms();
    let mut orphan = [0u8; 16];
    orphan[0..6].copy_from_slice(&(backup_point + 60_000).to_be_bytes()[2..8]);
    orphan[6] = 0x70;
    orphan[8] = 0x80;
    let n0 = c.node_index(&group.members[0]).unwrap();
    c.node_store(n0)
        .put(
            ShardKey {
                group_id: group.id,
                chunk_id: orphan,
                shard_index: 0,
            },
            &[9u8; 4096],
        )
        .unwrap();
    let shards_before = node_shards(&c).await;

    // The previous incarnation's processes: a gateway backend, guarded
    // stores of every class, and node clients stamping it on deletes.
    let old = compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap();
    for n in &c.nodes {
        c.clients
            .health_with(
                &n.addr.to_string(),
                arca_lake_protocol::storage::v1::HealthRequest {
                    format_write: 0,
                    incarnation: Bytes::copy_from_slice(&old.incarnation),
                    incarnation_changed_ms: old.changed_ms,
                },
            )
            .await
            .unwrap();
    }
    let stale_clients = NodeClients::new(Duration::from_secs(5));
    stale_clients.set_incarnation(Some(old.incarnation));
    let stale: Vec<GuardedKv<FdbKv>> = [OpClass::ClientWrite, OpClass::Delete, OpClass::Repair]
        .into_iter()
        .map(|cls| GuardedKv::new(Arc::clone(&c.meta), old.incarnation, cls))
        .collect();
    let probe_stale = async || {
        for s in &stale {
            let mut tx = s.begin().await.unwrap();
            tx.set(b"al-probe-stale", b"1");
            match tx.commit().await {
                Err(KvError::ClusterState(m)) => assert!(m.contains("incarnation"), "{m}"),
                other => panic!("a stale-incarnation write was not fenced: {other:?}"),
            }
        }
    };

    let marked = compat::mark_restoring(
        c.meta.as_ref(),
        Some("failover-drill".into()),
        Some(backup_point),
        "admin",
        now_ms(),
    )
    .await
    .unwrap();
    probe_stale().await;

    // Two controllers on the new incarnation; one page per pass so that
    // the failures land in the middle of the verification. Deleting
    // passes run often with no grace: they must all be skipped.
    let ctl = |id: &str| {
        let mut cfg = ControllerConfig::new(id);
        cfg.lease = Duration::from_secs(3);
        cfg.gc_interval = Duration::from_millis(300);
        cfg.reconcile_interval = Duration::from_millis(300);
        cfg.min_orphan_age = Duration::ZERO;
        cfg.gc_grace = Duration::ZERO;
        cfg.repair_interval = Duration::from_millis(300);
        cfg.stripe_repair_interval = Duration::from_millis(300);
        cfg.scrub.interval_ms = 0;
        cfg.health_interval = Duration::from_millis(300);
        cfg.recovery_interval = Duration::from_millis(100);
        cfg.recovery = RecoveryOptions {
            page: 2,
            page_bytes: 0,
            pages_per_pass: 1,
        };
        cfg
    };
    let a = spawn(Arc::clone(&c.meta), ctl("rec-a")).await.unwrap();
    let b = spawn(Arc::clone(&c.meta), ctl("rec-b")).await.unwrap();
    let meta = Arc::clone(&c.meta);
    let phase = async || {
        compat::cluster_state(meta.as_ref())
            .await
            .ok()
            .flatten()
            .map(|s| s.state)
    };
    wait_until(
        "verification to start",
        Duration::from_secs(60),
        async || phase().await == Some(ClusterPhase::VerificationPending),
    )
    .await;

    // One process lost (the cluster tolerates it), then the quorum.
    chaos.run("kill 2").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(node_shards(&c).await >= shards_before, "nothing deleted");
    chaos.run("start 2").await;
    chaos.run("kill 1").await;
    chaos.run("kill 3").await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(node_shards(&c).await >= shards_before, "nothing deleted");
    chaos.run("start 1").await;
    chaos.run("start 3").await;
    wait_for_store(&c).await;
    probe_stale().await;
    // The leader goes away mid-verification.
    let (gone, kept) = if a.is_leader() { (a, b) } else { (b, a) };
    gone.shutdown().await;
    wait_until(
        "verification to finish",
        Duration::from_mins(5),
        async || {
            matches!(
                phase().await,
                Some(ClusterPhase::ReadOnly | ClusterPhase::RecoveryRequired)
            )
        },
    )
    .await;
    assert_eq!(phase().await, Some(ClusterPhase::RecoveryRequired));
    let rep = recovery::report(c.meta.as_ref(), &marked.incarnation)
        .await
        .unwrap()
        .unwrap();
    eprintln!("report {:?} affected {:?}", rep.counts, rep.affected);
    let k = &rep.counts;
    assert!(rep.complete, "{rep:?}");
    // 12 objects × 3 shards, minus r0's three and r1's one: every page was
    // counted exactly once across the failures and the failover.
    assert_eq!(k.matched, 12 * 3 - 4, "{k:?}");
    assert_eq!(
        (k.unreadable_chunks, k.degraded_chunks, k.missing),
        (1, 1, 4),
        "{k:?}"
    );
    assert_eq!((k.orphan_post_t, k.orphan_pre_t), (1, 0), "{k:?}");
    assert_eq!(rep.affected.len(), 1);
    assert_eq!(rep.affected[0].key, "r0");

    // RecoveryRequired admits repair: r1 is rebuilt; still nothing deleted
    // (the orphan stays until the cluster is Active).
    wait_until("r1 repaired", Duration::from_secs(120), async || {
        node_shards(&c).await == shards_before + 1
    })
    .await;
    // Stale processes stay fenced: metadata writes and node deletes.
    probe_stale().await;
    let addr0 = c.nodes[n0].addr.to_string();
    let cur = compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap();
    wait_until(
        "node 0 adopted the new incarnation",
        Duration::from_secs(30),
        async || {
            c.clients
                .health(&addr0)
                .await
                .is_ok_and(|h| h.incarnation.as_ref() == cur.incarnation)
        },
    )
    .await;
    let e = stale_clients
        .delete_shard(
            &addr0,
            ShardRef {
                group_id: group.id,
                chunk_id: orphan,
                shard_index: 0,
            },
            u64::MAX,
        )
        .await
        .unwrap_err();
    assert!(e.message().starts_with("incarnation rejected"), "{e}");
    assert!(
        c.backend.cluster().incarnation == old.incarnation,
        "the in-process gateway predates the restore"
    );
    assert!(
        s3::put(
            &c.backend,
            BUCKET,
            "stale",
            payload(1024, 1),
            &PutOptions::default()
        )
        .await
        .is_err()
    );

    // The operator accepts the loss of r0 (`recovery finalize --force`);
    // Active again: reconciliation reclaims the orphan, a restarted gateway
    // writes, every other object reads.
    let mut accepted = rep.clone();
    accepted.accepted = Some(recovery::Acceptance {
        at_ms: now_ms(),
        by: "admin".into(),
        forced: true,
    });
    recovery::set_report(c.meta.as_ref(), &accepted)
        .await
        .unwrap();
    compat::set_phase(c.meta.as_ref(), ClusterPhase::Active, "admin", now_ms())
        .await
        .unwrap();
    wait_until(
        "the orphan reclaimed",
        Duration::from_secs(120),
        async || {
            !c.node_store(n0).contains(&ShardKey {
                group_id: group.id,
                chunk_id: orphan,
                shard_index: 0,
            })
        },
    )
    .await;
    c.restart_backend().await.unwrap();
    s3::put(
        &c.backend,
        BUCKET,
        "after",
        payload(4096, 2),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    for (key, data) in objects.iter().skip(1) {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    assert!(s3::get(&c.backend, BUCKET, "r0").await.is_err());
    kept.shutdown().await;
    c.shutdown().await;
}
