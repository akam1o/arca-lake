//! Commit timeouts on the multipart write path (Phase 3.5, R-71/R-72).
//!
//! A FoundationDB transaction that times out during its commit may have
//! been applied, may never be, or may still be applied later (a timeout
//! cancels the transaction after the proxy may already hold it). Every
//! metadata commit of UploadPart and CompleteMultipartUpload is hit with
//! each of those outcomes in turn; afterwards the client retries what failed
//! and completes the upload, garbage collection and reconciliation run, and
//! the test requires the object to read back whole and the stored shards to
//! be exactly the completed object's: nothing lost, nothing leaked, and no
//! node still holding capacity for a write (Phase 3.6).

#![allow(clippy::too_many_lines, clippy::cast_possible_truncation)]

use std::sync::Arc;

use arca_lake_metadata::fault::{CommitFault, FaultKv};
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::{Txn as _, keys};
use arca_lake_testkit::s3::{self, request};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::dto::{
    CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadInput, ETag, StreamingBlob, UploadPartInput,
};
use s3s::{S3, S3Result};

const MIB: u64 = 1 << 20;
const BUCKET: &str = "timeouts";
const KEY: &str = "mp";

fn part(n: i32) -> Bytes {
    Bytes::from(random_bytes(300 << 10, 7000 + u64::from(n.unsigned_abs())))
}

fn etag_of(e: Option<&ETag>) -> String {
    match e {
        Some(ETag::Strong(s) | ETag::Weak(s)) => s.clone(),
        None => String::new(),
    }
}

async fn create_upload(s3: &impl S3) -> String {
    s3.create_multipart_upload(request(CreateMultipartUploadInput {
        bucket: BUCKET.into(),
        key: KEY.into(),
        ..Default::default()
    }))
    .await
    .unwrap()
    .output
    .upload_id
    .unwrap()
}

async fn upload_part(s3: &impl S3, upload_id: &str, n: i32) -> S3Result<String> {
    let out = s3
        .upload_part(request(UploadPartInput {
            bucket: BUCKET.into(),
            key: KEY.into(),
            upload_id: upload_id.into(),
            part_number: n,
            body: Some(StreamingBlob::from_bytes(part(n))),
            ..Default::default()
        }))
        .await?
        .output;
    Ok(etag_of(out.e_tag.as_ref()))
}

async fn complete(s3: &impl S3, upload_id: &str, parts: &[(i32, String)]) -> S3Result<()> {
    s3.complete_multipart_upload(request(CompleteMultipartUploadInput {
        bucket: BUCKET.into(),
        key: KEY.into(),
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
    .await?;
    Ok(())
}

/// Which metadata commit of the write path the fault hits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// `begin_part`: the pending generation (header and pending index).
    Begin,
    /// `append_chunks`: chunk records.
    Append,
    /// `seal_part`: the part record.
    Seal,
    /// The gateway's `abort_put` after a failure: the retired queue entry.
    Abort,
    /// `complete`: the multipart upload record.
    Complete,
}

fn is_pending_index(k: &[u8]) -> bool {
    keys::pending_expired_range(u64::MAX).contains(k)
}

fn is_retired_entry(k: &[u8]) -> bool {
    keys::retired_until_range(u64::MAX).contains(k)
}

fn matcher(target: Target) -> impl Fn(&[Vec<u8>]) -> bool + Send + Sync + 'static {
    move |written: &[Vec<u8>]| {
        let chunk = written.iter().any(|k| keys::chunk_index_from(k).is_some());
        let part = written.iter().any(|k| keys::part_number_from(k).is_some());
        let mpu = written.iter().any(|k| keys::mpu_from(k).is_some());
        match target {
            Target::Begin => written.iter().any(|k| is_pending_index(k)) && !chunk && !part,
            Target::Append => chunk,
            Target::Seal => part && !mpu,
            Target::Abort => written.iter().any(|k| is_retired_entry(k)) && !part && !mpu,
            Target::Complete => mpu,
        }
    }
}

/// When a delayed commit is finally applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Late {
    /// Right after the failed request returned (the gateway's own
    /// resolution or abort has run).
    AfterFailure,
    /// After the client's retry of the failed request.
    AfterRetry,
}

async fn scenario(target: Target, fault: CommitFault, late: Late) -> Result<(), String> {
    let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let baseline = c.total_shards().await;
    let s3 = &c.backend;
    let upload_id = create_upload(s3).await;
    // The abort only runs after a failure: make part 1 fail first.
    if target == Target::Abort {
        c.meta
            .inject(matcher(Target::Seal), CommitFault::TimeoutWithoutApply, 1);
    }
    c.meta.inject(matcher(target), fault, 1);
    let mut etags = Vec::new();
    let mut log = Vec::new();
    let first = upload_part(s3, &upload_id, 1).await;
    log.push(format!("part 1: {:?}", first.as_ref().map(|_| ())));
    let late_results = |r: Vec<Result<u64, arca_lake_metadata::KvError>>| {
        r.into_iter()
            .map(|x| x.map(|_| ()).map_err(|e| e.to_string()))
            .collect::<Vec<_>>()
    };
    if late == Late::AfterFailure {
        log.push(format!(
            "late commit: {:?}",
            late_results(c.meta.apply_delayed().await)
        ));
    }
    if let Ok(e) = first {
        etags.push((1, e));
    } else {
        let e = upload_part(s3, &upload_id, 1)
            .await
            .map_err(|e| format!("retry of part 1 failed: {e}; {log:?}"))?;
        etags.push((1, e));
    }
    if late == Late::AfterRetry {
        log.push(format!(
            "late commit: {:?}",
            late_results(c.meta.apply_delayed().await)
        ));
    }
    etags.push((
        2,
        upload_part(s3, &upload_id, 2)
            .await
            .map_err(|e| format!("part 2: {e}; {log:?}"))?,
    ));
    let done = complete(s3, &upload_id, &etags).await;
    log.push(format!("complete: {:?}", done.as_ref()));
    if done.is_err() {
        // The client retries a Complete it saw fail: the same request.
        let again = complete(s3, &upload_id, &etags).await;
        log.push(format!("complete retry: {:?}", again.as_ref()));
        if let Err(e) = again {
            // Done already (the first one was applied): the object exists.
            if s3::head(s3, BUCKET, KEY).await.is_err() {
                return Err(format!("complete failed twice: {e}; {log:?}"));
            }
        }
    }
    let late_left = c.meta.apply_delayed().await;
    if !late_left.is_empty() {
        log.push(format!(
            "late commit at the end: {:?}",
            late_results(late_left)
        ));
    }
    let whole: Bytes = Bytes::from([part(1), part(2)].concat());
    let got = s3::get(s3, BUCKET, KEY)
        .await
        .map_err(|e| format!("GET: {e}; {log:?}"))?
        .0;
    if got != whole {
        return Err(format!("object differs ({} bytes); {log:?}", got.len()));
    }
    let far = now_ms() + 3_600_000;
    c.gc(far, 0).await;
    c.gc(far, 0).await;
    arca_lake_controller::gc::sweep_multipart(c.meta.as_ref(), far, Some(far), 100)
        .await
        .unwrap();
    c.gc(far, 0).await;
    c.reconcile(far, 0).await;
    let total = c.total_shards().await;
    // Two parts of one chunk each, three shards per chunk.
    if total != baseline + 2 * 3 {
        let mut left = Vec::new();
        let live: std::collections::HashSet<[u8; 16]> = c
            .manifest(BUCKET, KEY)
            .await
            .parts
            .iter()
            .map(|p| p.generation)
            .collect();
        for g in c.groups().await.iter().filter(|g| !g.is_cache()) {
            for i in 0..c.nodes.len() {
                for k in c.node_store(i).keys_in_group(g.id) {
                    let (prefix, index) = arca_lake_common::split_chunk_id(&k.chunk_id);
                    let a = arca_lake_metadata::ops::gc::attribute_shard(
                        c.meta.as_ref(),
                        &prefix,
                        index,
                        g.id,
                    )
                    .await
                    .unwrap();
                    let mine = live.iter().any(|g| g[..12] == prefix);
                    if !mine {
                        left.push(format!("node {i} group {} {a:?}", g.id));
                    }
                }
            }
        }
        return Err(format!(
            "{total} shards, expected {}: leftovers {left:?}; {log:?}",
            baseline + 6
        ));
    }
    // Capacity holds (Phase 3.6) involve no metadata: whatever the commit
    // outcomes, none is left on any node once the writes have ended.
    for i in 0..c.nodes.len() {
        let d = c.node_service(i).device_capacity().unwrap();
        if d.inflight_bytes != 0 {
            return Err(format!(
                "node {i} still holds {} bytes; {log:?}",
                d.inflight_bytes
            ));
        }
    }
    c.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_commit_timeout_on_the_multipart_path_neither_loses_nor_leaks() {
    let mut failures = Vec::new();
    for target in [
        Target::Begin,
        Target::Append,
        Target::Seal,
        Target::Abort,
        Target::Complete,
    ] {
        for (fault, late) in [
            (CommitFault::TimeoutAfterApply, Late::AfterFailure),
            (CommitFault::TimeoutWithoutApply, Late::AfterFailure),
            (CommitFault::TimeoutDelayed, Late::AfterFailure),
            (CommitFault::TimeoutDelayed, Late::AfterRetry),
        ] {
            if let Err(e) = scenario(target, fault, late).await {
                failures.push(format!("{target:?} {fault:?} {late:?}: {e}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

async fn abort(s3: &impl S3, upload_id: &str) -> S3Result<()> {
    s3.abort_multipart_upload(request(s3s::dto::AbortMultipartUploadInput {
        bucket: BUCKET.into(),
        key: KEY.into(),
        upload_id: upload_id.into(),
        ..Default::default()
    }))
    .await?;
    Ok(())
}

async fn retired_queue(c: &TestCluster<FaultKv<MemKv>>) -> usize {
    arca_lake_metadata::transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            Ok(tx
                .get_range(&keys::retired_until_range(u64::MAX), 10_000, false)
                .await?
                .len())
        })
    })
    .await
    .unwrap()
}

/// R-71, as seen in `fdb_failover` (2 of 59 runs): AbortMultipartUpload's
/// first transaction (`Open → Aborting`) is applied but its commit times
/// out. The gateway answered an error without discarding the parts, a
/// racing Complete answered `NoSuchUpload`, and the upload no longer listed
/// as open: the parts stayed committed until the controller's multipart
/// sweep retired them, after the GC passes the failover test runs first. The
/// abort is idempotent (`Aborting` is accepted again), so the gateway now
/// re-runs it on an unknown outcome and discards the parts itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abort_whose_commit_times_out_after_apply_discards_its_parts() {
    for fault in [
        CommitFault::TimeoutAfterApply,
        CommitFault::TimeoutWithoutApply,
        CommitFault::TimeoutDelayed,
    ] {
        let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
            Arc::new(FaultKv::new(MemKv::default())),
            ClusterOptions::rs(3, 2, 1, MIB, 0),
        )
        .await;
        s3::create_bucket(&c.backend, BUCKET).await.unwrap();
        let baseline = c.total_shards().await;
        let s3 = &c.backend;
        let upload_id = create_upload(s3).await;
        let e1 = upload_part(s3, &upload_id, 1).await.unwrap();
        let e2 = upload_part(s3, &upload_id, 2).await.unwrap();
        // Abort's first transaction writes only the upload record.
        c.meta.inject(
            |w: &[Vec<u8>]| w.len() == 1 && keys::mpu_from(&w[0]).is_some(),
            fault,
            1,
        );
        let r = abort(s3, &upload_id).await;
        // A late commit lands now, after the gateway answered.
        let late = c.meta.apply_delayed().await;
        assert!(
            r.is_ok(),
            "{fault:?}: an abort that can be decided is not reported as failed: {r:?} (late {late:?})"
        );
        assert!(
            complete(s3, &upload_id, &[(1, e1), (2, e2)]).await.is_err(),
            "{fault:?}: completed after the abort succeeded"
        );
        // The failover test's order: GC first, then the sweep.
        let far = now_ms() + 3_600_000;
        c.gc(far, 0).await;
        c.gc(far, 0).await;
        arca_lake_controller::gc::sweep_multipart(c.meta.as_ref(), far, Some(far), 100)
            .await
            .unwrap();
        c.reconcile(far, 0).await;
        let total = c.total_shards().await;
        let queued = retired_queue(&c).await;
        assert_eq!(
            (total, queued),
            (baseline, 0),
            "{fault:?}: parts of the aborted upload left behind"
        );
        assert!(s3::head(s3, BUCKET, KEY).await.is_err());
        c.shutdown().await;
    }
}

/// DELETE whose commit times out after it was applied (R-72): a
/// conditional delete is not retried into a wrong 412/404 (the outcome is
/// reported as unknown, 503, or decided as success); a delete by version is
/// re-run and answers success. A delayed commit landing after the
/// gateway's own decision is rejected by the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_whose_commit_times_out_are_never_reported_as_failed() {
    use s3s::dto::{
        BucketVersioningStatus, DeleteObjectInput, PutBucketVersioningInput,
        VersioningConfiguration,
    };
    for fault in [
        CommitFault::TimeoutAfterApply,
        CommitFault::TimeoutWithoutApply,
        CommitFault::TimeoutDelayed,
    ] {
        let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
            Arc::new(FaultKv::new(MemKv::default())),
            ClusterOptions::rs(3, 2, 1, MIB, 0),
        )
        .await;
        let s3 = &c.backend;
        s3::create_bucket(s3, BUCKET).await.unwrap();
        let b = arca_lake_metadata::ops::bucket::get_bucket(c.meta.as_ref(), BUCKET)
            .await
            .unwrap();
        let current = keys::object_current(b.id, KEY);
        let writes_current = move |w: &[Vec<u8>]| w.contains(&current);

        // Conditional delete.
        let etag = s3::put(s3, BUCKET, KEY, part(1), &s3::PutOptions::default())
            .await
            .unwrap();
        c.meta.inject(writes_current.clone(), fault, 1);
        let r = s3
            .delete_object(request(DeleteObjectInput {
                bucket: BUCKET.into(),
                key: KEY.into(),
                if_match: Some(s3s::dto::ETagCondition::ETag(ETag::Strong(
                    etag.trim_matches('"').to_string(),
                ))),
                ..Default::default()
            }))
            .await;
        let late = c.meta.apply_delayed().await;
        let gone = s3::head(s3, BUCKET, KEY).await.is_err();
        match &r {
            Ok(_) => assert!(gone, "{fault:?}: success reported, object still there"),
            Err(e) => assert_eq!(
                e.code(),
                &s3s::S3ErrorCode::ServiceUnavailable,
                "{fault:?}: a conditional delete with an unknown outcome answered {e:?} (late {late:?})"
            ),
        }
        if !gone {
            // Unknown and not applied: the client's retry decides it.
            s3::delete(s3, BUCKET, KEY).await.unwrap();
        }

        // Delete by version on a versioned bucket.
        let mut vb = PutBucketVersioningInput::builder();
        vb.set_bucket(BUCKET.into());
        vb.set_versioning_configuration(VersioningConfiguration {
            status: Some(BucketVersioningStatus::from_static(
                BucketVersioningStatus::ENABLED,
            )),
            ..Default::default()
        });
        s3.put_bucket_versioning(request(vb.build().unwrap()))
            .await
            .unwrap();
        s3::put(s3, BUCKET, KEY, part(2), &s3::PutOptions::default())
            .await
            .unwrap();
        let v = c.manifest(BUCKET, KEY).await.pointer.version_id;
        c.meta.inject(writes_current.clone(), fault, 1);
        let r = s3
            .delete_object(request(DeleteObjectInput {
                bucket: BUCKET.into(),
                key: KEY.into(),
                version_id: Some(v.clone()),
                ..Default::default()
            }))
            .await;
        let late = c.meta.apply_delayed().await;
        assert!(
            r.is_ok(),
            "{fault:?}: delete by version answered {r:?} (late {late:?})"
        );
        assert!(
            s3::head(s3, BUCKET, KEY).await.is_err(),
            "{fault:?}: version still current"
        );
        let far = now_ms() + 3_600_000;
        c.gc(far, 0).await;
        c.gc(far, 0).await;
        c.reconcile(far, 0).await;
        assert_eq!(
            c.total_shards().await,
            0,
            "{fault:?}: deleted objects' shards left"
        );
        c.shutdown().await;
    }
}

/// DELETE without a version id whose commit is applied but times out: the
/// gateway's re-run does not delete an object written between the lost
/// reply and the re-run (Phase 3.5 audit CON-1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_rerun_never_deletes_an_object_written_since() {
    use arca_lake_metadata::ops::object::{self, CommitInfo, Precondition};
    let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    let s3 = &c.backend;
    s3::create_bucket(s3, BUCKET).await.unwrap();
    s3::put(s3, BUCKET, KEY, part(1), &s3::PutOptions::default())
        .await
        .unwrap();
    let b = arca_lake_metadata::ops::bucket::get_bucket(c.meta.as_ref(), BUCKET)
        .await
        .unwrap();
    c.meta.inject_on_key(
        keys::object_current(b.id, KEY),
        CommitFault::TimeoutAfterApply,
        1,
    );
    let meta = Arc::clone(&c.meta);
    let written = Arc::new(std::sync::Mutex::new(None));
    let w = Arc::clone(&written);
    c.meta.interleave_next(move || {
        let g = std::thread::spawn(move || {
            futures::executor::block_on(async {
                let now = now_ms();
                let p = object::begin_put(meta.as_ref(), BUCKET, KEY, now, 60_000)
                    .await
                    .unwrap();
                object::commit_put(
                    meta.as_ref(),
                    &p.generation,
                    &CommitInfo {
                        size: 0,
                        etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
                        ..Default::default()
                    },
                    &Precondition::default(),
                    now,
                )
                .await
                .unwrap()
                .generation
            })
        })
        .join()
        .unwrap();
        *w.lock().unwrap() = Some(g);
    });
    s3::delete(s3, BUCKET, KEY).await.unwrap();
    let b_gen = written.lock().unwrap().expect("the interleaved write ran");
    assert_eq!(
        object::current_generation(c.meta.as_ref(), BUCKET, KEY)
            .await
            .unwrap(),
        Some(b_gen),
        "the object written after the applied delete was deleted by the re-run"
    );
    c.shutdown().await;
}

/// On a versioning-enabled bucket an unconditional delete whose outcome
/// stays unknown while another PUT lands is not reported as a success it
/// cannot prove (Phase 3.5 audit CON-4): its marker is absent, so 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_versioned_delete_with_an_unproven_outcome_is_not_reported_done() {
    use arca_lake_metadata::ops::object::{self, CommitInfo, Precondition};
    use s3s::dto::{BucketVersioningStatus, PutBucketVersioningInput, VersioningConfiguration};
    let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    let s3 = &c.backend;
    s3::create_bucket(s3, BUCKET).await.unwrap();
    let mut vb = PutBucketVersioningInput::builder();
    vb.set_bucket(BUCKET.into());
    vb.set_versioning_configuration(VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(
            BucketVersioningStatus::ENABLED,
        )),
        ..Default::default()
    });
    s3.put_bucket_versioning(request(vb.build().unwrap()))
        .await
        .unwrap();
    s3::put(s3, BUCKET, KEY, part(1), &s3::PutOptions::default())
        .await
        .unwrap();
    let b = arca_lake_metadata::ops::bucket::get_bucket(c.meta.as_ref(), BUCKET)
        .await
        .unwrap();
    c.meta.inject_on_key(
        keys::object_current(b.id, KEY),
        CommitFault::TimeoutWithoutApply,
        1,
    );
    let meta = Arc::clone(&c.meta);
    c.meta.interleave_next(move || {
        std::thread::spawn(move || {
            futures::executor::block_on(async {
                let now = now_ms();
                let p = object::begin_put(meta.as_ref(), BUCKET, KEY, now, 60_000)
                    .await
                    .unwrap();
                object::commit_put(
                    meta.as_ref(),
                    &p.generation,
                    &CommitInfo {
                        size: 0,
                        etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
                        ..Default::default()
                    },
                    &Precondition::default(),
                    now,
                )
                .await
                .unwrap();
            });
        })
        .join()
        .unwrap();
    });
    let r = s3::delete(s3, BUCKET, KEY).await;
    let e = r.expect_err("a delete that did not happen is not reported done");
    assert_eq!(e.code(), &s3s::S3ErrorCode::ServiceUnavailable, "{e:?}");
    c.shutdown().await;
}

/// A PUT whose first commit attempt has an unknown outcome and whose
/// retry then conflicts until the transaction loop gives up is not
/// answered `SlowDown` (a definite refusal, "retry"): the first attempt may
/// still be applied, and here it is, after the fencing abort failed
/// (Phase 4.1, R-112). The answer must say the outcome is unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_put_whose_outcome_is_unknown_is_never_answered_slowdown() {
    let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    let s3 = &c.backend;
    s3::create_bucket(s3, BUCKET).await.unwrap();
    let old = part(1);
    s3::put(s3, BUCKET, KEY, old.clone(), &s3::PutOptions::default())
        .await
        .unwrap();
    let b = arca_lake_metadata::ops::bucket::get_bucket(c.meta.as_ref(), BUCKET)
        .await
        .unwrap();
    let current = keys::object_current(b.id, KEY);
    let cur = current.clone();
    let writes_current = move |w: &[Vec<u8>]| w.contains(&cur);
    // The first commit attempt is held (a timeout whose transaction may
    // still commit); every attempt of the retry conflicts.
    c.meta
        .inject(writes_current.clone(), CommitFault::TimeoutDelayed, 1);
    c.meta.inject(
        writes_current,
        CommitFault::Conflict,
        arca_lake_metadata::kv::MAX_ATTEMPTS,
    );
    // Every abort fails (store unavailable): the generation stays pending.
    c.meta.inject(
        move |w: &[Vec<u8>]| w.iter().any(|k| is_retired_entry(k)) && !w.contains(&current),
        CommitFault::Unavailable,
        8,
    );
    let new = part(2);
    let r = s3::put(s3, BUCKET, KEY, new.clone(), &s3::PutOptions::default()).await;
    c.meta.clear_rules();
    let late = c.meta.apply_delayed().await;
    let now = s3::get(s3, BUCKET, KEY).await.unwrap().0;
    match &r {
        Ok(_) => assert_eq!(now, new, "acknowledged and not visible (late {late:?})"),
        Err(e) => {
            if now == new {
                assert_ne!(
                    e.code(),
                    &s3s::S3ErrorCode::SlowDown,
                    "answered SlowDown, then the write became visible (late {late:?}): {e:?}"
                );
            }
        }
    }
    assert!(now == new || now == old, "neither version is current");
    c.shutdown().await;
}

/// The same for UploadPart: a part whose first seal attempt has an
/// unknown outcome and whose retry conflicts until the loop gives up is
/// not answered `SlowDown` if it may still be sealed (Phase 4.1, R-112).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_part_whose_seal_is_unknown_is_never_answered_slowdown() {
    use s3s::dto::ListPartsInput;
    let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    let s3 = &c.backend;
    s3::create_bucket(s3, BUCKET).await.unwrap();
    let upload_id = create_upload(s3).await;
    c.meta
        .inject(matcher(Target::Seal), CommitFault::TimeoutDelayed, 1);
    c.meta.inject(
        matcher(Target::Seal),
        CommitFault::Conflict,
        arca_lake_metadata::kv::MAX_ATTEMPTS,
    );
    c.meta
        .inject(matcher(Target::Abort), CommitFault::Unavailable, 8);
    let r = upload_part(s3, &upload_id, 1).await;
    c.meta.clear_rules();
    let late = c.meta.apply_delayed().await;
    let listed = s3
        .list_parts(request(ListPartsInput {
            bucket: BUCKET.into(),
            key: KEY.into(),
            upload_id: upload_id.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .parts
        .unwrap_or_default()
        .iter()
        .any(|p| p.part_number == Some(1));
    if let Err(e) = &r
        && listed
    {
        assert_ne!(
            e.code(),
            &s3s::S3ErrorCode::SlowDown,
            "answered SlowDown, then the part was sealed (late {late:?}): {e:?}"
        );
    }
    c.shutdown().await;
}

/// A versioned PUT whose commit was applied but answered with an unknown
/// outcome, while another PUT became current before the outcome was
/// resolved, is answered with its own version id, not `null` (Phase 4.1
/// review of R-112).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resolved_versioned_put_answers_its_own_version_id() {
    use arca_lake_metadata::ops::object::{self, CommitInfo, Precondition};
    use s3s::dto::{
        BucketVersioningStatus, PutBucketVersioningInput, PutObjectInput, VersioningConfiguration,
    };
    let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    let s3 = &c.backend;
    s3::create_bucket(s3, BUCKET).await.unwrap();
    let mut vb = PutBucketVersioningInput::builder();
    vb.set_bucket(BUCKET.into());
    vb.set_versioning_configuration(VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(
            BucketVersioningStatus::ENABLED,
        )),
        ..Default::default()
    });
    s3.put_bucket_versioning(request(vb.build().unwrap()))
        .await
        .unwrap();
    let b = arca_lake_metadata::ops::bucket::get_bucket(c.meta.as_ref(), BUCKET)
        .await
        .unwrap();
    c.meta.inject_on_key(
        keys::object_current(b.id, KEY),
        CommitFault::UnknownAfterApply,
        1,
    );
    // Between the lost reply and the resolution, another PUT lands.
    let meta = Arc::clone(&c.meta);
    c.meta.interleave_next(move || {
        std::thread::spawn(move || {
            futures::executor::block_on(async {
                let now = now_ms();
                let p = object::begin_put(meta.as_ref(), BUCKET, KEY, now, 60_000)
                    .await
                    .unwrap();
                object::commit_put(
                    meta.as_ref(),
                    &p.generation,
                    &CommitInfo {
                        size: 0,
                        etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
                        ..Default::default()
                    },
                    &Precondition::default(),
                    now,
                )
                .await
                .unwrap();
            });
        })
        .join()
        .unwrap();
    });
    let data = part(3);
    let out = s3
        .put_object(request(PutObjectInput {
            bucket: BUCKET.into(),
            key: KEY.into(),
            body: Some(StreamingBlob::from_bytes(data.clone())),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let vid = out.version_id.expect("a version id");
    let got = s3
        .get_object(request(s3s::dto::GetObjectInput {
            bucket: BUCKET.into(),
            key: KEY.into(),
            version_id: Some(vid.clone()),
            ..Default::default()
        }))
        .await
        .unwrap_or_else(|e| panic!("version {vid} answered: {e:?}"))
        .output;
    let mut body = Vec::new();
    let mut stream = got.body.expect("a body");
    while let Some(chunk) = futures::StreamExt::next(&mut stream).await {
        body.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(
        body,
        data.to_vec(),
        "the version id answered is not this PUT's"
    );
    c.shutdown().await;
}

/// A part whose seal was not applied (outcome unknown) and whose pending
/// write then expired is not reported stored: "retiring while the upload is
/// open" proves supersession only if the part had been sealed (Phase 4.1
/// review of R-112).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_unsealed_part_is_not_reported_stored() {
    use s3s::dto::ListPartsInput;
    let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    let s3 = &c.backend;
    s3::create_bucket(s3, BUCKET).await.unwrap();
    let upload_id = create_upload(s3).await;
    c.meta
        .inject(matcher(Target::Seal), CommitFault::TimeoutWithoutApply, 1);
    // Between the lost seal and its resolution, the pending part expires.
    let meta = Arc::clone(&c.meta);
    c.meta.interleave_next(move || {
        std::thread::spawn(move || {
            futures::executor::block_on(async {
                let gone = arca_lake_metadata::ops::gc::expire_pending(
                    meta.as_ref(),
                    now_ms() + 86_400_000,
                    100,
                )
                .await
                .unwrap();
                assert!(!gone.is_empty(), "nothing expired");
            });
        })
        .join()
        .unwrap();
    });
    let r = upload_part(s3, &upload_id, 1).await;
    let listed = s3
        .list_parts(request(ListPartsInput {
            bucket: BUCKET.into(),
            key: KEY.into(),
            upload_id: upload_id.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .parts
        .unwrap_or_default()
        .iter()
        .any(|p| p.part_number == Some(1));
    assert!(
        r.is_err() || listed,
        "UploadPart answered {r:?} but the part is not stored"
    );
    c.shutdown().await;
}

/// CompleteMultipartUpload and DELETE: after an attempt whose outcome was
/// unknown (held, applied later), a retry that conflicts until the loop
/// gives up is not answered `SlowDown` if the first attempt then takes
/// effect (Phase 4.1 review of R-112).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn complete_and_delete_with_unknown_outcomes_are_never_answered_slowdown() {
    let max = arca_lake_metadata::kv::MAX_ATTEMPTS;
    // Complete.
    let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    let s3 = &c.backend;
    s3::create_bucket(s3, BUCKET).await.unwrap();
    let upload_id = create_upload(s3).await;
    let etag = upload_part(s3, &upload_id, 1).await.unwrap();
    c.meta
        .inject(matcher(Target::Complete), CommitFault::TimeoutDelayed, 1);
    c.meta
        .inject(matcher(Target::Complete), CommitFault::Conflict, max);
    let r = complete(s3, &upload_id, &[(1, etag)]).await;
    c.meta.clear_rules();
    let late = c.meta.apply_delayed().await;
    let visible = s3::head(s3, BUCKET, KEY).await.is_ok();
    if let Err(e) = &r
        && visible
    {
        assert_ne!(
            e.code(),
            &s3s::S3ErrorCode::SlowDown,
            "complete answered SlowDown, then the object appeared (late {late:?})"
        );
    }
    c.shutdown().await;

    // Delete.
    let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    let s3 = &c.backend;
    s3::create_bucket(s3, BUCKET).await.unwrap();
    s3::put(s3, BUCKET, KEY, part(1), &s3::PutOptions::default())
        .await
        .unwrap();
    let b = arca_lake_metadata::ops::bucket::get_bucket(c.meta.as_ref(), BUCKET)
        .await
        .unwrap();
    let current = keys::object_current(b.id, KEY);
    let cur = current.clone();
    c.meta.inject(
        move |w: &[Vec<u8>]| w.contains(&cur),
        CommitFault::TimeoutDelayed,
        1,
    );
    c.meta.inject(
        move |w: &[Vec<u8>]| w.contains(&current),
        CommitFault::Conflict,
        max,
    );
    let r = s3::delete(s3, BUCKET, KEY).await;
    c.meta.clear_rules();
    let late = c.meta.apply_delayed().await;
    let gone = s3::head(s3, BUCKET, KEY).await.is_err();
    if let Err(e) = &r
        && gone
    {
        assert_ne!(
            e.code(),
            &s3s::S3ErrorCode::SlowDown,
            "delete answered SlowDown, then the object was gone (late {late:?})"
        );
    }
    c.shutdown().await;
}
