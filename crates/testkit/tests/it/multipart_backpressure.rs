//! Multipart under backpressure (Phase 3.7, final-build verification of
//! the Phase 3.6 Test B paths): concurrent uploads with UploadPart and
//! UploadPartCopy while the tier is throttled to its floor rate, clients
//! retrying refused parts as SDKs do; some uploads completed, some aborted;
//! then the tier refuses new data (new uploads refused, completion and
//! abort still accepted); then deletes, GC, the multipart sweep and
//! reconciliation. Required: every completed object reads back whole,
//! nothing aborted or refused is visible, exactly the live objects' shards
//! remain, and no node holds capacity. In-memory metadata always;
//! FoundationDB with `--features fdb` and `ARCA_FDB_TESTS=1`.

#![allow(clippy::too_many_lines)]

use std::time::{Duration, Instant};

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::admission::{self, AdmissionRecord, TierAdmission};
use arca_lake_testkit::s3::{self, PutOptions, request};
use arca_lake_testkit::suite::Starter;
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::dto::{
    AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletedMultipartUpload,
    CompletedPart, CopySource, CreateMultipartUploadInput, ETag, StreamingBlob,
    UploadPartCopyInput, UploadPartInput,
};
use s3s::{S3, S3ErrorCode, S3Result};

const MIB: u64 = 1 << 20;
const BUCKET: &str = "mpbp";

async fn force<S: KvStore + 'static>(c: &TestCluster<S>, state: u8) {
    force_rate(c, state, 0).await;
}

async fn force_rate<S: KvStore + 'static>(c: &TestCluster<S>, state: u8, rate: u64) {
    let rec = AdmissionRecord {
        computed_ms: now_ms(),
        ttl_ms: 3_600_000,
        gateways: 1,
        tiers: vec![TierAdmission {
            tier: "default".into(),
            state,
            rate_bytes_per_s: rate,
            reason: "test".into(),
            ..Default::default()
        }],
        leader_token: u64::MAX,
        ..Default::default()
    };
    admission::publish(c.meta.as_ref(), &rec).await.unwrap();
    let rec = admission::current(c.meta.as_ref()).await.unwrap();
    c.backend.cluster().admission.observe(rec, Instant::now());
}

fn etag(e: Option<&ETag>) -> String {
    match e {
        Some(ETag::Strong(s) | ETag::Weak(s)) => s.clone(),
        None => String::new(),
    }
}

/// Retry `op` on `SlowDown` as an SDK does (bounded).
async fn retrying<T, F, Fut>(mut op: F) -> S3Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S3Result<T>>,
{
    let mut last = None;
    for attempt in 0..80u64 {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if *e.code() == S3ErrorCode::SlowDown => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(100 + attempt * 20)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap())
}

async fn create(s3: &impl S3, key: &str) -> S3Result<String> {
    Ok(s3
        .create_multipart_upload(request(CreateMultipartUploadInput {
            bucket: BUCKET.into(),
            key: key.into(),
            ..Default::default()
        }))
        .await?
        .output
        .upload_id
        .unwrap())
}

async fn part(s3: &impl S3, key: &str, id: &str, n: i32, data: &Bytes) -> S3Result<String> {
    Ok(etag(
        s3.upload_part(request(UploadPartInput {
            bucket: BUCKET.into(),
            key: key.into(),
            upload_id: id.into(),
            part_number: n,
            body: Some(StreamingBlob::from_bytes(data.clone())),
            ..Default::default()
        }))
        .await?
        .output
        .e_tag
        .as_ref(),
    ))
}

async fn part_copy(s3: &impl S3, key: &str, id: &str, n: i32) -> S3Result<String> {
    let mut b = UploadPartCopyInput::builder();
    b.set_bucket(BUCKET.into());
    b.set_key(key.into());
    b.set_upload_id(id.into());
    b.set_part_number(n);
    b.set_copy_source(CopySource::Bucket {
        bucket: BUCKET.into(),
        key: "source".into(),
        version_id: None,
    });
    let out = s3
        .upload_part_copy(request(b.build().unwrap()))
        .await?
        .output;
    Ok(etag(out.copy_part_result.and_then(|r| r.e_tag).as_ref()))
}

async fn complete(s3: &impl S3, key: &str, id: &str, parts: &[(i32, String)]) -> S3Result<()> {
    s3.complete_multipart_upload(request(CompleteMultipartUploadInput {
        bucket: BUCKET.into(),
        key: key.into(),
        upload_id: id.into(),
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

async fn abort(s3: &impl S3, key: &str, id: &str) -> S3Result<()> {
    s3.abort_multipart_upload(request(AbortMultipartUploadInput {
        bucket: BUCKET.into(),
        key: key.into(),
        upload_id: id.into(),
        ..Default::default()
    }))
    .await?;
    Ok(())
}

/// The part data of upload `u`, part `n`.
fn data(u: usize, n: i32) -> Bytes {
    Bytes::from(random_bytes(
        usize::try_from(MIB).unwrap(),
        (u * 10) as u64 + u64::from(n.unsigned_abs()),
    ))
}

pub(crate) async fn scenario<S: KvStore + 'static>(start: impl Starter<S>) {
    let c = start(ClusterOptions {
        groups: 1,
        multipart_min_part_bytes: MIB,
        ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let source = Bytes::from(random_bytes(usize::try_from(4 * MIB).unwrap(), 5));
    s3::put(
        &c.backend,
        BUCKET,
        "source",
        source.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let baseline = c.total_shards().await;

    // Throttled to 2 MiB/s: six uploads at once, three parts
    // of 1 MiB and one copied part of 4 MiB each, parts retried on
    // SlowDown; uploads 4 and 5 are aborted, the rest completed.
    force_rate(&c, 2, 2 * MIB).await;
    let jobs = (0..6usize).map(|u| {
        let b = c.backend.clone();
        async move {
            let key = format!("mp{u}");
            let id = retrying(|| create(&b, &key)).await.unwrap();
            let mut parts = Vec::new();
            for n in 1..=3 {
                let d = data(u, n);
                parts.push((n, retrying(|| part(&b, &key, &id, n, &d)).await.unwrap()));
            }
            parts.push((4, retrying(|| part_copy(&b, &key, &id, 4)).await.unwrap()));
            if u >= 4 {
                abort(&b, &key, &id).await.unwrap();
                (key, false)
            } else {
                complete(&b, &key, &id, &parts).await.unwrap();
                (key, true)
            }
        }
    });
    let done = futures::future::join_all(jobs).await;
    let refused = c.backend.cluster().admission.snapshot().throttled;
    assert!(refused > 0, "the rate refused some parts");

    // Refusing new data: no new upload, no new part; an upload already
    // holding its parts still completes, an abort still goes through.
    force(&c, 0).await;
    let late = create(&c.backend, "late").await.unwrap();
    let late_part = part(&c.backend, "late", &late, 1, &data(9, 1))
        .await
        .unwrap();
    let doomed = create(&c.backend, "doomed").await.unwrap();
    part(&c.backend, "doomed", &doomed, 1, &data(8, 1))
        .await
        .unwrap();
    force(&c, 3).await;
    assert_eq!(
        *create(&c.backend, "refused").await.unwrap_err().code(),
        S3ErrorCode::SlowDown
    );
    assert_eq!(
        *part(&c.backend, "late", &late, 2, &data(9, 2))
            .await
            .unwrap_err()
            .code(),
        S3ErrorCode::SlowDown
    );
    assert_eq!(
        *part_copy(&c.backend, "late", &late, 2)
            .await
            .unwrap_err()
            .code(),
        S3ErrorCode::SlowDown
    );
    complete(&c.backend, "late", &late, &[(1, late_part)])
        .await
        .unwrap();
    abort(&c.backend, "doomed", &doomed).await.unwrap();

    // Every completed object whole; nothing aborted visible.
    let whole = |u: usize| -> Bytes {
        Bytes::from([data(u, 1), data(u, 2), data(u, 3), source.clone()].concat())
    };
    for (key, completed) in &done {
        let r = s3::get(&c.backend, BUCKET, key).await;
        if *completed {
            let u: usize = key[2..].parse().unwrap();
            assert_eq!(r.unwrap().0, whole(u), "{key}");
        } else {
            assert_eq!(*r.unwrap_err().code(), S3ErrorCode::NoSuchKey, "{key}");
        }
    }
    assert_eq!(
        s3::get(&c.backend, BUCKET, "late").await.unwrap().0,
        data(9, 1)
    );
    for k in ["doomed", "refused"] {
        assert_eq!(
            *s3::head(&c.backend, BUCKET, k).await.unwrap_err().code(),
            S3ErrorCode::NoSuchKey
        );
    }

    // Delete two completed objects; collect everything collectable.
    force(&c, 0).await;
    s3::delete(&c.backend, BUCKET, "mp0").await.unwrap();
    s3::delete(&c.backend, BUCKET, "mp1").await.unwrap();
    let far = now_ms() + 3_600_000;
    for _ in 0..3 {
        c.gc(far, 0).await;
        arca_lake_controller::gc::sweep_multipart(c.meta.as_ref(), far, Some(far), 1000)
            .await
            .unwrap();
    }
    c.gc(far, 0).await;
    c.reconcile(far, 0).await;
    // Left: the source (4 chunks), mp2 and mp3 (3 + 4 chunks each), late
    // (1 chunk); three shards per chunk.
    assert_eq!(
        c.total_shards().await,
        baseline + (7 + 7 + 1) * 3,
        "only the live objects' shards"
    );
    for (k, u) in [("mp2", 2), ("mp3", 3)] {
        assert_eq!(s3::get(&c.backend, BUCKET, k).await.unwrap().0, whole(u));
    }
    for i in 0..3 {
        let d = c.node_service(i).device_capacity().unwrap();
        assert_eq!(d.inflight_bytes, 0, "node {i} holds capacity");
    }
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multipart_under_backpressure_completes_aborts_and_collects() {
    scenario(arca_lake_testkit::mem_starter()).await;
}

#[cfg(feature = "fdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multipart_under_backpressure_on_foundationdb() {
    let Some(start) = arca_lake_testkit::fdb::starter() else {
        eprintln!("ARCA_FDB_TESTS not set; skipping");
        return;
    };
    scenario(start).await;
}
