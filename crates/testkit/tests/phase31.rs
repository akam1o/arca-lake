//! Phase 3.1 end-to-end tests over the in-process cluster (RFC-007 §5.3–5.7):
//! multipart upload (complete, abort, re-upload, races, gateway death,
//! unknown commit outcome, GC of parts), versioning exposure, conditional
//! requests, CopyObject and checksums. Direct `S3` trait calls; the AWS
//! CLI/SDK conformance lives in `s3_compat.rs`.

#![allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::assert_is_empty,
    clippy::similar_names,
    clippy::cast_sign_loss,
    clippy::type_complexity,
    clippy::redundant_closure_for_method_calls
)]

use std::sync::Arc;

use arca_lake_gateway::checksum;
use arca_lake_gateway::cluster::FaultPoint;
use arca_lake_metadata::fault::{CommitFault, FaultKv};
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::object::get_generation;
use arca_lake_metadata::ops::{bucket, multipart};
use arca_lake_metadata::records::{ChecksumAlgorithm, GenState, upload_id_from_str};
use arca_lake_metadata::{KvStore, keys};
use arca_lake_testkit::s3::{self, PutOptions, collect, request};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::dto::{
    AbortMultipartUploadInput, BucketVersioningStatus, CompleteMultipartUploadInput,
    CompletedMultipartUpload, CompletedPart, CopyObjectInput, CopySource,
    CreateMultipartUploadInput, DeleteObjectInput, ETag, ETagCondition, GetObjectInput,
    HeadObjectInput, ListMultipartUploadsInput, ListObjectVersionsInput, ListPartsInput,
    PutBucketVersioningInput, PutObjectInput, StreamingBlob, Timestamp, UploadPartInput,
    VersioningConfiguration,
};
use s3s::{S3, S3ErrorCode, S3Result};

const MIB: u64 = 1 << 20;
const BUCKET: &str = "p31";

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

async fn fault_cluster() -> TestCluster<FaultKv<MemKv>> {
    let c = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions {
            groups: 2,
            ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
        },
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn etag_of(e: Option<&ETag>) -> String {
    match e {
        Some(ETag::Strong(s) | ETag::Weak(s)) => s.clone(),
        None => String::new(),
    }
}

// ---- multipart helpers ----------------------------------------------------

async fn create_upload(s3: &impl S3, key: &str, algo: Option<&str>) -> String {
    s3.create_multipart_upload(request(CreateMultipartUploadInput {
        bucket: BUCKET.into(),
        key: key.into(),
        content_type: Some("application/x-test".into()),
        checksum_algorithm: algo.map(|a| s3s::dto::ChecksumAlgorithm::from(a.to_string())),
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
    checksum: Option<(ChecksumAlgorithm, String)>,
) -> S3Result<(String, s3s::dto::UploadPartOutput)> {
    let mut input = UploadPartInput {
        bucket: BUCKET.into(),
        key: key.into(),
        upload_id: upload_id.into(),
        part_number: n,
        body: Some(StreamingBlob::from_bytes(data)),
        ..Default::default()
    };
    if let Some((algo, v)) = checksum {
        match algo {
            ChecksumAlgorithm::Crc32 => input.checksum_crc32 = Some(v),
            ChecksumAlgorithm::Crc32c => input.checksum_crc32c = Some(v),
            ChecksumAlgorithm::Crc64Nvme => input.checksum_crc64nvme = Some(v),
            ChecksumAlgorithm::Sha1 => input.checksum_sha1 = Some(v),
            ChecksumAlgorithm::Sha256 => input.checksum_sha256 = Some(v),
        }
    }
    let out = s3.upload_part(request(input)).await?.output;
    Ok((etag_of(out.e_tag.as_ref()), out))
}

fn completed(parts: &[(i32, String)]) -> CompletedMultipartUpload {
    CompletedMultipartUpload {
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
    }
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
            multipart_upload: Some(completed(parts)),
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

async fn list_parts(s3: &impl S3, key: &str, upload_id: &str) -> Vec<(i32, String, i64)> {
    s3.list_parts(request(ListPartsInput {
        bucket: BUCKET.into(),
        key: key.into(),
        upload_id: upload_id.into(),
        ..Default::default()
    }))
    .await
    .unwrap()
    .output
    .parts
    .unwrap_or_default()
    .into_iter()
    .map(|p| {
        (
            p.part_number.unwrap(),
            etag_of(p.e_tag.as_ref()),
            p.size.unwrap(),
        )
    })
    .collect()
}

async fn list_uploads(s3: &impl S3) -> Vec<(String, String)> {
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
    .map(|u| (u.key.unwrap(), u.upload_id.unwrap()))
    .collect()
}

fn md5_hex(data: &[u8]) -> String {
    use s3s::crypto::{Checksum as _, Md5};
    let mut m = Md5::new();
    m.update(data);
    checksum::hex(m.finalize().as_ref())
}

async fn get_version(
    s3: &impl S3,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<(Bytes, s3s::dto::GetObjectOutput)> {
    let out = s3
        .get_object(request(GetObjectInput {
            bucket: BUCKET.into(),
            key: key.into(),
            version_id: version_id.map(str::to_string),
            ..Default::default()
        }))
        .await?
        .output;
    let body = collect(out.body).await?;
    Ok((body, s3s::dto::GetObjectOutput { body: None, ..out }))
}

async fn set_versioning(s3: &impl S3, status: &'static str) {
    let mut b = PutBucketVersioningInput::builder();
    b.set_bucket(BUCKET.into());
    b.set_versioning_configuration(VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(status)),
        ..Default::default()
    });
    s3.put_bucket_versioning(request(b.build().unwrap()))
        .await
        .unwrap();
}

async fn list_versions(
    s3: &impl S3,
    prefix: &str,
) -> (Vec<(String, String, bool)>, Vec<(String, String, bool)>) {
    let out = s3
        .list_object_versions(request(ListObjectVersionsInput {
            bucket: BUCKET.into(),
            prefix: Some(prefix.into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let versions = out
        .versions
        .unwrap_or_default()
        .into_iter()
        .map(|v| (v.key.unwrap(), v.version_id.unwrap(), v.is_latest.unwrap()))
        .collect();
    let markers = out
        .delete_markers
        .unwrap_or_default()
        .into_iter()
        .map(|v| (v.key.unwrap(), v.version_id.unwrap(), v.is_latest.unwrap()))
        .collect();
    (versions, markers)
}

async fn gen_state<S: KvStore + 'static>(c: &TestCluster<S>, g: &[u8; 16]) -> Option<GenState> {
    get_generation(c.meta.as_ref(), g)
        .await
        .ok()
        .map(|h| h.state)
}

// ---- multipart -----------------------------------------------------------

#[tokio::test]
async fn multipart_upload_completes_atomically_and_reads_across_parts() {
    let c = cluster().await;
    let s3 = &c.backend;
    let baseline = c.total_shards().await;
    let key = "mp/object";
    let upload_id = create_upload(s3, key, None).await;
    assert_eq!(
        list_uploads(s3).await,
        vec![(key.to_string(), upload_id.clone())]
    );
    // Parts of 1.5 MiB (two chunks each) and a small last part.
    let p1 = payload((MIB + MIB / 2) as usize, 1);
    let p2 = payload((MIB + MIB / 2) as usize, 2);
    let p3 = payload(1000, 3);
    let (e1, _) = upload_part(s3, key, &upload_id, 1, p1.clone(), None)
        .await
        .unwrap();
    let (e2, _) = upload_part(s3, key, &upload_id, 2, p2.clone(), None)
        .await
        .unwrap();
    let (e3, _) = upload_part(s3, key, &upload_id, 3, p3.clone(), None)
        .await
        .unwrap();
    assert_eq!(e1, md5_hex(&p1), "part ETag is the part MD5");
    assert_eq!(
        list_parts(s3, key, &upload_id).await,
        vec![
            (1, e1.clone(), p1.len() as i64),
            (2, e2.clone(), p2.len() as i64),
            (3, e3.clone(), 1000)
        ]
    );
    // Not visible before Complete.
    assert_eq!(
        *s3::head(s3, BUCKET, key).await.unwrap_err().code(),
        S3ErrorCode::NoSuchKey
    );

    let out = complete(
        s3,
        key,
        &upload_id,
        &[(1, e1.clone()), (2, e2.clone()), (3, e3.clone())],
    )
    .await
    .unwrap();
    let etag = etag_of(out.e_tag.as_ref());
    assert_eq!(
        etag,
        checksum::composite_etag(&[e1.clone(), e2.clone(), e3.clone()])
    );
    assert!(etag.ends_with("-3"));
    let whole = [p1.clone(), p2.clone(), p3.clone()].concat();
    let (body, head) = get_version(s3, key, None).await.unwrap();
    assert_eq!(body, whole);
    assert_eq!(etag_of(head.e_tag.as_ref()), etag);
    assert_eq!(head.content_type.as_deref(), Some("application/x-test"));
    // Ranges across part boundaries and inside a part.
    let (len, _) = s3::head(s3, BUCKET, key).await.unwrap();
    assert_eq!(len as usize, whole.len());
    let r = s3::get_range(s3, BUCKET, key, MIB + MIB / 2 - 10, MIB + MIB / 2 + 10)
        .await
        .unwrap();
    assert_eq!(
        &r[..],
        &whole[(MIB + MIB / 2 - 10) as usize..=(MIB + MIB / 2 + 10) as usize]
    );
    let tail_start = whole.len() - 500;
    let r = s3::get_range(s3, BUCKET, key, tail_start as u64, (whole.len() - 1) as u64)
        .await
        .unwrap();
    assert_eq!(&r[..], &whole[tail_start..]);
    // partNumber GET returns one part with the parts count.
    let resp = s3
        .get_object(request(GetObjectInput {
            bucket: BUCKET.into(),
            key: key.into(),
            part_number: Some(2),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        resp.headers
            .get("x-amz-mp-parts-count")
            .map(|v| v.to_str().unwrap()),
        Some("3")
    );
    assert_eq!(collect(resp.output.body).await.unwrap(), p2);
    let err = s3
        .get_object(request(GetObjectInput {
            bucket: BUCKET.into(),
            key: key.into(),
            part_number: Some(4),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InvalidRange);
    // After Complete: no upload, ListParts and Abort fail with NoSuchUpload.
    assert!(list_uploads(s3).await.is_empty());
    assert_eq!(
        *abort(s3, key, &upload_id).await.unwrap_err().code(),
        S3ErrorCode::NoSuchUpload
    );
    // Retrying the identical Complete is idempotent.
    let again = complete(
        s3,
        key,
        &upload_id,
        &[(1, e1.clone()), (2, e2.clone()), (3, e3)],
    )
    .await
    .unwrap();
    assert_eq!(etag_of(again.e_tag.as_ref()), etag);
    // A different part list is NoSuchUpload.
    let err = complete(s3, key, &upload_id, &[(1, e1)]).await.unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::NoSuchUpload);

    // GC after Complete deletes nothing (I-MP1): parts are referenced.
    let shards_after_complete = c.total_shards().await;
    let report = c.gc(now_ms() + 600_000, 0).await;
    assert_eq!(report.skipped_referenced, 0);
    assert_eq!(c.total_shards().await, shards_after_complete);
    assert_eq!(s3::get(s3, BUCKET, key).await.unwrap().0, whole);
    // Delete → the root retires → GC retires parts (I-MP2) → next pass
    // collects their shards; everything returns to the baseline.
    s3::delete(s3, BUCKET, key).await.unwrap();
    for _ in 0..3 {
        c.gc(now_ms() + 600_000, 0).await;
    }
    assert_eq!(
        c.total_shards().await,
        baseline,
        "all part shards collected"
    );
    let uid = upload_id_from_str(&upload_id).unwrap();
    let b = bucket::get_bucket(c.meta.as_ref(), BUCKET).await.unwrap();
    assert!(
        multipart::get_upload(c.meta.as_ref(), BUCKET, key, &uid)
            .await
            .unwrap()
            .is_none(),
        "upload record removed with the root (I-MP4)"
    );
    let _ = b;
    c.shutdown().await;
}

#[tokio::test]
async fn multipart_abort_and_reupload_collect_exactly_the_right_shards() {
    let c = cluster().await;
    let s3 = &c.backend;
    let baseline = c.total_shards().await;
    let key = "mp/abort";
    let upload_id = create_upload(s3, key, None).await;
    for n in 1..=4 {
        upload_part(
            s3,
            key,
            &upload_id,
            n,
            payload(200 << 10, u64::from(n as u32)),
            None,
        )
        .await
        .unwrap();
    }
    let after_parts = c.total_shards().await;
    assert!(after_parts > baseline);
    abort(s3, key, &upload_id).await.unwrap();
    assert!(list_uploads(s3).await.is_empty());
    // UploadPart / Complete / ListParts after Abort: NoSuchUpload.
    let err = upload_part(s3, key, &upload_id, 5, payload(10, 9), None)
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::NoSuchUpload);
    let err = complete(s3, key, &upload_id, &[(1, "x".into())])
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::NoSuchUpload);
    c.gc(now_ms() + 600_000, 0).await;
    assert_eq!(c.total_shards().await, baseline, "aborted parts collected");

    // Re-upload of a part number: the superseded part's shards go away
    // after GC, the listed parts stay.
    let upload_id = create_upload(s3, key, None).await;
    let (e_old, _) = upload_part(s3, key, &upload_id, 1, payload(200 << 10, 11), None)
        .await
        .unwrap();
    let new_data = payload(200 << 10, 12);
    let (e_new, _) = upload_part(s3, key, &upload_id, 1, new_data.clone(), None)
        .await
        .unwrap();
    let (e2, _) = upload_part(s3, key, &upload_id, 2, payload(200 << 10, 13), None)
        .await
        .unwrap();
    // An uploaded but unlisted part (3) is discarded by Complete.
    upload_part(s3, key, &upload_id, 3, payload(200 << 10, 14), None)
        .await
        .unwrap();
    assert_eq!(list_parts(s3, key, &upload_id).await.len(), 3);
    let err = complete(s3, key, &upload_id, &[(1, e_old.clone()), (2, e2.clone())])
        .await
        .unwrap_err();
    assert_eq!(
        *err.code(),
        S3ErrorCode::InvalidPart,
        "stale ETag of a superseded part"
    );
    let err = complete(s3, key, &upload_id, &[(2, e2.clone()), (1, e_new.clone())])
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InvalidPartOrder);
    complete(s3, key, &upload_id, &[(1, e_new), (2, e2)])
        .await
        .unwrap();
    let two_parts = (200 << 10) * 2;
    let (len, _) = s3::head(s3, BUCKET, key).await.unwrap();
    assert_eq!(len, two_parts);
    assert_eq!(
        &s3::get(s3, BUCKET, key).await.unwrap().0[..new_data.len()],
        &new_data[..]
    );
    c.gc(now_ms() + 600_000, 0).await;
    let group = c.groups().await[0].id;
    let _ = group;
    // Exactly the two listed parts remain (each 200 KiB = 1 chunk → 3 shards
    // across the group), plus nothing else.
    assert_eq!(c.total_shards().await, baseline + 2 * 3);
    c.shutdown().await;
}

#[tokio::test]
async fn concurrent_completes_and_complete_abort_race_have_one_winner() {
    let c = cluster().await;
    let s3 = &c.backend;
    let key = "mp/race";
    let upload_id = create_upload(s3, key, None).await;
    let (e1, _) = upload_part(s3, key, &upload_id, 1, payload(100 << 10, 1), None)
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let b = c.backend.clone();
        let (uid, e1) = (upload_id.clone(), e1.clone());
        tasks.push(tokio::spawn(async move {
            complete(&b, key, &uid, &[(1, e1)]).await
        }));
    }
    let mut etags = std::collections::HashSet::new();
    let mut ok = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(o) => {
                ok += 1;
                etags.insert(etag_of(o.e_tag.as_ref()));
            }
            Err(e) => assert_eq!(*e.code(), S3ErrorCode::NoSuchUpload),
        }
    }
    assert!(
        ok >= 1 && etags.len() == 1,
        "every success reports the same object"
    );
    let (versions, _) = list_versions(s3, "mp/race").await;
    assert_eq!(versions.len(), 1);

    // Complete × Abort: one wins; GC never collects a referenced part.
    for seed in 0..4u64 {
        let key = format!("mp/ca{seed}");
        let upload_id = create_upload(s3, &key, None).await;
        let (e1, _) = upload_part(s3, &key, &upload_id, 1, payload(100 << 10, 20 + seed), None)
            .await
            .unwrap();
        let (b1, b2) = (c.backend.clone(), c.backend.clone());
        let (k1, k2, u1, u2) = (
            key.clone(),
            key.clone(),
            upload_id.clone(),
            upload_id.clone(),
        );
        let t1 = tokio::spawn(async move { complete(&b1, &k1, &u1, &[(1, e1)]).await.map(|_| ()) });
        let t2 = tokio::spawn(async move { abort(&b2, &k2, &u2).await });
        let (r1, r2) = (t1.await.unwrap(), t2.await.unwrap());
        match (r1, r2) {
            (Ok(()), Err(e)) => {
                assert_eq!(*e.code(), S3ErrorCode::NoSuchUpload);
                let report = c.gc(now_ms() + 600_000, 0).await;
                assert_eq!(report.skipped_referenced, 0);
                assert!(
                    s3::get(s3, BUCKET, &key).await.is_ok(),
                    "complete won: readable"
                );
            }
            (Err(e), Ok(())) => {
                assert_eq!(*e.code(), S3ErrorCode::NoSuchUpload);
                assert_eq!(
                    *s3::head(s3, BUCKET, &key).await.unwrap_err().code(),
                    S3ErrorCode::NoSuchKey
                );
            }
            other => panic!("seed {seed}: {other:?}"),
        }
    }
    c.shutdown().await;
}

#[tokio::test]
async fn gateway_death_during_upload_part_leaves_only_lease_garbage() {
    let c = cluster().await;
    let s3 = &c.backend;
    let baseline = c.total_shards().await;
    let key = "mp/die";
    let upload_id = create_upload(s3, key, None).await;
    c.backend.set_fault(Some(FaultPoint::DieBeforeCommit));
    let err = upload_part(s3, key, &upload_id, 1, payload(300 << 10, 1), None)
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InternalError);
    // The part is pending; ListParts does not show it; Complete cannot use it.
    assert!(list_parts(s3, key, &upload_id).await.is_empty());
    // Within the lease the pending part is never collected, whatever the grace.
    c.gc(now_ms(), 0).await;
    assert!(c.total_shards().await > baseline);
    // After the lease it expires and its shards are collected.
    let lease_ms = 60_000;
    c.gc(now_ms() + lease_ms + 1_000, 0).await;
    c.gc(now_ms() + lease_ms + 2_000, 0).await;
    assert_eq!(c.total_shards().await, baseline);
    // The upload is still open and usable.
    let (e1, _) = upload_part(s3, key, &upload_id, 1, payload(300 << 10, 2), None)
        .await
        .unwrap();
    complete(s3, key, &upload_id, &[(1, e1)]).await.unwrap();
    assert_eq!(s3::head(s3, BUCKET, key).await.unwrap().0, 300 << 10);
    c.shutdown().await;
}

#[tokio::test]
async fn complete_with_unknown_commit_outcome_publishes_exactly_once() {
    let c = fault_cluster().await;
    let s3 = &c.backend;
    let key = "mp/unknown";
    let upload_id = create_upload(s3, key, None).await;
    let (e1, _) = upload_part(s3, key, &upload_id, 1, payload(100 << 10, 1), None)
        .await
        .unwrap();
    let b = bucket::get_bucket(c.meta.as_ref(), BUCKET).await.unwrap();
    let uid = upload_id_from_str(&upload_id).unwrap();
    // The Complete transaction writes the upload record: lose its reply
    // once with the commit applied, then once without.
    c.meta.inject_on_key(
        keys::mpu(b.id, key, &uid),
        CommitFault::UnknownAfterApply,
        1,
    );
    let out = complete(s3, key, &upload_id, &[(1, e1.clone())])
        .await
        .unwrap();
    assert_eq!(c.meta.fired(), vec![CommitFault::UnknownAfterApply]);
    let (versions, _) = list_versions(s3, key).await;
    assert_eq!(
        versions.len(),
        1,
        "one visible version despite the lost reply"
    );
    let m = c.manifest(BUCKET, key).await;
    assert_eq!(m.parts.len(), 1);
    assert_eq!(etag_of(out.e_tag.as_ref()), m.header.etag);

    let key2 = "mp/unknown2";
    let upload_id = create_upload(s3, key2, None).await;
    let (e1, _) = upload_part(s3, key2, &upload_id, 1, payload(100 << 10, 2), None)
        .await
        .unwrap();
    let uid = upload_id_from_str(&upload_id).unwrap();
    c.meta.clear_rules();
    c.meta.inject_on_key(
        keys::mpu(b.id, key2, &uid),
        CommitFault::UnknownWithoutApply,
        1,
    );
    complete(s3, key2, &upload_id, &[(1, e1)]).await.unwrap();
    assert_eq!(s3::head(s3, BUCKET, key2).await.unwrap().0, 100 << 10);
    let report = c.gc(now_ms() + 600_000, 0).await;
    assert_eq!(report.skipped_referenced, 0);
    c.shutdown().await;
}

#[tokio::test]
async fn gateway_death_mid_abort_is_finished_by_the_controller_sweep() {
    let c = cluster().await;
    let s3 = &c.backend;
    let baseline = c.total_shards().await;
    let key = "mp/abort-crash";
    let upload_id = create_upload(s3, key, None).await;
    for n in 1..=3 {
        upload_part(
            s3,
            key,
            &upload_id,
            n,
            payload(150 << 10, u64::from(n as u32)),
            None,
        )
        .await
        .unwrap();
    }
    // The gateway dies right after transaction 1 of Abort (`Aborting`).
    let uid = upload_id_from_str(&upload_id).unwrap();
    multipart::abort(c.meta.as_ref(), BUCKET, key, &uid)
        .await
        .unwrap();
    assert!(
        list_uploads(s3).await.is_empty(),
        "aborting uploads are not listed"
    );
    // Nothing is collected before the sweep: parts are still committed.
    c.gc(now_ms() + 600_000, 0).await;
    assert!(c.total_shards().await > baseline);
    // The controller sweep finishes the abort; GC then collects the parts.
    let report = arca_lake_controller::gc::sweep_multipart(c.meta.as_ref(), now_ms(), None, 100)
        .await
        .unwrap();
    assert_eq!(report.parts_processed, 3);
    assert!(
        multipart::get_upload(c.meta.as_ref(), BUCKET, key, &uid)
            .await
            .unwrap()
            .is_none()
    );
    c.gc(now_ms() + 600_000, 0).await;
    assert_eq!(c.total_shards().await, baseline);
    // Expiry: an open upload older than the limit is aborted by the sweep.
    let upload_id = create_upload(s3, "mp/stale", None).await;
    upload_part(s3, "mp/stale", &upload_id, 1, payload(100 << 10, 9), None)
        .await
        .unwrap();
    let report = arca_lake_controller::gc::sweep_multipart(
        c.meta.as_ref(),
        now_ms(),
        Some(now_ms() + 1),
        100,
    )
    .await
    .unwrap();
    assert_eq!(report.expired, 1);
    assert!(list_uploads(s3).await.is_empty());
    c.gc(now_ms() + 600_000, 0).await;
    assert_eq!(c.total_shards().await, baseline);
    c.shutdown().await;
}

// ---- versioning -----------------------------------------------------------

#[tokio::test]
async fn versioning_exposes_versions_markers_and_version_specific_operations() {
    let c = cluster().await;
    let s3 = &c.backend;
    let key = "v/key";
    // Unversioned write first: becomes the null version when enabled.
    let v0 = payload(10 << 10, 0);
    s3::put(s3, BUCKET, key, v0.clone(), &PutOptions::default())
        .await
        .unwrap();
    set_versioning(s3, BucketVersioningStatus::ENABLED).await;
    let v1 = payload(20 << 10, 1);
    let v2 = payload(30 << 10, 2);
    let out1 = s3
        .put_object(request(PutObjectInput {
            bucket: BUCKET.into(),
            key: key.into(),
            body: Some(StreamingBlob::from_bytes(v1.clone())),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let id1 = out1.version_id.expect("version id on an enabled bucket");
    let out2 = s3
        .put_object(request(PutObjectInput {
            bucket: BUCKET.into(),
            key: key.into(),
            body: Some(StreamingBlob::from_bytes(v2.clone())),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let id2 = out2.version_id.unwrap();
    let (versions, markers) = list_versions(s3, key).await;
    assert_eq!(
        versions,
        vec![
            (key.into(), id2.clone(), true),
            (key.into(), id1.clone(), false),
            (key.into(), "null".into(), false),
        ]
    );
    assert!(markers.is_empty());
    assert_eq!(get_version(s3, key, None).await.unwrap().0, v2);
    assert_eq!(get_version(s3, key, Some(&id1)).await.unwrap().0, v1);
    assert_eq!(get_version(s3, key, Some("null")).await.unwrap().0, v0);
    let err = get_version(s3, key, Some("deadbeef")).await.unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::NoSuchVersion);
    // DELETE → marker; GET 404 with x-amz-delete-marker; versions remain.
    let d = s3
        .delete_object(request(DeleteObjectInput {
            bucket: BUCKET.into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(d.delete_marker, Some(true));
    let marker = d.version_id.unwrap();
    let err = get_version(s3, key, None).await.unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);
    assert_eq!(
        err.headers()
            .and_then(|h| h.get("x-amz-delete-marker"))
            .map(|v| v.to_str().unwrap()),
        Some("true")
    );
    let err = get_version(s3, key, Some(&marker)).await.unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::MethodNotAllowed);
    assert_eq!(get_version(s3, key, Some(&id2)).await.unwrap().0, v2);
    let (_, markers) = list_versions(s3, key).await;
    assert_eq!(markers, vec![(key.into(), marker.clone(), true)]);
    // Listing hides the key; HEAD is 404.
    let listed = s3
        .list_objects_v2(request(s3s::dto::ListObjectsV2Input {
            bucket: BUCKET.into(),
            prefix: Some("v/".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(listed.contents.is_none());
    // Delete the marker → v2 current again.
    let d = s3
        .delete_object(request(DeleteObjectInput {
            bucket: BUCKET.into(),
            key: key.into(),
            version_id: Some(marker.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(d.delete_marker, Some(true));
    assert_eq!(get_version(s3, key, None).await.unwrap().0, v2);
    // Delete the current data version → v1 current; v2's shards collected.
    let before = c.total_shards().await;
    s3.delete_object(request(DeleteObjectInput {
        bucket: BUCKET.into(),
        key: key.into(),
        version_id: Some(id2.clone()),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_eq!(get_version(s3, key, None).await.unwrap().0, v1);
    c.gc(now_ms() + 600_000, 0).await;
    assert!(c.total_shards().await < before);
    // Idempotent: deleting the same version again succeeds.
    s3.delete_object(request(DeleteObjectInput {
        bucket: BUCKET.into(),
        key: key.into(),
        version_id: Some(id2),
        ..Default::default()
    }))
    .await
    .unwrap();
    // Suspend: a PUT becomes the null version and replaces the old null.
    set_versioning(s3, BucketVersioningStatus::SUSPENDED).await;
    let v3 = payload(5 << 10, 3);
    let out3 = s3
        .put_object(request(PutObjectInput {
            bucket: BUCKET.into(),
            key: key.into(),
            body: Some(StreamingBlob::from_bytes(v3.clone())),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out3.version_id, None, "null version id is not reported");
    let (versions, _) = list_versions(s3, key).await;
    assert_eq!(versions[0], (key.into(), "null".into(), true));
    assert_eq!(versions.iter().filter(|v| v.1 == "null").count(), 1);
    assert_eq!(get_version(s3, key, Some("null")).await.unwrap().0, v3);
    assert_eq!(get_version(s3, key, Some(&id1)).await.unwrap().0, v1);
    c.shutdown().await;
}

// ---- conditional requests ------------------------------------------------

#[tokio::test]
async fn conditional_get_head_matrix_follows_rfc_9110_and_aws() {
    let c = cluster().await;
    let s3 = &c.backend;
    let key = "cond/key";
    let etag = s3::put(s3, BUCKET, key, payload(1000, 1), &PutOptions::default())
        .await
        .unwrap();
    let (_, head) = get_version(s3, key, None).await.unwrap();
    let lm = head.last_modified.unwrap();
    let lm_secs = {
        let mut buf = Vec::new();
        lm.format(s3s::dto::TimestampFormat::EpochSeconds, &mut buf)
            .unwrap();
        String::from_utf8(buf)
            .unwrap()
            .split('.')
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let secs = |s: u64| Timestamp::from(std::time::UNIX_EPOCH + std::time::Duration::from_secs(s));
    let before = secs(lm_secs - 100);
    let after = secs(lm_secs + 100);
    let same = ETagCondition::ETag(ETag::Strong(etag.clone()));
    let other = ETagCondition::ETag(ETag::Strong("0".repeat(32)));
    // (if_match, if_none_match, if_modified_since, if_unmodified_since) → expected
    let cases: Vec<(
        Option<ETagCondition>,
        Option<ETagCondition>,
        Option<Timestamp>,
        Option<Timestamp>,
        Option<S3ErrorCode>,
    )> = vec![
        (None, None, None, None, None),
        (Some(same.clone()), None, None, None, None),
        (
            Some(other.clone()),
            None,
            None,
            None,
            Some(S3ErrorCode::PreconditionFailed),
        ),
        (Some(ETagCondition::Any), None, None, None, None),
        (
            None,
            Some(same.clone()),
            None,
            None,
            Some(S3ErrorCode::NotModified),
        ),
        (None, Some(other.clone()), None, None, None),
        (
            None,
            Some(ETagCondition::Any),
            None,
            None,
            Some(S3ErrorCode::NotModified),
        ),
        (None, None, Some(before.clone()), None, None),
        (
            None,
            None,
            Some(after.clone()),
            None,
            Some(S3ErrorCode::NotModified),
        ),
        (None, None, None, Some(after.clone()), None),
        (
            None,
            None,
            None,
            Some(before.clone()),
            Some(S3ErrorCode::PreconditionFailed),
        ),
        // If-Match true + If-Unmodified-Since false → If-Match wins (200).
        (Some(same.clone()), None, None, Some(before.clone()), None),
        // If-Match false + If-Unmodified-Since true → 412.
        (
            Some(other.clone()),
            None,
            None,
            Some(after.clone()),
            Some(S3ErrorCode::PreconditionFailed),
        ),
        // If-None-Match false (no match) + If-Modified-Since "not modified" → If-None-Match wins (200).
        (None, Some(other.clone()), Some(after.clone()), None, None),
        // If-None-Match matches + If-Modified-Since modified → 304.
        (
            None,
            Some(same.clone()),
            Some(before.clone()),
            None,
            Some(S3ErrorCode::NotModified),
        ),
        // 412 takes precedence over 304 (If-Match evaluated first).
        (
            Some(other.clone()),
            Some(same.clone()),
            None,
            None,
            Some(S3ErrorCode::PreconditionFailed),
        ),
    ];
    assert_eq!(cases.len(), 16);
    for (i, (im, inm, ims, ius, expected)) in cases.iter().enumerate() {
        let get = s3
            .get_object(request(GetObjectInput {
                bucket: BUCKET.into(),
                key: key.into(),
                if_match: im.clone(),
                if_none_match: inm.clone(),
                if_modified_since: ims.clone(),
                if_unmodified_since: ius.clone(),
                ..Default::default()
            }))
            .await
            .map(|_| ())
            .map_err(|e| e.code().clone());
        let head = s3
            .head_object(request(HeadObjectInput {
                bucket: BUCKET.into(),
                key: key.into(),
                if_match: im.clone(),
                if_none_match: inm.clone(),
                if_modified_since: ims.clone(),
                if_unmodified_since: ius.clone(),
                ..Default::default()
            }))
            .await
            .map(|_| ())
            .map_err(|e| e.code().clone());
        let want = expected.clone().map_or(Ok(()), Err);
        assert_eq!(get, want, "GET case {i}");
        assert_eq!(head, want, "HEAD case {i}");
    }
    // Missing key: 404 regardless of conditions.
    let err = s3
        .get_object(request(GetObjectInput {
            bucket: BUCKET.into(),
            key: "cond/missing".into(),
            if_none_match: Some(ETagCondition::Any),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);
    // Conditional PUT with If-Match on a missing key is 404 (AWS).
    let err = s3::put(
        s3,
        BUCKET,
        "cond/missing",
        payload(10, 2),
        &PutOptions {
            if_match: Some(etag.clone()),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);
    // Conditional DELETE with If-Match.
    let err = s3
        .delete_object(request(DeleteObjectInput {
            bucket: BUCKET.into(),
            key: key.into(),
            if_match: Some(other),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::PreconditionFailed);
    s3.delete_object(request(DeleteObjectInput {
        bucket: BUCKET.into(),
        key: key.into(),
        if_match: Some(same),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_eq!(
        *s3::head(s3, BUCKET, key).await.unwrap_err().code(),
        S3ErrorCode::NoSuchKey
    );
    // Conditional Complete: If-None-Match: * against an existing key.
    s3::put(
        s3,
        BUCKET,
        "cond/mp",
        payload(10, 3),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let upload_id = create_upload(s3, "cond/mp", None).await;
    let (e1, _) = upload_part(s3, "cond/mp", &upload_id, 1, payload(10, 4), None)
        .await
        .unwrap();
    let err = s3
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: BUCKET.into(),
            key: "cond/mp".into(),
            upload_id: upload_id.clone(),
            multipart_upload: Some(completed(&[(1, e1.clone())])),
            if_none_match: Some(ETagCondition::Any),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::PreconditionFailed);
    assert_eq!(
        s3::head(s3, BUCKET, "cond/mp").await.unwrap().0,
        10,
        "nothing published"
    );
    complete(s3, "cond/mp", &upload_id, &[(1, e1)])
        .await
        .unwrap();
    c.shutdown().await;
}

// ---- CopyObject -----------------------------------------------------------

async fn copy(
    s3: &impl S3,
    src: &str,
    src_version: Option<&str>,
    dst: &str,
    replace: Option<(Option<String>, Option<(String, String)>)>,
    cond: Option<(Option<ETagCondition>, Option<ETagCondition>)>,
) -> S3Result<s3s::dto::CopyObjectOutput> {
    let (content_type, meta) = match &replace {
        Some((ct, m)) => (
            ct.clone(),
            m.clone().map(|(k, v)| std::iter::once((k, v)).collect()),
        ),
        None => (None, None),
    };
    let (cim, cinm) = cond.unwrap_or((None, None));
    let mut b = CopyObjectInput::builder();
    b.set_bucket(BUCKET.into());
    b.set_key(dst.into());
    b.set_copy_source(CopySource::Bucket {
        bucket: BUCKET.into(),
        key: src.into(),
        version_id: src_version.map(Into::into),
    });
    b.set_metadata_directive(
        replace.as_ref().map(|_| {
            s3s::dto::MetadataDirective::from_static(s3s::dto::MetadataDirective::REPLACE)
        }),
    );
    b.set_content_type(content_type);
    b.set_metadata(meta);
    b.set_copy_source_if_match(cim);
    b.set_copy_source_if_none_match(cinm);
    Ok(s3.copy_object(request(b.build().unwrap())).await?.output)
}

#[tokio::test]
async fn copy_object_is_a_physical_copy_with_fixed_source_version() {
    let c = cluster().await;
    let s3 = &c.backend;
    let data = payload((2 * MIB + 777) as usize, 1);
    let src_etag = s3::put(
        s3,
        BUCKET,
        "copy/src",
        data.clone(),
        &PutOptions {
            content_type: Some("text/plain".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let out = copy(s3, "copy/src", None, "copy/dst", None, None)
        .await
        .unwrap();
    let res = out.copy_object_result.unwrap();
    assert_eq!(
        etag_of(res.e_tag.as_ref()),
        src_etag,
        "same bytes, same MD5"
    );
    let (body, head) = get_version(s3, "copy/dst", None).await.unwrap();
    assert_eq!(body, data);
    assert_eq!(
        head.content_type.as_deref(),
        Some("text/plain"),
        "COPY keeps the content type"
    );
    // Distinct generations and shards: deleting the source leaves the copy.
    let src_gen = c.manifest(BUCKET, "copy/src").await.pointer.generation;
    let dst_gen = c.manifest(BUCKET, "copy/dst").await.pointer.generation;
    assert_ne!(src_gen, dst_gen);
    s3::delete(s3, BUCKET, "copy/src").await.unwrap();
    c.gc(now_ms() + 600_000, 0).await;
    assert_eq!(s3::get(s3, BUCKET, "copy/dst").await.unwrap().0, data);
    assert!(matches!(
        gen_state(&c, &src_gen).await,
        None | Some(GenState::Retiring { .. })
    ));
    // REPLACE changes metadata and content type; same-key copy needs REPLACE.
    let err = copy(s3, "copy/dst", None, "copy/dst", None, None)
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InvalidRequest);
    copy(
        s3,
        "copy/dst",
        None,
        "copy/dst",
        Some((
            Some("application/json".into()),
            Some(("k".into(), "v".into())),
        )),
        None,
    )
    .await
    .unwrap();
    let (_, head) = get_version(s3, "copy/dst", None).await.unwrap();
    assert_eq!(head.content_type.as_deref(), Some("application/json"));
    assert_eq!(
        head.metadata.unwrap().get("k").map(String::as_str),
        Some("v")
    );
    // Conditional copy: 412 on both mismatch kinds (never 304).
    let err = copy(
        s3,
        "copy/dst",
        None,
        "copy/dst2",
        None,
        Some((
            Some(ETagCondition::ETag(ETag::Strong("0".repeat(32)))),
            None,
        )),
    )
    .await
    .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::PreconditionFailed);
    let err = copy(
        s3,
        "copy/dst",
        None,
        "copy/dst2",
        None,
        Some((
            None,
            Some(ETagCondition::ETag(ETag::Strong(src_etag.clone()))),
        )),
    )
    .await
    .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::PreconditionFailed);
    assert_eq!(
        *s3::head(s3, BUCKET, "copy/dst2").await.unwrap_err().code(),
        S3ErrorCode::NoSuchKey
    );
    // Source version fixed: copy a specific version while the current one
    // changes; a concurrent DELETE of the source does not affect the copy.
    set_versioning(s3, BucketVersioningStatus::ENABLED).await;
    let old = payload(MIB as usize, 5);
    let out = s3
        .put_object(request(PutObjectInput {
            bucket: BUCKET.into(),
            key: "copy/versioned".into(),
            body: Some(StreamingBlob::from_bytes(old.clone())),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let old_id = out.version_id.unwrap();
    s3::put(
        s3,
        BUCKET,
        "copy/versioned",
        payload(MIB as usize, 6),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let b = c.backend.clone();
    let copying = tokio::spawn(async move {
        copy(
            &b,
            "copy/versioned",
            Some(&old_id),
            "copy/from-old",
            None,
            None,
        )
        .await
    });
    s3::delete(s3, BUCKET, "copy/versioned").await.unwrap();
    let out = copying.await.unwrap().unwrap();
    assert!(out.copy_source_version_id.is_some());
    assert_eq!(s3::get(s3, BUCKET, "copy/from-old").await.unwrap().0, old);
    // Copy of a multipart object is a single-part object with an MD5 ETag.
    set_versioning(s3, BucketVersioningStatus::SUSPENDED).await;
    let upload_id = create_upload(s3, "copy/mp", None).await;
    let p1 = payload(MIB as usize, 7);
    let p2 = payload(100, 8);
    let (e1, _) = upload_part(s3, "copy/mp", &upload_id, 1, p1.clone(), None)
        .await
        .unwrap();
    let (e2, _) = upload_part(s3, "copy/mp", &upload_id, 2, p2.clone(), None)
        .await
        .unwrap();
    complete(s3, "copy/mp", &upload_id, &[(1, e1), (2, e2)])
        .await
        .unwrap();
    let out = copy(s3, "copy/mp", None, "copy/mp-copy", None, None)
        .await
        .unwrap();
    let whole = [p1, p2].concat();
    assert_eq!(
        etag_of(out.copy_object_result.unwrap().e_tag.as_ref()),
        md5_hex(&whole)
    );
    assert_eq!(s3::get(s3, BUCKET, "copy/mp-copy").await.unwrap().0, whole);
    // Failure recovery: a copy that dies before commit publishes nothing
    // and its shards are collected (earlier displaced generations are
    // collected first so that the baseline is stable).
    c.gc(now_ms() + 600_000, 0).await;
    let baseline = c.total_shards().await;
    c.backend.set_fault(Some(FaultPoint::BeforeCommit));
    let err = copy(s3, "copy/mp", None, "copy/failed", None, None)
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InternalError);
    assert_eq!(
        *s3::head(s3, BUCKET, "copy/failed")
            .await
            .unwrap_err()
            .code(),
        S3ErrorCode::NoSuchKey
    );
    c.gc(now_ms() + 600_000, 0).await;
    assert_eq!(c.total_shards().await, baseline);
    c.shutdown().await;
}

// ---- checksums -------------------------------------------------------------

const ALGOS: [ChecksumAlgorithm; 5] = [
    ChecksumAlgorithm::Crc32,
    ChecksumAlgorithm::Crc32c,
    ChecksumAlgorithm::Crc64Nvme,
    ChecksumAlgorithm::Sha1,
    ChecksumAlgorithm::Sha256,
];

fn put_with_checksum(
    key: &str,
    data: Bytes,
    algo: ChecksumAlgorithm,
    value: String,
) -> PutObjectInput {
    let mut input = PutObjectInput {
        bucket: BUCKET.into(),
        key: key.into(),
        body: Some(StreamingBlob::from_bytes(data)),
        ..Default::default()
    };
    match algo {
        ChecksumAlgorithm::Crc32 => input.checksum_crc32 = Some(value),
        ChecksumAlgorithm::Crc32c => input.checksum_crc32c = Some(value),
        ChecksumAlgorithm::Crc64Nvme => input.checksum_crc64nvme = Some(value),
        ChecksumAlgorithm::Sha1 => input.checksum_sha1 = Some(value),
        ChecksumAlgorithm::Sha256 => input.checksum_sha256 = Some(value),
    }
    input
}

fn checksum_of(out: &s3s::dto::GetObjectOutput, algo: ChecksumAlgorithm) -> Option<String> {
    match algo {
        ChecksumAlgorithm::Crc32 => out.checksum_crc32.clone(),
        ChecksumAlgorithm::Crc32c => out.checksum_crc32c.clone(),
        ChecksumAlgorithm::Crc64Nvme => out.checksum_crc64nvme.clone(),
        ChecksumAlgorithm::Sha1 => out.checksum_sha1.clone(),
        ChecksumAlgorithm::Sha256 => out.checksum_sha256.clone(),
    }
}

#[tokio::test]
async fn checksums_are_verified_before_publish_and_returned_on_read() {
    let c = cluster().await;
    let s3 = &c.backend;
    let data = payload((MIB + 4321) as usize, 1);
    for algo in ALGOS {
        let key = format!("cs/{}", algo.name());
        let good = checksum::encode_b64(&checksum::digest(algo, &data));
        let bad = checksum::encode_b64(&vec![0x5a; algo.byte_len()]);
        // Mismatch: BadDigest, nothing published, shards collected.
        let baseline = c.total_shards().await;
        let err = s3
            .put_object(request(put_with_checksum(&key, data.clone(), algo, bad)))
            .await
            .unwrap_err();
        assert_eq!(*err.code(), S3ErrorCode::BadDigest, "{algo:?}");
        assert_eq!(
            *s3::head(s3, BUCKET, &key).await.unwrap_err().code(),
            S3ErrorCode::NoSuchKey
        );
        c.gc(now_ms() + 600_000, 0).await;
        assert_eq!(c.total_shards().await, baseline);
        // Match: stored and returned with checksum mode.
        let out = s3
            .put_object(request(put_with_checksum(
                &key,
                data.clone(),
                algo,
                good.clone(),
            )))
            .await
            .unwrap()
            .output;
        let returned = match algo {
            ChecksumAlgorithm::Crc32 => out.checksum_crc32,
            ChecksumAlgorithm::Crc32c => out.checksum_crc32c,
            ChecksumAlgorithm::Crc64Nvme => out.checksum_crc64nvme,
            ChecksumAlgorithm::Sha1 => out.checksum_sha1,
            ChecksumAlgorithm::Sha256 => out.checksum_sha256,
        };
        assert_eq!(returned.as_deref(), Some(good.as_str()));
        let got = s3
            .get_object(request(GetObjectInput {
                bucket: BUCKET.into(),
                key: key.clone(),
                checksum_mode: Some(s3s::dto::ChecksumMode::from_static(
                    s3s::dto::ChecksumMode::ENABLED,
                )),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(checksum_of(&got, algo).as_deref(), Some(good.as_str()));
        assert_eq!(
            got.checksum_type.as_ref().map(|t| t.as_str()),
            Some("FULL_OBJECT")
        );
        assert_eq!(collect(got.body).await.unwrap(), data);
        // Without checksum mode no checksum header is returned.
        let (_, plain) = get_version(s3, &key, None).await.unwrap();
        assert!(checksum_of(&plain, algo).is_none());
    }
    // Multipart: composite (CRC32C, SHA256) and full-object (CRC64NVME).
    let parts: Vec<Bytes> = vec![
        payload(MIB as usize, 10),
        payload((MIB / 2) as usize, 11),
        payload(999, 12),
    ];
    let whole = parts.concat();
    for (algo, want_type) in [
        (ChecksumAlgorithm::Crc32c, "COMPOSITE"),
        (ChecksumAlgorithm::Sha256, "COMPOSITE"),
        (ChecksumAlgorithm::Crc64Nvme, "FULL_OBJECT"),
    ] {
        let key = format!("cs/mp-{}", algo.name());
        let upload_id = create_upload(s3, &key, Some(algo.name())).await;
        let mut listed = Vec::new();
        let mut part_values = Vec::new();
        for (i, p) in parts.iter().enumerate() {
            let v = checksum::digest(algo, p);
            let b64 = checksum::encode_b64(&v);
            // A wrong part checksum is refused and leaves no part.
            let err = upload_part(
                s3,
                &key,
                &upload_id,
                i as i32 + 1,
                p.clone(),
                Some((algo, checksum::encode_b64(&vec![1; algo.byte_len()]))),
            )
            .await
            .unwrap_err();
            assert_eq!(*err.code(), S3ErrorCode::BadDigest);
            let (etag, _) = upload_part(
                s3,
                &key,
                &upload_id,
                i as i32 + 1,
                p.clone(),
                Some((algo, b64)),
            )
            .await
            .unwrap();
            listed.push((i as i32 + 1, etag));
            part_values.push((p.len() as u64, v));
        }
        assert_eq!(list_parts(s3, &key, &upload_id).await.len(), 3);
        let out = complete(s3, &key, &upload_id, &listed).await.unwrap();
        let expected = match want_type {
            "COMPOSITE" => format!(
                "{}-3",
                checksum::encode_b64(
                    &checksum::derive(
                        algo,
                        arca_lake_metadata::records::ChecksumType::Composite,
                        &part_values
                    )
                    .unwrap()
                )
            ),
            _ => checksum::encode_b64(&checksum::digest(algo, &whole)),
        };
        let got = match algo {
            ChecksumAlgorithm::Crc32c => out.checksum_crc32c.clone(),
            ChecksumAlgorithm::Sha256 => out.checksum_sha256.clone(),
            _ => out.checksum_crc64nvme.clone(),
        };
        assert_eq!(got.as_deref(), Some(expected.as_str()), "{algo:?}");
        assert_eq!(
            out.checksum_type.as_ref().map(|t| t.as_str()),
            Some(want_type)
        );
        let head = s3
            .head_object(request(HeadObjectInput {
                bucket: BUCKET.into(),
                key: key.clone(),
                checksum_mode: Some(s3s::dto::ChecksumMode::from_static(
                    s3s::dto::ChecksumMode::ENABLED,
                )),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let head_cs = match algo {
            ChecksumAlgorithm::Crc32c => head.checksum_crc32c,
            ChecksumAlgorithm::Sha256 => head.checksum_sha256,
            _ => head.checksum_crc64nvme,
        };
        assert_eq!(head_cs.as_deref(), Some(expected.as_str()));
        assert_eq!(s3::get(s3, BUCKET, &key).await.unwrap().0, whole);
    }
    // SHA256 with FULL_OBJECT is refused at creation.
    let err = s3
        .create_multipart_upload(request(CreateMultipartUploadInput {
            bucket: BUCKET.into(),
            key: "cs/bad".into(),
            checksum_algorithm: Some(s3s::dto::ChecksumAlgorithm::from_static(
                s3s::dto::ChecksumAlgorithm::SHA256,
            )),
            checksum_type: Some(s3s::dto::ChecksumType::from_static(
                s3s::dto::ChecksumType::FULL_OBJECT,
            )),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InvalidRequest);
    c.shutdown().await;
}
