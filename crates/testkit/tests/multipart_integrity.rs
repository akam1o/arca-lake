//! Multipart manifests under scrubbing, repair and GC (RFC-008 §8.6,
//! RFC-007 §5.3, I-MP1).

#![allow(clippy::too_many_lines)]

use arca_lake_controller::integrity::{
    RepairWorkerOptions, ScrubOptions, coders, repair_pass, scrub_group_now, verify_manifests,
};
use arca_lake_metadata::ops::integrity::{Finding, ManifestFinding};
use arca_lake_metadata::records::GenId;
use arca_lake_metadata::{Txn, keys, records, transact};
use arca_lake_testkit::s3::{self, request};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::S3;
use s3s::dto::{
    BucketVersioningStatus, CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadInput, ETag, GetObjectInput, PutBucketVersioningInput, StreamingBlob,
    UploadPartInput, VersioningConfiguration,
};

const MIB: u64 = 1 << 20;
const BUCKET: &str = "mpi";
const PART: usize = 600 << 10;

async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn etag_of(e: Option<&ETag>) -> String {
    match e {
        Some(ETag::Strong(s) | ETag::Weak(s)) => s.clone(),
        None => panic!("no etag"),
    }
}

/// Upload `parts` parts of [`PART`] bytes and complete; returns the whole
/// object's bytes and the version id (on a versioned bucket).
async fn multipart_put(
    c: &TestCluster,
    key: &str,
    parts: usize,
    seed: u64,
) -> (Bytes, Option<String>) {
    let upload_id = c
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
    let mut all = Vec::new();
    let mut done = Vec::new();
    for n in 1..=parts {
        let data = Bytes::from(random_bytes(PART, seed * 100 + n as u64));
        all.extend_from_slice(&data);
        let out = c
            .backend
            .upload_part(request(UploadPartInput {
                bucket: BUCKET.into(),
                key: key.into(),
                upload_id: upload_id.clone(),
                part_number: i32::try_from(n).unwrap(),
                body: Some(StreamingBlob::from_bytes(data)),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        done.push(CompletedPart {
            part_number: Some(i32::try_from(n).unwrap()),
            e_tag: Some(ETag::Strong(etag_of(out.e_tag.as_ref()))),
            ..Default::default()
        });
    }
    let out = c
        .backend
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: BUCKET.into(),
            key: key.into(),
            upload_id,
            multipart_upload: Some(CompletedMultipartUpload { parts: Some(done) }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    (Bytes::from(all), out.version_id)
}

async fn get_version(c: &TestCluster, key: &str, version: &str) -> Bytes {
    let out = c
        .backend
        .get_object(request(GetObjectInput {
            bucket: BUCKET.into(),
            key: key.into(),
            version_id: Some(version.into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    s3::collect(out.body).await.unwrap()
}

async fn scrub(c: &TestCluster) -> arca_lake_controller::integrity::ScrubReport {
    let group = c.groups().await[0].id;
    scrub_group_now(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        group,
        &ScrubOptions {
            page: 8,
            page_bytes: 0,
            rate_bytes_per_s: 0,
            interval_ms: 1,
            concurrency: 1,
            manifest_roots_per_pass: 256,
        },
        now_ms(),
    )
    .await
    .unwrap()
}

async fn manifests(c: &TestCluster) -> (u64, Vec<(GenId, ManifestFinding)>) {
    verify_manifests(c.meta.as_ref()).await.unwrap()
}

#[tokio::test]
async fn multipart_manifests_verify_clean_and_parts_are_scrubbed_repaired_and_kept_by_gc() {
    let c = cluster().await;
    let (data, _) = multipart_put(&c, "obj", 3, 1).await;
    let manifest = c.manifest(BUCKET, "obj").await;
    assert_eq!(manifest.parts.len(), 3);
    assert_eq!(
        c.total_shards().await,
        9,
        "three part chunks of three shards"
    );

    // Manifest level: root, references, part generations, chunk coverage,
    // checksums and layout agree.
    let (roots, findings) = manifests(&c).await;
    assert_eq!((roots, findings.len()), (1, 0), "{findings:?}");
    // Shard level: part chunks are ordinary live chunks.
    let r = scrub(&c).await;
    assert_eq!(r.findings, [0; 8], "{r:?}");
    assert_eq!(r.verified, 9);

    // Corrupt a shard of part 2's chunk: found, quarantined, repaired; the
    // object reads whole before and after.
    let group = c.groups().await.remove(0);
    let part2 = c
        .manifest(BUCKET, "obj")
        .await
        .parts
        .iter()
        .find(|p| p.part_number == 2)
        .unwrap()
        .generation;
    let part_chunks: Vec<records::ChunkRecord> = transact(c.meta.as_ref(), part2, |tx, g| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::gen_chunks_range(g), usize::MAX, false)
                .await?
            {
                out.push(records::decode::<records::ChunkRecord>(&v)?);
            }
            Ok(out)
        })
    })
    .await
    .unwrap();
    let chunk = &part_chunks[0];
    let victim = c.node_index(&group.members[0]).unwrap();
    assert_eq!(c.corrupt_chunk_on_node(victim, group.id, chunk.chunk_id), 1);
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data);
    let r = scrub(&c).await;
    assert_eq!(r.count(Finding::ChecksumMismatch), 1, "{r:?}");
    let r = repair_pass(
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
    assert_eq!((r.succeeded, r.shards_rewritten), (1, 1), "{r:?}");
    assert_eq!(scrub(&c).await.findings, [0; 8]);
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data);

    // GC after Complete never touches a referenced part (I-MP1): a GC pass
    // and the multipart sweep far in the future leave every shard.
    c.gc(now_ms() + 3_600_000, 0).await;
    let sweep = arca_lake_controller::gc::sweep_multipart(
        c.meta.as_ref(),
        now_ms() + 3_600_000,
        Some(now_ms() + 3_600_000),
        100,
    )
    .await
    .unwrap();
    assert_eq!(sweep.expired, 0, "{sweep:?}");
    assert_eq!(c.reconcile(now_ms() + 3_600_000, 0).await, 0);
    assert_eq!(c.total_shards().await, 9);
    assert_eq!(manifests(&c).await.1.len(), 0);
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data);

    // Overwrite: the old root retires, its parts are collected, the new one
    // is intact and verifies clean.
    let (data2, _) = multipart_put(&c, "obj", 2, 2).await;
    // Two passes: the first finishes the root (its part generations
    // retire), the second collects the parts' shards.
    c.gc(now_ms() + 3_600_000, 0).await;
    c.gc(now_ms() + 3_600_000, 0).await;
    assert_eq!(c.total_shards().await, 6);
    let (roots, findings) = manifests(&c).await;
    assert_eq!((roots, findings.len()), (1, 0), "{findings:?}");
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data2);
    assert_eq!(scrub(&c).await.findings, [0; 8]);
    c.shutdown().await;
}

#[tokio::test]
async fn versioned_multipart_objects_and_delete_markers_verify_and_keep_every_version() {
    let c = cluster().await;
    let mut b = PutBucketVersioningInput::builder();
    b.set_bucket(BUCKET.into());
    b.set_versioning_configuration(VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(
            BucketVersioningStatus::ENABLED,
        )),
        ..Default::default()
    });
    c.backend
        .put_bucket_versioning(request(b.build().unwrap()))
        .await
        .unwrap();
    let (v1, v1_id) = multipart_put(&c, "obj", 2, 11).await;
    let (v2, v2_id) = multipart_put(&c, "obj", 3, 12).await;
    let (v1_id, v2_id) = (v1_id.unwrap(), v2_id.unwrap());
    s3::delete(&c.backend, BUCKET, "obj").await.unwrap();
    assert!(
        s3::get(&c.backend, BUCKET, "obj").await.is_err(),
        "delete marker"
    );
    // Both roots are committed versions: verified, scrubbed, kept by GC.
    let (roots, findings) = manifests(&c).await;
    assert_eq!((roots, findings.len()), (2, 0), "{findings:?}");
    assert_eq!(scrub(&c).await.findings, [0; 8]);
    c.gc(now_ms() + 3_600_000, 0).await;
    assert_eq!(c.total_shards().await, 15);
    assert_eq!(get_version(&c, "obj", &v1_id).await, v1);
    assert_eq!(get_version(&c, "obj", &v2_id).await, v2);
    c.shutdown().await;
}

#[tokio::test]
async fn dangling_and_mismatched_part_references_are_findings_and_never_partial_reads() {
    let c = cluster().await;
    let (data, _) = multipart_put(&c, "obj", 3, 21).await;
    let manifest = c.manifest(BUCKET, "obj").await;
    let root = manifest.pointer.generation;
    // Mismatch: the reference of part 1 claims one more byte than its part.
    let mut bad = manifest.parts[0].clone();
    bad.size += 1;
    transact(c.meta.as_ref(), (root, bad.clone()), |tx, env| {
        Box::pin(async move {
            let (root, bad) = env;
            tx.set(
                &keys::part_ref(root, bad.part_number),
                &records::encode(bad)?,
            );
            Ok(())
        })
    })
    .await
    .unwrap();
    let (_, findings) = manifests(&c).await;
    assert!(
        findings
            .iter()
            .any(|(g, f)| *g == root
                && matches!(f, ManifestFinding::Mismatch { part_number: 1, .. })),
        "{findings:?}"
    );
    assert!(
        findings
            .iter()
            .any(|(g, f)| *g == root && matches!(f, ManifestFinding::Layout { .. })),
        "offsets no longer tile the root: {findings:?}"
    );
    // Restore the reference; clean again.
    let good = manifest.parts[0].clone();
    transact(c.meta.as_ref(), (root, good), |tx, env| {
        Box::pin(async move {
            let (root, good) = env;
            tx.set(
                &keys::part_ref(root, good.part_number),
                &records::encode(good)?,
            );
            Ok(())
        })
    })
    .await
    .unwrap();
    assert_eq!(manifests(&c).await.1.len(), 0);
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data);

    // Dangling: part 2's generation header disappears (a metadata loss).
    // The finding is reported and the reader gets an error, not the other
    // parts' bytes (I15).
    let part2 = manifest.parts[1].generation;
    transact(c.meta.as_ref(), part2, |tx, g| {
        Box::pin(async move {
            tx.clear(&keys::gen_header(g));
            Ok(())
        })
    })
    .await
    .unwrap();
    let (_, findings) = manifests(&c).await;
    assert_eq!(
        findings,
        vec![(root, ManifestFinding::Dangling { part_number: 2 })]
    );
    let err = s3::get(&c.backend, BUCKET, "obj").await.unwrap_err();
    assert!(err.to_string().contains("integrity"), "{err}");
    c.shutdown().await;
}

#[tokio::test]
async fn the_background_manifest_scrub_pages_through_a_persisted_cursor() {
    use arca_lake_controller::integrity::scrub_pass;
    use arca_lake_metadata::ops::integrity as itasks;
    let c = cluster().await;
    for i in 0..5u64 {
        multipart_put(&c, &format!("m{i}"), 2, 40 + i).await;
    }
    let opts = ScrubOptions {
        page: 8,
        page_bytes: 0,
        rate_bytes_per_s: 0,
        interval_ms: 3_600_000,
        concurrency: 1,
        manifest_roots_per_pass: 2,
    };
    let pass = |now: u64| {
        let meta = std::sync::Arc::clone(&c.meta);
        let clients = c.clients.clone();
        let opts = opts.clone();
        async move {
            scrub_pass(meta.as_ref(), &clients, &coders(), &opts, None, None, now)
                .await
                .unwrap()
        }
    };
    let t0 = now_ms();
    // Each pass examines one page of two roots and persists the cursor; a
    // controller that takes over simply calls the next pass (the cursor is
    // in metadata, nothing is in memory).
    let r = pass(t0).await;
    assert_eq!(r.manifest_roots, 2);
    let cur = itasks::manifest_cursor(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((cur.pass, cur.roots, cur.done), (1, 2, false));
    assert!(cur.after.is_some());
    // A reference ahead of the cursor goes bad before the scrub reaches it.
    let last = c.manifest(BUCKET, "m4").await;
    let mut bad = last.parts[0].clone();
    bad.size += 1;
    transact(
        c.meta.as_ref(),
        (last.pointer.generation, bad),
        |tx, env| {
            Box::pin(async move {
                let (root, bad) = env;
                tx.set(
                    &keys::part_ref(root, bad.part_number),
                    &records::encode(bad)?,
                );
                Ok(())
            })
        },
    )
    .await
    .unwrap();
    assert_eq!(pass(t0 + 1).await.manifest_roots, 2);
    let r = pass(t0 + 2).await;
    assert_eq!(r.manifest_roots, 1);
    assert!(
        r.manifest_findings
            .iter()
            .any(|(g, _)| *g == last.pointer.generation),
        "{:?}",
        r.manifest_findings
    );
    let cur = itasks::manifest_cursor(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((cur.pass, cur.roots, cur.done), (1, 5, true));
    assert!(cur.findings >= 1);
    // Finite load: a finished pass waits for the interval.
    assert_eq!(pass(t0 + 3).await.manifest_roots, 0);
    let cur2 = itasks::manifest_cursor(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cur2, cur);
    // After the interval a new pass starts from the beginning.
    let r = pass(t0 + 3_600_000 + 10).await;
    assert_eq!(r.manifest_roots, 2);
    let cur = itasks::manifest_cursor(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((cur.pass, cur.roots, cur.done), (2, 2, false));
    c.shutdown().await;
}

#[tokio::test]
async fn object_level_status_names_the_version_part_and_shard_of_a_loss() {
    use arca_lake_controller::objects::{ShardState, Verdict, affected_objects, verify_object};
    use arca_lake_metadata::ops::object::{self, ManifestFound};
    let c = cluster().await;
    let mut b = PutBucketVersioningInput::builder();
    b.set_bucket(BUCKET.into());
    b.set_versioning_configuration(VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(
            BucketVersioningStatus::ENABLED,
        )),
        ..Default::default()
    });
    c.backend
        .put_bucket_versioning(request(b.build().unwrap()))
        .await
        .unwrap();
    // v1: a two-part multipart object; then a plain current version.
    let (_, v1_id) = multipart_put(&c, "obj", 2, 31).await;
    let v1_id = v1_id.unwrap();
    let current = Bytes::from(random_bytes(PART, 32));
    s3::put(
        &c.backend,
        BUCKET,
        "obj",
        current.clone(),
        &s3::PutOptions::default(),
    )
    .await
    .unwrap();
    let ManifestFound::Manifest(m1) =
        object::read_manifest_version(c.meta.as_ref(), BUCKET, "obj", Some(&v1_id), None)
            .await
            .unwrap()
    else {
        panic!("v1 missing")
    };
    let group = c.groups().await.remove(0);
    let part_chunk = |n: usize| {
        let g = m1.parts[n].generation;
        let meta = std::sync::Arc::clone(&c.meta);
        async move {
            object::read_part_manifest(meta.as_ref(), &g)
                .await
                .unwrap()
                .unwrap()
                .0[0]
                .1
                .clone()
        }
    };
    let (p1, p2) = (part_chunk(0).await, part_chunk(1).await);
    let del = |ch: &records::ChunkRecord, i: usize| {
        let n = c.node_index(&group.members[i]).unwrap();
        c.node_store(n)
            .delete(arca_lake_storage_node::ShardKey {
                group_id: group.id,
                chunk_id: ch.chunk_id,
                shard_index: u16::try_from(i).unwrap(),
            })
            .unwrap();
    };
    // Part 1 loses two of three shards (unreadable); part 2 loses one.
    del(&p1, 0);
    del(&p1, 1);
    del(&p2, 2);

    let r = verify_object(c.meta.as_ref(), &c.clients, BUCKET, "obj", Some(&v1_id))
        .await
        .unwrap();
    assert!(r.multipart);
    assert_eq!(r.version_id, v1_id);
    assert_eq!(r.verdict, Verdict::Unreadable);
    let c1 = r.chunks.iter().find(|x| x.part_number == Some(1)).unwrap();
    assert_eq!((c1.good(), c1.need), (1, 2));
    assert_eq!(c1.shards[0].1, ShardState::Missing);
    assert_eq!(c1.shards[1].1, ShardState::Missing);
    let c2 = r.chunks.iter().find(|x| x.part_number == Some(2)).unwrap();
    assert_eq!(c2.good(), 2);
    assert_eq!(c2.shards[2].1, ShardState::Missing);
    // The current version is untouched.
    let cur = verify_object(c.meta.as_ref(), &c.clients, BUCKET, "obj", None)
        .await
        .unwrap();
    assert_eq!((cur.verdict, cur.multipart), (Verdict::Healthy, false));
    assert_ne!(cur.version_id, v1_id);

    // Scrub finds both, repair rebuilds part 2 and records part 1 as
    // unrecoverable; the object-level status resolves it to the
    // noncurrent version and the part.
    scrub(&c).await;
    let rep = repair_pass(
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
    assert_eq!((rep.succeeded, rep.unrecoverable), (1, 1), "{rep:?}");
    let affected = affected_objects(c.meta.as_ref()).await.unwrap();
    assert_eq!(affected.len(), 1, "{affected:?}");
    let a = &affected[0];
    assert_eq!((a.bucket.as_str(), a.key.as_str()), (BUCKET, "obj"));
    assert_eq!(a.version_id.as_deref(), Some(v1_id.as_str()));
    assert!(!a.is_current);
    assert_eq!((a.part_number, a.chunk_index), (Some(1), 0));
    assert_eq!(a.chunk_id, p1.chunk_id);
    let r = verify_object(c.meta.as_ref(), &c.clients, BUCKET, "obj", Some(&v1_id))
        .await
        .unwrap();
    let c2 = r.chunks.iter().find(|x| x.part_number == Some(2)).unwrap();
    assert_eq!(c2.good(), 3, "part 2 repaired");
    assert_eq!(r.verdict, Verdict::Unreadable, "part 1 is lost");
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, current);
    c.shutdown().await;
}
