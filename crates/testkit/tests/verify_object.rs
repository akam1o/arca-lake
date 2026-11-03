//! `admin integrity verify-object` (Phase 3.4 §9): read-only by default
//! (a corrupt shard is reported, nothing is quarantined), quarantine only
//! when asked, and `--decode` checks the object end to end: it decodes
//! around a corrupt shard and reports records that do not match the data
//! (ETag, part layout).

#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use arca_lake_controller::objects::{ShardState, Verdict, VerifyMode, verify_object};
use arca_lake_gateway::verify::{GoodShards, decode_object};
use arca_lake_metadata::records::{GenerationHeader, PartRef, decode, encode};
use arca_lake_metadata::{Txn, keys, transact};
use arca_lake_testkit::s3::{self, PutOptions, request};
use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use bytes::Bytes;
use s3s::S3;
use s3s::dto::{
    CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadInput, ETag, StreamingBlob, UploadPartInput,
};

const MIB: u64 = 1 << 20;
const BUCKET: &str = "verify";

async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn good_of(r: &arca_lake_controller::objects::ObjectCheck) -> GoodShards {
    r.chunks
        .iter()
        .map(|c| {
            (
                (c.generation, c.chunk_index),
                c.shards
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, s))| *s == ShardState::Ok)
                    .map(|(i, _)| i)
                    .collect(),
            )
        })
        .collect()
}

fn quarantined(c: &TestCluster) -> usize {
    (0..c.nodes.len())
        .map(|i| c.node_store(i).quarantine_stats().0)
        .sum()
}

#[tokio::test]
async fn verification_is_read_only_unless_asked_and_decode_goes_around_a_corrupt_shard() {
    let c = cluster().await;
    let data = Bytes::from(random_bytes(usize::try_from(3 * MIB / 2).unwrap(), 1));
    s3::put(
        &c.backend,
        BUCKET,
        "obj",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let ch = c.manifest(BUCKET, "obj").await.chunks[0].1.clone();
    let group = c.groups().await.remove(0);
    let n0 = c.node_index(&group.members[0]).unwrap();
    c.corrupt_chunk_on_node(n0, group.id, ch.chunk_id);

    // Read-only: reported, nothing quarantined, twice.
    for _ in 0..2 {
        let r = verify_object(
            c.meta.as_ref(),
            &c.clients,
            BUCKET,
            "obj",
            None,
            VerifyMode::ReadOnly,
        )
        .await
        .unwrap();
        assert_eq!(r.verdict, Verdict::Degraded);
        assert_eq!(r.chunks[0].shards[0].1, ShardState::Corrupt);
        assert_eq!(quarantined(&c), 0, "a read-only check quarantined a shard");
        // Decoding uses only the shards that verified good.
        let d = decode_object(
            c.meta.as_ref(),
            &c.clients,
            BUCKET,
            "obj",
            None,
            &good_of(&r),
        )
        .await
        .unwrap();
        assert!(d.ok(), "{d:?}");
        assert_eq!(d.bytes, data.len() as u64);
        assert_eq!(quarantined(&c), 0, "decoding quarantined a shard");
    }
    // Asked to: the node quarantines it (as a scrub would).
    let r = verify_object(
        c.meta.as_ref(),
        &c.clients,
        BUCKET,
        "obj",
        None,
        VerifyMode::Quarantine,
    )
    .await
    .unwrap();
    assert_eq!(r.chunks[0].shards[0].1, ShardState::Corrupt);
    assert_eq!(quarantined(&c), 1);
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data);
    c.shutdown().await;
}

#[tokio::test]
async fn decode_reports_records_that_do_not_match_the_data() {
    let c = cluster().await;
    // A single-part object whose recorded ETag is altered.
    let data = Bytes::from(random_bytes(usize::try_from(MIB / 2 + 7).unwrap(), 2));
    s3::put(&c.backend, BUCKET, "single", data, &PutOptions::default())
        .await
        .unwrap();
    let g = c.manifest(BUCKET, "single").await.pointer.generation;
    transact(c.meta.as_ref(), g, |tx, g| {
        Box::pin(async move {
            let k = keys::gen_header(g);
            let mut h: GenerationHeader = decode(&tx.get(&k).await?.unwrap())?;
            h.etag = "00000000000000000000000000000000".into();
            tx.set(&k, &encode(&h)?);
            Ok(())
        })
    })
    .await
    .unwrap();
    let d = decode_object(
        c.meta.as_ref(),
        &c.clients,
        BUCKET,
        "single",
        None,
        &GoodShards::new(),
    )
    .await
    .unwrap();
    assert!(!d.ok());
    assert!(
        d.checks.iter().any(|(w, ok)| w.contains("ETag") && !ok),
        "{d:?}"
    );
    assert_eq!(d.chunk_errors.len(), 0, "the data itself decodes");

    // A multipart object whose second part's recorded offset is altered.
    let up = c
        .backend
        .create_multipart_upload(request(CreateMultipartUploadInput {
            bucket: BUCKET.into(),
            key: "mp".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let mut parts = Vec::new();
    for n in 1..=2i32 {
        let body = Bytes::from(random_bytes(
            usize::try_from(5 * MIB + 3).unwrap(),
            10 + u64::try_from(n).unwrap(),
        ));
        let out = c
            .backend
            .upload_part(request(UploadPartInput {
                bucket: BUCKET.into(),
                key: "mp".into(),
                upload_id: up.clone(),
                part_number: n,
                body: Some(StreamingBlob::from_bytes(body)),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        parts.push(CompletedPart {
            part_number: Some(n),
            e_tag: out.e_tag.clone().or(Some(ETag::Strong(String::new()))),
            ..Default::default()
        });
    }
    c.backend
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: BUCKET.into(),
            key: "mp".into(),
            upload_id: up,
            multipart_upload: Some(CompletedMultipartUpload { parts: Some(parts) }),
            ..Default::default()
        }))
        .await
        .unwrap();
    let clean = decode_object(
        c.meta.as_ref(),
        &c.clients,
        BUCKET,
        "mp",
        None,
        &GoodShards::new(),
    )
    .await
    .unwrap();
    assert!(clean.ok(), "{clean:?}");
    let root = c.manifest(BUCKET, "mp").await.pointer.generation;
    transact(c.meta.as_ref(), root, |tx, root| {
        Box::pin(async move {
            let k = keys::part_ref(root, 2);
            let mut p: PartRef = decode(&tx.get(&k).await?.unwrap())?;
            p.offset += 1;
            tx.set(&k, &encode(&p)?);
            Ok(())
        })
    })
    .await
    .unwrap();
    let d = decode_object(
        c.meta.as_ref(),
        &c.clients,
        BUCKET,
        "mp",
        None,
        &GoodShards::new(),
    )
    .await
    .unwrap();
    assert!(!d.ok());
    assert!(
        d.layout_errors.iter().any(|e| e.contains("part 2: offset")),
        "{d:?}"
    );
    c.shutdown().await;
}
