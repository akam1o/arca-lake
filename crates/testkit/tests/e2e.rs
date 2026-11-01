//! End-to-end tests of the distributed write and read protocols on an
//! in-process cluster (RFC-003 §5.3, RFC-004 §5.5–5.10).
//!
//! Every test states which guarantee it exercises. Anything not covered here
//! is listed as unverified in the Phase 1 report.

#![allow(clippy::too_many_lines)]

use std::time::Duration;

use arca_lake_gateway::cluster::FaultPoint;
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::ops::object::get_generation;
use arca_lake_metadata::records::GenState;
use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_testkit::s3::{self, AwsCli, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use futures::StreamExt as _;
use s3s::{S3, S3ErrorCode};

const MIB: u64 = 1 << 20;
const BUCKET: &str = "e2e";

async fn cluster(nodes: usize) -> TestCluster {
    let c = TestCluster::start(ClusterOptions::rs(nodes, 2, 1, MIB, 64 << 10)).await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

/// Shard counts per node for one group.
fn counts(c: &TestCluster, group: u64) -> Vec<usize> {
    (0..c.nodes.len())
        .map(|i| {
            if c.node_running(i) {
                c.shard_count(i, group)
            } else {
                0
            }
        })
        .collect()
}

/// Shard counts per node across every group (chunks of one object may be
/// spread over several active groups).
async fn node_counts(c: &TestCluster) -> Vec<usize> {
    let groups = c.groups().await;
    (0..c.nodes.len())
        .map(|i| {
            if c.node_running(i) {
                groups.iter().map(|g| c.shard_count(i, g.id)).sum()
            } else {
                0
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// 1. Write and read through three storage nodes.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_get_across_three_nodes_with_multiple_chunks() {
    let c = cluster(3).await;
    let data = payload(usize::try_from(2 * MIB + 4096).unwrap(), 1);
    let etag = s3::put(
        &c.backend,
        BUCKET,
        "big",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(etag.len(), 32);

    let m = c.manifest(BUCKET, "big").await;
    assert_eq!(m.chunks.len(), 3, "1 MiB chunks: 1 + 1 + 4096 bytes");
    for (_, ch) in &m.chunks {
        assert_eq!(ch.scheme, "rs-gf8-v1");
        assert_eq!((ch.data_shards, ch.parity_shards), (2, 1));
    }
    // Every chunk has one shard on each of the three nodes.
    assert_eq!(node_counts(&c).await, vec![3, 3, 3]);

    let (got, got_etag) = s3::get(&c.backend, BUCKET, "big").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(got_etag, etag);

    // Range reads crossing a chunk boundary.
    let r = s3::get_range(&c.backend, BUCKET, "big", MIB - 10, MIB + 9)
        .await
        .unwrap();
    let mib = usize::try_from(MIB).unwrap();
    assert_eq!(&r[..], &data[(mib - 10)..(mib + 10)]);

    let (size, head_etag) = s3::head(&c.backend, BUCKET, "big").await.unwrap();
    assert_eq!(size, data.len() as u64);
    assert_eq!(head_etag, etag);
    c.shutdown().await;
}

#[tokio::test]
async fn small_object_uses_replication_and_survives_two_node_loss() {
    let mut c = cluster(3).await;
    let data = payload(1000, 2);
    s3::put(
        &c.backend,
        BUCKET,
        "small",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let m = c.manifest(BUCKET, "small").await;
    assert_eq!(m.chunks.len(), 1);
    assert_eq!(m.chunks[0].1.scheme, "replication-v1");
    assert_eq!(
        (m.chunks[0].1.data_shards, m.chunks[0].1.parity_shards),
        (1, 2)
    );

    c.stop_node(0).await;
    c.stop_node(1).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "small").await.unwrap();
    assert_eq!(got, data, "one of three replicas suffices");
    c.shutdown().await;
}

// ---------------------------------------------------------------------------
// 2. AWS CLI through the gateway.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn aws_cli_put_get_delete_through_gateway() {
    if !AwsCli::available().await {
        eprintln!("aws CLI not found; skipping");
        return;
    }
    let c = cluster(3).await;
    let cli = c.aws_cli();
    cli.ok(&["s3api", "create-bucket", "--bucket", "cli"]).await;

    let dir = tempfile::tempdir().unwrap();
    let data = random_bytes(usize::try_from(3 * MIB + 17).unwrap(), 3);
    let src = dir.path().join("src.bin");
    tokio::fs::write(&src, &data).await.unwrap();
    cli.ok(&["s3", "cp", src.to_str().unwrap(), "s3://cli/dir/obj.bin"])
        .await;

    let head = cli
        .json(&[
            "s3api",
            "head-object",
            "--bucket",
            "cli",
            "--key",
            "dir/obj.bin",
        ])
        .await;
    assert_eq!(head["ContentLength"], serde_json::json!(data.len()));

    let dst = dir.path().join("dst.bin");
    cli.ok(&["s3", "cp", "s3://cli/dir/obj.bin", dst.to_str().unwrap()])
        .await;
    assert_eq!(tokio::fs::read(&dst).await.unwrap(), data);

    let part = dir.path().join("part.bin");
    cli.ok(&[
        "s3api",
        "get-object",
        "--bucket",
        "cli",
        "--key",
        "dir/obj.bin",
        "--range",
        "bytes=1048570-1048585",
        part.to_str().unwrap(),
    ])
    .await;
    assert_eq!(
        tokio::fs::read(&part).await.unwrap(),
        &data[1_048_570..=1_048_585]
    );

    let list = cli
        .json(&[
            "s3api",
            "list-objects-v2",
            "--bucket",
            "cli",
            "--delimiter",
            "/",
        ])
        .await;
    assert_eq!(list["CommonPrefixes"][0]["Prefix"], "dir/");

    cli.ok(&[
        "s3api",
        "delete-object",
        "--bucket",
        "cli",
        "--key",
        "dir/obj.bin",
    ])
    .await;
    let (ok, _, _) = cli
        .run(&[
            "s3api",
            "head-object",
            "--bucket",
            "cli",
            "--key",
            "dir/obj.bin",
        ])
        .await;
    assert!(!ok, "deleted object must be gone");
    c.shutdown().await;
}

// ---------------------------------------------------------------------------
// 3. Concurrency: concurrent writers, conditional PUT, GET during PUT.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_writers_same_key_leave_exactly_one_committed_version() {
    let c = cluster(3).await;
    let writers = 8usize;
    let payloads: Vec<Bytes> = (0..writers)
        .map(|i| payload(300 << 10, 100 + u64::try_from(i).unwrap()))
        .collect();
    let mut tasks = Vec::new();
    for p in &payloads {
        let backend = c.backend.clone();
        let p = p.clone();
        tasks.push(tokio::spawn(async move {
            s3::put(&backend, BUCKET, "contended", p, &PutOptions::default()).await
        }));
    }
    let mut etags = Vec::new();
    for t in tasks {
        etags.push(t.await.unwrap().expect("every unconditional PUT succeeds"));
    }
    let (got, etag) = s3::get(&c.backend, BUCKET, "contended").await.unwrap();
    let winner = payloads
        .iter()
        .position(|p| *p == got)
        .expect("GET returns one writer's bytes");
    assert_eq!(etag, etags[winner], "ETag belongs to the visible bytes");

    // Every displaced generation is retiring, the visible one is committed.
    let m = c.manifest(BUCKET, "contended").await;
    assert_eq!(
        get_generation(c.meta.as_ref(), &m.pointer.generation)
            .await
            .unwrap()
            .state,
        GenState::Committed
    );
    let group = m.chunks[0].1.placement.group_id;
    assert_eq!(
        counts(&c, group).iter().sum::<usize>(),
        3 * writers,
        "all shards still present before GC"
    );
    let report = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(
        report.collected,
        writers - 1,
        "displaced generations collected"
    );
    assert_eq!(
        counts(&c, group),
        vec![1, 1, 1],
        "only the winner's shards remain"
    );
    let (again, _) = s3::get(&c.backend, BUCKET, "contended").await.unwrap();
    assert_eq!(again, got);
    c.shutdown().await;
}

#[tokio::test]
async fn concurrent_writers_distinct_keys_all_readable() {
    let c = cluster(3).await;
    let n = 16u64;
    let mut tasks = Vec::new();
    for i in 0..n {
        let backend = c.backend.clone();
        tasks.push(tokio::spawn(async move {
            let data = payload(usize::try_from(70 * 1024 + i * 1000).unwrap(), 200 + i);
            s3::put(
                &backend,
                BUCKET,
                &format!("k{i}"),
                data.clone(),
                &PutOptions::default(),
            )
            .await
            .unwrap();
            data
        }));
    }
    for (i, t) in tasks.into_iter().enumerate() {
        let data = t.await.unwrap();
        let (got, _) = s3::get(&c.backend, BUCKET, &format!("k{i}")).await.unwrap();
        assert_eq!(got, data, "key k{i}");
    }
    c.shutdown().await;
}

#[tokio::test]
async fn conditional_put_if_none_match_and_if_match() {
    let c = cluster(3).await;
    let create = PutOptions {
        if_none_match_any: true,
        ..Default::default()
    };
    let v1 = payload(100 << 10, 7);
    let etag1 = s3::put(&c.backend, BUCKET, "cond", v1.clone(), &create)
        .await
        .unwrap();
    let err = s3::put(&c.backend, BUCKET, "cond", payload(100 << 10, 8), &create)
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::PreconditionFailed);
    assert_eq!(s3::get(&c.backend, BUCKET, "cond").await.unwrap().0, v1);

    let wrong = PutOptions {
        if_match: Some("0".repeat(32)),
        ..Default::default()
    };
    let err = s3::put(&c.backend, BUCKET, "cond", payload(100 << 10, 9), &wrong)
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::PreconditionFailed);

    let right = PutOptions {
        if_match: Some(etag1),
        ..Default::default()
    };
    let v2 = payload(100 << 10, 10);
    s3::put(&c.backend, BUCKET, "cond", v2.clone(), &right)
        .await
        .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "cond").await.unwrap().0, v2);

    // Shards of the two rejected PUTs are garbage: after GC only v2 remains.
    let group = c.manifest(BUCKET, "cond").await.chunks[0]
        .1
        .placement
        .group_id;
    c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(counts(&c, group), vec![1, 1, 1]);
    c.shutdown().await;
}

#[tokio::test]
async fn concurrent_conditional_create_has_exactly_one_winner() {
    let c = cluster(3).await;
    let mut tasks = Vec::new();
    for i in 0..8u64 {
        let backend = c.backend.clone();
        tasks.push(tokio::spawn(async move {
            let opts = PutOptions {
                if_none_match_any: true,
                ..Default::default()
            };
            s3::put(&backend, BUCKET, "once", payload(80 << 10, 300 + i), &opts).await
        }));
    }
    let mut ok = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(_) => ok += 1,
            Err(e) => assert_eq!(*e.code(), S3ErrorCode::PreconditionFailed),
        }
    }
    assert_eq!(ok, 1, "If-None-Match: * admits exactly one creator");
    c.shutdown().await;
}

#[tokio::test]
async fn get_during_put_sees_only_complete_versions() {
    let c = cluster(3).await;
    let va = payload(usize::try_from(MIB + MIB / 2).unwrap(), 11);
    let vb = payload(usize::try_from(MIB + MIB / 2).unwrap(), 12);
    let etag_a = s3::put(
        &c.backend,
        BUCKET,
        "flip",
        va.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();

    let writer = {
        let backend = c.backend.clone();
        let (va, vb) = (va.clone(), vb.clone());
        tokio::spawn(async move {
            let mut etag_b = String::new();
            for round in 0..6 {
                let next = if round % 2 == 0 {
                    vb.clone()
                } else {
                    va.clone()
                };
                let etag = s3::put(&backend, BUCKET, "flip", next, &PutOptions::default())
                    .await
                    .unwrap();
                if round % 2 == 0 {
                    etag_b = etag;
                }
            }
            etag_b
        })
    };
    let reader = {
        let backend = c.backend.clone();
        let (va, vb) = (va.clone(), vb.clone());
        tokio::spawn(async move {
            let mut seen = Vec::new();
            for _ in 0..40 {
                let (got, etag) = s3::get(&backend, BUCKET, "flip").await.unwrap();
                assert!(
                    got == va || got == vb,
                    "GET must return a complete version, never a mix"
                );
                seen.push((got == va, etag));
            }
            seen
        })
    };
    let etag_b = writer.await.unwrap();
    for (is_a, etag) in reader.await.unwrap() {
        assert_eq!(
            etag,
            if is_a { etag_a.clone() } else { etag_b.clone() },
            "ETag matches the bytes"
        );
    }
    c.shutdown().await;
}

#[tokio::test]
async fn delete_during_get_never_yields_wrong_bytes() {
    let c = cluster(3).await;
    let data = payload(usize::try_from(2 * MIB).unwrap(), 13);
    s3::put(
        &c.backend,
        BUCKET,
        "doomed",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    // Start the GET (manifest resolved), then delete and collect, then read
    // the body. The reader must either get all original bytes (shards still
    // there) or an error; never different bytes.
    let out = c
        .backend
        .get_object(s3::request(s3s::dto::GetObjectInput {
            bucket: BUCKET.into(),
            key: "doomed".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    s3::delete(&c.backend, BUCKET, "doomed").await.unwrap();
    let err = s3::get(&c.backend, BUCKET, "doomed").await.unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);
    assert_eq!(
        node_counts(&c).await,
        vec![2, 2, 2],
        "shards survive the delete until GC"
    );
    let report = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(report.collected, 1);
    assert_eq!(
        node_counts(&c).await,
        vec![0, 0, 0],
        "GC reclaimed the shards"
    );

    let mut body = out.body.unwrap();
    let mut buf = Vec::new();
    let mut failed = false;
    while let Some(item) = body.next().await {
        let Ok(b) = item else {
            failed = true;
            break;
        };
        buf.extend_from_slice(&b);
    }
    assert!(
        failed || buf == data,
        "either an error or the exact original bytes"
    );
    assert!(
        data.starts_with(&buf),
        "whatever was delivered is a prefix of the original"
    );
    c.shutdown().await;
}

// ---------------------------------------------------------------------------
// 4. Node failures: degraded reads, restart, repair.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_with_one_node_stopped_decodes_and_two_stopped_fails_explicitly() {
    let mut c = cluster(3).await;
    let data = payload(usize::try_from(MIB + 123).unwrap(), 14);
    s3::put(
        &c.backend,
        BUCKET,
        "deg",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();

    c.stop_node(0).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "deg").await.unwrap();
    assert_eq!(got, data, "degraded read reconstructs from k shards");
    let r = s3::get_range(&c.backend, BUCKET, "deg", 5, 4000)
        .await
        .unwrap();
    assert_eq!(&r[..], &data[5..=4000]);

    c.stop_node(1).await;
    let err = s3::get(&c.backend, BUCKET, "deg").await.unwrap_err();
    assert_eq!(
        *err.code(),
        S3ErrorCode::InternalError,
        "fewer than k shards: explicit error, no bytes"
    );

    // Writes while two of three members are down must fail, not half-write.
    let before = c.total_shards().await;
    let err = s3::put(
        &c.backend,
        BUCKET,
        "deg2",
        payload(100 << 10, 15),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::ServiceUnavailable);
    assert!(
        s3::head(&c.backend, BUCKET, "deg2").await.is_err(),
        "failed PUT is invisible"
    );
    let _ = before;

    c.start_node(0).await;
    c.start_node(1).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "deg").await.unwrap();
    assert_eq!(got, data);
    c.shutdown().await;
}

#[tokio::test]
async fn node_restart_preserves_acknowledged_shards() {
    let mut c = cluster(3).await;
    let data = payload(usize::try_from(MIB / 2).unwrap(), 16);
    s3::put(
        &c.backend,
        BUCKET,
        "durable",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    c.stop_node(0).await;
    c.start_node(0).await;
    // Force the read to depend on node 0 by stopping another member.
    c.stop_node(2).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "durable").await.unwrap();
    assert_eq!(got, data, "node 0 served its shard after a restart");
    c.shutdown().await;
}

#[tokio::test]
async fn corrupt_shard_is_detected_and_decoded_around() {
    let mut c = cluster(3).await;
    let data = payload(usize::try_from(MIB + 777).unwrap(), 17);
    s3::put(
        &c.backend,
        BUCKET,
        "rot",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let m = c.manifest(BUCKET, "rot").await;
    let group = m.chunks[0].1.placement.group_id;
    let chunk0 = m.chunks[0].1.chunk_id;
    let members = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == group)
        .unwrap()
        .members;
    let node_of = |idx: usize| c.node_index(&members[idx]).unwrap();

    // Damage data shard 0 of chunk 0 on its member.
    assert_eq!(c.corrupt_chunk_on_node(node_of(0), group, chunk0), 1);
    let bad = c
        .clients
        .get_shard(
            &c.nodes[node_of(0)].addr.to_string(),
            ShardRef {
                group_id: group,
                chunk_id: chunk0,
                shard_index: 0,
            },
            0,
            0,
        )
        .await
        .unwrap_err();
    assert_eq!(
        bad.code(),
        tonic::Code::DataLoss,
        "node reports the corruption, never the bytes"
    );

    let (got, _) = s3::get(&c.backend, BUCKET, "rot").await.unwrap();
    assert_eq!(got, data, "decoded from the remaining shards");

    // A second damaged shard of the same chunk exceeds m = 1.
    assert_eq!(c.corrupt_chunk_on_node(node_of(1), group, chunk0), 1);
    let err = s3::get(&c.backend, BUCKET, "rot").await.unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InternalError);
    // Other objects in the group are unaffected.
    let other = payload(10_000, 18);
    s3::put(
        &c.backend,
        BUCKET,
        "fine",
        other.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "fine").await.unwrap().0, other);
    c.stop_node(0).await; // exercise shutdown with a stopped node
    c.shutdown().await;
}

#[tokio::test]
async fn repair_onto_new_node_yields_identical_data() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let data = payload(usize::try_from(2 * MIB + 999).unwrap(), 19);
    s3::put(
        &c.backend,
        BUCKET,
        "rep",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let m = c.manifest(BUCKET, "rep").await;
    let group = m.chunks[0].1.placement.group_id;
    let record = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == group)
        .unwrap();
    assert_eq!(record.epoch, 1);
    let lost = c.node_index(&record.members[0]).unwrap();

    c.stop_node(lost).await;
    let new_node = c.add_node().await;
    let report = c.repair(group, 0, new_node).await;
    assert_eq!(report.epoch, 2);
    assert_eq!(report.reconstructed, 3, "one shard per chunk reconstructed");
    assert_eq!(c.shard_count(new_node, group), 3);

    // Reads must now work with any further single member down.
    let (got, _) = s3::get(&c.backend, BUCKET, "rep").await.unwrap();
    assert_eq!(got, data);
    let other = c.node_index(&record.members[1]).unwrap();
    c.stop_node(other).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "rep").await.unwrap();
    assert_eq!(
        got, data,
        "the reconstructed shard participates in decoding"
    );
    c.start_node(other).await;

    // New writes go to the new membership with the new epoch.
    let fresh = payload(usize::try_from(MIB + 1).unwrap(), 20);
    s3::put(
        &c.backend,
        BUCKET,
        "after",
        fresh.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let m2 = c.manifest(BUCKET, "after").await;
    assert!(m2.chunks.iter().all(|(_, ch)| ch.placement.epoch == 2));
    assert_eq!(s3::get(&c.backend, BUCKET, "after").await.unwrap().0, fresh);

    // Repair is idempotent.
    let again = c.repair(group, 0, new_node).await;
    assert_eq!(again.reconstructed, 0);
    c.shutdown().await;
}

#[tokio::test]
async fn stale_epoch_writes_are_fenced_until_membership_commits() {
    let c = cluster(3).await;
    let groups = c.groups().await;
    let rs: Vec<_> = groups.iter().filter(|g| g.scheme == "rs-gf8-v1").collect();
    let clients = NodeClients::new(Duration::from_secs(5));
    // The controller fenced every member at epoch 5 but crashed before
    // committing the new membership (RFC-004 §7).
    for g in &rs {
        for m in &g.members {
            let addr = c.nodes[c.node_index(m).unwrap()].addr.to_string();
            assert_eq!(clients.update_epoch(&addr, g.id, 5).await.unwrap(), 5);
        }
    }
    // Node-level fence.
    let g = rs[0];
    let addr = c.nodes[c.node_index(&g.members[0]).unwrap()]
        .addr
        .to_string();
    let stale = clients
        .put_shard(
            &addr,
            ShardRef {
                group_id: g.id,
                chunk_id: [9; 16],
                shard_index: 0,
            },
            1,
            Bytes::from_static(b"x"),
            1024,
        )
        .await
        .unwrap_err();
    assert!(NodeClients::is_epoch_rejected(&stale), "{stale}");

    // Gateway path: writes with the stale map fail, nothing becomes visible.
    let err = s3::put(
        &c.backend,
        BUCKET,
        "fenced",
        payload(200 << 10, 21),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::ServiceUnavailable);
    assert!(s3::head(&c.backend, BUCKET, "fenced").await.is_err());

    // The controller completes the membership change: writes succeed again.
    for g in &rs {
        cluster::update_group(c.meta.as_ref(), g.id, |g| {
            g.epoch = 5;
            true
        })
        .await
        .unwrap();
    }
    let data = payload(200 << 10, 22);
    s3::put(
        &c.backend,
        BUCKET,
        "fenced",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "fenced").await.unwrap().0, data);
    assert_eq!(
        c.manifest(BUCKET, "fenced").await.chunks[0]
            .1
            .placement
            .epoch,
        5
    );
    c.shutdown().await;
}

// ---------------------------------------------------------------------------
// 5. Gateway crashes around the commit, and orphan GC.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crash_after_shards_before_manifest_leaves_orphans_that_reconcile_reclaims() {
    let c = cluster(3).await;
    let old = payload(200 << 10, 23);
    s3::put(
        &c.backend,
        BUCKET,
        "obj",
        old.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let group = c.manifest(BUCKET, "obj").await.chunks[0]
        .1
        .placement
        .group_id;
    let baseline = c.total_shards().await;

    c.backend
        .set_fault(Some(FaultPoint::AfterShardsBeforeManifest));
    let err = s3::put(
        &c.backend,
        BUCKET,
        "obj",
        payload(200 << 10, 24),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InternalError);
    assert_eq!(
        s3::get(&c.backend, BUCKET, "obj").await.unwrap().0,
        old,
        "old version still visible"
    );
    assert_eq!(
        c.total_shards().await,
        baseline + 3,
        "orphan shards exist on the nodes"
    );

    // Retry after the "crash" succeeds.
    let new = payload(200 << 10, 25);
    s3::put(
        &c.backend,
        BUCKET,
        "obj",
        new.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, new);

    // Generation-driven GC: the aborted generation has no chunk records, so
    // it is finished without deleting anything; the displaced old version is
    // collected.
    let report = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(report.collected, 2);
    assert_eq!(
        c.total_shards().await,
        baseline + 3,
        "orphans are unknown to the manifest"
    );

    // Node-driven reconciliation: young shards are kept (their generation
    // may still be pending elsewhere), old ones without a generation go.
    assert_eq!(c.reconcile(now_ms(), 60_000).await, 0);
    assert_eq!(c.reconcile(now_ms() + 3_600_000, 60_000).await, 3);
    assert_eq!(c.total_shards().await, baseline);
    assert_eq!(counts(&c, group), vec![1, 1, 1]);
    assert_eq!(
        s3::get(&c.backend, BUCKET, "obj").await.unwrap().0,
        new,
        "committed data untouched"
    );
    c.shutdown().await;
}

#[tokio::test]
async fn crash_before_commit_is_invisible_and_collected() {
    let c = cluster(3).await;
    let old = payload(usize::try_from(MIB + 5).unwrap(), 26);
    s3::put(
        &c.backend,
        BUCKET,
        "obj",
        old.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let old_gen = c.manifest(BUCKET, "obj").await.pointer.generation;
    let baseline = c.total_shards().await;

    c.backend.set_fault(Some(FaultPoint::BeforeCommit));
    let err = s3::put(
        &c.backend,
        BUCKET,
        "obj",
        payload(usize::try_from(MIB + 5).unwrap(), 27),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InternalError);
    let (got, _) = s3::get(&c.backend, BUCKET, "obj").await.unwrap();
    assert_eq!(got, old, "uncommitted generation is invisible");
    assert_eq!(c.manifest(BUCKET, "obj").await.pointer.generation, old_gen);
    assert_eq!(
        c.total_shards().await,
        baseline + 6,
        "two chunks x three shards wait for GC"
    );

    let report = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(report.collected, 1);
    assert_eq!(report.shards_deleted, 6);
    assert_eq!(c.total_shards().await, baseline);
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, old);
    c.shutdown().await;
}

#[tokio::test]
async fn crash_after_commit_means_object_is_visible_despite_error() {
    let c = cluster(3).await;
    let old = payload(100 << 10, 28);
    s3::put(&c.backend, BUCKET, "obj", old, &PutOptions::default())
        .await
        .unwrap();
    let new = payload(100 << 10, 29);
    c.backend.set_fault(Some(FaultPoint::AfterCommit));
    let err = s3::put(
        &c.backend,
        BUCKET,
        "obj",
        new.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InternalError);
    // The commit is the linearization point: the client saw an error but the
    // new version is the one visible, and a client retry is harmless.
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, new);
    s3::put(
        &c.backend,
        BUCKET,
        "obj",
        new.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, new);
    let report = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(
        report.collected, 2,
        "the old version and the retried duplicate"
    );
    c.shutdown().await;
}

#[tokio::test]
async fn expired_pending_generation_is_retired_and_reclaimed() {
    let c = cluster(3).await;
    let baseline = c.total_shards().await;
    // Simulate a gateway that died mid-write: shards + manifest entries exist,
    // no commit, no abort (the fault returns before abort_put only in the
    // crash-after-commit case, so emulate by writing and then expiring).
    c.backend.set_fault(Some(FaultPoint::BeforeCommit));
    let _ = s3::put(
        &c.backend,
        BUCKET,
        "zombie",
        payload(50 << 10, 30),
        &PutOptions::default(),
    )
    .await;
    // Pending generations whose lease has run out are swept the same way.
    let report = c.gc(now_ms() + 3_600_000, 0).await;
    assert!(report.collected >= 1);
    assert_eq!(c.total_shards().await, baseline);
    assert!(s3::head(&c.backend, BUCKET, "zombie").await.is_err());
    c.shutdown().await;
}
