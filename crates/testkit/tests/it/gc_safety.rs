//! Garbage collection never deletes what metadata still accounts for
//! (RFC-008): decisions are made on generation state and references, with
//! time only as an additional guard.

#![allow(clippy::too_many_lines)]

use arca_lake_controller::gc;
use arca_lake_gateway::cluster::FaultPoint;
use arca_lake_metadata::{KvStore, Txn, keys};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::S3ErrorCode;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "gc";

async fn cluster(groups: usize) -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups,
        ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

#[tokio::test]
async fn pending_generation_is_never_collected_before_its_lease_whatever_the_grace() {
    let c = cluster(2).await;
    let live = payload(300 << 10, 1);
    s3::put(
        &c.backend,
        BUCKET,
        "live",
        live.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let baseline = c.total_shards().await;

    // The gateway dies before the commit: shards and manifest entries exist,
    // the generation is pending with a live lease, nobody aborts it.
    c.backend.set_fault(Some(FaultPoint::DieBeforeCommit));
    let err = s3::put(
        &c.backend,
        BUCKET,
        "zombie",
        payload(usize::try_from(MIB + 1).unwrap(), 2),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InternalError);
    assert_eq!(c.total_shards().await, baseline + 6);

    // A controller with a correct clock, a clock far behind, and zero grace:
    // nothing is touched while the lease is alive.
    for now in [now_ms(), now_ms() - 86_400_000] {
        let r = c.gc(now, 0).await;
        assert_eq!(
            (r.expired, r.collected, r.shards_deleted),
            (0, 0, 0),
            "{r:?}"
        );
        assert_eq!(r.skipped_referenced, 0);
    }
    assert_eq!(c.total_shards().await, baseline + 6);
    assert_eq!(
        c.reconcile(now_ms() + 3_600_000, 0).await,
        0,
        "pending generations keep their shards"
    );
    assert_eq!(s3::get(&c.backend, BUCKET, "live").await.unwrap().0, live);

    // Once the lease has passed, expiry retires the generation and the next
    // step collects it; the committed object is untouched.
    let r = c.gc(now_ms() + 61_000, 0).await;
    assert_eq!(
        (r.expired, r.collected, r.shards_deleted),
        (1, 1, 6),
        "{r:?}"
    );
    assert_eq!(c.total_shards().await, baseline);
    assert!(s3::head(&c.backend, BUCKET, "zombie").await.is_err());
    assert_eq!(s3::get(&c.backend, BUCKET, "live").await.unwrap().0, live);
    c.shutdown().await;
}

#[tokio::test]
async fn gc_refuses_to_collect_a_referenced_generation_even_when_queued() {
    let c = cluster(2).await;
    let data = payload(200 << 10, 3);
    s3::put(
        &c.backend,
        BUCKET,
        "obj",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let generation = c.manifest(BUCKET, "obj").await.pointer.generation;
    let baseline = c.total_shards().await;

    // Forge a retired-queue entry for the committed, referenced generation
    // (an invariant violation that a bug elsewhere could produce).
    let mut tx = c.meta.begin().await.unwrap();
    tx.set(&keys::retired(1, &generation), &[]);
    tx.commit().await.unwrap();

    let r = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(r.collected, 0);
    assert_eq!(r.shards_deleted, 0);
    assert_eq!(
        r.skipped_referenced, 1,
        "the forged entry is skipped, not acted on"
    );
    assert_eq!(c.total_shards().await, baseline);
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data);
    c.shutdown().await;
}

#[tokio::test]
async fn reconcile_keeps_shards_of_existing_generations_regardless_of_age() {
    let c = cluster(2).await;
    let data = payload(usize::try_from(MIB + 7).unwrap(), 4);
    s3::put(
        &c.backend,
        BUCKET,
        "obj",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let before = c.total_shards().await;
    // Ten years ahead, zero minimum age: still nothing to delete, because
    // the generation exists and its chunk records place the shards here.
    assert_eq!(c.reconcile(now_ms() + 315_360_000_000, 0).await, 0);
    assert_eq!(c.total_shards().await, before);
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data);
    c.shutdown().await;
}

#[tokio::test]
async fn reconcile_reclaims_former_member_shards_only_after_repair_completes() {
    let mut c = cluster(1).await;
    let va = payload(usize::try_from(MIB + MIB / 2).unwrap(), 5);
    let vb = payload(300 << 10, 6);
    s3::put(&c.backend, BUCKET, "a", va.clone(), &PutOptions::default())
        .await
        .unwrap();
    s3::put(&c.backend, BUCKET, "b", vb.clone(), &PutOptions::default())
        .await
        .unwrap();
    let group = c.manifest(BUCKET, "a").await.chunks[0].1.placement.group_id;
    let record = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == group)
        .unwrap();
    // Planned replacement of a healthy member: the old node keeps running.
    let former = c.node_index(&record.members[0]).unwrap();
    let former_shards = c.shard_count(former, group);
    assert_eq!(former_shards, 3);
    let new_node = c.add_node().await;

    // While the repair is in progress, the former member's copies are kept.
    let report = c
        .repair_with(group, 0, new_node, async |step| {
            if let arca_lake_controller::repair::RepairStep::Chunk { index: 0, .. } = step {
                let g = c
                    .groups()
                    .await
                    .into_iter()
                    .find(|g| g.id == group)
                    .unwrap();
                let r = gc::reconcile_node(
                    c.meta.as_ref(),
                    &c.clients,
                    c.nodes[former].id,
                    &c.nodes[former].addr.to_string(),
                    &g,
                    now_ms() + 3_600_000,
                    0,
                    0,
                )
                .await
                .unwrap();
                assert_eq!(r.deleted(), 0, "{r:?}");
                assert_eq!(r.kept_referenced, 3);
            }
            Ok(())
        })
        .await
        .unwrap();
    assert!(report.complete);
    assert_eq!(c.shard_count(former, group), 3, "repair deletes nothing");

    // After completion the node is no longer a member: its copies go.
    assert_eq!(c.reconcile(now_ms(), 0).await, 3);
    assert_eq!(c.shard_count(former, group), 0);
    // Data is intact through the new membership, even with another member down.
    let other = c.node_index(&record.members[1]).unwrap();
    c.stop_node(other).await;
    assert_eq!(s3::get(&c.backend, BUCKET, "a").await.unwrap().0, va);
    assert_eq!(s3::get(&c.backend, BUCKET, "b").await.unwrap().0, vb);
    c.shutdown().await;
}

#[tokio::test]
async fn concurrent_puts_and_deletes_then_gc_leave_exactly_the_visible_shards() {
    let c = cluster(2).await;
    let mut tasks = Vec::new();
    for i in 0..6u64 {
        let backend = c.backend.clone();
        tasks.push(tokio::spawn(async move {
            if i % 2 == 0 {
                s3::put(
                    &backend,
                    BUCKET,
                    "x",
                    payload(200 << 10, 100 + i),
                    &PutOptions::default(),
                )
                .await
                .map(|_| ())
            } else {
                s3::delete(&backend, BUCKET, "x").await
            }
        }));
    }
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    let r = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(r.skipped_referenced, 0);
    let expected = match s3::get(&c.backend, BUCKET, "x").await {
        Ok(_) => c.manifest(BUCKET, "x").await.chunks.len() * 3,
        Err(e) => {
            assert_eq!(*e.code(), S3ErrorCode::NoSuchKey);
            0
        }
    };
    assert_eq!(
        c.total_shards().await,
        expected,
        "only the visible version's shards remain"
    );
    c.shutdown().await;
}

/// R-64 (Phase 3.6): a deleted multipart object's parts are collected in
/// the pass that collects its root, once the root's grace has passed:
/// they are retired as of the root, not given a second grace.
#[tokio::test]
async fn multipart_parts_are_collected_with_their_root_after_one_grace() {
    let c = TestCluster::start(ClusterOptions {
        multipart_min_part_bytes: MIB,
        ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let before = c.total_shards().await;
    let part = payload(usize::try_from(MIB).unwrap(), 7);
    crate::gc_bench::multipart_upload(&c.backend, BUCKET, "mp", &part, 3).await;
    assert!(c.total_shards().await > before);
    s3::delete(&c.backend, BUCKET, "mp").await.unwrap();
    let grace = 600_000;
    let due = now_ms() + grace + 1;
    // Not before the root's grace.
    let r = gc::run_once(c.meta.as_ref(), &c.clients, now_ms(), grace, 1000)
        .await
        .unwrap();
    assert_eq!(r.collected, 0);
    // One controller pass at `due`: the root's batch retires the parts,
    // the next batch of the same pass collects them.
    let mut collected = 0;
    for _ in 0..2 {
        collected += gc::run_once(c.meta.as_ref(), &c.clients, due, grace, 1000)
            .await
            .unwrap()
            .collected;
    }
    assert_eq!(collected, 4, "the root and its three parts");
    assert_eq!(c.total_shards().await, before, "every part shard deleted");
    let (left, _) = arca_lake_metadata::ops::gc::backlog(c.meta.as_ref(), 10)
        .await
        .unwrap();
    assert_eq!(left, 0);
    c.shutdown().await;
}
