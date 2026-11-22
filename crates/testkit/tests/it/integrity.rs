//! Background scrubbing, corruption classification, durable quarantine and
//! stripe repair (RFC-008 §8, ADR-0015).
//!
//! Every test drives the controller's scrub pass and stripe repair worker
//! explicitly (as the scheduler would) so that each stage is observable;
//! the last test runs real controller processes and fails the leader over.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use arca_lake_controller::integrity::{
    RepairWorkerOptions, ScrubOptions, coders, repair_pass, scrub_group_now,
};
use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_metadata::ops::integrity::{self as itasks, Finding, RepairState, ScrubPhase};
use arca_lake_metadata::records::{ChunkRecord, GroupRecord};
use arca_lake_metadata::{Txn, keys, records, transact};
use arca_lake_storage_node::ShardKey;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "integrity";

async fn cluster(nodes: usize, k: u16, m: u16) -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(nodes, k, m, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

async fn seed_objects(c: &TestCluster, n: u64, len: usize) -> Vec<(String, Bytes)> {
    let mut out = Vec::new();
    for i in 0..n {
        let key = format!("obj{i}");
        let data = payload(len, 100 + i);
        s3::put(
            &c.backend,
            BUCKET,
            &key,
            data.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        out.push((key, data));
    }
    out
}

async fn assert_all_readable(c: &TestCluster, objects: &[(String, Bytes)]) {
    for (key, data) in objects {
        let (got, _) = s3::get(&c.backend, BUCKET, key).await.unwrap();
        assert_eq!(&got, data, "{key}");
    }
}

async fn first_chunk(c: &TestCluster, key: &str) -> (GroupRecord, ChunkRecord) {
    let record = c.manifest(BUCKET, key).await.chunks[0].1.clone();
    let group = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == record.placement.group_id)
        .unwrap();
    (group, record)
}

fn shard_key(group: &GroupRecord, chunk: &ChunkRecord, index: usize) -> ShardKey {
    ShardKey {
        group_id: group.id,
        chunk_id: chunk.chunk_id,
        shard_index: u16::try_from(index).unwrap(),
    }
}

fn member_node(c: &TestCluster, group: &GroupRecord, index: usize) -> usize {
    c.node_index(&group.members[index]).unwrap()
}

fn unpaced() -> ScrubOptions {
    ScrubOptions {
        page: 4,
        page_bytes: 0,
        rate_bytes_per_s: 0,
        interval_ms: 1,
        concurrency: 1,
        adaptive: None,
        manifest_roots_per_pass: 256,
    }
}

fn worker() -> RepairWorkerOptions {
    RepairWorkerOptions {
        tasks_per_pass: 64,
        max_attempts: 3,
        recheck_ms: 1000,
        ..RepairWorkerOptions::default()
    }
}

async fn scrub(c: &TestCluster, group: u64) -> arca_lake_controller::integrity::ScrubReport {
    scrub_group_now(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        group,
        &unpaced(),
        now_ms(),
    )
    .await
    .unwrap()
}

async fn repair(c: &TestCluster, now: u64) -> arca_lake_controller::integrity::RepairWorkerReport {
    repair_pass(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        &worker(),
        None,
        None,
        now,
    )
    .await
    .unwrap()
}

async fn tasks(c: &TestCluster) -> Vec<(u64, [u8; 16], itasks::RepairTask)> {
    itasks::tasks(c.meta.as_ref(), usize::MAX).await.unwrap()
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Total shards of `group` across its members.
fn shards_of(c: &TestCluster, group: &GroupRecord) -> usize {
    (0..group.members.len())
        .map(|i| c.shard_count(member_node(c, group, i), group.id))
        .sum()
}

/// 0.1.0 three-host validation: a task for a chunk of a committed
/// generation whose record is in another group (the write was diverted
/// there; this group holds at most a stray copy) is dropped, not retried
/// every minute forever with the queue never empty.
#[tokio::test]
async fn a_task_for_a_chunk_committed_in_another_group_is_dropped() {
    let c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(6, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let objects = seed_objects(&c, 1, 4096).await;
    let (home, chunk) = first_chunk(&c, &objects[0].0).await;
    let other = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id != home.id)
        .unwrap();
    assert!(
        itasks::enqueue(
            c.meta.as_ref(),
            other.id,
            chunk.chunk_id,
            Finding::ChecksumMismatch,
            &[],
            now_ms(),
        )
        .await
        .unwrap()
    );
    let mut now = now_ms();
    for _ in 0..4 {
        repair(&c, now).await;
        now += 120_000;
    }
    assert!(
        tasks(&c).await.is_empty(),
        "the task stayed queued: {:?}",
        tasks(&c).await
    );
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn scrub_detects_checksum_mismatch_quarantines_and_repair_rewrites_the_shard() {
    let c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 2, usize::try_from(MIB + MIB / 2).unwrap()).await;
    let (group, chunk) = first_chunk(&c, "obj0").await;
    let victim = member_node(&c, &group, 1);
    let key = shard_key(&group, &chunk, 1);
    let before = c.node_store(victim).locate(&key).unwrap();
    let total = shards_of(&c, &group);
    assert_eq!(c.corrupt_chunk_on_node(victim, group.id, chunk.chunk_id), 1);

    // Scrub: the node finds the bad block, quarantines durably, the
    // controller classifies and enqueues exactly one task.
    let r = scrub(&c, group.id).await;
    assert_eq!(r.count(Finding::ChecksumMismatch), 1, "{r:?}");
    assert_eq!(r.enqueued, 1);
    assert_eq!(r.errors, 0);
    assert_eq!(r.verified, u64::try_from(total - 1).unwrap());
    let store = c.node_store(victim);
    let q = store
        .quarantine_entry(&key)
        .expect("quarantined on the node");
    assert_eq!(q.reason, "payload block crc mismatch");
    assert!(
        store
            .quarantine_log()
            .unwrap()
            .iter()
            .any(|l| l.starts_with("quarantine ")),
        "the quarantine is logged"
    );
    let t = tasks(&c).await;
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].2.reason, Finding::ChecksumMismatch);
    assert_eq!(t[0].2.state, RepairState::Detected);
    // Nothing was deleted (I16) and readers decode around the quarantined
    // shard meanwhile.
    assert_eq!(shards_of(&c, &group), total - 1);
    assert_all_readable(&c, &objects).await;

    // Repair: reconstruct, write under the group epoch, verify, commit.
    let r = repair(&c, now_ms()).await;
    assert_eq!(
        (r.succeeded, r.shards_rewritten, r.failed),
        (1, 1, 0),
        "{r:?}"
    );
    assert_eq!(tasks(&c).await.len(), 0, "task removed at Commit");
    let after = store.locate(&key).expect("rewritten shard is live");
    assert!(
        after.sequence > before.sequence,
        "a newer record superseded the bad one"
    );
    assert!(
        store.quarantine_entry(&key).is_none(),
        "quarantine released"
    );
    assert!(
        store
            .quarantine_log()
            .unwrap()
            .iter()
            .any(|l| l.starts_with("release ") && l.contains("cause=superseded")),
    );
    assert_eq!(shards_of(&c, &group), total);
    // The rewritten shard carries exactly the bytes the healthy stripe
    // encodes to: a second scrub is clean and the stripe decodes from any
    // k members.
    let r = scrub(&c, group.id).await;
    assert_eq!(r.findings, [0; 8], "{r:?}");
    assert_all_readable(&c, &objects).await;
    let mut c = c;
    c.stop_node(member_node(&c, &group, 0)).await;
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn missing_shards_single_and_multiple_are_found_by_presence_and_rebuilt() {
    let c = cluster(4, 2, 2).await;
    let objects = seed_objects(&c, 2, usize::try_from(MIB).unwrap()).await;
    let (group, chunk0) = first_chunk(&c, "obj0").await;
    let (_, chunk1) = first_chunk(&c, "obj1").await;
    // obj0: one shard missing; obj1: two shards missing (k of 4 remain).
    c.node_store(member_node(&c, &group, 3))
        .delete(shard_key(&group, &chunk0, 3))
        .unwrap();
    c.node_store(member_node(&c, &group, 0))
        .delete(shard_key(&group, &chunk1, 0))
        .unwrap();
    c.node_store(member_node(&c, &group, 2))
        .delete(shard_key(&group, &chunk1, 2))
        .unwrap();
    let total = shards_of(&c, &group);

    let r = scrub(&c, group.id).await;
    assert_eq!(r.count(Finding::MissingShard), 2, "{r:?}");
    assert_eq!(r.enqueued, 2);
    let t = tasks(&c).await;
    assert_eq!(t.len(), 2);
    let hint = |cid: [u8; 16]| {
        t.iter()
            .find(|(_, c, _)| *c == cid)
            .map(|(_, _, t)| t.hint_targets.clone())
            .unwrap()
    };
    assert_eq!(hint(chunk0.chunk_id), vec![3]);
    assert_eq!(hint(chunk1.chunk_id), vec![0, 2]);

    let r = repair(&c, now_ms()).await;
    assert_eq!((r.succeeded, r.shards_rewritten), (2, 3), "{r:?}");
    assert_eq!(tasks(&c).await.len(), 0);
    assert_eq!(shards_of(&c, &group), total + 3);
    assert_eq!(scrub(&c, group.id).await.findings, [0; 8]);
    assert_all_readable(&c, &objects).await;
    // Every rebuilt member is a usable source: lose two others.
    let mut c = c;
    c.stop_node(member_node(&c, &group, 1)).await;
    c.stop_node(member_node(&c, &group, 3)).await;
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn degraded_read_reports_a_task_and_a_second_report_is_deduplicated() {
    let c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 1, usize::try_from(MIB / 2).unwrap()).await;
    let (group, chunk) = first_chunk(&c, "obj0").await;
    c.node_store(member_node(&c, &group, 0))
        .delete(shard_key(&group, &chunk, 0))
        .unwrap();
    assert_all_readable(&c, &objects).await;
    assert_all_readable(&c, &objects).await;
    let meta = Arc::clone(&c.meta);
    wait_until(
        "the degraded-read report",
        Duration::from_secs(5),
        async || {
            itasks::task(meta.as_ref(), group.id, &chunk.chunk_id)
                .await
                .unwrap()
                .is_some()
        },
    )
    .await;
    let s = c.backend.integrity_stats();
    assert_eq!(s.degraded_reads, 2, "{s:?}");
    assert_eq!(s.integrity_reports, 1, "one report per chunk per interval");
    assert_eq!(s.integrity_reports_dropped, 1);
    let t = itasks::task(c.meta.as_ref(), group.id, &chunk.chunk_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t.reason, Finding::MissingShard);
    assert_eq!(t.hint_targets, vec![0]);
    let r = repair(&c, now_ms()).await;
    assert_eq!((r.succeeded, r.shards_rewritten), (1, 1), "{r:?}");
    assert_eq!(scrub(&c, group.id).await.findings, [0; 8]);
    c.shutdown().await;
}

#[tokio::test]
async fn unrecoverable_stripe_is_an_error_for_readers_and_repair_writes_nothing() {
    let c = cluster(3, 2, 1).await;
    seed_objects(&c, 1, usize::try_from(MIB / 2).unwrap()).await;
    let (group, chunk) = first_chunk(&c, "obj0").await;
    for i in [0, 2] {
        assert_eq!(
            c.corrupt_chunk_on_node(member_node(&c, &group, i), group.id, chunk.chunk_id),
            1
        );
    }
    let healthy = member_node(&c, &group, 1);
    let healthy_key = shard_key(&group, &chunk, 1);
    let before = c.node_store(healthy).locate(&healthy_key).unwrap();

    // The reader never gets bytes (I15).
    let err = s3::get(&c.backend, BUCKET, "obj0").await.unwrap_err();
    assert!(err.to_string().contains("unreadable"), "{err}");
    assert_eq!(c.backend.integrity_stats().unreadable_reads, 1);

    let r = scrub(&c, group.id).await;
    assert_eq!(r.count(Finding::ChecksumMismatch), 2, "{r:?}");
    let r = repair(&c, now_ms()).await;
    assert_eq!((r.unrecoverable, r.shards_rewritten), (1, 0), "{r:?}");
    let t = tasks(&c).await;
    assert_eq!(t.len(), 1);
    assert!(
        matches!(
            t[0].2.state,
            RepairState::Unrecoverable {
                have: 1,
                need: 2,
                ..
            }
        ),
        "{:?}",
        t[0].2
    );
    assert_eq!(
        c.node_store(healthy).locate(&healthy_key).unwrap(),
        before,
        "the surviving shard was not touched"
    );
    // Still unreadable, still no bytes; the task is not re-run before it is
    // due and stays unrecoverable when it is.
    assert!(s3::get(&c.backend, BUCKET, "obj0").await.is_err());
    let r = repair(&c, now_ms()).await;
    assert_eq!(r.deferred, 1, "{r:?}");
    let r = repair(&c, now_ms() + 3_600_000 + 1).await;
    assert_eq!((r.unrecoverable, r.shards_rewritten), (1, 0), "{r:?}");
    let (_, _, t) = tasks(&c).await.remove(0);
    let RepairState::Unrecoverable { since_ms, .. } = t.state else {
        panic!("{t:?}");
    };
    assert_eq!(since_ms, tasks(&c).await[0].2.created_ms.max(since_ms));
    c.shutdown().await;
}

#[tokio::test]
async fn a_member_that_is_down_is_a_retry_not_an_unrecoverable_verdict() {
    let mut c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 1, usize::try_from(MIB / 2).unwrap()).await;
    let (group, chunk) = first_chunk(&c, "obj0").await;
    assert_eq!(
        c.corrupt_chunk_on_node(member_node(&c, &group, 0), group.id, chunk.chunk_id),
        1
    );
    assert!(
        itasks::enqueue(
            c.meta.as_ref(),
            group.id,
            chunk.chunk_id,
            Finding::ChecksumMismatch,
            &[0],
            now_ms()
        )
        .await
        .unwrap()
    );
    let down = member_node(&c, &group, 2);
    c.stop_node(down).await;
    let t0 = now_ms();
    let r = repair(&c, t0).await;
    assert_eq!(
        (r.failed, r.unrecoverable, r.shards_rewritten),
        (1, 0, 0),
        "{r:?}"
    );
    let (_, _, t) = tasks(&c).await.remove(0);
    assert_eq!(t.state, RepairState::Detected);
    assert_eq!(t.attempts, 1);
    assert!(t.next_attempt_ms > t0, "backed off");
    assert_eq!(t.last_error.as_deref(), Some("members unavailable"));
    // Not due yet: deferred. Due and the member is back: repaired.
    assert_eq!(repair(&c, t0).await.deferred, 1);
    c.start_node(down).await;
    let r = repair(&c, t.next_attempt_ms).await;
    assert_eq!((r.succeeded, r.shards_rewritten), (1, 1), "{r:?}");
    assert_eq!(tasks(&c).await.len(), 0);
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn concurrent_detections_and_workers_converge_on_one_healthy_stripe() {
    let c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 1, usize::try_from(MIB / 2).unwrap()).await;
    let (group, chunk) = first_chunk(&c, "obj0").await;
    assert_eq!(
        c.corrupt_chunk_on_node(member_node(&c, &group, 1), group.id, chunk.chunk_id),
        1
    );
    // Three detectors at once: scrub, two explicit enqueues and a degraded
    // read. One task results.
    let enq = || {
        itasks::enqueue(
            c.meta.as_ref(),
            group.id,
            chunk.chunk_id,
            Finding::ChecksumMismatch,
            &[1],
            now_ms(),
        )
    };
    let (first, second, pass, got) = tokio::join!(
        enq(),
        enq(),
        scrub(&c, group.id),
        s3::get(&c.backend, BUCKET, "obj0")
    );
    assert_eq!(
        u8::from(first.unwrap()) + u8::from(second.unwrap()) + u8::from(pass.enqueued == 1),
        1,
        "exactly one creator"
    );
    assert_eq!(got.unwrap().0, objects[0].1);
    let meta = Arc::clone(&c.meta);
    wait_until("the read report", Duration::from_secs(5), async || {
        itasks::tasks(meta.as_ref(), usize::MAX)
            .await
            .unwrap()
            .len()
            == 1
    })
    .await;
    // Two workers on the same task (a leader failover's overlap, or an
    // operator re-issuing the repair): both write the same bytes under the
    // same epoch; both end with the stripe healthy and no task.
    let (r1, r2) = tokio::join!(repair(&c, now_ms()), repair(&c, now_ms()));
    assert_eq!(r1.succeeded + r2.succeeded, 2, "{r1:?} {r2:?}");
    assert_eq!(r1.failed + r2.failed, 0);
    assert_eq!(tasks(&c).await.len(), 0);
    assert_eq!(scrub(&c, group.id).await.findings, [0; 8]);
    assert_eq!(shards_of(&c, &group), 3);
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn repair_runs_safely_under_concurrent_get_put_and_delete() {
    let c = cluster(3, 2, 1).await;
    let mut objects = seed_objects(&c, 4, usize::try_from(MIB + MIB / 4).unwrap()).await;
    let group = first_chunk(&c, "obj0").await.0;
    // One corrupt shard per object, spread over members; obj3 is deleted
    // while its repair is queued.
    for (i, (key, _)) in objects.iter().enumerate() {
        let chunk = first_chunk(&c, key).await.1;
        assert_eq!(
            c.corrupt_chunk_on_node(member_node(&c, &group, i % 3), group.id, chunk.chunk_id),
            1
        );
    }
    let r = scrub(&c, group.id).await;
    assert_eq!(r.enqueued, 4, "{r:?}");
    let deleted_chunk = first_chunk(&c, "obj3").await.1;
    s3::delete(&c.backend, BUCKET, "obj3").await.unwrap();
    let (_, deleted_data) = objects.pop().unwrap();

    let getter = async {
        for _ in 0..8 {
            let (got, _) = s3::get(&c.backend, BUCKET, "obj0").await.unwrap();
            assert_eq!(got, objects[0].1);
        }
    };
    let putter = async {
        let mut out = Vec::new();
        for i in 0..3u64 {
            let key = format!("during{i}");
            let data = payload(700 << 10, 500 + i);
            s3::put(
                &c.backend,
                BUCKET,
                &key,
                data.clone(),
                &PutOptions::default(),
            )
            .await
            .unwrap();
            out.push((key, data));
        }
        out
    };
    let ((), during, r) = tokio::join!(getter, putter, repair(&c, now_ms()));
    assert_eq!(r.failed, 0, "{r:?}");
    assert_eq!(r.succeeded + r.dropped, 4, "{r:?}");
    // The deleted object's task is dropped or repaired, never left behind
    // once GC retires the generation; obj3 is gone for readers either way.
    c.gc(now_ms() + 1, 0).await;
    let r2 = repair(&c, now_ms()).await;
    assert_eq!(r2.failed, 0);
    assert_eq!(tasks(&c).await.len(), 0, "{:?}", tasks(&c).await);
    assert!(s3::get(&c.backend, BUCKET, "obj3").await.is_err());
    let _ = (deleted_chunk, deleted_data);
    objects.extend(during);
    assert_all_readable(&c, &objects).await;
    assert_eq!(
        scrub(&c, group.id).await.count(Finding::ChecksumMismatch),
        0
    );
    c.shutdown().await;
}

#[tokio::test]
async fn a_fenced_member_rejects_the_repair_write_and_the_task_is_replanned() {
    let mut c = cluster(4, 2, 2).await;
    let objects = seed_objects(&c, 1, usize::try_from(MIB / 2).unwrap()).await;
    let (group, chunk) = first_chunk(&c, "obj0").await;
    let victim_index = 1;
    let victim = member_node(&c, &group, victim_index);
    assert_eq!(c.corrupt_chunk_on_node(victim, group.id, chunk.chunk_id), 1);
    assert_eq!(scrub(&c, group.id).await.enqueued, 1);
    // The target node learns a newer epoch (a membership change in flight)
    // before the worker writes: the write under the planned epoch is
    // rejected (I12), nothing is written, the task goes back to Classify.
    let addr = c.nodes[victim].addr.to_string();
    c.clients
        .update_epoch(&addr, group.id, group.epoch + 1)
        .await
        .unwrap();
    let r = repair(&c, now_ms()).await;
    assert_eq!(
        (r.deferred, r.shards_rewritten, r.failed),
        (1, 0, 0),
        "{r:?}"
    );
    let (_, _, t) = tasks(&c).await.remove(0);
    assert_eq!(t.state, RepairState::Detected);
    assert!(t.last_error.unwrap().contains("epoch"));
    assert!(
        c.node_store(victim)
            .quarantine_entry(&shard_key(&group, &chunk, victim_index))
            .is_some()
    );
    // The membership change completes (member 3 moves to a new node, epoch
    // 2); the next pass plans under the new epoch and succeeds.
    let other = member_node(&c, &group, 3);
    c.stop_node(other).await;
    let new_node = c.add_node().await;
    let rep = c.repair(group.id, 3, new_node).await;
    // Member replacement rebuilt the new member but reports the group
    // incomplete: the quarantined shard on member 1 is still missing (it is
    // the stripe repair's job, under the new epoch).
    assert!(!rep.complete, "{rep:?}");
    assert_eq!(rep.completeness.missing, 1, "{rep:?}");
    assert_eq!(rep.epoch, 2);
    let r = repair(&c, now_ms()).await;
    assert_eq!((r.succeeded, r.shards_rewritten), (1, 1), "{r:?}");
    assert_eq!(tasks(&c).await.len(), 0);
    assert_eq!(scrub(&c, group.id).await.findings, [0; 8]);
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn scrub_cursor_resumes_after_an_interrupted_pass_and_a_node_restart() {
    let mut c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 5, usize::try_from(MIB / 2).unwrap()).await;
    let group = first_chunk(&c, "obj0").await.0;
    let total = u64::try_from(shards_of(&c, &group)).unwrap();
    // A heavily paced pass (1 byte/s) sleeps after its first page: abort it
    // there, as a leader crash would.
    let meta = Arc::clone(&c.meta);
    let clients = c.clients.clone();
    let gid = group.id;
    let paced = tokio::spawn(async move {
        let opts = ScrubOptions {
            page: 2,
            page_bytes: 0,
            rate_bytes_per_s: 1,
            interval_ms: 1,
            concurrency: 1,
            adaptive: None,
            manifest_roots_per_pass: 256,
        };
        scrub_group_now(meta.as_ref(), &clients, &coders(), gid, &opts, now_ms()).await
    });
    let meta = Arc::clone(&c.meta);
    wait_until("the first page", Duration::from_secs(10), async || {
        matches!(
            itasks::scrub_cursor(meta.as_ref(), gid).await.unwrap(),
            Some(cur) if matches!(cur.phase, ScrubPhase::Shards { after: Some(_), .. })
        )
    })
    .await;
    paced.abort();
    let _ = paced.await;
    let mid = itasks::scrub_cursor(c.meta.as_ref(), gid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mid.pass, 1);
    assert!(mid.verified >= 1 && mid.verified < total, "{mid:?}");

    // A node restarts between passes: the quarantine-free device reopens
    // and the resumed pass continues from the persisted cursor, verifying
    // exactly the remaining shards.
    let n = member_node(&c, &group, 0);
    c.stop_node(n).await;
    c.start_node(n).await;
    let r = scrub(&c, gid).await;
    assert_eq!(r.errors, 0, "{r:?}");
    let done = itasks::scrub_cursor(c.meta.as_ref(), gid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        done.pass, 1,
        "the interrupted pass was resumed, not restarted"
    );
    assert!(done.is_done());
    assert_eq!(done.verified, total, "{done:?}");
    assert_eq!(done.verified, mid.verified + r.verified);
    assert_eq!(done.findings, [0; 8]);
    // The next pass is a new one.
    scrub(&c, gid).await;
    assert_eq!(
        itasks::scrub_cursor(c.meta.as_ref(), gid)
            .await
            .unwrap()
            .unwrap()
            .pass,
        2
    );
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn byte_bounded_scrub_pages_cover_every_shard_exactly_once() {
    let c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 4, usize::try_from(MIB / 2).unwrap()).await;
    let group = first_chunk(&c, "obj0").await.0;
    let total = u64::try_from(shards_of(&c, &group)).unwrap();
    // One byte per page: the node stops after every shard and reports
    // `more`; the controller's cursor walks on; nothing is skipped or
    // repeated.
    let r = scrub_group_now(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        group.id,
        &ScrubOptions {
            page: 256,
            page_bytes: 1,
            rate_bytes_per_s: 0,
            interval_ms: 1,
            concurrency: 1,
            adaptive: None,
            manifest_roots_per_pass: 256,
        },
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!(r.verified, total, "{r:?}");
    assert_eq!(r.findings, [0; 8]);
    let stats = c.nodes[member_node(&c, &group, 0)]
        .handle_stats()
        .expect("running");
    assert_eq!(stats.scrub_shards, total / 3, "each shard once on its node");
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn scrub_pages_larger_than_the_key_bound_cover_every_shard() {
    let c = cluster(3, 2, 1).await;
    // More shards per member than one 256-key page.
    for i in 0..300u64 {
        s3::put(
            &c.backend,
            BUCKET,
            &format!("small{i}"),
            payload(4096, 1000 + i),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    let group = c.groups().await.remove(0);
    let total = u64::try_from(shards_of(&c, &group)).unwrap();
    assert_eq!(total, 900);
    for (page, page_bytes) in [(256u32, 0u64), (256, 8 << 20), (7, 0), (1000, 1)] {
        let r = scrub_group_now(
            c.meta.as_ref(),
            &c.clients,
            &coders(),
            group.id,
            &ScrubOptions {
                page,
                page_bytes,
                rate_bytes_per_s: 0,
                interval_ms: 1,
                concurrency: 1,
                adaptive: None,
                manifest_roots_per_pass: 256,
            },
            now_ms(),
        )
        .await
        .unwrap();
        assert_eq!(r.verified, total, "page {page} bytes {page_bytes}: {r:?}");
        assert_eq!(r.findings, [0; 8]);
    }
    c.shutdown().await;
}

#[tokio::test]
async fn scrub_skips_a_member_that_is_down_and_counts_the_error() {
    let mut c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 2, usize::try_from(MIB / 2).unwrap()).await;
    let group = first_chunk(&c, "obj0").await.0;
    let down = member_node(&c, &group, 1);
    c.stop_node(down).await;
    let r = scrub(&c, group.id).await;
    assert!(r.errors >= 1, "{r:?}");
    assert_eq!(
        r.count(Finding::MissingShard),
        0,
        "an unreachable member is not a missing shard"
    );
    assert_eq!(tasks(&c).await.len(), 0);
    c.start_node(down).await;
    let r = scrub(&c, group.id).await;
    assert_eq!((r.errors, r.findings), (0, [0; 8]), "{r:?}");
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn every_corruption_class_is_classified() {
    let c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 4, usize::try_from(MIB / 2).unwrap()).await;
    let group = first_chunk(&c, "obj0").await.0;
    let total = shards_of(&c, &group);
    let chunks: Vec<ChunkRecord> = {
        let mut v = Vec::new();
        for (k, _) in &objects {
            v.push(first_chunk(&c, k).await.1);
        }
        v
    };
    // obj0: invalid header on member 0.
    {
        use std::os::unix::fs::FileExt as _;
        let n = member_node(&c, &group, 0);
        let store = c.node_store(n);
        let key = shard_key(&group, &chunks[0], 0);
        let loc = store.locate(&key).unwrap();
        let path = c.nodes[n]
            .dir
            .join(format!("seg-{:016x}.arca", loc.segment_id));
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.write_all_at(&[0xEE; 4], loc.offset).unwrap();
        f.sync_all().unwrap();
    }
    // obj1: metadata/data mismatch on member 1 (a valid record of the wrong
    // length under the right key).
    {
        let n = member_node(&c, &group, 1);
        c.node_store(n)
            .put(shard_key(&group, &chunks[1], 1), &[7u8; 100])
            .unwrap();
    }
    // obj2: stale placement epoch (the chunk record is ahead of the group,
    // as after a metadata restore).
    {
        let g = c.manifest(BUCKET, "obj2").await.pointer.generation;
        let mut rec = chunks[2].clone();
        rec.placement.epoch = group.epoch + 1;
        transact(c.meta.as_ref(), (g, rec), |tx, env| {
            Box::pin(async move {
                let (g, rec) = env;
                tx.set(&keys::gen_chunk(g, 0), &records::encode(rec)?);
                Ok(())
            })
        })
        .await
        .unwrap();
    }
    // Unexpected shards: a copy under a foreign index on member 2 and a
    // shard of a chunk no generation owns.
    {
        let n = member_node(&c, &group, 2);
        let store = c.node_store(n);
        store
            .put(shard_key(&group, &chunks[3], 0), &[1u8; 64])
            .unwrap();
        store
            .put(
                ShardKey {
                    group_id: group.id,
                    chunk_id: [0xAB; 16],
                    shard_index: 2,
                },
                &[2u8; 64],
            )
            .unwrap();
    }
    // obj3: truncated record on member 0 — the segment is cut inside the
    // record (the node is running; its index still points at it).
    {
        let n = member_node(&c, &group, 0);
        let store = c.node_store(n);
        let key = shard_key(&group, &chunks[3], 0);
        let loc = store.locate(&key).unwrap();
        // obj3 was written last, so its record is the last one of its
        // segment on every member: cutting the file truncates only it.
        let seg = loc.segment_id;
        let last = store
            .keys_in_group(group.id)
            .into_iter()
            .filter_map(|k| store.locate(&k).map(|l| (l.segment_id, l.offset, k)))
            .filter(|(s, _, _)| *s == seg)
            .max_by_key(|(_, o, _)| *o)
            .map(|(_, _, k)| k)
            .unwrap();
        if last == key {
            let path = c.nodes[n].dir.join(format!("seg-{seg:016x}.arca"));
            let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            f.set_len(loc.payload_offset() + u64::from(loc.payload_len) / 2)
                .unwrap();
            f.sync_all().unwrap();
        } else {
            // The chunk's record is not last in its segment: fall back to
            // header damage, which classifies as InvalidHeader; the count
            // below accepts either.
            use std::os::unix::fs::FileExt as _;
            let path = c.nodes[n].dir.join(format!("seg-{seg:016x}.arca"));
            let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            f.write_all_at(&[0xEE; 4], loc.offset).unwrap();
            f.sync_all().unwrap();
        }
    }
    let r = scrub(&c, group.id).await;
    assert_eq!(r.count(Finding::ChecksumMismatch), 0, "{r:?}");
    assert_eq!(
        r.count(Finding::InvalidHeader) + r.count(Finding::TruncatedShard),
        2,
        "{r:?}"
    );
    assert_eq!(r.count(Finding::MetadataDataMismatch), 1, "{r:?}");
    // Findings count shards: every shard of the stale stripe is checked
    // against the stale record.
    assert_eq!(r.count(Finding::StalePlacementEpoch), 3, "{r:?}");
    assert_eq!(r.count(Finding::UnexpectedShard), 2, "{r:?}");
    assert_eq!(
        r.count(Finding::MissingShard),
        0,
        "quarantined shards are not re-counted by the presence walk: {r:?}"
    );
    assert_eq!(r.count(Finding::UnrecoverableStripe), 0, "{r:?}");
    assert_eq!(r.enqueued, 3, "header, mismatch and truncated findings");
    // Scrub deleted nothing (I16): the two unexpected shards are still there
    // for reconciliation to judge, the two damaged records are quarantined
    // (absent from the live index), not removed, and the wrong-length
    // record is live (it is valid on the node; only metadata disagrees).
    assert_eq!(shards_of(&c, &group), total + 2 - 2);
    // Repair rewrites the three; the stale-epoch stripe is left alone.
    let r = repair(&c, now_ms()).await;
    assert_eq!(
        (r.succeeded, r.shards_rewritten, r.failed),
        (3, 3, 0),
        "{r:?}"
    );
    assert_all_readable(&c, &objects).await;
    // Reconciliation removes the unexpected copies; nothing else.
    assert_eq!(c.reconcile(now_ms() + 1, 0).await, 2);
    assert_eq!(shards_of(&c, &group), total);
    c.shutdown().await;
}

#[tokio::test]
async fn quarantine_survives_a_node_restart_and_gc_reconciles_unreferenced_quarantined_shards() {
    let mut c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 1, usize::try_from(MIB / 2).unwrap()).await;
    let (group, chunk) = first_chunk(&c, "obj0").await;
    let n = member_node(&c, &group, 0);
    let key = shard_key(&group, &chunk, 0);
    assert_eq!(c.corrupt_chunk_on_node(n, group.id, chunk.chunk_id), 1);
    // A plain GET through the gateway quarantines on the node: the first
    // read of the bad (data) shard reports DATA_LOSS, the gateway decodes
    // around it with parity.
    assert_all_readable(&c, &objects).await;
    assert!(c.node_store(n).quarantine_entry(&key).is_some());
    let addr = c.nodes[n].addr.to_string();
    let err = c
        .clients
        .get_shard(
            &addr,
            arca_lake_protocol::client::ShardRef {
                group_id: group.id,
                chunk_id: chunk.chunk_id,
                shard_index: 0,
            },
            0,
            0,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::DataLoss, "{err}");
    let page = c
        .clients
        .list_shards_page_with(&addr, group.id, None, 0, true)
        .await
        .unwrap();
    assert!(
        page.entries
            .iter()
            .all(|(s, _)| s.chunk_id != chunk.chunk_id)
    );
    assert_eq!(page.quarantined.len(), 1);

    c.stop_node(n).await;
    c.start_node(n).await;
    let store = c.node_store(n);
    assert!(store.quarantine_entry(&key).is_some(), "re-derived at open");
    assert_eq!(store.quarantine_stats().0, 1);
    // Referenced: reconciliation keeps it (repair's job).
    assert_eq!(c.reconcile(now_ms() + 1, 0).await, 0);
    assert!(store.quarantine_entry(&key).is_some());
    // Deleted and collected: GC's tombstone of the generation's shards
    // releases the quarantine entry (GC and quarantine do not fight).
    s3::delete(&c.backend, BUCKET, "obj0").await.unwrap();
    c.gc(now_ms() + 1, 0).await;
    assert!(store.quarantine_entry(&key).is_none());
    assert_eq!(store.quarantine_stats(), (0, 0));
    assert!(
        store
            .quarantine_log()
            .unwrap()
            .iter()
            .any(|l| l.contains("cause=tombstone"))
    );
    assert_eq!(c.total_shards().await, 0);
    // A quarantined shard that no generation owns (a write GC never saw)
    // is an orphan for reconciliation: listed through `include_quarantined`,
    // deleted, and released by the tombstone.
    let orphan = ShardKey {
        group_id: group.id,
        chunk_id: [0xAB; 16],
        shard_index: 0,
    };
    store.put(orphan, &[9u8; 4096]).unwrap();
    assert_eq!(c.corrupt_chunk_on_node(n, group.id, orphan.chunk_id), 1);
    assert!(matches!(
        store.get(&orphan, None),
        Err(arca_lake_storage::StorageError::Corrupt { .. })
    ));
    assert!(store.quarantine_entry(&orphan).is_some());
    assert_eq!(
        c.reconcile(now_ms() + 2, 0).await,
        1,
        "the quarantined orphan"
    );
    assert!(store.quarantine_entry(&orphan).is_none());
    assert_eq!(store.quarantine_stats(), (0, 0));
    assert_eq!(c.total_shards().await, 0);
    c.shutdown().await;
}

fn fast(id: &str) -> ControllerConfig {
    let mut cfg = ControllerConfig::new(id);
    cfg.lease = Duration::from_millis(600);
    cfg.gc_interval = Duration::from_secs(3600);
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.repair_interval = Duration::from_secs(3600);
    cfg.health_interval = Duration::from_millis(200);
    cfg.scrub = ScrubOptions {
        page: 8,
        page_bytes: 0,
        rate_bytes_per_s: 0,
        interval_ms: 1,
        concurrency: 4,
        adaptive: None,
        manifest_roots_per_pass: 256,
    };
    cfg.scrub_tick = Duration::from_millis(200);
    cfg.stripe_repair_interval = Duration::from_millis(200);
    cfg.stripe_repair = RepairWorkerOptions {
        tasks_per_pass: 64,
        max_attempts: 8,
        recheck_ms: 1000,
        ..RepairWorkerOptions::default()
    };
    cfg.http_listen = Some("127.0.0.1:0".parse().unwrap());
    cfg
}

async fn metrics_of(h: &arca_lake_controller::process::ControllerHandle) -> String {
    let addr = h.http_addr.unwrap();
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    tokio::io::AsyncWriteExt::write_all(&mut s, b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut out = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut s, &mut out)
        .await
        .unwrap();
    out
}

#[tokio::test]
async fn parallel_repair_rebuilds_many_stripes_under_budget_and_stops_on_shutdown() {
    let c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 24, usize::try_from(MIB / 2).unwrap()).await;
    let group = first_chunk(&c, "obj0").await.0;
    // Wipe member 2's shard of every object: 24 stripes to rebuild.
    let victim = member_node(&c, &group, 2);
    for (key, _) in &objects {
        let chunk = first_chunk(&c, key).await.1;
        c.node_store(victim)
            .delete(shard_key(&group, &chunk, 2))
            .unwrap();
    }
    let r = scrub(&c, group.id).await;
    assert_eq!(r.enqueued, 24, "{r:?}");
    // A shutdown signal that fires during the pass: the pass stops between
    // tasks (and inside pacing sleeps), leaving the rest queued.
    let (tx, rx) = tokio::sync::watch::channel(false);
    let slow = RepairWorkerOptions {
        workers: 2,
        rate_bytes_per_s: 1 << 20,
        ..worker()
    };
    let meta = Arc::clone(&c.meta);
    let clients = c.clients.clone();
    let gid = group.id;
    let handle = tokio::spawn(async move {
        repair_pass(
            meta.as_ref(),
            &clients,
            &coders(),
            &slow,
            None,
            Some(&rx),
            now_ms(),
        )
        .await
        .unwrap()
    });
    let meta = Arc::clone(&c.meta);
    wait_until("some repairs", Duration::from_secs(20), async || {
        itasks::tasks(meta.as_ref(), usize::MAX)
            .await
            .unwrap()
            .len()
            < 24
    })
    .await;
    let started = std::time::Instant::now();
    tx.send(true).unwrap();
    let partial = handle.await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "stopped promptly: {:?}",
        started.elapsed()
    );
    let left = tasks(&c).await.len();
    assert!(
        partial.succeeded >= 1 && left >= 1,
        "{partial:?} left {left}"
    );
    assert_eq!(partial.failed, 0);
    // The rest, with four workers and a budget: every stripe is rebuilt,
    // and the wall time respects the budget (reads k shards + writes one +
    // reads one back per stripe).
    let fast = RepairWorkerOptions {
        workers: 4,
        rate_bytes_per_s: 0,
        ..worker()
    };
    let r = repair_pass(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        &fast,
        None,
        None,
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!(
        (r.succeeded, r.failed, r.shards_rewritten),
        (left, 0, left),
        "{r:?}"
    );
    assert_eq!(tasks(&c).await.len(), 0);
    assert_eq!(scrub(&c, gid).await.findings, [0; 8]);
    assert_all_readable(&c, &objects).await;
    // Budget: 4 stripes of 256 KiB shards at 256 KiB/s move (2 reads +
    // 1 write + 1 read-back) × 256 KiB each: at least 4 × 1 MiB / 256 KiB
    // = 16 s unpaced would be far less; expect ≥ 8 s with 2 workers.
    for (key, _) in objects.iter().take(4) {
        let chunk = first_chunk(&c, key).await.1;
        c.node_store(victim)
            .delete(shard_key(&group, &chunk, 2))
            .unwrap();
    }
    assert_eq!(scrub(&c, gid).await.enqueued, 4);
    let paced = RepairWorkerOptions {
        workers: 2,
        rate_bytes_per_s: 512 << 10,
        ..worker()
    };
    let t = std::time::Instant::now();
    let r = repair_pass(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        &paced,
        None,
        None,
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!(r.succeeded, 4, "{r:?}");
    assert!(
        t.elapsed() >= Duration::from_secs(6),
        "budget held: {:?}",
        t.elapsed()
    );
    c.shutdown().await;
}

#[tokio::test]
async fn shutdown_interrupts_a_paced_scrub_pass_promptly() {
    let c = cluster(3, 2, 1).await;
    seed_objects(&c, 4, usize::try_from(MIB / 2).unwrap()).await;
    let group = first_chunk(&c, "obj0").await.0;
    let mut cfg = fast("slow");
    // One shard per page at 1 B/s: the pass sleeps for ages after its
    // first page; shutdown must not wait for it.
    cfg.scrub = ScrubOptions {
        page: 1,
        page_bytes: 1,
        rate_bytes_per_s: 1,
        interval_ms: 1,
        concurrency: 1,
        adaptive: None,
        manifest_roots_per_pass: 256,
    };
    let h = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let meta = Arc::clone(&c.meta);
    wait_until("a pass in progress", Duration::from_secs(10), async || {
        matches!(
            itasks::scrub_cursor(meta.as_ref(), group.id).await.unwrap(),
            Some(cur) if matches!(cur.phase, ScrubPhase::Shards { after: Some(_), .. })
        )
    })
    .await;
    let started = std::time::Instant::now();
    h.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown took {:?}",
        started.elapsed()
    );
    // The cursor is persisted mid-pass: the next leader resumes it.
    let cur = itasks::scrub_cursor(c.meta.as_ref(), group.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!cur.is_done(), "{cur:?}");
    c.shutdown().await;
}

#[tokio::test]
async fn controller_processes_scrub_and_repair_and_a_leader_failover_resumes_them() {
    let c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 3, usize::try_from(MIB / 2).unwrap()).await;
    let (group, chunk0) = first_chunk(&c, "obj0").await;
    assert_eq!(
        c.corrupt_chunk_on_node(member_node(&c, &group, 0), group.id, chunk0.chunk_id),
        1
    );
    let a = spawn(Arc::clone(&c.meta), fast("a")).await.unwrap();
    let b = spawn(Arc::clone(&c.meta), fast("b")).await.unwrap();
    wait_until("a leader", Duration::from_secs(5), async || {
        a.is_leader() || b.is_leader()
    })
    .await;
    let meta = Arc::clone(&c.meta);
    wait_until(
        "the scheduled scrub and repair",
        Duration::from_secs(20),
        async || {
            let cur = itasks::scrub_cursor(meta.as_ref(), group.id).await.unwrap();
            cur.is_some_and(|cur| cur.finished_ms > 0)
                && itasks::tasks(meta.as_ref(), usize::MAX)
                    .await
                    .unwrap()
                    .is_empty()
                && c.node_store(member_node(&c, &group, 0))
                    .quarantine_entry(&shard_key(&group, &chunk0, 0))
                    .is_none()
        },
    )
    .await;
    let leader = if a.is_leader() { &a } else { &b };
    let m = metrics_of(leader).await;
    let counter = |name: &str| -> u64 {
        m.lines()
            .find_map(|l| l.strip_prefix(name).and_then(|v| v.trim().parse().ok()))
            .unwrap_or_else(|| panic!("{name} missing in {m}"))
    };
    assert_eq!(
        counter("arca_controller_stripe_repairs_succeeded_total "),
        1
    );
    assert_eq!(counter("arca_controller_stripe_repairs_failed_total "), 0);
    assert_eq!(counter("arca_controller_shards_rewritten_total "), 1);
    // Scrub passes repeat every tick here, so the quarantined shard is
    // reported on every pass until it is repaired: at least once.
    assert!(counter("arca_controller_integrity_findings_total{class=\"checksum_mismatch\"} ") >= 1);
    assert_eq!(counter("arca_controller_data_integrity_status "), 0);
    assert_eq!(counter("arca_controller_repair_queue_depth "), 0);
    assert!(counter("arca_controller_scrub_passes_total ") >= 1);

    // Fail the leader over, then corrupt a second shard: only the new
    // leader can find and repair it (corrupting first raced with the old
    // leader's scrub ticks).
    let (old, new) = if a.is_leader() { (a, b) } else { (b, a) };
    old.shutdown().await;
    wait_until("the new leader", Duration::from_secs(5), async || {
        new.is_leader()
    })
    .await;
    let (_, chunk1) = first_chunk(&c, "obj1").await;
    assert_eq!(
        c.corrupt_chunk_on_node(member_node(&c, &group, 2), group.id, chunk1.chunk_id),
        1
    );
    wait_until(
        "the repair by the new leader",
        Duration::from_secs(20),
        async || {
            metrics_of(&new)
                .await
                .lines()
                .any(|l| l == "arca_controller_stripe_repairs_succeeded_total 1")
                && itasks::tasks(meta.as_ref(), usize::MAX)
                    .await
                    .unwrap()
                    .is_empty()
                && c.node_store(member_node(&c, &group, 2))
                    .quarantine_entry(&shard_key(&group, &chunk1, 2))
                    .is_none()
        },
    )
    .await;
    let m = metrics_of(&new).await;
    assert!(
        m.lines()
            .any(|l| l == "arca_controller_stripe_repairs_succeeded_total 1"),
        "{m}"
    );
    assert_eq!(shards_of(&c, &group), 9);
    assert_all_readable(&c, &objects).await;
    assert_eq!(scrub(&c, group.id).await.findings, [0; 8]);
    new.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn an_adaptive_scrub_reads_the_node_load_and_completes_its_pass() {
    // Nodes report load (ADR-0018); the pass completes with every shard
    // verified, at an adapted rate the controller exports.
    use arca_lake_controller::adaptive::{AdaptivePolicy, Signals};
    use arca_lake_controller::integrity::scrub_pass;
    use arca_lake_controller::metrics::Metrics;
    let c = cluster(3, 2, 1).await;
    seed_objects(&c, 6, 300 << 10).await;
    let nodes = arca_lake_metadata::ops::cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap();
    // Two samples half a second apart: the second covers a window.
    let addr = &nodes[0].address;
    c.clients.health(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    let h = c.clients.health(addr).await.unwrap();
    assert!(
        Signals::from_health(&h).is_some(),
        "the node reports its load"
    );
    let opts = ScrubOptions {
        page: 2,
        page_bytes: 64 << 10,
        rate_bytes_per_s: 1 << 20,
        adaptive: Some(AdaptivePolicy::around(1 << 20)),
        ..unpaced()
    };
    let m = Metrics::default();
    let r = scrub_pass(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        &opts,
        Some(&m),
        None,
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!(r.groups_completed, 1, "{r:?}");
    assert_eq!(r.verified, 18, "6 objects x 3 shards: {r:?}");
    assert!(
        m.scrub_rate_bytes_per_second
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "the adapted rate is exported"
    );
    c.shutdown().await;
}

#[tokio::test]
async fn an_operator_scrub_applies_the_adaptive_policy() {
    // `admin integrity scrub --wait --adaptive` goes through
    // scrub_group_now: the policy must reach its pacer (it was ignored).
    use arca_lake_controller::adaptive::AdaptivePolicy;
    use arca_lake_controller::integrity::scrub_group_now_with;
    use arca_lake_controller::metrics::Metrics;
    let c = cluster(3, 2, 1).await;
    seed_objects(&c, 8, 300 << 10).await;
    let gid = c.groups().await[0].id;
    let opts = ScrubOptions {
        page: 1,
        page_bytes: 32 << 10,
        rate_bytes_per_s: 1 << 20,
        adaptive: Some(AdaptivePolicy::around(1 << 20)),
        ..unpaced()
    };
    let m = Metrics::default();
    let r = scrub_group_now_with(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        gid,
        &opts,
        Some(&m),
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!(r.verified, 24, "{r:?}");
    assert!(
        m.scrub_rate_bytes_per_second
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "the adaptive pacer sampled the node"
    );
    c.shutdown().await;
}
