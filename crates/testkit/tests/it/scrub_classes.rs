//! Per-shard scrub classification (0.2.0, RFC-008 §8.4) and the repair of
//! a migration target damaged before its switch (R-120).
//!
//! A record a delete retired and the node keeps until a reclaim (ADR-0028)
//! is held legitimately: the scrub reports it as retained, not as an
//! unexpected shard, from the node's word on that record and the metadata
//! of that chunk, never from counts. Damage stays visible: a damaged
//! retained record is counted under its damage class (not queued: nothing
//! references it); a damaged referenced shard is queued as before; an
//! orphan stays unexpected.

#![allow(clippy::too_many_lines)]

use std::collections::HashMap;
use std::time::Duration;

use arca_lake_controller::groups;
use arca_lake_controller::integrity::{
    RepairWorkerOptions, ScrubOptions, ScrubReport, coders, repair_pass, scrub_group_now,
};
use arca_lake_metadata::ops::integrity::{self as itasks, Finding};
use arca_lake_metadata::ops::{cluster, object};
use arca_lake_metadata::records::{ChunkRecord, GroupRecord, NodeRecord};
use arca_lake_metadata::{Txn, keys, transact};
use arca_lake_protocol::client::ShardRef;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use arca_lake_tiering::durable::Coders;
use arca_lake_tiering::migrate::{MigrateFault, MigrateOutcome, migrate_chunk_with};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "scrubclass";
/// Long enough that nothing is reclaimed unless a test waits it out.
const RETENTION: Duration = Duration::from_secs(2);

async fn cluster(retention: Duration) -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        reclaim_retention: retention,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(seed: u64) -> Bytes {
    Bytes::from(random_bytes(usize::try_from(MIB / 2).unwrap(), seed))
}

async fn put(c: &TestCluster, key: &str, seed: u64) -> Bytes {
    let d = payload(seed);
    s3::put(&c.backend, BUCKET, key, d.clone(), &PutOptions::default())
        .await
        .unwrap();
    d
}

async fn chunk_of(c: &TestCluster, key: &str) -> (GroupRecord, ChunkRecord) {
    let record = c.manifest(BUCKET, key).await.chunks[0].1.clone();
    let group = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == record.placement.group_id)
        .unwrap();
    (group, record)
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

async fn scrub<S: arca_lake_metadata::KvStore + 'static>(
    c: &TestCluster<S>,
    group: u64,
) -> ScrubReport {
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

async fn repair<S: arca_lake_metadata::KvStore + 'static>(
    c: &TestCluster<S>,
    now: u64,
) -> arca_lake_controller::integrity::RepairWorkerReport {
    repair_pass(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        &RepairWorkerOptions {
            tasks_per_pass: 64,
            max_attempts: 3,
            recheck_ms: 1000,
            ..RepairWorkerOptions::default()
        },
        None,
        None,
        now,
    )
    .await
    .unwrap()
}

async fn tasks<S: arca_lake_metadata::KvStore + 'static>(
    c: &TestCluster<S>,
) -> Vec<(u64, [u8; 16], itasks::RepairTask)> {
    itasks::tasks(c.meta.as_ref(), usize::MAX).await.unwrap()
}

async fn snapshot(c: &TestCluster) -> Vec<(Vec<u8>, Vec<u8>)> {
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            Ok(tx
                .get_range(&keys::all_range(), usize::MAX, false)
                .await?
                .into_iter()
                .collect::<Vec<_>>())
        })
    })
    .await
    .unwrap()
}

async fn restore(c: &TestCluster, snap: &[(Vec<u8>, Vec<u8>)]) {
    let snap = snap.to_vec();
    transact(c.meta.as_ref(), snap, |tx, snap| {
        Box::pin(async move {
            tx.clear_range(&keys::all_range());
            for (k, v) in snap {
                tx.set(k, v);
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// No finding of any class, no shard left unclassified.
fn clean(r: &ScrubReport) -> bool {
    r.findings == [0; 8] && r.unclassified == 0 && r.errors == 0
}

/// Before the retention: a deleted, collected object's records are kept on
/// the nodes and reported retained, not unexpected (0.1.0 reported them
/// as unexpected shards, like orphans); after it, the reclaim removes them
/// and they are no longer listed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn records_kept_for_reclaim_are_retained_not_unexpected() {
    let c = cluster(RETENTION).await;
    let kept = put(&c, "kept", 1).await;
    put(&c, "gone", 2).await;
    let (group, _) = chunk_of(&c, "kept").await;
    s3::delete(&c.backend, BUCKET, "gone").await.unwrap();
    let r = c.gc(now_ms() + 3_600_000, 0).await;
    assert!(r.collected >= 1, "{r:?}");
    let r = scrub(&c, group.id).await;
    assert_eq!(r.count(Finding::UnexpectedShard), 0, "{r:?}");
    assert!(clean(&r), "{r:?}");
    assert_eq!((r.retained, r.retained_damaged), (3, 0), "{r:?}");
    assert_eq!(r.verified, 6, "{r:?}");
    assert_eq!(r.enqueued, 0);
    // After the retention: reclaimed, gone from the listing.
    tokio::time::sleep(RETENTION + Duration::from_millis(500)).await;
    assert!(c.reclaim().await.reclaimed >= 3);
    let r = scrub(&c, group.id).await;
    assert!(clean(&r), "{r:?}");
    assert_eq!((r.retained, r.verified), (0, 3), "{r:?}");
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    c.shutdown().await;
}

/// Damage is never hidden by the retained class: a damaged retained record
/// is a finding of its damage class (not queued: nothing references it, so
/// nothing would rebuild it), a damaged referenced shard is a finding and
/// queued, and an unreferenced record no delete retired stays unexpected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn damage_of_retained_and_referenced_shards_and_orphans() {
    let c = cluster(Duration::from_secs(3600)).await;
    let kept = put(&c, "kept", 3).await;
    put(&c, "gone", 4).await;
    let (group, live) = chunk_of(&c, "kept").await;
    let (_, dead) = chunk_of(&c, "gone").await;
    s3::delete(&c.backend, BUCKET, "gone").await.unwrap();
    assert!(c.gc(now_ms() + 3_600_000, 0).await.collected >= 1);
    let n0 = c.node_index(&group.members[0]).unwrap();
    let n1 = c.node_index(&group.members[1]).unwrap();
    assert_eq!(c.corrupt_chunk_on_node(n0, group.id, dead.chunk_id), 1);
    assert_eq!(c.corrupt_chunk_on_node(n1, group.id, live.chunk_id), 1);
    // An orphan: a valid record of a chunk nothing knows, never deleted.
    let stray = ShardRef {
        group_id: group.id,
        chunk_id: [0xAB; 16],
        shard_index: 0,
    };
    c.clients
        .put_shard(
            &c.nodes[n0].addr.to_string(),
            stray,
            group.epoch,
            Bytes::from(vec![7u8; 4096]),
            256 << 10,
        )
        .await
        .unwrap();
    let r = scrub(&c, group.id).await;
    assert_eq!(r.count(Finding::ChecksumMismatch), 2, "{r:?}");
    assert_eq!(r.count(Finding::UnexpectedShard), 1, "the orphan: {r:?}");
    assert_eq!((r.retained, r.retained_damaged), (3, 1), "{r:?}");
    assert_eq!((r.unclassified, r.errors), (0, 0), "{r:?}");
    assert_eq!(r.enqueued, 1, "only the referenced shard: {r:?}");
    let t = tasks(&c).await;
    assert_eq!(t.len(), 1, "{t:?}");
    assert_eq!((t[0].0, t[0].1), (group.id, live.chunk_id));
    let r = repair(&c, now_ms()).await;
    assert_eq!((r.succeeded, r.shards_rewritten), (1, 1), "{r:?}");
    assert_eq!(tasks(&c).await, Vec::new());
    // Still found next pass (the damage of the retained record stays
    // visible until it is reclaimed), the referenced shard repaired.
    let r = scrub(&c, group.id).await;
    assert_eq!(r.count(Finding::ChecksumMismatch), 1, "{r:?}");
    assert_eq!(r.retained_damaged, 1, "{r:?}");
    assert_eq!(r.enqueued, 0, "{r:?}");
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    c.shutdown().await;
}

/// A restore within the retention makes the retained records referenced
/// again: the scrub then checks them as placed shards (no longer
/// retained), and damage to one is queued and repaired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_turns_retained_records_into_placed_shards() {
    let mut c = cluster(Duration::from_secs(3600)).await;
    let data = put(&c, "o", 5).await;
    let (group, chunk) = chunk_of(&c, "o").await;
    let backup = snapshot(&c).await;
    s3::delete(&c.backend, BUCKET, "o").await.unwrap();
    assert!(c.gc(now_ms() + 3_600_000, 0).await.collected >= 1);
    let r = scrub(&c, group.id).await;
    assert!(clean(&r), "{r:?}");
    assert_eq!(r.retained, 3, "{r:?}");
    restore(&c, &backup).await;
    c.restart_backend().await.unwrap();
    let r = scrub(&c, group.id).await;
    assert!(clean(&r), "{r:?}");
    assert_eq!((r.retained, r.verified), (0, 3), "{r:?}");
    let n = c.node_index(&group.members[2]).unwrap();
    assert_eq!(c.corrupt_chunk_on_node(n, group.id, chunk.chunk_id), 1);
    let r = scrub(&c, group.id).await;
    assert_eq!(
        (
            r.count(Finding::ChecksumMismatch),
            r.retained_damaged,
            r.enqueued
        ),
        (1, 0, 1),
        "{r:?}"
    );
    assert_eq!(repair(&c, now_ms()).await.shards_rewritten, 1);
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.shutdown().await;
}

/// Deletes and GC running while the group is scrubbed: whatever a pass
/// sees mid-collection is held (being collected), retained, or (a record
/// changing between the reads) unclassified; never an unexpected shard.
/// Settled, every collected record is retained.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_delete_and_gc_never_report_orphans() {
    let c = cluster(Duration::from_secs(3600)).await;
    let n = 12u64;
    for i in 0..n {
        put(&c, &format!("o{i}"), 10 + i).await;
    }
    let (group, _) = chunk_of(&c, "o0").await;
    let churn = async {
        for i in 0..n {
            s3::delete(&c.backend, BUCKET, &format!("o{i}"))
                .await
                .unwrap();
            if i % 3 == 2 {
                c.gc(now_ms() + 3_600_000, 0).await;
            }
        }
        c.gc(now_ms() + 3_600_000, 0).await;
    };
    let scrubs = async {
        let mut out = Vec::new();
        for _ in 0..6 {
            out.push(scrub(&c, group.id).await);
        }
        out
    };
    let ((), during) = tokio::join!(churn, scrubs);
    for r in &during {
        assert_eq!(r.count(Finding::UnexpectedShard), 0, "{r:?}");
        assert_eq!(r.findings, [0; 8], "{r:?}");
    }
    let r = scrub(&c, group.id).await;
    assert!(clean(&r), "{r:?}");
    assert_eq!(r.retained, 3 * n, "{r:?}");
    c.shutdown().await;
}

// ---- R-120: a migration target damaged before the switch -------------------

const TBUCKET: &str = "tiering";

/// Three nodes of tier `default` plus three of a second durable tier, as
/// `it/tiering.rs`.
async fn tiered() -> (TestCluster, GroupRecord) {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, TBUCKET).await.unwrap();
    for _ in 0..3 {
        c.add_node().await;
    }
    let nodes = cluster::list_nodes(c.meta.as_ref()).await.unwrap();
    for n in nodes.iter().skip(3) {
        cluster::register_node(
            c.meta.as_ref(),
            &n.address,
            vec![
                ("host".into(), format!("ssd-host{}", n.id)),
                ("rack".into(), "rack9".into()),
                ("device_class".into(), "ssd".into()),
            ],
            vec![0],
            now_ms(),
        )
        .await
        .unwrap();
    }
    let fast =
        groups::create_groups_for_class(c.meta.as_ref(), &c.profile, "fast-durable", "ssd", 1)
            .await
            .unwrap()
            .remove(0);
    c.backend.refresh_map().await.unwrap();
    (c, fast)
}

struct Migration {
    source: GroupRecord,
    generation: arca_lake_metadata::records::GenId,
    record: ChunkRecord,
    nodes: HashMap<u64, NodeRecord>,
}

async fn prepare(c: &TestCluster, key: &str, seed: u64) -> (Migration, Bytes) {
    let data = payload(seed);
    s3::put(
        &c.backend,
        TBUCKET,
        key,
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let m = c.manifest(TBUCKET, key).await;
    let source = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == m.chunks[0].1.placement.group_id)
        .unwrap();
    let nodes = cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    (
        Migration {
            source,
            generation: m.pointer.generation,
            record: m.chunks[0].1.clone(),
            nodes,
        },
        data,
    )
}

/// The target's member-0 shard is damaged after the copy was verified and
/// before the switch; the scrub of the target finds and queues it (the
/// copy is in flight: the intent targets the group) and a repair pass runs,
/// all before the switch. The task must survive until the switch makes the
/// record point at the target and then repair it: before the fix it was
/// dropped (committed generation, record in another group), and nothing
/// queued it again until a degraded read of the chunk or the target's next
/// scrub pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_migration_target_damaged_before_the_switch_is_repaired_after_it() {
    let (c, fast) = tiered().await;
    let (mig, data) = prepare(&c, "o", 61).await;
    let coders = Coders::default();
    let addr_of = |id: u64| mig.nodes.get(&id).map(|n| n.address.clone());
    let victim = c.node_index(&fast.members[0]).unwrap();
    let mut before = None;
    let outcome = migrate_chunk_with(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of,
        &mig.source,
        &fast,
        &mig.generation,
        0,
        &mig.record,
        None,
        async || {
            assert_eq!(
                c.corrupt_chunk_on_node(victim, fast.id, mig.record.chunk_id),
                1
            );
            let s = scrub(&c, fast.id).await;
            let r = repair(&c, now_ms()).await;
            let actionable = itasks::any_actionable(c.meta.as_ref()).await.unwrap();
            before = Some((s, r, tasks(&c).await, actionable));
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(outcome, MigrateOutcome::Switched { .. }),
        "{outcome:?}"
    );
    let (s, r, queued, actionable) = before.unwrap();
    assert!(
        !actionable,
        "a task waiting for the switch holds tiering and rebalancing"
    );
    assert_eq!(s.count(Finding::ChecksumMismatch), 1, "{s:?}");
    assert_eq!(
        (s.enqueued, s.in_flight, s.unclassified),
        (1, 2, 0),
        "{s:?}"
    );
    assert_eq!(
        r.dropped, 0,
        "the task was dropped before the switch: {r:?}"
    );
    assert_eq!(queued.len(), 1, "{queued:?}");
    // After the switch, before anything reads the object: the kept task
    // is the only trace of the damage (before the fix the queue was empty
    // here until a degraded read or the target's next scrub pass).
    assert_eq!(
        tasks(&c).await.len(),
        1,
        "the needed repair was lost after the switch"
    );
    assert!(
        itasks::any_actionable(c.meta.as_ref()).await.unwrap(),
        "after the switch the task is repair work"
    );
    c.backend.refresh_map().await.unwrap();
    assert_eq!(s3::get(&c.backend, TBUCKET, "o").await.unwrap().0, data);
    let r = repair(&c, now_ms() + 120_000).await;
    assert_eq!((r.succeeded, r.shards_rewritten), (1, 1), "{r:?}");
    assert_eq!(tasks(&c).await, Vec::new());
    let s = scrub(&c, fast.id).await;
    assert!(clean(&s), "{s:?}");
    assert_eq!(s.verified, 3, "{s:?}");
    c.shutdown().await;
}

/// The same damage, but the switch is refused (the policy that decided the
/// move is gone: the attempt cleans its target up and ends its intent):
/// the task ends at the next attempt, never retried forever; a retry of
/// the migration then copies afresh and switches to a whole stripe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_migration_ends_the_task_and_a_retry_switches_whole() {
    let (c, fast) = tiered().await;
    let (mig, data) = prepare(&c, "o", 62).await;
    let coders = Coders::default();
    let addr_of = |id: u64| mig.nodes.get(&id).map(|n| n.address.clone());
    let victim = c.node_index(&fast.members[1]).unwrap();
    let mut kept = None;
    let outcome = arca_lake_tiering::migrate::migrate_chunk_for_tiering(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of,
        &mig.source,
        &fast,
        &mig.generation,
        0,
        &mig.record,
        1,
        async || {
            c.corrupt_chunk_on_node(victim, fast.id, mig.record.chunk_id);
            assert_eq!(scrub(&c, fast.id).await.enqueued, 1);
            repair(&c, now_ms()).await;
            kept = Some(tasks(&c).await.len());
        },
    )
    .await
    .unwrap();
    // No tiering record: the switch is refused as a policy change.
    assert_eq!(outcome, MigrateOutcome::PolicyChanged);
    assert_eq!(kept, Some(1), "kept while the intent targeted the group");
    let cid = mig.record.chunk_id;
    let intent = transact(c.meta.as_ref(), cid, |tx, cid| {
        Box::pin(async move { Ok(tx.get(&keys::migration(cid)).await?) })
    })
    .await
    .unwrap();
    assert!(intent.is_none(), "the refused attempt ended its intent");
    let r = repair(&c, now_ms() + 120_000).await;
    assert_eq!(r.dropped, 1, "{r:?}");
    assert_eq!(tasks(&c).await, Vec::new());
    // The retry.
    let outcome = migrate_chunk_with(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of,
        &mig.source,
        &fast,
        &mig.generation,
        0,
        &mig.record,
        None,
        async || {},
    )
    .await
    .unwrap();
    assert!(
        matches!(outcome, MigrateOutcome::Switched { .. }),
        "{outcome:?}"
    );
    let s = scrub(&c, fast.id).await;
    assert!(clean(&s), "{s:?}");
    assert_eq!(s.verified, 3, "{s:?}");
    c.backend.refresh_map().await.unwrap();
    assert_eq!(s3::get(&c.backend, TBUCKET, "o").await.unwrap().0, data);
    c.shutdown().await;
}

/// A migration that stopped before its switch (a crash: the intent stays
/// until it is stale): the task waits while the intent is fresh, and is
/// dropped once it is stale (the copy can no longer switch), so the queue
/// empties; any later attempt starts from a fresh copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_migration_holds_the_task_only_while_its_intent_is_fresh() {
    let (c, fast) = tiered().await;
    let (mig, _) = prepare(&c, "o", 63).await;
    let coders = Coders::default();
    let addr_of = |id: u64| mig.nodes.get(&id).map(|n| n.address.clone());
    let r = migrate_chunk_with(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of,
        &mig.source,
        &fast,
        &mig.generation,
        0,
        &mig.record,
        Some(MigrateFault::BeforeSwitch),
        async || {},
    )
    .await;
    assert!(r.is_err());
    let victim = c.node_index(&fast.members[2]).unwrap();
    assert_eq!(
        c.corrupt_chunk_on_node(victim, fast.id, mig.record.chunk_id),
        1
    );
    assert_eq!(scrub(&c, fast.id).await.enqueued, 1);
    let now = now_ms();
    let r = repair(&c, now).await;
    assert_eq!((r.dropped, r.deferred), (0, 1), "{r:?}");
    assert_eq!(tasks(&c).await.len(), 1);
    // While it waits, tiering and rebalancing are not held by it (the
    // copy and the switch run in one call: a crashed copy never switches).
    assert!(!itasks::any_actionable(c.meta.as_ref()).await.unwrap());
    let rb = arca_lake_controller::rebalance::run_once(
        c.meta.as_ref(),
        &c.clients,
        &arca_lake_controller::rebalance::RebalancePolicy::default(),
        now,
    )
    .await
    .unwrap();
    assert_ne!(rb.reason, "repair tasks open: repair first", "{rb:?}");
    // A scrub after the intent's TTL: the stale copy is no longer in
    // flight (an orphan, the damaged one under its damage class).
    let late = scrub_group_now(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        fast.id,
        &unpaced(),
        now + object::MIGRATION_INTENT_TTL_MS + 1,
    )
    .await
    .unwrap();
    assert_eq!(
        (
            late.in_flight,
            late.count(Finding::UnexpectedShard),
            late.count(Finding::ChecksumMismatch)
        ),
        (0, 2, 1),
        "{late:?}"
    );
    // Past the intent's TTL (and the task's retry delay): stale, dropped.
    let r = repair(&c, now + object::MIGRATION_INTENT_TTL_MS + 120_000).await;
    assert_eq!(r.dropped, 1, "{r:?}");
    assert_eq!(tasks(&c).await, Vec::new());
    c.shutdown().await;
}

/// A record written again between the scrub's page and its re-read of the
/// key (another sequence): the pass cannot tell what it saw, so the shard
/// is unclassified (an error, not counted verified), never an orphan; the
/// next pass classifies the new record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_rewritten_between_the_reads_is_unclassified() {
    use arca_lake_protocol::client::{FaultAction, FaultHook, Rpc, RpcContext};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let c = cluster(Duration::from_secs(3600)).await;
    put(&c, "kept", 7).await;
    let (group, _) = chunk_of(&c, "kept").await;
    let n0 = c.node_index(&group.members[0]).unwrap();
    let addr = c.nodes[n0].addr.to_string();
    let stray = ShardRef {
        group_id: group.id,
        chunk_id: [0xCD; 16],
        shard_index: 0,
    };
    let bytes = Bytes::from(vec![9u8; 4096]);
    c.clients
        .put_shard(&addr, stray, group.epoch, bytes.clone(), 256 << 10)
        .await
        .unwrap();
    let clean_pass = scrub(&c, group.id).await;
    // The first ListShards (the scrub's re-read of the orphan candidate)
    // waits until the record was written again.
    let armed = Arc::new(AtomicBool::new(true));
    let held = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    c.clients.set_fault_hook(Some({
        let (armed, held, gate) = (Arc::clone(&armed), Arc::clone(&held), Arc::clone(&gate));
        let hook: FaultHook = Arc::new(move |ctx: &RpcContext<'_>| {
            if ctx.rpc == Rpc::ListShards && armed.swap(false, Ordering::SeqCst) {
                held.fetch_add(1, Ordering::SeqCst);
                FaultAction::Hold(Arc::clone(&gate))
            } else {
                FaultAction::None
            }
        });
        hook
    }));
    let rewrite = async {
        let t = std::time::Instant::now();
        while held.load(Ordering::SeqCst) == 0 {
            assert!(t.elapsed() < Duration::from_secs(20), "never re-read");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        c.clients
            .put_shard(&addr, stray, group.epoch, bytes.clone(), 256 << 10)
            .await
            .unwrap();
        gate.add_permits(1);
    };
    let (r, ()) = tokio::join!(scrub(&c, group.id), rewrite);
    c.clients.set_fault_hook(None);
    assert_eq!(r.count(Finding::UnexpectedShard), 0, "{r:?}");
    assert_eq!((r.unclassified, r.errors), (1, 1), "{r:?}");
    assert_eq!(r.verified, clean_pass.verified - 1, "{r:?}");
    let r = scrub(&c, group.id).await;
    assert_eq!(r.count(Finding::UnexpectedShard), 1, "{r:?}");
    assert_eq!(r.unclassified, 0, "{r:?}");
    c.shutdown().await;
}

/// A shard whose chunk ID carries the prefix of a pending generation is
/// held as that write's (the generation may still record it, as
/// reconciliation keeps it); once the generation is aborted nothing holds
/// it and it is an orphan. A shard with no generation at all is an orphan
/// at once (a `begin_put` still being retried included: reconciliation
/// waits `min_orphan_age` before removing it, the scrub does not excuse
/// it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_orphan_sharing_a_pending_generations_prefix() {
    let c = cluster(Duration::from_secs(3600)).await;
    put(&c, "kept", 8).await;
    let (group, _) = chunk_of(&c, "kept").await;
    let n0 = c.node_index(&group.members[0]).unwrap();
    let addr = c.nodes[n0].addr.to_string();
    let p = object::begin_put(c.meta.as_ref(), BUCKET, "pending", now_ms(), 600_000)
        .await
        .unwrap();
    let mut chunk_id = [0u8; 16];
    chunk_id[..12].copy_from_slice(&p.generation[..12]);
    chunk_id[12..].copy_from_slice(&99u32.to_be_bytes());
    let stray = ShardRef {
        group_id: group.id,
        chunk_id,
        shard_index: 0,
    };
    c.clients
        .put_shard(
            &addr,
            stray,
            group.epoch,
            Bytes::from(vec![5u8; 4096]),
            256 << 10,
        )
        .await
        .unwrap();
    let r = scrub(&c, group.id).await;
    assert!(clean(&r), "{r:?}");
    assert_eq!(r.in_flight, 1, "{r:?}");
    object::abort_put(c.meta.as_ref(), &p.generation, now_ms())
        .await
        .unwrap();
    let r = scrub(&c, group.id).await;
    assert_eq!(
        (r.in_flight, r.count(Finding::UnexpectedShard)),
        (0, 1),
        "{r:?}"
    );
    c.shutdown().await;
}
