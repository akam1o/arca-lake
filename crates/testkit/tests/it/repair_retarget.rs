//! Repair retargeting and liveness (Phase 3.13, R-104, ADR-0029): a
//! member rebuild whose target goes down, keeps refusing writes, or is
//! interrupted by a leader crash moves to another target through a fenced
//! epoch change; a second failed member is replaced only once the slot
//! being rebuilt is whole; a group that cannot finish does not hold drains
//! or other groups' repairs; and a group is never made `Active` without
//! every chunk's shards.

#![allow(clippy::too_many_lines)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arca_lake_controller::ControlError;
use arca_lake_controller::process::{ControllerConfig, RpcFaultHook, spawn};
use arca_lake_controller::repair::{RepairOptions, RepairStep, replace_member_with};
use arca_lake_controller::repair_liveness::{self, Pacing};
use arca_lake_metadata::ops::membership::{self, NodeLifecycle};
use arca_lake_metadata::ops::repair_status::{self, RepairReason};
use arca_lake_metadata::ops::{cluster, epoch_intent};
use arca_lake_metadata::records::{GroupMember, GroupRecord, GroupState};
use arca_lake_protocol::client::{FaultAction, NodeClients, Rpc, ShardRef};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "retarget";

fn fast(id: &str) -> ControllerConfig {
    let mut c = ControllerConfig::new(id);
    c.lease = Duration::from_millis(600);
    c.gc_interval = Duration::from_secs(3600);
    c.reconcile_interval = Duration::from_secs(3600);
    c.repair_interval = Duration::from_millis(200);
    c.health_interval = Duration::from_millis(100);
    c.unhealthy_after = Duration::from_millis(1500);
    c.stripe_repair_interval = Duration::from_secs(3600);
    c.scrub.interval_ms = 0;
    c.auto_repair = true;
    c.failure_repair_chunks_per_pass = 2;
    c.repair_pacing = Pacing {
        backoff_base_ms: 100,
        backoff_max_ms: 400,
        unrecoverable_retry_ms: 500,
    };
    c
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn group(c: &TestCluster, id: u64) -> GroupRecord {
    cluster::get_group(c.meta.as_ref(), id).await.unwrap()
}

async fn status(c: &TestCluster, id: u64) -> Option<repair_status::RepairStatus> {
    repair_status::get(c.meta.as_ref(), id).await.unwrap()
}

async fn put_objects(c: &TestCluster, n: u64, seed: u64) -> Vec<(String, Bytes)> {
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut out = Vec::new();
    for i in 0..n {
        let key = format!("o{i}");
        let data = Bytes::from(random_bytes(usize::try_from(MIB / 4).unwrap(), seed + i));
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

async fn all_read(c: &TestCluster, objects: &[(String, Bytes)]) {
    for (k, d) in objects {
        assert_eq!(&s3::get(&c.backend, BUCKET, k).await.unwrap().0, d, "{k}");
    }
}

fn durable(groups: Vec<GroupRecord>) -> Vec<GroupRecord> {
    let mut g: Vec<GroupRecord> = groups.into_iter().filter(|g| !g.is_cache()).collect();
    g.sort_by_key(|g| g.id);
    g
}

/// R-104 (1): the target of a failure repair goes down after the
/// membership change committed. Before Phase 3.13 the group stayed
/// `Repairing` onto the dead target for good. Now the leader (here a new
/// one: the first is stopped meanwhile) rebuilds the slot onto another
/// node; the first attempt's epoch is fenced on the new target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_that_goes_down_mid_repair_is_replaced_and_the_old_epoch_is_fenced() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(6, 2, 1, MIB, 0)
    })
    .await;
    let objects = put_objects(&c, 24, 100).await;
    let g0 = durable(c.groups().await).remove(0);
    let lost = g0.members[0].node_id;
    let li = c.nodes.iter().position(|n| n.id == lost).unwrap();
    c.stop_node(li).await;
    let mut cfg = fast("ctl-a");
    cfg.repair_interval = Duration::from_millis(500);
    cfg.failure_repair_chunks_per_pass = 1;
    let a = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    // Rebuilding onto a first target, with at least one chunk written.
    let mut first = 0;
    wait_until("a rebuild under way", Duration::from_secs(30), async || {
        let g = group(&c, g0.id).await;
        if g.state != GroupState::Repairing || g.members[0].node_id == lost {
            return false;
        }
        first = g.members[0].node_id;
        let ti = c.nodes.iter().position(|n| n.id == first).unwrap();
        c.shard_count(ti, g0.id) > 0
    })
    .await;
    let first_epoch = group(&c, g0.id).await.epoch;
    a.shutdown().await;
    let ti = c.nodes.iter().position(|n| n.id == first).unwrap();
    c.stop_node(ti).await;
    assert_eq!(
        group(&c, g0.id).await.state,
        GroupState::Repairing,
        "set-up: the rebuild finished before its target was stopped"
    );
    let b = spawn(Arc::clone(&c.meta), fast("ctl-b")).await.unwrap();
    wait_until(
        "the rebuild onto another target",
        Duration::from_secs(60),
        async || {
            let g = group(&c, g0.id).await;
            g.state == GroupState::Active && g.members[0].node_id != first
        },
    )
    .await;
    let done = group(&c, g0.id).await;
    assert_ne!(done.members[0].node_id, lost);
    assert!(done.epoch > first_epoch);
    assert!(b.metrics.repair_retargets_total.load(Ordering::Relaxed) >= 1);
    assert!(
        status(&c, g0.id).await.is_none(),
        "status cleared once Active"
    );
    all_read(&c, &objects).await;
    // A write of the first attempt's epoch is refused by the new target.
    let ni = c
        .nodes
        .iter()
        .position(|n| n.id == done.members[0].node_id)
        .unwrap();
    let stale = c
        .clients
        .put_shard(
            &c.nodes[ni].addr.to_string(),
            ShardRef {
                group_id: g0.id,
                chunk_id: [9; 16],
                shard_index: 0,
            },
            first_epoch,
            Bytes::from(vec![1u8; 4096]),
            1024,
        )
        .await
        .unwrap_err();
    assert!(NodeClients::is_epoch_rejected(&stale), "{stale}");
    // The first target comes back: nothing it holds is used or counted.
    c.start_node(ti).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(group(&c, g0.id).await.members, done.members);
    all_read(&c, &objects).await;
    b.shutdown().await;
    c.shutdown().await;
}

/// R-104 (2): the target answers but refuses every write (disk full, a
/// failing disk). It is replaced after repeated failures; the group never
/// becomes `Active` before the new target holds every chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_refusing_writes_is_replaced_after_repeated_failures() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(6, 2, 1, MIB, 0)
    })
    .await;
    let objects = put_objects(&c, 12, 200).await;
    let g0 = durable(c.groups().await).remove(0);
    let li = c.node_index(&g0.members[0]).unwrap();
    c.stop_node(li).await;
    // The first node that receives a rebuilt shard refuses it, every time.
    let refusing: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let refused = Arc::new(AtomicU64::new(0));
    let mut cfg = fast("ctl-refuse");
    let gid = g0.id;
    cfg.rpc_fault_hook = Some(RpcFaultHook({
        let (refusing, refused) = (Arc::clone(&refusing), Arc::clone(&refused));
        Arc::new(move |ctx| {
            if ctx.rpc != Rpc::PutShard || ctx.group_id != Some(gid) {
                return FaultAction::None;
            }
            let mut r = refusing.lock().unwrap();
            let addr = r.get_or_insert_with(|| ctx.addr.to_string());
            if addr == ctx.addr {
                refused.fetch_add(1, Ordering::SeqCst);
                FaultAction::FailBefore(tonic::Code::ResourceExhausted, "no room".into())
            } else {
                FaultAction::None
            }
        })
    }));
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    wait_until(
        "the repair to complete elsewhere",
        Duration::from_secs(60),
        async || {
            let g = group(&c, g0.id).await;
            g.state == GroupState::Active && g.members[0] != g0.members[0]
        },
    )
    .await;
    let done = group(&c, g0.id).await;
    let bad = refusing.lock().unwrap().clone().unwrap();
    let bad_id = c
        .nodes
        .iter()
        .find(|n| n.addr.to_string() == bad)
        .unwrap()
        .id;
    assert_ne!(done.members[0].node_id, bad_id);
    assert!(
        refused.load(Ordering::SeqCst) >= u64::from(repair_liveness::RETARGET_AFTER),
        "replaced only after repeated failures"
    );
    let ni = c.node_index(&done.members[0]).unwrap();
    assert_eq!(
        c.shard_count(ni, g0.id),
        c.shard_count(c.node_index(&done.members[1]).unwrap(), g0.id)
    );
    all_read(&c, &objects).await;
    ctl.shutdown().await;
    c.shutdown().await;
}

/// R-104 (3), a code with one parity: while slot 0 is rebuilt, member 1
/// fails too. The chunks slot 0 still lacks have one readable shard of
/// the two they need: the group waits for nodes, member 1 is not replaced
/// (its shards are needed), and the group is not made `Active`. Member 1
/// returns: the rebuild completes onto the same target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_failure_that_leaves_fewer_than_k_shards_waits_for_the_node() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(6, 2, 1, MIB, 0)
    })
    .await;
    let objects = put_objects(&c, 24, 300).await;
    let g0 = durable(c.groups().await).remove(0);
    let li = c.node_index(&g0.members[0]).unwrap();
    c.stop_node(li).await;
    let mut cfg = fast("ctl-double1");
    cfg.repair_interval = Duration::from_millis(500);
    cfg.failure_repair_chunks_per_pass = 1;
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let mut target = 0;
    wait_until("a rebuild under way", Duration::from_secs(30), async || {
        let g = group(&c, g0.id).await;
        target = g.members[0].node_id;
        g.state == GroupState::Repairing
            && target != g0.members[0].node_id
            && c.shard_count(c.nodes.iter().position(|n| n.id == target).unwrap(), g0.id) > 0
    })
    .await;
    let second = c.node_index(&g0.members[1]).unwrap();
    c.stop_node(second).await;
    wait_until("the wait for nodes", Duration::from_secs(30), async || {
        status(&c, g0.id)
            .await
            .is_some_and(|s| s.reason == RepairReason::AwaitingNodes)
    })
    .await;
    // Some seconds of passes: member 1 stays, the group stays Repairing.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let mid = group(&c, g0.id).await;
    assert_eq!(mid.state, GroupState::Repairing);
    assert_eq!(
        mid.members[1], g0.members[1],
        "a member whose shards are needed was replaced"
    );
    assert_eq!(mid.members[0].node_id, target);
    c.start_node(second).await;
    wait_until(
        "the rebuild to complete",
        Duration::from_secs(60),
        async || group(&c, g0.id).await.state == GroupState::Active,
    )
    .await;
    let done = group(&c, g0.id).await;
    assert_eq!(done.members[0].node_id, target);
    assert_eq!(done.members[1], g0.members[1]);
    all_read(&c, &objects).await;
    ctl.shutdown().await;
    c.shutdown().await;
}

/// R-104 (4), two parities: member 1 fails while slot 0 is rebuilt. Slot
/// 0 is rebuilt from the others; once whole, member 1 is replaced too, and
/// the group becomes `Active` with both slots on new nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_failure_is_replaced_once_the_first_slot_is_whole() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(8, 2, 2, MIB, 0)
    })
    .await;
    let objects = put_objects(&c, 16, 400).await;
    let g0 = durable(c.groups().await).remove(0);
    let li = c.node_index(&g0.members[0]).unwrap();
    c.stop_node(li).await;
    let mut cfg = fast("ctl-double2");
    cfg.repair_interval = Duration::from_millis(400);
    cfg.failure_repair_chunks_per_pass = 1;
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    wait_until("a rebuild under way", Duration::from_secs(30), async || {
        let g = group(&c, g0.id).await;
        g.state == GroupState::Repairing && g.members[0] != g0.members[0]
    })
    .await;
    let second = c.node_index(&g0.members[1]).unwrap();
    c.stop_node(second).await;
    wait_until("both slots rebuilt", Duration::from_secs(90), async || {
        let g = group(&c, g0.id).await;
        g.state == GroupState::Active && g.members[1] != g0.members[1]
    })
    .await;
    let done = group(&c, g0.id).await;
    assert_ne!(done.members[0], g0.members[0]);
    all_read(&c, &objects).await;
    ctl.shutdown().await;
    c.shutdown().await;
}

/// R-104 (5): a group with a chunk no member can rebuild (fewer than `k`
/// readable shards while every member answers) stays `Repairing`, reported
/// as such, retried rarely; it holds neither another group's drain nor
/// planned background work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unrecoverable_group_does_not_hold_drains_or_background_work() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(9, 2, 1, MIB, 0)
    })
    .await;
    let objects = put_objects(&c, 24, 500).await;
    let gs = durable(c.groups().await);
    let (bad, good) = (gs[0].clone(), gs[1].clone());
    // One chunk of the bad group: its shard on member 1 damaged, then
    // member 0 lost. The chunk has one readable shard of two.
    let chunk = {
        let mut found = None;
        for (k, _) in &objects {
            let m = c.manifest(BUCKET, k).await;
            if let Some((_, ch)) = m
                .chunks
                .iter()
                .find(|(_, ch)| ch.placement.group_id == bad.id)
            {
                found = Some(ch.chunk_id);
                break;
            }
        }
        found.expect("a chunk in the first group")
    };
    let m1 = c.node_index(&bad.members[1]).unwrap();
    assert_eq!(c.corrupt_chunk_on_node(m1, bad.id, chunk), 1);
    let m0 = c.node_index(&bad.members[0]).unwrap();
    c.stop_node(m0).await;
    let ctl = spawn(Arc::clone(&c.meta), fast("ctl-unrec")).await.unwrap();
    wait_until(
        "the unrecoverable status",
        Duration::from_secs(60),
        async || {
            status(&c, bad.id)
                .await
                .is_some_and(|s| s.reason == RepairReason::DataUnrecoverable)
        },
    )
    .await;
    assert_eq!(group(&c, bad.id).await.state, GroupState::Repairing);
    // The unrecoverable group never holds them (the other group may, while
    // its own repair progresses: the groups can share the stopped node).
    let statuses = repair_liveness::statuses(c.meta.as_ref()).await.unwrap();
    let holding = repair_liveness::holding_group(&c.groups().await, &statuses);
    assert!(
        holding.as_ref().is_none_or(|(g, _)| *g != bad.id),
        "a group that cannot progress holds rebalancing and tiering: {holding:?}"
    );
    // A drain of a member of the other group completes.
    let leaving = good.members[2].node_id;
    membership::drain_node(c.meta.as_ref(), leaving, "admin", now_ms())
        .await
        .unwrap();
    wait_until(
        "the other group's drain",
        Duration::from_secs(60),
        async || {
            membership::node_states(c.meta.as_ref())
                .await
                .unwrap()
                .into_iter()
                .any(|r| {
                    r.node_id == leaving && matches!(r.state, NodeLifecycle::Decommissioned { .. })
                })
        },
    )
    .await;
    assert_eq!(
        group(&c, bad.id).await.state,
        GroupState::Repairing,
        "never Active without the chunk"
    );
    // Phase 3.14: shown by reason (a possible data loss), never as
    // restored; the object of the chunk does not read back as if whole.
    let text = ctl.metrics.render();
    assert!(
        text.contains("arca_controller_repair_groups{reason=\"data-unrecoverable\"} 1"),
        "{}",
        text.lines()
            .filter(|l| l.contains("repair_groups"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    for (k, d) in &objects {
        let m = c.manifest(BUCKET, k).await;
        if m.chunks.iter().any(|(_, ch)| ch.chunk_id == chunk) {
            assert!(
                s3::get(&c.backend, BUCKET, k).await.is_err(),
                "an unrecoverable chunk's object read back"
            );
            continue;
        }
        assert_eq!(&s3::get(&c.backend, BUCKET, k).await.unwrap().0, d, "{k}");
    }
    ctl.shutdown().await;
    c.shutdown().await;
}

/// R-104 (6): a leader crashes between raising the new target's epoch and
/// committing a target change of a rebuild (the old target still answers:
/// the change was for repeated failures). The next leader resumes the
/// change rather than settling it, without auto repair.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_change_interrupted_by_a_leader_crash_is_resumed() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(6, 2, 1, MIB, 0)
    })
    .await;
    let objects = put_objects(&c, 12, 600).await;
    let g0 = durable(c.groups().await).remove(0);
    let spares: Vec<u64> = c
        .nodes
        .iter()
        .map(|n| n.id)
        .filter(|id| !g0.members.iter().any(|m| m.node_id == *id))
        .collect();
    let li = c.node_index(&g0.members[0]).unwrap();
    c.stop_node(li).await;
    let member = |id| GroupMember {
        node_id: id,
        device_id: 0,
    };
    // A rebuild onto spares[0], stopped after its first chunk.
    let e = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g0.id,
        0,
        member(spares[0]),
        &RepairOptions::default(),
        async |step| match step {
            RepairStep::Chunk { index: 0, .. } => Err(ControlError::Other("crash".into())),
            _ => Ok(()),
        },
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("crash"));
    // Its target change to spares[1], stopped after the fence.
    let e = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g0.id,
        0,
        member(spares[1]),
        &RepairOptions::default(),
        async |step| match step {
            RepairStep::Fenced => Err(ControlError::Other("crash".into())),
            _ => Ok(()),
        },
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("crash"));
    assert_eq!(epoch_intent::open(c.meta.as_ref()).await.unwrap().len(), 1);
    // Every running node seen just now (the old target answers: it is not
    // leaving for being unhealthy).
    for (i, n) in c.nodes.iter().enumerate() {
        if c.node_running(i) {
            cluster::touch_node(c.meta.as_ref(), n.id, now_ms())
                .await
                .unwrap();
        }
    }
    let mut cfg = fast("ctl-resume");
    cfg.auto_repair = false;
    // Every node that matters runs: a wider window than the others' so
    // that a slow first pass does not see the change's target as gone (a
    // settle would then be the safe, and correct, outcome).
    cfg.unhealthy_after = Duration::from_secs(10);
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    wait_until("the resumed change", Duration::from_secs(60), async || {
        group(&c, g0.id).await.state == GroupState::Active
    })
    .await;
    let done = group(&c, g0.id).await;
    assert_eq!(done.members[0].node_id, spares[1], "resumed, not settled");
    let left = epoch_intent::open(c.meta.as_ref()).await.unwrap().len();
    assert_eq!(left, 0, "the intent is finished");
    all_read(&c, &objects).await;
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Phase 3.13 (found with the previous-release run): a new leader makes no
/// repair decision before its own first health pass. The nodes' last-seen
/// times are an earlier leader's, stale by the failover gap; taken at face
/// value, every node looked failed and a healthy member was repaired away
/// (then a real failure left the group short of shards).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_repairs_nothing_before_its_first_health_pass() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(6, 2, 1, MIB, 0)
    })
    .await;
    let objects = put_objects(&c, 6, 700).await;
    let g0 = durable(c.groups().await).remove(0);
    // The members last seen long ago (no controller ran meanwhile), the
    // spares just now (as an earlier leader may have left them).
    tokio::time::sleep(Duration::from_secs(2)).await;
    for n in &c.nodes {
        if !g0.members.iter().any(|m| m.node_id == n.id) {
            cluster::touch_node(c.meta.as_ref(), n.id, now_ms())
                .await
                .unwrap();
        }
    }
    // Its health probes slow: the first health pass ends seconds after the
    // first repair passes.
    let mut cfg = fast("ctl-fresh");
    cfg.rpc_fault_hook = Some(RpcFaultHook(Arc::new(|ctx| {
        if ctx.rpc == Rpc::Health {
            FaultAction::DelayBefore(Duration::from_millis(1500))
        } else {
            FaultAction::None
        }
    })));
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    tokio::time::sleep(Duration::from_secs(8)).await;
    let g = group(&c, g0.id).await;
    assert_eq!(g.members, g0.members, "a healthy member was repaired away");
    assert_eq!(g.state, GroupState::Active);
    all_read(&c, &objects).await;
    ctl.shutdown().await;
    c.shutdown().await;
}
