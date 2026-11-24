//! Device replacement and node decommission (RFC-013, Phase 3.3 §6): the
//! controller leader carries out operator intents with the existing
//! member-replacement state machine, resumes after a crash, stops safely
//! when no replacement fits, pauses while the cluster is not Active, and
//! reports a node decommissioned only when no group needs it.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use arca_lake_controller::ControlError;
use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_controller::repair::{RepairOptions, RepairStep, replace_member_with};
use arca_lake_metadata::ops::membership::{self, NodeLifecycle};
use arca_lake_metadata::ops::{cluster, compat};
use arca_lake_metadata::records::{ClusterPhase, GroupMember, GroupRecord, GroupState};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "members";

fn fast(id: &str) -> ControllerConfig {
    let mut c = ControllerConfig::new(id);
    c.lease = Duration::from_millis(600);
    c.gc_interval = Duration::from_secs(3600);
    c.reconcile_interval = Duration::from_secs(3600);
    c.repair_interval = Duration::from_millis(100);
    c.health_interval = Duration::from_millis(100);
    c.unhealthy_after = Duration::from_millis(1500);
    c.stripe_repair_interval = Duration::from_secs(3600);
    c.scrub.interval_ms = 0;
    c
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait for `cond` as long as `progress` keeps changing: fails if it stays
/// the same for `stall`, or if the whole wait exceeds `limit`. For work
/// whose duration depends on how much there is and on the runner's speed.
async fn wait_while_progressing<P: PartialEq + std::fmt::Debug>(
    what: &str,
    stall: Duration,
    limit: Duration,
    mut progress: impl AsyncFnMut() -> P,
    mut cond: impl AsyncFnMut() -> bool,
) {
    let start = std::time::Instant::now();
    let mut last = progress().await;
    let mut since = std::time::Instant::now();
    while !cond().await {
        let now = progress().await;
        if now != last {
            last = now;
            since = std::time::Instant::now();
        }
        assert!(
            since.elapsed() < stall,
            "{what}: no progress for {stall:?} (at {last:?})"
        );
        assert!(start.elapsed() < limit, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn group(c: &TestCluster, id: u64) -> GroupRecord {
    cluster::get_group(c.meta.as_ref(), id).await.unwrap()
}

async fn lifecycle(c: &TestCluster, node: u64) -> Option<membership::NodeStateRecord> {
    membership::node_states(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.node_id == node)
}

async fn decommissioned(c: &TestCluster, node: u64) -> bool {
    lifecycle(c, node)
        .await
        .is_some_and(|r| matches!(r.state, NodeLifecycle::Decommissioned { .. }))
}

#[tokio::test]
async fn drains_resume_after_a_crash_cover_cache_groups_pause_when_not_active_and_stop_without_capacity()
 {
    // Five durable nodes (a 2+1 group uses three: two spares), three cache
    // nodes for a cache group.
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        cache_nodes: 3,
        cache: Some(ClusterOptions::default_cache()),
        ..ClusterOptions::rs(5, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut objects = Vec::new();
    for i in 0..10u64 {
        let d = Bytes::from(random_bytes(usize::try_from(MIB / 2).unwrap(), i));
        s3::put(
            &c.backend,
            BUCKET,
            &format!("o{i}"),
            d.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        objects.push((format!("o{i}"), d));
    }
    let groups = c.groups().await;
    let durable = groups.iter().find(|g| !g.is_cache()).unwrap().clone();
    let cache = groups.iter().find(|g| g.is_cache()).unwrap().clone();
    let all_hdd: Vec<u64> = cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .filter(|n| {
            n.labels
                .iter()
                .any(|(k, v)| k == "device_class" && v == "hdd")
        })
        .map(|n| n.id)
        .collect();
    assert_eq!(all_hdd.len(), 5);
    let spares: Vec<u64> = all_hdd
        .iter()
        .copied()
        .filter(|n| !durable.members.iter().any(|m| m.node_id == *n))
        .collect();
    assert_eq!(spares.len(), 2);

    // ---- 1. Drain a durable member; the leader that started replacing
    // it crashed mid-rebuild (the state it leaves behind is reproduced with
    // the same state machine stopped by its hook).
    let victim = durable.members[0].node_id;
    membership::drain_node(c.meta.as_ref(), victim, "admin", now_ms())
        .await
        .unwrap();
    let e = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        durable.id,
        0,
        GroupMember {
            node_id: spares[0],
            device_id: 0,
        },
        &RepairOptions::default(),
        async |step| match step {
            RepairStep::Chunk { index: 3, .. } => Err(ControlError::Other("crash".into())),
            _ => Ok(()),
        },
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("crash"), "{e}");
    let mid = group(&c, durable.id).await;
    assert_eq!(mid.state, GroupState::Repairing);
    assert!(mid.repair_cursor.is_some());
    assert!(!decommissioned(&c, victim).await, "not while rebuilding");
    let ctl = spawn(Arc::clone(&c.meta), fast("a")).await.unwrap();
    wait_until("the drain to finish", Duration::from_secs(30), async || {
        decommissioned(&c, victim).await
    })
    .await;
    let after = group(&c, durable.id).await;
    assert_eq!(after.state, GroupState::Active);
    assert!(after.epoch > durable.epoch);
    assert_eq!(
        after.members[0].node_id, spares[0],
        "resumed, not re-planned"
    );
    assert!(!after.members.iter().any(|m| m.node_id == victim));
    // The drained node can go away: every object still reads.
    let vi = c.nodes.iter().position(|n| n.id == victim).unwrap();
    c.stop_node(vi).await;
    for (k, d) in &objects {
        assert_eq!(&s3::get(&c.backend, BUCKET, k).await.unwrap().0, d, "{k}");
    }

    // ---- 2. A cache-group member (an unprotected one-copy group: the
    // membership change is the repair; reads refill from the durable
    // tier): replaced by a cache node, never by a durable one.
    let cache_victim = cache.members[0].node_id;
    let ci = c.add_cache_node().await;
    let cache_spare = c.nodes[ci].id;
    membership::drain_node(c.meta.as_ref(), cache_victim, "admin", now_ms())
        .await
        .unwrap();
    wait_until("the cache drain", Duration::from_secs(30), async || {
        decommissioned(&c, cache_victim).await
    })
    .await;
    let cg = group(&c, cache.id).await;
    let nvme: Vec<u64> = cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .filter(|n| {
            n.labels
                .iter()
                .any(|(k, v)| k == "device_class" && v == "nvme")
        })
        .map(|n| n.id)
        .collect();
    assert!(nvme.contains(&cg.members[0].node_id), "{cg:?}");
    assert_eq!(cg.state, GroupState::Active);
    let _ = cache_spare;
    assert!(!cg.members.iter().any(|m| m.node_id == cache_victim));
    assert_eq!(
        group(&c, durable.id).await.members,
        after.members,
        "the durable group was not touched"
    );

    // ---- 3. Not Active: a drain is paused (and says so), nothing moves.
    compat::set_phase(c.meta.as_ref(), ClusterPhase::ReadOnly, "admin", now_ms())
        .await
        .unwrap();
    let second = after.members[1].node_id;
    membership::drain_node(c.meta.as_ref(), second, "admin", now_ms())
        .await
        .unwrap();
    wait_until(
        "the pause to be reported",
        Duration::from_secs(10),
        async || {
            lifecycle(&c, second)
                .await
                .and_then(|r| r.blocked)
                .is_some_and(|b| b.contains("not Active"))
        },
    )
    .await;
    assert_eq!(group(&c, durable.id).await.members, after.members);
    membership::cancel_drain(c.meta.as_ref(), second)
        .await
        .unwrap();
    compat::set_phase(c.meta.as_ref(), ClusterPhase::Active, "admin", now_ms())
        .await
        .unwrap();

    // ---- 4. An operator replacement onto a named node.
    membership::request_replace(
        c.meta.as_ref(),
        durable.id,
        2,
        Some(spares[1]),
        "admin",
        now_ms(),
    )
    .await
    .unwrap();
    wait_until("the replacement", Duration::from_secs(30), async || {
        membership::replace_requests(c.meta.as_ref())
            .await
            .unwrap()
            .is_empty()
    })
    .await;
    let g4 = group(&c, durable.id).await;
    assert_eq!(g4.state, GroupState::Active);
    assert_eq!(g4.members[2].node_id, spares[1]);

    // ---- 5. No capacity left: the member freed by step 4 is stopped, so
    // no healthy hdd node is outside the group; the drain stops with a
    // reason and the group is untouched.
    let freed = after.members[2].node_id;
    let fi = c.nodes.iter().position(|n| n.id == freed).unwrap();
    c.stop_node(fi).await;
    let third = g4.members[1].node_id;
    membership::drain_node(c.meta.as_ref(), third, "admin", now_ms())
        .await
        .unwrap();
    wait_until("the blocked reason", Duration::from_secs(10), async || {
        lifecycle(&c, third)
            .await
            .and_then(|r| r.blocked)
            .is_some_and(|b| b.contains("no healthy replacement"))
    })
    .await;
    assert_eq!(group(&c, durable.id).await.members, g4.members);
    assert!(matches!(
        lifecycle(&c, third).await.unwrap().state,
        NodeLifecycle::Draining { .. }
    ));
    // A decommissioned node can be neither drained again nor named as a
    // replacement target that the leader would accept.
    assert!(
        membership::drain_node(c.meta.as_ref(), victim, "admin", now_ms())
            .await
            .is_err()
    );
    for (k, d) in &objects {
        assert_eq!(&s3::get(&c.backend, BUCKET, k).await.unwrap().0, d, "{k}");
    }
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Phase 3.10 audit (found under load in `membership_faults`): a write that
/// failed part-way in one group and was placed in another leaves a shard
/// of a live generation's chunk in the first group. Repair enumerated it
/// from the members' listings and, finding fewer than `k` shards, retried
/// it as unrecoverable forever: a member move of that group never
/// finished, although the object was whole in the other group. The
/// leftover is now an orphan to repair (the chunk record places it
/// elsewhere); the move completes and the object reads back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_is_not_held_up_by_a_failed_writes_leftover_in_the_group() {
    let c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(7, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let data = Bytes::from(random_bytes(usize::try_from(MIB / 2).unwrap(), 31));
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let chunk = c.manifest(BUCKET, "o").await.chunks[0].1.clone();
    let other = c
        .groups()
        .await
        .into_iter()
        .find(|g| !g.is_cache() && g.id != chunk.placement.group_id)
        .unwrap();
    // The leftover: shard 2 of the chunk on the other group's member 2.
    let holder = c.node_index(&other.members[2]).unwrap();
    c.clients
        .put_shard(
            &c.nodes[holder].addr.to_string(),
            arca_lake_protocol::client::ShardRef {
                group_id: other.id,
                chunk_id: chunk.chunk_id,
                shard_index: 2,
            },
            other.epoch,
            Bytes::from(vec![7u8; 4096]),
            1024,
        )
        .await
        .unwrap();
    let ctl = spawn(Arc::clone(&c.meta), fast("ctl-leftover"))
        .await
        .unwrap();
    membership::request_replace(c.meta.as_ref(), other.id, 0, None, "admin", now_ms())
        .await
        .unwrap();
    wait_until("the move", Duration::from_secs(30), async || {
        membership::replace_requests(c.meta.as_ref())
            .await
            .unwrap()
            .is_empty()
            && group(&c, other.id).await.state == GroupState::Active
    })
    .await;
    assert_ne!(group(&c, other.id).await.members[0], other.members[0]);
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Phase 3.10 audit: one group's repair failing every pass (its
/// replacement node, named by the operator, is down) no longer stops the
/// repair of the groups after it. Before, the error ended the pass at that
/// group, every pass. Phase 3.13 (R-104): and the group itself no longer
/// stays `Repairing` onto the dead node: its slot is rebuilt elsewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_that_cannot_finish_its_repair_does_not_stop_the_others() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(9, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut groups: Vec<GroupRecord> = c
        .groups()
        .await
        .into_iter()
        .filter(|g| !g.is_cache())
        .collect();
    groups.sort_by_key(|g| g.id);
    let (stuck, other) = (groups[0].clone(), groups[1].clone());
    for i in 0..40 {
        let key = format!("o{i}");
        let data = Bytes::from(random_bytes(usize::try_from(MIB / 2).unwrap(), 40 + i));
        s3::put(&c.backend, BUCKET, &key, data, &PutOptions::default())
            .await
            .unwrap();
    }
    // A spare node (in no group) as the first group's named replacement,
    // then down.
    let all = c.groups().await;
    let spare = c
        .nodes
        .iter()
        .map(|n| n.id)
        .find(|id| {
            !all.iter()
                .any(|g| g.members.iter().any(|m| m.node_id == *id))
        })
        .expect("a spare node");
    membership::request_replace(c.meta.as_ref(), stuck.id, 0, Some(spare), "admin", now_ms())
        .await
        .unwrap();
    // Moves of one chunk per pass: the first is still under way when its
    // replacement goes down.
    let mut cfg = fast("ctl-isolated");
    cfg.planned_move_chunks_per_pass = 1;
    // One chunk a second: the move takes many seconds (on a slow runner
    // a short one used to finish before its replacement was stopped).
    cfg.repair_interval = Duration::from_secs(1);
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    wait_until(
        "the first group's move",
        Duration::from_secs(30),
        async || group(&c, stuck.id).await.state == GroupState::Repairing,
    )
    .await;
    let i = c.nodes.iter().position(|n| n.id == spare).unwrap();
    c.stop_node(i).await;
    assert_eq!(
        group(&c, stuck.id).await.state,
        GroupState::Repairing,
        "set-up: the first group's move finished before its replacement was stopped"
    );
    membership::request_replace(c.meta.as_ref(), other.id, 0, None, "admin", now_ms())
        .await
        .unwrap();
    // The other group's move also takes one chunk a second (the budget is
    // the controller's): its duration is its chunk count, on a slow runner
    // more (CI, 3bcf22d: a fixed 30 s ran out). It must keep advancing,
    // a pass at a time, and finish within a bound from its chunk count.
    let mut other_chunks = 0u32;
    for i in 0..40 {
        other_chunks += u32::try_from(
            c.manifest(BUCKET, &format!("o{i}"))
                .await
                .chunks
                .iter()
                .filter(|(_, ch)| ch.placement.group_id == other.id)
                .count(),
        )
        .unwrap();
    }
    assert!(other_chunks > 0, "set-up: the other group holds chunks");
    wait_while_progressing(
        "the other group's move",
        Duration::from_secs(20),
        Duration::from_secs(30) + Duration::from_secs(3) * other_chunks,
        async || {
            let g = group(&c, other.id).await;
            (g.state, g.repair_cursor, g.members[0].node_id)
        },
        async || {
            let g = group(&c, other.id).await;
            g.state == GroupState::Active && g.members[0] != other.members[0]
        },
    )
    .await;
    wait_until(
        "the first group's rebuild onto another node",
        Duration::from_secs(60),
        async || {
            let g = group(&c, stuck.id).await;
            g.state == GroupState::Active
                && g.members[0].node_id != spare
                && g.members[0] != stuck.members[0]
        },
    )
    .await;
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Phase 3.10 audit: an operator's move off a healthy node no longer holds
/// up the repair of a failed member in another group. The failed member's
/// group is repaired first; the move goes on with a per-pass budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_member_is_repaired_before_a_planned_move() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(8, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut groups: Vec<GroupRecord> = c
        .groups()
        .await
        .into_iter()
        .filter(|g| !g.is_cache())
        .collect();
    groups.sort_by_key(|g| g.id);
    let (planned, failed) = (groups[0].clone(), groups[1].clone());
    for i in 0..40 {
        let key = format!("o{i}");
        let data = Bytes::from(random_bytes(usize::try_from(MIB / 2).unwrap(), 80 + i));
        s3::put(&c.backend, BUCKET, &key, data, &PutOptions::default())
            .await
            .unwrap();
    }
    // The planned move first in list order; the failed member second.
    membership::request_replace(c.meta.as_ref(), planned.id, 0, None, "admin", now_ms())
        .await
        .unwrap();
    let down = c.node_index(&failed.members[0]).unwrap();
    c.stop_node(down).await;
    membership::request_replace(c.meta.as_ref(), failed.id, 0, None, "admin", now_ms())
        .await
        .unwrap();
    let mut cfg = fast("ctl-order");
    cfg.repair_interval = Duration::from_millis(400);
    cfg.planned_move_chunks_per_pass = 1;
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    wait_until(
        "the failed member's repair",
        Duration::from_secs(60),
        async || {
            let g = group(&c, failed.id).await;
            g.state == GroupState::Active && g.members[0] != failed.members[0]
        },
    )
    .await;
    let p = group(&c, planned.id).await;
    assert!(
        p.state == GroupState::Repairing || p.members[0] == planned.members[0],
        "the planned move finished first: {p:?}"
    );
    wait_until("the planned move", Duration::from_secs(120), async || {
        let g = group(&c, planned.id).await;
        g.state == GroupState::Active && g.members[0] != planned.members[0]
    })
    .await;
    ctl.shutdown().await;
    c.shutdown().await;
}
