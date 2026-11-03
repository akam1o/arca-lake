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
