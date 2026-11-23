//! A group's required failure domain (R-119, ADR-0038): recorded with the
//! group, set by an operator for a group created by 0.1.0 (never inferred
//! from labels), and kept by every member move whatever the controller's
//! `--failure-domain` flag says; a resumed move chosen before the fix is
//! checked again; a member whose domain is unknown holds back only its own
//! group; a group that already breaks its domain is reported, never moved
//! or deleted by the check.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use arca_lake_controller::ControlError;
use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_controller::repair::{RepairOptions, RepairStep, replace_member_with};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::{cluster, epoch_intent, membership};
use arca_lake_metadata::records::{GroupMember, GroupRecord, GroupRole, GroupState, NodeRecord};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms};

const MIB: u64 = 1 << 20;

fn fast(id: &str, flag: &str) -> ControllerConfig {
    let mut c = ControllerConfig::new(id);
    c.lease = Duration::from_millis(600);
    c.gc_interval = Duration::from_secs(3600);
    c.reconcile_interval = Duration::from_secs(3600);
    c.repair_interval = Duration::from_millis(100);
    c.health_interval = Duration::from_millis(100);
    c.unhealthy_after = Duration::from_millis(1500);
    c.stripe_repair_interval = Duration::from_secs(3600);
    c.scrub.interval_ms = 0;
    c.failure_domain = flag.into();
    c
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn set_domain<S: KvStore>(meta: &S, group: u64, label: &str) {
    arca_lake_metadata::ops::group_domain::set(meta, group, label, false, now_ms())
        .await
        .unwrap();
}

async fn recorded<S: KvStore>(meta: &S, group: u64) -> Option<String> {
    arca_lake_metadata::ops::group_domain::get(meta, group)
        .await
        .unwrap()
        .map(|d| d.label)
}

async fn findings<S: KvStore>(meta: &S) -> Vec<String> {
    arca_lake_controller::failure_domain::audit(meta, None, now_ms())
        .await
        .unwrap()
        .findings
        .iter()
        .map(ToString::to_string)
        .collect()
}

async fn nodes<S: KvStore>(meta: &S) -> Vec<NodeRecord> {
    cluster::list_nodes(meta).await.unwrap()
}

fn label<'a>(n: &'a NodeRecord, key: &str) -> Option<&'a str> {
    n.labels
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// Re-register node `id` (same address and devices) with `labels`.
async fn relabel<S: KvStore>(
    meta: &S,
    id: u64,
    labels: &[(&str, &str)],
) -> Result<NodeRecord, arca_lake_metadata::MetaError> {
    let n = nodes(meta).await.into_iter().find(|n| n.id == id).unwrap();
    cluster::register_node(
        meta,
        &n.address,
        labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
        n.devices.clone(),
        now_ms(),
    )
    .await
}

/// Whether the members of `g` are on pairwise distinct values of `key`.
async fn distinct<S: KvStore>(meta: &S, g: &GroupRecord, key: &str) -> bool {
    let all = nodes(meta).await;
    let mut seen = std::collections::BTreeSet::new();
    g.members.iter().all(|m| {
        all.iter()
            .find(|n| n.id == m.node_id)
            .and_then(|n| label(n, key))
            .is_some_and(|v| seen.insert(v.to_string()))
    })
}

/// A group as 0.1.0 created it: no failure domain recorded.
async fn legacy_group<S: KvStore>(meta: &S, members: &[u64]) -> GroupRecord {
    cluster::create_group(
        meta,
        GroupRecord {
            id: 0,
            profile: "test".into(),
            tier: "default".into(),
            scheme: "rs-gf8-v1".into(),
            data_shards: 2,
            parity_shards: 1,
            members: members
                .iter()
                .map(|&node_id| GroupMember {
                    node_id,
                    device_id: 0,
                })
                .collect(),
            epoch: 0,
            state: GroupState::Active,
            repair_cursor: None,
            role: GroupRole::Durable,
        },
    )
    .await
    .unwrap()
}

async fn request_error<S: KvStore>(meta: &S, group: u64) -> Option<String> {
    membership::replace_requests(meta)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.group == group)
        .and_then(|r| r.last_error)
}

/// A group created now records the profile's failure domain with it.
#[tokio::test]
async fn a_new_group_records_its_failure_domain() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    let g = c.groups().await.remove(0);
    assert_eq!(
        recorded(c.meta.as_ref(), g.id).await.as_deref(),
        Some("host")
    );
}

/// A group created by 0.1.0 has no record: the leader moves none of its
/// members (and says why, on the request) until an operator sets it; then
/// the move runs within that domain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_without_a_recorded_domain_moves_no_member_until_it_is_set() {
    let c = TestCluster::start(ClusterOptions {
        groups: 0,
        ..ClusterOptions::rs(4, 2, 1, MIB, 0)
    })
    .await;
    let ids: Vec<u64> = c.nodes.iter().map(|n| n.id).collect();
    let g = legacy_group(c.meta.as_ref(), &ids[..3]).await;
    membership::request_replace(c.meta.as_ref(), g.id, 0, None, "admin", now_ms())
        .await
        .unwrap();
    let ctl = spawn(Arc::clone(&c.meta), fast("ctl-legacy", "host"))
        .await
        .unwrap();
    wait_until(
        "the request to show the refusal",
        Duration::from_secs(30),
        async || {
            request_error(c.meta.as_ref(), g.id)
                .await
                .is_some_and(|e| e.contains("no recorded failure domain"))
        },
    )
    .await;
    let after = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert_eq!(
        (after.epoch, after.members.clone()),
        (g.epoch, g.members.clone())
    );
    set_domain(c.meta.as_ref(), g.id, "host").await;
    wait_until("the move", Duration::from_secs(30), async || {
        let now = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
        now.state == GroupState::Active && now.members[0].node_id == ids[3]
    })
    .await;
    let after = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert!(distinct(c.meta.as_ref(), &after, "host").await);
    ctl.shutdown().await;
}

/// The controller's flag names another label (`rack`) than the group was
/// created with (`host`): the only node in a free rack shares a host with
/// a member, and is refused; the request says why.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_recorded_domain_wins_over_the_controller_flag() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(4, 2, 1, MIB, 0)
    })
    .await;
    let g = c.groups().await.remove(0);
    let spare = c
        .nodes
        .iter()
        .map(|n| n.id)
        .find(|id| !g.members.iter().any(|m| m.node_id == *id))
        .unwrap();
    let all = nodes(c.meta.as_ref()).await;
    let host_of = |id: u64| {
        label(all.iter().find(|n| n.id == id).unwrap(), "host")
            .unwrap()
            .to_string()
    };
    // Every node in its own rack; the spare on member 1's host.
    for (i, m) in g.members.iter().enumerate() {
        let rack = format!("r{i}");
        let host = host_of(m.node_id);
        relabel(
            c.meta.as_ref(),
            m.node_id,
            &[("host", &host), ("rack", &rack), ("device_class", "hdd")],
        )
        .await
        .unwrap();
    }
    let shared = host_of(g.members[1].node_id);
    relabel(
        c.meta.as_ref(),
        spare,
        &[("host", &shared), ("rack", "r9"), ("device_class", "hdd")],
    )
    .await
    .unwrap();
    membership::request_replace(c.meta.as_ref(), g.id, 0, None, "admin", now_ms())
        .await
        .unwrap();
    let ctl = spawn(Arc::clone(&c.meta), fast("ctl-flag", "rack"))
        .await
        .unwrap();
    wait_until(
        "the request to show that no node qualifies",
        Duration::from_secs(30),
        async || {
            let now = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
            assert!(
                distinct(c.meta.as_ref(), &now, "host").await,
                "a member moved onto a host the group uses: {:?}",
                now.members
            );
            request_error(c.meta.as_ref(), g.id).await.is_some()
        },
    )
    .await;
    let after = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert_eq!(after.members, g.members);
    // The dry run uses the same rule, whatever its flag.
    let p = arca_lake_controller::plan::plan_replace(
        c.meta.as_ref(),
        &c.clients,
        g.id,
        0,
        None,
        "rack",
        100,
    )
    .await
    .unwrap();
    assert_eq!(p.moves[0].to_node, None, "{p:?}");
    ctl.shutdown().await;
}

/// A member whose node lacks the group's label: that group's replacement
/// is refused, with the reason on the request, and the pass goes on to
/// the cache groups (a cache member on a stopped node is replaced).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_without_the_label_holds_back_only_its_group() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 0,
        cache_nodes: 2,
        cache: Some(ClusterOptions::default_cache()),
        ..ClusterOptions::rs(4, 2, 1, MIB, 0)
    })
    .await;
    let ids: Vec<u64> = c.nodes.iter().map(|n| n.id).collect();
    // A node registered without `host` while no group needed it, in a
    // group created by 0.1.0 whose domain is then set.
    relabel(c.meta.as_ref(), ids[0], &[("device_class", "hdd")])
        .await
        .unwrap();
    let g = legacy_group(c.meta.as_ref(), &ids[..3]).await;
    set_domain(c.meta.as_ref(), g.id, "host").await;
    let cache_before: Vec<GroupRecord> = c
        .groups()
        .await
        .into_iter()
        .filter(GroupRecord::is_cache)
        .collect();
    let stopped_cache = ids[4];
    let cg = cache_before
        .iter()
        .find(|g| g.members[0].node_id == stopped_cache)
        .unwrap()
        .clone();
    // A durable member and a cache node down.
    c.stop_node(1).await;
    c.stop_node(4).await;
    let mut cfg = fast("ctl-unlabeled", "host");
    cfg.auto_repair = true;
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    wait_until(
        "the cache group's member to move",
        Duration::from_secs(30),
        async || {
            cluster::get_group(c.meta.as_ref(), cg.id)
                .await
                .unwrap()
                .members[0]
                .node_id
                != stopped_cache
        },
    )
    .await;
    let after = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert_eq!(after.members, g.members, "no move without known domains");
    assert!(refusals(&ctl) > 0);
    // With the member back, an operator's request for the group says why.
    c.start_node(1).await;
    membership::request_replace(c.meta.as_ref(), g.id, 2, None, "admin", now_ms())
        .await
        .unwrap();
    wait_until("the request's error", Duration::from_secs(30), async || {
        request_error(c.meta.as_ref(), g.id)
            .await
            .is_some_and(|e| e.contains("no 'host' label"))
    })
    .await;
    ctl.shutdown().await;
}

/// A move started before the fix onto a node sharing a host with another
/// member, interrupted after its fence: the leader that finds the intent
/// does not resume it; it settles it with the membership unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resumed_pre_fix_move_that_breaks_the_domain_is_settled() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(4, 2, 1, MIB, 0)
    })
    .await;
    let g = c.groups().await.remove(0);
    let spare = c
        .nodes
        .iter()
        .map(|n| n.id)
        .find(|id| !g.members.iter().any(|m| m.node_id == *id))
        .unwrap();
    let all = nodes(c.meta.as_ref()).await;
    let shared = label(
        all.iter().find(|n| n.id == g.members[1].node_id).unwrap(),
        "host",
    )
    .unwrap()
    .to_string();
    relabel(
        c.meta.as_ref(),
        spare,
        &[("host", &shared), ("device_class", "hdd")],
    )
    .await
    .unwrap();
    let bad = GroupMember {
        node_id: spare,
        device_id: 0,
    };
    let r = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g.id,
        0,
        bad,
        &RepairOptions::default(),
        async |step| {
            if step == RepairStep::Fenced {
                Err(ControlError::Other(
                    "injected: crash after the fence".into(),
                ))
            } else {
                Ok(())
            }
        },
    )
    .await;
    assert!(r.is_err());
    assert_eq!(epoch_intent::open(c.meta.as_ref()).await.unwrap().len(), 1);
    // The slot is still wanted elsewhere (an operator's request).
    membership::request_replace(c.meta.as_ref(), g.id, 0, None, "admin", now_ms())
        .await
        .unwrap();
    let ctl = spawn(Arc::clone(&c.meta), fast("ctl-resume", "host"))
        .await
        .unwrap();
    wait_until(
        "the intent to be settled",
        Duration::from_secs(30),
        async || {
            epoch_intent::open(c.meta.as_ref())
                .await
                .unwrap()
                .is_empty()
        },
    )
    .await;
    let after = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert_eq!(after.members, g.members, "the pre-fix target was refused");
    assert!(after.epoch > g.epoch, "settled at the fenced epoch");
    assert!(distinct(c.meta.as_ref(), &after, "host").await);
    ctl.shutdown().await;
}

/// Registration: a node without a label that a recorded domain of a group
/// it could join requires is refused; a changed value is accepted, and
/// the group it now breaks is reported (members left in place).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missing_label_is_refused_and_a_changed_one_reported() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(4, 2, 1, MIB, 0)
    })
    .await;
    let g = c.groups().await.remove(0);
    let spare = c
        .nodes
        .iter()
        .map(|n| n.id)
        .find(|id| !g.members.iter().any(|m| m.node_id == *id))
        .unwrap();
    // A spare of the same device class, and a member, without `host`.
    for id in [spare, g.members[0].node_id] {
        let e = relabel(c.meta.as_ref(), id, &[("device_class", "hdd")]).await;
        assert!(
            e.as_ref().is_err_and(|e| e.to_string().contains("'host'")),
            "{e:?}"
        );
    }
    assert_eq!(findings(c.meta.as_ref()).await, Vec::<String>::new());
    // A member's host changed to another member's.
    let all = nodes(c.meta.as_ref()).await;
    let shared = label(
        all.iter().find(|n| n.id == g.members[1].node_id).unwrap(),
        "host",
    )
    .unwrap()
    .to_string();
    relabel(
        c.meta.as_ref(),
        g.members[0].node_id,
        &[("host", &shared), ("device_class", "hdd")],
    )
    .await
    .unwrap();
    let f = findings(c.meta.as_ref()).await;
    assert!(
        f.len() == 1 && f[0].contains(&format!("share 'host={shared}'")),
        "{f:?}"
    );
    // A leader reports it too, and moves nothing.
    let ctl = spawn(Arc::clone(&c.meta), fast("ctl-report", "host"))
        .await
        .unwrap();
    wait_until("a leader pass", Duration::from_secs(30), async || {
        ctl.is_leader() && violations(&ctl) == 1
    })
    .await;
    let after = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert_eq!((after.epoch, after.members), (g.epoch, g.members));
    ctl.shutdown().await;
}

fn violations(ctl: &arca_lake_controller::process::ControllerHandle) -> u64 {
    ctl.metrics
        .groups_failure_domain_violations
        .load(std::sync::atomic::Ordering::Relaxed)
}

fn refusals(ctl: &arca_lake_controller::process::ControllerHandle) -> u64 {
    ctl.metrics
        .failure_domain_refusals_total
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// A move whose target was chosen inside the group's domain, then the
/// target relabeled onto a member's host before the membership commit
/// (between the fence and the commit): the commit refuses in its own
/// transaction and the membership stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_membership_commit_refuses_a_relabel_after_the_choice() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(4, 2, 1, MIB, 0)
    })
    .await;
    let g = c.groups().await.remove(0);
    let spare = c
        .nodes
        .iter()
        .map(|n| n.id)
        .find(|id| !g.members.iter().any(|m| m.node_id == *id))
        .unwrap();
    let all = nodes(c.meta.as_ref()).await;
    let shared = label(
        all.iter().find(|n| n.id == g.members[1].node_id).unwrap(),
        "host",
    )
    .unwrap()
    .to_string();
    let meta = Arc::clone(&c.meta);
    let r = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g.id,
        0,
        GroupMember {
            node_id: spare,
            device_id: 0,
        },
        &RepairOptions::default(),
        async |step| {
            if step == RepairStep::Fenced {
                relabel(
                    meta.as_ref(),
                    spare,
                    &[("host", &shared), ("device_class", "hdd")],
                )
                .await
                .unwrap();
            }
            Ok(())
        },
    )
    .await;
    let e = r.unwrap_err().to_string();
    assert!(e.contains(&format!("host={shared}")), "{e}");
    let after = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert_eq!(after.members, g.members);
}

/// As above, but the group's label is changed (`set-failure-domain
/// --change`) to one the chosen target shares with a member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_membership_commit_refuses_a_changed_label_after_the_choice() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(4, 2, 1, MIB, 0)
    })
    .await;
    let g = c.groups().await.remove(0);
    let spare = c
        .nodes
        .iter()
        .map(|n| n.id)
        .find(|id| !g.members.iter().any(|m| m.node_id == *id))
        .unwrap();
    let all = nodes(c.meta.as_ref()).await;
    let host_of = |id: u64| {
        label(all.iter().find(|n| n.id == id).unwrap(), "host")
            .unwrap()
            .to_string()
    };
    // Racks: r0, r1, r2 for the members; the spare in member 1's rack.
    for (i, m) in g.members.iter().enumerate() {
        let rack = format!("r{i}");
        relabel(
            c.meta.as_ref(),
            m.node_id,
            &[
                ("host", &host_of(m.node_id)),
                ("rack", &rack),
                ("device_class", "hdd"),
            ],
        )
        .await
        .unwrap();
    }
    relabel(
        c.meta.as_ref(),
        spare,
        &[
            ("host", &host_of(spare)),
            ("rack", "r1"),
            ("device_class", "hdd"),
        ],
    )
    .await
    .unwrap();
    let meta = Arc::clone(&c.meta);
    let r = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g.id,
        0,
        GroupMember {
            node_id: spare,
            device_id: 0,
        },
        &RepairOptions::default(),
        async |step| {
            if step == RepairStep::Fenced {
                arca_lake_metadata::ops::group_domain::set(
                    meta.as_ref(),
                    g.id,
                    "rack",
                    true,
                    now_ms(),
                )
                .await
                .unwrap();
            }
            Ok(())
        },
    )
    .await;
    let e = r.unwrap_err().to_string();
    assert!(e.contains("rack=r1"), "{e}");
    let after = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert_eq!(after.members, g.members);
}

/// A drain of a node that is a member of a group without a recorded
/// domain is blocked with the reason, and the dry-run planner shows the
/// same blocker for the drain and for a replacement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_and_the_planner_are_blocked_by_an_unrecorded_group() {
    let c = TestCluster::start(ClusterOptions {
        groups: 0,
        ..ClusterOptions::rs(4, 2, 1, MIB, 0)
    })
    .await;
    let ids: Vec<u64> = c.nodes.iter().map(|n| n.id).collect();
    let g = legacy_group(c.meta.as_ref(), &ids[..3]).await;
    let blocker = |b: &[String]| b.iter().any(|b| b.contains("no recorded failure domain"));
    let p = arca_lake_controller::plan::plan_decommission(
        c.meta.as_ref(),
        &c.clients,
        ids[0],
        "host",
        100,
    )
    .await
    .unwrap();
    let m = p.moves.iter().find(|m| m.group == g.id).unwrap();
    assert!(m.to_node.is_none() && blocker(&m.blockers), "{p:?}");
    let p = arca_lake_controller::plan::plan_replace(
        c.meta.as_ref(),
        &c.clients,
        g.id,
        0,
        None,
        "host",
        100,
    )
    .await
    .unwrap();
    assert!(
        p.moves[0].to_node.is_none() && blocker(&p.moves[0].blockers),
        "{p:?}"
    );
    membership::drain_node(c.meta.as_ref(), ids[0], "admin", now_ms())
        .await
        .unwrap();
    let ctl = spawn(Arc::clone(&c.meta), fast("ctl-drain", "host"))
        .await
        .unwrap();
    wait_until(
        "the drain to show the refusal",
        Duration::from_secs(30),
        async || {
            membership::node_states(c.meta.as_ref())
                .await
                .unwrap()
                .iter()
                .any(|r| {
                    r.node_id == ids[0]
                        && r.blocked
                            .as_deref()
                            .is_some_and(|b| b.contains("no recorded failure domain"))
                })
        },
    )
    .await;
    let after = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert_eq!((after.epoch, after.members), (g.epoch, g.members));
    ctl.shutdown().await;
}

/// A `Repairing` group without a record (a move a 0.1.0 leader
/// committed) whose target fails: the retarget is refused only for the
/// missing record, which its repair status says (not "awaiting nodes"),
/// the at-risk gauge and the audit count; once recorded, it goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unrecorded_repair_awaits_its_failure_domain() {
    use arca_lake_metadata::ops::repair_status::{self, RepairReason};
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(5, 2, 1, MIB, 0)
    })
    .await;
    let g = c.groups().await.remove(0);
    let spare = c
        .nodes
        .iter()
        .position(|n| !g.members.iter().any(|m| m.node_id == n.id))
        .unwrap();
    let target = GroupMember {
        node_id: c.nodes[spare].id,
        device_id: 0,
    };
    let r = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g.id,
        0,
        target,
        &RepairOptions::default(),
        async |step| {
            if step == RepairStep::MembershipCommitted {
                Err(ControlError::Other(
                    "injected: stop after the commit".into(),
                ))
            } else {
                Ok(())
            }
        },
    )
    .await;
    assert!(r.is_err());
    // As if 0.1.0 had made the move: no record.
    arca_lake_metadata::transact(c.meta.as_ref(), g.id, |tx, g| {
        Box::pin(async move {
            arca_lake_metadata::Txn::clear(tx, &arca_lake_metadata::keys::group_domain(*g));
            Ok(())
        })
    })
    .await
    .unwrap();
    c.stop_node(spare).await;
    let mut cfg = fast("ctl-await", "host");
    cfg.auto_repair = true;
    cfg.repair_pacing = arca_lake_controller::repair_liveness::Pacing {
        backoff_base_ms: 100,
        backoff_max_ms: 400,
        unrecoverable_retry_ms: 500,
    };
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    wait_until(
        "the repair to await the failure domain",
        Duration::from_secs(30),
        async || {
            repair_status::get(c.meta.as_ref(), g.id)
                .await
                .unwrap()
                .is_some_and(|s| s.reason == RepairReason::AwaitingFailureDomain)
        },
    )
    .await;
    wait_until("the at-risk gauge", Duration::from_secs(30), async || {
        at_risk(&ctl) == 1
    })
    .await;
    let a = arca_lake_controller::failure_domain::audit(c.meta.as_ref(), None, now_ms())
        .await
        .unwrap();
    assert_eq!(a.at_risk, vec![g.id]);
    set_domain(c.meta.as_ref(), g.id, "host").await;
    wait_until(
        "the repair to finish",
        Duration::from_secs(30),
        async || {
            let now = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
            now.state == GroupState::Active
        },
    )
    .await;
    let after = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert!(!after.members.contains(&target));
    assert!(distinct(c.meta.as_ref(), &after, "host").await);
    wait_until(
        "the at-risk gauge to clear",
        Duration::from_secs(30),
        async || at_risk(&ctl) == 0,
    )
    .await;
    ctl.shutdown().await;
}

fn at_risk(ctl: &arca_lake_controller::process::ControllerHandle) -> u64 {
    ctl.metrics
        .groups_failure_domain_unrecorded_at_risk
        .load(std::sync::atomic::Ordering::Relaxed)
}
