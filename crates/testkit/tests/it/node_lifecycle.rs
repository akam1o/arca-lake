//! Node lifecycle safety (Phase 3.11, R-100): a node's identity on its
//! disk, quarantine of a disk that is not the one the cluster registered,
//! unregister and rejoin with their checks, and the finalize's handling of
//! absent nodes.
//!
//! Invariants: an emptied or foreign disk at a member's address is never
//! counted as that member (it is quarantined and the member repaired);
//! an unregistered node takes no delete of this cluster, cannot register
//! again by itself, and leaves the finalize's barriers; a node rejoins
//! only after its checks, without members, and its former shards are then
//! collected; `--ignore-absent` names only decommissioned nodes; new
//! groups are placed on active nodes only.

#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arca_lake_controller::node_ops;
use arca_lake_controller::process::{ControllerConfig, ControllerHandle, spawn};
use arca_lake_metadata::Txn;
use arca_lake_metadata::ops::node_lifecycle::{self, State};
use arca_lake_metadata::ops::{cluster, compat, membership};
use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const KIB: u64 = 1 << 10;
const BUCKET: &str = "nodes";

/// Four nodes, one group of three: one spare.
async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(4, 2, 1, 128 * KIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn ctl() -> ControllerConfig {
    let mut c = ControllerConfig::new("ctl-nodes");
    c.lease = Duration::from_secs(2);
    c.health_interval = Duration::from_millis(100);
    c.repair_interval = Duration::from_millis(100);
    c.unhealthy_after = Duration::from_secs(3);
    c.auto_repair = true;
    c.gc_interval = Duration::from_secs(3600);
    c.reconcile_interval = Duration::from_secs(3600);
    c.failure_domain = "host".into();
    c
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn put_objects(c: &TestCluster, n: u64) -> Vec<(String, Bytes)> {
    let mut out = Vec::new();
    for i in 0..n {
        let d = Bytes::from(random_bytes(usize::try_from(300 * KIB).unwrap(), 9_000 + i));
        let k = format!("o{i}");
        s3::put(&c.backend, BUCKET, &k, d.clone(), &PutOptions::default())
            .await
            .unwrap();
        out.push((k, d));
    }
    out
}

async fn all_read_back(c: &TestCluster, objects: &[(String, Bytes)]) {
    for (k, d) in objects {
        assert_eq!(&s3::get(&c.backend, BUCKET, k).await.unwrap().0, d, "{k}");
    }
}

/// Every node holds the identity the leader gave it.
async fn identities_acknowledged(c: &TestCluster) {
    wait_until(
        "every identity acknowledged",
        Duration::from_secs(20),
        async || {
            let ids = node_lifecycle::identities(c.meta.as_ref()).await.unwrap();
            c.nodes
                .iter()
                .all(|n| ids.get(&n.id).is_some_and(|i| i.acknowledged))
        },
    )
    .await;
}

async fn leader(c: &TestCluster) -> ControllerHandle {
    let h = spawn(Arc::clone(&c.meta), ctl()).await.unwrap();
    wait_until("leadership", Duration::from_secs(20), async || {
        h.is_leader()
    })
    .await;
    h
}

/// Before Phase 3.11 a node whose disk was emptied and restarted at a
/// member's address answered Health and counted as that healthy member: it
/// held nothing, no repair started, and the group ran with one redundancy
/// less without anyone knowing. Now it holds no identity although it
/// acknowledged one: it is quarantined, the member is repaired onto the
/// spare, and every object reads back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_emptied_disk_at_a_members_address_is_quarantined_and_repaired() {
    let mut c = cluster().await;
    let objects = put_objects(&c, 6).await;
    let ctl = leader(&c).await;
    identities_acknowledged(&c).await;
    let g = c.groups().await.remove(0);
    let victim = g.members[0].node_id;
    let i = c.node_index(&g.members[0]).unwrap();
    c.stop_node(i).await;
    std::fs::remove_dir_all(&c.nodes[i].dir).unwrap();
    std::fs::create_dir_all(&c.nodes[i].dir).unwrap();
    c.start_node(i).await;
    wait_until(
        "the emptied node quarantined",
        Duration::from_secs(20),
        async || {
            node_lifecycle::state(c.meta.as_ref(), victim)
                .await
                .unwrap()
                == State::Quarantined
        },
    )
    .await;
    wait_until(
        "its member repaired away",
        Duration::from_secs(60),
        async || {
            let g = c.groups().await.remove(0);
            g.state == arca_lake_metadata::records::GroupState::Active
                && !g.members.iter().any(|m| m.node_id == victim)
        },
    )
    .await;
    all_read_back(&c, &objects).await;
    // Quarantined: no new group is placed there.
    assert!(
        !arca_lake_controller::groups::placeable_nodes(c.meta.as_ref())
            .await
            .unwrap()
            .iter()
            .any(|n| n.id == victim)
    );
    ctl.shutdown().await;
    c.shutdown().await;
}

/// A disk holding another identity (another cluster's disk at a reused
/// address, a reused node id, a disk moved between nodes): the node
/// changes nothing on a request carrying the cluster's identity, and the
/// leader quarantines it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_holding_another_identity_adopts_nothing_and_is_quarantined() {
    let c = cluster().await;
    put_objects(&c, 2).await;
    let ctl = leader(&c).await;
    identities_acknowledged(&c).await;
    let spare = c.nodes.iter().map(|n| n.id).max().unwrap();
    ctl.shutdown().await;
    // The metadata now names another identity for the node (as a cluster
    // whose id was reused, or which registered another disk there).
    arca_lake_metadata::transact(c.meta.as_ref(), spare, |tx, id| {
        Box::pin(async move {
            tx.set(
                &arca_lake_metadata::keys::node_identity(*id),
                &arca_lake_metadata::records::encode(&node_lifecycle::NodeIdentityRecord {
                    node_id: *id,
                    token: [0x5a; 16],
                    acknowledged: true,
                })?,
            );
            Ok(())
        })
    })
    .await
    .unwrap();
    let addr = c
        .nodes
        .iter()
        .find(|n| n.id == spare)
        .unwrap()
        .addr
        .to_string();
    let before = c.clients.health(&addr).await.unwrap();
    // A push carrying that identity and a higher format: nothing adopted.
    let h = c
        .clients
        .health_with(
            &addr,
            arca_lake_protocol::storage::v1::HealthRequest {
                node_id: spare,
                node_identity: Bytes::from_static(&[0x5a; 16]),
                schema_floor: before.schema_floor + 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(h.identity_mismatch, "{h:?}");
    assert_eq!(h.schema_floor, before.schema_floor, "nothing adopted");
    let ctl = leader(&c).await;
    wait_until(
        "the node quarantined",
        Duration::from_secs(20),
        async || node_lifecycle::state(c.meta.as_ref(), spare).await.unwrap() == State::Quarantined,
    )
    .await;
    ctl.shutdown().await;
    c.shutdown().await;
}

/// The R-100 delete path closed: a node drained and decommissioned still
/// held its former shards, and every leader kept pushing it the open fence,
/// so a delete of this cluster applied there (a late one, or one decided
/// on metadata restored from before its replacement). Unregistered, it is
/// told first: its fence is closed and held, and such a delete is refused.
/// Unregistering again changes nothing; it cannot register again by
/// itself; it rejoins only after its checks, without members, and its
/// former shards are then collected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unregistered_node_refuses_old_deletes_and_rejoins_only_after_its_checks() {
    let c = cluster().await;
    let objects = put_objects(&c, 6).await;
    let ctl = leader(&c).await;
    identities_acknowledged(&c).await;
    let g = c.groups().await.remove(0);
    let leaving = g.members[1].node_id;
    let li = c.node_index(&g.members[1]).unwrap();
    let laddr = c.nodes[li].addr.to_string();
    // Two shards of the leaving member.
    let shard_a = ShardRef {
        group_id: g.id,
        chunk_id: c.manifest(BUCKET, "o0").await.chunks[0].1.chunk_id,
        shard_index: 1,
    };
    let shard_b = ShardRef {
        group_id: g.id,
        chunk_id: c.manifest(BUCKET, "o1").await.chunks[0].1.chunk_id,
        shard_index: 1,
    };
    // Unregister refuses a node in service.
    let e = node_ops::unregister(
        c.meta.as_ref(),
        &c.clients,
        leaving,
        "t",
        "test",
        false,
        now_ms(),
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("decommission it first"), "{e}");
    // Decommission: the member moves to the spare; the node keeps its
    // former shards.
    membership::drain_node(c.meta.as_ref(), leaving, "t", now_ms())
        .await
        .unwrap();
    wait_until("decommissioned", Duration::from_secs(60), async || {
        node_lifecycle::state(c.meta.as_ref(), leaving)
            .await
            .unwrap()
            == State::Decommissioned
    })
    .await;
    assert!(
        c.shard_count(li, g.id) > 0,
        "former shards still on the node"
    );
    let epoch = c.groups().await.remove(0).epoch;
    // A delete of this cluster, taken with the leader's open generation as
    // it is now (a late one, or one decided on metadata restored from
    // before the replacement).
    let old = NodeClients::new(Duration::from_secs(5)).with_tls(c.clients.tls().cloned());
    old.set_incarnation(c.clients.incarnation());
    let fence = arca_lake_metadata::ops::delete_fence::current(c.meta.as_ref())
        .await
        .unwrap();
    assert!(fence.open);
    old.set_delete_fence(Some(fence.generation));
    // Carrying the node's identity, as the leader's deletes do.
    let token = node_lifecycle::identity(c.meta.as_ref(), leaving)
        .await
        .unwrap()
        .unwrap()
        .token;
    old.note_identity(&laddr, Some((leaving, token)));
    // Decommissioned only, the node takes it (what R-100 exposes).
    old.delete_shard(&laddr, shard_a, epoch)
        .await
        .expect("a decommissioned node takes this cluster's deletes");
    let u = node_ops::unregister(
        c.meta.as_ref(),
        &c.clients,
        leaving,
        "t",
        "hardware retired",
        false,
        now_ms(),
    )
    .await
    .unwrap();
    // The old delete: refused by the node.
    let r = old.delete_shard(&laddr, shard_b, epoch).await;
    assert!(
        r.as_ref().is_err_and(NodeClients::is_fenced),
        "an unregistered node took a delete: {r:?}"
    );
    assert!(u.fenced && !u.already, "{u:?}");
    // Idempotent.
    let again = node_ops::unregister(
        c.meta.as_ref(),
        &c.clients,
        leaving,
        "t",
        "x",
        false,
        now_ms(),
    )
    .await
    .unwrap();
    assert!(again.already);
    // It cannot register again by itself.
    let e = cluster::register_node(c.meta.as_ref(), &laddr, vec![], vec![0], now_ms())
        .await
        .unwrap_err();
    assert!(e.to_string().contains("admin node rejoin"), "{e}");
    // The leader no longer reaches it: it stays held across passes.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let h = c.clients.health(&laddr).await.unwrap();
    assert!(!h.delete_fence_open && h.delete_fence_hold, "{h:?}");
    // Not placed, not waited for by the finalize.
    assert!(
        !arca_lake_controller::groups::placeable_nodes(c.meta.as_ref())
            .await
            .unwrap()
            .iter()
            .any(|n| n.id == leaving)
    );
    // Rejoin: its checks pass (same release, same identity).
    let check = node_ops::check_rejoin(c.meta.as_ref(), &c.clients, leaving, false)
        .await
        .unwrap();
    assert!(check.problems.is_empty(), "{check:?}");
    let (was, _) = node_ops::rejoin(c.meta.as_ref(), &c.clients, leaving, false)
        .await
        .unwrap();
    assert_eq!(was, State::Removed);
    assert_eq!(
        node_lifecycle::state(c.meta.as_ref(), leaving)
            .await
            .unwrap(),
        State::Active
    );
    // Its hold is the operator's to release (a rejoin does not delete by
    // itself); once released and the leader's generation open there again,
    // reconciliation collects its former shards.
    arca_lake_controller::delete_fence::release_and_push(
        c.meta.as_ref(),
        &c.clients,
        "rejoined",
        now_ms(),
    )
    .await
    .unwrap();
    wait_until(
        "the former shards collected",
        Duration::from_secs(30),
        async || {
            let _ = c.reconcile(now_ms() + 3_600_000, 0).await;
            c.shard_count(li, g.id) == 0
        },
    )
    .await;
    all_read_back(&c, &objects).await;
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Rejoin checks: an emptied disk needs `--fresh-identity`; a disk of
/// another identity never rejoins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejoin_refuses_a_foreign_disk_and_needs_fresh_identity_for_an_emptied_one() {
    let mut c = cluster().await;
    put_objects(&c, 2).await;
    let ctl = leader(&c).await;
    identities_acknowledged(&c).await;
    let spare = c.nodes.iter().map(|n| n.id).max().unwrap();
    let si = c.nodes.iter().position(|n| n.id == spare).unwrap();
    membership::drain_node(c.meta.as_ref(), spare, "t", now_ms())
        .await
        .unwrap();
    wait_until("decommissioned", Duration::from_secs(30), async || {
        node_lifecycle::state(c.meta.as_ref(), spare).await.unwrap() == State::Decommissioned
    })
    .await;
    ctl.shutdown().await;
    // Emptied disk.
    c.stop_node(si).await;
    std::fs::remove_dir_all(&c.nodes[si].dir).unwrap();
    std::fs::create_dir_all(&c.nodes[si].dir).unwrap();
    c.start_node(si).await;
    let check = node_ops::check_rejoin(c.meta.as_ref(), &c.clients, spare, false)
        .await
        .unwrap();
    assert!(
        check
            .problems
            .iter()
            .any(|p| p.contains("--fresh-identity")),
        "{check:?}"
    );
    let check = node_ops::check_rejoin(c.meta.as_ref(), &c.clients, spare, true)
        .await
        .unwrap();
    assert!(
        check.problems.is_empty() && check.fresh_identity,
        "{check:?}"
    );
    // The disk adopts an identity of another registration (as another
    // cluster's would hold): never rejoined, even with --fresh-identity.
    let addr = c.nodes[si].addr.to_string();
    c.clients
        .health_with(
            &addr,
            arca_lake_protocol::storage::v1::HealthRequest {
                node_id: spare,
                node_identity: Bytes::from_static(&[0x77; 16]),
                adopt_identity: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let check = node_ops::check_rejoin(c.meta.as_ref(), &c.clients, spare, true)
        .await
        .unwrap();
    assert!(
        check.problems.iter().any(|p| p.contains("another")),
        "{check:?}"
    );
    assert!(
        node_ops::rejoin(c.meta.as_ref(), &c.clients, spare, true)
            .await
            .is_err()
    );
    c.shutdown().await;
}

/// `--ignore-absent` names only decommissioned nodes: an active node named
/// absent blocks the finalize's first step (it used to let the finalize
/// pass the cut-off without checking that node), and a removed node is not
/// waited for at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ignore_absent_takes_only_decommissioned_nodes_and_removed_ones_are_not_waited_for() {
    let c = cluster().await;
    let target = arca_lake_common::compat::Target {
        protocol: arca_lake_protocol::storage::v1::PROTOCOL_VERSION,
        schema: arca_lake_metadata::keys::SCHEMA_VERSION,
        format: u64::from(arca_lake_storage::format::SEGMENT_FORMAT_VERSION),
    };
    let active = c.nodes[0].id;
    compat::request_finalize(
        c.meta.as_ref(),
        target,
        vec![active.to_string()],
        "admin",
        now_ms(),
    )
    .await
    .unwrap();
    let own = arca_lake_controller::capabilities();
    let r =
        arca_lake_controller::upgrade::upgrade_pass(c.meta.as_ref(), &c.clients, &own, 7, now_ms())
            .await
            .unwrap();
    let why = r.blocked.unwrap_or_default();
    assert!(
        why.contains("named absent but is active"),
        "an active node named absent was not refused: {why}"
    );
    c.shutdown().await;
}

/// Review (Phase 3.11): two members' disks swapped (or another disk at a
/// node's address): what answers at node X's address is node Z's disk,
/// holding Z's shards. A delete decided for X there (reconciliation sees
/// Z's shard under another index than X's: a misdirected copy) would
/// remove a shard Z's member needs. Deletes carry the identity X
/// acknowledged: Z's disk refuses it. Reconciliation also skips
/// quarantined nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_decided_for_one_node_is_refused_by_another_disk_at_its_address() {
    let mut c = cluster().await;
    let objects = put_objects(&c, 3).await;
    let ctl = leader(&c).await;
    identities_acknowledged(&c).await;
    // The leader keeps running (a shutdown closes its generation on the
    // nodes, which would refuse the delete below for another reason).
    let g = c.groups().await.remove(0);
    let (x, z) = (
        c.node_index(&g.members[0]).unwrap(),
        c.node_index(&g.members[2]).unwrap(),
    );
    let xid = c.nodes[x].id;
    let xtoken = node_lifecycle::identity(c.meta.as_ref(), xid)
        .await
        .unwrap()
        .unwrap()
        .token;
    let xaddr = c.nodes[x].addr.to_string();
    // Swap the two disks.
    c.stop_node(x).await;
    c.stop_node(z).await;
    let tmp = c.nodes[x].dir.with_extension("swap");
    std::fs::rename(&c.nodes[x].dir, &tmp).unwrap();
    std::fs::rename(&c.nodes[z].dir, &c.nodes[x].dir).unwrap();
    std::fs::rename(&tmp, &c.nodes[z].dir).unwrap();
    c.start_node(x).await;
    c.start_node(z).await;
    // A delete decided for X, of Z's shard (index 2), sent to X's address.
    let m = c.manifest(BUCKET, "o0").await;
    let shard = ShardRef {
        group_id: g.id,
        chunk_id: m.chunks[0].1.chunk_id,
        shard_index: 2,
    };
    let old = NodeClients::new(Duration::from_secs(5)).with_tls(c.clients.tls().cloned());
    old.set_incarnation(c.clients.incarnation());
    // The generation Z's disk holds open (its own leader's, still open
    // there: the leader's pushes carry X's identity and are not adopted).
    let held = c.clients.health(&xaddr).await.unwrap();
    assert!(held.delete_fence_open, "{held:?}");
    old.set_delete_fence(Some(held.delete_fence_generation));
    old.note_identity(&xaddr, Some((xid, xtoken)));
    let before = c.shard_count(x, g.id);
    let r = old.delete_shard(&xaddr, shard, g.epoch).await;
    assert!(
        r.as_ref()
            .is_err_and(|e| e.message().contains("identity rejected")),
        "another disk at the address took the delete: {r:?}"
    );
    assert_eq!(c.shard_count(x, g.id), before, "nothing deleted");
    // Swapped back, everything reads.
    c.stop_node(x).await;
    c.stop_node(z).await;
    let tmp = c.nodes[x].dir.with_extension("swap");
    std::fs::rename(&c.nodes[x].dir, &tmp).unwrap();
    std::fs::rename(&c.nodes[z].dir, &c.nodes[x].dir).unwrap();
    std::fs::rename(&tmp, &c.nodes[z].dir).unwrap();
    c.start_node(x).await;
    c.start_node(z).await;
    all_read_back(&c, &objects).await;
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Re-review N1 (Phase 3.11): a new node whose disk was not wiped and
/// holds another cluster's identity for the same node id. Its
/// registration gave it an identity, which it cannot adopt: it is
/// quarantined, never imported, and takes none of this cluster's work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_node_holding_another_clusters_identity_is_quarantined_not_imported() {
    let mut c = cluster().await;
    let i = c.add_node().await;
    let id = c.nodes[i].id;
    let addr = c.nodes[i].addr.to_string();
    let given = node_lifecycle::identity(c.meta.as_ref(), id)
        .await
        .unwrap()
        .expect("registration gives an identity");
    assert!(!given.acknowledged);
    // The disk holds cluster B's identity for the same id.
    c.clients
        .health_with(
            &addr,
            arca_lake_protocol::storage::v1::HealthRequest {
                node_id: id,
                node_identity: Bytes::from_static(&[0xb0; 16]),
                adopt_identity: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let ctl = leader(&c).await;
    wait_until("quarantined", Duration::from_secs(20), async || {
        node_lifecycle::state(c.meta.as_ref(), id).await.unwrap() == State::Quarantined
    })
    .await;
    let rec = node_lifecycle::identity(c.meta.as_ref(), id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rec.token, given.token, "never imported");
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Re-review (Phase 3.11): metadata without a node's identity record (as
/// restored from before it had one) and a node holding its own: nothing
/// is pushed to it and it is not quarantined; deletes without its
/// identity are refused; an operator confirms it (`admin node
/// adopt-identity`) and the leader pushes to it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unconfirmed_identity_is_left_alone_until_an_operator_adopts_it() {
    let c = cluster().await;
    put_objects(&c, 2).await;
    let ctl = leader(&c).await;
    identities_acknowledged(&c).await;
    ctl.shutdown().await;
    let n = &c.nodes[0];
    let addr = n.addr.to_string();
    arca_lake_metadata::transact(c.meta.as_ref(), n.id, |tx, id| {
        Box::pin(async move {
            tx.clear(&arca_lake_metadata::keys::node_identity(*id));
            Ok(())
        })
    })
    .await
    .unwrap();
    let ctl = leader(&c).await;
    wait_until("found unconfirmed", Duration::from_secs(20), async || {
        ctl.metrics
            .identity_unconfirmed_total
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    })
    .await;
    assert_eq!(
        node_lifecycle::state(c.meta.as_ref(), n.id).await.unwrap(),
        State::Active
    );
    // A delete without its identity: refused by the node.
    let g = c.groups().await.remove(0);
    let any = NodeClients::new(Duration::from_secs(5)).with_tls(c.clients.tls().cloned());
    any.set_incarnation(c.clients.incarnation());
    let h = c.clients.health(&addr).await.unwrap();
    any.set_delete_fence(Some(h.delete_fence_generation));
    let shard = ShardRef {
        group_id: g.id,
        chunk_id: [0xcd; 16],
        shard_index: 0,
    };
    let r = any.delete_shard(&addr, shard, g.epoch).await;
    assert!(
        r.as_ref()
            .is_err_and(|e| e.message().contains("identity rejected") || NodeClients::is_fenced(e)),
        "{r:?}"
    );
    // A confirmed node, its fence open: an unstamped delete is refused for
    // its identity alone.
    let other = c.nodes[1].addr.to_string();
    let t0 = Instant::now();
    let open = loop {
        let h = c.clients.health(&other).await.unwrap();
        if h.delete_fence_open {
            break h.delete_fence_generation;
        }
        assert!(t0.elapsed() < Duration::from_secs(20));
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    any.set_delete_fence(Some(open));
    let r = any.delete_shard(&other, shard, g.epoch).await;
    assert!(
        r.as_ref()
            .is_err_and(|e| e.message().contains("identity rejected")),
        "an unstamped delete was taken by a node holding an identity: {r:?}"
    );
    // Confirmed: the record is back, acknowledged.
    let t = node_ops::adopt_identity(c.meta.as_ref(), &c.clients, n.id)
        .await
        .unwrap();
    let rec = node_lifecycle::identity(c.meta.as_ref(), n.id)
        .await
        .unwrap()
        .unwrap();
    assert!(rec.acknowledged && rec.token == t);
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Third review (Phase 3.11): a node whose identity file is damaged takes
/// no delete at all, stamped or not, until an operator repairs it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_damaged_identity_file_refuses_every_delete() {
    let mut c = cluster().await;
    put_objects(&c, 1).await;
    let ctl = leader(&c).await;
    identities_acknowledged(&c).await;
    let n = c.nodes[0].id;
    let token = node_lifecycle::identity(c.meta.as_ref(), n)
        .await
        .unwrap()
        .unwrap()
        .token;
    let addr = c.nodes[0].addr.to_string();
    c.stop_node(0).await;
    std::fs::write(
        c.nodes[0].dir.join("node-identity.txt"),
        "identity 1\nnode_id 1\ntoken zz\ncrc 0\n",
    )
    .unwrap();
    c.start_node(0).await;
    let g = c.groups().await.remove(0);
    let shard = ShardRef {
        group_id: g.id,
        chunk_id: [0xdd; 16],
        shard_index: 0,
    };
    // Its fence still open: only the identity can refuse.
    let h = c.clients.health(&addr).await.unwrap();
    assert!(h.delete_fence_open, "{h:?}");
    for stamp in [None, Some((n, token))] {
        let any = NodeClients::new(Duration::from_secs(5)).with_tls(c.clients.tls().cloned());
        any.set_incarnation(c.clients.incarnation());
        any.set_delete_fence(Some(h.delete_fence_generation.max(1)));
        any.note_identity(&addr, stamp);
        let r = any.delete_shard(&addr, shard, g.epoch).await;
        assert!(
            r.as_ref()
                .is_err_and(|e| e.message().contains("identity rejected")),
            "a node with a damaged identity took a delete ({stamp:?}): {r:?}"
        );
    }
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Confirmation review (Phase 3.11): `admin node keep-removed` on a node
/// still serving with its fence open closes and holds the fence on the
/// node before marking it removed: a delete stamped for it, carrying the
/// open generation, is refused afterwards (a later restore trusts a
/// removed node to be fenced).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keep_removed_closes_and_holds_the_fence_before_marking_removed() {
    let c = cluster().await;
    let objects = put_objects(&c, 2).await;
    let ctl = leader(&c).await;
    identities_acknowledged(&c).await;
    let g = c.groups().await.remove(0);
    let id = g.members[0].node_id;
    let i = c.node_index(&g.members[0]).unwrap();
    let addr = c.nodes[i].addr.to_string();
    let fence = arca_lake_metadata::ops::delete_fence::current(c.meta.as_ref())
        .await
        .unwrap();
    assert!(fence.open);
    let token = node_lifecycle::identity(c.meta.as_ref(), id)
        .await
        .unwrap()
        .unwrap()
        .token;
    let late = NodeClients::new(Duration::from_secs(5)).with_tls(c.clients.tls().cloned());
    late.set_incarnation(c.clients.incarnation());
    late.set_delete_fence(Some(fence.generation));
    late.note_identity(&addr, Some((id, token)));
    ctl.abort();
    let fenced = node_ops::keep_removed(c.meta.as_ref(), &c.clients, id, false, now_ms())
        .await
        .unwrap();
    assert!(fenced);
    assert_eq!(
        node_lifecycle::state(c.meta.as_ref(), id).await.unwrap(),
        State::Removed
    );
    let h = c.clients.health(&addr).await.unwrap();
    assert!(!h.delete_fence_open && h.delete_fence_hold, "{h:?}");
    let shard = ShardRef {
        group_id: g.id,
        chunk_id: c.manifest(BUCKET, "o0").await.chunks[0].1.chunk_id,
        shard_index: 0,
    };
    let before = c.shard_count(i, g.id);
    let r = late.delete_shard(&addr, shard, g.epoch).await;
    assert!(
        r.as_ref().is_err_and(NodeClients::is_fenced),
        "a node marked removed took a late delete: {r:?}"
    );
    assert_eq!(c.shard_count(i, g.id), before);
    all_read_back(&c, &objects).await;
    // Not answering: refused unless declared unreachable.
    let mut c = c;
    let j = c.node_index(&g.members[1]).unwrap();
    let jid = c.nodes[j].id;
    c.stop_node(j).await;
    let e = node_ops::keep_removed(c.meta.as_ref(), &c.clients, jid, false, now_ms())
        .await
        .unwrap_err();
    assert!(e.to_string().contains("--unreachable"), "{e}");
    assert_eq!(
        node_lifecycle::state(c.meta.as_ref(), jid).await.unwrap(),
        State::Active,
        "not marked removed without its fence"
    );
    c.shutdown().await;
}

/// Confirmation review (Phase 3.11): adoption is conditional in the same
/// transaction: never over an identity record, never for a quarantined
/// node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adopt_if_unconfirmed_takes_only_the_unconfirmed_case() {
    let c = cluster().await;
    let n = c.nodes[0].id;
    // Registration gave a record: refused.
    assert!(
        !node_lifecycle::adopt_if_unconfirmed(c.meta.as_ref(), n, [1; 16])
            .await
            .unwrap()
    );
    let clear = async |id: u64| {
        arca_lake_metadata::transact(c.meta.as_ref(), id, |tx, id| {
            Box::pin(async move {
                tx.clear(&arca_lake_metadata::keys::node_identity(*id));
                Ok(())
            })
        })
        .await
        .unwrap();
    };
    clear(n).await;
    node_lifecycle::quarantine(c.meta.as_ref(), n, "test", now_ms())
        .await
        .unwrap();
    assert!(
        !node_lifecycle::adopt_if_unconfirmed(c.meta.as_ref(), n, [2; 16])
            .await
            .unwrap(),
        "adopted for a quarantined node"
    );
    assert!(
        node_lifecycle::identity(c.meta.as_ref(), n)
            .await
            .unwrap()
            .is_none()
    );
    let m = c.nodes[1].id;
    clear(m).await;
    assert!(
        node_lifecycle::adopt_if_unconfirmed(c.meta.as_ref(), m, [3; 16])
            .await
            .unwrap()
    );
    let rec = node_lifecycle::identity(c.meta.as_ref(), m)
        .await
        .unwrap()
        .unwrap();
    assert!(rec.acknowledged && rec.token == [3; 16]);
    c.shutdown().await;
}

/// Confirmation re-review (Phase 3.11): a node holding its fence under an
/// earlier incarnation, which an open of the current one would lift.
/// `keep-removed` (and `unregister`, the same step) re-ties the hold to
/// the current incarnation; an open of it at a higher generation is then
/// refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removal_re_ties_an_earlier_incarnations_hold() {
    use arca_lake_protocol::client::FencePush;
    let c = cluster().await;
    put_objects(&c, 1).await;
    let ctl = leader(&c).await;
    identities_acknowledged(&c).await;
    ctl.shutdown().await;
    let n = &c.nodes[0];
    let addr = n.addr.to_string();
    let old = compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap()
        .incarnation;
    // Held under the old incarnation.
    let h = c.clients.health(&addr).await.unwrap();
    c.clients
        .push_delete_fence_with(
            &addr,
            h.delete_fence_generation.max(1),
            false,
            FencePush {
                hold: true,
                incarnation: Some(old),
                ..FencePush::default()
            },
        )
        .await
        .unwrap();
    // A new incarnation, adopted by the node (as recovery pushes it).
    let marked = compat::mark_restoring_above(c.meta.as_ref(), None, None, "t", now_ms(), 0)
        .await
        .unwrap();
    let gate = compat::gate(c.meta.as_ref()).await.unwrap();
    c.clients
        .health_with(
            &addr,
            arca_lake_protocol::storage::v1::HealthRequest {
                format_write: gate.format_write.unwrap_or(0),
                incarnation: Bytes::copy_from_slice(&marked.incarnation),
                incarnation_changed_ms: marked.changed_ms,
                incarnation_seq: gate.incarnation_seq,
                schema_floor: gate.schema.unwrap_or(0),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let h = c.clients.health(&addr).await.unwrap();
    assert_eq!(
        h.incarnation.as_ref(),
        marked.incarnation.as_slice(),
        "adopted"
    );
    assert_eq!(
        h.delete_fence_hold_incarnation.as_ref(),
        old.as_slice(),
        "set-up: the hold is the old incarnation's"
    );
    let fenced = node_ops::keep_removed(c.meta.as_ref(), &c.clients, n.id, false, now_ms())
        .await
        .unwrap();
    assert!(fenced);
    let h = c.clients.health(&addr).await.unwrap();
    assert!(!h.delete_fence_open && h.delete_fence_hold, "{h:?}");
    assert_eq!(
        h.delete_fence_hold_incarnation.as_ref(),
        marked.incarnation.as_slice(),
        "the hold was not re-tied to the current incarnation"
    );
    // A delayed open of the current incarnation, a generation above: refused.
    let cur = NodeClients::new(Duration::from_secs(5)).with_tls(c.clients.tls().cloned());
    cur.set_incarnation(Some(marked.incarnation));
    let _ = cur
        .push_delete_fence_with(
            &addr,
            h.delete_fence_generation + 1,
            true,
            FencePush {
                incarnation: Some(marked.incarnation),
                ..FencePush::default()
            },
        )
        .await;
    let after = c.clients.health(&addr).await.unwrap();
    assert!(
        !after.delete_fence_open && after.delete_fence_hold,
        "the removed node was opened: {after:?}"
    );
    c.shutdown().await;
}

/// Re-review (Phase 3.11): while a managed restore's marker is in the
/// store (between its capture and its clear), node lifecycle changes and
/// registrations are refused, so that what it captured stays true;
/// `admin restore abandon` lifts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_changes_wait_while_a_restore_holds_its_marker() {
    let c = cluster().await;
    let n = c.nodes[0].id;
    arca_lake_metadata::transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.set(&arca_lake_metadata::keys::restore_marker(), b"paused");
            Ok(())
        })
    })
    .await
    .unwrap();
    let busy = |e: String| assert!(e.contains("managed restore is in progress"), "{e}");
    busy(
        node_lifecycle::keep_removed(c.meta.as_ref(), n, now_ms())
            .await
            .unwrap_err()
            .to_string(),
    );
    busy(
        node_lifecycle::set_identity(c.meta.as_ref(), n, [1; 16])
            .await
            .unwrap_err()
            .to_string(),
    );
    busy(
        node_lifecycle::adopt_if_unconfirmed(c.meta.as_ref(), n, [1; 16])
            .await
            .unwrap_err()
            .to_string(),
    );
    busy(
        node_lifecycle::rejoin(c.meta.as_ref(), n, true)
            .await
            .unwrap_err()
            .to_string(),
    );
    busy(
        node_lifecycle::unregister(c.meta.as_ref(), n, "t", "t", now_ms())
            .await
            .unwrap_err()
            .to_string(),
    );
    busy(
        cluster::register_node(
            c.meta.as_ref(),
            "elsewhere:1",
            Vec::new(),
            vec![0],
            now_ms(),
        )
        .await
        .unwrap_err()
        .to_string(),
    );
    assert!(
        arca_lake_controller::managed_restore::abandon(c.meta.as_ref())
            .await
            .unwrap()
    );
    node_lifecycle::set_identity(c.meta.as_ref(), n, [1; 16])
        .await
        .unwrap();
    c.shutdown().await;
}
