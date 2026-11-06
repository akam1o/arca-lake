//! Device replacement and node decommission under faults (Phase 3.4 §4):
//! a controller leader change, FoundationDB process and quorum loss, PUT,
//! GET, DELETE and GC, scrubbing and stripe repair while a member moves,
//! an old node rejoining, capacity shortage pausing and resuming a move,
//! a decommission across groups surviving a controller restart, a cache
//! member, and the dry runs. Every scenario runs on the in-memory store
//! and, with `ARCA_FDB_TESTS=1` (and a chaos driver for the process
//! faults, `arca_lake_testkit::chaos`), on FoundationDB.
//!
//! Safety checked throughout: every acknowledged object reads back
//! unchanged, deleted objects stay deleted, a write at a superseded group
//! epoch is refused, the leaving member keeps its shards while the group
//! rebuilds, a node is decommissioned only when no group lists it and no
//! group is rebuilding, and a dry run changes nothing.

#![allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::implicit_clone
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arca_lake_controller::plan;
use arca_lake_controller::process::{ControllerConfig, ControllerHandle, spawn};
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::ops::membership::{self, NodeLifecycle};
use arca_lake_metadata::records::{GroupRecord, GroupState};
use arca_lake_metadata::{KvStore, Txn};
use arca_lake_protocol::client::ShardRef;
use arca_lake_testkit::chaos::Chaos;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use tokio::sync::Mutex;

const KIB: u64 = 1 << 10;
const BUCKET: &str = "moves";

fn opts(nodes: usize, groups: usize, cache_nodes: usize) -> ClusterOptions {
    ClusterOptions {
        groups,
        cache_nodes,
        cache: (cache_nodes > 0).then(ClusterOptions::default_cache),
        ..ClusterOptions::rs(nodes, 2, 1, 128 * KIB, 0)
    }
}

fn ctl(id: &str) -> ControllerConfig {
    let mut c = ControllerConfig::new(id);
    c.lease = Duration::from_millis(800);
    c.gc_interval = Duration::from_millis(200);
    c.gc_grace = Duration::ZERO;
    c.reconcile_interval = Duration::from_secs(3600);
    c.repair_interval = Duration::from_millis(100);
    c.health_interval = Duration::from_millis(100);
    c.unhealthy_after = Duration::from_secs(3);
    c.stripe_repair_interval = Duration::from_millis(200);
    c.scrub.interval_ms = 500;
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

async fn group<S: KvStore>(c: &TestCluster<S>, id: u64) -> GroupRecord {
    // Metadata may be briefly unavailable under FoundationDB faults.
    loop {
        if let Ok(g) = cluster::get_group(c.meta.as_ref(), id).await {
            return g;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn put_objects<S: KvStore + 'static>(
    c: &TestCluster<S>,
    prefix: &str,
    n: u64,
) -> Vec<(String, Bytes)> {
    let mut out = Vec::new();
    for i in 0..n {
        let d = Bytes::from(random_bytes(usize::try_from(200 * KIB).unwrap(), 7_000 + i));
        let k = format!("{prefix}{i}");
        s3::put(&c.backend, BUCKET, &k, d.clone(), &PutOptions::default())
            .await
            .unwrap();
        out.push((k, d));
    }
    out
}

/// Every acknowledged object reads back unchanged; deleted ones are gone.
async fn check_objects<S: KvStore + 'static>(c: &TestCluster<S>, ledger: &Ledger) {
    let l = ledger.lock().await;
    for (k, d) in &l.live {
        assert_eq!(&s3::get(&c.backend, BUCKET, k).await.unwrap().0, d, "{k}");
    }
    for k in &l.deleted {
        assert!(
            s3::head(&c.backend, BUCKET, k).await.is_err(),
            "{k} came back"
        );
    }
}

#[derive(Default)]
struct LedgerState {
    live: HashMap<String, Bytes>,
    deleted: Vec<String>,
}

type Ledger = Arc<Mutex<LedgerState>>;

/// PUT, GET and DELETE in the background until `stop`; unknown outcomes
/// (a failed request) are left out of the ledger.
fn traffic<S: KvStore + 'static>(
    c: &TestCluster<S>,
    ledger: Ledger,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    let backend = c.backend.clone();
    tokio::spawn(async move {
        let mut i = 0u64;
        while !stop.load(Ordering::Relaxed) {
            i += 1;
            let k = format!("t{i}");
            let d = Bytes::from(random_bytes(
                usize::try_from(150 * KIB).unwrap(),
                50_000 + i,
            ));
            if s3::put(&backend, BUCKET, &k, d.clone(), &PutOptions::default())
                .await
                .is_ok()
            {
                ledger.lock().await.live.insert(k.clone(), d.clone());
                if let Ok((got, _)) = s3::get(&backend, BUCKET, &k).await {
                    assert_eq!(got, d, "{k} read back wrong during the move");
                }
            }
            if i.is_multiple_of(4) {
                let victim = format!("t{}", i - 2);
                if s3::delete(&backend, BUCKET, &victim).await.is_ok() {
                    let mut l = ledger.lock().await;
                    l.live.remove(&victim);
                    l.deleted.push(victim);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
}

async fn leader(a: &ControllerHandle, b: &ControllerHandle) {
    wait_until("a leader", Duration::from_secs(30), async || {
        a.is_leader() || b.is_leader()
    })
    .await;
}

fn shards_on<S: KvStore + 'static>(c: &TestCluster<S>, node: u64, group: u64) -> usize {
    c.nodes
        .iter()
        .position(|n| n.id == node)
        .map_or(0, |i| c.shard_keys(i, group).len())
}

/// A write stamped with a superseded group epoch is refused by every
/// current member.
async fn stale_epoch_refused<S: KvStore + 'static>(
    c: &TestCluster<S>,
    g: &GroupRecord,
    old_epoch: u64,
) {
    let nodes = cluster::list_nodes(c.meta.as_ref()).await.unwrap();
    for m in &g.members {
        let addr = &nodes.iter().find(|n| n.id == m.node_id).unwrap().address;
        let e = c
            .clients
            .put_shard(
                addr,
                ShardRef {
                    group_id: g.id,
                    chunk_id: [0xee; 16],
                    shard_index: 0,
                },
                old_epoch,
                Bytes::from_static(b"stale"),
                1024,
            )
            .await
            .unwrap_err();
        assert_eq!(
            e.code(),
            tonic::Code::FailedPrecondition,
            "node {}: {e}",
            m.node_id
        );
    }
}

// ---- A: leader change, PUT/GET/DELETE/GC, scrub and stripe repair ---------

async fn replace_during_leader_change_traffic_and_fdb_faults<S: KvStore + 'static>(
    c: TestCluster<S>,
    chaos: Option<Chaos>,
) {
    if std::env::var_os("ARCA_TEST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(
                "arca_lake_controller=info",
            ))
            .with_test_writer()
            .try_init();
    }
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let ledger: Ledger = Arc::default();
    for (k, d) in put_objects(&c, "o", 60).await {
        ledger.lock().await.live.insert(k, d);
    }
    let a = spawn(Arc::clone(&c.meta), ctl("ctl-a")).await.unwrap();
    let b = spawn(Arc::clone(&c.meta), ctl("ctl-b")).await.unwrap();
    leader(&a, &b).await;
    let stop = Arc::new(AtomicBool::new(false));
    let t = traffic(&c, Arc::clone(&ledger), Arc::clone(&stop));

    // The group holding the first object (writes may favour one group).
    let gid = c.manifest(BUCKET, "o0").await.chunks[0]
        .1
        .placement
        .group_id;
    let g0 = group(&c, gid).await;
    let old = g0.members[0].node_id;
    let before_old = shards_on(&c, old, g0.id);
    // A corrupt shard on another member of a chunk of the moving group:
    // with the leaving member out of the membership, fewer than k current
    // members have a good shard; the leaving member's copy is the source
    // that keeps the move (and the object) alive.
    let mut corrupted = None;
    for i in 0..60 {
        let ch = c.manifest(BUCKET, &format!("o{i}")).await.chunks[0]
            .1
            .clone();
        if ch.placement.group_id == g0.id {
            let n = c.node_index(&g0.members[1]).unwrap();
            c.corrupt_chunk_on_node(n, g0.id, ch.chunk_id);
            corrupted = Some(format!("o{i}"));
            break;
        }
    }
    let corrupted = corrupted.expect("an object in the moving group");
    membership::request_replace(c.meta.as_ref(), g0.id, 0, None, "test", now_ms())
        .await
        .unwrap();
    wait_until("the move to start", Duration::from_secs(30), async || {
        group(&c, g0.id).await.state == GroupState::Repairing
    })
    .await;
    // The leaving member keeps every shard while the group rebuilds.
    assert!(
        shards_on(&c, old, g0.id) >= before_old,
        "old member lost shards mid-move"
    );
    // The leader dies mid-move; the other controller takes over.
    let (gone, survivor) = if a.is_leader() { (a, b) } else { (b, a) };
    gone.shutdown().await;
    if let Some(ch) = &chaos {
        // One FoundationDB process lost (double redundancy survives it) ...
        ch.run("kill 2").await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        // ... then a second one: quorum lost, metadata unavailable.
        ch.run("kill 3").await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        ch.run("start 3").await;
        ch.run("start 2").await;
    }
    let restarted = spawn(Arc::clone(&c.meta), ctl("ctl-c")).await.unwrap();
    wait_until("the move to finish", Duration::from_secs(180), async || {
        let g = group(&c, g0.id).await;
        g.state == GroupState::Active && g.members[0].node_id != old
    })
    .await;
    stop.store(true, Ordering::Relaxed);
    t.await.unwrap();
    let g1 = group(&c, g0.id).await;
    assert!(g1.epoch > g0.epoch);
    assert!(!g1.members.iter().any(|m| m.node_id == old));
    stale_epoch_refused(&c, &g1, g0.epoch).await;
    eprintln!(
        "moved group {} (the corrupt shard was in {corrupted})",
        g0.id
    );
    // Everything acknowledged reads back, from the new membership alone:
    // the old node is stopped for the check.
    let mut c = c;
    let oi = c.nodes.iter().position(|n| n.id == old).unwrap();
    c.stop_node(oi).await;
    check_objects(&c, &ledger).await;
    survivor.shutdown().await;
    restarted.shutdown().await;
    c.shutdown().await;
}

// ---- B: an old node rejoins ------------------------------------------------

async fn an_old_node_rejoining_serves_nothing_stale<S: KvStore + 'static>(mut c: TestCluster<S>) {
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let ledger: Ledger = Arc::default();
    for (k, d) in put_objects(&c, "o", 20).await {
        ledger.lock().await.live.insert(k, d);
    }
    let g0 = c
        .groups()
        .await
        .into_iter()
        .find(|g| !g.is_cache())
        .unwrap();
    let old = g0.members[0].node_id;
    let oi = c.nodes.iter().position(|n| n.id == old).unwrap();
    // The node dies; auto repair replaces it while it is down (it never
    // hears the new epoch).
    c.stop_node(oi).await;
    let mut cfg = ctl("ctl-a");
    cfg.auto_repair = true;
    let a = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    wait_until("auto repair", Duration::from_secs(60), async || {
        let g = group(&c, g0.id).await;
        g.state == GroupState::Active && !g.members.iter().any(|m| m.node_id == old)
    })
    .await;
    // New data and an overwrite while the old node is away.
    let fresh = Bytes::from(random_bytes(usize::try_from(200 * KIB).unwrap(), 99));
    s3::put(
        &c.backend,
        BUCKET,
        "o0",
        fresh.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    ledger.lock().await.live.insert("o0".into(), fresh);
    // It comes back with its old shards and its old epochs.
    c.start_node(oi).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let g1 = group(&c, g0.id).await;
    // Reads come from the current members only, also degraded: stop one
    // of them; the old node's stale copies are never used.
    let mi = c.node_index(&g1.members[1]).unwrap();
    c.stop_node(mi).await;
    check_objects(&c, &ledger).await;
    c.start_node(mi).await;
    stale_epoch_refused(&c, &g1, g0.epoch).await;
    a.shutdown().await;
    c.shutdown().await;
}

// ---- C: capacity shortage pauses a move; room resumes it -------------------

async fn a_move_without_room_pauses_and_resumes<S: KvStore + 'static>(mut c: TestCluster<S>) {
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let ledger: Ledger = Arc::default();
    for (k, d) in put_objects(&c, "o", 30).await {
        ledger.lock().await.live.insert(k, d);
    }
    let g0 = c
        .groups()
        .await
        .into_iter()
        .find(|g| !g.is_cache())
        .unwrap();
    let spares: Vec<usize> = (0..c.nodes.len())
        .filter(|&i| !g0.members.iter().any(|m| m.node_id == c.nodes[i].id))
        .collect();
    // Every possible target advertises almost no room.
    for &i in &spares {
        c.set_node_capacity(i, Some(1 << 20)).await;
    }
    // The dry run says why, and changes nothing.
    let snapshot = metadata_snapshot(&c).await;
    let shards: Vec<usize> = (0..c.nodes.len())
        .map(|i| c.shard_keys(i, g0.id).len())
        .collect();
    let p = plan::plan_replace(c.meta.as_ref(), &c.clients, g0.id, 0, None, "host", 100)
        .await
        .unwrap();
    assert!(p.blocked(), "{p:?}");
    assert!(
        p.moves[0]
            .blockers
            .iter()
            .any(|b| b.contains("insufficient capacity")),
        "{p:?}"
    );
    let need = p.moves[0].size.unwrap();
    assert_eq!(
        need.shards,
        u64::try_from(
            c.shard_keys(c.node_index(&g0.members[1]).unwrap(), g0.id)
                .len()
        )
        .unwrap()
    );
    assert_eq!(
        metadata_snapshot(&c).await,
        snapshot,
        "the dry run wrote metadata"
    );
    assert_eq!(
        (0..c.nodes.len())
            .map(|i| c.shard_keys(i, g0.id).len())
            .collect::<Vec<_>>(),
        shards
    );
    // The real request pauses with the reason; the group is untouched.
    membership::request_replace(c.meta.as_ref(), g0.id, 0, None, "test", now_ms())
        .await
        .unwrap();
    let a = spawn(Arc::clone(&c.meta), ctl("ctl-a")).await.unwrap();
    wait_until("the pause reason", Duration::from_secs(30), async || {
        membership::replace_requests(c.meta.as_ref())
            .await
            .unwrap()
            .first()
            .and_then(|r| r.last_error.clone())
            .is_some_and(|e| e.contains("insufficient capacity"))
    })
    .await;
    let g = group(&c, g0.id).await;
    assert_eq!(
        (g.epoch, g.state, g.members.clone()),
        (g0.epoch, GroupState::Active, g0.members.clone())
    );
    // Room appears on one spare: the move resumes by itself and completes.
    c.set_node_capacity(spares[0], None).await;
    wait_until("the resumed move", Duration::from_secs(60), async || {
        membership::replace_requests(c.meta.as_ref())
            .await
            .unwrap()
            .is_empty()
    })
    .await;
    let g = group(&c, g0.id).await;
    assert_eq!(g.state, GroupState::Active);
    assert_eq!(g.members[0].node_id, c.nodes[spares[0]].id);
    check_objects(&c, &ledger).await;
    a.shutdown().await;
    c.shutdown().await;
}

async fn metadata_snapshot<S: KvStore + 'static>(c: &TestCluster<S>) -> Vec<(Vec<u8>, Vec<u8>)> {
    arca_lake_metadata::transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            Ok(tx
                .get_range(&arca_lake_metadata::keys::all_range(), usize::MAX, false)
                .await?
                .into_iter()
                // Heartbeats and leases move on their own.
                .filter(|(k, _)| !k.windows(4).any(|w| w == b"proc" || w == b"lead"))
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect())
        })
    })
    .await
    .unwrap()
}

// ---- D: decommission across groups, a controller restart, a cache member ---

async fn decommission_across_groups_survives_a_controller_restart<S: KvStore + 'static>(
    c: TestCluster<S>,
) {
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let ledger: Ledger = Arc::default();
    for (k, d) in put_objects(&c, "o", 40).await {
        ledger.lock().await.live.insert(k, d);
    }
    let groups = c.groups().await;
    // A durable node in more than one group.
    let durable: Vec<&GroupRecord> = groups.iter().filter(|g| !g.is_cache()).collect();
    let mut count: HashMap<u64, usize> = HashMap::new();
    for g in &durable {
        for m in &g.members {
            *count.entry(m.node_id).or_default() += 1;
        }
    }
    let (&node, &n_groups) = count.iter().max_by_key(|(_, n)| **n).unwrap();
    assert!(
        n_groups >= 2,
        "the layout puts a node in several groups: {count:?}"
    );
    // Dry run first: one move per group, nothing changed.
    let snapshot = metadata_snapshot(&c).await;
    let p = plan::plan_decommission(c.meta.as_ref(), &c.clients, node, "host", 100)
        .await
        .unwrap();
    assert_eq!(p.moves.len(), n_groups, "{p:?}");
    assert!(!p.blocked(), "{p:?}");
    assert!(
        p.write_bytes() > 0 && p.read_bytes() >= 2 * p.write_bytes(),
        "{p:?}"
    );
    assert_eq!(
        metadata_snapshot(&c).await,
        snapshot,
        "the dry run wrote metadata"
    );

    membership::drain_node(c.meta.as_ref(), node, "test", now_ms())
        .await
        .unwrap();
    let a = spawn(Arc::clone(&c.meta), ctl("ctl-a")).await.unwrap();
    wait_until("the first move", Duration::from_secs(30), async || {
        c.groups()
            .await
            .iter()
            .any(|g| g.state == GroupState::Repairing)
    })
    .await;
    // The controller restarts mid-drain.
    a.shutdown().await;
    let b = spawn(Arc::clone(&c.meta), ctl("ctl-b")).await.unwrap();
    // Decommissioned only when no group lists the node and none rebuilds.
    let start = Instant::now();
    loop {
        let state = membership::node_states(c.meta.as_ref())
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.node_id == node)
            .unwrap()
            .state;
        let gs = c.groups().await;
        if matches!(state, NodeLifecycle::Decommissioned { .. }) {
            assert!(
                !gs.iter()
                    .any(|g| g.members.iter().any(|m| m.node_id == node)),
                "{gs:?}"
            );
            assert!(
                !gs.iter().any(|g| g.state == GroupState::Repairing),
                "{gs:?}"
            );
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(120),
            "drain did not finish"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // A cache member is drained too: replaced by a cache node only.
    let cache = groups.iter().find(|g| g.is_cache()).unwrap();
    let cnode = cache.members[0].node_id;
    let cache_ids: Vec<u64> = c
        .groups()
        .await
        .iter()
        .filter(|g| g.is_cache())
        .flat_map(|g| g.members.iter().map(|m| m.node_id))
        .collect();
    membership::drain_node(c.meta.as_ref(), cnode, "test", now_ms())
        .await
        .unwrap();
    wait_until("the cache drain", Duration::from_secs(60), async || {
        membership::node_states(c.meta.as_ref())
            .await
            .unwrap()
            .iter()
            .any(|r| r.node_id == cnode && matches!(r.state, NodeLifecycle::Decommissioned { .. }))
    })
    .await;
    let cg = group(&c, cache.id).await;
    assert!(!cg.members.iter().any(|m| m.node_id == cnode));
    assert!(
        !durable
            .iter()
            .any(|g| g.members.iter().any(|m| m.node_id == cg.members[0].node_id)),
        "a durable node joined a cache group"
    );
    let _ = cache_ids;
    // The drained node goes away; everything reads.
    let mut c = c;
    let ni = c.nodes.iter().position(|n| n.id == node).unwrap();
    c.stop_node(ni).await;
    check_objects(&c, &ledger).await;
    b.shutdown().await;
    c.shutdown().await;
}

// ---- E: direct copy from the leaving member (Phase 3.4 §10) ----------------

async fn direct_copy_moves_verified_shards<S: KvStore + 'static>(mut c: TestCluster<S>) {
    use arca_lake_controller::repair::{RepairOptions, RepairSource, replace_member_with};
    use arca_lake_metadata::records::GroupMember;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let ledger: Ledger = Arc::default();
    for (k, d) in put_objects(&c, "o", 30).await {
        ledger.lock().await.live.insert(k, d);
    }
    let g0 = c
        .groups()
        .await
        .into_iter()
        .find(|g| !g.is_cache())
        .unwrap();
    let old = g0.members[0].node_id;
    let spare = (0..c.nodes.len())
        .map(|i| c.nodes[i].id)
        .find(|id| !g0.members.iter().any(|m| m.node_id == *id))
        .unwrap();
    let r = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g0.id,
        0,
        GroupMember {
            node_id: spare,
            device_id: 0,
        },
        &RepairOptions {
            former: vec![old],
            source: RepairSource::Direct,
            ..RepairOptions::default()
        },
        async |_| Ok(()),
    )
    .await
    .unwrap();
    assert!(r.complete, "{r:?}");
    assert_eq!(
        r.copied_direct, r.reconstructed,
        "every shard copied: {r:?}"
    );
    assert!(r.copied_direct > 0);
    // The copies are the same bytes: with the old member gone and another
    // member stopped, every object decodes from the copy.
    let oi = c.nodes.iter().position(|n| n.id == old).unwrap();
    c.stop_node(oi).await;
    let mi = c.node_index(&g0.members[1]).unwrap();
    c.stop_node(mi).await;
    check_objects(&c, &ledger).await;
    c.shutdown().await;
}

// ---- entry points ----------------------------------------------------------

#[tokio::test]
async fn mem_replace_during_leader_change_and_traffic() {
    replace_during_leader_change_traffic_and_fdb_faults(
        TestCluster::start(opts(5, 2, 0)).await,
        None,
    )
    .await;
}

#[tokio::test]
async fn mem_old_node_rejoining() {
    an_old_node_rejoining_serves_nothing_stale(TestCluster::start(opts(5, 1, 0)).await).await;
}

#[tokio::test]
async fn mem_capacity_pause_and_resume() {
    a_move_without_room_pauses_and_resumes(TestCluster::start(opts(5, 1, 0)).await).await;
}

#[tokio::test]
async fn mem_decommission_across_groups() {
    decommission_across_groups_survives_a_controller_restart(
        TestCluster::start(opts(5, 3, 3)).await,
    )
    .await;
}

#[tokio::test]
async fn mem_direct_copy() {
    direct_copy_moves_verified_shards(TestCluster::start(opts(5, 1, 0)).await).await;
}

#[cfg(feature = "fdb")]
mod fdb {
    use super::*;

    async fn start(o: ClusterOptions) -> Option<TestCluster<arca_lake_metadata::fdb::FdbKv>> {
        let st = arca_lake_testkit::fdb::starter()?;
        Some(st(o).await)
    }

    #[tokio::test]
    async fn fdb_replace_during_leader_change_traffic_and_process_faults() {
        let Some(c) = start(opts(5, 2, 0)).await else {
            return;
        };
        replace_during_leader_change_traffic_and_fdb_faults(c, Chaos::from_env()).await;
    }

    #[tokio::test]
    async fn fdb_old_node_rejoining() {
        let Some(c) = start(opts(5, 1, 0)).await else {
            return;
        };
        an_old_node_rejoining_serves_nothing_stale(c).await;
    }

    #[tokio::test]
    async fn fdb_capacity_pause_and_resume() {
        let Some(c) = start(opts(5, 1, 0)).await else {
            return;
        };
        a_move_without_room_pauses_and_resumes(c).await;
    }

    #[tokio::test]
    async fn fdb_direct_copy() {
        let Some(c) = start(opts(5, 1, 0)).await else {
            return;
        };
        direct_copy_moves_verified_shards(c).await;
    }

    #[tokio::test]
    async fn fdb_decommission_across_groups() {
        let Some(c) = start(opts(5, 3, 3)).await else {
            return;
        };
        decommission_across_groups_survives_a_controller_restart(c).await;
    }
}

/// Phase 3.4 §10 measurement (run explicitly, release build):
/// `cargo test --release -p arca-lake-testkit --test it
/// membership_faults::measure_member_move -- --ignored --nocapture`. One member of a 4+2
/// group with `CHUNKS` chunks of 1 MiB is moved to a spare node by EC
/// reconstruction (k shard reads per chunk) and by direct copy from the
/// leaving member (one read per shard; falls back to reconstruction for a
/// chunk it cannot read): time, shards read from sources, verified result.
#[tokio::test]
#[ignore = "measurement"]
async fn measure_member_move() {
    use arca_lake_controller::repair::{RepairOptions, RepairSource, replace_member_with};
    use arca_lake_metadata::records::GroupMember;
    const CHUNKS: u64 = 96;
    for rep in 0..3 {
        for source in [RepairSource::Reconstruct, RepairSource::Direct] {
            let c = TestCluster::start(ClusterOptions {
                groups: 1,
                ..ClusterOptions::rs(7, 4, 2, 1 << 20, 0)
            })
            .await;
            s3::create_bucket(&c.backend, BUCKET).await.unwrap();
            let data = Bytes::from(random_bytes(usize::try_from(CHUNKS << 20).unwrap(), rep));
            s3::put(
                &c.backend,
                BUCKET,
                "big",
                data.clone(),
                &PutOptions::default(),
            )
            .await
            .unwrap();
            let g0 = c
                .groups()
                .await
                .into_iter()
                .find(|g| !g.is_cache())
                .unwrap();
            let old = g0.members[0].node_id;
            let spare = c
                .nodes
                .iter()
                .map(|n| n.id)
                .find(|id| !g0.members.iter().any(|m| m.node_id == *id))
                .unwrap();
            let gets_before: u64 = (0..c.nodes.len())
                .map(|i| c.node_service(i).stats().get_serve.count)
                .sum();
            let t = Instant::now();
            let r = replace_member_with(
                c.meta.as_ref(),
                &c.clients,
                g0.id,
                0,
                GroupMember {
                    node_id: spare,
                    device_id: 0,
                },
                &RepairOptions {
                    former: vec![old],
                    source,
                    ..RepairOptions::default()
                },
                async |_| Ok(()),
            )
            .await
            .unwrap();
            let took = t.elapsed();
            let gets: u64 = (0..c.nodes.len())
                .map(|i| c.node_service(i).stats().get_serve.count)
                .sum::<u64>()
                - gets_before;
            assert!(r.complete);
            assert_eq!(s3::get(&c.backend, BUCKET, "big").await.unwrap().0, data);
            println!(
                "{{\"rep\":{rep},\"source\":\"{source:?}\",\"chunks\":{},\"copied_direct\":{},\"shard_reads\":{gets},\"seconds\":{:.3}}}",
                r.reconstructed,
                r.copied_direct,
                took.as_secs_f64()
            );
            c.shutdown().await;
        }
    }
}
