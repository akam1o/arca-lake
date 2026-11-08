//! The delete fence across a cluster (Phase 3.9, R-88, ADR-0023): deletes
//! a controller decided before a phase change, a metadata restore or a
//! leader change are delivered afterwards (delayed, duplicated, reordered)
//! and must not remove a shard the cluster needs. Each scenario builds the
//! decided deletes exactly as GC would (every shard of a generation it
//! verified unreferenced), holds them, changes the cluster, then sends
//! them.
//!
//! Invariants: A (acknowledged data is not lost), B (a stale decision
//! cannot remove a needed shard), C (a delete decided before a restore
//! cannot remove a shard the restored metadata references), D (after a
//! fence change no delete of the old generation completes), E (fence
//! unknown or closed: nothing is deleted), F (once safe again, GC and
//! reconciliation resume and converge).

#![allow(clippy::too_many_lines, clippy::type_complexity)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arca_lake_controller::delete_fence::close_and_push;
use arca_lake_controller::recovery::{RecoveryOptions, recovery_pass};
use arca_lake_metadata::guard::{GuardedKv, OpClass};
use arca_lake_metadata::ops::compat;
use arca_lake_metadata::ops::delete_fence::{self, sync_for_term};
use arca_lake_metadata::ops::recovery::{self, FinalizeRequest};
use arca_lake_metadata::records::ClusterPhase;
use arca_lake_metadata::{KvStore, Txn, keys, transact};
use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "fence";

async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

async fn put(c: &TestCluster, key: &str, seed: u64) -> Bytes {
    let d = Bytes::from(random_bytes(usize::try_from(2 * MIB).unwrap(), seed));
    s3::put(&c.backend, BUCKET, key, d.clone(), &PutOptions::default())
        .await
        .unwrap();
    d
}

/// The deletes GC would send for `key`'s current version: every shard of
/// every chunk, on its member, at the group's epoch.
async fn decided_deletes(c: &TestCluster, key: &str) -> Vec<(String, ShardRef, u64)> {
    let m = c.manifest(BUCKET, key).await;
    let groups = c.groups().await;
    let mut out = Vec::new();
    for (_, rec) in &m.chunks {
        let g = groups
            .iter()
            .find(|g| g.id == rec.placement.group_id)
            .unwrap();
        for (i, member) in g.members.iter().enumerate() {
            let n = c.node_index(member).unwrap();
            out.push((
                c.nodes[n].addr.to_string(),
                ShardRef {
                    group_id: g.id,
                    chunk_id: rec.chunk_id,
                    shard_index: u16::try_from(i).unwrap(),
                },
                g.epoch,
            ));
        }
    }
    out
}

/// A controller process as it was when it decided: its incarnation and
/// delete fence (a separate client state, unaffected by later changes).
fn frozen_controller(c: &TestCluster) -> NodeClients {
    let old = NodeClients::new(Duration::from_secs(5)).with_tls(c.clients.tls().cloned());
    old.set_incarnation(c.clients.incarnation());
    old.set_delete_fence(c.clients.delete_fence());
    old
}

/// What the nodes did with delivered deletes.
#[derive(Debug, Default, PartialEq, Eq)]
struct Delivered {
    applied: usize,
    fenced: usize,
    incarnation: usize,
}

/// Deliver held deletes.
async fn deliver(old: &NodeClients, deletes: &[(String, ShardRef, u64)]) -> Delivered {
    let mut d = Delivered::default();
    for (addr, shard, epoch) in deletes {
        match old.delete_shard(addr, *shard, *epoch).await {
            Ok(()) => d.applied += 1,
            Err(e) if NodeClients::is_fenced(&e) => d.fenced += 1,
            Err(e) if e.message().starts_with("incarnation rejected") => d.incarnation += 1,
            Err(e) => panic!("refused for another reason: {e}"),
        }
    }
    d
}

async fn snapshot<S: KvStore>(meta: &S) -> Vec<(Vec<u8>, Vec<u8>)> {
    transact(meta, (), |tx, ()| {
        Box::pin(async move {
            Ok(tx
                .get_range(&keys::all_range(), usize::MAX, false)
                .await?
                .into_iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
        })
    })
    .await
    .unwrap()
}

/// `fdbrestore` of `snap`: the arca-lake key space replaced as it was.
async fn restore<S: KvStore>(meta: &S, snap: &[(Vec<u8>, Vec<u8>)]) {
    let snap = snap.to_vec();
    transact(meta, snap, |tx, snap| {
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

/// The recovery procedure after the restore: mark, verify, finalize.
async fn recover(c: &TestCluster) -> [u8; 16] {
    // `recovery begin`: close everywhere (with the hold), then mark.
    let closed = close_and_push(
        c.meta.as_ref(),
        &c.clients,
        "recovery begin",
        true,
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!(closed.not_acknowledged(), Vec::<u64>::new());
    let marked = compat::mark_restoring(c.meta.as_ref(), None, None, "admin", now_ms())
        .await
        .unwrap();
    c.clients.set_incarnation(Some(marked.incarnation));
    let admin = GuardedKv::new(Arc::clone(&c.meta), marked.incarnation, OpClass::Admin);
    for _ in 0..20 {
        if recovery_pass(
            &admin,
            &c.clients,
            &RecoveryOptions::default(),
            None,
            "ctl",
            now_ms(),
        )
        .await
        .unwrap()
        .finished
        {
            break;
        }
    }
    let (v, _) = recovery::finalize(
        c.meta.as_ref(),
        &FinalizeRequest {
            incarnation: marked.incarnation,
            operator: "op".into(),
            reason: "drill".into(),
            force: true,
            now_ms: now_ms(),
        },
    )
    .await
    .unwrap();
    assert_eq!(v.refusal, None, "{v:?}");
    marked.incarnation
}

/// A leader's health pass, the fence part: sync for its term, push to the
/// nodes, take it for this cluster's clients.
async fn leader_pass(c: &TestCluster, token: u64) -> delete_fence::DeleteFenceRecord {
    let inc = compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap()
        .incarnation;
    let mut highest = 0;
    for n in &c.nodes {
        if let Ok(h) = c.clients.health(&n.addr.to_string()).await {
            // As the leader's FenceView: a closed generation needs one above.
            highest = highest.max(h.delete_fence_generation + u64::from(!h.delete_fence_open));
        }
    }
    let rec = sync_for_term(c.meta.as_ref(), token, inc, highest, now_ms())
        .await
        .unwrap()
        .record;
    c.push_delete_fence(&rec).await;
    rec
}

/// Invariant C, the R-88 race closed: GC decided to delete an object's
/// shards on the metadata before a restore; the fence is closed on every
/// node (the runbook's first step); the metadata is restored to a backup
/// in which the object exists; the decided deletes arrive afterwards. None
/// is applied, the object reads back, and once recovery finalized and a
/// leader opened a new generation, GC collects what is garbage now.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_decided_before_a_restore_never_removes_what_the_restored_metadata_needs() {
    let mut c = cluster().await;
    let kept = put(&c, "kept", 1).await;
    let backup = snapshot(c.meta.as_ref()).await;
    let deletes = decided_deletes(&c, "kept").await;
    // After the backup point the object is deleted and GC decides.
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    let old = frozen_controller(&c);
    // Runbook: close the fence everywhere, then restore.
    let closed = close_and_push(
        c.meta.as_ref(),
        &c.clients,
        "before restore",
        true,
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!(closed.not_acknowledged(), Vec::<u64>::new(), "{closed:?}");
    restore(c.meta.as_ref(), &backup).await;
    // The delayed deletes of the previous metadata arrive.
    {
        let d = deliver(&old, &deletes).await;
        assert_eq!(d.applied, 0, "{d:?}");
        assert_eq!(
            d.incarnation, 0,
            "refused by the fence, not the incarnation: {d:?}"
        );
    }
    let _inc = recover(&c).await;
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    // Invariant F: the hold went with finalize; a leader opens a new
    // generation; delete again; GC collects; the old deletes stay refused.
    let rec = delete_fence::current(c.meta.as_ref()).await.unwrap();
    assert!(!rec.operator_hold && !rec.open, "{rec:?}");
    let opened = leader_pass(&c, 77).await;

    assert!(opened.open && opened.generation > closed.record.generation);
    // Refused (the old incarnation now as well).
    assert_eq!(deliver(&old, &deletes).await.applied, 0);
    let shards = c.total_shards().await;
    c.restart_backend().await.unwrap();
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    let far = now_ms() + 3_600_000;
    let r = c.gc(far, 0).await;
    let mut fences = Vec::new();
    for n in &c.nodes {
        let h = c.clients.health(&n.addr.to_string()).await.unwrap();
        fences.push((
            h.delete_fence_generation,
            h.delete_fence_open,
            h.delete_fence_hold,
        ));
    }
    assert!(
        c.total_shards().await < shards,
        "GC resumed: {r:?} {fences:?} {:?}",
        c.clients.delete_fence()
    );
    c.shutdown().await;
}

/// The same sequence without closing the fence first: the decided deletes
/// are applied and the restored object loses its shards. This is the R-88
/// race itself (why the runbook's first step and `recovery begin`'s check
/// exist): the incarnation alone does not stop it, because nodes learn the
/// new incarnation only after the restore.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_closing_the_fence_first_the_restored_object_loses_its_shards() {
    let c = cluster().await;
    put(&c, "lost", 2).await;
    let backup = snapshot(c.meta.as_ref()).await;
    let deletes = decided_deletes(&c, "lost").await;
    s3::delete(&c.backend, BUCKET, "lost").await.unwrap();
    let old = frozen_controller(&c);
    restore(c.meta.as_ref(), &backup).await;
    assert_eq!(deliver(&old, &deletes).await.applied, deletes.len());
    // `recovery begin`'s check sees nodes whose fence was still open.
    let closed = close_and_push(c.meta.as_ref(), &c.clients, "begin", true, now_ms())
        .await
        .unwrap();
    assert_eq!(closed.were_open.len(), c.nodes.len(), "{closed:?}");
    c.shutdown().await;
}

/// Invariants D and E across a phase change: deletes GC decided while
/// Active, delivered after `set-read-only` closed the fence, are refused,
/// and still are after the cluster is Active again and the leader opened a
/// new generation; deletes decided under the new one go through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_decided_before_a_phase_change_are_refused_after_it() {
    let c = cluster().await;
    put(&c, "a", 3).await;
    let deletes = decided_deletes(&c, "a").await;
    let old = frozen_controller(&c);
    compat::transition(
        c.meta.as_ref(),
        ClusterPhase::Active,
        ClusterPhase::ReadOnly,
        "admin",
        now_ms(),
    )
    .await
    .unwrap();
    let closed = close_and_push(c.meta.as_ref(), &c.clients, "read-only", false, now_ms())
        .await
        .unwrap();
    assert_eq!(closed.not_acknowledged(), Vec::<u64>::new());
    {
        let d = deliver(&old, &deletes).await;
        assert_eq!(d.applied, 0, "{d:?}");
        assert_eq!(
            d.incarnation, 0,
            "refused by the fence, not the incarnation: {d:?}"
        );
    }
    // A leader in ReadOnly keeps it closed.
    assert!(!leader_pass(&c, 5).await.open);
    compat::transition(
        c.meta.as_ref(),
        ClusterPhase::ReadOnly,
        ClusterPhase::Active,
        "admin",
        now_ms(),
    )
    .await
    .unwrap();
    let rec = leader_pass(&c, 5).await;
    assert!(rec.open);
    {
        let d = deliver(&old, &deletes).await;
        assert_eq!(d.applied, 0, "{d:?}");
        assert_eq!(
            d.incarnation, 0,
            "refused by the fence, not the incarnation: {d:?}"
        );
    }
    // Decided under the new generation: applied.
    let fresh = frozen_controller(&c);
    assert_eq!(deliver(&fresh, &deletes).await.applied, deletes.len());
    c.shutdown().await;
}

/// S7: a former leader's deletes are refused once the new leader opened
/// its generation on the nodes; duplicates are harmless; a delete
/// reordered behind the change is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_former_leaders_late_deletes_are_refused() {
    let c = cluster().await;
    put(&c, "x", 4).await;
    put(&c, "y", 5).await;
    let first = leader_pass(&c, 10).await;
    let former = frozen_controller(&c);
    let x = decided_deletes(&c, "x").await;
    let y = decided_deletes(&c, "y").await;
    // The former leader's delete of one shard, twice (a retry): the second
    // is a harmless no-op.
    assert_eq!(deliver(&former, &x[..1]).await.applied, 1);
    assert_eq!(deliver(&former, &x[..1]).await.applied, 1);
    let second = leader_pass(&c, 11).await;
    assert!(second.generation > first.generation);
    // Its other deletes arrive after the new leader's fence: refused.
    {
        let d = deliver(&former, &x[1..]).await;
        assert_eq!(d.applied, 0, "{d:?}");
        assert_eq!(
            d.incarnation, 0,
            "refused by the fence, not the incarnation: {d:?}"
        );
    }
    {
        let d = deliver(&former, &y).await;
        assert_eq!(d.applied, 0, "{d:?}");
        assert_eq!(
            d.incarnation, 0,
            "refused by the fence, not the incarnation: {d:?}"
        );
    }
    assert_eq!(
        s3::get(&c.backend, BUCKET, "y").await.unwrap().0.len(),
        2 << 20
    );
    c.shutdown().await;
}

/// Partial fence update: a node that is down misses the close; the close
/// reports it (a restore must not proceed); once it is back and the close
/// is repeated, it holds the closed generation and refuses the old
/// deletes. A node restarted keeps what it acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_missed_the_close_is_reported_and_closed_once_back() {
    let mut c = cluster().await;
    put(&c, "p", 6).await;
    let deletes = decided_deletes(&c, "p").await;
    let old = frozen_controller(&c);
    c.stop_node(2).await;
    let closed = close_and_push(c.meta.as_ref(), &c.clients, "maintenance", true, now_ms())
        .await
        .unwrap();
    assert_eq!(closed.not_acknowledged(), vec![c.nodes[2].id]);
    c.start_node(2).await;
    let again = close_and_push(c.meta.as_ref(), &c.clients, "maintenance", true, now_ms())
        .await
        .unwrap();
    assert_eq!(again.not_acknowledged(), Vec::<u64>::new(), "{again:?}");
    {
        let d = deliver(&old, &deletes).await;
        assert_eq!(d.applied, 0, "{d:?}");
        assert_eq!(
            d.incarnation, 0,
            "refused by the fence, not the incarnation: {d:?}"
        );
    }
    // A restarted node keeps the closed fence.
    c.stop_node(0).await;
    c.start_node(0).await;
    {
        let d = deliver(&old, &deletes).await;
        assert_eq!(d.applied, 0, "{d:?}");
        assert_eq!(
            d.incarnation, 0,
            "refused by the fence, not the incarnation: {d:?}"
        );
    }
    // The hold keeps a leader from opening it while Active.
    assert!(!leader_pass(&c, 3).await.open);
    // Released in metadata only, the nodes still refuse to open.
    delete_fence::release_hold(c.meta.as_ref(), "done", now_ms())
        .await
        .unwrap();
    let r = leader_pass(&c, 3).await;
    assert!(r.open);
    for n in &c.nodes {
        let h = c.clients.health(&n.addr.to_string()).await.unwrap();
        assert!(!h.delete_fence_open && h.delete_fence_hold, "{h:?}");
    }
    // The release pushed to the nodes: the leader's next pass opens them.
    let (_, held) = arca_lake_controller::delete_fence::release_and_push(
        c.meta.as_ref(),
        &c.clients,
        "done",
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!(held, Vec::<u64>::new());
    assert!(leader_pass(&c, 3).await.open);
    let fresh = frozen_controller(&c);
    let d = deliver(&fresh, &deletes[..1]).await;
    assert_eq!(d.applied, 1, "{d:?}");
    assert_eq!(
        s3::get(&c.backend, BUCKET, "p").await.unwrap().0.len(),
        2 << 20
    );
    c.shutdown().await;
}

/// The controller process end to end: a leader opens the fence for its
/// term and nodes adopt it; when the cluster leaves Active the leader
/// closes it on every node within a pass; back to Active it opens a new
/// generation and GC resumes (Invariant F).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leader_opens_the_fence_for_its_term_and_closes_it_outside_active() {
    use arca_lake_controller::process::{ControllerConfig, spawn};
    let c = cluster().await;
    put(&c, "z", 7).await;
    let mut cfg = ControllerConfig::new("ctl-fence");
    cfg.lease = Duration::from_secs(2);
    cfg.health_interval = Duration::from_millis(200);
    cfg.gc_interval = Duration::from_millis(200);
    cfg.gc_grace = Duration::ZERO;
    cfg.failure_domain = "host".into();
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let wait = async |what: &str, f: &(dyn Fn(&[(u64, bool)]) -> bool + Sync)| {
        let t = Instant::now();
        loop {
            let mut fences = Vec::new();
            for n in &c.nodes {
                let h = c.clients.health(&n.addr.to_string()).await.unwrap();
                fences.push((h.delete_fence_generation, h.delete_fence_open));
            }
            if f(&fences) {
                return fences;
            }
            assert!(t.elapsed() < Duration::from_secs(20), "{what}: {fences:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let start = c.clients.delete_fence().unwrap();
    let opened = wait("opened for the term", &|f| {
        f.iter().all(|&(g, o)| o && g > start)
    })
    .await;
    compat::transition(
        c.meta.as_ref(),
        ClusterPhase::Active,
        ClusterPhase::ReadOnly,
        "admin",
        now_ms(),
    )
    .await
    .unwrap();
    let g = opened[0].0;
    wait("closed outside Active", &|f| {
        f.iter().all(|&(x, o)| !o && x > g)
    })
    .await;
    compat::transition(
        c.meta.as_ref(),
        ClusterPhase::ReadOnly,
        ClusterPhase::Active,
        "admin",
        now_ms(),
    )
    .await
    .unwrap();
    wait("reopened", &|f| f.iter().all(|&(x, o)| o && x > g + 1)).await;
    // GC by the leader collects a deleted object.
    let before = c.total_shards().await;
    s3::delete(&c.backend, BUCKET, "z").await.unwrap();
    let t = Instant::now();
    while c.total_shards().await >= before {
        assert!(t.elapsed() < Duration::from_secs(20), "GC never resumed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ctl.shutdown().await;
    c.shutdown().await;
}
