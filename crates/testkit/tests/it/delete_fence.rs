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
    recover_at(c, now_ms()).await
}

/// [`recover`] with the restore marked at `marked_ms` (a clock that went
/// back, Phase 3.10).
async fn recover_at(c: &TestCluster, marked_ms: u64) -> [u8; 16] {
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
    // Above every node's incarnation sequence, the schema back up to what
    // the nodes saw, as `recovery begin` does.
    let seq = arca_lake_controller::delete_fence::restore_floors(c.meta.as_ref(), &c.clients, &[])
        .await
        .unwrap();
    let marked = compat::mark_restoring_above(c.meta.as_ref(), None, None, "admin", marked_ms, seq)
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
    // Gateways start again on the restored metadata (runbook): one of the
    // old incarnation refuses once its refresh sees the new one.
    c.restart_backend().await.unwrap();
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
    let (state, _) = compat::transition(
        c.meta.as_ref(),
        ClusterPhase::ReadOnly,
        ClusterPhase::Active,
        "admin",
        now_ms(),
    )
    .await
    .unwrap();
    // The gateway sees it now rather than at its next refresh.
    c.backend.cluster().set(Some(state));
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

/// A leader killed while it pushed its new generation: only node 0 took
/// it. The deletes decided under the previous generation stay refused on
/// node 0 and still apply on the others (their fence is unchanged, and the
/// decision is still valid in the same metadata). The next leader opens
/// above what any node holds, so every node ends up at one generation and
/// both earlier generations are refused everywhere. A close interrupted the
/// same way (record written, one node pushed) is completed by running it
/// again: a higher generation, acknowledged by all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_killed_while_pushing_its_fence_is_superseded_cleanly() {
    let c = cluster().await;
    put(&c, "k", 8).await;
    let first = leader_pass(&c, 20).await;
    let before = frozen_controller(&c);
    let deletes = decided_deletes(&c, "k").await;
    // The killed leader: record written, pushed to node 0 only.
    let inc = compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap()
        .incarnation;
    let killed = sync_for_term(c.meta.as_ref(), 21, inc, first.generation, now_ms())
        .await
        .unwrap()
        .record;
    assert!(killed.open && killed.generation > first.generation);
    let addr0 = c.nodes[0].addr.to_string();
    c.clients
        .push_delete_fence_with(
            &addr0,
            killed.generation,
            true,
            arca_lake_protocol::client::FencePush {
                incarnation: Some(inc),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let on0: Vec<_> = deletes.iter().filter(|d| d.0 == addr0).cloned().collect();
    assert_ne!(on0.len(), 0);
    let d = deliver(&before, &on0).await;
    assert_eq!(d.fenced, on0.len(), "{d:?}");
    // The next leader: above every node's generation.
    let next = leader_pass(&c, 22).await;
    assert!(next.generation > killed.generation, "{next:?}");
    for n in &c.nodes {
        let h = c.clients.health(&n.addr.to_string()).await.unwrap();
        assert_eq!(
            (h.delete_fence_generation, h.delete_fence_open),
            (next.generation, true)
        );
    }
    let killed_leader = NodeClients::new(Duration::from_secs(5)).with_tls(c.clients.tls().cloned());
    killed_leader.set_incarnation(c.clients.incarnation());
    killed_leader.set_delete_fence(Some(killed.generation));
    for who in [&before, &killed_leader] {
        let d = deliver(who, &deletes).await;
        assert_eq!(d.fenced, deletes.len(), "{d:?}");
    }
    assert_eq!(
        s3::get(&c.backend, BUCKET, "k").await.unwrap().0.len(),
        2 << 20
    );
    // A close interrupted after its record and one push.
    let rec = delete_fence::close(
        c.meta.as_ref(),
        next.generation,
        "interrupted",
        true,
        now_ms(),
    )
    .await
    .unwrap();
    c.clients
        .push_delete_fence(&addr0, rec.generation, false)
        .await
        .unwrap();
    let again = close_and_push(c.meta.as_ref(), &c.clients, "again", true, now_ms())
        .await
        .unwrap();
    assert!(again.record.generation > rec.generation);
    assert_eq!(again.not_acknowledged(), Vec::<u64>::new());
    c.shutdown().await;
}

/// Phase 3.9 re-review: a restore whose close was skipped leaves the
/// nodes open at G while the restored record is older. A fresh controller
/// process on the restored metadata knows no node's generation; it opens
/// only after a round of reports, above G, so the deletes decided under G
/// before the restore are refused once the nodes adopt it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_never_reopens_a_nodes_own_generation_after_a_restore() {
    use arca_lake_controller::process::{ControllerConfig, spawn};
    let c = cluster().await;
    put(&c, "r", 9).await;
    // The backup's record is one generation behind the nodes' G: the
    // reopening a fresh view would choose is exactly G.
    leader_pass(&c, 40).await;
    let backup = snapshot(c.meta.as_ref()).await;
    let g = leader_pass(&c, 41).await.generation;
    let old = frozen_controller(&c);
    let deletes = decided_deletes(&c, "r").await;
    s3::delete(&c.backend, BUCKET, "r").await.unwrap();
    // No close: the restore brings back an older record.
    restore(c.meta.as_ref(), &backup).await;
    assert!(
        delete_fence::current(c.meta.as_ref())
            .await
            .unwrap()
            .generation
            < g
    );
    let mut cfg = ControllerConfig::new("ctl-restored");
    cfg.lease = Duration::from_secs(2);
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_secs(3600);
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.failure_domain = "host".into();
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let t = Instant::now();
    loop {
        let rec = delete_fence::current(c.meta.as_ref()).await.unwrap();
        // Written by the new controller (not the restored record of the
        // test's term 40). Since Phase 3.10 it sees the nodes ahead of the
        // metadata and holds the fence instead of opening above them.
        assert!(
            !(rec.open && rec.leader_token != 40 && rec.generation <= g),
            "reopened the nodes' own {g}: {rec:?}"
        );
        if !rec.open && rec.operator_hold && rec.generation > g {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "never held: {rec:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let t = Instant::now();
    loop {
        let mut all = true;
        for n in &c.nodes {
            let h = c.clients.health(&n.addr.to_string()).await.unwrap();
            all &= h.delete_fence_generation > g;
        }
        if all {
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "nodes never moved on"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let d = deliver(&old, &deletes).await;
    assert_eq!(d.applied, 0, "{d:?}");
    assert_eq!(
        s3::get(&c.backend, BUCKET, "r").await.unwrap().0.len(),
        2 << 20
    );
    ctl.shutdown().await;
    c.shutdown().await;
}

/// R-90 (Phase 3.10): every safeguard but the nodes' own skipped. The
/// restore is done without `admin delete-fence close`, a controller is
/// started on the restored metadata before `recovery begin`, and a node is
/// down during its first round of reports. The controller finds the
/// reachable nodes ahead of the metadata and closes the fence with a hold;
/// the node that was down rejoins closed. Deletes decided before the
/// restore are refused everywhere, before and after the node is back; the
/// restored object stays whole; `recovery begin` and finalize let GC
/// resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::many_single_char_names)]
async fn a_restore_with_every_procedural_safeguard_skipped_loses_nothing() {
    use arca_lake_controller::process::{ControllerConfig, spawn};
    let mut c = cluster().await;
    let kept = put(&c, "kept", 11).await;
    leader_pass(&c, 50).await;
    let backup = snapshot(c.meta.as_ref()).await;
    let g = leader_pass(&c, 51).await.generation;
    let old = frozen_controller(&c);
    let deletes = decided_deletes(&c, "kept").await;
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    // No close; restore; one node down.
    restore(c.meta.as_ref(), &backup).await;
    c.stop_node(0).await;
    let mut cfg = ControllerConfig::new("ctl-early");
    cfg.lease = Duration::from_secs(2);
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_millis(100);
    cfg.gc_grace = Duration::ZERO;
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.failure_domain = "host".into();
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let t = Instant::now();
    let held = loop {
        let rec = delete_fence::current(c.meta.as_ref()).await.unwrap();
        if rec.operator_hold && rec.generation > g {
            break rec;
        }
        assert!(
            !(rec.open && rec.leader_token != 50),
            "opened on restored metadata: {rec:?}"
        );
        assert!(t.elapsed() < Duration::from_secs(20), "never held: {rec:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(
        !held.open && held.reason.contains("above the metadata"),
        "{held:?}"
    );
    let addr0 = c.nodes[0].addr.to_string();
    let (on0, reachable): (Vec<_>, Vec<_>) = deletes.iter().cloned().partition(|d| d.0 == addr0);
    let d = deliver(&old, &reachable).await;
    assert_eq!(d.applied, 0, "{d:?}");
    // The node that was down comes back, nobody pushing it yet.
    c.start_node_quiet(0).await;
    let d = deliver(&old, &on0).await;
    assert_eq!(d.applied, 0, "the node rejoined open: {d:?}");
    // The controller reaches it: closed and held there too.
    let t = Instant::now();
    loop {
        let h = c.clients.health(&addr0).await.unwrap();
        if h.delete_fence_generation >= held.generation && h.delete_fence_hold {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "{h:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(deliver(&old, &deletes).await.applied, 0);
    ctl.shutdown().await;
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    // The procedure, late: recovery, then GC collects garbage again.
    recover(&c).await;
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    leader_pass(&c, 60).await;
    let shards = c.total_shards().await;
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    c.gc(now_ms() + 3_600_000, 0).await;
    assert!(c.total_shards().await < shards, "GC resumed");
    c.shutdown().await;
}

/// R-92 (Phase 3.10): two restores, the clock that marks the second one an
/// hour behind the first. Nodes adopt the second restore's incarnation
/// (its sequence is above every node's, collected by `recovery begin`),
/// and a controller of the first one, whose stamp is the later, cannot
/// push its incarnation back; its deletes are refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clock_going_back_between_restores_does_not_reorder_incarnations() {
    let mut c = cluster().await;
    let kept = put(&c, "kept", 12).await;
    let first_backup = snapshot(c.meta.as_ref()).await;
    restore(c.meta.as_ref(), &first_backup).await;
    let i1 = recover(&c).await;
    c.restart_backend().await.unwrap();
    let after_first = compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap();
    let first = frozen_controller(&c);
    // The second restore goes back to the first backup, marked by a clock
    // an hour behind.
    restore(c.meta.as_ref(), &first_backup).await;
    let i2 = recover_at(&c, now_ms() - 3_600_000).await;
    assert_ne!(i1, i2);
    let s2 = compat::gate(c.meta.as_ref()).await.unwrap().incarnation_seq;
    for n in &c.nodes {
        let h = c.clients.health(&n.addr.to_string()).await.unwrap();
        assert_eq!(h.incarnation.as_ref(), &i2, "node kept the first restore's");
        assert_eq!(h.incarnation_seq, s2);
    }
    // The first restore's controller pushes its incarnation, stamped later.
    for n in &c.nodes {
        c.clients
            .health_with(
                &n.addr.to_string(),
                arca_lake_protocol::storage::v1::HealthRequest {
                    incarnation: Bytes::copy_from_slice(&i1),
                    incarnation_changed_ms: after_first.changed_ms + 7_200_000,
                    incarnation_seq: s2 - 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let h = c.clients.health(&n.addr.to_string()).await.unwrap();
        assert_eq!(h.incarnation.as_ref(), &i2);
    }
    let deletes = decided_deletes(&c, "kept").await;
    let d = deliver(&first, &deletes).await;
    assert_eq!(d.applied, 0, "{d:?}");
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    c.shutdown().await;
}

/// Phase 3.10 downgrade gate. A store a release with the delete fence
/// (protocol 3) left at schema 2 still admits that release's controllers
/// and gateways (rollback before finalize). Once `upgrade finalize` raised
/// the schema to 3 and a leader told the nodes, that release refuses to
/// start, and so does every release without the fence. A restore of
/// metadata from before the finalize brings schema 2 back, but `recovery
/// begin` raises it to what the nodes saw: the release stays refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_finalized_upgrade_is_not_undone_by_a_rollback_or_a_restore() {
    use arca_lake_common::compat::Capabilities;
    use arca_lake_controller::process::{ControllerConfig, spawn};
    let mut c = cluster().await;
    put(&c, "kept", 13).await;
    // The store as the previous release left it: schema 2.
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.set(&keys::schema_version(), &2u64.to_le_bytes());
            Ok(())
        })
    })
    .await
    .unwrap();
    let previous = Capabilities {
        protocol_min: 2,
        protocol_max: 3,
        schema_min: 1,
        schema_max: 2,
        format_min: 1,
        format_max: 1,
    };
    let start = async |caps: &Capabilities| {
        compat::check_startup(
            c.meta.as_ref(),
            caps,
            compat::Stamp::FreshOnly,
            "previous",
            now_ms(),
        )
        .await
    };
    start(&previous).await.expect("rollback before finalize");
    let before_finalize = snapshot(c.meta.as_ref()).await;
    // Finalize raises the schema; the leader tells the nodes.
    compat::raise_version(
        c.meta.as_ref(),
        compat::VersionKey::Schema,
        keys::SCHEMA_VERSION,
    )
    .await
    .unwrap();
    let mut cfg = ControllerConfig::new("ctl-finalized");
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_secs(3600);
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.failure_domain = "host".into();
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let t = Instant::now();
    'told: loop {
        let mut all = true;
        for n in &c.nodes {
            all &= c
                .clients
                .health(&n.addr.to_string())
                .await
                .unwrap()
                .schema_floor
                == keys::SCHEMA_VERSION;
        }
        if all {
            break 'told;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "nodes never told");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ctl.shutdown().await;
    let e = start(&previous).await.unwrap_err();
    assert!(e.to_string().contains("schema"), "{e}");
    // A restore of metadata from before the finalize: schema 2 again ...
    restore(c.meta.as_ref(), &before_finalize).await;
    start(&previous)
        .await
        .expect("the restored store alone would admit it");
    // ... until `recovery begin` brings it back up to the nodes'.
    recover(&c).await;
    let e = start(&previous).await.unwrap_err();
    assert!(e.to_string().contains("schema"), "{e}");
    start(&arca_lake_controller::capabilities()).await.unwrap();
    c.restart_backend().await.unwrap();
    assert_eq!(
        s3::get(&c.backend, BUCKET, "kept").await.unwrap().0.len(),
        2 << 20
    );
    c.shutdown().await;
}

/// Phase 3.10 review: metadata restored without `recovery begin`, with no
/// fence change since the backup (the restored record is open, at the
/// generation the nodes hold). The nodes' rejoin lease on it has lapsed. A
/// controller started on the restored metadata must not push that
/// generation open before it has opened one of its own: the push would
/// renew the lease, and deletes decided before the restore under it would
/// apply. Delivered in a loop while the controller starts, none applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_open_fence_is_never_renewed_by_a_new_controller() {
    use arca_lake_controller::process::{ControllerConfig, spawn};
    let mut c = cluster().await;
    c.fence_lease = Duration::from_secs(2);
    for i in 0..c.nodes.len() {
        c.stop_node(i).await;
        c.start_node(i).await;
    }
    let kept = put(&c, "kept", 14).await;
    let g = leader_pass(&c, 51).await.generation;
    let backup = snapshot(c.meta.as_ref()).await;
    let old = frozen_controller(&c);
    let deletes = decided_deletes(&c, "kept").await;
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    restore(c.meta.as_ref(), &backup).await;
    // The former leader is gone: the lease lapses.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(deliver(&old, &deletes).await.applied, 0, "lease still live");
    let mut cfg = ControllerConfig::new("ctl-restored");
    cfg.lease = Duration::from_secs(2);
    cfg.health_interval = Duration::from_millis(500);
    cfg.gc_interval = Duration::from_secs(3600);
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.failure_domain = "host".into();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pump = {
        let stop = Arc::clone(&stop);
        let deletes = deletes.clone();
        tokio::spawn(async move {
            let mut applied = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                applied += deliver(&old, &deletes).await.applied;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            applied
        })
    };
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    // Until the controller has its own generation (or held the fence).
    let t = Instant::now();
    loop {
        let rec = delete_fence::current(c.meta.as_ref()).await.unwrap();
        if rec.generation > g {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "{rec:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(pump.await.unwrap(), 0, "a pre-restore delete applied");
    ctl.shutdown().await;
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    c.shutdown().await;
}

/// Phase 3.10 review: the metadata is restored under a leader that keeps
/// running (the backup taken while it led, its fence open and unchanged
/// since). Every check of its own fence still matched, and its deletes,
/// decided before the restore, would have applied. Its next pass reads
/// back a lineage counter below the one it wrote: it closes the fence
/// with a hold, and those deletes are refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_under_a_running_leader_closes_its_fence() {
    use arca_lake_controller::process::{ControllerConfig, spawn};
    let c = cluster().await;
    let kept = put(&c, "kept", 15).await;
    let mut cfg = ControllerConfig::new("ctl-running");
    cfg.lease = Duration::from_secs(2);
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_secs(3600);
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.failure_domain = "host".into();
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let t = Instant::now();
    let opened = loop {
        let rec = delete_fence::current(c.meta.as_ref()).await.unwrap();
        if rec.open && rec.leader_token != 0 && ctl.is_leader() {
            break rec;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "{rec:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let lineage = async || {
        transact(c.meta.as_ref(), (), |tx, ()| {
            Box::pin(async move {
                Ok(tx
                    .get(&keys::delete_fence_lineage())
                    .await?
                    .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
                    .map_or(0, u64::from_le_bytes))
            })
        })
        .await
        .unwrap()
    };
    let backup = snapshot(c.meta.as_ref()).await;
    let at_backup = lineage().await;
    // The leader goes on after the backup point.
    let t = Instant::now();
    while lineage().await < at_backup + 2 {
        assert!(t.elapsed() < Duration::from_secs(20));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Deletes the leader decided (taken with its fence) before the restore.
    let old = frozen_controller(&c);
    old.set_delete_fence(Some(opened.generation));
    let deletes = decided_deletes(&c, "kept").await;
    restore(c.meta.as_ref(), &backup).await;
    let t = Instant::now();
    let held = loop {
        let rec = delete_fence::current(c.meta.as_ref()).await.unwrap();
        if rec.operator_hold && !rec.open {
            break rec;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "never held: {rec:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(held.reason.contains("lineage"), "{held:?}");
    let t = Instant::now();
    loop {
        let mut closed = true;
        for n in &c.nodes {
            let h = c.clients.health(&n.addr.to_string()).await.unwrap();
            closed &= !h.delete_fence_open || h.delete_fence_generation > opened.generation;
        }
        if closed {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(20));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(deliver(&old, &deletes).await.applied, 0);
    ctl.shutdown().await;
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    c.shutdown().await;
}
