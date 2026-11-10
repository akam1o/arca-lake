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

/// Wait until `ctl` leads with its term's generation open on every node
/// (the metadata record opens before the pushes reach the nodes).
async fn term_open_everywhere<S: KvStore + 'static>(
    c: &TestCluster<S>,
    ctl: &arca_lake_controller::process::ControllerHandle,
) {
    let t = Instant::now();
    loop {
        let rec = delete_fence::current(c.meta.as_ref()).await.unwrap();
        let mut everywhere = ctl.is_leader() && rec.open && rec.leader_token == ctl.token();
        for n in &c.nodes {
            let h = c.clients.health(&n.addr.to_string()).await.unwrap();
            everywhere &= h.delete_fence_open && h.delete_fence_generation == rec.generation;
        }
        if everywhere {
            return;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "{rec:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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

/// R-94(b) (Phase 3.11): a leader keeps running through a restore done
/// without the runbook's fence close. Its GC decided, after the backup
/// point, to collect an object deleted after that point (the decision is
/// held between its reference check and its shard deletes). The operator
/// clears the keyspace (as before `fdbrestore`) and restores the backup,
/// in which the object exists; the held deletes are then sent, between
/// the clear and the restore or after it. The restored object must stay
/// whole: the leader saw the metadata go away (its next transaction
/// failed) before it sent them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_under_a_running_leaders_held_gc_batch_loses_nothing() {
    held_gc_batch(false).await;
}

/// [`a_restore_under_a_running_leaders_held_gc_batch_loses_nothing`], the
/// batch released only once the restore is done (scenario 12).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_gc_batch_released_after_the_restore_loses_nothing() {
    held_gc_batch(true).await;
}

async fn held_gc_batch(after_restore: bool) {
    use arca_lake_controller::process::{ControllerConfig, spawn};
    let mut c = cluster().await;
    let kept = put(&c, "kept", 16).await;
    let mut cfg = ControllerConfig::new("ctl-running");
    // Long: no renewal falls in the window, the trip alone must act.
    cfg.lease = Duration::from_secs(60);
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_millis(100);
    cfg.gc_grace = Duration::ZERO;
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.failure_domain = "host".into();
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    term_open_everywhere(&c, &ctl).await;
    // GC's deletes are held once decided.
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *ctl.metrics.gc.delete_gate.lock().unwrap() = Some(Arc::clone(&gate));
    // Release the batch and wait until it sent every delete (whatever
    // the nodes answered).
    let release = async |gate: &tokio::sync::Semaphore| {
        let sent = ctl.metrics.gc.delete_generation.snapshot().count;
        gate.add_permits(1_000);
        let t = Instant::now();
        while ctl.metrics.gc.delete_generation.snapshot().count == sent {
            assert!(
                t.elapsed() < Duration::from_secs(20),
                "the batch never finished"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let backup = snapshot(c.meta.as_ref()).await;
    let shards_at_backup = c.total_shards().await;
    // After the backup point: the client deletes the object; GC decides.
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    let t = Instant::now();
    while ctl
        .metrics
        .gc
        .gated
        .load(std::sync::atomic::Ordering::Relaxed)
        == 0
    {
        assert!(t.elapsed() < Duration::from_secs(20), "GC never decided");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The restore: the keyspace cleared, the leader's next passes see it
    // (bounded: without the trip they never do), the backup written back;
    // the held deletes are sent before or after it.
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.clear_range(&keys::all_range());
            Ok(())
        })
    })
    .await
    .unwrap();
    let t = Instant::now();
    while !ctl.metrics.delete_fence.restore_suspected() && t.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if !after_restore {
        release(&gate).await;
    }
    restore(c.meta.as_ref(), &backup).await;
    if after_restore {
        release(&gate).await;
    }
    assert_eq!(
        c.total_shards().await,
        shards_at_backup,
        "deletes decided before the restore applied after it"
    );
    ctl.abort();
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    c.shutdown().await;
}

/// R-94(b), deletes already sent (Phase 3.11): the running leader's GC
/// batch passed its own fence check and its `DeleteShard` RPCs are in
/// flight (held before they reach the nodes) when the operator clears the
/// keyspace for a restore. The leader's next transaction sees the store
/// replaced and closes its open generation on the nodes themselves (no
/// metadata needed); the delayed RPCs land after that and are refused, so
/// the restore that follows finds every shard it references.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_leaders_in_flight_deletes_are_refused_once_it_sees_the_keyspace_cleared() {
    use arca_lake_controller::process::{ControllerConfig, RpcFaultHook, spawn};
    use arca_lake_protocol::client::{FaultAction, Rpc};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let mut c = cluster().await;
    let kept = put(&c, "kept", 17).await;
    let armed = Arc::new(AtomicBool::new(false));
    let held = Arc::new(AtomicUsize::new(0));
    let rpc_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut cfg = ControllerConfig::new("ctl-running");
    cfg.lease = Duration::from_secs(2);
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_millis(100);
    cfg.gc_grace = Duration::ZERO;
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.failure_domain = "host".into();
    cfg.rpc_fault_hook = Some(RpcFaultHook({
        let (armed, held, rpc_gate) =
            (Arc::clone(&armed), Arc::clone(&held), Arc::clone(&rpc_gate));
        Arc::new(move |ctx| {
            if ctx.rpc == Rpc::DeleteShard && armed.load(Ordering::SeqCst) {
                held.fetch_add(1, Ordering::SeqCst);
                FaultAction::Hold(Arc::clone(&rpc_gate))
            } else {
                FaultAction::None
            }
        })
    }));
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    term_open_everywhere(&c, &ctl).await;
    armed.store(true, Ordering::SeqCst);
    let backup = snapshot(c.meta.as_ref()).await;
    let shards_at_backup = c.total_shards().await;
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    let t = Instant::now();
    while held.load(Ordering::SeqCst) == 0 {
        assert!(t.elapsed() < Duration::from_secs(20), "GC never sent");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The operator clears the keyspace; the leader trips on it.
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.clear_range(&keys::all_range());
            Ok(())
        })
    })
    .await
    .unwrap();
    let t = Instant::now();
    // Bounded: without the trip the leader never notices.
    while !ctl.metrics.delete_fence.restore_suspected() && t.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The trip's close reaches the nodes (bounded: without it the deletes
    // below would land on an open fence).
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(5) {
        let mut open = false;
        for n in &c.nodes {
            open |= c
                .clients
                .health(&n.addr.to_string())
                .await
                .unwrap()
                .delete_fence_open;
        }
        if !open {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The in-flight deletes land between the clear and the restore.
    rpc_gate.add_permits(1_000);
    tokio::time::sleep(Duration::from_secs(2)).await;
    restore(c.meta.as_ref(), &backup).await;
    assert_eq!(
        c.total_shards().await,
        shards_at_backup,
        "in-flight deletes applied after the leader saw the keyspace cleared"
    );
    ctl.abort();
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    c.shutdown().await;
}

/// R-94(b), an idle leader (Phase 3.11): as
/// [`a_running_leaders_in_flight_deletes_are_refused_once_it_sees_the_keyspace_cleared`],
/// but no task of the leader runs a transaction while its deletes are in
/// flight (its health pass stuck on a node RPC, the GC batch busy
/// sending, no renewal due): only the leader's store watch can see the
/// keyspace cleared.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_leaders_store_watch_sees_the_keyspace_cleared() {
    use arca_lake_controller::process::{ControllerConfig, RpcFaultHook, spawn};
    use arca_lake_protocol::client::{FaultAction, Rpc};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let mut c = cluster().await;
    let kept = put(&c, "kept", 21).await;
    let armed = Arc::new(AtomicBool::new(false));
    let held = Arc::new(AtomicUsize::new(0));
    let health_held = Arc::new(AtomicUsize::new(0));
    let health_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let rpc_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut cfg = ControllerConfig::new("ctl-running");
    cfg.lease = Duration::from_secs(60);
    // Every other periodic task far apart.
    let idle = Duration::from_secs(3600);
    cfg.repair_interval = idle;
    cfg.evict_interval = idle;
    cfg.tiering_interval = idle;
    cfg.rebalance_interval = idle;
    cfg.scrub_tick = idle;
    cfg.stripe_repair_interval = idle;
    cfg.recovery_interval = idle;
    cfg.upgrade_interval = idle;
    cfg.heartbeat = None;
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_millis(100);
    cfg.gc_grace = Duration::ZERO;
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.failure_domain = "host".into();
    cfg.rpc_fault_hook = Some(RpcFaultHook({
        let (armed, held, rpc_gate, health_held, health_gate) = (
            Arc::clone(&armed),
            Arc::clone(&held),
            Arc::clone(&rpc_gate),
            Arc::clone(&health_held),
            Arc::clone(&health_gate),
        );
        Arc::new(move |ctx| {
            // One health RPC of the leader's pass held (the pass waits for
            // it); the trip's own close pushes are not.
            if ctx.rpc == Rpc::Health
                && armed.load(Ordering::SeqCst)
                && health_held.fetch_add(1, Ordering::SeqCst) == 0
            {
                FaultAction::Hold(Arc::clone(&health_gate))
            } else if ctx.rpc == Rpc::DeleteShard && armed.load(Ordering::SeqCst) {
                held.fetch_add(1, Ordering::SeqCst);
                FaultAction::Hold(Arc::clone(&rpc_gate))
            } else {
                FaultAction::None
            }
        })
    }));
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    term_open_everywhere(&c, &ctl).await;
    armed.store(true, Ordering::SeqCst);
    // The health pass is stuck past its transactions.
    let t = Instant::now();
    while health_held.load(Ordering::SeqCst) == 0 {
        assert!(t.elapsed() < Duration::from_secs(20));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let backup = snapshot(c.meta.as_ref()).await;
    let shards_at_backup = c.total_shards().await;
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    let t = Instant::now();
    while held.load(Ordering::SeqCst) == 0 {
        assert!(t.elapsed() < Duration::from_secs(20), "GC never sent");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The operator clears the keyspace; the leader trips on it.
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.clear_range(&keys::all_range());
            Ok(())
        })
    })
    .await
    .unwrap();
    let t = Instant::now();
    // Bounded: without the trip the leader never notices.
    while !ctl.metrics.delete_fence.restore_suspected() && t.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The trip's close reaches the nodes (bounded: without it the deletes
    // below would land on an open fence).
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(5) {
        let mut open = false;
        for n in &c.nodes {
            open |= c
                .clients
                .health(&n.addr.to_string())
                .await
                .unwrap()
                .delete_fence_open;
        }
        if !open {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The in-flight deletes land between the clear and the restore.
    rpc_gate.add_permits(1_000);
    tokio::time::sleep(Duration::from_secs(2)).await;
    restore(c.meta.as_ref(), &backup).await;
    assert_eq!(
        c.total_shards().await,
        shards_at_backup,
        "in-flight deletes applied after the leader saw the keyspace cleared"
    );
    health_gate.add_permits(1);
    ctl.abort();
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    c.shutdown().await;
}

/// R-94(b), reconciliation (Phase 3.11): after the backup point the
/// object is deleted and its generation's metadata finished, so its shards
/// are orphans to the running leader's reconciliation, whose deletes are
/// in flight when the operator clears the keyspace for a restore. As for
/// GC, the leader trips on the cleared store, closes its generation on the
/// nodes, and the delayed deletes are refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_leaders_in_flight_reconcile_deletes_are_refused_once_it_sees_the_keyspace_cleared()
 {
    use arca_lake_controller::process::{ControllerConfig, RpcFaultHook, spawn};
    use arca_lake_protocol::client::{FaultAction, Rpc};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let mut c = cluster().await;
    let kept = put(&c, "kept", 18).await;
    let armed = Arc::new(AtomicBool::new(false));
    let held = Arc::new(AtomicUsize::new(0));
    let rpc_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut cfg = ControllerConfig::new("ctl-running");
    cfg.lease = Duration::from_secs(2);
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_secs(3600);
    cfg.reconcile_interval = Duration::from_millis(100);
    cfg.min_orphan_age = Duration::ZERO;
    cfg.failure_domain = "host".into();
    cfg.rpc_fault_hook = Some(RpcFaultHook({
        let (armed, held, rpc_gate) =
            (Arc::clone(&armed), Arc::clone(&held), Arc::clone(&rpc_gate));
        Arc::new(move |ctx| {
            if ctx.rpc == Rpc::DeleteShard && armed.load(Ordering::SeqCst) {
                held.fetch_add(1, Ordering::SeqCst);
                FaultAction::Hold(Arc::clone(&rpc_gate))
            } else {
                FaultAction::None
            }
        })
    }));
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    term_open_everywhere(&c, &ctl).await;
    armed.store(true, Ordering::SeqCst);
    let backup = snapshot(c.meta.as_ref()).await;
    let shards_at_backup = c.total_shards().await;
    let generation = c.manifest(BUCKET, "kept").await.pointer.generation;
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    arca_lake_metadata::ops::gc::finish(c.meta.as_ref(), &generation)
        .await
        .unwrap();
    let t = Instant::now();
    while held.load(Ordering::SeqCst) == 0 {
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "reconciliation never sent"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The operator clears the keyspace; the leader trips on it.
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.clear_range(&keys::all_range());
            Ok(())
        })
    })
    .await
    .unwrap();
    let t = Instant::now();
    // Bounded: without the trip the leader never notices.
    while !ctl.metrics.delete_fence.restore_suspected() && t.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The trip's close reaches the nodes (bounded: without it the deletes
    // below would land on an open fence).
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(5) {
        let mut open = false;
        for n in &c.nodes {
            open |= c
                .clients
                .health(&n.addr.to_string())
                .await
                .unwrap()
                .delete_fence_open;
        }
        if !open {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The in-flight deletes land between the clear and the restore.
    rpc_gate.add_permits(1_000);
    tokio::time::sleep(Duration::from_secs(2)).await;
    restore(c.meta.as_ref(), &backup).await;
    assert_eq!(
        c.total_shards().await,
        shards_at_backup,
        "in-flight reconcile deletes applied after the leader saw the keyspace cleared"
    );
    ctl.abort();
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    c.shutdown().await;
}

/// R-94(b), the cleanup after a migration (Phase 3.11): after the backup
/// point the object's chunks move to another group (rebalancing or
/// tiering); the source shards are then the former placement, which the
/// running leader's reconciliation deletes (a migration never deletes its
/// source itself, Phase 3.5 CON-5). Those deletes are in flight when the
/// operator clears the keyspace; the restored metadata places the chunks
/// in the source group again and needs every source shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_leaders_in_flight_migration_cleanup_is_refused_once_it_sees_the_keyspace_cleared()
 {
    use arca_lake_controller::process::{ControllerConfig, RpcFaultHook, spawn};
    use arca_lake_protocol::client::{FaultAction, Rpc};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let mut c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(6, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    // One chunk: one source group.
    let kept = Bytes::from(random_bytes(usize::try_from(MIB).unwrap(), 20));
    s3::put(
        &c.backend,
        BUCKET,
        "kept",
        kept.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let held = Arc::new(AtomicUsize::new(0));
    let rpc_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut cfg = ControllerConfig::new("ctl-running");
    cfg.lease = Duration::from_secs(2);
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_secs(3600);
    cfg.reconcile_interval = Duration::from_millis(100);
    cfg.min_orphan_age = Duration::ZERO;
    cfg.failure_domain = "host".into();
    cfg.rpc_fault_hook = Some(RpcFaultHook({
        let (armed, held, rpc_gate) =
            (Arc::clone(&armed), Arc::clone(&held), Arc::clone(&rpc_gate));
        Arc::new(move |ctx| {
            if ctx.rpc == Rpc::DeleteShard && armed.load(Ordering::SeqCst) {
                held.fetch_add(1, Ordering::SeqCst);
                FaultAction::Hold(Arc::clone(&rpc_gate))
            } else {
                FaultAction::None
            }
        })
    }));
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    term_open_everywhere(&c, &ctl).await;
    armed.store(true, Ordering::SeqCst);
    let backup = snapshot(c.meta.as_ref()).await;
    // The migration, as rebalancing or tiering runs it.
    let m = c.manifest(BUCKET, "kept").await;
    let groups = c.groups().await;
    let source = groups
        .iter()
        .find(|g| g.id == m.chunks[0].1.placement.group_id)
        .unwrap()
        .clone();
    let target = groups.iter().find(|g| g.id != source.id).unwrap().clone();
    let in_source = |c: &TestCluster| {
        (0..c.nodes.len())
            .map(|i| c.shard_count(i, source.id))
            .sum::<usize>()
    };
    let source_at_backup = in_source(&c);
    assert!(source_at_backup > 0);
    let nodes: std::collections::HashMap<u64, arca_lake_metadata::records::NodeRecord> =
        arca_lake_metadata::ops::cluster::list_nodes(c.meta.as_ref())
            .await
            .unwrap()
            .into_iter()
            .map(|n| (n.id, n))
            .collect();
    let addr_of = |id: u64| nodes.get(&id).map(|n| n.address.clone());
    for (index, record) in &m.chunks {
        let outcome = arca_lake_tiering::migrate::migrate_chunk(
            c.meta.as_ref(),
            &c.clients,
            arca_lake_controller::integrity::coders().as_ref(),
            &addr_of,
            &source,
            &target,
            &m.pointer.generation,
            *index,
            record,
            None,
        )
        .await
        .unwrap();
        assert!(
            matches!(
                outcome,
                arca_lake_tiering::migrate::MigrateOutcome::Switched { .. }
            ),
            "{outcome:?}"
        );
    }
    let t = Instant::now();
    while held.load(Ordering::SeqCst) == 0 {
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "the cleanup never sent"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The operator clears the keyspace; the leader trips on it.
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.clear_range(&keys::all_range());
            Ok(())
        })
    })
    .await
    .unwrap();
    let t = Instant::now();
    // Bounded: without the trip the leader never notices.
    while !ctl.metrics.delete_fence.restore_suspected() && t.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The trip's close reaches the nodes (bounded: without it the deletes
    // below would land on an open fence).
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(5) {
        let mut open = false;
        for n in &c.nodes {
            open |= c
                .clients
                .health(&n.addr.to_string())
                .await
                .unwrap()
                .delete_fence_open;
        }
        if !open {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The in-flight deletes land between the clear and the restore.
    rpc_gate.add_permits(1_000);
    tokio::time::sleep(Duration::from_secs(2)).await;
    restore(c.meta.as_ref(), &backup).await;
    assert_eq!(
        in_source(&c),
        source_at_backup,
        "in-flight migration cleanup applied after the leader saw the keyspace cleared"
    );
    ctl.abort();
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    c.shutdown().await;
}

/// R-94(b), the lock (Phase 3.11): `fdbrestore` locks the database while
/// it writes (FoundationDB 1038, `database_locked`). A running leader
/// whose GC deletes are in flight sees its next commit refused as locked:
/// it stops deleting, closes its generation on the nodes, and steps down;
/// the delayed deletes are refused. Once the lock is gone, its next term
/// closes the fence with an operator hold rather than reopening on what
/// may be restored metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_leader_that_sees_the_store_locked_stops_its_in_flight_deletes() {
    use arca_lake_controller::process::{ControllerConfig, RpcFaultHook, spawn};
    use arca_lake_metadata::fault::{CommitFault, FaultKv};
    use arca_lake_metadata::mem::MemKv;
    use arca_lake_protocol::client::{FaultAction, Rpc};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions {
            groups: 1,
            ..ClusterOptions::rs(3, 2, 1, MIB, 0)
        },
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let d = Bytes::from(random_bytes(usize::try_from(2 * MIB).unwrap(), 19));
    s3::put(&c.backend, BUCKET, "kept", d, &PutOptions::default())
        .await
        .unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let held = Arc::new(AtomicUsize::new(0));
    let rpc_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut cfg = ControllerConfig::new("ctl-running");
    cfg.lease = Duration::from_secs(2);
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_millis(100);
    cfg.gc_grace = Duration::ZERO;
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.failure_domain = "host".into();
    cfg.rpc_fault_hook = Some(RpcFaultHook({
        let (armed, held, rpc_gate) =
            (Arc::clone(&armed), Arc::clone(&held), Arc::clone(&rpc_gate));
        Arc::new(move |ctx| {
            if ctx.rpc == Rpc::DeleteShard && armed.load(Ordering::SeqCst) {
                held.fetch_add(1, Ordering::SeqCst);
                FaultAction::Hold(Arc::clone(&rpc_gate))
            } else {
                FaultAction::None
            }
        })
    }));
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    term_open_everywhere(&c, &ctl).await;
    armed.store(true, Ordering::SeqCst);
    let shards_before = c.total_shards().await;
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    let t = Instant::now();
    while held.load(Ordering::SeqCst) == 0 {
        assert!(t.elapsed() < Duration::from_secs(20), "GC never sent");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The restore takes the lock: every write is refused.
    c.meta
        .inject(|w| !w.is_empty(), CommitFault::Locked, usize::MAX);
    let t = Instant::now();
    // Bounded: without the trip the leader never notices.
    while !ctl.metrics.delete_fence.restore_suspected() && t.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(5) {
        let mut open = false;
        for n in &c.nodes {
            open |= c
                .clients
                .health(&n.addr.to_string())
                .await
                .unwrap()
                .delete_fence_open;
        }
        if !open {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    rpc_gate.add_permits(1_000);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        c.total_shards().await,
        shards_before,
        "in-flight deletes applied after the leader saw the store locked"
    );
    // The lock is gone: the next term holds the fence for the operator.
    c.meta.clear_rules();
    let t = Instant::now();
    let rec = loop {
        let rec = delete_fence::current(c.meta.as_ref()).await.unwrap();
        if rec.operator_hold && !rec.open {
            break rec;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "never held: {rec:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(rec.reason.contains("locked or replaced"), "{rec:?}");
    assert!(!ctl.metrics.delete_fence.restore_suspected());
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Hold `DeleteShard` RPCs of a controller once armed, and optionally the
/// first `Health` RPC of its health pass (the pass then waits on it, past
/// its transactions); the trip's own fence pushes are never held.
struct Holds {
    armed: Arc<std::sync::atomic::AtomicBool>,
    held: Arc<std::sync::atomic::AtomicUsize>,
    deletes: Arc<tokio::sync::Semaphore>,
    health: Arc<tokio::sync::Semaphore>,
}

impl Holds {
    fn new() -> Self {
        Self {
            armed: Arc::default(),
            held: Arc::default(),
            deletes: Arc::new(tokio::sync::Semaphore::new(0)),
            health: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }

    fn hook(&self, stall_health: bool) -> arca_lake_controller::process::RpcFaultHook {
        use arca_lake_protocol::client::{FaultAction, Rpc};
        use std::sync::atomic::Ordering;
        let (armed, held, deletes, health) = (
            Arc::clone(&self.armed),
            Arc::clone(&self.held),
            Arc::clone(&self.deletes),
            Arc::clone(&self.health),
        );
        let health_held = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        arca_lake_controller::process::RpcFaultHook(Arc::new(move |ctx| {
            if !armed.load(Ordering::SeqCst) {
                return FaultAction::None;
            }
            if stall_health
                && ctx.rpc == Rpc::Health
                && health_held.fetch_add(1, Ordering::SeqCst) == 0
            {
                return FaultAction::Hold(Arc::clone(&health));
            }
            if ctx.rpc == Rpc::DeleteShard {
                held.fetch_add(1, Ordering::SeqCst);
                return FaultAction::Hold(Arc::clone(&deletes));
            }
            FaultAction::None
        }))
    }
}

/// A controller whose only periodic metadata access is its health pass,
/// GC and the store watch (everything else far apart).
fn quiet_controller(
    hook: arca_lake_controller::process::RpcFaultHook,
) -> arca_lake_controller::process::ControllerConfig {
    let mut cfg = arca_lake_controller::process::ControllerConfig::new("ctl-running");
    cfg.lease = Duration::from_secs(60);
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_millis(100);
    cfg.gc_grace = Duration::ZERO;
    let idle = Duration::from_secs(3600);
    cfg.reconcile_interval = idle;
    cfg.repair_interval = idle;
    cfg.evict_interval = idle;
    cfg.tiering_interval = idle;
    cfg.rebalance_interval = idle;
    cfg.scrub_tick = idle;
    cfg.stripe_repair_interval = idle;
    cfg.recovery_interval = idle;
    cfg.upgrade_interval = idle;
    cfg.heartbeat = None;
    cfg.failure_domain = "host".into();
    cfg.rpc_fault_hook = Some(hook);
    cfg
}

/// Wait until `n` more delete RPCs of `ctl`'s GC completed.
async fn deletes_done(ctl: &arca_lake_controller::process::ControllerHandle, before: u64, n: u64) {
    let t = Instant::now();
    while ctl.metrics.gc.delete_rpc.snapshot().count < before + n {
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "deletes never completed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// R-94(b) BOUNDARY (Phase 3.11, ADR-0025; this test pins a residual, it
/// does not show a guarantee): a restore that replaces the metadata
/// between two observations of the leader is not seen by the trip. Here
/// the restore is atomic (the clear and the backup's records in one
/// transaction, the limit of "completes within one watch interval"; a
/// snapshot rollback of the store's disks is the same). The restored
/// leader record carries the same holder and token, the cluster state is
/// there: the store watch finds nothing wrong, and the deletes in flight
/// apply: the restored object loses its shards. The rollback is detected
/// only afterwards, by the lineage check of the next health pass (ADR-0024),
/// which closes the fence with a hold. If this test starts failing, the
/// boundary moved: update ADR-0025 and R-94.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boundary_a_restore_between_two_observations_is_not_seen() {
    use std::sync::atomic::Ordering;
    let c = cluster().await;
    put(&c, "kept", 22).await;
    let holds = Holds::new();
    let ctl = arca_lake_controller::process::spawn(
        Arc::clone(&c.meta),
        quiet_controller(holds.hook(true)),
    )
    .await
    .unwrap();
    term_open_everywhere(&c, &ctl).await;
    holds.armed.store(true, Ordering::SeqCst);
    let backup = snapshot(c.meta.as_ref()).await;
    let shards_at_backup = c.total_shards().await;
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    let t = Instant::now();
    while holds.held.load(Ordering::SeqCst) < 6 {
        assert!(t.elapsed() < Duration::from_secs(20), "GC never sent");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    restore(c.meta.as_ref(), &backup).await;
    // Several watch intervals: nothing to see.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !ctl.metrics.delete_fence.restore_suspected(),
        "the trip saw an atomic restore: the boundary moved"
    );
    let before = ctl.metrics.gc.delete_rpc.snapshot().count;
    holds.deletes.add_permits(1_000);
    deletes_done(&ctl, before, 6).await;
    let lost = shards_at_backup - c.total_shards().await;
    assert_eq!(lost, 6, "every in-flight delete applied after the restore");
    // Detected afterwards: the health pass resumes, the lineage went back.
    holds.health.add_permits(1_000);
    let t = Instant::now();
    loop {
        let rec = delete_fence::current(c.meta.as_ref()).await.unwrap();
        if rec.operator_hold && !rec.open {
            assert!(rec.reason.contains("lineage"), "{rec:?}");
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "never held: {rec:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ctl.abort();
    c.shutdown().await;
}

/// A store that can be cut off from its process (every operation fails as
/// a lost connection would), sharing the data with everyone else.
struct Cut {
    inner: Arc<arca_lake_metadata::mem::MemKv>,
    cut: Arc<std::sync::atomic::AtomicBool>,
}

struct CutTx {
    inner: <arca_lake_metadata::mem::MemKv as KvStore>::Tx,
    cut: Arc<std::sync::atomic::AtomicBool>,
}

impl CutTx {
    fn check(&self) -> Result<(), arca_lake_metadata::KvError> {
        if self.cut.load(std::sync::atomic::Ordering::SeqCst) {
            Err(arca_lake_metadata::KvError::Backend(
                "injected: connection lost".into(),
            ))
        } else {
            Ok(())
        }
    }
}

impl KvStore for Cut {
    type Tx = CutTx;

    async fn begin(&self) -> Result<CutTx, arca_lake_metadata::KvError> {
        let tx = CutTx {
            inner: self.inner.begin().await?,
            cut: Arc::clone(&self.cut),
        };
        tx.check()?;
        Ok(tx)
    }

    fn limits(&self) -> arca_lake_metadata::kv::Limits {
        self.inner.limits()
    }
}

impl Txn for CutTx {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, arca_lake_metadata::KvError> {
        self.check()?;
        self.inner.get(key).await
    }

    async fn get_range(
        &mut self,
        range: &arca_lake_metadata::kv::KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, arca_lake_metadata::KvError> {
        self.check()?;
        self.inner.get_range(range, limit, reverse).await
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.inner.set(key, value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.inner.clear(key);
    }

    fn clear_range(&mut self, range: &arca_lake_metadata::kv::KeyRange) {
        self.inner.clear_range(range);
    }

    async fn commit(self) -> Result<u64, arca_lake_metadata::KvError> {
        self.check()?;
        self.inner.commit().await
    }
}

/// R-94(b) BOUNDARY (Phase 3.11, ADR-0025; pins a residual): the leader is
/// cut off from FoundationDB with deletes in flight, and the operator
/// restores without the runbook. The leader sees only transient errors: it
/// stops sending (its first failed renewal) but cannot close anything, and
/// no one else can tell the nodes. Deletes in flight that land while the
/// nodes' rejoin lease still runs apply; those landing after it ended are
/// refused. The exposure is the rejoin lease (`--fence-lease-secs`, 30 s
/// by default; 3 s here).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boundary_a_leader_cut_off_from_the_store_applies_deletes_until_the_rejoin_lease_ends() {
    use std::sync::atomic::Ordering;
    let mut c = cluster().await;
    c.fence_lease = Duration::from_secs(3);
    for i in 0..c.nodes.len() {
        c.stop_node(i).await;
        c.start_node(i).await;
    }
    put(&c, "kept", 23).await;
    let holds = Holds::new();
    let cut = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let store = Arc::new(Cut {
        inner: Arc::clone(&c.meta),
        cut: Arc::clone(&cut),
    });
    let mut cfg = quiet_controller(holds.hook(false));
    cfg.lease = Duration::from_secs(6);
    let ctl = arca_lake_controller::process::spawn(store, cfg)
        .await
        .unwrap();
    term_open_everywhere(&c, &ctl).await;
    holds.armed.store(true, Ordering::SeqCst);
    let backup = snapshot(c.meta.as_ref()).await;
    let shards_at_backup = c.total_shards().await;
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    let t = Instant::now();
    while holds.held.load(Ordering::SeqCst) < 6 {
        assert!(t.elapsed() < Duration::from_secs(20), "GC never sent");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Cut off; the operator clears and restores.
    cut.store(true, Ordering::SeqCst);
    let cut_at = Instant::now();
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.clear_range(&keys::all_range());
            Ok(())
        })
    })
    .await
    .unwrap();
    restore(c.meta.as_ref(), &backup).await;
    // Half of the in-flight deletes land within the lease ...
    let before = ctl.metrics.gc.delete_rpc.snapshot().count;
    holds.deletes.add_permits(3);
    deletes_done(&ctl, before, 3).await;
    assert!(
        cut_at.elapsed() < Duration::from_secs(2),
        "not within the lease"
    );
    let lost_in_lease = shards_at_backup - c.total_shards().await;
    // ... the rest after it ended.
    tokio::time::sleep(Duration::from_secs(4).saturating_sub(cut_at.elapsed())).await;
    let mut open = Vec::new();
    for n in &c.nodes {
        open.push(
            c.clients
                .health(&n.addr.to_string())
                .await
                .unwrap()
                .delete_fence_lease_ms,
        );
    }
    holds.deletes.add_permits(1_000);
    deletes_done(&ctl, before, 6).await;
    let lost_after = shards_at_backup - c.total_shards().await - lost_in_lease;
    eprintln!(
        "lost within the lease: {lost_in_lease}, after it: {lost_after}, lease left {open:?}"
    );
    assert_eq!(lost_in_lease, 3, "in-flight deletes within the lease apply");
    assert_eq!(lost_after, 0, "deletes after the lease ended are refused");
    assert!(
        !ctl.metrics.delete_fence.restore_suspected(),
        "a cut-off leader sees nothing"
    );
    ctl.abort();
    c.shutdown().await;
}
