//! Deferred reclamation (Phase 3.12, ADR-0028), in process with a short
//! retention: a delete retires a durable record, which stays readable; a
//! reclaim removes it only on an observation at least the retention after
//! the retirement, of metadata that no longer needs it.
//!
//! The R-94(b) order: an object backed up, then deleted and collected (the
//! leader's GC deletes delivered), then the metadata restored to the
//! backup. With deferral the restored object reads back whole; without it
//! (retention 0, the same steps) its shards are gone.

#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use std::sync::Arc;
use std::time::Duration;

use arca_lake_metadata::{Txn, keys, transact};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "deferred";
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

async fn put(c: &TestCluster, key: &str, seed: u64) -> Bytes {
    let d = Bytes::from(random_bytes(usize::try_from(MIB).unwrap(), seed));
    s3::put(&c.backend, BUCKET, key, d.clone(), &PutOptions::default())
        .await
        .unwrap();
    d
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

/// A restore of the metadata (as `fdbrestore` into a cleared store).
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

fn durable_shards(c: &TestCluster) -> usize {
    (0..c.nodes.len())
        .map(|i| c.node_store(i).keys().len())
        .sum()
}

/// The R-94(b) order, with and without deferral.
async fn restore_after_collection(retention: Duration) -> (bool, TestCluster) {
    let mut c = cluster(retention).await;
    let data = put(&c, "kept", 1).await;
    let backup = snapshot(&c).await;
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    // The leader's GC: its deletes delivered (decided on the metadata of
    // before the restore).
    let r = c.gc(now_ms() + 3_600_000, 0).await;
    assert!(r.collected >= 1, "{r:?}");
    restore(&c, &backup).await;
    c.restart_backend().await.unwrap();
    let readable = s3::get(&c.backend, BUCKET, "kept")
        .await
        .is_ok_and(|(got, _)| got == data);
    (readable, c)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_after_the_collection_finds_the_object_whole() {
    let (readable, c) = restore_after_collection(RETENTION).await;
    assert!(readable, "the restored object lost its shards");
    c.shutdown().await;
    // The same order without deferral: what R-94(b) loses.
    let (readable, c) = restore_after_collection(Duration::ZERO).await;
    assert!(!readable, "without deferral the shards were expected gone");
    c.shutdown().await;
}

/// After the restore: a reclaim decided before it (the old leader's view,
/// observed before the restore) is refused as younger than the retention;
/// a reclaim pass on the restored metadata keeps what it needs, after the
/// retention too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_reclaim_removes_what_the_restored_metadata_needs() {
    let (readable, mut c) = restore_after_collection(RETENTION).await;
    assert!(readable);
    let restored_at = now_ms();
    let before = durable_shards(&c);
    let mut stale = 0;
    for n in &c.nodes {
        let addr = n.addr.to_string();
        let (page, _, retention) = c.clients.list_pending(&addr, None, 0).await.unwrap();
        assert_eq!(retention, u64::try_from(RETENTION.as_millis()).unwrap());
        for (shard, token, _) in page {
            // As the old leader would: its observation is from before the
            // restore.
            c.sync_delete_fence().await;
            let r = c
                .clients
                .reclaim_shard(
                    &addr,
                    shard,
                    c.groups().await[0].epoch,
                    token,
                    restored_at - 1,
                )
                .await;
            assert!(
                r.as_ref()
                    .is_err_and(|e| e.message().contains("less than the retention")),
                "{r:?}"
            );
            stale += 1;
        }
    }
    assert!(stale >= 3, "retired records listed: {stale}");
    tokio::time::sleep(RETENTION + Duration::from_millis(500)).await;
    let r = c.reclaim().await;
    assert_eq!(r.reclaimed, 0, "{r:?}");
    assert!(r.needed >= 3, "{r:?}");
    assert_eq!(durable_shards(&c), before);
    c.restart_backend().await.unwrap();
    assert!(s3::get(&c.backend, BUCKET, "kept").await.is_ok());
    c.shutdown().await;
}

/// The normal path: collected shards are reclaimed once the retention has
/// passed, not before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collected_shards_are_reclaimed_after_the_retention() {
    let c = cluster(RETENTION).await;
    put(&c, "gone", 2).await;
    let full = durable_shards(&c);
    s3::delete(&c.backend, BUCKET, "gone").await.unwrap();
    c.gc(now_ms() + 3_600_000, 0).await;
    assert_eq!(durable_shards(&c), full, "retained, not removed");
    let early = c.reclaim().await;
    assert_eq!(early.reclaimed, 0, "{early:?}");
    tokio::time::sleep(RETENTION + Duration::from_millis(500)).await;
    let r = c.reclaim().await;
    assert!(r.reclaimed >= 3, "{r:?}");
    assert_eq!(
        durable_shards(&c),
        full - usize::try_from(r.reclaimed).unwrap()
    );
    c.shutdown().await;
}

/// A record written again after its retirement (a chunk moved back) is not
/// reclaimed; a restart replays the retirements; a damaged log keeps
/// everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rewrite_restart_and_damage() {
    use arca_lake_protocol::client::ShardRef;
    let mut c = cluster(RETENTION).await;
    put(&c, "o", 3).await;
    let g = c.groups().await.remove(0);
    let addr = c.nodes[0].addr.to_string();
    let page = c
        .clients
        .list_shards_page_with(&addr, g.id, None, 0, false)
        .await
        .unwrap();
    let shard: ShardRef = page.entries[0].0;
    let token = page.sequences[&shard];
    c.sync_delete_fence().await;
    c.clients
        .delete_shard_if(&addr, shard, g.epoch, token)
        .await
        .unwrap();
    // Restart: the retirement is replayed.
    c.stop_node(0).await;
    c.start_node(0).await;
    c.sync_delete_fence().await;
    let (pending, _, _) = c.clients.list_pending(&addr, None, 0).await.unwrap();
    assert_eq!(pending.len(), 1, "{pending:?}");
    // Written again (a new sequence): the old retirement is not reclaimable.
    let bytes = c.node_store(0).get(&shard_key(shard), None).unwrap();
    let new_token = c
        .clients
        .put_shard(&addr, shard, g.epoch, Bytes::from(bytes), 256 << 10)
        .await
        .unwrap();
    let (pending, _, _) = c.clients.list_pending(&addr, None, 0).await.unwrap();
    assert!(
        pending.is_empty(),
        "the write did not end the retirement: {pending:?}"
    );
    tokio::time::sleep(RETENTION + Duration::from_millis(300)).await;
    let r = c
        .clients
        .reclaim_shard(&addr, shard, g.epoch, token, now_ms())
        .await;
    assert!(r.is_err(), "a superseded retirement was reclaimed");
    assert!(c.node_store(0).contains(&shard_key(shard)));
    let _ = new_token;
    // Damage: nothing is reclaimed.
    c.clients.delete_shard(&addr, shard, g.epoch).await.unwrap();
    c.stop_node(0).await;
    std::fs::write(c.nodes[0].dir.join("pending-reclaim.log"), "p garbage\n").unwrap();
    c.start_node(0).await;
    c.sync_delete_fence().await;
    tokio::time::sleep(RETENTION + Duration::from_millis(300)).await;
    let h = c.clients.health(&addr).await.unwrap();
    assert!(h.reclaim_damaged, "{h:?}");
    let r = c.reclaim().await;
    assert_eq!(r.reclaimed, 0, "{r:?}");
    assert!(c.node_store(0).contains(&shard_key(shard)));
    c.shutdown().await;
}

fn shard_key(s: arca_lake_protocol::client::ShardRef) -> arca_lake_storage_node::ShardKey {
    arca_lake_storage_node::ShardKey {
        group_id: s.group_id,
        chunk_id: s.chunk_id,
        shard_index: s.shard_index,
    }
}

/// The worst R-94(b) case, a relocation: an object never deleted is
/// migrated to another group after the backup point, and reconciliation
/// removes its former placement. Restored metadata of the backup places it
/// in the former group. With deferral it reads back whole; without, it is
/// lost although nobody deleted it.
async fn restore_after_relocation(retention: Duration) -> bool {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 2,
        reclaim_retention: retention,
        ..ClusterOptions::rs(6, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let data = put(&c, "moved", 4).await;
    let backup = snapshot(&c).await;
    let m = c.manifest(BUCKET, "moved").await;
    let groups = c.groups().await;
    let source = groups
        .iter()
        .find(|g| g.id == m.chunks[0].1.placement.group_id)
        .unwrap()
        .clone();
    let target = groups.iter().find(|g| g.id != source.id).unwrap().clone();
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
    // Reconciliation removes the former placement (its deletes decided on
    // the metadata of before the restore).
    let deleted = c.reconcile(now_ms() + 3_600_000, 0).await;
    assert!(deleted >= 3, "{deleted}");
    restore(&c, &backup).await;
    c.restart_backend().await.unwrap();
    let readable = s3::get(&c.backend, BUCKET, "moved")
        .await
        .is_ok_and(|(got, _)| got == data);
    c.shutdown().await;
    readable
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_before_a_relocation_finds_the_object_whole() {
    assert!(
        restore_after_relocation(RETENTION).await,
        "the restored object lost its former placement"
    );
    assert!(
        !restore_after_relocation(Duration::ZERO).await,
        "without deferral the former placement was expected gone"
    );
}

/// A group written with the cache class is a cache group on the node:
/// its deletes remove at once. A durable write makes it durable for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_class_deletes_stay_immediate_durable_is_sticky() {
    use arca_lake_protocol::client::ShardRef;
    use arca_lake_protocol::storage::v1::WriteClass;
    let c = cluster(RETENTION).await;
    c.sync_delete_fence().await;
    let addr = c.nodes[0].addr.to_string();
    let shard = |i: u8| ShardRef {
        group_id: 77,
        chunk_id: [i; 16],
        shard_index: 0,
    };
    let write = |i: u8, class: WriteClass| {
        let clients = c.clients.clone();
        let addr = addr.clone();
        async move {
            clients
                .put_shard_as(
                    &addr,
                    shard(i),
                    0,
                    Bytes::from(vec![i; 4096]),
                    1 << 16,
                    class,
                )
                .await
                .unwrap();
        }
    };
    write(1, WriteClass::Cache).await;
    c.clients.delete_shard(&addr, shard(1), 0).await.unwrap();
    assert!(
        !c.node_store(0).contains(&shard_key(shard(1))),
        "a cache delete was retained"
    );
    write(2, WriteClass::Client).await;
    write(3, WriteClass::Cache).await;
    c.clients.delete_shard(&addr, shard(3), 0).await.unwrap();
    assert!(
        c.node_store(0).contains(&shard_key(shard(3))),
        "a group once durable must stay durable"
    );
    let (pending, _, _) = c.clients.list_pending(&addr, None, 0).await.unwrap();
    assert_eq!(pending.len(), 1, "{pending:?}");
    c.shutdown().await;
}

/// A reclaim carries a delete's checks: another identity, another
/// incarnation or a fence the node closed refuse it, whatever its age.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reclaims_are_refused_on_identity_incarnation_and_fence() {
    let c = cluster(RETENTION).await;
    put(&c, "a", 5).await;
    put(&c, "b", 6).await;
    let g = c.groups().await.remove(0);
    let addr = c.nodes[0].addr.to_string();
    let page = c
        .clients
        .list_shards_page_with(&addr, g.id, None, 0, false)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 2);
    c.sync_delete_fence().await;
    let mut retired = Vec::new();
    for (shard, _) in &page.entries {
        let token = page.sequences[shard];
        c.clients
            .delete_shard_if(&addr, *shard, g.epoch, token)
            .await
            .unwrap();
        retired.push((*shard, token));
    }
    tokio::time::sleep(RETENTION + Duration::from_millis(300)).await;
    let (shard, token) = retired[0];
    let reclaim = || {
        c.clients
            .reclaim_shard(&addr, shard, g.epoch, token, now_ms())
    };
    // Another disk's identity.
    let held = c.nodes[0].id;
    c.clients.note_identity(&addr, Some((held, [9; 16])));
    let r = reclaim().await;
    assert!(
        r.as_ref().is_err_and(|e| e.message().contains("identity")),
        "{r:?}"
    );
    c.sync_delete_fence().await;
    // Another incarnation.
    let inc = arca_lake_metadata::ops::compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .map(|s| s.incarnation);
    c.clients.set_incarnation(Some([7; 16]));
    let r = reclaim().await;
    assert!(r.is_err(), "a reclaim of another incarnation was applied");
    c.clients.set_incarnation(inc);
    // The positive control, on the other record.
    let (other, other_token) = retired[1];
    c.clients
        .reclaim_shard(&addr, other, g.epoch, other_token, now_ms())
        .await
        .unwrap();
    assert!(!c.node_store(0).contains(&shard_key(other)));
    // A fence the node closed: this process still believes the old one.
    let open = arca_lake_metadata::ops::delete_fence::current(c.meta.as_ref())
        .await
        .unwrap()
        .generation;
    arca_lake_controller::delete_fence::close_and_push(
        c.meta.as_ref(),
        &c.clients,
        "test",
        true,
        now_ms(),
    )
    .await
    .unwrap();
    c.clients.set_delete_fence(Some(open));
    let r = reclaim().await;
    assert!(r.is_err(), "a reclaim under a closed fence was applied");
    assert!(c.node_store(0).contains(&shard_key(shard)));
    c.shutdown().await;
}

/// A real controller's reclaim pass, across a controller restart: GC
/// retires, nothing is removed before the retention, the next leader
/// reclaims what the previous one retired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_controller_reclaims_after_the_retention_across_a_restart() {
    use arca_lake_controller::process::{ControllerConfig, spawn};
    let c = cluster(Duration::from_secs(3)).await;
    put(&c, "x", 7).await;
    let full = durable_shards(&c);
    let config = |id: &str| {
        let mut cfg = ControllerConfig::new(id);
        cfg.lease = Duration::from_secs(1);
        cfg.health_interval = Duration::from_millis(100);
        cfg.gc_interval = Duration::from_millis(100);
        cfg.gc_grace = Duration::ZERO;
        cfg.reconcile_interval = Duration::from_secs(3600);
        cfg.reclaim_interval = Duration::from_millis(200);
        cfg.failure_domain = "host".into();
        cfg
    };
    let ctl = spawn(Arc::clone(&c.meta), config("ctl-a")).await.unwrap();
    s3::delete(&c.backend, BUCKET, "x").await.unwrap();
    let pending = || async {
        let mut n = 0;
        for node in &c.nodes {
            n += c
                .clients
                .health(&node.addr.to_string())
                .await
                .unwrap()
                .reclaim_pending;
        }
        n
    };
    let t = std::time::Instant::now();
    while pending().await < 3 {
        assert!(t.elapsed() < Duration::from_secs(20), "GC did not retire");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let retired_by = t.elapsed();
    assert_eq!(durable_shards(&c), full, "retired, not removed");
    ctl.shutdown().await;
    let ctl = spawn(Arc::clone(&c.meta), config("ctl-b")).await.unwrap();
    while durable_shards(&c) > full - 3 {
        assert!(t.elapsed() < Duration::from_secs(40), "not reclaimed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        t.elapsed() >= retired_by + Duration::from_millis(2_500),
        "reclaimed before the retention: {:?} after the retirement",
        t.elapsed().saturating_sub(retired_by)
    );
    assert_eq!(pending().await, 0);
    ctl.shutdown().await;
    c.shutdown().await;
}

/// `recovery begin` on restored metadata (close with the hold, mark a new
/// incarnation, verify, finalize), then a leader's fence pass: deletes and
/// reclaims go on under the new incarnation.
async fn recover(c: &TestCluster, token: u64) {
    use arca_lake_controller::recovery::{RecoveryOptions, recovery_pass};
    use arca_lake_metadata::guard::{GuardedKv, OpClass};
    use arca_lake_metadata::ops::compat;
    let closed = arca_lake_controller::delete_fence::close_and_push(
        c.meta.as_ref(),
        &c.clients,
        "recovery begin",
        true,
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!(closed.not_acknowledged(), Vec::<u64>::new());
    let (seq, _) =
        arca_lake_controller::delete_fence::max_incarnation_seq(c.meta.as_ref(), &c.clients)
            .await
            .unwrap();
    let marked = compat::mark_restoring_above(c.meta.as_ref(), None, None, "test", now_ms(), seq)
        .await
        .unwrap();
    let admin = GuardedKv::new(
        std::sync::Arc::clone(&c.meta),
        marked.incarnation,
        OpClass::Admin,
    );
    for _ in 0..30 {
        if recovery_pass(
            &admin,
            &c.clients,
            &RecoveryOptions::default(),
            None,
            "test",
            now_ms(),
        )
        .await
        .unwrap()
        .finished
        {
            break;
        }
    }
    let (v, _) = arca_lake_metadata::ops::recovery::finalize(
        c.meta.as_ref(),
        &arca_lake_metadata::ops::recovery::FinalizeRequest {
            incarnation: marked.incarnation,
            operator: "test".into(),
            reason: "test restore".into(),
            force: true,
            now_ms: now_ms(),
        },
    )
    .await
    .unwrap();
    assert_eq!(v.refusal, None, "{v:?}");
    c.clients.set_incarnation(Some(marked.incarnation));
    let mut required = 0;
    for n in &c.nodes {
        if let Ok(h) = c.clients.health(&n.addr.to_string()).await {
            required = required.max(h.delete_fence_generation + u64::from(!h.delete_fence_open));
        }
    }
    let rec = arca_lake_metadata::ops::delete_fence::sync_for_term(
        c.meta.as_ref(),
        token,
        marked.incarnation,
        required,
        now_ms(),
    )
    .await
    .unwrap()
    .record;
    c.push_delete_fence(&rec).await;
}

/// Every chunk of `key` migrated to the other group (switched; the former
/// placement left to reconciliation).
async fn migrate_away(c: &TestCluster, key: &str) {
    let m = c.manifest(BUCKET, key).await;
    let groups = c.groups().await;
    let source = groups
        .iter()
        .find(|g| g.id == m.chunks[0].1.placement.group_id)
        .unwrap()
        .clone();
    let target = groups.iter().find(|g| g.id != source.id).unwrap().clone();
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
}

/// Consecutive restores (Phase 3.12 review of ADR-0028): `x`'s shards
/// retired in one history (deleted, collected); a restore brings `x` back
/// (needed again, the same records); in the restored history a backup is
/// taken, then `x` moves to the other group, and a reclaim pass runs after
/// the retention but before reconciliation deletes the former placement.
/// The first retirement is of the incarnation before the restore: it is not
/// reclaimable, and the second restore (of the restored history's backup)
/// finds `x` whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retirement_of_an_earlier_incarnation_waits_for_a_new_delete() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 2,
        reclaim_retention: RETENTION,
        ..ClusterOptions::rs(6, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let data = put(&c, "x", 9).await;
    let b1 = snapshot(&c).await;
    s3::delete(&c.backend, BUCKET, "x").await.unwrap();
    let r = c.gc(now_ms() + 3_600_000, 0).await;
    assert!(r.collected >= 1, "{r:?}");
    // The first restore, made current under a new incarnation.
    restore(&c, &b1).await;
    c.restart_backend().await.unwrap();
    recover(&c, 5000).await;
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "x").await.unwrap().0, data);
    // The restored history: a backup, then `x` moves away.
    let b2 = snapshot(&c).await;
    migrate_away(&c, "x").await;
    tokio::time::sleep(RETENTION + Duration::from_millis(500)).await;
    let r = c.reclaim().await;
    assert_eq!(
        r.reclaimed, 0,
        "a retirement of the earlier incarnation was reclaimed: {r:?}"
    );
    assert!(r.refused >= 3, "{r:?}");
    // The second restore: `x` in its former placement, whole.
    restore(&c, &b2).await;
    c.restart_backend().await.unwrap();
    assert_eq!(
        s3::get(&c.backend, BUCKET, "x").await.map(|(d, _)| d).ok(),
        Some(data)
    );
    c.shutdown().await;
}

/// A member moved away and back (Phase 3.12 review of ADR-0028 H4): node
/// A's shard of `x` is retired when A stops being a member; when A becomes
/// a member again its retired record counts as absent and the move writes
/// the shard again (a new sequence ends the retirement). After the
/// retention A moves away again: the reclaim pass does not remove the
/// shard that a backup taken while A was a member again needs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_moved_away_and_back_writes_its_shard_again() {
    let mut c = cluster(RETENTION).await;
    let data = put(&c, "x", 10).await;
    let g = c.groups().await.remove(0);
    let a = c.node_index(&g.members[0]).unwrap();
    let b = c.add_node().await;
    let far = || now_ms() + 3_600_000;
    let held_by_a = |c: &TestCluster| c.shard_count(a, g.id);
    assert_eq!(held_by_a(&c), 1);
    // A → B; A's copy retired by reconciliation.
    c.repair_with(g.id, 0, b, async |_| Ok(())).await.unwrap();
    c.reconcile(far(), 0).await;
    let addr_a = c.nodes[a].addr.to_string();
    let (pending, _, _) = c.clients.list_pending(&addr_a, None, 0).await.unwrap();
    assert_eq!(pending.len(), 1, "{pending:?}");
    // B → A: the retired record is not taken as present.
    let r = c.repair_with(g.id, 0, a, async |_| Ok(())).await.unwrap();
    assert_eq!(r.reconstructed, 1, "the shard was not written again: {r:?}");
    let (pending, _, _) = c.clients.list_pending(&addr_a, None, 0).await.unwrap();
    assert!(
        pending.is_empty(),
        "the write did not end the retirement: {pending:?}"
    );
    let backup = snapshot(&c).await;
    // After the retention, A → B again, reconciled, and a reclaim pass.
    tokio::time::sleep(RETENTION + Duration::from_millis(500)).await;
    c.repair_with(g.id, 0, b, async |_| Ok(())).await.unwrap();
    c.reconcile(far(), 0).await;
    let r = c.reclaim().await;
    assert_eq!(r.reclaimed, 0, "{r:?}");
    assert_eq!(
        held_by_a(&c),
        1,
        "A's shard was reclaimed on its first retirement's age"
    );
    // The backup of A as a member again: whole, from A among others.
    restore(&c, &backup).await;
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "x").await.unwrap().0, data);
    c.shutdown().await;
}

/// After a restore, the restored object's shards are retired on every
/// member (deleted after the backup point; Phase 3.12 re-review). A member
/// move then finds the chunk and rebuilds it on the new member from the
/// retired copies (they are still the other members' shards), and only
/// then completes. A reclaim pass finds the records needed and ends their
/// retirements.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_a_restore_a_member_move_rebuilds_from_retired_copies() {
    let mut c = cluster(RETENTION).await;
    let data = put(&c, "x", 11).await;
    let backup = snapshot(&c).await;
    s3::delete(&c.backend, BUCKET, "x").await.unwrap();
    let r = c.gc(now_ms() + 3_600_000, 0).await;
    assert!(r.collected >= 1, "{r:?}");
    restore(&c, &backup).await;
    c.restart_backend().await.unwrap();
    recover(&c, 6000).await;
    c.restart_backend().await.unwrap();
    let g = c.groups().await.remove(0);
    let spare = c.add_node().await;
    let r = c
        .repair_with(g.id, 0, spare, async |_| Ok(()))
        .await
        .unwrap();
    assert!(r.complete, "{r:?}");
    assert_eq!(
        r.reconstructed, 1,
        "the chunk was not rebuilt on the new member: {r:?}"
    );
    assert_eq!(c.shard_count(spare, g.id), 1);
    assert_eq!(s3::get(&c.backend, BUCKET, "x").await.unwrap().0, data);
    // Needed (judged once due): the retirements end.
    tokio::time::sleep(RETENTION + Duration::from_millis(500)).await;
    let r = c.reclaim().await;
    assert!(r.needed >= 2, "{r:?}");
    assert_eq!(r.reclaimed, 0, "{r:?}");
    for i in 0..c.nodes.len() {
        let (p, _, _) = c
            .clients
            .list_pending(&c.nodes[i].addr.to_string(), None, 0)
            .await
            .unwrap();
        let members: Vec<u64> = c.groups().await[0]
            .members
            .iter()
            .map(|m| m.node_id)
            .collect();
        if members.contains(&c.nodes[i].id) {
            assert!(
                p.is_empty(),
                "node {i}: a needed record stayed retired: {p:?}"
            );
        }
    }
    assert_eq!(s3::get(&c.backend, BUCKET, "x").await.unwrap().0, data);
    c.shutdown().await;
}
