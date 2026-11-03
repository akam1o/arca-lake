//! Cluster recovery state machine and incarnation fencing (RFC-015 §5.3–
//! §5.6, ADR-0016): what every phase admits, how a stale process is fenced
//! in metadata and on the nodes, epoch catch-up and verification after a
//! simulated restore, cursor resume, and leader failover during
//! verification.

#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use std::sync::Arc;
use std::time::Duration;

use arca_lake_controller::integrity::{RepairWorkerOptions, coders, repair_pass};
use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_controller::recovery::{RecoveryOptions, recovery_pass};
use arca_lake_controller::{gc as cgc, recovery as crecovery};
use arca_lake_metadata::guard::{GuardedKv, OpClass};
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::integrity::{self as itasks, Finding};
use arca_lake_metadata::ops::{cluster, compat, recovery};
use arca_lake_metadata::records::{ClusterPhase, ClusterStateRecord, GroupRecord};
use arca_lake_metadata::{KvError, KvStore, MetaError, Txn, keys, records, transact};
use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_protocol::storage::v1::HealthRequest;
use arca_lake_storage_node::ShardKey;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::S3ErrorCode;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "recovery";

async fn cluster(nodes: usize, k: u16, m: u16) -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(nodes, k, m, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

async fn put(c: &TestCluster, key: &str, seed: u64) -> Bytes {
    let data = payload(usize::try_from(MIB / 2).unwrap(), seed);
    s3::put(
        &c.backend,
        BUCKET,
        key,
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    data
}

async fn state(c: &TestCluster) -> ClusterStateRecord {
    compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap()
}

/// Flip the phase in metadata and in the gateway's cached copy at once
/// (the refresher would do the latter within 2 s).
async fn set_phase(c: &TestCluster, phase: ClusterPhase) -> ClusterStateRecord {
    let rec = compat::set_phase(c.meta.as_ref(), phase, "test", now_ms())
        .await
        .unwrap();
    c.backend.cluster().set(Some(rec.clone()));
    rec
}

fn is_unavailable(e: &s3s::S3Error) -> bool {
    *e.code() == S3ErrorCode::ServiceUnavailable
}

async fn group_of(c: &TestCluster, key: &str) -> GroupRecord {
    let id = c.manifest(BUCKET, key).await.chunks[0].1.placement.group_id;
    c.groups().await.into_iter().find(|g| g.id == id).unwrap()
}

fn member_node(c: &TestCluster, group: &GroupRecord, index: usize) -> usize {
    c.node_index(&group.members[index]).unwrap()
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn every_phase_admits_exactly_what_rfc_015_says() {
    let c = cluster(3, 2, 1).await;
    let data = put(&c, "obj", 1).await;
    let inc = state(&c).await.incarnation;
    assert_eq!(state(&c).await.state, ClusterPhase::Active);

    // ReadOnly: reads yes, client writes and deletes no, repair yes.
    set_phase(&c, ClusterPhase::ReadOnly).await;
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data);
    let e = s3::put(
        &c.backend,
        BUCKET,
        "new",
        payload(1024, 2),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(is_unavailable(&e), "{e}");
    assert!(e.to_string().contains("ReadOnly"), "{e}");
    let e = s3::delete(&c.backend, BUCKET, "obj").await.unwrap_err();
    assert!(is_unavailable(&e), "{e}");
    assert!(
        c.backend
            .cluster()
            .refused
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 2
    );
    // A delete-class transaction is refused at commit even when a caller
    // bypasses the admission check (the guard is the last line).
    let del = GuardedKv::new(Arc::clone(&c.meta), inc, OpClass::Delete);
    let gc = cgc::run_once(&del, &c.clients, now_ms() + 3_600_000, 0, 100).await;
    // Nothing is retired, so GC has nothing to write and succeeds vacuously;
    // force a write: expire a pending lease path by writing directly.
    let _ = gc;
    let mut tx = del.begin().await.unwrap();
    tx.set(b"zz-delete-probe", b"1");
    assert!(matches!(tx.commit().await, Err(KvError::ClusterState(_))));
    // Repair is admitted: corrupt a shard, queue and repair it.
    let group = group_of(&c, "obj").await;
    let chunk = c.manifest(BUCKET, "obj").await.chunks[0].1.clone();
    assert_eq!(
        c.corrupt_chunk_on_node(member_node(&c, &group, 0), group.id, chunk.chunk_id),
        1
    );
    let rep = GuardedKv::new(Arc::clone(&c.meta), inc, OpClass::Repair);
    assert!(
        itasks::enqueue(
            &rep,
            group.id,
            chunk.chunk_id,
            Finding::ChecksumMismatch,
            &[0],
            now_ms()
        )
        .await
        .unwrap()
    );
    let r = repair_pass(
        &rep,
        &c.clients,
        &coders(),
        &RepairWorkerOptions::default(),
        None,
        None,
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!((r.succeeded, r.failed), (1, 0), "{r:?}");

    // Restoring: nothing for clients, not even reads.
    set_phase(&c, ClusterPhase::Restoring).await;
    let e = s3::get(&c.backend, BUCKET, "obj").await.unwrap_err();
    assert!(
        is_unavailable(&e) && e.to_string().contains("Restoring"),
        "{e}"
    );
    let e = s3::head(&c.backend, BUCKET, "obj").await.unwrap_err();
    assert!(is_unavailable(&e), "{e}");
    // Repair writes are not admitted while restoring either.
    let mut tx = rep.begin().await.unwrap();
    tx.set(b"zz-repair-probe", b"1");
    assert!(matches!(tx.commit().await, Err(KvError::ClusterState(_))));

    // VerificationPending: reads yes (verified bytes or an integrity
    // error), writes no, deletes no.
    set_phase(&c, ClusterPhase::VerificationPending).await;
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data);
    assert!(is_unavailable(
        &s3::put(
            &c.backend,
            BUCKET,
            "new",
            payload(1024, 2),
            &PutOptions::default()
        )
        .await
        .unwrap_err()
    ));
    let mut tx = del.begin().await.unwrap();
    tx.clear(b"zz-delete-probe");
    assert!(matches!(tx.commit().await, Err(KvError::ClusterState(_))));

    // RecoveryRequired: like ReadOnly.
    set_phase(&c, ClusterPhase::RecoveryRequired).await;
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data);
    assert!(is_unavailable(
        &s3::delete(&c.backend, BUCKET, "obj").await.unwrap_err()
    ));
    let mut tx = rep.begin().await.unwrap();
    tx.set(b"zz-repair-probe", b"1");
    tx.commit().await.unwrap();

    // Active again: everything.
    set_phase(&c, ClusterPhase::Active).await;
    put(&c, "new", 3).await;
    s3::delete(&c.backend, BUCKET, "obj").await.unwrap();
    let mut tx = del.begin().await.unwrap();
    tx.clear(b"zz-delete-probe");
    tx.commit().await.unwrap();
    c.shutdown().await;
}

#[tokio::test]
async fn a_stale_incarnation_is_fenced_in_metadata_and_on_the_nodes() {
    let c = cluster(3, 2, 1).await;
    put(&c, "obj", 1).await;
    let old = state(&c).await;
    let group = group_of(&c, "obj").await;
    let chunk = c.manifest(BUCKET, "obj").await.chunks[0].1.clone();

    // Nodes adopt the current incarnation from the controller's Health.
    let push = |inc: [u8; 16], changed_ms: u64| HealthRequest {
        format_write: 0,
        incarnation: Bytes::copy_from_slice(&inc),
        incarnation_changed_ms: changed_ms,
    };
    for n in &c.nodes {
        let h = c
            .clients
            .health_with(&n.addr.to_string(), push(old.incarnation, old.changed_ms))
            .await
            .unwrap();
        assert_eq!(h.incarnation.as_ref(), &old.incarnation);
    }
    // Stale guarded wrappers (what every pre-restore process holds).
    let stale: Vec<(OpClass, GuardedKv<MemKv>)> = [
        OpClass::ClientWrite,
        OpClass::Delete,
        OpClass::Repair,
        OpClass::Admin,
    ]
    .into_iter()
    .map(|cls| {
        (
            cls,
            GuardedKv::new(Arc::clone(&c.meta), old.incarnation, cls),
        )
    })
    .collect();
    let stale_clients = NodeClients::new(Duration::from_secs(5));
    stale_clients.set_incarnation(Some(old.incarnation));

    // The restore mark: a fresh incarnation, Restoring.
    let fresh = compat::mark_restoring(c.meta.as_ref(), Some("drill".into()), "admin", now_ms())
        .await
        .unwrap();
    assert_ne!(fresh.incarnation, old.incarnation);
    assert_eq!(fresh.state, ClusterPhase::Restoring);
    // Even after the cluster is Active again, every stale wrapper is fenced
    // (the incarnation, not the phase, is what fences them).
    compat::set_phase(c.meta.as_ref(), ClusterPhase::Active, "admin", now_ms())
        .await
        .unwrap();
    for (cls, s) in &stale {
        let mut tx = s.begin().await.unwrap();
        tx.set(b"zz-stale", b"1");
        let e = tx.commit().await.unwrap_err();
        assert!(matches!(e, KvError::ClusterState(_)), "{cls:?}: {e}");
        assert!(
            e.to_string().contains("incarnation changed"),
            "{cls:?}: {e}"
        );
    }
    // The gateway (started under the old incarnation) refuses at admission
    // once its refresh sees the new record, and at commit regardless.
    c.backend.cluster().set(Some(state(&c).await));
    let e = s3::put(
        &c.backend,
        BUCKET,
        "x",
        payload(1024, 2),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(
        is_unavailable(&e) && e.to_string().contains("incarnation"),
        "{e}"
    );

    // Nodes: the new controller pushes the new incarnation; a delete under
    // the old one is refused, under the new one accepted; an older
    // `changed_ms` cannot push the old incarnation back.
    let n0 = member_node(&c, &group, 0);
    let addr = c.nodes[n0].addr.to_string();
    let cur = state(&c).await;
    c.clients
        .health_with(&addr, push(cur.incarnation, cur.changed_ms))
        .await
        .unwrap();
    let h = c
        .clients
        .health_with(&addr, push(old.incarnation, old.changed_ms))
        .await
        .unwrap();
    assert_eq!(
        h.incarnation.as_ref(),
        &cur.incarnation,
        "older push ignored"
    );
    let shard = ShardRef {
        group_id: group.id,
        chunk_id: chunk.chunk_id,
        shard_index: 0,
    };
    let e = stale_clients
        .delete_shard(&addr, shard, group.epoch)
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e}");
    assert!(e.message().starts_with("incarnation rejected"), "{e}");
    assert!(NodeClients::is_incompatible(&e));
    assert!(
        c.shard_keys(n0, group.id)
            .iter()
            .any(|k| k.chunk_id == chunk.chunk_id)
    );
    // A client that predates incarnations (none stamped) is accepted: the
    // metadata guard is its fence.
    c.clients.set_incarnation(None);
    // The node persisted what it adopted: a restart keeps refusing.
    let mut c = c;
    c.stop_node(n0).await;
    c.start_node(n0).await;
    let e = stale_clients
        .delete_shard(&addr, shard, group.epoch)
        .await
        .unwrap_err();
    assert!(e.message().starts_with("incarnation rejected"), "{e}");
    c.clients.set_incarnation(Some(cur.incarnation));
    c.clients
        .delete_shard(&addr, shard, group.epoch)
        .await
        .unwrap();
    c.shutdown().await;
}

/// Write a shard that no generation references, with a chunk id whose
/// UUIDv7 prefix carries `unix_ms`.
fn write_orphan(c: &TestCluster, node: usize, group: u64, unix_ms: u64, index: u16) -> ShardKey {
    let mut id = [0u8; 16];
    id[0..6].copy_from_slice(&unix_ms.to_be_bytes()[2..8]);
    id[6] = 0x70;
    id[8] = 0x80;
    id[9] = u8::try_from(index).unwrap();
    let key = ShardKey {
        group_id: group,
        chunk_id: id,
        shard_index: index,
    };
    c.node_store(node).put(key, &[7u8; 4096]).unwrap();
    key
}

#[tokio::test]
async fn recovery_catches_up_epochs_and_classifies_every_restore_finding() {
    let c = cluster(3, 2, 1).await;
    let mut objects = Vec::new();
    for i in 0..6u64 {
        objects.push((format!("obj{i}"), put(&c, &format!("obj{i}"), 10 + i).await));
    }
    let group = group_of(&c, "obj0").await;
    let chunk = |k: usize| c.manifest(BUCKET, &objects[k].0);
    let ch0 = chunk(0).await.chunks[0].1.clone();
    let ch1 = chunk(1).await.chunks[0].1.clone();
    let ch2 = chunk(2).await.chunks[0].1.clone();
    let ch3 = chunk(3).await.chunks[0].1.clone();
    let key = |ch: &records::ChunkRecord, i: u16| ShardKey {
        group_id: group.id,
        chunk_id: ch.chunk_id,
        shard_index: i,
    };
    // obj0: one shard missing (degraded, recoverable).
    c.node_store(member_node(&c, &group, 2))
        .delete(key(&ch0, 2))
        .unwrap();
    // obj1: two shards missing (unreadable).
    c.node_store(member_node(&c, &group, 0))
        .delete(key(&ch1, 0))
        .unwrap();
    c.node_store(member_node(&c, &group, 1))
        .delete(key(&ch1, 1))
        .unwrap();
    // obj2: a corrupt shard (quarantined by the deep verification).
    assert_eq!(
        c.corrupt_chunk_on_node(member_node(&c, &group, 1), group.id, ch2.chunk_id),
        1
    );
    // obj3: placement epoch ahead of the group (metadata from the future).
    {
        let g = c.manifest(BUCKET, "obj3").await.pointer.generation;
        let mut rec = ch3.clone();
        rec.placement.epoch = group.epoch + 50;
        transact(c.meta.as_ref(), (g, rec), |tx, env| {
            Box::pin(async move {
                let (g, rec) = env;
                tx.set(&keys::gen_chunk(g, 0), &records::encode(rec)?);
                Ok(())
            })
        })
        .await
        .unwrap();
    }
    // A node enforces an epoch ahead of the group (writes happened after
    // the backup point that the restored metadata does not know).
    let ahead = member_node(&c, &group, 0);
    c.clients
        .update_epoch(&c.nodes[ahead].addr.to_string(), group.id, group.epoch + 3)
        .await
        .unwrap();
    // Orphans: one newer than the restore point, one older.
    let restore_point = now_ms();
    let _pre = write_orphan(
        &c,
        member_node(&c, &group, 1),
        group.id,
        restore_point - 60_000,
        1,
    );
    let _post = write_orphan(
        &c,
        member_node(&c, &group, 1),
        group.id,
        restore_point + 60_000,
        1,
    );
    // A shard of this group on a node that is not a member (former member).
    let mut c = c;
    let stranger = c.add_node().await;
    c.node_store(stranger)
        .put(key(&ch0, 0), &[1u8; 64])
        .unwrap();
    let total_before = c.total_shards().await;

    // The restore mark, then the leader's recovery passes.
    let marked = compat::mark_restoring(
        c.meta.as_ref(),
        Some("drill".into()),
        "admin",
        restore_point,
    )
    .await
    .unwrap();
    c.backend.cluster().set(Some(marked.clone()));
    c.clients.set_incarnation(Some(marked.incarnation));
    let admin = GuardedKv::new(Arc::clone(&c.meta), marked.incarnation, OpClass::Admin);
    let opts = RecoveryOptions {
        page: 4,
        page_bytes: 0,
        pages_per_pass: 1000,
    };
    let r1 = recovery_pass(&admin, &c.clients, &opts, None, "ctl", now_ms())
        .await
        .unwrap();
    assert_eq!(r1.phase_before, Some(ClusterPhase::Restoring));
    assert_eq!(r1.phase_after, Some(ClusterPhase::VerificationPending));
    assert_eq!(r1.epochs_raised, 1);
    let g2 = cluster::get_group(c.meta.as_ref(), group.id).await.unwrap();
    assert_eq!(g2.epoch, group.epoch + 4, "max(member epochs) + 1");
    for i in 0..3 {
        let h = c
            .clients
            .health(&c.nodes[member_node(&c, &group, i)].addr.to_string())
            .await
            .unwrap();
        let e = h
            .group_epochs
            .iter()
            .find(|x| x.group_id == group.id)
            .unwrap()
            .epoch;
        assert_eq!(e, g2.epoch, "member {i} fenced");
    }
    // Verification (in one pass here).
    let r2 = recovery_pass(&admin, &c.clients, &opts, None, "ctl", now_ms())
        .await
        .unwrap();
    assert!(r2.finished, "{r2:?}");
    assert_eq!(
        r2.phase_after,
        Some(ClusterPhase::RecoveryRequired),
        "unreadable data"
    );
    let rep = recovery::report(c.meta.as_ref(), &marked.incarnation)
        .await
        .unwrap()
        .unwrap();
    assert!(rep.complete, "{rep:?}");
    let k = &rep.counts;
    // obj0 (one shard missing) and obj2 (its corrupt shard was quarantined
    // by the deep verification, so the presence walk no longer sees it)
    // are degraded; obj1 is unreadable. Counts are per shard.
    assert_eq!(k.degraded_chunks, 2, "{k:?}");
    assert_eq!(k.unreadable_chunks, 1, "{k:?}");
    assert_eq!(k.missing, 4, "{k:?}");
    assert_eq!(k.corrupt, 1, "{k:?}");
    assert_eq!(
        k.placement_epoch_mismatch, 3,
        "one per shard of obj3: {k:?}"
    );
    assert_eq!(k.orphan_post_t, 1, "{k:?}");
    assert_eq!(k.orphan_pre_t, 1, "{k:?}");
    assert_eq!(k.former_member, 1, "{k:?}");
    assert_eq!(k.membership_mismatch, 1, "{k:?}");
    assert_eq!(k.epochs_raised, 1);
    assert_eq!(k.manifest_findings, 0);
    assert_eq!(rep.affected.len(), 1, "{:?}", rep.affected);
    let a = &rep.affected[0];
    assert_eq!(
        (a.bucket.as_str(), a.key.as_str(), a.why.as_str()),
        (BUCKET, "obj1", "unreadable")
    );
    assert_eq!((a.have, a.need), (1, 2));
    // Nothing was deleted (I6): the orphans, the former-member copy and the
    // corrupt (quarantined, bytes kept) shard are all still there.
    assert_eq!(
        c.total_shards().await,
        total_before - 1,
        "only the quarantined shard left the live index"
    );
    // Repair tasks queued for the degraded and the unreadable stripe.
    let tasks = itasks::tasks(c.meta.as_ref(), usize::MAX).await.unwrap();
    assert_eq!(tasks.len(), 3, "{tasks:?}");
    // The gateway that started before the restore is fenced out entirely
    // (its incarnation is stale); a restarted one serves: the unreadable
    // error for obj1, bytes for the rest.
    c.backend.cluster().set(Some(state(&c).await));
    let e = s3::get(&c.backend, BUCKET, "obj0").await.unwrap_err();
    assert!(
        is_unavailable(&e) && e.to_string().contains("incarnation"),
        "{e}"
    );
    c.restart_backend().await.unwrap();
    assert_eq!(c.backend.cluster().incarnation, marked.incarnation);
    let e = s3::get(&c.backend, BUCKET, "obj1").await.unwrap_err();
    assert!(e.to_string().contains("unreadable"), "{e}");
    for (key, data) in objects.iter().filter(|(k, _)| k != "obj1") {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    // The degraded stripe is repaired while RecoveryRequired (repair is
    // admitted), the unreadable one is recorded, not written.
    let rep_store = GuardedKv::new(Arc::clone(&c.meta), marked.incarnation, OpClass::Repair);
    let r = repair_pass(
        &rep_store,
        &c.clients,
        &coders(),
        &RepairWorkerOptions::default(),
        None,
        None,
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!((r.succeeded, r.unrecoverable, r.failed), (2, 1, 0), "{r:?}");
    // Object-level resolution of the unrecoverable chunk (R-54).
    let resolved = crecovery::resolve_chunk(c.meta.as_ref(), &ch1.chunk_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            resolved.bucket.as_str(),
            resolved.key.as_str(),
            resolved.chunk_index
        ),
        (BUCKET, "obj1", 0)
    );
    c.shutdown().await;
}

#[tokio::test]
async fn verification_resumes_from_its_cursor_and_survives_a_leader_failover() {
    let c = cluster(3, 2, 1).await;
    for i in 0..12u64 {
        put(&c, &format!("obj{i}"), 100 + i).await;
    }
    let group = group_of(&c, "obj0").await;
    let ch = c.manifest(BUCKET, "obj5").await.chunks[0].1.clone();
    c.node_store(member_node(&c, &group, 2))
        .delete(ShardKey {
            group_id: group.id,
            chunk_id: ch.chunk_id,
            shard_index: 2,
        })
        .unwrap();
    let marked = compat::mark_restoring(c.meta.as_ref(), None, "admin", now_ms())
        .await
        .unwrap();
    c.backend.cluster().set(Some(marked.clone()));
    let admin = GuardedKv::new(Arc::clone(&c.meta), marked.incarnation, OpClass::Admin);
    // One page per pass: the cursor moves one step at a time; "crashing"
    // between passes is just not calling it. Every pass re-reads the cursor.
    let opts = RecoveryOptions {
        page: 2,
        page_bytes: 0,
        pages_per_pass: 1,
    };
    let mut passes = 0;
    let mut phases = Vec::new();
    loop {
        let r = recovery_pass(&admin, &c.clients, &opts, None, "ctl", now_ms())
            .await
            .unwrap();
        passes += 1;
        let cur = recovery::cursor(c.meta.as_ref()).await.unwrap().unwrap();
        phases.push(format!("{:?}", cur.phase));
        if r.finished || passes > 500 {
            break;
        }
    }
    assert!(passes > 10, "the pass was split into pages: {passes}");
    assert_eq!(
        phases
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        phases.len(),
        "every pass advanced the cursor: {phases:?}"
    );
    let rep = recovery::report(c.meta.as_ref(), &marked.incarnation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rep.counts.degraded_chunks, 1, "{:?}", rep.counts);
    assert_eq!(rep.counts.matched, 12 * 3 - 1);
    assert!(rep.complete && rep.finished_ms > 0);
    assert_eq!(
        state(&c).await.state,
        ClusterPhase::ReadOnly,
        "clean and complete"
    );

    // Again with real controller processes and a leader failover in the
    // middle: `recovery verify` re-runs from the start.
    let fresh = recovery::RestoreCursor {
        incarnation: marked.incarnation,
        phase: recovery::RecoveryPhase::Shards {
            group: 0,
            member: 0,
            after: None,
        },
        restore_point_ms: marked.restored_at_ms.unwrap(),
        started_ms: now_ms(),
        updated_ms: now_ms(),
        unanswered_nodes: Vec::new(),
        excluded_nodes: Vec::new(),
    };
    recovery::set_cursor(c.meta.as_ref(), &fresh).await.unwrap();
    let mut blank = rep.clone();
    blank.finished_ms = 0;
    blank.counts = recovery::RestoreCounts::default();
    blank.affected.clear();
    recovery::set_report(c.meta.as_ref(), &blank).await.unwrap();
    compat::set_phase(
        c.meta.as_ref(),
        ClusterPhase::VerificationPending,
        "admin",
        now_ms(),
    )
    .await
    .unwrap();
    let mut cfg = ControllerConfig::new("a");
    cfg.lease = Duration::from_millis(600);
    cfg.gc_interval = Duration::from_secs(3600);
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.repair_interval = Duration::from_secs(3600);
    cfg.stripe_repair_interval = Duration::from_secs(3600);
    cfg.health_interval = Duration::from_millis(200);
    cfg.scrub.interval_ms = 0;
    cfg.recovery_interval = Duration::from_millis(30);
    cfg.recovery = RecoveryOptions {
        page: 2,
        page_bytes: 0,
        pages_per_pass: 1,
    };
    cfg.heartbeat = Some(Duration::from_millis(200));
    let mut cfg_b = cfg.clone();
    cfg_b.id = "b".into();
    let a = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let b = spawn(Arc::clone(&c.meta), cfg_b).await.unwrap();
    wait_until("a leader", Duration::from_secs(5), async || {
        a.is_leader() || b.is_leader()
    })
    .await;
    let meta = Arc::clone(&c.meta);
    wait_until("progress", Duration::from_secs(10), async || {
        matches!(
            recovery::cursor(meta.as_ref())
                .await
                .unwrap()
                .map(|c| c.phase),
            Some(
                recovery::RecoveryPhase::Shards { after: Some(_), .. }
                    | recovery::RecoveryPhase::Presence { .. }
            )
        )
    })
    .await;
    let (old, new) = if a.is_leader() { (a, b) } else { (b, a) };
    let before = recovery::cursor(c.meta.as_ref()).await.unwrap().unwrap();
    old.shutdown().await;
    wait_until("the new leader", Duration::from_secs(5), async || {
        new.is_leader()
    })
    .await;
    wait_until(
        "verification by the new leader",
        Duration::from_secs(30),
        async || {
            !matches!(
                compat::cluster_state(meta.as_ref())
                    .await
                    .unwrap()
                    .map(|s| s.state),
                Some(ClusterPhase::VerificationPending)
            )
        },
    )
    .await;
    let after = recovery::cursor(c.meta.as_ref()).await.unwrap().unwrap();
    assert_eq!(after.phase, recovery::RecoveryPhase::Done);
    assert_ne!(
        before.phase,
        recovery::RecoveryPhase::Done,
        "the failover happened mid-pass"
    );
    let rep2 = recovery::report(c.meta.as_ref(), &marked.incarnation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rep2.counts.degraded_chunks, 1);
    assert_eq!(
        rep2.counts.matched,
        12 * 3 - 1,
        "resumed, not restarted or doubled"
    );
    assert_eq!(state(&c).await.state, ClusterPhase::ReadOnly);
    // Heartbeats: both controllers wrote theirs; the stopped one removed it.
    let procs = compat::procs(c.meta.as_ref()).await.unwrap();
    assert_eq!(procs.len(), 1, "{procs:?}");
    assert_eq!(procs[0].incarnation, marked.incarnation);
    new.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn startup_gate_refuses_newer_schemas_and_stamps_legacy_stores() {
    use arca_lake_common::compat::Capabilities;
    let caps = |min: u64, max: u64| Capabilities {
        protocol_min: 1,
        protocol_max: 1,
        schema_min: min,
        schema_max: max,
        format_min: 1,
        format_max: 1,
    };
    // Empty store: initialised at the caller's versions by anyone.
    let empty = Arc::new(MemKv::default());
    let g = compat::check_startup(
        empty.as_ref(),
        &caps(1, 2),
        compat::Stamp::FreshOnly,
        "gw",
        1,
    )
    .await
    .unwrap();
    assert_eq!(
        (g.schema, g.schema_write, g.format_write),
        (Some(2), Some(2), Some(1))
    );
    assert_eq!(g.cluster.unwrap().state, ClusterPhase::Active);
    // A newer schema than this build reads: refused (the downgrade refusal,
    // RFC-014 §5.6); an older one within the window: accepted.
    for (stored, min, max, ok) in [
        (3, 1, 2, false),
        (2, 2, 3, true),
        (1, 2, 3, false),
        (2, 1, 2, true),
    ] {
        let s = Arc::new(MemKv::default());
        transact(s.as_ref(), stored, |tx, v: &u64| {
            Box::pin(async move {
                tx.set(&keys::schema_version(), &v.to_le_bytes());
                Ok(())
            })
        })
        .await
        .unwrap();
        let r = compat::check_startup(s.as_ref(), &caps(min, max), compat::Stamp::Legacy, "ctl", 1)
            .await;
        assert_eq!(r.is_ok(), ok, "stored {stored} caps [{min},{max}]: {r:?}");
        if !ok {
            let e = r.unwrap_err().to_string();
            assert!(e.contains(&format!("metadata schema M{stored}")), "{e}");
        }
    }
    // Data but no key: a gateway refuses, a controller stamps the legacy
    // versions (2 / 2 / 1) and a fresh incarnation.
    let legacy = Arc::new(MemKv::default());
    arca_lake_metadata::ops::bucket::create_bucket(
        legacy.as_ref(),
        "b",
        records::Versioning::Disabled,
        1,
    )
    .await
    .unwrap();
    let e = compat::check_startup(
        legacy.as_ref(),
        &caps(1, 2),
        compat::Stamp::FreshOnly,
        "gw",
        1,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(e, MetaError::InvalidArgument(ref m) if m.contains("admin init")),
        "{e}"
    );
    let g = compat::check_startup(
        legacy.as_ref(),
        &caps(1, 2),
        compat::Stamp::Legacy,
        "ctl",
        1,
    )
    .await
    .unwrap();
    assert_eq!(
        (g.schema, g.schema_write, g.format_write),
        (Some(2), Some(2), Some(1))
    );
    // Idempotent: a second call changes nothing.
    let g2 = compat::check_startup(
        legacy.as_ref(),
        &caps(1, 2),
        compat::Stamp::Legacy,
        "ctl",
        2,
    )
    .await
    .unwrap();
    assert_eq!(g, g2);
    // A build that cannot read M2 refuses even to stamp.
    let legacy2 = Arc::new(MemKv::default());
    arca_lake_metadata::ops::bucket::create_bucket(
        legacy2.as_ref(),
        "b",
        records::Versioning::Disabled,
        1,
    )
    .await
    .unwrap();
    assert!(
        compat::check_startup(
            legacy2.as_ref(),
            &caps(3, 4),
            compat::Stamp::Legacy,
            "ctl",
            1
        )
        .await
        .is_err()
    );

    // Record encoding follows schema_write, clamped to what this build
    // writes; a record of a newer version is refused by decode.
    records::set_schema_write(1);
    assert_eq!(records::schema_write_version(), records::RECORD_WRITE_MIN);
    records::set_schema_write(99);
    assert_eq!(records::schema_write_version(), records::RECORD_VERSION);
    records::set_schema_write(2);
    let b = records::encode(&records::NodeRecord {
        id: 1,
        address: "a".into(),
        labels: vec![],
        devices: vec![],
        registered_ms: 0,
        last_seen_ms: 0,
    })
    .unwrap();
    assert_eq!(b[0], 2);
    let mut newer = b.clone();
    newer[0] = records::RECORD_VERSION + 1;
    let e = records::decode::<records::NodeRecord>(&newer).unwrap_err();
    assert!(e.to_string().contains("unknown record version"), "{e}");
}
