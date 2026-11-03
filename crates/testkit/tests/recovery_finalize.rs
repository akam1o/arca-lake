//! Recovery finalize safety (RFC-015 §5.3, Phase 3.4 §3): what finalize
//! refuses even with `--force`, what `--force` needs and records, that
//! accepted losses stay visible, that finalize is one transaction (commit
//! faults, concurrent verification restarts), and that GC, repair and
//! tiering after a forced finalize never delete a shard still needed.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_controller::recovery::{RecoveryOptions, recovery_pass};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::fault::{CommitFault, FaultKv};
use arca_lake_metadata::guard::{GuardedKv, OpClass};
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::integrity::{self as itasks, Finding};
use arca_lake_metadata::ops::recovery::{self, FinalizeRequest};
use arca_lake_metadata::ops::{cluster, compat};
use arca_lake_metadata::records::ClusterPhase;
use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_storage_node::ShardKey;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "finalize";

async fn cluster<S: KvStore + 'static>(meta: Arc<S>) -> TestCluster<S> {
    let c = TestCluster::start_with(
        meta,
        ClusterOptions {
            groups: 1,
            ..ClusterOptions::rs(3, 2, 1, MIB, 0)
        },
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

async fn put<S: KvStore + 'static>(c: &TestCluster<S>, key: &str, seed: u64) -> Bytes {
    let data = Bytes::from(random_bytes(usize::try_from(MIB / 2).unwrap(), seed));
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

fn req(inc: [u8; 16], force: bool, reason: &str) -> FinalizeRequest {
    FinalizeRequest {
        incarnation: inc,
        operator: "op-test".into(),
        reason: reason.into(),
        force,
        now_ms: now_ms(),
    }
}

async fn phase<S: KvStore>(meta: &S) -> ClusterPhase {
    compat::cluster_state(meta).await.unwrap().unwrap().state
}

/// Mark a restore and run recovery passes until verification finished.
async fn restore_and_verify<S: KvStore + 'static>(c: &TestCluster<S>) -> [u8; 16] {
    let marked = compat::mark_restoring(c.meta.as_ref(), None, None, "admin", now_ms())
        .await
        .unwrap();
    let admin = GuardedKv::new(Arc::clone(&c.meta), marked.incarnation, OpClass::Admin);
    for _ in 0..20 {
        let r = recovery_pass(
            &admin,
            &c.clients,
            &RecoveryOptions::default(),
            None,
            "ctl",
            now_ms(),
        )
        .await
        .unwrap();
        if r.finished {
            break;
        }
    }
    assert!(matches!(
        phase(c.meta.as_ref()).await,
        ClusterPhase::ReadOnly | ClusterPhase::RecoveryRequired
    ));
    marked.incarnation
}

/// Remove two of three shards of `key`'s first chunk: unreadable.
async fn lose<S: KvStore + 'static>(c: &TestCluster<S>, key: &str) -> (u64, [u8; 16]) {
    let ch = c.manifest(BUCKET, key).await.chunks[0].1.clone();
    let group = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == ch.placement.group_id)
        .unwrap();
    for i in 0..2u16 {
        c.node_store(c.node_index(&group.members[usize::from(i)]).unwrap())
            .delete(ShardKey {
                group_id: group.id,
                chunk_id: ch.chunk_id,
                shard_index: i,
            })
            .unwrap();
    }
    (group.id, ch.chunk_id)
}

#[tokio::test]
async fn a_clean_verification_finalizes_and_nothing_bypasses_finalize() {
    let c = cluster(Arc::new(MemKv::default())).await;
    let data = put(&c, "obj", 1).await;
    let inc = restore_and_verify(&c).await;
    assert_eq!(phase(c.meta.as_ref()).await, ClusterPhase::ReadOnly);
    // `cluster activate` does not leave a restore around finalize.
    let r = recovery::activate_read_only(c.meta.as_ref(), "admin", now_ms())
        .await
        .unwrap();
    assert!(r.is_err(), "{r:?}");
    assert_eq!(phase(c.meta.as_ref()).await, ClusterPhase::ReadOnly);
    // An operator is required.
    let mut anon = req(inc, false, "");
    anon.operator = String::new();
    let (v, _) = recovery::finalize(c.meta.as_ref(), &anon).await.unwrap();
    assert!(v.refusal.is_some());
    // VerificationPending → (pass) → ReadOnly → Active.
    let (v, audit) = recovery::finalize(c.meta.as_ref(), &req(inc, false, ""))
        .await
        .unwrap();
    assert_eq!(v.refusal, None, "{v:?}");
    assert!(!v.forced && v.critical.is_empty());
    assert_eq!(audit.result, "activated");
    assert_eq!(phase(c.meta.as_ref()).await, ClusterPhase::Active);
    let rep = recovery::report(c.meta.as_ref(), &inc)
        .await
        .unwrap()
        .unwrap();
    let acc = rep.accepted.unwrap();
    assert_eq!((acc.by.as_str(), acc.forced), ("op-test", false));
    let audits = recovery::finalize_audits(c.meta.as_ref()).await.unwrap();
    assert_eq!(audits.len(), 2, "the refused attempt is audited too");
    assert!(audits[0].result.starts_with("refused"));
    // A second finalize is refused (and audited) and changes nothing.
    let (v, _) = recovery::finalize(c.meta.as_ref(), &req(inc, true, "again"))
        .await
        .unwrap();
    assert!(v.refusal.unwrap().contains("Active"));
    // The operator's own read-only switch keeps working after finalize.
    compat::transition(
        c.meta.as_ref(),
        ClusterPhase::Active,
        ClusterPhase::ReadOnly,
        "admin",
        now_ms(),
    )
    .await
    .unwrap();
    assert!(
        recovery::activate_read_only(c.meta.as_ref(), "admin", now_ms())
            .await
            .unwrap()
            .is_ok()
    );
    // A gateway restarted right after finalize serves the new incarnation.
    let mut c = c;
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "obj").await.unwrap().0, data);
    put(&c, "after", 2).await;
    c.shutdown().await;
}

#[tokio::test]
async fn unverified_or_incomplete_verification_is_refused_even_with_force() {
    let mut c = cluster(Arc::new(MemKv::default())).await;
    for i in 0..3 {
        put(&c, &format!("k{i}"), i).await;
    }
    let marked = compat::mark_restoring(c.meta.as_ref(), None, None, "admin", now_ms())
        .await
        .unwrap();
    let inc = marked.incarnation;
    let forced = req(inc, true, "accept anything");
    // Restoring: no report yet.
    let (v, _) = recovery::finalize(c.meta.as_ref(), &forced).await.unwrap();
    assert!(v.refusal.unwrap().contains("Restoring"));
    let admin = GuardedKv::new(Arc::clone(&c.meta), inc, OpClass::Admin);
    let opts = RecoveryOptions::default();
    recovery_pass(&admin, &c.clients, &opts, None, "ctl", now_ms())
        .await
        .unwrap();
    assert_eq!(
        phase(c.meta.as_ref()).await,
        ClusterPhase::VerificationPending
    );
    let (v, _) = recovery::finalize(c.meta.as_ref(), &forced).await.unwrap();
    assert!(v.refusal.unwrap().contains("VerificationPending"));
    // Someone flips the phase to RecoveryRequired while the cursor is
    // still mid-pass: the report is unfinished, force does not help.
    compat::set_phase(
        c.meta.as_ref(),
        ClusterPhase::RecoveryRequired,
        "test",
        now_ms(),
    )
    .await
    .unwrap();
    let (v, _) = recovery::finalize(c.meta.as_ref(), &forced).await.unwrap();
    assert!(v.refusal.unwrap().contains("has not finished"));
    // A node stopped during verification: the pass finishes incomplete
    // (RecoveryRequired); force is refused until it is verified.
    compat::set_phase(
        c.meta.as_ref(),
        ClusterPhase::VerificationPending,
        "test",
        now_ms(),
    )
    .await
    .unwrap();
    c.stop_node(0).await;
    for _ in 0..20 {
        if recovery_pass(&admin, &c.clients, &opts, None, "ctl", now_ms())
            .await
            .unwrap()
            .finished
        {
            break;
        }
    }
    assert_eq!(phase(c.meta.as_ref()).await, ClusterPhase::RecoveryRequired);
    let (v, _) = recovery::finalize(c.meta.as_ref(), &forced).await.unwrap();
    assert!(v.refusal.unwrap().contains("did not answer"));
    // Back, verified from the start, finalized normally.
    c.start_node(0).await;
    recovery::restart_verification(c.meta.as_ref(), inc, true, "admin", now_ms())
        .await
        .unwrap()
        .unwrap();
    for _ in 0..20 {
        if recovery_pass(&admin, &c.clients, &opts, None, "ctl", now_ms())
            .await
            .unwrap()
            .finished
        {
            break;
        }
    }
    let (v, _) = recovery::finalize(c.meta.as_ref(), &req(inc, false, ""))
        .await
        .unwrap();
    assert_eq!(v.refusal, None, "{v:?}");
    // Another incarnation (a newer restore mark) is refused.
    let (v, _) = recovery::finalize(c.meta.as_ref(), &req([9; 16], true, "x"))
        .await
        .unwrap();
    assert!(v.refusal.unwrap().contains("incarnation"));
    let audits = recovery::finalize_audits(c.meta.as_ref()).await.unwrap();
    assert_eq!(
        audits
            .iter()
            .filter(|a| a.result.starts_with("refused"))
            .count(),
        5
    );
    c.shutdown().await;
}

#[tokio::test]
async fn forced_finalize_needs_a_reason_keeps_every_finding_and_later_passes_keep_needed_shards() {
    let c = cluster(Arc::new(MemKv::default())).await;
    let good = put(&c, "good", 1).await;
    put(&c, "lost", 2).await;
    let (group, chunk) = lose(&c, "lost").await;
    let inc = restore_and_verify(&c).await;
    assert_eq!(phase(c.meta.as_ref()).await, ClusterPhase::RecoveryRequired);
    let before = recovery::report(c.meta.as_ref(), &inc)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.counts.unreadable_chunks, 1);

    // Without --force: refused, naming the finding.
    let (v, _) = recovery::finalize(c.meta.as_ref(), &req(inc, false, ""))
        .await
        .unwrap();
    let why = v.refusal.unwrap();
    assert!(why.contains("1 unreadable chunks"), "{why}");
    // --force without a reason: refused.
    let (v, _) = recovery::finalize(c.meta.as_ref(), &req(inc, true, " "))
        .await
        .unwrap();
    assert!(v.refusal.unwrap().contains("--reason"));
    assert_eq!(phase(c.meta.as_ref()).await, ClusterPhase::RecoveryRequired);
    // --force --reason: Active, recorded as forced.
    let (v, audit) = recovery::finalize(
        c.meta.as_ref(),
        &req(inc, true, "backup lost the newest object"),
    )
    .await
    .unwrap();
    assert_eq!(v.refusal, None);
    assert!(v.forced);
    assert_eq!(audit.result, "activated-forced");
    assert_eq!(audit.critical, vec!["1 unreadable chunks".to_string()]);
    assert_eq!(phase(c.meta.as_ref()).await, ClusterPhase::Active);
    // The finding is not erased: the report keeps its counts and objects.
    let after = recovery::report(c.meta.as_ref(), &inc)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.counts, before.counts);
    assert_eq!(after.affected, before.affected);
    assert!(
        after
            .accepted
            .as_ref()
            .is_some_and(|a| a.forced && a.by == "op-test")
    );
    // The loss is tracked persistently, per object version and chunk.
    let losses = recovery::data_losses(c.meta.as_ref()).await.unwrap();
    assert_eq!(losses.len(), 1);
    assert_eq!(
        (losses[0].object.key.as_str(), losses[0].object.why.as_str()),
        ("lost", "unreadable")
    );
    let unrecoverable_task = async |c: &TestCluster| {
        itasks::tasks(c.meta.as_ref(), 1000)
            .await
            .unwrap()
            .iter()
            .any(|(g, ch, t)| {
                *g == group && *ch == chunk && t.reason == Finding::UnrecoverableStripe
            })
    };
    assert!(
        unrecoverable_task(&c).await,
        "the unrecoverable stripe task stays"
    );
    // Reads (a gateway restarted on the new incarnation): the good object
    // reads, the lost one fails explicitly.
    let mut c = c;
    c.restart_backend().await.unwrap();
    c.backend
        .cluster()
        .set(compat::cluster_state(c.meta.as_ref()).await.unwrap());
    assert_eq!(s3::get(&c.backend, BUCKET, "good").await.unwrap().0, good);
    let e = s3::get(&c.backend, BUCKET, "lost").await.unwrap_err();
    assert!(e.to_string().contains("unreadable"), "{e}");

    // GC, reconciliation, repair and tiering passes after the finalize:
    // the remaining shard of the lost chunk and every shard of the good
    // object stay; nothing is fabricated for the lost chunk.
    let shards_of = |c: &TestCluster| -> usize {
        (0..c.nodes.len())
            .map(|i| c.shard_keys(i, group).len())
            .sum()
    };
    let total_before = shards_of(&c);
    let mut cfg = ControllerConfig::new("ctl-after");
    cfg.lease = Duration::from_millis(500);
    cfg.gc_interval = Duration::from_millis(100);
    cfg.gc_grace = Duration::ZERO;
    cfg.reconcile_interval = Duration::from_millis(200);
    cfg.min_orphan_age = Duration::ZERO;
    cfg.repair_interval = Duration::from_millis(100);
    cfg.stripe_repair_interval = Duration::from_millis(100);
    cfg.health_interval = Duration::from_millis(100);
    cfg.scrub.interval_ms = 0;
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        shards_of(&c),
        total_before,
        "GC, reconciliation and repair removed no shard of a live generation"
    );
    // A colder tier on the same nodes, and a demotion pass that would move
    // every generation (the lost one cannot be read, so must stay put).
    arca_lake_controller::groups::create_groups(
        c.meta.as_ref(),
        &ClusterOptions::rs(3, 2, 1, MIB, 0).profile,
        "cold",
        1,
    )
    .await
    .unwrap();
    let tiers = arca_lake_controller::tiering::TieringPolicy {
        tiers: vec!["cold".into(), "default".into()],
        promote_reads: 1,
        promote_window: Duration::from_secs(60),
        demote_after: Duration::ZERO,
    };
    let _ = arca_lake_controller::tiering::run_once(
        c.meta.as_ref(),
        &c.clients,
        &tiers,
        now_ms() + 3_600_000,
    )
    .await;
    ctl.shutdown().await;
    // The readable object may have moved to the colder tier; the lost
    // generation could not be read, so it stayed where it was.
    assert_eq!(
        c.manifest(BUCKET, "lost").await.chunks[0]
            .1
            .placement
            .group_id,
        group,
        "an unreadable generation is never moved"
    );
    assert_ne!(
        c.manifest(BUCKET, "good").await.chunks[0]
            .1
            .placement
            .group_id,
        group,
        "the tiering pass did run (the readable object was demoted)"
    );
    let remaining: Vec<_> = (0..c.nodes.len())
        .flat_map(|i| c.shard_keys(i, group))
        .filter(|k| k.chunk_id == chunk)
        .collect();
    assert_eq!(
        remaining.len(),
        1,
        "the surviving shard of the lost chunk stays"
    );
    assert_eq!(s3::get(&c.backend, BUCKET, "good").await.unwrap().0, good);
    assert!(s3::get(&c.backend, BUCKET, "lost").await.is_err());
    assert_eq!(
        recovery::data_losses(c.meta.as_ref()).await.unwrap().len(),
        1
    );
    assert!(
        unrecoverable_task(&c).await,
        "still tracked after the passes"
    );
    c.shutdown().await;
}

#[tokio::test]
async fn finalize_is_one_transaction_under_commit_faults_and_races() {
    let fk = Arc::new(FaultKv::new(MemKv::default()));
    let c = cluster(Arc::clone(&fk)).await;
    put(&c, "lost", 1).await;
    lose(&c, "lost").await;
    let inc = restore_and_verify(&c).await;
    let forced = req(inc, true, "accepted loss");
    let state_key = arca_lake_metadata::keys::cluster_state();

    // A crash before the commit (the controller or the admin process dies
    // with the transaction unapplied): nothing of it is visible.
    fk.inject_on_key(state_key.clone(), CommitFault::UnknownWithoutApply, 1);
    assert!(recovery::finalize(c.meta.as_ref(), &forced).await.is_err());
    assert_eq!(phase(c.meta.as_ref()).await, ClusterPhase::RecoveryRequired);
    assert_eq!(
        recovery::data_losses(c.meta.as_ref()).await.unwrap(),
        [] as [arca_lake_metadata::ops::recovery::DataLossRecord; 0]
    );
    assert_eq!(
        recovery::finalize_audits(c.meta.as_ref()).await.unwrap(),
        [] as [arca_lake_metadata::ops::recovery::FinalizeAudit; 0]
    );
    assert!(
        recovery::report(c.meta.as_ref(), &inc)
            .await
            .unwrap()
            .unwrap()
            .accepted
            .is_none()
    );
    // Applied but the outcome unknown: everything is visible at once, and
    // a retry is refused (already Active) without duplicating anything.
    fk.inject_on_key(state_key, CommitFault::UnknownAfterApply, 1);
    assert!(recovery::finalize(c.meta.as_ref(), &forced).await.is_err());
    assert_eq!(phase(c.meta.as_ref()).await, ClusterPhase::Active);
    assert_eq!(
        recovery::data_losses(c.meta.as_ref()).await.unwrap().len(),
        1
    );
    assert!(
        recovery::report(c.meta.as_ref(), &inc)
            .await
            .unwrap()
            .unwrap()
            .accepted
            .is_some()
    );
    let (v, _) = recovery::finalize(c.meta.as_ref(), &forced).await.unwrap();
    assert!(v.refusal.is_some());
    assert_eq!(
        recovery::data_losses(c.meta.as_ref()).await.unwrap().len(),
        1
    );
    c.shutdown().await;

    // Races with a verification restart: whichever commits first, the
    // cluster is never Active with an unfinished or unaccepted report,
    // and an accepted report is never overwritten.
    for round in 0..20u64 {
        let c = cluster(Arc::new(MemKv::default())).await;
        put(&c, "lost", round).await;
        lose(&c, "lost").await;
        let inc = restore_and_verify(&c).await;
        let r = req(inc, true, "race");
        let (fin, ver) = tokio::join!(
            recovery::finalize(c.meta.as_ref(), &r),
            recovery::restart_verification(c.meta.as_ref(), inc, true, "admin", now_ms()),
        );
        let (v, _) = fin.unwrap();
        let restarted = ver.unwrap().is_ok();
        assert_ne!(
            v.refusal.is_none(),
            restarted,
            "exactly one wins (round {round})"
        );
        let st = phase(c.meta.as_ref()).await;
        let rep = recovery::report(c.meta.as_ref(), &inc)
            .await
            .unwrap()
            .unwrap();
        if st == ClusterPhase::Active {
            assert!(
                rep.finished_ms > 0 && rep.accepted.is_some(),
                "round {round}"
            );
        } else {
            assert_eq!(st, ClusterPhase::VerificationPending);
            assert!(rep.accepted.is_none() && rep.finished_ms == 0);
        }
        c.shutdown().await;
    }
}

#[tokio::test]
async fn a_delayed_rpc_of_the_old_incarnation_is_refused_after_finalize() {
    let c = cluster(Arc::new(MemKv::default())).await;
    put(&c, "obj", 1).await;
    let old = compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap()
        .incarnation;
    let ch = c.manifest(BUCKET, "obj").await.chunks[0].1.clone();
    let inc = restore_and_verify(&c).await;
    recovery::finalize(c.meta.as_ref(), &req(inc, false, ""))
        .await
        .unwrap();
    // A controller of the pre-restore incarnation whose delete was delayed.
    let stale = NodeClients::new(Duration::from_secs(5)).with_tls(c.clients.tls().cloned());
    stale.set_incarnation(Some(old));
    let group = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == ch.placement.group_id)
        .unwrap();
    let nodes = cluster::list_nodes(c.meta.as_ref()).await.unwrap();
    let addr = &nodes
        .iter()
        .find(|n| n.id == group.members[0].node_id)
        .unwrap()
        .address;
    let e = stale
        .delete_shard(
            addr,
            ShardRef {
                group_id: group.id,
                chunk_id: ch.chunk_id,
                shard_index: 0,
            },
            u64::MAX,
        )
        .await
        .unwrap_err();
    assert!(e.message().starts_with("incarnation rejected"), "{e}");
    c.shutdown().await;
}
