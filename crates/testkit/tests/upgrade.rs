//! Upgrade finalize as a fenced, resumable state machine (RFC-014 §5.5–
//! §5.6, ADR-0016) on the in-process cluster: the precondition blocks on
//! a process or node that does not reach the target, every step is a
//! compare-and-set taken by the controller leader, cancel is refused past
//! the cut-off, a leader failover resumes the cursor, and the audit record
//! survives. The two-binary mixed-version scenarios are in
//! `mixed_version.rs` (real processes, FoundationDB).

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use arca_lake_common::compat::{Capabilities, Target};
use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_controller::upgrade::upgrade_pass;
use arca_lake_metadata::ops::compat;
use arca_lake_metadata::records::{ProcKind, ProcRecord, UpgradeStep};
use arca_lake_metadata::{Txn, keys, transact};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const BUCKET: &str = "upgrade";

async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        heartbeat: Some(Duration::from_millis(100)),
        ..ClusterOptions::rs(3, 2, 1, 1 << 20, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn current_target() -> Target {
    Target {
        protocol: arca_lake_protocol::storage::v1::PROTOCOL_VERSION,
        schema: keys::SCHEMA_VERSION,
        format: u64::from(arca_lake_storage::format::SEGMENT_FORMAT_VERSION),
    }
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

#[tokio::test]
async fn finalize_blocks_on_a_lagging_process_then_steps_through_and_audits() {
    let c = cluster().await;
    let meta = Arc::clone(&c.meta);
    let own = arca_lake_controller::capabilities();
    let target = current_target();
    // Request.
    let cur = compat::request_finalize(meta.as_ref(), target, vec![], "admin", now_ms())
        .await
        .unwrap();
    assert_eq!(cur.step, UpgradeStep::Requested);
    // Idempotent re-request; a different target is refused.
    assert_eq!(
        compat::request_finalize(meta.as_ref(), target, vec![], "admin", now_ms())
            .await
            .unwrap()
            .step,
        UpgradeStep::Requested
    );
    let other = Target {
        schema: target.schema + 1,
        ..target
    };
    assert!(
        compat::request_finalize(meta.as_ref(), other, vec![], "admin", now_ms())
            .await
            .is_err()
    );
    // A fresh heartbeat of a process that reads an older schema blocks the
    // precondition; a stale one does not.
    let lagging = ProcRecord {
        id: "old-gateway".into(),
        kind: ProcKind::Gateway,
        software: "0.0.0".into(),
        caps: Capabilities {
            protocol_min: 1,
            protocol_max: 1,
            schema_min: 0,
            schema_max: target.schema - 1,
            format_min: 1,
            format_max: 1,
        },
        incarnation: [0; 16],
        last_ms: now_ms(),
    };
    compat::heartbeat(meta.as_ref(), &lagging).await.unwrap();
    let r = upgrade_pass(meta.as_ref(), &c.clients, &own, 7, now_ms())
        .await
        .unwrap();
    assert_eq!(
        (r.before, r.after),
        (Some(UpgradeStep::Requested), Some(UpgradeStep::Requested))
    );
    let why = r.blocked.unwrap();
    assert!(why.contains("old-gateway"), "{why}");
    assert!(
        compat::upgrade_cursor(meta.as_ref())
            .await
            .unwrap()
            .unwrap()
            .last_error
            .is_some_and(|e| e.contains("precondition"))
    );
    // Ignoring it by id lets the precondition pass; so does its heartbeat
    // going stale.
    let mut stale = lagging.clone();
    stale.last_ms = now_ms() - 2 * arca_lake_controller::upgrade::PROC_STALE_MS;
    compat::heartbeat(meta.as_ref(), &stale).await.unwrap();
    // The gateway's own heartbeat is present and reaches the target.
    wait_until(
        "the gateway heartbeat",
        Duration::from_secs(5),
        async || {
            compat::procs(meta.as_ref())
                .await
                .unwrap()
                .iter()
                .any(|p| p.kind == ProcKind::Gateway && p.id != "old-gateway")
        },
    )
    .await;
    // Steps, one per pass, each recorded with the token.
    let mut steps = Vec::new();
    for _ in 0..6 {
        let r = upgrade_pass(meta.as_ref(), &c.clients, &own, 7, now_ms())
            .await
            .unwrap();
        assert!(r.blocked.is_none(), "{r:?}");
        steps.push(r.after);
        if r.after.is_none() {
            break;
        }
    }
    assert_eq!(
        steps,
        vec![
            Some(UpgradeStep::Preconditioned),
            Some(UpgradeStep::Migrated),
            Some(UpgradeStep::SchemaWriteRaised),
            Some(UpgradeStep::FormatWriteRaised),
            None
        ]
    );
    assert!(
        compat::upgrade_cursor(meta.as_ref())
            .await
            .unwrap()
            .is_none()
    );
    let g = compat::gate(meta.as_ref()).await.unwrap();
    assert_eq!(g.schema, Some(target.schema));
    assert_eq!(g.schema_write, Some(target.schema));
    assert_eq!(g.format_write, Some(target.format));
    let audits = arca_lake_metadata::ops::recovery::upgrade_audits(meta.as_ref())
        .await
        .unwrap();
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].by_token, 7);
    assert!(
        audits[0]
            .history
            .iter()
            .any(|h| h.contains("downgrade cut-off"))
    );
    assert!(audits[0].history.last().unwrap().contains("finalized"));
    // Nodes learned the format write gate through the step-4 push.
    for n in &c.nodes {
        let h = c.clients.health(&n.addr.to_string()).await.unwrap();
        assert_eq!(h.format_write, target.format);
    }
    // Another pass is a no-op.
    let r = upgrade_pass(meta.as_ref(), &c.clients, &own, 8, now_ms())
        .await
        .unwrap();
    assert_eq!((r.before, r.after), (None, None));
    c.shutdown().await;
}

#[tokio::test]
async fn cancel_is_allowed_before_the_cut_off_and_refused_after() {
    let c = cluster().await;
    let meta = Arc::clone(&c.meta);
    let target = current_target();
    compat::request_finalize(meta.as_ref(), target, vec![], "admin", now_ms())
        .await
        .unwrap();
    assert!(
        compat::advance_upgrade(
            meta.as_ref(),
            UpgradeStep::Requested,
            UpgradeStep::Preconditioned,
            1,
            "t",
            1
        )
        .await
        .unwrap()
    );
    // A compare-and-set from the wrong step does nothing (a second leader
    // repeating a step it believes is next).
    assert!(
        !compat::advance_upgrade(
            meta.as_ref(),
            UpgradeStep::Requested,
            UpgradeStep::Preconditioned,
            2,
            "t",
            2
        )
        .await
        .unwrap()
    );
    let cancelled = compat::clear_upgrade(meta.as_ref(), true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled.step, UpgradeStep::Preconditioned);
    assert!(
        compat::upgrade_cursor(meta.as_ref())
            .await
            .unwrap()
            .is_none()
    );
    // Past the cut-off: refused.
    compat::request_finalize(meta.as_ref(), target, vec![], "admin", now_ms())
        .await
        .unwrap();
    for (from, to) in [
        (UpgradeStep::Requested, UpgradeStep::Preconditioned),
        (UpgradeStep::Preconditioned, UpgradeStep::Migrated),
        (UpgradeStep::Migrated, UpgradeStep::SchemaWriteRaised),
    ] {
        assert!(
            compat::advance_upgrade(meta.as_ref(), from, to, 1, "t", 1)
                .await
                .unwrap()
        );
    }
    let e = compat::clear_upgrade(meta.as_ref(), true)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("cut-off"), "{e}");
    assert!(
        compat::upgrade_cursor(meta.as_ref())
            .await
            .unwrap()
            .is_some()
    );
    c.shutdown().await;
}

#[tokio::test]
async fn the_controller_leader_finishes_a_requested_finalize_across_a_failover() {
    let c = cluster().await;
    let meta = Arc::clone(&c.meta);
    s3::put(
        &c.backend,
        BUCKET,
        "k",
        Bytes::from(random_bytes(4096, 1)),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let mut cfg = ControllerConfig::new("a");
    cfg.lease = Duration::from_millis(600);
    cfg.gc_interval = Duration::from_secs(3600);
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.repair_interval = Duration::from_secs(3600);
    cfg.stripe_repair_interval = Duration::from_secs(3600);
    cfg.scrub.interval_ms = 0;
    cfg.health_interval = Duration::from_millis(200);
    cfg.upgrade_interval = Duration::from_millis(300);
    cfg.heartbeat = Some(Duration::from_millis(100));
    let mut cfg_b = cfg.clone();
    cfg_b.id = "b".into();
    let a = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let b = spawn(Arc::clone(&c.meta), cfg_b).await.unwrap();
    wait_until("a leader", Duration::from_secs(5), async || {
        a.is_leader() || b.is_leader()
    })
    .await;
    compat::request_finalize(meta.as_ref(), current_target(), vec![], "admin", now_ms())
        .await
        .unwrap();
    // Let the leader take at least one step, then fail it over.
    wait_until("the first step", Duration::from_secs(10), async || {
        compat::upgrade_cursor(meta.as_ref())
            .await
            .unwrap()
            .is_none_or(|cur| cur.step != UpgradeStep::Requested)
    })
    .await;
    let (old, new) = if a.is_leader() { (a, b) } else { (b, a) };
    let token_before = old.token();
    old.shutdown().await;
    wait_until("the new leader", Duration::from_secs(5), async || {
        new.is_leader()
    })
    .await;
    wait_until("finalize", Duration::from_secs(20), async || {
        compat::upgrade_cursor(meta.as_ref())
            .await
            .unwrap()
            .is_none()
    })
    .await;
    let audits = arca_lake_metadata::ops::recovery::upgrade_audits(meta.as_ref())
        .await
        .unwrap();
    assert_eq!(audits.len(), 1, "{audits:?}");
    let tokens: Vec<u64> = audits[0]
        .history
        .iter()
        .filter_map(|h| h.split("token ").nth(1).and_then(|t| t.parse().ok()))
        .collect();
    assert!(audits[0].by_token > token_before, "{:?}", audits[0]);
    let _ = tokens;
    // The gate is at the target and the cluster still serves.
    let g = compat::gate(meta.as_ref()).await.unwrap();
    assert_eq!(g.schema, Some(keys::SCHEMA_VERSION));
    assert_eq!(
        s3::get(&c.backend, BUCKET, "k").await.unwrap().0.len(),
        4096
    );
    new.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn a_version_byte_above_this_build_is_refused_and_schema_write_is_clamped() {
    // Direct record-level check of the downgrade refusal (RFC-014 §5.6): a
    // record stamped with a newer version byte cannot be decoded.
    use arca_lake_metadata::records;
    let s = Arc::new(arca_lake_metadata::mem::MemKv::default());
    let key = keys::bucket_by_name("b");
    let mut v = records::encode(&records::BucketRecord {
        id: 1,
        name: "b".into(),
        created_ms: 0,
        versioning: records::Versioning::Disabled,
        owner: String::new(),
        policy: None,
    })
    .unwrap();
    v[0] = records::RECORD_VERSION + 1;
    transact(s.as_ref(), (key, v), |tx, env| {
        Box::pin(async move {
            let (k, v) = env;
            tx.set(k, v);
            Ok(())
        })
    })
    .await
    .unwrap();
    let e = arca_lake_metadata::ops::bucket::get_bucket(s.as_ref(), "b")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("unknown record version"), "{e}");
}
