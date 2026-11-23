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

/// A cancellation before the downgrade cut-off and the controller's raise
/// of `schema_write` cannot both win: the raise goes with the cursor
/// advance in one transaction (Phase 3.5 audit CON-8: the raise landed
/// after a cancellation and left the gate raised with no cursor).
#[tokio::test]
async fn a_cancelled_upgrade_never_raises_the_schema_write_gate() {
    use arca_lake_metadata::ops::compat::VersionKey;
    let c = cluster().await;
    let meta = Arc::clone(&c.meta);
    let target = Target {
        schema: keys::SCHEMA_VERSION + 1,
        ..current_target()
    };
    compat::request_finalize(meta.as_ref(), target, vec![], "admin", now_ms())
        .await
        .unwrap();
    for (from, to) in [
        (UpgradeStep::Requested, UpgradeStep::Preconditioned),
        (UpgradeStep::Preconditioned, UpgradeStep::Migrated),
    ] {
        assert!(
            compat::advance_upgrade(meta.as_ref(), from, to, 1, "test", now_ms())
                .await
                .unwrap()
        );
    }
    let before = compat::gate(meta.as_ref()).await.unwrap().schema_write;
    // The operator cancels while the controller is about to raise.
    compat::clear_upgrade(meta.as_ref(), true).await.unwrap();
    let advanced = compat::advance_upgrade_raising(
        meta.as_ref(),
        UpgradeStep::Migrated,
        UpgradeStep::SchemaWriteRaised,
        Some((VersionKey::SchemaWrite, target.schema)),
        1,
        "test",
        now_ms(),
    )
    .await
    .unwrap();
    assert!(!advanced);
    assert_eq!(
        compat::gate(meta.as_ref()).await.unwrap().schema_write,
        before,
        "the gate was raised for a cancelled upgrade"
    );
    c.shutdown().await;
}

/// Phase 3.10 review: the finalize raises the schema only once every node
/// holds it as its floor. A node unreachable at the last step blocks it
/// (it would otherwise take the previous release's deletes on metadata
/// restored from before the finalize); once it answers, the finalize
/// completes and every node holds the floor.
#[tokio::test]
async fn finalize_waits_until_every_node_holds_the_schema_floor() {
    let mut c = cluster().await;
    let meta = Arc::clone(&c.meta);
    let own = arca_lake_controller::capabilities();
    let target = current_target();
    compat::request_finalize(meta.as_ref(), target, vec![], "admin", now_ms())
        .await
        .unwrap();
    for _ in 0..8 {
        let r = upgrade_pass(meta.as_ref(), &c.clients, &own, 7, now_ms())
            .await
            .unwrap();
        assert!(r.blocked.is_none(), "{r:?}");
        if r.after == Some(UpgradeStep::FormatWriteRaised) {
            break;
        }
    }
    c.stop_node(2).await;
    let r = upgrade_pass(meta.as_ref(), &c.clients, &own, 7, now_ms())
        .await
        .unwrap();
    let why = r.blocked.expect("finalized without node 2");
    assert!(why.contains("schema floor not held"), "{why}");
    assert!(
        compat::upgrade_cursor(meta.as_ref())
            .await
            .unwrap()
            .is_some()
    );
    // No new node while the step is open (it could escape the barrier).
    let e = arca_lake_metadata::ops::cluster::register_node(
        meta.as_ref(),
        "127.0.0.1:1",
        vec![],
        vec![0],
        now_ms(),
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("cut-off"), "{e}");
    c.start_node(2).await;
    let r = upgrade_pass(meta.as_ref(), &c.clients, &own, 7, now_ms())
        .await
        .unwrap();
    assert!(r.blocked.is_none() && r.after.is_none(), "{r:?}");
    assert!(
        compat::upgrade_cursor(meta.as_ref())
            .await
            .unwrap()
            .is_none()
    );
    for (i, n) in c.nodes.iter().enumerate() {
        let h = c.clients.health(&n.addr.to_string()).await.unwrap();
        assert!(h.schema_floor >= target.schema, "{h:?}");
        // The raised segment format is on the device at once (an earlier
        // release refuses the store from now on), not at the next rollover.
        assert_eq!(
            u64::from(c.node_store(i).newest_segment_format().unwrap()),
            target.format,
            "node {i}"
        );
    }
    c.shutdown().await;
}

/// A heartbeat of a 0.1.0 process (schema [3, 4], ADR-0039).
fn v010_proc(id: &str, kind: ProcKind) -> ProcRecord {
    ProcRecord {
        id: id.into(),
        kind,
        software: "0.1.0".into(),
        caps: Capabilities {
            protocol_min: 3,
            protocol_max: 4,
            schema_min: 3,
            schema_max: 4,
            format_min: 1,
            format_max: 2,
        },
        incarnation: [0; 16],
        last_ms: now_ms(),
    }
}

/// ADR-0039: the finalize to 0.2.0's schema is refused while a 0.1.0
/// process heartbeats: at step 1, at the cut-off (one started after step
/// 1, a rollback within the window) and at the last step (one started
/// after the cut-off, which its gate still admits); each step goes on once
/// it is gone.
#[tokio::test]
async fn finalize_to_schema_5_is_refused_while_a_0_1_0_process_runs() {
    let c = cluster().await;
    let meta = Arc::clone(&c.meta);
    let own = arca_lake_controller::capabilities();
    let target = current_target();
    assert!(target.schema >= keys::SCHEMA_V5);
    compat::request_finalize(meta.as_ref(), target, vec![], "admin", now_ms())
        .await
        .unwrap();
    let pass = async |token: u64| {
        upgrade_pass(meta.as_ref(), &c.clients, &own, token, now_ms())
            .await
            .unwrap()
    };
    let blocked_at = async |step: UpgradeStep, id: &str| {
        let r = pass(7).await;
        assert_eq!((r.before, r.after), (Some(step), Some(step)), "{r:?}");
        let why = r.blocked.unwrap_or_default();
        assert!(why.contains(id) && why.contains("0.1.0"), "{why}");
    };
    // Step 1.
    compat::heartbeat(meta.as_ref(), &v010_proc("gw-0.1.0", ProcKind::Gateway))
        .await
        .unwrap();
    blocked_at(UpgradeStep::Requested, "gw-0.1.0").await;
    compat::remove_heartbeat(meta.as_ref(), "gw-0.1.0")
        .await
        .unwrap();
    assert_eq!(pass(7).await.after, Some(UpgradeStep::Preconditioned));
    assert_eq!(pass(7).await.after, Some(UpgradeStep::Migrated));
    // The cut-off: schema_write is not raised past a 0.1.0 controller.
    let write_before = compat::gate(meta.as_ref()).await.unwrap().schema_write;
    compat::heartbeat(meta.as_ref(), &v010_proc("ctl-0.1.0", ProcKind::Controller))
        .await
        .unwrap();
    blocked_at(UpgradeStep::Migrated, "ctl-0.1.0").await;
    assert_eq!(
        compat::gate(meta.as_ref()).await.unwrap().schema_write,
        write_before
    );
    compat::remove_heartbeat(meta.as_ref(), "ctl-0.1.0")
        .await
        .unwrap();
    assert_eq!(pass(7).await.after, Some(UpgradeStep::SchemaWriteRaised));
    assert_eq!(pass(7).await.after, Some(UpgradeStep::FormatWriteRaised));
    // The last step.
    compat::heartbeat(meta.as_ref(), &v010_proc("gw-late", ProcKind::Gateway))
        .await
        .unwrap();
    blocked_at(UpgradeStep::FormatWriteRaised, "gw-late").await;
    compat::remove_heartbeat(meta.as_ref(), "gw-late")
        .await
        .unwrap();
    assert_eq!(pass(7).await.after, None);
    let g = compat::gate(meta.as_ref()).await.unwrap();
    assert_eq!(g.schema, Some(target.schema));
    assert!(g.schema_v5_enforced());
    c.shutdown().await;
}

/// ADR-0039: a store is initialised at 0.2.0's schema, protections
/// mandatory from the start (no 0.1.0 process has ever run on it).
#[tokio::test]
async fn a_new_store_starts_at_schema_5() {
    let c = cluster().await;
    let g = compat::gate(c.meta.as_ref()).await.unwrap();
    assert_eq!(g.schema, Some(keys::SCHEMA_VERSION));
    assert_eq!(g.schema_write, Some(keys::SCHEMA_VERSION));
    assert!(g.schema_v5_enforced());
    c.shutdown().await;
}

/// ADR-0039: 0.2.0 serves stores of schemas 4 (0.1.0) and 5, refuses one
/// of schema 3 (upgraded through 0.1.0 first) and a later one; 0.1.0's
/// gate (schema [3, 4]) refuses a store once it is finalized to 5.
#[tokio::test]
async fn the_0_2_0_gate_reads_schemas_4_and_5_and_0_1_0_refuses_5() {
    let v010 = v010_proc("x", ProcKind::Controller).caps;
    let own = arca_lake_controller::capabilities();
    // A `version-bump` build reads [5, 6].
    let bump = arca_lake_common::compat::BUMP == 1;
    for (stored, caps, ok) in [
        (3, own, false),
        (4, own, !bump),
        (5, own, true),
        (6, own, bump),
        (7, own, false),
        (4, v010, true),
        (5, v010, false),
    ] {
        let s = arca_lake_metadata::mem::MemKv::default();
        transact(&s, stored, |tx, v: &u64| {
            Box::pin(async move {
                tx.set(&keys::schema_version(), &v.to_le_bytes());
                Ok(())
            })
        })
        .await
        .unwrap();
        let r = compat::check_startup(&s, &caps, compat::Stamp::Legacy, "ctl", 1).await;
        assert_eq!(r.is_ok(), ok, "stored {stored} caps {caps:?}: {r:?}");
    }
    assert_eq!(
        (own.schema_min, own.schema_max),
        (keys::SCHEMA_VERSION_MIN, keys::SCHEMA_VERSION)
    );
}
