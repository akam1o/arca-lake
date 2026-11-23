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
use arca_lake_metadata::kv::{Key, KeyRange, Limits, Value};
use arca_lake_metadata::ops::compat;
use arca_lake_metadata::records::{ProcKind, ProcRecord, UpgradeStep};
use arca_lake_metadata::{KvError, KvStore, Txn, keys, transact};
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

/// A group of several members as a 0.1.0 `admin create-groups` creates
/// it: no recorded failure domain (ADR-0038).
async fn unrecorded_group(c: &TestCluster) -> u64 {
    let mut g = c.groups().await.remove(0);
    g.id = 0;
    arca_lake_metadata::ops::cluster::create_group(c.meta.as_ref(), g)
        .await
        .unwrap()
        .id
}

/// ADR-0038: the finalize to schema 5 waits until every group's failure
/// domain is recorded, at step 1 and again at the cut-off (a group a
/// 0.1.0 `admin create-groups` made after step 1).
#[tokio::test]
async fn finalize_waits_for_every_groups_failure_domain() {
    use arca_lake_metadata::ops::group_domain;
    let c = cluster().await;
    let meta = Arc::clone(&c.meta);
    let own = arca_lake_controller::capabilities();
    let target = current_target();
    assert!(target.schema >= keys::SCHEMA_V5);
    let first = unrecorded_group(&c).await;
    compat::request_finalize(meta.as_ref(), target, vec![], "admin", now_ms())
        .await
        .unwrap();
    let pass = async || {
        upgrade_pass(meta.as_ref(), &c.clients, &own, 7, now_ms())
            .await
            .unwrap()
    };
    let blocked_at = async |step: UpgradeStep, group: u64| {
        let r = pass().await;
        assert_eq!((r.before, r.after), (Some(step), Some(step)), "{r:?}");
        let why = r.blocked.unwrap_or_default();
        assert!(
            why.contains(&format!("[{group}]")) && why.contains("set-failure-domain"),
            "{why}"
        );
    };
    blocked_at(UpgradeStep::Requested, first).await;
    group_domain::set(meta.as_ref(), first, "host", false, now_ms())
        .await
        .unwrap();
    assert_eq!(pass().await.after, Some(UpgradeStep::Preconditioned));
    assert_eq!(pass().await.after, Some(UpgradeStep::Migrated));
    // The cut-off: a group created meanwhile without a record holds it.
    let write_before = compat::gate(meta.as_ref()).await.unwrap().schema_write;
    let late = unrecorded_group(&c).await;
    blocked_at(UpgradeStep::Migrated, late).await;
    assert_eq!(
        compat::gate(meta.as_ref()).await.unwrap().schema_write,
        write_before
    );
    group_domain::set(meta.as_ref(), late, "host", false, now_ms())
        .await
        .unwrap();
    assert_eq!(pass().await.after, Some(UpgradeStep::SchemaWriteRaised));
    c.shutdown().await;
}

/// A store on which a 0.1.0 gateway heartbeats right after the schema is
/// raised: one whose gate admitted it before the raise.
struct RaceAtRaise {
    inner: Arc<arca_lake_metadata::mem::MemKv>,
    raced: Arc<std::sync::atomic::AtomicBool>,
}

struct RaceAtRaiseTx {
    inner: <arca_lake_metadata::mem::MemKv as KvStore>::Tx,
    store: Arc<arca_lake_metadata::mem::MemKv>,
    raced: Arc<std::sync::atomic::AtomicBool>,
    raises: bool,
}

impl KvStore for RaceAtRaise {
    type Tx = RaceAtRaiseTx;

    async fn begin(&self) -> Result<RaceAtRaiseTx, KvError> {
        Ok(RaceAtRaiseTx {
            inner: self.inner.begin().await?,
            store: Arc::clone(&self.inner),
            raced: Arc::clone(&self.raced),
            raises: false,
        })
    }

    fn limits(&self) -> Limits {
        self.inner.limits()
    }
}

impl Txn for RaceAtRaiseTx {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.inner.get(key).await
    }

    async fn get_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Key, Value)>, KvError> {
        self.inner.get_range(range, limit, reverse).await
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.raises |= key == keys::schema_version().as_slice();
        self.inner.set(key, value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.inner.clear(key);
    }

    fn clear_range(&mut self, range: &KeyRange) {
        self.inner.clear_range(range);
    }

    async fn commit(self) -> Result<u64, KvError> {
        let v = self.inner.commit().await?;
        if self.raises && !self.raced.swap(true, std::sync::atomic::Ordering::SeqCst) {
            compat::heartbeat(
                self.store.as_ref(),
                &v010_proc("gw-raced", ProcKind::Gateway),
            )
            .await
            .unwrap();
        }
        Ok(v)
    }
}

/// ADR-0039: step 5 checks the heartbeats again after raising `schema`: a
/// 0.1.0 process that started while the step ran (its gate admitted it)
/// keeps the upgrade open until it is stopped.
#[tokio::test]
async fn step_5_rechecks_heartbeats_after_the_schema_raise() {
    let c = cluster().await;
    let meta = Arc::new(RaceAtRaise {
        inner: Arc::clone(&c.meta),
        raced: Arc::default(),
    });
    let own = arca_lake_controller::capabilities();
    let target = current_target();
    assert!(target.schema >= keys::SCHEMA_V5);
    // A store upgraded from 0.1.0 (a new one starts at schema 5).
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            let v4 = (keys::SCHEMA_V5 - 1).to_le_bytes();
            tx.set(&keys::schema_version(), &v4);
            tx.set(&keys::schema_write(), &v4);
            Ok(())
        })
    })
    .await
    .unwrap();
    compat::request_finalize(meta.as_ref(), target, vec![], "admin", now_ms())
        .await
        .unwrap();
    for step in [
        UpgradeStep::Preconditioned,
        UpgradeStep::Migrated,
        UpgradeStep::SchemaWriteRaised,
        UpgradeStep::FormatWriteRaised,
    ] {
        let r = upgrade_pass(meta.as_ref(), &c.clients, &own, 7, now_ms())
            .await
            .unwrap();
        assert_eq!(r.after, Some(step), "{r:?}");
    }
    let r = upgrade_pass(meta.as_ref(), &c.clients, &own, 7, now_ms())
        .await
        .unwrap();
    assert!(meta.raced.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        (r.before, r.after),
        (
            Some(UpgradeStep::FormatWriteRaised),
            Some(UpgradeStep::FormatWriteRaised)
        ),
        "{r:?}"
    );
    let why = r.blocked.unwrap_or_default();
    assert!(why.contains("gw-raced") && why.contains("0.1.0"), "{why}");
    compat::remove_heartbeat(meta.as_ref(), "gw-raced")
        .await
        .unwrap();
    let r = upgrade_pass(meta.as_ref(), &c.clients, &own, 7, now_ms())
        .await
        .unwrap();
    assert_eq!(r.after, None, "{r:?}");
    c.shutdown().await;
}

/// ADR-0039: a process named with `--ignore-absent` is passed over only
/// while its heartbeat has not advanced since the request.
#[tokio::test]
async fn ignore_absent_does_not_pass_over_a_process_that_heartbeats_after_the_request() {
    let c = cluster().await;
    let meta = Arc::clone(&c.meta);
    let own = arca_lake_controller::capabilities();
    let target = current_target();
    let requested = now_ms();
    // Its last heartbeat before the request (stopped; still fresh).
    let mut gone = v010_proc("gw-gone", ProcKind::Gateway);
    gone.last_ms = requested - 1_000;
    compat::heartbeat(meta.as_ref(), &gone).await.unwrap();
    compat::request_finalize(
        meta.as_ref(),
        target,
        vec!["gw-gone".into()],
        "admin",
        requested,
    )
    .await
    .unwrap();
    // Running after all: it heartbeats after the request.
    let mut back = gone.clone();
    back.last_ms = requested + 1_000;
    compat::heartbeat(meta.as_ref(), &back).await.unwrap();
    let r = upgrade_pass(meta.as_ref(), &c.clients, &own, 7, requested + 2_000)
        .await
        .unwrap();
    assert_eq!(r.after, Some(UpgradeStep::Requested), "{r:?}");
    let why = r.blocked.unwrap_or_default();
    assert!(
        why.contains("gw-gone") && why.contains("heartbeated after the request"),
        "{why}"
    );
    // Back to its last heartbeat before the request: passed over.
    compat::heartbeat(meta.as_ref(), &gone).await.unwrap();
    let r = upgrade_pass(meta.as_ref(), &c.clients, &own, 7, requested + 2_000)
        .await
        .unwrap();
    assert_eq!(r.after, Some(UpgradeStep::Preconditioned), "{r:?}");
    c.shutdown().await;
}

/// ADR-0039: after the finalize the leader flags, on every health pass,
/// fresh heartbeats of processes that read a schema below `schema_write`
/// (`arca_controller_procs_below_schema`); a stale one is not counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leader_flags_a_fresh_heartbeat_below_schema_write() {
    let c = cluster().await;
    let meta = Arc::clone(&c.meta);
    assert!(
        compat::gate(meta.as_ref())
            .await
            .unwrap()
            .schema_v5_enforced()
    );
    let mut cfg = ControllerConfig::new("a");
    cfg.lease = Duration::from_millis(600);
    cfg.gc_interval = Duration::from_secs(3600);
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.repair_interval = Duration::from_secs(3600);
    cfg.stripe_repair_interval = Duration::from_secs(3600);
    cfg.scrub.interval_ms = 0;
    cfg.health_interval = Duration::from_millis(200);
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let below = || {
        ctl.metrics
            .procs_below_schema
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    let beat = async |last_ms: u64| {
        let mut p = v010_proc("gw-0.1.0", ProcKind::Gateway);
        p.last_ms = last_ms;
        compat::heartbeat(meta.as_ref(), &p).await.unwrap();
    };
    beat(now_ms()).await;
    wait_until(
        "the 0.1.0 gateway flagged",
        Duration::from_secs(10),
        async || ctl.is_leader() && below() == 1,
    )
    .await;
    beat(now_ms() - 2 * arca_lake_controller::upgrade::PROC_STALE_MS).await;
    wait_until(
        "the stale record not counted",
        Duration::from_secs(10),
        async || below() == 0,
    )
    .await;
    ctl.shutdown().await;
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

/// ADR-0039 §2.4: once `schema_write` is 5, a node that reports a lower
/// schema (standing in for 0.1.0 restarted at a member's address) never
/// has a shard write counted: a stale capability is caught by the
/// acknowledgement, a fresh one before the write, and the chunks go to
/// the other group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_schema_5_no_shard_write_counts_on_an_earlier_release_node() {
    use arca_lake_protocol::client::{NodeClients, ShardRef};
    let mut c = TestCluster::start(ClusterOptions {
        groups: 2,
        heartbeat: Some(Duration::from_millis(100)),
        ..ClusterOptions::rs(6, 2, 1, 1 << 20, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let gate = compat::gate(c.meta.as_ref()).await.unwrap();
    assert!(gate.schema_v5_enforced(), "{gate:?}");
    let groups: Vec<_> = c
        .groups()
        .await
        .into_iter()
        .filter(|grp| !grp.is_cache())
        .collect();
    assert_eq!(groups.len(), 2, "{groups:?}");
    let victim_group = groups[0].clone();
    let i = c.node_index(&victim_group.members[0]).unwrap();
    let addr = c.nodes[i].addr.to_string();
    for k in 0..4 {
        let key = format!("before{k}");
        s3::put(
            &c.backend,
            BUCKET,
            &key,
            Bytes::from(random_bytes(300 << 10, k)),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    c.stop_node(i).await;
    c.start_node_reporting(i, keys::SCHEMA_V5 - 1).await;

    // A writer that still holds the node's earlier capabilities sends the
    // write; the acknowledgement shows the release and it does not count.
    let probe = NodeClients::new(Duration::from_secs(2));
    probe.set_required_node_schema(compat::required_node_schema(gate.schema_write));
    let other = c.nodes[(i + 1) % c.nodes.len()].addr.to_string();
    let fresh = c.clients.health(&other).await.unwrap();
    probe.note_health(&addr, &fresh);
    let shard = ShardRef {
        group_id: victim_group.id,
        chunk_id: [7; 16],
        shard_index: 0,
    };
    let err = probe
        .put_shard(
            &addr,
            shard,
            victim_group.epoch,
            Bytes::from_static(b"x"),
            1 << 16,
        )
        .await
        .unwrap_err();
    assert!(NodeClients::is_node_schema_refused(&err), "{err}");
    assert!(err.message().contains("acknowledged"), "{err}");
    // Now asked again: refused before anything is sent.
    let err = probe
        .put_shard(
            &addr,
            shard,
            victim_group.epoch,
            Bytes::from_static(b"x"),
            1 << 16,
        )
        .await
        .unwrap_err();
    assert!(
        NodeClients::is_node_schema_refused(&err) && err.message().contains("reports"),
        "{err}"
    );

    // The gateway: every object is placed in the other group.
    for k in 0..20 {
        let key = format!("after{k}");
        let d = Bytes::from(random_bytes(300 << 10, 100 + k));
        s3::put(&c.backend, BUCKET, &key, d.clone(), &PutOptions::default())
            .await
            .unwrap_or_else(|err| panic!("{key}: {err:?}"));
        let manifest = c.manifest(BUCKET, &key).await;
        assert!(
            manifest
                .chunks
                .iter()
                .all(|(_, ch)| ch.placement.group_id != victim_group.id),
            "{key} counted a write on the earlier-release node's group"
        );
        assert_eq!(s3::get(&c.backend, BUCKET, &key).await.unwrap().0, d);
    }
    c.shutdown().await;
}

/// ADR-0039: below schema 5 (the mixed window with 0.1.0, whose nodes
/// report no schema) no node schema is required for writes.
#[test]
fn no_node_schema_is_required_before_schema_5() {
    assert_eq!(compat::required_node_schema(None), 0);
    assert_eq!(compat::required_node_schema(Some(4)), 0);
    assert_eq!(compat::required_node_schema(Some(5)), 5);
    assert_eq!(compat::required_node_schema(Some(6)), 6);
}

/// ADR-0039: a repair pass reads `schema_write` again before it writes.
/// Raised mid-term (here after the writer's last read, as finalize step 3
/// or a restore's `restore_floors` does), it applies before any health
/// pass: the rebuilt shard is not counted on a node reporting schema 0,
/// and is once the node reports this release's schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repair_pass_applies_a_schema_write_raised_mid_term() {
    use arca_lake_controller::integrity::{
        RepairWorkerOptions, ScrubOptions, coders, repair_pass, scrub_group_now,
    };
    use arca_lake_protocol::client::NodeClients;
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, 1 << 20, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    // A store upgraded from 0.1.0, before the cut-off.
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            let v4 = (keys::SCHEMA_V5 - 1).to_le_bytes();
            tx.set(&keys::schema_version(), &v4);
            tx.set(&keys::schema_write(), &v4);
            Ok(())
        })
    })
    .await
    .unwrap();
    s3::put(
        &c.backend,
        BUCKET,
        "obj",
        Bytes::from(random_bytes(3 << 19, 5)),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let chunk = c.manifest(BUCKET, "obj").await.chunks[0].1.clone();
    let group = c.groups().await.remove(0);
    let victim = c.node_index(&group.members[1]).unwrap();
    assert_eq!(c.corrupt_chunk_on_node(victim, group.id, chunk.chunk_id), 1);
    let scrub = ScrubOptions {
        interval_ms: 1,
        ..ScrubOptions::default()
    };
    let r = scrub_group_now(
        c.meta.as_ref(),
        &c.clients,
        &coders(),
        group.id,
        &scrub,
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!(r.enqueued, 1, "{r:?}");
    // The writer last read schema_write 4: nothing required.
    let writer = NodeClients::new(Duration::from_secs(2));
    writer.set_required_node_schema(compat::required_node_schema(Some(keys::SCHEMA_V5 - 1)));
    assert_eq!(writer.required_node_schema(), 0);
    compat::raise_version(
        c.meta.as_ref(),
        compat::VersionKey::SchemaWrite,
        keys::SCHEMA_V5,
    )
    .await
    .unwrap();
    c.stop_node(victim).await;
    c.start_node_reporting(victim, 0).await;
    let worker = RepairWorkerOptions {
        tasks_per_pass: 64,
        max_attempts: 3,
        recheck_ms: 1000,
        ..RepairWorkerOptions::default()
    };
    let meta = Arc::clone(&c.meta);
    let pass = async |now: u64| {
        repair_pass(meta.as_ref(), &writer, &coders(), &worker, None, None, now)
            .await
            .unwrap()
    };
    let r = pass(now_ms()).await;
    assert_eq!(
        (r.succeeded, r.shards_rewritten),
        (0, 0),
        "a rebuilt shard counted on a node reporting schema 0: {r:?}"
    );
    let tasks = arca_lake_metadata::ops::integrity::tasks(c.meta.as_ref(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1, "{tasks:?}");
    assert_eq!(writer.required_node_schema(), keys::SCHEMA_V5);
    // The node back on this release: the task completes.
    c.stop_node(victim).await;
    c.start_node_quiet(victim).await;
    writer.forget_caps(&c.nodes[victim].addr.to_string());
    let r = pass(now_ms() + 120_000).await;
    assert_eq!((r.succeeded, r.shards_rewritten), (1, 1), "{r:?}");
    c.shutdown().await;
}

/// ADR-0039: after the finalize, the leader asks a node facts-free before
/// pushing anything; a node of an earlier release (here one reporting
/// schema 4 restarted at a member's address) is quarantined and never
/// receives what a push carries: here the delete fence generation the
/// leader opened while the node was down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_earlier_release_node_is_quarantined_before_any_push() {
    let mut c = cluster().await;
    let meta = Arc::clone(&c.meta);
    let mut cfg = ControllerConfig::new("a");
    cfg.lease = Duration::from_millis(600);
    cfg.gc_interval = Duration::from_secs(3600);
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.repair_interval = Duration::from_secs(3600);
    cfg.stripe_repair_interval = Duration::from_secs(3600);
    cfg.scrub.interval_ms = 0;
    cfg.health_interval = Duration::from_millis(200);
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let (addr0, addr2) = (c.nodes[0].addr.to_string(), c.nodes[2].addr.to_string());
    let id0 = c.nodes[0].id;
    // The leader pushes this release's nodes the open fence, the schema
    // floor and the incarnation.
    wait_until(
        "node 2 pushed by the leader",
        Duration::from_secs(10),
        async || {
            ctl.is_leader()
                && c.clients.health(&addr2).await.is_ok_and(|h| {
                    h.delete_fence_open && h.schema_floor > 0 && !h.incarnation.is_empty()
                })
        },
    )
    .await;
    let id2 = c.nodes[2].id;
    let before = c
        .clients
        .health(&addr2)
        .await
        .unwrap()
        .delete_fence_generation;
    c.stop_node(2).await;
    // While it is down the fence moves on: closed, then opened again in a
    // new generation by the leader's next pass.
    arca_lake_metadata::ops::delete_fence::close(meta.as_ref(), 0, "test", false, now_ms())
        .await
        .unwrap();
    wait_until(
        "a new open generation on node 0",
        Duration::from_secs(10),
        async || {
            c.clients
                .health(&addr0)
                .await
                .is_ok_and(|h| h.delete_fence_open && h.delete_fence_generation > before)
        },
    )
    .await;
    c.start_node_reporting(2, keys::SCHEMA_V5 - 1).await;
    let state = async || {
        arca_lake_metadata::ops::node_lifecycle::state(meta.as_ref(), id2)
            .await
            .unwrap()
    };
    wait_until("node 2 quarantined", Duration::from_secs(10), async || {
        state().await == arca_lake_metadata::ops::node_lifecycle::State::Quarantined
    })
    .await;
    // Several more passes (node 0 is seen at every one, 200 ms apart): the
    // quarantined node is asked facts-free only.
    let seen = async || {
        arca_lake_metadata::ops::cluster::list_nodes(meta.as_ref())
            .await
            .unwrap()
            .into_iter()
            .find(|n| n.id == id0)
            .unwrap()
            .last_seen_ms
    };
    let at = seen().await;
    wait_until(
        "five more health passes",
        Duration::from_secs(10),
        async || seen().await >= at + 1_000,
    )
    .await;
    let h = c.clients.health(&addr2).await.unwrap();
    assert_eq!(
        h.delete_fence_generation, before,
        "the leader's new fence generation reached it: {h:?}"
    );
    ctl.shutdown().await;
    c.shutdown().await;
}
