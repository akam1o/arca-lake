//! Capacity state × failover matrix (Phase 3.10), in process: each of the
//! five admission states (normal, warning, throttled, write rejected,
//! recovery only) is reached with real controllers on capped nodes, then
//! each fault is applied: leader kill, gateway restart, node restart,
//! metadata loss (every metadata commit fails for a while), report loss
//! (a node silent past the staleness limit), a stale record delivered to
//! the gateway, and a partition (a node unreachable while leadership
//! changes). Invariants checked around every fault:
//!
//! 1. every acknowledged object reads back whole;
//! 2. no node crosses its reserve;
//! 3. no node holds in-flight capacity once writes end;
//! 4. the published state is never less strict than the devices justify:
//!    after the fault it is at least the state before it;
//! 5. new data is refused (`SlowDown`) in write rejected and worse, also
//!    by a restarted gateway and against a stale record;
//! 6. deletes are admitted in every state;
//! 7. reads are served in every state;
//! 8. once space is back the state returns to normal and writes resume.
//!
//! FoundationDB loss with real processes is in `fdb_failover.rs`; the
//! metadata loss here fails every commit that writes (reads still
//! succeed).

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arca_lake_controller::process::{ControllerConfig, ControllerHandle, spawn};
use arca_lake_metadata::fault::{CommitFault, FaultKv};
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::admission::{self, AdmissionRecord};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::S3ErrorCode;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "capfail";
const STATES: [(u8, &str, f64); 5] = [
    (0, "normal", 0.50),
    (1, "warning", 0.15),
    (2, "throttled", 0.075),
    (3, "write_rejected", 0.035),
    (4, "recovery_only", 0.008),
];

type Cluster = TestCluster<FaultKv<MemKv>>;

#[derive(Debug, Clone, Copy)]
enum Fault {
    LeaderKill,
    GatewayRestart,
    NodeRestart,
    MetadataLoss,
    ReportLoss,
    StaleRecord,
    Partition,
}

fn controller(id: &str) -> ControllerConfig {
    let mut c = ControllerConfig::new(id);
    c.lease = Duration::from_millis(800);
    c.health_interval = Duration::from_millis(100);
    c.gc_interval = Duration::from_millis(200);
    c.gc_grace = Duration::ZERO;
    c.reconcile_interval = Duration::from_secs(3600);
    c.failure_domain = "host".into();
    c.admission.dwell_ms = 500;
    c.admission.stale_ms = 1500;
    // The placement threshold low enough that the free fraction alone
    // sets the state on these small allotments.
    c.admission.group_min_shards = 1;
    c.admission.min_rate_bytes_per_s = 32.0 * 1024.0 * 1024.0;
    c.admission_max_chunk_bytes = MIB;
    c
}

fn body(key: &str) -> Bytes {
    let seed = key
        .bytes()
        .fold(7u64, |h, b| h.wrapping_mul(31).wrapping_add(u64::from(b)));
    Bytes::from(random_bytes(usize::try_from(MIB).unwrap(), seed))
}

async fn record(c: &Cluster) -> AdmissionRecord {
    admission::current(c.meta.as_ref()).await.unwrap().unwrap()
}

async fn published(c: &Cluster) -> Option<(u64, u8)> {
    admission::current(c.meta.as_ref())
        .await
        .unwrap()
        .and_then(|r| r.tier("default").map(|t| (r.computed_ms, t.state)))
}

/// The next record computed after `after` (ms); the gateway sees it.
async fn fresh(c: &Cluster, after: u64, what: &str) -> u8 {
    let t = Instant::now();
    loop {
        if let Some((at, s)) = published(c).await
            && at > after
        {
            let rec = admission::current(c.meta.as_ref()).await.unwrap();
            c.backend.cluster().admission.observe(rec, Instant::now());
            return s;
        }
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "{what}: no fresh record"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Usable free fraction of the fullest node.
fn free_fraction(c: &Cluster) -> f64 {
    (0..c.nodes.len())
        .filter_map(|i| c.node_service(i).device_capacity().ok())
        .map(|d| {
            let usable = d.total_bytes.saturating_sub(d.reserved_bytes);
            #[allow(clippy::cast_precision_loss)]
            let f =
                d.available_bytes.saturating_sub(d.reserved_bytes) as f64 / usable.max(1) as f64;
            f
        })
        .fold(1.0, f64::min)
}

/// Cap every node so that the fullest has about `target` of its usable
/// space free (the allotment's reserve depends on the allotment: a few
/// rounds of adjusting).
async fn cap_to(c: &mut Cluster, target: f64) {
    let used: u64 = (0..c.nodes.len())
        .filter_map(|i| c.node_service(i).device_capacity().ok())
        .map(|d| d.total_bytes.saturating_sub(d.available_bytes))
        .max()
        .unwrap_or(0);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let mut cap = (used as f64 / (1.0 - target) * 1.4) as u64;
    for _ in 0..12 {
        for i in 0..c.nodes.len() {
            c.set_node_capacity(i, Some(cap)).await;
        }
        let f = free_fraction(c);
        if (f - target).abs() < target * 0.15 {
            return;
        }
        // Free space scales with the cap above what is used.
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let step = ((f - target) * cap as f64 * 0.7) as i64;
        cap = cap.saturating_add_signed(-step).max(used + MIB);
    }
    panic!(
        "could not cap the nodes to {target} free ({})",
        free_fraction(c)
    );
}

fn refused<T: std::fmt::Debug>(r: &Result<T, s3s::S3Error>) -> bool {
    matches!(r, Err(e) if *e.code() == S3ErrorCode::SlowDown)
}

struct Run {
    c: Cluster,
    ctl: Option<ControllerHandle>,
    generation: u32,
    keys: Vec<String>,
    target: u8,
    name: &'static str,
}

impl Run {
    async fn new_leader(&mut self) {
        self.generation += 1;
        let id = format!("ctl-{}", self.generation);
        self.ctl = Some(
            spawn(Arc::clone(&self.c.meta), controller(&id))
                .await
                .unwrap(),
        );
    }

    async fn kill_leader(&mut self) {
        if let Some(ctl) = self.ctl.take() {
            ctl.shutdown().await;
        }
    }

    /// Invariants 1, 2, 5, 6 and 7 with the state the gateway holds.
    async fn check(&mut self, when: &str) {
        let what = format!("{} / {when}", self.name);
        for k in &self.keys {
            assert_eq!(
                s3::get(&self.c.backend, BUCKET, k).await.unwrap().0,
                body(k),
                "{what}: {k}"
            );
        }
        let key = format!("probe-{when}-{}", now_ms());
        let r = s3::put(
            &self.c.backend,
            BUCKET,
            &key,
            body(&key),
            &PutOptions::default(),
        )
        .await;
        if self.target >= 3 {
            assert!(refused(&r), "{what}: new data admitted: {r:?}");
        } else if r.is_ok() {
            // Admitted (throttled admits at its rate): it reads back, and
            // goes again so that the state stays where it was.
            assert_eq!(
                s3::get(&self.c.backend, BUCKET, &key).await.unwrap().0,
                body(&key),
                "{what}"
            );
            s3::delete(&self.c.backend, BUCKET, &key).await.unwrap();
        }
        // A delete is admitted in every state (its reserve).
        let victim = self.keys.pop().expect("a key to delete");
        s3::delete(&self.c.backend, BUCKET, &victim)
            .await
            .unwrap_or_else(|e| panic!("{what}: delete refused: {e:?}"));
        let e = s3::get(&self.c.backend, BUCKET, &victim).await.unwrap_err();
        assert_eq!(*e.code(), S3ErrorCode::NoSuchKey, "{what}");
        self.reserves(&what).await;
    }

    /// Invariants 2 and 3.
    async fn reserves(&self, what: &str) {
        for i in 0..self.c.nodes.len() {
            if !self.c.node_running(i) {
                continue;
            }
            let t = Instant::now();
            loop {
                let d = self.c.node_service(i).device_capacity().unwrap();
                assert!(
                    d.available_bytes >= d.reserved_bytes,
                    "{what}: node {i} crossed its reserve: {d:?}"
                );
                if d.inflight_bytes == 0 {
                    break;
                }
                assert!(
                    t.elapsed() < Duration::from_secs(10),
                    "{what}: node {i} holds {} bytes",
                    d.inflight_bytes
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    /// Invariant 4: the next fresh record is at least the state reached.
    async fn not_relaxed(&self, after: u64, when: &str) {
        let s = fresh(&self.c, after, &format!("{} / {when}", self.name)).await;
        assert!(
            s >= self.target,
            "{} / {when}: published {s}, the devices are at {}",
            self.name,
            self.target
        );
    }
}

async fn scenario(fault: Fault) {
    for (target, state_name, free) in STATES {
        let name: &'static str = Box::leak(format!("{fault:?} in {state_name}").into_boxed_str());
        let mut c: Cluster = TestCluster::start_with(
            Arc::new(FaultKv::new(MemKv::default())),
            ClusterOptions::rs(3, 2, 1, MIB, 64 << 10),
        )
        .await;
        for i in 0..3 {
            c.set_node_capacity(i, Some(256 * MIB)).await;
        }
        s3::create_bucket(&c.backend, BUCKET).await.unwrap();
        let mut run = Run {
            c,
            ctl: None,
            generation: 0,
            keys: Vec::new(),
            target,
            name,
        };
        run.new_leader().await;
        fresh(&run.c, 0, name).await;
        for n in 0..40 {
            let k = format!("k{n}");
            s3::put(&run.c.backend, BUCKET, &k, body(&k), &PutOptions::default())
                .await
                .unwrap();
            run.keys.push(k);
        }
        // The devices to the target state: a new leader on the capped nodes.
        run.kill_leader().await;
        cap_to(&mut run.c, free).await;
        run.new_leader().await;
        let t = Instant::now();
        loop {
            let s = fresh(&run.c, now_ms(), name).await;
            if s == target {
                break;
            }
            assert!(
                t.elapsed() < Duration::from_secs(20),
                "{name}: reached state {s} ({:.3} free)",
                free_fraction(&run.c)
            );
        }
        run.check("before").await;
        let at = now_ms();
        match fault {
            Fault::LeaderKill => {
                run.kill_leader().await;
                run.new_leader().await;
            }
            Fault::GatewayRestart => {
                run.c.restart_backend().await.unwrap();
                // The restarted gateway decides from its first request.
                if target >= 3 {
                    let r = s3::put(
                        &run.c.backend,
                        BUCKET,
                        "first",
                        body("first"),
                        &PutOptions::default(),
                    )
                    .await;
                    assert!(refused(&r), "{name}: a restarted gateway admitted: {r:?}");
                }
            }
            Fault::NodeRestart => {
                run.c.stop_node(1).await;
                run.c.start_node(1).await;
            }
            Fault::MetadataLoss => {
                run.c
                    .meta
                    .inject(|w| !w.is_empty(), CommitFault::Unavailable, usize::MAX);
                tokio::time::sleep(Duration::from_secs(3)).await;
                // Reads still served from what the gateway holds.
                for k in &run.keys {
                    assert_eq!(
                        s3::get(&run.c.backend, BUCKET, k).await.unwrap().0,
                        body(k),
                        "{name}: during the outage"
                    );
                }
                run.c.meta.clear_rules();
            }
            Fault::ReportLoss => {
                run.c.stop_node(2).await;
                tokio::time::sleep(Duration::from_millis(2500)).await;
                run.not_relaxed(now_ms(), "a node silent").await;
                run.c.start_node(2).await;
            }
            Fault::StaleRecord => {
                // An older record of this incarnation saying normal.
                let mut old = record(&run.c).await;
                old.computed_ms = at.saturating_sub(60_000);
                old.generation = 0;
                for t in &mut old.tiers {
                    t.state = 0;
                    t.excluded_groups.clear();
                }
                run.c
                    .backend
                    .cluster()
                    .admission
                    .observe(Some(old), Instant::now());
            }
            Fault::Partition => {
                run.c.stop_node(2).await;
                run.kill_leader().await;
                run.new_leader().await;
                run.not_relaxed(now_ms(), "a new leader without node 2")
                    .await;
                run.c.start_node(2).await;
            }
        }
        if !matches!(fault, Fault::StaleRecord) {
            run.not_relaxed(now_ms(), "after").await;
        }
        run.check("after").await;
        // Space back: normal again, writes resume.
        run.kill_leader().await;
        for i in 0..3 {
            run.c.set_node_capacity(i, Some(512 * MIB)).await;
        }
        run.new_leader().await;
        let t = Instant::now();
        while fresh(&run.c, now_ms(), name).await != 0 {
            assert!(
                t.elapsed() < Duration::from_secs(30),
                "{name}: never normal"
            );
        }
        s3::put(
            &run.c.backend,
            BUCKET,
            "resumed",
            body("resumed"),
            &PutOptions::default(),
        )
        .await
        .unwrap_or_else(|e| panic!("{name}: writes did not resume: {e:?}"));
        run.reserves(name).await;
        eprintln!("capacity × failover: {name}: ok");
        run.kill_leader().await;
        run.c.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_kill_in_every_capacity_state() {
    scenario(Fault::LeaderKill).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_restart_in_every_capacity_state() {
    scenario(Fault::GatewayRestart).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_restart_in_every_capacity_state() {
    scenario(Fault::NodeRestart).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_loss_in_every_capacity_state() {
    scenario(Fault::MetadataLoss).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn report_loss_in_every_capacity_state() {
    scenario(Fault::ReportLoss).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_record_in_every_capacity_state() {
    scenario(Fault::StaleRecord).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_in_every_capacity_state() {
    scenario(Fault::Partition).await;
}
