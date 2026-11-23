//! The controller as a long-running process (RFC-011): leader election with
//! a lease and fencing token, and, while leader, scheduled garbage
//! collection, reconciliation, repair, and node health monitoring.
//!
//! Every metadata transaction a leader issues goes through
//! [`FencedKv`], so a process that lost leadership (lease expired, renewal
//! failed) cannot commit anything after its successor's first transaction.
//! Storage node RPCs issued by a stale leader are fenced by group epochs
//! (RFC-004 §5.4): a late delete or shard write carries the epoch under
//! which it was decided and is rejected by any member fenced since.

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::StreamExt as _;

use arca_lake_metadata::fence::FencedKv;
use arca_lake_metadata::guard::{GuardedKv, OpClass};
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::ops::leader;
use arca_lake_metadata::ops::repair_status::{self, RepairReason, RepairStatus};
use arca_lake_metadata::ops::reservation::{self, CapacityReservation, ReservationKind};
use arca_lake_metadata::ops::{bg_cursor, compat, epoch_intent, membership, node_lifecycle};
use arca_lake_metadata::records::{GroupMember, GroupRecord, GroupState, NodeRecord};
use arca_lake_metadata::{KvError, KvStore, MetaError};
use arca_lake_placement::{GroupSpec, choose_replacement};
use arca_lake_protocol::client::NodeClients;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::metrics::{Metrics, serve_http};
use crate::repair::RepairOptions;
use crate::repair_liveness::{self, Decision, Observation, Outcome};
use crate::{ControlError, gc, groups, repair};

/// Controller configuration.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// Identity in the leader record and in logs.
    pub id: String,
    /// Leader lease; renewed every third of it.
    pub lease: Duration,
    /// Interval between GC passes.
    pub gc_interval: Duration,
    /// Interval between reconciliation passes.
    pub reconcile_interval: Duration,
    /// Interval between reclaim passes (Phase 3.12, ADR-0028): retired
    /// records the metadata no longer needs, once the nodes' retention old.
    pub reclaim_interval: Duration,
    /// Interval between repair scheduler passes.
    pub repair_interval: Duration,
    /// Interval between health passes.
    pub health_interval: Duration,
    /// Interval of the leader's store watch (Phase 3.11, `trip::probe`):
    /// how soon a leader sees its metadata cleared, locked or replaced
    /// when its other tasks are idle. Zero disables it.
    pub store_watch_interval: Duration,
    /// A node is unhealthy when it has not been seen for this long.
    pub unhealthy_after: Duration,
    /// Replace unhealthy members automatically.
    pub auto_repair: bool,
    /// How member moves obtain shards when the leaving member answers
    /// (Phase 3.4 §10).
    pub repair_source: crate::repair::RepairSource,
    /// Grace period before a retired generation's shards are deleted.
    pub gc_grace: Duration,
    /// Open multipart uploads older than this are aborted by the sweep
    /// (RFC-007 §5.3.4, I-MP4); zero disables expiry.
    pub multipart_expiry: Duration,
    /// Minimum age for orphan shards without a generation (≥ gateway lease).
    pub min_orphan_age: Duration,
    /// Failure domain label for replacement choice.
    pub failure_domain: String,
    /// `ListShards`/`VerifyShards` page size (0 = node default).
    pub page_size: u32,
    /// Per-RPC timeout to storage nodes (derives the RPC policy when
    /// `rpc_policy` is `None`).
    pub node_timeout: Duration,
    /// Deadlines and retries of node RPCs (R-47).
    pub rpc_policy: Option<arca_lake_protocol::client::RpcPolicy>,
    /// Mutual TLS to storage nodes (ADR-0017); `None` = plaintext.
    pub tls: Option<Arc<arca_lake_protocol::tls::TlsContext>>,
    /// Metrics and health endpoint; `None` disables it.
    pub http_listen: Option<SocketAddr>,
    /// Cache tiers to evict (RFC-006 §5.4).
    pub cache_tiers: Vec<arca_lake_tiering::cache::CacheTierConfig>,
    /// Interval between eviction passes.
    pub evict_interval: Duration,
    /// Automatic promotion/demotion policy (RFC-006 §5.8); `None` disables.
    pub tiering: Option<crate::tiering::TieringPolicy>,
    /// Interval between tiering passes.
    pub tiering_interval: Duration,
    /// Capacity rebalancing between the groups of a tier (Phase 3.8);
    /// `None` disables it.
    pub rebalance: Option<crate::rebalance::RebalancePolicy>,
    /// Interval between rebalancing passes.
    pub rebalance_interval: Duration,
    /// Chunks a cache-group repair may reconstruct per pass before yielding
    /// (RFC-006 §5.9); 0 = unlimited.
    pub cache_repair_chunks_per_pass: usize,
    /// Chunks an operator's member move (a drain or replacement off a
    /// healthy node) may rebuild per pass before the pass yields to the
    /// repair of failed members (Phase 3.10 audit); 0 = unlimited.
    pub planned_move_chunks_per_pass: usize,
    /// Chunks the repair of a failed member rebuilds per group per pass
    /// (Phase 3.12 liveness audit): every group in repair progresses each
    /// pass instead of the last one waiting for every earlier one's whole
    /// rebuild; the walk resumes at its cursor. 0 = unlimited.
    pub failure_repair_chunks_per_pass: usize,
    /// `(group, node)` pairs a reconciliation pass lists at most (Phase
    /// 3.14): the sweep resumes at its persisted cursor; 0 = every pair
    /// (the default: rebalancing's settle time and in-transit credit assume
    /// a whole sweep per reconcile interval; review).
    pub reconcile_pairs_per_pass: usize,
    /// `(group, node)` pairs reconciled at once (Phase 4.1, ADR-0033; at
    /// least 1, at most [`LINEAGE_PAIRS`]).
    pub reconcile_parallel_pairs: usize,
    /// Nodes a reclaim pass works on at once (Phase 4.1, ADR-0033; at
    /// least 1).
    pub reclaim_parallel_nodes: usize,
    /// Generations a tiering pass examines at most (Phase 3.14): the sweep
    /// resumes at its persisted cursor; 0 = every generation.
    pub tiering_generations_per_pass: usize,
    /// Clock skew allowed between controllers and gateways in the grace
    /// after enabling tiering (ADR-0035 §10.4).
    pub tiering_clock_skew: Duration,
    /// Background scrubbing (RFC-008 §8.3); `interval_ms = 0` disables it.
    pub scrub: crate::integrity::ScrubOptions,
    /// Interval between scrub scheduler invocations (each resumes or starts
    /// the due groups).
    pub scrub_tick: Duration,
    /// Stripe repair worker (RFC-008 §8.5).
    pub stripe_repair: crate::integrity::RepairWorkerOptions,
    /// Interval between stripe repair worker passes.
    pub stripe_repair_interval: Duration,
    /// Heartbeat interval of the `("al","proc",id)` record (RFC-014 §5.5);
    /// `None` disables it.
    pub heartbeat: Option<Duration>,
    /// Interval between recovery state machine passes (RFC-015 §5.3).
    pub recovery_interval: Duration,
    /// Recovery pass options.
    pub recovery: crate::recovery::RecoveryOptions,
    /// Interval between upgrade finalize passes (RFC-014 §5.5).
    pub upgrade_interval: Duration,
    /// Write admission thresholds (Phase 3.6).
    pub admission: crate::backpressure::Policy,
    /// Largest chunk the gateways write: a group whose fullest member has
    /// no room for 64 of its shards leaves placement.
    pub admission_max_chunk_bytes: u64,
    /// Test hook: a fault hook on this process's node RPCs (Phase 3.11,
    /// deterministic delete delays). `None` in deployments.
    pub rpc_fault_hook: Option<RpcFaultHook>,
    /// Retry delays of a repair that makes no progress (Phase 3.13).
    pub repair_pacing: crate::repair_liveness::Pacing,
}

impl ControllerConfig {
    /// Defaults for a process named `id`. Intervals are production-ish
    /// (seconds); tests shorten them.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            lease: Duration::from_secs(10),
            gc_interval: Duration::from_secs(30),
            reclaim_interval: Duration::from_secs(60),
            reconcile_interval: Duration::from_mins(5),
            repair_interval: Duration::from_secs(15),
            health_interval: Duration::from_secs(5),
            store_watch_interval: Duration::from_millis(250),
            unhealthy_after: Duration::from_secs(60),
            auto_repair: false,
            repair_source: crate::repair::RepairSource::Reconstruct,
            gc_grace: Duration::from_mins(10),
            multipart_expiry: Duration::from_hours(7 * 24),
            min_orphan_age: Duration::from_mins(15),
            failure_domain: "host".into(),
            page_size: 0,
            node_timeout: Duration::from_secs(5),
            rpc_policy: None,
            tls: None,
            http_listen: None,
            cache_tiers: Vec::new(),
            evict_interval: Duration::from_secs(10),
            tiering: None,
            tiering_interval: Duration::from_mins(5),
            rebalance: None,
            rebalance_interval: Duration::from_mins(1),
            cache_repair_chunks_per_pass: 256,
            planned_move_chunks_per_pass: 512,
            reconcile_pairs_per_pass: 0,
            reconcile_parallel_pairs: 4,
            reclaim_parallel_nodes: 4,
            tiering_generations_per_pass: 4096,
            tiering_clock_skew: Duration::from_secs(60),
            failure_repair_chunks_per_pass: 4096,
            scrub: crate::integrity::ScrubOptions::default(),
            scrub_tick: Duration::from_secs(60),
            stripe_repair: crate::integrity::RepairWorkerOptions::default(),
            stripe_repair_interval: Duration::from_secs(15),
            heartbeat: Some(Duration::from_secs(30)),
            recovery_interval: Duration::from_secs(2),
            recovery: crate::recovery::RecoveryOptions::default(),
            upgrade_interval: Duration::from_secs(5),
            admission: crate::backpressure::Policy::default(),
            admission_max_chunk_bytes: 8 << 20,
            rpc_fault_hook: None,
            repair_pacing: crate::repair_liveness::Pacing::default(),
        }
    }
}

/// See [`ControllerConfig::rpc_fault_hook`].
#[derive(Clone)]
pub struct RpcFaultHook(pub arca_lake_protocol::client::FaultHook);

impl std::fmt::Debug for RpcFaultHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RpcFaultHook")
    }
}

/// A running controller.
pub struct ControllerHandle {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
    /// Metrics (shared with the HTTP endpoint).
    pub metrics: Arc<Metrics>,
    /// Bound address of the HTTP endpoint, if enabled.
    pub http_addr: Option<SocketAddr>,
}

impl ControllerHandle {
    /// Whether this process is currently leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.metrics.leader.load(Ordering::Relaxed)
    }

    /// Current fencing token (0 when not leader).
    #[must_use]
    pub fn token(&self) -> u64 {
        self.metrics.leader_token.load(Ordering::Relaxed)
    }

    /// Graceful shutdown: stop schedulers, release leadership, exit.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }

    /// Simulate a crash: abandon the process without releasing the lease.
    pub fn abort(self) {
        self.task.abort();
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Start a controller on `meta`.
///
/// # Errors
///
/// Binding the HTTP endpoint.
pub async fn spawn<S: KvStore + 'static>(
    meta: Arc<S>,
    cfg: ControllerConfig,
) -> std::io::Result<ControllerHandle> {
    // Startup gate (RFC-014 §5.4): refuse a schema outside this build's
    // window; initialise an empty store; stamp a legacy store.
    let gate = compat::check_startup(
        meta.as_ref(),
        &crate::capabilities(),
        compat::Stamp::Legacy,
        &cfg.id,
        now_ms(),
    )
    .await
    .map_err(|e| std::io::Error::other(format!("startup gate: {e}")))?;
    let state = gate
        .cluster
        .clone()
        .ok_or_else(|| std::io::Error::other("cluster state record missing"))?;
    arca_lake_metadata::records::set_schema_write(gate.schema_write.unwrap_or(0));
    // A cluster an earlier release initialised gets its uid (Phase 3.12):
    // `admin restore` tells this cluster's backups from others' by it.
    if matches!(
        state.state,
        arca_lake_metadata::records::ClusterPhase::Active
            | arca_lake_metadata::records::ClusterPhase::ReadOnly
    ) {
        compat::ensure_cluster_uid(meta.as_ref())
            .await
            .map_err(|e| std::io::Error::other(format!("cluster uid: {e}")))?;
    }
    info!(
        controller = %cfg.id,
        schema = ?gate.schema,
        schema_write = ?gate.schema_write,
        format_write = ?gate.format_write,
        phase = state.state.name(),
        incarnation = %compat::hex(&state.incarnation),
        "controller startup gate passed"
    );
    let metrics = Arc::new(Metrics::default());
    if let Some(t) = &cfg.tls {
        let _ = metrics.tls.set(Arc::clone(&t.stats));
    }
    metrics
        .cluster_phase
        .store(phase_code(state.state), Ordering::Relaxed);
    let (tx, rx) = watch::channel(false);
    let mut http_addr = None;
    if let Some(addr) = cfg.http_listen {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        http_addr = Some(listener.local_addr()?);
        tokio::spawn(serve_http(listener, Arc::clone(&metrics), rx.clone()));
    }
    let m = Arc::clone(&metrics);
    let task = tokio::spawn(run(meta, cfg, m, rx, state.incarnation));
    Ok(ControllerHandle {
        shutdown: tx,
        task,
        metrics,
        http_addr,
    })
}

/// Election loop: follower until the lease can be taken, leader until a
/// renewal fails or shutdown is requested.
async fn run<S: KvStore + 'static>(
    meta: Arc<S>,
    cfg: ControllerConfig,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
    incarnation: [u8; 16],
) {
    #[cfg(feature = "fault-injection")]
    warn!("tiering fault injection is compiled in (test build)");
    let renew_every = cfg.lease / 3;
    // The leader record names this process, not only its configured id: a
    // second process started with the same id (a restart while the old
    // one is paused) must not inherit the old one's token, or fencing
    // could not tell them apart (Phase 3.5 audit SEC-4).
    let holder = leader_holder(&cfg.id);
    // Heartbeat (RFC-014 §5.5) and `schema_write` refresh, leader or not.
    if let Some(every) = cfg.heartbeat {
        let meta = Arc::clone(&meta);
        let id = cfg.id.clone();
        let mut stop = shutdown.clone();
        tokio::spawn(async move {
            loop {
                let rec = arca_lake_metadata::records::ProcRecord {
                    id: id.clone(),
                    kind: arca_lake_metadata::records::ProcKind::Controller,
                    software: arca_lake_common::compat::software_version(),
                    caps: crate::capabilities(),
                    incarnation,
                    last_ms: now_ms(),
                };
                let _ = compat::heartbeat(meta.as_ref(), &rec).await;
                if let Ok(g) = compat::gate(meta.as_ref()).await
                    && let Some(sw) = g.schema_write
                {
                    arca_lake_metadata::records::set_schema_write(sw);
                }
                tokio::select! {
                    () = tokio::time::sleep(every) => {}
                    _ = stop.changed() => {
                        let _ = compat::remove_heartbeat(meta.as_ref(), &id).await;
                        return;
                    }
                }
            }
        });
    }
    // Logged once per stretch as a follower: who holds the lease.
    let mut standing_by = false;
    loop {
        if *shutdown.borrow() {
            return;
        }
        // Follower: try to acquire.
        let acquired =
            match leader::try_acquire_initialized(meta.as_ref(), &holder, now_ms(), ms(cfg.lease))
                .await
            {
                Ok(l) => l,
                Err(e) => {
                    warn!(controller = %cfg.id, error = %e, "leader acquisition failed");
                    None
                }
            };
        let Some(leadership) = acquired else {
            if !standing_by && let Ok(Some(l)) = leader::current(meta.as_ref()).await {
                info!(controller = %cfg.id, holder = %l.holder, token = l.token, "standing by: the lease is held");
                standing_by = true;
            }
            tokio::select! {
                () = tokio::time::sleep(renew_every) => {}
                _ = shutdown.changed() => return,
            }
            continue;
        };
        info!(controller = %cfg.id, token = leadership.token, "acquired leadership");
        standing_by = false;
        metrics.leader.store(true, Ordering::Relaxed);
        metrics
            .leader_token
            .store(leadership.token, Ordering::Relaxed);
        metrics
            .leader_acquired_total
            .fetch_add(1, Ordering::Relaxed);

        // Leader: fenced store, scheduler tasks, renewal loop.
        let fenced = Arc::new(FencedKv::new(Arc::clone(&meta), leadership.clone()));
        let (stop_tx, stop_rx) = watch::channel(false);
        // The node schema shard writes need (ADR-0039) before the first
        // health pass sets it; unknown (the read failed) refuses every
        // write until then.
        let node_schema = compat::gate(meta.as_ref())
            .await
            .map_or(u64::MAX, |g| compat::required_node_schema(g.schema_write));
        let (tasks, trip) =
            start_leader_tasks(&fenced, &cfg, &metrics, &stop_rx, incarnation, node_schema);
        let lost = hold_term(
            meta.as_ref(),
            &cfg,
            &leadership,
            &trip,
            &mut shutdown,
            renew_every,
        )
        .await;
        // Deletes stop now, not when the tasks have finished their passes
        // (Phase 3.11: a batch would go on sending).
        trip.stop(true, true, false);
        let _ = stop_tx.send(true);
        for t in tasks {
            let _ = t.await;
        }
        metrics.leader.store(false, Ordering::Relaxed);
        metrics.leader_token.store(0, Ordering::Relaxed);
        if lost {
            metrics.leader_lost_total.fetch_add(1, Ordering::Relaxed);
            warn!(controller = %cfg.id, token = leadership.token, "lost leadership");
        } else {
            let _ = leader::release(meta.as_ref(), &leadership).await;
            info!(controller = %cfg.id, "released leadership");
            return;
        }
    }
}

/// Renew the lease until it is lost (`true`), the term is tripped, or a
/// shutdown is asked (`false`).
async fn hold_term<S: KvStore>(
    meta: &S,
    cfg: &ControllerConfig,
    leadership: &leader::Leadership,
    trip: &crate::trip::Trip,
    shutdown: &mut watch::Receiver<bool>,
    renew_every: Duration,
) -> bool {
    loop {
        tokio::select! {
            () = tokio::time::sleep(renew_every) => {}
            () = trip.stepped_down() => {
                warn!(controller = %cfg.id, "stepping down: the store was replaced or another leader took over");
                break true;
            }
            _ = shutdown.changed() => break false,
        }
        match leader::renew(meta, leadership, now_ms(), ms(cfg.lease)).await {
            Ok(true) => {}
            Ok(false) => {
                lost_lease(meta, leadership, trip).await;
                break true;
            }
            Err(e) => {
                renewal_failed(trip, &e);
                warn!(controller = %cfg.id, error = %e, "lease renewal failed; retrying");
                // Keep going until the lease would have expired: a
                // transient store error must not flap leadership. The
                // fence on every transaction keeps this safe regardless.
            }
        }
    }
}

/// Quarantine a node; while a managed restore holds node changes, not
/// recorded (the node is still skipped this pass; re-review: one refusal
/// must not fail the whole health pass).
async fn quarantine_unless_held<S: KvStore>(
    meta: &S,
    node_id: u64,
    reason: &str,
    now: u64,
) -> Result<bool, ControlError> {
    match node_lifecycle::quarantine(meta, node_id, reason, now).await {
        Ok(q) => Ok(q),
        Err(e) if node_lifecycle::is_restore_in_progress(&e) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// The lease is lost: if the record is gone or another term's, the trip
/// learns why, so that a replaced store is remembered (Phase 3.11 review).
async fn lost_lease<S: KvStore>(
    meta: &S,
    leadership: &leader::Leadership,
    trip: &crate::trip::Trip,
) {
    if let Some(e) = crate::trip::probe(meta, leadership).await {
        trip.observe(&e);
    }
}

/// A failed lease renewal (Phase 3.11): a locked or replaced store ends
/// the term at once; any other failure stops the deletes until a health
/// pass succeeds again (a leader that cannot show its lease does not
/// delete). The lease itself is kept.
fn renewal_failed(trip: &crate::trip::Trip, e: &arca_lake_metadata::MetaError) {
    if let arca_lake_metadata::MetaError::Kv(k) = e {
        trip.observe(k);
    }
    trip.stop(false, false, false);
}

/// The leader's store watch (Phase 3.11): a probe of the store every
/// `every`, so that a restore under a leader whose tasks are idle or
/// between transactions is seen within one interval.
async fn store_watch<S: KvStore>(
    fenced: Arc<FencedKv<S>>,
    trip: Arc<crate::trip::Trip>,
    every: Duration,
    mut stop: watch::Receiver<bool>,
) {
    let leadership = fenced.leadership().clone();
    loop {
        tokio::select! {
            () = tokio::time::sleep(every) => {}
            _ = stop.changed() => return,
        }
        if let Some(e) = crate::trip::probe(fenced.inner().as_ref(), &leadership).await {
            trip.observe(&e);
        }
    }
}

/// This process's name in the leader record: the configured id and a
/// random instance suffix.
fn leader_holder(id: &str) -> String {
    let r = arca_lake_metadata::records::new_gen_id();
    let suffix = r[8..].iter().fold(String::new(), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    });
    format!("{id}@{suffix}")
}

#[allow(clippy::too_many_lines)]
fn start_leader_tasks<S: KvStore + 'static>(
    fenced: &Arc<FencedKv<S>>,
    cfg: &ControllerConfig,
    metrics: &Arc<Metrics>,
    stop: &watch::Receiver<bool>,
    incarnation: [u8; 16],
    node_schema: u64,
) -> (Vec<JoinHandle<()>>, Arc<crate::trip::Trip>) {
    let clients =
        NodeClients::with_policy(cfg.rpc_policy.clone().unwrap_or_else(|| {
            arca_lake_protocol::client::RpcPolicy::from_timeout(cfg.node_timeout)
        }))
        .with_tls(cfg.tls.clone());
    clients.set_incarnation(Some(incarnation));
    clients.set_required_node_schema(node_schema);
    clients.set_fault_hook(cfg.rpc_fault_hook.as_ref().map(|h| h.0.clone()));
    // Every task's store trips the term's deletes off on a failure that
    // means the metadata is no longer the one they were decided on.
    let trip = Arc::new(crate::trip::Trip::with_known(
        clients.clone(),
        metrics.delete_fence.suspect_flag(),
        {
            let metrics = Arc::clone(metrics);
            Arc::new(move || {
                metrics
                    .delete_fence
                    .max_generation()
                    .max(metrics.delete_fence.generation())
            })
        },
    ));
    let token = fenced.leadership().token;
    let watch = (!cfg.store_watch_interval.is_zero()).then(|| {
        tokio::spawn(store_watch(
            Arc::clone(fenced),
            Arc::clone(&trip),
            cfg.store_watch_interval,
            stop.clone(),
        ))
    });
    let inner = Arc::clone(fenced);
    let fenced = &Arc::new(crate::trip::TripKv::new(
        Arc::clone(fenced),
        Arc::clone(&trip),
    ));
    let coders = crate::integrity::coders();
    // One guarded store per operation class (RFC-015 §5.6, ADR-0016): every
    // write of a task is checked against the cluster state at commit. The
    // trip wraps the guard (review): the guard's own refusals (a cluster
    // state gone or changed) trip too.
    let guarded = |class: OpClass| {
        Arc::new(crate::trip::TripKv::new(
            Arc::new(GuardedKv::new(Arc::clone(&inner), incarnation, class)),
            Arc::clone(&trip),
        ))
    };
    let meta_delete = guarded(OpClass::Delete);
    let meta_repair = guarded(OpClass::Repair);
    let meta_book = guarded(OpClass::Bookkeeping);
    let meta_admin = guarded(OpClass::Admin);
    let raw = Arc::clone(fenced);
    let handles = vec![
        tokio::spawn(scheduler(
            "gc",
            cfg.gc_interval,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Delete,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_delete);
                let clients = clients.clone();
                let cfg = cfg.clone();
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    async move { gc_pass(meta.as_ref(), &clients, &cfg, &metrics).await }
                }
            },
        )),
        tokio::spawn(scheduler(
            "reconcile",
            cfg.reconcile_interval,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Delete,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_delete);
                let clients = clients.clone();
                let cfg = cfg.clone();
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    async move { reconcile_pass(meta.as_ref(), &clients, &cfg, &metrics).await }
                }
            },
        )),
        tokio::spawn(scheduler(
            "reclaim",
            cfg.reclaim_interval,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Delete,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_delete);
                let clients = clients.clone();
                let cfg = cfg.clone();
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    async move { reclaim_pass(meta.as_ref(), &clients, &cfg, &metrics).await }
                }
            },
        )),
        tokio::spawn(scheduler(
            "health",
            cfg.health_interval,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Bookkeeping,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_book);
                let clients = clients.clone();
                let cfg = cfg.clone();
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    async move {
                        health_pass(meta.as_ref(), &clients, &cfg, &metrics, incarnation).await
                    }
                }
            },
        )),
        tokio::spawn(scheduler(
            "evict",
            cfg.evict_interval,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Delete,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_delete);
                let clients = clients.clone();
                let cfg = cfg.clone();
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    async move { evict_pass(meta.as_ref(), &clients, &cfg, &metrics).await }
                }
            },
        )),
        tokio::spawn(scheduler(
            "rebalance",
            cfg.rebalance_interval,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Delete,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_delete);
                let clients = clients.clone();
                let cfg = cfg.clone();
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    async move { rebalance_pass(meta.as_ref(), &clients, &cfg, &metrics).await }
                }
            },
        )),
        tokio::spawn(scheduler(
            "tiering",
            cfg.tiering_interval,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Delete,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_delete);
                let clients = clients.clone();
                let cfg = cfg.clone();
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    async move {
                        tiering_pass(meta.as_ref(), &clients, &cfg, &metrics, incarnation).await
                    }
                }
            },
        )),
        tokio::spawn(scheduler(
            "repair",
            cfg.repair_interval,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Repair,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_repair);
                let clients = clients.clone();
                let cfg = cfg.clone();
                let estimates = Arc::new(crate::capacity::Estimates::default());
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    let estimates = Arc::clone(&estimates);
                    async move { repair_pass(meta.as_ref(), &clients, &cfg, &metrics, &estimates).await }
                }
            },
        )),
        tokio::spawn(scheduler(
            "scrub",
            cfg.scrub_tick,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Bookkeeping,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_book);
                let clients = clients.clone();
                let cfg = cfg.clone();
                let coders = Arc::clone(&coders);
                let stop = stop.clone();
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    let coders = Arc::clone(&coders);
                    let stop = stop.clone();
                    async move {
                        crate::integrity::scrub_pass(
                            meta.as_ref(),
                            &clients,
                            &coders,
                            &cfg.scrub,
                            Some(&metrics),
                            Some(&stop),
                            now_ms(),
                        )
                        .await
                        .map(|_| ())
                    }
                }
            },
        )),
        tokio::spawn(scheduler(
            "stripe-repair",
            cfg.stripe_repair_interval,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Repair,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_repair);
                let clients = clients.clone();
                let cfg = cfg.clone();
                let stop = stop.clone();
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    let coders = Arc::clone(&coders);
                    let stop = stop.clone();
                    async move {
                        crate::integrity::repair_pass(
                            meta.as_ref(),
                            &clients,
                            &coders,
                            &cfg.stripe_repair,
                            Some(&metrics),
                            Some(&stop),
                            now_ms(),
                        )
                        .await
                        .map(|_| ())
                    }
                }
            },
        )),
        tokio::spawn(scheduler(
            "recovery",
            cfg.recovery_interval,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Admin,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_admin);
                let clients = clients.clone();
                let cfg = cfg.clone();
                let stop = stop.clone();
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    let stop = stop.clone();
                    async move {
                        let r = crate::recovery::recovery_pass(
                            meta.as_ref(),
                            &clients,
                            &cfg.recovery,
                            Some(&stop),
                            &cfg.id,
                            now_ms(),
                        )
                        .await?;
                        if r.epochs_raised > 0 || r.finished || r.phase_before != r.phase_after {
                            metrics
                                .recovery_passes_total
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        if let Some(p) = r.phase_after {
                            metrics
                                .cluster_phase
                                .store(phase_code(p), Ordering::Relaxed);
                        }
                        Ok(())
                    }
                }
            },
        )),
        tokio::spawn(scheduler(
            "upgrade",
            cfg.upgrade_interval,
            stop.clone(),
            Arc::clone(metrics),
            OpClass::Admin,
            Arc::clone(&raw),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(&meta_admin);
                let clients = clients.clone();
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let metrics = Arc::clone(&metrics);
                    async move {
                        let r = crate::upgrade::upgrade_pass(
                            meta.as_ref(),
                            &clients,
                            &crate::capabilities(),
                            token,
                            now_ms(),
                        )
                        .await?;
                        metrics
                            .upgrade_step
                            .store(r.after.map_or(0, |st| st as u64 + 1), Ordering::Relaxed);
                        Ok(())
                    }
                }
            },
        )),
    ];
    let mut handles = handles;
    handles.extend(watch);
    (handles, trip)
}

/// Metric code of a phase.
fn phase_code(p: arca_lake_metadata::records::ClusterPhase) -> u64 {
    use arca_lake_metadata::records::ClusterPhase as P;
    match p {
        P::Active => 0,
        P::ReadOnly => 1,
        P::Restoring => 2,
        P::VerificationPending => 3,
        P::RecoveryRequired => 4,
    }
}

/// Run `pass` every `interval`, with exponential backoff (capped at eight
/// intervals) after errors, until `stop` fires. A fenced error ends the
/// task at once: leadership is gone.
async fn scheduler<S, F, Fut>(
    name: &'static str,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
    class: OpClass,
    state_store: Arc<S>,
    mut pass: F,
) where
    S: KvStore + 'static,
    F: FnMut() -> Fut + Send,
    Fut: std::future::Future<Output = Result<(), ControlError>> + Send,
{
    let mut failures: u32 = 0;
    loop {
        // RFC-015 §5.6: a pass runs only in a state that admits its class
        // (every write inside is checked again at commit by the guard).
        let admitted = match compat::cluster_state(state_store.as_ref()).await {
            Ok(Some(st)) => {
                metrics
                    .cluster_phase
                    .store(phase_code(st.state), Ordering::Relaxed);
                st.state.admits(class)
            }
            Ok(None) => false,
            Err(e) => {
                warn!(task = name, error = %e, "cluster state unreadable; pass skipped");
                false
            }
        };
        if !admitted {
            metrics
                .passes_skipped_by_state_total
                .fetch_add(1, Ordering::Relaxed);
            tokio::select! {
                () = tokio::time::sleep(interval) => {}
                _ = stop.changed() => return,
            }
            continue;
        }
        let pass_started = std::time::Instant::now();
        metrics.note_pass_start(name, now_ms());
        let result = pass().await;
        metrics.note_pass(name, pass_started.elapsed());
        match result {
            Ok(()) => failures = 0,
            Err(e) => {
                metrics.task_errors_total.fetch_add(1, Ordering::Relaxed);
                if is_fenced(&e) {
                    warn!(task = name, error = %e, "fenced; stopping task");
                    return;
                }
                if is_state_refusal(&e) {
                    metrics.guard_refusals_total.fetch_add(1, Ordering::Relaxed);
                }
                failures = failures.saturating_add(1);
                warn!(task = name, failures, error = %e, "pass failed");
            }
        }
        let backoff = interval * 2u32.saturating_pow(failures.min(3));
        tokio::select! {
            () = tokio::time::sleep(backoff) => {}
            _ = stop.changed() => return,
        }
    }
}

pub(crate) fn is_fenced(e: &ControlError) -> bool {
    matches!(e, ControlError::Meta(MetaError::Kv(KvError::Fenced(_))))
}

fn is_state_refusal(e: &ControlError) -> bool {
    matches!(
        e,
        ControlError::Meta(MetaError::Kv(KvError::ClusterState(_)))
    )
}

/// Pairs a reconciliation pass decides between two lineage advances.
pub const LINEAGE_PAIRS: usize = 8;

/// Generations collected per GC batch.
const GC_BATCH: usize = 1000;
/// Retired-queue entries counted for the backlog gauge at most.
const GC_BACKLOG_CAP: usize = 200_000;
/// Shortest budget of a GC pass for running batches; the budget is the GC
/// interval when that is longer, so that a backlog is worked on without
/// idle intervals (R-64: a 30 s budget every 60 s halved the throughput).
const GC_PASS_BUDGET: Duration = Duration::from_secs(30);

async fn gc_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
) -> Result<(), ControlError> {
    // Batches until one comes back short (the backlog is drained) or the
    // pass has run for GC_PASS_BUDGET: one batch per interval could not
    // keep up with a sustained overwrite/delete rate (soak test).
    let started = std::time::Instant::now();
    loop {
        let parts_before = metrics.gc.parts_retired.load(Ordering::Relaxed);
        advance_lineage(meta, metrics).await?;
        let report = gc::run_once_with(
            meta,
            clients,
            now_ms(),
            ms(cfg.gc_grace),
            GC_BATCH,
            &metrics.gc,
        )
        .await?;
        metrics.gc_passes_total.fetch_add(1, Ordering::Relaxed);
        metrics
            .gc_collected_total
            .fetch_add(report.collected as u64, Ordering::Relaxed);
        metrics
            .shards_deleted_total
            .fetch_add(report.shards_deleted as u64, Ordering::Relaxed);
        metrics
            .gc_skipped_referenced_total
            .fetch_add(report.skipped_referenced as u64, Ordering::Relaxed);
        if report.skipped_referenced > 0 {
            warn!(
                skipped = report.skipped_referenced,
                "retired queue names referenced generations (invariant violation)"
            );
        }
        info!(?report, "gc pass");
        // Parts a multipart root retired are due at once (retired as of
        // the root): another batch collects them in this pass.
        let parts_due = metrics.gc.parts_retired.load(Ordering::Relaxed) > parts_before;
        if parts_due {
            // Parts are queued as of their root, possibly behind the
            // cursor: from the head again (re-review).
            *metrics
                .gc
                .cursor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
        // A full page of the queue also continues (past generations that
        // are deferred, Phase 3.10 audit).
        let full =
            report.collected >= GC_BATCH || report.expired >= GC_BATCH || parts_due || report.more;
        if !full || started.elapsed() >= GC_PASS_BUDGET.max(cfg.gc_interval) {
            break;
        }
    }
    // What is left for the next pass (counted up to GC_BACKLOG_CAP).
    if let Ok(b) = arca_lake_metadata::ops::gc::backlog_bytes(meta, GC_BACKLOG_CAP).await {
        metrics
            .gc_backlog
            .store(b.entries as u64, Ordering::Relaxed);
        metrics.gc_backlog_oldest_age_seconds.store(
            b.oldest_ms.map_or(0, |t| now_ms().saturating_sub(t) / 1000),
            Ordering::Relaxed,
        );
        metrics.gc_backlog_bytes.store(b.bytes, Ordering::Relaxed);
        metrics.admission.note_backlog(b.bytes, now_ms());
    }
    let expire_before = (!cfg.multipart_expiry.is_zero())
        .then(|| now_ms().saturating_sub(ms(cfg.multipart_expiry)));
    let sweep = gc::sweep_multipart(meta, now_ms(), expire_before, 1000).await?;
    if sweep != gc::SweepReport::default() {
        info!(?sweep, "multipart sweep");
    }
    Ok(())
}

/// Advance the lineage counter before a batch of delete decisions and
/// remember it (see `delete_fence::advance_lineage`).
async fn advance_lineage<S: KvStore>(meta: &S, metrics: &Metrics) -> Result<(), ControlError> {
    // One lineage operation at a time in this process (re-review: a GC
    // batch and the health pass interleaving could leave the remembered
    // value equal to a restored one).
    let _one = metrics.lineage_lock.lock().await;
    let remembered = metrics.delete_fence.lineage();
    let v = arca_lake_metadata::ops::delete_fence::advance_lineage(meta, remembered).await?;
    metrics.delete_fence.set_lineage(v.max(remembered));
    Ok(())
}

/// One reclaim pass over every node in service (Phase 3.12, ADR-0028).
async fn reclaim_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
) -> Result<(), ControlError> {
    let retired: BTreeSet<u64> = node_lifecycle::retirements(meta)
        .await?
        .into_iter()
        .map(|r| r.node_id)
        .collect();
    let mut not_deferring = 0;
    let nodes: Vec<NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .filter(|n| !retired.contains(&n.id))
        .collect();
    // Nodes are independent (ADR-0033): a few at once.
    let work: Vec<_> = nodes
        .iter()
        .map(|n| {
            Box::pin(async move {
                (
                    n.id,
                    crate::gc::reclaim_node(meta, clients, n.id, &n.address, cfg.page_size, now_ms)
                        .await,
                )
            })
        })
        .collect();
    let results: Vec<_> = futures::stream::iter(work)
        .buffered(cfg.reclaim_parallel_nodes.max(1))
        .collect()
        .await;
    for (node, result) in results {
        // One node's failure never stops the others (review).
        match result {
            Ok(r) => {
                not_deferring += r.nodes_not_deferring;
                metrics
                    .reclaimed_total
                    .fetch_add(r.reclaimed, Ordering::Relaxed);
                metrics
                    .reclaimed_bytes_total
                    .fetch_add(r.reclaimed_bytes, Ordering::Relaxed);
                metrics
                    .room_returned_bytes
                    .fetch_add(r.reclaimed_bytes, Ordering::Relaxed);
                metrics
                    .reclaim_refused_total
                    .fetch_add(r.refused, Ordering::Relaxed);
                metrics
                    .reclaim_errors_total
                    .fetch_add(r.errors, Ordering::Relaxed);
            }
            Err(e) => {
                metrics.reclaim_errors_total.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(node, error = %e, "reclaim pass: node skipped until the next pass");
            }
        }
    }
    metrics
        .nodes_not_deferring
        .store(not_deferring, Ordering::Relaxed);
    // Which nodes defer is taken from their health every health pass
    // (Phase 3.14): a reclaim pass's error on a node says nothing about its
    // retention (a new leader's first pass, before its fence is open,
    // could fail on every node and count them all as deferring, so GC's
    // deletes were not counted as room for a reclaim interval).
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn reconcile_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
) -> Result<(), ControlError> {
    // Not removed or quarantined nodes (Phase 3.11, review): what answers
    // there may be another disk.
    let retired: BTreeSet<u64> = node_lifecycle::retirements(meta)
        .await?
        .into_iter()
        .map(|r| r.node_id)
        .collect();
    let nodes: Vec<NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .filter(|n| !retired.contains(&n.id))
        .collect();
    let groups = cluster::list_groups(meta).await?;
    // A bounded slice of the sweep from its persisted cursor (Phase 3.14):
    // no pass lists every pair of a large cluster, a new leader resumes
    // where the last pass stopped, and a pair that fails is passed over.
    let mut cursor = bg_cursor::get(meta, bg_cursor::RECONCILE).await?;
    let budget = if cfg.reconcile_pairs_per_pass == 0 {
        usize::MAX
    } else {
        cfg.reconcile_pairs_per_pass
    };
    let batch = crate::sweep::next_pairs(
        bg_cursor::parse_pair(&cursor.after),
        &groups.iter().map(|g| g.id).collect::<Vec<_>>(),
        &nodes.iter().map(|n| n.id).collect::<Vec<_>>(),
        budget,
    );
    if cursor.after.is_empty() {
        // Recorded at once (review): the settle time reads a sweep in
        // progress from it, whoever leads.
        cursor.sweep_started_ms = now_ms();
        bg_cursor::put(meta, bg_cursor::RECONCILE, &cursor).await?;
    }
    let by_group: HashMap<u64, &GroupRecord> = groups.iter().map(|g| (g.id, g)).collect();
    let by_node: HashMap<u64, &NodeRecord> = nodes.iter().map(|n| (n.id, n)).collect();
    let mut deleted = 0usize;
    let mut unreachable = 0usize;
    let mut errors = 0u64;
    let mut done = 0u64;
    // A few pairs at once (ADR-0033), in slices of `LINEAGE_PAIRS`: before
    // each slice the lineage advances (the batch a restore under this
    // leader could leave undetected stays small, at a commit per slice, not
    // per (group, node) pair; re-review) and the cursor records the pairs
    // before it, all done.
    let parallel = cfg.reconcile_parallel_pairs.clamp(1, LINEAGE_PAIRS);
    for (c, slice) in batch.pairs.chunks(LINEAGE_PAIRS).enumerate() {
        let first = c * LINEAGE_PAIRS;
        advance_lineage(meta, metrics).await?;
        if first > 0 {
            cursor.after =
                bg_cursor::pair_position(batch.pairs[first - 1].0, batch.pairs[first - 1].1);
            bg_cursor::put(meta, bg_cursor::RECONCILE, &cursor).await?;
        }
        let work: Vec<_> = slice
            .iter()
            .enumerate()
            .map(|(k, &(gid, nid))| {
                let (g, n) = (by_group.get(&gid).copied(), by_node.get(&nid).copied());
                Box::pin(async move {
                    let (Some(g), Some(n)) = (g, n) else {
                        return (k, None);
                    };
                    (
                        k,
                        Some(
                            gc::reconcile_node(
                                meta,
                                clients,
                                n.id,
                                &n.address,
                                g,
                                now_ms(),
                                ms(cfg.min_orphan_age),
                                cfg.page_size,
                            )
                            .await,
                        ),
                    )
                })
            })
            .collect();
        // Results as they come, in any order; a store or leadership error
        // returns at once, dropping the slice's pairs still running (review:
        // as a crash there; every decision is re-made by a later pass), with
        // the cursor after the pairs that all finished before it.
        let mut finished = vec![false; slice.len()];
        let mut results = futures::stream::iter(work).buffer_unordered(parallel);
        while let Some((k, result)) = results.next().await {
            let i = first + k;
            let (gid, nid) = batch.pairs[i];
            let Some(result) = result else {
                finished[k] = true;
                continue;
            };
            done += 1;
            if let Err(e) = &result
                && (is_fenced(e)
                    || matches!(e, ControlError::Meta(m) if !matches!(m, MetaError::Codec(_))))
            {
                let through = first + finished.iter().take_while(|f| **f).count();
                if through > 0 {
                    cursor.after = bg_cursor::pair_position(
                        batch.pairs[through - 1].0,
                        batch.pairs[through - 1].1,
                    );
                    let _ = bg_cursor::put(meta, bg_cursor::RECONCILE, &cursor).await;
                }
                return result.map(|_| ());
            }
            finished[k] = true;
            match result {
                Ok(r) => {
                    deleted += r.deleted();
                    metrics
                        .delete_fence_refusals_total
                        .fetch_add(r.kept_fenced as u64, Ordering::Relaxed);
                }
                Err(ControlError::Node { .. }) => unreachable += 1,
                // A record of this pair that cannot be decoded is this pair's
                // problem (review): passed over, as any other.
                Err(e) => {
                    warn!(group = gid, node = nid, error = %e, "reconcile of a node's shards failed; passed over until the next sweep");
                    errors += 1;
                }
            }
        }
    }
    if batch.wraps {
        cursor.after.clear();
        cursor.sweeps += 1;
        cursor.last_sweep_ms = now_ms();
        if cursor.sweep_started_ms > 0 {
            metrics.reconcile_sweep_ms.store(
                cursor.last_sweep_ms.saturating_sub(cursor.sweep_started_ms),
                Ordering::Relaxed,
            );
        }
    } else if let Some(&(g, n)) = batch.pairs.last() {
        cursor.after = bg_cursor::pair_position(g, n);
    }
    cursor.last_pass_ms = now_ms();
    cursor.last_pass_items = done;
    cursor.last_pass_errors = errors;
    bg_cursor::put(meta, bg_cursor::RECONCILE, &cursor).await?;
    metrics
        .reconcile_errors_total
        .fetch_add(errors, Ordering::Relaxed);
    metrics
        .shards_deleted_total
        .fetch_add(deleted as u64, Ordering::Relaxed);
    info!(deleted, unreachable, "reconcile pass");
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn health_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
    incarnation: [u8; 16],
) -> Result<(), ControlError> {
    // ADR-0035 §10.1: the tiering record is acknowledged by a pass that
    // always runs (a tiering pass does not without a policy).
    if let Err(e) = acknowledge_tiering_if_no_pass(meta, metrics, incarnation).await {
        if is_fenced(&e) {
            return Err(e);
        }
        warn!(error = %e, "tiering record acknowledgment failed; retried at the next pass");
    }
    let nodes = cluster::list_nodes(meta).await?;
    let now = now_ms();
    let mut unhealthy = 0u64;
    // Cluster facts for the nodes (RFC-014 §5.3, RFC-015 §5.3): the format
    // they may write and the incarnation, with its change time.
    let gate = compat::gate(meta).await?;
    // Repair and integrity writes count only on nodes of schema_write's
    // release once it is 5 or later (ADR-0039).
    let node_schema = compat::required_node_schema(gate.schema_write);
    clients.set_required_node_schema(node_schema);
    // Controllers and gateways of an earlier release that still heartbeat
    // after the finalize (ADR-0039: one started between the cut-off and
    // the schema raise, or one the operator named absent): flagged on
    // every pass until stopped.
    let mut procs_below = 0u64;
    if node_schema > 0 {
        let at = now_ms();
        for p in compat::procs(meta).await? {
            if at.saturating_sub(p.last_ms) <= crate::upgrade::PROC_STALE_MS
                && p.caps.schema_max < node_schema
            {
                procs_below += 1;
                tracing::error!(process = %p.id, kind = ?p.kind, software = %p.software, schema_max = p.caps.schema_max, schema_write = node_schema, "a process of an earlier release runs after the finalize: stop it");
            }
        }
    }
    metrics
        .procs_below_schema
        .store(procs_below, Ordering::Relaxed);
    // The delete fence (ADR-0023): open for this term while Active, closed
    // otherwise; pushed to every node with the other facts. Deletes of
    // this process carry the generation only while it is open.
    let highest = metrics.delete_fence.highest_reported();
    let token = metrics.leader_token.load(Ordering::Relaxed);
    // Opens only after a whole round of node reports in this term (a fresh
    // process knows no node's generation yet). A failure does not stop the
    // pass: the other facts still go out, and this process stops deleting
    // (Phase 3.9 review).
    let view = arca_lake_metadata::ops::delete_fence::LeaderView {
        token,
        incarnation,
        opener: metrics.delete_fence.opener(),
        required: highest,
        max_node_generation: metrics.delete_fence.max_generation(),
        may_open: metrics.delete_fence.round_done(token),
        lineage: metrics.delete_fence.lineage(),
        restore_suspected: metrics.delete_fence.restore_suspected(),
    };
    let lineage_guard = metrics.lineage_lock.lock().await;
    let view = arca_lake_metadata::ops::delete_fence::LeaderView {
        lineage: metrics.delete_fence.lineage(),
        ..view
    };
    let synced = arca_lake_metadata::ops::delete_fence::sync_leader(meta, view, now).await;
    if let Ok(f) = &synced {
        metrics.delete_fence.set_lineage(f.lineage);
        if f.metadata_behind && view.restore_suspected {
            metrics.delete_fence.clear_suspected();
        }
    }
    drop(lineage_guard);
    let fence = match synced {
        Ok(f) => {
            metrics.delete_fence.set_lineage(f.lineage);
            if f.metadata_behind {
                tracing::error!(
                    generation = f.fence.record.generation,
                    reason = %f.fence.record.reason,
                    "storage nodes are ahead of the metadata: delete fence closed and held"
                );
                metrics
                    .delete_fence_metadata_behind_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some((f.fence, f.ours))
        }
        // Also a phase that refuses the leader's bookkeeping writes
        // (recovery): no fence change from this pass, the other facts go
        // out (re-review).
        Err(e)
            if matches!(
                e,
                arca_lake_metadata::MetaError::InvalidArgument(_)
                    | arca_lake_metadata::MetaError::Kv(arca_lake_metadata::KvError::ClusterState(
                        _
                    ))
            ) =>
        {
            warn!(error = %e, "delete fence not advanced: deletes stop in this process");
            None
        }
        Err(e) => return Err(e.into()),
    };
    // Without a fence: generation 0 (no fence change pushed), none held.
    let (fence, ours) = fence.unwrap_or((
        arca_lake_metadata::ops::delete_fence::TermFence {
            record: arca_lake_metadata::ops::delete_fence::DeleteFenceRecord::default(),
            changed: false,
        },
        false,
    ));
    if fence.changed {
        info!(
            generation = fence.record.generation,
            open = fence.record.open,
            reason = %fence.record.reason,
            "delete fence changed"
        );
    }
    // Only a generation this process opened (a former leader's, kept
    // while the first round of reports is collected, or one restored with
    // the metadata under a repeated term number, is not).
    clients.set_delete_fence(ours.then_some(fence.record.generation));
    // An open generation that is not ours is not pushed (it would renew
    // the nodes' rejoin lease on it): no fence change goes out, and the
    // nodes' leases on it lapse (Phase 3.10 review).
    let (push_generation, push_open) = if fence.record.open && !ours {
        (0, false)
    } else {
        (fence.record.generation, fence.record.open)
    };
    metrics.delete_fence.set_record(&fence.record);
    let req = arca_lake_protocol::storage::v1::HealthRequest {
        format_write: gate.format_write.unwrap_or(0),
        incarnation: gate
            .cluster
            .as_ref()
            .map(|c| bytes::Bytes::copy_from_slice(&c.incarnation))
            .unwrap_or_default(),
        incarnation_changed_ms: gate.cluster.as_ref().map_or(0, |c| c.changed_ms),
        incarnation_seq: gate.incarnation_seq,
        schema_floor: gate.schema.unwrap_or(0),
        delete_fence_generation: push_generation,
        delete_fence_open: push_open,
        delete_fence_opener: if push_open {
            metrics.delete_fence.opener()
        } else {
            0
        },
        // A recorded hold reaches nodes that missed the operator's push;
        // a leader never releases one (only `admin delete-fence release`,
        // or a new incarnation, does): one started on restored metadata
        // whose record lost the hold cannot reopen deletes.
        // Only a hold of this incarnation: the one `recovery begin` set
        // was set before the restore's incarnation, and repeating it now
        // would tie it on a node that missed the close to this incarnation,
        // which then could not lift it after finalize.
        delete_fence_hold: fence.record.operator_hold
            && !fence.record.open
            && fence.record.incarnation == incarnation,
        delete_fence_release: false,
        delete_fence_release_token: 0,
        node_id: 0,
        node_identity: bytes::Bytes::new(),
        adopt_identity: false,
    };
    // Lifecycle and identity (Phase 3.11, R-100): a removed node gets
    // nothing; a quarantined one only a probe; every other node its
    // identity, adopted until it acknowledged one, compared afterwards.
    let states = node_lifecycle::states(meta).await?;
    let mut identities = node_lifecycle::identities(meta).await?;
    // Nodes that defer deletes, as their health says: every node in
    // service but those that answered with a retention of 0, a sound
    // retirement state and no retirement left (one that does not answer,
    // or is probed only,
    // counts as deferring: the safe side). Admission counts GC's deletes as
    // room only while none defers (Phase 3.14: taken from the reclaim pass
    // before, whose errors at a new leader's first pass counted every node
    // as deferring for a reclaim interval).
    let mut in_service = 0u64;
    let mut not_deferring_seen = 0u64;
    for n in &nodes {
        clients.note_node_id(&n.address, n.id);
        let state = states
            .get(&n.id)
            .copied()
            .unwrap_or(node_lifecycle::State::Active);
        if state == node_lifecycle::State::Removed {
            continue;
        }
        in_service += 1;
        if state == node_lifecycle::State::Quarantined {
            let _ = clients.health(&n.address).await;
            unhealthy += 1;
            continue;
        }
        // No record: metadata restored from before this node had one (a
        // new registration has one). A facts-free probe first: what the
        // node holds decides (review: never imported, never replaced).
        let mut ident = identities.remove(&n.id);
        if ident.is_none() {
            let Ok(p) = clients.health(&n.address).await else {
                if now.saturating_sub(n.last_seen_ms) > ms(cfg.unhealthy_after) {
                    unhealthy += 1;
                }
                continue;
            };
            if p.identity_supported && p.identity_node_id == 0 {
                match node_lifecycle::ensure_identity(meta, n.id).await {
                    Ok(i) => ident = Some(i),
                    // A managed restore holds node changes: next pass.
                    Err(e) if node_lifecycle::is_restore_in_progress(&e) => continue,
                    Err(e) => return Err(e.into()),
                }
            } else if p.identity_supported && p.identity_node_id == n.id {
                // Its own identity, unknown to this metadata: nothing is
                // pushed (no facts, no fence: it takes no delete) until an
                // operator confirms it (`admin node adopt-identity`) or a
                // managed restore reinstalls it. Reads and writes go on.
                warn!(node = n.id, address = %n.address, "storage node holds an identity this metadata does not know (restored from before it?): confirm with `admin node adopt-identity`");
                metrics
                    .identity_unconfirmed_total
                    .fetch_add(1, Ordering::Relaxed);
                metrics.delete_fence.note_node(
                    n.id,
                    p.delete_fence_generation,
                    p.delete_fence_open,
                );
                // Its capacity still counts for admission (third review).
                metrics
                    .admission
                    .note_device(crate::admission::device_of(n.id, &p, now_ms()));
                cluster::touch_node(meta, n.id, now).await?;
                continue;
            } else if p.identity_supported {
                let reason = format!(
                    "the node holds the identity of node {}: a disk moved or an id reused",
                    p.identity_node_id
                );
                if quarantine_unless_held(meta, n.id, &reason, now).await? {
                    tracing::error!(node = n.id, address = %n.address, %reason, "storage node quarantined");
                }
                metrics
                    .nodes_quarantined_total
                    .fetch_add(1, Ordering::Relaxed);
                unhealthy += 1;
                continue;
            }
        }
        // An earlier release at an address whose disk acknowledged an
        // identity cannot show it is that disk, and would apply deletes
        // whatever identity they carry (Phase 3.12 audit): it is asked
        // first, facts-free, and gets no push (no open fence, no lease: it
        // takes no delete) until this release answers there again. Asked
        // before every push: a node rolled back restarts with no lease, and
        // only a push would give it one. Reads and writes go on.
        if ident.as_ref().is_some_and(|i| i.acknowledged) {
            match clients.health(&n.address).await {
                Ok(p) if !p.identity_supported => {
                    warn!(node = n.id, address = %n.address, "an earlier release answers where this node acknowledged an identity: no delete fence is pushed to it until it runs this release again");
                    metrics
                        .identity_unverifiable_total
                        .fetch_add(1, Ordering::Relaxed);
                    metrics.delete_fence.note_node(
                        n.id,
                        p.delete_fence_generation,
                        p.delete_fence_open,
                    );
                    metrics
                        .admission
                        .note_device(crate::admission::device_of(n.id, &p, now_ms()));
                    cluster::touch_node(meta, n.id, now).await?;
                    continue;
                }
                Ok(_) => {}
                Err(_) => {
                    if now.saturating_sub(n.last_seen_ms) > ms(cfg.unhealthy_after) {
                        unhealthy += 1;
                    }
                    continue;
                }
            }
        }
        // Deletes to this address carry the identity it acknowledged (a
        // disk that is not this node's refuses them); none while it has
        // not acknowledged one (review: no stale token after a rejoin).
        clients.note_identity(
            &n.address,
            ident
                .as_ref()
                .filter(|i| i.acknowledged)
                .map(|i| (n.id, i.token)),
        );
        // A node of an earlier release once schema 5 is written (ADR-0039):
        // asked facts-free first, so that it receives nothing (no fence, no
        // schema floor, no incarnation) before it is quarantined below.
        if node_schema > 0 {
            match clients.health(&n.address).await {
                Ok(p) if crate::upgrade::node_below_schema(&p, node_schema) => {
                    let reason = format!(
                        "the node runs {} and enforces schema at most {}; the cluster writes schema {node_schema}: an earlier release after the finalize",
                        p.software, p.schema_max
                    );
                    if quarantine_unless_held(meta, n.id, &reason, now).await? {
                        tracing::error!(node = n.id, address = %n.address, %reason, "storage node quarantined");
                    }
                    metrics
                        .nodes_quarantined_total
                        .fetch_add(1, Ordering::Relaxed);
                    unhealthy += 1;
                    continue;
                }
                Ok(_) => {}
                Err(_) => {
                    if now.saturating_sub(n.last_seen_ms) > ms(cfg.unhealthy_after) {
                        unhealthy += 1;
                    }
                    continue;
                }
            }
        }
        let mut node_req = req.clone();
        if let Some(i) = &ident {
            node_req.node_id = n.id;
            node_req.node_identity = bytes::Bytes::copy_from_slice(&i.token);
            node_req.adopt_identity = !i.acknowledged;
        }
        match clients.health_with(&n.address, node_req).await {
            Ok(h) => {
                // At 0 a node still keeps what it retired before (an
                // earlier retention): deferring while any is left.
                if h.reclaim_retention_ms == 0 && !h.reclaim_damaged && h.reclaim_pending == 0 {
                    not_deferring_seen += 1;
                }
                // Its fence counts whatever its identity (review).
                metrics.delete_fence.note_node(
                    n.id,
                    h.delete_fence_generation,
                    h.delete_fence_open,
                );
                if h.identity_supported {
                    let held = <[u8; 16]>::try_from(h.identity_token.as_ref()).ok();
                    let verdict: Option<String> = match &ident {
                        // Not carried (an earlier release's node: not judged).
                        None => None,
                        Some(i) => {
                            let holds = h.identity_node_id == n.id && held == Some(i.token);
                            if !h.identity_mismatch && holds {
                                if !i.acknowledged {
                                    match node_lifecycle::acknowledge_identity(meta, n.id, i.token)
                                        .await
                                    {
                                        Ok(()) => {
                                            clients
                                                .note_identity(&n.address, Some((n.id, i.token)));
                                        }
                                        // Held by a managed restore: next pass.
                                        Err(e) if node_lifecycle::is_restore_in_progress(&e) => {}
                                        Err(e) => return Err(e.into()),
                                    }
                                }
                                None
                            } else if h.identity_node_id == 0 && i.acknowledged {
                                Some("the node holds no identity although it acknowledged one: an emptied or replaced disk".to_string())
                            } else if h.identity_node_id == 0 {
                                Some("the node did not adopt its identity: a damaged identity file (remove it, then `admin node rejoin --fresh-identity`)".to_string())
                            } else if h.identity_node_id == n.id {
                                Some("the node holds another identity token for its id: another cluster's disk, or a re-registered id".to_string())
                            } else {
                                Some(format!(
                                    "the node holds the identity of node {}: a disk moved or an id reused",
                                    h.identity_node_id
                                ))
                            }
                        }
                    };
                    if let Some(reason) = verdict {
                        if quarantine_unless_held(meta, n.id, &reason, now).await? {
                            tracing::error!(node = n.id, address = %n.address, %reason, "storage node quarantined");
                        }
                        metrics
                            .nodes_quarantined_total
                            .fetch_add(1, Ordering::Relaxed);
                        unhealthy += 1;
                        continue;
                    }
                }
                // Replaced by an earlier release between the probe above and
                // this push (ADR-0039): quarantined at once.
                if crate::upgrade::node_below_schema(&h, node_schema) {
                    if quarantine_unless_held(
                        meta,
                        n.id,
                        "the node answered the push as an earlier release after the finalize",
                        now,
                    )
                    .await?
                    {
                        tracing::error!(node = n.id, address = %n.address, "storage node quarantined: an earlier release after the finalize");
                    }
                    metrics
                        .nodes_quarantined_total
                        .fetch_add(1, Ordering::Relaxed);
                    unhealthy += 1;
                    continue;
                }
                // A node at this incarnation's sequence with another
                // incarnation, or above it, refuses this cluster's opens
                // and deletes until a `recovery begin` marks one above it
                // (Phase 3.10 review).
                if gate.incarnation_seq > 0
                    && (h.incarnation_seq > gate.incarnation_seq
                        || (h.incarnation_seq == gate.incarnation_seq
                            && h.incarnation.as_ref() != req.incarnation.as_ref()))
                {
                    warn!(
                        node = n.id,
                        node_seq = h.incarnation_seq,
                        cluster_seq = gate.incarnation_seq,
                        "storage node holds another incarnation at or above this one's sequence: it refuses this cluster's deletes and shard writes; run `admin recovery begin`"
                    );
                    metrics
                        .incarnation_conflicts_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                metrics
                    .admission
                    .note_device(crate::admission::device_of(n.id, &h, now_ms()));
                cluster::touch_node(meta, n.id, now).await?;
                let caps = arca_lake_protocol::client::NodeCaps::from_health(&h);
                arca_lake_metadata::ops::recovery::set_node_caps(
                    meta,
                    &arca_lake_metadata::ops::recovery::NodeCapRecord {
                        node_id: n.id,
                        caps: caps.caps,
                        software: caps.software,
                        format_write: caps.format_write,
                        incarnation: caps.incarnation,
                        group_epochs: h
                            .group_epochs
                            .iter()
                            .map(|g| (g.group_id, g.epoch))
                            .collect(),
                        seen_ms: now,
                    },
                )
                .await?;
            }
            Err(_) => {
                if now.saturating_sub(n.last_seen_ms) > ms(cfg.unhealthy_after) {
                    unhealthy += 1;
                }
            }
        }
    }
    metrics.nodes_deferring.store(
        in_service.saturating_sub(not_deferring_seen),
        Ordering::Relaxed,
    );
    metrics.deferral_known.store(true, Ordering::Relaxed);
    metrics
        .nodes_total
        .store(nodes.len() as u64, Ordering::Relaxed);
    metrics.nodes_unhealthy.store(unhealthy, Ordering::Relaxed);
    metrics.delete_fence.finish_round(token);
    // Write admission (Phase 3.6), from what the nodes just reported.
    // The term is the leader's fencing token: a failover (or leading
    // again) starts the admission state afresh (Phase 3.7, R-85).
    // Room GC gives back counts only while no node defers deletes (Phase
    // 3.13): a deferred delete frees nothing before its reclaim, which
    // counts instead (otherwise a throttled tier would take in, for the
    // whole retention, what GC deleted as if it were free).
    let gc_now = metrics.gc.reclaimed_bytes.load(Ordering::Relaxed);
    let gc_delta = gc_now.saturating_sub(metrics.gc_bytes_seen.swap(gc_now, Ordering::Relaxed));
    if metrics.deferral_known.load(Ordering::Relaxed)
        && metrics.nodes_deferring.load(Ordering::Relaxed) == 0
    {
        metrics
            .room_returned_bytes
            .fetch_add(gc_delta, Ordering::Relaxed);
    }
    let totals = crate::admission::pass_in_term(
        meta,
        &metrics.admission,
        &cfg.admission,
        cfg.admission_max_chunk_bytes,
        metrics.room_returned_bytes.load(Ordering::Relaxed),
        crate::admission::Term {
            token: metrics.leader_token.load(Ordering::Relaxed),
            pass_started_ms: now,
            incarnation,
        },
        now_ms(),
    )
    .await?;
    *metrics
        .capacity
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = totals;
    metrics.health_seen_term.store(
        metrics.leader_token.load(Ordering::Relaxed),
        Ordering::Relaxed,
    );
    Ok(())
}

/// Resume interrupted repairs; when `auto_repair` is on, replace members
/// of `Active` groups whose node is unhealthy with a healthy device in
/// another failure domain.
#[allow(clippy::too_many_lines)]
async fn repair_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
    estimates: &crate::capacity::Estimates,
) -> Result<(), ControlError> {
    // Not before this term's first health pass (Phase 3.13): the nodes'
    // last-seen times are an earlier leader's, stale by the failover gap,
    // and a node would be taken as failed (and repaired away) for it.
    let token = metrics.leader_token.load(Ordering::Relaxed);
    if token == 0 || metrics.health_seen_term.load(Ordering::Relaxed) != token {
        return Ok(());
    }
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let all_groups = cluster::list_groups(meta).await?;
    metrics.groups_repairing.store(
        all_groups
            .iter()
            .filter(|g| g.state == GroupState::Repairing)
            .count() as u64,
        Ordering::Relaxed,
    );
    let now = now_ms();
    // A removed or quarantined node is never healthy (Phase 3.11, R-100):
    // members on it are repaired away at once.
    let out_of_service: BTreeSet<u64> = node_lifecycle::retirements(meta)
        .await?
        .into_iter()
        .map(|r| r.node_id)
        .collect();
    let healthy = |id: u64| {
        !out_of_service.contains(&id)
            && nodes
                .get(&id)
                .is_some_and(|n| now.saturating_sub(n.last_seen_ms) <= ms(cfg.unhealthy_after))
    };
    let opts = RepairOptions {
        page_size: cfg.page_size,
        verify_all_members: false,
        max_chunks: cfg.failure_repair_chunks_per_pass,
        former: Vec::new(),
        source: cfg.repair_source,
        keep_client_reserve: false,
        verify_cache: Some(std::sync::Arc::clone(&metrics.verify_cache)),
        // Chunks rebuilt at a time (Phase 3.14): reads of `k` shards and
        // the target's writes overlap; results are taken in order.
        parallel: 8,
    };
    // Operator intents (Phase 3.3 §6): draining nodes and replacement
    // requests. They change placement on purpose, so they progress only
    // while the cluster is Active (RFC-015 §5.6); repairs of unhealthy
    // members and resumed repairs run in every state that admits repair.
    let intents = Intents::load(meta, &all_groups).await?;
    // Repair statuses (Phase 3.13, R-104): those of groups gone or Active
    // again are done.
    let mut statuses = repair_liveness::statuses(meta).await?;
    for (id, _) in statuses.clone() {
        if all_groups
            .iter()
            .find(|g| g.id == id)
            .is_none_or(|g| g.state == GroupState::Active)
        {
            repair_status::clear(meta, id).await?;
            statuses.remove(&id);
        }
    }
    // Capacity reservations (Phase 3.14, ADR-0032): those that no longer
    // describe their group's move are released; the rest are this pass's
    // (and the planned ones say which moves are planned, whichever leader
    // started them).
    let released = reservation::sweep(meta).await?;
    metrics
        .capacity_reservations_released_total
        .fetch_add(released as u64, Ordering::Relaxed);
    let reservations = reservation::list(meta).await?;
    metrics
        .capacity_reservations
        .store(reservations.len() as u64, Ordering::Relaxed);
    metrics.capacity_reserved_bytes.store(
        reservations
            .iter()
            .map(CapacityReservation::remaining)
            .sum(),
        Ordering::Relaxed,
    );
    let planned_reserved: BTreeSet<u64> = reservations
        .iter()
        .filter(|r| r.kind == ReservationKind::Planned)
        .map(|r| r.group)
        .collect();
    // Why each rebuilding group is where it is, and the longest stalled
    // one (Phase 3.14): by reason, never summed into one figure.
    let mut by_reason = [0u64; 7];
    let mut stall: Option<(u64, u64)> = None;
    for g in all_groups
        .iter()
        .filter(|g| g.state == GroupState::Repairing)
    {
        let s = statuses.get(&g.id);
        let i = s.map_or(6, |s| {
            crate::metrics::REPAIR_REASONS
                .iter()
                .position(|r| *r == s.reason.as_str())
                .unwrap_or(6)
        });
        by_reason[i] += 1;
        let since = s.map_or(now, |s| s.last_progress_ms.max(s.since_ms));
        let age = now.saturating_sub(since) / 1000;
        if stall.is_none_or(|(a, _)| age > a) {
            stall = Some((age, g.id));
        }
    }
    for (a, v) in metrics.repair_groups_by_reason.iter().zip(by_reason) {
        a.store(v, Ordering::Relaxed);
    }
    let (age, group) = stall.unwrap_or((0, 0));
    metrics
        .repair_longest_stall_seconds
        .store(age, Ordering::Relaxed);
    metrics
        .repair_longest_stall_group
        .store(group, Ordering::Relaxed);
    metrics.repair_attempt_max_ms.store(0, Ordering::Relaxed);
    metrics.groups_repair_waiting.store(
        all_groups
            .iter()
            .filter(|g| g.state == GroupState::Repairing)
            .filter(|g| {
                statuses
                    .get(&g.id)
                    .is_some_and(|s| !s.reason.holds_background_work())
            })
            .count() as u64,
        Ordering::Relaxed,
    );
    // Open epoch changes first (Phase 3.11, M2): a move that raised node
    // epochs and did not commit is resumed if still wanted, otherwise its
    // epoch is settled (metadata at the raised epoch, membership
    // unchanged), so that the group takes writes again. Finished or
    // overtaken intents are removed.
    let mut settled_any = false;
    // Groups whose intent could not be resumed this pass: skipped below
    // (review: one stuck intent must not stop every other repair).
    let mut held_groups: BTreeSet<u64> = BTreeSet::new();
    for intent in epoch_intent::open(meta).await? {
        let Some(g) = all_groups.iter().find(|g| g.id == intent.group) else {
            epoch_intent::discard(meta, intent.group).await?;
            continue;
        };
        if g.epoch != intent.from_epoch {
            epoch_intent::discard(meta, intent.group).await?;
            continue;
        }
        let target = intent.replacement.node_id;
        // Still wanted: the target can host it, and the slot still holds
        // another member that is to leave (unhealthy, draining, or named
        // by a replacement request for this index), or a member still
        // being rebuilt (Phase 3.13: a target change of a repair, which
        // holds nothing the group counts on yet).
        let rebuilding = g.state == GroupState::Repairing
            && g.repair_cursor.is_some_and(|c| c.member == intent.index);
        let leaving = |m: &GroupMember| {
            rebuilding
                || !healthy(m.node_id)
                || intents.draining.contains(&m.node_id)
                || intents
                    .requests
                    .get(&g.id)
                    .is_some_and(|r| r.index == intent.index)
        };
        // A resume that keeps failing is settled after ten minutes.
        let wanted = now.saturating_sub(intent.since_ms) < 600_000
            && healthy(target)
            && intents.may_host(target)
            && g.members
                .get(usize::from(intent.index))
                .is_some_and(|m| *m != intent.replacement && leaving(m));
        if wanted {
            info!(
                group = g.id,
                index = intent.index,
                node = target,
                "resuming an interrupted epoch change"
            );
            let leaving = g.members[usize::from(intent.index)].node_id;
            // The nodes that left the slot during this repair too (Phase
            // 3.13); for a direct copy, not those that stored bytes that
            // did not check.
            let mut former: Vec<u64> = vec![leaving];
            if let Some(s) = statuses.get(&g.id).filter(|s| s.index == intent.index) {
                former.extend(s.former.iter().copied());
                // The leaving node too (independent review): an untrusted
                // one is never copied from as it is.
                if cfg.repair_source != repair::RepairSource::Reconstruct {
                    former.retain(|n| !s.untrusted.contains(n));
                }
            }
            former.retain(|&n| healthy(n) && n != target);
            former.dedup();
            let resume_opts = RepairOptions {
                keep_client_reserve: healthy(leaving) && !rebuilding,
                former,
                ..opts.clone()
            };
            match crate::repair::replace_member_with(
                meta,
                clients,
                g.id,
                usize::from(intent.index),
                intent.replacement,
                &resume_opts,
                async |_| Ok(()),
            )
            .await
            {
                Ok(_) => settled_any = true,
                Err(e) => {
                    warn!(group = g.id, error = %e, "resumed epoch change failed; retried next pass");
                    held_groups.insert(g.id);
                }
            }
        } else {
            for m in &g.members {
                if let Some(n) = nodes.get(&m.node_id) {
                    let _ = clients
                        .update_epoch(&n.address, g.id, intent.from_epoch + 1)
                        .await;
                }
            }
            let after = epoch_intent::settle(meta, &intent).await?;
            info!(
                group = g.id,
                epoch = after.epoch,
                "settled an interrupted epoch change: membership unchanged"
            );
            settled_any = true;
        }
    }
    if settled_any {
        // The groups changed: the rest of the pass runs on fresh records
        // next time.
        return Ok(());
    }
    intents
        .complete_drains(meta, clients, &nodes, &all_groups, now)
        .await?;
    // Groups rebuilding a member without the node it left answering: their
    // reads and repairs have one shard fewer than the profile promises
    // until the rebuild completes (R-65). A healthy member's move keeps it.
    let reduced = all_groups
        .iter()
        .filter(|g| !g.is_cache() && g.state == GroupState::Repairing)
        .filter(|g| {
            !intents
                .requests
                .get(&g.id)
                .map(|r| r.from_node)
                .into_iter()
                .chain(intents.draining.iter().copied())
                .any(|n| healthy(n) && !g.members.iter().any(|m| m.node_id == n))
        })
        .inspect(|g| warn!(group = g.id, "rebuilding a member without its former node: redundancy reduced until the rebuild completes"))
        .count() as u64;
    metrics
        .groups_reduced_redundancy
        .store(reduced, Ordering::Relaxed);
    // Durable groups first; cache groups only when no durable group needed
    // attention in this pass, and with a per-pass budget (RFC-006 §5.9).
    // Among durable groups, failed members first; an operator's move off
    // a healthy node runs with a per-pass budget after them, so that it
    // never holds up the repair of a failure (Phase 3.10 audit). One
    // group's error does not stop the others' repair.
    let planned = |g: &GroupRecord| {
        let leaving: Vec<u64> = intents
            .requests
            .get(&g.id)
            .map(|r| r.from_node)
            .into_iter()
            .chain(intents.draining.iter().copied())
            .collect();
        // Only a move this group's own intent names: an operator's
        // replacement request, or (not yet started) a drain of one of its
        // members. A group already repairing for any other reason is
        // failure work (re-review: a drain elsewhere does not make it
        // planned).
        let started_planned = g.state == GroupState::Repairing
            && (planned_reserved.contains(&g.id)
                || metrics
                    .planned_moves
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .contains(&g.id));
        let moving = intents.requests.contains_key(&g.id)
            || started_planned
            || (g.state == GroupState::Active
                && g.members
                    .iter()
                    .any(|m| intents.draining.contains(&m.node_id)));
        moving
            && leaving.iter().all(|&n| healthy(n))
            && g.members
                .iter()
                .all(|m| healthy(m.node_id) || leaving.contains(&m.node_id))
    };
    let mut durable: Vec<&GroupRecord> = all_groups.iter().filter(|g| !g.is_cache()).collect();
    durable.sort_by_key(|g| planned(g));
    let planned_opts = RepairOptions {
        max_chunks: cfg.planned_move_chunks_per_pass,
        ..opts.clone()
    };
    let mut durable_busy = false;
    for g in durable {
        if held_groups.contains(&g.id) {
            continue;
        }
        let is_planned = planned(g);
        if g.state == GroupState::Active {
            let mut pm = metrics
                .planned_moves
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if is_planned {
                pm.insert(g.id);
            } else {
                pm.remove(&g.id);
            }
        }
        let o = if is_planned { &planned_opts } else { &opts };
        match repair_one(
            meta,
            clients,
            cfg,
            metrics,
            &nodes,
            &all_groups,
            g,
            o,
            &healthy,
            &intents,
            estimates,
            &statuses,
        )
        .await
        {
            Ok(busy) => durable_busy |= busy,
            Err(e) if is_fenced(&e) => return Err(e),
            Err(e) => {
                warn!(group = g.id, error = %e, "repair of the group failed; the others go on");
                metrics
                    .repair_group_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                durable_busy = true;
            }
        }
    }
    if durable_busy {
        return Ok(());
    }
    let cache_opts = RepairOptions {
        max_chunks: cfg.cache_repair_chunks_per_pass,
        ..opts
    };
    for g in all_groups
        .iter()
        .filter(|g| g.is_cache() && !held_groups.contains(&g.id))
    {
        match repair_one(
            meta,
            clients,
            cfg,
            metrics,
            &nodes,
            &all_groups,
            g,
            &cache_opts,
            &healthy,
            &intents,
            estimates,
            &statuses,
        )
        .await
        {
            Ok(_) => {}
            Err(e) if is_fenced(&e) => return Err(e),
            Err(e) => {
                warn!(group = g.id, error = %e, "repair of the cache group failed; the others go on");
                metrics
                    .repair_group_errors_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

/// Draining nodes and open replacement requests, as read at the start of
/// a repair pass.
struct Intents {
    active: bool,
    draining: BTreeSet<u64>,
    retired: BTreeSet<u64>,
    requests: HashMap<u64, membership::ReplaceRequest>,
}

impl Intents {
    async fn load<S: KvStore>(meta: &S, groups: &[GroupRecord]) -> Result<Self, ControlError> {
        let active = compat::cluster_state(meta)
            .await?
            .is_some_and(|c| c.state == arca_lake_metadata::records::ClusterPhase::Active);
        let mut draining = BTreeSet::new();
        let mut retired = BTreeSet::new();
        for r in membership::node_states(meta).await? {
            match r.state {
                membership::NodeLifecycle::Draining { .. } => draining.insert(r.node_id),
                membership::NodeLifecycle::Decommissioned { .. } => retired.insert(r.node_id),
            };
        }
        for r in node_lifecycle::retirements(meta).await? {
            retired.insert(r.node_id);
        }
        let mut requests = HashMap::new();
        for r in membership::replace_requests(meta).await? {
            // Satisfied: another node holds the index and the group is
            // Active again (the replacement passed its completeness check).
            let done = groups.iter().find(|g| g.id == r.group).is_none_or(|g| {
                g.state == GroupState::Active
                    && g.members
                        .get(usize::from(r.index))
                        .is_none_or(|m| m.node_id != r.from_node)
            });
            if done {
                membership::clear_replace(meta, r.group).await?;
                info!(
                    group = r.group,
                    index = r.index,
                    "replacement request completed"
                );
            } else {
                requests.insert(r.group, r);
            }
        }
        Ok(Self {
            active,
            draining,
            retired,
            requests,
        })
    }

    /// Whether `node` may receive a new member.
    fn may_host(&self, node: u64) -> bool {
        !self.draining.contains(&node) && !self.retired.contains(&node)
    }

    /// A draining node is decommissioned once no group lists it and it
    /// holds no shard of a group still rebuilding (its shards may still be
    /// the better source until that replacement has passed its
    /// completeness check). Phase 3.13 (R-104): only the groups it holds
    /// shards of keep it, not every group rebuilding anywhere, and what it
    /// waits for is reported.
    async fn complete_drains<S: KvStore>(
        &self,
        meta: &S,
        clients: &NodeClients,
        nodes: &HashMap<u64, NodeRecord>,
        groups: &[GroupRecord],
        now: u64,
    ) -> Result<(), ControlError> {
        let repairing: Vec<&GroupRecord> = groups
            .iter()
            .filter(|g| g.state == GroupState::Repairing)
            .collect();
        for &n in &self.draining {
            let member_of = groups
                .iter()
                .filter(|g| g.members.iter().any(|m| m.node_id == n))
                .count();
            let mut waits = None;
            if member_of == 0
                && let Some(node) = nodes.get(&n)
            {
                for g in &repairing {
                    match clients.list_shards_page(&node.address, g.id, None, 1).await {
                        Ok(p) if p.entries.is_empty() && p.next.is_none() => {}
                        Ok(_) => {
                            waits = Some(format!(
                                "holds shards of group {}, which is rebuilding a member: kept as a source until it completes",
                                g.id
                            ));
                            break;
                        }
                        Err(e) => {
                            waits = Some(format!(
                                "cannot list its shards of group {} (rebuilding a member): {}",
                                g.id,
                                e.message()
                            ));
                            break;
                        }
                    }
                }
            }
            if member_of == 0 && waits.is_none() {
                membership::update_drain(meta, n, Some(now), None).await?;
                info!(node = n, "node decommissioned: no group lists it");
            } else if member_of == 0 {
                membership::update_drain(meta, n, None, waits).await?;
            } else if !self.active {
                membership::update_drain(
                    meta,
                    n,
                    None,
                    Some("cluster not Active; drain paused".into()),
                )
                .await?;
            }
        }
        Ok(())
    }
}

/// Resume or start the repair of one group when needed. Returns whether
/// the group holds back cache repair in this pass (a repair ran or made
/// progress, or one is needed and could not start for a failed member).
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn repair_one<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
    nodes: &HashMap<u64, NodeRecord>,
    all_groups: &[GroupRecord],
    g: &GroupRecord,
    opts: &RepairOptions,
    healthy: &impl Fn(u64) -> bool,
    intents: &Intents,
    estimates: &crate::capacity::Estimates,
    statuses: &HashMap<u64, RepairStatus>,
) -> Result<bool, ControlError> {
    let candidate = |id: u64| healthy(id) && intents.may_host(id);
    let now = now_ms();
    let prior = statuses.get(&g.id);
    match g.state {
        GroupState::Repairing => {
            let blocked = async |detail: &str| -> Result<bool, ControlError> {
                warn!(group = g.id, "{detail}");
                let s = RepairStatus {
                    group: g.id,
                    epoch: g.epoch,
                    reason: RepairReason::PermanentlyBlocked,
                    detail: detail.to_string(),
                    since_ms: prior
                        .filter(|p| p.reason == RepairReason::PermanentlyBlocked)
                        .map_or(now, |p| p.since_ms),
                    ..RepairStatus::default()
                };
                put_status(meta, prior, &s).await?;
                Ok(false)
            };
            let Some(cursor) = g.repair_cursor else {
                return blocked("repairing without a cursor; needs an operator").await;
            };
            let index = usize::from(cursor.member);
            let Some(&member) = g.members.get(index) else {
                return blocked("repair cursor names no member; needs an operator").await;
            };
            let obs = Observation {
                group: g.id,
                epoch: g.epoch,
                index: cursor.member,
                target: member.node_id,
                target_candidate: candidate(member.node_id),
                unhealthy_others: g
                    .members
                    .iter()
                    .enumerate()
                    .filter(|(i, m)| *i != index && !healthy(m.node_id))
                    .map(|(i, _)| u16::try_from(i).unwrap_or(u16::MAX))
                    .collect(),
                auto_repair: cfg.auto_repair,
                data_shards: usize::from(g.data_shards),
                members: g.members.len(),
                status: prior.cloned(),
                now_ms: now,
                pacing: cfg.repair_pacing,
            };
            let current = repair_liveness::current(&obs);
            // The node the member left, when an intent names it, and the
            // nodes that left the slot during this repair (its first node,
            // targets replaced since): sources of its shards while they
            // answer. Targets left because they stored bytes that did not
            // check are not copied from as they are (`Direct`); as one of
            // the `k` inputs of a reconstruction they are, since its result
            // is checked against the chunk's recorded checksum.
            let trust_all = cfg.repair_source == repair::RepairSource::Reconstruct;
            // Untrusted nodes are never a direct-copy source, whatever
            // names them (re-review: a drained or requested one too).
            let intent_former: Vec<u64> = intents
                .requests
                .get(&g.id)
                .map(|r| r.from_node)
                .into_iter()
                .chain(intents.draining.iter().copied())
                .filter(|&n| healthy(n) && !g.members.iter().any(|m| m.node_id == n))
                .filter(|n| trust_all || !current.untrusted.contains(n))
                .collect();
            let former = |also: Option<u64>| -> Vec<u64> {
                let mut f = intent_former.clone();
                for n in current.former.iter().copied().chain(also) {
                    if healthy(n)
                        && (trust_all || !current.untrusted.contains(&n))
                        && !g.members.iter().any(|m| m.node_id == n)
                        && !f.contains(&n)
                    {
                        f.push(n);
                    }
                }
                f
            };
            match repair_liveness::decide(&obs) {
                Decision::Wait => Ok(current.reason.holds_background_work()),
                // `decide` asks neither (only the walk just run decides on a
                // second failure, below).
                Decision::AwaitNodes(_) | Decision::ReplaceOther(_) => Ok(false),
                Decision::Retarget(why) => {
                    let leaving_former = former(Some(member.node_id));
                    let ropts = RepairOptions {
                        keep_client_reserve: !intent_former.is_empty(),
                        former: leaving_former,
                        ..opts.clone()
                    };
                    let mut exclude = current.former.clone();
                    exclude.extend(current.excluded.iter().copied());
                    exclude.push(member.node_id);
                    info!(
                        group = g.id,
                        index,
                        node = member.node_id,
                        "{why}: choosing another target"
                    );
                    match start_move(
                        meta,
                        clients,
                        cfg,
                        metrics,
                        nodes,
                        all_groups,
                        g,
                        index,
                        "retarget",
                        None,
                        &ropts,
                        healthy,
                        intents,
                        estimates,
                        Some(&current),
                        &exclude,
                    )
                    .await?
                    {
                        Started::Ran(busy) => {
                            metrics
                                .repair_retargets_total
                                .fetch_add(1, Ordering::Relaxed);
                            Ok(busy)
                        }
                        Started::NoTarget { capacity, msg } => {
                            warn!(group = g.id, index, "{why}; no other target: {msg}");
                            let mut s = repair_liveness::no_new_target(
                                &obs,
                                capacity,
                                format!("{why}; no other target: {msg}"),
                            );
                            // A target that still answers is tried again
                            // (after the backoff) rather than not at all.
                            if obs.target_candidate {
                                s.failures = 0;
                            }
                            put_status(meta, prior, &s).await?;
                            Ok(false)
                        }
                    }
                }
                Decision::Run => {
                    let ropts = RepairOptions {
                        keep_client_reserve: !intent_former.is_empty(),
                        former: former(None),
                        ..opts.clone()
                    };
                    let (busy, second) = attempt(
                        meta,
                        clients,
                        cfg,
                        metrics,
                        nodes,
                        g.id,
                        index,
                        member,
                        &ropts,
                        g.is_cache(),
                        healthy,
                        Some(&current),
                    )
                    .await?;
                    match second {
                        Some(Decision::ReplaceOther(_))
                            if {
                                // Former nodes down hold shards nobody
                                // lists either (re-review): a chunk only
                                // they and the members down hold is
                                // invisible to the walk. Within the code's
                                // tolerance (members and former nodes down
                                // at most m), every chunk keeps a shard on a
                                // member that answers.
                                let former_down = current
                                    .former
                                    .iter()
                                    .chain(current.excluded.iter())
                                    .filter(|n| !healthy(**n))
                                    .count();
                                let members_down =
                                    g.members.iter().filter(|m| !healthy(m.node_id)).count();
                                former_down > 0
                                    && members_down + former_down > usize::from(g.parity_shards)
                            } =>
                        {
                            if let Some(mut s) =
                                repair_liveness::statuses(meta).await?.remove(&g.id)
                            {
                                s.reason = RepairReason::AwaitingNodes;
                                s.detail = "more members and former nodes down than the group's parity: a chunk could have its only shards on them; bring them back".into();
                                s.next_attempt_ms = now_ms()
                                    + repair_liveness::backoff_ms(
                                        &cfg.repair_pacing,
                                        s.stalls.max(1),
                                    );
                                repair_status::put(meta, &s).await?;
                            }
                            Ok(false)
                        }
                        Some(Decision::ReplaceOther(_))
                            if !repair::former_shards_on_target(
                                meta,
                                clients,
                                nodes,
                                g,
                                index,
                                // Every former node that answers, untrusted
                                // ones too (re-review): evidence of a
                                // chunk, not a copy source.
                                // `excluded` too: after a crash between a
                                // status and its intent, this slot's former
                                // nodes are there (only this slot's shards
                                // are counted).
                                &current
                                    .former
                                    .iter()
                                    .chain(current.excluded.iter())
                                    .chain(intents.requests.get(&g.id).map(|r| &r.from_node))
                                    .chain(intents.draining.iter())
                                    .copied()
                                    .filter(|&n| healthy(n))
                                    .collect::<Vec<u64>>(),
                            )
                            .await? =>
                        {
                            // A former node holds shards of the slot that its
                            // member lacks: not whole after all.
                            if let Some(mut s) =
                                repair_liveness::statuses(meta).await?.remove(&g.id)
                            {
                                s.walked_clean = false;
                                s.reason = RepairReason::TemporarilyBlocked;
                                s.detail = format!(
                                    "a former node of slot {index} holds shards its member lacks"
                                );
                                repair_status::put(meta, &s).await?;
                            }
                            Ok(true)
                        }
                        Some(Decision::ReplaceOther(j)) => {
                            let j = usize::from(j);
                            info!(
                                group = g.id,
                                rebuilt = index,
                                index = j,
                                "slot rebuilt but for a failed member: replacing it"
                            );
                            // Never onto a node that left the slot just
                            // rebuilt (its shards of that slot stay a
                            // source; as a member of another slot they would
                            // be reconciliation's).
                            let mut exclude = current.former.clone();
                            exclude.extend(current.excluded.iter().copied());
                            exclude.push(member.node_id);
                            let fresh = repair_liveness::statuses(meta).await?;
                            match start_move(
                                meta,
                                clients,
                                cfg,
                                metrics,
                                nodes,
                                all_groups,
                                g,
                                j,
                                "auto repair",
                                None,
                                opts,
                                healthy,
                                intents,
                                estimates,
                                fresh.get(&g.id),
                                &exclude,
                            )
                            .await?
                            {
                                Started::Ran(busy) => Ok(busy),
                                Started::NoTarget { capacity, msg } => {
                                    let mut s = fresh.get(&g.id).cloned().unwrap_or(current);
                                    s.reason = if capacity {
                                        RepairReason::AwaitingCapacity
                                    } else {
                                        RepairReason::AwaitingNodes
                                    };
                                    s.detail = format!("member {j} does not answer; {msg}");
                                    // Not walked again before the backoff
                                    // (re-review).
                                    s.next_attempt_ms = now_ms()
                                        + repair_liveness::backoff_ms(
                                            &cfg.repair_pacing,
                                            s.stalls.max(1),
                                        );
                                    repair_status::put(meta, &s).await?;
                                    Ok(false)
                                }
                            }
                        }
                        Some(Decision::AwaitNodes(detail)) => {
                            if let Some(mut s) =
                                repair_liveness::statuses(meta).await?.remove(&g.id)
                            {
                                s.reason = RepairReason::AwaitingNodes;
                                s.detail = detail;
                                // Not walked again before the backoff
                                // (re-review).
                                s.next_attempt_ms = now_ms()
                                    + repair_liveness::backoff_ms(
                                        &cfg.repair_pacing,
                                        s.stalls.max(1),
                                    );
                                repair_status::put(meta, &s).await?;
                            }
                            Ok(false)
                        }
                        _ => Ok(busy),
                    }
                }
            }
        }
        GroupState::Active => {
            // Unhealthy members first (auto repair), then an operator
            // request, then members on draining nodes.
            let unhealthy = cfg
                .auto_repair
                .then(|| g.members.iter().position(|m| !healthy(m.node_id)))
                .flatten();
            let request = intents.requests.get(&g.id).filter(|r| {
                g.members
                    .get(usize::from(r.index))
                    .is_some_and(|m| m.node_id == r.from_node)
            });
            let drained = g
                .members
                .iter()
                .position(|m| intents.draining.contains(&m.node_id));
            let (index, why, to_node) = match (unhealthy, request, drained) {
                (Some(i), _, _) => (i, "auto repair", None),
                (None, Some(r), _) if intents.active => {
                    (usize::from(r.index), "replace request", r.to_node)
                }
                (None, _, Some(i)) if intents.active => (i, "drain", None),
                _ => return Ok(false),
            };
            let leaving = g.members[index].node_id;
            let mopts = RepairOptions {
                former: if healthy(leaving) {
                    vec![leaving]
                } else {
                    Vec::new()
                },
                keep_client_reserve: why != "auto repair" && healthy(leaving),
                ..opts.clone()
            };
            match start_move(
                meta,
                clients,
                cfg,
                metrics,
                nodes,
                all_groups,
                g,
                index,
                why,
                to_node,
                &mopts,
                healthy,
                intents,
                estimates,
                None,
                &[],
            )
            .await?
            {
                Started::Ran(busy) => Ok(busy),
                Started::NoTarget { msg, .. } => {
                    warn!(group = g.id, index, why, "{msg}");
                    if why == "replace request" {
                        membership::note_replace_error(meta, g.id, Some(msg)).await?;
                    } else if why == "drain" {
                        membership::update_drain(meta, leaving, None, Some(msg)).await?;
                    }
                    // Only an unhealthy member keeps cache repair waiting.
                    Ok(why == "auto repair")
                }
            }
        }
        _ => Ok(false),
    }
}

/// How [`start_move`] ended.
enum Started {
    /// The move started (or resumed); whether it holds cache repair.
    Ran(bool),
    /// No node can take the member (`capacity`: candidates exist, none
    /// with room).
    NoTarget { capacity: bool, msg: String },
}

/// Rebuild slot `index` of `g` onto a node chosen now (capacity checked,
/// `exclude` never chosen), then run the repair: a member move of an
/// `Active` group, the replacement of a target that is gone or keeps
/// failing, or of a second failed member of a `Repairing` group (Phase
/// 3.13). The membership change is an epoch intent (fenced, resumable).
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn start_move<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
    nodes: &HashMap<u64, NodeRecord>,
    all_groups: &[GroupRecord],
    g: &GroupRecord,
    index: usize,
    why: &str,
    to_node: Option<u64>,
    opts: &RepairOptions,
    healthy: &impl Fn(u64) -> bool,
    intents: &Intents,
    estimates: &crate::capacity::Estimates,
    prior: Option<&RepairStatus>,
    exclude: &[u64],
) -> Result<Started, ControlError> {
    let candidate = |id: u64| {
        healthy(id)
            && intents.may_host(id)
            && !exclude.contains(&id)
            && !prior.is_some_and(|p| p.excluded.contains(&id))
    };
    // Capacity (Phase 3.4 §4): the member's bytes, measured on a surviving
    // member, must fit on the target with headroom; a node that does not
    // report its free space is not judged.
    let one_copy_cache = g.is_cache() && g.parity_shards == 0 && g.members.len() == 1;
    let need = if one_copy_cache {
        0
    } else {
        estimates
            .member_size(clients, nodes, g, index, cfg.page_size)
            .await
            .map_or(0, |s| s.bytes)
    };
    // A failure repair may use the space down to the repair floor; a
    // planned move (and a retarget of one) keeps the client reserve.
    let planned = why != "auto repair" && (why != "retarget" || opts.keep_client_reserve);
    let kind = if planned {
        ReservationKind::Planned
    } else {
        ReservationKind::Repair
    };
    // Room other moves reserved (Phase 3.14, ADR-0032): a repair counts
    // only other repairs' (it comes first), a planned move every one.
    let reserved = reserved_on(meta, all_groups, g.id, kind).await?;
    let held = |id: u64| reserved.get(&id).copied().unwrap_or(0);
    let mut free: HashMap<u64, Option<u64>> = HashMap::new();
    for n in nodes.values().filter(|n| candidate(n.id)) {
        if !g.members.iter().any(|m| m.node_id == n.id) {
            free.insert(
                n.id,
                crate::capacity::node_room(clients, &n.address, planned).await,
            );
        }
    }
    let roomy = |id: u64| {
        crate::capacity::fits(
            free.get(&id)
                .copied()
                .flatten()
                .map(|f| f.saturating_sub(held(id))),
            need,
        )
    };
    let pick = |with_room: bool| {
        choose_for(nodes, all_groups, g, index, &cfg.failure_domain, &|id| {
            to_node.is_none_or(|n| n == id) && candidate(id) && (!with_room || roomy(id))
        })
    };
    let Some(replacement) = pick(true)? else {
        let without_room = pick(false)?;
        let msg = match (without_room, to_node) {
            (Some(m), _) => format!(
                "insufficient capacity: node {} has {} bytes free ({} reserved by other moves), the member needs {}; paused until a node has room",
                m.node_id,
                free.get(&m.node_id).copied().flatten().unwrap_or(0),
                held(m.node_id),
                crate::capacity::needed(need)
            ),
            (None, Some(n)) => {
                format!("requested node {n} is unhealthy, draining, or breaks the failure domain")
            }
            (None, None) => "no healthy replacement with free failure domain".to_string(),
        };
        return Ok(Started::NoTarget {
            capacity: without_room.is_some(),
            msg,
        });
    };
    // Reserve the member's bytes on the target, checked again in the
    // transaction against every reservation there (two moves never take
    // one room); written before the move begins, at its epoch: if it never
    // begins, the reservation does not hold and is released.
    if !g.is_cache() && need > 0 {
        let r = CapacityReservation {
            node: replacement.node_id,
            group: g.id,
            epoch: g.epoch + 1,
            index: u16::try_from(index).unwrap_or(u16::MAX),
            kind,
            bytes: need,
            written: 0,
            created_ms: now_ms(),
        };
        let room = free
            .get(&replacement.node_id)
            .copied()
            .flatten()
            .map_or(u64::MAX, |f| {
                f.saturating_sub(crate::capacity::needed(need) - need)
            });
        if let reservation::Reserved::No { held } = reservation::reserve(meta, &r, room).await? {
            metrics
                .capacity_reservation_refusals_total
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Started::NoTarget {
                capacity: true,
                msg: format!(
                    "insufficient capacity: node {} has {room} bytes of room, {held} reserved by other moves, the member needs {need}; paused until a node has room",
                    replacement.node_id
                ),
            });
        }
    }
    info!(
        group = g.id,
        index,
        node = replacement.node_id,
        cache = g.is_cache(),
        why,
        "member replacement"
    );
    // A move that was paused (capacity, no target) resumes: drop the
    // reason it showed.
    if why == "replace request" {
        membership::note_replace_error(meta, g.id, None).await?;
    } else if why == "drain" {
        membership::update_drain(meta, g.members[index].node_id, None, None).await?;
    }
    let leaving = g.members[index].node_id;
    // Recorded before the change (Phase 3.13): which node the slot leaves
    // stays known to any leader, as a source while it answers.
    let started = repair_liveness::started(
        prior,
        g.id,
        g.epoch + 1,
        u16::try_from(index).unwrap_or(u16::MAX),
        replacement.node_id,
        leaving,
        false,
        now_ms(),
    );
    repair_status::put(meta, &started).await?;
    let busy = attempt(
        meta,
        clients,
        cfg,
        metrics,
        nodes,
        g.id,
        index,
        replacement,
        opts,
        g.is_cache(),
        healthy,
        Some(&started),
    )
    .await?
    .0;
    Ok(Started::Ran(busy))
}

/// Per node, the bytes the moves of other groups still need there, of the
/// reservations that count against a new one of `kind` and still hold
/// (Phase 3.14, ADR-0032).
pub(crate) async fn reserved_on<S: KvStore>(
    meta: &S,
    groups: &[GroupRecord],
    except: u64,
    kind: ReservationKind,
) -> Result<HashMap<u64, u64>, ControlError> {
    // A move under way (its epoch change open, not committed) counts too
    // (Phase 3.14 review).
    let intents: HashMap<u64, epoch_intent::EpochIntent> = epoch_intent::open(meta)
        .await?
        .into_iter()
        .map(|i| (i.group, i))
        .collect();
    let mut out: HashMap<u64, u64> = HashMap::new();
    for r in reservation::list(meta).await? {
        if r.group != except
            && r.counts_against(kind)
            && (groups.iter().any(|g| r.holds(g))
                || intents.get(&r.group).is_some_and(|i| r.pending(i)))
        {
            *out.entry(r.node).or_default() += r.remaining();
        }
    }
    Ok(out)
}

/// Run one repair attempt of slot `index` onto `target` and record how it
/// ended (Phase 3.13). Returns whether the group holds back cache repair.
#[allow(clippy::too_many_arguments)]
async fn attempt<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
    nodes: &HashMap<u64, NodeRecord>,
    group_id: u64,
    index: usize,
    target: GroupMember,
    opts: &RepairOptions,
    cache: bool,
    healthy: &impl Fn(u64) -> bool,
    prior: Option<&RepairStatus>,
) -> Result<(bool, Option<Decision>), ControlError> {
    let attempt_started = std::time::Instant::now();
    let result = run_repair(meta, clients, group_id, index, target, opts, metrics, cache).await;
    metrics.repair_attempt_max_ms.fetch_max(
        u64::try_from(attempt_started.elapsed().as_millis()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    // What the move put on its target no longer needs reserving.
    if let Ok(r) = &result
        && !cache
        && !r.complete
        && r.target_bytes > 0
    {
        reservation::note_written(meta, target.node_id, group_id, r.epoch, r.target_bytes).await?;
    }
    match &result {
        Err(e) if is_fenced(e) => return result.map(|_| (true, None)),
        Err(ControlError::Meta(_)) => return result.map(|_| (true, None)),
        Err(e) => {
            warn!(group = group_id, error = %e, "repair of the group failed; the others go on");
            metrics
                .repair_group_errors_total
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(_) => {}
    }
    let after = cluster::get_group(meta, group_id).await?;
    let (Some(cursor), GroupState::Repairing) = (after.repair_cursor, after.state) else {
        if after.state == GroupState::Active {
            repair_status::clear(meta, group_id).await?;
            reservation::clear(meta, target.node_id, group_id).await?;
            metrics.verify_cache.forget_group(group_id);
        }
        return Ok((true, None));
    };
    let rebuilt = usize::from(cursor.member);
    let Some(now_target) = after.members.get(rebuilt).copied() else {
        return Ok((true, None));
    };
    let target_addr = nodes.get(&target.node_id).map(|n| n.address.as_str());
    let members_down = after.members.iter().any(|m| !healthy(m.node_id));
    let outcome = classify(
        &result,
        target_addr,
        members_down,
        |i| after.members.get(i).is_some_and(|m| !healthy(m.node_id)),
        (usize::from(after.data_shards), after.members.len()),
    );
    let obs = Observation {
        group: group_id,
        epoch: after.epoch,
        index: cursor.member,
        target: now_target.node_id,
        target_candidate: true,
        unhealthy_others: after
            .members
            .iter()
            .enumerate()
            .filter(|(i, m)| *i != rebuilt && !healthy(m.node_id))
            .map(|(i, _)| u16::try_from(i).unwrap_or(u16::MAX))
            .collect(),
        auto_repair: cfg.auto_repair,
        data_shards: usize::from(after.data_shards),
        members: after.members.len(),
        // Whatever slot it names (re-review): `current` keeps what another
        // slot's status excludes and distrusts.
        status: prior.cloned(),
        now_ms: now_ms(),
        pacing: cfg.repair_pacing,
    };
    let second = repair_liveness::second_failure(&obs, &outcome);
    if let Some(s) = repair_liveness::after(&obs, &outcome) {
        let busy = s.reason.holds_background_work();
        repair_status::put(meta, &s).await?;
        Ok((busy, second))
    } else {
        repair_status::clear(meta, group_id).await?;
        Ok((true, None))
    }
}

/// How a repair attempt ended, for its status (Phase 3.13).
fn classify(
    result: &Result<repair::RepairReport, ControlError>,
    target_addr: Option<&str>,
    members_down: bool,
    unhealthy: impl Fn(usize) -> bool,
    shape: (usize, usize),
) -> Outcome {
    match result {
        Ok(r) if r.complete => Outcome::Complete,
        Ok(r) if r.budget_exhausted => Outcome::Budget,
        Ok(r) => {
            let c = &r.completeness;
            // Per chunk: shards on the members that answered (re-review).
            let (k, n) = shape;
            let down: BTreeSet<usize> = c
                .unavailable
                .iter()
                .copied()
                .chain((0..n).filter(|&i| unhealthy(i)))
                .collect();
            let chunks_short = c.missing_elsewhere.len() >= 1000
                || c.missing_elsewhere.iter().any(|(_, missing)| {
                    let gone: BTreeSet<usize> = down
                        .iter()
                        .copied()
                        .chain(missing.iter().map(|&i| usize::from(i)))
                        .collect();
                    n.saturating_sub(gone.len()) < k
                });
            Outcome::Incomplete {
                chunks_short,
                slot_whole: r.slot_whole,
                unavailable: c
                    .unavailable
                    .iter()
                    .map(|&i| u16::try_from(i).unwrap_or(u16::MAX))
                    .collect(),
                unhealthy_unavailable: c.unavailable.iter().any(|&i| unhealthy(i)),
                progressed: r.reconstructed > 0,
                detail: format!(
                    "completeness: {} chunks missing on some member, {} length mismatches, corrupt {:?}, members not answering {:?}",
                    c.missing, c.length_mismatch, c.corrupt, c.unavailable
                ),
            }
        }
        // The target stored bytes that do not check (`repair.rs`: the
        // replacement's own report), as opposed to a chunk the sources
        // cannot give (any other integrity error, found before the write).
        Err(ControlError::Corrupt(m)) if m.starts_with("replacement ") => Outcome::TargetFailure {
            corrupt: true,
            detail: m.clone(),
        },
        Err(e @ (ControlError::Unrecoverable { .. } | ControlError::Corrupt(_))) => {
            Outcome::Unrebuilt {
                members_down,
                detail: e.to_string(),
            }
        }
        Err(e @ ControlError::Node { addr, .. }) if Some(addr.as_str()) == target_addr => {
            Outcome::TargetFailure {
                corrupt: false,
                detail: e.to_string(),
            }
        }
        Err(ControlError::Other(m)) if m.starts_with("replacement ") => Outcome::TargetFailure {
            corrupt: false,
            detail: m.clone(),
        },
        Err(e) => Outcome::Transient(e.to_string()),
    }
}

/// Write `s` unless it is what is recorded already.
async fn put_status<S: KvStore>(
    meta: &S,
    prior: Option<&RepairStatus>,
    s: &RepairStatus,
) -> Result<(), ControlError> {
    if prior != Some(s) {
        repair_status::put(meta, s).await?;
    }
    Ok(())
}

pub(crate) fn choose_for(
    nodes: &HashMap<u64, NodeRecord>,
    all_groups: &[GroupRecord],
    g: &GroupRecord,
    index: usize,
    failure_domain: &str,
    healthy: &impl Fn(u64) -> bool,
) -> Result<Option<GroupMember>, ControlError> {
    let node_list: Vec<NodeRecord> = nodes.values().filter(|n| healthy(n.id)).cloned().collect();
    let devices = groups::devices_of(&node_list, all_groups);
    // The remaining members' domains come from their node records' labels,
    // healthy or not, and never from the candidates (which leave the
    // members out): a replacement sharing a domain with any of them would
    // leave the group one domain short. The device need not be registered
    // any more (a node restarted without a dead disk); a member whose node
    // record is gone has no known domain: no replacement is chosen.
    let mut remaining = Vec::new();
    for (_, m) in g.members.iter().enumerate().filter(|(i, _)| *i != index) {
        let Some(n) = nodes.get(&m.node_id) else {
            warn!(
                group = g.id,
                node = m.node_id,
                "no replacement chosen: a remaining member's node record is gone, so its failure domain is unknown"
            );
            return Ok(None);
        };
        remaining.push(arca_lake_placement::Device {
            node_id: m.node_id,
            device_id: m.device_id,
            labels: n.labels.iter().cloned().collect(),
            free_bytes: 0,
            group_count: 0,
        });
    }
    // Never pick a node that is already a member, and stay on the device
    // class the group's other members use (a durable member must not move
    // onto a cache-tier node, nor the reverse).
    let member_nodes: BTreeSet<u64> = g.members.iter().map(|m| m.node_id).collect();
    let class_of = |id: u64| {
        nodes.get(&id).and_then(|n| {
            n.labels
                .iter()
                .find(|(k, _)| k == "device_class")
                .map(|(_, v)| v.clone())
        })
    };
    // Every member, the one being replaced included (its node record keeps
    // its labels after it died): a one-member cache group has no other.
    let classes: BTreeSet<Option<String>> = g.members.iter().map(|m| class_of(m.node_id)).collect();
    let candidates: Vec<_> = devices
        .into_iter()
        .filter(|d| !member_nodes.contains(&d.node_id))
        .filter(|d| classes.is_empty() || classes.contains(&class_of(d.node_id)))
        .collect();
    let spec = GroupSpec {
        total_shards: g.members.len(),
        failure_domain: failure_domain.to_string(),
    };
    match choose_replacement(&candidates, &spec, &remaining) {
        Ok(m) => Ok(Some(GroupMember {
            node_id: m.node_id,
            device_id: m.device_id,
        })),
        Err(arca_lake_placement::PlacementError::InsufficientDomains { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_repair<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    group_id: u64,
    index: usize,
    replacement: GroupMember,
    opts: &RepairOptions,
    metrics: &Metrics,
    cache: bool,
) -> Result<repair::RepairReport, ControlError> {
    let (started, reconstructed, completed) = if cache {
        (
            &metrics.cache_repairs_started_total,
            &metrics.cache_chunks_reconstructed_total,
            &metrics.cache_repairs_completed_total,
        )
    } else {
        (
            &metrics.repairs_started_total,
            &metrics.chunks_reconstructed_total,
            &metrics.repairs_completed_total,
        )
    };
    started.fetch_add(1, Ordering::Relaxed);
    let report = repair::replace_member_with(
        meta,
        clients,
        group_id,
        index,
        replacement,
        opts,
        async |_| Ok(()),
    )
    .await?;
    reconstructed.fetch_add(report.reconstructed as u64, Ordering::Relaxed);
    if report.complete {
        completed.fetch_add(1, Ordering::Relaxed);
    }
    info!(group = group_id, index, cache, ?report, "repair pass");
    Ok(report)
}

/// LRU eviction of every configured cache tier (RFC-006 §5.4).
async fn evict_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
) -> Result<(), ControlError> {
    if cfg.cache_tiers.is_empty() {
        return Ok(());
    }
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let groups: HashMap<u64, GroupRecord> = cluster::list_groups(meta)
        .await?
        .into_iter()
        .map(|g| (g.id, g))
        .collect();
    let addr_of = |node_id: u64| nodes.get(&node_id).map(|n| n.address.clone());
    for tier in &cfg.cache_tiers {
        let r = arca_lake_tiering::cache::evict_tier(meta, clients, tier, &groups, &addr_of)
            .await
            .map_err(|e| ControlError::Other(e.to_string()))?;
        metrics
            .cache_evicted_total
            .fetch_add(r.evicted as u64, Ordering::Relaxed);
        metrics
            .cache_usage_bytes
            .store(r.usage_after, Ordering::Relaxed);
        metrics
            .cache_over_watermark
            .store(r.over_watermark, Ordering::Relaxed);
        if r.evicted > 0 || r.over_watermark {
            info!(tier = %tier.tier, ?r, "evict pass");
        }
    }
    Ok(())
}

/// The reconciliation cycle as measured (ADR-0033), ms: the longest of the
/// last full sweep this process saw, the last one the cursor records, and
/// the sweep in progress: its time so far, extrapolated to every pair from
/// the pairs before its cursor (`progress`: done, total; the cursor lags
/// the work by up to a slice, so this errs long). A leader that took over
/// in the middle of a sweep has only that (review).
fn reconcile_cycle_ms(
    c: &bg_cursor::BackgroundCursor,
    seen_ms: u64,
    now_ms: u64,
    progress: (usize, usize),
) -> u64 {
    let recorded = if c.sweep_started_ms > 0 && c.last_sweep_ms >= c.sweep_started_ms {
        c.last_sweep_ms - c.sweep_started_ms
    } else if c.sweep_started_ms > 0 {
        let elapsed = now_ms.saturating_sub(c.sweep_started_ms);
        match progress {
            (done, total) if done > 0 && total >= done => {
                elapsed.saturating_mul(total as u64) / done as u64
            }
            _ => elapsed,
        }
    } else {
        0
    };
    recorded.max(seen_ms)
}

/// Capacity rebalancing pass (Phase 3.8).
async fn rebalance_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
) -> Result<(), ControlError> {
    let Some(policy) = cfg.rebalance.as_ref() else {
        return Ok(());
    };
    // A migration's cleanup deletes too (its own target copies, which a
    // restore can make the referenced ones): no pass on metadata that went
    // back (re-review).
    advance_lineage(meta, metrics).await?;
    // A plan's sources are freed by reconciliation: the settle time covers
    // the reconciliation cycle as measured (a sweep, and the interval
    // before the next: a pair visited just before a move is visited again
    // only then; review) and the minimum orphan age (ADR-0033), never less
    // than configured.
    let cursor = bg_cursor::get(meta, bg_cursor::RECONCILE).await?;
    let groups: Vec<u64> = cluster::list_groups(meta)
        .await?
        .iter()
        .map(|g| g.id)
        .collect();
    let nodes: Vec<u64> = cluster::list_nodes(meta)
        .await?
        .iter()
        .map(|n| n.id)
        .collect();
    let cycle = reconcile_cycle_ms(
        &cursor,
        metrics.reconcile_sweep_ms.load(Ordering::Relaxed),
        now_ms(),
        crate::sweep::progress(bg_cursor::parse_pair(&cursor.after), &groups, &nodes),
    );
    let settle_ms = policy.settle_ms.max(
        cycle
            .saturating_add(ms(cfg.reconcile_interval))
            .saturating_add(ms(cfg.min_orphan_age)),
    );
    metrics
        .rebalance_settle_ms
        .store(settle_ms, Ordering::Relaxed);
    // The admission's placement threshold, and what the rest of the
    // controller is doing: rebalancing yields to them (R-93).
    let policy = crate::rebalance::RebalancePolicy {
        settle_ms,
        max_chunk_bytes: cfg.admission_max_chunk_bytes,
        group_min_shards: cfg.admission.group_min_shards,
        // A record the leader republishes every health pass.
        report_max_age_ms: policy.report_max_age_ms.max(3 * ms(cfg.health_interval)),
        ..policy.clone()
    };
    let pressure = crate::rebalance::Pressure {
        gc_backlog_bytes: metrics.gc_backlog_bytes.load(Ordering::Relaxed),
        tiering_active: metrics.tiering_active.load(Ordering::Relaxed),
    };
    let r = crate::rebalance::run_once_with(meta, clients, &policy, pressure, now_ms()).await?;
    metrics
        .rebalance_capacity_stops_total
        .fetch_add(r.capacity_stops, Ordering::Relaxed);
    metrics
        .rebalance_moved_chunks_total
        .fetch_add(r.moved_chunks, Ordering::Relaxed);
    metrics
        .rebalance_moved_bytes_total
        .fetch_add(r.moved_bytes, Ordering::Relaxed);
    metrics
        .rebalance_skipped_chunks_total
        .fetch_add(r.skipped_chunks, Ordering::Relaxed);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let permille = r.plan.as_ref().map_or(0, |p| {
        if p.target_room == 0 {
            0
        } else {
            ((1.0 - p.source_room as f64 / p.target_room as f64) * 1000.0) as u64
        }
    });
    metrics
        .rebalance_imbalance_permille
        .store(permille, Ordering::Relaxed);
    if r.moved_chunks + r.planned_chunks > 0 {
        info!(?r, "rebalance pass");
    }
    Ok(())
}

/// Clears the tiering-active flag when a tiering pass ends (R-93).
struct TieringActive<'a>(&'a std::sync::atomic::AtomicBool);

impl Drop for TieringActive<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// Acknowledge the tiering record's current generation for this term and
/// incarnation (ADR-0035 §10.1). Returns the record as this leader may use
/// it.
/// The health pass's acknowledgment: only when no tiering pass holds the
/// gate (a new generation after the pass that read the previous one);
/// `Ok(false)` when one does.
async fn acknowledge_tiering_if_no_pass<S: KvStore>(
    meta: &S,
    metrics: &Metrics,
    incarnation: [u8; 16],
) -> Result<bool, ControlError> {
    let Ok(_gate) = metrics.tiering_gate.try_lock() else {
        return Ok(false);
    };
    acknowledge_tiering(meta, metrics, incarnation).await?;
    Ok(true)
}

/// The caller holds `metrics.tiering_gate`.
async fn acknowledge_tiering<S: KvStore>(
    meta: &S,
    metrics: &Metrics,
    incarnation: [u8; 16],
) -> Result<Option<arca_lake_metadata::ops::tiering::TieringRecord>, ControlError> {
    use arca_lake_metadata::ops::tiering;
    let term = metrics.leader_token.load(Ordering::Relaxed);
    let Some(r) = tiering::current(meta).await? else {
        return Ok(None);
    };
    if r.acknowledged_by(term) && r.applied_incarnation == incarnation {
        return Ok(Some(r));
    }
    let acked = tiering::acknowledge(meta, term, incarnation, r.generation, now_ms()).await?;
    if let Some(a) = &acked {
        info!(
            generation = a.generation,
            enabled = a.enabled,
            term,
            grace_from_ms = a.grace_from_ms,
            promotion_from_ms = a.promotion_from_ms,
            "tiering record acknowledged"
        );
    }
    Ok(acked)
}

/// Automatic tiering pass (RFC-006 §5.8), under the tiering record
/// (ADR-0035 option A): the record is the source of truth; the
/// controller's `--tiering-*` flags only seed an absent record.
async fn tiering_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
    incarnation: [u8; 16],
) -> Result<(), ControlError> {
    use arca_lake_metadata::ops::tiering;
    // From the record's read to the pass's end (ADR-0035 §10.1).
    let _gate = metrics.tiering_gate.lock().await;
    let Some(record) = tiering::current(meta).await? else {
        if let Some(flags) = cfg.tiering.as_ref()
            && tiering::seed_if_absent(meta, &flags.to_record(), now_ms()).await?
        {
            info!(
                policy = ?flags.to_record(),
                term = metrics.leader_token.load(Ordering::Relaxed),
                "tiering record seeded from the controller's flags"
            );
        }
        // It runs once a leader has acknowledged it.
        return Ok(());
    };
    if let Some(flags) = cfg.tiering.as_ref()
        && flags.to_record() != record.policy
    {
        warn!(
            flags = ?flags.to_record(),
            record = ?record.policy,
            generation = record.generation,
            "tiering: --tiering-* flags differ from the tiering record, which is used"
        );
    }
    // Acknowledged first (this pass holds the gate).
    let Some(record) = acknowledge_tiering(meta, metrics, incarnation).await? else {
        return Ok(());
    };
    let term = metrics.leader_token.load(Ordering::Relaxed);
    if !(record.enabled
        && record.acknowledged_by(term)
        && record.applied_incarnation == incarnation)
    {
        return Ok(());
    }
    let policy = crate::tiering::TieringPolicy::from_record(&record.policy);
    let policy = &policy;
    let fence = crate::tiering::PolicyFence {
        generation: record.generation,
        grace_from_ms: record.grace_from_ms,
        promotion_from_ms: record.promotion_from_ms,
        margin_ms: tiering::grace_margin_ms(
            u64::try_from(cfg.tiering_clock_skew.as_millis()).unwrap_or(u64::MAX),
        ),
    };
    advance_lineage(meta, metrics).await?;
    // Moves need room on their target: none while any tier is throttled or
    // worse (Phase 3.6 admission). Decided on the published record, not on
    // this process's tracker, which is empty (normal) at a new leader's
    // first pass; none without a current record, and none while repair has
    // work (Phase 3.10 audit).
    if metrics.admission.worst() >= crate::backpressure::State::Throttled {
        return Ok(());
    }
    let now = now_ms();
    let max_age = 60_000u64.max(3 * ms(cfg.health_interval));
    match arca_lake_metadata::ops::admission::current(meta).await? {
        Some(r)
            if now.saturating_sub(r.computed_ms) <= max_age
                && r.tiers.iter().all(|t| t.state < 2) => {}
        _ => return Ok(()),
    }
    // Phase 3.13 (R-104): only a repair making progress holds tiering.
    let groups = cluster::list_groups(meta).await?;
    let statuses = crate::repair_liveness::statuses(meta).await?;
    if crate::repair_liveness::holding_group(&groups, &statuses).is_some() {
        return Ok(());
    }
    if arca_lake_metadata::ops::integrity::any_actionable(meta).await? {
        return Ok(());
    }
    metrics.tiering_active.store(true, Ordering::Relaxed);
    let _active = TieringActive(&metrics.tiering_active);
    let r = crate::tiering::run_once_bounded_fenced(
        meta,
        clients,
        policy,
        now_ms(),
        cfg.tiering_generations_per_pass,
        Some(fence),
    )
    .await?;
    metrics
        .tiering_errors_total
        .fetch_add(r.errors as u64, Ordering::Relaxed);
    metrics
        .chunks_promoted_total
        .fetch_add(r.promoted_chunks as u64, Ordering::Relaxed);
    metrics
        .chunks_demoted_total
        .fetch_add(r.demoted_chunks as u64, Ordering::Relaxed);
    if r.promoted_chunks + r.demoted_chunks > 0 {
        info!(?r, "tiering pass");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Three-host validation of 0.1.0: the replacement of a failed member
    /// keeps every member on its own host, the unhealthy members' hosts
    /// included; with no free host, none is chosen.
    #[test]
    fn a_replacement_member_keeps_the_groups_hosts_distinct() {
        use arca_lake_metadata::records::{GroupRole, GroupState};
        // Nodes 1..=6: hosts a b c a b c.
        let nodes: HashMap<u64, NodeRecord> = (1..=6u64)
            .map(|id| {
                let host = ["a", "b", "c"][usize::try_from((id - 1) % 3).unwrap()];
                let rec = NodeRecord {
                    id,
                    address: format!("127.0.0.1:{}", 9000 + id),
                    labels: vec![
                        ("host".into(), host.into()),
                        ("device_class".into(), "hdd".into()),
                    ],
                    devices: vec![0],
                    registered_ms: 1,
                    last_seen_ms: 1,
                };
                (id, rec)
            })
            .collect();
        let group = |id: u64, members: [u64; 3]| GroupRecord {
            id,
            profile: "p".into(),
            tier: "default".into(),
            scheme: "rs".into(),
            data_shards: 2,
            parity_shards: 1,
            members: members
                .iter()
                .map(|&node_id| GroupMember {
                    node_id,
                    device_id: 0,
                })
                .collect(),
            epoch: 1,
            state: GroupState::Active,
            repair_cursor: None,
            role: GroupRole::Durable,
        };
        let groups = vec![group(1, [1, 2, 3]), group(2, [4, 5, 6])];
        let pick = |down: &[u64]| {
            choose_for(&nodes, &groups, &groups[1], 1, "host", &|id| {
                !down.contains(&id)
            })
            .unwrap()
            .map(|m| m.node_id)
        };
        // Node 5 (host b) failed: node 2 is the only node on a free host.
        assert_eq!(pick(&[5]), Some(2));
        // Node 6 (host c) unhealthy as well: its host still counts.
        assert_eq!(pick(&[5, 6]), Some(2));
        // Node 2 unavailable too: no node on a free host, none chosen.
        assert_eq!(pick(&[5, 2]), None);
        // A remaining member's node re-registered without its device (a dead
        // disk): its host still counts.
        let mut n = nodes.clone();
        n.get_mut(&6).unwrap().devices.clear();
        let r = choose_for(&n, &groups, &groups[1], 1, "host", &|id| id != 5).unwrap();
        assert_eq!(r.map(|m| m.node_id), Some(2));
        // A remaining member's node record gone: its domain is unknown.
        let mut n = nodes.clone();
        n.remove(&6);
        let r = choose_for(&n, &groups, &groups[1], 1, "host", &|id| id != 5).unwrap();
        assert_eq!(r, None);
    }

    /// ADR-0035 §10.1: the controller's flags only seed an absent record;
    /// the record wins over different flags; a leader acknowledges for its
    /// term and incarnation (a restore acknowledges again); a running pass
    /// holds the gate the health pass acknowledges under.
    #[tokio::test]
    async fn the_tiering_record_is_seeded_once_and_acknowledged_per_term_and_incarnation() {
        use arca_lake_metadata::ops::tiering;
        let meta = arca_lake_metadata::mem::MemKv::default();
        let clients = NodeClients::new(Duration::from_secs(1));
        let metrics = Metrics::default();
        metrics.leader_token.store(4, Ordering::Relaxed);
        let inc = [3u8; 16];
        let flags = crate::tiering::TieringPolicy {
            tiers: vec!["default".into(), "fast".into()],
            promote_reads: 5,
            promote_window: Duration::from_mins(10),
            demote_after: Duration::from_mins(30),
        };
        // No flags: nothing seeded.
        let cfg = ControllerConfig::new("c");
        tiering_pass(&meta, &clients, &cfg, &metrics, inc)
            .await
            .unwrap();
        assert_eq!(tiering::current(&meta).await.unwrap(), None);
        // Flags: seeded, then acknowledged by the next pass.
        let mut cfg = ControllerConfig::new("c");
        cfg.tiering = Some(flags.clone());
        tiering_pass(&meta, &clients, &cfg, &metrics, inc)
            .await
            .unwrap();
        let r = tiering::current(&meta).await.unwrap().unwrap();
        assert!(
            r.enabled && r.applied_term == 0,
            "seeded, not yet acknowledged"
        );
        tiering_pass(&meta, &clients, &cfg, &metrics, inc)
            .await
            .unwrap();
        let r = tiering::current(&meta).await.unwrap().unwrap();
        assert!(r.acknowledged_by(4) && r.applied_incarnation == inc);
        // Different flags do not change the record.
        cfg.tiering = Some(crate::tiering::TieringPolicy {
            demote_after: Duration::from_mins(1),
            ..flags.clone()
        });
        tiering_pass(&meta, &clients, &cfg, &metrics, inc)
            .await
            .unwrap();
        assert_eq!(
            tiering::current(&meta).await.unwrap().unwrap().policy,
            flags.to_record()
        );
        // An admin change is acknowledged by the health path only while no
        // pass holds the gate.
        let g = tiering::admin_change(
            &meta,
            &tiering::AdminChange::SetPolicy(
                crate::tiering::TieringPolicy {
                    promote_reads: 9,
                    ..flags.clone()
                }
                .to_record(),
            ),
            1,
        )
        .await
        .unwrap()
        .generation;
        {
            // A pass running: the health pass's acknowledgment waits.
            let _pass = metrics.tiering_gate.lock().await;
            assert!(
                !acknowledge_tiering_if_no_pass(&meta, &metrics, inc)
                    .await
                    .unwrap()
            );
            let r = tiering::current(&meta).await.unwrap().unwrap();
            assert!(!r.acknowledged_by(4), "acknowledged during a pass");
        }
        assert!(
            acknowledge_tiering_if_no_pass(&meta, &metrics, inc)
                .await
                .unwrap()
        );
        let r = tiering::current(&meta).await.unwrap().unwrap();
        assert_eq!((r.applied_generation, r.applied_term), (g, 4));
        // A new term, then a restore (another incarnation): acknowledged
        // again; the restore restarts the grace.
        metrics.leader_token.store(5, Ordering::Relaxed);
        let r = acknowledge_tiering(&meta, &metrics, inc)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.applied_term, 5);
        let grace = r.grace_from_ms;
        let r = acknowledge_tiering(&meta, &metrics, [4u8; 16])
            .await
            .unwrap()
            .unwrap();
        assert!(r.applied_incarnation == [4u8; 16] && r.grace_from_ms >= grace);
    }

    #[test]
    fn the_reconcile_cycle_is_the_longest_measured() {
        let c = |started, last| bg_cursor::BackgroundCursor {
            sweep_started_ms: started,
            last_sweep_ms: last,
            ..bg_cursor::BackgroundCursor::default()
        };
        let none = (0, 0);
        // Never swept: what this process saw, else nothing.
        assert_eq!(reconcile_cycle_ms(&c(0, 0), 0, 9_000, none), 0);
        assert_eq!(reconcile_cycle_ms(&c(0, 0), 700, 9_000, none), 700);
        // A sweep completed: its length.
        assert_eq!(reconcile_cycle_ms(&c(1_000, 4_000), 0, 9_000, none), 3_000);
        assert_eq!(
            reconcile_cycle_ms(&c(1_000, 4_000), 5_000, 9_000, none),
            5_000
        );
        // A sweep in progress: at least as long as it has run.
        assert_eq!(
            reconcile_cycle_ms(&c(5_000, 4_000), 2_000, 9_000, none),
            4_000
        );
        assert_eq!(
            reconcile_cycle_ms(&c(5_000, 4_000), 6_000, 9_000, none),
            6_000
        );
        // ... extrapolated from its cursor: 4 s for a quarter of the pairs.
        assert_eq!(
            reconcile_cycle_ms(&c(5_000, 4_000), 0, 9_000, (8, 32)),
            16_000
        );
        assert_eq!(
            reconcile_cycle_ms(&c(5_000, 4_000), 0, 9_000, (32, 32)),
            4_000
        );
    }
}
