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

use arca_lake_metadata::fence::FencedKv;
use arca_lake_metadata::guard::{GuardedKv, OpClass};
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::ops::leader;
use arca_lake_metadata::ops::{compat, membership};
use arca_lake_metadata::records::{GroupMember, GroupRecord, GroupState, NodeRecord};
use arca_lake_metadata::{KvError, KvStore, MetaError};
use arca_lake_placement::{GroupSpec, choose_replacement};
use arca_lake_protocol::client::NodeClients;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::metrics::{Metrics, serve_http};
use crate::repair::RepairOptions;
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
    /// Interval between repair scheduler passes.
    pub repair_interval: Duration,
    /// Interval between health passes.
    pub health_interval: Duration,
    /// A node is unhealthy when it has not been seen for this long.
    pub unhealthy_after: Duration,
    /// Replace unhealthy members automatically.
    pub auto_repair: bool,
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
    /// Chunks a cache-group repair may reconstruct per pass before yielding
    /// (RFC-006 §5.9); 0 = unlimited.
    pub cache_repair_chunks_per_pass: usize,
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
            reconcile_interval: Duration::from_mins(5),
            repair_interval: Duration::from_secs(15),
            health_interval: Duration::from_secs(5),
            unhealthy_after: Duration::from_secs(60),
            auto_repair: false,
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
            cache_repair_chunks_per_pass: 256,
            scrub: crate::integrity::ScrubOptions::default(),
            scrub_tick: Duration::from_secs(60),
            stripe_repair: crate::integrity::RepairWorkerOptions::default(),
            stripe_repair_interval: Duration::from_secs(15),
            heartbeat: Some(Duration::from_secs(30)),
            recovery_interval: Duration::from_secs(2),
            recovery: crate::recovery::RecoveryOptions::default(),
            upgrade_interval: Duration::from_secs(5),
        }
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
    let renew_every = cfg.lease / 3;
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
    loop {
        if *shutdown.borrow() {
            return;
        }
        // Follower: try to acquire.
        let acquired =
            match leader::try_acquire(meta.as_ref(), &cfg.id, now_ms(), ms(cfg.lease)).await {
                Ok(l) => l,
                Err(e) => {
                    warn!(controller = %cfg.id, error = %e, "leader acquisition failed");
                    None
                }
            };
        let Some(leadership) = acquired else {
            tokio::select! {
                () = tokio::time::sleep(renew_every) => {}
                _ = shutdown.changed() => return,
            }
            continue;
        };
        info!(controller = %cfg.id, token = leadership.token, "acquired leadership");
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
        let tasks = start_leader_tasks(&fenced, &cfg, &metrics, &stop_rx, incarnation);
        let lost = loop {
            tokio::select! {
                () = tokio::time::sleep(renew_every) => {}
                _ = shutdown.changed() => break false,
            }
            match leader::renew(meta.as_ref(), &leadership, now_ms(), ms(cfg.lease)).await {
                Ok(true) => {}
                Ok(false) => break true,
                Err(e) => {
                    warn!(controller = %cfg.id, error = %e, "lease renewal failed; retrying");
                    // Keep going until the lease would have expired: a
                    // transient store error must not flap leadership. The
                    // fence on every transaction keeps this safe regardless.
                }
            }
        };
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

#[allow(clippy::too_many_lines)]
fn start_leader_tasks<S: KvStore + 'static>(
    fenced: &Arc<FencedKv<S>>,
    cfg: &ControllerConfig,
    metrics: &Arc<Metrics>,
    stop: &watch::Receiver<bool>,
    incarnation: [u8; 16],
) -> Vec<JoinHandle<()>> {
    let clients =
        NodeClients::with_policy(cfg.rpc_policy.clone().unwrap_or_else(|| {
            arca_lake_protocol::client::RpcPolicy::from_timeout(cfg.node_timeout)
        }))
        .with_tls(cfg.tls.clone());
    clients.set_incarnation(Some(incarnation));
    let coders = crate::integrity::coders();
    // One guarded store per operation class (RFC-015 §5.6, ADR-0016): every
    // write of a task is checked against the cluster state at commit.
    let guarded = |class: OpClass| Arc::new(GuardedKv::new(Arc::clone(fenced), incarnation, class));
    let meta_delete = guarded(OpClass::Delete);
    let meta_repair = guarded(OpClass::Repair);
    let meta_book = guarded(OpClass::Bookkeeping);
    let meta_admin = guarded(OpClass::Admin);
    let raw = Arc::clone(fenced);
    vec![
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
                    async move { health_pass(meta.as_ref(), &clients, &cfg, &metrics).await }
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
                    async move { tiering_pass(meta.as_ref(), &clients, &cfg, &metrics).await }
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
                move || {
                    let meta = Arc::clone(&meta);
                    let clients = clients.clone();
                    let cfg = cfg.clone();
                    let metrics = Arc::clone(&metrics);
                    async move { repair_pass(meta.as_ref(), &clients, &cfg, &metrics).await }
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
                let token = fenced.leadership().token;
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
    ]
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
        match pass().await {
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

fn is_fenced(e: &ControlError) -> bool {
    matches!(e, ControlError::Meta(MetaError::Kv(KvError::Fenced(_))))
}

fn is_state_refusal(e: &ControlError) -> bool {
    matches!(
        e,
        ControlError::Meta(MetaError::Kv(KvError::ClusterState(_)))
    )
}

async fn gc_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
) -> Result<(), ControlError> {
    let report = gc::run_once(meta, clients, now_ms(), ms(cfg.gc_grace), 1000).await?;
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
    let expire_before = (!cfg.multipart_expiry.is_zero())
        .then(|| now_ms().saturating_sub(ms(cfg.multipart_expiry)));
    let sweep = gc::sweep_multipart(meta, now_ms(), expire_before, 1000).await?;
    if sweep != gc::SweepReport::default() {
        info!(?sweep, "multipart sweep");
    }
    Ok(())
}

async fn reconcile_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
) -> Result<(), ControlError> {
    let nodes = cluster::list_nodes(meta).await?;
    let groups = cluster::list_groups(meta).await?;
    let mut deleted = 0usize;
    let mut unreachable = 0usize;
    for g in &groups {
        for n in &nodes {
            match gc::reconcile_node(
                meta,
                clients,
                n.id,
                &n.address,
                g,
                now_ms(),
                ms(cfg.min_orphan_age),
                cfg.page_size,
            )
            .await
            {
                Ok(r) => deleted += r.deleted(),
                Err(ControlError::Node { .. }) => unreachable += 1,
                Err(e) => return Err(e),
            }
        }
    }
    metrics
        .shards_deleted_total
        .fetch_add(deleted as u64, Ordering::Relaxed);
    info!(deleted, unreachable, "reconcile pass");
    Ok(())
}

async fn health_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
) -> Result<(), ControlError> {
    let nodes = cluster::list_nodes(meta).await?;
    let now = now_ms();
    let mut unhealthy = 0u64;
    // Cluster facts for the nodes (RFC-014 §5.3, RFC-015 §5.3): the format
    // they may write and the incarnation, with its change time.
    let gate = compat::gate(meta).await?;
    let req = arca_lake_protocol::storage::v1::HealthRequest {
        format_write: gate.format_write.unwrap_or(0),
        incarnation: gate
            .cluster
            .as_ref()
            .map(|c| bytes::Bytes::copy_from_slice(&c.incarnation))
            .unwrap_or_default(),
        incarnation_changed_ms: gate.cluster.as_ref().map_or(0, |c| c.changed_ms),
    };
    for n in &nodes {
        clients.note_node_id(&n.address, n.id);
        match clients.health_with(&n.address, req.clone()).await {
            Ok(h) => {
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
    metrics
        .nodes_total
        .store(nodes.len() as u64, Ordering::Relaxed);
    metrics.nodes_unhealthy.store(unhealthy, Ordering::Relaxed);
    Ok(())
}

/// Resume interrupted repairs; when `auto_repair` is on, replace members
/// of `Active` groups whose node is unhealthy with a healthy device in
/// another failure domain.
async fn repair_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
) -> Result<(), ControlError> {
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let all_groups = cluster::list_groups(meta).await?;
    let now = now_ms();
    let healthy = |id: u64| {
        nodes
            .get(&id)
            .is_some_and(|n| now.saturating_sub(n.last_seen_ms) <= ms(cfg.unhealthy_after))
    };
    let opts = RepairOptions {
        page_size: cfg.page_size,
        verify_all_members: false,
        max_chunks: 0,
    };
    // Operator intents (Phase 3.3 §6): draining nodes and replacement
    // requests. They change placement on purpose, so they progress only
    // while the cluster is Active (RFC-015 §5.6); repairs of unhealthy
    // members and resumed repairs run in every state that admits repair.
    let intents = Intents::load(meta, &all_groups).await?;
    intents.complete_drains(meta, &all_groups, now).await?;
    // Durable groups first; cache groups only when no durable group needed
    // attention in this pass, and with a per-pass budget (RFC-006 §5.9).
    let mut durable_busy = false;
    for g in all_groups.iter().filter(|g| !g.is_cache()) {
        if repair_one(
            meta,
            clients,
            cfg,
            metrics,
            &nodes,
            &all_groups,
            g,
            &opts,
            &healthy,
            &intents,
        )
        .await?
        {
            durable_busy = true;
        }
    }
    if durable_busy {
        return Ok(());
    }
    let cache_opts = RepairOptions {
        max_chunks: cfg.cache_repair_chunks_per_pass,
        ..opts
    };
    for g in all_groups.iter().filter(|g| g.is_cache()) {
        repair_one(
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
        )
        .await?;
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

    /// A draining node is decommissioned once no group lists it and no
    /// group is still rebuilding (its shards may still be the better
    /// source until every replacement has passed its completeness check).
    async fn complete_drains<S: KvStore>(
        &self,
        meta: &S,
        groups: &[GroupRecord],
        now: u64,
    ) -> Result<(), ControlError> {
        let repairing = groups.iter().any(|g| g.state == GroupState::Repairing);
        for &n in &self.draining {
            let member_of = groups
                .iter()
                .filter(|g| g.members.iter().any(|m| m.node_id == n))
                .count();
            if member_of == 0 && !repairing {
                membership::update_drain(meta, n, Some(now), None).await?;
                info!(node = n, "node decommissioned: no group lists it");
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

/// Resume or start the repair of one group when needed. Returns whether a
/// repair ran (or was needed and could not start).
#[allow(clippy::too_many_arguments)]
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
) -> Result<bool, ControlError> {
    let candidate = |id: u64| healthy(id) && intents.may_host(id);
    match g.state {
        GroupState::Repairing => {
            let Some(cursor) = g.repair_cursor else {
                warn!(
                    group = g.id,
                    "repairing without a cursor; needs an operator"
                );
                return Ok(true);
            };
            let index = usize::from(cursor.member);
            let Some(&member) = g.members.get(index) else {
                return Ok(true);
            };
            run_repair(
                meta,
                clients,
                g.id,
                index,
                member,
                opts,
                metrics,
                g.is_cache(),
            )
            .await?;
            Ok(true)
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
            let replacement = match to_node {
                Some(n) => choose_for(nodes, all_groups, g, index, &cfg.failure_domain, &|id| {
                    id == n && candidate(id)
                })?,
                None => choose_for(nodes, all_groups, g, index, &cfg.failure_domain, &candidate)?,
            };
            let Some(replacement) = replacement else {
                let msg = match to_node {
                    Some(n) => format!(
                        "requested node {n} is unhealthy, draining, or breaks the failure domain"
                    ),
                    None => "no healthy replacement with free failure domain".to_string(),
                };
                warn!(group = g.id, index, why, "{msg}");
                if why == "replace request" {
                    membership::note_replace_error(meta, g.id, Some(msg)).await?;
                } else if why == "drain" {
                    let node = g.members[index].node_id;
                    membership::update_drain(meta, node, None, Some(msg)).await?;
                }
                // Only an unhealthy member keeps cache repair waiting.
                return Ok(why == "auto repair");
            };
            info!(
                group = g.id,
                index,
                node = replacement.node_id,
                cache = g.is_cache(),
                why,
                "member replacement"
            );
            run_repair(
                meta,
                clients,
                g.id,
                index,
                replacement,
                opts,
                metrics,
                g.is_cache(),
            )
            .await?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn choose_for(
    nodes: &HashMap<u64, NodeRecord>,
    all_groups: &[GroupRecord],
    g: &GroupRecord,
    index: usize,
    failure_domain: &str,
    healthy: &impl Fn(u64) -> bool,
) -> Result<Option<GroupMember>, ControlError> {
    let node_list: Vec<NodeRecord> = nodes.values().filter(|n| healthy(n.id)).cloned().collect();
    let devices = groups::devices_of(&node_list, all_groups);
    let remaining: Vec<arca_lake_placement::Member> = g
        .members
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != index)
        .map(|(_, m)| arca_lake_placement::Member {
            node_id: m.node_id,
            device_id: m.device_id,
        })
        .collect();
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
) -> Result<(), ControlError> {
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
    Ok(())
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

/// Automatic tiering pass (RFC-006 §5.8).
async fn tiering_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    cfg: &ControllerConfig,
    metrics: &Metrics,
) -> Result<(), ControlError> {
    let Some(policy) = cfg.tiering.as_ref() else {
        return Ok(());
    };
    let r = crate::tiering::run_once(meta, clients, policy, now_ms()).await?;
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
