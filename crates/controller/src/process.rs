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
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::ops::leader;
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
            http_listen: None,
            cache_tiers: Vec::new(),
            evict_interval: Duration::from_secs(10),
            tiering: None,
            tiering_interval: Duration::from_mins(5),
            cache_repair_chunks_per_pass: 256,
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
    let metrics = Arc::new(Metrics::default());
    let (tx, rx) = watch::channel(false);
    let mut http_addr = None;
    if let Some(addr) = cfg.http_listen {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        http_addr = Some(listener.local_addr()?);
        tokio::spawn(serve_http(listener, Arc::clone(&metrics), rx.clone()));
    }
    let m = Arc::clone(&metrics);
    let task = tokio::spawn(run(meta, cfg, m, rx));
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
) {
    let renew_every = cfg.lease / 3;
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
        let tasks = start_leader_tasks(&fenced, &cfg, &metrics, stop_rx);
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
    meta: &Arc<FencedKv<S>>,
    cfg: &ControllerConfig,
    metrics: &Arc<Metrics>,
    stop: watch::Receiver<bool>,
) -> Vec<JoinHandle<()>> {
    let clients =
        NodeClients::with_policy(cfg.rpc_policy.clone().unwrap_or_else(|| {
            arca_lake_protocol::client::RpcPolicy::from_timeout(cfg.node_timeout)
        }));
    vec![
        tokio::spawn(scheduler(
            "gc",
            cfg.gc_interval,
            stop.clone(),
            Arc::clone(metrics),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(meta);
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
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(meta);
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
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(meta);
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
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(meta);
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
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(meta);
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
            stop,
            Arc::clone(metrics),
            {
                let metrics = Arc::clone(metrics);
                let meta = Arc::clone(meta);
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
    ]
}

/// Run `pass` every `interval`, with exponential backoff (capped at eight
/// intervals) after errors, until `stop` fires. A fenced error ends the
/// task at once: leadership is gone.
async fn scheduler<F, Fut>(
    name: &'static str,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
    mut pass: F,
) where
    F: FnMut() -> Fut + Send,
    Fut: std::future::Future<Output = Result<(), ControlError>> + Send,
{
    let mut failures: u32 = 0;
    loop {
        match pass().await {
            Ok(()) => failures = 0,
            Err(e) => {
                metrics.task_errors_total.fetch_add(1, Ordering::Relaxed);
                if is_fenced(&e) {
                    warn!(task = name, error = %e, "fenced; stopping task");
                    return;
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
    for n in &nodes {
        match clients.health(&n.address).await {
            Ok(_) => {
                cluster::touch_node(meta, n.id, now).await?;
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
        )
        .await?;
    }
    Ok(())
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
) -> Result<bool, ControlError> {
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
        GroupState::Active if cfg.auto_repair => {
            let Some(index) = g.members.iter().position(|m| !healthy(m.node_id)) else {
                return Ok(false);
            };
            let replacement =
                choose_for(nodes, all_groups, g, index, &cfg.failure_domain, healthy)?;
            let Some(replacement) = replacement else {
                warn!(group = g.id, index, "no healthy replacement available");
                return Ok(true);
            };
            info!(
                group = g.id,
                index,
                node = replacement.node_id,
                cache = g.is_cache(),
                "auto repair"
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
    // Never pick a node that is already a member.
    let member_nodes: BTreeSet<u64> = g.members.iter().map(|m| m.node_id).collect();
    let candidates: Vec<_> = devices
        .into_iter()
        .filter(|d| !member_nodes.contains(&d.node_id))
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
