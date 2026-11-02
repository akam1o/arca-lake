//! arca-lake command-line entry point: gateway, storage node, controller,
//! and admin subcommands. Multi-process deployments use the FoundationDB
//! metadata backend (`--cluster-file`, feature `fdb`); the in-memory backend
//! exists for single-process experiments only.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use arca_lake_common::{EcProfile, SmallObjectPolicy};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "arca-lake",
    version,
    about = "arca-lake distributed object storage"
)]
struct Cli {
    /// Emit logs as JSON lines.
    #[arg(long, global = true)]
    log_json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run an S3 gateway.
    Gateway(GatewayArgs),
    /// Run a storage node.
    StorageNode(StorageNodeArgs),
    /// Run the control plane process.
    Controller(ControllerArgs),
    /// Cluster administration.
    Admin(AdminArgs),
}

#[derive(Debug, clap::Args)]
struct ProfileArgs {
    /// Data shards `k`.
    #[arg(long, default_value_t = 2)]
    k: u16,
    /// Parity shards `m`.
    #[arg(long, default_value_t = 1)]
    m: u16,
    /// Maximum chunk size in bytes.
    #[arg(long, default_value_t = 4 << 20)]
    chunk_bytes: u64,
    /// Replicate objects below this size instead of coding them (0 = never).
    #[arg(long, default_value_t = 64 << 10)]
    small_threshold: u64,
    /// Topology label whose values must differ between members.
    #[arg(long, default_value = "host")]
    failure_domain: String,
}

impl ProfileArgs {
    fn profile(&self) -> EcProfile {
        EcProfile {
            name: "default".into(),
            scheme: "rs-gf8-v1".into(),
            data_shards: self.k,
            parity_shards: self.m,
            required_parity_acks: None,
            max_chunk_bytes: self.chunk_bytes,
            small_object: (self.small_threshold > 0).then_some(SmallObjectPolicy {
                threshold_bytes: self.small_threshold,
                replicas: 3,
            }),
            failure_domain: self.failure_domain.clone(),
        }
    }
}

/// Read-through cache tier (RFC-006). Absent `--cache-tier` disables it.
/// Field names carry the `cache_` prefix on purpose: clap identifies
/// flattened arguments by field name, and `tier` already exists on the
/// gateway arguments.
#[allow(clippy::struct_field_names)]
#[derive(Debug, clap::Args)]
struct CacheArgs {
    /// Name of the cache tier (its cache groups are created with
    /// `admin create-cache-groups`).
    #[arg(long = "cache-tier")]
    cache_tier: Option<String>,
    /// Capacity of the cache tier in bytes (0 = unlimited, no eviction).
    #[arg(long = "cache-capacity-bytes", default_value_t = 0)]
    cache_capacity_bytes: u64,
    /// Fraction of the capacity above which eviction starts and admissions
    /// are rejected.
    #[arg(long = "cache-high-watermark", default_value_t = 0.9)]
    cache_high_watermark: f64,
    /// Fraction of the capacity eviction aims for.
    #[arg(long = "cache-low-watermark", default_value_t = 0.8)]
    cache_low_watermark: f64,
    /// Chunks larger than this are never cached (0 = no limit).
    #[arg(long = "cache-max-chunk-bytes", default_value_t = 0)]
    cache_max_chunk_bytes: u64,
    /// Admit a chunk after this many misses observed by one gateway
    /// (0 or 1 = on the first miss).
    #[arg(long = "cache-admit-after-misses", default_value_t = 1)]
    cache_admit_after_misses: u32,
    /// Populations one gateway runs at once; misses beyond that are served
    /// without populating.
    #[arg(long = "cache-max-populations", default_value_t = 8)]
    cache_max_populations: u32,
    /// Cache populations waiting in this gateway's queue at most; beyond
    /// that a miss is served without populating. A queued population
    /// holds its chunk's bytes, so memory is bounded by this times the
    /// chunk size.
    #[arg(long = "cache-population-queue-depth", default_value_t = 8)]
    cache_population_queue_depth: usize,
    /// Cache populations one node receives from this gateway at once.
    #[arg(long = "cache-populations-per-node", default_value_t = 2)]
    cache_populations_per_node: usize,
}

impl CacheArgs {
    fn config(&self) -> Result<Option<arca_lake_tiering::cache::CacheTierConfig>, String> {
        let Some(tier) = self.cache_tier.clone() else {
            return Ok(None);
        };
        let ok = |f: f64| (0.0..=1.0).contains(&f);
        if !ok(self.cache_high_watermark)
            || !ok(self.cache_low_watermark)
            || self.cache_low_watermark > self.cache_high_watermark
        {
            return Err("cache watermarks must satisfy 0 <= low <= high <= 1".into());
        }
        Ok(Some(arca_lake_tiering::cache::CacheTierConfig {
            tier,
            capacity_bytes: self.cache_capacity_bytes,
            high_watermark: self.cache_high_watermark,
            low_watermark: self.cache_low_watermark,
            max_cacheable_bytes: self.cache_max_chunk_bytes,
            admit_after_misses: self.cache_admit_after_misses,
            max_populations_in_flight: self.cache_max_populations,
        }))
    }
}

/// Deadlines and retries of storage node RPCs (RFC-009 §7, R-47).
#[allow(clippy::struct_field_names)]
#[derive(Debug, clap::Args)]
struct NodeRpcArgs {
    /// Timeout of unary node RPCs and deadline floor of shard streams, s.
    #[arg(long = "node-timeout-secs", default_value_t = 10)]
    node_timeout_secs: u64,
    /// A shard stream with no frame moving for this long is aborted, s.
    #[arg(long = "node-progress-timeout-secs", default_value_t = 10)]
    node_progress_timeout_secs: u64,
    /// Minimum transfer rate that extends the absolute deadline of a shard
    /// stream, KiB/s (0 = the deadline is the timeout alone).
    #[arg(long = "node-min-rate-kib", default_value_t = 256)]
    node_min_rate_kib: u64,
    /// Attempts per shard RPC on transport errors (1 = no retry).
    #[arg(long = "node-max-attempts", default_value_t = 3)]
    node_max_attempts: u32,
}

impl NodeRpcArgs {
    fn policy(&self) -> arca_lake_protocol::client::RpcPolicy {
        let timeout = Duration::from_secs(self.node_timeout_secs.max(1));
        arca_lake_protocol::client::RpcPolicy {
            connect_timeout: (timeout / 5).max(Duration::from_millis(200)),
            unary_timeout: timeout,
            progress_timeout: Duration::from_secs(self.node_progress_timeout_secs.max(1)),
            base_deadline: timeout,
            min_rate_bytes_per_s: self.node_min_rate_kib << 10,
            max_attempts: self.node_max_attempts.max(1),
            backoff: Duration::from_millis(100),
        }
    }
}

#[derive(Debug, clap::Args)]
struct GatewayArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:9000")]
    listen: SocketAddr,
    /// Access key for static authentication. Requires `--secret-key`.
    #[arg(long, env = "ARCA_LAKE_ACCESS_KEY", requires = "secret_key")]
    access_key: Option<String>,
    /// Secret key for static authentication. Requires `--access-key`.
    #[arg(
        long,
        env = "ARCA_LAKE_SECRET_KEY",
        requires = "access_key",
        hide_env_values = true
    )]
    secret_key: Option<String>,
    /// Allow anonymous access (no authentication). Experiments only.
    #[arg(long, conflicts_with = "access_key")]
    allow_anonymous: bool,
    /// `memory` (single process, no durability) or `cluster` (FoundationDB
    /// metadata and storage nodes; needs `--cluster-file`).
    #[arg(long, default_value = "memory", value_parser = ["memory", "cluster"])]
    backend: String,
    /// FoundationDB cluster file.
    #[arg(long, env = "ARCA_FDB_CLUSTER_FILE")]
    cluster_file: Option<PathBuf>,
    /// Pending-generation lease in seconds.
    #[arg(long, default_value_t = 300)]
    lease_secs: u64,
    /// Durable tier that writes target.
    #[arg(long, default_value = "default")]
    tier: String,
    #[command(flatten)]
    profile: ProfileArgs,
    #[command(flatten)]
    cache: CacheArgs,
    #[command(flatten)]
    rpc: NodeRpcArgs,
    /// Chunks read ahead of the one being streamed per GET (1 = sequential);
    /// bounds buffered bytes per GET to about this many chunks.
    #[arg(long, default_value_t = 4)]
    read_parallelism: usize,
    /// Chunk placements of one PUT in flight at once (memory: that many
    /// chunks plus their shards per PUT).
    #[arg(long = "write-pipeline", default_value_t = 2)]
    write_pipeline: usize,
    /// Serve gateway cache counters as Prometheus text on this address
    /// (`GET /metrics`); cluster backend only.
    #[arg(long)]
    metrics_listen: Option<SocketAddr>,
}

#[derive(Debug, clap::Args)]
struct StorageNodeArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:9100")]
    listen: SocketAddr,
    /// Device directory.
    #[arg(long)]
    dir: PathBuf,
    /// Serve node timing counters in the Prometheus text format
    /// (`GET /metrics`).
    #[arg(long)]
    metrics_listen: Option<SocketAddr>,
}

#[derive(Debug, clap::Args)]
struct ControllerArgs {
    /// FoundationDB cluster file.
    #[arg(long, env = "ARCA_FDB_CLUSTER_FILE")]
    cluster_file: PathBuf,
    /// Identity in the leader record and logs.
    #[arg(long, default_value = "controller")]
    id: String,
    /// Metrics and health endpoint.
    #[arg(long)]
    http: Option<SocketAddr>,
    /// Leader lease in seconds.
    #[arg(long, default_value_t = 10)]
    lease_secs: u64,
    /// Replace members of unhealthy nodes automatically.
    #[arg(long)]
    auto_repair: bool,
    /// Seconds without a health answer before a node counts as unhealthy.
    #[arg(long, default_value_t = 60)]
    unhealthy_after_secs: u64,
    /// GC grace period in seconds.
    #[arg(long, default_value_t = 600)]
    gc_grace_secs: u64,
    /// Minimum age in seconds of orphan shards without a generation (must
    /// be at least the gateways' lease).
    #[arg(long, default_value_t = 900)]
    min_orphan_age_secs: u64,
    /// GC interval in seconds.
    #[arg(long, default_value_t = 30)]
    gc_interval_secs: u64,
    /// Reconciliation interval in seconds.
    #[arg(long, default_value_t = 300)]
    reconcile_interval_secs: u64,
    /// Repair scheduler interval in seconds.
    #[arg(long, default_value_t = 15)]
    repair_interval_secs: u64,
    /// Health probe interval in seconds.
    #[arg(long, default_value_t = 5)]
    health_interval_secs: u64,
    /// Failure domain label for replacement choice.
    #[arg(long, default_value = "host")]
    failure_domain: String,
    #[command(flatten)]
    cache: CacheArgs,
    /// Eviction pass interval in seconds.
    #[arg(long, default_value_t = 10)]
    evict_interval_secs: u64,
    /// Durable tiers from coldest to hottest for automatic promotion and
    /// demotion (RFC-006 §5.8); fewer than two disables tiering.
    #[arg(long = "tiering-tier")]
    tiering_tiers: Vec<String>,
    /// Promote a generation read at least this many times within the window.
    #[arg(long, default_value_t = 10)]
    tiering_promote_reads: u64,
    /// Promotion window in seconds.
    #[arg(long, default_value_t = 3_600)]
    tiering_promote_window_secs: u64,
    /// Demote a generation not read for this many seconds.
    #[arg(long, default_value_t = 86_400)]
    tiering_demote_after_secs: u64,
    /// Tiering pass interval in seconds.
    #[arg(long, default_value_t = 300)]
    tiering_interval_secs: u64,
    /// Chunks a cache-group repair may reconstruct per pass (0 = unlimited).
    #[arg(long, default_value_t = 256)]
    cache_repair_chunks_per_pass: usize,
    #[command(flatten)]
    rpc: NodeRpcArgs,
}

#[derive(Debug, clap::Args)]
struct AdminArgs {
    /// FoundationDB cluster file.
    #[arg(long, env = "ARCA_FDB_CLUSTER_FILE")]
    cluster_file: PathBuf,
    #[command(subcommand)]
    command: AdminCommand,
}

#[derive(Debug, Subcommand)]
enum AdminCommand {
    /// Register (or update) a storage node.
    RegisterNode {
        /// gRPC address of the node.
        #[arg(long)]
        address: String,
        /// Topology labels, `key=value`.
        #[arg(long = "label")]
        labels: Vec<String>,
        /// Device IDs served by the node.
        #[arg(long, default_value = "0")]
        devices: Vec<u32>,
    },
    /// Create stripe groups for the profile on a durable tier.
    CreateGroups {
        /// Groups per scheme.
        #[arg(long, default_value_t = 1)]
        count: usize,
        /// Durable tier name recorded on the groups.
        #[arg(long, default_value = "default")]
        tier: String,
        /// Only use nodes whose `device_class` label equals this value.
        #[arg(long)]
        device_class: Option<String>,
        #[command(flatten)]
        profile: ProfileArgs,
    },
    /// Create cache groups for a cache tier from the nodes of a device
    /// class (RFC-006 §5.1, §5.9).
    CreateCacheGroups {
        /// Cache tier name.
        #[arg(long)]
        tier: String,
        /// `device_class` label of the nodes to use.
        #[arg(long)]
        device_class: String,
        /// Protection mode: `none` (one copy per device), `replication`
        /// (`--replicas` copies), or `ec` (`--data-shards` + `--parity-shards`).
        #[arg(long, default_value = "none", value_parser = ["none", "replication", "ec"])]
        protection: String,
        /// Copies for `replication`.
        #[arg(long, default_value_t = 2)]
        replicas: u16,
        /// `k` for `ec`.
        #[arg(long, default_value_t = 2)]
        data_shards: u16,
        /// `m` for `ec`.
        #[arg(long, default_value_t = 1)]
        parity_shards: u16,
        /// EC scheme for `ec`.
        #[arg(long, default_value = "rs-gf8-v1")]
        scheme: String,
        /// Groups to create for `replication` and `ec`.
        #[arg(long, default_value_t = 1)]
        count: usize,
        /// Topology label whose values must differ between members.
        #[arg(long, default_value = "host")]
        failure_domain: String,
    },
    /// Print the bytes recorded as cached on a tier (JSON).
    CacheUsage {
        /// Cache tier name.
        #[arg(long)]
        tier: String,
    },
    /// Print nodes, groups, and the leader record.
    Status,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    init_logging(cli.log_json);
    match cli.command {
        Command::Gateway(args) => run_gateway(args).await,
        Command::StorageNode(args) => run_storage_node(args).await,
        Command::Controller(args) => run_controller(args).await,
        Command::Admin(args) => run_admin(args).await,
    }
}

fn init_logging(json: bool) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}

fn fail(msg: impl std::fmt::Display) -> ExitCode {
    eprintln!("error: {msg}");
    ExitCode::from(1)
}

#[cfg(feature = "fdb")]
fn open_meta(path: &std::path::Path) -> Result<Arc<arca_lake_metadata::fdb::FdbKv>, String> {
    let s = path
        .to_str()
        .ok_or_else(|| "cluster file path is not UTF-8".to_string())?;
    arca_lake_metadata::fdb::FdbKv::open(Some(s))
        .map(Arc::new)
        .map_err(|e| format!("cannot open FoundationDB ({s}): {e}"))
}

#[cfg(not(feature = "fdb"))]
fn open_meta(_path: &std::path::Path) -> Result<Arc<arca_lake_metadata::mem::MemKv>, String> {
    Err("this binary was built without the `fdb` feature; FoundationDB is required for multi-process deployments".into())
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn run_gateway(args: GatewayArgs) -> ExitCode {
    let credentials = match (args.access_key, args.secret_key) {
        (Some(ak), Some(sk)) => Some((ak, sk)),
        _ if args.allow_anonymous => {
            eprintln!("WARNING: authentication disabled; every request is accepted");
            None
        }
        _ => return fail("provide --access-key/--secret-key or --allow-anonymous"),
    };
    let cache = match args.cache.config() {
        Ok(c) => c,
        Err(e) => return fail(e),
    };
    let cfg = arca_lake_gateway::GatewayConfig {
        listen: args.listen,
        credentials,
    };
    let mut queue_shutdown: Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    )> = None;
    let server = if args.backend == "cluster" {
        let Some(path) = args.cluster_file.as_deref() else {
            return fail("--backend cluster needs --cluster-file");
        };
        let meta = match open_meta(path) {
            Ok(m) => m,
            Err(e) => return fail(e),
        };
        let metrics_listen = args.metrics_listen;
        let backend = match arca_lake_gateway::cluster::ClusterBackend::new(
            meta,
            arca_lake_gateway::cluster::ClusterConfig {
                profile: args.profile.profile(),
                tier: args.tier.clone(),
                lease: Duration::from_secs(args.lease_secs),
                chunk_batch: 64,
                shard_frame_bytes: 256 << 10,
                node_timeout: Duration::from_secs(10),
                rpc_policy: Some(args.rpc.policy()),
                cache,
                read_parallelism: args.read_parallelism,
                population_queue_depth: args.cache.cache_population_queue_depth,
                populations_per_node: args.cache.cache_populations_per_node,
                write_pipeline: args.write_pipeline,
            },
        )
        .await
        {
            Ok(b) => b,
            Err(e) => return fail(format!("cannot start the cluster backend: {e}")),
        };
        if let Some(addr) = metrics_listen {
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => return fail(format!("cannot bind metrics endpoint {addr}: {e}")),
            };
            tracing::info!(%addr, "gateway metrics endpoint");
            let stats_backend = backend.clone();
            tokio::spawn(serve_gateway_metrics(listener, move || {
                let mut out = render_cache_stats(
                    &stats_backend.cache_stats(),
                    &stats_backend.node_clients().stats(),
                );
                render_put_stats(&mut out, &stats_backend.put_stats());
                render_queue_and_txn_stats(&mut out, &stats_backend.population_queue_stats());
                out
            }));
        }
        let queue_backend = backend.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        // Drain the population queue once the HTTP server has stopped.
        let drain = tokio::spawn(async move {
            let _ = rx.await;
            queue_backend.shutdown().await;
        });
        queue_shutdown = Some((tx, drain));
        arca_lake_gateway::serve(cfg, backend).await
    } else {
        arca_lake_gateway::serve(cfg, arca_lake_gateway::memory::MemoryBackend::new()).await
    };
    let server = match server {
        Ok(s) => s,
        Err(e) => return fail(format!("cannot bind {}: {e}", args.listen)),
    };
    tracing::info!(addr = %server.local_addr, backend = %args.backend, "gateway listening");
    wait_for_shutdown().await;
    tracing::info!("shutting down");
    server.shutdown().await;
    if let Some((tx, drain)) = queue_shutdown {
        let _ = tx.send(());
        let _ = drain.await;
    }
    ExitCode::SUCCESS
}

async fn run_storage_node(args: StorageNodeArgs) -> ExitCode {
    if let Err(e) = std::fs::create_dir_all(&args.dir) {
        return fail(format!("cannot create {}: {e}", args.dir.display()));
    }
    let cfg = arca_lake_storage_node::NodeConfig::new(args.listen, &args.dir);
    let node = match arca_lake_storage_node::serve(cfg).await {
        Ok(n) => n,
        Err(e) => return fail(format!("cannot start the storage node: {e}")),
    };
    tracing::info!(addr = %node.local_addr, dir = %args.dir.display(), "storage node listening");
    if let Some(addr) = args.metrics_listen {
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => return fail(format!("cannot bind metrics endpoint {addr}: {e}")),
        };
        tracing::info!(%addr, "storage node metrics endpoint");
        let service = Arc::clone(node.service());
        tokio::spawn(serve_gateway_metrics(listener, move || {
            render_node_stats(&service.stats(), &service.store().timings())
        }));
    }
    wait_for_shutdown().await;
    tracing::info!("shutting down");
    node.shutdown().await;
    ExitCode::SUCCESS
}

async fn run_controller(args: ControllerArgs) -> ExitCode {
    let meta = match open_meta(&args.cluster_file) {
        Ok(m) => m,
        Err(e) => return fail(e),
    };
    let mut cfg = arca_lake_controller::process::ControllerConfig::new(args.id);
    cfg.lease = Duration::from_secs(args.lease_secs);
    cfg.auto_repair = args.auto_repair;
    cfg.unhealthy_after = Duration::from_secs(args.unhealthy_after_secs);
    cfg.gc_grace = Duration::from_secs(args.gc_grace_secs);
    cfg.min_orphan_age = Duration::from_secs(args.min_orphan_age_secs);
    cfg.gc_interval = Duration::from_secs(args.gc_interval_secs);
    cfg.reconcile_interval = Duration::from_secs(args.reconcile_interval_secs);
    cfg.repair_interval = Duration::from_secs(args.repair_interval_secs);
    cfg.health_interval = Duration::from_secs(args.health_interval_secs);
    cfg.failure_domain = args.failure_domain;
    cfg.http_listen = args.http;
    match args.cache.config() {
        Ok(Some(c)) => cfg.cache_tiers = vec![c],
        Ok(None) => {}
        Err(e) => return fail(e),
    }
    cfg.evict_interval = Duration::from_secs(args.evict_interval_secs);
    if args.tiering_tiers.len() >= 2 {
        cfg.tiering = Some(arca_lake_controller::tiering::TieringPolicy {
            tiers: args.tiering_tiers,
            promote_reads: args.tiering_promote_reads,
            promote_window: Duration::from_secs(args.tiering_promote_window_secs),
            demote_after: Duration::from_secs(args.tiering_demote_after_secs),
        });
    }
    cfg.tiering_interval = Duration::from_secs(args.tiering_interval_secs);
    cfg.cache_repair_chunks_per_pass = args.cache_repair_chunks_per_pass;
    cfg.node_timeout = Duration::from_secs(args.rpc.node_timeout_secs.max(1));
    cfg.rpc_policy = Some(args.rpc.policy());
    let handle = match arca_lake_controller::process::spawn(meta, cfg).await {
        Ok(h) => h,
        Err(e) => return fail(format!("cannot start the controller: {e}")),
    };
    if let Some(addr) = handle.http_addr {
        tracing::info!(%addr, "controller metrics endpoint");
    }
    wait_for_shutdown().await;
    tracing::info!("shutting down");
    handle.shutdown().await;
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)]
async fn run_admin(args: AdminArgs) -> ExitCode {
    let meta = match open_meta(&args.cluster_file) {
        Ok(m) => m,
        Err(e) => return fail(e),
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0);
    match args.command {
        AdminCommand::RegisterNode {
            address,
            labels,
            devices,
        } => {
            let labels: Vec<(String, String)> = labels
                .iter()
                .filter_map(|l| {
                    l.split_once('=')
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                })
                .collect();
            match arca_lake_metadata::ops::cluster::register_node(
                meta.as_ref(),
                &address,
                labels,
                devices,
                now,
            )
            .await
            {
                Ok(n) => {
                    println!("node {} registered at {}", n.id, n.address);
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        AdminCommand::CreateGroups {
            count,
            tier,
            device_class,
            profile,
        } => {
            let p = profile.profile();
            if let Err(e) = p.validate() {
                return fail(e);
            }
            println!("{}", p.fault_tolerance_statement());
            let created = match device_class {
                Some(class) => {
                    arca_lake_controller::groups::create_groups_for_class(
                        meta.as_ref(),
                        &p,
                        &tier,
                        &class,
                        count,
                    )
                    .await
                }
                None => {
                    arca_lake_controller::groups::create_groups(meta.as_ref(), &p, &tier, count)
                        .await
                }
            };
            match created {
                Ok(groups) => {
                    print_groups(&groups);
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        AdminCommand::CreateCacheGroups {
            tier,
            device_class,
            protection,
            replicas,
            data_shards,
            parity_shards,
            scheme,
            count,
            failure_domain,
        } => {
            use arca_lake_controller::groups::CacheProtection;
            let protection = match protection.as_str() {
                "replication" => CacheProtection::Replication { replicas },
                "ec" => CacheProtection::ErasureCoding {
                    scheme,
                    data_shards,
                    parity_shards,
                },
                _ => CacheProtection::None,
            };
            match arca_lake_controller::groups::create_cache_groups(
                meta.as_ref(),
                &tier,
                &device_class,
                &protection,
                &failure_domain,
                count,
            )
            .await
            {
                Ok(groups) => {
                    print_groups(&groups);
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        AdminCommand::CacheUsage { tier } => {
            match arca_lake_metadata::ops::cache::usage(meta.as_ref(), &tier).await {
                Ok(bytes) => {
                    println!("{{\"tier\":\"{tier}\",\"usage_bytes\":{bytes}}}");
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        AdminCommand::Status => {
            let nodes = arca_lake_metadata::ops::cluster::list_nodes(meta.as_ref()).await;
            let groups = arca_lake_metadata::ops::cluster::list_groups(meta.as_ref()).await;
            let leader = arca_lake_metadata::ops::leader::current(meta.as_ref()).await;
            match (nodes, groups, leader) {
                (Ok(nodes), Ok(groups), Ok(leader)) => {
                    for n in nodes {
                        println!(
                            "node {} {} labels {:?} last_seen_ms {}",
                            n.id, n.address, n.labels, n.last_seen_ms
                        );
                    }
                    for g in groups {
                        println!(
                            "group {} tier {} {} {}+{} epoch {} {:?} members {:?}",
                            g.id,
                            g.tier,
                            g.scheme,
                            g.data_shards,
                            g.parity_shards,
                            g.epoch,
                            g.state,
                            g.members.iter().map(|m| m.node_id).collect::<Vec<_>>()
                        );
                    }
                    match leader {
                        Some(l) => println!(
                            "leader {} token {} lease_until_ms {}",
                            l.holder, l.token, l.lease_until_ms
                        ),
                        None => println!("leader: none"),
                    }
                    ExitCode::SUCCESS
                }
                (n, g, l) => fail(
                    n.err()
                        .or(g.err())
                        .or(l.err())
                        .map_or_else(|| "unknown".to_string(), |e| e.to_string()),
                ),
            }
        }
    }
}

fn print_groups(groups: &[arca_lake_metadata::records::GroupRecord]) {
    for g in groups {
        println!(
            "group {} tier {} {} {}+{} members {:?}",
            g.id,
            g.tier,
            g.scheme,
            g.data_shards,
            g.parity_shards,
            g.members.iter().map(|m| m.node_id).collect::<Vec<_>>()
        );
    }
}

fn render_cache_stats(
    s: &arca_lake_gateway::cluster::CacheStatsSnapshot,
    r: &arca_lake_protocol::client::RpcStatsSnapshot,
) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for (name, value) in [
        ("arca_gateway_cache_hits_total", s.hits),
        ("arca_gateway_cache_misses_total", s.misses),
        ("arca_gateway_cache_fallbacks_total", s.fallbacks),
        ("arca_gateway_cache_invalidations_total", s.invalidations),
        ("arca_gateway_cache_populated_total", s.populated),
        (
            "arca_gateway_cache_population_failures_total",
            s.population_failures,
        ),
        (
            "arca_gateway_cache_populations_deduplicated_total",
            s.populations_deduplicated,
        ),
        (
            "arca_gateway_cache_admissions_rejected_total",
            s.admissions_rejected,
        ),
        ("arca_gateway_cache_degraded_hits_total", s.degraded_hits),
        (
            "arca_gateway_cache_shards_repaired_total",
            s.shards_repaired,
        ),
        (
            "arca_gateway_cache_shard_repair_failures_total",
            s.shard_repair_failures,
        ),
        (
            "arca_gateway_cache_shard_repairs_skipped_total",
            s.shard_repairs_skipped,
        ),
        (
            "arca_gateway_cache_populations_skipped_total",
            s.populations_skipped,
        ),
        ("arca_gateway_cache_lookup_hits_total", s.lookup_hits),
        (
            "arca_gateway_cache_fallback_unreadable_total",
            s.fallback_unreadable,
        ),
        (
            "arca_gateway_cache_fallback_corrupt_total",
            s.fallback_corrupt,
        ),
        ("arca_gateway_cache_fallback_node_total", s.fallback_node),
        (
            "arca_gateway_cache_fallback_unknown_group_total",
            s.fallback_unknown_group,
        ),
        (
            "arca_gateway_cache_admission_rejected_size_total",
            s.admission_rejected_size,
        ),
        (
            "arca_gateway_cache_admission_rejected_threshold_total",
            s.admission_rejected_threshold,
        ),
        (
            "arca_gateway_cache_admission_rejected_capacity_total",
            s.admission_rejected_capacity,
        ),
        (
            "arca_gateway_cache_admission_rejected_generation_total",
            s.admission_rejected_generation,
        ),
        (
            "arca_gateway_cache_population_failures_node_total",
            s.population_failures_node,
        ),
        (
            "arca_gateway_cache_population_failures_meta_total",
            s.population_failures_meta,
        ),
        (
            "arca_gateway_cache_population_failures_corrupt_total",
            s.population_failures_corrupt,
        ),
        ("arca_gateway_node_rpc_stalled_total", r.stalled),
        (
            "arca_gateway_node_rpc_deadline_exceeded_total",
            r.deadline_exceeded,
        ),
        (
            "arca_gateway_node_rpc_unary_timeouts_total",
            r.unary_timeouts,
        ),
        ("arca_gateway_node_rpc_retries_total", r.retries),
        ("arca_gateway_node_rpc_exhausted_total", r.exhausted),
    ] {
        let _ = writeln!(out, "# TYPE {name} counter\n{name} {value}");
    }
    out
}

fn render_put_stats(out: &mut String, p: &arca_lake_gateway::cluster::PutStatsSnapshot) {
    use std::fmt::Write as _;
    let _ = writeln!(
        out,
        "# TYPE arca_gateway_put_in_flight gauge\narca_gateway_put_in_flight {}",
        p.in_flight
    );
    let _ = writeln!(out, "# TYPE arca_gateway_put_phase_seconds histogram");
    for (phase, h) in [
        ("body_read", &p.body_read),
        ("encode", &p.encode),
        ("shard_write", &p.shard_write),
        ("append", &p.append),
        ("lease_renew", &p.lease_renew),
        ("commit", &p.commit),
        ("total", &p.total),
    ] {
        h.render_prometheus(
            out,
            "arca_gateway_put_phase_seconds",
            &format!("phase=\"{phase}\""),
        );
    }
}

fn render_queue_and_txn_stats(
    out: &mut String,
    q: &arca_lake_gateway::cluster::PopQueueStatsSnapshot,
) {
    use std::fmt::Write as _;
    for (name, value) in [
        ("arca_gateway_population_queue_enqueued_total", q.enqueued),
        (
            "arca_gateway_population_queue_dropped_full_total",
            q.dropped_full,
        ),
        (
            "arca_gateway_population_queue_dropped_shutdown_total",
            q.dropped_shutdown,
        ),
        ("arca_gateway_population_queue_completed_total", q.completed),
        ("arca_gateway_population_queue_retries_total", q.retries),
        (
            "arca_gateway_population_queue_deferred_for_reads_total",
            q.deferred_for_reads,
        ),
    ] {
        let _ = writeln!(out, "# TYPE {name} counter\n{name} {value}");
    }
    for (name, value) in [
        ("arca_gateway_population_queue_queued", q.queued),
        ("arca_gateway_population_queue_running", q.running),
    ] {
        let _ = writeln!(out, "# TYPE {name} gauge\n{name} {value}");
    }
    let t = arca_lake_metadata::kv::transact_stats();
    for (name, value) in [
        ("arca_gateway_meta_txn_attempts_total", t.attempts),
        ("arca_gateway_meta_txn_conflicts_total", t.conflicts),
        (
            "arca_gateway_meta_txn_unknown_retries_total",
            t.unknown_retries,
        ),
        ("arca_gateway_meta_txn_exhausted_total", t.exhausted),
    ] {
        let _ = writeln!(out, "# TYPE {name} counter\n{name} {value}");
    }
}

fn render_node_stats(
    n: &arca_lake_storage_node::NodeStatsSnapshot,
    t: &arca_lake_storage_node::StoreTimingsSnapshot,
) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# TYPE arca_node_put_in_flight gauge\narca_node_put_in_flight {}",
        n.puts_in_flight
    );
    let _ = writeln!(out, "# TYPE arca_node_phase_seconds histogram");
    for (phase, h) in [
        ("put_recv", &n.put_recv),
        ("put_queue", &n.put_queue),
        ("put_store", &n.put_store),
        ("get_serve", &n.get_serve),
        ("store_append", &t.append),
        ("store_rollover", &t.rollover),
        ("store_sync_wait", &t.sync_wait),
        ("store_fsync", &t.fsync),
    ] {
        h.render_prometheus(
            &mut out,
            "arca_node_phase_seconds",
            &format!("phase=\"{phase}\""),
        );
    }
    out
}

/// Minimal HTTP/1.1 responder: every request gets `200 text/plain` with the
/// rendered metrics (no routing beyond that; one request per connection).
async fn serve_gateway_metrics(
    listener: tokio::net::TcpListener,
    render: impl Fn() -> String + Send + Sync + 'static,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let render = Arc::new(render);
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        let render = Arc::clone(&render);
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let body = render();
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(resp.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}
