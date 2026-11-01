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
#[derive(Debug, clap::Args)]
struct CacheArgs {
    /// Name of the cache tier (its cache groups are created with
    /// `admin create-cache-groups`).
    #[arg(long = "cache-tier")]
    tier: Option<String>,
    /// Capacity of the cache tier in bytes (0 = unlimited, no eviction).
    #[arg(long = "cache-capacity-bytes", default_value_t = 0)]
    capacity_bytes: u64,
    /// Fraction of the capacity above which eviction starts and admissions
    /// are rejected.
    #[arg(long = "cache-high-watermark", default_value_t = 0.9)]
    high_watermark: f64,
    /// Fraction of the capacity eviction aims for.
    #[arg(long = "cache-low-watermark", default_value_t = 0.8)]
    low_watermark: f64,
    /// Chunks larger than this are never cached (0 = no limit).
    #[arg(long = "cache-max-chunk-bytes", default_value_t = 0)]
    max_chunk_bytes: u64,
    /// Admit a chunk after this many misses observed by one gateway
    /// (0 or 1 = on the first miss).
    #[arg(long = "cache-admit-after-misses", default_value_t = 1)]
    admit_after_misses: u32,
}

impl CacheArgs {
    fn config(&self) -> Result<Option<arca_lake_tiering::cache::CacheTierConfig>, String> {
        let Some(tier) = self.tier.clone() else {
            return Ok(None);
        };
        let ok = |f: f64| (0.0..=1.0).contains(&f);
        if !ok(self.high_watermark)
            || !ok(self.low_watermark)
            || self.low_watermark > self.high_watermark
        {
            return Err("cache watermarks must satisfy 0 <= low <= high <= 1".into());
        }
        Ok(Some(arca_lake_tiering::cache::CacheTierConfig {
            tier,
            capacity_bytes: self.capacity_bytes,
            high_watermark: self.high_watermark,
            low_watermark: self.low_watermark,
            max_cacheable_bytes: self.max_chunk_bytes,
            admit_after_misses: self.admit_after_misses,
        }))
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
}

#[derive(Debug, clap::Args)]
struct StorageNodeArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:9100")]
    listen: SocketAddr,
    /// Device directory.
    #[arg(long)]
    dir: PathBuf,
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
    let server = if args.backend == "cluster" {
        let Some(path) = args.cluster_file.as_deref() else {
            return fail("--backend cluster needs --cluster-file");
        };
        let meta = match open_meta(path) {
            Ok(m) => m,
            Err(e) => return fail(e),
        };
        let backend = match arca_lake_gateway::cluster::ClusterBackend::new(
            meta,
            arca_lake_gateway::cluster::ClusterConfig {
                profile: args.profile.profile(),
                tier: args.tier.clone(),
                lease: Duration::from_secs(args.lease_secs),
                chunk_batch: 64,
                shard_frame_bytes: 256 << 10,
                node_timeout: Duration::from_secs(10),
                cache,
            },
        )
        .await
        {
            Ok(b) => b,
            Err(e) => return fail(format!("cannot start the cluster backend: {e}")),
        };
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
