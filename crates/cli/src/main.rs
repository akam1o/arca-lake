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
    /// Development PKI for test and lab clusters: a CA, SPIFFE role
    /// certificates and CRLs (ADR-0017). Production clusters use their own
    /// CA or SPIRE.
    Tls {
        #[command(subcommand)]
        command: TlsCommand,
    },
}

#[derive(Debug, Subcommand)]
enum TlsCommand {
    /// Create a CA in `--dir` (`ca.pem`, `ca-key.pem` with mode 0600).
    InitCa {
        /// Output directory.
        #[arg(long)]
        dir: PathBuf,
        /// CA common name.
        #[arg(long, default_value = "arca-lake development CA")]
        name: String,
        /// Validity in days.
        #[arg(long, default_value_t = 365)]
        days: u64,
    },
    /// Issue `spiffe://<trust-domain>/<role>/<name>` into `--out`
    /// (`cert.pem`, `key.pem` mode 0600, `ca.pem`).
    Issue {
        /// Directory of the CA (`ca.pem`, `ca-key.pem`).
        #[arg(long)]
        ca_dir: PathBuf,
        /// Trust domain.
        #[arg(long)]
        trust_domain: String,
        /// Role: node, gateway, controller, admin, or fdb (a FoundationDB
        /// server certificate).
        #[arg(long)]
        role: String,
        /// Name (the node id for nodes).
        #[arg(long)]
        name: String,
        /// Output directory.
        #[arg(long)]
        out: PathBuf,
        /// Validity in days.
        #[arg(long, default_value_t = 30)]
        days: u64,
        /// Serial number (default: from the clock).
        #[arg(long)]
        serial: Option<u64>,
        /// Start the validity this many days ago (with `--days` smaller,
        /// an already expired certificate: for tests).
        #[arg(long, default_value_t = 0)]
        backdate_days: u64,
    },
    /// Write a CRL revoking `--revoke` serials.
    Crl {
        /// Directory of the CA.
        #[arg(long)]
        ca_dir: PathBuf,
        /// Serials to revoke.
        #[arg(long)]
        revoke: Vec<u64>,
        /// CRL number (monotonic).
        #[arg(long)]
        number: u64,
        /// Output file.
        #[arg(long)]
        out: PathBuf,
    },
    /// Print the SPIFFE identities and expiry of a certificate.
    Show {
        /// Certificate (PEM).
        #[arg(long)]
        cert: PathBuf,
    },
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
    #[command(flatten)]
    tls: TlsArgs,
}

/// Adaptive scrub budget of the controller (ADR-0018).
#[derive(Debug, clap::Args)]
struct AdaptiveScrubArgs {
    /// Adapt the scrub budget to the scrubbed node's load (device
    /// utilisation and queue depth, client GET/PUT p99); without load
    /// signals the fixed `--scrub-rate-mib` applies.
    #[arg(long = "scrub-adaptive")]
    on: bool,
    /// Floor of the adaptive budget, MiB/s (scrubbing never stops).
    #[arg(long = "scrub-min-rate-mib", default_value_t = 1)]
    min_mib: u64,
    /// Ceiling, MiB/s (0 = four times `--scrub-rate-mib`).
    #[arg(long = "scrub-max-rate-mib", default_value_t = 0)]
    max_mib: u64,
    /// Client GET p99 above which the budget is halved, ms.
    #[arg(long = "scrub-target-get-p99-ms", default_value_t = 50)]
    get_p99_ms: u64,
}

fn adaptive_policy(
    base_mib: u64,
    min_mib: u64,
    max_mib: u64,
    get_p99_ms: u64,
) -> arca_lake_controller::adaptive::AdaptivePolicy {
    let base = (base_mib.max(1)) << 20;
    let mut p = arca_lake_controller::adaptive::AdaptivePolicy::around(base);
    p.min = (min_mib.max(1) << 20).min(base);
    if max_mib > 0 {
        p.max = (max_mib << 20).max(base);
    }
    p.get_p99_us = get_p99_ms.max(1) * 1000;
    p
}

/// Dry runs of member moves: node access and the controller's settings.
#[derive(Debug, clap::Args)]
struct PlanArgs {
    /// Failure domain label the controller uses for a one-member group
    /// without a recorded domain; every other group's move keeps its
    /// recorded domain (ADR-0038).
    #[arg(long = "failure-domain", default_value = "host")]
    plan_failure_domain: String,
    /// Estimated rebuild rate for the time estimate, MiB/s.
    #[arg(long, default_value_t = 64)]
    rate_mib: u64,
    #[command(flatten)]
    rpc: NodeRpcArgs,
}

/// Mutual TLS to and from storage nodes (RFC-012 §5.5, ADR-0017).
#[derive(Debug, Clone, clap::Args)]
struct TlsArgs {
    /// Trust bundle: PEM file with one or more CA certificates (two during
    /// a CA rotation).
    #[arg(long, env = "ARCA_TLS_CA")]
    tls_ca: Option<PathBuf>,
    /// This process's certificate chain (PEM, leaf first) carrying a
    /// `spiffe://<trust-domain>/<role>/<name>` URI SAN.
    #[arg(long, env = "ARCA_TLS_CERT")]
    tls_cert: Option<PathBuf>,
    /// This process's private key (PEM). Never logged.
    #[arg(long, env = "ARCA_TLS_KEY")]
    tls_key: Option<PathBuf>,
    /// Certificate revocation lists (PEM), optional.
    #[arg(long, env = "ARCA_TLS_CRL")]
    tls_crl: Option<PathBuf>,
    /// SPIFFE trust domain every peer must belong to.
    #[arg(long, env = "ARCA_TRUST_DOMAIN")]
    trust_domain: Option<String>,
    /// Seconds between checks of the TLS files for changes (rotation).
    #[arg(long, default_value_t = 30)]
    tls_reload_secs: u64,
    /// Run node RPCs in plaintext with no authentication or authorization
    /// (test and lab clusters only). Refused together with TLS flags.
    #[arg(long, env = "ARCA_INSECURE_PLAINTEXT", value_parser = clap::builder::BoolishValueParser::new())]
    insecure_plaintext: bool,
}

impl TlsArgs {
    /// The TLS context for a process of `role`; `None` only when plaintext
    /// was requested explicitly.
    fn context(
        &self,
        role: arca_lake_protocol::tls::Role,
    ) -> Result<Option<Arc<arca_lake_protocol::tls::TlsContext>>, String> {
        let any = self.tls_ca.is_some()
            || self.tls_cert.is_some()
            || self.tls_key.is_some()
            || self.tls_crl.is_some()
            || self.trust_domain.is_some();
        if self.insecure_plaintext {
            if any {
                return Err("--insecure-plaintext cannot be combined with TLS flags".into());
            }
            tracing::warn!(
                role = role.as_str(),
                "node RPCs run in PLAINTEXT with no authentication or authorization (--insecure-plaintext)"
            );
            return Ok(None);
        }
        let (Some(ca), Some(cert), Some(key), Some(td)) = (
            self.tls_ca.clone(),
            self.tls_cert.clone(),
            self.tls_key.clone(),
            self.trust_domain.clone(),
        ) else {
            return Err(if any {
                "TLS needs --tls-ca, --tls-cert, --tls-key and --trust-domain".into()
            } else {
                "no TLS configuration for node RPCs: give --tls-ca, --tls-cert, --tls-key and --trust-domain, or --insecure-plaintext for a test cluster".into()
            });
        };
        let ctx = arca_lake_protocol::tls::TlsContext::load(
            arca_lake_protocol::tls::TlsFiles {
                ca,
                cert,
                key,
                crl: self.tls_crl.clone(),
                trust_domain: td,
            },
            role,
        )
        .map_err(|e| format!("TLS: {e}"))?;
        tracing::info!(
            identity = %ctx.identity(),
            not_after = ctx.not_after(),
            "mutual TLS enabled for node RPCs"
        );
        ctx.spawn_reloader(Duration::from_secs(self.tls_reload_secs.max(1)));
        Ok(Some(ctx))
    }
}

/// TLS of the FoundationDB metadata connection (ADR-0017).
#[derive(Debug, Clone, clap::Args)]
#[allow(clippy::struct_field_names)]
struct FdbTlsArgs {
    /// Trust bundle for the FoundationDB servers (PEM).
    #[arg(long, env = "ARCA_FDB_TLS_CA")]
    fdb_tls_ca: Option<PathBuf>,
    /// Client certificate for FoundationDB (PEM).
    #[arg(long, env = "ARCA_FDB_TLS_CERT")]
    fdb_tls_cert: Option<PathBuf>,
    /// Client private key for FoundationDB (PEM). Never logged.
    #[arg(long, env = "ARCA_FDB_TLS_KEY")]
    fdb_tls_key: Option<PathBuf>,
    /// FoundationDB peer verification string (`TLS_verify_peers`).
    #[arg(long, env = "ARCA_FDB_TLS_VERIFY_PEERS")]
    fdb_tls_verify_peers: Option<String>,
    /// Connect to FoundationDB in plaintext (test and lab clusters only).
    #[arg(long, env = "ARCA_FDB_INSECURE_PLAINTEXT", value_parser = clap::builder::BoolishValueParser::new())]
    fdb_insecure_plaintext: bool,
}

impl FdbTlsArgs {
    /// Decide the metadata transport before any connection.
    fn configure(&self) -> Result<(), String> {
        let any = self.fdb_tls_ca.is_some()
            || self.fdb_tls_cert.is_some()
            || self.fdb_tls_key.is_some()
            || self.fdb_tls_verify_peers.is_some();
        let tls = if self.fdb_insecure_plaintext {
            if any {
                return Err(
                    "--fdb-insecure-plaintext cannot be combined with --fdb-tls-* flags".into(),
                );
            }
            tracing::warn!(
                "metadata (FoundationDB) connection is PLAINTEXT (--fdb-insecure-plaintext)"
            );
            None
        } else {
            let (Some(ca), Some(cert), Some(key)) = (
                self.fdb_tls_ca.clone(),
                self.fdb_tls_cert.clone(),
                self.fdb_tls_key.clone(),
            ) else {
                return Err(if any {
                    "FoundationDB TLS needs --fdb-tls-ca, --fdb-tls-cert and --fdb-tls-key".into()
                } else {
                    "no TLS configuration for FoundationDB: give --fdb-tls-ca, --fdb-tls-cert and --fdb-tls-key, or --fdb-insecure-plaintext for a test cluster".into()
                });
            };
            tracing::info!(ca = %ca.display(), cert = %cert.display(), "metadata connection uses FoundationDB TLS");
            Some((ca, cert, key, self.fdb_tls_verify_peers.clone()))
        };
        configure_fdb_tls(tls)
    }
}

#[cfg(feature = "fdb")]
fn configure_fdb_tls(
    tls: Option<(PathBuf, PathBuf, PathBuf, Option<String>)>,
) -> Result<(), String> {
    arca_lake_metadata::fdb::configure_network_tls(tls.map(|(ca, cert, key, verify_peers)| {
        arca_lake_metadata::fdb::FdbTls {
            ca,
            cert,
            key,
            verify_peers,
        }
    }))
}

#[cfg(not(feature = "fdb"))]
fn configure_fdb_tls(
    _tls: Option<(PathBuf, PathBuf, PathBuf, Option<String>)>,
) -> Result<(), String> {
    Ok(())
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
    /// Credentials file (TOML; RFC-012 §5.2): principals and their access
    /// keys, reloaded when it changes. Takes precedence over
    /// `--access-key`.
    #[arg(long, env = "ARCA_LAKE_CREDENTIALS_FILE")]
    credentials_file: Option<std::path::PathBuf>,
    /// Authorization: `owner-policy` (RFC-012 §5.3: bucket owner and bucket
    /// policy; default with a credentials file and the cluster backend) or
    /// `none` (every authenticated request is allowed; the memory backend
    /// and experiments).
    #[arg(long, value_parser = ["owner-policy", "none"])]
    authorize: Option<String>,
    /// Who may create buckets under `owner-policy`: `authenticated`,
    /// `deny`, or a comma-separated list of principal names.
    #[arg(long, default_value = "authenticated")]
    bucket_creation: String,
    /// Address of a TLS-terminating proxy in front of the gateway whose
    /// `x-forwarded-proto` header is believed for `aws:SecureTransport`
    /// (repeatable). Without it the header is ignored: the gateway serves
    /// plaintext and any client could send it.
    #[arg(long = "trusted-proxy")]
    trusted_proxies: Vec<std::net::IpAddr>,
    /// Disable the per-request audit record (target `arca.audit`).
    #[arg(long)]
    no_audit: bool,
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
    #[command(flatten)]
    fdb_tls: FdbTlsArgs,
    /// Chunks read ahead of the one being streamed per GET (1 = sequential);
    /// bounds buffered bytes per GET to about this many chunks.
    #[arg(long, default_value_t = 4)]
    read_parallelism: usize,
    /// Chunk placements of one PUT in flight at once (memory: that many
    /// chunks plus their shards per PUT).
    #[arg(long = "write-pipeline", default_value_t = 2)]
    write_pipeline: usize,
    /// Minimum size of every multipart part but the last. AWS requires
    /// 5 MiB; smaller values are a non-AWS extension for small labs.
    #[arg(long, default_value_t = 5 << 20)]
    multipart_min_part_bytes: u64,
    /// Record reads for automatic tiering: `on` (the default: a bounded
    /// table flushed in batches, ADR-0034), `off` (no record; tiering then
    /// sees no reads through this gateway) or `legacy` (one write per
    /// generation per second, as earlier releases; for one release).
    #[arg(long, default_value = "on", value_parser = ["on", "off", "legacy"])]
    access_records: String,
    /// Generations pending at most in the access-record table (table and
    /// batch in flight); reads of new generations beyond it are dropped
    /// and counted.
    #[arg(long, default_value_t = 65_536)]
    access_capacity: usize,
    /// Generations per access-record flush transaction at most (larger
    /// batches commit less but their bursts delay GETs: Phase 4.1).
    #[arg(long, default_value_t = 128)]
    access_batch: usize,
    /// Time between access-record flushes, ms (±20 % at random).
    #[arg(long, default_value_t = 1000)]
    access_flush_interval_ms: u64,
    /// Fraction of the capacity at which a flush starts at once.
    #[arg(long, default_value_t = 0.5)]
    access_high_watermark: f64,
    /// Reads older than this when their flush is attempted are dropped
    /// (counted stale), ms.
    #[arg(long, default_value_t = 30_000)]
    access_max_age_ms: u64,
    /// Time a graceful stop spends flushing pending reads, ms.
    #[arg(long, default_value_t = 2000)]
    access_shutdown_flush_ms: u64,
    /// Clock skew allowed between this gateway and the controller leader
    /// when its lease confirms that tiering is disabled (ADR-0035; reads
    /// are then not recorded), ms. Keep it below a third of the leader
    /// lease.
    #[arg(long, default_value_t = 2000)]
    tiering_lease_skew_ms: u64,
    /// Serve gateway cache counters as Prometheus text on this address
    /// (`GET /metrics`); cluster backend only.
    #[arg(long)]
    metrics_listen: Option<SocketAddr>,
}

#[derive(Debug, clap::Args)]
#[command(subcommand_negates_reqs = true)]
struct StorageNodeArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:9100")]
    listen: SocketAddr,
    /// Device directory.
    #[arg(long, required = true)]
    dir: Option<PathBuf>,
    /// Serve node timing counters in the Prometheus text format
    /// (`GET /metrics`).
    #[arg(long)]
    metrics_listen: Option<SocketAddr>,
    /// Roll to a new segment at this size (bytes); the default is the
    /// storage engine's 1 GiB. Small values are for tests.
    #[arg(long)]
    segment_max_bytes: Option<u64>,
    /// Seconds between background compaction passes (RFC-005 §5.6);
    /// 0 disables compaction.
    #[arg(long, default_value_t = 60)]
    compaction_interval_secs: u64,
    /// Compact a sealed segment once this fraction of it is dead.
    #[arg(long, default_value_t = 0.5)]
    compaction_min_dead_fraction: f64,
    /// Segments compacted per pass at most.
    #[arg(long, default_value_t = 64)]
    compaction_max_segments: usize,
    /// Live bytes copied by compaction per pass at most, MiB.
    #[arg(long, default_value_t = 512)]
    compaction_max_copy_mib: u64,
    /// Capacity this node advertises for placement of replacements (bytes;
    /// default: the file system's). Free space is reported as the smaller
    /// of the file system's and this minus the segment bytes.
    #[arg(long)]
    capacity_bytes: Option<u64>,
    /// Bytes kept free for compaction output, segment rollover, quarantine
    /// and repair: client, migration and cache writes are refused when a
    /// write would leave less (default: two segments and 64 MiB, or 2 % of
    /// the device, at most a quarter of it).
    #[arg(long)]
    reserve_bytes: Option<u64>,
    /// Rejoin lease of the delete fence, s (Phase 3.10): an open fence
    /// admits deletes only this long after the leader's last push of it
    /// (a start, a partition or a silent leader closes it until then).
    /// Keep it well above the controllers' `--health-interval-secs`.
    #[arg(long = "fence-lease-secs", default_value_t = 30)]
    fence_lease_secs: u64,
    /// Deferred reclamation, s (Phase 3.12, ADR-0028): a delete of a
    /// durable shard retires its record, which stays readable and is
    /// removed only by a reclaim whose metadata observation is at least this
    /// long after the retirement. Restores of backups older than this
    /// (minus the clock skew allowance, twice) are outside the protection;
    /// freed space arrives this much later. 0 removes records at once (no
    /// protection on this node).
    #[arg(long = "reclaim-retention-secs", default_value_t = 86_400)]
    reclaim_retention_secs: u64,
    /// Clock skew allowed between a reclaim's observation and this node's
    /// clock, s (ADR-0028, assumption H2).
    #[arg(long = "reclaim-skew-secs", default_value_t = 60)]
    reclaim_skew_secs: u64,
    #[command(flatten)]
    tls: TlsArgs,
    #[command(subcommand)]
    command: Option<StorageNodeCommand>,
}

#[derive(Debug, Subcommand)]
enum StorageNodeCommand {
    /// Whether a build that reads segment formats up to `--format` can
    /// open this device (RFC-014 §5.3 downgrade check; the node must not
    /// be running on the device). Exit code 0 = yes, 2 = no.
    CheckDowngrade {
        /// Device directory.
        #[arg(long)]
        dir: PathBuf,
        /// Newest segment format the target build reads.
        #[arg(long)]
        format: u16,
    },
    /// Offline inspection of a device's quarantine state (RFC-008 §8.2):
    /// prints `quarantine.log`, and with `--scan` opens the store and
    /// prints the open report (the node must not be running on the device).
    QuarantineList {
        /// Device directory.
        #[arg(long)]
        dir: PathBuf,
        /// Open the store and print its recovery report and live quarantine
        /// entries.
        #[arg(long)]
        scan: bool,
    },
    /// Offline space breakdown of a device directory (Phase 3.4): per
    /// segment and in total, live and dead data, tombstones still needed or
    /// droppable (and those compaction keeps today), damaged records and
    /// slack, one JSON object per line. Reads the segment files only; run
    /// it on a directory no node is serving.
    SegmentReport {
        /// Device directory.
        #[arg(long)]
        dir: PathBuf,
        /// Print only the total.
        #[arg(long)]
        total_only: bool,
    },
    /// Test-only: flip bytes of a stored shard on a stopped node so that
    /// scrubbing and repair can be exercised (RFC-008 §8.9).
    #[command(hide = true)]
    CorruptShard {
        /// Device directory.
        #[arg(long)]
        dir: PathBuf,
        /// Group id.
        #[arg(long, required_unless_present = "pick")]
        group: Option<u64>,
        /// Chunk id, 32 hex digits.
        #[arg(long, required_unless_present = "pick")]
        chunk: Option<String>,
        /// Shard index.
        #[arg(long, required_unless_present = "pick")]
        index: Option<u16>,
        /// Instead of naming a shard, corrupt the live shard at this
        /// position (modulo their number) in key order; the chosen shard
        /// is printed (soak tests).
        #[arg(long, conflicts_with_all = ["group", "chunk", "index"])]
        pick: Option<u64>,
        /// Corrupt the record header instead of the payload.
        #[arg(long)]
        header: bool,
    },
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
    /// Write admission: seconds a better state must hold before the tier
    /// moves to it (one step at a time).
    #[arg(long, default_value_t = 60)]
    admission_dwell_secs: u64,
    /// Write admission: throttle when the tier would be full within this
    /// many seconds at the recent rate (warning at four times it). The
    /// default suits deployments; small test clusters written fast reach
    /// it with most of their space free (Phase 4.0).
    #[arg(long, default_value_t = 3600)]
    admission_horizon_secs: u64,
    /// Write admission: largest chunk the gateways write (bytes); a group
    /// whose fullest member has room for fewer than
    /// `--admission-group-min-shards` of its shards leaves placement.
    #[arg(long, default_value_t = 8 << 20)]
    admission_max_chunk_bytes: u64,
    /// See `--admission-max-chunk-bytes`.
    #[arg(long, default_value_t = 64)]
    admission_group_min_shards: u64,
    /// Keep placing on storage nodes that share a file system without
    /// `--capacity-bytes` allotments that fit it (development and
    /// single-disk test clusters only: their reserves are then not
    /// guaranteed). Without it their groups leave placement.
    #[arg(long)]
    allow_shared_filesystem: bool,
    /// Reconciliation interval in seconds.
    #[arg(long, default_value_t = 300)]
    reconcile_interval_secs: u64,
    /// Reclaim pass interval in seconds (Phase 3.12, ADR-0028): retired
    /// records the metadata no longer needs, once the nodes' retention old.
    #[arg(long, default_value_t = 60)]
    reclaim_interval_secs: u64,
    /// Repair scheduler interval in seconds.
    #[arg(long, default_value_t = 15)]
    repair_interval_secs: u64,
    /// Health probe interval in seconds.
    #[arg(long, default_value_t = 5)]
    health_interval_secs: u64,
    /// Failure domain label for replacement choice in a one-member group
    /// without a recorded domain. Every other group's moves keep its
    /// recorded domain (ADR-0038); a different value here is reported.
    #[arg(long, default_value = "host")]
    failure_domain: String,
    /// How a member move obtains shards while the leaving member still
    /// answers: `reconstruct` (EC from k members, validated against the
    /// chunk checksum; the leaving member's shard only when fewer than k
    /// members have one) or `direct` (copy the leaving member's shard,
    /// checked by block checksums and the target's CRC).
    #[arg(long, default_value = "reconstruct", value_parser = ["reconstruct", "direct"])]
    repair_source: String,
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
    /// Move chunks from the fullest group of a tier to the roomiest one of
    /// the same shape (Phase 3.8 capacity rebalancing; `admin rebalance`
    /// pauses, resumes and switches dry run).
    #[arg(long)]
    rebalance: bool,
    /// Rebalancing pass interval in seconds.
    #[arg(long, default_value_t = 60)]
    rebalance_interval_secs: u64,
    /// Logical MiB moved per rebalancing pass at most (the rate limit).
    #[arg(long, default_value_t = 256)]
    rebalance_mib_per_pass: u64,
    /// Start rebalancing a tier when its least roomy group has this
    /// fraction less room than its roomiest one.
    #[arg(long, default_value_t = 0.25)]
    rebalance_start_imbalance: f64,
    /// Stop once the imbalance is below this fraction.
    #[arg(long, default_value_t = 0.10)]
    rebalance_stop_imbalance: f64,
    /// After a plan moved what it needed, wait this long before planning
    /// its tier again, s (default: the minimum orphan age plus two
    /// reconcile intervals, so that the sources' old copies are gone).
    #[arg(long)]
    rebalance_settle_secs: Option<u64>,
    /// No rebalancing while the GC backlog is above this, MiB (Phase 3.10:
    /// GC goes first).
    #[arg(long, default_value_t = 4096)]
    rebalance_gc_backlog_pause_mib: u64,
    /// Chunks a cache-group repair may reconstruct per pass (0 = unlimited).
    #[arg(long, default_value_t = 256)]
    cache_repair_chunks_per_pass: usize,
    /// Chunks an operator's member move (drain, replacement off a healthy
    /// node) rebuilds per pass before failed members' repairs go first
    /// (0 = unlimited).
    #[arg(long, default_value_t = 512)]
    planned_move_chunks_per_pass: usize,
    /// Chunks the repair of a failed member rebuilds per group per pass, so
    /// that every group in repair progresses each pass (0 = unlimited).
    #[arg(long, default_value_t = 4096)]
    failure_repair_chunks_per_pass: usize,
    /// (group, node) pairs a reconciliation pass lists at most; the sweep
    /// resumes at its persisted cursor (0 = every pair, the default).
    /// Rebalancing's settle time covers the measured sweep.
    #[arg(long, default_value_t = 0)]
    reconcile_pairs_per_pass: usize,
    /// (group, node) pairs reconciled at once (1–8).
    #[arg(long, default_value_t = 4)]
    reconcile_parallel_pairs: usize,
    /// Nodes a reclaim pass works on at once (at least 1).
    #[arg(long, default_value_t = 4)]
    reclaim_parallel_nodes: usize,
    /// Generations a tiering pass examines at most; the sweep resumes at
    /// its persisted cursor (0 = every generation).
    #[arg(long, default_value_t = 4096)]
    tiering_generations_per_pass: usize,
    /// Clock skew allowed between controllers and gateways in the grace
    /// after tiering is enabled (ADR-0035 §10.4), seconds.
    #[arg(long, default_value_t = 60)]
    tiering_clock_skew_secs: u64,
    /// Scrub a group when its last pass finished this many seconds ago
    /// (RFC-008 §8.3); 0 disables background scrubbing.
    #[arg(long, default_value_t = 86_400)]
    scrub_interval_secs: u64,
    /// Seconds between scrub scheduler ticks (each resumes the due groups).
    #[arg(long, default_value_t = 60)]
    scrub_tick_secs: u64,
    /// Shards per `ScrubShards` request (upper bound).
    #[arg(long, default_value_t = 256)]
    scrub_page: u32,
    /// Payload MiB per `ScrubShards` request: the burst a scrub puts on a
    /// device between two pacing sleeps (0 = unbounded).
    #[arg(long, default_value_t = 8)]
    scrub_page_mib: u64,
    /// Scrub read budget in MiB per second across groups (0 = unpaced);
    /// with `--scrub-adaptive`, the starting and fallback rate.
    #[arg(long, default_value_t = 32)]
    scrub_rate_mib: u64,
    #[command(flatten)]
    scrub_adaptive: AdaptiveScrubArgs,
    /// Groups scrubbed per tick.
    #[arg(long, default_value_t = 1)]
    scrub_concurrency: usize,
    /// Seconds between stripe repair worker passes (RFC-008 §8.5).
    #[arg(long, default_value_t = 15)]
    stripe_repair_interval_secs: u64,
    /// Stripe repair tasks examined per pass.
    #[arg(long, default_value_t = 64)]
    stripe_repair_tasks_per_pass: usize,
    /// Attempts before a stripe repair task is marked failed.
    #[arg(long, default_value_t = 8)]
    stripe_repair_max_attempts: u32,
    /// Stripes repaired concurrently.
    #[arg(long, default_value_t = 4)]
    stripe_repair_workers: usize,
    /// Repair byte budget in MiB per second over reads and writes (0 =
    /// unpaced); separate from `--scrub-rate-mib`.
    #[arg(long, default_value_t = 64)]
    stripe_repair_rate_mib: u64,
    /// Repair transfers in flight per node (1 on nodes busy with clients).
    #[arg(long, default_value_t = 2)]
    stripe_repair_per_node: usize,
    #[command(flatten)]
    rpc: NodeRpcArgs,
    #[command(flatten)]
    fdb_tls: FdbTlsArgs,
}

#[derive(Debug, clap::Args)]
struct AdminArgs {
    /// FoundationDB cluster file.
    #[arg(long, env = "ARCA_FDB_CLUSTER_FILE")]
    cluster_file: PathBuf,
    #[command(flatten)]
    fdb_tls: FdbTlsArgs,
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
    /// Check the capacity configuration from the leader's admission
    /// record (Phase 3.7): nodes sharing a file system without allotments
    /// that fit it (R-80), nodes whose sharing cannot be checked, groups
    /// out of placement and each group's room (R-82), the record's age.
    /// Exits 1 when a conflict is found or the record is stale or missing.
    CheckConfig,
    /// The delete fence (ADR-0023): every node's state; close it on every
    /// node (before a metadata restore or maintenance); release the
    /// operator hold so that the leader opens a new generation.
    DeleteFence {
        #[command(subcommand)]
        command: DeleteFenceCommand,
    },
    /// Capacity rebalancing (Phase 3.8): state, pause, resume, dry run.
    Rebalance {
        #[command(subcommand)]
        command: RebalanceCommand,
    },
    /// Automatic tiering (ADR-0035): the cluster's tiering record, the
    /// source of truth for whether tiering acts and its policy. Honoured
    /// by controllers that read it (from the release with metadata schema
    /// 4); once they do, it overrides their `--tiering-*` flags.
    Tiering {
        #[command(subcommand)]
        command: TieringCommand,
    },
    /// Print nodes, groups, and the leader record.
    Status {
        /// Also print every process heartbeat and node capability record
        /// (software version and P/M/F ranges, RFC-014 §5.8).
        #[arg(long)]
        versions: bool,
    },
    /// Initialise an empty metadata store at this build's versions (RFC-014
    /// §5.4), or stamp a store that holds data but no schema key with the
    /// legacy versions (ADR-0016). Idempotent.
    Init,
    /// Rolling upgrade finalization (RFC-014 §5.5).
    Upgrade {
        #[command(subcommand)]
        command: UpgradeCommand,
    },
    /// Cluster recovery after a metadata restore (RFC-015 §5.3).
    Recovery {
        #[command(subcommand)]
        command: RecoveryCommand,
    },
    /// Replace one member of a group (RFC-013 device replacement): the
    /// controller leader picks a replacement (or uses `--node`), fences the
    /// group's epoch, rebuilds the member's shards and verifies them; the
    /// request survives a controller crash. Only while the cluster is
    /// Active. Requires `--yes`; `--cancel` withdraws a request that has
    /// not started rebuilding.
    ReplaceMember {
        /// Group id.
        #[arg(long)]
        group: u64,
        /// Member index.
        #[arg(long)]
        index: u16,
        /// Replacement node id (default: the controller chooses).
        #[arg(long)]
        node: Option<u64>,
        /// Confirm.
        #[arg(long)]
        yes: bool,
        /// Withdraw the request.
        #[arg(long)]
        cancel: bool,
        /// Show what would happen (target, capacity, bytes to move, what
        /// would block it) without writing metadata or touching a shard.
        #[arg(long, conflicts_with_all = ["yes", "cancel"])]
        dry_run: bool,
        #[command(flatten)]
        plan: PlanArgs,
    },
    /// Drain a node (RFC-013 decommission): every group member on it is
    /// replaced one at a time, durable groups first; the node is reported
    /// decommissioned only when no group lists it and no group is still
    /// rebuilding. Only while the cluster is Active. Requires `--yes`;
    /// `--cancel` stops the drain (members already moved stay moved).
    /// Managed metadata restore (Phase 3.11, ADR-0026 option D): plan,
    /// run, finish, status. The supported restore path: it refuses to
    /// start while any node's safety cannot be confirmed. A restore done
    /// any other way is outside its guarantee (R-94(b)).
    Restore {
        #[command(subcommand)]
        command: RestoreCommand,
    },
    /// Node lifecycle (Phase 3.11, R-100): list, status, decommission
    /// (alias drain), unregister, rejoin.
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    DecommissionNode {
        /// Node id.
        #[arg(long)]
        node: u64,
        /// Confirm.
        #[arg(long)]
        yes: bool,
        /// Stop draining.
        #[arg(long)]
        cancel: bool,
        /// Show every move the drain would make without writing metadata
        /// or touching a shard.
        #[arg(long, conflicts_with_all = ["yes", "cancel"])]
        dry_run: bool,
        #[command(flatten)]
        plan: PlanArgs,
    },
    /// Metadata backup verification (RFC-015 B-A5): checks of a backup
    /// restored into an isolated cluster (`--cluster-file` names that
    /// cluster). A metadata backup holds no shard data.
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },
    /// Cluster read-only switch (RFC-015 §5.3).
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
    /// Data integrity: scrub progress and stripe repair tasks (RFC-008 §8.9).
    Integrity {
        #[command(subcommand)]
        command: IntegrityCommand,
    },
    /// Stripe groups' failure domains (ADR-0038): check, set.
    Group {
        #[command(subcommand)]
        command: GroupCommand,
    },
}

#[derive(Debug, Subcommand)]
enum GroupCommand {
    /// Check every group's members against its recorded failure domain:
    /// groups with two members in one domain, groups without a recorded
    /// domain, members whose node lacks the label. Reads only (never moves
    /// a member or deletes data). Exits 1 when anything is found.
    Check,
    /// Record the failure domain (a topology label) of a group created by a
    /// release that did not record it (0.1.0): until then no member of the
    /// group is moved. Never inferred from the nodes: name the label the
    /// group was created with. Moves nothing. Lists the groups first;
    /// refuses (writing nothing) a label that no member's node of one of
    /// them carries, unless `--force`. Exits 0 when recorded, 2 when
    /// recorded with warnings (members that break the label or lack it,
    /// also shown by `admin group check`), 1 when refused.
    SetFailureDomain {
        /// Group id.
        #[arg(long, required_unless_present = "all_unrecorded")]
        group: Option<u64>,
        /// Every group of several members without a recorded domain.
        #[arg(long, conflicts_with = "group")]
        all_unrecorded: bool,
        /// Topology label whose values must differ between the members.
        #[arg(long)]
        label: String,
        /// Replace a different label already recorded.
        #[arg(long)]
        change: bool,
        /// Record a label that no member's node carries (refused without
        /// it: most likely a typo).
        #[arg(long)]
        force: bool,
        /// List the groups and what would be recorded; write nothing.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Subcommand)]
enum UpgradeCommand {
    /// Gate values, cursor, heartbeats and node capabilities.
    Status,
    /// Request a finalize to this binary's versions; the controller leader
    /// executes the steps (each a fenced compare-and-set). With `--wait`
    /// the command polls until the cursor is gone or the timeout passes.
    Finalize {
        /// Process ids (controller ids, gateway ids, node ids) whose absence
        /// does not block the precondition.
        #[arg(long = "ignore-absent")]
        ignore_absent: Vec<String>,
        /// Poll until done.
        #[arg(long)]
        wait: bool,
        /// Seconds to wait with `--wait`.
        #[arg(long, default_value_t = 600)]
        timeout_secs: u64,
    },
    /// Cancel a finalize that has not passed the downgrade cut-off.
    Cancel,
    /// Whether a build reading metadata schema up to `--schema` may run on
    /// this store (RFC-014 §5.6).
    CheckDowngrade {
        /// Newest schema the target build reads.
        #[arg(long)]
        schema: u64,
    },
}

#[derive(Debug, Subcommand)]
enum RecoveryCommand {
    /// Cluster state, incarnation, cursor and the latest report.
    Status,
    /// Mark restored metadata (`restore mark`, RFC-015 §5.3.1): writes a
    /// fresh incarnation and `Restoring`. Run before any other arca-lake
    /// process is started on the restored store. Destructive for every
    /// process still running with the old incarnation (they are fenced
    /// out); requires `--yes`.
    Begin {
        /// Label of the backup the metadata was restored from.
        #[arg(long)]
        from: Option<String>,
        /// Consistent point of that backup, Unix milliseconds (from
        /// `fdbbackup describe`): separates shards written after it
        /// (post-restore-point orphans) from older garbage in the report.
        /// Without it every orphan is reported as older.
        #[arg(long)]
        backup_point_ms: Option<u64>,
        /// Confirm.
        #[arg(long)]
        yes: bool,
        /// Allow marking a cluster that is not Active or ReadOnly.
        #[arg(long)]
        force: bool,
        /// Storage node RPCs: `begin` closes the delete fence on every
        /// registered node first and refuses unless each acknowledges
        /// (ADR-0023).
        #[command(flatten)]
        rpc: NodeRpcArgs,
        /// Node ids whose process the operator declares stopped (they
        /// cannot acknowledge; recorded in the reason). Never a running
        /// node: its deletes would go on.
        #[arg(long = "absent-node")]
        absent_node: Vec<u64>,
    },
    /// Re-run the verification pass from the start (`ReadOnly` or
    /// `RecoveryRequired` → `VerificationPending`).
    Verify {
        /// Confirm (the cluster leaves ReadOnly while verifying).
        #[arg(long)]
        yes: bool,
    },
    /// Continue an interrupted verification (`RecoveryRequired` →
    /// `VerificationPending`, cursor kept).
    Resume,
    /// Accept the report and activate (`restore accept` + `cluster
    /// activate`) in one transaction. Refused, even with `--force`, while
    /// verification has not finished or a node's listing is missing.
    /// Unresolved critical findings (unreadable chunks, broken multipart
    /// manifests, placement epochs ahead of their group) need `--force`
    /// and `--reason`; they stay in the report, every listed object gets a
    /// data-loss record, and every attempt is audited.
    Finalize {
        /// Confirm.
        #[arg(long)]
        yes: bool,
        /// Accept unresolved critical findings (data loss).
        #[arg(long)]
        force: bool,
        /// Who finalizes (audit).
        #[arg(long, env = "ARCA_OPERATOR")]
        operator: String,
        /// Why (audit; required with --force).
        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Accepted data losses and the finalize audit trail.
    Losses,
}

#[derive(Debug, Subcommand)]
enum BackupCommand {
    /// Verify restored metadata (read-only): schema, cluster state and
    /// epochs, every committed generation against its chunk records and
    /// groups, multipart roots against their parts, one latest version
    /// per key and every version resolving. Exit 2 on findings.
    VerifyRestored {
        /// Write Prometheus text-file metrics of this run here
        /// (atomically; for a text-file collector).
        #[arg(long)]
        metrics_file: Option<PathBuf>,
        /// End of the verified backup (restorable point), Unix ms: exported
        /// as the backup's age.
        #[arg(long)]
        backup_end_ms: Option<u64>,
        /// Seconds the restore took (measured by the job), exported.
        #[arg(long)]
        restore_seconds: Option<f64>,
    },
    /// Print the arca-lake key range as `fdbbackup -k` expects it.
    KeyRange,
}

#[derive(Debug, Subcommand)]
enum ClusterCommand {
    /// `Active` → `ReadOnly`: client writes and deletes stop, repair
    /// continues; the delete fence is closed on every node it reaches
    /// (the leader closes it on the others at its next pass).
    SetReadOnly {
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// `ReadOnly` → `Active`.
    Activate,
}

#[derive(Debug, Subcommand)]
enum IntegrityCommand {
    /// Scrub cursors per group, repair queue summary, and every task that
    /// is unrecoverable or failed.
    Status {
        /// Resolve every unrecoverable or failed task to its bucket, key,
        /// version, part and chunk (R-54; one lookup per task, bounded by
        /// the versions of the affected key).
        #[arg(long)]
        objects: bool,
    },
    /// Verify every shard of one object version on its members with the
    /// nodes' deep verification (a corrupt shard is quarantined, as a
    /// scrub would). Exit code 0 healthy, 1 degraded or incomplete, 2
    /// unreadable.
    VerifyObject {
        /// Bucket.
        #[arg(long)]
        bucket: String,
        /// Key.
        #[arg(long)]
        key: String,
        /// Version id (default: the current version).
        #[arg(long)]
        version_id: Option<String>,
        /// Also decode every chunk from shards that verified good, check
        /// each against its record, and recompute the object's checksums
        /// and ETag (reads the whole object; read-only).
        #[arg(long)]
        decode: bool,
        /// Let the nodes quarantine the corrupt shards they find (as a
        /// scrub would). Without it the check changes nothing anywhere.
        #[arg(long)]
        quarantine: bool,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// Start a scrub pass of a group now: resets its cursor so the leader's
    /// next tick picks it up, or runs the pass from this process with
    /// `--wait` (the group's nodes must be reachable from here).
    Scrub {
        /// Group id.
        #[arg(long)]
        group: u64,
        /// Run the pass to completion from this process and print the
        /// report.
        #[arg(long)]
        wait: bool,
        /// Shards per request when `--wait`.
        #[arg(long, default_value_t = 256)]
        page: u32,
        /// Payload MiB per request when `--wait` (0 = unbounded).
        #[arg(long, default_value_t = 8)]
        page_mib: u64,
        /// Read budget in MiB per second when `--wait` (0 = unpaced); with
        /// `--adaptive`, the starting and fallback rate.
        #[arg(long, default_value_t = 0)]
        rate_mib: u64,
        /// Adapt the budget to the nodes' load (ADR-0018).
        #[arg(long)]
        adaptive: bool,
        /// Floor of the adaptive budget, MiB/s.
        #[arg(long, default_value_t = 1)]
        adaptive_min_mib: u64,
        /// Ceiling of the adaptive budget, MiB/s (0 = four times the rate).
        #[arg(long, default_value_t = 0)]
        adaptive_max_mib: u64,
        /// Client GET p99 target of the adaptive budget, ms.
        #[arg(long, default_value_t = 50)]
        adaptive_get_p99_ms: u64,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// Enqueue a stripe repair task for one chunk.
    Enqueue {
        /// Group id.
        #[arg(long)]
        group: u64,
        /// Chunk id, 32 hex digits.
        #[arg(long)]
        chunk: String,
    },
    /// List every stripe repair task.
    List,
    /// Verify every committed multipart root against its part generations
    /// (RFC-008 §8.6) and print the findings.
    VerifyManifests,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    init_logging(cli.log_json);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let code = rt.block_on(async {
        match cli.command {
            Command::Gateway(args) => run_gateway(args).await,
            Command::StorageNode(args) => run_storage_node(args).await,
            Command::Tls { command } => run_tls(command),
            Command::Controller(args) => run_controller(args).await,
            Command::Admin(args) => Box::pin(run_admin(args)).await,
        }
    });
    // Every task has stopped and every worker thread is joined.
    drop(rt);
    exit_past_the_metadata_client(code)
}

/// End the process without running exit handlers once the FoundationDB
/// client network has been started (R-68): its thread is never stopped
/// (stopping it with handles alive crashed every test binary), and
/// `exit()` then ran the client library's static destructors while that
/// thread was still running, which crashed a cleanly stopped gateway or
/// controller now and then (SIGSEGV after "shutting down"). All work is
/// done and the runtime is gone at this point; only the standard streams
/// need flushing.
#[cfg(feature = "fdb")]
#[allow(unsafe_code)]
fn exit_past_the_metadata_client(code: ExitCode) -> ExitCode {
    use std::io::Write as _;
    if !arca_lake_metadata::fdb::network_started() {
        return code;
    }
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    let status = (0..=u8::MAX)
        .find(|v| ExitCode::from(*v) == code)
        .map_or(1, i32::from);
    // SAFETY: `_exit` takes a status and never returns; it skips atexit
    // handlers and static destructors on purpose (see above).
    unsafe { libc::_exit(status) }
}

#[cfg(not(feature = "fdb"))]
fn exit_past_the_metadata_client(code: ExitCode) -> ExitCode {
    code
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
fn open_meta(
    path: &std::path::Path,
    tls: &FdbTlsArgs,
) -> Result<Arc<arca_lake_metadata::fdb::FdbKv>, String> {
    tls.configure()?;
    let s = path
        .to_str()
        .ok_or_else(|| "cluster file path is not UTF-8".to_string())?;
    arca_lake_metadata::fdb::FdbKv::open(Some(s))
        .map(Arc::new)
        .map_err(|e| format!("cannot open FoundationDB ({s}): {e}"))
}

#[cfg(not(feature = "fdb"))]
fn open_meta(
    _path: &std::path::Path,
    tls: &FdbTlsArgs,
) -> Result<Arc<arca_lake_metadata::mem::MemKv>, String> {
    tls.configure()?;
    Err("this binary was built without the `fdb` feature; FoundationDB is required for multi-process deployments".into())
}

fn write_secret(path: &std::path::Path, text: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        o.mode(0o600);
    }
    o.open(path)?.write_all(text.as_bytes())
}

fn run_tls(command: TlsCommand) -> ExitCode {
    use arca_lake_protocol::pki::Ca;
    use arca_lake_protocol::tls::Role;
    let day = |d: u64| Duration::from_secs(d.max(1) * 86_400);
    let load_ca = |dir: &std::path::Path| -> Result<Ca, String> {
        let cert =
            std::fs::read_to_string(dir.join("ca.pem")).map_err(|e| format!("ca.pem: {e}"))?;
        let key = std::fs::read_to_string(dir.join("ca-key.pem"))
            .map_err(|e| format!("ca-key.pem: {e}"))?;
        Ca::from_pem(&cert, &key)
    };
    let r: Result<String, String> = (|| match command {
        TlsCommand::InitCa { dir, name, days } => {
            std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
            let ca = Ca::new(&name, day(days))?;
            std::fs::write(dir.join("ca.pem"), &ca.cert_pem).map_err(|e| e.to_string())?;
            write_secret(&dir.join("ca-key.pem"), &ca.key_pem()).map_err(|e| e.to_string())?;
            Ok(format!("CA written to {}", dir.display()))
        }
        TlsCommand::Issue {
            ca_dir,
            trust_domain,
            role,
            name,
            out,
            days,
            serial,
            backdate_days,
        } => {
            let ca = load_ca(&ca_dir)?;
            let serial = serial.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(1, |d| u64::try_from(d.as_micros()).unwrap_or(1))
            });
            // arca-lake roles, or `fdb` for FoundationDB server
            // certificates (`spiffe://<td>/fdb/<name>`).
            let path = match Role::parse(&role) {
                Some(r) => format!("{}/{name}", r.as_str()),
                None if role == "fdb" => format!("fdb/{name}"),
                None => return Err("role must be node, gateway, controller, admin or fdb".into()),
            };
            let not_before = std::time::SystemTime::now()
                - Duration::from_secs(300)
                - Duration::from_secs(backdate_days * 86_400);
            let issued = ca.issue_path_at(&trust_domain, &path, serial, not_before, day(days))?;
            std::fs::create_dir_all(&out).map_err(|e| e.to_string())?;
            std::fs::write(out.join("cert.pem"), &issued.cert_pem).map_err(|e| e.to_string())?;
            write_secret(&out.join("key.pem"), &issued.key_pem).map_err(|e| e.to_string())?;
            std::fs::write(out.join("ca.pem"), &ca.cert_pem).map_err(|e| e.to_string())?;
            Ok(format!(
                "issued spiffe://{trust_domain}/{path} serial {serial} into {}",
                out.display()
            ))
        }
        TlsCommand::Crl {
            ca_dir,
            revoke,
            number,
            out,
        } => {
            let ca = load_ca(&ca_dir)?;
            std::fs::write(&out, ca.crl(&revoke, number)?).map_err(|e| e.to_string())?;
            Ok(format!(
                "CRL {number} revoking {revoke:?} written to {}",
                out.display()
            ))
        }
        TlsCommand::Show { cert } => {
            let pem = std::fs::read(&cert).map_err(|e| e.to_string())?;
            let (ids, not_after) = arca_lake_protocol::tls::pem_certificate_identities(&pem)
                .map_err(|e| e.to_string())?;
            Ok(format!(
                "identities {:?} not_after {not_after}",
                ids.iter().map(ToString::to_string).collect::<Vec<_>>()
            ))
        }
    })();
    match r {
        Ok(msg) => {
            println!("{msg}");
            ExitCode::SUCCESS
        }
        Err(e) => fail(e),
    }
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

#[allow(clippy::too_many_lines)]
async fn run_gateway(args: GatewayArgs) -> ExitCode {
    use arca_lake_gateway::auth::{Authorizer, BucketCreation, CredentialStore};
    let store = match &args.credentials_file {
        Some(path) => match CredentialStore::load(path) {
            Ok(s) => {
                tracing::info!(keys = s.len(), "credentials loaded");
                let s = std::sync::Arc::new(s);
                let _reloader = s.spawn_reloader(Duration::from_secs(30));
                Some(s)
            }
            Err(e) => return fail(format!("credentials file: {e}")),
        },
        None => None,
    };
    let credentials = match (args.access_key, args.secret_key) {
        _ if store.is_some() => None,
        (Some(ak), Some(sk)) => Some((ak, sk)),
        _ if args.allow_anonymous => {
            eprintln!("WARNING: authentication disabled; every request is accepted");
            None
        }
        _ => {
            return fail(
                "provide --credentials-file, --access-key/--secret-key or --allow-anonymous",
            );
        }
    };
    let authorize = match args.authorize.as_deref() {
        Some("owner-policy") => true,
        Some(_) => false,
        None => store.is_some() && args.backend == "cluster",
    };
    if authorize && args.backend != "cluster" {
        return fail("--authorize owner-policy needs --backend cluster");
    }
    let (store, credentials) = match gateway_auth(authorize, store, credentials) {
        Ok(v) => v,
        Err(e) => return fail(e),
    };
    let trusted_proxies = args.trusted_proxies.clone();
    let bucket_creation = match args.bucket_creation.as_str() {
        "authenticated" => BucketCreation::Authenticated,
        "deny" => BucketCreation::Deny,
        list => BucketCreation::Principals(list.split(',').map(|s| s.trim().to_string()).collect()),
    };
    if !authorize {
        tracing::warn!("authorization disabled: every authenticated request is allowed");
    }
    let cache = match args.cache.config() {
        Ok(c) => c,
        Err(e) => return fail(e),
    };
    let s3_stats = std::sync::Arc::new(arca_lake_gateway::requests::S3Stats::default());
    let cfg = arca_lake_gateway::GatewayConfig {
        listen: args.listen,
        credentials,
        store: store.clone(),
        audit: !args.no_audit,
        stats: Some(std::sync::Arc::clone(&s3_stats)),
    };
    let mut queue_shutdown: Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    )> = None;
    let server = if args.backend == "cluster" {
        let Some(path) = args.cluster_file.as_deref() else {
            return fail("--backend cluster needs --cluster-file");
        };
        let meta = match open_meta(path, &args.fdb_tls) {
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
                tls: match args.rpc.tls.context(arca_lake_protocol::tls::Role::Gateway) {
                    Ok(t) => t,
                    Err(e) => return fail(e),
                },
                cache,
                read_parallelism: args.read_parallelism,
                population_queue_depth: args.cache.cache_population_queue_depth,
                populations_per_node: args.cache.cache_populations_per_node,
                write_pipeline: args.write_pipeline,
                multipart_min_part_bytes: args.multipart_min_part_bytes,
                process_id: format!("gateway@{}", args.listen),
                heartbeat: Some(Duration::from_secs(30)),
                admission_without_controller: false,
                record_access: args.access_records != "off",
                access: arca_lake_gateway::cluster::access::AccessConfig {
                    legacy: args.access_records == "legacy",
                    capacity: args.access_capacity.max(1),
                    batch: args.access_batch.max(1),
                    flush_interval: Duration::from_millis(args.access_flush_interval_ms.max(1)),
                    high_watermark: args.access_high_watermark.clamp(0.01, 1.0),
                    max_age: Duration::from_millis(access_max_age_ms(args.access_max_age_ms)),
                    shutdown_flush: Duration::from_millis(args.access_shutdown_flush_ms),
                    lease_skew_ms: args.tiering_lease_skew_ms,
                    ..Default::default()
                },
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
                render_integrity_stats(&mut out, &stats_backend.integrity_stats());
                render_admission_stats(&mut out, &stats_backend.cluster().admission.snapshot());
                render_access_stats(&mut out, &stats_backend.access_stats());
                s3_stats.render(&mut out);
                out += &arca_lake_protocol::tls::render_metrics(
                    "arca_gateway",
                    stats_backend.node_clients().tls().map(|t| &*t.stats),
                );
                arca_lake_common::metrics::annotate(&out)
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
        if authorize {
            let Some(store) = store.clone() else {
                return fail("internal: owner-policy without a credential store");
            };
            let access = Authorizer::new(
                std::sync::Arc::clone(backend.metadata()),
                store,
                bucket_creation,
            )
            .with_trusted_proxies(trusted_proxies);
            arca_lake_gateway::serve_with_access(cfg, backend, Some(access)).await
        } else {
            arca_lake_gateway::serve(cfg, backend).await
        }
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
    if let Some(command) = args.command {
        return run_storage_node_command(command);
    }
    let Some(dir) = args.dir else {
        return fail("--dir is required");
    };
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return fail(format!("cannot create {}: {e}", dir.display()));
    }
    let mut cfg = arca_lake_storage_node::NodeConfig::new(args.listen, &dir);
    if let Some(b) = args.segment_max_bytes {
        cfg.store.segment_max_bytes = b;
    }
    if !(0.0..=1.0).contains(&args.compaction_min_dead_fraction) {
        return fail("--compaction-min-dead-fraction must be between 0 and 1");
    }
    cfg.capacity_bytes = args.capacity_bytes;
    cfg.reserve_bytes = args.reserve_bytes;
    cfg.fence_lease = Duration::from_secs(args.fence_lease_secs.max(1));
    if args.reclaim_retention_secs > 0
        && args.reclaim_skew_secs.saturating_mul(2) >= args.reclaim_retention_secs
    {
        // ADR-0028: a backup is protected for the retention less twice the
        // skew allowance; with nothing left, a reclaim could follow a
        // retirement almost at once (Phase 3.12 review).
        return fail("--reclaim-retention-secs must exceed twice --reclaim-skew-secs (or be 0)");
    }
    cfg.reclaim_retention = Duration::from_secs(args.reclaim_retention_secs);
    cfg.reclaim_skew = Duration::from_secs(args.reclaim_skew_secs);
    if args.reclaim_retention_secs == 0 {
        tracing::warn!(
            "--reclaim-retention-secs 0: deletes remove records at once; restores are not protected on this node (R-94(b), ADR-0028)"
        );
    } else {
        tracing::info!(
            retention_secs = args.reclaim_retention_secs,
            skew_secs = args.reclaim_skew_secs,
            "deferred reclamation on"
        );
    }
    cfg.compaction = arca_lake_storage_node::compactor::CompactionPolicy {
        interval: Duration::from_secs(args.compaction_interval_secs),
        min_dead_fraction: args.compaction_min_dead_fraction,
        max_segments: args.compaction_max_segments.max(1),
        max_copy_bytes: args.compaction_max_copy_mib << 20,
    };
    cfg.tls = match args.tls.context(arca_lake_protocol::tls::Role::Node) {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
    let node = match arca_lake_storage_node::serve(cfg).await {
        Ok(n) => n,
        Err(e) => return fail(format!("cannot start the storage node: {e}")),
    };
    tracing::info!(addr = %node.local_addr, dir = %dir.display(), "storage node listening");
    if let Some(addr) = args.metrics_listen {
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => return fail(format!("cannot bind metrics endpoint {addr}: {e}")),
        };
        tracing::info!(%addr, "storage node metrics endpoint");
        let service = Arc::clone(node.service());
        tokio::spawn(serve_gateway_metrics(listener, move || {
            let mut out = render_node_stats(&service.stats(), &service.store().timings());
            render_node_reclaim(&mut out, service.reclaim_state());
            render_compaction_stats(&mut out, service.compaction_stats());
            render_device_space(&mut out, service.device_space());
            render_node_capacity(
                &mut out,
                service.device_capacity().ok().as_ref(),
                service.admission_counts(),
                &service.capacity_breakdown(),
            );
            let (generation, open) = service.delete_fence();
            render_node_fence(
                &mut out,
                (generation, open, service.delete_fence_held()),
                service.fence_refusals(),
                service.format_rollover_failures(),
            );
            out += &arca_lake_protocol::tls::render_metrics(
                "arca_node",
                service.tls_stats().as_deref(),
            );
            arca_lake_common::metrics::annotate(&out)
        }));
    }
    wait_for_shutdown().await;
    tracing::info!("shutting down");
    node.shutdown().await;
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)]
fn run_storage_node_command(command: StorageNodeCommand) -> ExitCode {
    match command {
        StorageNodeCommand::CheckDowngrade { dir, format } => {
            let cfg = arca_lake_storage_node::NodeConfig::new(
                "127.0.0.1:0".parse().expect("literal"),
                &dir,
            )
            .store;
            match arca_lake_storage_node::Store::open(cfg) {
                Ok((store, _)) => match store.newest_segment_format() {
                    Ok(newest) => {
                        println!(
                            "newest segment format {newest}; a build reading up to {format} {}",
                            if newest <= format {
                                "can open this device"
                            } else {
                                "cannot open this device"
                            }
                        );
                        if newest <= format {
                            ExitCode::SUCCESS
                        } else {
                            ExitCode::from(2)
                        }
                    }
                    Err(e) => fail(e),
                },
                Err(e) => fail(format!("cannot open the store: {e}")),
            }
        }
        StorageNodeCommand::SegmentReport { dir, total_only } => {
            let r = match arca_lake_storage::analysis::analyze_dir(&dir) {
                Ok(r) => r,
                Err(e) => return fail(format!("cannot analyse {}: {e}", dir.display())),
            };
            let line = |what: &str, s: &arca_lake_storage::analysis::SegmentBreakdown| {
                format!(
                    "{{\"what\":\"{what}\",\"segment\":{},\"file_bytes\":{},\"live_bytes\":{},\"live_records\":{},\"dead_bytes\":{},\"dead_records\":{},\"tombstones_needed\":{},\"tombstones_droppable\":{},\"tombstones_kept_by_rule\":{},\"tombstone_bytes\":{},\"damaged_records\":{},\"slack_bytes\":{},\"min_seq\":{}}}",
                    s.segment_id,
                    s.file_bytes,
                    s.live_bytes,
                    s.live_records,
                    s.dead_bytes,
                    s.dead_records,
                    s.tombstones_needed,
                    s.tombstones_droppable,
                    s.tombstones_kept_by_rule,
                    s.tombstone_bytes,
                    s.damaged_records,
                    s.slack_bytes,
                    s.min_seq
                        .map_or_else(|| "null".to_string(), |m| m.to_string())
                )
            };
            if !total_only {
                for s in &r.segments {
                    println!("{}", line("segment", s));
                }
            }
            println!("{}", line("total", &r.total()));
            ExitCode::SUCCESS
        }
        StorageNodeCommand::QuarantineList { dir, scan } => {
            match std::fs::read_to_string(dir.join("quarantine.log")) {
                Ok(text) => {
                    for line in text.lines() {
                        println!("{line}");
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    println!("no quarantine log");
                }
                Err(e) => return fail(format!("cannot read quarantine.log: {e}")),
            }
            if scan {
                let cfg = arca_lake_storage_node::NodeConfig::new(
                    "127.0.0.1:0".parse().expect("literal"),
                    &dir,
                )
                .store;
                match arca_lake_storage_node::Store::open(cfg) {
                    Ok((store, report)) => {
                        println!(
                            "open segments {} live_shards {} superseded {} truncated_bytes {} corrupt_segments {:?} quarantined {:?} holes {:?}",
                            report.segments,
                            report.live_shards,
                            report.superseded_records,
                            report.truncated_bytes,
                            report.corrupt_segments,
                            report.quarantined,
                            report.holes
                        );
                        for (k, q) in store.quarantined() {
                            println!(
                                "quarantined group {} chunk {} index {} segment {} offset {} sequence {} len {} reason {:?} since_ms {}",
                                k.group_id,
                                hex(&k.chunk_id),
                                k.shard_index,
                                q.location.segment_id,
                                q.location.offset,
                                q.location.sequence,
                                q.location.payload_len,
                                q.reason,
                                q.since_unix_ms
                            );
                        }
                    }
                    Err(e) => return fail(format!("cannot open the store: {e}")),
                }
            }
            ExitCode::SUCCESS
        }
        StorageNodeCommand::CorruptShard {
            dir,
            group,
            chunk,
            index,
            pick,
            header,
        } => {
            use std::os::unix::fs::FileExt as _;
            let cfg = arca_lake_storage_node::NodeConfig::new(
                "127.0.0.1:0".parse().expect("literal"),
                &dir,
            )
            .store;
            let (store, _) = match arca_lake_storage_node::Store::open(cfg) {
                Ok(s) => s,
                Err(e) => return fail(format!("cannot open the store: {e}")),
            };
            let key = if let Some(n) = pick {
                let mut keys = store.keys();
                if keys.is_empty() {
                    return fail("no live shard on this device");
                }
                keys.sort_by_key(|k| (k.group_id, k.chunk_id, k.shard_index));
                let k = keys[usize::try_from(n % keys.len() as u64).unwrap_or(0)];
                println!(
                    "picked group {} chunk {} index {}",
                    k.group_id,
                    k.chunk_id.iter().fold(String::new(), |mut s, b| {
                        let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{b:02x}"));
                        s
                    }),
                    k.shard_index
                );
                k
            } else {
                let (Some(group), Some(chunk), Some(index)) = (group, chunk, index) else {
                    return fail("--group, --chunk and --index are required without --pick");
                };
                let chunk_id = match parse_chunk_id(&chunk) {
                    Ok(c) => c,
                    Err(e) => return fail(e),
                };
                arca_lake_storage_node::ShardKey {
                    group_id: group,
                    chunk_id,
                    shard_index: index,
                }
            };
            let Some(loc) = store.locate(&key) else {
                return fail("shard is not live on this device");
            };
            let path = store
                .dir()
                .join(format!("seg-{:016x}.arca", loc.segment_id));
            let pos = if header {
                loc.offset + 8
            } else {
                loc.payload_offset() + u64::from(loc.payload_len) / 2
            };
            drop(store);
            let f = match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
            {
                Ok(f) => f,
                Err(e) => return fail(format!("cannot open {}: {e}", path.display())),
            };
            let mut b = [0u8; 1];
            if let Err(e) = f.read_exact_at(&mut b, pos) {
                return fail(format!("read: {e}"));
            }
            if let Err(e) = f.write_all_at(&[b[0] ^ 0x5a], pos) {
                return fail(format!("write: {e}"));
            }
            if let Err(e) = f.sync_all() {
                return fail(format!("sync: {e}"));
            }
            println!(
                "flipped one byte of {} at offset {pos} (segment {} record offset {})",
                if header {
                    "the record header"
                } else {
                    "the payload"
                },
                loc.segment_id,
                loc.offset
            );
            ExitCode::SUCCESS
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn run_controller(args: ControllerArgs) -> ExitCode {
    let meta = match open_meta(&args.cluster_file, &args.fdb_tls) {
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
    cfg.admission.dwell_ms = args.admission_dwell_secs.saturating_mul(1000);
    #[allow(clippy::cast_precision_loss)]
    {
        cfg.admission.horizon_s = args.admission_horizon_secs.max(1) as f64;
    }
    cfg.admission.group_min_shards = args.admission_group_min_shards.max(1);
    cfg.admission.allow_shared_filesystem = args.allow_shared_filesystem;
    cfg.admission_max_chunk_bytes = args.admission_max_chunk_bytes.max(1);
    cfg.reconcile_interval = Duration::from_secs(args.reconcile_interval_secs);
    cfg.reclaim_interval = Duration::from_secs(args.reclaim_interval_secs.max(1));
    cfg.repair_interval = Duration::from_secs(args.repair_interval_secs);
    cfg.health_interval = Duration::from_secs(args.health_interval_secs);
    cfg.failure_domain = args.failure_domain;
    cfg.repair_source = if args.repair_source == "direct" {
        arca_lake_controller::repair::RepairSource::Direct
    } else {
        arca_lake_controller::repair::RepairSource::Reconstruct
    };
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
    if args.rebalance {
        cfg.rebalance = Some(arca_lake_controller::rebalance::RebalancePolicy {
            start_imbalance: args.rebalance_start_imbalance.clamp(0.0, 1.0),
            stop_imbalance: args
                .rebalance_stop_imbalance
                .clamp(0.0, args.rebalance_start_imbalance.clamp(0.0, 1.0)),
            bytes_per_pass: args.rebalance_mib_per_pass << 20,
            settle_ms: 1000
                * args
                    .rebalance_settle_secs
                    .unwrap_or(args.min_orphan_age_secs + 2 * args.reconcile_interval_secs),
            gc_backlog_pause_bytes: args.rebalance_gc_backlog_pause_mib.saturating_mul(1 << 20),
            ..Default::default()
        });
    }
    cfg.rebalance_interval = Duration::from_secs(args.rebalance_interval_secs.max(1));
    cfg.cache_repair_chunks_per_pass = args.cache_repair_chunks_per_pass;
    cfg.planned_move_chunks_per_pass = args.planned_move_chunks_per_pass;
    cfg.reconcile_pairs_per_pass = args.reconcile_pairs_per_pass;
    cfg.reconcile_parallel_pairs = args.reconcile_parallel_pairs;
    cfg.reclaim_parallel_nodes = args.reclaim_parallel_nodes;
    cfg.tiering_generations_per_pass = args.tiering_generations_per_pass;
    cfg.tiering_clock_skew = Duration::from_secs(args.tiering_clock_skew_secs);
    cfg.failure_repair_chunks_per_pass = args.failure_repair_chunks_per_pass;
    cfg.scrub = arca_lake_controller::integrity::ScrubOptions {
        page: args.scrub_page,
        page_bytes: args.scrub_page_mib << 20,
        rate_bytes_per_s: args.scrub_rate_mib << 20,
        interval_ms: args.scrub_interval_secs * 1000,
        concurrency: args.scrub_concurrency,
        manifest_roots_per_pass: 256,
        adaptive: args.scrub_adaptive.on.then(|| {
            adaptive_policy(
                args.scrub_rate_mib,
                args.scrub_adaptive.min_mib,
                args.scrub_adaptive.max_mib,
                args.scrub_adaptive.get_p99_ms,
            )
        }),
    };
    cfg.scrub_tick = Duration::from_secs(args.scrub_tick_secs.max(1));
    cfg.stripe_repair = arca_lake_controller::integrity::RepairWorkerOptions {
        tasks_per_pass: args.stripe_repair_tasks_per_pass,
        max_attempts: args.stripe_repair_max_attempts,
        workers: args.stripe_repair_workers,
        rate_bytes_per_s: args.stripe_repair_rate_mib << 20,
        per_node_in_flight: args.stripe_repair_per_node,
        ..Default::default()
    };
    cfg.stripe_repair_interval = Duration::from_secs(args.stripe_repair_interval_secs.max(1));
    cfg.node_timeout = Duration::from_secs(args.rpc.node_timeout_secs.max(1));
    cfg.rpc_policy = Some(args.rpc.policy());
    cfg.tls = match args
        .rpc
        .tls
        .context(arca_lake_protocol::tls::Role::Controller)
    {
        Ok(t) => t,
        Err(e) => return fail(e),
    };
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
    let meta = match open_meta(&args.cluster_file, &args.fdb_tls) {
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
                    // A changed label can put two members of a group in one
                    // domain: reported, not refused (ADR-0038).
                    if let Ok(a) =
                        arca_lake_controller::failure_domain::audit(meta.as_ref(), None, now).await
                    {
                        for f in a.findings.iter().filter(|f| finding_names(f, n.id)) {
                            println!("warning: {f}");
                        }
                    }
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
        AdminCommand::Integrity { command } => run_admin_integrity(meta, command, now).await,
        AdminCommand::Group { command } => group_command(meta.as_ref(), command, now).await,
        AdminCommand::Init => {
            match arca_lake_metadata::ops::compat::check_startup(
                meta.as_ref(),
                &arca_lake_controller::capabilities(),
                arca_lake_metadata::ops::compat::Stamp::Legacy,
                "admin",
                now,
            )
            .await
            {
                Ok(g) => {
                    print_gate(&g);
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        AdminCommand::Upgrade { command } => run_admin_upgrade(meta, command, now).await,
        AdminCommand::Recovery { command } => run_admin_recovery(meta, command, now).await,
        AdminCommand::Cluster { command } => run_admin_cluster(meta, command, now).await,
        AdminCommand::Backup { command } => run_admin_backup(meta.as_ref(), command, now).await,
        AdminCommand::ReplaceMember {
            group,
            index,
            node,
            yes,
            cancel,
            dry_run,
            plan,
        } => {
            use arca_lake_metadata::ops::membership;
            if dry_run {
                return dry_run_plan(&plan, async |clients| {
                    arca_lake_controller::plan::plan_replace(
                        meta.as_ref(),
                        clients,
                        group,
                        index,
                        node,
                        &plan.plan_failure_domain,
                        1000,
                    )
                    .await
                })
                .await;
            }
            if cancel {
                return match membership::clear_replace(meta.as_ref(), group).await {
                    Ok(Some(r)) => {
                        println!(
                            "withdrew the request for member {} of group {group} (a rebuild already started continues until the group is complete)",
                            r.index
                        );
                        ExitCode::SUCCESS
                    }
                    Ok(None) => {
                        println!("no request for group {group}");
                        ExitCode::SUCCESS
                    }
                    Err(e) => fail(e),
                };
            }
            if !yes {
                return fail(
                    "replace-member moves a member and rebuilds its shards; re-run with --yes",
                );
            }
            if let Err(e) = domain_recorded(meta.as_ref(), &[group]).await {
                return fail(e);
            }
            match membership::request_replace(meta.as_ref(), group, index, node, "admin", now).await
            {
                Ok(r) => {
                    println!(
                        "requested: group {} member {} (node {}) -> {}; the controller leader carries it out (admin status shows progress)",
                        r.group,
                        r.index,
                        r.from_node,
                        r.to_node.map_or_else(
                            || "a node it chooses".to_string(),
                            |n| format!("node {n}")
                        )
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        AdminCommand::DecommissionNode {
            node,
            yes,
            cancel,
            dry_run,
            plan,
        } => decommission_command(meta.as_ref(), node, yes, cancel, dry_run, &plan, now).await,
        AdminCommand::Node { command } => node_command(meta.as_ref(), command, now).await,
        AdminCommand::Restore { command } => {
            restore_command(&meta, &args.cluster_file, command, now).await
        }
        AdminCommand::CheckConfig => check_config(meta.as_ref()).await,
        AdminCommand::Rebalance { command } => rebalance_command(meta.as_ref(), command).await,
        AdminCommand::Tiering { command } => tiering_command(meta.as_ref(), command).await,
        AdminCommand::DeleteFence { command } => {
            delete_fence_command(meta.as_ref(), command, now).await
        }
        AdminCommand::Status { versions } => {
            if versions {
                let g = arca_lake_metadata::ops::compat::gate(meta.as_ref()).await;
                let procs = arca_lake_metadata::ops::compat::procs(meta.as_ref()).await;
                let caps = arca_lake_metadata::ops::recovery::node_caps(meta.as_ref()).await;
                match (g, procs, caps) {
                    (Ok(g), Ok(procs), Ok(caps)) => {
                        print_gate(&g);
                        println!(
                            "this binary software {} protocol [{}, {}] schema [{}, {}] format max {}",
                            arca_lake_common::compat::software_version(),
                            arca_lake_protocol::storage::v1::PROTOCOL_VERSION_MIN,
                            arca_lake_protocol::storage::v1::PROTOCOL_VERSION,
                            arca_lake_metadata::keys::SCHEMA_VERSION_MIN,
                            arca_lake_metadata::keys::SCHEMA_VERSION,
                            arca_lake_storage::format::SEGMENT_FORMAT_VERSION
                        );
                        for p in procs {
                            println!(
                                "proc {:?} {} software {} protocol [{}, {}] schema [{}, {}] incarnation {} last_ms {}",
                                p.kind,
                                p.id,
                                p.software,
                                p.caps.protocol_min,
                                p.caps.protocol_max,
                                p.caps.schema_min,
                                p.caps.schema_max,
                                arca_lake_metadata::ops::compat::hex(&p.incarnation),
                                p.last_ms
                            );
                        }
                        for c in caps {
                            println!(
                                "node {} software {} protocol [{}, {}] format [{}, {}] format_write {} incarnation {} epochs {:?} seen_ms {}",
                                c.node_id,
                                c.software,
                                c.caps.protocol_min,
                                c.caps.protocol_max,
                                c.caps.format_min,
                                c.caps.format_max,
                                c.format_write,
                                c.incarnation.map_or_else(
                                    || "-".into(),
                                    |i| arca_lake_metadata::ops::compat::hex(&i)
                                ),
                                c.group_epochs,
                                c.seen_ms
                            );
                        }
                    }
                    (g, p, c) => {
                        return fail(
                            g.err()
                                .or(p.err())
                                .or(c.err())
                                .map_or_else(|| "unknown".to_string(), |e| e.to_string()),
                        );
                    }
                }
            }
            let nodes = arca_lake_metadata::ops::cluster::list_nodes(meta.as_ref()).await;
            let groups = arca_lake_metadata::ops::cluster::list_groups(meta.as_ref()).await;
            let leader = arca_lake_metadata::ops::leader::current(meta.as_ref()).await;
            let domains = arca_lake_metadata::ops::group_domain::list(meta.as_ref())
                .await
                .unwrap_or_default();
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
                    // A line of its own (scripts parse the group lines).
                    for d in domains.values() {
                        println!(
                            "failure_domain group {} label {} source {} set_ms {}",
                            d.group,
                            d.label,
                            d.source.as_str(),
                            d.set_ms
                        );
                    }
                    // ADR-0038 (R-119): groups that break or lack their
                    // failure domain.
                    if let Ok(a) =
                        arca_lake_controller::failure_domain::audit(meta.as_ref(), None, now).await
                    {
                        println!("{}", a.enforcement());
                        for f in &a.findings {
                            println!("failure domain: {f}");
                        }
                        if !a.at_risk.is_empty() {
                            println!("{}", a.at_risk_line());
                        }
                    }
                    match leader {
                        Some(l) => println!(
                            "leader {} token {} lease_until_ms {}",
                            l.holder, l.token, l.lease_until_ms
                        ),
                        None => println!("leader: none"),
                    }
                    if let Ok(states) =
                        arca_lake_metadata::ops::membership::node_states(meta.as_ref()).await
                    {
                        for r in states {
                            println!(
                                "node {} lifecycle {:?} blocked {:?}",
                                r.node_id, r.state, r.blocked
                            );
                        }
                    }
                    if let Ok(reqs) =
                        arca_lake_metadata::ops::membership::replace_requests(meta.as_ref()).await
                    {
                        for r in reqs {
                            println!(
                                "replace group {} member {} from node {} to {:?} requested_ms {} last_error {:?}",
                                r.group,
                                r.index,
                                r.from_node,
                                r.to_node,
                                r.requested_ms,
                                r.last_error
                            );
                        }
                    }
                    // Phase 3.13 (R-104): why each rebuilding group is
                    // where it is.
                    if let Ok(statuses) =
                        arca_lake_metadata::ops::repair_status::list(meta.as_ref()).await
                    {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(0));
                        for s in statuses {
                            println!(
                                "repair group {} member {} target {} epoch {} status {} since_ms {} last_progress_ms {} stalled_s {} failures {} retargets {} next_attempt_ms {} former {:?} detail {:?}",
                                s.group,
                                s.index,
                                s.target,
                                s.epoch,
                                s.reason.as_str(),
                                s.since_ms,
                                s.last_progress_ms,
                                now.saturating_sub(s.last_progress_ms.max(s.since_ms)) / 1000,
                                s.failures,
                                s.retargets,
                                s.next_attempt_ms,
                                s.former,
                                s.detail
                            );
                            // Phase 3.14: what the operator can do.
                            if let Some(action) = repair_action(s.reason) {
                                println!("  group {}: {action}", s.group);
                            }
                        }
                    }
                    // Phase 3.14 (ADR-0032): room reserved by member moves.
                    if let (Ok(resv), Ok(groups)) = (
                        arca_lake_metadata::ops::reservation::list(meta.as_ref()).await,
                        arca_lake_metadata::ops::cluster::list_groups(meta.as_ref()).await,
                    ) {
                        for r in resv {
                            println!(
                                "reservation node {} group {} member {} epoch {} kind {} bytes {} written {} remaining {} holds {}",
                                r.node,
                                r.group,
                                r.index,
                                r.epoch,
                                r.kind.as_str(),
                                r.bytes,
                                r.written,
                                r.remaining(),
                                groups.iter().any(|g| r.holds(g))
                            );
                        }
                    }
                    // Phase 3.14: background sweeps' cursors.
                    if let Ok(cursors) =
                        arca_lake_metadata::ops::bg_cursor::list(meta.as_ref()).await
                    {
                        for (name, c) in cursors {
                            println!(
                                "sweep {name} sweeps {} last_sweep_ms {} sweep_started_ms {} last_pass_ms {} last_pass_items {} last_pass_errors {} at_start {}",
                                c.sweeps,
                                c.last_sweep_ms,
                                c.sweep_started_ms,
                                c.last_pass_ms,
                                c.last_pass_items,
                                c.last_pass_errors,
                                c.after.is_empty()
                            );
                        }
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

/// What an operator can do about a repair waiting for `reason` (Phase
/// 3.14, `admin status`); `None` when it progresses on its own.
fn repair_action(
    reason: arca_lake_metadata::ops::repair_status::RepairReason,
) -> Option<&'static str> {
    use arca_lake_metadata::ops::repair_status::RepairReason as R;
    match reason {
        R::Progressing | R::TemporarilyBlocked => None,
        R::AwaitingCapacity => Some(
            "waiting for capacity: add a node or free space on one (the repair floor and reserves are never lowered for it); redundancy is reduced meanwhile",
        ),
        R::AwaitingNodes => Some(
            "waiting for nodes: bring back the members that do not answer (a node that will not return: `admin node` lifecycle commands); with as many members down as the parity, another failure loses data",
        ),
        R::DataUnrecoverable => Some(
            "WARNING: possible data loss: chunks with fewer than k readable shards while every member answers; bring back any node or disk that held this group, the chunks are listed by `admin integrity list`; the group stays Repairing and nothing is deleted for it",
        ),
        R::PermanentlyBlocked => Some("needs an operator: see the detail and the controller log"),
        R::AwaitingFailureDomain => Some(
            "waiting for a failure domain: the group has no recorded failure domain and the move its repair needs is refused until `admin group set-failure-domain --group <id> --label <label>` records the label it was created with; redundancy is reduced meanwhile",
        ),
    }
}

/// Run a dry-run planner and print the plan; exit 0 when nothing blocks
/// it, 2 when something would (the plan is printed either way).
#[allow(clippy::cast_precision_loss)]
async fn dry_run_plan(
    args: &PlanArgs,
    planner: impl AsyncFnOnce(
        &arca_lake_protocol::client::NodeClients,
    ) -> Result<
        arca_lake_controller::plan::Plan,
        arca_lake_controller::ControlError,
    >,
) -> ExitCode {
    let clients = match args.rpc.tls.context(arca_lake_protocol::tls::Role::Admin) {
        Ok(t) => {
            arca_lake_protocol::client::NodeClients::with_policy(args.rpc.policy()).with_tls(t)
        }
        Err(e) => return fail(e),
    };
    let p = match planner(&clients).await {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
    let mib = |b: u64| b as f64 / f64::from(1u32 << 20);
    println!(
        "DRY RUN (nothing was changed): cluster {}",
        p.phase.map_or("unknown", |p| p.name())
    );
    for m in &p.moves {
        println!(
            "move group {} (tier {}{}) member {}: node {} -> {} | chunks {} | write {:.1} MiB | read {:.1} MiB | sources {}/{} | target free {}",
            m.group,
            m.tier,
            if m.cache { ", cache" } else { "" },
            m.index,
            m.from_node,
            m.to_node
                .map_or_else(|| "NONE".to_string(), |n| format!("node {n}")),
            m.size
                .map_or_else(|| "?".to_string(), |s| s.shards.to_string()),
            mib(m.size.map_or(0, |s| s.bytes)),
            mib(m.read_bytes),
            m.sources.0,
            m.sources.1,
            m.to_free
                .map_or_else(|| "unknown".to_string(), |f| format!("{:.1} MiB", mib(f)))
        );
        for b in &m.blockers {
            println!("  BLOCKED: {b}");
        }
        for n in &m.notes {
            println!("  note: {n}");
        }
    }
    for b in &p.blockers {
        println!("BLOCKED: {b}");
    }
    for n in &p.notes {
        println!("note: {n}");
    }
    let secs = mib(p.read_bytes().max(p.write_bytes())) / args.rate_mib.max(1) as f64;
    println!(
        "total: write {:.1} MiB, read {:.1} MiB, about {:.0} s at {} MiB/s; {}",
        mib(p.write_bytes()),
        mib(p.read_bytes()),
        secs,
        args.rate_mib,
        if p.blocked() {
            "would be BLOCKED"
        } else {
            "would proceed"
        }
    );
    if p.blocked() {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    }
}

/// `admin delete-fence` subcommands.
#[derive(Debug, Subcommand)]
enum DeleteFenceCommand {
    /// The recorded fence and every registered node's.
    Status {
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// Close the fence on every node with an operator hold: a new closed
    /// generation, recorded and pushed; each node acknowledges it after
    /// the deletes past its old one finished. Exits 1 unless every node
    /// acknowledged (re-run once they answer).
    Close {
        /// Why (recorded).
        #[arg(long)]
        reason: String,
        /// Confirm (deletes, GC and reconciliation stop until released).
        #[arg(long)]
        yes: bool,
        /// Node ids whose process the operator declares stopped (they
        /// cannot acknowledge; recorded in the reason). Never a running
        /// node: its deletes would go on.
        #[arg(long = "absent-node")]
        absent_node: Vec<u64>,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// Release the operator hold (cluster `Active` only): the leader opens
    /// a new generation at its next pass and pushes it to the nodes.
    Release {
        /// Why (recorded).
        #[arg(long)]
        reason: String,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
}

fn admin_node_clients(
    rpc: &NodeRpcArgs,
) -> Result<arca_lake_protocol::client::NodeClients, String> {
    rpc.tls
        .context(arca_lake_protocol::tls::Role::Admin)
        .map(|t| arca_lake_protocol::client::NodeClients::with_policy(rpc.policy()).with_tls(t))
        .map_err(|e| e.clone())
}

fn print_node_fences(nodes: &[arca_lake_controller::delete_fence::NodeFence]) {
    for n in nodes {
        match &n.fence {
            Ok((g, open)) => println!(
                "node {} generation {} {}{}",
                n.node_id,
                g,
                if *open { "open" } else { "closed" },
                if n.held { " held" } else { "" }
            ),
            Err(e) => println!("node {} unreachable ({e})", n.node_id),
        }
        match &n.identity {
            arca_lake_controller::delete_fence::IdentityCheck::Verified => {}
            arca_lake_controller::delete_fence::IdentityCheck::Unverifiable(why) => {
                println!("node {} identity unverified ({why})", n.node_id);
            }
            arca_lake_controller::delete_fence::IdentityCheck::Mismatch(why) => {
                println!("node {} identity MISMATCH ({why}): not counted", n.node_id);
            }
        }
    }
}

/// Close the fence on every node; `Err` with the message when a node did
/// not acknowledge.
async fn close_fence_everywhere<S: arca_lake_metadata::KvStore>(
    meta: &S,
    rpc: &NodeRpcArgs,
    reason: &str,
    hold: bool,
    absent: &[u64],
    now: u64,
) -> Result<arca_lake_controller::delete_fence::Closed, String> {
    let clients = admin_node_clients(rpc)?;
    let reason = if absent.is_empty() {
        reason.to_string()
    } else {
        format!("{reason} (nodes {absent:?} declared stopped by the operator)")
    };
    let closed =
        arca_lake_controller::delete_fence::close_and_push(meta, &clients, &reason, hold, now)
            .await
            .map_err(|e| e.to_string())?;
    println!(
        "delete fence closed: generation {}{}",
        closed.record.generation,
        if closed.record.operator_hold {
            " (operator hold)"
        } else {
            ""
        }
    );
    print_node_fences(&closed.nodes);
    let (declared, missing): (Vec<u64>, Vec<u64>) = closed
        .not_acknowledged()
        .into_iter()
        .partition(|id| absent.contains(id));
    if !declared.is_empty() {
        eprintln!(
            "warning: nodes {declared:?} did not acknowledge; declared stopped (--absent-node): keep them stopped until the cluster's leader has pushed them a fence again (their old fence is still open on their disk)"
        );
    }
    if missing.is_empty() {
        Ok(closed)
    } else {
        Err(format!(
            "nodes {missing:?} did not acknowledge the closed fence: deletes may still run there; make them reachable and re-run, or, for a node whose process is stopped and stays stopped, re-run with --absent-node ID"
        ))
    }
}

/// `admin restore` subcommands (Phase 3.11).
#[derive(Debug, Subcommand)]
enum RestoreCommand {
    /// Check the cluster and every node, show the target cluster, the
    /// source and the destination, and write the journal with the
    /// confirmation token `run` needs. Changes nothing in the cluster.
    Plan {
        /// Backup URL (as `fdbrestore -r` takes it).
        #[arg(long)]
        source: String,
        /// Version to restore to (`fdbrestore -v`; the backup's minimum
        /// restorable version for its snapshot point).
        #[arg(long)]
        version: u64,
        /// Backup point, wall clock ms (splits orphans in the report; with
        /// the nodes' retention, bounds the backup's age, ADR-0028).
        #[arg(long)]
        backup_point_ms: Option<u64>,
        /// Journal file (outside the metadata store; keep it until
        /// `finish` completed).
        #[arg(long)]
        journal: PathBuf,
        /// Restore a backup outside the protection of deferred
        /// reclamation (older than the nodes' retention less twice their
        /// skew allowance, of unknown age, or with nodes that do not
        /// defer): objects whose shards were reclaimed since are lost.
        #[arg(long)]
        accept_unprotected_backup: bool,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// Replace the backup of a restore whose store was cleared and whose
    /// restore failed or was refused (another cluster's backup).
    Amend {
        /// Journal file.
        #[arg(long)]
        journal: PathBuf,
        /// Backup URL.
        #[arg(long)]
        source: String,
        /// Version to restore to.
        #[arg(long)]
        version: u64,
        /// Backup point, wall clock ms.
        #[arg(long)]
        backup_point_ms: Option<u64>,
    },
    /// Freeze, close and hold the fence on every node, clear and restore,
    /// `recovery begin`, verify. Resumes from the journal.
    Run {
        /// Journal file written by `plan`.
        #[arg(long)]
        journal: PathBuf,
        /// The token `plan` printed.
        #[arg(long)]
        confirm: String,
        /// A process whose heartbeat is fresh but which is stopped.
        #[arg(long)]
        stopped: Vec<String>,
        /// A node stopped for good: it need not acknowledge the fence on the
        /// restored metadata nor answer on a retry after the clear (the
        /// operator's statement, as for `recovery begin`).
        #[arg(long)]
        absent_node: Vec<u64>,
        /// Command prefix running `fdbrestore` where the backup agent
        /// lives (e.g. `docker exec NAME`).
        #[arg(long)]
        fdbrestore_cmd: Option<String>,
        /// Cluster file as that command sees it (default: this one).
        #[arg(long)]
        fdbrestore_cluster_file: Option<String>,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// Finalize on the report (`admin recovery status` shows it), then
    /// wait until a leader opened a new generation on every node.
    Finish {
        /// Journal file.
        #[arg(long)]
        journal: PathBuf,
        /// Who decides.
        #[arg(long)]
        operator: String,
        /// Finalize despite critical findings (needs --reason).
        #[arg(long)]
        force: bool,
        /// Why (required with --force).
        #[arg(long, default_value = "")]
        reason: String,
        /// How long to wait for the new generation, s.
        #[arg(long, default_value_t = 120)]
        wait_secs: u64,
        /// A node stopped for good, not waited for.
        #[arg(long)]
        absent_node: Vec<u64>,
        /// Confirm.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// Give up a restore before its clear: remove its marker, which holds
    /// node changes (the cluster stays read-only and held).
    Abandon {
        /// Journal file.
        #[arg(long)]
        journal: PathBuf,
        /// Confirm.
        #[arg(long)]
        yes: bool,
    },
    /// Show the journal's steps.
    Status {
        /// Journal file.
        #[arg(long)]
        journal: PathBuf,
    },
}

/// This command's destination as the journal records it: the cluster
/// file's path and the cluster's connection id (`description:id`, the part
/// of its content before `@`; Phase 3.12), so that a file that now names
/// another cluster at the same path is not the planned destination.
fn destination_of(cluster_file: &std::path::Path) -> String {
    let id = std::fs::read_to_string(cluster_file)
        .ok()
        .and_then(|t| {
            t.lines()
                .map(str::trim)
                .find(|l| !l.is_empty() && !l.starts_with('#'))
                .and_then(|l| l.split_once('@'))
                .map(|(id, _)| id.to_string())
        })
        .unwrap_or_else(|| "unreadable".into());
    format!("{} (cluster {id})", cluster_file.display())
}

/// A private copy of `from` at `to` (owner-only, synced), read by the
/// restore tools of `admin restore run`.
fn pin_cluster_file(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let text = std::fs::read(from)?;
    // Never replaces anything (create_new): a name in use is refused.
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(to)?;
    f.write_all(&text)?;
    f.sync_all()
}

/// Runs `fdbrestore` into this command's cluster.
struct FdbRestorer {
    prefix: Vec<String>,
    cluster_file: String,
    /// [`destination_of`] this command's cluster file.
    destination: String,
}

impl FdbRestorer {
    fn tool(&self, name: &str) -> tokio::process::Command {
        match self.prefix.split_first() {
            Some((first, rest)) => {
                let mut c = tokio::process::Command::new(first);
                c.args(rest).arg(name);
                c
            }
            None => tokio::process::Command::new(name),
        }
    }
}

impl arca_lake_controller::managed_restore::Restorer for FdbRestorer {
    fn destination(&self) -> String {
        self.destination.clone()
    }

    async fn probe(&self, key: &[u8], value: &[u8]) -> Result<bool, String> {
        use arca_lake_controller::managed_restore as mr;
        let out = self
            .tool("fdbcli")
            .args([
                "-C",
                &self.cluster_file,
                "--timeout",
                "30",
                "--exec",
                &mr::fdbcli_get(key),
            ])
            .output()
            .await
            .map_err(|e| format!("fdbcli: {e}"))?;
        let text = String::from_utf8_lossy(&out.stdout);
        if !out.status.success() {
            return Err(format!(
                "fdbcli exited {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(mr::fdbcli_shows(&text, value))
    }

    async fn restore(&self, source: &str, version: u64) -> Result<(), String> {
        let mut cmd = self.tool("fdbrestore");
        cmd.args([
            "start",
            "--dest-cluster-file",
            &self.cluster_file,
            "-r",
            source,
            "-v",
            &version.to_string(),
            "-w",
        ]);
        let out = cmd.output().await.map_err(|e| format!("fdbrestore: {e}"))?;
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        if out.status.success() {
            Ok(())
        } else {
            Err(format!("fdbrestore exited {}: {}", out.status, text.trim()))
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn restore_command<S: arca_lake_metadata::KvStore + 'static>(
    meta: &Arc<S>,
    cluster_file: &std::path::Path,
    command: RestoreCommand,
    now: u64,
) -> ExitCode {
    use arca_lake_controller::managed_restore as mr;
    let clock = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| u64::try_from(d.as_millis()).ok())
            .unwrap_or(0)
    };
    match command {
        RestoreCommand::Plan {
            source,
            version,
            backup_point_ms,
            journal,
            accept_unprotected_backup,
            rpc,
        } => {
            let clients = match admin_node_clients(&rpc) {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            match mr::plan(
                meta.as_ref(),
                &clients,
                &source,
                version,
                backup_point_ms,
                &destination_of(cluster_file),
                &journal,
                accept_unprotected_backup,
                now,
            )
            .await
            {
                Ok(p) => {
                    for l in &p.summary {
                        println!("{l}");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => fail(format!("restore not planned: {e}")),
            }
        }
        RestoreCommand::Run {
            journal,
            confirm,
            stopped,
            absent_node,
            fdbrestore_cmd,
            fdbrestore_cluster_file,
            rpc,
        } => {
            let clients = match admin_node_clients(&rpc) {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            let prefix: Vec<String> = fdbrestore_cmd
                .map(|c| c.split_whitespace().map(str::to_string).collect())
                .unwrap_or_default();
            // Run here on this command's cluster file: the probe and the
            // restore both read a private copy of it, taken now (Phase 3.12
            // review: a file replaced or relinked between the probe and
            // `fdbrestore` cannot redirect the restore). With a command
            // prefix (another file system) the tool reads its own file: the
            // probe checks it, and the boundary is that file's owner.
            // A name of its own (re-review: a fixed extension could be the
            // journal's own path, or the cluster file's).
            let pinned = (prefix.is_empty() && fdbrestore_cluster_file.is_none()).then(|| {
                let name = journal
                    .file_name()
                    .map_or_else(|| "restore".into(), |n| n.to_string_lossy().into_owned());
                journal.with_file_name(format!("{name}.{}.pinned-cluster", std::process::id()))
            });
            if let Some(p) = &pinned
                && let Err(e) = pin_cluster_file(cluster_file, p)
            {
                return fail(format!(
                    "cannot pin the cluster file to {}: {e}",
                    p.display()
                ));
            }
            let restorer = FdbRestorer {
                prefix,
                cluster_file: pinned.as_ref().map_or_else(
                    || {
                        fdbrestore_cluster_file
                            .clone()
                            .unwrap_or_else(|| cluster_file.display().to_string())
                    },
                    |p| p.display().to_string(),
                ),
                destination: destination_of(cluster_file),
            };
            let result = mr::run(
                meta,
                &clients,
                &restorer,
                &journal,
                &confirm,
                &stopped,
                &absent_node,
                clock,
            )
            .await;
            if let Some(p) = &pinned {
                let _ = std::fs::remove_file(p);
            }
            match result {
                Ok(j) => {
                    println!(
                        "restored and verified (new incarnation {}): review `admin recovery status`, then `admin restore finish`",
                        j.new_incarnation.unwrap_or_default()
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => fail(format!(
                    "admin restore stopped (re-run resumes; journal {}): {e}",
                    journal.display()
                )),
            }
        }
        RestoreCommand::Finish {
            journal,
            operator,
            force,
            reason,
            wait_secs,
            absent_node,
            yes,
            rpc,
        } => {
            if !yes {
                return fail("finish makes the restored cluster Active; re-run with --yes");
            }
            if force && reason.is_empty() {
                return fail("--force needs --reason");
            }
            let clients = match admin_node_clients(&rpc) {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            match mr::finish(
                meta.as_ref(),
                &clients,
                &journal,
                &operator,
                &reason,
                force,
                &absent_node,
                std::time::Duration::from_secs(wait_secs),
                clock,
            )
            .await
            {
                Ok(_) => {
                    println!(
                        "restore finished: Active, a new delete fence generation open on every node"
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        RestoreCommand::Abandon { journal, yes } => {
            if !yes {
                return fail(
                    "abandon lets node changes go on before the restore cleared anything; re-run with --yes",
                );
            }
            match mr::abandon(meta.as_ref(), &journal).await {
                Ok(true) => {
                    println!(
                        "restore marker removed; the cluster stays read-only with its fence held (activate it and release the hold when ready)"
                    );
                    ExitCode::SUCCESS
                }
                Ok(false) => {
                    println!("no restore marker");
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        RestoreCommand::Amend {
            journal,
            source,
            version,
            backup_point_ms,
        } => match mr::amend(&journal, &source, version, backup_point_ms) {
            Ok(_) => {
                println!("backup replaced: re-run `admin restore run`");
                ExitCode::SUCCESS
            }
            Err(e) => fail(e),
        },
        RestoreCommand::Status { journal } => match mr::Journal::load(&journal) {
            Ok(j) => {
                println!(
                    "source {} version {} destination {}",
                    j.source, j.version, j.destination
                );
                match (&j.unprotected, j.protected_until_ms) {
                    (Some(why), _) => println!("accepted outside deferred reclamation: {why}"),
                    (None, Some(until)) => println!("protected until {until} (ms)"),
                    (None, None) => println!("protection unknown"),
                }
                println!(
                    "steps done: {}",
                    j.done
                        .iter()
                        .map(|s| s.name())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                ExitCode::SUCCESS
            }
            Err(e) => fail(e),
        },
    }
}

/// Whether finding `f` is about node `node`.
fn finding_names(f: &arca_lake_controller::failure_domain::Finding, node: u64) -> bool {
    use arca_lake_controller::failure_domain::Finding;
    match f {
        Finding::SharedDomain { nodes, .. } => nodes.contains(&node),
        Finding::MemberUnlabeled { node: n, .. } => *n == node,
        Finding::Unrecorded { .. } => false,
    }
}

/// Refuse an operator move of a member of `groups` when one of them has
/// several members and no recorded failure domain (ADR-0038): the leader
/// would refuse it anyway, and the reason is clearer here.
async fn domain_recorded<S: arca_lake_metadata::KvStore>(
    meta: &S,
    groups: &[u64],
) -> Result<(), String> {
    let domains = arca_lake_metadata::ops::group_domain::list(meta)
        .await
        .map_err(|e| e.to_string())?;
    let all = arca_lake_metadata::ops::cluster::list_groups(meta)
        .await
        .map_err(|e| e.to_string())?;
    let missing: Vec<u64> = all
        .iter()
        .filter(|g| groups.contains(&g.id) && g.members.len() > 1 && !domains.contains_key(&g.id))
        .map(|g| g.id)
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "groups {missing:?} have no recorded failure domain, and no member of theirs is moved until it is set: run `admin group set-failure-domain --group <id> --label <label>` with the label they were created with"
        ))
    }
}

/// `admin group` (ADR-0038).
async fn group_command<S: arca_lake_metadata::KvStore>(
    meta: &S,
    command: GroupCommand,
    now: u64,
) -> ExitCode {
    match command {
        GroupCommand::Check => {
            match arca_lake_controller::failure_domain::audit(meta, None, now).await {
                Ok(a) => {
                    println!("{}", a.enforcement());
                    for f in &a.findings {
                        println!("{f}");
                    }
                    if !a.at_risk.is_empty() {
                        println!("{}", a.at_risk_line());
                    }
                    println!(
                        "groups breaking their failure domain: {}; groups without a recorded failure domain: {} ({} with a member down or a repair under way)",
                        a.violating_groups(),
                        a.unrecorded_groups(),
                        a.at_risk.len()
                    );
                    if a.findings.is_empty() {
                        ExitCode::SUCCESS
                    } else {
                        ExitCode::from(1)
                    }
                }
                Err(e) => fail(e),
            }
        }
        GroupCommand::SetFailureDomain {
            group,
            all_unrecorded,
            label,
            change,
            force,
            dry_run,
        } => {
            set_failure_domain(
                meta,
                group,
                all_unrecorded,
                &label,
                (change, force, dry_run),
                now,
            )
            .await
        }
    }
}

/// `admin group set-failure-domain` (ADR-0038); `flags`: `--change`,
/// `--force`, `--dry-run`.
async fn set_failure_domain<S: arca_lake_metadata::KvStore>(
    meta: &S,
    group: Option<u64>,
    all_unrecorded: bool,
    label: &str,
    flags: (bool, bool, bool),
    now: u64,
) -> ExitCode {
    use arca_lake_metadata::ops::{cluster, group_domain};
    let (change, force, dry_run) = flags;
    let (groups, nodes, domains) = match (
        cluster::list_groups(meta).await,
        cluster::list_nodes(meta).await,
        group_domain::list(meta).await,
    ) {
        (Ok(g), Ok(n), Ok(d)) => (g, n, d),
        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => return fail(e),
    };
    let targets: Vec<u64> = if all_unrecorded {
        let t: Vec<u64> = groups
            .iter()
            .filter(|g| g.members.len() > 1 && !domains.contains_key(&g.id))
            .map(|g| g.id)
            .collect();
        println!("groups without a recorded failure domain: {t:?}");
        t
    } else {
        group.into_iter().collect()
    };
    if targets.is_empty() {
        println!("no group without a recorded failure domain");
        return ExitCode::SUCCESS;
    }
    // A label no member's node carries is most likely a typo: refused for
    // every group before anything is written.
    let unknown: Vec<u64> = groups
        .iter()
        .filter(|g| targets.contains(&g.id))
        .filter(|g| {
            !g.members.iter().any(|m| {
                nodes.iter().any(|n| {
                    n.id == m.node_id && group_domain::label_value(&n.labels, label).is_some()
                })
            })
        })
        .map(|g| g.id)
        .collect();
    if !unknown.is_empty() && !force {
        return fail(format!(
            "no member node of groups {unknown:?} carries the label '{label}' (a typo?): nothing recorded; `--force` records it anyway"
        ));
    }
    if dry_run {
        for g in &targets {
            println!(
                "group {g}: would record failure domain '{label}'{}",
                domains
                    .get(g)
                    .map_or_else(String::new, |d| format!(" (recorded: '{}')", d.label))
            );
        }
        return ExitCode::SUCCESS;
    }
    for g in &targets {
        match group_domain::set(meta, *g, label, change, now).await {
            Ok(group_domain::Set::Recorded { previous }) => println!(
                "group {g}: failure domain '{label}' recorded{}",
                previous.map_or_else(String::new, |p| format!(" (was '{p}')"))
            ),
            Ok(group_domain::Set::Unchanged) => {
                println!("group {g}: failure domain '{label}' already recorded");
            }
            Err(e) => return fail(e),
        }
    }
    // What the members look like under it (nothing is moved).
    let mut warned = !unknown.is_empty();
    for g in &unknown {
        println!("warning: group {g}: no member node carries the label '{label}' (--force)");
    }
    match arca_lake_controller::failure_domain::audit(meta, None, now).await {
        Ok(a) => {
            for f in a.findings.iter().filter(|f| targets.contains(&f.group())) {
                println!("warning: {f}");
                warned = true;
            }
        }
        Err(e) => {
            println!("warning: the members could not be checked: {e}");
            warned = true;
        }
    }
    if warned {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    }
}

/// `admin decommission-node` and `admin node decommission`.
async fn decommission_command<S: arca_lake_metadata::KvStore>(
    meta: &S,
    node: u64,
    yes: bool,
    cancel: bool,
    dry_run: bool,
    plan: &PlanArgs,
    now: u64,
) -> ExitCode {
    use arca_lake_metadata::ops::membership;
    if dry_run {
        return dry_run_plan(plan, async |clients| {
            arca_lake_controller::plan::plan_decommission(
                meta,
                clients,
                node,
                &plan.plan_failure_domain,
                1000,
            )
            .await
        })
        .await;
    }
    if cancel {
        return match membership::cancel_drain(meta, node).await {
            Ok(true) => {
                println!("node {node}: drain stopped (members already moved stay moved)");
                ExitCode::SUCCESS
            }
            Ok(false) => {
                println!("node {node} is not draining");
                ExitCode::SUCCESS
            }
            Err(e) => fail(e),
        };
    }
    if !yes {
        return fail("decommission-node moves every member off the node; re-run with --yes");
    }
    let member_of: Vec<u64> = match arca_lake_metadata::ops::cluster::list_groups(meta).await {
        Ok(gs) => gs
            .iter()
            .filter(|g| g.members.iter().any(|m| m.node_id == node))
            .map(|g| g.id)
            .collect(),
        Err(e) => return fail(e),
    };
    if let Err(e) = domain_recorded(meta, &member_of).await {
        return fail(e);
    }
    match membership::drain_node(meta, node, "admin", now).await {
        Ok(r) => {
            println!(
                "node {node}: {:?}; the controller leader replaces its members (admin status shows progress)",
                r.state
            );
            ExitCode::SUCCESS
        }
        Err(e) => fail(e),
    }
}

/// `admin node` subcommands (Phase 3.11, R-100).
#[derive(Debug, Subcommand)]
enum NodeCommand {
    /// Every registered node with its lifecycle state.
    List,
    /// One node: state, identity, groups, and what it reports.
    Status {
        /// Node id.
        #[arg(long)]
        node: u64,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// Move every member off the node; it becomes decommissioned once no
    /// group lists it.
    #[command(alias = "drain")]
    Decommission {
        /// Node id.
        #[arg(long)]
        node: u64,
        /// Confirm.
        #[arg(long)]
        yes: bool,
        /// Stop draining.
        #[arg(long)]
        cancel: bool,
        /// Show every move without writing anything.
        #[arg(long, conflicts_with_all = ["yes", "cancel"])]
        dry_run: bool,
        #[command(flatten)]
        plan: PlanArgs,
    },
    /// Take a decommissioned (or quarantined) node out for good: its
    /// delete fence is closed with a hold on the node first; its id is
    /// never reused; only `rejoin` brings it back. Idempotent.
    Unregister {
        /// Node id.
        #[arg(long)]
        node: u64,
        /// Why (recorded).
        #[arg(long)]
        reason: String,
        /// The node does not answer and its disk will never be started
        /// again unwiped.
        #[arg(long)]
        unreachable: bool,
        /// Confirm.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// Mark a node removed again right after a manual restore of metadata
    /// from before it was unregistered (with `admin restore` this is done
    /// for you). No state or membership check: its members are repaired
    /// away by the leader.
    KeepRemoved {
        /// Node id.
        #[arg(long)]
        node: u64,
        /// The node does not answer and its disk will never be started
        /// again unwiped.
        #[arg(long)]
        unreachable: bool,
        /// Confirm.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// Confirm that the identity the node holds is this cluster's (after
    /// metadata restored from before the node had one, nothing is pushed to
    /// it until then).
    AdoptIdentity {
        /// Node id.
        #[arg(long)]
        node: u64,
        /// Confirm: the disk at the node's address is that node's.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
    /// Bring a removed, quarantined or decommissioned node back, without
    /// members, after checking it (release, segment format, incarnation,
    /// schema floor, identity).
    Rejoin {
        /// Node id.
        #[arg(long)]
        node: u64,
        /// The node's disk was emptied: give it a new identity (refused if
        /// it holds shards).
        #[arg(long)]
        fresh_identity: bool,
        /// Only run the checks.
        #[arg(long, conflicts_with = "yes")]
        check: bool,
        /// Confirm.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        rpc: NodeRpcArgs,
    },
}

#[allow(clippy::too_many_lines)]
async fn node_command<S: arca_lake_metadata::KvStore>(
    meta: &S,
    command: NodeCommand,
    now: u64,
) -> ExitCode {
    use arca_lake_metadata::ops::{cluster, node_lifecycle};
    match command {
        NodeCommand::List => {
            let (nodes, states, groups) = match (
                cluster::list_nodes(meta).await,
                node_lifecycle::states(meta).await,
                cluster::list_groups(meta).await,
            ) {
                (Ok(n), Ok(s), Ok(g)) => (n, s, g),
                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => return fail(e),
            };
            println!(
                "{:<6} {:<28} {:<15} {:>7} {:>12}",
                "id", "address", "state", "groups", "seen_ago_s"
            );
            for n in nodes {
                let state = states
                    .get(&n.id)
                    .copied()
                    .unwrap_or(node_lifecycle::State::Active);
                let member = groups
                    .iter()
                    .filter(|g| g.members.iter().any(|m| m.node_id == n.id))
                    .count();
                println!(
                    "{:<6} {:<28} {:<15} {:>7} {:>12}",
                    n.id,
                    n.address,
                    state.name(),
                    member,
                    now.saturating_sub(n.last_seen_ms) / 1000
                );
            }
            ExitCode::SUCCESS
        }
        NodeCommand::Status { node, rpc } => {
            let Some(n) = cluster::list_nodes(meta)
                .await
                .ok()
                .and_then(|ns| ns.into_iter().find(|n| n.id == node))
            else {
                return fail(format!("node {node} unknown"));
            };
            let state = match node_lifecycle::state(meta, node).await {
                Ok(s) => s,
                Err(e) => return fail(e),
            };
            println!("node {node} at {}: {}", n.address, state.name());
            if let Ok(Some(r)) = node_lifecycle::retirements(meta)
                .await
                .map(|rs| rs.into_iter().find(|r| r.node_id == node))
            {
                println!("  {:?}", r.state);
            }
            if let Ok(Some(i)) = node_lifecycle::identity(meta, node).await {
                println!("  identity given (acknowledged: {})", i.acknowledged);
            }
            let clients = match admin_node_clients(&rpc) {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            clients.note_node_id(&n.address, n.id);
            match clients.health(&n.address).await {
                Ok(h) => println!(
                    "  answers: {} protocol {} format {}..{} schema floor {} incarnation seq {} live shards {} identity {}",
                    h.software,
                    h.protocol_max,
                    h.format_min,
                    h.format_max,
                    h.schema_floor,
                    h.incarnation_seq,
                    h.live_shards,
                    if !h.identity_supported {
                        "not reported (earlier release)".to_string()
                    } else if h.identity_node_id == 0 {
                        "none".to_string()
                    } else {
                        format!("node {}", h.identity_node_id)
                    }
                ),
                Err(e) => println!("  does not answer: {}", e.code()),
            }
            ExitCode::SUCCESS
        }
        NodeCommand::Decommission {
            node,
            yes,
            cancel,
            dry_run,
            plan,
        } => decommission_command(meta, node, yes, cancel, dry_run, &plan, now).await,
        NodeCommand::Unregister {
            node,
            reason,
            unreachable,
            yes,
            rpc,
        } => {
            if !yes {
                return fail(
                    "unregister takes the node out for good (only `admin node rejoin` brings it back); re-run with --yes",
                );
            }
            let clients = match admin_node_clients(&rpc) {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            match arca_lake_controller::node_ops::unregister(
                meta,
                &clients,
                node,
                "admin",
                &reason,
                unreachable,
                now,
            )
            .await
            {
                Ok(u) if u.already => {
                    println!("node {node} is already removed");
                    ExitCode::SUCCESS
                }
                Ok(u) => {
                    println!(
                        "node {node} removed{}",
                        if u.fenced {
                            "; its delete fence is closed and held"
                        } else {
                            " while unreachable: its disk must never be started again unwiped"
                        }
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        NodeCommand::KeepRemoved {
            node,
            unreachable,
            yes,
            rpc,
        } => {
            if !yes {
                return fail("keep-removed takes the node out again; re-run with --yes");
            }
            let clients = match admin_node_clients(&rpc) {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            match arca_lake_controller::node_ops::keep_removed(
                meta,
                &clients,
                node,
                unreachable,
                now,
            )
            .await
            {
                Ok(fenced) => {
                    println!(
                        "node {node} removed again{}; start the controllers now",
                        if fenced {
                            ", its delete fence closed and held"
                        } else {
                            " while unreachable: its disk must never be started again unwiped"
                        }
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        NodeCommand::AdoptIdentity { node, yes, rpc } => {
            if !yes {
                return fail(
                    "adopt-identity takes the disk at the node's address as that node's; re-run with --yes",
                );
            }
            let clients = match admin_node_clients(&rpc) {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            match arca_lake_controller::node_ops::adopt_identity(meta, &clients, node).await {
                Ok(_) => {
                    println!(
                        "node {node}: its identity is adopted; the leader pushes it again from its next pass"
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        NodeCommand::Rejoin {
            node,
            fresh_identity,
            check,
            yes,
            rpc,
        } => {
            let clients = match admin_node_clients(&rpc) {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            if check || !yes {
                return match arca_lake_controller::node_ops::check_rejoin(
                    meta,
                    &clients,
                    node,
                    fresh_identity,
                )
                .await
                {
                    Ok(c) if c.problems.is_empty() => {
                        println!(
                            "node {node} may rejoin{}{}",
                            if c.fresh_identity {
                                " with a new identity"
                            } else {
                                ""
                            },
                            if check { "" } else { "; re-run with --yes" }
                        );
                        if check {
                            ExitCode::SUCCESS
                        } else {
                            ExitCode::from(1)
                        }
                    }
                    Ok(c) => {
                        for p in &c.problems {
                            eprintln!("refused: {p}");
                        }
                        ExitCode::from(1)
                    }
                    Err(e) => fail(e),
                };
            }
            match arca_lake_controller::node_ops::rejoin(meta, &clients, node, fresh_identity).await
            {
                Ok((was, c)) => {
                    println!(
                        "node {node} rejoined (was {}){}: active without members; reconciliation collects its former shards",
                        was.name(),
                        if c.fresh_identity {
                            " with a new identity"
                        } else {
                            ""
                        }
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
    }
}

async fn delete_fence_command<S: arca_lake_metadata::KvStore>(
    meta: &S,
    command: DeleteFenceCommand,
    now: u64,
) -> ExitCode {
    use arca_lake_metadata::ops::delete_fence;
    match command {
        DeleteFenceCommand::Status { rpc } => {
            let rec = match delete_fence::current(meta).await {
                Ok(r) => r,
                Err(e) => return fail(e),
            };
            println!(
                "record generation {} {}{} leader_token {} changed_ms {} reason {}",
                rec.generation,
                if rec.open { "open" } else { "closed" },
                if rec.operator_hold {
                    " operator_hold"
                } else {
                    ""
                },
                rec.leader_token,
                rec.changed_ms,
                rec.reason
            );
            let clients = match admin_node_clients(&rpc) {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            match arca_lake_controller::delete_fence::node_fences(meta, &clients).await {
                Ok(nodes) => {
                    print_node_fences(&nodes);
                    let behind = nodes
                        .iter()
                        .filter(|n| n.fence != Ok((rec.generation, rec.open)))
                        .count();
                    println!(
                        "nodes {} at the record's generation and state {} other {}",
                        nodes.len(),
                        nodes.len() - behind,
                        behind
                    );
                    if !rec.open {
                        println!(
                            "deletes stopped: {}",
                            if rec.operator_hold {
                                "operator hold (admin delete-fence release, or recovery finalize)"
                            } else {
                                "the cluster is not Active, or the leader has not opened a generation yet"
                            }
                        );
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        DeleteFenceCommand::Close {
            reason,
            yes,
            absent_node,
            rpc,
        } => {
            if !yes {
                return fail(
                    "closing the delete fence stops every delete, GC and reconciliation; re-run with --yes",
                );
            }
            match close_fence_everywhere(meta, &rpc, &reason, true, &absent_node, now).await {
                Ok(_) => ExitCode::SUCCESS,
                Err(e) => fail(e),
            }
        }
        DeleteFenceCommand::Release { reason, rpc } => {
            let clients = match admin_node_clients(&rpc) {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            match arca_lake_controller::delete_fence::release_and_push(meta, &clients, &reason, now)
                .await
            {
                Ok((r, held)) => {
                    println!(
                        "operator hold released (generation {} closed): the leader opens a new generation at its next health pass",
                        r.generation
                    );
                    if held.is_empty() {
                        ExitCode::SUCCESS
                    } else {
                        fail(format!(
                            "nodes {held:?} still hold it (unreachable?): they refuse deletes until a release reaches them; re-run"
                        ))
                    }
                }
                Err(e) => fail(e),
            }
        }
    }
}

/// `admin rebalance` subcommands.
#[derive(Debug, Subcommand)]
enum RebalanceCommand {
    /// Print the state: switches, the move in progress, totals.
    Status,
    /// Stop moving (a pass in progress finishes its current chunk).
    Pause,
    /// Move again.
    Resume,
    /// Plan and report only (`on`), or move (`off`).
    DryRun {
        /// `on` or `off`.
        #[arg(value_parser = ["on", "off"])]
        mode: String,
    },
}

/// The access-record age bound a gateway uses: at most the flush delay the
/// tiering grace allows for (ADR-0035 §10.4); 0 (no bound) and larger
/// values are lowered to it, with a warning.
fn access_max_age_ms(asked: u64) -> u64 {
    let limit = arca_lake_metadata::ops::tiering::FLUSH_DELAY_MS;
    if asked == 0 || asked > limit {
        tracing::warn!(
            asked,
            limit,
            "--access-max-age-ms above the tiering grace's flush bound: lowered"
        );
        limit
    } else {
        asked
    }
}

/// A tiering policy on the command line (every field, or none).
#[derive(Debug, clap::Args)]
struct TieringPolicyArgs {
    /// Durable tiers from lowest (coldest) to highest (hottest); repeat.
    #[arg(long = "tier")]
    tiers: Vec<String>,
    /// Promote a generation read at least this many times...
    #[arg(long)]
    promote_reads: Option<u64>,
    /// ...within this window, seconds.
    #[arg(long)]
    promote_window_secs: Option<u64>,
    /// Demote a generation not read for this long, seconds.
    #[arg(long)]
    demote_after_secs: Option<u64>,
}

impl TieringPolicyArgs {
    /// `None` when no field is given; an error when only some are.
    fn policy(
        &self,
    ) -> Result<Option<arca_lake_metadata::ops::tiering::TieringPolicyRecord>, String> {
        match (
            self.tiers.is_empty(),
            self.promote_reads,
            self.promote_window_secs,
            self.demote_after_secs,
        ) {
            (true, None, None, None) => Ok(None),
            (false, Some(r), Some(w), Some(d)) => Ok(Some(
                arca_lake_metadata::ops::tiering::TieringPolicyRecord {
                    tiers: self.tiers.clone(),
                    promote_reads: r,
                    promote_window_ms: w.saturating_mul(1000),
                    demote_after_ms: d.saturating_mul(1000),
                },
            )),
            _ => Err(
                "give every policy field (--tier ... --promote-reads --promote-window-secs \
                 --demote-after-secs) or none"
                    .into(),
            ),
        }
    }
}

#[derive(Debug, Subcommand)]
enum TieringCommand {
    /// Print the record: enabled, policy, generation, acknowledgment,
    /// grace and promotion cutoff.
    Status,
    /// Enable tiering (with this policy, or the record's).
    Enable {
        #[command(flatten)]
        policy: TieringPolicyArgs,
    },
    /// Disable tiering: gateways stop recording reads once the leader has
    /// acknowledged it. Refused until `admin upgrade finalize` wrote the
    /// schema every process of which reads the record.
    Disable,
    /// Replace the policy (enabled or not).
    Set {
        #[command(flatten)]
        policy: TieringPolicyArgs,
    },
    /// Print, for the current objects of a bucket, the tier of each
    /// object's chunks and its access record (read-only).
    Objects {
        /// Bucket.
        #[arg(long)]
        bucket: String,
        /// Only keys starting with this prefix.
        #[arg(long, default_value = "")]
        prefix: String,
        /// Objects at most.
        #[arg(long, default_value_t = 1000)]
        max: usize,
    },
}

/// `admin tiering objects`: one line per object (its tiers, its access
/// record) and a total.
async fn tiering_objects<S: arca_lake_metadata::KvStore>(
    meta: &S,
    bucket: &str,
    prefix: &str,
    max: usize,
) -> Result<(), arca_lake_metadata::MetaError> {
    use arca_lake_metadata::ops::{cluster, list, object};
    let tier_of: std::collections::HashMap<u64, String> = cluster::list_groups(meta)
        .await?
        .into_iter()
        .map(|g| (g.id, g.tier))
        .collect();
    let mut req = list::ListRequest {
        prefix: prefix.to_string(),
        max_keys: max.clamp(1, 1000),
        ..list::ListRequest::default()
    };
    let (mut objects, mut count_sum) = (0usize, 0u64);
    let mut by_tier: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    'pages: loop {
        let page = list::list_objects(meta, bucket, &req).await?;
        for e in &page.objects {
            if objects >= max {
                break 'pages;
            }
            let Some(m) = object::read_manifest(meta, bucket, &e.key, None).await? else {
                continue;
            };
            let mut tiers: Vec<String> = m
                .chunks
                .iter()
                .map(|(_, c)| {
                    tier_of
                        .get(&c.placement.group_id)
                        .cloned()
                        .unwrap_or_else(|| format!("group-{}", c.placement.group_id))
                })
                .collect();
            tiers.sort();
            tiers.dedup();
            let access = object::access_of(meta, &m.pointer.generation).await?;
            count_sum += access.as_ref().map_or(0, |a| a.count);
            *by_tier.entry(tiers.join("+")).or_default() += 1;
            objects += 1;
            println!(
                "object {} generation {} tiers {} access_count {} access_last_ms {}",
                e.key,
                arca_lake_metadata::ops::compat::hex(&m.pointer.generation),
                tiers.join("+"),
                access
                    .as_ref()
                    .map_or_else(|| "-".into(), |a| a.count.to_string()),
                access
                    .as_ref()
                    .map_or_else(|| "-".into(), |a| a.last_ms.to_string()),
            );
        }
        match page.next_continuation {
            Some(c) if page.is_truncated => req.continuation = Some(c),
            _ => break,
        }
    }
    println!("total objects {objects} access_count_sum {count_sum} by_tier {by_tier:?}");
    Ok(())
}

async fn tiering_command<S: arca_lake_metadata::KvStore>(
    meta: &S,
    command: TieringCommand,
) -> ExitCode {
    use arca_lake_metadata::ops::tiering::{self, AdminChange};
    let now_ms = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| u64::try_from(d.as_millis()).ok())
            .unwrap_or(0)
    };
    // Acknowledged means by the current leader (a failover or a restore
    // leaves an earlier acknowledgment behind).
    let leader_term = match arca_lake_metadata::ops::leader::current(meta).await {
        Ok(l) => l.map_or(0, |l| l.token),
        Err(e) => return fail(e),
    };
    let r = match command {
        TieringCommand::Status => tiering::current(meta).await,
        TieringCommand::Objects {
            bucket,
            prefix,
            max,
        } => {
            return match tiering_objects(meta, &bucket, &prefix, max).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => fail(e),
            };
        }
        TieringCommand::Enable { policy } => match policy.policy() {
            Ok(p) => tiering::admin_change(meta, &AdminChange::Enable(p), now_ms())
                .await
                .map(Some),
            Err(e) => return fail(e),
        },
        TieringCommand::Disable => tiering::admin_change(meta, &AdminChange::Disable, now_ms())
            .await
            .map(Some),
        TieringCommand::Set { policy } => match policy.policy() {
            Ok(Some(p)) => tiering::admin_change(meta, &AdminChange::SetPolicy(p), now_ms())
                .await
                .map(Some),
            Ok(None) => return fail("give the policy to set"),
            Err(e) => return fail(e),
        },
    };
    match r {
        Ok(None) => {
            println!(
                "no tiering record (gateways record reads; a controller with --tiering-* flags seeds it)"
            );
            ExitCode::SUCCESS
        }
        Ok(Some(t)) => {
            println!(
                "enabled {} generation {} acknowledged {} (last by term {} generation {}; leader term {}) tiers {:?} promote_reads {} promote_window_s {} demote_after_s {} grace_from_ms {} promotion_from_ms {} changed_ms {}",
                t.enabled,
                t.generation,
                t.acknowledged_by(leader_term),
                t.applied_term,
                t.applied_generation,
                leader_term,
                t.policy.tiers,
                t.policy.promote_reads,
                t.policy.promote_window_ms / 1000,
                t.policy.demote_after_ms / 1000,
                t.grace_from_ms,
                t.promotion_from_ms,
                t.changed_ms
            );
            if !t.acknowledged_by(leader_term) {
                println!(
                    "not applied yet: the controller leader applies a change when it acknowledges it (`acknowledged true`)"
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => fail(e),
    }
}

async fn rebalance_command<S: arca_lake_metadata::KvStore>(
    meta: &S,
    command: RebalanceCommand,
) -> ExitCode {
    use arca_lake_metadata::ops::rebalance;
    let r = match command {
        RebalanceCommand::Status => rebalance::state(meta).await,
        RebalanceCommand::Pause => rebalance::set_switches(meta, Some(true), None).await,
        RebalanceCommand::Resume => rebalance::set_switches(meta, Some(false), None).await,
        RebalanceCommand::DryRun { mode } => {
            rebalance::set_switches(meta, None, Some(mode == "on")).await
        }
    };
    match r {
        Ok(s) => {
            println!(
                "paused {} dry_run {} moved_chunks {} moved_bytes {} skipped_chunks {} last_pass_ms {} last_reason {}",
                s.paused,
                s.dry_run,
                s.moved_chunks,
                s.moved_bytes,
                s.skipped_chunks,
                s.last_pass_ms,
                s.last_reason
            );
            match &s.plan {
                Some(p) => println!(
                    "plan tier {} source {} (room {}) target {} (room {})",
                    p.tier, p.source, p.source_room, p.target, p.target_room
                ),
                None => println!("plan none"),
            }
            println!(
                "cursor {}",
                s.cursor.map_or_else(
                    || "start".to_string(),
                    |c| arca_lake_metadata::ops::compat::hex(&c)
                )
            );
            ExitCode::SUCCESS
        }
        Err(e) => fail(e),
    }
}

#[allow(clippy::cast_precision_loss)]
async fn check_config<S: arca_lake_metadata::KvStore>(meta: &S) -> ExitCode {
    let rec = match arca_lake_metadata::ops::admission::current(meta).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            println!("FAIL no admission record: no controller leader has evaluated capacity");
            return ExitCode::from(1);
        }
        Err(e) => return fail(e),
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    let age = now.saturating_sub(rec.computed_ms);
    let mut bad = false;
    println!(
        "record leader_token {} generation {} age_ms {} ttl_ms {} gateways {}{}",
        rec.leader_token,
        rec.generation,
        age,
        rec.ttl_ms,
        rec.gateways,
        if rec.warming_up { " warming_up" } else { "" }
    );
    if age > rec.ttl_ms {
        println!("FAIL the record is older than its TTL: no leader is publishing");
        bad = true;
    }
    if !rec.capacity_conflicts.is_empty() {
        println!(
            "FAIL nodes {:?} share a file system without --capacity-bytes allotments that fit it: per-node capacity admission cannot keep their reserves (give each node its own file system or device, or allotments summing to at most its size)",
            rec.capacity_conflicts
        );
        bad = true;
    }
    if !rec.capacity_unverified.is_empty() {
        println!(
            "FAIL nodes {:?}: file system sharing cannot be checked (overlay, FUSE, network, or not reported) and they have no --capacity-bytes allotment: their groups leave placement unless --allow-shared-filesystem",
            rec.capacity_unverified
        );
        bad = true;
    }
    for t in &rec.tiers {
        println!(
            "tier {} state {} free_permille {} excluded_groups {:?} reason {}",
            t.tier, t.state, t.free_permille, t.excluded_groups, t.reason
        );
        let rooms: Vec<u64> = t.group_room.iter().map(|g| g.room).collect();
        if let (Some(min), Some(max)) = (rooms.iter().min(), rooms.iter().max()) {
            println!(
                "tier {} group_room min {} max {} (imbalance {:.2})",
                t.tier,
                min,
                max,
                if *max == 0 {
                    0.0
                } else {
                    1.0 - *min as f64 / *max as f64
                }
            );
        }
        for g in &t.group_room {
            println!("  group {} room {}", g.group, g.room);
        }
    }
    if bad {
        ExitCode::from(1)
    } else {
        println!("OK");
        ExitCode::SUCCESS
    }
}

fn print_gate(g: &arca_lake_metadata::ops::compat::Gate) {
    println!(
        "schema {:?} schema_write {:?} format_write {:?} has_data {}",
        g.schema, g.schema_write, g.format_write, g.has_data
    );
    match &g.cluster {
        Some(c) => println!(
            "cluster {} incarnation {} changed_ms {} by {} restored_at_ms {:?} restored_from {:?}",
            c.state.name(),
            arca_lake_metadata::ops::compat::hex(&c.incarnation),
            c.changed_ms,
            c.changed_by,
            c.restored_at_ms,
            c.restored_from
        ),
        None => println!("cluster: not initialised"),
    }
}

#[allow(clippy::too_many_lines)]
async fn run_admin_upgrade<S: arca_lake_metadata::KvStore + 'static>(
    meta: Arc<S>,
    command: UpgradeCommand,
    now: u64,
) -> ExitCode {
    use arca_lake_metadata::ops::compat;
    match command {
        UpgradeCommand::Status => {
            match (
                compat::gate(meta.as_ref()).await,
                compat::upgrade_cursor(meta.as_ref()).await,
            ) {
                (Ok(g), Ok(cur)) => {
                    print_gate(&g);
                    match cur {
                        Some(c) => println!(
                            "upgrade target {:?} step {:?} started_ms {} updated_ms {} by {} token {} last_error {:?}\n  history: {}",
                            c.target,
                            c.step,
                            c.started_ms,
                            c.updated_ms,
                            c.requested_by,
                            c.by_token,
                            c.last_error,
                            c.history.join(" | ")
                        ),
                        None => println!("upgrade: none in progress"),
                    }
                    if let Ok(audits) =
                        arca_lake_metadata::ops::recovery::upgrade_audits(meta.as_ref()).await
                    {
                        for a in audits {
                            println!(
                                "finalized {:?} at {} ({} steps)",
                                a.target,
                                a.updated_ms,
                                a.history.len()
                            );
                        }
                    }
                    ExitCode::SUCCESS
                }
                (g, c) => fail(
                    g.err()
                        .or(c.err())
                        .map_or_else(|| "unknown".to_string(), |e| e.to_string()),
                ),
            }
        }
        UpgradeCommand::Finalize {
            ignore_absent,
            wait,
            timeout_secs,
        } => {
            let target = arca_lake_common::compat::Target {
                protocol: arca_lake_protocol::storage::v1::PROTOCOL_VERSION,
                schema: arca_lake_metadata::keys::SCHEMA_VERSION,
                format: u64::from(arca_lake_storage::format::SEGMENT_FORMAT_VERSION),
            };
            let cur =
                match compat::request_finalize(meta.as_ref(), target, ignore_absent, "admin", now)
                    .await
                {
                    Ok(c) => c,
                    Err(e) => return fail(e),
                };
            println!(
                "finalize to {:?} requested (step {:?}); the controller leader executes it",
                cur.target, cur.step
            );
            if !wait {
                return ExitCode::SUCCESS;
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                match compat::upgrade_cursor(meta.as_ref()).await {
                    Ok(None) => {
                        println!("finalized");
                        return ExitCode::SUCCESS;
                    }
                    Ok(Some(c)) => {
                        if let Some(e) = &c.last_error {
                            eprintln!("step {:?}: {e}", c.step);
                        }
                        if std::time::Instant::now() > deadline {
                            return fail(format!("timed out at step {:?}", c.step));
                        }
                    }
                    Err(e) => return fail(e),
                }
            }
        }
        UpgradeCommand::Cancel => match compat::clear_upgrade(meta.as_ref(), true).await {
            Ok(Some(c)) => {
                println!("cancelled upgrade to {:?} at step {:?}", c.target, c.step);
                ExitCode::SUCCESS
            }
            Ok(None) => {
                println!("no upgrade in progress");
                ExitCode::SUCCESS
            }
            Err(e) => fail(e),
        },
        UpgradeCommand::CheckDowngrade { schema } => match compat::gate(meta.as_ref()).await {
            Ok(g) => {
                let stored = g.schema.unwrap_or(0);
                let write = g.schema_write.unwrap_or(0);
                let ok = stored <= schema && write <= schema;
                println!(
                    "schema {stored} schema_write {write}; a build reading up to M{schema} {}",
                    if ok {
                        "can run on this store"
                    } else {
                        "cannot run on this store (past the cut-off)"
                    }
                );
                if ok {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::from(2)
                }
            }
            Err(e) => fail(e),
        },
    }
}

#[allow(clippy::too_many_lines)]
async fn run_admin_recovery<S: arca_lake_metadata::KvStore + 'static>(
    meta: Arc<S>,
    command: RecoveryCommand,
    now: u64,
) -> ExitCode {
    use arca_lake_metadata::ops::{compat, recovery};
    use arca_lake_metadata::records::ClusterPhase;
    let state = match compat::cluster_state(meta.as_ref()).await {
        Ok(Some(s)) => s,
        Ok(None) => return fail("cluster state record missing; run `admin init`"),
        Err(e) => return fail(e),
    };
    match command {
        RecoveryCommand::Status => {
            println!(
                "cluster {} incarnation {} changed_ms {} by {} restored_at_ms {:?} restored_from {:?} backup_point_ms {:?}",
                state.state.name(),
                compat::hex(&state.incarnation),
                state.changed_ms,
                state.changed_by,
                state.restored_at_ms,
                state.restored_from,
                state.backup_point_ms
            );
            if let Ok(Some(c)) = recovery::cursor(meta.as_ref()).await {
                println!(
                    "cursor incarnation {} phase {:?} restore_point_ms {} unanswered {:?} excluded {:?} updated_ms {}",
                    compat::hex(&c.incarnation),
                    c.phase,
                    c.restore_point_ms,
                    c.unanswered_nodes,
                    c.excluded_nodes,
                    c.updated_ms
                );
            }
            if let Ok(Some(r)) = recovery::report(meta.as_ref(), &state.incarnation).await {
                println!(
                    "report complete {} finished_ms {} unanswered {:?} accepted {:?}\n  counts {:?}",
                    r.complete, r.finished_ms, r.unanswered_nodes, r.accepted, r.counts
                );
                for a in &r.affected {
                    println!(
                        "  affected {} bucket {} key {} generation {} chunk {} group {} have {} need {}",
                        a.why,
                        a.bucket,
                        a.key,
                        compat::hex(&a.generation),
                        a.chunk_index,
                        a.group,
                        a.have,
                        a.need
                    );
                }
            }
            ExitCode::SUCCESS
        }
        RecoveryCommand::Begin {
            from,
            backup_point_ms,
            yes,
            force,
            rpc,
            absent_node,
        } => {
            if !yes {
                return fail(
                    "recovery begin fences out every running process and starts a restore; re-run with --yes",
                );
            }
            if !force && !matches!(state.state, ClusterPhase::Active | ClusterPhase::ReadOnly) {
                return fail(format!(
                    "cluster is {}; use --force to mark again",
                    state.state.name()
                ));
            }
            if backup_point_ms.is_none() {
                eprintln!(
                    "warning: no --backup-point-ms; orphans cannot be split into pre- and post-backup in the report"
                );
            }
            // No delete decided on the metadata before the restore may run
            // on the nodes after it (R-88, ADR-0023): the fence is closed
            // on every node, each acknowledging after the deletes past its
            // old fence have finished, before the restored metadata is
            // marked; the hold stays until finalize.
            match close_fence_everywhere(
                meta.as_ref(),
                &rpc,
                "recovery begin",
                true,
                &absent_node,
                now,
            )
            .await
            {
                Ok(closed) => {
                    if !closed.were_open.is_empty() {
                        eprintln!(
                            "warning: nodes {:?} still had an open delete fence: the fence was not closed before the restore (admin delete-fence close); deletes decided on the previous metadata may have run during it, and verification will report any shard they removed",
                            closed.were_open
                        );
                    }
                }
                Err(e) => return fail(format!("recovery begin refused: {e}")),
            }
            // The new incarnation's sequence goes above every node's (the
            // restored counter went back with the restore; R-92): nodes
            // order incarnations by it, never by a clock.
            let at_least = match admin_node_clients(&rpc) {
                Ok(clients) => match arca_lake_controller::delete_fence::restore_floors(
                    meta.as_ref(),
                    &clients,
                    &absent_node,
                )
                .await
                {
                    Ok(seq) => seq,
                    Err(e) => return fail(format!("recovery begin refused: {e}")),
                },
                Err(e) => return fail(e),
            };
            match compat::mark_restoring_above(
                meta.as_ref(),
                from,
                backup_point_ms,
                "admin",
                now,
                at_least,
            )
            .await
            {
                Ok(s) => {
                    println!(
                        "cluster Restoring, incarnation {} (restore point {}): start the controllers; writes after the backup point are lost (RPO) and objects whose shards are gone are reported, not recovered",
                        compat::hex(&s.incarnation),
                        now
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        RecoveryCommand::Verify { yes } => {
            if !yes {
                return fail("verify leaves ReadOnly for VerificationPending; re-run with --yes");
            }
            match recovery::restart_verification(
                meta.as_ref(),
                state.incarnation,
                true,
                "admin",
                now,
            )
            .await
            {
                Ok(Ok(())) => {
                    println!("VerificationPending: the controller leader verifies from the start");
                    ExitCode::SUCCESS
                }
                Ok(Err(why)) => fail(why),
                Err(e) => fail(e),
            }
        }
        RecoveryCommand::Resume => {
            match recovery::restart_verification(
                meta.as_ref(),
                state.incarnation,
                false,
                "admin",
                now,
            )
            .await
            {
                Ok(Ok(())) => {
                    println!(
                        "VerificationPending: the controller leader continues from the cursor"
                    );
                    ExitCode::SUCCESS
                }
                Ok(Err(why)) => fail(why),
                Err(e) => fail(e),
            }
        }
        RecoveryCommand::Finalize {
            yes,
            force,
            operator,
            reason,
        } => {
            if !yes {
                return fail("finalize activates the cluster; re-run with --yes");
            }
            let req = recovery::FinalizeRequest {
                incarnation: state.incarnation,
                operator,
                reason,
                force,
                now_ms: now,
            };
            let (verdict, _) = match recovery::finalize(meta.as_ref(), &req).await {
                Ok(v) => v,
                Err(e) => return fail(e),
            };
            for c in &verdict.critical {
                println!("unresolved: {c}");
            }
            for w in &verdict.warnings {
                println!("note: {w}");
            }
            if let Some(why) = verdict.refusal {
                return fail(format!("finalize refused (audited): {why}"));
            }
            println!(
                "cluster Active{}",
                if verdict.forced {
                    " (forced: the findings above stay in the report and in `recovery losses`)"
                } else {
                    ""
                }
            );
            ExitCode::SUCCESS
        }
        RecoveryCommand::Losses => {
            let (losses, audits) = match (
                recovery::data_losses(meta.as_ref()).await,
                recovery::finalize_audits(meta.as_ref()).await,
            ) {
                (Ok(l), Ok(a)) => (l, a),
                (Err(e), _) | (_, Err(e)) => return fail(e),
            };
            for l in &losses {
                println!(
                    "loss bucket {} key {} generation {} chunk {} group {} have {}/{} why {} accepted_ms {} by {} incarnation {}",
                    l.object.bucket,
                    l.object.key,
                    compat::hex(&l.object.generation),
                    l.object.chunk_index,
                    l.object.group,
                    l.object.have,
                    l.object.need,
                    l.object.why,
                    l.accepted_ms,
                    l.accepted_by,
                    compat::hex(&l.incarnation)
                );
            }
            for a in &audits {
                println!(
                    "finalize at_ms {} incarnation {} operator {} force {} phase_before {} result {:?} reason {:?} critical {:?} warnings {:?} affected {}/{}",
                    a.at_ms,
                    compat::hex(&a.incarnation),
                    a.operator,
                    a.force,
                    a.phase_before,
                    a.result,
                    a.reason,
                    a.critical,
                    a.warnings,
                    a.affected_listed,
                    a.affected_total
                );
            }
            ExitCode::SUCCESS
        }
    }
}

fn fdb_escaped(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter().fold(String::new(), |mut s, c| {
        if c.is_ascii_alphanumeric() {
            s.push(char::from(*c));
        } else {
            let _ = write!(s, "\\x{c:02x}");
        }
        s
    })
}

/// Text-file metrics of a backup verification run (registry process
/// `backup`).
#[allow(clippy::cast_precision_loss)]
fn render_backup_metrics(
    r: &arca_lake_controller::backup::RestoredReport,
    now_ms: u64,
    verify_seconds: f64,
    restore_seconds: Option<f64>,
    backup_end_ms: Option<u64>,
) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let ok = r.findings_total == 0;
    let now = now_ms as f64 / 1000.0;
    let mut g = |name: &str, v: f64| {
        let _ = writeln!(out, "# TYPE {name} gauge\n{name} {v}");
    };
    g("arca_backup_verify_last_run_timestamp_seconds", now);
    g("arca_backup_verify_success", if ok { 1.0 } else { 0.0 });
    if ok {
        g("arca_backup_verify_last_success_timestamp_seconds", now);
    }
    g("arca_backup_verify_duration_seconds", verify_seconds);
    g(
        "arca_backup_restore_duration_seconds",
        restore_seconds.unwrap_or(0.0),
    );
    g(
        "arca_backup_age_seconds",
        backup_end_ms.map_or(0.0, |e| now_ms.saturating_sub(e) as f64 / 1000.0),
    );
    g("arca_backup_verified_bytes", r.bytes as f64);
    g("arca_backup_verified_versions", r.versions as f64);
    g("arca_backup_unresolved_findings", r.findings_total as f64);
    arca_lake_common::metrics::annotate(&out)
}

async fn run_admin_backup<S: arca_lake_metadata::KvStore>(
    meta: &S,
    command: BackupCommand,
    now: u64,
) -> ExitCode {
    match command {
        BackupCommand::KeyRange => {
            let r = arca_lake_metadata::keys::all_range();
            println!("{} {}", fdb_escaped(&r.begin), fdb_escaped(&r.end));
            ExitCode::SUCCESS
        }
        BackupCommand::VerifyRestored {
            metrics_file,
            backup_end_ms,
            restore_seconds,
        } => {
            let started = std::time::Instant::now();
            let r = match arca_lake_controller::backup::verify_restored(meta).await {
                Ok(r) => r,
                Err(e) => return fail(e),
            };
            let secs = started.elapsed().as_secs_f64();
            println!(
                "restored metadata: schema {:?} format_write {:?} cluster {:?} groups {} nodes {} buckets {} versions {} delete_markers {} generations committed {} pending {} retiring {} bytes {} multipart roots {} parts {} findings {} ({:.1} s)",
                r.schema,
                r.format_write,
                r.cluster,
                r.groups,
                r.nodes,
                r.buckets,
                r.versions,
                r.delete_markers,
                r.committed,
                r.pending,
                r.retiring,
                r.bytes,
                r.multipart_roots,
                r.multipart_parts,
                r.findings_total,
                secs
            );
            for f in &r.findings {
                println!("finding: {f}");
            }
            println!("note: a metadata backup holds no shard data; this verifies metadata only");
            if let Some(path) = metrics_file {
                let text = render_backup_metrics(&r, now, secs, restore_seconds, backup_end_ms);
                let tmp = path.with_extension("tmp");
                if let Err(e) =
                    std::fs::write(&tmp, text).and_then(|()| std::fs::rename(&tmp, &path))
                {
                    return fail(format!("{}: {e}", path.display()));
                }
            }
            if r.findings_total == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(2)
            }
        }
    }
}

async fn run_admin_cluster<S: arca_lake_metadata::KvStore + 'static>(
    meta: Arc<S>,
    command: ClusterCommand,
    now: u64,
) -> ExitCode {
    use arca_lake_metadata::ops::compat;
    use arca_lake_metadata::records::ClusterPhase;
    let (from, to, rpc) = match command {
        ClusterCommand::SetReadOnly { rpc } => (ClusterPhase::Active, ClusterPhase::ReadOnly, rpc),
        ClusterCommand::Activate => {
            // Not around a restore's finalize (and its audit).
            return match arca_lake_metadata::ops::recovery::activate_read_only(
                meta.as_ref(),
                "admin",
                now,
            )
            .await
            {
                Ok(Ok(p)) => {
                    println!("cluster {}", p.name());
                    ExitCode::SUCCESS
                }
                Ok(Err(why)) => fail(why),
                Err(e) => fail(e),
            };
        }
    };
    match compat::transition(meta.as_ref(), from, to, "admin", now).await {
        Ok((rec, true)) => {
            println!("cluster {}", rec.state.name());
            // Not Active: no new deletes are decided (the guard), and the
            // fence closes those already decided (ADR-0023). No hold: the
            // leader opens a new generation once Active again.
            if let Err(e) = close_fence_everywhere(
                meta.as_ref(),
                &rpc,
                "cluster set-read-only",
                false,
                &[],
                now,
            )
            .await
            {
                // The phase changed; deletes already decided may still run
                // on the nodes that did not acknowledge (Phase 3.9 review).
                return fail(format!(
                    "cluster ReadOnly, but the delete fence is not closed everywhere: {e}; re-run `admin delete-fence close`"
                ));
            }
            ExitCode::SUCCESS
        }
        Ok((rec, false)) => fail(format!(
            "cluster is {}, not {}",
            rec.state.name(),
            from.name()
        )),
        Err(e) => fail(e),
    }
}

fn parse_chunk_id(s: &str) -> Result<[u8; 16], String> {
    let s = s.trim();
    if s.len() != 32 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("chunk id must be 32 hex digits".into());
    }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|e| e.to_string())?;
    }
    Ok(out)
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[allow(clippy::too_many_lines)]
async fn run_admin_integrity<S: arca_lake_metadata::KvStore + 'static>(
    meta: Arc<S>,
    command: IntegrityCommand,
    now: u64,
) -> ExitCode {
    use arca_lake_metadata::ops::integrity::{self, RepairState};
    match command {
        IntegrityCommand::VerifyObject {
            bucket,
            key,
            version_id,
            decode,
            quarantine,
            rpc,
        } => {
            use arca_lake_controller::objects::{ShardState, Verdict, VerifyMode, verify_object};
            let clients = match rpc.tls.context(arca_lake_protocol::tls::Role::Admin) {
                Ok(t) => {
                    arca_lake_protocol::client::NodeClients::with_policy(rpc.policy()).with_tls(t)
                }
                Err(e) => return fail(e),
            };
            // Deletes carry the cluster incarnation: a node that adopted one
            // refuses a delete without it (SEC-5).
            if let Ok(Some(st)) =
                arca_lake_metadata::ops::compat::cluster_state(meta.as_ref()).await
            {
                clients.set_incarnation(Some(st.incarnation));
            }
            let mode = if quarantine {
                VerifyMode::Quarantine
            } else {
                VerifyMode::ReadOnly
            };
            match verify_object(
                meta.as_ref(),
                &clients,
                &bucket,
                &key,
                version_id.as_deref(),
                mode,
            )
            .await
            {
                Ok(r) => {
                    println!(
                        "object {}/{} version {} generation {} multipart {} chunks {} verdict {:?}",
                        r.bucket,
                        r.key,
                        r.version_id,
                        hex(&r.generation),
                        r.multipart,
                        r.chunks.len(),
                        r.verdict
                    );
                    for p in &r.dangling_parts {
                        println!("  part {p}: generation gone (dangling reference)");
                    }
                    for c in &r.chunks {
                        if c.good() == c.shards.len() {
                            continue;
                        }
                        let bad: Vec<String> = c
                            .shards
                            .iter()
                            .enumerate()
                            .filter(|(_, (_, st))| *st != ShardState::Ok)
                            .map(|(i, (node, st))| format!("{i}@node{node}:{st:?}"))
                            .collect();
                        println!(
                            "  part {:?} chunk {} id {} group {} good {}/{} need {} affected [{}]",
                            c.part_number,
                            c.chunk_index,
                            hex(&c.chunk_id),
                            c.group,
                            c.good(),
                            c.shards.len(),
                            c.need,
                            bad.join(", ")
                        );
                    }
                    println!(
                        "mode {}",
                        if quarantine {
                            "quarantine (corrupt shards were quarantined by their nodes)"
                        } else {
                            "read-only (nothing was changed)"
                        }
                    );
                    let mut decoded_ok = true;
                    if decode {
                        // Only shards that verified good are read.
                        let mut good = arca_lake_gateway::verify::GoodShards::new();
                        for c in &r.chunks {
                            good.insert(
                                (c.generation, c.chunk_index),
                                c.shards
                                    .iter()
                                    .enumerate()
                                    .filter(|(_, (_, st))| *st == ShardState::Ok)
                                    .map(|(i, _)| i)
                                    .collect(),
                            );
                        }
                        match arca_lake_gateway::verify::decode_object(
                            meta.as_ref(),
                            &clients,
                            &bucket,
                            &key,
                            Some(&r.version_id),
                            &good,
                        )
                        .await
                        {
                            Ok(d) => {
                                println!(
                                    "decode chunks_ok {} bytes {} {}",
                                    d.chunks_ok,
                                    d.bytes,
                                    if d.ok() { "OK" } else { "MISMATCH" }
                                );
                                for (part, index, why) in &d.chunk_errors {
                                    println!("  chunk part {part:?} index {index}: {why}");
                                }
                                for e in &d.layout_errors {
                                    println!("  layout: {e}");
                                }
                                for (what, ok) in &d.checks {
                                    println!(
                                        "  {what}: {}",
                                        if *ok { "match" } else { "MISMATCH" }
                                    );
                                }
                                decoded_ok = d.ok();
                            }
                            Err(e) => return fail(e),
                        }
                    }
                    match r.verdict {
                        Verdict::Unreadable => ExitCode::from(2),
                        _ if !decoded_ok => ExitCode::from(2),
                        Verdict::Healthy => ExitCode::SUCCESS,
                        Verdict::Degraded | Verdict::Incomplete => ExitCode::from(1),
                    }
                }
                Err(e) => fail(e),
            }
        }
        IntegrityCommand::Status { objects } => {
            if objects {
                match arca_lake_controller::objects::affected_objects(meta.as_ref()).await {
                    Ok(list) => {
                        for a in &list {
                            println!(
                                "affected bucket {} key {} version {} current {} part {:?} chunk {} generation {} group {} chunk_id {} shards {:?} state {:?}",
                                if a.bucket.is_empty() { "?" } else { &a.bucket },
                                if a.key.is_empty() { "?" } else { &a.key },
                                a.version_id.as_deref().unwrap_or("?"),
                                a.is_current,
                                a.part_number,
                                a.chunk_index,
                                a.generation.map_or_else(|| "?".into(), |g| hex(&g)),
                                a.group,
                                hex(&a.chunk_id),
                                a.shards,
                                a.state
                            );
                        }
                    }
                    Err(e) => return fail(e),
                }
            }
            let cursors = match integrity::scrub_cursors(meta.as_ref()).await {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            for (g, c) in cursors {
                println!(
                    "scrub group {g} pass {} phase {:?} started_ms {} finished_ms {} verified {} bytes {} findings {:?} errors {}",
                    c.pass,
                    c.phase,
                    c.started_ms,
                    c.finished_ms,
                    c.verified,
                    c.bytes,
                    c.findings,
                    c.errors
                );
            }
            let tasks = match integrity::tasks(meta.as_ref(), usize::MAX).await {
                Ok(t) => t,
                Err(e) => return fail(e),
            };
            let unrecoverable = tasks
                .iter()
                .filter(|(_, _, t)| matches!(t.state, RepairState::Unrecoverable { .. }))
                .count();
            let failed = tasks
                .iter()
                .filter(|(_, _, t)| matches!(t.state, RepairState::Failed { .. }))
                .count();
            println!(
                "repair queue depth {} unrecoverable {unrecoverable} failed {failed} status {}",
                tasks.len(),
                if unrecoverable > 0 {
                    2
                } else {
                    i32::from(!tasks.is_empty())
                }
            );
            for (g, c, t) in tasks.iter().filter(|(_, _, t)| {
                matches!(
                    t.state,
                    RepairState::Unrecoverable { .. } | RepairState::Failed { .. }
                )
            }) {
                println!(
                    "task group {g} chunk {} reason {:?} state {:?} attempts {} last_error {:?}",
                    hex(c),
                    t.reason,
                    t.state,
                    t.attempts,
                    t.last_error
                );
            }
            ExitCode::SUCCESS
        }
        IntegrityCommand::Scrub {
            group,
            wait,
            page,
            page_mib,
            rate_mib,
            adaptive,
            adaptive_min_mib,
            adaptive_max_mib,
            adaptive_get_p99_ms,
            rpc,
        } => {
            if !wait {
                return match arca_lake_controller::integrity::request_scrub(
                    meta.as_ref(),
                    group,
                    now,
                )
                .await
                {
                    Ok(()) => {
                        println!("scrub of group {group} requested");
                        ExitCode::SUCCESS
                    }
                    Err(e) => fail(e),
                };
            }
            let clients = match rpc.tls.context(arca_lake_protocol::tls::Role::Admin) {
                Ok(t) => {
                    arca_lake_protocol::client::NodeClients::with_policy(rpc.policy()).with_tls(t)
                }
                Err(e) => return fail(e),
            };
            // Deletes carry the cluster incarnation: a node that adopted one
            // refuses a delete without it (SEC-5).
            if let Ok(Some(st)) =
                arca_lake_metadata::ops::compat::cluster_state(meta.as_ref()).await
            {
                clients.set_incarnation(Some(st.incarnation));
            }
            let coders = arca_lake_controller::integrity::coders();
            let opts = arca_lake_controller::integrity::ScrubOptions {
                page,
                page_bytes: page_mib << 20,
                rate_bytes_per_s: rate_mib << 20,
                interval_ms: 1,
                concurrency: 1,
                manifest_roots_per_pass: 256,
                adaptive: adaptive.then(|| {
                    adaptive_policy(
                        rate_mib.max(32),
                        adaptive_min_mib,
                        adaptive_max_mib,
                        adaptive_get_p99_ms,
                    )
                }),
            };
            let m = arca_lake_controller::metrics::Metrics::default();
            match arca_lake_controller::integrity::scrub_group_now_with(
                meta.as_ref(),
                &clients,
                &coders,
                group,
                &opts,
                Some(&m),
                now,
            )
            .await
            {
                Ok(r) => {
                    println!(
                        "scrub group {group} verified {} bytes {} findings {:?} enqueued {} errors {} retained {} retained_damaged {} in_flight {} unclassified {}",
                        r.verified,
                        r.bytes,
                        r.findings,
                        r.enqueued,
                        r.errors,
                        r.retained,
                        r.retained_damaged,
                        r.in_flight,
                        r.unclassified
                    );
                    if r.unclassified > 0 {
                        eprintln!(
                            "{} shards not classified (a node that does not report retirement, e.g. 0.1.0, or records that changed during the pass): counted as errors; scrub again",
                            r.unclassified
                        );
                    }
                    if opts.adaptive.is_some() {
                        use std::sync::atomic::Ordering::Relaxed;
                        println!(
                            "adaptive: last rate {} MiB/s, throttles {}",
                            m.scrub_rate_bytes_per_second.load(Relaxed) >> 20,
                            m.scrub_throttles_total.load(Relaxed)
                        );
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        IntegrityCommand::Enqueue { group, chunk } => {
            let chunk_id = match parse_chunk_id(&chunk) {
                Ok(c) => c,
                Err(e) => return fail(e),
            };
            match integrity::enqueue(
                meta.as_ref(),
                group,
                chunk_id,
                integrity::Finding::MissingShard,
                &[],
                now,
            )
            .await
            {
                Ok(true) => {
                    println!("task created");
                    ExitCode::SUCCESS
                }
                Ok(false) => {
                    println!("task already exists");
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e),
            }
        }
        IntegrityCommand::VerifyManifests => {
            match arca_lake_controller::integrity::verify_manifests(meta.as_ref()).await {
                Ok((roots, findings)) => {
                    for (root, f) in &findings {
                        println!("finding root {} {f:?}", hex(root));
                    }
                    println!("roots {roots} findings {}", findings.len());
                    if findings.is_empty() {
                        ExitCode::SUCCESS
                    } else {
                        ExitCode::from(2)
                    }
                }
                Err(e) => fail(e),
            }
        }
        IntegrityCommand::List => match integrity::tasks(meta.as_ref(), usize::MAX).await {
            Ok(tasks) => {
                for (g, c, t) in tasks {
                    println!(
                        "task group {g} chunk {} reason {:?} state {:?} attempts {} next_attempt_ms {} created_ms {} last_error {:?}",
                        hex(&c),
                        t.reason,
                        t.state,
                        t.attempts,
                        t.next_attempt_ms,
                        t.created_ms,
                        t.last_error
                    );
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e),
        },
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

fn render_integrity_stats(
    out: &mut String,
    s: &arca_lake_gateway::cluster::IntegrityStatsSnapshot,
) {
    use std::fmt::Write as _;
    for (name, value) in [
        ("arca_gateway_degraded_reads_total", s.degraded_reads),
        ("arca_gateway_unreadable_reads_total", s.unreadable_reads),
        ("arca_gateway_moved_reads_total", s.moved_reads),
        ("arca_gateway_integrity_reports_total", s.integrity_reports),
        (
            "arca_gateway_integrity_reports_dropped_total",
            s.integrity_reports_dropped,
        ),
    ] {
        let _ = writeln!(out, "# TYPE {name} counter\n{name} {value}");
    }
}

fn render_put_stats(out: &mut String, p: &arca_lake_gateway::cluster::PutStatsSnapshot) {
    use std::fmt::Write as _;
    let _ = writeln!(
        out,
        "# TYPE arca_gateway_put_in_flight gauge\narca_gateway_put_in_flight {}",
        p.in_flight
    );
    let phases = [
        ("body_read", &p.body_read),
        ("encode", &p.encode),
        ("shard_write", &p.shard_write),
        ("append", &p.append),
        ("lease_renew", &p.lease_renew),
        ("commit", &p.commit),
        ("total", &p.total),
    ];
    let _ = writeln!(out, "# TYPE arca_gateway_put_phase_seconds histogram");
    for (phase, h) in phases {
        h.render_prometheus(
            out,
            "arca_gateway_put_phase_seconds",
            &format!("phase=\"{phase}\""),
        );
    }
    let _ = writeln!(out, "# TYPE arca_gateway_put_phase_max_seconds gauge");
    for (phase, h) in phases {
        h.render_max(
            out,
            "arca_gateway_put_phase_max_seconds",
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

fn render_node_capacity(
    out: &mut String,
    device: Option<&arca_lake_protocol::storage::v1::DeviceCapacity>,
    counts: (u64, [u64; 4]),
    b: &arca_lake_storage_node::CapacityBreakdown,
) {
    use std::fmt::Write as _;
    let d = device.cloned().unwrap_or_default();
    // Where the device's bytes go (Phase 3.8): written since start by kind
    // (data records split by class), and on the device now.
    let _ = writeln!(out, "# TYPE arca_node_written_bytes_total counter");
    for (kind, v) in [
        ("client", b.by_class[0]),
        ("repair", b.by_class[1]),
        ("migration", b.by_class[2]),
        ("cache", b.by_class[3]),
        ("data_records", b.written.data),
        ("tombstone", b.written.tombstone),
        ("rollover", b.written.rollover),
        ("compaction", b.written.compaction),
        ("quarantine_log", b.written.quarantine_log),
        ("sidecar", b.written.sidecar),
    ] {
        let _ = writeln!(out, "arca_node_written_bytes_total{{kind=\"{kind}\"}} {v}");
    }
    let _ = writeln!(out, "# TYPE arca_node_device_usage_bytes gauge");
    for (kind, v) in [
        ("segments", b.segment_bytes),
        ("side_files", b.side_file_bytes),
        ("fs_used", b.fs_used_bytes),
        ("fs_overhead", b.fs_overhead_bytes()),
        ("delete_reserve", b.delete_reserve_bytes),
    ] {
        let _ = writeln!(out, "arca_node_device_usage_bytes{{kind=\"{kind}\"}} {v}");
    }
    let _ = writeln!(out, "# TYPE arca_node_capacity_bytes gauge");
    for (kind, v) in [
        ("total", d.total_bytes),
        ("available", d.available_bytes),
        ("allocated", d.allocated_bytes),
        ("live", d.live_bytes),
        ("reclaimable", d.reclaimable_bytes),
        ("quarantined", d.quarantined_bytes),
        ("reserved", d.reserved_bytes),
        ("repair_floor", d.repair_floor_bytes),
        ("inflight", d.inflight_bytes),
    ] {
        let _ = writeln!(out, "arca_node_capacity_bytes{{kind=\"{kind}\"}} {v}");
    }
    let _ = writeln!(
        out,
        "# TYPE arca_node_admission_admitted_total counter\narca_node_admission_admitted_total {}",
        counts.0
    );
    let _ = writeln!(out, "# TYPE arca_node_admission_refused_total counter");
    for (class, v) in ["client", "repair", "migration", "cache"]
        .iter()
        .zip(counts.1)
    {
        let _ = writeln!(
            out,
            "arca_node_admission_refused_total{{class=\"{class}\"}} {v}"
        );
    }
}

/// The node's delete fence (ADR-0023): generation, open, operator hold,
/// and the deletes it refused.
fn render_node_fence(
    out: &mut String,
    fence: (u64, bool, bool),
    refusals: u64,
    rollover_failures: u64,
) {
    use std::fmt::Write as _;
    for (name, value) in [
        ("arca_node_delete_fence_generation", fence.0),
        ("arca_node_delete_fence_open", u64::from(fence.1)),
        ("arca_node_delete_fence_hold", u64::from(fence.2)),
    ] {
        let _ = writeln!(out, "# TYPE {name} gauge\n{name} {value}");
    }
    let _ = writeln!(
        out,
        "# TYPE arca_node_delete_fence_refusals_total counter\narca_node_delete_fence_refusals_total {refusals}"
    );
    let _ = writeln!(
        out,
        "# TYPE arca_node_format_rollover_failures_total counter\narca_node_format_rollover_failures_total {rollover_failures}"
    );
}

fn render_admission_stats(
    out: &mut String,
    a: &arca_lake_gateway::cluster::admission::AdmissionStatsSnapshot,
) {
    use std::fmt::Write as _;
    for (name, kind, value) in [
        ("arca_gateway_admission_state", "gauge", u64::from(a.state)),
        (
            "arca_gateway_admission_record_age_seconds",
            "gauge",
            a.record_age_secs,
        ),
        (
            "arca_gateway_admission_throttled_total",
            "counter",
            a.throttled,
        ),
        (
            "arca_gateway_admission_rejected_total",
            "counter",
            a.rejected,
        ),
        (
            "arca_gateway_node_capacity_refusals_total",
            "counter",
            a.node_capacity_refusals,
        ),
        (
            "arca_gateway_placement_capacity_failures_total",
            "counter",
            a.placement_capacity_failures,
        ),
        (
            "arca_gateway_admission_charged_bytes_total",
            "counter",
            a.charged_bytes,
        ),
    ] {
        let _ = writeln!(out, "# TYPE {name} {kind}\n{name} {value}");
    }
}

/// Access-record table of a gateway (ADR-0034): bounded labels only.
#[allow(clippy::cast_precision_loss)]
fn render_access_stats(
    out: &mut String,
    a: &arca_lake_gateway::cluster::access::AccessStatsSnapshot,
) {
    use std::fmt::Write as _;
    for (name, kind, value) in [
        ("arca_gateway_access_queue_entries", "gauge", a.entries),
        ("arca_gateway_access_queue_capacity", "gauge", a.capacity),
        ("arca_gateway_access_queue_in_flight", "gauge", a.in_flight),
        (
            "arca_gateway_access_isolated_generations",
            "gauge",
            a.isolated,
        ),
        ("arca_gateway_access_flush_batch_max", "gauge", a.batch_max),
        ("arca_gateway_access_reads_total", "counter", a.reads),
        (
            "arca_gateway_access_coalesced_total",
            "counter",
            a.coalesced,
        ),
        ("arca_gateway_access_inserted_total", "counter", a.inserted),
        (
            "arca_gateway_access_uncertain_total",
            "counter",
            a.uncertain,
        ),
        (
            "arca_gateway_access_uncertain_batches_total",
            "counter",
            a.uncertain_batches,
        ),
        ("arca_gateway_access_flushes_total", "counter", a.flushes),
        (
            "arca_gateway_access_flushed_generations_total",
            "counter",
            a.flushed_generations,
        ),
        (
            "arca_gateway_access_flushed_reads_total",
            "counter",
            a.flushed_reads,
        ),
        (
            "arca_gateway_access_skipped_finished_total",
            "counter",
            a.skipped_finished,
        ),
        (
            "arca_gateway_access_skipped_finished_reads_total",
            "counter",
            a.skipped_finished_reads,
        ),
        (
            "arca_gateway_access_unrecorded_total",
            "counter",
            a.unrecorded,
        ),
        (
            "arca_gateway_access_recording",
            "gauge",
            u64::from(a.recording),
        ),
    ] {
        let _ = writeln!(out, "# TYPE {name} {kind}\n{name} {value}");
    }
    let _ = writeln!(
        out,
        "# TYPE arca_gateway_access_queue_oldest_seconds gauge\narca_gateway_access_queue_oldest_seconds {}",
        a.oldest_age_ms as f64 / 1000.0
    );
    let _ = writeln!(out, "# TYPE arca_gateway_access_dropped_total counter");
    for (reason, v) in [
        ("full", a.dropped_full),
        ("stale", a.dropped_stale),
        ("error", a.dropped_error),
        ("shutdown", a.dropped_shutdown),
        ("cutoff", a.dropped_cutoff),
    ] {
        let _ = writeln!(
            out,
            "arca_gateway_access_dropped_total{{reason=\"{reason}\"}} {v}"
        );
    }
    let _ = writeln!(out, "# TYPE arca_gateway_access_flush_errors_total counter");
    for (kind, v) in [
        ("conflict", a.errors_conflict),
        ("backend", a.errors_backend),
        ("too_large", a.errors_too_large),
        ("other", a.errors_other),
    ] {
        let _ = writeln!(
            out,
            "arca_gateway_access_flush_errors_total{{kind=\"{kind}\"}} {v}"
        );
    }
    let _ = writeln!(out, "# TYPE arca_gateway_access_flush_seconds histogram");
    a.flush_time
        .render_prometheus(out, "arca_gateway_access_flush_seconds", "");
}

fn render_device_space(out: &mut String, space: Option<(u64, u64)>) {
    use std::fmt::Write as _;
    let (free, total) = space.unwrap_or((0, 0));
    let _ = writeln!(
        out,
        "# TYPE arca_node_device_free_bytes gauge\narca_node_device_free_bytes {free}"
    );
    let _ = writeln!(
        out,
        "# TYPE arca_node_device_total_bytes gauge\narca_node_device_total_bytes {total}"
    );
}

fn render_compaction_stats(
    out: &mut String,
    c: &arca_lake_storage_node::compactor::CompactionStats,
) {
    use std::fmt::Write as _;
    use std::sync::atomic::Ordering::Relaxed;
    for (name, help, v) in [
        (
            "arca_node_compactions_total",
            "sealed segments compacted",
            &c.compactions,
        ),
        (
            "arca_node_compaction_errors_total",
            "compactions that failed and were left for the next pass",
            &c.errors,
        ),
        (
            "arca_node_compaction_skipped_no_space_total",
            "segments not compacted because the device lacked room for their live bytes",
            &c.skipped_no_space,
        ),
        (
            "arca_node_compaction_copied_bytes_total",
            "live payload bytes copied by compaction",
            &c.copied_bytes,
        ),
        (
            "arca_node_compaction_reclaimed_bytes_total",
            "segment bytes released by compaction",
            &c.reclaimed_bytes,
        ),
    ] {
        let _ = writeln!(
            out,
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {}",
            v.load(Relaxed)
        );
    }
    for (name, help, v) in [
        (
            "arca_node_segment_bytes",
            "segment file bytes on the device at the last compaction pass",
            &c.segment_bytes,
        ),
        (
            "arca_node_live_bytes",
            "bytes of live records at the last compaction pass",
            &c.live_bytes,
        ),
        // HELP lines are replaced from the metric registry.
        (
            "arca_node_compaction_backlog_segments",
            "",
            &c.backlog_segments,
        ),
        ("arca_node_compaction_backlog_bytes", "", &c.backlog_bytes),
        (
            "arca_node_compaction_blocked_segments",
            "",
            &c.blocked_segments,
        ),
        ("arca_node_dead_records", "", &c.dead_records),
        ("arca_node_active_segment_id", "", &c.active_segment),
    ] {
        let _ = writeln!(
            out,
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {}",
            v.load(Relaxed)
        );
    }
    let _ = writeln!(out, "# TYPE arca_node_index_entries gauge");
    for (kind, v) in [
        ("live", &c.index_live),
        ("tombstone_history", &c.index_tombstone_history),
        ("dead_tracked", &c.index_dead_tracked),
        ("quarantined", &c.index_quarantined),
    ] {
        let _ = writeln!(
            out,
            "arca_node_index_entries{{kind=\"{kind}\"}} {}",
            v.load(Relaxed)
        );
    }
    let m = arca_lake_common::process::memory();
    let _ = writeln!(out, "# TYPE arca_node_process_memory_bytes gauge");
    for (kind, v) in [
        ("resident", m.resident),
        ("anon", m.anon),
        ("heap_in_use", m.heap_in_use),
        ("heap_free", m.heap_free),
        ("heap_mmap", m.heap_mmap),
    ] {
        let _ = writeln!(out, "arca_node_process_memory_bytes{{kind=\"{kind}\"}} {v}");
    }
}

/// Deferred reclamation (Phase 3.14): `(retention ms, retired records,
/// damaged, retained bytes)`.
fn render_node_reclaim(
    out: &mut String,
    (retention_ms, pending, damaged, retained): (u64, u64, bool, u64),
) {
    use std::fmt::Write as _;
    for (name, v) in [
        ("arca_node_reclaim_retained_bytes", retained),
        ("arca_node_reclaim_pending_records", pending),
        ("arca_node_reclaim_retention_seconds", retention_ms / 1000),
        ("arca_node_reclaim_damaged", u64::from(damaged)),
    ] {
        let _ = writeln!(out, "# TYPE {name} gauge\n{name} {v}");
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
    for (name, value) in [
        ("arca_node_quarantined_shards", n.quarantined_shards),
        ("arca_node_quarantined_bytes", n.quarantined_bytes),
    ] {
        let _ = writeln!(out, "# TYPE {name} gauge\n{name} {value}");
    }
    for (name, value) in [
        ("arca_node_corrupt_reads_total", n.corrupt_reads),
        ("arca_node_scrub_shards_total", n.scrub_shards),
        ("arca_node_scrub_bytes_total", n.scrub_bytes),
        ("arca_node_scrub_corrupt_total", n.scrub_corrupt),
    ] {
        let _ = writeln!(out, "# TYPE {name} counter\n{name} {value}");
    }
    let phases = [
        ("put_recv", &n.put_recv),
        ("put_queue", &n.put_queue),
        ("put_store", &n.put_store),
        ("get_serve", &n.get_serve),
        ("store_append", &t.append),
        ("store_rollover", &t.rollover),
        ("store_sync_wait", &t.sync_wait),
        ("store_fsync", &t.fsync),
    ];
    let _ = writeln!(out, "# TYPE arca_node_phase_seconds histogram");
    for (phase, h) in phases {
        h.render_prometheus(
            &mut out,
            "arca_node_phase_seconds",
            &format!("phase=\"{phase}\""),
        );
    }
    let _ = writeln!(out, "# TYPE arca_node_phase_max_seconds gauge");
    for (phase, h) in phases {
        h.render_max(
            &mut out,
            "arca_node_phase_max_seconds",
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

#[cfg(test)]
#[allow(clippy::default_trait_access)]
mod metrics_registry {
    //! Every exported metric family matches the registry (RFC-013 §7,
    //! R-62): the expositions below are built by the same renderers the
    //! processes serve, with every optional part enabled.
    use super::*;

    fn tls(on: bool) -> Option<arca_lake_protocol::tls::TlsStats> {
        on.then(arca_lake_protocol::tls::TlsStats::default)
    }

    fn gateway_text(tls_on: bool) -> String {
        let mut out = render_cache_stats(&Default::default(), &Default::default());
        render_put_stats(&mut out, &Default::default());
        render_queue_and_txn_stats(&mut out, &Default::default());
        render_integrity_stats(&mut out, &Default::default());
        render_admission_stats(&mut out, &Default::default());
        render_access_stats(&mut out, &Default::default());
        let s3 = arca_lake_gateway::requests::S3Stats::default();
        s3.record("PutObject", 200, 10, 0, Duration::from_millis(3));
        s3.record("", 403, 0, 0, Duration::from_millis(1));
        s3.render(&mut out);
        out += &arca_lake_protocol::tls::render_metrics("arca_gateway", tls(tls_on).as_ref());
        arca_lake_common::metrics::annotate(&out)
    }

    fn node_text(tls_on: bool) -> String {
        let mut out = render_node_stats(&Default::default(), &Default::default());
        render_node_reclaim(&mut out, (86_400_000, 3, false, 4096));
        render_compaction_stats(&mut out, &Default::default());
        render_device_space(&mut out, Some((1, 2)));
        render_node_capacity(&mut out, None, (0, [0; 4]), &Default::default());
        render_node_fence(&mut out, (0, false, false), 0, 0);
        out += &arca_lake_protocol::tls::render_metrics("arca_node", tls(tls_on).as_ref());
        arca_lake_common::metrics::annotate(&out)
    }

    fn controller_text(tls_on: bool) -> String {
        let m = arca_lake_controller::metrics::Metrics::default();
        if let Some(t) = tls(tls_on) {
            let _ = m.tls.set(std::sync::Arc::new(t));
        }
        m.render()
    }

    #[test]
    fn every_exported_family_is_registered_and_every_registered_family_is_exported() {
        for (process, text) in [
            ("gateway", gateway_text(true)),
            ("node", node_text(true)),
            ("controller", controller_text(true)),
        ] {
            let problems = arca_lake_common::metrics::validate(process, &text);
            assert!(problems.is_empty(), "{process}:\n{}", problems.join("\n"));
        }
        let r = arca_lake_controller::backup::RestoredReport::default();
        let text = render_backup_metrics(&r, 1_000, 1.0, Some(2.0), Some(500));
        let problems = arca_lake_common::metrics::validate("backup", &text);
        assert!(problems.is_empty(), "backup:\n{}", problems.join("\n"));
        // Explicitly plaintext processes export `_tls_enabled 0` and none
        // of the other TLS families.
        for (process, text) in [
            ("gateway", gateway_text(false)),
            ("node", node_text(false)),
            ("controller", controller_text(false)),
        ] {
            let problems = arca_lake_common::metrics::validate(process, &text);
            assert!(
                problems
                    .iter()
                    .all(|p| p.contains("_tls_") && p.contains("not exported")),
                "{process}:\n{}",
                problems.join("\n")
            );
        }
    }

    #[test]
    fn alert_rules_reference_registered_metrics_only() {
        let rules = include_str!("../../../deploy/prometheus/arca-lake-alerts.yml");
        let mut n = 0;
        for word in rules.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
            if !word.starts_with("arca_") {
                continue;
            }
            n += 1;
            let base = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|s| word.strip_suffix(*s))
                .filter(|b| {
                    arca_lake_common::metrics::family(b).is_some_and(|f| f.kind == "histogram")
                })
                .unwrap_or(word);
            assert!(
                arca_lake_common::metrics::family(base).is_some(),
                "alert rule uses {word}, which no process exports"
            );
        }
        assert!(n >= 10, "the rules reference {n} metrics");
    }
}

type GatewayAuth = (
    Option<std::sync::Arc<arca_lake_gateway::auth::CredentialStore>>,
    Option<(String, String)>,
);

/// The credential store SigV4 and the authorizer share. Static keys become
/// a one-key store when the authorizer is on (a separate empty store made
/// every signed request `InvalidAccessKeyId`); the authorizer without any
/// credentials is refused, because s3s then never calls the access hook
/// and every request would be allowed (Phase 3.5 audit SEC-3, SEC-8).
fn gateway_auth(
    authorize: bool,
    store: Option<std::sync::Arc<arca_lake_gateway::auth::CredentialStore>>,
    credentials: Option<(String, String)>,
) -> Result<GatewayAuth, String> {
    match (authorize, store, credentials) {
        (true, None, Some((ak, sk))) => Ok((
            Some(std::sync::Arc::new(arca_lake_gateway::auth::CredentialStore::from_keys(&[(
                ak.as_str(),
                sk.as_str(),
                ak.as_str(),
            )]))),
            None,
        )),
        (true, None, None) => Err(
            "--authorize owner-policy needs credentials (--credentials-file or --access-key/--secret-key); anonymous requests are then evaluated against bucket policies".into(),
        ),
        (_, store, credentials) => Ok((store, credentials)),
    }
}

#[cfg(test)]
mod tls_args {
    //! Node RPCs never fall back to plaintext by accident (ADR-0017).
    use super::*;

    fn none() -> TlsArgs {
        TlsArgs {
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            tls_crl: None,
            trust_domain: None,
            tls_reload_secs: 30,
            insecure_plaintext: false,
        }
    }

    #[test]
    fn plaintext_only_when_asked_and_never_with_tls_flags() {
        use arca_lake_protocol::tls::Role;
        let e = none().context(Role::Node).unwrap_err();
        assert!(e.contains("no TLS configuration"), "{e}");
        let partial = TlsArgs {
            tls_ca: Some("/ca.pem".into()),
            ..none()
        };
        assert!(
            partial
                .context(Role::Gateway)
                .unwrap_err()
                .contains("TLS needs")
        );
        let mixed = TlsArgs {
            trust_domain: Some("td".into()),
            insecure_plaintext: true,
            ..none()
        };
        assert!(
            mixed
                .context(Role::Controller)
                .unwrap_err()
                .contains("cannot be combined")
        );
        let plain = TlsArgs {
            insecure_plaintext: true,
            ..none()
        };
        assert!(plain.context(Role::Node).unwrap().is_none());
    }

    #[test]
    fn the_plaintext_switch_parses_as_a_flag() {
        #[derive(clap::Parser)]
        struct T {
            #[command(flatten)]
            tls: TlsArgs,
        }
        let t = <T as clap::Parser>::try_parse_from(["x", "--insecure-plaintext"]).unwrap();
        assert!(t.tls.insecure_plaintext);
        let t = <T as clap::Parser>::try_parse_from(["x"]).unwrap();
        assert!(!t.tls.insecure_plaintext || std::env::var_os("ARCA_INSECURE_PLAINTEXT").is_some());
    }
}

#[cfg(test)]
mod gateway_auth_tests {
    use super::*;

    #[test]
    fn the_authorizer_shares_the_signing_keys_and_never_runs_without_them() {
        let (store, creds) = gateway_auth(true, None, Some(("ak".into(), "sk".into()))).unwrap();
        assert!(creds.is_none());
        assert_eq!(store.unwrap().principal_of("ak").as_deref(), Some("ak"));
        assert!(gateway_auth(true, None, None).is_err());
        let (store, creds) = gateway_auth(false, None, None).unwrap();
        assert!(store.is_none() && creds.is_none());
    }
}

#[cfg(test)]
mod tiering_args {
    //! `admin tiering` takes every policy field or none (ADR-0035).
    use super::*;

    fn args(tiers: &[&str], r: Option<u64>, w: Option<u64>, d: Option<u64>) -> TieringPolicyArgs {
        TieringPolicyArgs {
            tiers: tiers.iter().map(ToString::to_string).collect(),
            promote_reads: r,
            promote_window_secs: w,
            demote_after_secs: d,
        }
    }

    #[test]
    fn every_policy_field_or_none() {
        assert_eq!(args(&[], None, None, None).policy(), Ok(None));
        let p = args(&["default", "fast"], Some(5), Some(600), Some(1800))
            .policy()
            .unwrap()
            .unwrap();
        assert_eq!(
            (p.promote_window_ms, p.demote_after_ms),
            (600_000, 1_800_000)
        );
        assert!(
            args(&["default"], Some(5), None, Some(1800))
                .policy()
                .is_err()
        );
        assert!(args(&[], Some(5), Some(600), Some(1800)).policy().is_err());
    }
}

#[cfg(test)]
mod access_age {
    //! The gateways' access-record age bound fits the tiering grace
    //! (ADR-0035 §10.4).
    use super::access_max_age_ms;

    #[test]
    fn the_age_bound_is_at_most_the_graces_flush_delay() {
        let limit = arca_lake_metadata::ops::tiering::FLUSH_DELAY_MS;
        assert_eq!(access_max_age_ms(5_000), 5_000);
        assert_eq!(access_max_age_ms(limit), limit);
        assert_eq!(access_max_age_ms(limit + 1), limit);
        assert_eq!(access_max_age_ms(0), limit, "no bound is not allowed");
    }
}

#[cfg(test)]
mod set_failure_domain_cli {
    //! `admin group set-failure-domain` through the parsed command line
    //! (ADR-0038): listing, dry run, `--change`, the refusal of a label no
    //! member carries, `--force`, exit codes.
    use super::*;
    use arca_lake_metadata::mem::MemKv;
    use arca_lake_metadata::ops::{cluster, group_domain};
    use arca_lake_metadata::records::{GroupMember, GroupRecord, GroupRole, GroupState};

    async fn run(meta: &MemKv, args: &[&str]) -> ExitCode {
        let line = ["arca-lake", "admin", "--cluster-file", "/none", "group"]
            .into_iter()
            .chain(args.iter().copied());
        let Command::Admin(a) = Cli::try_parse_from(line).unwrap().command else {
            unreachable!()
        };
        let AdminCommand::Group { command } = a.command else {
            unreachable!()
        };
        group_command(meta, command, 1_000).await
    }

    async fn store() -> (MemKv, u64, u64) {
        let kv = MemKv::default();
        for (i, host) in ["a", "b", "c", "a"].iter().enumerate() {
            cluster::register_node(
                &kv,
                &format!("n{i}"),
                vec![
                    ("host".into(), (*host).into()),
                    ("rack".into(), format!("r{i}")),
                ],
                vec![0],
                1_000,
            )
            .await
            .unwrap();
        }
        let ids: Vec<u64> = cluster::list_nodes(&kv)
            .await
            .unwrap()
            .iter()
            .map(|n| n.id)
            .collect();
        let legacy = |members: &[u64]| GroupRecord {
            id: 0,
            profile: "p".into(),
            tier: "t".into(),
            scheme: "rs-gf8-v1".into(),
            data_shards: 2,
            parity_shards: 1,
            members: members
                .iter()
                .map(|&node_id| GroupMember {
                    node_id,
                    device_id: 0,
                })
                .collect(),
            epoch: 0,
            state: GroupState::Active,
            repair_cursor: None,
            role: GroupRole::Durable,
        };
        let clean = cluster::create_group(&kv, legacy(&ids[..3])).await.unwrap();
        let shared = cluster::create_group(&kv, legacy(&[ids[0], ids[1], ids[3]]))
            .await
            .unwrap();
        (kv, clean.id, shared.id)
    }

    async fn label(kv: &MemKv, g: u64) -> Option<String> {
        group_domain::get(kv, g).await.unwrap().map(|d| d.label)
    }

    #[tokio::test]
    async fn all_unrecorded_lists_writes_and_warns() {
        let (kv, clean, shared) = store().await;
        let dry = ["set-failure-domain", "--all-unrecorded", "--label", "host"];
        assert_eq!(
            run(&kv, &[&dry[..], &["--dry-run"]].concat()).await,
            ExitCode::SUCCESS
        );
        assert_eq!(
            (label(&kv, clean).await, label(&kv, shared).await),
            (None, None)
        );
        // Group `shared` has two members on host `a`: recorded, exit 2.
        assert_eq!(run(&kv, &dry).await, ExitCode::from(2));
        assert_eq!(label(&kv, clean).await.as_deref(), Some("host"));
        assert_eq!(label(&kv, shared).await.as_deref(), Some("host"));
        // Nothing left to record.
        assert_eq!(run(&kv, &dry).await, ExitCode::SUCCESS);
    }

    #[tokio::test]
    async fn an_unknown_label_is_refused_unless_forced() {
        let (kv, clean, shared) = store().await;
        let typo = ["set-failure-domain", "--all-unrecorded", "--label", "hots"];
        assert_eq!(run(&kv, &typo).await, ExitCode::from(1));
        assert_eq!(
            (label(&kv, clean).await, label(&kv, shared).await),
            (None, None)
        );
        let g = clean.to_string();
        let one = ["set-failure-domain", "--group", &g, "--label", "hots"];
        assert_eq!(run(&kv, &one).await, ExitCode::from(1));
        assert_eq!(label(&kv, clean).await, None);
        assert_eq!(
            run(&kv, &[&one[..], &["--force"]].concat()).await,
            ExitCode::from(2)
        );
        assert_eq!(label(&kv, clean).await.as_deref(), Some("hots"));
    }

    #[tokio::test]
    async fn change_needs_the_flag_and_a_carried_label() {
        let (kv, clean, _) = store().await;
        let g = clean.to_string();
        let set = |l| ["set-failure-domain", "--group", g.as_str(), "--label", l];
        assert_eq!(run(&kv, &set("host")).await, ExitCode::SUCCESS);
        // Without --change: refused, the record kept.
        assert_eq!(run(&kv, &set("rack")).await, ExitCode::from(1));
        assert_eq!(label(&kv, clean).await.as_deref(), Some("host"));
        // --change to a label no member carries: refused too.
        assert_eq!(
            run(&kv, &[&set("zone")[..], &["--change"]].concat()).await,
            ExitCode::from(1)
        );
        assert_eq!(label(&kv, clean).await.as_deref(), Some("host"));
        assert_eq!(
            run(&kv, &[&set("rack")[..], &["--change"]].concat()).await,
            ExitCode::SUCCESS
        );
        assert_eq!(label(&kv, clean).await.as_deref(), Some("rack"));
    }
}
