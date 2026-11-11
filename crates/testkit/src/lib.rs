//! In-process test cluster: N storage nodes, a metadata store (in-memory or
//! FoundationDB), and a gateway with the cluster backend, plus helpers to
//! stop and restart nodes, corrupt shards, and drive GC and repair. The
//! end-to-end suite lives in [`suite`] and is instantiated per backend with
//! [`e2e_suite!`].

pub mod chaos;
#[cfg(feature = "fdb")]
pub mod fdb;
pub mod pki;
pub mod s3;
pub mod suite;
pub mod suite_cache;
pub mod suite_cache_ec;
pub mod suite_faults;
pub mod suite_rebalance;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arca_lake_common::{EcProfile, SmallObjectPolicy};
use arca_lake_controller::groups::CacheProtection;
use arca_lake_controller::{gc, groups, repair};
use arca_lake_gateway::auth::{Authorizer, BucketCreation, CredentialStore};
use arca_lake_gateway::cluster::{ClusterBackend, ClusterConfig};
use arca_lake_gateway::{GatewayConfig, ServerHandle, serve, serve_with_access};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::fault::FaultKv;
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::{cluster, object};
use arca_lake_metadata::records::{GroupMember, GroupRecord};
use arca_lake_protocol::client::NodeClients;
use arca_lake_storage::{ShardKey, Store, StoreConfig, SyncPolicy};
use arca_lake_storage_node::{NodeConfig, NodeHandle};
use arca_lake_tiering::cache::CacheTierConfig;

/// A storage node slot: directory, address, and the running handle if any.
pub struct TestNode {
    /// Node ID in metadata.
    pub id: u64,
    /// Device directory.
    pub dir: PathBuf,
    /// Address (stable across restarts).
    pub addr: SocketAddr,
    handle: Option<NodeHandle>,
    tls: Option<Arc<arca_lake_protocol::tls::TlsContext>>,
    capacity: Option<u64>,
}

impl TestNode {
    /// The node's counters, if it is running.
    #[must_use]
    pub fn handle_stats(&self) -> Option<arca_lake_storage_node::NodeStatsSnapshot> {
        self.handle.as_ref().map(|h| h.service().stats())
    }
}

/// The cluster.
pub struct TestCluster<S: KvStore + 'static = MemKv> {
    /// Metadata store.
    pub meta: Arc<S>,
    /// Storage nodes.
    pub nodes: Vec<TestNode>,
    /// Gateway backend (for direct calls and fault injection).
    pub backend: ClusterBackend<S>,
    /// Gateway HTTP server.
    pub gateway: Option<ServerHandle>,
    /// Gateway address.
    pub gateway_addr: SocketAddr,
    /// Credentials accepted by the gateway (principal [`DEFAULT_PRINCIPAL`]).
    pub credentials: (String, String),
    /// The gateway's credential store (keys of every principal).
    pub store: Arc<CredentialStore>,
    /// Node clients.
    pub clients: NodeClients,
    /// Rejoin lease given to nodes started from now on (Phase 3.10).
    pub fence_lease: Duration,
    /// EC profile in use.
    pub profile: EcProfile,
    /// Number of durable nodes (the first `durable_nodes` entries of `nodes`).
    pub durable_nodes: usize,
    /// `ListShards`/`VerifyShards` page size used by GC and repair helpers
    /// (0 = node default).
    pub page_size: u32,
    /// Repair options used by [`TestCluster::repair`].
    pub repair_options: repair::RepairOptions,
    cluster_cfg: ClusterConfig,
    /// The development CA of a TLS cluster.
    pub pki: Option<Arc<pki::TestPki>>,
    tmp: tempfile::TempDir,
}

/// Options for [`TestCluster::start`].
#[derive(Debug, Clone)]
pub struct ClusterOptions {
    /// Number of storage nodes (each one device, its own host label).
    pub nodes: usize,
    /// EC profile.
    pub profile: EcProfile,
    /// Groups per scheme to create.
    pub groups: usize,
    /// Pending-generation lease.
    pub lease: Duration,
    /// Extra nodes with device class `nvme` forming cache groups of tier
    /// `fast` (RFC-006); 0 disables the cache.
    pub cache_nodes: usize,
    /// Cache tier configuration given to the gateway (defaults when
    /// `cache_nodes > 0` and this is `None`).
    pub cache: Option<CacheTierConfig>,
    /// Protection mode of the cache groups (RFC-006 §5.9).
    pub cache_protection: CacheProtection,
    /// Cache groups to create for replicated or coded protection.
    pub cache_groups: usize,
    /// Gateway chunk read-ahead (see `ClusterConfig::read_parallelism`).
    /// Test clusters default to 3 so that every suite exercises ordered
    /// parallel delivery.
    pub read_parallelism: usize,
    /// Population queue depth (see `ClusterConfig::population_queue_depth`).
    pub population_queue_depth: usize,
    /// Populations per node (see `ClusterConfig::populations_per_node`).
    pub populations_per_node: usize,
    /// Chunk placements in flight per PUT (see `ClusterConfig::write_pipeline`).
    pub write_pipeline: usize,
    /// Minimum multipart part size (see `ClusterConfig::multipart_min_part_bytes`);
    /// test clusters default to 1 KiB so that small parts are usable.
    pub multipart_min_part_bytes: u64,
    /// Install the RFC-012 authorizer (bucket owner + policy). Off by
    /// default: most suites create buckets directly on the backend.
    pub authorize: bool,
    /// Extra principals `(access key, secret key, name)` besides the
    /// cluster's default credentials (principal `tenant-a`).
    pub principals: Vec<(String, String, String)>,
    /// Gateway heartbeat interval (RFC-014 §5.5); `None` disables it.
    pub heartbeat: Option<Duration>,
    /// Run every node, the gateway and the test's own clients over mutual
    /// TLS with a development CA (ADR-0017).
    pub tls: bool,
    /// Use this credential store instead of one built from `principals`
    /// (a file-backed store for rotation tests); the cluster's generated
    /// `credentials` are then not registered anywhere.
    pub credential_store: Option<Arc<CredentialStore>>,
}

/// Allotment every test node starts with (`--capacity-bytes`): the
/// nodes of a test cluster share one file system, which capacity
/// admission refuses without allotments that fit it (Phase 3.7, R-80).
/// [`TestCluster::set_node_capacity`] changes it (`None`: none).
/// Small enough that a dozen nodes fit a CI runner's temporary file
/// system (often a memory file system of a few GiB).
pub const TEST_ALLOTMENT: u64 = 512 << 20;

/// Rejoin lease of the nodes' delete fence in test clusters (Phase 3.10):
/// long, since a test cluster runs no controller to renew it; tests of
/// the lease set [`TestCluster::fence_lease`] and restart a node.
pub const TEST_FENCE_LEASE: Duration = Duration::from_secs(3600);

/// The cache tier name used by test clusters.
pub const CACHE_TIER: &str = "fast";

/// Principal name of the cluster's default credentials.
pub const DEFAULT_PRINCIPAL: &str = "tenant-a";

impl ClusterOptions {
    /// Default cache tier configuration for tests: unlimited capacity,
    /// admit everything on the first miss.
    #[must_use]
    pub fn default_cache() -> CacheTierConfig {
        CacheTierConfig {
            tier: CACHE_TIER.into(),
            capacity_bytes: 0,
            high_watermark: 0.9,
            low_watermark: 0.7,
            max_cacheable_bytes: 0,
            admit_after_misses: 0,
            max_populations_in_flight: 64,
        }
    }
}

impl ClusterOptions {
    /// `nodes` nodes with an RS `k+m` profile and 3-way replication below `small_threshold`.
    #[must_use]
    pub fn rs(nodes: usize, k: u16, m: u16, max_chunk: u64, small_threshold: u64) -> Self {
        Self {
            nodes,
            profile: EcProfile {
                name: "test".into(),
                scheme: "rs-gf8-v1".into(),
                data_shards: k,
                parity_shards: m,
                required_parity_acks: None,
                max_chunk_bytes: max_chunk,
                small_object: (small_threshold > 0).then_some(SmallObjectPolicy {
                    threshold_bytes: small_threshold,
                    replicas: 3,
                }),
                failure_domain: "host".into(),
            },
            groups: 2,
            lease: Duration::from_secs(60),
            cache_nodes: 0,
            cache: None,
            cache_protection: CacheProtection::None,
            cache_groups: 1,
            read_parallelism: 3,
            population_queue_depth: 64,
            populations_per_node: 4,
            write_pipeline: 2,
            multipart_min_part_bytes: 1024,
            authorize: false,
            principals: Vec::new(),
            heartbeat: None,
            tls: false,
            credential_store: None,
        }
    }
}

fn node_store_config(dir: &std::path::Path) -> StoreConfig {
    StoreConfig {
        dir: dir.to_path_buf(),
        segment_max_bytes: 8 << 20,
        record_alignment: 512,
        block_size: 4096,
        sync: SyncPolicy::Group,
        format_write: arca_lake_storage::default_format_write(),
    }
}

impl TestCluster<MemKv> {
    /// Start a cluster on a fresh in-memory metadata store.
    pub async fn start(opts: ClusterOptions) -> Self {
        Self::start_with(Arc::new(MemKv::default()), opts).await
    }
}

/// Starter for the in-memory backend, for [`e2e_suite!`].
pub fn mem_starter() -> impl suite::Starter<MemKv> {
    |opts: ClusterOptions| -> futures::future::BoxFuture<'static, TestCluster<MemKv>> {
        Box::pin(TestCluster::start(opts))
    }
}

/// Starter for the in-memory backend with every node RPC over mutual TLS
/// (ADR-0017), for running the end-to-end suite authenticated.
pub fn mem_tls_starter() -> impl suite::Starter<MemKv> {
    |mut opts: ClusterOptions| -> futures::future::BoxFuture<'static, TestCluster<MemKv>> {
        opts.tls = true;
        Box::pin(TestCluster::start(opts))
    }
}

/// Starter for the in-memory backend wrapped in a fault injector, for
/// [`fault_suite!`].
pub fn fault_mem_starter() -> impl suite_faults::FaultStarter<MemKv> {
    |opts: ClusterOptions| -> futures::future::BoxFuture<'static, TestCluster<FaultKv<MemKv>>> {
        Box::pin(TestCluster::start_with(
            Arc::new(FaultKv::new(MemKv::default())),
            opts,
        ))
    }
}

impl<S: KvStore + 'static> TestCluster<S> {
    /// Start nodes, register them, create groups, start the gateway on an
    /// existing (empty) metadata store.
    ///
    /// # Panics
    ///
    /// On any setup failure (tests).
    #[allow(clippy::too_many_lines)]
    pub async fn start_with(meta: Arc<S>, opts: ClusterOptions) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let pki = opts.tls.then(|| pki::TestPki::new(&tmp.path().join("pki")));
        // The test's own clients act as an operator (admin role).
        let clients = NodeClients::new(Duration::from_secs(5)).with_tls(
            pki.as_ref()
                .map(|p| p.ctx(arca_lake_protocol::tls::Role::Admin, "test")),
        );
        let mut nodes = Vec::new();
        for i in 0..opts.nodes {
            let dir = tmp.path().join(format!("node{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            let mut cfg = NodeConfig::new(SocketAddr::from(([127, 0, 0, 1], 0)), &dir);
            cfg.fence_lease = TEST_FENCE_LEASE;
            cfg.store = node_store_config(&dir);
            cfg.capacity_bytes = Some(TEST_ALLOTMENT);
            let tls = pki.as_ref().map(|p| p.pending_node(&dir));
            cfg.tls = tls.clone();
            let handle = arca_lake_storage_node::serve(cfg).await.unwrap();
            let addr = handle.local_addr;
            let record = cluster::register_node(
                meta.as_ref(),
                &addr.to_string(),
                vec![
                    ("host".into(), format!("host{i}")),
                    ("rack".into(), format!("rack{}", i % 2)),
                    ("device_class".into(), "hdd".into()),
                ],
                vec![0],
                1,
            )
            .await
            .unwrap();
            if let (Some(p), Some(t)) = (pki.as_ref(), tls.as_ref()) {
                p.name_node(&dir, record.id, t);
            }
            nodes.push(TestNode {
                id: record.id,
                dir,
                addr,
                handle: Some(handle),
                tls,
                capacity: Some(TEST_ALLOTMENT),
            });
        }
        groups::create_groups(meta.as_ref(), &opts.profile, "default", opts.groups)
            .await
            .unwrap();
        // Cache nodes: device class nvme, one cache group each.
        for j in 0..opts.cache_nodes {
            let i = opts.nodes + j;
            let dir = tmp.path().join(format!("node{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            let mut cfg = NodeConfig::new(SocketAddr::from(([127, 0, 0, 1], 0)), &dir);
            cfg.fence_lease = TEST_FENCE_LEASE;
            cfg.store = node_store_config(&dir);
            cfg.capacity_bytes = Some(TEST_ALLOTMENT);
            let tls = pki.as_ref().map(|p| p.pending_node(&dir));
            cfg.tls = tls.clone();
            let handle = arca_lake_storage_node::serve(cfg).await.unwrap();
            let addr = handle.local_addr;
            let record = cluster::register_node(
                meta.as_ref(),
                &addr.to_string(),
                vec![
                    ("host".into(), format!("cache{j}")),
                    ("device_class".into(), "nvme".into()),
                ],
                vec![0],
                1,
            )
            .await
            .unwrap();
            if let (Some(p), Some(t)) = (pki.as_ref(), tls.as_ref()) {
                p.name_node(&dir, record.id, t);
            }
            nodes.push(TestNode {
                id: record.id,
                dir,
                addr,
                handle: Some(handle),
                tls,
                capacity: Some(TEST_ALLOTMENT),
            });
        }
        let cache = if opts.cache_nodes > 0 {
            groups::create_cache_groups(
                meta.as_ref(),
                CACHE_TIER,
                "nvme",
                &opts.cache_protection,
                "host",
                opts.cache_groups,
            )
            .await
            .unwrap();
            Some(
                opts.cache
                    .clone()
                    .unwrap_or_else(ClusterOptions::default_cache),
            )
        } else {
            None
        };

        let cluster_cfg = ClusterConfig {
            profile: opts.profile.clone(),
            tier: "default".into(),
            lease: opts.lease,
            chunk_batch: 16,
            shard_frame_bytes: 256 << 10,
            node_timeout: Duration::from_secs(5),
            rpc_policy: None,
            tls: pki
                .as_ref()
                .map(|p| p.ctx(arca_lake_protocol::tls::Role::Gateway, "gateway")),
            cache,
            read_parallelism: opts.read_parallelism,
            population_queue_depth: opts.population_queue_depth,
            populations_per_node: opts.populations_per_node,
            write_pipeline: opts.write_pipeline,
            multipart_min_part_bytes: opts.multipart_min_part_bytes,
            process_id: format!("gateway-test-{}", uuid::Uuid::new_v4().simple()),
            heartbeat: opts.heartbeat,
            // Test clusters run no controller unless a test spawns one.
            admission_without_controller: true,
        };
        let backend = ClusterBackend::new(Arc::clone(&meta), cluster_cfg.clone())
            .await
            .unwrap();
        // A test cluster runs no controller: the fence a leader would open
        // (ADR-0023), so that deletes go through (the gateway takes it
        // now rather than at its next refresh).
        if let Ok(rec) = arca_lake_metadata::ops::delete_fence::open_without_leader(
            meta.as_ref(),
            0,
            "test cluster",
            now_ms(),
        )
        .await
        {
            for n in &nodes {
                let _ = clients
                    .health_with(
                        &n.addr.to_string(),
                        arca_lake_protocol::storage::v1::HealthRequest {
                            delete_fence_generation: rec.generation,
                            delete_fence_open: true,
                            ..Default::default()
                        },
                    )
                    .await;
            }
            clients.set_delete_fence(Some(rec.generation));
            backend
                .node_clients()
                .set_delete_fence(Some(rec.generation));
        }
        // As a controller's: deletes carry the cluster incarnation.
        clients.set_incarnation(Some(backend.cluster().incarnation));
        let credentials = (
            format!("ARCATEST{}", uuid::Uuid::new_v4().simple()),
            uuid::Uuid::new_v4().simple().to_string(),
        );
        let mut keys: Vec<(&str, &str, &str)> =
            vec![(&credentials.0, &credentials.1, DEFAULT_PRINCIPAL)];
        for (ak, sk, name) in &opts.principals {
            keys.push((ak, sk, name));
        }
        let store = opts
            .credential_store
            .clone()
            .unwrap_or_else(|| Arc::new(CredentialStore::from_keys(&keys)));
        let cfg = GatewayConfig {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            credentials: None,
            store: Some(Arc::clone(&store)),
            audit: true,
            stats: None,
        };
        let gateway = if opts.authorize {
            serve_with_access(
                cfg,
                backend.clone(),
                Some(Authorizer::new(
                    Arc::clone(&meta),
                    Arc::clone(&store),
                    BucketCreation::Authenticated,
                )),
            )
            .await
            .unwrap()
        } else {
            serve(cfg, backend.clone()).await.unwrap()
        };
        let gateway_addr = gateway.local_addr;
        Self {
            meta,
            nodes,
            backend,
            gateway: Some(gateway),
            gateway_addr,
            credentials,
            store,
            clients,
            profile: opts.profile,
            durable_nodes: opts.nodes,
            page_size: 0,
            repair_options: repair::RepairOptions::default(),
            cluster_cfg,
            pki,
            tmp,
            fence_lease: TEST_FENCE_LEASE,
        }
    }

    /// Push the delete fence `rec` to every running node and take it for
    /// this cluster's clients (open: its generation; closed: none).
    pub async fn push_delete_fence(
        &self,
        rec: &arca_lake_metadata::ops::delete_fence::DeleteFenceRecord,
    ) {
        // As a controller of the cluster's incarnation pushes it (an
        // operator hold of that incarnation stays; ADR-0023).
        let incarnation = arca_lake_metadata::ops::compat::cluster_state(self.meta.as_ref())
            .await
            .ok()
            .flatten()
            .map(|s| s.incarnation);
        // As its opener: the same open generation from another opener is
        // a conflict the node closes (Phase 3.10).
        let opener = arca_lake_metadata::ops::delete_fence::opener_of(self.meta.as_ref())
            .await
            .ok()
            .filter(|o| rec.open && o.generation == rec.generation)
            .map_or(0, |o| o.opener);
        for n in self.nodes.iter().filter(|n| n.handle.is_some()) {
            let _ = self
                .clients
                .push_delete_fence_with(
                    &n.addr.to_string(),
                    rec.generation,
                    rec.open,
                    arca_lake_protocol::client::FencePush {
                        hold: rec.operator_hold && !rec.open,
                        release: false,
                        incarnation,
                        release_token: 0,
                        opener,
                        identity: None,
                    },
                )
                .await;
        }
        self.clients
            .set_delete_fence(rec.open.then_some(rec.generation));
    }

    /// Take the delete fence as recorded now (a test's controller may have
    /// opened or closed another) and push it to the running nodes.
    ///
    /// # Panics
    ///
    /// On metadata errors.
    pub async fn sync_delete_fence(&self) {
        let rec = arca_lake_metadata::ops::delete_fence::current(self.meta.as_ref())
            .await
            .unwrap();
        self.push_delete_fence(&rec).await;
        self.acknowledge_held_identities().await;
        self.stamp_identities().await;
    }

    /// As a leader's health pass does: a record not acknowledged yet whose
    /// token the node reports holding is acknowledged (metadata restored
    /// from before the acknowledgement, Phase 3.11). Nothing is adopted.
    pub async fn acknowledge_held_identities(&self) {
        use arca_lake_metadata::ops::node_lifecycle;
        let Ok(ids) = node_lifecycle::identities(self.meta.as_ref()).await else {
            return;
        };
        for n in self.nodes.iter().filter(|n| n.handle.is_some()) {
            let Some(rec) = ids.get(&n.id).filter(|r| !r.acknowledged) else {
                continue;
            };
            let Ok(h) = self.clients.health(&n.addr.to_string()).await else {
                continue;
            };
            if h.identity_node_id == n.id && h.identity_token.as_ref() == rec.token.as_slice() {
                let _ =
                    node_lifecycle::acknowledge_identity(self.meta.as_ref(), n.id, rec.token).await;
            }
        }
    }

    /// Every running node adopts its identity and it is recorded
    /// acknowledged, as a leader does (Phase 3.11): for tests that run no
    /// controller but need verifiable nodes (`admin restore`, removals).
    ///
    /// # Panics
    ///
    /// On metadata or node errors.
    pub async fn acknowledge_identities(&self) {
        use arca_lake_metadata::ops::node_lifecycle;
        for n in self.nodes.iter().filter(|n| n.handle.is_some()) {
            let rec = node_lifecycle::ensure_identity(self.meta.as_ref(), n.id)
                .await
                .unwrap();
            if rec.acknowledged {
                continue;
            }
            self.clients
                .health_with(
                    &n.addr.to_string(),
                    arca_lake_protocol::storage::v1::HealthRequest {
                        node_id: n.id,
                        node_identity: bytes::Bytes::copy_from_slice(&rec.token),
                        adopt_identity: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            node_lifecycle::acknowledge_identity(self.meta.as_ref(), n.id, rec.token)
                .await
                .unwrap();
        }
        self.stamp_identities().await;
    }

    /// Stamp this cluster's clients with the identities nodes acknowledged
    /// (as a leader does, Phase 3.11): a node holding one takes only
    /// deletes carrying it.
    pub async fn stamp_identities(&self) {
        let ids = arca_lake_metadata::ops::node_lifecycle::identities(self.meta.as_ref())
            .await
            .unwrap_or_default();
        for n in &self.nodes {
            self.clients.note_identity(
                &n.addr.to_string(),
                ids.get(&n.id)
                    .filter(|i| i.acknowledged)
                    .map(|i| (n.id, i.token)),
            );
        }
    }

    /// Advertise `bytes` as node `i`'s capacity (`None`: the file
    /// system's) from its next start; restarts it now if it runs.
    pub async fn set_node_capacity(&mut self, i: usize, bytes: Option<u64>) {
        self.nodes[i].capacity = bytes;
        if self.nodes[i].handle.is_some() {
            self.stop_node(i).await;
            self.start_node(i).await;
        }
    }

    /// The gateway's TLS context on a TLS cluster.
    #[must_use]
    pub fn gateway_tls(&self) -> Option<Arc<arca_lake_protocol::tls::TlsContext>> {
        self.cluster_cfg.tls.clone()
    }

    /// Re-issue node `i`'s certificate (same CA, new serial) and reload it
    /// in the running node: a leaf rotation.
    ///
    /// # Panics
    ///
    /// On a non-TLS cluster or failures.
    pub fn rotate_node_cert(&self, i: usize) {
        let n = &self.nodes[i];
        let (Some(p), Some(t)) = (self.pki.as_ref(), n.tls.as_ref()) else {
            panic!("not a TLS cluster");
        };
        p.name_node(&n.dir, n.id, t);
    }

    /// Replace `backend` with a freshly constructed one (a gateway restarted
    /// after a metadata restore picks up the new incarnation, RFC-015
    /// §5.3). The HTTP gateway keeps serving the old backend.
    ///
    /// # Errors
    ///
    /// The startup gate's refusal.
    pub async fn restart_backend(&mut self) -> Result<(), arca_lake_metadata::MetaError> {
        self.backend =
            ClusterBackend::new(Arc::clone(&self.meta), self.cluster_cfg.clone()).await?;
        Ok(())
    }

    /// The cluster incarnation the gateway serves (admission records a
    /// test publishes carry it).
    #[must_use]
    pub fn incarnation(&self) -> [u8; 16] {
        self.backend.cluster().incarnation
    }

    /// Another gateway backend on the same metadata and nodes (concurrent
    /// gateways).
    ///
    /// # Errors
    ///
    /// The startup gate's refusal.
    pub async fn extra_backend(&self) -> Result<ClusterBackend<S>, arca_lake_metadata::MetaError> {
        ClusterBackend::new(Arc::clone(&self.meta), self.cluster_cfg.clone()).await
    }

    /// Stop node `i` (its process is gone until [`TestCluster::start_node`]).
    pub async fn stop_node(&mut self, i: usize) {
        if let Some(h) = self.nodes[i].handle.take() {
            h.shutdown().await;
        }
        // Drop the gateway's cached channel so it reconnects.
        self.backend_clients_forget(i);
    }

    fn backend_clients_forget(&self, i: usize) {
        self.clients.forget(&self.nodes[i].addr.to_string());
    }

    /// Restart node `i` on the same address and directory.
    ///
    /// # Panics
    ///
    /// If the address cannot be re-bound.
    pub async fn start_node(&mut self, i: usize) {
        self.start_node_quiet(i).await;
        // A started node rejoins closed (Phase 3.10): push the recorded
        // fence, as a leader's next pass does.
        self.sync_delete_fence().await;
    }

    /// [`Self::start_node`] without pushing the delete fence: the node
    /// stays as it rejoins (closed) until something pushes it.
    ///
    /// # Panics
    ///
    /// When the node cannot be started.
    pub async fn start_node_quiet(&mut self, i: usize) {
        let n = &mut self.nodes[i];
        let mut cfg = NodeConfig::new(n.addr, &n.dir);
        cfg.fence_lease = self.fence_lease;
        cfg.store = node_store_config(&n.dir);
        cfg.tls = n.tls.clone();
        cfg.capacity_bytes = n.capacity;
        let mut last_err = None;
        for _ in 0..50 {
            match arca_lake_storage_node::serve(cfg.clone()).await {
                Ok(h) => {
                    n.handle = Some(h);
                    return;
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        panic!("could not restart node {i}: {last_err:?}");
    }

    /// Add a new node (registered, not yet in any group). Returns its index.
    ///
    /// # Panics
    ///
    /// On setup failure.
    pub async fn add_node(&mut self) -> usize {
        let i = self.nodes.len();
        let dir = self.tmp.path().join(format!("node{i}"));
        std::fs::create_dir_all(&dir).unwrap();
        let mut cfg = NodeConfig::new(SocketAddr::from(([127, 0, 0, 1], 0)), &dir);
        cfg.fence_lease = self.fence_lease;
        cfg.store = node_store_config(&dir);
        cfg.capacity_bytes = Some(TEST_ALLOTMENT);
        let tls = self.pki.as_ref().map(|p| p.pending_node(&dir));
        cfg.tls = tls.clone();
        let handle = arca_lake_storage_node::serve(cfg).await.unwrap();
        let addr = handle.local_addr;
        let record = cluster::register_node(
            self.meta.as_ref(),
            &addr.to_string(),
            vec![
                ("host".into(), format!("host{i}")),
                ("rack".into(), format!("rack{}", i % 2)),
                ("device_class".into(), "hdd".into()),
            ],
            vec![0],
            1,
        )
        .await
        .unwrap();
        if let (Some(p), Some(t)) = (self.pki.as_ref(), tls.as_ref()) {
            p.name_node(&dir, record.id, t);
        }
        self.nodes.push(TestNode {
            id: record.id,
            dir,
            addr,
            handle: Some(handle),
            tls,
            capacity: Some(TEST_ALLOTMENT),
        });
        self.sync_delete_fence().await;
        i
    }

    /// Whether node `i` is running.
    #[must_use]
    pub fn node_running(&self, i: usize) -> bool {
        self.nodes[i].handle.is_some()
    }

    /// The running service of node `i` (counters, stores).
    ///
    /// # Panics
    ///
    /// If node `i` is stopped.
    #[must_use]
    pub fn node_service(&self, i: usize) -> Arc<arca_lake_storage_node::StorageNodeService> {
        Arc::clone(
            self.nodes[i]
                .handle
                .as_ref()
                .expect("node running")
                .service(),
        )
    }

    /// The store of a running node.
    ///
    /// # Panics
    ///
    /// If the node is stopped.
    pub fn node_store(&self, i: usize) -> Arc<Store> {
        Arc::clone(
            self.nodes[i]
                .handle
                .as_ref()
                .expect("node running")
                .service()
                .store(),
        )
    }

    /// Flip a byte in the payload of every shard of `chunk_id` on node `i`.
    /// Returns how many shards were damaged.
    ///
    /// # Panics
    ///
    /// On I/O errors.
    pub fn corrupt_chunk_on_node(&self, i: usize, group_id: u64, chunk_id: [u8; 16]) -> usize {
        use std::os::unix::fs::FileExt;
        let store = self.node_store(i);
        let mut n = 0;
        for key in store.keys_in_group(group_id) {
            if key.chunk_id != chunk_id {
                continue;
            }
            let loc = store.locate(&key).unwrap();
            let path = self.nodes[i]
                .dir
                .join(format!("seg-{:016x}.arca", loc.segment_id));
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let pos = loc.payload_offset() + u64::from(loc.payload_len) / 2;
            let mut b = [0u8; 1];
            f.read_exact_at(&mut b, pos).unwrap();
            f.write_all_at(&[b[0] ^ 0xFF], pos).unwrap();
            f.sync_all().unwrap();
            n += 1;
        }
        n
    }

    /// Count shards of `group_id` on node `i` via the store.
    #[must_use]
    pub fn shard_count(&self, i: usize, group_id: u64) -> usize {
        self.node_store(i).keys_in_group(group_id).len()
    }

    /// Shard keys of `group_id` on node `i`.
    #[must_use]
    pub fn shard_keys(&self, i: usize, group_id: u64) -> Vec<ShardKey> {
        self.node_store(i).keys_in_group(group_id)
    }

    /// All groups.
    ///
    /// # Panics
    ///
    /// On metadata errors.
    pub async fn groups(&self) -> Vec<GroupRecord> {
        cluster::list_groups(self.meta.as_ref()).await.unwrap()
    }

    /// Index of the node that holds `member`.
    #[must_use]
    pub fn node_index(&self, member: &GroupMember) -> Option<usize> {
        self.nodes.iter().position(|n| n.id == member.node_id)
    }

    /// Run one GC pass at `now_ms`.
    ///
    /// # Panics
    ///
    /// On errors.
    pub async fn gc(&self, now_ms: u64, grace_ms: u64) -> gc::GcReport {
        self.sync_delete_fence().await;
        gc::run_once(self.meta.as_ref(), &self.clients, now_ms, grace_ms, 1000)
            .await
            .unwrap()
    }

    /// Reconcile every running node against metadata (orphan shards).
    ///
    /// # Panics
    ///
    /// On errors.
    pub async fn reconcile(&self, now_ms: u64, min_age_ms: u64) -> usize {
        self.sync_delete_fence().await;
        let mut deleted = 0;
        for g in self.groups().await {
            for n in &self.nodes {
                if n.handle.is_none() {
                    continue;
                }
                deleted += gc::reconcile_node(
                    self.meta.as_ref(),
                    &self.clients,
                    n.id,
                    &n.addr.to_string(),
                    &g,
                    now_ms,
                    min_age_ms,
                    self.page_size,
                )
                .await
                .unwrap()
                .deleted();
            }
        }
        deleted
    }

    /// [`TestCluster::repair`] with a hook at every repair step; returns the
    /// controller's result instead of panicking.
    ///
    /// # Errors
    ///
    /// Whatever the controller or the hook returns.
    pub async fn repair_with(
        &self,
        group_id: u64,
        index: usize,
        new_node: usize,
        hook: impl AsyncFnMut(repair::RepairStep) -> Result<(), arca_lake_controller::ControlError>,
    ) -> Result<repair::RepairReport, arca_lake_controller::ControlError> {
        let member = GroupMember {
            node_id: self.nodes[new_node].id,
            device_id: 0,
        };
        repair::replace_member_with(
            self.meta.as_ref(),
            &self.clients,
            group_id,
            index,
            member,
            &self.repair_options,
            hook,
        )
        .await
    }

    /// Replace member `index` of `group_id` with node `new_node`.
    ///
    /// # Panics
    ///
    /// On errors.
    pub async fn repair(
        &self,
        group_id: u64,
        index: usize,
        new_node: usize,
    ) -> repair::RepairReport {
        self.repair_with(group_id, index, new_node, async |_| Ok(()))
            .await
            .unwrap()
    }

    /// AWS CLI runner against this cluster's gateway.
    #[must_use]
    pub fn aws_cli(&self) -> s3::AwsCli {
        s3::AwsCli {
            endpoint: format!("http://{}", self.gateway_addr),
            access_key: self.credentials.0.clone(),
            secret_key: self.credentials.1.clone(),
            config_file: None,
        }
    }

    /// AWS CLI runner signing with other credentials.
    #[must_use]
    pub fn aws_cli_as(&self, access_key: &str, secret_key: &str) -> s3::AwsCli {
        s3::AwsCli {
            endpoint: format!("http://{}", self.gateway_addr),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            config_file: None,
        }
    }

    /// Manifest of the current version of `bucket/key`.
    ///
    /// # Panics
    ///
    /// If the object does not exist.
    pub async fn manifest(&self, bucket: &str, key: &str) -> object::Manifest {
        object::read_manifest(self.meta.as_ref(), bucket, key, None)
            .await
            .unwrap()
            .expect("object exists")
    }

    /// Add a cache node (device class `nvme`, its own host label) and
    /// register it. It joins no group until a cache repair or group
    /// creation names it.
    ///
    /// # Panics
    ///
    /// On errors.
    pub async fn add_cache_node(&mut self) -> usize {
        let i = self.nodes.len();
        let dir = self.tmp.path().join(format!("node{i}"));
        std::fs::create_dir_all(&dir).unwrap();
        let mut cfg = NodeConfig::new(SocketAddr::from(([127, 0, 0, 1], 0)), &dir);
        cfg.fence_lease = self.fence_lease;
        cfg.store = node_store_config(&dir);
        cfg.capacity_bytes = Some(TEST_ALLOTMENT);
        let tls = self.pki.as_ref().map(|p| p.pending_node(&dir));
        cfg.tls = tls.clone();
        let handle = arca_lake_storage_node::serve(cfg).await.unwrap();
        let addr = handle.local_addr;
        let record = cluster::register_node(
            self.meta.as_ref(),
            &addr.to_string(),
            vec![
                ("host".into(), format!("cache{i}")),
                ("device_class".into(), "nvme".into()),
            ],
            vec![0],
            1,
        )
        .await
        .unwrap();
        if let (Some(p), Some(t)) = (self.pki.as_ref(), tls.as_ref()) {
            p.name_node(&dir, record.id, t);
        }
        self.nodes.push(TestNode {
            id: record.id,
            dir,
            addr,
            handle: Some(handle),
            tls,
            capacity: Some(TEST_ALLOTMENT),
        });
        i
    }

    /// Indexes of the cache nodes.
    #[must_use]
    pub fn cache_node_indexes(&self) -> Vec<usize> {
        (self.durable_nodes..self.nodes.len()).collect()
    }

    /// Cache placements of the current version of `bucket/key`.
    ///
    /// # Panics
    ///
    /// If the object does not exist.
    pub async fn cache_placements(
        &self,
        bucket: &str,
        key: &str,
    ) -> Vec<(u32, arca_lake_metadata::records::CachePlacement)> {
        self.manifest(bucket, key).await.cache
    }

    /// Wait until `n` chunks of `bucket/key` are recorded as cached, or panic
    /// after `timeout`.
    ///
    /// # Panics
    ///
    /// On timeout.
    pub async fn wait_cached(&self, bucket: &str, key: &str, n: usize, timeout: Duration) {
        let start = std::time::Instant::now();
        loop {
            if self.cache_placements(bucket, key).await.len() >= n {
                return;
            }
            assert!(
                start.elapsed() < timeout,
                "{bucket}/{key}: fewer than {n} cached chunks after {timeout:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Reconcile every running cache node (orphan copies).
    ///
    /// # Panics
    ///
    /// On errors.
    pub async fn reconcile_cache(&self) -> usize {
        self.sync_delete_fence().await;
        let mut deleted = 0;
        for g in self.groups().await {
            if !g.is_cache() {
                continue;
            }
            for n in &self.nodes {
                if n.handle.is_none() || !g.members.iter().any(|m| m.node_id == n.id) {
                    continue;
                }
                deleted += gc::reconcile_node(
                    self.meta.as_ref(),
                    &self.clients,
                    n.id,
                    &n.addr.to_string(),
                    &g,
                    now_ms(),
                    0,
                    self.page_size,
                )
                .await
                .unwrap()
                .deleted();
            }
        }
        deleted
    }

    /// Total shards stored on all running nodes across all durable groups
    /// (cache copies are not counted).
    pub async fn total_shards(&self) -> usize {
        let mut n = 0;
        for g in self.groups().await {
            if g.is_cache() {
                continue;
            }
            for (i, node) in self.nodes.iter().enumerate() {
                if node.handle.is_some() {
                    n += self.shard_count(i, g.id);
                }
            }
        }
        n
    }

    /// Shut everything down.
    pub async fn shutdown(mut self) {
        if let Some(g) = self.gateway.take() {
            g.shutdown().await;
        }
        for n in &mut self.nodes {
            if let Some(h) = n.handle.take() {
                h.shutdown().await;
            }
        }
    }
}

/// Current time in Unix milliseconds.
#[must_use]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

/// Deterministic pseudo-random bytes.
#[must_use]
pub fn random_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            u8::try_from(x >> 56).unwrap_or(0)
        })
        .collect()
}
