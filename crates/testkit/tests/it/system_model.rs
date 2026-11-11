//! Model-based test of the whole data path (Phase 3.8, H5/H6): random
//! sequences of client operations (PUT, overwrite, DELETE, multipart
//! complete and abort) interleaved with every background process that
//! moves or removes data (GC, multipart sweep, reconciliation, compaction,
//! shard loss with scrub and repair, chunk migration between groups with
//! crashes before and after the switch, cache population and eviction,
//! a former controller leader's late GC), run on an in-memory cluster and
//! on a reference model (key → bytes) at once.
//!
//! After every step every key reads back what the model says (whole
//! bytes, or `NoSuchKey`). At the end, once everything is collected, the
//! cluster holds exactly the shards of the live objects (no referenced
//! shard was deleted, every orphan converged) and no node holds capacity.
//!
//! Reproduction: each run's seed is printed on failure and appended to
//! `$CARGO_TARGET_TMPDIR/system-model-failures.txt`; `ARCA_MODEL_SEED=<n>`
//! replays one, `ARCA_MODEL_RUNS` and `ARCA_MODEL_STEPS` (default 8 and
//! 80) set the volume.

#![allow(clippy::too_many_lines, clippy::cast_possible_truncation)]

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use arca_lake_controller::integrity::{
    RepairWorkerOptions, ScrubOptions, coders, repair_pass, scrub_group_now,
};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::compat;
use arca_lake_metadata::records::ClusterPhase;
use arca_lake_metadata::records::GroupRecord;
use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_testkit::s3::{self, PutOptions, request};
use arca_lake_testkit::suite::Starter;
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use arca_lake_tiering::durable::Coders;
use arca_lake_tiering::migrate::{MigrateFault, migrate_chunk};
use bytes::Bytes;
use s3s::dto::{
    AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletedMultipartUpload,
    CompletedPart, CreateMultipartUploadInput, ETag, StreamingBlob, UploadPartInput,
};
use s3s::{S3, S3ErrorCode};

const BUCKET: &str = "model";
const KEYS: usize = 8;
const CHUNK: u64 = 256 << 10;

/// xorshift64*: deterministic from the seed, no dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Put,
    Delete,
    MultipartComplete,
    MultipartAbort,
    Gc,
    Reconcile,
    Compact,
    LoseShardAndRepair,
    Migrate(Option<MigrateFault>),
    CacheReadAndEvict,
    FormerLeaderGc,
    /// Take a backup of the metadata (one pending at a time).
    Snapshot,
    /// Restore the pending backup through the recovery procedure (close
    /// the fence everywhere, restore, deliver every held delete, recover,
    /// a new leader reopens); the model goes back to the backup's.
    Restore,
    /// `Active` → `ReadOnly` (fence closed) → held deletes delivered →
    /// `Active`, a new leader term.
    PhaseFlip,
    /// A new leader term (a new fence generation).
    LeaderChange,
    /// A storage node stops and starts.
    NodeRestart,
    /// Held deletes delivered, in shuffled order.
    DeliverDelayed,
    /// Restore the pending backup through `admin restore` (Phase 3.11):
    /// every held delete is delivered in the middle of the restore.
    ManagedRestore,
    /// `admin restore` attempted with a node down: refused before anything
    /// changes (Phase 3.11).
    UnsafeRestoreAttempt,
    /// A member move interrupted after raising the epoch, then settled as
    /// a leader does (Phase 3.11, M2).
    EpochChangeCrash,
    /// The spare node decommissioned, unregistered (a delete to it is
    /// refused), and rejoined (Phase 3.11, R-100).
    NodeRetireRejoin,
}

fn pick(rng: &mut Rng) -> Op {
    match rng.below(31) {
        20 => return Op::Snapshot,
        21 => return Op::Restore,
        22 => return Op::PhaseFlip,
        23 => return Op::LeaderChange,
        24 => return Op::NodeRestart,
        25..=26 => return Op::DeliverDelayed,
        27 => return Op::ManagedRestore,
        28 => return Op::UnsafeRestoreAttempt,
        29 => return Op::EpochChangeCrash,
        30 => return Op::NodeRetireRejoin,
        _ => {}
    }
    match rng.below(20) {
        0..=5 => Op::Put,
        6..=7 => Op::Delete,
        8 => Op::MultipartComplete,
        9 => Op::MultipartAbort,
        10..=11 => Op::Gc,
        12 => Op::Reconcile,
        13 => Op::Compact,
        14 => Op::LoseShardAndRepair,
        15 => Op::Migrate(None),
        16 => Op::Migrate(Some(MigrateFault::BeforeSwitch)),
        17 => Op::Migrate(Some(MigrateFault::AfterSwitch)),
        18 => Op::CacheReadAndEvict,
        _ => Op::FormerLeaderGc,
    }
}

fn etag(e: Option<&ETag>) -> String {
    match e {
        Some(ETag::Strong(s) | ETag::Weak(s)) => s.clone(),
        None => String::new(),
    }
}

async fn multipart<S: KvStore + 'static>(
    c: &TestCluster<S>,
    key: &str,
    parts: &[Bytes],
    complete: bool,
) {
    let id = c
        .backend
        .create_multipart_upload(request(CreateMultipartUploadInput {
            bucket: BUCKET.into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let mut done = Vec::new();
    for (i, p) in parts.iter().enumerate() {
        let n = i32::try_from(i + 1).unwrap();
        let out = c
            .backend
            .upload_part(request(UploadPartInput {
                bucket: BUCKET.into(),
                key: key.into(),
                upload_id: id.clone(),
                part_number: n,
                body: Some(StreamingBlob::from_bytes(p.clone())),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        done.push(CompletedPart {
            part_number: Some(n),
            e_tag: Some(ETag::Strong(etag(out.e_tag.as_ref()))),
            ..Default::default()
        });
    }
    if complete {
        c.backend
            .complete_multipart_upload(request(CompleteMultipartUploadInput {
                bucket: BUCKET.into(),
                key: key.into(),
                upload_id: id,
                multipart_upload: Some(CompletedMultipartUpload { parts: Some(done) }),
                ..Default::default()
            }))
            .await
            .unwrap();
    } else {
        c.backend
            .abort_multipart_upload(request(AbortMultipartUploadInput {
                bucket: BUCKET.into(),
                key: key.into(),
                upload_id: id,
                ..Default::default()
            }))
            .await
            .unwrap();
    }
}

fn scrub_opts() -> ScrubOptions {
    ScrubOptions {
        page: 256,
        page_bytes: 0,
        rate_bytes_per_s: 0,
        interval_ms: 1,
        concurrency: 1,
        adaptive: None,
        manifest_roots_per_pass: 0,
    }
}

async fn ec_groups<S: KvStore + 'static>(c: &TestCluster<S>) -> Vec<GroupRecord> {
    let mut g: Vec<GroupRecord> = c
        .groups()
        .await
        .into_iter()
        .filter(|g| !g.is_cache() && g.scheme == "rs-gf8-v1")
        .collect();
    g.sort_by_key(|g| g.id);
    g
}

/// Every chunk of the current version of `key` (a multipart object's are
/// in its parts' generations): `(generation, index, record)`.
async fn object_chunks<S: KvStore + 'static>(
    c: &TestCluster<S>,
    key: &str,
) -> Vec<(
    arca_lake_metadata::records::GenId,
    u32,
    arca_lake_metadata::records::ChunkRecord,
)> {
    let m = c.manifest(BUCKET, key).await;
    if m.parts.is_empty() {
        return m
            .chunks
            .into_iter()
            .map(|(i, r)| (m.pointer.generation, i, r))
            .collect();
    }
    let mut out = Vec::new();
    for p in &m.parts {
        let (chunks, _) =
            arca_lake_metadata::ops::object::read_part_manifest(c.meta.as_ref(), &p.generation)
                .await
                .unwrap()
                .unwrap();
        out.extend(chunks.into_iter().map(|(i, r)| (p.generation, i, r)));
    }
    out
}

/// A decided shard delete: node address, shard, group epoch.
type Decided = (String, ShardRef, u64);

/// A backup: the metadata and the model as they were.
type Snapshot = (Vec<(Vec<u8>, Vec<u8>)>, BTreeMap<String, Bytes>);

/// A controller as it was when it decided (its own incarnation and fence).
fn frozen<S: KvStore + 'static>(c: &TestCluster<S>) -> NodeClients {
    let f = NodeClients::new(std::time::Duration::from_secs(5)).with_tls(c.clients.tls().cloned());
    f.set_incarnation(c.clients.incarnation());
    f.set_delete_fence(c.clients.delete_fence());
    f
}

/// The shard deletes GC would send for `key`'s current version.
async fn decided_deletes<S: KvStore + 'static>(
    c: &TestCluster<S>,
    key: &str,
) -> Vec<(String, ShardRef, u64)> {
    let groups = c.groups().await;
    let mut out = Vec::new();
    for (_, _, rec) in object_chunks(c, key).await {
        let Some(g) = groups.iter().find(|g| g.id == rec.placement.group_id) else {
            continue;
        };
        for (i, m) in g.members.iter().enumerate() {
            if let Some(n) = c.node_index(m) {
                out.push((
                    c.nodes[n].addr.to_string(),
                    ShardRef {
                        group_id: g.id,
                        chunk_id: rec.chunk_id,
                        shard_index: u16::try_from(i).unwrap(),
                    },
                    g.epoch,
                ));
            }
        }
    }
    out
}

/// A leader's health pass, the fence part (sync for term `token`, push).
async fn leader_pass<S: KvStore + 'static>(c: &TestCluster<S>, token: u64) {
    let inc = compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap()
        .incarnation;
    let mut required = 0;
    for n in &c.nodes {
        if let Ok(h) = c.clients.health(&n.addr.to_string()).await {
            required = required.max(h.delete_fence_generation + u64::from(!h.delete_fence_open));
        }
    }
    let rec = arca_lake_metadata::ops::delete_fence::sync_for_term(
        c.meta.as_ref(),
        token,
        inc,
        required,
        now_ms(),
    )
    .await
    .unwrap()
    .record;
    c.push_delete_fence(&rec).await;
}

async fn metadata_snapshot<S: arca_lake_metadata::KvStore>(meta: &S) -> Vec<(Vec<u8>, Vec<u8>)> {
    use arca_lake_metadata::Txn as _;
    arca_lake_metadata::transact(meta, (), |tx, ()| {
        Box::pin(async move {
            Ok(tx
                .get_range(&arca_lake_metadata::keys::all_range(), usize::MAX, false)
                .await?
                .into_iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
        })
    })
    .await
    .unwrap()
}

/// `fdbrestore`: the key space replaced by `snap`.
async fn metadata_restore<S: arca_lake_metadata::KvStore>(meta: &S, snap: &[(Vec<u8>, Vec<u8>)]) {
    use arca_lake_metadata::Txn as _;
    let snap = snap.to_vec();
    arca_lake_metadata::transact(meta, snap, |tx, snap| {
        Box::pin(async move {
            tx.clear_range(&arca_lake_metadata::keys::all_range());
            for (k, v) in snap {
                tx.set(k, v);
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// After a restore: `recovery begin` (close with the hold, mark), the
/// verification passes, finalize. Returns the new incarnation.
async fn model_recover<S: KvStore + 'static>(c: &TestCluster<S>) -> [u8; 16] {
    use arca_lake_controller::recovery::{RecoveryOptions, recovery_pass};
    use arca_lake_metadata::guard::{GuardedKv, OpClass};
    let closed = arca_lake_controller::delete_fence::close_and_push(
        c.meta.as_ref(),
        &c.clients,
        "recovery begin",
        true,
        now_ms(),
    )
    .await
    .unwrap();
    assert_eq!(closed.not_acknowledged(), Vec::<u64>::new());
    let (seq, _) =
        arca_lake_controller::delete_fence::max_incarnation_seq(c.meta.as_ref(), &c.clients)
            .await
            .unwrap();
    let marked = compat::mark_restoring_above(c.meta.as_ref(), None, None, "model", now_ms(), seq)
        .await
        .unwrap();
    let admin = GuardedKv::new(
        std::sync::Arc::clone(&c.meta),
        marked.incarnation,
        OpClass::Admin,
    );
    for _ in 0..30 {
        if recovery_pass(
            &admin,
            &c.clients,
            &RecoveryOptions::default(),
            None,
            "model",
            now_ms(),
        )
        .await
        .unwrap()
        .finished
        {
            break;
        }
    }
    let (v, _) = arca_lake_metadata::ops::recovery::finalize(
        c.meta.as_ref(),
        &arca_lake_metadata::ops::recovery::FinalizeRequest {
            incarnation: marked.incarnation,
            operator: "model".into(),
            reason: "model restore".into(),
            force: true,
            now_ms: now_ms(),
        },
    )
    .await
    .unwrap();
    assert_eq!(v.refusal, None, "{v:?}");
    marked.incarnation
}

/// `admin restore`'s restorer in the model: the held deletes arrive in
/// the middle of the restore (between the clear and the write-back), then
/// the backup's metadata is written.
struct ModelRestorer<S> {
    meta: std::sync::Arc<S>,
    snap: Vec<(Vec<u8>, Vec<u8>)>,
    delayed: std::sync::Mutex<Vec<(NodeClients, Vec<Decided>)>>,
}

impl<S: KvStore + 'static> arca_lake_controller::managed_restore::Restorer for ModelRestorer<S> {
    fn destination(&self) -> String {
        "model store".into()
    }

    async fn restore(&self, _source: &str, _version: u64) -> Result<(), String> {
        let held = std::mem::take(&mut *self.delayed.lock().unwrap());
        for (who, deletes) in held {
            for (addr, shard, epoch) in deletes {
                let _ = who.delete_shard(&addr, shard, epoch).await;
            }
        }
        metadata_restore(self.meta.as_ref(), &self.snap).await;
        Ok(())
    }
}

/// Every object of the model has, for every chunk, at least its data
/// shards on its group's members (durable data intact, whatever a cache
/// copy still serves; Phase 3.11).
async fn durable_intact<S: KvStore + 'static>(
    c: &TestCluster<S>,
    model: &BTreeMap<String, Bytes>,
    seed: u64,
    step: usize,
) {
    let groups = c.groups().await;
    for k in model.keys() {
        for (_, _, rec) in object_chunks(c, k).await {
            let Some(g) = groups.iter().find(|g| g.id == rec.placement.group_id) else {
                continue;
            };
            let mut present = 0;
            for (i, m) in g.members.iter().enumerate() {
                if let Some(n) = c.node_index(m) {
                    present +=
                        usize::from(c.node_store(n).keys_in_group(g.id).iter().any(|sk| {
                            sk.chunk_id == rec.chunk_id && usize::from(sk.shard_index) == i
                        }));
                }
            }
            assert!(
                present >= usize::from(rec.data_shards),
                "seed {seed} step {step}: {k} has {present} of {} shards of a chunk on its members after a restore",
                rec.data_shards + rec.parity_shards
            );
        }
    }
}

/// Every key reads what the model says.
async fn check<S: KvStore + 'static>(
    c: &TestCluster<S>,
    model: &BTreeMap<String, Bytes>,
    seed: u64,
    step: usize,
) {
    for i in 0..KEYS {
        let k = format!("k{i}");
        match (model.get(&k), s3::get(&c.backend, BUCKET, &k).await) {
            (Some(v), Ok((got, _))) => {
                assert_eq!(&got, v, "seed {seed} step {step}: {k} reads other bytes");
            }
            (None, Err(e)) if *e.code() == S3ErrorCode::NoSuchKey => {}
            (want, got) => panic!(
                "seed {seed} step {step}: {k} expected {:?}, got {:?}",
                want.map(Bytes::len),
                got.map(|(b, _)| b.len())
            ),
        }
    }
}

async fn run<S: KvStore + 'static>(
    start: impl Starter<S>,
    seed: u64,
    steps: usize,
) -> BTreeMap<String, usize> {
    let mut rng = Rng(seed | 1);
    let mut c = start(ClusterOptions {
        groups: 2,
        multipart_min_part_bytes: CHUNK / 4,
        cache_nodes: 2,
        cache: Some(ClusterOptions::default_cache()),
        // Seven durable nodes for two groups of three: one spare.
        ..ClusterOptions::rs(7, 2, 1, CHUNK, 16 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut model: BTreeMap<String, Bytes> = BTreeMap::new();
    let nodes: HashMap<u64, String> = c.nodes.iter().map(|n| (n.id, n.addr.to_string())).collect();
    let addr_of = |id: u64| nodes.get(&id).cloned();
    let mut trace = Vec::new();
    // A pending backup (metadata, model) and the deletes decided but not
    // yet delivered (by a frozen controller each).
    let mut backup: Option<Snapshot> = None;
    let mut delayed: Vec<(NodeClients, Vec<Decided>)> = Vec::new();
    let mut token = 1000u64;
    for step in 0..steps {
        let mut op = pick(&mut rng);
        // A pending backup is restored soon (one step in six).
        if backup.is_some() && rng.below(6) == 0 {
            // Either path, as often (Phase 3.11): the runbook by hand, or
            // `admin restore`.
            op = if rng.below(2) == 0 {
                Op::Restore
            } else {
                Op::ManagedRestore
            };
        }
        // While a backup is pending nothing removes data (GC, reconcile,
        // moves, delayed deletes): what a restore expects back is then
        // exactly the backup's model; a removal before the fence closes
        // would be ordinary post-backup loss, not what is tested here.
        if backup.is_some()
            && matches!(
                op,
                Op::Gc
                    | Op::Reconcile
                    | Op::Migrate(_)
                    | Op::FormerLeaderGc
                    | Op::DeliverDelayed
                    | Op::PhaseFlip
                    | Op::LoseShardAndRepair
            )
        {
            op = Op::Compact;
        }
        trace.push(op);
        let mut key = format!("k{}", rng.below(KEYS as u64));
        // While a backup is pending, a delete targets an object the backup
        // holds unchanged half the time (Phase 3.11): what a stale delete
        // landing during a restore would destroy.
        if matches!(op, Op::Delete)
            && let Some((_, at_backup)) = &backup
            && rng.below(2) == 0
        {
            let same: Vec<&String> = at_backup
                .iter()
                .filter(|(k, v)| model.get(*k) == Some(*v))
                .map(|(k, _)| k)
                .collect();
            if !same.is_empty() {
                key = same[rng.below(same.len() as u64) as usize].clone();
            }
        }
        let size = match rng.below(4) {
            0 => 1 + rng.below(16 << 10),
            1 => CHUNK,
            _ => 1 + rng.below(3 * CHUNK),
        };
        let data = Bytes::from(random_bytes(size as usize, rng.next()));
        match op {
            Op::Put => {
                s3::put(
                    &c.backend,
                    BUCKET,
                    &key,
                    data.clone(),
                    &PutOptions::default(),
                )
                .await
                .unwrap();
                model.insert(key, data);
            }
            Op::Delete => {
                // GC decides on the deleted version's shards; the deletes
                // are held to be delivered late (or never).
                // Always within a backup window (what a restore brings back
                // is what a stale delete could destroy).
                if model.contains_key(&key) && (backup.is_some() || rng.below(2) == 0) {
                    delayed.push((frozen(&c), decided_deletes(&c, &key).await));
                }
                s3::delete(&c.backend, BUCKET, &key).await.unwrap();
                model.remove(&key);
            }
            Op::Snapshot => {
                if backup.is_none() {
                    backup = Some((metadata_snapshot(c.meta.as_ref()).await, model.clone()));
                }
            }
            Op::Restore => {
                if let Some((snap, before)) = backup.take() {
                    // Runbook: the fence closed everywhere, then restore.
                    let closed = arca_lake_controller::delete_fence::close_and_push(
                        c.meta.as_ref(),
                        &c.clients,
                        "model restore",
                        true,
                        now_ms(),
                    )
                    .await
                    .unwrap();
                    assert_eq!(closed.not_acknowledged(), Vec::<u64>::new(), "seed {seed}");
                    metadata_restore(c.meta.as_ref(), &snap).await;
                    // The race: every held delete arrives now.
                    for (who, deletes) in delayed.drain(..) {
                        for (addr, shard, epoch) in deletes {
                            let _ = who.delete_shard(&addr, shard, epoch).await;
                        }
                    }
                    let inc = model_recover(&c).await;
                    c.clients.set_incarnation(Some(inc));
                    c.restart_backend().await.unwrap();
                    token += 1;
                    leader_pass(&c, token).await;
                    model = before;
                    durable_intact(&c, &model, seed, step).await;
                }
            }
            Op::PhaseFlip => {
                compat::transition(
                    c.meta.as_ref(),
                    ClusterPhase::Active,
                    ClusterPhase::ReadOnly,
                    "model",
                    now_ms(),
                )
                .await
                .unwrap();
                arca_lake_controller::delete_fence::close_and_push(
                    c.meta.as_ref(),
                    &c.clients,
                    "model phase",
                    false,
                    now_ms(),
                )
                .await
                .unwrap();
                for (who, deletes) in delayed.drain(..) {
                    for (addr, shard, epoch) in deletes {
                        let _ = who.delete_shard(&addr, shard, epoch).await;
                    }
                }
                let (state, _) = compat::transition(
                    c.meta.as_ref(),
                    ClusterPhase::ReadOnly,
                    ClusterPhase::Active,
                    "model",
                    now_ms(),
                )
                .await
                .unwrap();
                // The gateway sees it now rather than at its next refresh.
                c.backend.cluster().set(Some(state));
                token += 1;
                leader_pass(&c, token).await;
            }
            Op::LeaderChange => {
                token += 1;
                leader_pass(&c, token).await;
            }
            Op::ManagedRestore => {
                if let Some((snap, before)) = backup.take() {
                    use arca_lake_controller::managed_restore as mr;
                    let dir = tempfile::tempdir().unwrap();
                    let journal = dir.path().join("journal");
                    let restorer = ModelRestorer {
                        meta: std::sync::Arc::clone(&c.meta),
                        snap,
                        delayed: std::sync::Mutex::new(std::mem::take(&mut delayed)),
                    };
                    let plan = mr::plan(
                        c.meta.as_ref(),
                        &c.clients,
                        "model://backup",
                        1,
                        None,
                        &mr::Restorer::destination(&restorer),
                        &journal,
                        now_ms(),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("seed {seed} step {step}: plan: {e}"));
                    mr::run(
                        &c.meta,
                        &c.clients,
                        &restorer,
                        &journal,
                        &plan.journal.token,
                        &[],
                        now_ms,
                    )
                    .await
                    .unwrap_or_else(|e| panic!("seed {seed} step {step}: run: {e}"));
                    // Finalized; no leader yet: then one opens a generation.
                    let _ = mr::finish(
                        c.meta.as_ref(),
                        &c.clients,
                        &journal,
                        "model",
                        "model",
                        true,
                        std::time::Duration::ZERO,
                        now_ms,
                    )
                    .await;
                    let inc = compat::cluster_state(c.meta.as_ref())
                        .await
                        .unwrap()
                        .unwrap()
                        .incarnation;
                    c.clients.set_incarnation(Some(inc));
                    c.restart_backend().await.unwrap();
                    token += 1;
                    leader_pass(&c, token).await;
                    mr::finish(
                        c.meta.as_ref(),
                        &c.clients,
                        &journal,
                        "model",
                        "model",
                        true,
                        std::time::Duration::from_secs(5),
                        now_ms,
                    )
                    .await
                    .unwrap_or_else(|e| panic!("seed {seed} step {step}: finish: {e}"));
                    model = before;
                    durable_intact(&c, &model, seed, step).await;
                }
            }
            Op::UnsafeRestoreAttempt => {
                // A node down: the managed restore does not start.
                let i = rng.below(7) as usize;
                c.stop_node(i).await;
                let dir = tempfile::tempdir().unwrap();
                let journal = dir.path().join("journal");
                let r = arca_lake_controller::managed_restore::plan(
                    c.meta.as_ref(),
                    &c.clients,
                    "model://backup",
                    1,
                    None,
                    "model store",
                    &journal,
                    now_ms(),
                )
                .await;
                assert!(
                    r.is_err() && !journal.exists(),
                    "seed {seed} step {step}: a restore was planned with a node down"
                );
                c.start_node(i).await;
            }
            Op::EpochChangeCrash => {
                // Member `index` of a durable group moved to a node outside
                // it; the move stops after raising the epoch; a leader then
                // settles it (the move no longer wanted).
                let groups = ec_groups(&c).await;
                let g = &groups[rng.below(groups.len() as u64) as usize];
                let index = rng.below(g.members.len() as u64) as usize;
                if let Some(n) = c
                    .nodes
                    .iter()
                    .find(|n| !g.members.iter().any(|m| m.node_id == n.id))
                {
                    let to = arca_lake_metadata::records::GroupMember {
                        node_id: n.id,
                        device_id: 0,
                    };
                    let _ = arca_lake_controller::repair::replace_member_with(
                        c.meta.as_ref(),
                        &c.clients,
                        g.id,
                        index,
                        to,
                        &arca_lake_controller::repair::RepairOptions::default(),
                        async |step| {
                            if step == arca_lake_controller::repair::RepairStep::Fenced {
                                Err(arca_lake_controller::ControlError::Other(
                                    "model crash".into(),
                                ))
                            } else {
                                Ok(())
                            }
                        },
                    )
                    .await;
                    for intent in arca_lake_metadata::ops::epoch_intent::open(c.meta.as_ref())
                        .await
                        .unwrap()
                    {
                        for m in &g.members {
                            if let Some(i) = c.node_index(m) {
                                let _ = c
                                    .clients
                                    .update_epoch(
                                        &c.nodes[i].addr.to_string(),
                                        g.id,
                                        intent.from_epoch + 1,
                                    )
                                    .await;
                            }
                        }
                        arca_lake_metadata::ops::epoch_intent::settle(c.meta.as_ref(), &intent)
                            .await
                            .unwrap();
                    }
                    c.backend.refresh_map().await.unwrap();
                }
            }
            Op::NodeRetireRejoin => {
                use arca_lake_metadata::ops::{membership, node_lifecycle};
                let groups = c.groups().await;
                let spare = c
                    .nodes
                    .iter()
                    .find(|n| {
                        !groups
                            .iter()
                            .any(|g| g.members.iter().any(|m| m.node_id == n.id))
                    })
                    .map(|n| (n.id, n.addr.to_string()));
                if let Some((id, addr)) = spare
                    && node_lifecycle::state(c.meta.as_ref(), id).await.unwrap()
                        == node_lifecycle::State::Active
                {
                    membership::drain_node(c.meta.as_ref(), id, "model", now_ms())
                        .await
                        .unwrap();
                    membership::update_drain(c.meta.as_ref(), id, Some(now_ms()), None)
                        .await
                        .unwrap();
                    let old = frozen(&c);
                    arca_lake_controller::node_ops::unregister(
                        c.meta.as_ref(),
                        &c.clients,
                        id,
                        "model",
                        "model",
                        false,
                        now_ms(),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("seed {seed} step {step}: unregister: {e}"));
                    let probe = ShardRef {
                        group_id: groups[0].id,
                        chunk_id: [0xee; 16],
                        shard_index: 0,
                    };
                    let r = old.delete_shard(&addr, probe, groups[0].epoch).await;
                    assert!(
                        r.as_ref().is_err_and(NodeClients::is_fenced),
                        "seed {seed} step {step}: an unregistered node took a delete: {r:?}"
                    );
                    // The node has acknowledged no identity in the model
                    // (no leader process gives one): it rejoins with a
                    // fresh one only if empty; it is.
                    arca_lake_controller::node_ops::rejoin(c.meta.as_ref(), &c.clients, id, true)
                        .await
                        .unwrap_or_else(|e| panic!("seed {seed} step {step}: rejoin: {e}"));
                    arca_lake_controller::delete_fence::release_and_push(
                        c.meta.as_ref(),
                        &c.clients,
                        "model rejoin",
                        now_ms(),
                    )
                    .await
                    .ok();
                    token += 1;
                    leader_pass(&c, token).await;
                }
            }
            Op::NodeRestart => {
                let i = rng.below(7) as usize;
                c.stop_node(i).await;
                c.start_node(i).await;
            }
            Op::DeliverDelayed => {
                // Reordered: a random batch, in shuffled order.
                let n = delayed.len();
                if n > 0 {
                    let (who, mut deletes) = delayed.swap_remove(rng.below(n as u64) as usize);
                    for i in (1..deletes.len()).rev() {
                        deletes.swap(i, rng.below(i as u64 + 1) as usize);
                    }
                    for (addr, shard, epoch) in deletes {
                        let _ = who.delete_shard(&addr, shard, epoch).await;
                    }
                }
            }
            Op::MultipartComplete | Op::MultipartAbort => {
                let parts: Vec<Bytes> = (0..=rng.below(3))
                    .map(|_| Bytes::from(random_bytes(CHUNK as usize / 2, rng.next())))
                    .collect();
                let complete = matches!(op, Op::MultipartComplete);
                multipart(&c, &key, &parts, complete).await;
                if complete {
                    model.insert(key, Bytes::from(parts.concat()));
                }
            }
            Op::Gc => {
                // Nothing is in flight between steps: an hour ahead is
                // safe and lets every retired generation go.
                let far = now_ms() + 3_600_000;
                c.gc(far, 0).await;
                arca_lake_controller::gc::sweep_multipart(c.meta.as_ref(), far, Some(far), 1000)
                    .await
                    .unwrap();
            }
            Op::Reconcile => {
                c.reconcile(now_ms() + 3_600_000, 0).await;
            }
            Op::Compact => {
                for i in 0..c.nodes.len() {
                    let s = c.node_store(i);
                    for seg in s.compaction_candidates(0.0).unwrap_or_default() {
                        let _ = s.compact(seg);
                    }
                }
            }
            Op::LoseShardAndRepair => {
                // One shard of a live object's chunk is lost on its node;
                // a scrub finds it and repair rebuilds it.
                if let Some(k) = model.keys().nth(rng.below(model.len() as u64) as usize) {
                    let chunks = object_chunks(&c, k).await;
                    let (_, _, rec) = &chunks[rng.below(chunks.len() as u64) as usize];
                    if rec.scheme == "rs-gf8-v1" {
                        let g = c
                            .groups()
                            .await
                            .into_iter()
                            .find(|g| g.id == rec.placement.group_id)
                            .unwrap();
                        let member = &g.members[rng.below(g.members.len() as u64) as usize];
                        let i = c.node_index(member).unwrap();
                        let store = c.node_store(i);
                        for sk in store.keys_in_group(g.id) {
                            if sk.chunk_id == rec.chunk_id {
                                store.delete(sk).unwrap();
                            }
                        }
                        scrub_group_now(
                            c.meta.as_ref(),
                            &c.clients,
                            &coders(),
                            g.id,
                            &scrub_opts(),
                            now_ms(),
                        )
                        .await
                        .unwrap();
                        // Sometimes with the smallest pages (Phase 3.11, M4):
                        // one task per pass, a window of one entry.
                        let small = rng.below(2) == 0;
                        let ro = RepairWorkerOptions {
                            tasks_per_pass: if small { 1 } else { 256 },
                            scan_window: if small { 1 } else { 4096 },
                            ..RepairWorkerOptions::default()
                        };
                        for _ in 0..if small { 8 } else { 1 } {
                            repair_pass(
                                c.meta.as_ref(),
                                &c.clients,
                                &coders(),
                                &ro,
                                None,
                                None,
                                now_ms(),
                            )
                            .await
                            .unwrap();
                        }
                    }
                }
            }
            Op::Migrate(fault) => {
                if let Some(k) = model.keys().nth(rng.below(model.len() as u64) as usize) {
                    let groups = ec_groups(&c).await;
                    let chunks = object_chunks(&c, k).await;
                    for (generation, index, rec) in
                        chunks.iter().filter(|(_, _, r)| r.scheme == "rs-gf8-v1")
                    {
                        let Some(src) = groups.iter().find(|g| g.id == rec.placement.group_id)
                        else {
                            continue;
                        };
                        let dst = groups.iter().find(|g| g.id != src.id).unwrap();
                        // Errors are the injected crashes.
                        let _ = migrate_chunk(
                            c.meta.as_ref(),
                            &c.clients,
                            &Coders::default(),
                            &addr_of,
                            src,
                            dst,
                            generation,
                            *index,
                            rec,
                            fault,
                        )
                        .await;
                    }
                    if fault.is_some() {
                        // The crashed attempt's intent expires later.
                        arca_lake_metadata::ops::object::expire_migrations(
                            c.meta.as_ref(),
                            now_ms() + 2 * arca_lake_metadata::ops::object::MIGRATION_INTENT_TTL_MS,
                        )
                        .await
                        .unwrap();
                    }
                }
            }
            Op::CacheReadAndEvict => {
                if let Some(k) = model.keys().next() {
                    // A miss populates the cache in the background.
                    let _ = s3::get(&c.backend, BUCKET, k).await;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                let cfg = arca_lake_tiering::cache::CacheTierConfig {
                    capacity_bytes: 1,
                    high_watermark: 0.0,
                    low_watermark: 0.0,
                    ..ClusterOptions::default_cache()
                };
                let map = c.backend.map();
                let addr = |node_id: u64| map.node_addr(node_id).map(str::to_string);
                arca_lake_tiering::cache::evict_tier(
                    c.meta.as_ref(),
                    &c.clients,
                    &cfg,
                    &map.groups,
                    &addr,
                )
                .await
                .unwrap();
            }
            Op::FormerLeaderGc => {
                // A leader whose lease lapsed runs a late GC pass: every
                // metadata write it makes is fenced.
                use arca_lake_metadata::fence::FencedKv;
                use arca_lake_metadata::ops::leader;
                let t = now_ms();
                let old = leader::try_acquire(c.meta.as_ref(), "old", t, 1)
                    .await
                    .unwrap();
                if let Some(old) = old {
                    leader::try_acquire(c.meta.as_ref(), "new", t + 10, 1)
                        .await
                        .unwrap();
                    let fenced = FencedKv::new(std::sync::Arc::clone(&c.meta), old);
                    let r = arca_lake_controller::gc::run_once(
                        &fenced,
                        &c.clients,
                        now_ms() + 3_600_000,
                        0,
                        1000,
                    )
                    .await;
                    if let Ok(r) = r {
                        assert_eq!(
                            r.collected, 0,
                            "seed {seed} step {step}: a fenced GC collected"
                        );
                    }
                }
            }
        }
        check(&c, &model, seed, step).await;
    }
    // Converge: collect everything collectable (the fence as recorded).
    drop(backup);
    c.sync_delete_fence().await;
    let far = now_ms() + 3_600_000;
    for _ in 0..3 {
        c.gc(far, 0).await;
        arca_lake_controller::gc::sweep_multipart(c.meta.as_ref(), far, Some(far), 1000)
            .await
            .unwrap();
    }
    arca_lake_metadata::ops::object::expire_migrations(
        c.meta.as_ref(),
        far + 2 * arca_lake_metadata::ops::object::MIGRATION_INTENT_TTL_MS,
    )
    .await
    .unwrap();
    c.reconcile(far, 0).await;
    let _ = c.reconcile_cache().await;
    check(&c, &model, seed, steps).await;
    // Exactly the live objects' shards on the durable groups.
    let mut expected = 0usize;
    for k in model.keys() {
        for (_, _, rec) in object_chunks(&c, k).await {
            expected += usize::from(rec.data_shards + rec.parity_shards);
        }
    }
    let durable: usize = {
        let groups: Vec<GroupRecord> = c
            .groups()
            .await
            .into_iter()
            .filter(|g| !g.is_cache())
            .collect();
        let mut n = 0;
        for g in &groups {
            for m in &g.members {
                if let Some(i) = c.node_index(m) {
                    n += c.shard_count(i, g.id);
                }
            }
        }
        n
    };
    assert_eq!(
        durable, expected,
        "seed {seed}: durable shards left {durable}, live objects need {expected}; trace {trace:?}"
    );
    // Background writes (cache populations) end; then nothing is held.
    for i in 0..c.nodes.len() {
        let t = Instant::now();
        while c.node_service(i).device_capacity().unwrap().inflight_bytes != 0 {
            assert!(
                t.elapsed() < std::time::Duration::from_secs(10),
                "seed {seed}: node {i} holds capacity"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
    c.shutdown().await;
    let mut counts = BTreeMap::new();
    for op in trace {
        *counts.entry(format!("{op:?}")).or_insert(0) += 1;
    }
    counts
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Run the model for the seeds the environment selects (`ARCA_MODEL_SEED`,
/// or `runs` seeds from the clock), at most until `budget` has passed
/// (no new seed is started after it); a failing seed is recorded in
/// `$CARGO_TARGET_TMPDIR/system-model-failures-<backend>.txt`.
async fn drive<S, St>(
    backend: &str,
    starter: impl Fn() -> St + Send + Copy + 'static,
    runs: u64,
    steps: usize,
    budget: std::time::Duration,
) where
    S: KvStore + 'static,
    St: Starter<S> + 'static,
{
    let seeds: Vec<u64> = if let Ok(s) = std::env::var("ARCA_MODEL_SEED") {
        vec![s.parse().expect("ARCA_MODEL_SEED")]
    } else {
        let base = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos() as u64);
        (0..runs).map(|i| base.wrapping_add(i * 7919)).collect()
    };
    let begun = Instant::now();
    for seed in seeds {
        if begun.elapsed() > budget {
            eprintln!("system model ({backend}): time budget spent, remaining seeds not run");
            break;
        }
        let clock = Instant::now();
        match tokio::spawn(async move { run(starter(), seed, steps).await }).await {
            Ok(counts) => eprintln!(
                "system model ({backend}) seed {seed}: {steps} steps in {:?}: {counts:?}",
                clock.elapsed()
            ),
            Err(e) => {
                let line = format!("{seed} steps {steps}\n");
                let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
                    .join(format!("system-model-failures-{backend}.txt"));
                let _ = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()));
                panic!(
                    "system model ({backend}) failed for seed {seed} (replay: ARCA_MODEL_SEED={seed}; recorded in {}): {e:?}",
                    path.display()
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_operation_sequences_agree_with_the_model() {
    drive(
        "mem",
        arca_lake_testkit::mem_starter,
        env("ARCA_MODEL_RUNS", 8),
        env("ARCA_MODEL_STEPS", 80) as usize,
        std::time::Duration::MAX,
    )
    .await;
}

/// The same model on FoundationDB (Phase 3.10): every metadata
/// transaction of the data path, the background processes and the
/// restore goes through a real cluster. Bounded for CI:
/// `ARCA_MODEL_FDB_RUNS` (default 3) seeds of `ARCA_MODEL_FDB_STEPS`
/// (default 40) steps, no new seed after `ARCA_MODEL_FDB_SECS` (default
/// 600). Every operation of the in-memory model runs here too.
#[cfg(feature = "fdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_operation_sequences_agree_with_the_model_on_foundationdb() {
    if arca_lake_testkit::fdb::starter().is_none() {
        eprintln!("ARCA_FDB_TESTS / ARCA_FDB_CLUSTER_FILE not set; skipping");
        return;
    }
    drive(
        "fdb",
        || arca_lake_testkit::fdb::starter().expect("FoundationDB starter"),
        env("ARCA_MODEL_FDB_RUNS", 3),
        env("ARCA_MODEL_FDB_STEPS", 40) as usize,
        std::time::Duration::from_secs(env("ARCA_MODEL_FDB_SECS", 600)),
    )
    .await;
}
