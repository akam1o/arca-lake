//! Concurrent writes to one hot key (R-112, ADR-0041): through one gateway
//! they take turns and are all acknowledged; through several, the order of
//! versions follows the order of acknowledgements, no acknowledged write is
//! lost, and a write answered `SlowDown` is never applied.
//!
//! The in-memory store applies a commit at once, so concurrent transactions
//! seldom overlap; [`SlowKv`] holds every commit for a fixed time first (a
//! store's commit latency), so that transactions that read the same key
//! overlap and conflict as they do on FoundationDB.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arca_lake_metadata::fault::{CommitFault, FaultKv};
use arca_lake_metadata::keys;
use arca_lake_metadata::kv::{Key, KeyRange, KvError, KvStore, Limits, Txn, Value};
use arca_lake_metadata::mem::MemKv;
use arca_lake_testkit::s3::{self, request};
use arca_lake_testkit::{ClusterOptions, TestCluster};
use bytes::Bytes;
use s3s::S3;
use s3s::dto::{
    BucketVersioningStatus, CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadInput, ETag, GetObjectInput, ListObjectVersionsInput,
    PutBucketVersioningInput, PutObjectInput, StreamingBlob, UploadPartInput,
    VersioningConfiguration,
};

const MIB: u64 = 1 << 20;
const BUCKET: &str = "hot";
const KEY: &str = "k";
/// Commit latency of [`SlowKv`].
const COMMIT_LATENCY: Duration = Duration::from_millis(3);

/// A store whose commits each take [`COMMIT_LATENCY`] before they are
/// applied, counting the conflicts of commits that write `watched`.
struct SlowKv<S> {
    inner: S,
    watched: Arc<Mutex<Option<Key>>>,
    conflicts: Arc<AtomicU64>,
}

impl<S> SlowKv<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            watched: Arc::default(),
            conflicts: Arc::default(),
        }
    }

    fn watch(&self, key: Key) {
        *self.watched.lock().unwrap() = Some(key);
    }

    fn conflicts(&self) -> u64 {
        self.conflicts.load(Relaxed)
    }
}

struct SlowTx<T> {
    inner: T,
    writes_watched: bool,
    watched: Option<Key>,
    conflicts: Arc<AtomicU64>,
}

impl<S: KvStore> KvStore for SlowKv<S>
where
    S::Tx: 'static,
{
    type Tx = SlowTx<S::Tx>;

    async fn begin(&self) -> Result<Self::Tx, KvError> {
        Ok(SlowTx {
            inner: self.inner.begin().await?,
            writes_watched: false,
            watched: self.watched.lock().unwrap().clone(),
            conflicts: Arc::clone(&self.conflicts),
        })
    }

    fn limits(&self) -> Limits {
        self.inner.limits()
    }
}

impl<T: Txn + 'static> SlowTx<T> {
    fn note(&mut self, key: &[u8]) {
        if self.watched.as_deref() == Some(key) {
            self.writes_watched = true;
        }
    }
}

impl<T: Txn + 'static> Txn for SlowTx<T> {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.inner.get(key).await
    }

    async fn get_snapshot(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.inner.get_snapshot(key).await
    }

    async fn get_many(&mut self, keys: &[Key]) -> Result<Vec<Option<Value>>, KvError> {
        self.inner.get_many(keys).await
    }

    async fn get_first_many(
        &mut self,
        ranges: &[KeyRange],
    ) -> Result<Vec<Option<(Key, Value)>>, KvError> {
        self.inner.get_first_many(ranges).await
    }

    async fn atomic_add(&mut self, key: &[u8], delta: i64) -> Result<(), KvError> {
        self.note(key);
        self.inner.atomic_add(key, delta).await
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
        self.note(key);
        self.inner.set(key, value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.note(key);
        self.inner.clear(key);
    }

    fn clear_range(&mut self, range: &KeyRange) {
        if self.watched.as_ref().is_some_and(|k| range.contains(k)) {
            self.writes_watched = true;
        }
        self.inner.clear_range(range);
    }

    async fn commit(self) -> Result<u64, KvError> {
        tokio::time::sleep(COMMIT_LATENCY).await;
        let r = self.inner.commit().await;
        if self.writes_watched && matches!(r, Err(KvError::Conflict)) {
            self.conflicts.fetch_add(1, Relaxed);
        }
        r
    }
}

fn body(n: usize) -> Bytes {
    Bytes::from(format!("put-{n:05}-").repeat(100))
}

async fn enable_versioning(s3: &impl S3) {
    let mut vb = PutBucketVersioningInput::builder();
    vb.set_bucket(BUCKET.into());
    vb.set_versioning_configuration(VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(
            BucketVersioningStatus::ENABLED,
        )),
        ..Default::default()
    });
    s3.put_bucket_versioning(request(vb.build().unwrap()))
        .await
        .unwrap();
}

async fn watch_current<S: KvStore + 'static>(c: &TestCluster<SlowKv<S>>) {
    let b = arca_lake_metadata::ops::bucket::get_bucket(c.meta.as_ref(), BUCKET)
        .await
        .unwrap();
    c.meta.watch(keys::object_current(b.id, KEY));
}

/// Concurrent PUTs to one key through one gateway are all acknowledged:
/// they take turns instead of conflicting until the transaction loop gives
/// up (before ADR-0041 about 30 % of them were answered `SlowDown`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_puts_to_one_key_through_one_gateway_are_all_acknowledged() {
    let c: TestCluster<SlowKv<MemKv>> = TestCluster::start_with(
        Arc::new(SlowKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    watch_current(&c).await;
    let puts = (0..128).map(|n| {
        let s3 = c.backend.clone();
        tokio::spawn(
            async move { s3::put(&s3, BUCKET, KEY, body(n), &s3::PutOptions::default()).await },
        )
    });
    let results = futures::future::join_all(puts).await;
    let failed: Vec<_> = results
        .into_iter()
        .filter_map(|r| r.unwrap().err())
        .map(|e| format!("{:?}: {:?}", e.code(), e.message()))
        .collect();
    assert!(
        failed.is_empty(),
        "{} of 128 refused ({} conflicts): {failed:?}",
        failed.len(),
        c.meta.conflicts()
    );
    c.shutdown().await;
}

/// DELETE and CompleteMultipartUpload take the same turns as PUT: mixed
/// with concurrent PUTs to the key through one gateway, every one succeeds
/// (before ADR-0041 an unconditional DELETE whose pin kept moving was
/// answered 412, and a Complete could be refused `SlowDown`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_and_completes_take_turns_with_puts() {
    let c: TestCluster<SlowKv<MemKv>> = TestCluster::start_with(
        Arc::new(SlowKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    let s3 = &c.backend;
    s3::create_bucket(s3, BUCKET).await.unwrap();
    // Uploads with one part each, completed during the PUTs.
    let mut uploads = Vec::new();
    for n in 0..4 {
        let up = s3
            .create_multipart_upload(request(CreateMultipartUploadInput {
                bucket: BUCKET.into(),
                key: KEY.into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output
            .upload_id
            .unwrap();
        let etag = s3
            .upload_part(request(UploadPartInput {
                bucket: BUCKET.into(),
                key: KEY.into(),
                upload_id: up.clone(),
                part_number: 1,
                body: Some(StreamingBlob::from_bytes(body(1000 + n))),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output
            .e_tag;
        uploads.push((up, etag));
    }
    let mut tasks = Vec::new();
    for n in 0..32 {
        let s3 = c.backend.clone();
        tasks.push(tokio::spawn(async move {
            s3::put(&s3, BUCKET, KEY, body(n), &s3::PutOptions::default())
                .await
                .map(|_| ())
                .map_err(|e| format!("PUT {:?}: {:?}", e.code(), e.message()))
        }));
        if n % 4 == 0 {
            let s3 = c.backend.clone();
            tasks.push(tokio::spawn(async move {
                s3::delete(&s3, BUCKET, KEY)
                    .await
                    .map_err(|e| format!("DELETE {:?}: {:?}", e.code(), e.message()))
            }));
        }
        if n % 8 == 0 {
            let s3 = c.backend.clone();
            let (up, etag): (String, Option<ETag>) = uploads.pop().unwrap();
            tasks.push(tokio::spawn(async move {
                s3.complete_multipart_upload(request(CompleteMultipartUploadInput {
                    bucket: BUCKET.into(),
                    key: KEY.into(),
                    upload_id: up,
                    multipart_upload: Some(CompletedMultipartUpload {
                        parts: Some(vec![CompletedPart {
                            part_number: Some(1),
                            e_tag: etag,
                            ..Default::default()
                        }]),
                    }),
                    ..Default::default()
                }))
                .await
                .map(|_| ())
                .map_err(|e| format!("Complete {:?}: {:?}", e.code(), e.message()))
            }));
        }
    }
    let failed: Vec<String> = futures::future::join_all(tasks)
        .await
        .into_iter()
        .filter_map(|r| r.unwrap().err())
        .collect();
    assert!(failed.is_empty(), "{failed:?}");
    c.shutdown().await;
}

/// The behaviour change, measured: conflicts of commits on the key with
/// the queue off (every commit at once, as 0.1.0) and on (one gateway: none).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_queue_removes_conflicts_within_a_gateway() {
    let mut seen = Vec::new();
    for enabled in [false, true] {
        let mut opts = ClusterOptions::rs(3, 2, 1, MIB, 0);
        opts.commit_queue.enabled = enabled;
        let c: TestCluster<SlowKv<MemKv>> =
            TestCluster::start_with(Arc::new(SlowKv::new(MemKv::default())), opts).await;
        s3::create_bucket(&c.backend, BUCKET).await.unwrap();
        watch_current(&c).await;
        let puts = (0..32).map(|n| {
            let s3 = c.backend.clone();
            tokio::spawn(async move {
                s3::put(&s3, BUCKET, KEY, body(n), &s3::PutOptions::default()).await
            })
        });
        let acked = futures::future::join_all(puts)
            .await
            .into_iter()
            .filter(|r| r.as_ref().unwrap().is_ok())
            .count();
        let q = c.backend.commit_queue_stats();
        eprintln!(
            "queue {}: 32 PUTs, {acked} acknowledged, {} conflicts on the key's commits, {} turns waited",
            if enabled { "on" } else { "off" },
            c.meta.conflicts(),
            q.waited
        );
        seen.push((acked, c.meta.conflicts(), q));
        c.shutdown().await;
    }
    let (off, on) = (&seen[0], &seen[1]);
    assert_eq!(on.0, 32, "every PUT acknowledged with the queue");
    assert_eq!(on.1, 0, "no conflict within one gateway");
    assert_eq!(on.2.turns, 32);
    assert!(on.2.waited > 0, "the PUTs did overlap");
    assert_eq!(
        off.2,
        arca_lake_gateway::cluster::commit_queue::CommitQueueStatsSnapshot::default(),
        "the queue off records nothing"
    );
}

struct Outcome {
    n: usize,
    sent: Instant,
    answered: Instant,
    result: Result<String, (s3s::S3ErrorCode, String)>,
}

/// Version ids of the key, newest first, with each one's body's PUT number.
async fn versions(s3: &impl S3) -> Vec<(String, usize)> {
    let out = s3
        .list_object_versions(request(ListObjectVersionsInput {
            bucket: BUCKET.into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.delete_markers.unwrap_or_default(), vec![]);
    let mut found = Vec::new();
    for v in out.versions.unwrap_or_default() {
        let id = v.version_id.unwrap();
        let got = s3
            .get_object(request(GetObjectInput {
                bucket: BUCKET.into(),
                key: KEY.into(),
                version_id: Some(id.clone()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let bytes = s3::collect(got.body).await.unwrap();
        let n = std::str::from_utf8(&bytes[4..9]).unwrap().parse().unwrap();
        assert_eq!(bytes, body(n), "a body that is no PUT's");
        found.push((id, n));
    }
    found
}

/// What every run through several gateways must show: each acknowledged
/// PUT is exactly one version, with the id it was answered with; no PUT
/// answered `SlowDown` is a version; versions are in the real-time order
/// of the acknowledgements; the newest is current; no pending generation
/// is left behind (a refused write's was aborted).
async fn check_history<S: KvStore + 'static>(c: &TestCluster<S>, outcomes: &[Outcome]) {
    let s3 = &c.backend;
    let found = versions(s3).await;
    let position: HashMap<usize, usize> = found
        .iter()
        .enumerate()
        .map(|(i, (_, n))| (*n, found.len() - i))
        .collect();
    assert_eq!(position.len(), found.len(), "a PUT is several versions");
    let mut acked = Vec::new();
    for o in outcomes {
        match &o.result {
            Ok(id) => {
                let v = found.iter().find(|(_, n)| *n == o.n);
                assert_eq!(
                    v.map(|(id, _)| id),
                    Some(id),
                    "acknowledged PUT {} is not the version it was answered with",
                    o.n
                );
                acked.push(o);
            }
            Err((code, m)) => {
                assert_eq!(code, &s3s::S3ErrorCode::SlowDown, "PUT {}: {m}", o.n);
                assert!(
                    !position.contains_key(&o.n),
                    "PUT {} answered SlowDown ({m}) is a version",
                    o.n
                );
            }
        }
    }
    assert_eq!(found.len(), acked.len(), "a version no PUT was told of");
    for a in &acked {
        for b in &acked {
            if a.answered < b.sent {
                assert!(
                    position[&a.n] < position[&b.n],
                    "PUT {} acknowledged before PUT {} was sent is the newer version",
                    a.n,
                    b.n
                );
            }
        }
    }
    if let Some((_, newest)) = found.first() {
        let (now, _) = s3::get(s3, BUCKET, KEY).await.unwrap();
        assert_eq!(now, body(*newest), "the newest version is not current");
    }
    let pending = arca_lake_metadata::transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            Ok(tx
                .get_range(&keys::pending_expired_range(u64::MAX), 1000, false)
                .await?)
        })
    })
    .await
    .unwrap();
    assert!(pending.is_empty(), "pending generations left: {pending:?}");
}

async fn put_through(
    gateways: &[arca_lake_gateway::cluster::ClusterBackend<impl KvStore + 'static>],
    writers_per_gateway: usize,
) -> Vec<Outcome> {
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for (g, gw) in gateways.iter().enumerate() {
        for w in 0..writers_per_gateway {
            let s3 = gw.clone();
            let outcomes = Arc::clone(&outcomes);
            let n = g * writers_per_gateway + w;
            tasks.push(tokio::spawn(async move {
                let sent = Instant::now();
                let r = s3
                    .put_object(request(PutObjectInput {
                        bucket: BUCKET.into(),
                        key: KEY.into(),
                        body: Some(StreamingBlob::from_bytes(body(n))),
                        ..Default::default()
                    }))
                    .await;
                let answered = Instant::now();
                let result = r
                    .map(|o| o.output.version_id.unwrap())
                    .map_err(|e| (e.code().clone(), format!("{:?}", e.message())));
                outcomes.lock().unwrap().push(Outcome {
                    n,
                    sent,
                    answered,
                    result,
                });
            }));
        }
    }
    futures::future::join_all(tasks).await;
    Arc::try_unwrap(outcomes)
        .ok()
        .unwrap()
        .into_inner()
        .unwrap()
}

/// Several gateways on one key, versioned, with a short wait bound so that
/// some PUTs are refused: the history is consistent and every refused PUT
/// left nothing behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn several_gateways_on_one_key_keep_a_consistent_history() {
    let mut opts = ClusterOptions::rs(3, 2, 1, MIB, 0);
    opts.commit_queue.wait_bound = Duration::from_millis(10);
    let c: TestCluster<SlowKv<MemKv>> =
        TestCluster::start_with(Arc::new(SlowKv::new(MemKv::default())), opts).await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    enable_versioning(&c.backend).await;
    watch_current(&c).await;
    let gateways = vec![
        c.backend.clone(),
        c.extra_backend().await.unwrap(),
        c.extra_backend().await.unwrap(),
    ];
    let outcomes = put_through(&gateways, 12).await;
    let refused = outcomes.iter().filter(|o| o.result.is_err()).count();
    let waited: u64 = gateways
        .iter()
        .map(|g| g.commit_queue_stats().refused_wait)
        .sum();
    eprintln!(
        "36 PUTs through 3 gateways: {refused} refused ({waited} at the wait bound), {} conflicts",
        c.meta.conflicts()
    );
    assert!(
        waited > 0,
        "no PUT waited beyond the bound: nothing refused to check"
    );
    check_history(&c, &outcomes).await;
    for g in &gateways {
        assert_eq!(g.commit_queue_stats().keys, 0, "a lock table entry left");
    }
    c.shutdown().await;
}

/// Unknown commit outcomes on the hot key, with other PUTs queued behind:
/// the turn is held while the outcome is resolved, and the history stays
/// consistent (an unknown applied commit is acknowledged with its id; one
/// not applied is retried under the same turn).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_outcomes_are_resolved_under_the_turn() {
    for fault in [
        CommitFault::UnknownAfterApply,
        CommitFault::UnknownWithoutApply,
        CommitFault::TimeoutAfterApply,
    ] {
        let c: TestCluster<SlowKv<FaultKv<MemKv>>> = TestCluster::start_with(
            Arc::new(SlowKv::new(FaultKv::new(MemKv::default()))),
            ClusterOptions::rs(3, 2, 1, MIB, 0),
        )
        .await;
        s3::create_bucket(&c.backend, BUCKET).await.unwrap();
        enable_versioning(&c.backend).await;
        watch_current(&c).await;
        let b = arca_lake_metadata::ops::bucket::get_bucket(c.meta.as_ref(), BUCKET)
            .await
            .unwrap();
        c.meta
            .inner
            .inject_on_key(keys::object_current(b.id, KEY), fault, 3);
        let gateways = vec![c.backend.clone(), c.extra_backend().await.unwrap()];
        let outcomes = put_through(&gateways, 8).await;
        let fired = c.meta.inner.fired();
        assert_eq!(fired.len(), 3, "{fault:?}: {fired:?}");
        let failed: Vec<_> = outcomes
            .iter()
            .filter_map(|o| o.result.as_ref().err())
            .collect();
        assert!(failed.is_empty(), "{fault:?}: {failed:?}");
        check_history(&c, &outcomes).await;
        let seen: HashSet<usize> = outcomes.iter().map(|o| o.n).collect();
        assert_eq!(seen.len(), 16);
        c.shutdown().await;
    }
}

/// Every DELETE form waits on the queue's backoff after a conflict, as a
/// single DELETE does: a version-specific DELETE, and both kinds of
/// DeleteObjects entry (review of ADR-0041). One injected conflict each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_delete_form_backs_off_after_a_conflict() {
    use s3s::dto::{Delete, DeleteObjectInput, DeleteObjectsInput, ObjectIdentifier};
    let c: TestCluster<FaultKv<MemKv>> = TestCluster::start_with(
        Arc::new(FaultKv::new(MemKv::default())),
        ClusterOptions::rs(3, 2, 1, MIB, 0),
    )
    .await;
    let s3 = &c.backend;
    s3::create_bucket(s3, BUCKET).await.unwrap();
    enable_versioning(s3).await;
    let b = arca_lake_metadata::ops::bucket::get_bucket(c.meta.as_ref(), BUCKET)
        .await
        .unwrap();
    let current = keys::object_current(b.id, KEY);
    let put = |n| async move {
        s3.put_object(request(PutObjectInput {
            bucket: BUCKET.into(),
            key: KEY.into(),
            body: Some(StreamingBlob::from_bytes(body(n))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .version_id
        .unwrap()
    };
    let conflicts = || c.backend.commit_queue_stats().conflicts;
    let entries = |version_id: Option<String>| {
        let mut i = DeleteObjectsInput::builder();
        i.set_bucket(BUCKET.into());
        i.set_delete(Delete {
            objects: vec![ObjectIdentifier {
                key: KEY.into(),
                version_id,
                ..Default::default()
            }],
            quiet: None,
        });
        i.build().unwrap()
    };

    // A version-specific DELETE of the current version.
    let v = put(1).await;
    c.meta
        .inject_on_key(current.clone(), CommitFault::Conflict, 1);
    let before = conflicts();
    s3.delete_object(request(DeleteObjectInput {
        bucket: BUCKET.into(),
        key: KEY.into(),
        version_id: Some(v),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_eq!(conflicts() - before, 1, "DELETE with a version id");

    // DeleteObjects: an entry with a version id, then one without.
    let v = put(2).await;
    c.meta
        .inject_on_key(current.clone(), CommitFault::Conflict, 1);
    let before = conflicts();
    let out = s3
        .delete_objects(request(entries(Some(v))))
        .await
        .unwrap()
        .output;
    assert_eq!(out.errors.unwrap_or_default(), [] as [s3s::dto::Error; 0]);
    assert_eq!(
        conflicts() - before,
        1,
        "DeleteObjects entry with a version id"
    );

    put(3).await;
    c.meta.inject_on_key(current, CommitFault::Conflict, 1);
    let before = conflicts();
    let out = s3
        .delete_objects(request(entries(None)))
        .await
        .unwrap()
        .output;
    assert_eq!(out.errors.unwrap_or_default(), [] as [s3s::dto::Error; 0]);
    assert_eq!(
        conflicts() - before,
        1,
        "DeleteObjects entry without a version id"
    );
    assert_eq!(c.meta.fired().len(), 3);
    c.shutdown().await;
}
