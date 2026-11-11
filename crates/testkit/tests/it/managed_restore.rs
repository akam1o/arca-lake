//! `admin restore` (Phase 3.11, ADR-0026 option D): the managed restore
//! procedure under the Phase 3.11 restore scenarios. The restore itself is
//! a snapshot of the metadata store written back by a restorer that, as
//! `fdbrestore`, refuses a non-empty destination (and can be made to fail);
//! `fdb_restore.rs` runs the same procedure with `fdbrestore` on a real
//! FoundationDB cluster.
//!
//! Invariants: no delete decided before the restore applies after it,
//! whoever sends it and whenever; the procedure does not start while a
//! node's safety cannot be confirmed (a node not answering, a process
//! running); every step is resumable after a failure or a crash; the
//! restored cluster deletes again only once finalized and a leader opened
//! a generation above every earlier one.

#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arca_lake_controller::managed_restore::{self as mr, Restorer, Step};
use arca_lake_controller::process::{ControllerConfig, ControllerHandle, spawn};
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::delete_fence;
use arca_lake_metadata::records::ClusterPhase;
use arca_lake_metadata::{Txn, keys, transact};
use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "managed";

async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    // Verifiable nodes, as after a leader ran (`admin restore` checks each
    // node's identity).
    c.acknowledge_identities().await;
    c
}

async fn put(c: &TestCluster, key: &str, seed: u64) -> Bytes {
    let d = Bytes::from(random_bytes(usize::try_from(MIB / 2).unwrap(), seed));
    s3::put(&c.backend, BUCKET, key, d.clone(), &PutOptions::default())
        .await
        .unwrap();
    d
}

/// The backup: a snapshot of the store, written back by `restore`.
struct Snapshot {
    meta: Arc<MemKv>,
    data: Vec<(Vec<u8>, Vec<u8>)>,
    /// Restores that fail before writing anything (a lost connection).
    fail: AtomicUsize,
    restores: AtomicUsize,
}

impl Snapshot {
    async fn take(meta: &Arc<MemKv>) -> Self {
        let data = transact(meta.as_ref(), (), |tx, ()| {
            Box::pin(async move {
                Ok(tx
                    .get_range(&keys::all_range(), usize::MAX, false)
                    .await?
                    .into_iter()
                    .collect::<Vec<_>>())
            })
        })
        .await
        .unwrap();
        Self {
            meta: Arc::clone(meta),
            data,
            fail: AtomicUsize::new(0),
            restores: AtomicUsize::new(0),
        }
    }
}

impl Restorer for Snapshot {
    fn destination(&self) -> String {
        "in-memory test store".into()
    }

    async fn probe(&self, key: &[u8], value: &[u8]) -> Result<bool, String> {
        arca_lake_controller::managed_restore::probe_store(self.meta.as_ref(), key, value).await
    }

    async fn restore(&self, source: &str, _version: u64) -> Result<(), String> {
        assert_eq!(source, "snapshot://test");
        if self
            .fail
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err("injected: connection to the cluster lost".into());
        }
        let data = self.data.clone();
        transact(self.meta.as_ref(), data, |tx, data| {
            Box::pin(async move {
                if !tx.get_range(&keys::all_range(), 1, false).await?.is_empty() {
                    return Err(arca_lake_metadata::MetaError::InvalidArgument(
                        "Attempted to restore into a non-empty destination database".into(),
                    ));
                }
                for (k, v) in data {
                    tx.set(k, v);
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())?;
        self.restores.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn controller(id: &str) -> ControllerConfig {
    let mut cfg = ControllerConfig::new(id);
    cfg.lease = Duration::from_secs(2);
    cfg.health_interval = Duration::from_millis(100);
    cfg.gc_interval = Duration::from_millis(200);
    cfg.gc_grace = Duration::ZERO;
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.recovery_interval = Duration::from_millis(100);
    cfg.failure_domain = "host".into();
    cfg.heartbeat = None;
    cfg
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A leader with its generation open on every node.
async fn leading(c: &TestCluster, id: &str) -> ControllerHandle {
    let ctl = spawn(Arc::clone(&c.meta), controller(id)).await.unwrap();
    wait_until(
        "a leader's generation open everywhere",
        Duration::from_secs(30),
        async || {
            let rec = delete_fence::current(c.meta.as_ref()).await.unwrap();
            let mut all = ctl.is_leader() && rec.open && rec.leader_token == ctl.token();
            for n in &c.nodes {
                let h = c.clients.health(&n.addr.to_string()).await.unwrap();
                all &= h.delete_fence_open && h.delete_fence_generation == rec.generation;
            }
            all
        },
    )
    .await;
    ctl
}

/// Every shard of `key` on its member, as GC decides them (or a
/// reconciliation of those shards as orphans, or a migration's cleanup of
/// them as a former placement: the same `DeleteShard` RPCs).
async fn decided_deletes(c: &TestCluster, key: &str) -> Vec<(String, ShardRef, u64)> {
    let m = c.manifest(BUCKET, key).await;
    let groups = c.groups().await;
    let mut out = Vec::new();
    for (_, rec) in &m.chunks {
        let g = groups
            .iter()
            .find(|g| g.id == rec.placement.group_id)
            .unwrap();
        for (i, member) in g.members.iter().enumerate() {
            let n = c.node_index(member).unwrap();
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
    out
}

/// A process as it was when it decided: the cluster's incarnation and the
/// leader's open generation.
async fn frozen(c: &TestCluster) -> NodeClients {
    let old = NodeClients::new(Duration::from_secs(5)).with_tls(c.clients.tls().cloned());
    old.set_incarnation(Some(
        arca_lake_metadata::ops::compat::cluster_state(c.meta.as_ref())
            .await
            .unwrap()
            .unwrap()
            .incarnation,
    ));
    let rec = delete_fence::current(c.meta.as_ref()).await.unwrap();
    assert!(rec.open, "{rec:?}");
    old.set_delete_fence(Some(rec.generation));
    // Stamped with the identities, as a leader's deletes are: refused by
    // the fence, not for a missing identity.
    let ids = arca_lake_metadata::ops::node_lifecycle::identities(c.meta.as_ref())
        .await
        .unwrap();
    for n in &c.nodes {
        old.note_identity(
            &n.addr.to_string(),
            ids.get(&n.id)
                .filter(|i| i.acknowledged)
                .map(|i| (n.id, i.token)),
        );
    }
    old
}

async fn deliver(old: &NodeClients, deletes: &[(String, ShardRef, u64)]) -> usize {
    let mut applied = 0;
    for (addr, shard, epoch) in deletes {
        if old.delete_shard(addr, *shard, *epoch).await.is_ok() {
            applied += 1;
        }
    }
    applied
}

fn journal_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("restore.journal")
}

async fn plan(c: &TestCluster, r: &Snapshot, path: &std::path::Path) -> Result<mr::Plan, String> {
    mr::plan(
        c.meta.as_ref(),
        &c.clients,
        "snapshot://test",
        1,
        Some(now_ms()),
        &r.destination(),
        path,
        true,
        now_ms(),
    )
    .await
    .map_err(|e| e.to_string())
}

async fn run(
    c: &TestCluster,
    r: &Snapshot,
    path: &std::path::Path,
    token: &str,
    stopped: &[String],
) -> Result<mr::Journal, String> {
    mr::run(&c.meta, &c.clients, r, path, token, stopped, &[], now_ms)
        .await
        .map_err(|e| e.to_string())
}

async fn finish(c: &TestCluster, path: &std::path::Path) -> Result<mr::Journal, String> {
    mr::finish(
        c.meta.as_ref(),
        &c.clients,
        path,
        "op",
        "drill",
        true,
        &[],
        Duration::from_secs(30),
        now_ms,
    )
    .await
    .map_err(|e| e.to_string())
}

/// Scenarios 1, 9, 10, 11, 12, 15: the normal managed restore. A leader
/// decided deletes after the backup point (GC of an object deleted then,
/// and the same RPCs as a reconciliation or a migration's cleanup would
/// send) and was stopped; they are delivered after the restore, by the old
/// leader returning: none applies. The restored cluster reads every object
/// of the backup point, writes and deletes again after `finish`, and the
/// old deletes stay refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_managed_restore_applies_no_delete_decided_before_it() {
    let mut c = cluster().await;
    let kept = put(&c, "kept", 1).await;
    let ctl = leading(&c, "ctl-before").await;
    let backup = Snapshot::take(&c.meta).await;
    // After the backup point: deleted; the leader decided its deletes.
    let deletes = decided_deletes(&c, "kept").await;
    let old = frozen(&c).await;
    let _ = put(&c, "post", 2).await;
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    ctl.abort();
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    assert!(p.summary.iter().any(|l| l.starts_with("target cluster:")));
    assert!(
        p.summary
            .iter()
            .any(|l| l.starts_with("source: snapshot://test"))
    );
    assert!(p.summary.iter().any(|l| l.starts_with("destination:")));
    // A wrong token changes nothing.
    assert!(run(&c, &backup, &path, "wrong", &[]).await.is_err());
    assert_eq!(
        arca_lake_metadata::ops::compat::cluster_state(c.meta.as_ref())
            .await
            .unwrap()
            .unwrap()
            .state,
        ClusterPhase::Active
    );
    let j = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    assert_eq!(j.last(), Some(Step::Verified));
    // The old leader returns: its deletes are refused.
    assert_eq!(deliver(&old, &deletes).await, 0, "after run");
    let ctl = spawn(Arc::clone(&c.meta), controller("ctl-after"))
        .await
        .unwrap();
    let j = finish(&c, &path).await.unwrap();
    assert_eq!(j.last(), Some(Step::Reopened));
    assert_eq!(deliver(&old, &deletes).await, 0, "after finish");
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    assert!(s3::get(&c.backend, BUCKET, "post").await.is_err(), "RPO");
    // Writes and GC work again.
    let again = put(&c, "again", 3).await;
    assert_eq!(s3::get(&c.backend, BUCKET, "again").await.unwrap().0, again);
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Scenarios 3, 4, 14: a controller or a gateway still running (fresh
/// heartbeat) stops the procedure before anything is restored; a new
/// leader started meanwhile is refused at the clear. Once stopped (or
/// declared stopped), the same journal resumes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_processes_stop_the_procedure_before_anything_is_restored() {
    let c = cluster().await;
    put(&c, "kept", 4).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    // A controller with a heartbeat.
    let mut cfg = controller("ctl-running");
    cfg.heartbeat = Some(Duration::from_millis(200));
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    wait_until("its heartbeat", Duration::from_secs(10), async || {
        !arca_lake_metadata::ops::compat::procs(c.meta.as_ref())
            .await
            .unwrap()
            .is_empty()
    })
    .await;
    let e = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("processes still running"), "{e}");
    assert_eq!(backup.restores.load(Ordering::SeqCst), 0);
    // A gateway heartbeat (as a running gateway writes it).
    ctl.shutdown().await;
    let gw = arca_lake_metadata::records::ProcRecord {
        id: "gw-1".into(),
        kind: arca_lake_metadata::records::ProcKind::Gateway,
        software: "test".into(),
        caps: arca_lake_controller::capabilities(),
        incarnation: [0; 16],
        last_ms: now_ms(),
    };
    arca_lake_metadata::ops::compat::heartbeat(c.meta.as_ref(), &gw)
        .await
        .unwrap();
    let running: Vec<String> = arca_lake_metadata::ops::compat::procs(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .filter(|p| now_ms().saturating_sub(p.last_ms) < 90_000)
        .map(|p| p.id)
        .collect();
    let e = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("Gateway gw-1"), "{e}");
    // Declared stopped by the operator: the same journal goes on.
    let j = run(&c, &backup, &path, &p.journal.token, &running)
        .await
        .unwrap();
    assert_eq!(j.last(), Some(Step::Verified));
    assert_eq!(backup.restores.load(Ordering::SeqCst), 1);
    c.shutdown().await;
}

/// Scenario 5: a node that does not answer: the procedure does not start
/// (plan refuses), nor resumes past the fence while one stopped answering
/// after the plan; nothing is restored. A removed node is not waited for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_not_answering_stops_the_procedure_before_anything_changes() {
    let mut c = cluster().await;
    put(&c, "kept", 5).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    c.stop_node(2).await;
    let e = plan(&c, &backup, &path).await.unwrap_err();
    assert!(e.contains("cannot be confirmed"), "{e}");
    assert!(!path.exists());
    c.start_node(2).await;
    let p = plan(&c, &backup, &path).await.unwrap();
    c.stop_node(2).await;
    let e = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("did not acknowledge"), "{e}");
    assert_eq!(backup.restores.load(Ordering::SeqCst), 0);
    assert_eq!(
        mr::Journal::load(&path).unwrap().last(),
        Some(Step::Frozen),
        "stopped before the fence step completed"
    );
    // Back: the same journal resumes.
    c.start_node(2).await;
    let j = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    assert_eq!(j.last(), Some(Step::Verified));
    c.shutdown().await;
}

/// Scenarios 7, 8, 16, 17: the restore fails (the connection to the
/// cluster lost) after the destination was cleared; a storage node is
/// restarted meanwhile and keeps its held fence; the re-run clears again
/// and restores. A node stopped after the restore stops the mark until it
/// is back. The procedure then completes, and old deletes stay refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_restore_and_a_node_restart_resume_without_reopening_deletes() {
    let mut c = cluster().await;
    put(&c, "kept", 6).await;
    let ctl = leading(&c, "ctl-before").await;
    let backup = Snapshot::take(&c.meta).await;
    let deletes = decided_deletes(&c, "kept").await;
    let old = frozen(&c).await;
    ctl.abort();
    backup.fail.store(1, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    let e = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("restore failed"), "{e}");
    assert_eq!(
        mr::Journal::load(&path).unwrap().last(),
        Some(Step::Cleared)
    );
    // A node restarts while the store is empty: still closed and held.
    c.stop_node(1).await;
    c.start_node_quiet(1).await;
    let h = c
        .clients
        .health(&c.nodes[1].addr.to_string())
        .await
        .unwrap();
    assert!(!h.delete_fence_open && h.delete_fence_hold, "{h:?}");
    assert_eq!(deliver(&old, &deletes).await, 0, "while restoring");
    // Something wrote into the cleared store (as a process left running
    // would): the restore refuses a non-empty destination; the re-run
    // clears it again.
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.set(&keys::leader(), b"stray");
            Ok(())
        })
    })
    .await
    .unwrap();
    // A node down at the retry: its hold cannot be verified, nothing is
    // cleared again until it is back (re-review).
    c.stop_node(0).await;
    let e = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("stopped answering"), "{e}");
    assert_eq!(
        mr::Journal::load(&path).unwrap().last(),
        Some(Step::Cleared)
    );
    c.start_node_quiet(0).await;
    let j = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    assert_eq!(j.last(), Some(Step::Verified));
    assert_eq!(backup.restores.load(Ordering::SeqCst), 1);
    assert_eq!(deliver(&old, &deletes).await, 0, "after the restore");
    let ctl = spawn(Arc::clone(&c.meta), controller("ctl-after"))
        .await
        .unwrap();
    finish(&c, &path).await.unwrap();
    assert_eq!(deliver(&old, &deletes).await, 0, "after finish");
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Scenario 6: the procedure itself crashes between a step and its record
/// (here after the mark): the re-run adopts the mark instead of making
/// another incarnation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_mark_is_resumed_without_a_second_incarnation() {
    let c = cluster().await;
    put(&c, "kept", 7).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    let j = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    let marked = j.new_incarnation.clone().unwrap();
    // As if the process died right after marking: the journal says
    // `Restored`.
    let mut back = j.clone();
    back.done.retain(|s| *s <= Step::Restored);
    back.new_incarnation = None;
    back.save(&path).unwrap();
    let j = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    assert_eq!(j.new_incarnation.as_deref(), Some(marked.as_str()));
    assert_eq!(j.last(), Some(Step::Verified));
    c.shutdown().await;
}

/// Scenario 13: two restores in a row; the second plans only once the
/// first finished; deletes of either era stay refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_restores_in_a_row() {
    let c = cluster().await;
    put(&c, "kept", 8).await;
    let ctl = leading(&c, "ctl-0").await;
    let backup = Snapshot::take(&c.meta).await;
    let deletes = decided_deletes(&c, "kept").await;
    let old = frozen(&c).await;
    ctl.abort();
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    // A second plan while the first is unfinished is refused.
    assert!(
        plan(&c, &backup, &path)
            .await
            .unwrap_err()
            .contains("unfinished")
    );
    let ctl = spawn(Arc::clone(&c.meta), controller("ctl-1"))
        .await
        .unwrap();
    finish(&c, &path).await.unwrap();
    let old1 = frozen(&c).await;
    ctl.shutdown().await;
    // The second restore, of the same backup.
    let backup2 = Snapshot::take(&c.meta).await;
    let p2 = plan(&c, &backup2, &path).await.unwrap();
    run(&c, &backup2, &path, &p2.journal.token, &[])
        .await
        .unwrap();
    let ctl = spawn(Arc::clone(&c.meta), controller("ctl-2"))
        .await
        .unwrap();
    finish(&c, &path).await.unwrap();
    assert_eq!(deliver(&old, &deletes).await, 0, "first era");
    let deletes1 = decided_deletes(&c, "kept").await;
    assert_eq!(deliver(&old1, &deletes1).await, 0, "second era");
    ctl.shutdown().await;
    c.shutdown().await;
}

/// A restorer that delivers held deletes between the clear and the
/// write-back, counting those applied.
struct Delivering<'a> {
    inner: &'a Snapshot,
    old: NodeClients,
    deletes: Vec<(String, ShardRef, u64)>,
    applied: AtomicUsize,
}

impl Restorer for Delivering<'_> {
    fn destination(&self) -> String {
        self.inner.destination()
    }

    async fn probe(&self, key: &[u8], value: &[u8]) -> Result<bool, String> {
        self.inner.probe(key, value).await
    }

    async fn restore(&self, source: &str, version: u64) -> Result<(), String> {
        self.applied
            .fetch_add(deliver(&self.old, &self.deletes).await, Ordering::SeqCst);
        self.inner.restore(source, version).await
    }
}

/// A restorer that, after restoring, writes a process heartbeat there.
struct Then<'a>(&'a Snapshot, arca_lake_metadata::records::ProcRecord);
impl Restorer for Then<'_> {
    fn destination(&self) -> String {
        self.0.destination()
    }
    async fn probe(&self, key: &[u8], value: &[u8]) -> Result<bool, String> {
        self.0.probe(key, value).await
    }
    async fn restore(&self, source: &str, version: u64) -> Result<(), String> {
        self.0.restore(source, version).await?;
        arca_lake_metadata::ops::compat::heartbeat(self.0.meta.as_ref(), &self.1)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Review (Phase 3.11): the clear committed and the run died before
/// recording it (the journal says `Clearing`): the re-run resumes, though
/// the cluster state is gone. A process heartbeating on the restored
/// metadata stops the mark until it is stopped (or declared so).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_clear_resumes_and_a_process_on_the_restored_metadata_stops_the_mark() {
    let c = cluster().await;
    put(&c, "kept", 9).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    backup.fail.store(1, Ordering::SeqCst);
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    let mut j = mr::Journal::load(&path).unwrap();
    assert_eq!(j.last(), Some(Step::Cleared));
    // As if the run died between the clear's commit and its record.
    j.done.retain(|s| *s < Step::Cleared);
    assert_eq!(j.last(), Some(Step::Clearing));
    j.save(&path).unwrap();
    // A gateway starts on what is restored: its heartbeat there.
    let gw = arca_lake_metadata::records::ProcRecord {
        id: "gw-late".into(),
        kind: arca_lake_metadata::records::ProcKind::Gateway,
        software: "test".into(),
        caps: arca_lake_controller::capabilities(),
        incarnation: [0; 16],
        last_ms: now_ms(),
    };
    let then = Then(&backup, gw);
    let e = mr::run(
        &c.meta,
        &c.clients,
        &then,
        &path,
        &p.journal.token,
        &[],
        &[],
        now_ms,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        e.contains("processes running on the restored metadata"),
        "{e}"
    );
    assert_eq!(
        mr::Journal::load(&path).unwrap().last(),
        Some(Step::Restored)
    );
    let j = mr::run(
        &c.meta,
        &c.clients,
        &then,
        &path,
        &p.journal.token,
        &["gw-late".into()],
        &[],
        now_ms,
    )
    .await
    .unwrap();
    assert_eq!(j.last(), Some(Step::Verified));
    c.shutdown().await;
}

/// Review #1 (Phase 3.11): a backup from before the nodes held an
/// identity, restored once they acknowledged one: the leader on the
/// restored metadata takes each node's own identity, quarantines none,
/// and the restore finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backup_from_before_node_identities_quarantines_no_node() {
    use arca_lake_metadata::ops::node_lifecycle;
    let c = cluster().await;
    put(&c, "kept", 10).await;
    // No controller has run: no identity records in the backup.
    let backup = Snapshot::take(&c.meta).await;
    let ctl = leading(&c, "ctl-0").await;
    wait_until(
        "identities acknowledged",
        Duration::from_secs(20),
        async || {
            let ids = node_lifecycle::identities(c.meta.as_ref()).await.unwrap();
            c.nodes
                .iter()
                .all(|n| ids.get(&n.id).is_some_and(|i| i.acknowledged))
        },
    )
    .await;
    ctl.shutdown().await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    let ctl = spawn(Arc::clone(&c.meta), controller("ctl-1"))
        .await
        .unwrap();
    finish(&c, &path).await.unwrap();
    for n in &c.nodes {
        assert_eq!(
            node_lifecycle::state(c.meta.as_ref(), n.id).await.unwrap(),
            node_lifecycle::State::Active,
            "node {} quarantined after the restore",
            n.id
        );
    }
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Review #3 (Phase 3.11): another cluster's backup (a wrong `--source`)
/// is refused before the mark: no fence or incarnation of this cluster
/// reaches its nodes. Since Phase 3.12 by its uid, right after the
/// restore; a backup without a uid (an earlier release's) by the nodes it
/// names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_clusters_backup_is_refused_before_the_mark() {
    let c = cluster().await;
    put(&c, "kept", 11).await;
    let other = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    let theirs = Snapshot::take(&other.meta).await;
    let wrong = Snapshot {
        meta: Arc::clone(&c.meta),
        data: theirs.data.clone(),
        fail: AtomicUsize::new(0),
        restores: AtomicUsize::new(0),
    };
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &wrong, &path).await.unwrap();
    let e = run(&c, &wrong, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("another cluster"), "{e}");
    assert_eq!(
        mr::Journal::load(&path).unwrap().last(),
        Some(Step::Cleared)
    );
    let no_uid = Snapshot {
        meta: Arc::clone(&c.meta),
        data: theirs
            .data
            .iter()
            .filter(|(k, _)| *k != keys::cluster_uid())
            .cloned()
            .collect(),
        fail: AtomicUsize::new(0),
        restores: AtomicUsize::new(0),
    };
    mr::amend(&path, "snapshot://test", 1, None).unwrap();
    let e = run(&c, &no_uid, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("not this cluster's backup"), "{e}");
    assert_eq!(
        mr::Journal::load(&path).unwrap().last(),
        Some(Step::Restored)
    );
    other.shutdown().await;
    c.shutdown().await;
}

/// Review #4 (Phase 3.11): a backup from before a node was unregistered:
/// the node stays removed on the restored metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_unregistered_after_the_backup_stays_removed() {
    use arca_lake_metadata::ops::{membership, node_lifecycle};
    let mut c = cluster().await;
    put(&c, "kept", 12).await;
    let spare = c.add_node().await;
    let spare_id = c.nodes[spare].id;
    // Its identity adopted and acknowledged, as a leader does.
    let token = node_lifecycle::identity(c.meta.as_ref(), spare_id)
        .await
        .unwrap()
        .unwrap()
        .token;
    c.clients
        .health_with(
            &c.nodes[spare].addr.to_string(),
            arca_lake_protocol::storage::v1::HealthRequest {
                node_id: spare_id,
                node_identity: Bytes::copy_from_slice(&token),
                adopt_identity: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    node_lifecycle::acknowledge_identity(c.meta.as_ref(), spare_id, token)
        .await
        .unwrap();
    let backup = Snapshot::take(&c.meta).await;
    membership::drain_node(c.meta.as_ref(), spare_id, "t", now_ms())
        .await
        .unwrap();
    membership::update_drain(c.meta.as_ref(), spare_id, Some(now_ms()), None)
        .await
        .unwrap();
    arca_lake_controller::node_ops::unregister(
        c.meta.as_ref(),
        &c.clients,
        spare_id,
        "t",
        "retired",
        false,
        now_ms(),
    )
    .await
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    assert_eq!(
        node_lifecycle::state(c.meta.as_ref(), spare_id)
            .await
            .unwrap(),
        node_lifecycle::State::Removed
    );
    c.shutdown().await;
}

/// Confirmation review (Phase 3.11): the run stopped after recording
/// `Clearing` but before the clear committed; meanwhile a node's identity
/// changed (a fresh rejoin). The resume, finding the planned store still
/// there, takes the identities again: the restored metadata gets the
/// current one, not the one captured before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_before_the_clear_takes_the_identities_again() {
    use arca_lake_metadata::ops::node_lifecycle;
    let c = cluster().await;
    put(&c, "kept", 13).await;
    let ctl = leading(&c, "ctl-0").await;
    wait_until(
        "identities acknowledged",
        Duration::from_secs(20),
        async || {
            let ids = node_lifecycle::identities(c.meta.as_ref()).await.unwrap();
            c.nodes
                .iter()
                .all(|n| ids.get(&n.id).is_some_and(|i| i.acknowledged))
        },
    )
    .await;
    ctl.shutdown().await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    // A run up to the fence; then the journal as if it died after
    // recording `Clearing`, with a stale identity captured.
    backup.fail.store(1, Ordering::SeqCst);
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    let n = c.nodes[0].id;
    let mut j = mr::Journal::load(&path).unwrap();
    j.done.retain(|s| *s <= Step::Fenced);
    j.done.push(Step::Clearing);
    for (id, t) in &mut j.identities {
        if *id == n {
            *t = [0x5a; 16];
        }
    }
    j.save(&path).unwrap();
    // The planned store is back (the clear did not commit): it still
    // holds the marker written right before the clear.
    backup.restore("snapshot://test", 1).await.ok();
    let marker = j.marker.clone().expect("the attempt's marker");
    transact(c.meta.as_ref(), marker, |tx, marker| {
        Box::pin(async move {
            tx.set(&keys::restore_marker(), marker.as_bytes());
            Ok(())
        })
    })
    .await
    .unwrap();
    let current = node_lifecycle::identity(c.meta.as_ref(), n)
        .await
        .unwrap()
        .unwrap()
        .token;
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    let restored = node_lifecycle::identity(c.meta.as_ref(), n)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.token, current, "the stale identity was put back");
    c.shutdown().await;
}

/// Confirmation re-review (Phase 3.11): the clear and the restore committed
/// and the run died before recording them; the restored copy has the
/// planned incarnation but no marker: the resume keeps the snapshot taken
/// before the clear (not the backup's older identities).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_copy_with_the_planned_incarnation_is_not_taken_for_the_planned_store() {
    use arca_lake_metadata::ops::node_lifecycle;
    let c = cluster().await;
    put(&c, "kept", 14).await;
    // The backup: identities given at registration, not acknowledged yet.
    let backup = Snapshot::take(&c.meta).await;
    let ctl = leading(&c, "ctl-0").await;
    wait_until(
        "identities acknowledged",
        Duration::from_secs(20),
        async || {
            let ids = node_lifecycle::identities(c.meta.as_ref()).await.unwrap();
            c.nodes
                .iter()
                .all(|n| ids.get(&n.id).is_some_and(|i| i.acknowledged))
        },
    )
    .await;
    ctl.shutdown().await;
    let before = node_lifecycle::identities(c.meta.as_ref()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    backup.fail.store(1, Ordering::SeqCst);
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    // Restored, but the journal says `Cleared` (died before recording
    // `Restored`; `Cleared` is always recorded before the restore).
    // The copy even holds a marker: an earlier attempt's, captured by the
    // backup (re-review).
    let mut data = backup.data.clone();
    data.push((keys::restore_marker(), b"an-earlier-attempt".to_vec()));
    let copy = Snapshot {
        meta: Arc::clone(&c.meta),
        data,
        fail: AtomicUsize::new(0),
        restores: AtomicUsize::new(0),
    };
    copy.restore("snapshot://test", 1).await.unwrap();
    let mut j = mr::Journal::load(&path).unwrap();
    j.done.retain(|s| *s <= Step::Cleared);
    j.save(&path).unwrap();
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    for n in &c.nodes {
        assert_eq!(
            node_lifecycle::identity(c.meta.as_ref(), n.id)
                .await
                .unwrap()
                .map(|i| (i.token, i.acknowledged)),
            before.get(&n.id).map(|i| (i.token, i.acknowledged)),
            "node {}: the snapshot from before the clear was replaced",
            n.id
        );
    }
    c.shutdown().await;
}

/// Re-review (Phase 3.11): `admin restore abandon`. Refused while another
/// `admin restore` command holds the journal; otherwise the journal is
/// marked abandoned (never run again) and the marker that holds node
/// changes removed; refused once the store was cleared.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandon_gives_up_a_restore_before_its_clear_only() {
    use arca_lake_metadata::ops::node_lifecycle;
    let mut c = cluster().await;
    put(&c, "kept", 15).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    // Paused before its clear: the marker in the store, `Clearing` recorded.
    c.stop_node(2).await;
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    c.start_node(2).await;
    let mut j = mr::Journal::load(&path).unwrap();
    j.marker = Some("paused-attempt".into());
    j.done.push(Step::Fenced);
    j.done.push(Step::Clearing);
    j.save(&path).unwrap();
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.set(&keys::restore_marker(), b"paused-attempt");
            Ok(())
        })
    })
    .await
    .unwrap();
    // Another command holds the journal.
    {
        let held = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.with_extension("lock"))
            .unwrap();
        held.try_lock().unwrap();
        let e = mr::abandon(c.meta.as_ref(), &path)
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("one at a time"), "{e}");
    }
    assert!(mr::abandon(c.meta.as_ref(), &path).await.unwrap());
    assert!(mr::Journal::load(&path).unwrap().abandoned);
    let e = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("abandoned"), "{e}");
    assert_eq!(
        backup.restores.load(Ordering::SeqCst),
        0,
        "nothing restored"
    );
    // Node changes go on again (its own token, set again).
    let own = node_lifecycle::identity(c.meta.as_ref(), c.nodes[0].id)
        .await
        .unwrap()
        .unwrap()
        .token;
    node_lifecycle::set_identity(c.meta.as_ref(), c.nodes[0].id, own)
        .await
        .unwrap();
    // A new plan is allowed over an abandoned journal; once its store is
    // cleared, abandon is refused.
    let p2 = plan(&c, &backup, &path).await.unwrap();
    backup.fail.store(1, Ordering::SeqCst);
    run(&c, &backup, &path, &p2.journal.token, &[])
        .await
        .unwrap_err();
    assert_eq!(
        mr::Journal::load(&path).unwrap().last(),
        Some(Step::Cleared)
    );
    let e = mr::abandon(c.meta.as_ref(), &path)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("cleared"), "{e}");
    c.shutdown().await;
}

/// Re-review (Phase 3.11): `Clearing` recorded, the marker gone (removed by
/// hand, or another journal's abandon), the planned store live: the
/// resume clears nothing. Another journal's abandon leaves this
/// restore's marker in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_store_without_this_restores_marker_is_never_cleared() {
    let c = cluster().await;
    put(&c, "kept", 16).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    // Journal B, another plan elsewhere, and A paused before its clear.
    let other = dir.path().join("other.journal");
    let mut b = p.journal.clone();
    b.done = vec![Step::Planned];
    b.marker = None;
    b.save(&other).unwrap();
    let mut j = p.journal.clone();
    j.done = vec![Step::Planned, Step::Frozen, Step::Fenced, Step::Clearing];
    j.marker = Some("attempt-a".into());
    j.save(&path).unwrap();
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.set(&keys::restore_marker(), b"attempt-a");
            Ok(())
        })
    })
    .await
    .unwrap();
    // B's abandon leaves A's marker.
    assert!(!mr::abandon(c.meta.as_ref(), &other).await.unwrap());
    let held = transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move { Ok(tx.get(&keys::restore_marker()).await?) })
    })
    .await
    .unwrap();
    assert_eq!(held.as_deref(), Some(&b"attempt-a"[..]));
    // Removed by hand: A's resume clears nothing.
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.clear(&keys::restore_marker());
            Ok(())
        })
    })
    .await
    .unwrap();
    let e = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("not this restore's marker"), "{e}");
    assert!(
        arca_lake_metadata::ops::compat::cluster_state(c.meta.as_ref())
            .await
            .unwrap()
            .is_some(),
        "the live store was cleared"
    );
    assert_eq!(backup.restores.load(Ordering::SeqCst), 0);
    c.shutdown().await;
}

/// Re-review (Phase 3.11): a retry wrote a new marker and died before
/// recording it (the journal names it as pending): the resume and abandon
/// both take it as this journal's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pending_marker_is_this_journals_own() {
    let c = cluster().await;
    put(&c, "kept", 17).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    let pending = |name: &'static str| {
        let mut j = p.journal.clone();
        j.done = vec![Step::Planned, Step::Frozen, Step::Fenced, Step::Clearing];
        j.marker = Some("attempt-a".into());
        j.pending_marker = Some(name.into());
        j
    };
    let write = async |m: &'static [u8]| {
        transact(c.meta.as_ref(), (), |tx, ()| {
            Box::pin(async move {
                tx.set(&keys::restore_marker(), m);
                Ok(())
            })
        })
        .await
        .unwrap();
    };
    // Abandon accepts it.
    let other = dir.path().join("b.journal");
    pending("attempt-b").save(&other).unwrap();
    write(b"attempt-b").await;
    assert!(mr::abandon(c.meta.as_ref(), &other).await.unwrap());
    // The resume goes on from it (the store is the planned one).
    pending("attempt-c").save(&path).unwrap();
    write(b"attempt-c").await;
    let j = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    assert_eq!(j.last(), Some(Step::Verified));
    assert_eq!(backup.restores.load(Ordering::SeqCst), 1);
    c.shutdown().await;
}

/// Re-review (Phase 3.11): a marker carried into a store restored by hand
/// (a backup taken while an attempt was paused) is removed by `recovery
/// begin`'s mark; node changes go on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_begin_removes_a_carried_marker() {
    use arca_lake_metadata::ops::{compat, node_lifecycle};
    let c = cluster().await;
    transact(c.meta.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.set(&keys::restore_marker(), b"carried");
            Ok(())
        })
    })
    .await
    .unwrap();
    assert!(
        node_lifecycle::set_identity(c.meta.as_ref(), c.nodes[0].id, [1; 16])
            .await
            .is_err()
    );
    compat::mark_restoring(c.meta.as_ref(), None, None, "t", now_ms())
        .await
        .unwrap();
    node_lifecycle::set_identity(c.meta.as_ref(), c.nodes[0].id, [1; 16])
        .await
        .unwrap();
    c.shutdown().await;
}

/// Re-review (Phase 3.11): another disk at a node's address (holding
/// another node's identity) cannot acknowledge the restore's hold for that
/// node: the stamped close is refused there, and the restore does not
/// start; nothing is cleared.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_disk_at_a_nodes_address_stops_the_restore_before_the_clear() {
    let mut c = cluster().await;
    put(&c, "kept", 18).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    // Node 0's address now answers with a disk holding another identity.
    let addr = c.nodes[0].addr.to_string();
    c.stop_node(0).await;
    std::fs::remove_file(c.nodes[0].dir.join("node-identity.txt")).unwrap();
    c.start_node_quiet(0).await;
    c.clients
        .health_with(
            &addr,
            arca_lake_protocol::storage::v1::HealthRequest {
                node_id: 97,
                node_identity: Bytes::from_static(&[0xb4; 16]),
                adopt_identity: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let e = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(
        e.contains("did not acknowledge") || e.contains("cannot be verified"),
        "{e}"
    );
    assert!(
        arca_lake_metadata::ops::compat::cluster_state(c.meta.as_ref())
            .await
            .unwrap()
            .is_some(),
        "cleared"
    );
    assert_eq!(backup.restores.load(Ordering::SeqCst), 0);
    c.shutdown().await;
}

/// Re-review (Phase 3.11): a run fenced every node and stopped before the
/// clear; the cluster was then reactivated and a leader opened a new
/// generation and decided deletes. The resume does not trust the journal's
/// `Fenced`: it freezes and fences again, so those deletes are refused
/// after the restore.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fence_reopened_between_runs_is_closed_again_before_the_clear() {
    let mut c = cluster().await;
    put(&c, "kept", 19).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    // Fenced, stopped before the clear (a process started meanwhile).
    c.stop_node(2).await;
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    c.start_node(2).await;
    let closed = arca_lake_controller::delete_fence::close_and_push(
        c.meta.as_ref(),
        &c.clients,
        "admin restore",
        true,
        now_ms(),
    )
    .await
    .unwrap();
    let mut j = mr::Journal::load(&path).unwrap();
    j.closed_generation = closed.record.generation;
    j.done.push(Step::Fenced);
    j.save(&path).unwrap();
    // Reactivated: Active, hold released, a leader opens and decides.
    arca_lake_metadata::ops::recovery::activate_read_only(c.meta.as_ref(), "op", now_ms())
        .await
        .unwrap()
        .unwrap();
    arca_lake_controller::delete_fence::release_and_push(
        c.meta.as_ref(),
        &c.clients,
        "op",
        now_ms(),
    )
    .await
    .unwrap();
    let ctl = leading(&c, "ctl-between").await;
    let deletes = decided_deletes(&c, "kept").await;
    let old = frozen(&c).await;
    ctl.abort();
    // Delivered in the middle of the restore (before the mark closes the
    // fence again on the restored metadata).
    let during = Delivering {
        inner: &backup,
        old,
        deletes,
        applied: AtomicUsize::new(0),
    };
    let j = mr::run(
        &c.meta,
        &c.clients,
        &during,
        &path,
        &p.journal.token,
        &[],
        &[],
        now_ms,
    )
    .await
    .unwrap();
    assert_eq!(j.last(), Some(Step::Verified));
    assert_eq!(
        during.applied.load(Ordering::SeqCst),
        0,
        "a reopened generation's deletes applied during the restore"
    );
    assert!(
        j.closed_generation > closed.record.generation,
        "fenced again"
    );
    c.shutdown().await;
}

/// Re-review (Phase 3.11): the restore was written back and the run died
/// before recording `Restored`; the restored copy was then put to use and
/// every node's fence reopened by hand (the copy's own leader would find
/// the nodes ahead of its record and hold the fence; ADR-0024). A retry,
/// which would clear and restore again, finds the nodes no longer held
/// and clears nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_after_the_clear_refuses_once_the_fence_was_reopened() {
    use arca_lake_protocol::client::FencePush;
    let c = cluster().await;
    put(&c, "kept", 20).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    backup.fail.store(1, Ordering::SeqCst);
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert_eq!(
        mr::Journal::load(&path).unwrap().last(),
        Some(Step::Cleared)
    );
    // Written back; the run died.
    backup.restore("snapshot://test", 1).await.unwrap();
    let inc = arca_lake_metadata::ops::compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap()
        .incarnation;
    let ids = arca_lake_metadata::ops::node_lifecycle::identities(c.meta.as_ref())
        .await
        .unwrap();
    for n in &c.nodes {
        let addr = n.addr.to_string();
        let identity = ids.get(&n.id).map(|i| (n.id, i.token));
        let h = c.clients.health(&addr).await.unwrap();
        c.clients
            .push_delete_fence_with(
                &addr,
                h.delete_fence_generation,
                false,
                FencePush {
                    release: true,
                    incarnation: Some(inc),
                    identity,
                    ..FencePush::default()
                },
            )
            .await
            .unwrap();
        c.clients
            .push_delete_fence_with(
                &addr,
                h.delete_fence_generation + 1,
                true,
                FencePush {
                    incarnation: Some(inc),
                    opener: 7,
                    identity,
                    ..FencePush::default()
                },
            )
            .await
            .unwrap();
        assert!(c.clients.health(&addr).await.unwrap().delete_fence_open);
    }
    let e = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("no longer hold the restore's fence"), "{e}");
    assert_eq!(backup.restores.load(Ordering::SeqCst), 1, "restored again");
    c.shutdown().await;
}

/// Re-review (Phase 3.11): the clear committed and the run died before
/// recording it (the journal says `Clearing`, the store is empty); every
/// node's fence was then reopened by hand. The retry finds the nodes no
/// longer held and restores nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clear_committed_before_its_record_refuses_once_the_fence_was_reopened() {
    use arca_lake_protocol::client::FencePush;
    let c = cluster().await;
    put(&c, "kept", 20).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    backup.fail.store(1, Ordering::SeqCst);
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert_eq!(
        mr::Journal::load(&path).unwrap().last(),
        Some(Step::Cleared)
    );
    // As if it died between the clear's commit and its record.
    let mut j = mr::Journal::load(&path).unwrap();
    j.done.retain(|s| *s <= Step::Clearing);
    j.save(&path).unwrap();
    let ids: std::collections::HashMap<
        u64,
        arca_lake_metadata::ops::node_lifecycle::NodeIdentityRecord,
    > = j
        .identities
        .iter()
        .map(|(id, t)| {
            (
                *id,
                arca_lake_metadata::ops::node_lifecycle::NodeIdentityRecord {
                    node_id: *id,
                    token: *t,
                    acknowledged: true,
                },
            )
        })
        .collect();
    for n in &c.nodes {
        let addr = n.addr.to_string();
        let identity = ids.get(&n.id).map(|i| (n.id, i.token));
        let h = c.clients.health(&addr).await.unwrap();
        let inc = <[u8; 16]>::try_from(h.incarnation.as_ref()).unwrap();
        c.clients
            .push_delete_fence_with(
                &addr,
                h.delete_fence_generation,
                false,
                FencePush {
                    release: true,
                    incarnation: Some(inc),
                    identity,
                    ..FencePush::default()
                },
            )
            .await
            .unwrap();
        c.clients
            .push_delete_fence_with(
                &addr,
                h.delete_fence_generation + 1,
                true,
                FencePush {
                    incarnation: Some(inc),
                    opener: 7,
                    identity,
                    ..FencePush::default()
                },
            )
            .await
            .unwrap();
        assert!(c.clients.health(&addr).await.unwrap().delete_fence_open);
    }
    let e = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("no longer hold the restore's fence"), "{e}");
    assert_eq!(backup.restores.load(Ordering::SeqCst), 0, "restored");
    c.shutdown().await;
}

/// Final review (Phase 3.11): a node lost for good after the clear does
/// not trap the restore: the retry refuses until the operator names it
/// with --absent-node, then completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_lost_after_the_clear_can_be_declared_absent() {
    let mut c = cluster().await;
    put(&c, "kept", 21).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    backup.fail.store(1, Ordering::SeqCst);
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert_eq!(
        mr::Journal::load(&path).unwrap().last(),
        Some(Step::Cleared)
    );
    c.stop_node(2).await;
    let gone = c.nodes[2].id;
    let e = run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap_err();
    assert!(e.contains("--absent-node"), "{e}");
    let j = mr::run(
        &c.meta,
        &c.clients,
        &backup,
        &path,
        &p.journal.token,
        &[],
        &[gone],
        now_ms,
    )
    .await
    .unwrap();
    assert_eq!(j.last(), Some(Step::Verified));
    c.shutdown().await;
}

/// A restorer whose tool reaches another database (a cluster file at the
/// same path that names another cluster, a link or mount replaced since
/// the plan, a container's own file): it does not see the probe.
struct Elsewhere<'a>(&'a Snapshot);
impl Restorer for Elsewhere<'_> {
    fn destination(&self) -> String {
        self.0.destination()
    }
    fn probe(
        &self,
        _key: &[u8],
        _value: &[u8],
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send {
        std::future::ready(Ok(false))
    }
    async fn restore(&self, source: &str, version: u64) -> Result<(), String> {
        self.0.restore(source, version).await
    }
}

/// Phase 3.12 (§4.1): the destination is identified by content. A tool
/// that does not read back the probe written through this command's own
/// connection clears nothing; the planned store is untouched and the same
/// journal then completes with the right tool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_tool_reaching_another_database_clears_nothing() {
    let c = cluster().await;
    let kept = put(&c, "kept", 30).await;
    let backup = Snapshot::take(&c.meta).await;
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &backup, &path).await.unwrap();
    let e = mr::run(
        &c.meta,
        &c.clients,
        &Elsewhere(&backup),
        &path,
        &p.journal.token,
        &[],
        &[],
        now_ms,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(e.contains("not the store this command uses"), "{e}");
    let j = mr::Journal::load(&path).unwrap();
    assert!(!j.done.contains(&Step::Clearing), "{:?}", j.done);
    assert_eq!(backup.restores.load(Ordering::SeqCst), 0);
    assert!(
        arca_lake_metadata::ops::compat::cluster_state(c.meta.as_ref())
            .await
            .unwrap()
            .is_some(),
        "the planned store was cleared"
    );
    run(&c, &backup, &path, &p.journal.token, &[])
        .await
        .unwrap();
    let ctl = spawn(Arc::clone(&c.meta), controller("ctl-after"))
        .await
        .unwrap();
    finish(&c, &path).await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    ctl.shutdown().await;
    c.shutdown().await;
}

/// The right backup only after an amend: the restorer serves another
/// cluster's backup until the test switches it.
struct Foreign<'a> {
    theirs: Snapshot,
    ours: &'a Snapshot,
    amended: std::sync::atomic::AtomicBool,
}
impl Restorer for Foreign<'_> {
    fn destination(&self) -> String {
        self.ours.destination()
    }
    async fn probe(&self, key: &[u8], value: &[u8]) -> Result<bool, String> {
        self.ours.probe(key, value).await
    }
    async fn restore(&self, source: &str, version: u64) -> Result<(), String> {
        if self.amended.load(Ordering::SeqCst) {
            self.ours.restore(source, version).await
        } else {
            self.theirs.restore(source, version).await
        }
    }
}

/// Phase 3.12 (§4.1, wrong cluster): a backup carrying another cluster's
/// uid is refused before the mark, nothing is pushed to the nodes, and the
/// cleared store's journal goes on once the backup is replaced (`amend`);
/// amend is refused before the clear.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_clusters_backup_is_refused_and_amended() {
    let c = cluster().await;
    let kept = put(&c, "kept", 31).await;
    arca_lake_metadata::ops::compat::ensure_cluster_uid(c.meta.as_ref())
        .await
        .unwrap();
    let ours = Snapshot::take(&c.meta).await;
    let mut data: Vec<(Vec<u8>, Vec<u8>)> = ours
        .data
        .iter()
        .filter(|(k, _)| *k != keys::cluster_uid())
        .cloned()
        .collect();
    data.push((keys::cluster_uid(), vec![0xee; 16]));
    let r = Foreign {
        theirs: Snapshot {
            meta: Arc::clone(&c.meta),
            data,
            fail: AtomicUsize::new(0),
            restores: AtomicUsize::new(0),
        },
        ours: &ours,
        amended: std::sync::atomic::AtomicBool::new(false),
    };
    let dir = tempfile::tempdir().unwrap();
    let path = journal_path(&dir);
    let p = plan(&c, &ours, &path).await.unwrap();
    assert!(
        mr::amend(&path, "snapshot://test", 1, None).is_err(),
        "amend before the clear"
    );
    let e = mr::run(
        &c.meta,
        &c.clients,
        &r,
        &path,
        &p.journal.token,
        &[],
        &[],
        now_ms,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(e.contains("another cluster"), "{e}");
    let j = mr::Journal::load(&path).unwrap();
    assert!(j.done.contains(&Step::Cleared) && !j.done.contains(&Step::Restored));
    // Nothing marked: the nodes still hold the fence closed for the restore.
    for n in &c.nodes {
        let h = c.clients.health(&n.addr.to_string()).await.unwrap();
        assert!(!h.delete_fence_open && h.delete_fence_hold, "{h:?}");
    }
    mr::amend(&path, "snapshot://test", 1, Some(now_ms())).unwrap();
    r.amended.store(true, Ordering::SeqCst);
    mr::run(
        &c.meta,
        &c.clients,
        &r,
        &path,
        &p.journal.token,
        &[],
        &[],
        now_ms,
    )
    .await
    .unwrap();
    let ctl = spawn(Arc::clone(&c.meta), controller("ctl-after"))
        .await
        .unwrap();
    finish(&c, &path).await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Phase 3.12 (ADR-0028 H1): `plan` refuses a backup outside deferred
/// reclamation's protection (nodes that do not defer, an unknown backup
/// point, a backup older than the retention less twice the skew), unless
/// accepted explicitly; `run` refuses one that left it since the plan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plan_refuses_a_backup_outside_the_retention_unless_accepted() {
    let plan_at = |c: &TestCluster, point: Option<u64>, accept: bool, path: std::path::PathBuf| {
        let meta = Arc::clone(&c.meta);
        let clients = c.clients.clone();
        async move {
            mr::plan(
                meta.as_ref(),
                &clients,
                "snapshot://test",
                1,
                point,
                "in-memory test store",
                &path,
                accept,
                now_ms(),
            )
            .await
            .map_err(|e| e.to_string())
        }
    };
    let dir = tempfile::tempdir().unwrap();
    // Nodes that do not defer (retention 0, as an earlier release).
    let c = cluster().await;
    let e = plan_at(&c, Some(now_ms()), false, dir.path().join("a"))
        .await
        .unwrap_err();
    assert!(e.contains("do not defer"), "{e}");
    let p = plan_at(&c, Some(now_ms()), true, dir.path().join("a"))
        .await
        .unwrap();
    assert!(p.journal.unprotected.is_some());
    assert!(
        p.summary.iter().any(|l| l.starts_with("ACCEPTED")),
        "{:?}",
        p.summary
    );
    c.shutdown().await;
    // Nodes deferring for 4 s with the default skew allowance (60 s):
    // nothing is younger than R - 2ε < 0, so every backup is outside.
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        reclaim_retention: Duration::from_secs(4),
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    c.acknowledge_identities().await;
    let e = plan_at(&c, Some(now_ms()), false, dir.path().join("b"))
        .await
        .unwrap_err();
    assert!(e.contains("older than the nodes' retention"), "{e}");
    let e = plan_at(&c, None, false, dir.path().join("b"))
        .await
        .unwrap_err();
    assert!(e.contains("backup point is not given"), "{e}");
    c.shutdown().await;
    // Retention of an hour: a backup of now is inside, one of two hours
    // ago is not; inside at plan, outside at run: refused before the clear.
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        reclaim_retention: Duration::from_secs(3600),
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    c.acknowledge_identities().await;
    let e = plan_at(&c, Some(now_ms() - 7_200_000), false, dir.path().join("c"))
        .await
        .unwrap_err();
    assert!(e.contains("older than"), "{e}");
    let p = plan_at(&c, Some(now_ms()), false, dir.path().join("c"))
        .await
        .unwrap();
    assert!(p.journal.unprotected.is_none());
    let until = p.journal.protected_until_ms.unwrap();
    assert!(until > now_ms() + 3_000_000, "{until}");
    let backup = Snapshot::take(&c.meta).await;
    let late = move || until + 1;
    let e = mr::run(
        &c.meta,
        &c.clients,
        &backup,
        &dir.path().join("c"),
        &p.journal.token,
        &[],
        &[],
        late,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(e.contains("left the protection"), "{e}");
    assert!(
        arca_lake_metadata::ops::compat::cluster_state(c.meta.as_ref())
            .await
            .unwrap()
            .is_some()
    );
    c.shutdown().await;
}
