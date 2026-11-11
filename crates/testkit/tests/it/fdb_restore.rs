//! Metadata backup and restore drill on a real FoundationDB cluster with
//! `fdbbackup`/`fdbrestore` (RFC-015 §5.3.1, Phase 3.3 §2.4): the
//! metadata goes back to the backup point while the storage nodes keep
//! their current shards; the recovery state machine catches up the
//! epochs, classifies every difference, refuses writes and deletes until
//! an operator accepts the report, and the cluster then serves exactly
//! what the restored metadata describes. Writes after the backup point
//! are lost (RPO) and objects whose shards were collected after the point
//! are reported, never recovered: a metadata backup is not a data backup.
//!
//! Requires `ARCA_FDB_TESTS=1`, `ARCA_FDB_CLUSTER_FILE`, and a backup
//! driver: `ARCA_FDB_BACKUP_URL` (a `file://` URL as the backup agent
//! sees it) and optionally `ARCA_FDB_BACKUP_CMD` (a command prefix such
//! as `docker exec <container>` that runs `fdbbackup`/`fdbrestore` where
//! the agent lives) and `ARCA_FDB_BACKUP_CLUSTER_FILE` (the cluster file
//! path as that command sees it). A `backup_agent` must be running
//! against the cluster. Skips otherwise.

#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_controller::recovery::RecoveryOptions;
use arca_lake_metadata::fdb::FdbKv;
use arca_lake_metadata::guard::{GuardedKv, OpClass};
use arca_lake_metadata::ops::{compat, recovery};
use arca_lake_metadata::records::ClusterPhase;
use arca_lake_metadata::{KvError, KvStore, Txn};
use arca_lake_testkit::s3::{self, PutOptions, request};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::S3;
use s3s::dto::{
    BucketVersioningStatus, CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadInput, ETag, PutBucketVersioningInput, StreamingBlob, UploadPartInput,
    VersioningConfiguration,
};

const MIB: u64 = 1 << 20;
const BUCKET: &str = "restore";

struct Backup {
    prefix: Vec<String>,
    url: String,
    cluster_file: String,
}

impl Backup {
    fn from_env() -> Option<Self> {
        std::env::var_os("ARCA_FDB_TESTS")?;
        let url = std::env::var("ARCA_FDB_BACKUP_URL").ok()?;
        let prefix = std::env::var("ARCA_FDB_BACKUP_CMD")
            .ok()
            .map(|p| p.split_whitespace().map(str::to_string).collect())
            .unwrap_or_default();
        let cluster_file = std::env::var("ARCA_FDB_BACKUP_CLUSTER_FILE")
            .ok()
            .or_else(|| std::env::var("ARCA_FDB_CLUSTER_FILE").ok())?;
        Some(Self {
            prefix,
            url,
            cluster_file,
        })
    }

    fn run(&self, tool: &str, args: &[&str]) -> (bool, String) {
        let mut cmd = if self.prefix.is_empty() {
            Command::new(tool)
        } else {
            let mut c = Command::new(&self.prefix[0]);
            c.args(&self.prefix[1..]);
            c.arg(tool);
            c
        };
        // Subcommand first, then options (`fdbbackup start -C <file> ...`).
        cmd.args(args);
        if tool == "fdbbackup" && args.first() != Some(&"list") {
            cmd.arg("-C").arg(&self.cluster_file);
        }
        let out = cmd.output().expect("run backup tool");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        eprintln!("{tool} {args:?}: {}", text.trim());
        (out.status.success(), text)
    }

    /// Take a full backup and wait for it; returns the backup's own URL.
    /// The mutation log runs on for a while past the snapshot, so the
    /// restore names its target version explicitly: the backup's minimum
    /// restorable version (the end of the snapshot, before any write the
    /// test makes after this returns). Without `-v`, `fdbrestore` would
    /// bring back everything up to the last log and there would be no RPO
    /// to drill.
    fn backup(&self) -> String {
        let (ok, text) = self.run("fdbbackup", &["start", "-d", &self.url, "-w"]);
        assert!(ok, "fdbbackup start: {text}");
        let (ok, list) = self.run("fdbbackup", &["list", "-b", &self.url]);
        assert!(ok, "fdbbackup list: {list}");
        list.lines()
            .filter(|l| l.trim().starts_with("file://"))
            .map(|l| l.trim().to_string())
            .max()
            .expect("a backup url")
    }

    /// The snapshot end of a backup (`MinRestorableVersion`).
    fn min_restorable_version(&self, backup_url: &str) -> u64 {
        let (ok, text) = self.run("fdbbackup", &["describe", "-d", backup_url]);
        assert!(ok, "fdbbackup describe: {text}");
        text.lines()
            .find_map(|l| {
                l.trim()
                    .strip_prefix("MinRestorableVersion:")
                    .and_then(|v| v.split_whitespace().next())
                    .and_then(|v| v.parse().ok())
            })
            .unwrap_or_else(|| panic!("no MinRestorableVersion in {text}"))
    }

    /// Restore `backup_url` as of `version` into the (emptied) cluster and
    /// wait.
    fn restore(&self, backup_url: &str, version: u64) {
        let v = version.to_string();
        let (ok, text) = self.run(
            "fdbrestore",
            &[
                "start",
                "--dest-cluster-file",
                &self.cluster_file,
                "-r",
                backup_url,
                "-v",
                &v,
                "-w",
            ],
        );
        assert!(ok, "fdbrestore: {text}");
    }
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn shards_total(c: &TestCluster<FdbKv>) -> u64 {
    let mut n = 0;
    for node in &c.nodes {
        n += c
            .clients
            .health(&node.addr.to_string())
            .await
            .unwrap()
            .live_shards;
    }
    n
}

async fn multipart_put(c: &TestCluster<FdbKv>, key: &str, parts: usize, seed: u64) -> Bytes {
    let upload_id = c
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
    let mut all = Vec::new();
    let mut done = Vec::new();
    for n in 1..=parts {
        let data = payload(600 << 10, seed * 100 + n as u64);
        all.extend_from_slice(&data);
        let out = c
            .backend
            .upload_part(request(UploadPartInput {
                bucket: BUCKET.into(),
                key: key.into(),
                upload_id: upload_id.clone(),
                part_number: i32::try_from(n).unwrap(),
                body: Some(StreamingBlob::from_bytes(data)),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let Some(ETag::Strong(etag) | ETag::Weak(etag)) = out.e_tag else {
            panic!("no etag")
        };
        done.push(CompletedPart {
            part_number: Some(i32::try_from(n).unwrap()),
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        });
    }
    c.backend
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: BUCKET.into(),
            key: key.into(),
            upload_id,
            multipart_upload: Some(CompletedMultipartUpload { parts: Some(done) }),
            ..Default::default()
        }))
        .await
        .unwrap();
    Bytes::from(all)
}

#[tokio::test]
async fn metadata_restore_drill_with_fdbbackup() {
    let Some(backup) = Backup::from_env() else {
        eprintln!("ARCA_FDB_TESTS / ARCA_FDB_BACKUP_URL not set; skipping");
        return;
    };
    let path = std::env::var("ARCA_FDB_CLUSTER_FILE").ok();
    let kv = FdbKv::open_with_timeout(path.as_deref(), 10_000).expect("open FoundationDB");
    kv.clear_range_blocking(&arca_lake_metadata::keys::all_range())
        .await
        .unwrap();
    // fdbrestore needs an empty destination: refuse (never clear) keys
    // outside arca-lake's subspace, which may belong to someone else.
    {
        let mut tx = kv.begin().await.unwrap();
        let foreign = tx
            .get_range(
                &arca_lake_metadata::kv::KeyRange::new(Vec::new(), vec![0xff]),
                5,
                false,
            )
            .await
            .unwrap();
        let _ = tx.commit().await;
        assert!(
            foreign.is_empty(),
            "the drill needs a dedicated, otherwise empty FoundationDB cluster; found keys {:?}",
            foreign
                .iter()
                .map(|(k, _)| String::from_utf8_lossy(k).to_string())
                .collect::<Vec<_>>()
        );
    }
    let meta = Arc::new(kv);
    let mut c = TestCluster::start_with(
        Arc::clone(&meta),
        ClusterOptions {
            groups: 1,
            // No gateway heartbeat: fdbrestore needs an empty database, and
            // in the real procedure every process is stopped before the
            // restore; the backend is kept only to prove it is fenced.
            heartbeat: None,
            ..ClusterOptions::rs(3, 2, 1, MIB, 0)
        },
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut b = PutBucketVersioningInput::builder();
    b.set_bucket(BUCKET.into());
    b.set_versioning_configuration(VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(
            BucketVersioningStatus::ENABLED,
        )),
        ..Default::default()
    });
    c.backend
        .put_bucket_versioning(request(b.build().unwrap()))
        .await
        .unwrap();

    // ---- Before the backup point: four objects and a multipart object.
    let mut pre: Vec<(String, Bytes)> = Vec::new();
    for i in 0..4u64 {
        let d = payload(usize::try_from(MIB / 2).unwrap(), i);
        s3::put(
            &c.backend,
            BUCKET,
            &format!("pre{i}"),
            d.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        pre.push((format!("pre{i}"), d));
    }
    let mp = multipart_put(&c, "mp", 2, 7).await;
    pre.push(("mp".into(), mp));
    let shards_at_t = shards_total(&c).await;
    let backup_url = backup.backup();
    // The restore point: the end of the snapshot.
    let restore_version = backup.min_restorable_version(&backup_url);
    let restore_point = now_ms();
    eprintln!("backup at {restore_point} (version {restore_version}): {backup_url}");

    // ---- After the backup point: new objects, an overwrite (a new version
    // of pre1), a delete whose shards GC collects (pre2), a multipart
    // object, and a delete marker on pre3.
    let mut post: Vec<(String, Bytes)> = Vec::new();
    for i in 0..3u64 {
        let d = payload(usize::try_from(MIB / 2).unwrap(), 100 + i);
        s3::put(
            &c.backend,
            BUCKET,
            &format!("post{i}"),
            d.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        post.push((format!("post{i}"), d));
    }
    let pre1_v2 = payload(usize::try_from(MIB / 2).unwrap(), 201);
    s3::put(
        &c.backend,
        BUCKET,
        "pre1",
        pre1_v2.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let mp2 = multipart_put(&c, "mp2", 2, 8).await;
    post.push(("mp2".into(), mp2));
    // pre2: its shards are gone from every node after T (what a delete
    // followed by GC leaves behind); the restored metadata still
    // references them.
    let pre2 = c.manifest(BUCKET, "pre2").await;
    let group = c.groups().await.remove(0);
    for (_, ch) in &pre2.chunks {
        for (i, m) in group.members.iter().enumerate() {
            let n = c.node_index(m).unwrap();
            c.node_store(n)
                .delete(arca_lake_storage_node::ShardKey {
                    group_id: group.id,
                    chunk_id: ch.chunk_id,
                    shard_index: u16::try_from(i).unwrap(),
                })
                .unwrap();
        }
    }
    // pre0: one shard lost after T (a failed disk): degraded, recoverable.
    {
        let ch = &c.manifest(BUCKET, "pre0").await.chunks[0].1;
        let n = c.node_index(&group.members[2]).unwrap();
        c.node_store(n)
            .delete(arca_lake_storage_node::ShardKey {
                group_id: group.id,
                chunk_id: ch.chunk_id,
                shard_index: 2,
            })
            .unwrap();
    }
    s3::delete(&c.backend, BUCKET, "pre3").await.unwrap();
    let shards_now = shards_total(&c).await;
    assert!(shards_now > shards_at_t, "post-T shards exist on the nodes");
    let pre_shard_loss: u64 = 3; // pre2: one 512 KiB chunk of three shards, gone

    // ---- The restore: every arca-lake process would be stopped now; the
    // in-process gateway backend and the clients stay around to prove they
    // are fenced out.
    meta.clear_range_blocking(&arca_lake_metadata::keys::all_range())
        .await
        .unwrap();
    backup.restore(&backup_url, restore_version);
    // Freshly opened handle on the restored metadata.
    let restored = Arc::new(FdbKv::open_with_timeout(path.as_deref(), 10_000).unwrap());
    let g = compat::gate(restored.as_ref()).await.unwrap();
    assert_eq!(
        g.schema,
        Some(arca_lake_metadata::keys::SCHEMA_VERSION),
        "the schema key came back"
    );
    let old = g.cluster.clone().unwrap();
    // `admin recovery begin --yes`.
    let marked = compat::mark_restoring(
        restored.as_ref(),
        Some(backup_url.clone()),
        Some(restore_point),
        "admin",
        now_ms(),
    )
    .await
    .unwrap();
    assert_ne!(marked.incarnation, old.incarnation);
    // The stale gateway (old incarnation) is fenced at commit even before
    // its refresh notices.
    let e = s3::put(
        &c.backend,
        BUCKET,
        "stale",
        payload(1024, 9),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(
        e.to_string().contains("incarnation") || e.to_string().contains("cluster"),
        "{e}"
    );
    // Metadata of post-T objects is gone; pre-T metadata is back.
    let mut tx = restored.begin().await.unwrap();
    assert!(
        tx.get(&arca_lake_metadata::keys::object_current(1, "post0"))
            .await
            .unwrap()
            .is_none()
    );
    let _ = tx.commit().await;

    // ---- A controller on the restored store runs catch-up and the
    // verification; nothing is deleted meanwhile.
    let mut cfg = ControllerConfig::new("restore-ctl");
    cfg.lease = Duration::from_millis(1500);
    cfg.gc_interval = Duration::from_millis(500);
    cfg.reconcile_interval = Duration::from_millis(500);
    cfg.min_orphan_age = Duration::ZERO;
    cfg.gc_grace = Duration::ZERO;
    cfg.repair_interval = Duration::from_millis(500);
    cfg.stripe_repair_interval = Duration::from_millis(500);
    cfg.scrub.interval_ms = 0;
    cfg.health_interval = Duration::from_millis(300);
    cfg.recovery_interval = Duration::from_millis(100);
    cfg.recovery = RecoveryOptions {
        page: 8,
        page_bytes: 0,
        pages_per_pass: 4,
    };
    cfg.heartbeat = Some(Duration::from_millis(500));
    let ctl = spawn(Arc::clone(&restored), cfg).await.unwrap();
    let r = Arc::clone(&restored);
    wait_until(
        "verification to finish",
        Duration::from_secs(120),
        async || {
            matches!(
                compat::cluster_state(r.as_ref())
                    .await
                    .unwrap()
                    .map(|s| s.state),
                Some(ClusterPhase::ReadOnly | ClusterPhase::RecoveryRequired)
            )
        },
    )
    .await;
    let state = compat::cluster_state(restored.as_ref())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        state.state,
        ClusterPhase::RecoveryRequired,
        "pre2's shards were collected after T"
    );
    let rep = recovery::report(restored.as_ref(), &marked.incarnation)
        .await
        .unwrap()
        .unwrap();
    let k = &rep.counts;
    eprintln!("report: {k:?} affected {:?}", rep.affected);
    assert!(rep.complete, "{rep:?}");
    assert_eq!(k.epochs_raised, 1);
    assert_eq!(k.unreadable_chunks, 1, "pre2: {k:?}");
    assert_eq!(k.missing, pre_shard_loss + 1, "pre2 and one of pre0: {k:?}");
    // Post-T shards: post0..2 (one chunk each), pre1 v2 (one), mp2 (two
    // parts of one chunk): 6 chunks of 3 shards.
    assert_eq!(k.orphan_post_t, 6 * 3, "{k:?}");
    assert_eq!(k.orphan_pre_t, 0, "{k:?}");
    assert_eq!(k.corrupt, 0);
    assert_eq!(k.degraded_chunks, 1, "pre0: {k:?}");
    assert_eq!(k.manifest_findings, 0, "the pre-T multipart root verifies");
    assert_eq!(k.matched, shards_at_t - pre_shard_loss - 1);
    assert_eq!(rep.affected.len(), 1, "{:?}", rep.affected);
    assert!(
        rep.affected
            .iter()
            .all(|a| a.bucket == BUCKET && a.key == "pre2" && a.why == "unreadable")
    );
    // Nothing was deleted while the cluster was not Active: every post-T
    // shard is still on the nodes, and the controller's GC/reconcile
    // passes were skipped by state. Repair is admitted in
    // RecoveryRequired: the controller rebuilds pre0's lost shard; pre2
    // stays an unrecoverable task (recorded, never written).
    assert!(shards_total(&c).await >= shards_now);
    let r = Arc::clone(&restored);
    wait_until("repair of pre0", Duration::from_secs(60), async || {
        let t = arca_lake_metadata::ops::integrity::tasks(r.as_ref(), usize::MAX)
            .await
            .unwrap();
        t.len() == 1
            && t[0].2.reason == arca_lake_metadata::ops::integrity::Finding::UnrecoverableStripe
    })
    .await;
    assert_eq!(
        shards_total(&c).await,
        shards_now + 1,
        "one shard rebuilt, none deleted"
    );
    let m = {
        let addr = ctl.http_addr;
        let _ = addr;
        ctl.metrics
            .passes_skipped_by_state_total
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    assert!(m > 0, "deleting passes were skipped while not Active");
    // A restarted gateway serves reads only: writes are refused, GET of a
    // pre-T object returns the restore-point content, post-T keys are gone
    // (RPO), pre2 is an integrity error.
    c.restart_backend().await.unwrap();
    assert_eq!(c.backend.cluster().incarnation, marked.incarnation);
    assert!(
        s3::put(
            &c.backend,
            BUCKET,
            "x",
            payload(1024, 9),
            &PutOptions::default()
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("RecoveryRequired")
    );
    for (key, data) in &pre {
        match key.as_str() {
            "pre2" => {
                let e = s3::get(&c.backend, BUCKET, key).await.unwrap_err();
                assert!(e.to_string().contains("unreadable"), "{e}");
            }
            _ => assert_eq!(
                &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
                data,
                "{key}"
            ),
        }
    }
    for (key, _) in &post {
        assert!(
            s3::head(&c.backend, BUCKET, key).await.is_err(),
            "{key} was written after T"
        );
    }
    assert_ne!(
        s3::get(&c.backend, BUCKET, "pre1").await.unwrap().0,
        pre1_v2,
        "the post-T version is gone"
    );
    // The delete class is refused by the guard under RecoveryRequired.
    let del = GuardedKv::new(Arc::clone(&restored), marked.incarnation, OpClass::Delete);
    let mut tx = del.begin().await.unwrap();
    tx.set(b"zz-probe", b"1");
    assert!(matches!(tx.commit().await, Err(KvError::ClusterState(_))));

    // ---- `admin recovery finalize`: refused without --force (pre2 is
    // unreadable), refused with --force but no reason, then accepted; the
    // finding stays in the report and pre2 gets a data-loss record.
    let fin = |force: bool, reason: &str| recovery::FinalizeRequest {
        incarnation: marked.incarnation,
        operator: "drill".into(),
        reason: reason.into(),
        force,
        now_ms: now_ms(),
    };
    let (v, _) = recovery::finalize(restored.as_ref(), &fin(false, ""))
        .await
        .unwrap();
    assert!(
        v.refusal
            .as_deref()
            .is_some_and(|w| w.contains("unreadable")),
        "{v:?}"
    );
    let (v, _) = recovery::finalize(restored.as_ref(), &fin(true, ""))
        .await
        .unwrap();
    assert!(v.refusal.is_some(), "{v:?}");
    let (v, audit) = recovery::finalize(
        restored.as_ref(),
        &fin(true, "restore drill: pre2 written after the backup point"),
    )
    .await
    .unwrap();
    assert_eq!(v.refusal, None, "{v:?}");
    assert_eq!(audit.result, "activated-forced");
    let losses = recovery::data_losses(restored.as_ref()).await.unwrap();
    assert!(losses.iter().any(|l| l.object.key == "pre2"), "{losses:?}");
    assert_eq!(
        recovery::finalize_audits(restored.as_ref())
            .await
            .unwrap()
            .len(),
        3
    );
    c.backend
        .cluster()
        .set(compat::cluster_state(restored.as_ref()).await.unwrap());
    let d = payload(4096, 300);
    s3::put(
        &c.backend,
        BUCKET,
        "after",
        d.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "after").await.unwrap().0, d);
    // Now Active: the controller's reconciliation reclaims the post-T
    // orphans (minimum age zero here); pre-T objects stay intact.
    wait_until(
        "reconciliation of the post-T orphans",
        Duration::from_secs(60),
        async || shards_total(&c).await <= shards_at_t - pre_shard_loss + 3,
    )
    .await;
    for (key, data) in pre.iter().filter(|(k, _)| k != "pre2") {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    let reports = recovery::reports(restored.as_ref()).await.unwrap();
    assert_eq!(reports.len(), 1);
    assert!(reports[0].accepted.as_ref().is_some_and(|a| a.forced));
    ctl.shutdown().await;
    c.shutdown().await;
}

/// R-94(b) on a real FoundationDB cluster (Phase 3.11, ADR-0025): a
/// controller leader is left running through `fdbrestore`, without the
/// runbook's fence close. Its GC decided, after the backup point, to
/// collect an object deleted after that point, and those `DeleteShard`
/// RPCs are held in flight. The operator clears the keyspace; the held
/// RPCs land; the operator restores the backup. The leader saw the
/// cleared store first, closed its generation on the nodes and stepped
/// down: every shard the restored
/// metadata references is still there, and the next term holds the fence
/// for the operator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_left_running_through_fdbrestore_deletes_nothing_the_restore_needs() {
    use arca_lake_controller::process::RpcFaultHook;
    use arca_lake_metadata::ops::delete_fence;
    use arca_lake_protocol::client::{FaultAction, Rpc};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let Some(backup) = Backup::from_env() else {
        eprintln!("ARCA_FDB_TESTS / ARCA_FDB_BACKUP_URL not set; skipping");
        return;
    };
    let path = std::env::var("ARCA_FDB_CLUSTER_FILE").ok();
    let kv = FdbKv::open_with_timeout(path.as_deref(), 10_000).expect("open FoundationDB");
    kv.clear_range_blocking(&arca_lake_metadata::keys::all_range())
        .await
        .unwrap();
    let meta = Arc::new(kv);
    let mut c = TestCluster::start_with(
        Arc::clone(&meta),
        ClusterOptions {
            groups: 1,
            heartbeat: None,
            ..ClusterOptions::rs(3, 2, 1, MIB, 0)
        },
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let kept = payload(usize::try_from(2 * MIB).unwrap(), 31);
    s3::put(
        &c.backend,
        BUCKET,
        "kept",
        kept.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let held = Arc::new(AtomicUsize::new(0));
    let rpc_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut cfg = ControllerConfig::new("ctl-running");
    cfg.lease = Duration::from_secs(60);
    cfg.health_interval = Duration::from_millis(200);
    cfg.gc_interval = Duration::from_millis(200);
    cfg.gc_grace = Duration::ZERO;
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.failure_domain = "host".into();
    cfg.heartbeat = None;
    cfg.rpc_fault_hook = Some(RpcFaultHook({
        let (armed, held, rpc_gate) =
            (Arc::clone(&armed), Arc::clone(&held), Arc::clone(&rpc_gate));
        Arc::new(move |ctx| {
            if ctx.rpc == Rpc::DeleteShard && armed.load(Ordering::SeqCst) {
                held.fetch_add(1, Ordering::SeqCst);
                FaultAction::Hold(Arc::clone(&rpc_gate))
            } else {
                FaultAction::None
            }
        })
    }));
    let ctl = spawn(Arc::clone(&meta), cfg).await.unwrap();
    // The leader's generation open on every node.
    wait_until(
        "the term open everywhere",
        Duration::from_secs(60),
        async || {
            let rec = delete_fence::current(meta.as_ref()).await.unwrap();
            let mut all = ctl.is_leader() && rec.open && rec.leader_token == ctl.token();
            for n in &c.nodes {
                let h = c.clients.health(&n.addr.to_string()).await.unwrap();
                all &= h.delete_fence_open && h.delete_fence_generation == rec.generation;
            }
            all
        },
    )
    .await;
    armed.store(true, Ordering::SeqCst);
    let shards_at_backup = shards_total(&c).await;
    let backup_url = backup.backup();
    let restore_version = backup.min_restorable_version(&backup_url);
    // After the backup point: deleted; GC decides and sends (held).
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    wait_until(
        "GC's deletes in flight",
        Duration::from_secs(60),
        async || held.load(Ordering::SeqCst) > 0,
    )
    .await;
    // The operator: clear, then fdbrestore; the held deletes land while
    // the restore runs.
    meta.clear_range_blocking(&arca_lake_metadata::keys::all_range())
        .await
        .unwrap();
    let t = std::time::Instant::now();
    while !ctl.metrics.delete_fence.restore_suspected() && t.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let tripped = ctl.metrics.delete_fence.restore_suspected();
    eprintln!("tripped after {:?}: {tripped}", t.elapsed());
    // The trip's close reaches the nodes (bounded: without the trip
    // nothing closes them while the store is empty).
    let t = std::time::Instant::now();
    while t.elapsed() < Duration::from_secs(5) {
        let mut open = false;
        for n in &c.nodes {
            open |= c
                .clients
                .health(&n.addr.to_string())
                .await
                .unwrap()
                .delete_fence_open;
        }
        if !open {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The in-flight deletes land before the restore (once it is done, the
    // restored lineage closes the fence anyway: ADR-0024), and the batch
    // finishes.
    let sent = ctl.metrics.gc.delete_generation.snapshot().count;
    rpc_gate.add_permits(1_000);
    wait_until(
        "the GC batch to finish",
        Duration::from_secs(60),
        async || ctl.metrics.gc.delete_generation.snapshot().count > sent,
    )
    .await;
    let url = backup_url.clone();
    tokio::task::spawn_blocking(move || backup.restore(&url, restore_version))
        .await
        .unwrap();
    assert_eq!(
        shards_total(&c).await,
        shards_at_backup,
        "deletes decided before the restore applied during it"
    );
    assert!(tripped, "the leader never saw the store cleared or locked");
    // The next term on the restored metadata holds the fence.
    wait_until(
        "the fence held for the operator",
        Duration::from_secs(60),
        async || {
            delete_fence::current(meta.as_ref())
                .await
                .is_ok_and(|r| r.operator_hold && !r.open)
        },
    )
    .await;
    ctl.abort();
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    c.shutdown().await;
}

/// `fdbrestore` into the test cluster; the first restore may be given a
/// version the backup cannot restore to (a failed restore).
struct Fdb {
    backup: Backup,
    // Version to use instead of the planned one, once.
    bad_once: std::sync::Mutex<Option<u64>>,
}
impl arca_lake_controller::managed_restore::Restorer for Fdb {
    fn destination(&self) -> String {
        self.backup.cluster_file.clone()
    }
    // Blocking: the test runs on a multi-threaded runtime and does
    // nothing else meanwhile.
    fn restore(
        &self,
        source: &str,
        version: u64,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send {
        let v = self.bad_once.lock().unwrap().take().unwrap_or(version);
        let (ok, text) = self.backup.run(
            "fdbrestore",
            &[
                "start",
                "--dest-cluster-file",
                &self.backup.cluster_file,
                "-r",
                source,
                "-v",
                &v.to_string(),
                "-w",
            ],
        );
        std::future::ready(if ok { Ok(()) } else { Err(text) })
    }
}

/// `admin restore` (Phase 3.11, ADR-0026 option D) with `fdbrestore` on a
/// real FoundationDB cluster: plan, a first run whose restore fails (a
/// version the backup cannot restore to), the re-run resuming from the
/// clear, finish. Deletes a leader decided after the backup point,
/// delivered after the restore, are refused; the restored object reads
/// back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_restore_with_fdbrestore_resumes_after_a_failed_restore() {
    use arca_lake_controller::managed_restore::{self as mr, Restorer, Step};
    use arca_lake_metadata::ops::delete_fence;
    let Some(backup) = Backup::from_env() else {
        eprintln!("ARCA_FDB_TESTS / ARCA_FDB_BACKUP_URL not set; skipping");
        return;
    };
    let path = std::env::var("ARCA_FDB_CLUSTER_FILE").ok();
    let kv = FdbKv::open_with_timeout(path.as_deref(), 10_000).expect("open FoundationDB");
    kv.clear_range_blocking(&arca_lake_metadata::keys::all_range())
        .await
        .unwrap();
    let meta = Arc::new(kv);
    let mut c = TestCluster::start_with(
        Arc::clone(&meta),
        ClusterOptions {
            groups: 1,
            heartbeat: None,
            ..ClusterOptions::rs(3, 2, 1, MIB, 0)
        },
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let kept = payload(usize::try_from(MIB).unwrap(), 41);
    s3::put(
        &c.backend,
        BUCKET,
        "kept",
        kept.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let mut cfg = ControllerConfig::new("ctl-before");
    cfg.lease = Duration::from_secs(5);
    cfg.health_interval = Duration::from_millis(200);
    cfg.failure_domain = "host".into();
    cfg.heartbeat = None;
    let ctl = spawn(Arc::clone(&meta), cfg.clone()).await.unwrap();
    wait_until(
        "the leader's generation open",
        Duration::from_secs(60),
        async || {
            let rec = delete_fence::current(meta.as_ref()).await.unwrap();
            let mut all = ctl.is_leader() && rec.open && rec.leader_token == ctl.token();
            for n in &c.nodes {
                let h = c.clients.health(&n.addr.to_string()).await.unwrap();
                all &= h.delete_fence_open && h.delete_fence_generation == rec.generation;
            }
            all
        },
    )
    .await;
    let backup_url = backup.backup();
    let version = backup.min_restorable_version(&backup_url);
    // After the backup point: deleted; the leader's decided deletes.
    let m = c.manifest(BUCKET, "kept").await;
    let g = c.groups().await.remove(0);
    let mut deletes = Vec::new();
    for (_, ch) in &m.chunks {
        for (i, mem) in g.members.iter().enumerate() {
            let n = c.node_index(mem).unwrap();
            deletes.push((
                c.nodes[n].addr.to_string(),
                arca_lake_protocol::client::ShardRef {
                    group_id: g.id,
                    chunk_id: ch.chunk_id,
                    shard_index: u16::try_from(i).unwrap(),
                },
                g.epoch,
            ));
        }
    }
    let old = arca_lake_protocol::client::NodeClients::new(Duration::from_secs(5))
        .with_tls(c.clients.tls().cloned());
    old.set_incarnation(Some(
        compat::cluster_state(meta.as_ref())
            .await
            .unwrap()
            .unwrap()
            .incarnation,
    ));
    old.set_delete_fence(Some(
        delete_fence::current(meta.as_ref())
            .await
            .unwrap()
            .generation,
    ));
    s3::delete(&c.backend, BUCKET, "kept").await.unwrap();
    ctl.shutdown().await;
    let shards_at_backup = shards_total(&c).await;
    let fdb = Fdb {
        backup,
        bad_once: std::sync::Mutex::new(Some(1)),
    };
    let dir = tempfile::tempdir().unwrap();
    let journal = dir.path().join("restore.journal");
    let plan = mr::plan(
        meta.as_ref(),
        &c.clients,
        &backup_url,
        version,
        Some(now_ms()),
        &fdb.destination(),
        &journal,
        now_ms(),
    )
    .await
    .unwrap();
    for l in &plan.summary {
        eprintln!("plan: {l}");
    }
    let e = mr::run(
        &meta,
        &c.clients,
        &fdb,
        &journal,
        &plan.journal.token,
        &[],
        now_ms,
    )
    .await
    .unwrap_err();
    eprintln!("first run: {e}");
    assert!(e.to_string().contains("restore failed"), "{e}");
    assert_eq!(
        mr::Journal::load(&journal).unwrap().last(),
        Some(Step::Cleared)
    );
    let mut applied = 0;
    for (addr, shard, epoch) in &deletes {
        applied += usize::from(old.delete_shard(addr, *shard, *epoch).await.is_ok());
    }
    assert_eq!(applied, 0, "while the restore failed");
    let j = mr::run(
        &meta,
        &c.clients,
        &fdb,
        &journal,
        &plan.journal.token,
        &[],
        now_ms,
    )
    .await
    .unwrap();
    assert_eq!(j.last(), Some(Step::Verified));
    let mut cfg = cfg;
    cfg.id = "ctl-after".into();
    let ctl = spawn(Arc::clone(&meta), cfg).await.unwrap();
    let j = mr::finish(
        meta.as_ref(),
        &c.clients,
        &journal,
        "op",
        "drill",
        true,
        Duration::from_secs(60),
        now_ms,
    )
    .await
    .unwrap();
    assert_eq!(j.last(), Some(Step::Reopened));
    for (addr, shard, epoch) in &deletes {
        applied += usize::from(old.delete_shard(addr, *shard, *epoch).await.is_ok());
    }
    assert_eq!(applied, 0, "after the restore");
    assert_eq!(shards_total(&c).await, shards_at_backup);
    c.restart_backend().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "kept").await.unwrap().0, kept);
    ctl.shutdown().await;
    c.shutdown().await;
}
