//! FoundationDB failover on a multi-process cluster (RFC-011 §6).
//!
//! Requires `--features fdb`, `ARCA_FDB_TESTS=1`, `ARCA_FDB_FAILOVER=1`, a
//! cluster file, and a chaos driver that can kill and restart the
//! FoundationDB processes `arca-fdb-1..3`:
//!
//! - `ARCA_CHAOS_DOCKER=1`: the test runs `docker kill|start arca-fdb-N`
//!   itself (CI, test on the host).
//! - `ARCA_CHAOS_DIR=<dir>`: the test writes `req-N` files and waits for
//!   `ack-N` (local, test inside a container without a docker socket;
//!   `deploy/dev/fdb-failover.sh` answers them).
//!
//! The cluster is configured `double` with three coordinators, so it
//! tolerates one lost process; losing two coordinators loses the quorum and
//! every operation must fail explicitly within the transaction timeout.
//! This describes the test configuration, not a production availability
//! guarantee.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arca_lake_controller::ControlError;
use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_controller::repair::RepairStep;
use arca_lake_metadata::fdb::FdbKv;
use arca_lake_metadata::records::GroupState;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "failover";

struct Chaos {
    dir: Option<std::path::PathBuf>,
    docker: bool,
    seq: AtomicUsize,
}

impl Chaos {
    fn from_env() -> Option<Self> {
        let docker = std::env::var_os("ARCA_CHAOS_DOCKER").is_some();
        let dir = std::env::var_os("ARCA_CHAOS_DIR").map(std::path::PathBuf::from);
        (docker || dir.is_some()).then_some(Self {
            dir,
            docker,
            seq: AtomicUsize::new(0),
        })
    }

    /// `kill N` or `start N`.
    async fn run(&self, action: &str) {
        eprintln!("chaos: {action}");
        if self.docker {
            let mut parts = action.split_whitespace();
            let verb = parts.next().unwrap();
            let n = parts.next().unwrap();
            let status = tokio::process::Command::new("docker")
                .args([verb, &format!("arca-fdb-{n}")])
                .status()
                .await
                .expect("docker");
            assert!(status.success(), "docker {verb} arca-fdb-{n} failed");
            return;
        }
        let dir = self.dir.as_ref().unwrap();
        let n = self.seq.fetch_add(1, Ordering::SeqCst);
        let req = dir.join(format!("req-{n}"));
        let ack = dir.join(format!("ack-{n}"));
        std::fs::write(&req, action).unwrap();
        let start = Instant::now();
        while !ack.exists() {
            assert!(
                start.elapsed() < Duration::from_secs(180),
                "chaos driver did not answer {action}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait until the store answers again after processes were restarted.
async fn wait_for_store(c: &TestCluster<FdbKv>) {
    wait_until("store available", Duration::from_secs(120), async || {
        arca_lake_metadata::ops::cluster::map_version(c.meta.as_ref())
            .await
            .is_ok()
    })
    .await;
}

#[tokio::test]
async fn foundationdb_process_and_coordinator_failures() {
    if std::env::var_os("ARCA_FDB_TESTS").is_none()
        || std::env::var_os("ARCA_FDB_FAILOVER").is_none()
    {
        eprintln!("ARCA_FDB_FAILOVER not set; skipping");
        return;
    }
    // Nothing in this test may wait forever: a hung step is a failure.
    tokio::time::timeout(Duration::from_mins(15), scenario())
        .await
        .expect("the failover scenario exceeded 15 minutes");
}

async fn scenario() {
    let Some(chaos) = Chaos::from_env() else {
        eprintln!("no chaos driver configured; skipping");
        return;
    };
    let path = std::env::var("ARCA_FDB_CLUSTER_FILE").ok();
    // A short timeout so that a lost quorum fails fast (test configuration).
    let kv = FdbKv::open_with_timeout(path.as_deref(), 5_000).expect("open FoundationDB");
    kv.clear_range_blocking(&arca_lake_metadata::keys::all_range())
        .await
        .unwrap();
    let mut c = TestCluster::start_with(
        Arc::new(kv),
        ClusterOptions {
            groups: 1,
            ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
        },
    )
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();

    // 1. Baseline.
    let mut committed: Vec<(String, Bytes)> = Vec::new();
    let base = payload(300 << 10, 1);
    s3::put(
        &c.backend,
        BUCKET,
        "base",
        base.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    committed.push(("base".into(), base));

    // 2. One process dies during concurrent PUTs: the cluster (double
    //    redundancy) stays available; every PUT that reported success is
    //    readable afterwards, every failure is explicit.
    let writers: Vec<_> = (0..6u64)
        .map(|i| {
            let backend = c.backend.clone();
            tokio::spawn(async move {
                let data = payload(200 << 10, 10 + i);
                let key = format!("during-kill-{i}");
                let r = s3::put(&backend, BUCKET, &key, data.clone(), &PutOptions::default()).await;
                (key, data, r.map(|_| ()))
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(50)).await;
    chaos.run("kill 2").await;
    let mut failed = 0;
    for w in writers {
        let (key, data, r) = w.await.unwrap();
        match r {
            Ok(()) => committed.push((key, data)),
            Err(e) => {
                failed += 1;
                eprintln!("put {key} failed explicitly during process loss: {e}");
            }
        }
    }
    eprintln!(
        "process loss: {} committed, {failed} failed",
        committed.len() - 1
    );
    wait_for_store(&c).await;
    for (key, data) in &committed {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    // A PUT with one process down succeeds (redundancy double).
    let d2 = payload(200 << 10, 20);
    s3::put(
        &c.backend,
        BUCKET,
        "one-down",
        d2.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    committed.push(("one-down".into(), d2));
    chaos.run("start 2").await;
    wait_for_store(&c).await;

    // 3. The original coordinator dies: two of three coordinators remain.
    chaos.run("kill 1").await;
    wait_for_store(&c).await;
    let d3 = payload(200 << 10, 30);
    s3::put(
        &c.backend,
        BUCKET,
        "coordinator-down",
        d3.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    committed.push(("coordinator-down".into(), d3));

    // 4. A second coordinator dies: quorum lost. Operations fail explicitly
    //    within the transaction timeout; nothing hangs, nothing is lost.
    chaos.run("kill 3").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let started = Instant::now();
    let err = s3::put(
        &c.backend,
        BUCKET,
        "no-quorum",
        payload(64 << 10, 40),
        &PutOptions::default(),
    )
    .await
    .expect_err("PUT without quorum must fail");
    eprintln!("no quorum: PUT failed after {:?}: {err}", started.elapsed());
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "failed fast, not hung"
    );
    let started = Instant::now();
    assert!(
        s3::get(&c.backend, BUCKET, "base").await.is_err(),
        "GET without quorum fails"
    );
    assert!(started.elapsed() < Duration::from_secs(60));
    chaos.run("start 1").await;
    chaos.run("start 3").await;
    wait_for_store(&c).await;
    assert!(
        s3::head(&c.backend, BUCKET, "no-quorum").await.is_err(),
        "the failed PUT did not become visible after recovery"
    );
    for (key, data) in &committed {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    let report = c.gc(now_ms() + 3_600_000, 0).await;
    assert_eq!(report.skipped_referenced, 0);

    // 5. Controller leader failover on the real store.
    let mut cfg = ControllerConfig::new("f1");
    cfg.lease = Duration::from_millis(1_500);
    cfg.gc_interval = Duration::from_millis(500);
    cfg.repair_interval = Duration::from_millis(500);
    cfg.health_interval = Duration::from_millis(500);
    cfg.gc_grace = Duration::ZERO;
    let mut cfg2 = cfg.clone();
    cfg2.id = "f2".into();
    let ctl_a = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let ctl_b = spawn(Arc::clone(&c.meta), cfg2).await.unwrap();
    wait_until("leader", Duration::from_secs(20), async || {
        ctl_a.is_leader() || ctl_b.is_leader()
    })
    .await;
    let (leader, follower) = if ctl_a.is_leader() {
        (ctl_a, ctl_b)
    } else {
        (ctl_b, ctl_a)
    };
    leader.abort();
    wait_until("failover", Duration::from_secs(20), async || {
        follower.is_leader()
    })
    .await;
    assert_eq!(follower.token(), 2);
    follower.shutdown().await;

    // 6. Repair while a FoundationDB process dies and comes back: the repair
    //    either completes or stops with an error, and a re-run completes.
    let group = c.groups().await.remove(0);
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;
    let outcome = c
        .repair_with(group.id, 0, new_node, async |step| {
            if let RepairStep::Chunk { index: 0, .. } = step {
                chaos.run("kill 2").await;
            }
            Ok(())
        })
        .await;
    match &outcome {
        Ok(r) => eprintln!("repair during process loss completed: {r:?}"),
        Err(e) => eprintln!("repair during process loss stopped: {e}"),
    }
    chaos.run("start 2").await;
    wait_for_store(&c).await;
    let done = c.repair(group.id, 0, new_node).await;
    assert!(done.complete, "{done:?}");
    assert_eq!(c.groups().await[0].state, GroupState::Active);

    // 7. GC while a process dies: the pass defers or errors, the next pass
    //    collects; totals stay consistent.
    for (key, _) in committed.iter().take(3) {
        s3::delete(&c.backend, BUCKET, key).await.unwrap();
    }
    chaos.run("kill 3").await;
    let gc_result = arca_lake_controller::gc::run_once(
        c.meta.as_ref(),
        &c.clients,
        now_ms() + 3_600_000,
        0,
        100,
    )
    .await;
    match &gc_result {
        Ok(r) => eprintln!("gc during process loss: {r:?}"),
        Err(e) => {
            assert!(matches!(e, ControlError::Meta(_)), "{e}");
            eprintln!("gc during process loss stopped: {e}");
        }
    }
    chaos.run("start 3").await;
    wait_for_store(&c).await;
    let report = c.gc(now_ms() + 3_600_000, 0).await;
    assert_eq!(report.skipped_referenced, 0);
    for (key, data) in committed.iter().skip(3) {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    for (key, _) in committed.iter().take(3) {
        assert!(s3::head(&c.backend, BUCKET, key).await.is_err());
    }
    let other = c.node_index(&group.members[1]).unwrap();
    c.stop_node(other).await;
    for (key, data) in committed.iter().skip(3) {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    c.shutdown().await;
}
