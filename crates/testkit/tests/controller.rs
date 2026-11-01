//! The controller process (RFC-011): leader election with lease and fencing
//! token, failover, fenced old leaders, and schedulers that resume work
//! (repair, GC) after a leader change.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use arca_lake_controller::ControlError;
use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_controller::repair::RepairStep;
use arca_lake_metadata::fence::FencedKv;
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::{cluster, leader};
use arca_lake_metadata::records::GroupState;
use arca_lake_metadata::{KvError, KvStore, Txn};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "ctl";

fn fast(id: &str) -> ControllerConfig {
    let mut c = ControllerConfig::new(id);
    c.lease = Duration::from_millis(600);
    c.gc_interval = Duration::from_millis(200);
    c.reconcile_interval = Duration::from_millis(400);
    c.repair_interval = Duration::from_millis(200);
    c.health_interval = Duration::from_millis(200);
    c.unhealthy_after = Duration::from_millis(1_500);
    c.gc_grace = Duration::ZERO;
    c.min_orphan_age = Duration::ZERO;
    c.http_listen = Some("127.0.0.1:0".parse().unwrap());
    c
}

async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn exactly_one_leader_and_failover_after_the_lease() {
    let meta = Arc::new(MemKv::default());
    let a = spawn(Arc::clone(&meta), fast("a")).await.unwrap();
    let b = spawn(Arc::clone(&meta), fast("b")).await.unwrap();
    wait_until("a leader", Duration::from_secs(5), async || {
        a.is_leader() || b.is_leader()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(a.is_leader() ^ b.is_leader(), "exactly one leader");
    let (first, second) = if a.is_leader() { (a, b) } else { (b, a) };
    let token1 = first.token();
    assert_eq!(token1, 1);

    // The HTTP endpoint reports the role.
    let body = http_get(first.http_addr.unwrap(), "/healthz").await;
    assert!(body.contains("\"leader\":true"), "{body}");
    let metrics = http_get(first.http_addr.unwrap(), "/metrics").await;
    assert!(metrics.contains("arca_controller_leader 1"), "{metrics}");

    // Crash the leader (no release): the follower takes over after the
    // lease with a new token.
    first.abort();
    wait_until("failover", Duration::from_secs(5), async || {
        second.is_leader()
    })
    .await;
    assert_eq!(second.token(), 2);
    let rec = leader::current(meta.as_ref()).await.unwrap().unwrap();
    assert_eq!((rec.holder.as_str(), rec.token), ("b", 2));

    // Graceful shutdown releases the lease so a successor does not wait.
    second.shutdown().await;
    let rec = leader::current(meta.as_ref()).await.unwrap().unwrap();
    assert_eq!(rec.lease_until_ms, 0);
    let c = spawn(Arc::clone(&meta), fast("c")).await.unwrap();
    wait_until("c leader", Duration::from_secs(2), async || c.is_leader()).await;
    assert_eq!(c.token(), 3);
    c.shutdown().await;
}

#[tokio::test]
async fn a_fenced_old_leader_cannot_commit_after_takeover() {
    let meta = Arc::new(MemKv::default());
    let old = leader::try_acquire(meta.as_ref(), "old", now_ms(), 100)
        .await
        .unwrap()
        .unwrap();
    let fenced = FencedKv::new(Arc::clone(&meta), old.clone());
    let mut tx = fenced.begin().await.unwrap();
    tx.set(b"k", b"before");
    tx.commit().await.unwrap();

    // Lease expires; a new leader takes over.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let new = leader::try_acquire(meta.as_ref(), "new", now_ms(), 10_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(new.token, old.token + 1);

    // The old leader's long-stopped work resumes: every transaction fails.
    let mut tx = fenced.begin().await.unwrap();
    tx.set(b"k", b"after");
    assert!(matches!(tx.commit().await, Err(KvError::Fenced(_))));
    let err = cluster::register_node(&fenced, "127.0.0.1:1", vec![], vec![0], 1)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        arca_lake_metadata::MetaError::Kv(KvError::Fenced(_))
    ));
    let mut tx = meta.begin().await.unwrap();
    assert_eq!(tx.get(b"k").await.unwrap(), Some(b"before".to_vec()));
    assert!(
        !leader::renew(meta.as_ref(), &old, now_ms(), 100)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn gc_scheduler_collects_and_old_leader_gc_is_fenced() {
    let c = cluster().await;
    let data = payload(300 << 10, 1);
    s3::put(
        &c.backend,
        BUCKET,
        "k",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    s3::put(
        &c.backend,
        BUCKET,
        "k",
        payload(300 << 10, 2),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let before = c.total_shards().await;
    assert_eq!(before, 6);

    let ctl = spawn(Arc::clone(&c.meta), fast("gc")).await.unwrap();
    wait_until("collected", Duration::from_secs(10), async || {
        c.total_shards().await == 3
    })
    .await;
    assert!(s3::get(&c.backend, BUCKET, "k").await.is_ok());

    // An old leader whose lease lapsed cannot run GC any more.
    let stale = leader::Leadership {
        holder: "gc".into(),
        token: ctl.token(),
    };
    ctl.abort();
    let successor = spawn(Arc::clone(&c.meta), fast("gc2")).await.unwrap();
    wait_until("successor", Duration::from_secs(5), async || {
        successor.is_leader()
    })
    .await;
    let fenced = FencedKv::new(Arc::clone(&c.meta), stale);
    let err = arca_lake_controller::gc::run_once(&fenced, &c.clients, now_ms(), 0, 100)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ControlError::Meta(arca_lake_metadata::MetaError::Kv(KvError::Fenced(_)))
    ));
    successor.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn repair_interrupted_by_a_leader_crash_is_resumed_by_the_successor() {
    let mut c = cluster().await;
    let mut objects = Vec::new();
    for i in 0..3u64 {
        let data = payload(usize::try_from(MIB + MIB / 2).unwrap(), 10 + i);
        s3::put(
            &c.backend,
            BUCKET,
            &format!("o{i}"),
            data.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        objects.push((format!("o{i}"), data));
    }
    let group = c.groups().await.remove(0);
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;

    // A controller ("manual" path) starts the repair and dies after the
    // first chunk.
    let err = c
        .repair_with(group.id, 0, new_node, async |step| match step {
            RepairStep::Chunk { index: 0, .. } => {
                Err(ControlError::Other("controller crashed".into()))
            }
            _ => Ok(()),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::Other(_)));
    assert_eq!(c.groups().await[0].state, GroupState::Repairing);

    // A fresh controller process finds the Repairing group with its cursor
    // and finishes it without any operator input.
    let ctl = spawn(Arc::clone(&c.meta), fast("r")).await.unwrap();
    wait_until("repair resumed", Duration::from_secs(15), async || {
        c.groups().await[0].state == GroupState::Active
    })
    .await;
    assert_eq!(c.shard_count(new_node, group.id), 6);
    let metrics = http_get(ctl.http_addr.unwrap(), "/metrics").await;
    assert!(
        metrics.contains("arca_controller_repairs_completed_total 1"),
        "{metrics}"
    );
    ctl.shutdown().await;
    let other = c.node_index(&group.members[1]).unwrap();
    c.stop_node(other).await;
    for (key, data) in &objects {
        assert_eq!(&s3::get(&c.backend, BUCKET, key).await.unwrap().0, data);
    }
    c.shutdown().await;
}

#[tokio::test]
async fn health_monitor_triggers_auto_repair_onto_a_spare_node() {
    let mut c = cluster().await;
    let data = payload(usize::try_from(MIB + 1).unwrap(), 20);
    s3::put(
        &c.backend,
        BUCKET,
        "k",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let spare = c.add_node().await;
    let group = c.groups().await.remove(0);
    let mut cfg = fast("auto");
    cfg.auto_repair = true;
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    wait_until("leader", Duration::from_secs(5), async || ctl.is_leader()).await;
    // Let the health pass record every node as seen, then kill one member.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    wait_until("auto repair", Duration::from_secs(20), async || {
        let g = c.groups().await.remove(0);
        g.state == GroupState::Active && c.node_index(&g.members[0]) == Some(spare)
    })
    .await;
    assert_eq!(c.shard_count(spare, group.id), 2);
    ctl.shutdown().await;
    let other = c.node_index(&group.members[1]).unwrap();
    c.stop_node(other).await;
    assert_eq!(s3::get(&c.backend, BUCKET, "k").await.unwrap().0, data);
    c.shutdown().await;
}

async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    String::from_utf8_lossy(&out).into_owned()
}
