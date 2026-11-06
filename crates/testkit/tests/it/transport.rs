//! Transport fault injection at the node-client boundary (see
//! `arca_lake_protocol::client`). These reproduce the observable effects
//! of timeouts, lost replies, resets, partitions, slow nodes, delayed
//! acknowledgments, duplicated requests, and reordered completion on the
//! write, read, and repair protocols. Behaviour on a real network is not
//! covered here.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arca_lake_controller::ControlError;
use arca_lake_metadata::records::GroupState;
use arca_lake_protocol::client::{FaultAction, FaultHook, Rpc, RpcContext};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::S3ErrorCode;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "transport";

async fn cluster(groups: usize) -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups,
        ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

fn hook(f: impl Fn(&RpcContext<'_>) -> FaultAction + Send + Sync + 'static) -> FaultHook {
    Arc::new(f)
}

#[tokio::test]
async fn lost_reply_on_put_shard_fails_the_group_and_leaves_reconcilable_orphans() {
    let c = cluster(2).await;
    // Whichever group the gateway tries first: every PutShard to it is
    // applied by the node but the reply is lost.
    let doomed: Arc<std::sync::Mutex<Option<u64>>> = Arc::default();
    let seen = Arc::clone(&doomed);
    c.backend
        .node_clients()
        .set_fault_hook(Some(hook(move |ctx| {
            if ctx.rpc != Rpc::PutShard {
                return FaultAction::None;
            }
            let mut d = seen.lock().unwrap();
            if d.is_none() {
                *d = ctx.group_id;
            }
            if *d == ctx.group_id {
                FaultAction::FailAfter(tonic::Code::Unavailable, "reply lost".into())
            } else {
                FaultAction::None
            }
        })));
    let data = payload(300 << 10, 1);
    s3::put(
        &c.backend,
        BUCKET,
        "k",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .expect("the write is diverted to the other group");
    let bad = doomed.lock().unwrap().expect("a group was tried");
    let m = c.manifest(BUCKET, "k").await;
    assert!(m.chunks.iter().all(|(_, ch)| ch.placement.group_id != bad));
    assert_eq!(s3::get(&c.backend, BUCKET, "k").await.unwrap().0, data);

    // The acknowledged-but-unreported shards in the first group are orphans
    // attributed to an existing generation whose chunk record points
    // elsewhere.
    let in_bad: usize = (0..3).map(|i| c.shard_count(i, bad)).sum();
    assert!(in_bad >= 3, "the node applied the writes: {in_bad}");
    c.backend.node_clients().set_fault_hook(None);
    assert_eq!(
        c.reconcile(now_ms(), 0).await,
        in_bad,
        "reconciled by reference, not by age"
    );
    assert_eq!((0..3).map(|i| c.shard_count(i, bad)).sum::<usize>(), 0);
    assert_eq!(s3::get(&c.backend, BUCKET, "k").await.unwrap().0, data);
    c.shutdown().await;
}

#[tokio::test]
async fn partition_from_one_node_degrades_reads_and_fails_writes_until_healed() {
    let c = cluster(1).await;
    let data = payload(usize::try_from(MIB + 3).unwrap(), 2);
    s3::put(
        &c.backend,
        BUCKET,
        "k",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let cut = c.nodes[0].addr.to_string();
    c.backend
        .node_clients()
        .set_fault_hook(Some(hook(move |ctx| {
            if ctx.addr == cut {
                FaultAction::FailBefore(tonic::Code::Unavailable, "partitioned".into())
            } else {
                FaultAction::None
            }
        })));
    assert_eq!(
        s3::get(&c.backend, BUCKET, "k").await.unwrap().0,
        data,
        "degraded read"
    );
    let err = s3::put(
        &c.backend,
        BUCKET,
        "k2",
        payload(100 << 10, 3),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        *err.code(),
        S3ErrorCode::ServiceUnavailable,
        "m_ack = m: no partial chunk"
    );
    assert!(s3::head(&c.backend, BUCKET, "k2").await.is_err());
    c.backend.node_clients().set_fault_hook(None);
    let d2 = payload(100 << 10, 4);
    s3::put(&c.backend, BUCKET, "k2", d2.clone(), &PutOptions::default())
        .await
        .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "k2").await.unwrap().0, d2);
    c.shutdown().await;
}

#[tokio::test]
async fn request_timeout_and_slow_node_on_reads_fall_back_to_parity() {
    let c = cluster(1).await;
    let data = payload(usize::try_from(MIB + 5).unwrap(), 5);
    s3::put(
        &c.backend,
        BUCKET,
        "k",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let slow = c.nodes[0].addr.to_string();
    let timeout = c.nodes[1].addr.to_string();
    c.backend
        .node_clients()
        .set_fault_hook(Some(hook(move |ctx| match ctx.rpc {
            Rpc::GetShard if ctx.addr == timeout => {
                FaultAction::FailBefore(tonic::Code::DeadlineExceeded, "request timeout".into())
            }
            Rpc::GetShard if ctx.addr == slow => {
                FaultAction::DelayBefore(Duration::from_millis(200))
            }
            _ => FaultAction::None,
        })));
    let t = std::time::Instant::now();
    assert_eq!(s3::get(&c.backend, BUCKET, "k").await.unwrap().0, data);
    assert!(
        t.elapsed() >= Duration::from_millis(200),
        "the slow node was waited for"
    );
    c.shutdown().await;
}

#[tokio::test]
async fn delayed_ack_duplicate_and_reordered_put_shards_commit_once() {
    let c = cluster(1).await;
    let n0 = c.nodes[0].addr.to_string();
    let n1 = c.nodes[1].addr.to_string();
    c.backend
        .node_clients()
        .set_fault_hook(Some(hook(move |ctx| {
            if ctx.rpc != Rpc::PutShard {
                return FaultAction::None;
            }
            if ctx.addr == n0 {
                FaultAction::DelayAfter(Duration::from_millis(150))
            } else if ctx.addr == n1 {
                FaultAction::Duplicate
            } else {
                // Shard 2 completes first, then 1, then 0.
                match ctx.shard.map(|s| s.shard_index) {
                    Some(0) => FaultAction::DelayBefore(Duration::from_millis(100)),
                    Some(1) => FaultAction::DelayBefore(Duration::from_millis(50)),
                    _ => FaultAction::None,
                }
            }
        })));
    let data = payload(usize::try_from(2 * MIB + 9).unwrap(), 6);
    s3::put(
        &c.backend,
        BUCKET,
        "k",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let group = c.manifest(BUCKET, "k").await.chunks[0].1.placement.group_id;
    for i in 0..3 {
        assert_eq!(
            c.shard_count(i, group),
            3,
            "one key per shard on node {i}, duplicates included"
        );
    }
    assert_eq!(s3::get(&c.backend, BUCKET, "k").await.unwrap().0, data);
    c.backend.node_clients().set_fault_hook(None);
    assert_eq!(
        c.gc(now_ms() + 120_000, 0).await.collected,
        0,
        "a single generation was committed"
    );
    c.shutdown().await;
}

#[tokio::test]
async fn connection_reset_during_repair_is_resumed_after_healing() {
    let mut c = cluster(1).await;
    let mut objects = Vec::new();
    for i in 0..2u64 {
        let data = payload(usize::try_from(MIB + MIB / 2).unwrap(), 20 + i);
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
    let group = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.scheme == "rs-gf8-v1")
        .unwrap();
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;
    let target = c.nodes[new_node].addr.to_string();
    // From the second PutShard to the replacement on, every attempt hits a
    // reset (a link that stays broken): the policy's retries are exhausted
    // and the repair stops.
    let puts = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&puts);
    c.clients.set_fault_hook(Some(hook(move |ctx| {
        if ctx.rpc == Rpc::PutShard
            && ctx.addr == target
            && counter.fetch_add(1, Ordering::SeqCst) >= 1
        {
            return FaultAction::Reset;
        }
        FaultAction::None
    })));
    let before = c.clients.stats();
    let err = c
        .repair_with(group.id, 0, new_node, async |_| Ok(()))
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::Node { .. }), "{err}");
    let stats = c.clients.stats();
    assert!(stats.retries > before.retries, "{stats:?}");
    assert_eq!(stats.exhausted - before.exhausted, 1, "{stats:?}");
    let mid = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == group.id)
        .unwrap();
    assert_eq!(mid.state, GroupState::Repairing);
    assert_eq!(c.shard_count(new_node, group.id), 1);

    c.clients.set_fault_hook(None);
    let done = c.repair(group.id, 0, new_node).await;
    assert!(done.resumed && done.complete, "{done:?}");
    assert_eq!(c.shard_count(new_node, group.id), 4);
    let other = c.node_index(&group.members[1]).unwrap();
    c.stop_node(other).await;
    for (key, data) in &objects {
        assert_eq!(&s3::get(&c.backend, BUCKET, key).await.unwrap().0, data);
    }
    c.shutdown().await;
}

/// A reset on the first attempt of a shard write is retried by the client
/// and the PUT succeeds; the duplicate write is idempotent on the node.
#[tokio::test]
async fn transient_reset_on_put_shard_is_retried_and_commits_once() {
    let c = cluster(1).await;
    let data = payload(usize::try_from(MIB + MIB / 2).unwrap(), 30);
    let resets = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&resets);
    c.backend
        .node_clients()
        .set_fault_hook(Some(hook(move |ctx| {
            if ctx.rpc == Rpc::PutShard
                && ctx.shard.is_some_and(|s| s.shard_index == 1)
                && counter.fetch_add(1, Ordering::SeqCst) == 0
            {
                return FaultAction::Reset;
            }
            FaultAction::None
        })));
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let stats = c.backend.node_clients().stats();
    assert_eq!(stats.retries, 1, "{stats:?}");
    assert_eq!(stats.exhausted, 0, "{stats:?}");
    c.backend.node_clients().set_fault_hook(None);
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    // Two chunks, three shards each, exactly once.
    assert_eq!(c.total_shards().await, 6);
    c.shutdown().await;
}

/// A shard stream that stops after its header is detected by the progress
/// watchdog (not by an absolute timeout), the attempt is retried, and the
/// PUT succeeds; a slow but progressing stream is never cut.
#[tokio::test]
async fn stalled_shard_stream_is_detected_and_retried_slow_stream_is_not() {
    let c = cluster(1).await;
    let data = payload(usize::try_from(MIB / 2).unwrap(), 31);
    let stalls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&stalls);
    c.backend
        .node_clients()
        .set_fault_hook(Some(hook(move |ctx| {
            if ctx.rpc != Rpc::PutShard {
                return FaultAction::None;
            }
            match ctx.shard.map(|s| s.shard_index) {
                // Shard 0: stalls once, then flows.
                Some(0) if counter.fetch_add(1, Ordering::SeqCst) == 0 => FaultAction::StallStream,
                // Shard 2: slow (one frame per 300 ms) but progressing.
                Some(2) => FaultAction::SlowStream(Duration::from_millis(300)),
                _ => FaultAction::None,
            }
        })));
    let started = std::time::Instant::now();
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let elapsed = started.elapsed();
    let stats = c.backend.node_clients().stats();
    assert_eq!(stats.stalled, 1, "{stats:?}");
    assert_eq!(stats.deadline_exceeded, 0, "{stats:?}");
    assert_eq!(stats.retries, 1, "{stats:?}");
    let policy = c.backend.node_clients().policy().clone();
    assert!(
        elapsed >= policy.progress_timeout,
        "the stall was detected by the progress timeout: {elapsed:?}"
    );
    assert!(
        elapsed < policy.base_deadline + Duration::from_secs(2),
        "not by the absolute deadline: {elapsed:?}"
    );
    c.backend.node_clients().set_fault_hook(None);
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    assert_eq!(c.total_shards().await, 3);
    c.shutdown().await;
}

/// A stream that progresses but too slowly hits the absolute deadline even
/// though no single gap exceeds the progress timeout.
#[tokio::test]
async fn too_slow_shard_stream_hits_the_absolute_deadline() {
    use arca_lake_protocol::client::{NodeClients, RpcPolicy, ShardRef};
    let c = cluster(1).await;
    let clients = NodeClients::with_policy(RpcPolicy {
        connect_timeout: Duration::from_secs(1),
        unary_timeout: Duration::from_secs(5),
        progress_timeout: Duration::from_secs(5),
        base_deadline: Duration::from_millis(600),
        min_rate_bytes_per_s: 0,
        max_attempts: 1,
        backoff: Duration::from_millis(10),
    });
    clients.set_fault_hook(Some(hook(|ctx| {
        if ctx.rpc == Rpc::PutShard {
            FaultAction::SlowStream(Duration::from_millis(250))
        } else {
            FaultAction::None
        }
    })));
    let group = c
        .groups()
        .await
        .into_iter()
        .find(|g| !g.is_cache())
        .unwrap();
    let addr = c.nodes[c.node_index(&group.members[0]).unwrap()]
        .addr
        .to_string();
    let shard = ShardRef {
        group_id: group.id,
        chunk_id: [7; 16],
        shard_index: 0,
    };
    let err = clients
        .put_shard(&addr, shard, group.epoch, payload(256 << 10, 32), 64 << 10)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded, "{err}");
    assert!(err.message().contains("absolute deadline"), "{err}");
    let stats = clients.stats();
    assert_eq!(
        (stats.deadline_exceeded, stats.stalled),
        (1, 0),
        "{stats:?}"
    );
    // Nothing was stored: the node only commits a complete stream.
    assert_eq!(
        c.shard_count(c.node_index(&group.members[0]).unwrap(), group.id),
        0
    );
    c.shutdown().await;
}

/// An upload slower than the pending-generation lease keeps its generation
/// because every chunk append renews the lease; the object commits and is
/// readable, and nothing is collected by GC meanwhile.
#[tokio::test]
async fn slow_upload_renews_its_lease_and_commits() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        lease: Duration::from_millis(1000),
        ..ClusterOptions::rs(3, 2, 1, 256 << 10, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    // Every shard write crawls: 600 ms per frame on one shard, two frames
    // per 128 KiB shard, so a single chunk's upload (1.2 s) already
    // outlasts the lease and the 6-chunk object takes several seconds.
    c.backend.node_clients().set_fault_hook(Some(hook(|ctx| {
        if ctx.rpc == Rpc::PutShard && ctx.shard.is_some_and(|s| s.shard_index == 0) {
            FaultAction::SlowStream(Duration::from_millis(600))
        } else {
            FaultAction::None
        }
    })));
    let data = payload(usize::try_from(6 * (256u64 << 10) - 5).unwrap(), 40);
    let started = std::time::Instant::now();
    s3::put(
        &c.backend,
        BUCKET,
        "slow",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    // With pipelined placements the upload still takes well over one
    // lease (1 s): what matters is that it outlasts the lease at all.
    assert!(
        started.elapsed() > Duration::from_millis(1500),
        "the upload must outlast the lease for this test to mean anything"
    );
    c.backend.node_clients().set_fault_hook(None);
    assert_eq!(s3::get(&c.backend, BUCKET, "slow").await.unwrap().0, data);
    // GC with the lease long expired by wall clock finds nothing pending.
    let r = c.gc(now_ms() + 60_000, 0).await;
    assert_eq!(r.collected, 0, "{r:?}");
    assert_eq!(s3::get(&c.backend, BUCKET, "slow").await.unwrap().0, data);
    c.shutdown().await;
}
