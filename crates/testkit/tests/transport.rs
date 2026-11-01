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
    // The second PutShard to the replacement hits a reset.
    let puts = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&puts);
    c.clients.set_fault_hook(Some(hook(move |ctx| {
        if ctx.rpc == Rpc::PutShard
            && ctx.addr == target
            && counter.fetch_add(1, Ordering::SeqCst) == 1
        {
            return FaultAction::Reset;
        }
        FaultAction::None
    })));
    let err = c
        .repair_with(group.id, 0, new_node, async |_| Ok(()))
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::Node { .. }), "{err}");
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
