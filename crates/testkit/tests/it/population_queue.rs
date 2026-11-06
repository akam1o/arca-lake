//! The population queue (Phase 2.3 §5): bounded, node-aware, durable
//! reads first, retried on node errors, drained at shutdown, and never on
//! the GET's critical path.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arca_lake_protocol::client::{FaultAction, FaultHook, Rpc, RpcContext};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use bytes::Bytes;

const BUCKET: &str = "popq";
const CHUNK: u64 = 256 << 10;

fn hook(f: impl Fn(&RpcContext<'_>) -> FaultAction + Send + Sync + 'static) -> FaultHook {
    Arc::new(f)
}

fn payload(chunks: u64, seed: u64) -> Bytes {
    Bytes::from(random_bytes(
        usize::try_from(chunks * CHUNK - 7).unwrap(),
        seed,
    ))
}

fn options(queue_depth: usize, per_node: usize, workers: u32) -> ClusterOptions {
    let mut cache = ClusterOptions::default_cache();
    cache.max_populations_in_flight = workers;
    cache.admit_after_misses = 1;
    ClusterOptions {
        groups: 1,
        cache_nodes: 2,
        cache: Some(cache),
        population_queue_depth: queue_depth,
        populations_per_node: per_node,
        ..ClusterOptions::rs(3, 2, 1, CHUNK, 0)
    }
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A queue of depth 1 with one worker drops most of a 16-chunk burst
/// (counted as skipped) while every GET succeeds with correct data.
#[tokio::test]
async fn full_queue_drops_populations_and_gets_still_succeed() {
    let c = TestCluster::start(options(1, 1, 1)).await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    // Hold every population for a while so that the queue fills.
    c.backend.node_clients().set_fault_hook(Some(hook(|ctx| {
        if ctx.rpc == Rpc::PutShard && ctx.group_id.is_some_and(|g| g > 1) {
            FaultAction::DelayBefore(Duration::from_millis(150))
        } else {
            FaultAction::None
        }
    })));
    let data = payload(16, 1);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let got = s3::get(&c.backend, BUCKET, "o").await.unwrap().0;
    assert_eq!(got, data);
    let q = c.backend.population_queue_stats();
    assert!(q.dropped_full > 0, "{q:?}");
    assert!(q.enqueued >= 1, "{q:?}");
    assert_eq!(
        c.backend.cache_stats().populations_skipped,
        q.dropped_full,
        "every drop is a skipped population"
    );
    // The chunks that did get queued populate; nothing is stuck.
    wait_until("queue drained", Duration::from_secs(10), async || {
        let q = c.backend.population_queue_stats();
        q.queued == 0 && q.running == 0
    })
    .await;
    c.backend.node_clients().set_fault_hook(None);
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.shutdown().await;
}

/// With one population per node allowed, no cache node ever has two
/// population streams from the gateway at once, even with eight workers.
#[tokio::test]
async fn per_node_limit_holds() {
    let c = TestCluster::start(options(64, 1, 8)).await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let cache_addrs: Vec<String> = c
        .cache_node_indexes()
        .into_iter()
        .map(|i| c.nodes[i].addr.to_string())
        .collect();
    // Each cache PutShard is held for 200 ms; the hook records the
    // interval it holds the stream for, per node address.
    let intervals: Arc<Mutex<Vec<(String, Instant, Instant)>>> = Arc::default();
    let rec = Arc::clone(&intervals);
    let addrs = cache_addrs.clone();
    c.backend
        .node_clients()
        .set_fault_hook(Some(hook(move |ctx| {
            if ctx.rpc == Rpc::PutShard && addrs.iter().any(|a| a == ctx.addr) {
                let now = Instant::now();
                rec.lock().unwrap().push((
                    ctx.addr.to_string(),
                    now,
                    now + Duration::from_millis(200),
                ));
                FaultAction::DelayBefore(Duration::from_millis(200))
            } else {
                FaultAction::None
            }
        })));
    let data = payload(12, 2);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.wait_cached(BUCKET, "o", 12, Duration::from_secs(30))
        .await;
    let iv = intervals.lock().unwrap().clone();
    assert!(iv.len() >= 12, "{} population streams recorded", iv.len());
    for addr in &cache_addrs {
        let mut mine: Vec<(Instant, Instant)> = iv
            .iter()
            .filter(|(a, _, _)| a == addr)
            .map(|(_, s, e)| (*s, *e))
            .collect();
        mine.sort();
        // A stream that starts before the previous one on the same node
        // would have ended is a second concurrent population (with a
        // 20 ms tolerance for scheduling).
        for w in mine.windows(2) {
            assert!(
                w[1].0 + Duration::from_millis(20) >= w[0].1,
                "two populations overlapped on one node"
            );
        }
    }
    let q = c.backend.population_queue_stats();
    assert_eq!(q.dropped_full, 0, "{q:?}");
    c.backend.node_clients().set_fault_hook(None);
    c.shutdown().await;
}

/// Populations yield to shard reads in flight: a durable read held open
/// by the fault hook while another GET's misses are queued makes the
/// population defer; every chunk is still populated and both GETs return
/// their bytes.
#[tokio::test]
async fn populations_defer_to_durable_reads() {
    // Deterministic (R-52): a GET of `p` is held inside the node client
    // (its reads count as in flight) until the test releases it; a GET of
    // `o` meanwhile completes and enqueues `o`'s population, which must
    // find the held reads and yield. No sleeps decide the outcome.
    let c = TestCluster::start(options(64, 2, 4)).await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let data_o = payload(8, 3);
    let data_p = payload(4, 5);
    for (k, d) in [("o", &data_o), ("p", &data_p)] {
        s3::put(&c.backend, BUCKET, k, d.clone(), &PutOptions::default())
            .await
            .unwrap();
    }
    let held: std::collections::HashSet<[u8; 16]> = c
        .manifest(BUCKET, "p")
        .await
        .chunks
        .iter()
        .map(|(_, ch)| ch.chunk_id)
        .collect();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let entered = Arc::new(AtomicU64::new(0));
    {
        let gate = Arc::clone(&gate);
        let entered = Arc::clone(&entered);
        c.backend
            .node_clients()
            .set_fault_hook(Some(hook(move |ctx| {
                if ctx.rpc == Rpc::GetShard && ctx.shard.is_some_and(|s| held.contains(&s.chunk_id))
                {
                    entered.fetch_add(1, Ordering::SeqCst);
                    FaultAction::Hold(Arc::clone(&gate))
                } else {
                    FaultAction::None
                }
            })));
    }
    let backend = c.backend.clone();
    let get_p = tokio::spawn(async move { s3::get(&backend, BUCKET, "p").await.unwrap().0 });
    // Event wait: the held read is registered (the counter rises before the
    // hook is consulted).
    while entered.load(Ordering::SeqCst) == 0
        || c.backend.node_clients().reads_in_flight_total() == 0
    {
        tokio::task::yield_now().await;
    }
    let before = c.backend.population_queue_stats().deferred_for_reads;
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data_o);
    c.wait_cached(BUCKET, "o", 8, Duration::from_secs(30)).await;
    let q = c.backend.population_queue_stats();
    assert!(
        q.deferred_for_reads > before,
        "the population of o yielded to the held read of p: {q:?}"
    );
    assert!(!get_p.is_finished(), "p is still held");
    // Release: p completes with its bytes.
    gate.add_permits(1 << 20);
    assert_eq!(get_p.await.unwrap(), data_p);
    c.backend.node_clients().set_fault_hook(None);
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data_o);
    c.shutdown().await;
}

/// A node error on the first attempt is retried with backoff and the
/// population completes. The RPC layer itself retries transport errors
/// (ADR-0011); the queue sees a node error only once those attempts are
/// exhausted, so enough consecutive attempts for one shard fail to
/// exhaust them once.
#[tokio::test]
async fn node_errors_are_retried() {
    // One worker, so that the failures land on one population's RPC
    // instead of being spread over two concurrent ones.
    let c = TestCluster::start(options(64, 2, 1)).await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let rpc_attempts = u64::from(c.backend.node_clients().policy().max_attempts);
    let failures = Arc::new(AtomicU64::new(0));
    let f = Arc::clone(&failures);
    c.backend
        .node_clients()
        .set_fault_hook(Some(hook(move |ctx| {
            if ctx.rpc == Rpc::PutShard
                && ctx.group_id.is_some_and(|g| g > 1)
                && ctx.shard.is_some_and(|s| s.shard_index == 0)
                && f.fetch_add(1, Ordering::Relaxed) < rpc_attempts + 1
            {
                FaultAction::FailBefore(tonic::Code::Unavailable, "node away".into())
            } else {
                FaultAction::None
            }
        })));
    let data = payload(2, 4);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(30)).await;
    let q = c.backend.population_queue_stats();
    assert!(q.retries >= 1, "{q:?}");
    assert_eq!(c.backend.cache_stats().population_failures_node, 0);
    c.backend.node_clients().set_fault_hook(None);
    c.shutdown().await;
}

/// Shutdown with a full queue: running attempts finish, queued ones are
/// dropped and counted, dedup markers are released, and shutdown returns.
#[tokio::test]
async fn shutdown_drains_the_queue() {
    let c = TestCluster::start(options(8, 1, 1)).await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c.backend.node_clients().set_fault_hook(Some(hook(|ctx| {
        if ctx.rpc == Rpc::PutShard && ctx.group_id.is_some_and(|g| g > 1) {
            FaultAction::DelayBefore(Duration::from_millis(400))
        } else {
            FaultAction::None
        }
    })));
    let data = payload(8, 5);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    let before = c.backend.population_queue_stats();
    assert!(before.queued + before.running >= 1, "{before:?}");
    let started = Instant::now();
    c.backend.shutdown().await;
    assert!(started.elapsed() < Duration::from_secs(5));
    let after = c.backend.population_queue_stats();
    assert_eq!(after.queued, 0, "{after:?}");
    assert_eq!(after.running, 0, "{after:?}");
    assert_eq!(
        after.enqueued,
        after.completed + after.dropped_shutdown,
        "every queued population either completed or was dropped: {after:?}"
    );
    // After shutdown a miss is served and simply not populated.
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.backend.node_clients().set_fault_hook(None);
    c.shutdown().await;
}
