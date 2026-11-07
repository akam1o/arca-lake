//! Capacity safety of a cluster (Phase 3.6): concurrent gateways never
//! write past the nodes' reserve, a full cluster refuses new data with
//! `SlowDown` and nothing else, ACKed objects stay readable, deletes and
//! GC free space, and the controller's admission record throttles and
//! rejects writes before the reserve is reached and lifts one step at a
//! time.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arca_lake_controller::admission::{Tracker, device_of, pass};
use arca_lake_controller::backpressure::{Policy, State};
use arca_lake_metadata::ops::admission;
use arca_lake_testkit::s3::{self, PutOptions, request};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use futures::StreamExt as _;
use s3s::S3;
use s3s::S3ErrorCode;
use s3s::dto::CreateMultipartUploadInput;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "cap";

fn body(key: &str) -> Bytes {
    let seed = key
        .bytes()
        .fold(7u64, |h, b| h.wrapping_mul(31).wrapping_add(u64::from(b)));
    Bytes::from(random_bytes(usize::try_from(MIB).unwrap(), seed))
}

/// Three nodes (2 + 1), each capped at `cap` bytes.
async fn capped(cap: u64) -> TestCluster {
    let mut c = TestCluster::start(ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)).await;
    for i in 0..3 {
        c.set_node_capacity(i, Some(cap)).await;
    }
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn assert_reserves_kept(c: &TestCluster) {
    for i in 0..3 {
        let d = c.node_service(i).device_capacity().unwrap();
        assert!(
            d.available_bytes >= d.reserved_bytes,
            "node {i} crossed its reserve: {d:?}"
        );
        assert_eq!(
            d.inflight_bytes, 0,
            "node {i} holds bytes after the writes ended"
        );
    }
}

/// Free space again: delete `keys`, collect, compact every node.
async fn reclaim(c: &TestCluster, keys: &[String]) {
    for k in keys {
        s3::delete(&c.backend, BUCKET, k).await.unwrap();
    }
    c.gc(now_ms() + 3_600_000, 0).await;
    compact_all(c);
}

fn compact_all(c: &TestCluster) {
    for i in 0..3 {
        let store = c.node_store(i);
        for seg in store.compaction_candidates(0.05).unwrap() {
            store.compact(seg).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_gateways_fill_to_the_reserve_and_get_only_slowdown() {
    let c = capped(160 * MIB).await;
    let mut gateways = vec![c.backend.clone()];
    for _ in 0..3 {
        gateways.push(c.extra_backend().await.unwrap());
    }
    let acked = Arc::new(Mutex::new(BTreeSet::new()));
    let refused = Arc::new(Mutex::new(BTreeSet::new()));
    // Four gateways, eight writers each, until every writer is refused
    // three times in a row.
    let writers = gateways
        .iter()
        .enumerate()
        .flat_map(|(g, gw)| (0..8).map(move |w| (g, w, gw.clone())));
    futures::stream::iter(writers)
        .for_each_concurrent(None, |(g, w, gw)| {
            let (acked, refused) = (Arc::clone(&acked), Arc::clone(&refused));
            async move {
                let mut misses = 0;
                for n in 0.. {
                    let key = format!("g{g}/w{w}/{n}");
                    match s3::put(&gw, BUCKET, &key, body(&key), &PutOptions::default()).await {
                        Ok(_) => {
                            acked.lock().unwrap().insert(key);
                            misses = 0;
                        }
                        Err(e) => {
                            assert_eq!(*e.code(), S3ErrorCode::SlowDown, "{key}: {e:?}");
                            let retry = e
                                .headers()
                                .and_then(|h| h.get(http::header::RETRY_AFTER))
                                .and_then(|v| v.to_str().ok()?.parse::<u64>().ok());
                            assert!(retry.is_some_and(|s| s >= 1), "no Retry-After: {e:?}");
                            refused.lock().unwrap().insert(key);
                            misses += 1;
                            if misses == 3 {
                                return;
                            }
                        }
                    }
                }
            }
        })
        .await;
    let acked: Vec<String> = acked.lock().unwrap().iter().cloned().collect();
    let refused: Vec<String> = refused.lock().unwrap().iter().cloned().collect();
    // 120 MiB above each node's 40 MiB reserve, 512 KiB per node and
    // object: about 230 objects fit.
    assert!(acked.len() > 150, "{} acked", acked.len());
    assert_reserves_kept(&c);
    // ACKed objects read back; refused ones are not visible at all.
    for k in &acked {
        let (data, _) = s3::get(&c.backend, BUCKET, k).await.unwrap();
        assert_eq!(data, body(k), "{k}");
    }
    for k in &refused {
        let e = s3::head(&c.backend, BUCKET, k).await.unwrap_err();
        assert_eq!(*e.code(), S3ErrorCode::NoSuchKey, "{k}");
    }
    // Deleting, GC and compaction give the space back; writes resume.
    reclaim(&c, &acked[..acked.len() / 2]).await;
    s3::put(
        &c.backend,
        BUCKET,
        "after",
        body("after"),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    // No shard left of a refused write: the pending generations of the
    // refused PUTs expire and are collected, and the shards a refused
    // chunk left on the members that had room (no chunk record names
    // them) are reconciliation's.
    c.gc(now_ms() + 3_600_000, 0).await;
    c.gc(now_ms() + 3_600_000, 0).await;
    c.reconcile(now_ms() + 3_600_000, 0).await;
    let keep = acked.len() - acked.len() / 2 + 1;
    assert_eq!(
        c.total_shards().await,
        keep * 3,
        "orphans of refused writes left"
    );
    c.shutdown().await;
}

/// One admission pass as the controller leader runs it after a health
/// pass, and the gateway's refresh.
async fn evaluate(c: &TestCluster, tracker: &Tracker, policy: &Policy) -> State {
    let now = now_ms();
    for n in &c.nodes[..3] {
        let h = c.clients.health(&n.addr.to_string()).await.unwrap();
        tracker.note_device(device_of(n.id, &h, now));
    }
    pass(c.meta.as_ref(), tracker, policy, MIB, 0, now)
        .await
        .unwrap();
    let rec = admission::current(c.meta.as_ref()).await.unwrap();
    c.backend.cluster().admission.observe(rec, Instant::now());
    tracker.worst()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_admission_record_refuses_new_data_keeps_reads_and_lifts_step_by_step() {
    let c = capped(160 * MIB).await;
    let tracker = Tracker::default();
    let policy = Policy {
        dwell_ms: 0,
        // A throttle the test can write through quickly.
        min_rate_bytes_per_s: 32.0 * 1024.0 * 1024.0,
        // 64 shards of room per member is a third of these small devices.
        group_min_shards: 4,
        ..Policy::default()
    };
    assert_eq!(evaluate(&c, &tracker, &policy).await, State::Normal);
    // Fill, evaluating as the controller would every few writes: the
    // record throttles, then rejects, before the nodes refuse anything.
    let mut keys = Vec::new();
    let mut seen = BTreeSet::new();
    let mut n = 0;
    loop {
        assert!(n < 1000, "never rejected");
        let key = format!("k{n}");
        match s3::put(&c.backend, BUCKET, &key, body(&key), &PutOptions::default()).await {
            Ok(_) => {
                keys.push(key);
                n += 1;
            }
            Err(e) => {
                assert_eq!(*e.code(), S3ErrorCode::SlowDown, "{e:?}");
                // Throttled: retry as a client would after Retry-After
                // (shortened here); rejected: done.
                if evaluate(&c, &tracker, &policy).await >= State::WriteRejected {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        }
        if n % 4 == 0 {
            seen.insert(evaluate(&c, &tracker, &policy).await);
        }
    }
    let state = evaluate(&c, &tracker, &policy).await;
    assert!(state >= State::WriteRejected, "{state:?}");
    assert!(seen.contains(&State::Warning), "{seen:?}");
    assert!(seen.contains(&State::Throttled), "{seen:?}");
    let refusals = c.backend.cluster().admission.snapshot();
    assert!(refusals.throttled + refusals.rejected > 0, "{refusals:?}");
    assert_eq!(
        refusals.node_capacity_refusals, 0,
        "the record stopped writes before any node had to"
    );
    // New data refused; reads, heads and deletes go on; no new upload.
    let e = s3::put(
        &c.backend,
        BUCKET,
        "new",
        body("new"),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(*e.code(), S3ErrorCode::SlowDown);
    let e = c
        .backend
        .create_multipart_upload(request(CreateMultipartUploadInput {
            bucket: BUCKET.into(),
            key: "mp".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(*e.code(), S3ErrorCode::SlowDown);
    for k in &keys {
        assert_eq!(s3::get(&c.backend, BUCKET, k).await.unwrap().0, body(k));
        s3::head(&c.backend, BUCKET, k).await.unwrap();
    }
    assert_reserves_kept(&c);
    // Space back: the state lifts one step per evaluation (dwell 0 here).
    reclaim(&c, &keys[..keys.len() * 3 / 4]).await;
    let mut path = vec![state];
    while *path.last().unwrap() != State::Normal {
        path.push(evaluate(&c, &tracker, &policy).await);
        assert!(path.len() < 10, "{path:?}");
    }
    for w in path.windows(2) {
        assert!(
            w[1].code() + 1 >= w[0].code(),
            "more than one step at a time: {path:?}"
        );
    }
    s3::put(
        &c.backend,
        BUCKET,
        "after",
        body("after"),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    c.shutdown().await;
}

#[tokio::test]
async fn stale_or_missing_capacity_reports_fail_closed() {
    let c = capped(160 * MIB).await;
    let tracker = Tracker::default();
    let policy = Policy::default();
    // No device heard yet: nothing is known about the tier's capacity.
    pass(c.meta.as_ref(), &tracker, &policy, MIB, 0, now_ms())
        .await
        .unwrap();
    assert_eq!(tracker.worst(), State::RecoveryOnly);
    let rec = admission::current(c.meta.as_ref()).await.unwrap();
    c.backend.cluster().admission.observe(rec, Instant::now());
    let e = s3::put(&c.backend, BUCKET, "x", body("x"), &PutOptions::default())
        .await
        .unwrap_err();
    assert_eq!(*e.code(), S3ErrorCode::SlowDown);
    // Reports older than the staleness bound count as none.
    assert_eq!(
        evaluate(&c, &tracker, &policy).await,
        State::RecoveryOnly,
        "worse holds until it lifts"
    );
    let later = now_ms() + policy.stale_ms + 1;
    pass(c.meta.as_ref(), &tracker, &policy, MIB, 0, later)
        .await
        .unwrap();
    assert_eq!(tracker.worst(), State::RecoveryOnly);
    c.shutdown().await;
}

#[tokio::test]
async fn repair_stops_on_a_full_target_and_resumes_once_it_has_room() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut keys = Vec::new();
    for i in 0..6 {
        let key = format!("r{i}");
        let data = Bytes::from(random_bytes(usize::try_from(2 * MIB).unwrap(), 40 + i));
        s3::put(
            &c.backend,
            BUCKET,
            &key,
            data.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        keys.push((key, data));
    }
    let group = c
        .groups()
        .await
        .into_iter()
        .find(|g| !g.is_cache())
        .unwrap();
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    // The replacement has room for about half of the member's 6 MiB.
    let target = c.add_node().await;
    c.set_node_capacity(target, Some(4 * MIB)).await;
    let result = c.repair_with(group.id, 0, target, async |_| Ok(())).await;
    let e = result.expect_err("a target without room cannot take the member");
    assert!(format!("{e}").contains("insufficient capacity"), "{e}");
    let dev = c.node_service(target).device_capacity().unwrap();
    assert!(dev.available_bytes >= dev.repair_floor_bytes, "{dev:?}");
    // Nothing was lost meanwhile: reads are served from the other members.
    for (k, data) in &keys {
        assert_eq!(&s3::get(&c.backend, BUCKET, k).await.unwrap().0, data);
    }
    // Room again: the repair resumes and completes.
    c.set_node_capacity(target, None).await;
    c.repair(group.id, 0, target).await;
    let repaired = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == group.id)
        .unwrap();
    assert_eq!(
        repaired.state,
        arca_lake_metadata::records::GroupState::Active
    );
    assert!(
        repaired
            .members
            .iter()
            .any(|m| m.node_id == c.nodes[target].id)
    );
    for (k, data) in &keys {
        assert_eq!(&s3::get(&c.backend, BUCKET, k).await.unwrap().0, data);
    }
    c.shutdown().await;
}

fn controller(id: &str) -> arca_lake_controller::process::ControllerConfig {
    let mut c = arca_lake_controller::process::ControllerConfig::new(id);
    c.lease = std::time::Duration::from_millis(800);
    c.health_interval = std::time::Duration::from_millis(100);
    c.gc_interval = std::time::Duration::from_millis(200);
    c.gc_grace = std::time::Duration::ZERO;
    c.reconcile_interval = std::time::Duration::from_secs(3600);
    c.failure_domain = "host".into();
    c.admission.dwell_ms = 500;
    c.admission.group_min_shards = 4;
    c.admission.min_rate_bytes_per_s = 32.0 * 1024.0 * 1024.0;
    c.admission_max_chunk_bytes = MIB;
    c
}

async fn published_state(c: &TestCluster) -> Option<(u64, u8)> {
    admission::current(c.meta.as_ref())
        .await
        .unwrap()
        .and_then(|r| r.tier("default").map(|t| (r.computed_ms, t.state)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_rebuilds_the_admission_state_from_fresh_reports() {
    use arca_lake_controller::process::spawn;
    let c = capped(160 * MIB).await;
    let a = spawn(Arc::clone(&c.meta), controller("ctl-a"))
        .await
        .unwrap();
    // Fill until the leader refuses new data.
    let mut keys = Vec::new();
    let start = Instant::now();
    for n in 0.. {
        assert!(start.elapsed().as_secs() < 60, "never rejected");
        let key = format!("l{n}");
        if s3::put(&c.backend, BUCKET, &key, body(&key), &PutOptions::default())
            .await
            .is_ok()
        {
            keys.push(key);
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        if published_state(&c).await.is_some_and(|(_, s)| s >= 3) {
            break;
        }
        // The gateway's refresh, without waiting for its timer.
        let rec = admission::current(c.meta.as_ref()).await.unwrap();
        c.backend.cluster().admission.observe(rec, Instant::now());
    }
    let (t0, s0) = published_state(&c).await.unwrap();
    assert!(s0 >= 3, "never rejected");
    // The leader dies; its record stays as it was.
    a.shutdown().await;
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    assert_eq!(published_state(&c).await.unwrap().0, t0, "no publisher");
    // A new leader evaluates from the nodes' fresh reports: still full,
    // still refused; nothing is inherited but the data on the devices.
    let b = spawn(Arc::clone(&c.meta), controller("ctl-b"))
        .await
        .unwrap();
    let start = Instant::now();
    let (t1, s1) = loop {
        if let Some((t, s)) = published_state(&c).await
            && t > t0
        {
            break (t, s);
        }
        assert!(
            start.elapsed().as_secs() < 20,
            "the new leader never published"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert!(t1 > t0);
    assert!(
        s1 >= 3,
        "a new leader must not admit onto full devices ({s1})"
    );
    // Space back: the new leader collects (it has given the nodes the
    // cluster's incarnation: deletes without it are refused) and lifts
    // the state step by step.
    for k in &keys {
        s3::delete(&c.backend, BUCKET, k).await.unwrap();
    }
    let start = Instant::now();
    while arca_lake_metadata::ops::gc::backlog(c.meta.as_ref(), 10)
        .await
        .unwrap()
        .0
        > 0
    {
        assert!(
            start.elapsed().as_secs() < 30,
            "the new leader never collected"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    compact_all(&c);
    let start = Instant::now();
    while published_state(&c).await.unwrap().1 != 0 {
        assert!(
            start.elapsed().as_secs() < 30,
            "never back to normal: {:?} now {} nodes {:?}",
            admission::current(c.meta.as_ref()).await.unwrap(),
            now_ms(),
            (0..3)
                .map(|i| c.node_service(i).device_capacity().unwrap())
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let rec = admission::current(c.meta.as_ref()).await.unwrap();
    c.backend.cluster().admission.observe(rec, Instant::now());
    s3::put(
        &c.backend,
        BUCKET,
        "after",
        body("after"),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_reserves_kept(&c);
    b.shutdown().await;
    c.shutdown().await;
}
