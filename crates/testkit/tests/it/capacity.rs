//! Capacity safety of a cluster (Phase 3.6): concurrent gateways never
//! write past the nodes' reserve, a full cluster refuses new data with
//! `SlowDown` and nothing else, ACKed objects stay readable, deletes and
//! GC free space, and the controller's admission record throttles and
//! rejects writes before the reserve is reached and lifts one step at a
//! time.

#![allow(clippy::too_many_lines)]

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

/// Publish `state` (and `rate`) for tier `default` as a leader would and
/// let the gateway see it.
async fn force_state(c: &TestCluster, state: u8, rate: u64) {
    let rec = arca_lake_metadata::ops::admission::AdmissionRecord {
        computed_ms: now_ms(),
        ttl_ms: 30_000,
        gateways: 1,
        tiers: vec![arca_lake_metadata::ops::admission::TierAdmission {
            tier: "default".into(),
            state,
            rate_bytes_per_s: rate,
            excluded_groups: Vec::new(),
            free_permille: 500,
            reason: "test".into(),
            group_room: Vec::new(),
        }],
        leader_token: u64::MAX,
        incarnation: c.incarnation(),
        ..Default::default()
    };
    admission::publish(c.meta.as_ref(), &rec).await.unwrap();
    let rec = admission::current(c.meta.as_ref()).await.unwrap();
    c.backend.cluster().admission.observe(rec, Instant::now());
}

fn charged(c: &TestCluster) -> u64 {
    c.backend.cluster().admission.snapshot().charged_bytes
}

async fn copy(c: &TestCluster, src: &str, dst: &str) -> s3s::S3Result<s3s::dto::CopyObjectOutput> {
    let mut b = s3s::dto::CopyObjectInput::builder();
    b.set_bucket(BUCKET.into());
    b.set_key(dst.into());
    b.set_copy_source(s3s::dto::CopySource::Bucket {
        bucket: BUCKET.into(),
        key: src.into(),
        version_id: None,
    });
    Ok(c.backend
        .copy_object(request(b.build().unwrap()))
        .await?
        .output)
}

/// R-81 (Phase 3.7): PUTs without a length and copies are charged by the
/// chunks they actually write; a throttled writer waits a bounded time per
/// chunk, a write refused part way is never visible and leaves nothing
/// once collected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_length_puts_and_copies_are_charged_by_what_they_write() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let big = Bytes::from(random_bytes(usize::try_from(8 * MIB).unwrap(), 70));
    s3::put(
        &c.backend,
        BUCKET,
        "src",
        big.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(charged(&c), 8 * MIB, "a PUT without a length: 8 chunks");
    let baseline = c.total_shards().await;

    // Throttled at 4 MiB/s: the PUT (no length) waits chunk by chunk.
    force_state(&c, 2, 4 * MIB).await;
    let t = Instant::now();
    let before = charged(&c);
    s3::put(
        &c.backend,
        BUCKET,
        "slow",
        big.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(charged(&c) - before, 8 * MIB);
    assert!(t.elapsed().as_secs_f64() >= 1.5, "{:?}", t.elapsed());
    // A copy is charged by the destination bytes it writes.
    let before = charged(&c);
    copy(&c, "src", "copied").await.unwrap();
    assert_eq!(charged(&c) - before, 8 * MIB, "the copy's 8 chunks");
    assert_eq!(s3::get(&c.backend, BUCKET, "copied").await.unwrap().0, big);

    // The floor rate (1 MiB/s) shared by four concurrent writers: chunks
    // cannot all wait their turn; some writes fail part way, and nothing
    // of those becomes visible.
    force_state(&c, 2, 0).await;
    let results = futures::future::join_all((0..4).map(|i| {
        let (b, data) = (c.backend.clone(), big.clone());
        async move {
            let key = format!("contended{i}");
            (
                key.clone(),
                s3::put(&b, BUCKET, &key, data, &PutOptions::default()).await,
            )
        }
    }))
    .await;
    let mut kept = 0;
    let mut refused = 0;
    for (key, r) in &results {
        match r {
            Ok(_) => {
                kept += 1;
                assert_eq!(s3::get(&c.backend, BUCKET, key).await.unwrap().0, big);
            }
            Err(e) => {
                refused += 1;
                assert_eq!(*e.code(), S3ErrorCode::SlowDown, "{e:?}");
                let e = s3::head(&c.backend, BUCKET, key).await.unwrap_err();
                assert_eq!(*e.code(), S3ErrorCode::NoSuchKey, "{key} visible");
            }
        }
    }
    assert!(refused > 0, "four writers at 1 MiB/s with a 2 s bound");
    // Refusing new data refuses copies too, before anything is written.
    force_state(&c, 3, 0).await;
    let e = copy(&c, "src", "copied2").await.unwrap_err();
    assert_eq!(*e.code(), S3ErrorCode::SlowDown);
    assert_eq!(
        *s3::head(&c.backend, BUCKET, "copied2")
            .await
            .unwrap_err()
            .code(),
        S3ErrorCode::NoSuchKey
    );
    // Reads go on.
    assert_eq!(s3::get(&c.backend, BUCKET, "src").await.unwrap().0, big);
    // Whatever the refused PUT wrote is collected.
    force_state(&c, 0, 0).await;
    c.gc(now_ms() + 3_600_000, 0).await;
    c.gc(now_ms() + 3_600_000, 0).await;
    c.reconcile(now_ms() + 3_600_000, 0).await;
    assert_eq!(
        c.total_shards().await,
        baseline + (2 + kept) * 8 * 3,
        "the new objects' shards only"
    );
    for i in 0..3 {
        assert_eq!(
            c.node_service(i).device_capacity().unwrap().inflight_bytes,
            0
        );
    }
    c.shutdown().await;
}

/// R-85 (Phase 3.7): a new leadership term never relaxes on partial
/// reports. For each state the former leader published, a new term with a
/// member not yet heard keeps at least that state (until the staleness
/// bound, after which missing reports count as stale), and once every
/// member has reported it leaves the state only through the hysteresis,
/// within a bounded number of passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_term_keeps_the_published_state_until_every_member_reported() {
    use arca_lake_controller::admission::{Term, pass_in_term};
    let mut c = capped(160 * MIB).await;
    let policy = Policy {
        dwell_ms: 0,
        ..Policy::default()
    };
    let node_ids: Vec<u64> = c.nodes[..3].iter().map(|n| n.id).collect();
    for (round, published) in [0u8, 2, 3, 4].into_iter().enumerate() {
        force_state(&c, published, 4 * MIB).await;
        let tracker = Tracker::default();
        let token = 100 + round as u64;
        let t0 = now_ms();
        // Only two of three members reported in the new term.
        for n in &c.nodes[..2] {
            let h = c.clients.health(&n.addr.to_string()).await.unwrap();
            tracker.note_device(device_of(n.id, &h, t0));
        }
        let term = Term {
            token,
            pass_started_ms: t0,
            incarnation: c.incarnation(),
        };
        pass_in_term(c.meta.as_ref(), &tracker, &policy, MIB, 0, term, t0)
            .await
            .unwrap();
        let rec = admission::current(c.meta.as_ref()).await.unwrap().unwrap();
        let t = rec.tier("default").unwrap();
        assert!(rec.warming_up, "round {round}");
        assert_eq!(rec.leader_token, token);
        assert!(
            t.state >= published.max(1),
            "published {published}, new leader {} ({})",
            t.state,
            t.reason
        );
        // Past the staleness bound the missing member counts as stale:
        // the usual rules, no longer the floor, and never `normal`.
        let late = t0 + policy.stale_ms + 1;
        for n in &c.nodes[..2] {
            let h = c.clients.health(&n.addr.to_string()).await.unwrap();
            tracker.note_device(device_of(n.id, &h, late));
        }
        pass_in_term(c.meta.as_ref(), &tracker, &policy, MIB, 0, term, late)
            .await
            .unwrap();
        let rec = admission::current(c.meta.as_ref()).await.unwrap().unwrap();
        assert!(!rec.warming_up);
        assert!(rec.tier("default").unwrap().state >= 1, "a member is stale");
        // Every member reports: the state converges to `normal` one step
        // per pass (dwell 0 here), however bad the published one was.
        let mut now = late;
        let mut path = Vec::new();
        for _ in 0..8 {
            now += 1;
            for id in &node_ids {
                let n = c.nodes.iter().find(|n| n.id == *id).unwrap();
                let h = c.clients.health(&n.addr.to_string()).await.unwrap();
                tracker.note_device(device_of(*id, &h, now));
            }
            pass_in_term(c.meta.as_ref(), &tracker, &policy, MIB, 0, term, now)
                .await
                .unwrap();
            let s = admission::current(c.meta.as_ref())
                .await
                .unwrap()
                .unwrap()
                .tier("default")
                .unwrap()
                .state;
            path.push(s);
            if s == 0 {
                break;
            }
        }
        assert_eq!(path.last(), Some(&0), "round {round}: {path:?}");
        for w in path.windows(2) {
            assert!(w[1] + 1 >= w[0], "{path:?}");
        }
    }
    // No record at all and a member missing: at least `throttled`.
    let fresh = c.meta.as_ref();
    {
        use arca_lake_metadata::Txn as _;
        arca_lake_metadata::transact(fresh, (), |tx, ()| {
            Box::pin(async move {
                tx.clear(&arca_lake_metadata::keys::admission());
                Ok(())
            })
        })
        .await
        .unwrap();
    }
    let tracker = Tracker::default();
    let t0 = now_ms();
    let h = c
        .clients
        .health(&c.nodes[0].addr.to_string())
        .await
        .unwrap();
    tracker.note_device(device_of(c.nodes[0].id, &h, t0));
    let term = Term {
        token: 999,
        pass_started_ms: t0,
        incarnation: c.incarnation(),
    };
    pass_in_term(fresh, &tracker, &policy, MIB, 0, term, t0)
        .await
        .unwrap();
    let rec = admission::current(fresh).await.unwrap().unwrap();
    assert!(rec.tier("default").unwrap().state >= 2);
    c.stop_node(2).await;
    c.shutdown().await;
}

/// R-85: a former leader cannot publish once another holds leadership
/// (the record is written through the fenced store).
#[tokio::test]
async fn a_former_leader_cannot_publish_admission() {
    use arca_lake_metadata::fence::FencedKv;
    use arca_lake_metadata::ops::leader;
    let c = capped(160 * MIB).await;
    let t = now_ms();
    let old = leader::try_acquire(c.meta.as_ref(), "old", t, 1000)
        .await
        .unwrap()
        .unwrap();
    let old_store = FencedKv::new(Arc::clone(&c.meta), old);
    admission::publish(
        &old_store,
        &arca_lake_metadata::ops::admission::AdmissionRecord::default(),
    )
    .await
    .unwrap();
    // The lease lapses; a new leader takes over.
    leader::try_acquire(c.meta.as_ref(), "new", t + 5000, 1000)
        .await
        .unwrap()
        .unwrap();
    let late = admission::publish(
        &old_store,
        &arca_lake_metadata::ops::admission::AdmissionRecord {
            computed_ms: t + 6000,
            ..Default::default()
        },
    )
    .await;
    assert!(
        late.is_err(),
        "a delayed write of the former leader was accepted"
    );
    c.shutdown().await;
}

/// R-80 (Phase 3.7): nodes on one file system without allotments that fit
/// it are detected; their groups leave placement (the tier refuses new
/// data, saying why) unless shared file systems are allowed; allotments
/// that fit make the same layout safe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_sharing_a_file_system_without_allotments_leave_placement() {
    let mut c = capped(160 * MIB).await;
    let tracker = Tracker::default();
    let policy = Policy::default();
    // Allotted (the test layout): one domain, safe.
    assert_eq!(evaluate(&c, &tracker, &policy).await, State::Normal);
    let rec = admission::current(c.meta.as_ref()).await.unwrap().unwrap();
    assert!(rec.capacity_conflicts.is_empty(), "{rec:?}");
    // One node loses its allotment: all three share the file system and
    // nothing proves their reserves hold.
    c.set_node_capacity(2, None).await;
    let tracker = Tracker::default();
    let state = evaluate(&c, &tracker, &policy).await;
    let rec = admission::current(c.meta.as_ref()).await.unwrap().unwrap();
    let mut ids: Vec<u64> = c.nodes[..3].iter().map(|n| n.id).collect();
    ids.sort_unstable();
    assert_eq!(rec.capacity_conflicts, ids);
    assert_eq!(state, State::WriteRejected);
    assert!(
        rec.tier("default")
            .unwrap()
            .reason
            .contains("share a file system"),
        "{rec:?}"
    );
    let e = s3::put(&c.backend, BUCKET, "x", body("x"), &PutOptions::default())
        .await
        .unwrap_err();
    assert_eq!(*e.code(), S3ErrorCode::SlowDown);
    // Explicitly allowed (a development cluster): placed again, still
    // reported.
    let allow = Policy {
        allow_shared_filesystem: true,
        ..Policy::default()
    };
    let allow = Policy {
        dwell_ms: 0,
        ..allow
    };
    let tracker = Tracker::default();
    // A new term starts at the published state and steps down.
    let mut st = State::RecoveryOnly;
    for _ in 0..5 {
        st = evaluate(&c, &tracker, &allow).await;
    }
    assert!(
        st < State::WriteRejected,
        "{:?}",
        admission::current(c.meta.as_ref()).await.unwrap()
    );
    let rec = admission::current(c.meta.as_ref()).await.unwrap().unwrap();
    assert_eq!(rec.capacity_conflicts, ids);
    s3::put(&c.backend, BUCKET, "y", body("y"), &PutOptions::default())
        .await
        .unwrap();
    c.shutdown().await;
}

/// R-82 (Phase 3.7): new chunks go to the groups with the most room.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_chunks_prefer_the_group_with_more_room() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(6, 2, 1, MIB, 64 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let groups: Vec<_> = c
        .groups()
        .await
        .into_iter()
        .filter(|g| !g.is_cache() && g.scheme == "rs-gf8-v1")
        .collect();
    assert_eq!(groups.len(), 2, "{groups:?}");
    let small_members: Vec<usize> = groups[0]
        .members
        .iter()
        .map(|m| c.node_index(m).unwrap())
        .collect();
    let others: Vec<usize> = (0..6).filter(|i| !small_members.contains(i)).collect();
    for &i in &small_members {
        c.set_node_capacity(i, Some(300 * MIB)).await;
    }
    for &i in &others {
        c.set_node_capacity(i, Some(1 << 30)).await;
    }
    let tracker = Tracker::default();
    let policy = Policy {
        group_min_shards: 4,
        ..Policy::default()
    };
    let now = now_ms();
    for n in &c.nodes {
        let h = c.clients.health(&n.addr.to_string()).await.unwrap();
        tracker.note_device(device_of(n.id, &h, now));
    }
    pass(c.meta.as_ref(), &tracker, &policy, MIB, 0, now)
        .await
        .unwrap();
    let rec = admission::current(c.meta.as_ref()).await.unwrap();
    c.backend.cluster().admission.observe(rec, Instant::now());
    let mut in_roomy = 0;
    for i in 0..20 {
        let key = format!("p{i}");
        s3::put(&c.backend, BUCKET, &key, body(&key), &PutOptions::default())
            .await
            .unwrap();
        if c.manifest(BUCKET, &key).await.chunks[0]
            .1
            .placement
            .group_id
            == groups[1].id
        {
            in_roomy += 1;
        }
    }
    assert_eq!(in_roomy, 20, "every chunk in the group with more room");
    c.shutdown().await;
}

/// R-85: during warm-up a new term also keeps the groups the published
/// record excluded (a member not yet heard may be the full one).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_term_keeps_published_exclusions_until_every_member_reported() {
    use arca_lake_controller::admission::{Term, pass_in_term};
    let c = capped(160 * MIB).await;
    let group = c
        .groups()
        .await
        .into_iter()
        .find(|g| !g.is_cache())
        .unwrap()
        .id;
    let rec = arca_lake_metadata::ops::admission::AdmissionRecord {
        computed_ms: now_ms(),
        ttl_ms: 30_000,
        gateways: 1,
        tiers: vec![arca_lake_metadata::ops::admission::TierAdmission {
            tier: "default".into(),
            state: 2,
            excluded_groups: vec![group],
            reason: "test".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    admission::publish(c.meta.as_ref(), &rec).await.unwrap();
    let tracker = Tracker::default();
    let t0 = now_ms();
    let h = c
        .clients
        .health(&c.nodes[0].addr.to_string())
        .await
        .unwrap();
    tracker.note_device(device_of(c.nodes[0].id, &h, t0));
    let term = Term {
        token: 7,
        pass_started_ms: t0,
        incarnation: c.incarnation(),
    };
    pass_in_term(
        c.meta.as_ref(),
        &tracker,
        &Policy::default(),
        MIB,
        0,
        term,
        t0,
    )
    .await
    .unwrap();
    let rec = admission::current(c.meta.as_ref()).await.unwrap().unwrap();
    assert!(rec.warming_up);
    assert!(
        rec.tier("default")
            .unwrap()
            .excluded_groups
            .contains(&group),
        "{rec:?}"
    );
    c.shutdown().await;
}

/// Phase 3.8 (H1/H2): the Phase 3.7 finding reproduced. A node filled
/// with small shards to the client reserve, then everything deleted: each
/// delete writes a tombstone (one record alignment), which used to take
/// the bytes out of the reserve (11 MiB in Phase 3.7 Test C). The delete
/// reserve now keeps them above it: after every tombstone, the node still
/// has its plain reserve free; the breakdown accounts for the tombstones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_everything_on_a_full_node_stays_within_the_delete_reserve() {
    let mut c = capped(96 * MIB).await;
    let data = Bytes::from(random_bytes(32 << 10, 81));
    let mut keys = Vec::new();
    'fill: for round in 0.. {
        let batch: Vec<String> = (0..16).map(|i| format!("s{round}-{i}")).collect();
        let results = futures::future::join_all(batch.iter().map(|k| {
            let (b, d) = (c.backend.clone(), data.clone());
            async move { s3::put(&b, BUCKET, k, d, &PutOptions::default()).await }
        }))
        .await;
        for (k, r) in batch.into_iter().zip(results) {
            match r {
                Ok(_) => keys.push(k),
                Err(e) => {
                    assert_eq!(*e.code(), S3ErrorCode::SlowDown, "{e:?}");
                    break 'fill;
                }
            }
        }
    }
    assert!(keys.len() > 1000, "{} objects", keys.len());
    assert_reserves_kept(&c);
    // A restart while full: nothing held, the same delete reserve (it is
    // derived from the records on disk), the reserve still kept.
    let d0 = c.node_service(0).device_capacity().unwrap();
    c.stop_node(0).await;
    c.start_node(0).await;
    let d1 = c.node_service(0).device_capacity().unwrap();
    assert_eq!(d1.inflight_bytes, 0);
    assert_eq!(d1.delete_reserve_bytes, d0.delete_reserve_bytes);
    for i in 0..8 {
        if s3::put(
            &c.backend,
            BUCKET,
            &format!("more{i}"),
            data.clone(),
            &PutOptions::default(),
        )
        .await
        .is_ok()
        {
            keys.push(format!("more{i}"));
        }
    }
    assert_reserves_kept(&c);
    let plain: Vec<u64> = (0..3)
        .map(|i| {
            let d = c.node_service(i).device_capacity().unwrap();
            assert!(
                d.delete_reserve_bytes >= 2000 * c.node_store(i).tombstone_bytes(),
                "{d:?}"
            );
            d.reserved_bytes - d.delete_reserve_bytes
        })
        .collect();
    let before: Vec<u64> = (0..3)
        .map(|i| c.node_service(i).capacity_breakdown().written.tombstone)
        .collect();
    for k in &keys {
        s3::delete(&c.backend, BUCKET, k).await.unwrap();
    }
    // Collect without compacting: every tombstone is on the devices.
    for _ in 0..6 {
        c.gc(now_ms() + 3_600_000, 0).await;
    }
    // The refused writes' orphans (placed on some members before the
    // refusal) are reconciliation's.
    c.reconcile(now_ms() + 3_600_000, 0).await;
    // At most the refused batch's pending writes are left (their leases
    // run on the real clock).
    assert!(
        c.total_shards().await <= 16 * 3,
        "everything else collected"
    );
    for i in 0..3 {
        let s = c.node_service(i);
        let d = s.device_capacity().unwrap();
        assert!(
            d.available_bytes >= plain[i],
            "node {i}: tombstones went into the plain reserve ({} free, {} kept): {d:?}",
            d.available_bytes,
            plain[i]
        );
        let b = s.capacity_breakdown();
        let tombs = b.written.tombstone - before[i];
        assert!(
            tombs >= keys.len() as u64 * s.store().tombstone_bytes(),
            "node {i}: {tombs} tombstone bytes for {} deletes",
            keys.len()
        );
    }
    // Compaction gives the space back; writes resume.
    compact_all(&c);
    s3::put(
        &c.backend,
        BUCKET,
        "after",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_reserves_kept(&c);
    c.shutdown().await;
}

/// Phase 3.8 (H3/H4): which admission record a gateway takes. `write_
/// rejected` published in this incarnation; then a `normal` record of
/// another incarnation (a restored store's leader), and a `normal` record
/// of an older term computed an hour later: neither lifts the refusal.
/// The record removed: the last one stays in force. A newer term of this
/// incarnation lifts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_newer_records_of_this_incarnation_lift_a_refusal() {
    use arca_lake_metadata::ops::admission::{AdmissionRecord, TierAdmission};
    let c = capped(160 * MIB).await;
    let publish = |state: u8, token: u64, incarnation: [u8; 16], computed_ms: u64| {
        let meta = Arc::clone(&c.meta);
        let backend = c.backend.clone();
        async move {
            let rec = AdmissionRecord {
                computed_ms,
                ttl_ms: 3_600_000,
                gateways: 1,
                tiers: vec![TierAdmission {
                    tier: "default".into(),
                    state,
                    reason: "test".into(),
                    ..Default::default()
                }],
                leader_token: token,
                incarnation,
                ..Default::default()
            };
            admission::publish(meta.as_ref(), &rec).await.unwrap();
            let rec = admission::current(meta.as_ref()).await.unwrap();
            backend.cluster().admission.observe(rec, Instant::now());
        }
    };
    let put = |k: &'static str| {
        let b = c.backend.clone();
        async move { s3::put(&b, BUCKET, k, body(k), &PutOptions::default()).await }
    };
    let ours = c.incarnation();
    let mut other = ours;
    other[0] ^= 0xff;
    publish(3, 10, ours, now_ms()).await;
    assert_eq!(*put("a").await.unwrap_err().code(), S3ErrorCode::SlowDown);
    publish(0, 99, other, now_ms()).await;
    assert_eq!(*put("b").await.unwrap_err().code(), S3ErrorCode::SlowDown);
    publish(0, 9, ours, now_ms() + 3_600_000).await;
    assert_eq!(*put("c").await.unwrap_err().code(), S3ErrorCode::SlowDown);
    assert_eq!(c.backend.cluster().admission.ignored_records(), 2);
    {
        use arca_lake_metadata::Txn as _;
        arca_lake_metadata::transact(c.meta.as_ref(), (), |tx, ()| {
            Box::pin(async move {
                tx.clear(&arca_lake_metadata::keys::admission());
                Ok(())
            })
        })
        .await
        .unwrap();
    }
    c.backend.cluster().admission.observe(
        admission::current(c.meta.as_ref()).await.unwrap(),
        Instant::now(),
    );
    assert_eq!(*put("d").await.unwrap_err().code(), S3ErrorCode::SlowDown);
    publish(0, 11, ours, now_ms()).await;
    put("e").await.unwrap();
    c.shutdown().await;
}
