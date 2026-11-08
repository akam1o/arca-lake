//! Backpressure responsiveness (Phase 3.7, R-83). Measurement, run
//! explicitly:
//!
//! `cargo test --release -p arca-lake-testkit --test it capacity_bench:: --
//! --ignored --nocapture --test-threads=1`
//!
//! Three capped nodes (2 + 1), a controller with time-scaled admission
//! (trend window 60 s, recent window 10 s, dwell 10 s, horizon 300 s,
//! health every second, GC every second with a 10 s grace) and
//! compaction every 5 s. Load: 256 KiB overwrites of 400 keys at
//! `HIGH` ops/s for 120 s (past what the devices hold), then `LOW` ops/s
//! for 180 s. One JSON line per run and policy: accepted and refused per
//! second in each phase, PUT and GET p50/p95/p99, state transitions, the
//! time from the load drop to `normal`, the lowest headroom, and whether
//! any node had to refuse a write (it must not). `ARCA_CAP_BENCH_RUNS`
//! (default 3) repeats each policy.

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use arca_lake_controller::backpressure::Policy;
use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_metadata::ops::admission;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "capbench";
const HIGH: u64 = 200;
const LOW: u64 = 30;

fn runs() -> usize {
    std::env::var("ARCA_CAP_BENCH_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
}

fn controller(policy: Policy) -> ControllerConfig {
    let mut c = ControllerConfig::new("bench");
    c.lease = Duration::from_secs(2);
    c.health_interval = Duration::from_secs(1);
    c.gc_interval = Duration::from_secs(1);
    c.gc_grace = Duration::from_secs(10);
    c.reconcile_interval = Duration::from_secs(3600);
    c.failure_domain = "host".into();
    c.admission = policy;
    c.admission_max_chunk_bytes = MIB;
    c
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

#[derive(Default)]
struct Phase {
    ok: AtomicU64,
    refused: AtomicU64,
    lat: std::sync::Mutex<Vec<f64>>,
    get_lat: std::sync::Mutex<Vec<f64>>,
}

async fn drive(c: &TestCluster, rate: u64, secs: u64, phase: &Phase, data: &Bytes) {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(secs);
    let issued = AtomicU64::new(0);
    let worker = async || {
        loop {
            let n = issued.fetch_add(1, Relaxed);
            let due = started + Duration::from_secs_f64(n as f64 / rate as f64);
            if due >= deadline {
                return;
            }
            tokio::time::sleep_until(due.into()).await;
            let key = format!("k{}", n % 400);
            let t = Instant::now();
            if n.is_multiple_of(10) {
                let _ = s3::get(&c.backend, BUCKET, &key).await;
                phase
                    .get_lat
                    .lock()
                    .unwrap()
                    .push(t.elapsed().as_secs_f64() * 1e3);
                continue;
            }
            match s3::put(
                &c.backend,
                BUCKET,
                &key,
                data.clone(),
                &PutOptions::default(),
            )
            .await
            {
                Ok(_) => {
                    phase.ok.fetch_add(1, Relaxed);
                    phase
                        .lat
                        .lock()
                        .unwrap()
                        .push(t.elapsed().as_secs_f64() * 1e3);
                }
                Err(_) => {
                    phase.refused.fetch_add(1, Relaxed);
                }
            }
        }
    };
    futures::future::join_all((0..48).map(|_| worker())).await;
}

async fn run(label: &str, policy: Policy, iteration: usize) {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)
    })
    .await;
    for i in 0..3 {
        c.set_node_capacity(i, Some(600 * MIB)).await;
    }
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let ctl = spawn(Arc::clone(&c.meta), controller(policy))
        .await
        .unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Compaction every 5 s and the gateway's refresh every second.
    let side = {
        let (meta, stores, gw, stop) = (
            Arc::clone(&c.meta),
            (0..3).map(|i| c.node_store(i)).collect::<Vec<_>>(),
            c.backend.clone(),
            Arc::clone(&stop),
        );
        tokio::spawn(async move {
            let mut ticks = 0u64;
            let mut transitions = 0u64;
            let mut last = None;
            let mut min_headroom = u64::MAX;
            let mut normal_at: Vec<(u64, u8)> = Vec::new();
            let t0 = Instant::now();
            while !stop.load(Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                ticks += 1;
                if ticks.is_multiple_of(5) {
                    for s in &stores {
                        let s = Arc::clone(s);
                        let _ = tokio::task::spawn_blocking(move || {
                            for seg in s.compaction_candidates(0.3).unwrap_or_default() {
                                let _ = s.compact(seg);
                            }
                        })
                        .await;
                    }
                }
                if let Ok(rec) = admission::current(meta.as_ref()).await {
                    if let Some(t) = rec.as_ref().and_then(|r| r.tier("default")) {
                        if last != Some(t.state) {
                            transitions += 1;
                            last = Some(t.state);
                            normal_at.push((t0.elapsed().as_secs(), t.state));
                        }
                        min_headroom = min_headroom.min(
                            t.group_room
                                .iter()
                                .map(|g| g.room)
                                .min()
                                .unwrap_or(u64::MAX),
                        );
                    }
                    gw.cluster().admission.observe(rec, Instant::now());
                }
            }
            (transitions, min_headroom, normal_at)
        })
    };
    let data = Bytes::from(random_bytes(256 << 10, 3));
    let (hi, lo) = (Phase::default(), Phase::default());
    let t0 = Instant::now();
    drive(&c, HIGH, 120, &hi, &data).await;
    let drop_at = t0.elapsed().as_secs();
    drive(&c, LOW, 180, &lo, &data).await;
    stop.store(true, Relaxed);
    let (transitions, min_headroom, path) = side.await.unwrap();
    let back_to_normal = path
        .iter()
        .filter(|(t, s)| *t >= drop_at && *s == 0)
        .map(|(t, _)| t - drop_at)
        .next();
    let node_refusals: u64 = (0..3)
        .map(|i| c.node_service(i).admission_counts().1.iter().sum::<u64>())
        .sum();
    let stats = |p: &Phase, secs: f64| {
        let mut l = std::mem::take(&mut *p.lat.lock().unwrap());
        let mut g = std::mem::take(&mut *p.get_lat.lock().unwrap());
        serde_json::json!({
            "accepted_per_s": p.ok.load(Relaxed) as f64 / secs,
            "refused_per_s": p.refused.load(Relaxed) as f64 / secs,
            "gets_per_s": g.len() as f64 / secs,
            "put_ms": [pct(&mut l, 0.5), pct(&mut l, 0.95), pct(&mut l, 0.99)],
            "get_ms": [pct(&mut g, 0.5), pct(&mut g, 0.95), pct(&mut g, 0.99)],
        })
    };
    println!(
        "{}",
        serde_json::json!({
            "policy": label,
            "run": iteration,
            "high": stats(&hi, 120.0),
            "low": stats(&lo, 180.0),
            "low_target_puts_per_s": LOW as f64 * 0.9,
            "transitions": transitions,
            "state_path": path,
            "normal_after_drop_s": back_to_normal,
            "min_group_room_mib": min_headroom / MIB,
            "node_refusals": node_refusals,
        })
    );
    ctl.shutdown().await;
    c.shutdown().await;
}

fn scaled(responsive: bool) -> Policy {
    Policy {
        trend_window_ms: 60_000,
        recent_window_ms: if responsive { 10_000 } else { 0 },
        adaptive_rate: responsive,
        dwell_ms: 10_000,
        horizon_s: 300.0,
        group_min_shards: 4,
        ..Policy::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement"]
async fn responsiveness_before_and_after() {
    for i in 0..runs() {
        run("phase-3.6", scaled(false), i).await;
        run("phase-3.7", scaled(true), i).await;
    }
}
