//! Controller background passes at scale (Phase 4.0, ignored: a
//! measurement). In process: real storage-node services on loopback, the
//! in-memory metadata store, one controller with one-second intervals, and
//! `ARCA_BG_SCALE` = `nodes:groups,...` configurations (default
//! `4:10,8:100,16:1000,32:1000`). Each configuration writes a few objects,
//! runs the controller until every pass of interest ran three times (or a
//! time limit), and prints each task's last and longest pass duration and
//! the process's resident memory. A pass that does not finish three times
//! within the limit is reported as such: the result is that it cannot keep
//! up.
//!
//! Not FoundationDB: metadata latency is that of memory, so pass times
//! are a lower bound of the work per pass (RPCs, encoding, scans).

#![allow(clippy::too_many_lines, clippy::format_push_string)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use bytes::Bytes;

const TASKS: [&str; 6] = [
    "health",
    "repair",
    "reconcile",
    "gc",
    "reclaim",
    "rebalance",
];

fn rss_mib() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| {
                l.strip_prefix("VmRSS:")
                    .map(|v| v.trim().trim_end_matches(" kB").trim().to_string())
            })
        })
        .and_then(|v| v.parse::<f64>().ok())
        .map_or(0.0, |k| k / 1024.0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement"]
async fn controller_passes_at_scale() {
    let configs: Vec<(usize, usize)> = std::env::var("ARCA_BG_SCALE")
        .unwrap_or_else(|_| "4:10,8:100,16:1000,32:1000".into())
        .split(',')
        .filter_map(|c| {
            let (n, g) = c.split_once(':')?;
            Some((n.parse().ok()?, g.parse().ok()?))
        })
        .collect();
    let limit = Duration::from_secs(
        std::env::var("ARCA_BG_LIMIT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(600),
    );
    for (nodes, groups) in configs {
        let rss0 = rss_mib();
        let t = Instant::now();
        let c = TestCluster::start(ClusterOptions {
            groups,
            ..ClusterOptions::rs(nodes, 2, 1, 1 << 20, 0)
        })
        .await;
        let setup = t.elapsed();
        s3::create_bucket(&c.backend, "bg").await.unwrap();
        for i in 0..64u64 {
            s3::put(
                &c.backend,
                "bg",
                &format!("o{i}"),
                Bytes::from(random_bytes(64 << 10, i)),
                &PutOptions::default(),
            )
            .await
            .unwrap();
        }
        let mut cfg = ControllerConfig::new("ctl-scale");
        cfg.health_interval = Duration::from_secs(1);
        cfg.repair_interval = Duration::from_secs(1);
        cfg.reconcile_interval = Duration::from_secs(1);
        cfg.gc_interval = Duration::from_secs(1);
        cfg.reclaim_interval = Duration::from_secs(1);
        cfg.rebalance = Some(arca_lake_controller::rebalance::RebalancePolicy::default());
        cfg.rebalance_interval = Duration::from_secs(1);
        cfg.scrub.interval_ms = 0;
        let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
        let t = Instant::now();
        let mut done = false;
        while t.elapsed() < limit {
            let p = ctl.metrics.passes.lock().unwrap().clone();
            if TASKS.iter().all(|k| p.get(k).is_some_and(|v| v.0 >= 3)) {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let p = ctl.metrics.passes.lock().unwrap().clone();
        let mut line = format!(
            "BGSCALE nodes={nodes} groups={groups} setup_s={:.1} complete={done} waited_s={:.1} rss_mib_delta={:.0}",
            setup.as_secs_f64(),
            t.elapsed().as_secs_f64(),
            rss_mib() - rss0
        );
        for k in TASKS {
            let (n, last, max) = p.get(k).copied().unwrap_or((0, 0, 0));
            line.push_str(&format!(" {k}=passes:{n},last_ms:{last},max_ms:{max}"));
        }
        eprintln!("{line}");
        ctl.shutdown().await;
        c.shutdown().await;
    }
}
