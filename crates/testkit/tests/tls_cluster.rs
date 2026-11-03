//! A cluster with mutual TLS everywhere (ADR-0017): controllers work over
//! authenticated connections (GC, leader failover), fencing still refuses
//! an authenticated but stale caller, and certificates rotate under
//! traffic without losing an acknowledged write.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_metadata::ops::compat;
use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_protocol::tls::Role;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const BUCKET: &str = "tls";

async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        tls: true,
        ..ClusterOptions::rs(3, 2, 1, 1 << 20, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn ctl(c: &TestCluster, id: &str) -> ControllerConfig {
    let mut cfg = ControllerConfig::new(id);
    cfg.lease = Duration::from_millis(600);
    cfg.gc_interval = Duration::from_millis(150);
    cfg.gc_grace = Duration::ZERO;
    cfg.reconcile_interval = Duration::from_secs(3600);
    cfg.repair_interval = Duration::from_secs(3600);
    cfg.stripe_repair_interval = Duration::from_secs(3600);
    cfg.health_interval = Duration::from_millis(200);
    cfg.scrub.interval_ms = 0;
    cfg.tls = Some(c.pki.as_ref().unwrap().ctx(Role::Controller, id));
    cfg
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn controllers_work_over_mtls_and_fencing_still_refuses_stale_callers() {
    let c = cluster().await;
    for i in 0..4u64 {
        s3::put(
            &c.backend,
            BUCKET,
            &format!("k{i}"),
            Bytes::from(random_bytes(300 << 10, i)),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    let before = c.total_shards().await;
    let a = spawn(Arc::clone(&c.meta), ctl(&c, "ctl-a")).await.unwrap();
    let b = spawn(Arc::clone(&c.meta), ctl(&c, "ctl-b")).await.unwrap();
    wait_until("a leader", Duration::from_secs(5), async || {
        a.is_leader() || b.is_leader()
    })
    .await;
    // GC deletes shards on the nodes over mutual TLS (controller role).
    s3::delete(&c.backend, BUCKET, "k0").await.unwrap();
    wait_until(
        "GC by the first leader",
        Duration::from_secs(10),
        async || c.total_shards().await == before - 3,
    )
    .await;
    // Leader failover: the new leader's GC works the same way.
    let (old, new) = if a.is_leader() { (a, b) } else { (b, a) };
    old.shutdown().await;
    wait_until("the new leader", Duration::from_secs(5), async || {
        new.is_leader()
    })
    .await;
    s3::delete(&c.backend, BUCKET, "k1").await.unwrap();
    wait_until(
        "GC by the new leader",
        Duration::from_secs(10),
        async || c.total_shards().await == before - 6,
    )
    .await;
    // An authenticated controller identity holding the previous
    // incarnation is refused by the node after a restore mark; nodes adopt
    // the new incarnation from the leader's health pass.
    let group = c.groups().await.remove(0);
    let chunk = c.manifest(BUCKET, "k2").await.chunks[0].1.chunk_id;
    let old_inc = compat::cluster_state(c.meta.as_ref())
        .await
        .unwrap()
        .unwrap()
        .incarnation;
    let stale = NodeClients::new(Duration::from_secs(5)).with_tls(Some(
        c.pki.as_ref().unwrap().ctx(Role::Controller, "ctl-stale"),
    ));
    stale.set_incarnation(Some(old_inc));
    let fresh = compat::mark_restoring(c.meta.as_ref(), None, None, "admin", now_ms())
        .await
        .unwrap();
    // A controller restarted on the restored store (new incarnation) pushes
    // it to every node as its recovery pass starts.
    new.shutdown().await;
    let restarted = spawn(Arc::clone(&c.meta), ctl(&c, "ctl-c")).await.unwrap();
    let addr0 = c.nodes[c.node_index(&group.members[0]).unwrap()]
        .addr
        .to_string();
    wait_until(
        "nodes adopt the new incarnation",
        Duration::from_secs(10),
        async || {
            c.clients
                .health(&addr0)
                .await
                .is_ok_and(|h| h.incarnation.as_ref() == fresh.incarnation)
        },
    )
    .await;
    let e = stale
        .delete_shard(
            &addr0,
            ShardRef {
                group_id: group.id,
                chunk_id: chunk,
                shard_index: 0,
            },
            u64::MAX,
        )
        .await
        .unwrap_err();
    assert!(e.message().starts_with("incarnation rejected"), "{e}");
    // A gateway identity cannot move epochs even though it is authenticated.
    let gw = NodeClients::new(Duration::from_secs(5)).with_tls(c.gateway_tls());
    let e = gw
        .update_epoch(&addr0, group.id, group.epoch + 10)
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied, "{e}");
    restarted.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn certificates_rotate_under_traffic_without_losing_acknowledged_writes() {
    let c = cluster().await;
    let stop = Arc::new(AtomicBool::new(false));
    let traffic = {
        let backend = c.backend.clone();
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut acked = Vec::new();
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let key = format!("t{i}");
                let data = Bytes::from(random_bytes(200 << 10, 1000 + i));
                if s3::put(&backend, BUCKET, &key, data.clone(), &PutOptions::default())
                    .await
                    .is_ok()
                {
                    // Read back right away too: reads keep working.
                    assert_eq!(s3::get(&backend, BUCKET, &key).await.unwrap().0, data);
                    acked.push((key, data));
                }
            }
            acked
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Rotate every node's certificate and the gateway's, twice.
    for _ in 0..2 {
        for i in 0..c.nodes.len() {
            c.rotate_node_cert(i);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let gw = c.gateway_tls().unwrap();
        c.pki.as_ref().unwrap().write(
            &c.pki.as_ref().unwrap().dir_of(Role::Gateway, "gateway"),
            Role::Gateway,
            "gateway",
        );
        assert!(gw.reload().unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    stop.store(true, Ordering::Relaxed);
    let acked = traffic.await.unwrap();
    assert!(acked.len() >= 5, "traffic ran: {}", acked.len());
    for (key, data) in &acked {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    c.shutdown().await;
}
