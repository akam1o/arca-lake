//! Persisted cursors of reconciliation and tiering (Phase 3.14). Before,
//! each pass swept everything from the start: a large cluster's pass never
//! reached its end before a leader change, the start was swept again and
//! again, and one generation whose records could not be read stopped every
//! tiering pass there.
//!
//! Checked with the controller process: a reconciliation pass handles at
//! most its budget of `(group, node)` pairs; after a leader failover the
//! next leader resumes after the persisted cursor (its cursor never goes
//! back before the sweep wraps); a stale cursor (a group gone) resumes
//! after it; every pair is reached and the sweep wraps. Tiering: bounded
//! passes resume at the cursor, a generation whose access record cannot be
//! decoded is passed over and counted, the sweep wraps.
//! The scheduling itself under churn, failures and failovers is
//! `arca_lake_controller::sweep`'s deterministic simulator.

#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_metadata::ops::bg_cursor;
use arca_lake_metadata::{Txn, keys, transact};
use arca_lake_protocol::client::ShardRef;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;

fn config(id: &str) -> ControllerConfig {
    let mut c = ControllerConfig::new(id);
    c.lease = Duration::from_secs(2);
    c.health_interval = Duration::from_millis(100);
    c.repair_interval = Duration::from_secs(3600);
    c.gc_interval = Duration::from_secs(3600);
    c.reconcile_interval = Duration::from_millis(150);
    c.min_orphan_age = Duration::ZERO;
    c.reconcile_pairs_per_pass = 4;
    c
}

/// An orphan shard of `group` on node `i` (no metadata names it).
async fn orphan(c: &TestCluster, i: usize, group: u64, epoch: u64, seed: u8) -> ShardRef {
    let generation = *uuid::Uuid::now_v7().as_bytes();
    let shard = ShardRef {
        group_id: group,
        chunk_id: arca_lake_common::chunk_id(&generation, u32::from(seed)),
        shard_index: 0,
    };
    c.clients
        .put_shard_as(
            &c.nodes[i].addr.to_string(),
            shard,
            epoch,
            Bytes::from(vec![seed; 4096]),
            256 << 10,
            arca_lake_protocol::storage::v1::WriteClass::Client,
        )
        .await
        .unwrap();
    shard
}

fn present(c: &TestCluster, i: usize, s: &ShardRef) -> bool {
    c.shard_keys(i, s.group_id)
        .iter()
        .any(|k| k.chunk_id == s.chunk_id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconciliation_resumes_after_its_cursor_across_a_failover() {
    let c = TestCluster::start(ClusterOptions {
        groups: 10,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    let groups = c.groups().await;
    let node_of = |id: u64| c.nodes.iter().position(|n| n.id == id).unwrap();
    // One orphan per (group, node) pair: 30 pairs, budget 4 a pass.
    let mut first = Vec::new();
    for g in &groups {
        for m in &g.members {
            let i = node_of(m.node_id);
            first.push((g.id, m.node_id, i, orphan(&c, i, g.id, g.epoch, 1).await));
        }
    }
    let ctl = spawn(Arc::clone(&c.meta), config("ctl-a")).await.unwrap();
    // Stop the first leader in the middle of the sweep.
    let t = Instant::now();
    let cursor = loop {
        let cur = bg_cursor::get(c.meta.as_ref(), bg_cursor::RECONCILE)
            .await
            .unwrap();
        assert!(cur.last_pass_items <= 4, "a pass over its budget: {cur:?}");
        if let Some(p) = bg_cursor::parse_pair(&cur.after)
            && p.0 >= groups[3].id
        {
            break p;
        }
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "no progress: {cur:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    ctl.shutdown().await;
    let cursor = bg_cursor::parse_pair(
        &bg_cursor::get(c.meta.as_ref(), bg_cursor::RECONCILE)
            .await
            .unwrap()
            .after,
    )
    .unwrap_or(cursor);
    // New orphans on pairs already swept, and the old ones after the
    // cursor still waiting.
    let mut behind = Vec::new();
    for &(g, n, i, _) in first.iter().filter(|p| (p.0, p.1) <= cursor) {
        let e = groups.iter().find(|x| x.id == g).unwrap().epoch;
        behind.push((g, n, i, orphan(&c, i, g, e, 2).await));
    }
    let ahead: Vec<_> = first.iter().filter(|p| (p.0, p.1) > cursor).collect();
    assert!(!ahead.is_empty() && !behind.is_empty());
    let sweeps_before = bg_cursor::get(c.meta.as_ref(), bg_cursor::RECONCILE)
        .await
        .unwrap()
        .sweeps;
    let ctl = spawn(Arc::clone(&c.meta), config("ctl-b")).await.unwrap();
    // Until the sweep wraps, the new leader's cursor only moves on from
    // where the first one stopped: it does not start again at the
    // beginning (pairs behind it wait for the next sweep).
    let t = Instant::now();
    loop {
        let cur = bg_cursor::get(c.meta.as_ref(), bg_cursor::RECONCILE)
            .await
            .unwrap();
        assert!(cur.last_pass_items <= 4, "a pass over its budget: {cur:?}");
        if cur.sweeps > sweeps_before {
            break;
        }
        if let Some(p) = bg_cursor::parse_pair(&cur.after) {
            assert!(
                p >= cursor,
                "swept again from the start after the failover: {p:?} < {cursor:?}"
            );
        }
        assert!(
            t.elapsed() < Duration::from_secs(60),
            "the sweep never wrapped: {cur:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Every pair is reached (a delete the nodes refused while the new
    // leader's fence was not open yet is retried at the next sweep).
    let t = Instant::now();
    loop {
        let left = ahead
            .iter()
            .copied()
            .chain(behind.iter())
            .filter(|p| present(&c, p.2, &p.3))
            .count();
        if left == 0 {
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(60),
            "{left} orphans never reconciled"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let cur = bg_cursor::get(c.meta.as_ref(), bg_cursor::RECONCILE)
        .await
        .unwrap();
    assert!(cur.sweeps >= 1, "{cur:?}");
    // A stale cursor (a group that does not exist) resumes after it.
    ctl.shutdown().await;
    let last = groups.last().unwrap();
    let mut stale = cur.clone();
    stale.after = bg_cursor::pair_position(last.id + 1000, 0);
    bg_cursor::put(c.meta.as_ref(), bg_cursor::RECONCILE, &stale)
        .await
        .unwrap();
    let i = node_of(last.members[0].node_id);
    let o = orphan(&c, i, last.id, last.epoch, 3).await;
    let ctl = spawn(Arc::clone(&c.meta), config("ctl-c")).await.unwrap();
    let t = Instant::now();
    while present(&c, i, &o) {
        assert!(
            t.elapsed() < Duration::from_secs(60),
            "stale cursor: never swept"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ctl.shutdown().await;
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tiering_passes_are_bounded_resume_and_pass_over_an_unreadable_generation() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, "tier").await.unwrap();
    for i in 0..25u64 {
        let d = Bytes::from(random_bytes(4096, 50 + i));
        s3::put(
            &c.backend,
            "tier",
            &format!("t{i}"),
            d,
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    let gens: Vec<_> =
        arca_lake_metadata::ops::object::list_generations(c.meta.as_ref(), None, 1000)
            .await
            .unwrap();
    assert_eq!(gens.len(), 25);
    // One generation's access record is garbage.
    let bad = gens[12].0;
    transact(c.meta.as_ref(), bad, |tx, g| {
        Box::pin(async move {
            tx.set(&keys::access(g), &[0xff, 0xff, 0xff]);
            Ok(())
        })
    })
    .await
    .unwrap();
    // A policy that moves nothing (one tier): every generation examined.
    let policy = arca_lake_controller::tiering::TieringPolicy {
        tiers: vec!["default".into()],
        promote_reads: u64::MAX,
        promote_window: Duration::from_secs(1),
        demote_after: Duration::from_secs(1 << 40),
    };
    let run = async || {
        arca_lake_controller::tiering::run_once_bounded(
            c.meta.as_ref(),
            &c.clients,
            &policy,
            arca_lake_testkit::now_ms(),
            10,
        )
        .await
        .unwrap()
    };
    let mut examined = BTreeSet::new();
    let r1 = run().await;
    let r2 = run().await;
    let r3 = run().await;
    for r in [&r1, &r2, &r3] {
        assert!(r.examined <= 10, "{r:?}");
    }
    assert!(
        !r1.reached_end && !r2.reached_end && r3.reached_end,
        "{r1:?} {r2:?} {r3:?}"
    );
    assert_eq!(r1.examined + r2.examined + r3.examined, 25);
    assert_eq!(
        r1.errors + r2.errors + r3.errors,
        1,
        "the bad generation counted once"
    );
    examined.extend(r1.last);
    examined.extend(r2.last);
    assert_eq!(examined.len(), 2, "each pass resumed after the last");
    let cur = bg_cursor::get(c.meta.as_ref(), bg_cursor::TIERING)
        .await
        .unwrap();
    assert_eq!(cur.sweeps, 1);
    assert_eq!(cur.after, Vec::<u8>::new());
    // The next sweep starts at the beginning again.
    let r4 = run().await;
    assert_eq!(r4.examined, 10);
    c.shutdown().await;
}

/// Review (Phase 3.14): a pair whose metadata cannot be decoded (here a
/// garbage migration intent for one of its shards) is passed over and
/// counted; the sweep goes on to the other pairs instead of stopping at it
/// every pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pair_with_undecodable_metadata_does_not_stop_reconciliation() {
    let c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, "bad").await.unwrap();
    s3::put(
        &c.backend,
        "bad",
        "x",
        Bytes::from(random_bytes(4096, 9)),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let m = c.manifest("bad", "x").await;
    let placed = &m.chunks[0].1;
    let (bad_group, good_group) = {
        let gs = c.groups().await;
        let bad = gs
            .iter()
            .find(|g| g.id == placed.placement.group_id)
            .unwrap()
            .clone();
        let good = gs.into_iter().find(|g| g.id != bad.id).unwrap();
        (bad, good)
    };
    let (prefix, _) = arca_lake_common::split_chunk_id(&placed.chunk_id);
    let mut generation = [0u8; 16];
    generation[..12].copy_from_slice(&prefix);
    let stray = arca_lake_common::chunk_id(&generation, 99);
    let i = c
        .nodes
        .iter()
        .position(|n| n.id == bad_group.members[0].node_id)
        .unwrap();
    c.clients
        .put_shard_as(
            &c.nodes[i].addr.to_string(),
            ShardRef {
                group_id: bad_group.id,
                chunk_id: stray,
                shard_index: 0,
            },
            bad_group.epoch,
            Bytes::from(vec![1; 4096]),
            256 << 10,
            arca_lake_protocol::storage::v1::WriteClass::Client,
        )
        .await
        .unwrap();
    transact(c.meta.as_ref(), stray, |tx, cid| {
        Box::pin(async move {
            tx.set(&keys::migration(cid), &[0xff, 0xfe]);
            Ok(())
        })
    })
    .await
    .unwrap();
    let j = c
        .nodes
        .iter()
        .position(|n| n.id == good_group.members[0].node_id)
        .unwrap();
    let o = orphan(&c, j, good_group.id, good_group.epoch, 7).await;
    let mut cfg = config("ctl-bad");
    cfg.reconcile_pairs_per_pass = 1;
    let ctl = spawn(Arc::clone(&c.meta), cfg).await.unwrap();
    let t = Instant::now();
    while present(&c, j, &o) {
        assert!(
            t.elapsed() < Duration::from_secs(60),
            "reconciliation stopped at the undecodable pair"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let t = Instant::now();
    while ctl
        .metrics
        .reconcile_errors_total
        .load(std::sync::atomic::Ordering::Relaxed)
        == 0
    {
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "the failing pair not counted"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ctl.shutdown().await;
    c.shutdown().await;
}
