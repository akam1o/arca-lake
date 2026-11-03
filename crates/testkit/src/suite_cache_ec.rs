//! Protected cache groups (RFC-006 §5.9): replicated or erasure-coded
//! cache copies, degraded hits, read-triggered and controller cache repair,
//! and the fallback to the durable tier when a copy is unreadable. Generic
//! over the metadata backend and the protection mode; instantiate with
//! [`crate::cache_ec_suite!`].

#![allow(
    clippy::too_many_lines,
    clippy::missing_panics_doc,
    clippy::many_single_char_names
)]

use std::time::Duration;

use arca_lake_common::chunk_id;
use arca_lake_controller::groups::CacheProtection;
use arca_lake_controller::repair::{self, RepairOptions};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::cache as meta_cache;
use arca_lake_metadata::records::{GroupMember, GroupRecord};
use arca_lake_protocol::client::ShardRef;
use arca_lake_tiering::cache::{CacheTierConfig, evict_tier};
use bytes::Bytes;

use crate::s3::{self, PutOptions};
use crate::suite::Starter;
use crate::{CACHE_TIER, ClusterOptions, TestCluster, now_ms, random_bytes};

const BUCKET: &str = "cache-ec";
const CHUNK: u64 = 256 << 10;

/// Replication with two copies.
#[must_use]
pub fn replication() -> CacheProtection {
    CacheProtection::Replication { replicas: 2 }
}

/// Reed–Solomon 2+1.
#[must_use]
pub fn erasure_coding() -> CacheProtection {
    CacheProtection::ErasureCoding {
        scheme: "rs-gf8-v1".into(),
        data_shards: 2,
        parity_shards: 1,
    }
}

fn options(protection: CacheProtection) -> ClusterOptions {
    ClusterOptions {
        groups: 1,
        cache_nodes: 3,
        cache_protection: protection,
        cache_groups: 1,
        ..ClusterOptions::rs(3, 2, 1, CHUNK, 0)
    }
}

async fn cluster<S: KvStore + 'static>(
    start: &impl Starter<S>,
    protection: CacheProtection,
) -> TestCluster<S> {
    let c = start(options(protection)).await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(chunks: u64, seed: u64) -> Bytes {
    Bytes::from(random_bytes(
        usize::try_from(chunks * CHUNK - 7).unwrap(),
        seed,
    ))
}

async fn cache_group<S: KvStore + 'static>(c: &TestCluster<S>) -> GroupRecord {
    c.groups()
        .await
        .into_iter()
        .find(GroupRecord::is_cache)
        .unwrap()
}

/// Node index of every member, in shard order.
fn member_nodes<S: KvStore + 'static>(c: &TestCluster<S>, g: &GroupRecord) -> Vec<usize> {
    g.members.iter().map(|m| c.node_index(m).unwrap()).collect()
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn put_and_cache<S: KvStore + 'static>(
    c: &TestCluster<S>,
    key: &str,
    chunks: u64,
    seed: u64,
) -> Bytes {
    let data = payload(chunks, seed);
    s3::put(
        &c.backend,
        BUCKET,
        key,
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    s3::get(&c.backend, BUCKET, key).await.unwrap();
    c.wait_cached(
        BUCKET,
        key,
        usize::try_from(chunks).unwrap(),
        Duration::from_secs(10),
    )
    .await;
    data
}

/// Case `coded_hit_reads_shards_and_leaves_durable_alone`.
pub async fn coded_hit_reads_shards_and_leaves_durable_alone<S: KvStore + 'static>(
    start: impl Starter<S>,
    protection: CacheProtection,
) {
    let c = cluster(&start, protection.clone()).await;
    let g = cache_group(&c).await;
    let (_, k, m) = protection.spec().unwrap();
    assert_eq!(g.members.len(), usize::from(k + m));
    let hosts: std::collections::BTreeSet<u64> = g.members.iter().map(|x| x.node_id).collect();
    assert_eq!(hosts.len(), g.members.len(), "members on distinct hosts");

    let data = payload(2, 1);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let durable_before = c.total_shards().await;
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    assert_eq!(
        c.total_shards().await,
        durable_before,
        "durable placement untouched"
    );
    for n in member_nodes(&c, &g) {
        assert_eq!(c.shard_count(n, g.id), 2, "one shard per chunk per member");
    }
    let placements = c.cache_placements(BUCKET, "o").await;
    let usage = meta_cache::usage(c.meta.as_ref(), CACHE_TIER)
        .await
        .unwrap();
    assert_eq!(usage, placements.iter().map(|(_, p)| p.bytes).sum::<u64>());
    assert!(
        usage > data.len() as u64,
        "protection stores more than the data"
    );

    let before = c.backend.cache_stats();
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    let s = c.backend.cache_stats();
    assert_eq!(s.hits - before.hits, 2, "{s:?}");
    assert_eq!(s.degraded_hits, 0, "{s:?}");
    c.shutdown().await;
}

/// Case `shard_loss_is_a_degraded_hit_repaired_on_read`.
pub async fn shard_loss_is_a_degraded_hit_repaired_on_read<S: KvStore + 'static>(
    start: impl Starter<S>,
    protection: CacheProtection,
) {
    let c = cluster(&start, protection).await;
    let g = cache_group(&c).await;
    let nodes = member_nodes(&c, &g);
    let data = put_and_cache(&c, "o", 2, 2).await;
    let generation = c.manifest(BUCKET, "o").await.pointer.generation;

    // Lose shard 0 of chunk 0 on member 0.
    c.clients
        .delete_shard(
            &c.nodes[nodes[0]].addr.to_string(),
            ShardRef {
                group_id: g.id,
                chunk_id: chunk_id(&generation, 0),
                shard_index: 0,
            },
            g.epoch,
        )
        .await
        .unwrap();
    assert_eq!(c.shard_count(nodes[0], g.id), 1);

    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data, "degraded hit serves the right bytes");
    let s = c.backend.cache_stats();
    assert_eq!((s.degraded_hits, s.fallbacks), (1, 0), "{s:?}");
    wait_until(
        "read-triggered repair",
        Duration::from_secs(10),
        async || c.backend.cache_stats().shards_repaired >= 1,
    )
    .await;
    assert_eq!(c.shard_count(nodes[0], g.id), 2, "shard rewritten");
    assert_eq!(
        c.cache_placements(BUCKET, "o").await.len(),
        2,
        "record untouched"
    );

    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    let s = c.backend.cache_stats();
    assert_eq!(s.degraded_hits, 1, "clean hit after repair ({s:?})");
    c.shutdown().await;
}

/// Case `member_down_degrades_then_unreadable_falls_back`.
pub async fn member_down_degrades_then_unreadable_falls_back<S: KvStore + 'static>(
    start: impl Starter<S>,
    protection: CacheProtection,
) {
    let mut c = cluster(&start, protection).await;
    let g = cache_group(&c).await;
    let nodes = member_nodes(&c, &g);
    let data = put_and_cache(&c, "o", 2, 3).await;

    // One member down: still a hit, degraded; repairs cannot reach it.
    c.stop_node(nodes[0]).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    let s = c.backend.cache_stats();
    assert_eq!((s.degraded_hits, s.fallbacks), (2, 0), "{s:?}");
    wait_until(
        "repair attempts settle",
        Duration::from_secs(10),
        async || {
            let s = c.backend.cache_stats();
            s.shard_repair_failures + s.shard_repairs_skipped >= 2
        },
    )
    .await;
    assert_eq!(
        c.cache_placements(BUCKET, "o").await.len(),
        2,
        "records stay"
    );

    // Fewer than k members left: unreadable → durable fallback, invalidate.
    for &n in &nodes[1..nodes.len() - 1] {
        c.stop_node(n).await;
    }
    let last = nodes[nodes.len() - 1];
    if nodes.len() > 1 {
        // Keep exactly one member alive: not enough for k >= ... unless k == 1
        // with a surviving copy, so stop that one too.
        c.stop_node(last).await;
    }
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data, "served from the durable tier");
    let s = c.backend.cache_stats();
    assert_eq!(s.fallbacks, 2, "{s:?}");
    wait_until("invalidation", Duration::from_secs(10), async || {
        c.cache_placements(BUCKET, "o").await.is_empty()
    })
    .await;
    assert_eq!(
        meta_cache::usage(c.meta.as_ref(), CACHE_TIER)
            .await
            .unwrap(),
        0
    );
    assert_eq!(c.total_shards().await, 6, "durable shards intact");
    c.shutdown().await;
}

/// Case `corrupt_shards_are_missing_and_too_many_fall_back`.
pub async fn corrupt_shards_are_missing_and_too_many_fall_back<S: KvStore + 'static>(
    start: impl Starter<S>,
    protection: CacheProtection,
) {
    let c = cluster(&start, protection).await;
    let g = cache_group(&c).await;
    let nodes = member_nodes(&c, &g);
    let data = put_and_cache(&c, "o", 2, 4).await;
    let generation = c.manifest(BUCKET, "o").await.pointer.generation;

    // One corrupt shard (chunk 0, member 0): degraded hit, repaired.
    assert_eq!(
        c.corrupt_chunk_on_node(nodes[0], g.id, chunk_id(&generation, 0)),
        1
    );
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data, "corrupt shard never served");
    let s = c.backend.cache_stats();
    assert_eq!((s.degraded_hits, s.fallbacks), (1, 0), "{s:?}");
    wait_until("repair", Duration::from_secs(10), async || {
        c.backend.cache_stats().shards_repaired >= 1
    })
    .await;
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(
        c.backend.cache_stats().degraded_hits,
        1,
        "clean after repair"
    );

    // Corrupt chunk 1 on every member: unreadable, fallback, invalidated.
    for &n in &nodes {
        assert_eq!(
            c.corrupt_chunk_on_node(n, g.id, chunk_id(&generation, 1)),
            1
        );
    }
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    let s = c.backend.cache_stats();
    assert_eq!(s.fallbacks, 1, "{s:?}");
    wait_until("invalidation", Duration::from_secs(10), async || {
        c.cache_placements(BUCKET, "o").await.len() == 1
    })
    .await;
    // Read-through repopulates chunk 1.
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    c.shutdown().await;
}

/// Case `population_with_a_member_down_records_nothing`.
pub async fn population_with_a_member_down_records_nothing<S: KvStore + 'static>(
    start: impl Starter<S>,
    protection: CacheProtection,
) {
    let mut c = cluster(&start, protection).await;
    let g = cache_group(&c).await;
    let nodes = member_nodes(&c, &g);
    let data = payload(2, 5);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let lost = nodes[nodes.len() - 1];
    c.stop_node(lost).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    wait_until("population failures", Duration::from_secs(10), async || {
        c.backend.cache_stats().population_failures >= 2
    })
    .await;
    assert!(
        c.cache_placements(BUCKET, "o").await.is_empty(),
        "partial copy never recorded"
    );
    for &n in &nodes {
        if c.node_running(n) {
            assert_eq!(c.shard_count(n, g.id), 0, "partial shards deleted");
        }
    }
    assert_eq!(
        meta_cache::usage(c.meta.as_ref(), CACHE_TIER)
            .await
            .unwrap(),
        0
    );

    c.start_node(lost).await;
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    c.shutdown().await;
}

/// Case `eviction_with_a_member_down_leaves_orphans_for_reconciliation`.
pub async fn eviction_with_a_member_down_leaves_orphans_for_reconciliation<S: KvStore + 'static>(
    start: impl Starter<S>,
    protection: CacheProtection,
) {
    let mut c = cluster(&start, protection).await;
    let g = cache_group(&c).await;
    let nodes = member_nodes(&c, &g);
    let data = put_and_cache(&c, "o", 2, 6).await;
    let usage = meta_cache::usage(c.meta.as_ref(), CACHE_TIER)
        .await
        .unwrap();
    c.stop_node(nodes[1 % nodes.len()]).await;

    let cfg = CacheTierConfig {
        capacity_bytes: usage,
        high_watermark: 0.1,
        low_watermark: 0.0,
        ..ClusterOptions::default_cache()
    };
    let map = c.backend.map();
    let addr_of = |node_id: u64| map.node_addr(node_id).map(str::to_string);
    let r = evict_tier(c.meta.as_ref(), &c.clients, &cfg, &map.groups, &addr_of)
        .await
        .unwrap();
    assert_eq!(r.evicted, 2, "{r:?}");
    assert!(r.delete_failures >= 2, "unreachable member: {r:?}");
    assert_eq!(r.usage_after, 0);
    assert_eq!(c.cache_placements(BUCKET, "o").await.len(), 0);
    for &n in &nodes {
        if c.node_running(n) {
            assert_eq!(c.shard_count(n, g.id), 0);
        }
    }
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);

    // The dead member comes back with its two orphan shards (the GET above
    // could not populate: one member was unreachable, so nothing was
    // recorded).
    c.start_node(nodes[1 % nodes.len()]).await;
    assert_eq!(c.shard_count(nodes[1 % nodes.len()], g.id), 2);
    // A read now repopulates both chunks over the stale shards (same chunk
    // IDs); reconciliation then finds every shard referenced.
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    let deleted = c.reconcile_cache().await;
    assert!(deleted <= 2, "{deleted}");
    assert_eq!(c.reconcile_cache().await, 0, "idempotent");
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.shutdown().await;
}

/// Case `controller_cache_repair_rebuilds_a_lost_member_within_budget`.
pub async fn controller_cache_repair_rebuilds_a_lost_member_within_budget<S: KvStore + 'static>(
    start: impl Starter<S>,
    protection: CacheProtection,
) {
    let mut c = cluster(&start, protection).await;
    let g = cache_group(&c).await;
    let nodes = member_nodes(&c, &g);
    let a = put_and_cache(&c, "a", 2, 7).await;
    let b = put_and_cache(&c, "b", 2, 8).await;
    let spare = c.add_cache_node().await;
    let replacement = GroupMember {
        node_id: c.nodes[spare].id,
        device_id: 0,
    };
    c.stop_node(nodes[0]).await;

    let opts = RepairOptions {
        page_size: 0,
        verify_all_members: false,
        max_chunks: 1,
        ..RepairOptions::default()
    };
    let mut passes = 0;
    let mut reconstructed = 0;
    loop {
        passes += 1;
        let report = repair::replace_member_with(
            c.meta.as_ref(),
            &c.clients,
            g.id,
            0,
            replacement,
            &opts,
            async |_| Ok(()),
        )
        .await
        .unwrap();
        reconstructed += report.reconstructed;
        if report.complete {
            break;
        }
        assert!(
            report.budget_exhausted || report.reconstructed == 0,
            "{report:?}"
        );
        assert!(report.reconstructed <= 1, "budget respected: {report:?}");
        assert!(passes < 10, "no progress: {report:?}");
    }
    assert_eq!(reconstructed, 4, "every cached chunk rebuilt");
    assert!(passes >= 4, "one chunk per pass: {passes}");
    let after = c.groups().await.into_iter().find(|x| x.id == g.id).unwrap();
    assert_eq!(after.members[0], replacement);
    assert_eq!(after.epoch, g.epoch + 1);
    assert_eq!(c.shard_count(spare, g.id), 4);

    // Readers see clean hits from the new membership.
    c.backend.refresh_map().await.unwrap();
    let before = c.backend.cache_stats();
    assert_eq!(s3::get(&c.backend, BUCKET, "a").await.unwrap().0, a);
    assert_eq!(s3::get(&c.backend, BUCKET, "b").await.unwrap().0, b);
    let s = c.backend.cache_stats();
    assert_eq!(s.hits - before.hits, 4, "{s:?}");
    assert_eq!(s.degraded_hits - before.degraded_hits, 0, "{s:?}");
    assert_eq!(s.fallbacks - before.fallbacks, 0, "{s:?}");
    c.shutdown().await;
}

/// Case `epoch_change_fences_stale_population_and_keeps_placements`.
pub async fn epoch_change_fences_stale_population_and_keeps_placements<S: KvStore + 'static>(
    start: impl Starter<S>,
    protection: CacheProtection,
) {
    let c = cluster(&start, protection).await;
    let g = cache_group(&c).await;
    let nodes = member_nodes(&c, &g);
    let a = put_and_cache(&c, "a", 2, 9).await;
    for &n in &nodes {
        c.clients
            .update_epoch(&c.nodes[n].addr.to_string(), g.id, g.epoch + 1)
            .await
            .unwrap();
    }
    // Existing placements are read without an epoch: still hits.
    let before = c.backend.cache_stats();
    assert_eq!(s3::get(&c.backend, BUCKET, "a").await.unwrap().0, a);
    let s = c.backend.cache_stats();
    assert_eq!(s.hits - before.hits, 2, "{s:?}");

    // A population with the gateway's stale epoch is rejected by the node
    // and never recorded; the GET is unaffected.
    let b = payload(1, 10);
    s3::put(&c.backend, BUCKET, "b", b.clone(), &PutOptions::default())
        .await
        .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "b").await.unwrap().0, b);
    wait_until("population failure", Duration::from_secs(10), async || {
        c.backend.cache_stats().population_failures >= 1
    })
    .await;
    assert_eq!(c.cache_placements(BUCKET, "b").await.len(), 0);
    for &n in &nodes {
        assert_eq!(c.shard_count(n, g.id), 2, "no partial shard of b");
    }
    assert_eq!(s3::get(&c.backend, BUCKET, "b").await.unwrap().0, b);
    c.shutdown().await;
}

/// Case `durable_repair_and_gc_leave_the_coded_cache_alone`.
pub async fn durable_repair_and_gc_leave_the_coded_cache_alone<S: KvStore + 'static>(
    start: impl Starter<S>,
    protection: CacheProtection,
) {
    let mut c = cluster(&start, protection).await;
    let data = put_and_cache(&c, "o", 2, 11).await;
    let durable = c
        .groups()
        .await
        .into_iter()
        .find(|x| !x.is_cache())
        .unwrap();
    let lost = c.node_index(&durable.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;
    let report = c.repair(durable.id, 0, new_node).await;
    assert!(report.complete);
    assert_eq!(
        c.cache_placements(BUCKET, "o").await.len(),
        2,
        "repair ignores the cache"
    );
    let before = c.backend.cache_stats();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    let s = c.backend.cache_stats();
    assert_eq!(s.hits - before.hits, 2, "{s:?}");

    let r = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(r.collected, 0);
    assert_eq!(
        c.reconcile(now_ms() + 3_600_000, 0).await + c.reconcile_cache().await,
        0
    );

    // Overwrite: GC deletes every shard of the old generation's copies.
    let v2 = payload(2, 12);
    s3::put(&c.backend, BUCKET, "o", v2.clone(), &PutOptions::default())
        .await
        .unwrap();
    let r = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(r.collected, 1);
    let g = cache_group(&c).await;
    for n in member_nodes(&c, &g) {
        assert_eq!(c.shard_count(n, g.id), 0, "old copies gone on every member");
    }
    assert_eq!(
        meta_cache::usage(c.meta.as_ref(), CACHE_TIER)
            .await
            .unwrap(),
        0
    );
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, v2);
    c.shutdown().await;
}

/// Instantiate every protected-cache case for one backend and protection
/// mode inside a module named `$module`.
#[macro_export]
macro_rules! cache_ec_suite {
    ($module:ident, $start:expr, $protection:expr) => {
        mod $module {
            $crate::cache_ec_suite!(@each $start, $protection;
                coded_hit_reads_shards_and_leaves_durable_alone,
                shard_loss_is_a_degraded_hit_repaired_on_read,
                member_down_degrades_then_unreadable_falls_back,
                corrupt_shards_are_missing_and_too_many_fall_back,
                population_with_a_member_down_records_nothing,
                eviction_with_a_member_down_leaves_orphans_for_reconciliation,
                controller_cache_repair_rebuilds_a_lost_member_within_budget,
                epoch_change_fences_stale_population_and_keeps_placements,
                durable_repair_and_gc_leave_the_coded_cache_alone,
            );
        }
    };
    (@each $start:expr, $protection:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(start) = $start else {
                    eprintln!("{}: backend not available; skipping", stringify!($name));
                    return;
                };
                $crate::suite_cache_ec::$name(start, $protection).await;
            }
        )*
    };
}
