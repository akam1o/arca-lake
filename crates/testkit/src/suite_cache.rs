//! Read-through cache cases (RFC-006 §5.3–5.7), generic over the metadata
//! backend. Instantiate with [`crate::cache_suite!`].

#![allow(clippy::too_many_lines, clippy::missing_panics_doc)]

use std::time::Duration;

use arca_lake_common::chunk_id;
use arca_lake_metadata::ops::cache as meta_cache;
use arca_lake_metadata::{KvStore, Txn, keys};
use arca_lake_protocol::client::ShardRef;
use arca_lake_tiering::cache::{CacheTierConfig, evict_tier};
use bytes::Bytes;

use crate::s3::{self, PutOptions};
use crate::suite::Starter;
use crate::{CACHE_TIER, ClusterOptions, TestCluster, now_ms, random_bytes};

const BUCKET: &str = "cache";
/// Chunk size: small so that objects have several chunks cheaply.
const CHUNK: u64 = 256 << 10;
const CHUNK_USIZE: usize = 256 << 10;

fn options() -> ClusterOptions {
    ClusterOptions {
        groups: 1,
        cache_nodes: 2,
        ..ClusterOptions::rs(3, 2, 1, CHUNK, 0)
    }
}

async fn cluster<S: KvStore + 'static>(start: &impl Starter<S>) -> TestCluster<S> {
    let c = start(options()).await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(chunks: u64, seed: u64) -> Bytes {
    Bytes::from(random_bytes(
        usize::try_from(chunks * CHUNK - 7).unwrap(),
        seed,
    ))
}

/// Shards on all running cache nodes.
fn cache_copies<S: KvStore + 'static>(
    c: &TestCluster<S>,
    groups: &[arca_lake_metadata::records::GroupRecord],
) -> usize {
    c.cache_node_indexes()
        .into_iter()
        .filter(|&i| c.node_running(i))
        .map(|i| {
            groups
                .iter()
                .filter(|g| g.is_cache())
                .map(|g| c.shard_count(i, g.id))
                .sum::<usize>()
        })
        .sum()
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// E2E case `miss_then_hit_serves_from_cache`.
pub async fn miss_then_hit_serves_from_cache<S: KvStore + 'static>(start: impl Starter<S>) {
    let c = cluster(&start).await;
    let data = payload(3, 1);
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

    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    let s = c.backend.cache_stats();
    assert_eq!((s.hits, s.misses), (0, 3), "{s:?}");
    c.wait_cached(BUCKET, "o", 3, Duration::from_secs(10)).await;
    let groups = c.groups().await;
    assert_eq!(
        cache_copies(&c, &groups),
        3,
        "one copy per chunk on the cache tier"
    );
    assert_eq!(
        c.total_shards().await,
        durable_before,
        "durable placement untouched"
    );
    assert_eq!(
        meta_cache::usage(c.meta.as_ref(), CACHE_TIER)
            .await
            .unwrap(),
        data.len() as u64
    );

    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    let s = c.backend.cache_stats();
    assert_eq!((s.hits, s.misses, s.fallbacks), (3, 3, 0), "{s:?}");
    for (_, p) in c.cache_placements(BUCKET, "o").await {
        assert_eq!(p.tier, CACHE_TIER);
        assert!(!p.pinned);
    }
    c.shutdown().await;
}

/// E2E case `partial_hits_and_range_gets`.
pub async fn partial_hits_and_range_gets<S: KvStore + 'static>(start: impl Starter<S>) {
    let c = cluster(&start).await;
    let data = payload(3, 2);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    // A range inside chunk 1 populates only chunk 1.
    let r = s3::get_range(&c.backend, BUCKET, "o", CHUNK + 10, CHUNK + 100)
        .await
        .unwrap();
    assert_eq!(&r[..], &data[(CHUNK_USIZE + 10)..=(CHUNK_USIZE + 100)]);
    c.wait_cached(BUCKET, "o", 1, Duration::from_secs(10)).await;
    assert_eq!(c.cache_placements(BUCKET, "o").await[0].0, 1);

    // A full read: chunk 1 hits, chunks 0 and 2 miss and get populated.
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    let s = c.backend.cache_stats();
    assert_eq!((s.hits, s.misses), (1, 3), "{s:?}");
    c.wait_cached(BUCKET, "o", 3, Duration::from_secs(10)).await;

    // A range across chunks 1 and 2: two hits, byte-exact.
    let r = s3::get_range(&c.backend, BUCKET, "o", 2 * CHUNK - 5, 2 * CHUNK + 5)
        .await
        .unwrap();
    assert_eq!(&r[..], &data[(2 * CHUNK_USIZE - 5)..=(2 * CHUNK_USIZE + 5)]);
    let s = c.backend.cache_stats();
    assert_eq!(s.hits, 3, "{s:?}");
    c.shutdown().await;
}

/// E2E case `concurrent_gets_populate_each_chunk_once`.
pub async fn concurrent_gets_populate_each_chunk_once<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let c = cluster(&start).await;
    let data = payload(2, 3);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let backend = c.backend.clone();
        tasks.push(tokio::spawn(async move {
            s3::get(&backend, BUCKET, "o").await.unwrap().0
        }));
    }
    for t in tasks {
        assert_eq!(t.await.unwrap(), data);
    }
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    // Give in-flight populations time to finish, then check that every
    // chunk has exactly one copy and was recorded once.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let groups = c.groups().await;
    assert_eq!(cache_copies(&c, &groups), 2);
    let s = c.backend.cache_stats();
    assert_eq!(
        s.populated, 2,
        "stampede: each chunk populated once ({s:?})"
    );
    assert_eq!(c.cache_placements(BUCKET, "o").await.len(), 2);
    c.shutdown().await;
}

/// E2E case `population_failure_never_fails_the_get`.
pub async fn population_failure_never_fails_the_get<S: KvStore + 'static>(start: impl Starter<S>) {
    let mut c = cluster(&start).await;
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
    for i in c.cache_node_indexes() {
        c.stop_node(i).await;
    }
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data, "served from durable placement");
    wait_until("population failures", Duration::from_secs(10), async || {
        c.backend.cache_stats().population_failures >= 2
    })
    .await;
    assert!(
        c.cache_placements(BUCKET, "o").await.is_empty(),
        "no record for a failed copy"
    );
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    c.shutdown().await;
}

/// E2E case `cache_node_failure_falls_back_and_invalidates`.
pub async fn cache_node_failure_falls_back_and_invalidates<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let mut c = cluster(&start).await;
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
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    for i in c.cache_node_indexes() {
        c.stop_node(i).await;
    }
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data, "records exist, copies unreachable: fallback");
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
    c.shutdown().await;
}

/// E2E case `overwrite_never_serves_a_stale_generation`.
pub async fn overwrite_never_serves_a_stale_generation<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let c = cluster(&start).await;
    let v1 = payload(2, 6);
    s3::put(&c.backend, BUCKET, "o", v1.clone(), &PutOptions::default())
        .await
        .unwrap();
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    let old_gen = c.manifest(BUCKET, "o").await.pointer.generation;

    let v2 = payload(2, 7);
    s3::put(&c.backend, BUCKET, "o", v2.clone(), &PutOptions::default())
        .await
        .unwrap();
    // Readers after the overwrite: v2 only, from durable (new generation
    // has no cache), then from cache once populated.
    for _ in 0..3 {
        let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
        assert_eq!(got, v2, "never the stale cached generation");
    }
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, v2);
    assert_eq!(
        meta_cache::placements_of(c.meta.as_ref(), &old_gen)
            .await
            .unwrap()
            .len(),
        2,
        "the old generation's cache records live until GC"
    );

    // GC of the displaced generation deletes its cache copies and usage.
    let r = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(r.collected, 1);
    assert_eq!(
        meta_cache::placements_of(c.meta.as_ref(), &old_gen)
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        meta_cache::usage(c.meta.as_ref(), CACHE_TIER)
            .await
            .unwrap(),
        v2.len() as u64
    );
    let groups = c.groups().await;
    assert_eq!(cache_copies(&c, &groups), 2, "only v2's copies remain");
    c.shutdown().await;
}

/// E2E case `corrupt_cache_copy_falls_back_and_is_invalidated`.
pub async fn corrupt_cache_copy_falls_back_and_is_invalidated<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let c = cluster(&start).await;
    let data = payload(2, 8);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    let generation = c.manifest(BUCKET, "o").await.pointer.generation;
    let placements = c.cache_placements(BUCKET, "o").await;
    let (index, p) = &placements[0];
    let node = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == p.group_id)
        .and_then(|g| c.node_index(&g.members[0]))
        .unwrap();
    assert_eq!(
        c.corrupt_chunk_on_node(node, p.group_id, chunk_id(&generation, *index)),
        1
    );

    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data, "corrupt copy never served");
    let s = c.backend.cache_stats();
    assert_eq!((s.hits, s.fallbacks), (1, 1), "{s:?}");
    wait_until("invalidation", Duration::from_secs(10), async || {
        c.cache_placements(BUCKET, "o").await.len() == 1
    })
    .await;
    // The next read repopulates it.
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    c.shutdown().await;
}

/// E2E case `eviction_respects_watermarks_lru_and_pins`.
pub async fn eviction_respects_watermarks_lru_and_pins<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let c = cluster(&start).await;
    let mut objects = Vec::new();
    for i in 0..4u64 {
        let key = format!("o{i}");
        let data = payload(2, 10 + i);
        s3::put(
            &c.backend,
            BUCKET,
            &key,
            data.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        objects.push((key, data));
    }
    // Populate in order o0..o3 with distinct recency.
    for (key, _) in &objects {
        s3::get(&c.backend, BUCKET, key).await.unwrap();
        c.wait_cached(BUCKET, key, 2, Duration::from_secs(10)).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let total = objects.iter().map(|(_, d)| d.len() as u64).sum::<u64>();
    assert_eq!(
        meta_cache::usage(c.meta.as_ref(), CACHE_TIER)
            .await
            .unwrap(),
        total
    );
    c.backend.pin(BUCKET, "o0").await.unwrap();

    // Evict down to half: LRU order would drop o0, o1 first; o0 is pinned,
    // so o1 and o2 go.
    let cfg = CacheTierConfig {
        capacity_bytes: total,
        high_watermark: 0.6,
        low_watermark: 0.5,
        ..ClusterOptions::default_cache()
    };
    let map = c.backend.map();
    let addr_of = |node_id: u64| map.node_addr(node_id).map(str::to_string);
    let r = evict_tier(c.meta.as_ref(), &c.clients, &cfg, &map.groups, &addr_of)
        .await
        .unwrap();
    assert_eq!(r.evicted, 4, "{r:?}");
    assert!(r.skipped_pinned >= 2, "{r:?}");
    assert!(!r.over_watermark);
    assert!(r.usage_after <= cfg.low_bytes());
    assert_eq!(
        c.cache_placements(BUCKET, "o0").await.len(),
        2,
        "pinned stays"
    );
    assert_eq!(c.cache_placements(BUCKET, "o1").await.len(), 0);
    assert_eq!(c.cache_placements(BUCKET, "o2").await.len(), 0);
    assert_eq!(
        c.cache_placements(BUCKET, "o3").await.len(),
        2,
        "most recent stays"
    );
    let groups = c.groups().await;
    assert_eq!(cache_copies(&c, &groups), 4, "evicted copies deleted");
    for (key, data) in &objects {
        assert_eq!(&s3::get(&c.backend, BUCKET, key).await.unwrap().0, data);
    }
    // Those reads repopulated o1 and o2 through the read-through path.
    c.wait_cached(BUCKET, "o1", 2, Duration::from_secs(10))
        .await;
    c.wait_cached(BUCKET, "o2", 2, Duration::from_secs(10))
        .await;

    // Everything pinned: eviction cannot reach the target and says so.
    for key in ["o1", "o2", "o3"] {
        c.backend.pin(BUCKET, key).await.unwrap();
    }
    let r = evict_tier(
        c.meta.as_ref(),
        &c.clients,
        &CacheTierConfig {
            high_watermark: 0.1,
            low_watermark: 0.0,
            ..cfg.clone()
        },
        &map.groups,
        &addr_of,
    )
    .await
    .unwrap();
    assert_eq!(r.evicted, 0);
    assert!(r.over_watermark, "{r:?}");
    for key in ["o0", "o1", "o2", "o3"] {
        c.backend.unpin(BUCKET, key).await.unwrap();
    }
    let r = evict_tier(
        c.meta.as_ref(),
        &c.clients,
        &CacheTierConfig {
            high_watermark: 0.1,
            low_watermark: 0.0,
            ..cfg
        },
        &map.groups,
        &addr_of,
    )
    .await
    .unwrap();
    assert_eq!(r.usage_after, 0);
    assert!(!r.over_watermark);
    c.shutdown().await;
}

/// E2E case `admission_rejects_over_capacity_and_large_chunks`.
pub async fn admission_rejects_over_capacity_and_large_chunks<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let c = start(ClusterOptions {
        // One population at a time: the capacity check is a snapshot read
        // (RFC-006 §5.4), so up to `max_populations_in_flight` copies may
        // be admitted past the limit; serializing them makes the refusal
        // deterministic here.
        cache: Some(CacheTierConfig {
            capacity_bytes: 3 * CHUNK,
            high_watermark: 1.0,
            low_watermark: 0.5,
            max_cacheable_bytes: 0,
            admit_after_misses: 2,
            max_populations_in_flight: 1,
            ..ClusterOptions::default_cache()
        }),
        ..options()
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let data = payload(2, 20);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    // First miss is not admitted (threshold 2), second is.
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(c.cache_placements(BUCKET, "o").await.len(), 0);
    // With one population in flight a GET populates at most one chunk
    // (the other miss is skipped), so read until both are cached.
    wait_until("o cached", Duration::from_secs(10), async || {
        s3::get(&c.backend, BUCKET, "o").await.unwrap();
        c.cache_placements(BUCKET, "o").await.len() >= 2
    })
    .await;

    // A second object would exceed the capacity: rejected, GET still fine.
    let big = payload(2, 21);
    s3::put(
        &c.backend,
        BUCKET,
        "big",
        big.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    wait_until("rejections", Duration::from_secs(10), async || {
        assert_eq!(s3::get(&c.backend, BUCKET, "big").await.unwrap().0, big);
        c.backend.cache_stats().admissions_rejected >= 1
    })
    .await;
    assert!(
        c.cache_placements(BUCKET, "big").await.len() < 2,
        "not fully admitted"
    );
    assert!(
        meta_cache::usage(c.meta.as_ref(), CACHE_TIER)
            .await
            .unwrap()
            <= 3 * CHUNK
    );
    c.shutdown().await;
}

/// E2E case `prefetch_and_pin_populate_synchronously`.
pub async fn prefetch_and_pin_populate_synchronously<S: KvStore + 'static>(start: impl Starter<S>) {
    let c = cluster(&start).await;
    let data = payload(3, 30);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(c.backend.prefetch(BUCKET, "o").await.unwrap(), 3);
    assert_eq!(c.cache_placements(BUCKET, "o").await.len(), 3);
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    let s = c.backend.cache_stats();
    assert_eq!((s.hits, s.misses), (3, 0), "{s:?}");
    assert_eq!(c.backend.pin(BUCKET, "o").await.unwrap(), 3);
    assert!(
        c.cache_placements(BUCKET, "o")
            .await
            .iter()
            .all(|(_, p)| p.pinned)
    );
    assert_eq!(c.backend.unpin(BUCKET, "o").await.unwrap(), 3);
    assert!(
        c.cache_placements(BUCKET, "o")
            .await
            .iter()
            .all(|(_, p)| !p.pinned)
    );
    assert!(c.backend.prefetch(BUCKET, "missing").await.is_err());
    c.shutdown().await;
}

/// E2E case `crash_states_are_recovered_idempotently`.
pub async fn crash_states_are_recovered_idempotently<S: KvStore + 'static>(start: impl Starter<S>) {
    let c = cluster(&start).await;
    let data = payload(2, 40);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    let generation = c.manifest(BUCKET, "o").await.pointer.generation;
    let groups = c.groups().await;
    let cache_group = groups.iter().find(|g| g.is_cache()).unwrap().clone();
    let cache_node = c.node_index(&cache_group.members[0]).unwrap();
    let addr = c.nodes[cache_node].addr.to_string();

    // (a) A copy without a record (crash between write and insert).
    let stray = ShardRef {
        group_id: cache_group.id,
        chunk_id: chunk_id(&generation, 99),
        shard_index: 0,
    };
    c.clients
        .put_shard(
            &addr,
            stray,
            cache_group.epoch,
            Bytes::from_static(b"stray"),
            1024,
        )
        .await
        .unwrap();
    assert_eq!(c.reconcile_cache().await, 1, "orphan copy deleted");
    assert_eq!(c.reconcile_cache().await, 0, "idempotent");
    assert_eq!(
        c.cache_placements(BUCKET, "o").await.len(),
        2,
        "recorded copies kept"
    );

    // (b) A record without a copy: the reader falls back and invalidates.
    let (index, p) = c.cache_placements(BUCKET, "o").await[0].clone();
    let node = groups
        .iter()
        .find(|g| g.id == p.group_id)
        .and_then(|g| c.node_index(&g.members[0]))
        .unwrap();
    c.clients
        .delete_shard(
            &c.nodes[node].addr.to_string(),
            ShardRef {
                group_id: p.group_id,
                chunk_id: chunk_id(&generation, index),
                shard_index: 0,
            },
            p.epoch,
        )
        .await
        .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    wait_until("invalidation", Duration::from_secs(10), async || {
        c.cache_placements(BUCKET, "o").await.len() == 1
    })
    .await;
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;

    // (c) All cache metadata lost: copies become orphans, reads fall back,
    // the cache refills by read-through.
    {
        let mut tx = c.meta.begin().await.unwrap();
        tx.clear_range(&keys::cache_placements_range(&generation));
        tx.clear_range(&keys::cache_recency_range_from(CACHE_TIER, 0));
        tx.clear(&keys::cache_usage(CACHE_TIER));
        tx.commit().await.unwrap();
    }
    assert_eq!(
        c.reconcile_cache().await,
        2,
        "copies without records deleted"
    );
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
    assert_eq!(
        meta_cache::usage(c.meta.as_ref(), CACHE_TIER)
            .await
            .unwrap(),
        data.len() as u64
    );
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    c.shutdown().await;
}

/// E2E case `cache_survives_durable_repair_gc_and_fenced_cache_epoch`.
pub async fn cache_survives_durable_repair_gc_and_fenced_cache_epoch<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let mut c = cluster(&start).await;
    let data = payload(2, 50);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    s3::get(&c.backend, BUCKET, "o").await.unwrap();
    c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;

    // Durable repair while the cache serves hits.
    let group = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.scheme == "rs-gf8-v1")
        .unwrap();
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;
    let report = c.repair(group.id, 0, new_node).await;
    assert!(report.complete);
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    assert_eq!(
        c.cache_placements(BUCKET, "o").await.len(),
        2,
        "repair ignores the cache"
    );

    // GC and reconciliation leave live cache records alone.
    let r = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(r.collected, 0);
    assert_eq!(
        c.reconcile(now_ms() + 3_600_000, 0).await + c.reconcile_cache().await,
        0
    );
    assert_eq!(c.cache_placements(BUCKET, "o").await.len(), 2);

    // A fenced cache node rejects stale-epoch populations; the GET is fine.
    let other = payload(1, 51);
    s3::put(
        &c.backend,
        BUCKET,
        "p",
        other.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    for g in c.groups().await {
        if g.is_cache() {
            let n = c.node_index(&g.members[0]).unwrap();
            c.clients
                .update_epoch(&c.nodes[n].addr.to_string(), g.id, g.epoch + 1)
                .await
                .unwrap();
        }
    }
    assert_eq!(s3::get(&c.backend, BUCKET, "p").await.unwrap().0, other);
    wait_until("population failure", Duration::from_secs(10), async || {
        c.backend.cache_stats().population_failures >= 1
    })
    .await;
    assert_eq!(c.cache_placements(BUCKET, "p").await.len(), 0);
    assert_eq!(s3::get(&c.backend, BUCKET, "p").await.unwrap().0, other);
    c.shutdown().await;
}

/// Instantiate every cache case as a `#[tokio::test]` for one backend.
#[macro_export]
macro_rules! cache_suite {
    ($start:expr) => {
        $crate::cache_suite!(@each $start;
            miss_then_hit_serves_from_cache,
            partial_hits_and_range_gets,
            concurrent_gets_populate_each_chunk_once,
            population_failure_never_fails_the_get,
            cache_node_failure_falls_back_and_invalidates,
            overwrite_never_serves_a_stale_generation,
            corrupt_cache_copy_falls_back_and_is_invalidated,
            eviction_respects_watermarks_lru_and_pins,
            admission_rejects_over_capacity_and_large_chunks,
            prefetch_and_pin_populate_synchronously,
            crash_states_are_recovered_idempotently,
            cache_survives_durable_repair_gc_and_fenced_cache_epoch,
        );
    };
    (@each $start:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(start) = $start else {
                    eprintln!("{}: backend not available; skipping", stringify!($name));
                    return;
                };
                $crate::suite_cache::$name(start).await;
            }
        )*
    };
}
