//! Bounded parallel chunk fetch in the gateway read path (RFC-009 §9):
//! ordered delivery with a mix of cached, uncached and degraded chunks,
//! range GETs across chunk boundaries, an unreadable chunk behind
//! prefetched ones, and a body dropped by the client.

use std::time::Duration;

use arca_lake_common::chunk_id;
use arca_lake_metadata::records::GroupRecord;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use bytes::Bytes;
use s3s::S3;
use s3s::dto::GetObjectInput;

const CHUNK: u64 = 256 << 10;
const BUCKET: &str = "parallel";

async fn cluster(parallelism: usize) -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        cache_nodes: 2,
        read_parallelism: parallelism,
        ..ClusterOptions::rs(3, 2, 1, CHUNK, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(chunks: u64, seed: u64) -> Bytes {
    Bytes::from(random_bytes(
        usize::try_from(chunks * CHUNK - 11).unwrap(),
        seed,
    ))
}

#[tokio::test]
async fn parallel_and_sequential_reads_return_the_same_bytes() {
    for parallelism in [1usize, 4, 16] {
        let c = cluster(parallelism).await;
        let data = payload(7, 1);
        s3::put(
            &c.backend,
            BUCKET,
            "o",
            data.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        // Warm the cache for chunks 1 and 4 only (range reads inside them).
        for (first, last) in [(CHUNK + 1, CHUNK + 2), (4 * CHUNK + 1, 4 * CHUNK + 2)] {
            s3::get_range(&c.backend, BUCKET, "o", first, last)
                .await
                .unwrap();
        }
        c.wait_cached(BUCKET, "o", 2, Duration::from_secs(10)).await;
        // Degrade chunk 3: one data shard lost; it decodes from parity.
        let m = c.manifest(BUCKET, "o").await;
        let (idx3, rec3) = &m.chunks[3];
        let group = c
            .groups()
            .await
            .into_iter()
            .find(|g| g.id == rec3.placement.group_id)
            .unwrap();
        let holder = c.node_index(&group.members[0]).unwrap();
        assert_eq!(
            c.corrupt_chunk_on_node(holder, group.id, chunk_id(&m.pointer.generation, *idx3)),
            1
        );

        let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
        assert_eq!(got, data, "parallelism {parallelism}: full read");
        let ranges = [
            (0, 5),
            (CHUNK - 3, CHUNK + 3),
            (2 * CHUNK + 7, 5 * CHUNK + 9),
            (6 * CHUNK + 1, data.len() as u64 - 1),
        ];
        for (first, last) in ranges {
            let r = s3::get_range(&c.backend, BUCKET, "o", first, last)
                .await
                .unwrap();
            assert_eq!(
                &r[..],
                &data[usize::try_from(first).unwrap()..=usize::try_from(last).unwrap()],
                "parallelism {parallelism}: range {first}-{last}"
            );
        }
        let s = c.backend.cache_stats();
        assert!(s.hits >= 2, "{s:?}");
        c.shutdown().await;
    }
}

#[tokio::test]
async fn unreadable_chunk_behind_prefetched_ones_ends_the_stream_with_an_error() {
    let c = cluster(4).await;
    let data = payload(6, 2);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    // Chunk 4 loses two of three shards: unreadable while the chunks before
    // and after it stay readable.
    let m = c.manifest(BUCKET, "o").await;
    let (idx, rec) = &m.chunks[4];
    let group = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == rec.placement.group_id)
        .unwrap();
    for member in &group.members[..2] {
        let node = c.node_index(member).unwrap();
        assert_eq!(
            c.corrupt_chunk_on_node(node, group.id, chunk_id(&m.pointer.generation, *idx)),
            1
        );
    }
    let out = c
        .backend
        .get_object(s3::request(GetObjectInput {
            bucket: BUCKET.to_string(),
            key: "o".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let collected = s3::collect(out.body).await;
    assert!(collected.is_err(), "the stream must end with an error");
    // The chunks before the unreadable one are still served correctly.
    let r = s3::get_range(&c.backend, BUCKET, "o", 0, 4 * CHUNK - 1)
        .await
        .unwrap();
    assert_eq!(&r[..], &data[..usize::try_from(4 * CHUNK).unwrap()]);
    c.shutdown().await;
}

#[tokio::test]
async fn dropping_the_body_cancels_the_prefetch_and_later_reads_succeed() {
    let c = cluster(4).await;
    let data = payload(8, 3);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    for _ in 0..5 {
        let out = c
            .backend
            .get_object(s3::request(GetObjectInput {
                bucket: BUCKET.to_string(),
                key: "o".to_string(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        // Take the first frame only, then drop the body.
        let mut body = out.body.unwrap();
        let first = futures::StreamExt::next(&mut body).await;
        assert!(first.is_some());
        drop(body);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (got, _) = s3::get(&c.backend, BUCKET, "o").await.unwrap();
    assert_eq!(got, data);
    c.shutdown().await;
}

/// Cache populations that hit a transient reset on one member are retried
/// by the client; each chunk is recorded once, every member holds exactly
/// one shard per chunk, and the next read is a validated hit.
#[tokio::test]
async fn population_retried_after_resets_records_each_chunk_once() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arca_lake_protocol::client::{FaultAction, Rpc};

    let c = cluster(4).await;
    let data = payload(6, 4);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let cache_group = c
        .groups()
        .await
        .into_iter()
        .find(GroupRecord::is_cache)
        .unwrap();
    let cache_addr = c.nodes[c.node_index(&cache_group.members[0]).unwrap()]
        .addr
        .to_string();
    // Every first PutShard of a chunk to the cache member is reset once.
    let seen = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let resets = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&resets);
    c.backend
        .node_clients()
        .set_fault_hook(Some(Arc::new(move |ctx| {
            if ctx.rpc == Rpc::PutShard
                && ctx.addr == cache_addr
                && let Some(shard) = ctx.shard
                && seen.lock().unwrap().insert(shard.chunk_id)
            {
                counter.fetch_add(1, Ordering::SeqCst);
                return FaultAction::Reset;
            }
            FaultAction::None
        })));
    // Several concurrent readers: populations are deduplicated per chunk
    // and retried once each.
    let mut tasks = Vec::new();
    for _ in 0..6 {
        let backend = c.backend.clone();
        tasks.push(tokio::spawn(async move {
            s3::get(&backend, BUCKET, "o").await.unwrap().0
        }));
    }
    for t in tasks {
        assert_eq!(t.await.unwrap(), data);
    }
    c.wait_cached(BUCKET, "o", 6, Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(resets.load(Ordering::SeqCst) >= 1);
    let stats = c.backend.node_clients().stats();
    assert!(stats.retries >= 1, "{stats:?}");
    assert_eq!(stats.exhausted, 0, "{stats:?}");
    let s = c.backend.cache_stats();
    assert_eq!(s.population_failures, 0, "{s:?}");
    assert_eq!(c.cache_placements(BUCKET, "o").await.len(), 6);
    for g in c.groups().await.into_iter().filter(GroupRecord::is_cache) {
        for m in &g.members {
            let n = c.node_index(m).unwrap();
            let count = c.shard_count(n, g.id);
            assert!(count <= 6, "no duplicate shards on member: {count}");
        }
    }
    let total: usize = c
        .groups()
        .await
        .into_iter()
        .filter(GroupRecord::is_cache)
        .flat_map(|g| g.members.into_iter().map(move |m| (g.id, m)))
        .map(|(gid, m)| c.shard_count(c.node_index(&m).unwrap(), gid))
        .sum();
    assert_eq!(total, 6, "one copy per chunk across the cache tier");
    c.backend.node_clients().set_fault_hook(None);
    let before = c.backend.cache_stats();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    let after = c.backend.cache_stats();
    assert_eq!(after.hits - before.hits, 6, "{after:?}");
    assert_eq!(after.fallbacks - before.fallbacks, 0, "{after:?}");
    c.shutdown().await;
}
