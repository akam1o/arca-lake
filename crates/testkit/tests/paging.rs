//! Paged `ListShards` / `VerifyShards` (RFC-008 §6.5): completeness of a
//! walk, bounded pages, resumption from a cursor, behaviour under concurrent
//! writes and deletes, and convergence of GC and repair with small pages.

#![allow(clippy::too_many_lines)]

use std::collections::BTreeSet;

use arca_lake_controller::repair::RepairOptions;
use arca_lake_protocol::client::ShardRef;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const BUCKET: &str = "paging";

/// Small chunks so that a few objects produce many shards.
async fn cluster() -> TestCluster {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, 64 << 10, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c.page_size = 7;
    c.repair_options = RepairOptions {
        page_size: 7,
        verify_all_members: false,
        max_chunks: 0,
    };
    c
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

/// Walk every page of `node` for `group` and return the keys in order.
async fn walk(c: &TestCluster, node: usize, group: u64, page: u32) -> (Vec<ShardRef>, usize) {
    let addr = c.nodes[node].addr.to_string();
    let mut out = Vec::new();
    let mut pages = 0;
    let mut after = None;
    loop {
        let p = c
            .clients
            .list_shards_page(&addr, group, after, page)
            .await
            .unwrap();
        pages += 1;
        if page > 0 {
            assert!(
                p.entries.len() <= usize::try_from(page).unwrap(),
                "page bounded"
            );
        }
        out.extend(p.entries.into_iter().map(|(k, _)| k));
        match p.next {
            Some(n) => after = Some(n),
            None => return (out, pages),
        }
    }
}

#[tokio::test]
async fn paged_walk_is_complete_ordered_and_resumable() {
    let c = cluster().await;
    // 20 objects x 6 chunks = 120 shards per node.
    for i in 0..20u64 {
        s3::put(
            &c.backend,
            BUCKET,
            &format!("o{i}"),
            payload(6 * (64 << 10) - 100, i),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    let group = c.groups().await[0].id;
    let expected: BTreeSet<_> = c
        .shard_keys(0, group)
        .into_iter()
        .map(|k| (k.chunk_id, k.shard_index))
        .collect();
    assert_eq!(expected.len(), 120);

    let (keys, pages) = walk(&c, 0, group, 7).await;
    assert_eq!(pages, 120 / 7 + 1, "ceil(120 / 7) pages");
    let got: Vec<_> = keys.iter().map(|k| (k.chunk_id, k.shard_index)).collect();
    assert!(
        got.windows(2).all(|w| w[0] < w[1]),
        "strictly increasing, no duplicates"
    );
    assert_eq!(got.into_iter().collect::<BTreeSet<_>>(), expected);

    // Resume from the middle: the tail after a cursor equals the tail of
    // the full walk.
    let cursor = keys[50];
    let addr = c.nodes[0].addr.to_string();
    let mut rest = Vec::new();
    let mut after = Some(cursor);
    loop {
        let p = c
            .clients
            .list_shards_page(&addr, group, after, 13)
            .await
            .unwrap();
        rest.extend(p.entries.into_iter().map(|(k, _)| k));
        match p.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    assert_eq!(rest, keys[51..].to_vec());

    // Default and large page sizes give the same set.
    let (all, pages) = walk(&c, 0, group, 0).await;
    assert_eq!(pages, 1);
    assert_eq!(all, keys);

    // VerifyShards pages agree and every shard verifies.
    let mut after = None;
    let mut verified = 0;
    loop {
        let p = c
            .clients
            .verify_shards_page(&addr, group, after, 11)
            .await
            .unwrap();
        for v in &p.shards {
            assert!(v.ok);
            assert!(v.payload_len > 0);
            verified += 1;
        }
        match p.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    assert_eq!(verified, 120);
    c.shutdown().await;
}

#[tokio::test]
async fn walk_during_concurrent_puts_and_deletes_never_duplicates_and_sees_the_stable_set() {
    let c = cluster().await;
    for i in 0..10u64 {
        s3::put(
            &c.backend,
            BUCKET,
            &format!("stable{i}"),
            payload(3 * (64 << 10), 100 + i),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    for i in 0..10u64 {
        s3::put(
            &c.backend,
            BUCKET,
            &format!("doomed{i}"),
            payload(3 * (64 << 10), 200 + i),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    let group = c.groups().await[0].id;
    let stable: BTreeSet<_> = {
        let mut s = BTreeSet::new();
        for i in 0..10 {
            for (_, ch) in c.manifest(BUCKET, &format!("stable{i}")).await.chunks {
                s.insert(ch.chunk_id);
            }
        }
        s
    };

    // Writer: adds new objects and deletes + collects the doomed ones while
    // the walk pages through node 0.
    let churn = {
        let backend = c.backend.clone();
        let meta = std::sync::Arc::clone(&c.meta);
        let clients = c.clients.clone();
        tokio::spawn(async move {
            for i in 0..10u64 {
                s3::put(
                    &backend,
                    BUCKET,
                    &format!("new{i}"),
                    payload(64 << 10, 300 + i),
                    &PutOptions::default(),
                )
                .await
                .unwrap();
                s3::delete(&backend, BUCKET, &format!("doomed{i}"))
                    .await
                    .unwrap();
                let _ = arca_lake_controller::gc::run_once(
                    meta.as_ref(),
                    &clients,
                    now_ms() + 120_000,
                    0,
                    100,
                )
                .await;
            }
        })
    };
    let (keys, _) = walk(&c, 0, group, 5).await;
    churn.await.unwrap();

    let seen: Vec<_> = keys.iter().map(|k| (k.chunk_id, k.shard_index)).collect();
    assert!(
        seen.windows(2).all(|w| w[0] < w[1]),
        "no duplicates even under churn"
    );
    let seen_chunks: BTreeSet<_> = keys.iter().map(|k| k.chunk_id).collect();
    assert!(
        stable.is_subset(&seen_chunks),
        "every shard that existed throughout the walk was listed"
    );
    c.shutdown().await;
}

#[tokio::test]
async fn gc_and_repair_converge_with_small_pages() {
    let mut c = cluster().await;
    let mut objects = Vec::new();
    for i in 0..8u64 {
        let data = payload(5 * (64 << 10) + 17, 400 + i);
        s3::put(
            &c.backend,
            BUCKET,
            &format!("o{i}"),
            data.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        objects.push((format!("o{i}"), data));
    }
    let group = c.groups().await.remove(0);
    // Delete half, collect, and reconcile with 7-entry pages.
    for i in 0..4 {
        s3::delete(&c.backend, BUCKET, &format!("o{i}"))
            .await
            .unwrap();
    }
    let r = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(r.collected, 4);
    assert_eq!(
        c.reconcile(now_ms() + 3_600_000, 0).await,
        0,
        "nothing left to reconcile"
    );
    assert_eq!(c.shard_count(0, group.id), 4 * 6);

    // Repair with small pages: 24 chunks across 4 pages per member.
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;
    let report = c.repair(group.id, 0, new_node).await;
    assert_eq!(report.reconstructed, 24);
    assert!(report.complete, "{report:?}");
    assert_eq!(report.completeness.chunks, 24);
    assert_eq!(c.shard_count(new_node, group.id), 24);
    let other = c.node_index(&group.members[1]).unwrap();
    c.stop_node(other).await;
    for (key, data) in &objects[4..] {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, key).await.unwrap().0,
            data,
            "{key}"
        );
    }
    c.shutdown().await;
}
