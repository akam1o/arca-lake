//! Shard write ownership end to end (R-117, ADR-0037): a gateway that still
//! maps a node to an address now served by a node of another cluster never
//! has its shards stored there, whether that node adopted its own cluster's
//! incarnation or none yet, nor on another node of the same cluster
//! answering at a member's address; this cluster's own paths (first
//! writes, restart, a new node and a repair onto it) keep working, also
//! strict.

use std::time::Duration;

use arca_lake_protocol::client::NodeClients;
use arca_lake_protocol::storage::v1::HealthRequest;
use arca_lake_storage_node::{NodeConfig, serve};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster};
use bytes::Bytes;

const BUCKET: &str = "own";

fn payload(n: usize, seed: u8) -> Bytes {
    Bytes::from(
        (0..n)
            .map(|i| seed.wrapping_add(u8::try_from(i % 251).unwrap()))
            .collect::<Vec<u8>>(),
    )
}

async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, 1 << 20, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

/// Node 0's address of cluster `a` is now served by a node of another
/// cluster (`foreign` pushes it that cluster's facts, or none). A PUT
/// through `a`'s gateway stores nothing there; if it succeeds, it reads
/// back from `a`'s own nodes.
async fn put_against_a_foreign_node_at_a_reused_address(foreign: Option<[u8; 16]>) {
    let mut a = cluster().await;
    // The gateway knows the node (a first PUT through it).
    s3::put(
        &a.backend,
        BUCKET,
        "before",
        payload(64 << 10, 1),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let addr = a.stop_node_releasing_address(0).await;
    let dir = tempfile::tempdir().unwrap();
    let mut started = None;
    for _ in 0..50 {
        match serve(NodeConfig::new(addr, dir.path())).await {
            Ok(h) => {
                started = Some(h);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let other = started.expect("the released address could not be bound");
    let theirs = NodeClients::new(Duration::from_secs(5));
    if let Some(inc) = foreign {
        theirs
            .health_with(
                &addr.to_string(),
                HealthRequest {
                    incarnation: Bytes::copy_from_slice(&inc),
                    incarnation_changed_ms: 1,
                    incarnation_seq: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let data = payload(256 << 10, 2);
    let put = s3::put(
        &a.backend,
        BUCKET,
        "after",
        data.clone(),
        &PutOptions::default(),
    )
    .await;
    assert_eq!(
        other.service().stats().put_store.count,
        0,
        "{foreign:?}: a shard of this cluster was stored on another cluster's node"
    );
    assert_eq!(
        theirs.health(&addr.to_string()).await.unwrap().live_shards,
        0,
        "{foreign:?}"
    );
    if put.is_ok() {
        let (got, _) = s3::get(&a.backend, BUCKET, "after").await.unwrap();
        assert_eq!(got, data);
    }
    other.shutdown().await;
    a.shutdown().await;
}

/// The other cluster's node already adopted its own incarnation (its
/// controller reached it): refused since 0.1.0 (R-117 mitigation).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_put_to_another_clusters_node_at_a_reused_address_is_refused() {
    put_against_a_foreign_node_at_a_reused_address(Some([0xb0; 16])).await;
}

/// The other cluster's node has adopted nothing yet (a new disk whose
/// controller has not reached it): 0.1.0 stored this cluster's shards
/// there and counted them written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_put_to_a_node_that_adopted_nothing_at_a_reused_address_is_refused() {
    put_against_a_foreign_node_at_a_reused_address(None).await;
}

/// Member 0's address now serves another node of the same cluster (its
/// disk moved there, or the address went to another node while the
/// gateway's map still names the member there): a PUT through the gateway
/// names the member, the node there refuses it, and nothing is stored or
/// counted written on it. 0.1.0 stored the shard (same incarnation).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_put_to_another_node_of_the_same_cluster_at_a_members_address_is_refused() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(4, 2, 1, 1 << 20, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    // As a controller's first pass: every node holds its identity.
    c.acknowledge_identities().await;
    s3::put(
        &c.backend,
        BUCKET,
        "before",
        payload(64 << 10, 1),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let group = c.groups().await.remove(0);
    let member = c.node_index(&group.members[0]).unwrap();
    let spare = (0..c.nodes.len())
        .find(|i| !group.members.iter().any(|m| m.node_id == c.nodes[*i].id))
        .unwrap();
    let addr = c.stop_node_releasing_address(member).await;
    c.stop_node(spare).await;
    let dir = c.nodes[spare].dir.clone();
    let mut started = None;
    for _ in 0..50 {
        match serve(NodeConfig::new(addr, &dir)).await {
            Ok(h) => {
                started = Some(h);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let other = started.expect("the released address could not be bound");
    let data = payload(256 << 10, 2);
    let put = s3::put(
        &c.backend,
        BUCKET,
        "after",
        data.clone(),
        &PutOptions::default(),
    )
    .await;
    assert_eq!(
        other.service().stats().put_store.count,
        0,
        "a shard for node {} was stored on node {}",
        c.nodes[member].id,
        c.nodes[spare].id
    );
    if put.is_ok() {
        let (got, _) = s3::get(&c.backend, BUCKET, "after").await.unwrap();
        assert_eq!(got, data);
    }
    other.shutdown().await;
    c.shutdown().await;
}

/// A strict cluster (every node told the strict schema) keeps its own
/// paths: writes, a node restart, a new node registered and given the
/// cluster's facts, and a repair onto it; a writer holding no incarnation
/// is refused by every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_strict_cluster_keeps_its_own_writes_restarts_and_repairs() {
    use arca_lake_protocol::storage::v1 as pb;
    let mut c = cluster().await;
    // As a controller's first pass: identities, then the strict schema.
    c.acknowledge_identities().await;
    for n in &c.nodes {
        let h = c
            .clients
            .health_with(
                &n.addr.to_string(),
                HealthRequest {
                    schema_floor: pb::WRITE_OWNERSHIP_SCHEMA,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(h.write_ownership_strict);
    }
    let one = payload(300 << 10, 3);
    s3::put(
        &c.backend,
        BUCKET,
        "one",
        one.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    // A restart: the node keeps its incarnation and strict mode.
    c.stop_node(1).await;
    c.start_node(1).await;
    assert!(c.node_service(1).write_ownership_strict());
    let two = payload(300 << 10, 4);
    s3::put(
        &c.backend,
        BUCKET,
        "two",
        two.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    // A tool that holds no incarnation.
    let tool = NodeClients::new(Duration::from_secs(5));
    let g = c.groups().await[0].clone();
    for n in &c.nodes {
        let e = tool
            .put_shard(
                &n.addr.to_string(),
                arca_lake_protocol::client::ShardRef {
                    group_id: g.id,
                    chunk_id: [0x77; 16],
                    shard_index: 0,
                },
                g.epoch,
                payload(4096, 5),
                1024,
            )
            .await
            .unwrap_err();
        assert!(e.message().starts_with("incarnation rejected"), "{e}");
    }
    // A new node, then a repair of member 2 onto it.
    let new = c.add_node().await;
    c.acknowledge_identities().await;
    let g = c.groups().await[0].clone();
    c.stop_node(2).await;
    let report = c.repair(g.id, 2, new).await;
    assert!(
        report.reconstructed + report.copied_direct > 0,
        "{report:?}"
    );
    assert_eq!(report.unrebuilt, 0, "{report:?}");
    for (key, data) in [("one", &one), ("two", &two)] {
        let (got, _) = s3::get(&c.backend, BUCKET, key).await.unwrap();
        assert_eq!(&got, data, "{key}");
    }
    assert_eq!(c.node_service(new).writes_unproven(), 0);
    c.shutdown().await;
}

/// A cluster initialised at the strict schema (0.2.0 initialises at
/// schema 5, ADR-0039; a `version-bump` build at 6)
/// is strict from its first write: every node, given its facts as a
/// controller's first health pass does, refuses unproven writes, and the
/// gateway's writes go through (ADR-0037 §5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_initialised_at_the_strict_schema_is_strict_from_its_first_write() {
    let c = cluster().await;
    // The identities a controller's first pass gives with the facts.
    c.acknowledge_identities().await;
    for (i, n) in c.nodes.iter().enumerate() {
        assert!(c.node_service(i).write_ownership_strict(), "node {}", n.id);
    }
    let data = payload(200 << 10, 9);
    s3::put(
        &c.backend,
        BUCKET,
        "first",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "first").await.unwrap().0, data);
    let unnamed = NodeClients::new(Duration::from_secs(5));
    unnamed.set_incarnation(Some(c.incarnation()));
    let g = c.groups().await.remove(0);
    let r = unnamed
        .put_shard(
            &c.nodes[0].addr.to_string(),
            arca_lake_protocol::client::ShardRef {
                group_id: g.id,
                chunk_id: [0x55; 16],
                shard_index: 0,
            },
            g.epoch,
            payload(4096, 1),
            1024,
        )
        .await;
    assert!(
        r.unwrap_err().message().starts_with("node rejected"),
        "an unnamed write"
    );
    // Nothing unproven was taken anywhere.
    for i in 0..c.nodes.len() {
        assert_eq!(c.node_service(i).writes_unproven(), 0, "node {i}");
    }
    c.shutdown().await;
}

/// A writer's cleanup of its own partial copy (a cache population that
/// could not write every member) is refused by nodes holding an identity
/// when its deletes carry none (R-100): the shards stay for
/// reconciliation, and the refusal is counted
/// (`arca_gateway_cache_cleanup_refused_total`). With the identities the
/// nodes acknowledged, which the gateway now reads with the cluster state,
/// the same cleanup goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partial_copys_cleanup_needs_the_nodes_identities_and_its_refusals_are_counted() {
    use arca_lake_tiering::durable::write_coded;
    let mut c = cluster().await;
    let g = c.groups().await.remove(0);
    let ids: Vec<u64> = c.nodes.iter().map(|n| n.id).collect();
    let addrs: Vec<String> = c.nodes.iter().map(|n| n.addr.to_string()).collect();
    let addr_of = |id: u64| ids.iter().position(|i| *i == id).map(|i| addrs[i].clone());
    let last = c.node_index(&g.members[2]).unwrap();
    c.stop_node(last).await;
    let shards: Vec<Bytes> = (0..3).map(|i| payload(4096, i)).collect();
    // A writer of the cluster (incarnation, open fence, node names), as a
    // gateway's pool is.
    let writer = || {
        let w = NodeClients::new(Duration::from_secs(2));
        w.set_incarnation(Some(c.incarnation()));
        w.set_delete_fence(c.clients.delete_fence());
        for (id, addr) in ids.iter().zip(&addrs) {
            w.note_node_id(addr, *id);
        }
        w
    };
    let left = |chunk: [u8; 16]| {
        g.members
            .iter()
            .filter_map(|m| c.node_index(m))
            .filter(|i| c.node_running(*i))
            .map(|i| {
                c.shard_keys(i, g.id)
                    .iter()
                    .filter(|k| k.chunk_id == chunk)
                    .count()
            })
            .sum::<usize>()
    };
    // Without the identities: refused, counted, left behind.
    let bare = writer();
    assert!(
        write_coded(&bare, &g, &addr_of, [0xc1; 16], &shards, None)
            .await
            .is_err()
    );
    assert_eq!(bare.stats().cleanup_refused, 2);
    assert_eq!(left([0xc1; 16]), 2, "the partial copy is left behind");
    // With them: deleted, nothing counted.
    let stamped = writer();
    let acknowledged = arca_lake_metadata::ops::node_lifecycle::identities(c.meta.as_ref())
        .await
        .unwrap()
        .into_values()
        .filter(|i| i.acknowledged)
        .map(|i| (i.node_id, i.token))
        .collect();
    stamped.note_acknowledged_identities(&acknowledged);
    assert!(
        write_coded(&stamped, &g, &addr_of, [0xc2; 16], &shards, None)
            .await
            .is_err()
    );
    assert_eq!(stamped.stats().cleanup_refused, 0);
    assert_eq!(left([0xc2; 16]), 0, "the partial copy was deleted");
    c.shutdown().await;
}

/// The gateway's own pool takes the identities the nodes acknowledged at
/// its start and with its cluster refresh (every 2 s), not from the test:
/// after `node rejoin --fresh-identity` (a new, not yet acknowledged token)
/// its cleanup of a partial copy is refused by that node, counted and left
/// behind; once the new identity is acknowledged, its next refresh stamps
/// it and the same cleanup goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gateways_identities_follow_its_start_its_refresh_and_a_rejoin() {
    use arca_lake_metadata::ops::node_lifecycle;
    use arca_lake_tiering::durable::write_coded;
    let mut c = cluster().await;
    // At start: a new gateway backend reads them before its first refresh.
    c.restart_backend().await.unwrap();
    let ids = node_lifecycle::identities(c.meta.as_ref()).await.unwrap();
    for n in &c.nodes {
        let rec = &ids[&n.id];
        assert!(rec.acknowledged);
        assert_eq!(
            c.backend.node_clients().noted_identity(&n.addr.to_string()),
            Some((n.id, rec.token)),
            "node {}",
            n.id
        );
    }
    let g = c.groups().await.remove(0);
    let x = c.node_index(&g.members[0]).unwrap();
    let (xid, xaddr) = (c.nodes[x].id, c.nodes[x].addr.to_string());
    // X rejoins with a fresh identity: its disk's file removed, a new token
    // recorded (not acknowledged yet) and adopted by the disk.
    c.stop_node(x).await;
    std::fs::remove_file(c.nodes[x].dir.join("node-identity.txt")).unwrap();
    // Started and given the open fence (a started node takes no delete
    // before a push), still without an identity.
    c.start_node(x).await;
    arca_lake_metadata::transact(c.meta.as_ref(), xid, |tx, id| {
        Box::pin(async move {
            // What `rejoin --fresh-identity` and the next pass record.
            arca_lake_metadata::Txn::clear(tx, &arca_lake_metadata::keys::node_identity(*id));
            node_lifecycle::new_identity_in_tx(tx, *id).await
        })
    })
    .await
    .unwrap();
    let fresh = node_lifecycle::identity(c.meta.as_ref(), xid)
        .await
        .unwrap()
        .unwrap();
    assert!(!fresh.acknowledged);
    c.clients
        .health_with(
            &xaddr,
            HealthRequest {
                node_id: xid,
                node_identity: Bytes::copy_from_slice(&fresh.token),
                adopt_identity: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let pool = c.backend.node_clients().clone();
    let wait_noted = async |want: Option<(u64, [u8; 16])>| {
        let t = std::time::Instant::now();
        while pool.noted_identity(&xaddr) != want {
            assert!(t.elapsed() < Duration::from_secs(10), "never {want:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    // The refresh drops the old token (no acknowledged identity for X).
    wait_noted(None).await;
    let ids: Vec<u64> = c.nodes.iter().map(|n| n.id).collect();
    let addrs: Vec<String> = c.nodes.iter().map(|n| n.addr.to_string()).collect();
    let addr_of = |id: u64| ids.iter().position(|i| *i == id).map(|i| addrs[i].clone());
    let down = c.node_index(&g.members[2]).unwrap();
    c.stop_node(down).await;
    let shards: Vec<Bytes> = (0..3).map(|i| payload(4096, i)).collect();
    let on_x = |chunk: [u8; 16]| {
        c.shard_keys(x, g.id)
            .iter()
            .filter(|k| k.chunk_id == chunk)
            .count()
    };
    let refused = pool.stats().cleanup_refused;
    assert!(
        write_coded(&pool, &g, &addr_of, [0xd1; 16], &shards, None)
            .await
            .is_err()
    );
    assert_eq!(pool.stats().cleanup_refused, refused + 1);
    assert_eq!(on_x([0xd1; 16]), 1, "left for reconciliation");
    // Acknowledged: the next refresh stamps it, the cleanup goes through.
    node_lifecycle::acknowledge_identity(c.meta.as_ref(), xid, fresh.token)
        .await
        .unwrap();
    wait_noted(Some((xid, fresh.token))).await;
    assert!(
        write_coded(&pool, &g, &addr_of, [0xd2; 16], &shards, None)
            .await
            .is_err()
    );
    assert_eq!(pool.stats().cleanup_refused, refused + 1);
    assert_eq!(on_x([0xd2; 16]), 0);
    c.shutdown().await;
}

/// The gateway's invalidations and evictions delete cache copies other
/// gateways may have populated, after their placement was removed from
/// metadata; with the identities it reads, nodes holding one take them
/// (before, they refused every one, and the copies stayed until
/// reconciliation).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gateways_invalidations_and_evictions_delete_on_identity_holding_nodes() {
    use arca_lake_tiering::cache::{CacheTierConfig, evict_tier, invalidate};
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        cache_nodes: 3,
        cache_groups: 1,
        ..ClusterOptions::rs(3, 2, 1, 256 << 10, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    for key in ["a", "b"] {
        let d = payload(200 << 10, 7);
        s3::put(&c.backend, BUCKET, key, d.clone(), &PutOptions::default())
            .await
            .unwrap();
        s3::get(&c.backend, BUCKET, key).await.unwrap();
        c.wait_cached(BUCKET, key, 1, Duration::from_secs(10)).await;
    }
    let cache_group = c.cache_placements(BUCKET, "a").await[0].1.group_id;
    let cached = || {
        c.cache_node_indexes()
            .into_iter()
            .map(|i| c.shard_count(i, cache_group))
            .sum::<usize>()
    };
    assert!(cached() > 0);
    let pool = c.backend.node_clients().clone();
    let map = c.backend.map();
    let addr_of = |id: u64| map.node_addr(id).map(str::to_string);
    let before = pool.stats().cleanup_refused;
    // Invalidate `a`.
    let generation = c.manifest(BUCKET, "a").await.pointer.generation;
    let group = c.groups().await.into_iter().find(|g| g.id == cache_group);
    assert!(
        invalidate(
            c.meta.as_ref(),
            &pool,
            group.as_ref(),
            &addr_of,
            &generation,
            0
        )
        .await
        .unwrap()
    );
    // Evict everything else.
    let usage =
        arca_lake_metadata::ops::cache::usage(c.meta.as_ref(), arca_lake_testkit::CACHE_TIER)
            .await
            .unwrap();
    let cfg = CacheTierConfig {
        capacity_bytes: usage,
        high_watermark: 0.1,
        low_watermark: 0.0,
        ..ClusterOptions::default_cache()
    };
    let r = evict_tier(c.meta.as_ref(), &pool, &cfg, &map.groups, &addr_of)
        .await
        .unwrap();
    assert!(r.evicted >= 1 && r.delete_failures == 0, "{r:?}");
    assert_eq!(pool.stats().cleanup_refused, before);
    assert_eq!(cached(), 0, "a cache copy was left behind");
    c.shutdown().await;
}
