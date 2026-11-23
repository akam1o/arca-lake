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

/// A cluster initialised at the strict schema (a `version-bump` build
/// reports schema 5 today; a normal build once the schema-5 change lands)
/// is strict from its first write: every node, given its facts as a
/// controller's first health pass does, refuses unproven writes, and the
/// gateway's writes go through. A normal build is not strict (ADR-0037
/// §5). CI's all-features step is a `version-bump` build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_initialised_at_the_strict_schema_is_strict_from_its_first_write() {
    use arca_lake_protocol::storage::v1 as pb;
    let c = cluster().await;
    // The identities a controller's first pass gives with the facts.
    c.acknowledge_identities().await;
    let strict = arca_lake_metadata::keys::SCHEMA_VERSION >= pb::WRITE_OWNERSHIP_SCHEMA;
    for (i, n) in c.nodes.iter().enumerate() {
        assert_eq!(
            c.node_service(i).write_ownership_strict(),
            strict,
            "node {}",
            n.id
        );
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
    if strict {
        assert!(
            r.unwrap_err().message().starts_with("node rejected"),
            "an unnamed write"
        );
    } else {
        r.unwrap();
    }
    if strict {
        // Nothing unproven was taken anywhere.
        for i in 0..c.nodes.len() {
            assert_eq!(c.node_service(i).writes_unproven(), 0, "node {i}");
        }
    } else {
        // Not strict: the unnamed write was taken, counted.
        assert_eq!(c.node_service(0).writes_unproven(), 1);
    }
    c.shutdown().await;
}
