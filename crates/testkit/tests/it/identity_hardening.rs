//! Node identity hardening of manual operations (Phase 3.13, R-105): a
//! manual close of the delete fence (`admin delete-fence close`, `recovery
//! begin`) counts an acknowledgement only from the disk the metadata
//! records at that address; adopting an identity needs the facts role; a
//! node never adopts an identity for another node id than its
//! certificate's.

use std::time::Duration;

use arca_lake_controller::delete_fence::{IdentityCheck, close_and_push};
use arca_lake_metadata::ops::node_lifecycle;
use arca_lake_protocol::client::NodeClients;
use arca_lake_protocol::storage::v1::HealthRequest;
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms};
use bytes::Bytes;

async fn adopt(c: &TestCluster, i: usize, node_id: u64, token: [u8; 16]) {
    c.clients
        .health_with(
            &c.nodes[i].addr.to_string(),
            HealthRequest {
                node_id,
                node_identity: Bytes::copy_from_slice(&token),
                adopt_identity: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

/// Three disks: one holding another node's identity (a disk moved, an id
/// reused), one holding another registration of its own id (metadata
/// restored from before it), one holding nothing yet. The close does not
/// count the first two as acknowledging, and lists the third as
/// unverified. Phase 3.12 counted all three.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_manual_close_counts_only_the_disk_the_metadata_records() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(4, 2, 1, 1 << 20, 0)
    })
    .await;
    let ids: Vec<u64> = c.nodes.iter().map(|n| n.id).collect();
    // Node 0's disk holds node 1's id.
    adopt(&c, 0, ids[1], [0x55; 16]).await;
    // Node 1: a recorded (not yet acknowledged) identity; its disk holds
    // another token of the same id.
    let rec = node_lifecycle::ensure_identity(c.meta.as_ref(), ids[1])
        .await
        .unwrap();
    assert!(!rec.acknowledged);
    adopt(&c, 1, ids[1], [0x66; 16]).await;
    // Node 2: recorded and held alike.
    let rec2 = node_lifecycle::ensure_identity(c.meta.as_ref(), ids[2])
        .await
        .unwrap();
    adopt(&c, 2, ids[2], rec2.token).await;
    // Node 3 holds nothing.
    let closed = close_and_push(c.meta.as_ref(), &c.clients, "test", false, now_ms())
        .await
        .unwrap();
    let missing = closed.not_acknowledged();
    assert!(missing.contains(&ids[0]), "{closed:?}");
    assert!(missing.contains(&ids[1]), "{closed:?}");
    assert!(!missing.contains(&ids[2]), "{closed:?}");
    assert!(!missing.contains(&ids[3]), "{closed:?}");
    let by_id = |id: u64| {
        closed
            .nodes
            .iter()
            .find(|n| n.node_id == id)
            .unwrap()
            .identity
            .clone()
    };
    assert!(matches!(by_id(ids[0]), IdentityCheck::Mismatch(_)));
    assert!(matches!(by_id(ids[1]), IdentityCheck::Mismatch(_)));
    assert_eq!(by_id(ids[2]), IdentityCheck::Verified);
    assert!(matches!(by_id(ids[3]), IdentityCheck::Unverifiable(_)));
    // Independent review: once the metadata says node 3 acknowledged an
    // identity, its disk holding none is another disk.
    node_lifecycle::set_identity(c.meta.as_ref(), ids[3], [0x11; 16])
        .await
        .unwrap();
    let closed = close_and_push(c.meta.as_ref(), &c.clients, "test", false, now_ms())
        .await
        .unwrap();
    assert!(closed.not_acknowledged().contains(&ids[3]), "{closed:?}");
    c.shutdown().await;
}

/// Over mutual TLS: a gateway may ask a node's health, not make it adopt
/// an identity; a controller may, but not for another node id than the
/// node's certificate names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adopting_an_identity_needs_the_facts_role_and_the_certificates_node_id() {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        tls: true,
        ..ClusterOptions::rs(3, 2, 1, 1 << 20, 0)
    })
    .await;
    let addr = c.nodes[0].addr.to_string();
    let id = c.nodes[0].id;
    let gw = NodeClients::new(Duration::from_secs(5)).with_tls(c.gateway_tls());
    let req = |node_id: u64| HealthRequest {
        node_id,
        node_identity: Bytes::from_static(&[0x42; 16]),
        adopt_identity: true,
        ..Default::default()
    };
    let e = gw.health_with(&addr, req(id)).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied, "{e}");
    assert!(
        gw.health(&addr).await.is_ok(),
        "a plain health still answers"
    );
    // The controller role, another node id: nothing adopted.
    let h = c.clients.health_with(&addr, req(id + 1000)).await.unwrap();
    assert!(h.identity_mismatch);
    assert_eq!(c.clients.health(&addr).await.unwrap().identity_node_id, 0);
    // Its own id: adopted.
    let h = c.clients.health_with(&addr, req(id)).await.unwrap();
    assert!(!h.identity_mismatch);
    assert_eq!(c.clients.health(&addr).await.unwrap().identity_node_id, id);
    c.shutdown().await;
}
