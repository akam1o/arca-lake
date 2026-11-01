//! Repair completion is a verified fact (RFC-008 §7): the group is `Active`
//! only when every member lists every chunk with consistent lengths and the
//! rebuilt member's shards verify; corrupt shards are never mistaken for
//! completion, and reconstructed bytes are checked against the recorded
//! chunk checksum. Also the deletion-epoch mechanism (RFC-008 §6.4).

#![allow(clippy::too_many_lines)]

use arca_lake_controller::ControlError;
use arca_lake_controller::repair::{RepairOptions, RepairStep};
use arca_lake_metadata::records::GroupState;
use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "completion";

async fn cluster() -> TestCluster {
    cluster_with(3, 2, 1).await
}

async fn cluster_with(nodes: usize, k: u16, m: u16) -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(nodes, k, m, MIB, 64 << 10)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

async fn seed(c: &TestCluster, n: u64) -> Vec<(String, Bytes)> {
    let mut out = Vec::new();
    for i in 0..n {
        let key = format!("obj{i}");
        let data = payload(usize::try_from(MIB + MIB / 2).unwrap(), 10 + i);
        s3::put(
            &c.backend,
            BUCKET,
            &key,
            data.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        out.push((key, data));
    }
    out
}

#[tokio::test]
async fn corrupt_shard_on_the_replacement_blocks_completion_until_rewritten() {
    let mut c = cluster().await;
    let objects = seed(&c, 2).await;
    let group = c.groups().await.remove(0);
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;

    // Damage the first rebuilt shard on the replacement right after it is
    // written; the completeness check must notice.
    let mut damaged = None;
    let report = c
        .repair_with(group.id, 0, new_node, async |step| {
            if let RepairStep::Chunk { index: 0, chunk_id } = step {
                assert_eq!(c.corrupt_chunk_on_node(new_node, group.id, chunk_id), 1);
                damaged = Some(chunk_id);
            }
            Ok(())
        })
        .await
        .unwrap();
    assert!(!report.complete, "{report:?}");
    assert_eq!(report.completeness.corrupt, vec![(0, 1)]);
    assert_eq!(
        report.completeness.missing, 0,
        "listing alone would have called it complete"
    );
    assert_eq!(
        c.groups().await[0].state,
        GroupState::Repairing,
        "a corrupt replacement shard keeps the group in repair"
    );

    // Re-issuing rewrites exactly the corrupt shard and completes.
    let again = c.repair(group.id, 0, new_node).await;
    assert!(again.resumed);
    assert_eq!(again.reconstructed, 1, "only the corrupt shard is rebuilt");
    assert!(again.complete, "{again:?}");
    assert_eq!(c.groups().await[0].state, GroupState::Active);
    let _ = damaged;
    // Data is intact through the replacement.
    let other = c.node_index(&group.members[1]).unwrap();
    c.stop_node(other).await;
    for (key, data) in &objects {
        assert_eq!(&s3::get(&c.backend, BUCKET, key).await.unwrap().0, data);
    }
    c.shutdown().await;
}

#[tokio::test]
async fn corrupt_source_shard_is_not_propagated_and_blocks_full_verification() {
    // 2+2: losing a member and having a corrupt shard on another still
    // leaves k = 2 good sources.
    let mut c = cluster_with(4, 2, 2).await;
    c.repair_options = RepairOptions {
        page_size: 0,
        verify_all_members: true,
    };
    let objects = seed(&c, 2).await;
    let group = c.groups().await.remove(0);
    let chunk0 = c.manifest(BUCKET, "obj0").await.chunks[0].1.chunk_id;
    // Corrupt a source member's shard of chunk 0 before the repair.
    let source = c.node_index(&group.members[1]).unwrap();
    assert_eq!(c.corrupt_chunk_on_node(source, group.id, chunk0), 1);
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;

    let report = c.repair(group.id, 0, new_node).await;
    // Reconstruction succeeded (the node reported the corrupt shard as
    // unreadable and the other members filled in) and every rebuilt chunk
    // matched its recorded checksum.
    assert_eq!(report.reconstructed, 4);
    // The node quarantined the corrupt shard when the repair read it, so
    // the completeness check sees it as missing on member 1 (a shard that is
    // corrupt but never read would show up under `corrupt` instead); either
    // way the group is not complete.
    assert!(
        !report.complete,
        "full verification sees the damaged source: {report:?}"
    );
    assert_eq!(
        report.completeness.missing + report.completeness.corrupt.len(),
        1,
        "{report:?}"
    );
    assert_eq!(c.groups().await[0].state, GroupState::Repairing);
    // The rebuilt bytes are correct: read with the corrupt member stopped.
    c.stop_node(source).await;
    for (key, data) in &objects {
        assert_eq!(&s3::get(&c.backend, BUCKET, key).await.unwrap().0, data);
    }
    c.shutdown().await;
}

#[tokio::test]
async fn completion_requires_every_member_to_answer() {
    let mut c = cluster().await;
    let _objects = seed(&c, 1).await;
    let group = c.groups().await.remove(0);
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;
    // Stop another member right before the completeness check: the group
    // must not be declared complete on partial knowledge.
    let other = c.node_index(&group.members[1]).unwrap();
    let addr = c.nodes[other].addr.to_string();
    let clients = c.clients.clone();
    let report = c
        .repair_with(group.id, 0, new_node, async |step| {
            if step == RepairStep::Reconstructed {
                // Partition the member from the controller for the check.
                clients.set_fault_hook(Some(std::sync::Arc::new({
                    let addr = addr.clone();
                    move |ctx: &arca_lake_protocol::client::RpcContext<'_>| {
                        if ctx.addr == addr {
                            arca_lake_protocol::client::FaultAction::FailBefore(
                                tonic::Code::Unavailable,
                                "partitioned".into(),
                            )
                        } else {
                            arca_lake_protocol::client::FaultAction::None
                        }
                    }
                })));
            }
            Ok(())
        })
        .await
        .unwrap();
    assert!(!report.complete, "{report:?}");
    assert_eq!(report.completeness.unavailable, vec![1]);
    assert_eq!(c.groups().await[0].state, GroupState::Repairing);
    c.clients.set_fault_hook(None);
    let again = c.repair(group.id, 0, new_node).await;
    assert!(again.complete && again.reconstructed == 0, "{again:?}");
    c.shutdown().await;
}

#[tokio::test]
async fn delete_decided_under_an_old_epoch_is_rejected_after_a_fence() {
    let c = cluster().await;
    let _objects = seed(&c, 1).await;
    let group = c.groups().await.remove(0);
    let node = c.node_index(&group.members[0]).unwrap();
    let addr = c.nodes[node].addr.to_string();
    let key = c.shard_keys(node, group.id)[0];
    let shard = ShardRef {
        group_id: group.id,
        chunk_id: key.chunk_id,
        shard_index: key.shard_index,
    };
    // A reconciler read epoch 1 and decided to delete; meanwhile a
    // membership change fenced the node to epoch 2.
    let clients = NodeClients::new(std::time::Duration::from_secs(5));
    assert_eq!(clients.update_epoch(&addr, group.id, 2).await.unwrap(), 2);
    let err = clients
        .delete_shard(&addr, shard, group.epoch)
        .await
        .unwrap_err();
    assert!(NodeClients::is_epoch_rejected(&err), "{err}");
    assert_eq!(c.shard_count(node, group.id), 2, "nothing was deleted");
    // A delete under the current epoch goes through.
    clients.delete_shard(&addr, shard, 2).await.unwrap();
    assert_eq!(c.shard_count(node, group.id), 1);
    c.shutdown().await;
}

#[tokio::test]
async fn repair_reports_corruption_instead_of_writing_mismatched_bytes() {
    // Sanity check of the error type surface: a chunk whose recorded
    // checksum cannot be met is reported as Corrupt. Here we emulate it by
    // corrupting two of three shards of a chunk (the node hides them, so
    // the repair sees an unrecoverable chunk of a live generation).
    let mut c = cluster().await;
    let _objects = seed(&c, 1).await;
    let group = c.groups().await.remove(0);
    let chunk0 = c.manifest(BUCKET, "obj0").await.chunks[0].1.chunk_id;
    for i in 1..3 {
        let n = c.node_index(&group.members[i]).unwrap();
        assert_eq!(c.corrupt_chunk_on_node(n, group.id, chunk0), 1);
    }
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;
    let err = c
        .repair_with(group.id, 0, new_node, async |_| Ok(()))
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::Unrecoverable { .. }), "{err}");
    assert_eq!(c.groups().await[0].state, GroupState::Repairing);
    assert_eq!(
        c.shard_count(new_node, group.id),
        0,
        "nothing wrong was written"
    );
    c.shutdown().await;
}
