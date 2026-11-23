//! Repair under concurrent operations, interruptions, and membership changes
//! (RFC-004 §5.7 and the controller `repair` module documentation).

#![allow(clippy::too_many_lines)]

use arca_lake_controller::ControlError;
use arca_lake_controller::repair::RepairStep;
use arca_lake_metadata::records::GroupState;
use arca_lake_protocol::client::{NodeClients, ShardRef};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "repair";

async fn cluster(nodes: usize, k: u16, m: u16) -> TestCluster {
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

/// Write `n` objects of two chunks each; returns their payloads.
async fn seed_objects(c: &TestCluster, n: u64) -> Vec<(String, Bytes)> {
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

async fn assert_all_readable(c: &TestCluster, objects: &[(String, Bytes)]) {
    for (key, data) in objects {
        let (got, _) = s3::get(&c.backend, BUCKET, key).await.unwrap();
        assert_eq!(&got, data, "{key}");
    }
}

async fn group_of(c: &TestCluster, key: &str) -> arca_lake_metadata::records::GroupRecord {
    let id = c.manifest(BUCKET, key).await.chunks[0].1.placement.group_id;
    c.groups().await.into_iter().find(|g| g.id == id).unwrap()
}

#[tokio::test]
async fn concurrent_puts_and_gets_during_repair_keep_every_object_intact() {
    let mut c = cluster(3, 2, 1).await;
    let mut objects = seed_objects(&c, 3).await;
    let group = group_of(&c, "obj0").await;
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;

    let mut during: Vec<(String, Bytes)> = Vec::new();
    let report = c
        .repair_with(group.id, 0, new_node, async |step| {
            if let RepairStep::Chunk { index, .. } = step {
                // A writer keeps using the group while it is repairing.
                let key = format!("during{index}");
                let data = payload(900 << 10, 50 + u64::try_from(index).unwrap());
                s3::put(
                    &c.backend,
                    BUCKET,
                    &key,
                    data.clone(),
                    &PutOptions::default(),
                )
                .await
                .unwrap();
                during.push((key, data));
                // And readers see complete objects.
                let (got, _) = s3::get(&c.backend, BUCKET, "obj0").await.unwrap();
                assert_eq!(got, objects[0].1);
            }
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(report.reconstructed, 6, "two chunks x three seeded objects");
    assert!(report.complete, "{report:?}");
    assert_eq!(group_of(&c, "obj0").await.state, GroupState::Active);
    for (_, ch) in &c.manifest(BUCKET, "during0").await.chunks {
        assert_eq!(
            ch.placement.epoch, 2,
            "writes during repair carry the new epoch"
        );
    }

    objects.append(&mut during);
    assert_all_readable(&c, &objects).await;
    // The reconstructed member must carry its weight: stop another member.
    let other = c.node_index(&group.members[1]).unwrap();
    c.stop_node(other).await;
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn delete_and_gc_during_repair_leave_no_leaked_shards() {
    let mut c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 2).await;
    let group = group_of(&c, "obj0").await;
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;

    let report = c
        .repair_with(group.id, 0, new_node, async |step| {
            if let RepairStep::Chunk { index: 0, .. } = step {
                s3::delete(&c.backend, BUCKET, "obj1").await.unwrap();
                let r = c.gc(now_ms() + 120_000, 0).await;
                assert_eq!(r.collected, 1);
            }
            Ok(())
        })
        .await
        .unwrap();
    assert!(report.complete);
    // Repair may have rebuilt shards of the deleted generation after GC
    // removed them from the other members; reconciliation drops them.
    c.reconcile(now_ms() + 3_600_000, 0).await;
    let running: usize = (0..c.nodes.len())
        .filter(|&i| c.node_running(i))
        .map(|i| c.shard_count(i, group.id))
        .sum();
    assert_eq!(
        running,
        2 * 3,
        "obj0's two chunks on three members, nothing else"
    );
    let other = c.node_index(&group.members[1]).unwrap();
    c.stop_node(other).await;
    assert_all_readable(&c, &objects[..1]).await;
    c.shutdown().await;
}

#[tokio::test]
async fn controller_crash_mid_repair_resumes_from_the_cursor() {
    let mut c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 3).await;
    let group = group_of(&c, "obj0").await;
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;

    let err = c
        .repair_with(group.id, 0, new_node, async |step| match step {
            RepairStep::Chunk { index: 1, .. } => {
                Err(ControlError::Other("injected controller crash".into()))
            }
            _ => Ok(()),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::Other(_)), "{err}");
    let after_crash = group_of(&c, "obj0").await;
    assert_eq!(after_crash.state, GroupState::Repairing);
    assert_eq!(after_crash.epoch, 2);
    assert!(
        after_crash.repair_cursor.is_some(),
        "progress was persisted"
    );
    assert_eq!(c.shard_count(new_node, group.id), 2);
    // Reads work in the half-repaired state (k shards remain reachable).
    assert_all_readable(&c, &objects).await;

    // Re-issuing the same command resumes: no new epoch, the rest is built.
    let resumed = c.repair(group.id, 0, new_node).await;
    assert!(resumed.resumed);
    assert_eq!(resumed.epoch, 2);
    assert_eq!(resumed.reconstructed, 4);
    assert!(resumed.complete);
    assert_eq!(c.shard_count(new_node, group.id), 6);
    assert_eq!(group_of(&c, "obj0").await.state, GroupState::Active);
    let other = c.node_index(&group.members[1]).unwrap();
    c.stop_node(other).await;
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn membership_change_during_repair_stops_the_stale_repair_and_resumes_cleanly() {
    // 2+2 tolerates the two simultaneous replacements this test performs.
    let mut c = cluster(4, 2, 2).await;
    let objects = seed_objects(&c, 2).await;
    let group = group_of(&c, "obj0").await;
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_a = c.add_node().await;
    let new_b = c.add_node().await;

    // While repairing member 0 onto A, the operator replaces member 1 with B.
    let err = c
        .repair_with(group.id, 0, new_a, async |step| {
            if let RepairStep::Chunk { index: 0, .. } = step {
                let inner = c
                    .repair_with(group.id, 1, new_b, async |_| Ok(()))
                    .await
                    .unwrap();
                assert_eq!(inner.epoch, 3);
                assert!(
                    !inner.complete,
                    "member 0 is still missing shards: not Active yet"
                );
            }
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::EpochChanged { .. }), "{err}");
    let mid = group_of(&c, "obj0").await;
    assert_eq!(mid.state, GroupState::Repairing);
    assert_eq!(mid.epoch, 3);
    assert_eq!(c.node_index(&mid.members[1]), Some(new_b));

    // Re-issue the first repair: it resumes under epoch 3 and completes.
    let done = c.repair(group.id, 0, new_a).await;
    assert!(done.resumed);
    assert_eq!(done.epoch, 3);
    assert!(done.complete);
    let fin = group_of(&c, "obj0").await;
    assert_eq!(fin.state, GroupState::Active);
    assert_eq!(c.shard_count(new_a, group.id), 4);
    assert_eq!(c.shard_count(new_b, group.id), 4);
    // Data survives the loss of yet another original member.
    let other = c.node_index(&group.members[2]).unwrap();
    c.stop_node(other).await;
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn stale_epoch_writer_and_returning_old_member_cannot_affect_reads() {
    let mut c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 1).await;
    let group = group_of(&c, "obj0").await;
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;
    let report = c.repair(group.id, 0, new_node).await;
    assert!(report.complete);

    // The old member returns without ever having learned epoch 2.
    c.start_node(lost).await;
    // A writer of the cluster (its incarnation, naming the node it writes:
    // a strict node refuses any other, ADR-0037) at the old epoch.
    let clients = NodeClients::new(std::time::Duration::from_secs(5));
    clients.set_incarnation(c.clients.incarnation());
    for n in &c.nodes {
        clients.note_node_id(&n.addr.to_string(), n.id);
    }
    let bogus = ShardRef {
        group_id: group.id,
        chunk_id: [0; 16],
        shard_index: 0,
    };
    clients
        .put_shard(
            &c.nodes[lost].addr.to_string(),
            bogus,
            1,
            Bytes::from_static(b"stale"),
            1024,
        )
        .await
        .expect("a member that missed the fence still accepts the old epoch");
    // Current members reject it.
    let current = c
        .node_index(&group_of(&c, "obj0").await.members[1])
        .unwrap();
    let rejected = clients
        .put_shard(
            &c.nodes[current].addr.to_string(),
            bogus,
            1,
            Bytes::from_static(b"stale"),
            1024,
        )
        .await
        .unwrap_err();
    assert!(NodeClients::is_epoch_rejected(&rejected));
    // Readers never consult the former member.
    assert_all_readable(&c, &objects).await;
    // Reconciliation reclaims everything on the former member, bogus included.
    assert_eq!(c.reconcile(now_ms() + 3_600_000, 0).await, 3);
    assert_eq!(c.shard_count(lost, group.id), 0);
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}

#[tokio::test]
async fn repair_of_an_already_replaced_member_is_a_no_op() {
    let mut c = cluster(3, 2, 1).await;
    let objects = seed_objects(&c, 1).await;
    let group = group_of(&c, "obj0").await;
    let lost = c.node_index(&group.members[0]).unwrap();
    c.stop_node(lost).await;
    let new_node = c.add_node().await;
    let first = c.repair(group.id, 0, new_node).await;
    assert_eq!((first.epoch, first.reconstructed), (2, 2));
    let again = c.repair(group.id, 0, new_node).await;
    assert_eq!(again.epoch, 2, "no second epoch bump");
    assert_eq!(again.reconstructed, 0);
    assert!(again.complete && !again.resumed);
    assert_eq!(group_of(&c, "obj0").await.epoch, 2);
    assert_all_readable(&c, &objects).await;
    c.shutdown().await;
}
