//! Replacing a healthy member keeps the read path's fault tolerance
//! (Phase 3.5, R-65): while the new member is rebuilt, the node it replaces
//! still holds its shards and serves them as an alternate reader, so one
//! further failure during the move does not make objects unreadable. The
//! move still completes, and the former member's shards are reclaimed only
//! after the group is `Active` again.

use arca_lake_controller::ControlError;
use arca_lake_controller::repair::RepairStep;
use arca_lake_metadata::ops::membership;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "move";

#[tokio::test]
async fn a_failure_during_a_healthy_member_move_does_not_make_objects_unreadable() {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut objects = Vec::new();
    for i in 0..12u64 {
        let d = Bytes::from(random_bytes(usize::try_from(3 * MIB / 2).unwrap(), i));
        s3::put(
            &c.backend,
            BUCKET,
            &format!("o{i}"),
            d.clone(),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        objects.push(d);
    }
    let spare = c.add_node().await;
    let group = c
        .groups()
        .await
        .into_iter()
        .find(|g| !g.is_cache())
        .unwrap();
    let leaving = c.node_index(&group.members[0]).unwrap();
    let other = c.node_index(&group.members[1]).unwrap();
    let spare_id = c.nodes[spare].id;
    membership::request_replace(
        c.meta.as_ref(),
        group.id,
        0,
        Some(spare_id),
        "test",
        now_ms(),
    )
    .await
    .unwrap();
    // Commit the membership change, then stop before rebuilding anything.
    let stopped = c
        .repair_with(group.id, 0, spare, async |step| {
            if step == RepairStep::MembershipCommitted {
                Err(ControlError::Other(
                    "paused after the membership commit".into(),
                ))
            } else {
                Ok(())
            }
        })
        .await;
    assert!(stopped.is_err());
    c.backend.refresh_map().await.unwrap();
    // A second member fails mid-move: the new member has nothing yet, the
    // node it replaces still answers.
    c.stop_node(other).await;
    for (i, d) in objects.iter().enumerate() {
        let got = s3::get(&c.backend, BUCKET, &format!("o{i}")).await;
        assert_eq!(
            got.map(|g| g.0).ok().as_ref(),
            Some(d),
            "object o{i} unreadable during the move with one more failure"
        );
    }
    c.start_node(other).await;
    let done = c.repair(group.id, 0, spare).await;
    assert!(done.complete, "{done:?}");
    c.backend.refresh_map().await.unwrap();
    // After the move the former member's shards are reclaimed.
    c.reconcile(now_ms(), 0).await;
    assert_eq!(c.shard_count(leaving, group.id), 0);
    for (i, d) in objects.iter().enumerate() {
        assert_eq!(
            &s3::get(&c.backend, BUCKET, &format!("o{i}"))
                .await
                .unwrap()
                .0,
            d
        );
    }
    c.shutdown().await;
}
