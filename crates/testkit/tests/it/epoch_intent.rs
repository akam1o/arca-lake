//! Epoch change intents (Phase 3.11, M2 of the Phase 3.10 audit): a member
//! move that raised the group's epoch on the nodes and stopped before
//! committing the membership (a crash, an error) used to leave the nodes
//! ahead of the metadata: the group refused every write until the same
//! move happened to run again. Now any leader finds the intent and either
//! resumes the move (still wanted) or settles the epoch (membership
//! unchanged); either way the group takes writes again, and no
//! acknowledged object is lost.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arca_lake_controller::ControlError;
use arca_lake_controller::process::{ControllerConfig, spawn};
use arca_lake_controller::repair::{RepairOptions, RepairStep, replace_member_with};
use arca_lake_metadata::ops::{epoch_intent, membership};
use arca_lake_metadata::records::{GroupMember, GroupState};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const KIB: u64 = 1 << 10;
const BUCKET: &str = "epochs";

async fn cluster() -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(4, 2, 1, 128 * KIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl AsyncFnMut() -> bool) {
    let start = Instant::now();
    while !cond().await {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn ctl() -> ControllerConfig {
    let mut c = ControllerConfig::new("ctl-epochs");
    c.lease = Duration::from_secs(2);
    c.health_interval = Duration::from_millis(100);
    c.repair_interval = Duration::from_millis(100);
    c.gc_interval = Duration::from_secs(3600);
    c.reconcile_interval = Duration::from_secs(3600);
    c.failure_domain = "host".into();
    c
}

/// Raise the epoch on the nodes for a move of member 0 to the spare, then
/// stop before the commit (a crash). Returns the objects written before.
async fn interrupted_move(c: &TestCluster) -> (Vec<(String, Bytes)>, u64, GroupMember) {
    let mut objects = Vec::new();
    for i in 0..4u64 {
        let d = Bytes::from(random_bytes(usize::try_from(200 * KIB).unwrap(), 50 + i));
        let k = format!("o{i}");
        s3::put(&c.backend, BUCKET, &k, d.clone(), &PutOptions::default())
            .await
            .unwrap();
        objects.push((k, d));
    }
    let g = c.groups().await.remove(0);
    let spare = c
        .nodes
        .iter()
        .find(|n| !g.members.iter().any(|m| m.node_id == n.id))
        .unwrap()
        .id;
    let replacement = GroupMember {
        node_id: spare,
        device_id: 0,
    };
    let r = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g.id,
        0,
        replacement,
        &RepairOptions::default(),
        async |step| {
            if step == RepairStep::Fenced {
                Err(ControlError::Other(
                    "injected: crash after the fence".into(),
                ))
            } else {
                Ok(())
            }
        },
    )
    .await;
    assert!(r.is_err());
    // Nodes ahead of the metadata; the intent is open.
    let after = c.groups().await.remove(0);
    assert_eq!(after.epoch, g.epoch, "nothing committed");
    let h = c
        .clients
        .health(
            &c.nodes[c.node_index(&g.members[1]).unwrap()]
                .addr
                .to_string(),
        )
        .await
        .unwrap();
    assert!(
        h.group_epochs
            .iter()
            .any(|e| e.group_id == g.id && e.epoch == g.epoch + 1),
        "{:?}",
        h.group_epochs
    );
    assert_eq!(epoch_intent::open(c.meta.as_ref()).await.unwrap().len(), 1);
    (objects, g.epoch, replacement)
}

async fn writes_work(c: &TestCluster) -> bool {
    c.backend.refresh_map().await.is_ok()
        && s3::put(
            &c.backend,
            BUCKET,
            "probe",
            Bytes::from_static(b"probe"),
            &PutOptions::default(),
        )
        .await
        .is_ok()
}

/// No longer wanted (no request, no drain, the member healthy): the leader
/// settles the epoch with the membership unchanged; writes resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_epoch_change_is_settled_and_the_group_takes_writes_again() {
    let c = cluster().await;
    let (objects, epoch, _) = interrupted_move(&c).await;
    let members = c.groups().await.remove(0).members;
    // The group refuses writes (nodes ahead).
    assert!(
        !writes_work(&c).await,
        "a write went through with nodes ahead"
    );
    let ctl = spawn(Arc::clone(&c.meta), ctl()).await.unwrap();
    wait_until("the epoch settled", Duration::from_secs(30), async || {
        epoch_intent::open(c.meta.as_ref())
            .await
            .unwrap()
            .is_empty()
    })
    .await;
    let g = c.groups().await.remove(0);
    assert_eq!(g.epoch, epoch + 1);
    assert_eq!(g.members, members, "membership unchanged");
    assert_eq!(g.state, GroupState::Active);
    wait_until("writes again", Duration::from_secs(10), async || {
        writes_work(&c).await
    })
    .await;
    for (k, d) in &objects {
        assert_eq!(&s3::get(&c.backend, BUCKET, k).await.unwrap().0, d, "{k}");
    }
    ctl.shutdown().await;
    c.shutdown().await;
}

/// Still wanted (an operator's replacement request for that member): the
/// leader resumes the move to the recorded node and completes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_epoch_change_still_wanted_is_resumed_to_its_target() {
    let c = cluster().await;
    let g = c.groups().await.remove(0);
    let spare = c
        .nodes
        .iter()
        .find(|n| !g.members.iter().any(|m| m.node_id == n.id))
        .unwrap()
        .id;
    membership::request_replace(c.meta.as_ref(), g.id, 0, Some(spare), "op", now_ms())
        .await
        .unwrap();
    let (objects, epoch, replacement) = interrupted_move(&c).await;
    let ctl = spawn(Arc::clone(&c.meta), ctl()).await.unwrap();
    wait_until("the move completed", Duration::from_secs(60), async || {
        let g = c.groups().await.remove(0);
        g.state == GroupState::Active && g.members[0] == replacement
    })
    .await;
    let g = c.groups().await.remove(0);
    assert!(g.epoch > epoch);
    assert_eq!(
        epoch_intent::open(c.meta.as_ref()).await.unwrap(),
        Vec::new()
    );
    wait_until("writes again", Duration::from_secs(10), async || {
        writes_work(&c).await
    })
    .await;
    for (k, d) in &objects {
        assert_eq!(&s3::get(&c.backend, BUCKET, k).await.unwrap().0, d, "{k}");
    }
    ctl.shutdown().await;
    c.shutdown().await;
}
