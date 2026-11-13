//! Capacity reservations of member moves (Phase 3.14, ADR-0032).
//!
//! Before, a move's target was chosen on the node's free space as measured
//! when the move began: a second move started before the first had written
//! its member took the same room (both stalled at the reserve, half done),
//! and rebalancing could fill the target of a repair waiting for a node.
//!
//! Checked: concurrent reservations of one room conflict and only what
//! fits is granted; a repair counts only other repairs (repair first), a
//! planned move everything; a reservation of a finished, retargeted or
//! never-begun move counts for nothing and is swept; a second repair is
//! not started onto the room the first holds, also across a leader
//! failover; the first's reservation is released when it completes.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::ops::reservation::{self, CapacityReservation, ReservationKind, Reserved};
use arca_lake_metadata::records::GroupState;
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;

const GIB: u64 = 1 << 30;
const MIB: u64 = 1 << 20;

fn resv(
    node: u64,
    group: u64,
    epoch: u64,
    kind: ReservationKind,
    bytes: u64,
) -> CapacityReservation {
    CapacityReservation {
        node,
        group,
        epoch,
        index: 0,
        kind,
        bytes,
        written: 0,
        created_ms: now_ms(),
    }
}

/// Groups 1..=n `Repairing` with slot 0 on one node: returns (node, epochs).
async fn repairing(c: &TestCluster) -> (u64, Vec<(u64, u64)>) {
    let mut out = Vec::new();
    let mut node = 0;
    for g in c.groups().await {
        let g = cluster::update_group(c.meta.as_ref(), g.id, |g| {
            g.state = GroupState::Repairing;
            true
        })
        .await
        .unwrap();
        node = g.members[0].node_id;
        out.push((g.id, g.epoch));
    }
    (node, out)
}

/// Sixteen reservations of 1 GiB raced onto a node with 4 GiB of room:
/// exactly four are granted (one transaction reads every reservation of
/// the node and conflicts with any other that writes one).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reservations_of_one_room_grant_only_what_fits() {
    let c = TestCluster::start(ClusterOptions {
        groups: 16,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    let (node, groups) = repairing(&c).await;
    let tasks: Vec<_> = groups
        .iter()
        .map(|&(g, e)| {
            let meta = Arc::clone(&c.meta);
            tokio::spawn(async move {
                reservation::reserve(
                    meta.as_ref(),
                    &resv(node, g, e, ReservationKind::Repair, GIB),
                    4 * GIB,
                )
                .await
                .unwrap()
            })
        })
        .collect();
    let mut granted = 0;
    for t in tasks {
        if t.await.unwrap() == Reserved::Yes {
            granted += 1;
        }
    }
    assert_eq!(granted, 4);
    let held: u64 = reservation::list(c.meta.as_ref())
        .await
        .unwrap()
        .iter()
        .map(CapacityReservation::remaining)
        .sum();
    assert_eq!(held, 4 * GIB);
    c.shutdown().await;
}

/// Which reservations count: repairs before planned moves; a finished,
/// retargeted or never-begun move's reservation counts for nothing and is
/// swept; what a move wrote is no longer held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_reservations_of_current_moves_count_and_repair_comes_first() {
    let c = TestCluster::start(ClusterOptions {
        groups: 4,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    let (node, groups) = repairing(&c).await;
    let [(g1, e1), (g2, e2), (g3, e3), (g4, e4)] = groups[..] else {
        panic!("{groups:?}")
    };
    let room = 4 * GIB;
    // A planned move holds 3 GiB.
    assert_eq!(
        reservation::reserve(
            c.meta.as_ref(),
            &resv(node, g1, e1, ReservationKind::Planned, 3 * GIB),
            room
        )
        .await
        .unwrap(),
        Reserved::Yes
    );
    // Another planned move of 2 GiB does not fit beside it...
    assert!(matches!(
        reservation::reserve(c.meta.as_ref(), &resv(node, g2, e2, ReservationKind::Planned, 2 * GIB), room)
            .await
            .unwrap(),
        Reserved::No { held } if held == 3 * GIB
    ));
    // ...a repair of 2 GiB does: it does not count planned moves.
    assert_eq!(
        reservation::reserve(
            c.meta.as_ref(),
            &resv(node, g2, e2, ReservationKind::Repair, 2 * GIB),
            room
        )
        .await
        .unwrap(),
        Reserved::Yes
    );
    // A third repair of 3 GiB does not fit beside the first repair.
    assert!(matches!(
        reservation::reserve(c.meta.as_ref(), &resv(node, g3, e3, ReservationKind::Repair, 3 * GIB), room)
            .await
            .unwrap(),
        Reserved::No { held } if held == 2 * GIB
    ));
    // The repair wrote 1.5 GiB: only what is still to come is held.
    reservation::note_written(c.meta.as_ref(), node, g2, e2, 3 * GIB / 2)
        .await
        .unwrap();
    // A note of an epoch before the reservation's changes nothing.
    reservation::note_written(c.meta.as_ref(), node, g2, e2 - 1, GIB)
        .await
        .unwrap();
    assert!(matches!(
        reservation::reserve(c.meta.as_ref(), &resv(node, g3, e3, ReservationKind::Repair, 4 * GIB), room)
            .await
            .unwrap(),
        Reserved::No { held } if held == GIB / 2
    ));
    assert_eq!(
        reservation::reserve(
            c.meta.as_ref(),
            &resv(node, g3, e3, ReservationKind::Repair, 7 * GIB / 2),
            room
        )
        .await
        .unwrap(),
        Reserved::Yes
    );
    // A reservation of an epoch the group never reached (a move that never
    // began, or a former leader's) counts for nothing.
    reservation::clear(c.meta.as_ref(), node, g3).await.unwrap();
    assert_eq!(
        reservation::reserve(
            c.meta.as_ref(),
            &resv(node, g4, e4 + 1, ReservationKind::Repair, 4 * GIB),
            u64::MAX
        )
        .await
        .unwrap(),
        Reserved::Yes
    );
    assert_eq!(
        reservation::reserve(
            c.meta.as_ref(),
            &resv(node, g3, e3, ReservationKind::Repair, 7 * GIB / 2),
            room
        )
        .await
        .unwrap(),
        Reserved::Yes,
        "a reservation that does not hold was counted"
    );
    // The first group's move completed; the second's moved to another
    // node: theirs count no more, and the sweep removes them with g4's.
    cluster::update_group(c.meta.as_ref(), g1, |g| {
        g.state = GroupState::Active;
        true
    })
    .await
    .unwrap();
    let other = c.groups().await[1].members[1].node_id;
    cluster::update_group(c.meta.as_ref(), g2, |g| {
        g.members[0].node_id = other;
        true
    })
    .await
    .unwrap();
    assert_eq!(reservation::sweep(c.meta.as_ref()).await.unwrap(), 3);
    let left: Vec<u64> = reservation::list(c.meta.as_ref())
        .await
        .unwrap()
        .iter()
        .map(|r| r.group)
        .collect();
    assert_eq!(left, vec![g3]);
    c.shutdown().await;
}

/// Two groups on the same three nodes lose a member to one node failure;
/// the one spare (the only target) has room for one member. The
/// first repair reserves it (in attempts of one chunk); the second is not
/// started onto it while the first holds it, by this leader nor by the
/// next one after a failover; the first completes and its reservation is
/// released; the second waits for capacity, and the spare's reserve holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_repair_is_not_started_onto_room_the_first_holds() {
    use arca_lake_controller::process::{ControllerConfig, spawn};
    let mut c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, "resv").await.unwrap();
    for i in 0..24u64 {
        let d = Bytes::from(random_bytes(usize::try_from(MIB).unwrap(), 300 + i));
        s3::put(
            &c.backend,
            "resv",
            &format!("o{i}"),
            d,
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    let groups = c.groups().await;
    assert_eq!(groups.len(), 2);
    let lost = groups[0].members[0];
    assert!(
        groups[1].members.iter().any(|m| m.node_id == lost.node_id),
        "set-up: the groups share their nodes"
    );
    // Room for either member with the margin, not for both: free =
    // needed(larger) + smaller / 2.
    let mut needs = Vec::new();
    for g in &groups {
        let other = g
            .members
            .iter()
            .find(|m| m.node_id != lost.node_id)
            .unwrap();
        let oi = c.node_index(other).unwrap();
        needs.push(c.shard_keys(oi, g.id).len() as u64 * (MIB / 2));
    }
    let (small, large) = (needs[0].min(needs[1]), needs[0].max(needs[1]));
    let want = arca_lake_controller::capacity::needed(large) + small / 2;
    let spare = c.add_node().await;
    let spare_id = c.nodes[spare].id;
    let mut cap = want + 256 * MIB;
    for _ in 0..3 {
        c.set_node_capacity(spare, Some(cap)).await;
        let a = c
            .node_service(spare)
            .device_capacity()
            .unwrap()
            .available_bytes;
        cap = (cap + want).saturating_sub(a);
    }
    c.set_node_capacity(spare, Some(cap)).await;
    let a = c
        .node_service(spare)
        .device_capacity()
        .unwrap()
        .available_bytes;
    assert!(
        a >= arca_lake_controller::capacity::needed(large)
            && a < arca_lake_controller::capacity::needed(small) + large,
        "set-up: free {a} for members of {small} and {large}"
    );
    let config = |id: &str| {
        let mut cfg = ControllerConfig::new(id);
        cfg.lease = Duration::from_secs(2);
        cfg.health_interval = Duration::from_millis(100);
        cfg.repair_interval = Duration::from_millis(200);
        cfg.unhealthy_after = Duration::from_secs(2);
        cfg.auto_repair = true;
        cfg.gc_interval = Duration::from_secs(3600);
        cfg.reconcile_interval = Duration::from_secs(3600);
        cfg.failure_domain = "host".into();
        cfg.failure_repair_chunks_per_pass = 1;
        cfg
    };
    let li = c.node_index(&lost).unwrap();
    c.stop_node(li).await;
    let ctl = spawn(Arc::clone(&c.meta), config("ctl-r1")).await.unwrap();
    let on_spare = |gs: &[arca_lake_metadata::records::GroupRecord]| {
        gs.iter()
            .filter(|g| g.members.iter().any(|m| m.node_id == spare_id))
            .map(|g| g.id)
            .collect::<Vec<_>>()
    };
    let t = Instant::now();
    let first = loop {
        let r = reservation::list(c.meta.as_ref()).await.unwrap();
        if let Some(r) = r.iter().find(|r| r.node == spare_id) {
            break r.group;
        }
        assert!(t.elapsed() < Duration::from_secs(30), "no reservation");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    // Through the first repair, with a failover at its start: never both
    // groups on the spare.
    let mut ctl = Some(ctl);
    let mut failed_over = false;
    let t = Instant::now();
    loop {
        let gs = c.groups().await;
        let on = on_spare(&gs);
        assert!(on.len() <= 1, "both repairs onto one room: {on:?}");
        if !failed_over {
            ctl.take().unwrap().shutdown().await;
            ctl = Some(spawn(Arc::clone(&c.meta), config("ctl-r2")).await.unwrap());
            failed_over = true;
        }
        let g1 = gs.iter().find(|g| g.id == first).unwrap();
        if g1.state == GroupState::Active {
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(120),
            "first repair never completed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(failed_over);
    // Released once complete; the second group never went onto the spare.
    let t = Instant::now();
    while reservation::list(c.meta.as_ref())
        .await
        .unwrap()
        .iter()
        .any(|r| r.group == first)
    {
        assert!(t.elapsed() < Duration::from_secs(30), "reservation leaked");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let gs = c.groups().await;
    assert_eq!(on_spare(&gs), vec![first]);
    let dev = c.node_service(spare).device_capacity().unwrap();
    assert!(dev.available_bytes >= dev.repair_floor_bytes, "{dev:?}");
    ctl.take().unwrap().shutdown().await;
    c.shutdown().await;
}

/// Review (Phase 3.14): a move whose membership change is open (its epoch
/// intent recorded, not committed) holds its reservation: another move is
/// not granted the same room before the change commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_under_way_keeps_its_reservation() {
    use arca_lake_metadata::ops::epoch_intent::{self, EpochIntent};
    use arca_lake_metadata::records::GroupMember;
    let c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    let gs = c.groups().await;
    let (a, b) = (&gs[0], &gs[1]);
    let node = 999_999;
    // A: Active at its epoch, the change to `node` in slot 0 open.
    assert_eq!(
        epoch_intent::begin(
            c.meta.as_ref(),
            &EpochIntent {
                group: a.id,
                from_epoch: a.epoch,
                index: 0,
                replacement: GroupMember {
                    node_id: node,
                    device_id: 0,
                },
                since_ms: now_ms(),
            },
        )
        .await
        .unwrap(),
        epoch_intent::Begin::Recorded
    );
    assert_eq!(
        reservation::reserve(
            c.meta.as_ref(),
            &resv(node, a.id, a.epoch + 1, ReservationKind::Repair, 3 * GIB),
            4 * GIB
        )
        .await
        .unwrap(),
        Reserved::Yes
    );
    let b = cluster::update_group(c.meta.as_ref(), b.id, |g| {
        g.state = GroupState::Repairing;
        true
    })
    .await
    .unwrap();
    assert!(matches!(
        reservation::reserve(c.meta.as_ref(), &resv(node, b.id, b.epoch, ReservationKind::Repair, 2 * GIB), 4 * GIB)
            .await
            .unwrap(),
        Reserved::No { held } if held == 3 * GIB
    ));
    // The sweep keeps it while the change is open; the change settled
    // (the epoch raised, membership unchanged: the move did not happen),
    // it no longer counts and goes.
    assert_eq!(reservation::sweep(c.meta.as_ref()).await.unwrap(), 0);
    let open = epoch_intent::open(c.meta.as_ref()).await.unwrap();
    epoch_intent::settle(c.meta.as_ref(), &open[0])
        .await
        .unwrap();
    assert_eq!(reservation::sweep(c.meta.as_ref()).await.unwrap(), 1);
    c.shutdown().await;
}

/// Review (Phase 3.14): what a walk finds already on the target is not
/// counted as written: a walk started again from the beginning (as after a
/// completeness check that reset the cursor) releases nothing twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_walk_started_again_releases_nothing_twice() {
    use arca_lake_controller::repair::{RepairOptions, replace_member_with};
    use arca_lake_metadata::records::GroupMember;
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(4, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, "twice").await.unwrap();
    for i in 0..12u64 {
        let d = Bytes::from(random_bytes(usize::try_from(MIB).unwrap(), 800 + i));
        s3::put(
            &c.backend,
            "twice",
            &format!("o{i}"),
            d,
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    let g = c.groups().await.remove(0);
    let li = c.node_index(&g.members[0]).unwrap();
    c.stop_node(li).await;
    let spare = c
        .nodes
        .iter()
        .map(|n| n.id)
        .find(|id| !g.members.iter().any(|m| m.node_id == *id))
        .unwrap();
    let member = GroupMember {
        node_id: spare,
        device_id: 0,
    };
    let opts = RepairOptions {
        max_chunks: 5,
        ..RepairOptions::default()
    };
    let r1 = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g.id,
        0,
        member,
        &opts,
        async |_| Ok(()),
    )
    .await
    .unwrap();
    assert_eq!(r1.reconstructed, 5);
    assert_eq!(r1.target_bytes, 5 * (MIB / 2));
    // The cursor back to the start: the next walk finds the five there.
    cluster::update_group(c.meta.as_ref(), g.id, |g| {
        if let Some(cur) = g.repair_cursor.as_mut() {
            cur.chunk_id = [0; 16];
        }
        true
    })
    .await
    .unwrap();
    let r2 = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g.id,
        0,
        member,
        &opts,
        async |_| Ok(()),
    )
    .await
    .unwrap();
    assert!(r2.skipped >= 5, "{r2:?}");
    assert_eq!(
        r2.target_bytes,
        r2.reconstructed as u64 * (MIB / 2),
        "{r2:?}"
    );
    c.shutdown().await;
}

/// Review (Phase 3.14): a later epoch change of the group (a second
/// member's move, a settle) keeps the reservation of the move still
/// running onto its slot's node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_epoch_of_the_group_keeps_the_reservation() {
    let c = TestCluster::start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    let (node, groups) = repairing(&c).await;
    let [(g1, e1), (g2, e2)] = groups[..] else {
        panic!("{groups:?}")
    };
    assert_eq!(
        reservation::reserve(
            c.meta.as_ref(),
            &resv(node, g1, e1, ReservationKind::Repair, 3 * GIB),
            4 * GIB
        )
        .await
        .unwrap(),
        Reserved::Yes
    );
    cluster::update_group(c.meta.as_ref(), g1, |g| {
        g.epoch += 1;
        true
    })
    .await
    .unwrap();
    assert!(matches!(
        reservation::reserve(c.meta.as_ref(), &resv(node, g2, e2, ReservationKind::Repair, 2 * GIB), 4 * GIB)
            .await
            .unwrap(),
        Reserved::No { held } if held == 3 * GIB
    ));
    assert_eq!(reservation::sweep(c.meta.as_ref()).await.unwrap(), 0);
    c.shutdown().await;
}
