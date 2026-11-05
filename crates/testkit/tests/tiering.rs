//! Automatic tiering between durable tiers (RFC-006 §5.8): migration as
//! copy, verify, switch, retire, with crashes before and after the switch,
//! and the controller policy driven by access records.

#![allow(clippy::too_many_lines)]

use std::collections::HashMap;
use std::time::Duration;

use arca_lake_controller::groups;
use arca_lake_metadata::ops::{cluster, object};
use arca_lake_metadata::records::{GroupMember, GroupRecord, GroupState, NodeRecord};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, now_ms, random_bytes};
use arca_lake_tiering::durable::Coders;
use arca_lake_tiering::migrate::{MigrateFault, MigrateOutcome, migrate_chunk};
use bytes::Bytes;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "tiering";

/// Three durable nodes of class `hdd` (tier `default`) plus three added
/// nodes of class `ssd` forming a second durable tier `fast-durable`.
async fn cluster() -> (TestCluster, GroupRecord) {
    let mut c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    for _ in 0..3 {
        c.add_node().await;
    }
    // Re-label the added nodes with another device class and host names
    // distinct from the first tier, then create a group on that tier.
    let nodes = cluster::list_nodes(c.meta.as_ref()).await.unwrap();
    for n in nodes.iter().skip(3) {
        cluster::register_node(
            c.meta.as_ref(),
            &n.address,
            vec![
                ("host".into(), format!("ssd-host{}", n.id)),
                ("rack".into(), "rack9".into()),
                ("device_class".into(), "ssd".into()),
            ],
            vec![0],
            now_ms(),
        )
        .await
        .unwrap();
    }
    let fast =
        groups::create_groups_for_class(c.meta.as_ref(), &c.profile, "fast-durable", "ssd", 1)
            .await
            .unwrap()
            .remove(0);
    c.backend.refresh_map().await.unwrap();
    (c, fast)
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

fn addr_of(nodes: &HashMap<u64, NodeRecord>) -> impl Fn(u64) -> Option<String> + Sync + '_ {
    move |id| nodes.get(&id).map(|n| n.address.clone())
}

#[tokio::test]
async fn migration_copies_verifies_switches_and_retires() {
    let (c, fast) = cluster().await;
    let data = payload(usize::try_from(2 * MIB + 11).unwrap(), 1);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let m = c.manifest(BUCKET, "o").await;
    let source = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == m.chunks[0].1.placement.group_id)
        .unwrap();
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let coders = Coders::default();
    let before = c.total_shards().await;

    for (index, record) in &m.chunks {
        let outcome = migrate_chunk(
            c.meta.as_ref(),
            &c.clients,
            &coders,
            &addr_of(&nodes),
            &source,
            &fast,
            &m.pointer.generation,
            *index,
            record,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            MigrateOutcome::Switched {
                from: source.id,
                to: fast.id
            }
        );
    }
    let after = c.manifest(BUCKET, "o").await;
    assert!(
        after
            .chunks
            .iter()
            .all(|(_, r)| r.placement.group_id == fast.id)
    );
    assert!(
        after
            .chunks
            .iter()
            .all(|(_, r)| r.placement.epoch == fast.epoch)
    );
    assert_eq!(
        after.pointer.generation, m.pointer.generation,
        "identity unchanged"
    );
    c.backend.refresh_map().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    // Source shards are left to reconciliation (it deletes only what it
    // listed, Phase 3.5 audit CON-5); then the total is unchanged.
    assert_eq!(c.reconcile(now_ms() + 3_600_000, 0).await, 9);
    assert_eq!(c.total_shards().await, before);
    for i in 0..3 {
        assert_eq!(c.shard_count(i, source.id), 0, "source retired");
    }
    // Migrating again is a no-op.
    let again = migrate_chunk(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of(&nodes),
        &fast,
        &fast,
        &after.pointer.generation,
        0,
        &after.chunks[0].1,
        None,
    )
    .await
    .unwrap();
    assert_eq!(again, MigrateOutcome::AlreadyThere);
    c.shutdown().await;
}

#[tokio::test]
async fn crash_before_switch_leaves_source_authoritative_and_target_reclaimable() {
    let (c, fast) = cluster().await;
    let data = payload(usize::try_from(MIB + 3).unwrap(), 2);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let m = c.manifest(BUCKET, "o").await;
    let source = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == m.chunks[0].1.placement.group_id)
        .unwrap();
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let coders = Coders::default();
    let (index, record) = &m.chunks[0];
    let err = migrate_chunk(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of(&nodes),
        &source,
        &fast,
        &m.pointer.generation,
        *index,
        record,
        Some(MigrateFault::BeforeSwitch),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("before switch"));
    let now = c.manifest(BUCKET, "o").await;
    assert_eq!(
        now.chunks[0].1.placement.group_id, source.id,
        "record unchanged"
    );
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    // Target shards exist; the crashed migration's intent keeps them from
    // reconciliation (a migration paused there may still switch, Phase 3.5
    // audit CON-1) until the intent is stale; then they are reclaimed.
    let on_target: usize = (3..6).map(|i| c.shard_count(i, fast.id)).sum();
    assert_eq!(on_target, 3);
    assert_eq!(c.reconcile(now_ms() + 3_600_000, 0).await, 0);
    let later = now_ms() + arca_lake_metadata::ops::object::MIGRATION_INTENT_TTL_MS + 1;
    assert_eq!(
        object::expire_migrations(c.meta.as_ref(), later)
            .await
            .unwrap(),
        1
    );
    assert_eq!(c.reconcile(later, 0).await, 3);
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.shutdown().await;
}

#[tokio::test]
async fn crash_after_switch_leaves_target_authoritative_and_source_reclaimable() {
    let (c, fast) = cluster().await;
    let data = payload(usize::try_from(MIB + 5).unwrap(), 3);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let m = c.manifest(BUCKET, "o").await;
    let source = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == m.chunks[0].1.placement.group_id)
        .unwrap();
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let coders = Coders::default();
    let (index, record) = &m.chunks[0];
    let err = migrate_chunk(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of(&nodes),
        &source,
        &fast,
        &m.pointer.generation,
        *index,
        record,
        Some(MigrateFault::AfterSwitch),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("after switch"));
    let now = c.manifest(BUCKET, "o").await;
    assert_eq!(now.chunks[0].1.placement.group_id, fast.id, "switched");
    c.backend.refresh_map().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    // Source shards of chunk 0 are still there but unreferenced.
    let on_source: usize = (0..3).map(|i| c.shard_count(i, source.id)).sum();
    assert_eq!(on_source, 6, "both chunks' shards still on the source");
    assert_eq!(
        c.reconcile(now_ms() + 3_600_000, 0).await,
        3,
        "chunk 0's source shards reclaimed"
    );
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.shutdown().await;
}

#[tokio::test]
async fn switch_is_refused_when_the_record_changed_or_generation_retired() {
    let (c, fast) = cluster().await;
    let data = payload(usize::try_from(MIB / 2).unwrap(), 4);
    s3::put(&c.backend, BUCKET, "o", data, &PutOptions::default())
        .await
        .unwrap();
    let m = c.manifest(BUCKET, "o").await;
    let source = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == m.chunks[0].1.placement.group_id)
        .unwrap();
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let coders = Coders::default();
    // Overwrite retires the generation before the migration switches.
    let new = payload(usize::try_from(MIB / 2).unwrap(), 5);
    s3::put(&c.backend, BUCKET, "o", new.clone(), &PutOptions::default())
        .await
        .unwrap();
    let outcome = migrate_chunk(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of(&nodes),
        &source,
        &fast,
        &m.pointer.generation,
        0,
        &m.chunks[0].1,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(outcome, MigrateOutcome::Skipped(_)), "{outcome:?}");
    let on_target: usize = (3..6).map(|i| c.shard_count(i, fast.id)).sum();
    assert_eq!(on_target, 0, "target shards cleaned up");
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, new);
    c.shutdown().await;
}

#[tokio::test]
async fn controller_policy_promotes_hot_and_demotes_cold_generations() {
    let (c, fast) = cluster().await;
    let hot = payload(usize::try_from(MIB / 2).unwrap(), 6);
    let cold = payload(usize::try_from(MIB / 2).unwrap(), 7);
    s3::put(
        &c.backend,
        BUCKET,
        "hot",
        hot.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    s3::put(
        &c.backend,
        BUCKET,
        "cold",
        cold.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    // Record accesses directly (the gateway coalesces them per second).
    let hot_gen = c.manifest(BUCKET, "hot").await.pointer.generation;
    let cold_gen = c.manifest(BUCKET, "cold").await.pointer.generation;
    object::record_access(c.meta.as_ref(), &hot_gen, now_ms(), 10)
        .await
        .unwrap();
    object::record_access(c.meta.as_ref(), &cold_gen, now_ms() - 3_600_000, 1)
        .await
        .unwrap();

    let policy = arca_lake_controller::tiering::TieringPolicy {
        tiers: vec!["default".into(), "fast-durable".into()],
        promote_reads: 5,
        promote_window: Duration::from_mins(10),
        demote_after: Duration::from_mins(30),
    };
    let report =
        arca_lake_controller::tiering::run_once(c.meta.as_ref(), &c.clients, &policy, now_ms())
            .await
            .unwrap();
    assert_eq!(report.promoted_chunks, 1, "{report:?}");
    assert_eq!(
        report.demoted_chunks, 0,
        "cold is already on the lowest tier"
    );
    assert_eq!(
        c.manifest(BUCKET, "hot").await.chunks[0]
            .1
            .placement
            .group_id,
        fast.id
    );
    c.backend.refresh_map().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "hot").await.unwrap().0, hot);

    // Later, with no reads, the hot object is demoted back.
    let report = arca_lake_controller::tiering::run_once(
        c.meta.as_ref(),
        &c.clients,
        &policy,
        now_ms() + 7_200_000,
    )
    .await
    .unwrap();
    assert_eq!(report.demoted_chunks, 1, "{report:?}");
    assert_ne!(
        c.manifest(BUCKET, "hot").await.chunks[0]
            .1
            .placement
            .group_id,
        fast.id
    );
    assert_eq!(s3::get(&c.backend, BUCKET, "hot").await.unwrap().0, hot);
    assert_eq!(s3::get(&c.backend, BUCKET, "cold").await.unwrap().0, cold);
    assert_eq!(fast.state, GroupState::Active);
    let _ = GroupMember {
        node_id: 0,
        device_id: 0,
    };
    c.shutdown().await;
}

/// Moves spread over every group of the target tier (Phase 3.4 soak
/// finding: the first group of a hash map took every move of a controller
/// process, and a leader change moved all later ones to another group).
#[tokio::test]
async fn tier_moves_spread_over_the_groups_of_the_target_tier() {
    let (c, first) = cluster().await;
    let second =
        groups::create_groups_for_class(c.meta.as_ref(), &c.profile, "fast-durable", "ssd", 1)
            .await
            .unwrap()
            .remove(0);
    c.backend.refresh_map().await.unwrap();
    let n = 40u64;
    for i in 0..n {
        let k = format!("hot{i}");
        s3::put(
            &c.backend,
            BUCKET,
            &k,
            payload(64 << 10, 900 + i),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        let g = c.manifest(BUCKET, &k).await.pointer.generation;
        object::record_access(c.meta.as_ref(), &g, now_ms(), 10)
            .await
            .unwrap();
    }
    let policy = arca_lake_controller::tiering::TieringPolicy {
        tiers: vec!["default".into(), "fast-durable".into()],
        promote_reads: 5,
        promote_window: Duration::from_mins(10),
        demote_after: Duration::from_mins(30),
    };
    let r = arca_lake_controller::tiering::run_once(c.meta.as_ref(), &c.clients, &policy, now_ms())
        .await
        .unwrap();
    assert_eq!(r.promoted_chunks, usize::try_from(n).unwrap(), "{r:?}");
    let mut by: HashMap<u64, u64> = HashMap::new();
    for i in 0..n {
        let g = c.manifest(BUCKET, &format!("hot{i}")).await.chunks[0]
            .1
            .placement
            .group_id;
        *by.entry(g).or_default() += 1;
    }
    for id in [first.id, second.id] {
        assert!(
            by.get(&id).copied().unwrap_or(0) >= n / 4,
            "group {id}: {by:?}"
        );
    }
    c.backend.refresh_map().await.unwrap();
    for i in 0..n {
        assert_eq!(
            s3::get(&c.backend, BUCKET, &format!("hot{i}"))
                .await
                .unwrap()
                .0,
            payload(64 << 10, 900 + i)
        );
    }
    c.shutdown().await;
}

/// A migration paused between writing its target and the switch keeps the
/// target through a reconciliation of the target group, and a second
/// migration of the same chunk does not run meanwhile (Phase 3.5 audit
/// CON-1, CON-2: reconciliation deleted the target as unreferenced, and a
/// losing migration's cleanup could delete the winner's shards; either
/// left the committed chunk with no shards after the switch).
#[tokio::test]
async fn a_paused_migration_keeps_its_target_and_excludes_a_second_one() {
    use arca_lake_tiering::migrate::migrate_chunk_with;
    let (c, fast) = cluster().await;
    let data = payload(usize::try_from(MIB / 2).unwrap(), 41);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let m = c.manifest(BUCKET, "o").await;
    let source = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == m.chunks[0].1.placement.group_id)
        .unwrap();
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let coders = Coders::default();
    let mut second = None;
    let outcome = migrate_chunk_with(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of(&nodes),
        &source,
        &fast,
        &m.pointer.generation,
        0,
        &m.chunks[0].1,
        None,
        async || {
            // Reconcile the target group with every guard off.
            c.reconcile(now_ms() + 3_600_000, 0).await;
            // A second migration of the same chunk in the meantime.
            second = Some(
                migrate_chunk(
                    c.meta.as_ref(),
                    &c.clients,
                    &coders,
                    &addr_of(&nodes),
                    &source,
                    &fast,
                    &m.pointer.generation,
                    0,
                    &m.chunks[0].1,
                    None,
                )
                .await
                .unwrap(),
            );
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(outcome, MigrateOutcome::Switched { .. }),
        "{outcome:?}"
    );
    assert!(
        matches!(second, Some(MigrateOutcome::Skipped(_))),
        "{second:?}"
    );
    let on_target: usize = (3..6).map(|i| c.shard_count(i, fast.id)).sum();
    assert_eq!(on_target, 3, "the switched target keeps its shards");
    c.backend.refresh_map().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.shutdown().await;
}

/// A chunk migrated A→B and back B→A: deletes decided on A's shards before
/// the return (by reconciliation or the first migration's cleanup) do not
/// remove the shards the return wrote (Phase 3.5 audit CON-5).
#[tokio::test]
async fn deletes_decided_before_a_migration_back_do_not_remove_its_shards() {
    use arca_lake_protocol::client::ShardRef;
    let (c, fast) = cluster().await;
    let data = payload(usize::try_from(MIB / 2).unwrap(), 51);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let m = c.manifest(BUCKET, "o").await;
    let a = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == m.chunks[0].1.placement.group_id)
        .unwrap();
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let coders = Coders::default();
    // What a delete of A's shards would have named when they were listed.
    let mut stale = Vec::new();
    for (i, member) in a.members.iter().enumerate() {
        let addr = nodes[&member.node_id].address.clone();
        let page = c
            .clients
            .list_shards_page_with(&addr, a.id, None, 100, false)
            .await
            .unwrap();
        let shard = ShardRef {
            group_id: a.id,
            chunk_id: m.chunks[0].1.chunk_id,
            shard_index: u16::try_from(i).unwrap(),
        };
        stale.push((addr, shard, page.sequences[&shard]));
    }
    let go = migrate_chunk(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of(&nodes),
        &a,
        &fast,
        &m.pointer.generation,
        0,
        &m.chunks[0].1,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(go, MigrateOutcome::Switched { .. }), "{go:?}");
    let moved = c.manifest(BUCKET, "o").await;
    let back = migrate_chunk(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of(&nodes),
        &fast,
        &a,
        &m.pointer.generation,
        0,
        &moved.chunks[0].1,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(back, MigrateOutcome::Switched { .. }), "{back:?}");
    // The stale deletes arrive now.
    for (addr, shard, seq) in &stale {
        let r = c.clients.delete_shard_if(addr, *shard, a.epoch, *seq).await;
        assert!(
            r.is_err(),
            "a stale delete removed a shard the return wrote"
        );
    }
    c.backend.refresh_map().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.reconcile(now_ms() + 3_600_000, 0).await;
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.shutdown().await;
}
