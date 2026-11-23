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
    cluster_on(TestCluster::start(options()).await).await
}

/// The options of [`cluster`] (also on FoundationDB, `tiering_fdb`).
pub(crate) fn options() -> ClusterOptions {
    ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    }
}

/// [`cluster`] on any store.
pub(crate) async fn cluster_on<S: arca_lake_metadata::KvStore + 'static>(
    mut c: TestCluster<S>,
) -> (TestCluster<S>, GroupRecord) {
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

/// Phase 4.0: a gateway with access records off writes none on GET (the
/// default records one per generation, for tiering).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_without_access_records_writes_none_on_get() {
    use arca_lake_metadata::ops::object;
    for record in [true, false] {
        let c = TestCluster::start(ClusterOptions {
            record_access: record,
            ..ClusterOptions::rs(3, 2, 1, 1 << 20, 0)
        })
        .await;
        s3::create_bucket(&c.backend, "acc").await.unwrap();
        s3::put(
            &c.backend,
            "acc",
            "k",
            payload(4096, 1),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        let generation = c.manifest("acc", "k").await.pointer.generation;
        let _ = s3::get(&c.backend, "acc", "k").await.unwrap();
        // The record is written by a spawned task.
        let mut found = None;
        for _ in 0..50 {
            found = object::access_of(c.meta.as_ref(), &generation)
                .await
                .unwrap();
            if found.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(found.is_some(), record, "record_access {record}: {found:?}");
        c.shutdown().await;
    }
}

/// Phase 4.1 (ADR-0034 decision 5): an object in an upper tier whose
/// reads were never recorded (never read, or every read lost) counts as
/// touched when it was written: not demoted before `demote_after` from
/// its write (earlier, a missing record read as time 0 and the first pass
/// demoted it).
#[tokio::test]
async fn an_object_without_an_access_record_is_not_demoted_before_demote_after() {
    use arca_lake_metadata::kv::{KvStore as _, Txn as _};
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
    let generation = c.manifest(BUCKET, "o").await.pointer.generation;
    object::record_access(c.meta.as_ref(), &generation, now_ms(), 10)
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
    // Its record lost: as if never read.
    let mut tx = c.meta.begin().await.unwrap();
    tx.clear(&arca_lake_metadata::keys::access(&generation));
    tx.commit().await.unwrap();
    assert_eq!(
        object::access_of(c.meta.as_ref(), &generation)
            .await
            .unwrap(),
        None
    );
    // Ten minutes after its write: kept where it is.
    let report = arca_lake_controller::tiering::run_once(
        c.meta.as_ref(),
        &c.clients,
        &policy,
        now_ms() + 600_000,
    )
    .await
    .unwrap();
    assert_eq!(report.demoted_chunks, 0, "{report:?}");
    assert_eq!(
        c.manifest(BUCKET, "o").await.chunks[0].1.placement.group_id,
        fast.id
    );
    // Past `demote_after` from its write: demoted.
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
        c.manifest(BUCKET, "o").await.chunks[0].1.placement.group_id,
        fast.id
    );
    c.backend.refresh_map().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.shutdown().await;
}

/// Phase 4.1 (ADR-0034): GETs through the gateway are recorded in batches:
/// the records hold every read, in far fewer commits than GETs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gets_are_recorded_exactly_in_few_batched_commits() {
    let c = TestCluster::start(ClusterOptions::rs(3, 2, 1, MIB, 0)).await;
    s3::create_bucket(&c.backend, "acc").await.unwrap();
    let mut gens = Vec::new();
    for i in 0..50u64 {
        let key = format!("k{i}");
        s3::put(
            &c.backend,
            "acc",
            &key,
            payload(1024, i),
            &PutOptions::default(),
        )
        .await
        .unwrap();
        gens.push(c.manifest("acc", &key).await.pointer.generation);
    }
    let before = c.backend.access_stats();
    for _ in 0..4 {
        for i in 0..50u64 {
            let _ = s3::get(&c.backend, "acc", &format!("k{i}")).await.unwrap();
        }
    }
    // Every read written within a few flush intervals (100 ms here).
    let mut done = false;
    for _ in 0..100 {
        let s = c.backend.access_stats();
        if s.flushed_reads - before.flushed_reads >= 200 {
            done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let s = c.backend.access_stats();
    assert!(done, "{s:?}");
    for g in &gens {
        let r = object::access_of(c.meta.as_ref(), g)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.count, 4, "{r:?}");
    }
    let commits = s.flushes - before.flushes;
    assert!(commits * 10 <= 200, "{commits} commits for 200 GETs");
    assert_eq!(
        s.dropped_full + s.dropped_error + s.dropped_stale + s.uncertain,
        0,
        "{s:?}"
    );
    c.shutdown().await;
}

/// Phase 4.1 (ADR-0034 decision 4): a graceful stop of the gateway writes
/// the reads still pending; a gateway started after it records anew.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_stop_flushes_pending_reads_and_a_restart_records_anew() {
    let mut c = TestCluster::start(ClusterOptions {
        access: arca_lake_gateway::cluster::access::AccessConfig {
            flush_interval: Duration::from_secs(3600),
            ..Default::default()
        },
        ..ClusterOptions::rs(3, 2, 1, MIB, 0)
    })
    .await;
    s3::create_bucket(&c.backend, "acc").await.unwrap();
    s3::put(
        &c.backend,
        "acc",
        "k",
        payload(1024, 1),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let generation = c.manifest("acc", "k").await.pointer.generation;
    for _ in 0..3 {
        let _ = s3::get(&c.backend, "acc", "k").await.unwrap();
    }
    assert_eq!(
        object::access_of(c.meta.as_ref(), &generation)
            .await
            .unwrap(),
        None
    );
    c.backend.shutdown().await;
    assert_eq!(
        object::access_of(c.meta.as_ref(), &generation)
            .await
            .unwrap()
            .map(|r| r.count),
        Some(3)
    );
    c.restart_backend().await.unwrap();
    let _ = s3::get(&c.backend, "acc", "k").await.unwrap();
    c.backend.shutdown().await;
    assert_eq!(
        object::access_of(c.meta.as_ref(), &generation)
            .await
            .unwrap()
            .map(|r| r.count),
        Some(4)
    );
    c.shutdown().await;
}

fn fenced_policy() -> arca_lake_controller::tiering::TieringPolicy {
    arca_lake_controller::tiering::TieringPolicy {
        tiers: vec!["default".into(), "fast-durable".into()],
        promote_reads: 5,
        promote_window: Duration::from_mins(10),
        demote_after: Duration::from_mins(30),
    }
}

/// A tiering record enabled and acknowledged (term 1) at `at`, as a leader
/// leaves it: returns its fence with `margin_ms`.
async fn enabled_record<S: arca_lake_metadata::KvStore + 'static>(
    c: &TestCluster<S>,
    at: u64,
    margin_ms: u64,
) -> arca_lake_controller::tiering::PolicyFence {
    use arca_lake_metadata::ops::tiering::{self, AdminChange};
    let p = fenced_policy().to_record();
    let r = tiering::admin_change(c.meta.as_ref(), &AdminChange::Enable(Some(p)), at)
        .await
        .unwrap();
    let r = tiering::acknowledge(c.meta.as_ref(), 1, [9u8; 16], r.generation, at)
        .await
        .unwrap()
        .unwrap();
    arca_lake_controller::tiering::PolicyFence {
        generation: r.generation,
        grace_from_ms: r.grace_from_ms,
        promotion_from_ms: r.promotion_from_ms,
        margin_ms,
    }
}

/// ADR-0035 §10.4: under a tiering record, counts from before the
/// promotion cutoff do not promote, and a generation is not demoted before
/// `demote_after + margin` from the grace start, whatever its record.
#[tokio::test]
async fn under_the_tiering_record_old_counts_do_not_promote_and_demotion_waits_for_the_grace() {
    let (c, fast) = cluster().await;
    under_the_tiering_record_old_counts_do_not_promote_and_demotion_waits_for_the_grace_on(c, fast)
        .await;
}

/// [`under_the_tiering_record_old_counts_do_not_promote_and_demotion_waits_for_the_grace`] on any store.
pub(crate) async fn under_the_tiering_record_old_counts_do_not_promote_and_demotion_waits_for_the_grace_on<
    S: arca_lake_metadata::KvStore + 'static,
>(
    c: TestCluster<S>,
    fast: GroupRecord,
) {
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
    let generation = c.manifest(BUCKET, "o").await.pointer.generation;
    let t0 = now_ms();
    let margin = 120_000;
    let fence = enabled_record(&c, t0, margin).await;
    let run = async |at: u64| {
        arca_lake_controller::tiering::run_once_bounded_fenced(
            c.meta.as_ref(),
            &c.clients,
            &fenced_policy(),
            at,
            0,
            Some(fence),
        )
        .await
        .unwrap()
    };
    // Ten reads, all before the cutoff: no promotion.
    object::record_access(c.meta.as_ref(), &generation, t0 - 1_000, 10)
        .await
        .unwrap();
    let report = run(t0 + 1_000).await;
    assert_eq!(
        report.promoted_chunks, 0,
        "an old count promoted: {report:?}"
    );
    // Reads after the cutoff promote (a fresh record).
    object::reset_access(c.meta.as_ref(), &generation, t0)
        .await
        .unwrap();
    object::record_access(c.meta.as_ref(), &generation, t0 + 2_000, 6)
        .await
        .unwrap();
    let report = run(t0 + 3_000).await;
    assert_eq!(report.promoted_chunks, 1, "{report:?}");
    assert_eq!(
        c.manifest(BUCKET, "o").await.chunks[0].1.placement.group_id,
        fast.id
    );
    // The promotion reset its record (last read t0 + 3 s). Past
    // `demote_after` from there but within the grace margin: kept.
    let demote = 1_800_000;
    let report = run(t0 + 3_000 + demote + 1_000).await;
    assert_eq!(
        report.demoted_chunks, 0,
        "demoted within the grace: {report:?}"
    );
    // Past `demote_after` from the grace start plus the margin: demoted.
    let report = run(t0 + margin + demote + 5_000).await;
    assert_eq!(report.demoted_chunks, 1, "{report:?}");
    c.backend.refresh_map().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.shutdown().await;
}

/// ADR-0035 §10.1: a pass decided under a tiering generation that has
/// changed switches nothing, stops, and resets no access record; a move
/// without a policy fence (capacity rebalancing) is not affected, even
/// with tiering disabled.
#[tokio::test]
async fn a_changed_policy_refuses_the_pass_but_not_rebalancing() {
    let (c, fast) = cluster().await;
    a_changed_policy_refuses_the_pass_but_not_rebalancing_on(c, fast).await;
}

/// [`a_changed_policy_refuses_the_pass_but_not_rebalancing`] on any store.
pub(crate) async fn a_changed_policy_refuses_the_pass_but_not_rebalancing_on<
    S: arca_lake_metadata::KvStore + 'static,
>(
    c: TestCluster<S>,
    fast: GroupRecord,
) {
    use arca_lake_metadata::kv::Txn as _;
    use arca_lake_metadata::ops::tiering::{self, AdminChange};
    let data = payload(usize::try_from(MIB / 2).unwrap(), 52);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let generation = c.manifest(BUCKET, "o").await.pointer.generation;
    let t0 = now_ms();
    let fence = enabled_record(&c, t0 - 10_000, 0).await;
    object::record_access(c.meta.as_ref(), &generation, t0, 10)
        .await
        .unwrap();
    // The policy changes after the pass's terms were read.
    tiering::admin_change(
        c.meta.as_ref(),
        &AdminChange::SetPolicy(fenced_policy().to_record()),
        t0,
    )
    .await
    .unwrap();
    let report = arca_lake_controller::tiering::run_once_bounded_fenced(
        c.meta.as_ref(),
        &c.clients,
        &fenced_policy(),
        t0 + 1_000,
        0,
        Some(fence),
    )
    .await
    .unwrap();
    assert!(report.policy_changed, "{report:?}");
    assert_eq!(report.promoted_chunks, 0, "{report:?}");
    let manifest = c.manifest(BUCKET, "o").await;
    assert_ne!(manifest.chunks[0].1.placement.group_id, fast.id);
    assert_eq!(
        object::access_of(c.meta.as_ref(), &generation)
            .await
            .unwrap()
            .map(|r| r.count),
        Some(10),
        "the access record was reset"
    );
    let on_target: usize = (3..6).map(|i| c.shard_count(i, fast.id)).sum();
    assert_eq!(on_target, 0, "target shards left behind");
    // Tiering disabled (at schema 4): a move without a fence still switches.
    let mut tx = c.meta.begin().await.unwrap();
    tx.set(
        &arca_lake_metadata::keys::schema_write(),
        &tiering::TIERING_RECORD_SCHEMA.to_le_bytes(),
    );
    tx.commit().await.unwrap();
    tiering::admin_change(c.meta.as_ref(), &AdminChange::Disable, t0)
        .await
        .unwrap();
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let groups = cluster::list_groups(c.meta.as_ref()).await.unwrap();
    let (index, record) = manifest.chunks[0].clone();
    let source = groups
        .iter()
        .find(|g| g.id == record.placement.group_id)
        .unwrap()
        .clone();
    let outcome = migrate_chunk(
        c.meta.as_ref(),
        &c.clients,
        &Coders::default(),
        &addr_of(&nodes),
        &source,
        &fast,
        &generation,
        index,
        &record,
        None,
    )
    .await
    .unwrap();
    assert!(
        matches!(outcome, MigrateOutcome::Switched { .. }),
        "{outcome:?}"
    );
    c.backend.refresh_map().await.unwrap();
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, data);
    c.shutdown().await;
}

/// ADR-0035 §10.3: a gateway stops recording reads once its refresh
/// confirms tiering disabled by the current leader, records again within a
/// refresh of tiering being enabled, and records after a restart until its
/// first refresh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_records_reads_only_while_tiering_is_not_confirmed_disabled() {
    use arca_lake_metadata::ops::{leader, tiering};
    let mut c = TestCluster::start(ClusterOptions::rs(3, 2, 1, MIB, 0)).await;
    s3::create_bucket(&c.backend, "acc").await.unwrap();
    s3::put(
        &c.backend,
        "acc",
        "k",
        payload(1024, 1),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    // A leader holding a long lease, and the record disabled and acknowledged
    // by it (the store is at schema 4).
    let l = leader::try_acquire(c.meta.as_ref(), "ctl-test", now_ms(), 600_000)
        .await
        .unwrap()
        .unwrap();
    tiering::admin_change(
        c.meta.as_ref(),
        &tiering::AdminChange::Enable(Some(fenced_policy().to_record())),
        now_ms(),
    )
    .await
    .unwrap();
    let r = tiering::admin_change(c.meta.as_ref(), &tiering::AdminChange::Disable, now_ms())
        .await
        .unwrap();
    tiering::acknowledge(
        c.meta.as_ref(),
        l.token,
        c.incarnation(),
        r.generation,
        now_ms(),
    )
    .await
    .unwrap();
    // Within a couple of refreshes the gateway stops recording.
    let mut stopped = false;
    for _ in 0..100 {
        if !c.backend.access_stats().recording {
            stopped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        stopped,
        "the gateway kept recording: {:?}",
        c.backend.access_stats()
    );
    let before = c.backend.access_stats();
    for _ in 0..5 {
        let _ = s3::get(&c.backend, "acc", "k").await.unwrap();
    }
    let s = c.backend.access_stats();
    assert_eq!(
        (s.reads - before.reads, s.unrecorded - before.unrecorded),
        (0, 5),
        "{s:?}"
    );
    // A restarted gateway records until its first refresh.
    c.restart_backend().await.unwrap();
    assert!(
        c.backend.access_stats().recording,
        "recording off before any refresh"
    );
    // Enabled again: recording resumes within a refresh.
    tiering::admin_change(
        c.meta.as_ref(),
        &tiering::AdminChange::Enable(None),
        now_ms(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(c.backend.access_stats().recording);
    let before = c.backend.access_stats();
    let _ = s3::get(&c.backend, "acc", "k").await.unwrap();
    assert_eq!(c.backend.access_stats().reads - before.reads, 1);
    c.shutdown().await;
}

/// ADR-0035 §10.4 (§10.5 coverage): in an upper tier, a generation with
/// no access record and one with an old record are not demoted before
/// `demote_after` plus the margin from the grace start, the margin
/// standing for the clock skew a leader may have against the reads'
/// writers; past it both are.
#[tokio::test]
async fn generations_without_or_with_old_records_wait_for_the_grace() {
    let (c, fast) = cluster().await;
    generations_without_or_with_old_records_wait_for_the_grace_on(c, fast).await;
}

/// [`generations_without_or_with_old_records_wait_for_the_grace`] on any store.
pub(crate) async fn generations_without_or_with_old_records_wait_for_the_grace_on<
    S: arca_lake_metadata::KvStore + 'static,
>(
    c: TestCluster<S>,
    fast: GroupRecord,
) {
    let mut gens = Vec::new();
    for (key, seed) in [("none", 61u64), ("old", 62)] {
        let data = payload(usize::try_from(MIB / 2).unwrap(), seed);
        s3::put(&c.backend, BUCKET, key, data, &PutOptions::default())
            .await
            .unwrap();
        gens.push(c.manifest(BUCKET, key).await.pointer.generation);
    }
    // Both moved to the upper tier first (unfenced, as an earlier policy
    // would have), then their records as tiering finds them on enabling.
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(c.meta.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let groups = cluster::list_groups(c.meta.as_ref()).await.unwrap();
    for key in ["none", "old"] {
        let (index, record) = c.manifest(BUCKET, key).await.chunks[0].clone();
        let source = groups
            .iter()
            .find(|g| g.id == record.placement.group_id)
            .unwrap()
            .clone();
        let generation = c.manifest(BUCKET, key).await.pointer.generation;
        let o = migrate_chunk(
            c.meta.as_ref(),
            &c.clients,
            &Coders::default(),
            &addr_of(&nodes),
            &source,
            &fast,
            &generation,
            index,
            &record,
            None,
        )
        .await
        .unwrap();
        assert!(matches!(o, MigrateOutcome::Switched { .. }), "{o:?}");
    }
    let t0 = now_ms();
    object::record_access(c.meta.as_ref(), &gens[1], t0 - 86_400_000, 3)
        .await
        .unwrap();
    // The leader's clock 60 s behind the writers: the margin covers it.
    let skew = 60_000;
    let margin = arca_lake_metadata::ops::tiering::grace_margin_ms(skew);
    let fence = enabled_record(&c, t0, margin).await;
    let run = async |at: u64| {
        arca_lake_controller::tiering::run_once_bounded_fenced(
            c.meta.as_ref(),
            &c.clients,
            &fenced_policy(),
            at,
            0,
            Some(fence),
        )
        .await
        .unwrap()
    };
    let demote = 1_800_000;
    let report = run(t0 + demote + margin - 1_000).await;
    assert_eq!(
        report.demoted_chunks, 0,
        "demoted within the grace: {report:?}"
    );
    for key in ["none", "old"] {
        assert_eq!(
            c.manifest(BUCKET, key).await.chunks[0].1.placement.group_id,
            fast.id,
            "{key}"
        );
    }
    let report = run(t0 + demote + margin + 1_000).await;
    assert_eq!(report.demoted_chunks, 2, "{report:?}");
    c.shutdown().await;
}

/// A tiering pass that finds another migration of a chunk in flight (a
/// deposed leader's move, held before its switch) moves nothing and keeps
/// the generation's access record: the reads recorded meanwhile are not
/// reset by a pass that did not act on them (ADR-0035 §10.1; found by the
/// integration harness, scenario B3b).
#[tokio::test]
async fn a_move_already_in_flight_resets_no_access_record() {
    let (c, fast) = cluster().await;
    a_move_already_in_flight_resets_no_access_record_on(c, fast).await;
}

/// [`a_move_already_in_flight_resets_no_access_record`] on any store.
pub(crate) async fn a_move_already_in_flight_resets_no_access_record_on<
    S: arca_lake_metadata::KvStore + 'static,
>(
    c: TestCluster<S>,
    fast: GroupRecord,
) {
    let data = payload(usize::try_from(MIB / 2).unwrap(), 71);
    s3::put(
        &c.backend,
        BUCKET,
        "o",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let manifest = c.manifest(BUCKET, "o").await;
    let generation = manifest.pointer.generation;
    let t0 = now_ms();
    let fence = enabled_record(&c, t0 - 10_000, 0).await;
    // Another attempt holds the chunk's migration intent.
    let other = arca_lake_metadata::records::new_gen_id();
    assert!(
        object::begin_migration(
            c.meta.as_ref(),
            &manifest.chunks[0].1.chunk_id,
            fast.id,
            &other,
            t0
        )
        .await
        .unwrap()
    );
    object::record_access(c.meta.as_ref(), &generation, t0, 9)
        .await
        .unwrap();
    let report = arca_lake_controller::tiering::run_once_bounded_fenced(
        c.meta.as_ref(),
        &c.clients,
        &fenced_policy(),
        t0 + 1_000,
        0,
        Some(fence),
    )
    .await
    .unwrap();
    assert_eq!(report.promoted_chunks, 0, "{report:?}");
    assert_eq!(
        c.manifest(BUCKET, "o").await.chunks[0].1.placement.group_id,
        manifest.chunks[0].1.placement.group_id
    );
    assert_eq!(
        object::access_of(c.meta.as_ref(), &generation)
            .await
            .unwrap()
            .map(|r| r.count),
        Some(9),
        "the access record was reset by a pass that moved nothing"
    );
    c.shutdown().await;
}

/// A fenced pass at `at` with `hook` before every switch (R-118).
async fn pass_with<S: arca_lake_metadata::KvStore + 'static>(
    c: &TestCluster<S>,
    fence: arca_lake_controller::tiering::PolicyFence,
    at: u64,
    hook: Option<std::sync::Arc<arca_lake_controller::tiering::BeforeSwitch>>,
) -> arca_lake_controller::tiering::TieringReport {
    arca_lake_controller::tiering::run_once_bounded_fenced_with(
        c.meta.as_ref(),
        &c.clients,
        &fenced_policy(),
        at,
        0,
        Some(fence),
        hook,
    )
    .await
    .unwrap()
}

/// A hook running `f` on the metadata store once, before the first switch.
fn once_before_switch<S, F>(
    meta: std::sync::Arc<S>,
    f: F,
) -> std::sync::Arc<arca_lake_controller::tiering::BeforeSwitch>
where
    S: arca_lake_metadata::KvStore + 'static,
    F: Fn(std::sync::Arc<S>) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        + 'static,
{
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    std::sync::Arc::new(move |_, _| {
        if done.swap(true, std::sync::atomic::Ordering::SeqCst) {
            Box::pin(std::future::ready(()))
        } else {
            f(meta.clone())
        }
    })
}

/// Put `key` with `chunks` chunks, record 9 reads of it at `t0` and enable
/// tiering before them: the next pass promotes it.
async fn hot_object<S: arca_lake_metadata::KvStore + 'static>(
    c: &TestCluster<S>,
    key: &str,
    chunks: u64,
    t0: u64,
) -> (object::Manifest, arca_lake_controller::tiering::PolicyFence) {
    let len = usize::try_from(chunks * MIB - MIB / 2).unwrap();
    s3::put(
        &c.backend,
        BUCKET,
        key,
        payload(len, 118),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let manifest = c.manifest(BUCKET, key).await;
    assert_eq!(manifest.chunks.len(), usize::try_from(chunks).unwrap());
    let fence = enabled_record(c, t0 - 10_000, 0).await;
    object::record_access(c.meta.as_ref(), &manifest.pointer.generation, t0, 9)
        .await
        .unwrap();
    (manifest, fence)
}

async fn placements<S: arca_lake_metadata::KvStore + 'static>(
    c: &TestCluster<S>,
    key: &str,
) -> Vec<u64> {
    c.manifest(BUCKET, key)
        .await
        .chunks
        .iter()
        .map(|(_, r)| r.placement.group_id)
        .collect()
}

async fn access<S: arca_lake_metadata::KvStore + 'static>(
    c: &TestCluster<S>,
    m: &object::Manifest,
) -> Option<object::AccessRecord> {
    object::access_of(c.meta.as_ref(), &m.pointer.generation)
        .await
        .unwrap()
}

/// R-118: a switch refused because the target group's epoch changed
/// between the copy and the switch moved nothing; the pass keeps the
/// generation's access record, and the next pass promotes it.
#[tokio::test]
async fn a_switch_refused_by_a_target_epoch_change_resets_no_access_record() {
    let (c, fast) = cluster().await;
    let t0 = now_ms();
    let (m, fence) = hot_object(&c, "o", 1, t0).await;
    let source = m.chunks[0].1.placement.group_id;
    let target = fast.id;
    let hook = once_before_switch(c.meta.clone(), move |meta| {
        Box::pin(async move {
            cluster::update_group(meta.as_ref(), target, |g| {
                g.epoch += 1;
                true
            })
            .await
            .unwrap();
        })
    });
    let report = pass_with(&c, fence, t0 + 1_000, Some(hook)).await;
    assert_eq!(report.promoted_chunks, 0, "{report:?}");
    assert_eq!(placements(&c, "o").await, vec![source]);
    assert_eq!(
        access(&c, &m).await,
        Some(object::AccessRecord {
            last_ms: t0,
            count: 9
        }),
        "a refused switch reset the access record"
    );
    let report = pass_with(&c, fence, t0 + 2_000, None).await;
    assert_eq!(report.promoted_chunks, 1, "{report:?}");
    assert_eq!(placements(&c, "o").await, vec![fast.id]);
    assert_eq!(
        access(&c, &m).await,
        Some(object::AccessRecord {
            last_ms: t0 + 2_000,
            count: 0
        })
    );
    c.shutdown().await;
}

/// R-118: a switch refused because the attempt's intent expired and was
/// removed before it (`Skipped`) moved nothing; the access record is kept
/// and the next pass promotes the generation.
#[tokio::test]
async fn a_switch_skipped_after_its_intent_expired_resets_no_access_record() {
    let (c, fast) = cluster().await;
    let t0 = now_ms();
    let (m, fence) = hot_object(&c, "o", 1, t0).await;
    let source = m.chunks[0].1.placement.group_id;
    let hook = once_before_switch(c.meta.clone(), |meta| {
        Box::pin(async move {
            let later = now_ms() + object::MIGRATION_INTENT_TTL_MS + 1;
            assert_eq!(
                object::expire_migrations(meta.as_ref(), later)
                    .await
                    .unwrap(),
                1
            );
        })
    });
    let report = pass_with(&c, fence, t0 + 1_000, Some(hook)).await;
    assert_eq!(report.promoted_chunks, 0, "{report:?}");
    assert_eq!(placements(&c, "o").await, vec![source]);
    assert_eq!(
        access(&c, &m).await.map(|r| r.count),
        Some(9),
        "a skipped switch reset the access record"
    );
    let report = pass_with(&c, fence, t0 + 2_000, None).await;
    assert_eq!(report.promoted_chunks, 1, "{report:?}");
    assert_eq!(placements(&c, "o").await, vec![fast.id]);
    assert_eq!(access(&c, &m).await.map(|r| r.count), Some(0));
    c.shutdown().await;
}

/// R-118: a two-chunk generation whose second chunk another migration
/// holds is moved in part; its access record is kept, and the next pass
/// (judging the generation by its chunk left behind) moves that chunk and
/// only then resets the record.
#[tokio::test]
async fn a_partly_moved_generation_keeps_its_access_record_and_is_finished_later() {
    let (c, fast) = cluster().await;
    let t0 = now_ms();
    let (m, fence) = hot_object(&c, "o", 2, t0).await;
    let source = m.chunks[1].1.placement.group_id;
    let other = arca_lake_metadata::records::new_gen_id();
    let chunk = m.chunks[1].1.chunk_id;
    assert!(
        object::begin_migration(c.meta.as_ref(), &chunk, fast.id, &other, t0)
            .await
            .unwrap()
    );
    let report = pass_with(&c, fence, t0 + 1_000, None).await;
    assert_eq!(report.promoted_chunks, 1, "{report:?}");
    assert_eq!(placements(&c, "o").await, vec![fast.id, source]);
    assert_eq!(
        access(&c, &m).await.map(|r| r.count),
        Some(9),
        "a partly moved generation had its access record reset"
    );
    object::end_migration(c.meta.as_ref(), &chunk, &other)
        .await
        .unwrap();
    let report = pass_with(&c, fence, t0 + 2_000, None).await;
    assert_eq!(report.promoted_chunks, 1, "{report:?}");
    assert_eq!(placements(&c, "o").await, vec![fast.id, fast.id]);
    assert_eq!(access(&c, &m).await.map(|r| r.count), Some(0));
    // Nothing left to move: a third pass changes nothing.
    let report = pass_with(&c, fence, t0 + 3_000, None).await;
    assert_eq!(
        (report.promoted_chunks, report.demoted_chunks),
        (0, 0),
        "{report:?}"
    );
    c.shutdown().await;
}

/// R-118: a two-chunk generation whose second chunk cannot be read (two of
/// its three shards damaged) is moved in part; its access record is kept.
#[tokio::test]
async fn a_generation_with_a_failed_chunk_keeps_its_access_record() {
    let (c, fast) = cluster().await;
    let t0 = now_ms();
    let (m, fence) = hot_object(&c, "o", 2, t0).await;
    let (_, record) = &m.chunks[1];
    let source = c
        .groups()
        .await
        .into_iter()
        .find(|g| g.id == record.placement.group_id)
        .unwrap();
    for member in source.members.iter().take(2) {
        let i = c.node_index(member).unwrap();
        assert_eq!(c.corrupt_chunk_on_node(i, source.id, record.chunk_id), 1);
    }
    let report = pass_with(&c, fence, t0 + 1_000, None).await;
    assert_eq!(report.promoted_chunks, 1, "{report:?}");
    assert_eq!(placements(&c, "o").await, vec![fast.id, source.id]);
    assert_eq!(
        access(&c, &m).await.map(|r| r.count),
        Some(9),
        "a generation with a failed chunk had its access record reset"
    );
    c.shutdown().await;
}

/// R-118: reads recorded while a pass moves a generation are not erased
/// by the pass's reset: the record keeps them (and their time), only the
/// reads the pass decided on are consumed.
#[tokio::test]
async fn reads_recorded_during_a_pass_survive_its_reset() {
    let (c, fast) = cluster().await;
    let t0 = now_ms();
    let (m, fence) = hot_object(&c, "o", 1, t0).await;
    let generation = m.pointer.generation;
    let hook = once_before_switch(c.meta.clone(), move |meta| {
        Box::pin(async move {
            object::record_access(meta.as_ref(), &generation, t0 + 1_500, 4)
                .await
                .unwrap();
        })
    });
    let report = pass_with(&c, fence, t0 + 1_000, Some(hook)).await;
    assert_eq!(report.promoted_chunks, 1, "{report:?}");
    assert_eq!(placements(&c, "o").await, vec![fast.id]);
    assert_eq!(
        access(&c, &m).await,
        Some(object::AccessRecord {
            last_ms: t0 + 1_500,
            count: 4
        }),
        "the reads recorded during the pass were reset"
    );
    c.shutdown().await;
}
