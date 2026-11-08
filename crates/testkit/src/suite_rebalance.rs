//! Capacity rebalancing (Phase 3.8, H7–H9; generic over the metadata
//! backend since Phase 3.9, run on FoundationDB by `rebalance_fdb.rs`): chunks move from the fullest
//! group of a tier to the roomiest one of the same shape, with reads,
//! writes, deletes and GC going on; the cursor survives a leader change;
//! nothing moves while paused, in a dry run, while a group needs repair, or
//! once the target is at its reserve.

#![allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::many_single_char_names,
    clippy::items_after_statements,
    clippy::missing_panics_doc
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use arca_lake_controller::admission::{Tracker, device_of, pass};
use arca_lake_controller::backpressure::Policy;
use arca_lake_controller::rebalance::{RebalancePolicy, run_once};
use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::admission::{self, AdmissionRecord, TierAdmission};
use arca_lake_metadata::ops::rebalance;
use arca_lake_metadata::records::GroupRecord;

use crate::s3::{self, PutOptions};
use crate::suite::Starter;
use crate::{ClusterOptions, TestCluster, now_ms, random_bytes};
use bytes::Bytes;
use s3s::S3ErrorCode;

const MIB: u64 = 1 << 20;
const BUCKET: &str = "rebalance";

fn data(i: u64) -> Bytes {
    Bytes::from(random_bytes(usize::try_from(MIB).unwrap(), 1000 + i))
}

/// Six nodes capped at `cap`, two 2+1 groups of tier `default`.
async fn cluster<S: KvStore + 'static>(
    start: &impl Starter<S>,
    cap: u64,
) -> (TestCluster<S>, GroupRecord, GroupRecord) {
    let mut c = start(ClusterOptions {
        groups: 2,
        ..ClusterOptions::rs(6, 2, 1, MIB, 64 << 10)
    })
    .await;
    for i in 0..6 {
        c.set_node_capacity(i, Some(cap)).await;
    }
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    let mut ec: Vec<GroupRecord> = c
        .groups()
        .await
        .into_iter()
        .filter(|g| !g.is_cache() && g.scheme == "rs-gf8-v1")
        .collect();
    ec.sort_by_key(|g| g.id);
    assert_eq!(ec.len(), 2);
    let b = ec.pop().unwrap();
    let a = ec.pop().unwrap();
    (c, a, b)
}

/// The gateway places only outside `excluded` (token 0: the controller
/// passes of the test, also token 0, supersede it).
async fn exclude<S: KvStore + 'static>(c: &TestCluster<S>, excluded: Vec<u64>) {
    let rec = AdmissionRecord {
        computed_ms: now_ms(),
        ttl_ms: 3_600_000,
        gateways: 1,
        tiers: vec![TierAdmission {
            tier: "default".into(),
            excluded_groups: excluded,
            reason: "test".into(),
            ..Default::default()
        }],
        incarnation: c.incarnation(),
        ..Default::default()
    };
    admission::publish(c.meta.as_ref(), &rec).await.unwrap();
    let rec = admission::current(c.meta.as_ref()).await.unwrap();
    c.backend.cluster().admission.observe(rec, Instant::now());
}

/// A controller admission pass over fresh reports (the group rooms the
/// rebalancer plans from).
async fn evaluate<S: KvStore + 'static>(c: &TestCluster<S>) -> AdmissionRecord {
    let tracker = Tracker::default();
    let now = now_ms();
    for n in &c.nodes {
        let h = c.clients.health(&n.addr.to_string()).await.unwrap();
        tracker.note_device(device_of(n.id, &h, now));
    }
    let policy = Policy {
        group_min_shards: 4,
        ..Policy::default()
    };
    pass(c.meta.as_ref(), &tracker, &policy, MIB, 0, now)
        .await
        .unwrap();
    let rec = admission::current(c.meta.as_ref()).await.unwrap();
    c.backend
        .cluster()
        .admission
        .observe(rec.clone(), Instant::now());
    rec.unwrap()
}

fn room(rec: &AdmissionRecord, group: u64) -> u64 {
    rec.tier("default")
        .unwrap()
        .group_room
        .iter()
        .find(|g| g.group == group)
        .map_or(0, |g| g.room)
}

fn compact_all<S: KvStore + 'static>(c: &TestCluster<S>) {
    for i in 0..c.nodes.len() {
        let s = c.node_store(i);
        for seg in s.compaction_candidates(0.0).unwrap_or_default() {
            let _ = s.compact(seg);
        }
    }
}

async fn group_of<S: KvStore + 'static>(c: &TestCluster<S>, key: &str) -> u64 {
    c.manifest(BUCKET, key).await.chunks[0].1.placement.group_id
}

fn policy(bytes_per_pass: u64) -> RebalancePolicy {
    RebalancePolicy {
        start_imbalance: 0.2,
        stop_imbalance: 0.05,
        bytes_per_pass,
        ..RebalancePolicy::default()
    }
}

/// Everything in `live` reads back whole.
async fn check<S: KvStore + 'static>(c: &TestCluster<S>, live: &BTreeMap<String, Bytes>) {
    for (k, v) in live {
        assert_eq!(&s3::get(&c.backend, BUCKET, k).await.unwrap().0, v, "{k}");
    }
}

/// H7: chunks move from the fuller group to the roomier one while reads,
/// writes, deletes and GC go on; dry run and pause move nothing; the
/// source's space comes back after reconciliation and compaction.
pub async fn rebalancing_moves_chunks_to_the_roomier_group_while_clients_and_gc_go_on<
    S: KvStore + 'static,
>(
    start: impl Starter<S>,
) {
    let (c, a, b) = cluster(&start, 160 * MIB).await;
    // Everything into group A.
    exclude(&c, vec![b.id]).await;
    let mut live = BTreeMap::new();
    for i in 0..80 {
        let k = format!("o{i}");
        s3::put(&c.backend, BUCKET, &k, data(i), &PutOptions::default())
            .await
            .unwrap();
        assert_eq!(group_of(&c, &k).await, a.id);
        live.insert(k, data(i));
    }
    let before = evaluate(&c).await;
    let (ra, rb) = (room(&before, a.id), room(&before, b.id));
    assert!(ra < rb, "{ra} {rb}");
    let imbalance0 = 1.0 - ra as f64 / rb as f64;
    assert!(imbalance0 > 0.2, "{imbalance0}");

    // Dry run: a plan, nothing moved.
    rebalance::set_switches(c.meta.as_ref(), None, Some(true))
        .await
        .unwrap();
    let r = run_once(c.meta.as_ref(), &c.clients, &policy(64 * MIB), now_ms())
        .await
        .unwrap();
    assert_eq!(r.moved_chunks, 0);
    assert!(r.planned_chunks > 0, "{r:?}");
    let plan = r.plan.clone().unwrap();
    assert_eq!((plan.source, plan.target), (a.id, b.id));
    assert!(r.reason.starts_with("dry run"), "{r:?}");
    assert_eq!(
        rebalance::state(c.meta.as_ref()).await.unwrap().cursor,
        None
    );
    // Paused: nothing either.
    rebalance::set_switches(c.meta.as_ref(), Some(true), Some(false))
        .await
        .unwrap();
    let r = run_once(c.meta.as_ref(), &c.clients, &policy(64 * MIB), now_ms())
        .await
        .unwrap();
    assert_eq!((r.moved_chunks, r.reason.as_str()), (0, "paused"));
    rebalance::set_switches(c.meta.as_ref(), Some(false), None)
        .await
        .unwrap();

    // Moves with readers, writers and deletes (and a GC) going on.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers = {
        let (b, live, stop) = (c.backend.clone(), live.clone(), Arc::clone(&stop));
        tokio::spawn(async move {
            let mut reads = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                for (k, v) in live.iter().filter(|(k, _)| !k.ends_with('7')) {
                    let got = s3::get(&b, BUCKET, k)
                        .await
                        .unwrap_or_else(|e| panic!("GET {k}: {e:?}"));
                    assert_eq!(&got.0, v, "{k}");
                    reads += 1;
                }
            }
            reads
        })
    };
    let writer = {
        let b = c.backend.clone();
        tokio::spawn(async move {
            let mut written = BTreeMap::new();
            for i in 0..10u64 {
                let k = format!("w{i}");
                s3::put(&b, BUCKET, &k, data(500 + i), &PutOptions::default())
                    .await
                    .unwrap();
                written.insert(k, data(500 + i));
            }
            written
        })
    };
    for k in live
        .keys()
        .filter(|k| k.ends_with('7'))
        .cloned()
        .collect::<Vec<_>>()
    {
        s3::delete(&c.backend, BUCKET, &k).await.unwrap();
        live.remove(&k);
    }
    let mut moved = 0;
    for _ in 0..3 {
        let r = run_once(c.meta.as_ref(), &c.clients, &policy(16 * MIB), now_ms())
            .await
            .unwrap();
        assert!(r.moved_bytes <= 16 * MIB, "{r:?}");
        moved += r.moved_chunks;
        // The real clock: a GC an hour ahead would expire the concurrent
        // writer's pending generations.
        c.gc(now_ms(), 0).await;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(readers.await.unwrap() > 0);
    live.extend(writer.await.unwrap());
    assert!(moved >= 16, "{moved}");
    check(&c, &live).await;
    let in_b = {
        let mut n = 0;
        for k in live.keys().filter(|k| k.starts_with('o')) {
            if group_of(&c, k).await == b.id {
                n += 1;
            }
        }
        n
    };
    assert!(in_b > 0, "chunks of A now in B");

    // The source space comes back (reconcile, compact); the imbalance is
    // smaller; every object still reads.
    c.reconcile(now_ms() + 3_600_000, 0).await;
    compact_all(&c);
    let after = evaluate(&c).await;
    let imbalance1 = 1.0 - room(&after, a.id) as f64 / room(&after, b.id).max(1) as f64;
    assert!(imbalance1 < imbalance0, "{imbalance0} -> {imbalance1}");
    check(&c, &live).await;
    let s = rebalance::state(c.meta.as_ref()).await.unwrap();
    assert_eq!(s.moved_chunks, moved);
    c.shutdown().await;
}

/// H9: the cursor is persisted after each pass; a new leader (another
/// pass over the same metadata) continues after it, a crash in the middle
/// of a move leaves an intent that the next pass expires, and every chunk
/// is moved at most once.
pub async fn a_new_leader_resumes_rebalancing_at_the_cursor<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let (c, a, b) = cluster(&start, 160 * MIB).await;
    exclude(&c, vec![b.id]).await;
    let mut live = BTreeMap::new();
    for i in 0..60 {
        let k = format!("o{i}");
        s3::put(&c.backend, BUCKET, &k, data(i), &PutOptions::default())
            .await
            .unwrap();
        live.insert(k, data(i));
    }
    evaluate(&c).await;
    // A move crashed before its switch (a former leader's): its intent and
    // target shards are left behind.
    let gens = arca_lake_metadata::ops::object::list_generations(c.meta.as_ref(), None, 10)
        .await
        .unwrap();
    let (g0, _) = gens[0];
    let manifest = arca_lake_metadata::ops::object::read_manifest_of(c.meta.as_ref(), &g0)
        .await
        .unwrap()
        .unwrap();
    let nodes: std::collections::HashMap<u64, String> =
        c.nodes.iter().map(|n| (n.id, n.addr.to_string())).collect();
    let crashed = arca_lake_tiering::migrate::migrate_chunk(
        c.meta.as_ref(),
        &c.clients,
        &arca_lake_tiering::durable::Coders::default(),
        &|id| nodes.get(&id).cloned(),
        &a,
        &b,
        &g0,
        manifest[0].0,
        &manifest[0].1,
        Some(arca_lake_tiering::migrate::MigrateFault::BeforeSwitch),
    )
    .await;
    assert!(crashed.is_err());

    // Small passes: each persists its cursor.
    let p = policy(4 * MIB);
    let r1 = run_once(c.meta.as_ref(), &c.clients, &p, now_ms())
        .await
        .unwrap();
    let s1 = rebalance::state(c.meta.as_ref()).await.unwrap();
    assert!(s1.cursor.is_some(), "{s1:?}");
    assert!(r1.moved_chunks > 0);
    // "Leader change": a pass with nothing in memory continues from the
    // cursor (the intent of the crashed move has expired by then).
    let later = now_ms() + 3_600_000;
    let r2 = run_once(c.meta.as_ref(), &c.clients, &p, later)
        .await
        .unwrap();
    let s2 = rebalance::state(c.meta.as_ref()).await.unwrap();
    assert!(r2.moved_chunks > 0, "{r2:?}");
    assert!(
        s2.cursor > s1.cursor || s2.cursor.is_none(),
        "{s1:?} {s2:?}"
    );
    assert_eq!(s2.moved_chunks, r1.moved_chunks + r2.moved_chunks);
    // Every moved chunk is in B once; every object reads back.
    let mut in_b = 0;
    for k in live.keys() {
        if group_of(&c, k).await == b.id {
            in_b += 1;
        }
    }
    assert_eq!(in_b, s2.moved_chunks);
    check(&c, &live).await;
    // The crashed move's leftovers are collected, the moved sources too.
    c.reconcile(now_ms() + 7_200_000, 0).await;
    check(&c, &live).await;
    let shards = c.total_shards().await;
    assert_eq!(shards, live.len() * 3, "one copy of each chunk");
    c.shutdown().await;
}

/// Repair first; a target at its reserve stops the pass; nothing is lost.
pub async fn rebalancing_waits_for_repair_and_stops_at_the_targets_reserve<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let (mut c, _a, b) = cluster(&start, 160 * MIB).await;
    exclude(&c, vec![b.id]).await;
    let mut live = BTreeMap::new();
    for i in 0..80 {
        let k = format!("o{i}");
        s3::put(&c.backend, BUCKET, &k, data(i), &PutOptions::default())
            .await
            .unwrap();
        live.insert(k, data(i));
    }
    evaluate(&c).await;
    // A group not Active: no pass.
    use arca_lake_metadata::records::GroupState;
    arca_lake_metadata::ops::cluster::update_group(c.meta.as_ref(), b.id, |g| {
        g.state = GroupState::Degraded;
        true
    })
    .await
    .unwrap();
    let r = run_once(c.meta.as_ref(), &c.clients, &policy(64 * MIB), now_ms())
        .await
        .unwrap();
    assert_eq!(r.moved_chunks, 0);
    assert!(r.reason.contains("repair first"), "{r:?}");
    arca_lake_metadata::ops::cluster::update_group(c.meta.as_ref(), b.id, |g| {
        g.state = GroupState::Active;
        true
    })
    .await
    .unwrap();
    // The target's nodes shrink to just above their reserve: the plan
    // still sees room (an old report), the nodes refuse; the pass stops.
    let plan = evaluate(&c).await;
    for m in &b.members {
        let i = c.node_index(m).unwrap();
        c.set_node_capacity(i, Some(8 * MIB)).await;
    }
    c.backend
        .cluster()
        .admission
        .observe(Some(plan), Instant::now());
    let r = run_once(c.meta.as_ref(), &c.clients, &policy(64 * MIB), now_ms())
        .await
        .unwrap();
    assert!(r.reason.contains("at its reserve"), "{r:?}");
    for i in 0..6 {
        let d = c.node_service(i).device_capacity().unwrap();
        assert!(d.available_bytes >= d.reserved_bytes, "node {i}: {d:?}");
        assert_eq!(d.inflight_bytes, 0);
    }
    check(&c, &live).await;
    let e = s3::get(&c.backend, BUCKET, "missing").await.unwrap_err();
    assert_eq!(*e.code(), S3ErrorCode::NoSuchKey);
    c.shutdown().await;
}

/// Phase 3.8 review (H): a GET whose manifest predates a move, with the
/// source shards already reconciled away, reads the chunk at its new
/// placement instead of failing or filing a false integrity report.
pub async fn a_read_started_before_a_move_follows_the_chunk<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    use futures::StreamExt;
    use s3s::S3;
    let (c, a, b) = cluster(&start, 400 * MIB).await;
    exclude(&c, vec![b.id]).await;
    let big = Bytes::from(random_bytes(usize::try_from(6 * MIB).unwrap(), 9));
    s3::put(
        &c.backend,
        BUCKET,
        "big",
        big.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    let out = c
        .backend
        .get_object(s3::request(s3s::dto::GetObjectInput {
            bucket: BUCKET.into(),
            key: "big".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let mut body = out.body.unwrap();
    let mut got = Vec::new();
    got.extend_from_slice(&body.next().await.unwrap().unwrap());
    // Every chunk moves to B; reconciliation deletes the A copies.
    let m = c.manifest(BUCKET, "big").await;
    let nodes: std::collections::HashMap<u64, String> =
        c.nodes.iter().map(|n| (n.id, n.addr.to_string())).collect();
    for (index, rec) in &m.chunks {
        let r = arca_lake_tiering::migrate::migrate_chunk(
            c.meta.as_ref(),
            &c.clients,
            &arca_lake_tiering::durable::Coders::default(),
            &|id| nodes.get(&id).cloned(),
            &a,
            &b,
            &m.pointer.generation,
            *index,
            rec,
            None,
        )
        .await
        .unwrap();
        assert!(matches!(
            r,
            arca_lake_tiering::migrate::MigrateOutcome::Switched { .. }
        ));
    }
    c.reconcile(now_ms() + 3_600_000, 0).await;
    while let Some(frame) = body.next().await {
        got.extend_from_slice(&frame.unwrap());
    }
    assert_eq!(Bytes::from(got), big);
    let s = c.backend.integrity_stats();
    assert!(s.moved_reads > 0, "{s:?}");
    assert_eq!(s.unreadable_reads, 0, "{s:?}");
    assert_eq!(s.integrity_reports, 0, "no false repair task: {s:?}");
    c.shutdown().await;
}

/// Phase 3.8 review (H): the target's membership changes (a repair bumps
/// its epoch) after the copy and before the switch: the switch is refused,
/// the copy deleted, the chunk stays where it was and reads back.
pub async fn a_move_whose_target_changed_epoch_is_abandoned<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let (c, a, b) = cluster(&start, 400 * MIB).await;
    exclude(&c, vec![b.id]).await;
    let d = data(1);
    s3::put(&c.backend, BUCKET, "o", d.clone(), &PutOptions::default())
        .await
        .unwrap();
    let m = c.manifest(BUCKET, "o").await;
    let (index, rec) = &m.chunks[0];
    let nodes: std::collections::HashMap<u64, String> =
        c.nodes.iter().map(|n| (n.id, n.addr.to_string())).collect();
    let before = c.total_shards().await;
    let meta = std::sync::Arc::clone(&c.meta);
    let r = arca_lake_tiering::migrate::migrate_chunk_with(
        c.meta.as_ref(),
        &c.clients,
        &arca_lake_tiering::durable::Coders::default(),
        &|id| nodes.get(&id).cloned(),
        &a,
        &b,
        &m.pointer.generation,
        *index,
        rec,
        None,
        async || {
            arca_lake_metadata::ops::cluster::update_group(meta.as_ref(), b.id, |g| {
                g.epoch += 1;
                true
            })
            .await
            .unwrap();
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(r, arca_lake_tiering::migrate::MigrateOutcome::Skipped(_)),
        "{r:?}"
    );
    assert_eq!(group_of(&c, "o").await, a.id);
    assert_eq!(c.total_shards().await, before, "the copy was deleted");
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, d);
    c.shutdown().await;
}

/// Phase 3.8 review (M): a chunk larger than the pass budget still moves
/// (one per pass), so a pass never stops at the same chunk forever.
pub async fn a_chunk_larger_than_the_budget_still_moves<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let (c, _a, b) = cluster(&start, 160 * MIB).await;
    exclude(&c, vec![b.id]).await;
    for i in 0..80 {
        s3::put(
            &c.backend,
            BUCKET,
            &format!("o{i}"),
            data(i),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    evaluate(&c).await;
    let r = run_once(c.meta.as_ref(), &c.clients, &policy(MIB / 4), now_ms())
        .await
        .unwrap();
    assert_eq!(r.moved_chunks, 1, "{r:?}");
    c.shutdown().await;
}

/// Phase 3.8 re-review (Critical): attempt A copies a chunk and stalls
/// past its intent's expiry; attempt B takes the intent over, writes the
/// same target keys and switches. A then fails its switch and must not
/// delete the target records, which B's committed placement uses.
pub async fn an_expired_move_never_deletes_its_successors_shards<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    use arca_lake_tiering::migrate::{MigrateOutcome, migrate_chunk, migrate_chunk_with};
    let (c, a, b) = cluster(&start, 400 * MIB).await;
    exclude(&c, vec![b.id]).await;
    let d = data(3);
    s3::put(&c.backend, BUCKET, "o", d.clone(), &PutOptions::default())
        .await
        .unwrap();
    let m = c.manifest(BUCKET, "o").await;
    let (index, rec) = m.chunks[0].clone();
    let generation = m.pointer.generation;
    let nodes: std::collections::HashMap<u64, String> =
        c.nodes.iter().map(|n| (n.id, n.addr.to_string())).collect();
    let addr_of = |id: u64| nodes.get(&id).cloned();
    let coders = arca_lake_tiering::durable::Coders::default();
    let (meta, clients) = (std::sync::Arc::clone(&c.meta), c.clients.clone());
    let (ga, gb, rec2) = (a.clone(), b.clone(), rec.clone());
    let r = migrate_chunk_with(
        c.meta.as_ref(),
        &c.clients,
        &coders,
        &addr_of,
        &a,
        &b,
        &generation,
        index,
        &rec,
        None,
        async || {
            // A stalls: its intent expires, B moves the chunk.
            arca_lake_metadata::ops::object::expire_migrations(
                meta.as_ref(),
                now_ms() + 2 * arca_lake_metadata::ops::object::MIGRATION_INTENT_TTL_MS,
            )
            .await
            .unwrap();
            let r = migrate_chunk(
                meta.as_ref(),
                &clients,
                &arca_lake_tiering::durable::Coders::default(),
                &|id| nodes.get(&id).cloned(),
                &ga,
                &gb,
                &generation,
                index,
                &rec2,
                None,
            )
            .await
            .unwrap();
            assert!(matches!(r, MigrateOutcome::Switched { .. }), "{r:?}");
        },
    )
    .await
    .unwrap();
    assert!(matches!(r, MigrateOutcome::Skipped(_)), "{r:?}");
    assert_eq!(group_of(&c, "o").await, b.id);
    // B's placement is whole: every target member holds its shard.
    for (i, member) in b.members.iter().enumerate() {
        let n = c.node_index(member).unwrap();
        assert!(
            c.shard_keys(n, b.id)
                .iter()
                .any(|k| k.chunk_id == rec.chunk_id && usize::from(k.shard_index) == i),
            "member {i} of the target lost its shard"
        );
    }
    c.reconcile(now_ms() + 7_200_000, 0).await;
    assert_eq!(s3::get(&c.backend, BUCKET, "o").await.unwrap().0, d);
    c.shutdown().await;
}

/// Phase 3.9 (load run on FoundationDB): a plan moves what it needed when
/// it started, computed once from the rooms then, and its tier settles
/// afterwards, although the rooms (the source's old copies not yet
/// reclaimed) still show an imbalance: no overshoot, no move back. After
/// reclamation and the settle time the next plan, if any, goes the same
/// way and is smaller.
pub async fn a_finished_plan_settles_instead_of_overshooting<S: KvStore + 'static>(
    start: impl Starter<S>,
) {
    let (c, a, b) = cluster(&start, 160 * MIB).await;
    exclude(&c, vec![b.id]).await;
    let mut live = BTreeMap::new();
    for i in 0..80 {
        let k = format!("o{i}");
        s3::put(&c.backend, BUCKET, &k, data(i), &PutOptions::default())
            .await
            .unwrap();
        live.insert(k, data(i));
    }
    evaluate(&c).await;
    let p = RebalancePolicy {
        settle_ms: 600_000,
        ..policy(8 * MIB)
    };
    let now = now_ms();
    let mut need = None;
    let mut moved = 0u64;
    let mut done = false;
    for _ in 0..40 {
        evaluate(&c).await;
        let r = run_once(c.meta.as_ref(), &c.clients, &p, now)
            .await
            .unwrap();
        if let Some(plan) = &r.plan {
            assert_eq!((plan.source, plan.target), (a.id, b.id), "{r:?}");
            need.get_or_insert((plan.target_room - plan.source_room) / 2);
        }
        moved += r.moved_bytes;
        if r.reason.starts_with("plan done") {
            done = true;
            break;
        }
    }
    assert!(done, "the plan never finished");
    let need = need.unwrap();
    assert!(moved <= need + MIB, "moved {moved}, needed {need}");
    // The rooms still show an imbalance (nothing reclaimed yet), the tier
    // settles.
    evaluate(&c).await;
    let r = run_once(c.meta.as_ref(), &c.clients, &p, now + 1)
        .await
        .unwrap();
    assert_eq!(r.moved_chunks, 0, "{r:?}");
    assert!(r.reason.starts_with("settling"), "{r:?}");
    check(&c, &live).await;
    // Reclaimed, settled: the next plan (if any) goes the same way, less.
    c.reconcile(now_ms() + 3_600_000, 0).await;
    compact_all(&c);
    evaluate(&c).await;
    let r = run_once(c.meta.as_ref(), &c.clients, &p, now + 600_001)
        .await
        .unwrap();
    if let Some(plan) = &r.plan {
        assert_eq!((plan.source, plan.target), (a.id, b.id), "reversed: {r:?}");
        assert!((plan.target_room - plan.source_room) / 2 < need, "{r:?}");
    }
    check(&c, &live).await;
    c.shutdown().await;
}

/// Instantiate the rebalancing cases for a backend: `$start` is an
/// `Option` of a [`Starter`] (`None`: skipped).
#[macro_export]
macro_rules! rebalance_suite {
    ($start:expr) => {
        $crate::rebalance_suite!(@each $start;
            rebalancing_moves_chunks_to_the_roomier_group_while_clients_and_gc_go_on,
            a_new_leader_resumes_rebalancing_at_the_cursor,
            rebalancing_waits_for_repair_and_stops_at_the_targets_reserve,
            a_read_started_before_a_move_follows_the_chunk,
            a_move_whose_target_changed_epoch_is_abandoned,
            a_chunk_larger_than_the_budget_still_moves,
            an_expired_move_never_deletes_its_successors_shards,
            a_finished_plan_settles_instead_of_overshooting,
        );
    };
    (@each $start:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn $name() {
                let Some(start) = $start else {
                    eprintln!("{}: backend not available; skipping", stringify!($name));
                    return;
                };
                $crate::suite_rebalance::$name(start).await;
            }
        )*
    };
}
