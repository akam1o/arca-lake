//! Differential verification of repair targets (Phase 3.14): a repair
//! attempt verified every shard of the group on its target at its start,
//! so under the per-pass chunk budget a large group's repair read its
//! target over and over. The leader now keeps what a pass verified (per
//! group, epoch, slot and target; while the node's store epoch holds) and
//! reads only records written since; the completeness check, before the
//! group becomes `Active`, still reads everything.
//!
//! Checked: a record decayed after it was verified is still found before
//! `Active`; a node restart (new store epoch) verifies everything again;
//! another epoch or target never reuses a result; and the reads drop from
//! O(chunks × attempts) to O(chunks). `measure_repair_attempts` (ignored)
//! prints the times.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arca_lake_controller::repair::{RepairOptions, VerifyCache, replace_member_with};
use arca_lake_metadata::kv::{Key, KeyRange, Limits, Value};
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::cluster;
use arca_lake_metadata::records::{GroupMember, GroupState};
use arca_lake_metadata::{KvError, KvStore, Txn};
use arca_lake_testkit::s3::{self, PutOptions};
use arca_lake_testkit::{ClusterOptions, TestCluster, random_bytes};
use bytes::Bytes;

const BUCKET: &str = "verify";
const CHUNK: u64 = 128 << 10;

/// Counts transactions.
struct Counting {
    inner: Arc<MemKv>,
    txns: Arc<AtomicU64>,
}

struct CountingTx(<MemKv as KvStore>::Tx);

impl KvStore for Counting {
    type Tx = CountingTx;

    async fn begin(&self) -> Result<CountingTx, KvError> {
        self.txns.fetch_add(1, Ordering::Relaxed);
        Ok(CountingTx(self.inner.begin().await?))
    }

    fn limits(&self) -> Limits {
        self.inner.limits()
    }
}

impl Txn for CountingTx {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.0.get(key).await
    }

    async fn get_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Key, Value)>, KvError> {
        self.0.get_range(range, limit, reverse).await
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.0.set(key, value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.0.clear(key);
    }

    fn clear_range(&mut self, range: &KeyRange) {
        self.0.clear_range(range);
    }

    async fn commit(self) -> Result<u64, KvError> {
        self.0.commit().await
    }
}

async fn cluster(objects: u64) -> TestCluster {
    let c = TestCluster::start(ClusterOptions {
        groups: 1,
        ..ClusterOptions::rs(6, 2, 1, CHUNK, 0)
    })
    .await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    for i in 0..objects {
        let d = Bytes::from(random_bytes(usize::try_from(CHUNK).unwrap(), 900 + i));
        s3::put(
            &c.backend,
            BUCKET,
            &format!("o{i}"),
            d,
            &PutOptions::default(),
        )
        .await
        .unwrap();
    }
    c
}

/// One repair of member 0 onto the first spare, in attempts of `budget`
/// chunks, as the leader runs it: returns (attempts, wall time, shards read
/// by verification, metadata transactions, summed step times).
#[allow(clippy::many_single_char_names)]
async fn repair_in_attempts(
    c: &TestCluster,
    cache: Option<Arc<VerifyCache>>,
    budget: usize,
    target: u64,
    parallel: usize,
) -> (
    u64,
    Duration,
    u64,
    u64,
    arca_lake_controller::repair::RepairTimings,
) {
    let g = c.groups().await.remove(0);
    let opts = RepairOptions {
        max_chunks: budget,
        verify_cache: cache.clone(),
        parallel,
        ..RepairOptions::default()
    };
    let meta = Counting {
        inner: Arc::clone(&c.meta),
        txns: Arc::default(),
    };
    let t = Instant::now();
    let mut attempts = 0;
    let mut sum = arca_lake_controller::repair::RepairTimings::default();
    loop {
        attempts += 1;
        let r = replace_member_with(
            &meta,
            &c.clients,
            g.id,
            0,
            GroupMember {
                node_id: target,
                device_id: 0,
            },
            &opts,
            async |_| Ok(()),
        )
        .await
        .unwrap();
        let s = &r.timings;
        sum.verify += s.verify;
        sum.walk += s.walk;
        sum.reconstruct += s.reconstruct;
        sum.write += s.write;
        sum.cursor += s.cursor;
        sum.completeness += s.completeness;
        if r.complete {
            break;
        }
        assert!(attempts < 10_000, "no progress");
    }
    let read = cache.map_or(0, |c| c.shards_read.load(Ordering::Relaxed));
    (
        attempts,
        t.elapsed(),
        read,
        meta.txns.load(Ordering::Relaxed),
        sum,
    )
}

async fn spare(c: &TestCluster, n: usize) -> u64 {
    let g: Vec<u64> = c.groups().await[0]
        .members
        .iter()
        .map(|m| m.node_id)
        .collect();
    c.nodes
        .iter()
        .map(|x| x.id)
        .filter(|id| !g.contains(id))
        .nth(n)
        .unwrap()
}

/// The reads of a repair in many attempts are O(chunks), not O(chunks ×
/// attempts), and the group becomes `Active` only with every shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verification_reads_only_what_was_written_since() {
    let mut c = cluster(60).await;
    let g = c.groups().await.remove(0);
    let lost = c.node_index(&g.members[0]).unwrap();
    c.stop_node(lost).await;
    let target = spare(&c, 0).await;
    let cache = Arc::new(VerifyCache::default());
    let (attempts, _, read, _, _) =
        repair_in_attempts(&c, Some(Arc::clone(&cache)), 5, target, 1).await;
    assert!(attempts >= 12, "{attempts}");
    // Start passes read only new records (about one budget each); the
    // completeness check reads the target's 60 once (plus the walk's own).
    assert!(read <= 60 * 3, "read {read} in {attempts} attempts");
    assert!(cache.passes_differential.load(Ordering::Relaxed) >= attempts - 2);
    let done = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert_eq!(done.state, GroupState::Active);
    c.shutdown().await;
}

/// A shard that decays on the target after it was verified (same record,
/// same sequence: the differential pass does not read it again) is found
/// by the completeness check: the group does not become `Active` with it;
/// the next walk rebuilds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shard_decayed_after_verification_is_found_before_active() {
    let mut c = cluster(20).await;
    let g = c.groups().await.remove(0);
    let lost = c.node_index(&g.members[0]).unwrap();
    c.stop_node(lost).await;
    let target = spare(&c, 0).await;
    let ti = c.nodes.iter().position(|n| n.id == target).unwrap();
    let cache = Arc::new(VerifyCache::default());
    let opts = RepairOptions {
        max_chunks: 10,
        verify_cache: Some(Arc::clone(&cache)),
        ..RepairOptions::default()
    };
    let member = GroupMember {
        node_id: target,
        device_id: 0,
    };
    // First attempt: 10 rebuilt (budget), then a start pass caches them.
    let r = replace_member_with(
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
    assert!(r.budget_exhausted);
    let r = replace_member_with(
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
    assert!(!r.complete);
    // One of the shards verified (cached) decays on the target.
    let victim = c.shard_keys(ti, g.id)[0];
    assert_eq!(c.corrupt_chunk_on_node(ti, g.id, victim.chunk_id), 1);
    let mut completed = false;
    for _ in 0..10 {
        let r = replace_member_with(
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
        if r.complete {
            completed = true;
            break;
        }
    }
    assert!(completed, "never completed");
    // Complete means the decayed shard was found and rebuilt.
    let held = c.node_store(ti).get(&victim, None);
    assert!(held.is_ok(), "the decayed shard was not rebuilt: {held:?}");
    let done = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert_eq!(done.state, GroupState::Active);
    c.shutdown().await;
}

/// A node restart (another store epoch), another target and another epoch
/// on the same target never reuse a result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_or_another_target_verifies_everything_again() {
    let mut c = cluster(30).await;
    let g = c.groups().await.remove(0);
    let lost = c.node_index(&g.members[0]).unwrap();
    c.stop_node(lost).await;
    let target = spare(&c, 0).await;
    let ti = c.nodes.iter().position(|n| n.id == target).unwrap();
    let cache = Arc::new(VerifyCache::default());
    let opts = RepairOptions {
        max_chunks: 8,
        verify_cache: Some(Arc::clone(&cache)),
        ..RepairOptions::default()
    };
    let member = GroupMember {
        node_id: target,
        device_id: 0,
    };
    for _ in 0..2 {
        let _ = replace_member_with(
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
    }
    let full_before = cache.passes_full.load(Ordering::Relaxed);
    let diff_before = cache.passes_differential.load(Ordering::Relaxed);
    assert!(diff_before >= 1);
    // Restart: the next start pass reads everything.
    c.stop_node(ti).await;
    c.start_node(ti).await;
    let _ = replace_member_with(
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
    assert_eq!(
        cache.passes_full.load(Ordering::Relaxed),
        full_before + 1,
        "a restarted node's records taken as verified"
    );
    // Another target (a new epoch): never the old result.
    let other = spare(&c, 1).await;
    let diff_mid = cache.passes_differential.load(Ordering::Relaxed);
    let r = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g.id,
        0,
        GroupMember {
            node_id: other,
            device_id: 0,
        },
        &opts,
        async |_| Ok(()),
    )
    .await
    .unwrap();
    assert!(r.epoch > g.epoch + 1);
    assert_eq!(
        cache.passes_differential.load(Ordering::Relaxed),
        diff_mid,
        "another target used an earlier result"
    );
    // Back to the first target (yet another epoch): its result of the
    // earlier epoch is not reused either.
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
    assert!(r2.epoch > r.epoch);
    assert_eq!(
        cache.passes_differential.load(Ordering::Relaxed),
        diff_mid,
        "a result of an earlier epoch was reused"
    );
    c.shutdown().await;
}

/// The first record written after a pass gets the pass's watermark as its
/// sequence: a differential pass reads it (a corrupt one is reported).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_record_after_the_watermark_is_read_again() {
    let c = cluster(5).await;
    let g = c.groups().await.remove(0);
    let mi = c.node_index(&g.members[0]).unwrap();
    let addr = c.nodes[mi].addr.to_string();
    let before = c.shard_keys(mi, g.id);
    let page = c
        .clients
        .verify_shards_since(&addr, g.id, None, 0, None)
        .await
        .unwrap();
    let since = (page.store_epoch, page.indexed_below);
    assert!(since.0 > 0 && since.1 > 0);
    let d = Bytes::from(random_bytes(usize::try_from(CHUNK).unwrap(), 7));
    s3::put(&c.backend, BUCKET, "after", d, &PutOptions::default())
        .await
        .unwrap();
    let new = c
        .shard_keys(mi, g.id)
        .into_iter()
        .find(|k| !before.contains(k))
        .unwrap();
    assert_eq!(c.node_store(mi).locate(&new).unwrap().sequence, since.1);
    assert_eq!(c.corrupt_chunk_on_node(mi, g.id, new.chunk_id), 1);
    let page = c
        .clients
        .verify_shards_since(&addr, g.id, None, 0, Some(since))
        .await
        .unwrap();
    assert!(page.since_applied);
    let seen: Vec<_> = page
        .shards
        .iter()
        .filter(|v| v.shard.chunk_id == new.chunk_id)
        .collect();
    assert_eq!(seen.len(), 1, "the record at the watermark was not read");
    assert!(!seen[0].ok);
    assert_eq!(
        page.shards.len(),
        1,
        "records below the watermark read again"
    );
    c.shutdown().await;
}

/// Chunks rebuilt eight at a time (the leader's setting): a crash after
/// the fourth recorded one resumes from the cursor; what was written past
/// it is found present; every object reads back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::many_single_char_names)]
async fn parallel_rebuilds_resume_after_a_crash_and_read_back() {
    let mut c = cluster(40).await;
    let g = c.groups().await.remove(0);
    let lost = c.node_index(&g.members[0]).unwrap();
    c.stop_node(lost).await;
    let member = GroupMember {
        node_id: spare(&c, 0).await,
        device_id: 0,
    };
    let opts = RepairOptions {
        parallel: 8,
        verify_cache: Some(Arc::new(VerifyCache::default())),
        ..RepairOptions::default()
    };
    let e = replace_member_with(
        c.meta.as_ref(),
        &c.clients,
        g.id,
        0,
        member,
        &opts,
        async |step| match step {
            arca_lake_controller::repair::RepairStep::Chunk { index: 3, .. } => {
                Err(arca_lake_controller::ControlError::Other("crash".into()))
            }
            _ => Ok(()),
        },
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("crash"));
    let mid = cluster::get_group(c.meta.as_ref(), g.id).await.unwrap();
    assert_eq!(mid.state, GroupState::Repairing);
    let r = replace_member_with(
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
    assert!(r.resumed && r.complete, "{r:?}");
    for i in 0..40u64 {
        let d = Bytes::from(random_bytes(usize::try_from(CHUNK).unwrap(), 900 + i));
        assert_eq!(
            s3::get(&c.backend, BUCKET, &format!("o{i}"))
                .await
                .unwrap()
                .0,
            d
        );
    }
    c.shutdown().await;
}

/// The configurations measured: `ARCA_REPAIR_VERIFY_CONFIGS=base,cache,par8`
/// (default all three; one per process gives its CPU and memory).
fn configs() -> Vec<(bool, usize)> {
    std::env::var("ARCA_REPAIR_VERIFY_CONFIGS")
        .unwrap_or_else(|_| "base,cache,par8".into())
        .split(',')
        .filter_map(|s| match s {
            "base" => Some((false, 1)),
            "cache" => Some((true, 1)),
            "par8" => Some((true, 8)),
            _ => None,
        })
        .collect()
}

/// Measurement (ignored): repairs of 200, 1000 and 4000 chunks in
/// attempts of 64 chunks, without (Phase 3.13) and with the cache; prints
/// wall time, attempts, shards read by verification and metadata
/// transactions, and the time of one full verification pass and of one
/// listing walk of the target for the breakdown.
/// `ARCA_REPAIR_VERIFY_SIZES=200,1000` to choose.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement"]
#[allow(clippy::many_single_char_names)]
async fn measure_repair_attempts() {
    let sizes: Vec<u64> = std::env::var("ARCA_REPAIR_VERIFY_SIZES")
        .unwrap_or_else(|_| "200,1000,4000".into())
        .split(',')
        .filter_map(|s| s.parse().ok())
        .collect();
    for n in sizes {
        for (cached, parallel) in configs() {
            let mut c = cluster(n).await;
            let g = c.groups().await.remove(0);
            let lost = c.node_index(&g.members[0]).unwrap();
            c.stop_node(lost).await;
            let target = spare(&c, 0).await;
            // Without reuse every pass reads everything (Phase 3.13);
            // counted either way.
            let cache = Arc::new(if cached {
                VerifyCache::default()
            } else {
                VerifyCache::counting_only()
            });
            let (attempts, wall, read, txns, sum) =
                repair_in_attempts(&c, Some(Arc::clone(&cache)), 64, target, parallel).await;
            let ti = c.nodes.iter().position(|x| x.id == target).unwrap();
            let addr = c.nodes[ti].addr.to_string();
            let t = Instant::now();
            let mut after = None;
            let mut full = 0;
            loop {
                let p = c
                    .clients
                    .verify_shards_page(&addr, g.id, after, 0)
                    .await
                    .unwrap();
                full += p.shards.len();
                match p.next {
                    Some(x) => after = Some(x),
                    None => break,
                }
            }
            let verify_pass = t.elapsed();
            let t = Instant::now();
            let listed = c.clients.list_shards(&addr, g.id).await.unwrap().len();
            let list_pass = t.elapsed();
            eprintln!(
                "REPAIR chunks={n} cache={cached} parallel={parallel} attempts={attempts} wall={wall:?} verify_shards_read={read} txns={txns} one_full_verify={verify_pass:?} ({full} shards) one_listing={list_pass:?} ({listed} shards) steps verify={:?} walk={:?} reconstruct={:?} write={:?} cursor={:?} completeness={:?}",
                sum.verify, sum.walk, sum.reconstruct, sum.write, sum.cursor, sum.completeness
            );
            c.shutdown().await;
        }
    }
}

/// Measurement (ignored): a repair half done on one target is retargeted
/// to another spare; prints the time from the retarget to `Active`, with
/// and without reuse (the new target is another key: everything on it is
/// verified, the gain is from later attempts on it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement"]
async fn measure_retarget() {
    let sizes: Vec<u64> = std::env::var("ARCA_REPAIR_VERIFY_SIZES")
        .unwrap_or_else(|_| "200,1000,4000".into())
        .split(',')
        .filter_map(|s| s.parse().ok())
        .collect();
    for n in sizes {
        for (cached, parallel) in configs() {
            let mut c = cluster(n).await;
            let g = c.groups().await.remove(0);
            let lost = c.node_index(&g.members[0]).unwrap();
            c.stop_node(lost).await;
            let first = spare(&c, 0).await;
            let second = spare(&c, 1).await;
            let cache = Arc::new(if cached {
                VerifyCache::default()
            } else {
                VerifyCache::counting_only()
            });
            let opts = RepairOptions {
                max_chunks: 64,
                verify_cache: Some(Arc::clone(&cache)),
                parallel,
                ..RepairOptions::default()
            };
            let mut done = 0;
            while done < usize::try_from(n / 2).unwrap() {
                let r = replace_member_with(
                    c.meta.as_ref(),
                    &c.clients,
                    g.id,
                    0,
                    GroupMember {
                        node_id: first,
                        device_id: 0,
                    },
                    &opts,
                    async |_| Ok(()),
                )
                .await
                .unwrap();
                done += r.reconstructed;
            }
            let before = cache.shards_read.load(Ordering::Relaxed);
            let (attempts, wall, read, txns, sum) =
                repair_in_attempts(&c, Some(Arc::clone(&cache)), 64, second, parallel).await;
            eprintln!(
                "RETARGET chunks={n} cache={cached} parallel={parallel} first_done={done} attempts={attempts} retarget_to_active={wall:?} verify_shards_read={} txns={txns} steps verify={:?} walk={:?} reconstruct={:?} write={:?} cursor={:?} completeness={:?}",
                read - before,
                sum.verify,
                sum.walk,
                sum.reconstruct,
                sum.write,
                sum.cursor,
                sum.completeness
            );
            c.shutdown().await;
        }
    }
}
