//! Model-based test of the cache placement records (RFC-006 §5.2, §5.4).
//!
//! Random sequences of insert / remove / touch / pin / finish are applied
//! to the in-memory store and to a reference model. After every step:
//!
//! - C1: the tier usage counter equals the sum of the bytes of its
//!   placements.
//! - C2: recency index entries and placements correspond one to one, and
//!   `lru_candidates` lists placements in ascending `last_access_ms`.
//! - C3: a placement exists only for a committed generation; `insert`
//!   refuses generations that are not committed and copies that would
//!   exceed the capacity.
//! - C4: `finish` of a generation removes its placements, index entries,
//!   and usage, and never touches other generations.

use std::collections::BTreeMap;

use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::cache::{self, InsertOutcome};
use arca_lake_metadata::ops::object::{CommitInfo, Precondition, abort_put, begin_put, commit_put};
use arca_lake_metadata::ops::{bucket, gc};
use arca_lake_metadata::records::{CachePlacement, GenId, Versioning};
use proptest::prelude::*;

const TIER: &str = "fast";
const CAPACITY: u64 = 10_000;

#[derive(Debug, Clone)]
enum Op {
    /// Insert chunk `1` of generation `0` with `2` bytes.
    Insert(usize, u32, u64),
    Remove(usize, u32),
    Touch(usize, u32),
    Pin(usize, bool),
    /// Retire and finish generation `0`.
    Finish(usize),
    Tick(u64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..4usize, 0..3u32, 1..4_000u64).prop_map(|(g, i, b)| Op::Insert(g, i, b)),
        (0..4usize, 0..3u32).prop_map(|(g, i)| Op::Remove(g, i)),
        (0..4usize, 0..3u32).prop_map(|(g, i)| Op::Touch(g, i)),
        (0..4usize, any::<bool>()).prop_map(|(g, p)| Op::Pin(g, p)),
        (0..4usize).prop_map(Op::Finish),
        (1..50u64).prop_map(Op::Tick),
    ]
}

#[derive(Debug, Default)]
struct Model {
    now: u64,
    /// `(gen index, chunk)` → `(bytes, pinned, last_access)`.
    entries: BTreeMap<(usize, u32), (u64, bool, u64)>,
    committed: Vec<bool>,
}

impl Model {
    fn usage(&self) -> u64 {
        self.entries.values().map(|(b, _, _)| *b).sum()
    }
}

async fn setup(kv: &MemKv) -> Vec<GenId> {
    bucket::create_bucket(kv, "b", Versioning::Disabled, 0)
        .await
        .unwrap();
    let mut gens = Vec::new();
    for i in 0..4 {
        let p = begin_put(kv, "b", &format!("k{i}"), 1, 1_000_000)
            .await
            .unwrap();
        // Generations 0..3 committed, generation 3 left pending/aborted to
        // exercise C3.
        if i < 3 {
            commit_put(
                kv,
                &p.generation,
                &CommitInfo {
                    size: 1,
                    etag: format!("e{i}"),
                    content_type: None,
                    user_metadata: vec![],
                    chunk_count: 3,
                    checksum: None,
                },
                &Precondition::default(),
                2,
            )
            .await
            .unwrap();
        } else {
            abort_put(kv, &p.generation, 2).await.unwrap();
        }
        gens.push(p.generation);
    }
    gens
}

#[allow(clippy::many_single_char_names)]
async fn step(kv: &MemKv, gens: &[GenId], m: &mut Model, op: &Op) {
    match *op {
        Op::Insert(g, i, bytes) => {
            let p = CachePlacement {
                tier: TIER.into(),
                group_id: 7,
                epoch: 1,
                bytes,
                pinned: false,
                last_access_ms: m.now,
                created_ms: m.now,
            };
            let r = cache::insert(kv, &gens[g], i, &p, CAPACITY).await.unwrap();
            let expected = if !m.committed[g] {
                InsertOutcome::GenerationGone
            } else if m.entries.contains_key(&(g, i)) {
                InsertOutcome::AlreadyPresent
            } else if m.usage() + bytes > CAPACITY {
                InsertOutcome::OverCapacity
            } else {
                m.entries.insert((g, i), (bytes, false, m.now));
                InsertOutcome::Inserted
            };
            assert_eq!(r, expected, "C3: insert gen {g} chunk {i}");
        }
        Op::Remove(g, i) => {
            let r = cache::remove(kv, &gens[g], i).await.unwrap();
            assert_eq!(r.is_some(), m.entries.remove(&(g, i)).is_some());
        }
        Op::Touch(g, i) => {
            cache::touch(kv, &gens[g], i, m.now).await.unwrap();
            if let Some(e) = m.entries.get_mut(&(g, i))
                && e.2 < m.now
            {
                e.2 = m.now;
            }
        }
        Op::Pin(g, pinned) => {
            let changed = cache::set_pinned(kv, &gens[g], pinned).await.unwrap();
            let mut expected = 0;
            for ((eg, _), e) in &mut m.entries {
                if *eg == g && e.1 != pinned {
                    e.1 = pinned;
                    expected += 1;
                }
            }
            assert_eq!(changed, expected);
        }
        Op::Finish(g) => {
            if !m.committed[g] {
                return;
            }
            // Displace the generation by deleting the key, then finish it.
            arca_lake_metadata::ops::object::delete_object(kv, "b", &format!("k{g}"), m.now)
                .await
                .unwrap();
            gc::finish(kv, &gens[g]).await.unwrap();
            m.committed[g] = false;
            m.entries.retain(|(eg, _), _| *eg != g);
        }
        Op::Tick(ms) => m.now += ms,
    }
}

async fn check(kv: &MemKv, gens: &[GenId], m: &Model) {
    // C1
    assert_eq!(
        cache::usage(kv, TIER).await.unwrap(),
        m.usage(),
        "C1: usage"
    );
    // C2: candidates cover every entry, ordered by recency, with the model's
    // pinned flags and bytes.
    let candidates = cache::lru_candidates(kv, TIER, 0, 1000).await.unwrap();
    assert_eq!(
        candidates.len(),
        m.entries.len(),
        "C2: one index entry per placement"
    );
    let mut last = 0;
    for (generation, index, p) in &candidates {
        assert!(p.last_access_ms >= last, "C2: ascending recency");
        last = p.last_access_ms;
        let g = gens.iter().position(|x| x == generation).unwrap();
        let e = m
            .entries
            .get(&(g, *index))
            .expect("C2: candidate is a placement");
        assert_eq!((p.bytes, p.pinned, p.last_access_ms), *e);
    }
    // C4: no placement for finished generations.
    for (g, generation) in gens.iter().enumerate() {
        let placements = cache::placements_of(kv, generation).await.unwrap();
        let expected = m.entries.keys().filter(|(eg, _)| *eg == g).count();
        assert_eq!(placements.len(), expected, "C4: gen {g}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, failure_persistence: None, ..ProptestConfig::default() })]
    #[test]
    fn cache_records_match_model(ops in prop::collection::vec(op(), 1..60)) {
        futures::executor::block_on(async {
            let kv = MemKv::default();
            let gens = setup(&kv).await;
            let mut m = Model { committed: vec![true, true, true, false], ..Model::default() };
            for op in &ops {
                step(&kv, &gens, &mut m, op).await;
                check(&kv, &gens, &m).await;
            }
        });
    }
}
