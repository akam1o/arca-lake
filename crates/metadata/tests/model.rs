//! Model-based test of the generation state machine (RFC-003 §5.3, §5.6,
//! §5.8; RFC-008 invariants).
//!
//! A random sequence of protocol operations is applied to the in-memory
//! store and to a small reference model at the same time. After every
//! operation the store must agree with the model about what is visible,
//! and the invariants below must hold over every generation:
//!
//! - I1 (visibility): the current pointer of a key names exactly the
//!   generation of the last successful commit for that key, or nothing.
//! - I2 (unreferenced retirement): a `Retiring` generation is referenced by
//!   no current pointer.
//! - I3 (committed stays): a `Committed` generation is referenced by a
//!   pointer (unversioned buckets) and is never removed by GC.
//! - I4 (expiry only of expired leases): `expire_pending` retires only
//!   generations that were `Pending` with `lease_until <= now`.
//! - I5 (commit exclusivity): after a generation is retired, its commit
//!   fails; after it is committed, abort and a second commit fail.

#![allow(clippy::too_many_lines)]

use std::collections::BTreeMap;

use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::object::{
    CommitInfo, Precondition, abort_put, append_chunks, begin_put, commit_put, delete_object,
    get_generation, head_object,
};
use arca_lake_metadata::ops::{bucket, gc};
use arca_lake_metadata::records::{ChunkRecord, DurablePlacement, GenId, GenState, Versioning};
use arca_lake_metadata::{MetaError, keys};
use proptest::prelude::*;

const LEASE: u64 = 100;
const KEYS: [&str; 3] = ["a", "b", "c"];

#[derive(Debug, Clone)]
enum Op {
    Begin(usize),
    Append(usize),
    /// Commit generation `0` with `If-None-Match: *` when `1` is true.
    Commit(usize, bool),
    Abort(usize),
    Delete(usize),
    Expire,
    Collect,
    Tick(u64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..3usize).prop_map(Op::Begin),
        (0..6usize).prop_map(Op::Append),
        (0..6usize, any::<bool>()).prop_map(|(g, c)| Op::Commit(g, c)),
        (0..6usize).prop_map(Op::Abort),
        (0..3usize).prop_map(Op::Delete),
        Just(Op::Expire),
        Just(Op::Collect),
        (1..80u64).prop_map(Op::Tick),
    ]
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ModelState {
    Pending { lease_until: u64 },
    Committed,
    Retiring,
    Gone,
}

#[derive(Debug, Clone)]
struct ModelGen {
    id: GenId,
    key: usize,
    state: ModelState,
}

#[derive(Debug, Default)]
struct Model {
    now: u64,
    gens: Vec<ModelGen>,
    /// Visible generation per key.
    visible: BTreeMap<usize, usize>,
}

fn chunk(i: u32) -> (u32, ChunkRecord) {
    (
        i,
        ChunkRecord {
            chunk_id: [u8::try_from(i % 256).unwrap(); 16],
            offset: 0,
            len: 1,
            scheme: "rs-gf8-v1".into(),
            data_shards: 2,
            parity_shards: 1,
            crc32c: i,
            placement: DurablePlacement {
                group_id: 1,
                epoch: 1,
            },
        },
    )
}

async fn step(kv: &MemKv, m: &mut Model, op: &Op) {
    match *op {
        Op::Begin(key) => {
            let p = begin_put(kv, "b", KEYS[key], m.now, LEASE).await.unwrap();
            m.gens.push(ModelGen {
                id: p.generation,
                key,
                state: ModelState::Pending {
                    lease_until: m.now + LEASE,
                },
            });
        }
        Op::Append(g) => {
            let Some(entry) = m.gens.get(g).cloned() else {
                return;
            };
            let r = append_chunks(kv, &entry.id, &[chunk(0)], m.now).await;
            match entry.state {
                ModelState::Pending { lease_until } if lease_until > m.now => r.unwrap(),
                ModelState::Pending { .. } => assert_eq!(r.unwrap_err(), MetaError::LeaseExpired),
                _ => assert!(r.is_err(), "append to a non-pending generation"),
            }
        }
        Op::Commit(g, create) => {
            let Some(entry) = m.gens.get(g).cloned() else {
                return;
            };
            let pre = Precondition {
                if_none_match_any: create,
                ..Default::default()
            };
            let info = CommitInfo {
                size: 1,
                etag: format!("e{g}"),
                content_type: None,
                user_metadata: vec![],
                chunk_count: 0,
            };
            let r = commit_put(kv, &entry.id, &info, &pre, m.now).await;
            let leased =
                matches!(entry.state, ModelState::Pending { lease_until } if lease_until > m.now);
            let occupied = m.visible.contains_key(&entry.key);
            if !leased {
                assert!(r.is_err(), "I5: commit of a non-leased generation");
                return;
            }
            if create && occupied {
                assert_eq!(r.unwrap_err(), MetaError::PreconditionFailed);
                return;
            }
            let c = r.unwrap();
            // Displaced generation retires (unversioned bucket).
            if let Some(old) = m.visible.insert(entry.key, g) {
                m.gens[old].state = ModelState::Retiring;
                assert_eq!(c.retired, Some(m.gens[old].id));
            } else {
                assert_eq!(c.retired, None);
            }
            m.gens[g].state = ModelState::Committed;
        }
        Op::Abort(g) => {
            let Some(entry) = m.gens.get(g).cloned() else {
                return;
            };
            let r = abort_put(kv, &entry.id, m.now).await;
            match entry.state {
                ModelState::Pending { .. } => {
                    r.unwrap();
                    m.gens[g].state = ModelState::Retiring;
                }
                ModelState::Gone => assert_eq!(r.unwrap_err(), MetaError::NoSuchGeneration),
                _ => assert!(r.is_err(), "I5: abort of a non-pending generation"),
            }
        }
        Op::Delete(key) => {
            let d = delete_object(kv, "b", KEYS[key], m.now).await.unwrap();
            match m.visible.remove(&key) {
                Some(old) => {
                    assert_eq!(d.retired, Some(m.gens[old].id));
                    m.gens[old].state = ModelState::Retiring;
                }
                None => assert_eq!(d.retired, None),
            }
        }
        Op::Expire => {
            let retired = gc::expire_pending(kv, m.now, 100).await.unwrap();
            for id in &retired {
                let g = m.gens.iter_mut().find(|g| g.id == *id).unwrap();
                assert!(
                    matches!(g.state, ModelState::Pending { lease_until } if lease_until <= m.now),
                    "I4: expired a generation that was {:?} at {}",
                    g.state,
                    m.now
                );
                g.state = ModelState::Retiring;
            }
            // Every expired pending generation was retired.
            for g in &m.gens {
                assert!(
                    !matches!(g.state, ModelState::Pending { lease_until } if lease_until <= m.now),
                    "expiry missed a generation"
                );
            }
        }
        Op::Collect => {
            for id in gc::collectable(kv, m.now, 0, 100).await.unwrap() {
                let g = m.gens.iter_mut().find(|g| g.id == id).unwrap();
                assert_eq!(
                    g.state,
                    ModelState::Retiring,
                    "I3: only retiring generations are collectable"
                );
                gc::finish(kv, &id).await.unwrap();
                g.state = ModelState::Gone;
            }
        }
        Op::Tick(ms) => m.now += ms,
    }
}

async fn check_invariants(kv: &MemKv, m: &Model) {
    // I1: visibility agrees with the model.
    for (key, name) in KEYS.iter().enumerate() {
        let head = head_object(kv, "b", name).await.unwrap();
        match m.visible.get(&key) {
            Some(g) => assert_eq!(
                head.map(|h| h.pointer.generation),
                Some(m.gens[*g].id),
                "I1: key {name}"
            ),
            None => assert!(head.is_none(), "I1: key {name} should be invisible"),
        }
    }
    // Per-generation state agrees; I2 and I3 follow from I1 plus the state.
    for g in &m.gens {
        let header = get_generation(kv, &g.id).await;
        match g.state {
            ModelState::Gone => assert_eq!(header.unwrap_err(), MetaError::NoSuchGeneration),
            ModelState::Pending { lease_until } => assert_eq!(
                header.unwrap().state,
                GenState::Pending {
                    lease_until_ms: lease_until
                }
            ),
            ModelState::Committed => {
                assert_eq!(header.unwrap().state, GenState::Committed);
                assert_eq!(
                    m.visible.get(&g.key).map(|i| m.gens[*i].id),
                    Some(g.id),
                    "I3"
                );
            }
            ModelState::Retiring => {
                assert!(matches!(header.unwrap().state, GenState::Retiring { .. }));
                assert_ne!(
                    m.visible.get(&g.key).map(|i| m.gens[*i].id),
                    Some(g.id),
                    "I2"
                );
            }
        }
    }
    // No stray pending-index or retired-index entries point at unknown
    // generations.
    let pending = kv.dump_prefix(&keys::pending_expired_range(u64::MAX).begin);
    for (k, _) in pending {
        let id = keys::gen_from_index_key(&k).unwrap();
        let g = m
            .gens
            .iter()
            .find(|g| g.id == id)
            .expect("pending index names a known generation");
        assert!(matches!(g.state, ModelState::Pending { .. }));
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, failure_persistence: None, ..ProptestConfig::default() })]
    #[test]
    fn generation_state_machine_matches_model(ops in prop::collection::vec(op(), 1..60)) {
        futures::executor::block_on(async {
            let kv = MemKv::default();
            bucket::create_bucket(&kv, "b", Versioning::Disabled, 0).await.unwrap();
            let mut m = Model::default();
            for op in &ops {
                step(&kv, &mut m, op).await;
                check_invariants(&kv, &m).await;
            }
        });
    }
}
