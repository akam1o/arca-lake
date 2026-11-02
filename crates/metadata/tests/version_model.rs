//! Model-based test of the versioned-key state machine (RFC-007 §5.4,
//! acceptance criterion A15).
//!
//! A random sequence of PUT, DELETE, DELETE-version, versioning changes and
//! reads is applied to the in-memory store and to a reference model of one
//! key. After every operation the store must agree with the model about the
//! current version, the version list (ids, order, markers, `IsLatest`) and
//! the bytes behind every version id, and every generation the model
//! considers gone must be retiring.

#![allow(clippy::too_many_lines)]

use arca_lake_metadata::MetaError;
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::bucket;
use arca_lake_metadata::ops::list::{self, ListVersionsRequest};
use arca_lake_metadata::ops::object::{
    self, CommitInfo, Found, Precondition, begin_put, commit_put, delete_object, delete_version,
    get_generation, head_version,
};
use arca_lake_metadata::records::{GenId, GenState, Versioning};
use proptest::prelude::*;

const KEY: &str = "k";
const LEASE: u64 = 1_000_000;

#[derive(Debug, Clone)]
enum Op {
    Put(u8),
    Delete,
    /// Delete the version at this index of the model's list (modulo len).
    DeleteVersion(usize),
    Enable,
    Suspend,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => any::<u8>().prop_map(Op::Put),
        2 => Just(Op::Delete),
        2 => (0..8usize).prop_map(Op::DeleteVersion),
        1 => Just(Op::Enable),
        1 => Just(Op::Suspend),
    ]
}

/// One version in the model, newest first in `Model::versions`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MVersion {
    id: String,
    marker: bool,
    /// Data tag (the ETag we committed) for data versions.
    tag: Option<u8>,
    generation: Option<GenId>,
}

#[derive(Debug, Default)]
struct Model {
    versioning: Option<Versioning>,
    /// Newest first; the first entry is current.
    versions: Vec<MVersion>,
    /// Generations that must be retiring (displaced or deleted).
    gone: Vec<GenId>,
}

impl Model {
    fn state(&self) -> Versioning {
        self.versioning.unwrap_or(Versioning::Disabled)
    }

    /// Remove the null version (data or marker) from anywhere in the list,
    /// recording its generation as gone.
    fn drop_null(&mut self) {
        if let Some(pos) = self.versions.iter().position(|v| v.id == "null") {
            let v = self.versions.remove(pos);
            if let Some(g) = v.generation {
                self.gone.push(g);
            }
        }
    }

    fn put(&mut self, tag: u8, id: String, generation: GenId) {
        match self.state() {
            Versioning::Enabled => {}
            Versioning::Disabled | Versioning::Suspended => self.drop_null(),
        }
        self.versions.insert(
            0,
            MVersion {
                id,
                marker: false,
                tag: Some(tag),
                generation: Some(generation),
            },
        );
    }

    fn delete(&mut self, id: Option<String>) {
        match self.state() {
            Versioning::Disabled => {
                if let Some(v) = self.versions.first()
                    && let Some(g) = v.generation
                {
                    self.gone.push(g);
                }
                self.versions.clear();
            }
            Versioning::Enabled => self.versions.insert(
                0,
                MVersion {
                    id: id.expect("marker id"),
                    marker: true,
                    tag: None,
                    generation: None,
                },
            ),
            Versioning::Suspended => {
                self.drop_null();
                self.versions.insert(
                    0,
                    MVersion {
                        id: "null".into(),
                        marker: true,
                        tag: None,
                        generation: None,
                    },
                );
            }
        }
    }

    fn delete_version(&mut self, index: usize) -> Option<String> {
        if self.versions.is_empty() {
            return None;
        }
        let v = self.versions.remove(index % self.versions.len());
        if let Some(g) = v.generation {
            self.gone.push(g);
        }
        Some(v.id)
    }
}

async fn put(kv: &MemKv, tag: u8) -> (GenId, String) {
    let p = begin_put(kv, "b", KEY, 1, LEASE).await.unwrap();
    let c = commit_put(
        kv,
        &p.generation,
        &CommitInfo {
            size: u64::from(tag) + 1,
            etag: format!("{tag:02x}"),
            ..Default::default()
        },
        &Precondition::default(),
        1,
    )
    .await
    .unwrap();
    (c.generation, c.version_id)
}

async fn check(kv: &MemKv, model: &Model, step: usize) {
    // Version list: ids, order, markers, IsLatest.
    let listed = list::list_versions(
        kv,
        "b",
        &ListVersionsRequest {
            max_keys: 1000,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .versions;
    let got: Vec<(String, bool, bool)> = listed
        .iter()
        .map(|v| (v.version_id.clone(), v.delete_marker, v.is_latest))
        .collect();
    let want: Vec<(String, bool, bool)> = model
        .versions
        .iter()
        .enumerate()
        .map(|(i, v)| (v.id.clone(), v.marker, i == 0))
        .collect();
    assert_eq!(got, want, "step {step}: version list");
    // Current version.
    match (
        head_version(kv, "b", KEY, None).await.unwrap(),
        model.versions.first(),
    ) {
        (Found::Missing, None) => {}
        (Found::DeleteMarker { version_id, .. }, Some(v)) if v.marker => {
            assert_eq!(version_id, v.id);
        }
        (Found::Object(i), Some(v)) if !v.marker => {
            assert_eq!(i.pointer.version_id, v.id, "step {step}");
            assert_eq!(
                i.header.etag,
                format!("{:02x}", v.tag.unwrap()),
                "step {step}"
            );
        }
        (got, want) => panic!("step {step}: current {got:?} vs model {want:?}"),
    }
    // Every version by id.
    for v in &model.versions {
        match head_version(kv, "b", KEY, Some(&v.id)).await.unwrap() {
            Found::DeleteMarker { version_id, .. } => {
                assert!(v.marker && version_id == v.id, "step {step}");
            }
            Found::Object(i) => {
                assert!(!v.marker, "step {step}: {} should be a marker", v.id);
                assert_eq!(i.header.etag, format!("{:02x}", v.tag.unwrap()));
                assert_eq!(i.header.state, GenState::Committed);
            }
            Found::Missing => panic!("step {step}: version {} missing", v.id),
        }
    }
    // Gone generations are retiring (never committed, never resurrected).
    for g in &model.gone {
        match get_generation(kv, g).await {
            Ok(h) => assert!(
                matches!(h.state, GenState::Retiring { .. }),
                "step {step}: {h:?}"
            ),
            Err(MetaError::NoSuchGeneration) => {}
            Err(e) => panic!("{e}"),
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 400, .. ProptestConfig::default() })]
    #[test]
    fn versioned_key_matches_the_model(ops in proptest::collection::vec(op(), 1..40)) {
        futures::executor::block_on(async {
            let kv = MemKv::default();
            bucket::create_bucket(&kv, "b", Versioning::Disabled, 1).await.unwrap();
            let mut model = Model::default();
            for (step, o) in ops.iter().enumerate() {
                match o {
                    Op::Put(tag) => {
                        let (g, id) = put(&kv, *tag).await;
                        model.put(*tag, id, g);
                    }
                    Op::Delete => {
                        let d = delete_object(&kv, "b", KEY, 2).await.unwrap();
                        model.delete(d.delete_marker_version.clone());
                    }
                    Op::DeleteVersion(i) => {
                        if let Some(id) = model.delete_version(*i) {
                            let d = delete_version(&kv, "b", KEY, &id, 3).await.unwrap();
                            prop_assert!(d.existed, "step {step}: version {id} should exist");
                            // Idempotent.
                            let again = delete_version(&kv, "b", KEY, &id, 3).await.unwrap();
                            prop_assert!(!again.existed);
                        }
                    }
                    Op::Enable => {
                        bucket::set_versioning(&kv, "b", Versioning::Enabled).await.unwrap();
                        model.versioning = Some(Versioning::Enabled);
                    }
                    Op::Suspend => {
                        // AWS allows Suspended only after Enabled; a never-
                        // enabled bucket stays unversioned.
                        if model.versioning.is_some() {
                            bucket::set_versioning(&kv, "b", Versioning::Suspended).await.unwrap();
                            model.versioning = Some(Versioning::Suspended);
                        }
                    }
                }
                check(&kv, &model, step).await;
            }
            // Deleting every version leaves nothing behind.
            while let Some(id) = model.delete_version(0) {
                delete_version(&kv, "b", KEY, &id, 9).await.unwrap();
            }
            check(&kv, &model, usize::MAX).await;
            prop_assert!(matches!(head_version(&kv, "b", KEY, None).await.unwrap(), Found::Missing));
            prop_assert!(object::read_manifest(&kv, "b", KEY, None).await.unwrap().is_none());
            Ok(())
        })?;
    }
}
