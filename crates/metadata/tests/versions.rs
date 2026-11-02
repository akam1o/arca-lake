//! Versioning semantics exposed in Phase 3.1 (RFC-007 §5.4): version-
//! specific reads and deletes, null-version preservation when versioning
//! is enabled after writes, suspended-bucket null semantics,
//! ListObjectVersions ordering and pagination.

#![allow(clippy::too_many_lines, clippy::assert_is_empty)]

use std::future::Future;

use arca_lake_metadata::MetaError;
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::list::{self, ListVersionsRequest};
use arca_lake_metadata::ops::object::{
    self, CommitInfo, Found, ManifestFound, Precondition, begin_put, commit_put, delete_object,
    delete_version, get_generation, head_object, head_version,
};
use arca_lake_metadata::ops::{bucket, gc};
use arca_lake_metadata::records::{GenId, GenState, Versioning};

fn run<T>(f: impl Future<Output = T>) -> T {
    futures::executor::block_on(f)
}

const LEASE: u64 = 60_000;

async fn put(kv: &MemKv, key: &str, etag: &str, now: u64) -> (GenId, String) {
    let p = begin_put(kv, "b", key, now, LEASE).await.unwrap();
    let c = commit_put(
        kv,
        &p.generation,
        &CommitInfo {
            size: 1,
            etag: etag.into(),
            ..Default::default()
        },
        &Precondition::default(),
        now,
    )
    .await
    .unwrap();
    (c.generation, c.version_id)
}

async fn versions(kv: &MemKv, max: usize) -> Vec<(String, String, bool, bool)> {
    list::list_versions(
        kv,
        "b",
        &ListVersionsRequest {
            max_keys: max,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .versions
    .into_iter()
    .map(|v| (v.key, v.version_id, v.is_latest, v.delete_marker))
    .collect()
}

async fn state(kv: &MemKv, g: &GenId) -> GenState {
    get_generation(kv, g).await.unwrap().state
}

#[test]
fn enabling_versioning_keeps_the_null_version_as_a_version() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let (g_null, v_null) = put(&kv, "k", "a", 2).await;
        assert_eq!(v_null, "null");
        bucket::set_versioning(&kv, "b", Versioning::Enabled)
            .await
            .unwrap();
        let (g2, v2) = put(&kv, "k", "b", 3).await;
        assert_ne!(v2, "null");
        // The null version is kept (AWS), not retired.
        assert_eq!(state(&kv, &g_null).await, GenState::Committed);
        assert_eq!(
            versions(&kv, 10).await,
            vec![
                ("k".into(), v2.clone(), true, false),
                ("k".into(), "null".into(), false, false),
            ]
        );
        // GET versionId=null reads the old generation.
        match object::read_manifest_version(&kv, "b", "k", Some("null"), None)
            .await
            .unwrap()
        {
            ManifestFound::Manifest(m) => {
                assert_eq!(m.pointer.generation, g_null);
                assert!(!m.is_latest);
                assert_eq!(m.header.etag, "a");
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            head_version(&kv, "b", "k", Some("nope")).await.unwrap(),
            Found::Missing
        ));
        // Delete the null version explicitly: retired, current unchanged.
        let d = delete_version(&kv, "b", "k", "null", 4).await.unwrap();
        assert!(d.existed && !d.was_delete_marker && d.retired == Some(g_null));
        assert!(matches!(
            state(&kv, &g_null).await,
            GenState::Retiring { .. }
        ));
        assert_eq!(
            versions(&kv, 10).await,
            vec![("k".into(), v2.clone(), true, false)]
        );
        // Idempotent: again is a no-op.
        let d = delete_version(&kv, "b", "k", "null", 5).await.unwrap();
        assert!(!d.existed);
        let _ = g2;
    });
}

#[test]
fn delete_marker_then_delete_of_marker_restores_and_version_delete_of_current() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Enabled, 1)
            .await
            .unwrap();
        let (g1, v1) = put(&kv, "k", "a", 2).await;
        let (g2, v2) = put(&kv, "k", "b", 3).await;
        let d = delete_object(&kv, "b", "k", 4).await.unwrap();
        let marker = d.delete_marker_version.unwrap();
        assert!(matches!(
            head_version(&kv, "b", "k", None).await.unwrap(),
            Found::DeleteMarker { .. }
        ));
        assert!(matches!(
            head_version(&kv, "b", "k", Some(&marker)).await.unwrap(),
            Found::DeleteMarker { .. }
        ));
        // The old versions are still readable by id.
        assert!(matches!(
            head_version(&kv, "b", "k", Some(&v1)).await.unwrap(),
            Found::Object(i) if i.pointer.generation == g1
        ));
        assert_eq!(
            versions(&kv, 10).await,
            vec![
                ("k".into(), marker.clone(), true, true),
                ("k".into(), v2.clone(), false, false),
                ("k".into(), v1.clone(), false, false),
            ]
        );
        // Deleting the marker restores v2 as current.
        let d = delete_version(&kv, "b", "k", &marker, 5).await.unwrap();
        assert!(d.was_delete_marker && d.retired.is_none());
        let info = head_object(&kv, "b", "k").await.unwrap().unwrap();
        assert_eq!(info.pointer.generation, g2);
        assert_eq!(info.pointer.version_id, v2);
        assert_eq!(info.pointer.next_ordinal, 3, "ordinals keep increasing");
        // Deleting the current data version makes v1 current and retires g2.
        let d = delete_version(&kv, "b", "k", &v2, 6).await.unwrap();
        assert_eq!(d.retired, Some(g2));
        let info = head_object(&kv, "b", "k").await.unwrap().unwrap();
        assert_eq!(info.pointer.generation, g1);
        assert_eq!(
            versions(&kv, 10).await,
            vec![("k".into(), v1.clone(), true, false)]
        );
        // Deleting the last version removes the key entirely.
        delete_version(&kv, "b", "k", &v1, 7).await.unwrap();
        assert!(head_object(&kv, "b", "k").await.unwrap().is_none());
        assert!(versions(&kv, 10).await.is_empty());
        // Everything retired is collectable; nothing referenced remains.
        let collectable = gc::collectable(&kv, 1_000, 0, 100).await.unwrap();
        assert_eq!(collectable.len(), 2);
        bucket::delete_bucket(&kv, "b").await.unwrap();
    });
}

#[test]
fn suspended_bucket_overwrites_the_null_version_and_keeps_real_versions() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Enabled, 1)
            .await
            .unwrap();
        let (g1, v1) = put(&kv, "k", "a", 2).await;
        bucket::set_versioning(&kv, "b", Versioning::Suspended)
            .await
            .unwrap();
        let (g_null1, v) = put(&kv, "k", "b", 3).await;
        assert_eq!(v, "null");
        assert_eq!(
            state(&kv, &g1).await,
            GenState::Committed,
            "real version kept"
        );
        let (g_null2, _) = put(&kv, "k", "c", 4).await;
        assert!(
            matches!(state(&kv, &g_null1).await, GenState::Retiring { .. }),
            "null overwritten"
        );
        assert_eq!(
            versions(&kv, 10).await,
            vec![
                ("k".into(), "null".into(), true, false),
                ("k".into(), v1.clone(), false, false),
            ]
        );
        // DELETE on a suspended bucket writes a null delete marker that
        // replaces the null version.
        let d = delete_object(&kv, "b", "k", 5).await.unwrap();
        assert_eq!(d.delete_marker_version.as_deref(), Some("null"));
        assert_eq!(d.retired, Some(g_null2));
        assert_eq!(
            versions(&kv, 10).await,
            vec![
                ("k".into(), "null".into(), true, true),
                ("k".into(), v1.clone(), false, false),
            ]
        );
        // Re-enable: a new PUT keeps the null marker as a version.
        bucket::set_versioning(&kv, "b", Versioning::Enabled)
            .await
            .unwrap();
        let (_, v3) = put(&kv, "k", "d", 6).await;
        assert_eq!(
            versions(&kv, 10).await,
            vec![
                ("k".into(), v3, true, false),
                ("k".into(), "null".into(), false, true),
                ("k".into(), v1, false, false),
            ]
        );
        // A non-current null version from before is overwritten when the
        // bucket is suspended again and written to.
        bucket::set_versioning(&kv, "b", Versioning::Suspended)
            .await
            .unwrap();
        put(&kv, "k", "e", 7).await;
        let vs = versions(&kv, 10).await;
        assert_eq!(vs.iter().filter(|v| v.1 == "null").count(), 1);
        assert!(vs[0].2 && vs[0].1 == "null");
    });
}

#[test]
fn list_versions_orders_paginates_and_groups_by_delimiter() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Enabled, 1)
            .await
            .unwrap();
        let (_, a1) = put(&kv, "a", "1", 2).await;
        let (_, a2) = put(&kv, "a", "2", 3).await;
        let (_, b1) = put(&kv, "b", "1", 4).await;
        put(&kv, "dir/x", "1", 5).await;
        put(&kv, "dir/y", "1", 6).await;
        delete_object(&kv, "b", "b", 7).await.unwrap();
        // Page of 2, then resume with the markers: newest first per key.
        let p1 = list::list_versions(
            &kv,
            "b",
            &ListVersionsRequest {
                max_keys: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(p1.is_truncated);
        assert_eq!(
            p1.versions
                .iter()
                .map(|v| v.version_id.clone())
                .collect::<Vec<_>>(),
            vec![a2.clone(), a1.clone()]
        );
        let (km, vm) = p1.next_markers.clone().unwrap();
        assert_eq!((km.as_str(), vm.as_str()), ("a", a1.as_str()));
        let p2 = list::list_versions(
            &kv,
            "b",
            &ListVersionsRequest {
                max_keys: 2,
                key_marker: Some(km),
                version_id_marker: Some(vm),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(p2.versions.len(), 2);
        assert_eq!(p2.versions[0].key, "b");
        assert!(p2.versions[0].delete_marker && p2.versions[0].is_latest);
        assert_eq!(p2.versions[1].version_id, b1);
        assert!(p2.is_truncated);
        // Key marker without a version marker skips the whole key.
        let p3 = list::list_versions(
            &kv,
            "b",
            &ListVersionsRequest {
                max_keys: 10,
                key_marker: Some("b".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            p3.versions
                .iter()
                .map(|v| v.key.clone())
                .collect::<Vec<_>>(),
            vec!["dir/x", "dir/y"]
        );
        assert!(!p3.is_truncated);
        // Delimiter groups the directory.
        let p4 = list::list_versions(
            &kv,
            "b",
            &ListVersionsRequest {
                max_keys: 10,
                delimiter: Some("/".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(p4.common_prefixes, vec!["dir/"]);
        assert_eq!(p4.versions.len(), 4);
        // Prefix.
        let p5 = list::list_versions(
            &kv,
            "b",
            &ListVersionsRequest {
                max_keys: 10,
                prefix: "dir/".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(p5.versions.len(), 2);
        assert_eq!(
            list::list_versions(&kv, "b", &ListVersionsRequest::default())
                .await
                .unwrap_err(),
            MetaError::InvalidArgument("max_keys must be >= 1".into())
        );
    });
}

#[test]
fn delete_with_if_match_is_evaluated_in_the_transaction() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        put(&kv, "k", "a", 2).await;
        let wrong = Precondition {
            if_match: Some("zzz".into()),
            ..Default::default()
        };
        assert_eq!(
            object::delete_object_if(&kv, "b", "k", &wrong, 3)
                .await
                .unwrap_err(),
            MetaError::PreconditionFailed
        );
        assert!(head_object(&kv, "b", "k").await.unwrap().is_some());
        let right = Precondition {
            if_match: Some("a".into()),
            ..Default::default()
        };
        object::delete_object_if(&kv, "b", "k", &right, 4)
            .await
            .unwrap();
        assert!(head_object(&kv, "b", "k").await.unwrap().is_none());
        assert_eq!(
            object::delete_object_if(&kv, "b", "k", &right, 5)
                .await
                .unwrap_err(),
            MetaError::NoSuchKey
        );
    });
}
