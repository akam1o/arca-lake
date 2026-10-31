//! Write-protocol tests against the in-memory store with FoundationDB limits.
//!
//! Concurrency is exercised with `futures::join!`: the in-memory store
//! yields at every operation, so joined transactions interleave and real
//! optimistic conflicts occur.

#![allow(
    clippy::assert_is_empty,
    clippy::unnecessary_unwrap,
    clippy::too_many_lines
)]

use std::future::Future;

use arca_lake_metadata::MetaError;
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::object::{
    CommitInfo, Precondition, abort_put, append_chunks, begin_put, commit_put, delete_object,
    get_generation, head_object, read_manifest,
};
use arca_lake_metadata::ops::{bucket, gc, list};
use arca_lake_metadata::records::{ChunkRecord, DurablePlacement, GenState, Versioning};

fn run<T>(f: impl Future<Output = T>) -> T {
    futures::executor::block_on(f)
}

const LEASE: u64 = 60_000;

fn chunk(i: u32) -> (u32, ChunkRecord) {
    (
        i,
        ChunkRecord {
            chunk_id: [u8::try_from(i % 256).unwrap(); 16],
            offset: u64::from(i) * 4096,
            len: 4096,
            scheme: "rs-gf8-v1".into(),
            data_shards: 8,
            parity_shards: 4,
            crc32c: i,
            placement: DurablePlacement {
                group_id: 1,
                epoch: 1,
            },
        },
    )
}

fn info(etag: &str, chunks: u32) -> CommitInfo {
    CommitInfo {
        size: u64::from(chunks) * 4096,
        etag: etag.into(),
        content_type: None,
        user_metadata: vec![],
        chunk_count: chunks,
    }
}

/// Full PUT: begin, append, commit.
async fn put(
    kv: &MemKv,
    bucket: &str,
    key: &str,
    etag: &str,
    chunks: u32,
    now: u64,
    pre: &Precondition,
) -> Result<arca_lake_metadata::ops::object::Committed, MetaError> {
    let p = begin_put(kv, bucket, key, now, LEASE).await?;
    let recs: Vec<_> = (0..chunks).map(chunk).collect();
    append_chunks(kv, &p.generation, &recs, now).await?;
    commit_put(kv, &p.generation, &info(etag, chunks), pre, now).await
}

#[test]
fn put_get_overwrite_delete_unversioned() {
    let kv = MemKv::default();
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        assert!(matches!(
            bucket::create_bucket(&kv, "b", Versioning::Disabled, 1).await,
            Err(MetaError::BucketAlreadyExists)
        ));

        let c1 = put(&kv, "b", "k", "e1", 3, 10, &Precondition::default())
            .await
            .unwrap();
        assert_eq!(c1.version_id, "null");
        assert!(c1.retired.is_none());

        let m = read_manifest(&kv, "b", "k", None).await.unwrap().unwrap();
        assert_eq!(m.header.etag, "e1");
        assert_eq!(m.chunks.len(), 3);
        assert_eq!(m.chunks[2].0, 2);
        let partial = read_manifest(&kv, "b", "k", Some(1..2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(partial.chunks.len(), 1);
        assert_eq!(partial.chunks[0].0, 1);

        // Overwrite retires the old generation; it is gone from the pointer
        // but its metadata stays until GC finishes.
        let c2 = put(&kv, "b", "k", "e2", 1, 20, &Precondition::default())
            .await
            .unwrap();
        assert_eq!(c2.retired, Some(c1.generation));
        assert_eq!(
            head_object(&kv, "b", "k")
                .await
                .unwrap()
                .unwrap()
                .header
                .etag,
            "e2"
        );
        assert!(matches!(
            get_generation(&kv, &c1.generation).await.unwrap().state,
            GenState::Retiring { since_ms: 20 }
        ));

        // GC: not collectable before the grace period, collectable after.
        assert!(gc::collectable(&kv, 25, 10, 10).await.unwrap().is_empty());
        assert_eq!(
            gc::collectable(&kv, 31, 10, 10).await.unwrap(),
            vec![c1.generation]
        );
        gc::finish(&kv, &c1.generation).await.unwrap();
        assert!(matches!(
            get_generation(&kv, &c1.generation).await,
            Err(MetaError::NoSuchGeneration)
        ));
        gc::finish(&kv, &c1.generation).await.unwrap(); // idempotent
        assert!(matches!(
            gc::finish(&kv, &c2.generation).await,
            Err(MetaError::WrongGenerationState("committed", "retiring"))
        ));

        // Delete.
        let d = delete_object(&kv, "b", "k", 40).await.unwrap();
        assert_eq!(d.retired, Some(c2.generation));
        assert!(d.delete_marker_version.is_none());
        assert!(head_object(&kv, "b", "k").await.unwrap().is_none());
        assert!(
            delete_object(&kv, "b", "missing", 41)
                .await
                .unwrap()
                .retired
                .is_none()
        );

        assert!(matches!(bucket::delete_bucket(&kv, "b").await, Ok(())));
        assert!(matches!(
            bucket::get_bucket(&kv, "b").await,
            Err(MetaError::NoSuchBucket)
        ));
    });
}

#[test]
fn get_during_put_never_sees_the_pending_generation() {
    let kv = MemKv::default();
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        put(&kv, "b", "k", "old", 1, 10, &Precondition::default())
            .await
            .unwrap();
        let p = begin_put(&kv, "b", "k", 20, LEASE).await.unwrap();
        append_chunks(&kv, &p.generation, &[chunk(0)], 20)
            .await
            .unwrap();
        // Before commit: readers see the old generation.
        assert_eq!(
            head_object(&kv, "b", "k")
                .await
                .unwrap()
                .unwrap()
                .header
                .etag,
            "old"
        );
        commit_put(
            &kv,
            &p.generation,
            &info("new", 1),
            &Precondition::default(),
            21,
        )
        .await
        .unwrap();
        assert_eq!(
            head_object(&kv, "b", "k")
                .await
                .unwrap()
                .unwrap()
                .header
                .etag,
            "new"
        );
    });
}

#[test]
fn concurrent_conditional_creates_exactly_one_wins() {
    let kv = MemKv::default();
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let pre = Precondition {
            if_none_match_any: true,
            ..Default::default()
        };
        for round in 0..20u64 {
            let key = format!("k{round}");
            let (a, b) = futures::join!(
                put(&kv, "b", &key, "A", 1, 10, &pre),
                put(&kv, "b", &key, "B", 1, 10, &pre),
            );
            let ok = usize::from(a.is_ok()) + usize::from(b.is_ok());
            assert_eq!(ok, 1, "round {round}: {a:?} {b:?}");
            let loser = if a.is_err() {
                a.unwrap_err()
            } else {
                b.unwrap_err()
            };
            assert_eq!(loser, MetaError::PreconditionFailed);
            let winner_etag = head_object(&kv, "b", &key)
                .await
                .unwrap()
                .unwrap()
                .header
                .etag;
            assert!(winner_etag == "A" || winner_etag == "B");
        }
        // Losers' generations remain pending (the gateway aborts them); the
        // expiry sweep must retire them once the lease is over.
        let retired = gc::expire_pending(&kv, 10 + LEASE, 100).await.unwrap();
        assert_eq!(retired.len(), 20);
        assert!(
            gc::expire_pending(&kv, 10 + LEASE, 100)
                .await
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn concurrent_overwrites_serialize_and_retire_exactly_one() {
    let kv = MemKv::default();
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let base = put(&kv, "b", "k", "base", 1, 1, &Precondition::default())
            .await
            .unwrap();
        let pre = Precondition::default();
        let (a, b) = futures::join!(
            put(&kv, "b", "k", "A", 2, 10, &pre),
            put(&kv, "b", "k", "B", 2, 10, &pre),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        // One of them displaced `base`, the other displaced the first.
        let mut retired = vec![a.retired.unwrap(), b.retired.unwrap()];
        retired.sort_unstable();
        let mut expected = vec![
            base.generation,
            if retired.contains(&a.generation) {
                a.generation
            } else {
                b.generation
            },
        ];
        expected.sort_unstable();
        assert_eq!(retired, expected);
        let current = head_object(&kv, "b", "k").await.unwrap().unwrap();
        let loser = if current.pointer.generation == a.generation {
            b.generation
        } else {
            a.generation
        };
        assert!(matches!(
            get_generation(&kv, &loser).await.unwrap().state,
            GenState::Retiring { .. }
        ));
        assert!(matches!(
            get_generation(&kv, &current.pointer.generation)
                .await
                .unwrap()
                .state,
            GenState::Committed
        ));
    });
}

#[test]
fn if_match_requires_the_current_etag() {
    let kv = MemKv::default();
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        put(&kv, "b", "k", "v1", 1, 1, &Precondition::default())
            .await
            .unwrap();
        let wrong = Precondition {
            if_match: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(
            put(&kv, "b", "k", "v2", 1, 2, &wrong).await.unwrap_err(),
            MetaError::PreconditionFailed
        );
        let right = Precondition {
            if_match: Some("v1".into()),
            ..Default::default()
        };
        put(&kv, "b", "k", "v2", 1, 3, &right).await.unwrap();
        assert_eq!(
            head_object(&kv, "b", "k")
                .await
                .unwrap()
                .unwrap()
                .header
                .etag,
            "v2"
        );
        // If-Match on a missing key fails.
        assert_eq!(
            put(&kv, "b", "other", "x", 1, 4, &right).await.unwrap_err(),
            MetaError::PreconditionFailed
        );
    });
}

#[test]
fn large_manifest_is_split_across_transactions() {
    let kv = MemKv::default();
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        // 200k chunk records exceed one 10 MB transaction several times over.
        let p = begin_put(&kv, "b", "big", 1, LEASE).await.unwrap();
        let recs: Vec<_> = (0..200_000).map(chunk).collect();
        let v0 = kv.version();
        append_chunks(&kv, &p.generation, &recs, 1).await.unwrap();
        assert!(kv.version() - v0 > 1, "must have used several transactions");
        commit_put(
            &kv,
            &p.generation,
            &info("big", 200_000),
            &Precondition::default(),
            2,
        )
        .await
        .unwrap();
        let m = read_manifest(&kv, "b", "big", Some(199_990..200_000))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.chunks.len(), 10);
        assert_eq!(m.chunks[9].0, 199_999);
    });
}

#[test]
fn oversized_record_is_an_explicit_error() {
    let kv = MemKv::default();
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let p = begin_put(&kv, "b", "k", 1, LEASE).await.unwrap();
        let huge = CommitInfo {
            user_metadata: vec![("m".into(), "x".repeat(200_000))],
            ..info("e", 0)
        };
        let err = commit_put(&kv, &p.generation, &huge, &Precondition::default(), 2)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                MetaError::Kv(arca_lake_metadata::KvError::TooLarge { what: "value", .. })
            ),
            "{err}"
        );
        // Nothing was published.
        assert!(head_object(&kv, "b", "k").await.unwrap().is_none());
    });
}

#[test]
fn lease_expiry_and_abort() {
    let kv = MemKv::default();
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let p = begin_put(&kv, "b", "k", 100, LEASE).await.unwrap();
        append_chunks(&kv, &p.generation, &[chunk(0)], 100)
            .await
            .unwrap();
        // After the lease: appends and commits are refused.
        let late = 100 + LEASE;
        assert_eq!(
            append_chunks(&kv, &p.generation, &[chunk(1)], late)
                .await
                .unwrap_err(),
            MetaError::LeaseExpired
        );
        assert_eq!(
            commit_put(
                &kv,
                &p.generation,
                &info("e", 1),
                &Precondition::default(),
                late
            )
            .await
            .unwrap_err(),
            MetaError::LeaseExpired
        );
        // The sweep retires it; a second commit attempt sees the new state.
        assert_eq!(
            gc::expire_pending(&kv, late, 10).await.unwrap(),
            vec![p.generation]
        );
        assert!(matches!(
            commit_put(
                &kv,
                &p.generation,
                &info("e", 1),
                &Precondition::default(),
                late
            )
            .await
            .unwrap_err(),
            MetaError::WrongGenerationState("retiring", "pending")
        ));
        assert!(head_object(&kv, "b", "k").await.unwrap().is_none());

        // Explicit abort of another pending generation.
        let q = begin_put(&kv, "b", "k", 200, LEASE).await.unwrap();
        abort_put(&kv, &q.generation, 201).await.unwrap();
        assert!(matches!(
            get_generation(&kv, &q.generation).await.unwrap().state,
            GenState::Retiring { .. }
        ));
        assert!(matches!(
            abort_put(&kv, &q.generation, 202).await,
            Err(MetaError::WrongGenerationState(..))
        ));
        // Both are in the GC queue.
        let mut c = gc::collectable(&kv, 1_000_000, 0, 10).await.unwrap();
        c.sort_unstable();
        let mut e = vec![p.generation, q.generation];
        e.sort_unstable();
        assert_eq!(c, e);
    });
}

#[test]
fn versioned_bucket_keeps_history_and_delete_markers() {
    let kv = MemKv::default();
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Enabled, 1)
            .await
            .unwrap();
        let c1 = put(&kv, "b", "k", "v1", 1, 10, &Precondition::default())
            .await
            .unwrap();
        let c2 = put(&kv, "b", "k", "v2", 1, 20, &Precondition::default())
            .await
            .unwrap();
        assert_ne!(c1.version_id, "null");
        assert_ne!(c1.version_id, c2.version_id);
        assert!(
            c2.retired.is_none(),
            "versioned overwrite keeps the old generation"
        );
        assert!(matches!(
            get_generation(&kv, &c1.generation).await.unwrap().state,
            GenState::Committed
        ));

        let d = delete_object(&kv, "b", "k", 30).await.unwrap();
        assert!(d.delete_marker_version.is_some());
        assert!(d.retired.is_none());
        assert!(head_object(&kv, "b", "k").await.unwrap().is_none());
        // Listing hides delete markers.
        let l = list::list_objects(
            &kv,
            "b",
            &list::ListRequest {
                max_keys: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(l.objects.is_empty());
        // Bucket is not empty because of version history.
        assert!(matches!(
            bucket::delete_bucket(&kv, "b").await,
            Err(MetaError::BucketNotEmpty)
        ));

        // Suspended: writes replace the null version only.
        bucket::set_versioning(&kv, "b", Versioning::Suspended)
            .await
            .unwrap();
        let s1 = put(&kv, "b", "k", "s1", 1, 40, &Precondition::default())
            .await
            .unwrap();
        assert_eq!(s1.version_id, "null");
        assert!(
            s1.retired.is_none(),
            "displaced a delete marker, nothing to retire"
        );
        let s2 = put(&kv, "b", "k", "s2", 1, 50, &Precondition::default())
            .await
            .unwrap();
        assert_eq!(
            s2.retired,
            Some(s1.generation),
            "null version displaced by null version is retired"
        );
    });
}

#[test]
fn listing_with_prefix_delimiter_and_pagination() {
    let kv = MemKv::default();
    run(async {
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let keys = ["a", "dir/x", "dir/y", "dir/sub/z", "e", "f/1", "f/2", "g"];
        for (i, k) in keys.iter().enumerate() {
            put(
                &kv,
                "b",
                k,
                &format!("e{i}"),
                1,
                1,
                &Precondition::default(),
            )
            .await
            .unwrap();
        }
        delete_object(&kv, "b", "g", 2).await.unwrap(); // not listed

        let all = list::list_objects(
            &kv,
            "b",
            &list::ListRequest {
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let names: Vec<_> = all.objects.iter().map(|o| o.key.as_str()).collect();
        assert_eq!(
            names,
            ["a", "dir/sub/z", "dir/x", "dir/y", "e", "f/1", "f/2"]
        );
        assert!(!all.is_truncated);

        let top = list::list_objects(
            &kv,
            "b",
            &list::ListRequest {
                delimiter: Some("/".into()),
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            top.objects
                .iter()
                .map(|o| o.key.as_str())
                .collect::<Vec<_>>(),
            ["a", "e"]
        );
        assert_eq!(top.common_prefixes, ["dir/", "f/"]);

        let dir = list::list_objects(
            &kv,
            "b",
            &list::ListRequest {
                prefix: "dir/".into(),
                delimiter: Some("/".into()),
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            dir.objects
                .iter()
                .map(|o| o.key.as_str())
                .collect::<Vec<_>>(),
            ["dir/x", "dir/y"]
        );
        assert_eq!(dir.common_prefixes, ["dir/sub/"]);

        // Pagination with max_keys = 2 across objects and common prefixes.
        let mut continuation = None;
        let mut seen = Vec::new();
        let mut pages = 0;
        loop {
            let page = list::list_objects(
                &kv,
                "b",
                &list::ListRequest {
                    delimiter: Some("/".into()),
                    max_keys: 2,
                    continuation: continuation.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            pages += 1;
            seen.extend(page.objects.iter().map(|o| o.key.clone()));
            seen.extend(page.common_prefixes.iter().cloned());
            if !page.is_truncated {
                break;
            }
            continuation = page.next_continuation;
        }
        assert_eq!(seen, ["a", "dir/", "e", "f/"]);
        assert_eq!(pages, 2);

        // start-after is a plain key; combined with a prefix.
        let after = list::list_objects(
            &kv,
            "b",
            &list::ListRequest {
                prefix: "dir/".into(),
                start_after: Some("dir/sub/z".into()),
                max_keys: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            after
                .objects
                .iter()
                .map(|o| o.key.as_str())
                .collect::<Vec<_>>(),
            ["dir/x", "dir/y"]
        );
    });
}
