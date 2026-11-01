//! End-to-end cases with metadata commit faults (RFC-003 §5.8), generic
//! over the wrapped backend. Instantiate with [`crate::fault_suite!`].

#![allow(clippy::too_many_lines, clippy::missing_panics_doc)]

use arca_lake_metadata::fault::{CommitFault, FaultKv};
use arca_lake_metadata::ops::object::get_generation;
use arca_lake_metadata::records::GenState;
use arca_lake_metadata::{KvStore, keys};
use bytes::Bytes;
use futures::future::BoxFuture;
use s3s::S3ErrorCode;

use crate::s3::{self, PutOptions};
use crate::{ClusterOptions, TestCluster, now_ms, random_bytes};

/// A function that starts a cluster whose metadata store is wrapped in
/// [`FaultKv`].
pub trait FaultStarter<S: KvStore + 'static>:
    Fn(ClusterOptions) -> BoxFuture<'static, TestCluster<FaultKv<S>>> + Send + Sync
{
}
impl<S: KvStore + 'static, F> FaultStarter<S> for F where
    F: Fn(ClusterOptions) -> BoxFuture<'static, TestCluster<FaultKv<S>>> + Send + Sync
{
}

const BUCKET: &str = "faults";
const MIB: u64 = 1 << 20;

async fn cluster<S: KvStore + 'static>(start: &impl FaultStarter<S>) -> TestCluster<FaultKv<S>> {
    let c = start(ClusterOptions::rs(3, 2, 1, MIB, 64 << 10)).await;
    s3::create_bucket(&c.backend, BUCKET).await.unwrap();
    c
}

fn payload(len: usize, seed: u64) -> Bytes {
    Bytes::from(random_bytes(len, seed))
}

/// The commit (and the delete) transaction is the one that writes a
/// current-pointer key.
fn writes_pointer(written: &[Vec<u8>]) -> bool {
    written.iter().any(|k| keys::object_key_from(k).is_some())
}

/// E2E case `commit_reply_lost_after_apply_is_resolved_as_success`.
pub async fn commit_reply_lost_after_apply_is_resolved_as_success<S: KvStore + 'static>(
    start: impl FaultStarter<S>,
) {
    let c = cluster(&start).await;
    let old = payload(200 << 10, 1);
    s3::put(&c.backend, BUCKET, "k", old, &PutOptions::default())
        .await
        .unwrap();
    let new = payload(200 << 10, 2);
    c.meta
        .inject(writes_pointer, CommitFault::UnknownAfterApply, 1);
    let etag = s3::put(&c.backend, BUCKET, "k", new.clone(), &PutOptions::default())
        .await
        .expect("a lost reply after a successful commit is a success");
    assert_eq!(c.meta.fired(), vec![CommitFault::UnknownAfterApply]);
    let (got, got_etag) = s3::get(&c.backend, BUCKET, "k").await.unwrap();
    assert_eq!(got, new);
    assert_eq!(got_etag, etag);
    let report = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(report.collected, 1, "only the displaced version is garbage");
    assert_eq!(s3::get(&c.backend, BUCKET, "k").await.unwrap().0, new);
    c.shutdown().await;
}

/// E2E case `commit_request_lost_before_apply_is_retried`.
pub async fn commit_request_lost_before_apply_is_retried<S: KvStore + 'static>(
    start: impl FaultStarter<S>,
) {
    let c = cluster(&start).await;
    let data = payload(usize::try_from(MIB + 10).unwrap(), 3);
    c.meta
        .inject(writes_pointer, CommitFault::UnknownWithoutApply, 1);
    s3::put(
        &c.backend,
        BUCKET,
        "k",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .expect("a lost request is retried while the generation is pending");
    assert_eq!(c.meta.fired(), vec![CommitFault::UnknownWithoutApply]);
    assert_eq!(s3::get(&c.backend, BUCKET, "k").await.unwrap().0, data);
    let m = c.manifest(BUCKET, "k").await;
    assert_eq!(m.pointer.next_ordinal, 1, "published exactly once");
    c.shutdown().await;
}

/// E2E case `commit_persistently_unknown_is_fenced_and_invisible`.
pub async fn commit_persistently_unknown_is_fenced_and_invisible<S: KvStore + 'static>(
    start: impl FaultStarter<S>,
) {
    let c = cluster(&start).await;
    let old = payload(100 << 10, 4);
    s3::put(&c.backend, BUCKET, "k", old.clone(), &PutOptions::default())
        .await
        .unwrap();
    let baseline = c.total_shards().await;
    // Every commit attempt loses its request; the abort (which writes no
    // pointer) goes through and fences the generation.
    c.meta
        .inject(writes_pointer, CommitFault::UnknownWithoutApply, 100);
    let err = s3::put(
        &c.backend,
        BUCKET,
        "k",
        payload(100 << 10, 5),
        &PutOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::ServiceUnavailable, "{err}");
    c.meta.clear_rules();
    assert_eq!(
        s3::get(&c.backend, BUCKET, "k").await.unwrap().0,
        old,
        "the old version is still the visible one"
    );
    // The fenced generation is retiring and its shards are collected.
    let report = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(report.collected, 1);
    assert_eq!(c.total_shards().await, baseline);
    assert_eq!(s3::get(&c.backend, BUCKET, "k").await.unwrap().0, old);
    c.shutdown().await;
}

/// E2E case `conditional_create_with_lost_reply_keeps_single_winner`.
pub async fn conditional_create_with_lost_reply_keeps_single_winner<S: KvStore + 'static>(
    start: impl FaultStarter<S>,
) {
    let c = cluster(&start).await;
    let create = PutOptions {
        if_none_match_any: true,
        ..Default::default()
    };
    let first = payload(80 << 10, 6);
    c.meta
        .inject(writes_pointer, CommitFault::UnknownAfterApply, 1);
    s3::put(&c.backend, BUCKET, "once", first.clone(), &create)
        .await
        .expect("the creator whose reply was lost did create the object");
    let err = s3::put(&c.backend, BUCKET, "once", payload(80 << 10, 7), &create)
        .await
        .unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::PreconditionFailed);
    assert_eq!(s3::get(&c.backend, BUCKET, "once").await.unwrap().0, first);
    let m = c.manifest(BUCKET, "once").await;
    assert_eq!(
        get_generation(c.meta.as_ref(), &m.pointer.generation)
            .await
            .unwrap()
            .state,
        GenState::Committed
    );
    c.shutdown().await;
}

/// E2E case `delete_with_lost_reply_is_retried_idempotently`.
pub async fn delete_with_lost_reply_is_retried_idempotently<S: KvStore + 'static>(
    start: impl FaultStarter<S>,
) {
    let c = cluster(&start).await;
    s3::put(
        &c.backend,
        BUCKET,
        "k",
        payload(100 << 10, 8),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    c.meta
        .inject(writes_pointer, CommitFault::UnknownAfterApply, 1);
    s3::delete(&c.backend, BUCKET, "k").await.unwrap();
    assert_eq!(
        *s3::get(&c.backend, BUCKET, "k").await.unwrap_err().code(),
        S3ErrorCode::NoSuchKey
    );
    let report = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(report.collected, 1, "the deleted version, once");
    assert_eq!(c.total_shards().await, 0);
    c.shutdown().await;
}

/// E2E case `begin_and_append_with_lost_replies_publish_once`.
pub async fn begin_and_append_with_lost_replies_publish_once<S: KvStore + 'static>(
    start: impl FaultStarter<S>,
) {
    let c = cluster(&start).await;
    let gen_only = |written: &[Vec<u8>]| {
        written.iter().any(|k| keys::gen_from_gen_key(k).is_some()) && !writes_pointer(written)
    };
    c.meta.inject(gen_only, CommitFault::UnknownAfterApply, 1);
    c.meta.inject(gen_only, CommitFault::UnknownWithoutApply, 1);
    let data = payload(usize::try_from(2 * MIB + 1).unwrap(), 9);
    s3::put(
        &c.backend,
        BUCKET,
        "k",
        data.clone(),
        &PutOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(c.meta.fired().len(), 2);
    assert_eq!(s3::get(&c.backend, BUCKET, "k").await.unwrap().0, data);
    assert_eq!(c.manifest(BUCKET, "k").await.chunks.len(), 3);
    let report = c.gc(now_ms() + 120_000, 0).await;
    assert_eq!(report.collected, 0, "no stray generation was created");
    c.shutdown().await;
}

/// Instantiate every fault case as a `#[tokio::test]` for one backend.
/// `$start` yields `Option<impl FaultStarter<S>>`; `None` skips.
#[macro_export]
macro_rules! fault_suite {
    ($start:expr) => {
        $crate::fault_suite!(@each $start;
            commit_reply_lost_after_apply_is_resolved_as_success,
            commit_request_lost_before_apply_is_retried,
            commit_persistently_unknown_is_fenced_and_invisible,
            conditional_create_with_lost_reply_keeps_single_winner,
            delete_with_lost_reply_is_retried_idempotently,
            begin_and_append_with_lost_replies_publish_once,
        );
    };
    (@each $start:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(start) = $start else {
                    eprintln!("{}: backend not available; skipping", stringify!($name));
                    return;
                };
                $crate::suite_faults::$name(start).await;
            }
        )*
    };
}
