//! Commit outcome unknown (RFC-003 §5.8): the recipes under injected lost
//! replies and lost requests, and the invariants GC relies on.

use std::future::Future;

use arca_lake_metadata::fault::{CommitFault, FaultKv};
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::object::{
    CommitInfo, CommitOutcome, Precondition, abort_put, append_chunks, begin_put, commit_put,
    get_generation, head_object, resolve_commit,
};
use arca_lake_metadata::ops::{bucket, gc};
use arca_lake_metadata::records::{ChunkRecord, DurablePlacement, GenState, Versioning};
use arca_lake_metadata::{KvError, MetaError, keys};

fn run<T>(f: impl Future<Output = T>) -> T {
    futures::executor::block_on(f)
}

const LEASE: u64 = 10_000;

fn store() -> FaultKv<MemKv> {
    let kv = FaultKv::new(MemKv::default());
    run(bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)).unwrap();
    kv
}

fn chunk(i: u32) -> (u32, ChunkRecord) {
    (
        i,
        ChunkRecord {
            chunk_id: [u8::try_from(i % 256).unwrap(); 16],
            offset: u64::from(i) * 4096,
            len: 4096,
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

fn info(etag: &str) -> CommitInfo {
    CommitInfo {
        size: 4096,
        etag: etag.into(),
        content_type: None,
        user_metadata: vec![],
        chunk_count: 1,
        checksum: None,
    }
}

fn is_object_current(written: &[Vec<u8>]) -> bool {
    written.iter().any(|k| keys::object_key_from(k).is_some())
}

#[test]
fn lost_reply_after_apply_resolves_to_committed_and_abort_is_refused() {
    let kv = store();
    run(async {
        let p = begin_put(&kv, "b", "k", 1, LEASE).await.unwrap();
        append_chunks(&kv, &p.generation, &[chunk(0)], 1)
            .await
            .unwrap();
        kv.inject(is_object_current, CommitFault::UnknownAfterApply, 1);
        let err = commit_put(&kv, &p.generation, &info("e1"), &Precondition::default(), 2)
            .await
            .unwrap_err();
        assert!(
            matches!(err, MetaError::Kv(KvError::CommitUnknown(_))),
            "{err}"
        );

        // Resolution: the generation is committed and visible.
        let outcome = resolve_commit(&kv, &p.generation, 3).await.unwrap();
        assert!(
            matches!(outcome, CommitOutcome::Committed(_)),
            "{outcome:?}"
        );
        let head = head_object(&kv, "b", "k").await.unwrap().unwrap();
        assert_eq!(head.pointer.generation, p.generation);

        // Neither a retried commit nor an abort can touch it.
        assert_eq!(
            commit_put(&kv, &p.generation, &info("e1"), &Precondition::default(), 4)
                .await
                .unwrap_err(),
            MetaError::WrongGenerationState("committed", "pending")
        );
        assert_eq!(
            abort_put(&kv, &p.generation, 4).await.unwrap_err(),
            MetaError::WrongGenerationState("committed", "pending")
        );
        assert_eq!(
            gc::finish(&kv, &p.generation).await.unwrap_err(),
            MetaError::WrongGenerationState("committed", "retiring"),
            "GC never removes a committed generation"
        );
    });
}

#[test]
fn lost_request_resolves_to_pending_and_retry_publishes_exactly_once() {
    let kv = store();
    run(async {
        let p = begin_put(&kv, "b", "k", 1, LEASE).await.unwrap();
        append_chunks(&kv, &p.generation, &[chunk(0)], 1)
            .await
            .unwrap();
        kv.inject(is_object_current, CommitFault::UnknownWithoutApply, 1);
        let err = commit_put(&kv, &p.generation, &info("e1"), &Precondition::default(), 2)
            .await
            .unwrap_err();
        assert!(matches!(err, MetaError::Kv(KvError::CommitUnknown(_))));
        assert_eq!(
            resolve_commit(&kv, &p.generation, 3).await.unwrap(),
            CommitOutcome::Pending
        );
        assert!(head_object(&kv, "b", "k").await.unwrap().is_none());

        let c = commit_put(&kv, &p.generation, &info("e1"), &Precondition::default(), 4)
            .await
            .unwrap();
        assert_eq!(c.generation, p.generation);
        assert_eq!(c.retired, None, "nothing was displaced by the retry");
        let head = head_object(&kv, "b", "k").await.unwrap().unwrap();
        assert_eq!(head.pointer.next_ordinal, 1, "exactly one publication");
    });
}

#[test]
fn abort_fences_a_late_commit() {
    let kv = store();
    run(async {
        let p = begin_put(&kv, "b", "k", 1, LEASE).await.unwrap();
        kv.inject(is_object_current, CommitFault::UnknownWithoutApply, 1);
        let _ = commit_put(&kv, &p.generation, &info("e1"), &Precondition::default(), 2).await;
        assert_eq!(
            resolve_commit(&kv, &p.generation, 3).await.unwrap(),
            CommitOutcome::Pending
        );
        // The caller decides to fail: fence by aborting.
        abort_put(&kv, &p.generation, 3).await.unwrap();
        // The "late" commit now finds a retiring generation and is rejected.
        assert_eq!(
            commit_put(&kv, &p.generation, &info("e1"), &Precondition::default(), 4)
                .await
                .unwrap_err(),
            MetaError::WrongGenerationState("retiring", "pending")
        );
        assert!(head_object(&kv, "b", "k").await.unwrap().is_none());
        assert_eq!(
            resolve_commit(&kv, &p.generation, 5).await.unwrap(),
            CommitOutcome::Retiring
        );
    });
}

#[test]
fn expiry_and_commit_exclude_each_other() {
    let kv = store();
    run(async {
        // Expiry first: the commit is rejected.
        let p = begin_put(&kv, "b", "a", 1, LEASE).await.unwrap();
        assert_eq!(
            gc::expire_pending(&kv, 1 + LEASE, 10).await.unwrap(),
            vec![p.generation]
        );
        assert_eq!(
            commit_put(
                &kv,
                &p.generation,
                &info("e"),
                &Precondition::default(),
                1 + LEASE
            )
            .await
            .unwrap_err(),
            MetaError::WrongGenerationState("retiring", "pending")
        );
        assert_eq!(
            resolve_commit(&kv, &p.generation, 1 + LEASE).await.unwrap(),
            CommitOutcome::Retiring
        );

        // Commit first: expiry leaves it alone even long after the lease.
        let q = begin_put(&kv, "b", "c", 1, LEASE).await.unwrap();
        commit_put(&kv, &q.generation, &info("e"), &Precondition::default(), 2)
            .await
            .unwrap();
        assert_eq!(
            gc::expire_pending(&kv, 1 + 10 * LEASE, 10).await.unwrap(),
            Vec::<arca_lake_metadata::records::GenId>::new()
        );
        assert_eq!(
            get_generation(&kv, &q.generation).await.unwrap().state,
            GenState::Committed
        );

        // A pending generation past its lease is reported as such so that
        // the caller does not retry the commit.
        let r = begin_put(&kv, "b", "d", 1, LEASE).await.unwrap();
        assert_eq!(
            resolve_commit(&kv, &r.generation, 1 + LEASE).await.unwrap(),
            CommitOutcome::PendingExpired
        );
        assert_eq!(
            commit_put(
                &kv,
                &r.generation,
                &info("e"),
                &Precondition::default(),
                1 + LEASE
            )
            .await
            .unwrap_err(),
            MetaError::LeaseExpired
        );
    });
}

#[test]
fn begin_and_append_retry_unknown_outcomes_idempotently() {
    let kv = store();
    run(async {
        let is_gen_only = |written: &[Vec<u8>]| {
            written.iter().any(|k| keys::gen_from_gen_key(k).is_some())
                && !is_object_current(written)
        };
        kv.inject(is_gen_only, CommitFault::UnknownAfterApply, 1);
        kv.inject(is_gen_only, CommitFault::UnknownWithoutApply, 1);
        let p = begin_put(&kv, "b", "k", 1, LEASE).await.unwrap();
        append_chunks(&kv, &p.generation, &[chunk(0), chunk(1)], 1)
            .await
            .unwrap();
        assert_eq!(kv.fired().len(), 2, "both faults were exercised");
        commit_put(&kv, &p.generation, &info("e"), &Precondition::default(), 2)
            .await
            .unwrap();
        let m = arca_lake_metadata::ops::object::read_manifest(&kv, "b", "k", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.chunks.len(), 2);
        assert_eq!(
            kv.inner()
                .dump_prefix(&keys::gen_header(&p.generation))
                .len(),
            1,
            "one header"
        );
    });
}

#[test]
fn conditional_create_with_lost_reply_still_has_one_winner() {
    let kv = store();
    run(async {
        let pre = Precondition {
            if_none_match_any: true,
            ..Default::default()
        };
        let a = begin_put(&kv, "b", "k", 1, LEASE).await.unwrap();
        let b = begin_put(&kv, "b", "k", 1, LEASE).await.unwrap();
        kv.inject(is_object_current, CommitFault::UnknownAfterApply, 1);
        let _ = commit_put(&kv, &a.generation, &info("A"), &pre, 2).await;
        assert!(matches!(
            resolve_commit(&kv, &a.generation, 3).await.unwrap(),
            CommitOutcome::Committed(_)
        ));
        // The second creator is rejected by the precondition, and a retry of
        // the first is rejected by the state check: still one winner.
        assert_eq!(
            commit_put(&kv, &b.generation, &info("B"), &pre, 3)
                .await
                .unwrap_err(),
            MetaError::PreconditionFailed
        );
        assert_eq!(
            commit_put(&kv, &a.generation, &info("A"), &pre, 3)
                .await
                .unwrap_err(),
            MetaError::WrongGenerationState("committed", "pending")
        );
        assert_eq!(
            head_object(&kv, "b", "k")
                .await
                .unwrap()
                .unwrap()
                .header
                .etag,
            "A"
        );
    });
}
