//! Multipart upload recipes (RFC-007 §5.3): hierarchical manifest,
//! linearization points, part liveness (I-MP1), bounded transactions
//! (10 000 parts), races, idempotent Complete, abort and root finish.

#![allow(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::assert_is_empty
)]

use std::future::Future;

use arca_lake_metadata::MetaError;
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::multipart::{
    self, CompletePart, CompleteRequest, PartInfo, UploadInfo,
};
use arca_lake_metadata::ops::object::{
    self, Precondition, append_chunks, get_generation, head_object, read_manifest,
};
use arca_lake_metadata::ops::{bucket, gc};
use arca_lake_metadata::records::{
    Checksum, ChecksumAlgorithm, ChecksumType, ChunkRecord, DurablePlacement, GenId, GenKind,
    GenState, MultipartState, UploadId, Versioning, new_gen_id,
};
use arca_lake_metadata::{keys, transact};

fn run<T>(f: impl Future<Output = T>) -> T {
    futures::executor::block_on(f)
}

const LEASE: u64 = 60_000;
const MIN_PART: u64 = 5 << 20;

fn chunk(i: u32, len: u32) -> (u32, ChunkRecord) {
    (
        i,
        ChunkRecord {
            chunk_id: [u8::try_from(i % 256).unwrap(); 16],
            offset: u64::from(i) * u64::from(len),
            len,
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

/// Upload one part of `size` bytes in `chunks` chunk records.
async fn upload_part(
    kv: &MemKv,
    bucket: &str,
    key: &str,
    upload: &UploadId,
    n: u32,
    size: u64,
    etag: &str,
    checksum: Option<Checksum>,
    now: u64,
) -> Result<GenId, MetaError> {
    let p = multipart::begin_part(kv, bucket, key, upload, n, now, LEASE).await?;
    let chunks = 2u32;
    let recs: Vec<_> = (0..chunks)
        .map(|i| chunk(i, u32::try_from(size / u64::from(chunks)).unwrap()))
        .collect();
    append_chunks(kv, &p.generation, &recs, now).await?;
    multipart::seal_part(
        kv,
        &p.generation,
        &PartInfo {
            size,
            etag: etag.into(),
            checksum,
            chunk_count: chunks,
        },
        now,
    )
    .await?;
    Ok(p.generation)
}

fn no_checksum(_: ChecksumAlgorithm, _: ChecksumType, _: &[(u64, Vec<u8>)]) -> Option<Vec<u8>> {
    None
}

fn concat_checksum(
    _: ChecksumAlgorithm,
    kind: ChecksumType,
    parts: &[(u64, Vec<u8>)],
) -> Option<Vec<u8>> {
    (kind == ChecksumType::Composite).then(|| parts.iter().flat_map(|(_, v)| v.clone()).collect())
}

fn complete_req<'a>(
    parts: &'a [CompletePart],
    root: GenId,
    pre: &'a Precondition,
    etag: &'a str,
) -> CompleteRequest<'a> {
    CompleteRequest {
        parts,
        root,
        pre,
        min_part_size: MIN_PART,
        etag,
        expected_checksum: None,
        derive_checksum: &no_checksum,
    }
}

fn listed(parts: &[(u32, &str)]) -> Vec<CompletePart> {
    parts
        .iter()
        .map(|(n, e)| CompletePart {
            part_number: *n,
            etag: (*e).to_string(),
            checksum: None,
        })
        .collect()
}

async fn state(kv: &MemKv, g: &GenId) -> GenState {
    get_generation(kv, g).await.unwrap().state
}

#[test]
fn create_upload_seal_complete_and_read() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let up = multipart::create_upload(&kv, "b", "k", &UploadInfo::default(), 1)
            .await
            .unwrap();
        let uid = up.upload_id;
        let g1 = upload_part(&kv, "b", "k", &uid, 1, MIN_PART, "e1", None, 2)
            .await
            .unwrap();
        let g2 = upload_part(&kv, "b", "k", &uid, 2, 100, "e2", None, 3)
            .await
            .unwrap();
        assert_eq!(state(&kv, &g1).await, GenState::Committed);
        // ListParts sees both; the object is not visible yet (linearization
        // point is Complete).
        let page = multipart::list_parts(&kv, "b", "k", &uid, 0, 10)
            .await
            .unwrap();
        assert_eq!(
            page.parts.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(head_object(&kv, "b", "k").await.unwrap().is_none());
        let uploads = multipart::list_uploads(
            &kv,
            "b",
            &multipart::ListUploadsRequest {
                max_uploads: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(uploads.uploads.len(), 1);

        let root = new_gen_id();
        let parts = listed(&[(1, "e1"), (2, "e2")]);
        let done = multipart::complete(
            &kv,
            "b",
            "k",
            &uid,
            &complete_req(&parts, root, &Precondition::default(), "etag-2"),
            4,
        )
        .await
        .unwrap();
        assert_eq!(done.root, root);
        assert_eq!(done.size, MIN_PART + 100);
        assert_eq!(done.unlisted, Vec::<u32>::new());
        // Visible, as a root with two references and no chunk records.
        let m = read_manifest(&kv, "b", "k", None).await.unwrap().unwrap();
        assert_eq!(m.pointer.generation, root);
        assert!(m.chunks.is_empty());
        assert_eq!(m.parts.len(), 2);
        assert_eq!(m.parts[0].generation, g1);
        assert_eq!(m.parts[1].offset, MIN_PART);
        assert_eq!(m.header.etag, "etag-2");
        assert!(matches!(
            m.header.kind,
            GenKind::MultipartRoot { part_count: 2, .. }
        ));
        let (chunks, _) = object::read_part_manifest(&kv, &g2).await.unwrap().unwrap();
        assert_eq!(chunks.len(), 2);
        // After Complete: ListParts and UploadPart fail; ListMultipartUploads
        // no longer lists it; the record says Completed.
        assert_eq!(
            multipart::list_parts(&kv, "b", "k", &uid, 0, 10)
                .await
                .unwrap_err(),
            MetaError::NoSuchUpload
        );
        assert_eq!(
            multipart::begin_part(&kv, "b", "k", &uid, 3, 5, LEASE)
                .await
                .unwrap_err(),
            MetaError::NoSuchUpload
        );
        let uploads = multipart::list_uploads(
            &kv,
            "b",
            &multipart::ListUploadsRequest {
                max_uploads: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(uploads.uploads.is_empty());
        let rec = multipart::get_upload(&kv, "b", "k", &uid)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.state, MultipartState::Completed { root });
        // Parts stay committed (referenced by the root): GC never collects
        // them (I-MP1).
        assert_eq!(state(&kv, &g1).await, GenState::Committed);
        assert_eq!(state(&kv, &g2).await, GenState::Committed);
        // Idempotent retry of the same Complete returns the same result.
        let again = multipart::complete(
            &kv,
            "b",
            "k",
            &uid,
            &complete_req(&parts, new_gen_id(), &Precondition::default(), "etag-2"),
            5,
        )
        .await
        .unwrap();
        assert!(again.already_completed);
        assert_eq!(again.root, root);
        assert_eq!(again.version_id, done.version_id);
        // A different part list after completion is NoSuchUpload.
        let other = listed(&[(1, "e1")]);
        assert_eq!(
            multipart::complete(
                &kv,
                "b",
                "k",
                &uid,
                &complete_req(&other, new_gen_id(), &Precondition::default(), "x"),
                6,
            )
            .await
            .unwrap_err(),
            MetaError::NoSuchUpload
        );
        // Abort after Complete is NoSuchUpload.
        assert_eq!(
            multipart::abort(&kv, "b", "k", &uid).await.unwrap_err(),
            MetaError::NoSuchUpload
        );
    });
}

#[test]
fn complete_validates_order_etag_size_and_discards_unlisted() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let uid = multipart::create_upload(&kv, "b", "k", &UploadInfo::default(), 1)
            .await
            .unwrap()
            .upload_id;
        let g1 = upload_part(&kv, "b", "k", &uid, 1, MIN_PART, "e1", None, 2)
            .await
            .unwrap();
        let g2 = upload_part(&kv, "b", "k", &uid, 2, MIN_PART, "e2", None, 2)
            .await
            .unwrap();
        let g3 = upload_part(&kv, "b", "k", &uid, 3, 10, "e3", None, 2)
            .await
            .unwrap();
        let pre = Precondition::default();
        let bad_order = listed(&[(2, "e2"), (1, "e1")]);
        assert_eq!(
            multipart::complete(
                &kv,
                "b",
                "k",
                &uid,
                &complete_req(&bad_order, new_gen_id(), &pre, "x"),
                3
            )
            .await
            .unwrap_err(),
            MetaError::InvalidPartOrder
        );
        let bad_etag = listed(&[(1, "wrong"), (2, "e2")]);
        assert_eq!(
            multipart::complete(
                &kv,
                "b",
                "k",
                &uid,
                &complete_req(&bad_etag, new_gen_id(), &pre, "x"),
                3
            )
            .await
            .unwrap_err(),
            MetaError::InvalidPart(1)
        );
        let missing = listed(&[(1, "e1"), (9, "e9")]);
        assert_eq!(
            multipart::complete(
                &kv,
                "b",
                "k",
                &uid,
                &complete_req(&missing, new_gen_id(), &pre, "x"),
                3
            )
            .await
            .unwrap_err(),
            MetaError::InvalidPart(9)
        );
        // Part 3 (10 bytes) is too small unless it is the last listed part.
        let small_mid = listed(&[(1, "e1"), (3, "e3")]);
        let ok = multipart::complete(
            &kv,
            "b",
            "k",
            &uid,
            &complete_req(&small_mid, new_gen_id(), &pre, "x"),
            3,
        )
        .await
        .unwrap();
        assert_eq!(ok.size, MIN_PART + 10);
        // Part 2 was uploaded but not listed: reported, still committed until
        // discarded, then retired (transition 3 of I-MP1).
        assert_eq!(ok.unlisted, vec![2]);
        assert_eq!(state(&kv, &g2).await, GenState::Committed);
        let n = multipart::discard_parts(&kv, 1, "k", &uid, 100, 4)
            .await
            .unwrap();
        assert_eq!(n, 3, "all three part records processed");
        assert!(matches!(state(&kv, &g2).await, GenState::Retiring { .. }));
        assert_eq!(state(&kv, &g1).await, GenState::Committed);
        assert_eq!(state(&kv, &g3).await, GenState::Committed);
        assert_eq!(
            multipart::discard_parts(&kv, 1, "k", &uid, 100, 4)
                .await
                .unwrap(),
            0
        );
        // A part number out of range or an empty list is refused.
        assert!(matches!(
            multipart::begin_part(&kv, "b", "k", &uid, 10_001, 1, LEASE).await,
            Err(MetaError::InvalidArgument(_))
        ));
        // EntityTooSmall when a small part precedes another one.
        let uid2 = multipart::create_upload(&kv, "b", "k2", &UploadInfo::default(), 1)
            .await
            .unwrap()
            .upload_id;
        upload_part(&kv, "b", "k2", &uid2, 1, 10, "a", None, 2)
            .await
            .unwrap();
        upload_part(&kv, "b", "k2", &uid2, 2, 10, "b", None, 2)
            .await
            .unwrap();
        let both = listed(&[(1, "a"), (2, "b")]);
        assert_eq!(
            multipart::complete(
                &kv,
                "b",
                "k2",
                &uid2,
                &complete_req(&both, new_gen_id(), &pre, "x"),
                3
            )
            .await
            .unwrap_err(),
            MetaError::EntityTooSmall(1)
        );
    });
}

#[test]
fn reupload_of_a_part_number_supersedes_and_retires_the_old_part() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let uid = multipart::create_upload(&kv, "b", "k", &UploadInfo::default(), 1)
            .await
            .unwrap()
            .upload_id;
        let old = upload_part(&kv, "b", "k", &uid, 1, 10, "old", None, 2)
            .await
            .unwrap();
        let new = upload_part(&kv, "b", "k", &uid, 1, 10, "new", None, 3)
            .await
            .unwrap();
        assert!(matches!(state(&kv, &old).await, GenState::Retiring { .. }));
        assert_eq!(state(&kv, &new).await, GenState::Committed);
        let page = multipart::list_parts(&kv, "b", "k", &uid, 0, 10)
            .await
            .unwrap();
        assert_eq!(page.parts.len(), 1);
        assert_eq!(page.parts[0].1.generation, new);
        // Complete with the old ETag fails: the client must re-list.
        let stale = listed(&[(1, "old")]);
        assert_eq!(
            multipart::complete(
                &kv,
                "b",
                "k",
                &uid,
                &complete_req(&stale, new_gen_id(), &Precondition::default(), "x"),
                4
            )
            .await
            .unwrap_err(),
            MetaError::InvalidPart(1)
        );
        // The superseded part is collectable: not referenced.
        let referenced = transact(&kv, old, |tx, g| {
            Box::pin(async move {
                let h = get_generation_in(tx, g).await?;
                multipart::part_is_referenced(tx, &h, g).await
            })
        })
        .await
        .unwrap();
        assert!(!referenced);
    });
}

async fn get_generation_in<X: arca_lake_metadata::Txn>(
    tx: &mut X,
    g: &GenId,
) -> Result<arca_lake_metadata::records::GenerationHeader, MetaError> {
    let v = tx
        .get(&keys::gen_header(g))
        .await?
        .ok_or(MetaError::NoSuchGeneration)?;
    arca_lake_metadata::records::decode(&v)
}

#[test]
fn abort_is_batched_and_a_part_in_flight_cannot_seal_afterwards() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let uid = multipart::create_upload(&kv, "b", "k", &UploadInfo::default(), 1)
            .await
            .unwrap()
            .upload_id;
        let mut gens = Vec::new();
        for n in 1..=5 {
            gens.push(
                upload_part(&kv, "b", "k", &uid, n, 10, "e", None, 2)
                    .await
                    .unwrap(),
            );
        }
        // A part still streaming when the abort starts.
        let pending = multipart::begin_part(&kv, "b", "k", &uid, 6, 2, LEASE)
            .await
            .unwrap();
        let rec = multipart::abort(&kv, "b", "k", &uid).await.unwrap();
        assert_eq!(rec.state, MultipartState::Aborting);
        // Linearization point of Abort: Complete now fails.
        let parts = listed(&[(1, "e")]);
        assert_eq!(
            multipart::complete(
                &kv,
                "b",
                "k",
                &uid,
                &complete_req(&parts, new_gen_id(), &Precondition::default(), "x"),
                3
            )
            .await
            .unwrap_err(),
            MetaError::NoSuchUpload
        );
        // Batches of 2: 2, 2, 1 → the record is cleared with the last.
        assert_eq!(
            multipart::discard_parts(&kv, 1, "k", &uid, 2, 3)
                .await
                .unwrap(),
            2
        );
        assert!(
            multipart::get_upload(&kv, "b", "k", &uid)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            multipart::discard_parts(&kv, 1, "k", &uid, 2, 3)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            multipart::discard_parts(&kv, 1, "k", &uid, 2, 3)
                .await
                .unwrap(),
            1
        );
        assert!(
            multipart::get_upload(&kv, "b", "k", &uid)
                .await
                .unwrap()
                .is_none()
        );
        for g in &gens {
            assert!(matches!(state(&kv, g).await, GenState::Retiring { .. }));
        }
        // The in-flight part cannot seal (upload gone) and expires by lease.
        assert_eq!(
            multipart::seal_part(
                &kv,
                &pending.generation,
                &PartInfo {
                    size: 1,
                    etag: "x".into(),
                    checksum: None,
                    chunk_count: 0,
                },
                4,
            )
            .await
            .unwrap_err(),
            MetaError::NoSuchUpload
        );
        let expired = gc::expire_pending(&kv, LEASE + 10, 100).await.unwrap();
        assert_eq!(expired, vec![pending.generation]);
        // Abort of a gone upload: NoSuchUpload.
        assert_eq!(
            multipart::abort(&kv, "b", "k", &uid).await.unwrap_err(),
            MetaError::NoSuchUpload
        );
    });
}

#[test]
fn ten_thousand_parts_complete_in_one_transaction_within_limits() {
    run(async {
        // The in-memory store enforces FoundationDB's size limits.
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Enabled, 1)
            .await
            .unwrap();
        let key = "k".repeat(1024); // longest allowed key: worst case for header rewrites
        let info = UploadInfo {
            checksum_algorithm: Some(ChecksumAlgorithm::Crc32c),
            checksum_type: Some(ChecksumType::Composite),
            ..Default::default()
        };
        let uid = multipart::create_upload(&kv, "b", &key, &info, 1)
            .await
            .unwrap()
            .upload_id;
        let mut parts = Vec::with_capacity(10_000);
        let mut gens = Vec::with_capacity(10_000);
        for n in 1..=10_000u32 {
            let etag = format!("{n:032x}");
            let p = multipart::begin_part(&kv, "b", &key, &uid, n, 2, LEASE)
                .await
                .unwrap();
            // No chunk records: Complete never reads them, so their number
            // is irrelevant to it (and the in-memory store is slow).
            multipart::seal_part(
                &kv,
                &p.generation,
                &PartInfo {
                    size: MIN_PART,
                    etag: etag.clone(),
                    checksum: Some(Checksum {
                        algorithm: ChecksumAlgorithm::Crc32c,
                        kind: ChecksumType::FullObject,
                        value: n.to_be_bytes().to_vec(),
                        parts: None,
                    }),
                    chunk_count: 0,
                },
                2,
            )
            .await
            .unwrap();
            gens.push(p.generation);
            parts.push(CompletePart {
                part_number: n,
                etag,
                checksum: Some(n.to_be_bytes().to_vec()),
            });
        }
        let before = arca_lake_metadata::kv::transact_stats();
        let root = new_gen_id();
        let done = multipart::complete(
            &kv,
            "b",
            &key,
            &uid,
            &CompleteRequest {
                parts: &parts,
                root,
                pre: &Precondition::default(),
                min_part_size: MIN_PART,
                etag: "composite-10000",
                expected_checksum: None,
                derive_checksum: &concat_checksum,
            },
            3,
        )
        .await
        .unwrap();
        let after = arca_lake_metadata::kv::transact_stats();
        assert_eq!(
            after.attempts - before.attempts,
            1,
            "Complete is exactly one transaction attempt"
        );
        assert_eq!(done.size, MIN_PART * 10_000);
        let cs = done.checksum.unwrap();
        assert_eq!(cs.parts, Some(10_000));
        assert_eq!(cs.value.len(), 4 * 10_000);
        let m = read_manifest(&kv, "b", &key, None).await.unwrap().unwrap();
        assert_eq!(m.parts.len(), 10_000);
        assert_eq!(m.parts[9_999].offset, MIN_PART * 9_999);
        // Discarding the (now redundant) part records is batched too.
        let mut total = 0;
        loop {
            let n = multipart::discard_parts(&kv, 1, &key, &uid, 512, 4)
                .await
                .unwrap();
            total += n;
            if n < 512 {
                break;
            }
        }
        assert_eq!(total, 10_000);
        assert_eq!(state(&kv, &gens[0]).await, GenState::Committed);
        // Overwrite the object: the root retires; finishing it retires the
        // parts in bounded steps (I-MP2) and never in one transaction.
        bucket::set_versioning(&kv, "b", Versioning::Disabled)
            .await
            .unwrap();
        object::delete_version(&kv, "b", &key, &done.version_id, 5)
            .await
            .unwrap();
        assert!(matches!(state(&kv, &root).await, GenState::Retiring { .. }));
        assert!(matches!(
            gc::finish(&kv, &root).await.unwrap_err(),
            MetaError::InvalidArgument(_)
        ));
        let mut steps = 0;
        while multipart::finish_root_step(&kv, &root, 256, 6)
            .await
            .unwrap()
        {
            steps += 1;
        }
        assert!(steps >= 39, "10 000 refs in batches of 256: {steps} steps");
        for g in gens.iter().step_by(997) {
            assert!(matches!(
                state(kv_ref(&kv), g).await,
                GenState::Retiring { .. }
            ));
        }
        gc::finish(&kv, &root).await.unwrap();
        assert!(
            multipart::get_upload(&kv, "b", &key, &uid)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            get_generation(&kv, &root).await,
            Err(MetaError::NoSuchGeneration)
        ));
    });
}

fn kv_ref(kv: &MemKv) -> &MemKv {
    kv
}

#[test]
fn concurrent_completes_publish_exactly_one_root() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Enabled, 1)
            .await
            .unwrap();
        let uid = multipart::create_upload(&kv, "b", "k", &UploadInfo::default(), 1)
            .await
            .unwrap()
            .upload_id;
        upload_part(&kv, "b", "k", &uid, 1, 10, "e", None, 2)
            .await
            .unwrap();
        let parts = listed(&[(1, "e")]);
        let pre = Precondition::default();
        let roots: Vec<GenId> = (0..8).map(|_| new_gen_id()).collect();
        let results = futures::future::join_all(roots.iter().map(|r| {
            let parts = &parts;
            let pre = &pre;
            let kv = &kv;
            async move {
                multipart::complete(kv, "b", "k", &uid, &complete_req(parts, *r, pre, "x"), 3).await
            }
        }))
        .await;
        let winner = results
            .iter()
            .filter(|r| r.as_ref().is_ok_and(|c| !c.already_completed))
            .count();
        assert_eq!(winner, 1, "exactly one Complete created the root");
        let ok: Vec<_> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
        assert!(
            ok.iter()
                .all(|c| c.root == ok[0].root && c.version_id == ok[0].version_id)
        );
        // Every failure is NoSuchUpload (never a second root).
        for r in &results {
            if let Err(e) = r {
                assert_eq!(*e, MetaError::NoSuchUpload);
            }
        }
        let m = read_manifest(&kv, "b", "k", None).await.unwrap().unwrap();
        assert_eq!(m.pointer.generation, ok[0].root);
        let versions = arca_lake_metadata::ops::list::list_versions(
            &kv,
            "b",
            &arca_lake_metadata::ops::list::ListVersionsRequest {
                max_keys: 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(versions.versions.len(), 1);
    });
}

#[test]
fn complete_and_abort_race_has_exactly_one_winner() {
    for seed in 0..6u64 {
        run(async {
            let kv = MemKv::default();
            bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
                .await
                .unwrap();
            let uid = multipart::create_upload(&kv, "b", "k", &UploadInfo::default(), 1)
                .await
                .unwrap()
                .upload_id;
            let g = upload_part(&kv, "b", "k", &uid, 1, 10, "e", None, 2)
                .await
                .unwrap();
            let parts = listed(&[(1, "e")]);
            let pre = Precondition::default();
            let root = new_gen_id();
            let req = complete_req(&parts, root, &pre, "x");
            let complete = multipart::complete(&kv, "b", "k", &uid, &req, 3);
            let abort = multipart::abort(&kv, "b", "k", &uid);
            let (c, a) = if seed % 2 == 0 {
                futures::join!(complete, abort)
            } else {
                let (a, c) = futures::join!(abort, complete);
                (c, a)
            };
            match (c, a) {
                (Ok(_), Err(MetaError::NoSuchUpload)) => {
                    // Complete won: the part is referenced by the root and
                    // the abort sweep must not touch it.
                    assert_eq!(
                        multipart::discard_parts(&kv, 1, "k", &uid, 10, 4)
                            .await
                            .unwrap(),
                        1
                    );
                    assert_eq!(state(&kv, &g).await, GenState::Committed);
                    assert!(head_object(&kv, "b", "k").await.unwrap().is_some());
                }
                (Err(MetaError::NoSuchUpload), Ok(_)) => {
                    // Abort won: no root, the part is retired by the sweep.
                    assert!(head_object(&kv, "b", "k").await.unwrap().is_none());
                    assert!(matches!(
                        get_generation(&kv, &root).await,
                        Err(MetaError::NoSuchGeneration)
                    ));
                    multipart::discard_parts(&kv, 1, "k", &uid, 10, 4)
                        .await
                        .unwrap();
                    assert!(matches!(state(&kv, &g).await, GenState::Retiring { .. }));
                }
                other => panic!("seed {seed}: unexpected outcome {other:?}"),
            }
        });
    }
}

#[test]
fn part_is_referenced_reflects_every_state() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let uid = multipart::create_upload(&kv, "b", "k", &UploadInfo::default(), 1)
            .await
            .unwrap()
            .upload_id;
        let g1 = upload_part(&kv, "b", "k", &uid, 1, 10, "e1", None, 2)
            .await
            .unwrap();
        let g2 = upload_part(&kv, "b", "k", &uid, 2, 10, "e2", None, 2)
            .await
            .unwrap();
        let referenced = |g: GenId| {
            let kv = &kv;
            async move {
                transact(kv, g, |tx, g| {
                    Box::pin(async move {
                        let h = get_generation_in(tx, g).await?;
                        multipart::part_is_referenced(tx, &h, g).await
                    })
                })
                .await
                .unwrap()
            }
        };
        assert!(referenced(g1).await && referenced(g2).await, "open upload");
        let parts = listed(&[(1, "e1")]);
        let root = new_gen_id();
        multipart::complete(
            &kv,
            "b",
            "k",
            &uid,
            &complete_req(&parts, root, &Precondition::default(), "x"),
            3,
        )
        .await
        .unwrap();
        assert!(referenced(g1).await, "listed part after Complete");
        assert!(!referenced(g2).await, "unlisted part after Complete");
        // Even if g1 were wrongly queued for GC, verify step would refuse.
        // Root retired and finished: nothing is referenced any more.
        object::delete_object(&kv, "b", "k", 4).await.unwrap();
        while multipart::finish_root_step(&kv, &root, 10, 5)
            .await
            .unwrap()
        {}
        gc::finish(&kv, &root).await.unwrap();
        assert!(!referenced(g1).await);
        assert!(matches!(state(&kv, &g1).await, GenState::Retiring { .. }));
    });
}

#[test]
fn sweep_candidates_cover_aborting_completed_with_records_and_expired_open() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let info = UploadInfo::default();
        let old = multipart::create_upload(&kv, "b", "old", &info, 10)
            .await
            .unwrap();
        let young = multipart::create_upload(&kv, "b", "young", &info, 1_000)
            .await
            .unwrap();
        let aborting = multipart::create_upload(&kv, "b", "ab", &info, 1_000)
            .await
            .unwrap();
        multipart::abort(&kv, "b", "ab", &aborting.upload_id)
            .await
            .unwrap();
        let done = multipart::create_upload(&kv, "b", "done", &info, 1_000)
            .await
            .unwrap();
        upload_part(&kv, "b", "done", &done.upload_id, 1, 10, "e", None, 2)
            .await
            .unwrap();
        let parts = listed(&[(1, "e")]);
        multipart::complete(
            &kv,
            "b",
            "done",
            &done.upload_id,
            &complete_req(&parts, new_gen_id(), &Precondition::default(), "x"),
            3,
        )
        .await
        .unwrap();
        let c = multipart::sweep_candidates(&kv, Some(500), 100)
            .await
            .unwrap();
        let keys: Vec<&str> = c.iter().map(|r| r.key.as_str()).collect();
        assert!(keys.contains(&"old"), "expired open upload");
        assert!(!keys.contains(&"young"), "young open upload is left alone");
        assert!(keys.contains(&"ab"), "aborting upload");
        assert!(keys.contains(&"done"), "completed upload with part records");
        let _ = young;
        // Once the completed upload's records are discarded it drops out.
        multipart::discard_parts(&kv, 1, "done", &done.upload_id, 10, 4)
            .await
            .unwrap();
        let c = multipart::sweep_candidates(&kv, None, 100).await.unwrap();
        let keys: Vec<&str> = c.iter().map(|r| r.key.as_str()).collect();
        assert_eq!(keys, vec!["ab"]);
        let _ = old;
    });
}

#[test]
fn complete_evaluates_conditional_headers_and_checksums() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        let info = UploadInfo {
            checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
            ..Default::default()
        };
        let uid = multipart::create_upload(&kv, "b", "k", &info, 1)
            .await
            .unwrap()
            .upload_id;
        let cs = |b: u8| Checksum {
            algorithm: ChecksumAlgorithm::Sha256,
            kind: ChecksumType::FullObject,
            value: vec![b; 32],
            parts: None,
        };
        upload_part(&kv, "b", "k", &uid, 1, 10, "e1", Some(cs(1)), 2)
            .await
            .unwrap();
        upload_part(&kv, "b", "k", &uid, 2, 10, "e2", Some(cs(2)), 2)
            .await
            .unwrap();
        // Wrong part checksum in the request → InvalidPart.
        let mut parts = listed(&[(1, "e1"), (2, "e2")]);
        parts[1].checksum = Some(vec![9; 32]);
        let pre = Precondition::default();
        let req = CompleteRequest {
            parts: &parts,
            root: new_gen_id(),
            pre: &pre,
            min_part_size: 1,
            etag: "x",
            expected_checksum: None,
            derive_checksum: &concat_checksum,
        };
        assert_eq!(
            multipart::complete(&kv, "b", "k", &uid, &req, 3)
                .await
                .unwrap_err(),
            MetaError::InvalidPart(2)
        );
        parts[1].checksum = None;
        // Expected object checksum that does not match the derived one.
        let wrong = Checksum {
            algorithm: ChecksumAlgorithm::Sha256,
            kind: ChecksumType::Composite,
            value: vec![0; 64],
            parts: Some(2),
        };
        let req = CompleteRequest {
            parts: &parts,
            root: new_gen_id(),
            pre: &pre,
            min_part_size: 1,
            etag: "x",
            expected_checksum: Some(&wrong),
            derive_checksum: &concat_checksum,
        };
        assert!(matches!(
            multipart::complete(&kv, "b", "k", &uid, &req, 3).await,
            Err(MetaError::InvalidArgument(_))
        ));
        assert!(
            head_object(&kv, "b", "k").await.unwrap().is_none(),
            "nothing published"
        );
        // If-None-Match: * with an existing object fails inside the transaction.
        let p = object::begin_put(&kv, "b", "k", 1, LEASE).await.unwrap();
        object::commit_put(
            &kv,
            &p.generation,
            &object::CommitInfo {
                size: 1,
                etag: "existing".into(),
                ..Default::default()
            },
            &pre,
            2,
        )
        .await
        .unwrap();
        let pre_none = Precondition {
            if_none_match_any: true,
            ..Default::default()
        };
        let req = CompleteRequest {
            parts: &parts,
            root: new_gen_id(),
            pre: &pre_none,
            min_part_size: 1,
            etag: "x",
            expected_checksum: None,
            derive_checksum: &concat_checksum,
        };
        assert_eq!(
            multipart::complete(&kv, "b", "k", &uid, &req, 3)
                .await
                .unwrap_err(),
            MetaError::PreconditionFailed
        );
        // If-Match with the right ETag succeeds and the derived composite
        // checksum is stored.
        let pre_match = Precondition {
            if_match: Some("existing".into()),
            ..Default::default()
        };
        let root = new_gen_id();
        let req = CompleteRequest {
            parts: &parts,
            root,
            pre: &pre_match,
            min_part_size: 1,
            etag: "x",
            expected_checksum: None,
            derive_checksum: &concat_checksum,
        };
        let done = multipart::complete(&kv, "b", "k", &uid, &req, 3)
            .await
            .unwrap();
        let c = done.checksum.unwrap();
        assert_eq!(c.kind, ChecksumType::Composite);
        assert_eq!(c.parts, Some(2));
        assert_eq!(c.value, [vec![1; 32], vec![2; 32]].concat());
        let m = read_manifest(&kv, "b", "k", None).await.unwrap().unwrap();
        assert_eq!(
            m.header.checksum.as_ref().map(|c| c.kind),
            Some(ChecksumType::Composite)
        );
        assert_eq!(m.pointer.generation, root);
    });
}

/// The sweep's candidates are found page by page beyond the first page of
/// the upload space, and at most `limit` are returned (Phase 3.5 audit
/// OPS-1: the whole space used to be read in one transaction).
#[test]
fn sweep_candidates_page_through_many_uploads() {
    run(async {
        let kv = MemKv::default();
        bucket::create_bucket(&kv, "b", Versioning::Disabled, 1)
            .await
            .unwrap();
        // 2500 open uploads (not yet expired), keys sorting first; then
        // three aborting ones at the end of the space.
        for i in 0..2500 {
            multipart::create_upload(&kv, "b", &format!("a/{i:05}"), &UploadInfo::default(), 10)
                .await
                .unwrap();
        }
        for i in 0..3 {
            let up =
                multipart::create_upload(&kv, "b", &format!("z/{i}"), &UploadInfo::default(), 10)
                    .await
                    .unwrap();
            multipart::abort(&kv, "b", &format!("z/{i}"), &up.upload_id)
                .await
                .unwrap();
        }
        let c = multipart::sweep_candidates(&kv, Some(5), 100)
            .await
            .unwrap();
        assert_eq!(c.len(), 3);
        assert!(c.iter().all(|r| r.state == MultipartState::Aborting));
        let c = multipart::sweep_candidates(&kv, Some(11), 1000)
            .await
            .unwrap();
        assert_eq!(c.len(), 1000, "expired open uploads, at most the limit");
    });
}
