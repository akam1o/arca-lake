//! Golden encodings of every metadata record (Phase 3.4 §8, ADR-0006):
//! the bytes of one sample of each record type (every enum variant that
//! changes the layout, every optional field set) are fixed in
//! `tests/golden/records-v2.hex`. The test fails when an encoding changes
//! (an unintended serialization change), checks that the fixed bytes still
//! decode to the same values (old data stays readable), and that unknown
//! encoding versions and damaged records are refused.
//!
//! A deliberate format change adds a new record version and a new golden
//! file; `ARCA_METADATA_WRITE_GOLDEN=1 cargo test -p arca-lake-metadata
//! --test golden_records` rewrites the current one (review the diff).

use std::fmt::Debug;
use std::path::PathBuf;

use arca_lake_common::compat::{Capabilities, Target};
use arca_lake_metadata::ops::admission::{AdmissionRecord, GroupRoom, TierAdmission};
use arca_lake_metadata::ops::delete_fence::DeleteFenceRecord;
use arca_lake_metadata::ops::integrity::{
    Finding, ManifestCursor, RepairState, RepairTask, ScrubCursor, ScrubPhase,
};
use arca_lake_metadata::ops::membership::{NodeLifecycle, NodeStateRecord, ReplaceRequest};
use arca_lake_metadata::ops::rebalance::{
    RebalanceInTransit, RebalancePacing, RebalancePlan, RebalanceState,
};
use arca_lake_metadata::ops::recovery::{
    Acceptance, AffectedObject, DataLossRecord, FinalizeAudit, NodeCapRecord, RecoveryPhase,
    RestoreCounts, RestoreCursor, RestoreReport,
};
use arca_lake_metadata::ops::repair_status::{RepairReason, RepairStatus};
use arca_lake_metadata::records::{
    BucketRecord, CacheCopyTokens, CachePlacement, CacheShardToken, Checksum, ChecksumAlgorithm,
    ChecksumType, ChunkRecord, ClusterPhase, ClusterStateRecord, CurrentPointer, DurablePlacement,
    GenKind, GenState, GenerationHeader, GroupMember, GroupRecord, GroupRole, GroupState,
    MultipartRecord, MultipartState, NodeRecord, PartRecord, PartRef, ProcKind, ProcRecord,
    RECORD_VERSION, Record, RepairCursor, UpgradeCursor, UpgradeStep, VersionRecord, Versioning,
    decode, encode,
};
use serde::Serialize;

const G: [u8; 16] = [
    0x01, 0xa1, 0xa0, 0x5d, 0, 0, 0x70, 1, 0x80, 2, 3, 4, 5, 6, 7, 8,
];
const U: [u8; 16] = [
    0x01, 0xa1, 0xa0, 0x5e, 0, 0, 0x70, 9, 0x80, 8, 7, 6, 5, 4, 3, 2,
];

fn golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden/records-v2.hex")
}

fn checksum() -> Checksum {
    Checksum {
        algorithm: ChecksumAlgorithm::Crc32c,
        kind: ChecksumType::Composite,
        value: vec![1, 2, 3, 4],
        parts: Some(3),
    }
}

fn caps() -> Capabilities {
    Capabilities {
        protocol_min: 1,
        protocol_max: 2,
        schema_min: 1,
        schema_max: 2,
        format_min: 1,
        format_max: 1,
    }
}

fn affected() -> AffectedObject {
    AffectedObject {
        bucket: "b".into(),
        key: "k/x".into(),
        generation: G,
        chunk_index: 2,
        group: 7,
        have: 1,
        need: 2,
        why: "unreadable".into(),
    }
}

fn counts() -> RestoreCounts {
    RestoreCounts {
        matched: 1,
        orphan_post_t: 2,
        orphan_pre_t: 3,
        former_member: 4,
        missing: 5,
        corrupt: 6,
        degraded_chunks: 7,
        unreadable_chunks: 8,
        placement_epoch_mismatch: 9,
        membership_mismatch: 10,
        manifest_findings: 11,
        cache_mismatch: 12,
        epochs_raised: 13,
    }
}

fn header(state: GenState, kind: GenKind) -> GenerationHeader {
    GenerationHeader {
        state,
        bucket_id: 3,
        key: "photos/2026/a.jpg".into(),
        size: 5_242_881,
        etag: "9e107d9d372bb6826bd81d3542a419d6".into(),
        content_type: Some("image/jpeg".into()),
        user_metadata: vec![("owner".into(), "x".into())],
        chunk_count: 2,
        created_ms: 1_793_000_000_000,
        kind,
        checksum: Some(checksum()),
    }
}

/// `(name, encoded sample, round trip)`: the round trip decodes bytes as
/// the sample's type and compares.
type Case = (
    &'static str,
    Vec<u8>,
    Box<dyn Fn(&[u8]) -> Result<(), String>>,
);

fn case<T: Record + Serialize + PartialEq + Debug + 'static>(name: &'static str, v: T) -> Case {
    let bytes = encode(&v).expect("encode");
    let check = move |b: &[u8]| -> Result<(), String> {
        let got: T = decode(b).map_err(|e| format!("{name}: {e}"))?;
        if got == v {
            Ok(())
        } else {
            Err(format!("{name}: decoded {got:?}, expected {v:?}"))
        }
    };
    (name, bytes, Box::new(check))
}

#[allow(clippy::too_many_lines)]
fn cases() -> Vec<Case> {
    vec![
        case(
            "ClusterStateRecord",
            ClusterStateRecord {
                incarnation: G,
                state: ClusterPhase::RecoveryRequired,
                restored_at_ms: Some(10),
                restored_from: Some("backup-1".into()),
                backup_point_ms: Some(9),
                changed_ms: 11,
                changed_by: "admin".into(),
            },
        ),
        case(
            "ProcRecord",
            ProcRecord {
                id: "gw-a".into(),
                kind: ProcKind::Gateway,
                software: "0.0.0".into(),
                caps: caps(),
                incarnation: G,
                last_ms: 12,
            },
        ),
        case(
            "UpgradeCursor",
            UpgradeCursor {
                target: Target {
                    protocol: 2,
                    schema: 2,
                    format: 1,
                },
                step: UpgradeStep::SchemaWriteRaised,
                ignore_absent: vec!["ctl-old".into()],
                started_ms: 1,
                updated_ms: 2,
                requested_by: "admin".into(),
                by_token: 3,
                last_error: Some("waiting".into()),
                history: vec!["requested".into()],
            },
        ),
        case(
            "BucketRecord",
            BucketRecord {
                id: 3,
                name: "photos".into(),
                versioning: Versioning::Suspended,
                created_ms: 4,
                owner: "alice".into(),
                policy: Some("{}".into()),
            },
        ),
        case(
            "CurrentPointer",
            CurrentPointer {
                generation: G,
                version_id: "01a1a05d0000700180020304".into(),
                delete_marker: false,
                next_ordinal: 7,
                size: 100,
                etag: "e".into(),
                last_modified_ms: 5,
            },
        ),
        case(
            "VersionRecord",
            VersionRecord {
                generation: G,
                version_id: "v".into(),
                delete_marker: true,
                committed_ms: 6,
                size: 0,
                etag: String::new(),
            },
        ),
        case(
            "GenerationHeader/object-pending",
            header(GenState::Pending { lease_until_ms: 99 }, GenKind::Object),
        ),
        case(
            "GenerationHeader/part-committed",
            header(
                GenState::Committed,
                GenKind::Part {
                    upload_id: U,
                    part_number: 2,
                },
            ),
        ),
        case(
            "GenerationHeader/root-retiring",
            header(
                GenState::Retiring { since_ms: 98 },
                GenKind::MultipartRoot {
                    upload_id: U,
                    part_count: 3,
                },
            ),
        ),
        case(
            "MultipartRecord",
            MultipartRecord {
                upload_id: U,
                bucket_id: 3,
                key: "k".into(),
                initiated_ms: 7,
                owner: "alice".into(),
                content_type: Some("text/plain".into()),
                user_metadata: vec![("a".into(), "b".into())],
                checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
                checksum_type: Some(ChecksumType::FullObject),
                state: MultipartState::Completed { root: G },
            },
        ),
        case(
            "PartRecord",
            PartRecord {
                generation: G,
                size: 5 << 20,
                etag: "p".into(),
                checksum: Some(checksum()),
                chunk_count: 5,
                uploaded_ms: 8,
            },
        ),
        case(
            "PartRef",
            PartRef {
                generation: G,
                part_number: 2,
                offset: 5 << 20,
                size: 5 << 20,
                chunk_count: 5,
                etag: "p".into(),
                checksum: Some(checksum()),
            },
        ),
        case(
            "ChunkRecord",
            ChunkRecord {
                chunk_id: G,
                offset: 1 << 20,
                len: 1 << 20,
                scheme: "rs-gf8-v1".into(),
                data_shards: 4,
                parity_shards: 2,
                crc32c: 0xdead_beef,
                placement: DurablePlacement {
                    group_id: 7,
                    epoch: 3,
                },
            },
        ),
        case(
            "CachePlacement",
            CachePlacement {
                tier: "fast".into(),
                group_id: 8,
                epoch: 2,
                bytes: 1 << 20,
                pinned: true,
                last_access_ms: 9,
                created_ms: 8,
            },
        ),
        case(
            "NodeRecord",
            NodeRecord {
                id: 4,
                address: "node-4:9100".into(),
                labels: vec![("host".into(), "h4".into())],
                devices: vec![0, 1],
                registered_ms: 1,
                last_seen_ms: 2,
            },
        ),
        case(
            "GroupRecord",
            GroupRecord {
                id: 7,
                profile: "default".into(),
                tier: "default".into(),
                scheme: "rs-gf8-v1".into(),
                data_shards: 2,
                parity_shards: 1,
                members: vec![
                    GroupMember {
                        node_id: 1,
                        device_id: 0,
                    },
                    GroupMember {
                        node_id: 2,
                        device_id: 1,
                    },
                ],
                epoch: 3,
                state: GroupState::Repairing,
                repair_cursor: Some(RepairCursor {
                    member: 1,
                    chunk_id: G,
                }),
                role: GroupRole::Cache,
            },
        ),
        case(
            "NodeCapRecord",
            NodeCapRecord {
                node_id: 4,
                caps: caps(),
                software: "0.0.0".into(),
                format_write: 1,
                incarnation: Some(G),
                group_epochs: vec![(7, 3)],
                seen_ms: 10,
            },
        ),
        case(
            "NodeStateRecord/draining",
            NodeStateRecord {
                node_id: 4,
                state: NodeLifecycle::Draining {
                    since_ms: 1,
                    by: "admin".into(),
                },
                blocked: Some("insufficient capacity".into()),
            },
        ),
        case(
            "NodeStateRecord/decommissioned",
            NodeStateRecord {
                node_id: 5,
                state: NodeLifecycle::Decommissioned {
                    since_ms: 1,
                    at_ms: 2,
                },
                blocked: None,
            },
        ),
        case(
            "ReplaceRequest",
            ReplaceRequest {
                group: 7,
                index: 1,
                from_node: 2,
                to_node: Some(9),
                requested_ms: 3,
                by: "admin".into(),
                last_error: Some("e".into()),
            },
        ),
        case(
            "RepairTask/planned",
            RepairTask {
                reason: Finding::ChecksumMismatch,
                hint_targets: vec![1],
                state: RepairState::Planned {
                    targets: vec![1, 2],
                    epoch: 3,
                },
                attempts: 1,
                next_attempt_ms: 2,
                created_ms: 3,
                updated_ms: 4,
                last_error: Some("x".into()),
            },
        ),
        case(
            "RepairTask/unrecoverable",
            RepairTask {
                reason: Finding::UnrecoverableStripe,
                hint_targets: vec![],
                state: RepairState::Unrecoverable {
                    have: 1,
                    need: 2,
                    since_ms: 5,
                },
                attempts: 3,
                next_attempt_ms: 6,
                created_ms: 7,
                updated_ms: 8,
                last_error: None,
            },
        ),
        case(
            "ScrubCursor",
            ScrubCursor {
                pass: 4,
                phase: ScrubPhase::Shards {
                    member: 1,
                    after: Some(G),
                },
                started_ms: 1,
                finished_ms: 2,
                verified: 3,
                bytes: 4,
                findings: [1, 2, 3, 4, 5, 6, 7, 8],
                errors: 9,
            },
        ),
        case(
            "ManifestCursor",
            ManifestCursor {
                pass: 2,
                after: Some(G),
                done: false,
                started_ms: 1,
                finished_ms: 0,
                roots: 5,
                findings: 1,
            },
        ),
        case(
            "RestoreCursor",
            RestoreCursor {
                incarnation: G,
                phase: RecoveryPhase::Presence {
                    group: 7,
                    after: Some(U),
                },
                restore_point_ms: 1,
                started_ms: 2,
                updated_ms: 3,
                unanswered_nodes: vec![4],
                excluded_nodes: vec![5],
            },
        ),
        case(
            "RestoreCursor/records",
            RestoreCursor {
                incarnation: G,
                phase: RecoveryPhase::Records { after: Some(U) },
                restore_point_ms: 1,
                started_ms: 2,
                updated_ms: 3,
                unanswered_nodes: vec![],
                excluded_nodes: vec![],
            },
        ),
        case(
            "RestoreReport",
            RestoreReport {
                incarnation: G,
                restore_point_ms: 1,
                counts: counts(),
                affected: vec![affected()],
                complete: true,
                unanswered_nodes: vec![],
                finished_ms: 2,
                accepted: Some(Acceptance {
                    at_ms: 3,
                    by: "op".into(),
                    forced: true,
                }),
            },
        ),
        case(
            "FinalizeAudit",
            FinalizeAudit {
                incarnation: G,
                at_ms: 1,
                operator: "op".into(),
                reason: "accepted loss".into(),
                force: true,
                phase_before: "RecoveryRequired".into(),
                result: "activated-forced".into(),
                critical: vec!["1 unreadable chunks".into()],
                warnings: vec!["2 missing shards (repair queued)".into()],
                counts: Some(counts()),
                affected_listed: 1,
                affected_total: 1,
            },
        ),
        case(
            "DataLossRecord",
            DataLossRecord {
                incarnation: G,
                object: affected(),
                accepted_ms: 1,
                accepted_by: "op".into(),
            },
        ),
        // Phase 3.6/3.7 records (added before any release that writes
        // them).
        case(
            "AdmissionRecord",
            AdmissionRecord {
                computed_ms: 1,
                ttl_ms: 30_000,
                gateways: 2,
                tiers: vec![TierAdmission {
                    tier: "default".into(),
                    state: 2,
                    rate_bytes_per_s: 1 << 20,
                    excluded_groups: vec![3],
                    free_permille: 87,
                    reason: "full in 100 s".into(),
                    group_room: vec![GroupRoom { group: 1, room: 7 }],
                }],
                leader_token: 9,
                generation: 4,
                warming_up: true,
                capacity_conflicts: vec![5, 6],
                capacity_unverified: vec![7],
                incarnation: [9; 16],
            },
        ),
        case(
            "DeleteFenceRecord",
            DeleteFenceRecord {
                generation: 12,
                open: false,
                leader_token: 3,
                incarnation: [5; 16],
                changed_ms: 9,
                reason: "before restore".into(),
                operator_hold: true,
            },
        ),
        case(
            "RebalancePacing",
            RebalancePacing {
                plan: Some(("default".into(), 1, 2)),
                remaining_bytes: 4 << 20,
                settling: vec![("cold".into(), 9)],
            },
        ),
        case(
            "RebalanceState",
            RebalanceState {
                paused: true,
                dry_run: false,
                cursor: Some(G),
                plan: Some(RebalancePlan {
                    tier: "default".into(),
                    source: 1,
                    target: 2,
                    source_room: 3,
                    target_room: 4,
                }),
                moved_chunks: 5,
                moved_bytes: 6,
                skipped_chunks: 7,
                last_pass_ms: 8,
                last_reason: "pass budget spent".into(),
            },
        ),
        case(
            "CacheCopyTokens",
            CacheCopyTokens {
                group_id: 2,
                shards: vec![CacheShardToken {
                    index: 1,
                    store_epoch: 3,
                    sequence: 4,
                }],
            },
        ),
        case(
            "RebalanceInTransit",
            RebalanceInTransit {
                entries: vec![(1, 4 << 20, 9 << 20, 99), (2, 7, 0, 100)],
            },
        ),
        case(
            "RepairStatus",
            RepairStatus {
                group: 4,
                epoch: 9,
                index: 2,
                target: 17,
                reason: RepairReason::AwaitingCapacity,
                detail: "no room".into(),
                since_ms: 1_793_000_000_000,
                last_progress_ms: 1_793_000_000_500,
                failures: 3,
                stalls: 4,
                next_attempt_ms: 1_793_000_060_000,
                former: vec![5, 17],
                untrusted: vec![17],
                walked_clean: true,
                retargets: 1,
                excluded: vec![3],
                no_target: true,
            },
        ),
    ]
}

fn hex(b: &[u8]) -> String {
    b.iter().fold(String::new(), |mut s, x| {
        use std::fmt::Write as _;
        let _ = write!(s, "{x:02x}");
        s
    })
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

#[test]
fn every_record_encoding_is_fixed_and_old_bytes_decode() {
    let cases = cases();
    let path = golden_path();
    if std::env::var_os("ARCA_METADATA_WRITE_GOLDEN").is_some() {
        let text = cases.iter().fold(String::new(), |mut t, (n, b, _)| {
            t.push_str(n);
            t.push(' ');
            t.push_str(&hex(b));
            t.push('\n');
            t
        });
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, text).unwrap();
    }
    let golden = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("missing golden file {}: {e}", path.display()));
    let golden: Vec<(&str, Vec<u8>)> = golden
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            let (n, h) = l.split_once(' ').expect("name hex");
            (n, unhex(h))
        })
        .collect();
    assert_eq!(
        golden.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        cases.iter().map(|(n, _, _)| *n).collect::<Vec<_>>(),
        "the golden file lists every case"
    );
    for ((name, bytes, check), (_, fixed)) in cases.iter().zip(&golden) {
        // The current encoder still produces the fixed bytes ...
        assert_eq!(hex(bytes), hex(fixed), "{name}: the encoding changed");
        // ... and the fixed bytes still decode to the same value.
        check(fixed).unwrap();
        // An unknown (newer or zero) version is refused.
        for v in [RECORD_VERSION + 1, 0] {
            let mut b = fixed.clone();
            b[0] = v;
            assert!(check(&b).is_err(), "{name}: version {v} accepted");
        }
        // A truncated record is refused, never decoded to something else.
        let short = &fixed[..fixed.len() - 1];
        assert!(check(short).is_err(), "{name}: a truncated record decoded");
        assert!(check(&[]).is_err());
    }
}
