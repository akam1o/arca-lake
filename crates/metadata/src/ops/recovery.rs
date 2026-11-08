//! Restore verification state (RFC-015 §5.3.2, ADR-0016): the cursor the
//! controller leader resumes from and the report per incarnation. Node
//! capability records (RFC-014 §5.1) live here too: nodes cannot write
//! metadata, so the controller records what `Health` reported.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::{KvStore, Txn};
use crate::ops::{read, write};
use crate::records::{GenId, Record, decode};
use crate::{keys, transact};

/// What the controller learned about a node from `Health`
/// (`("al","ncap",node_id)`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapRecord {
    /// Node id.
    pub node_id: u64,
    /// Capabilities (schema axes are 0: nodes hold no metadata).
    pub caps: arca_lake_common::compat::Capabilities,
    /// Software version string.
    pub software: String,
    /// Format the node stamps on new segments.
    pub format_write: u64,
    /// Incarnation the node adopted, if any.
    pub incarnation: Option<[u8; 16]>,
    /// Group epochs the node enforces.
    pub group_epochs: Vec<(u64, u64)>,
    /// When this was observed, Unix milliseconds.
    pub seen_ms: u64,
}

impl Record for NodeCapRecord {}

/// Record what a node reported.
///
/// # Errors
///
/// Backend errors.
pub async fn set_node_caps<S: KvStore>(store: &S, rec: &NodeCapRecord) -> Result<()> {
    transact(store, rec, |tx, rec| {
        Box::pin(async move { write(tx, &keys::node_caps(rec.node_id), *rec) })
    })
    .await
}

/// Every node capability record.
///
/// # Errors
///
/// Backend errors.
pub async fn node_caps<S: KvStore>(store: &S) -> Result<Vec<NodeCapRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::node_caps_range(), usize::MAX, false)
                .await?
            {
                out.push(decode::<NodeCapRecord>(&v)?);
            }
            Ok(out)
        })
    })
    .await
}

/// Where the recovery pass is (RFC-015 §5.3.1 steps 5–6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryPhase {
    /// Epoch catch-up (§5.4): groups up to and including `after_group` are
    /// done.
    CatchUp {
        /// Last group whose epoch was raised and fenced.
        after_group: Option<u64>,
    },
    /// Verifying shards of `group` on `member`, after chunk `after`.
    Shards {
        /// Group being verified.
        group: u64,
        /// Member index.
        member: u16,
        /// Last chunk id verified.
        after: Option<[u8; 16]>,
    },
    /// Presence and orphan classification of `group` after chunk `after`.
    Presence {
        /// Group.
        group: u64,
        /// Last chunk id classified.
        after: Option<[u8; 16]>,
    },
    /// Multipart manifests after root `after`.
    Manifests {
        /// Last root verified.
        after: Option<GenId>,
    },
    /// Everything was examined; the report is final.
    Done,
    /// Metadata-side walk of live chunk records after generation `after`:
    /// chunks no member lists at all (the node-driven presence walk never
    /// sees them). Declared last to keep the encoding of the others.
    Records {
        /// Last generation examined.
        after: Option<GenId>,
    },
}

/// `("al","meta","restore","cursor")`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreCursor {
    /// Incarnation this pass belongs to.
    pub incarnation: [u8; 16],
    /// Where the pass is.
    pub phase: RecoveryPhase,
    /// Restore point (`restored_at_ms`), separating pre- and post-T orphans.
    pub restore_point_ms: u64,
    /// When the pass started.
    pub started_ms: u64,
    /// Last change.
    pub updated_ms: u64,
    /// Nodes whose listing could not be completed (RFC-015 §5.3.2
    /// completion rule).
    pub unanswered_nodes: Vec<u64>,
    /// Node ids the operator excluded (`--exclude-node`).
    pub excluded_nodes: Vec<u64>,
}

impl Record for RestoreCursor {}

/// One unreadable or dangling object found by verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AffectedObject {
    /// Bucket name (empty when the bucket record is gone).
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Generation.
    pub generation: GenId,
    /// Chunk index.
    pub chunk_index: u32,
    /// Group.
    pub group: u64,
    /// Good shards found.
    pub have: u16,
    /// Shards needed.
    pub need: u16,
    /// Why (`unreadable`, `dangling-part`, …).
    pub why: String,
}

/// Verification counts (RFC-015 §5.3.2 classes). Shard counts unless noted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreCounts {
    /// Shards whose record and metadata agree.
    pub matched: u64,
    /// Orphans whose generation id is newer than the restore point.
    pub orphan_post_t: u64,
    /// Orphans older than the restore point (pre-restore garbage).
    pub orphan_pre_t: u64,
    /// Shards on former members.
    pub former_member: u64,
    /// Shards a live chunk record places on a member that lacks them.
    pub missing: u64,
    /// Shards the node reported corrupt or quarantined.
    pub corrupt: u64,
    /// Chunks with at least `k` but not all shards (repair queued).
    pub degraded_chunks: u64,
    /// Chunks with fewer than `k` good shards.
    pub unreadable_chunks: u64,
    /// Chunk records whose placement epoch is ahead of the group.
    pub placement_epoch_mismatch: u64,
    /// Node/group pairs where the node's epoch disagrees with the group
    /// (before catch-up), or a node enforces a group metadata lacks.
    pub membership_mismatch: u64,
    /// Multipart manifest findings.
    pub manifest_findings: u64,
    /// Cache copies without a placement record, or placements without a
    /// copy.
    pub cache_mismatch: u64,
    /// Groups whose epoch was raised by catch-up.
    pub epochs_raised: u64,
}

/// `("al","meta","restore","report",incarnation)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreReport {
    /// Incarnation.
    pub incarnation: [u8; 16],
    /// Restore point.
    pub restore_point_ms: u64,
    /// Counts.
    pub counts: RestoreCounts,
    /// Unreadable or dangling objects (bounded to [`MAX_AFFECTED`]).
    pub affected: Vec<AffectedObject>,
    /// Whether every node answered completely.
    pub complete: bool,
    /// Nodes that did not answer.
    pub unanswered_nodes: Vec<u64>,
    /// When verification finished (0 = still running).
    pub finished_ms: u64,
    /// Operator acceptance (`admin recovery finalize`), if any.
    pub accepted: Option<Acceptance>,
}

impl Record for RestoreReport {}

/// Operator acceptance of a report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acceptance {
    /// When.
    pub at_ms: u64,
    /// Who.
    pub by: String,
    /// Whether `--force` was needed (incomplete or unreadable).
    pub forced: bool,
}

/// Objects listed per report.
pub const MAX_AFFECTED: usize = 1000;

/// The cursor, if any.
///
/// # Errors
///
/// Backend errors.
pub async fn cursor<S: KvStore>(store: &S) -> Result<Option<RestoreCursor>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move { read(tx, &keys::restore_cursor()).await })
    })
    .await
}

/// Replace the cursor.
///
/// # Errors
///
/// Backend errors.
pub async fn set_cursor<S: KvStore>(store: &S, c: &RestoreCursor) -> Result<()> {
    transact(store, c, |tx, c| {
        Box::pin(async move { write(tx, &keys::restore_cursor(), *c) })
    })
    .await
}

/// Replace the cursor and the report in one transaction: a crash (or an
/// unknown commit outcome) between pages can neither advance the cursor
/// without the page's counts nor count a page twice. Both values are
/// absolute, so a retried commit is idempotent.
///
/// # Errors
///
/// Backend errors.
pub async fn set_progress<S: KvStore>(
    store: &S,
    c: &RestoreCursor,
    r: &RestoreReport,
) -> Result<()> {
    transact(store, (c, r), |tx, env| {
        Box::pin(async move {
            let (c, r) = *env;
            write(tx, &keys::restore_cursor(), c)?;
            write(tx, &keys::restore_report(&r.incarnation), r)
        })
    })
    .await
}

/// The report of an incarnation.
///
/// # Errors
///
/// Backend errors.
pub async fn report<S: KvStore>(
    store: &S,
    incarnation: &[u8; 16],
) -> Result<Option<RestoreReport>> {
    transact(store, *incarnation, |tx, inc| {
        Box::pin(async move { read(tx, &keys::restore_report(inc)).await })
    })
    .await
}

/// Replace a report (written at the end of every page so that a crash
/// loses at most one page of counts; the cursor says where to resume).
///
/// # Errors
///
/// Backend errors.
pub async fn set_report<S: KvStore>(store: &S, r: &RestoreReport) -> Result<()> {
    transact(store, r, |tx, r| {
        Box::pin(async move { write(tx, &keys::restore_report(&r.incarnation), *r) })
    })
    .await
}

/// Every report, oldest first.
///
/// # Errors
///
/// Backend errors.
pub async fn reports<S: KvStore>(store: &S) -> Result<Vec<RestoreReport>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::restore_reports_range(), usize::MAX, false)
                .await?
            {
                out.push(decode::<RestoreReport>(&v)?);
            }
            out.sort_by_key(|r| r.restore_point_ms);
            Ok(out)
        })
    })
    .await
}

/// Keep a finalized upgrade's cursor as an audit record.
///
/// # Errors
///
/// Backend errors.
pub async fn append_upgrade_audit<S: KvStore>(
    store: &S,
    cursor: &crate::records::UpgradeCursor,
    finished_ms: u64,
) -> Result<()> {
    transact(store, (cursor, finished_ms), |tx, env| {
        Box::pin(async move {
            let (c, ms) = env;
            write(tx, &keys::upgrade_audit(*ms), *c)
        })
    })
    .await
}

/// Every upgrade audit record, oldest first.
///
/// # Errors
///
/// Backend errors.
pub async fn upgrade_audits<S: KvStore>(store: &S) -> Result<Vec<crate::records::UpgradeCursor>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::upgrade_audits_range(), usize::MAX, false)
                .await?
            {
                out.push(decode::<crate::records::UpgradeCursor>(&v)?);
            }
            Ok(out)
        })
    })
    .await
}

// ---- finalize (RFC-015 §5.3, Phase 3.4 §3) --------------------------------

/// An accepted, unrecoverable chunk of one object version
/// (`("al","meta","loss",generation,chunk_index)`). Written by a forced
/// finalize for every unreadable object the report lists; never removed
/// by finalize, GC or repair, so the loss stays visible after the cluster
/// is Active again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataLossRecord {
    /// Incarnation whose verification found it.
    pub incarnation: [u8; 16],
    /// The object as the report named it.
    pub object: AffectedObject,
    /// When the loss was accepted.
    pub accepted_ms: u64,
    /// Who accepted it.
    pub accepted_by: String,
}

impl Record for DataLossRecord {}

/// One `admin recovery finalize` attempt
/// (`("al","meta","restore","finalize",incarnation,at_ms)`): accepted or
/// refused, with the findings as they were.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizeAudit {
    /// Target incarnation.
    pub incarnation: [u8; 16],
    /// When.
    pub at_ms: u64,
    /// Operator.
    pub operator: String,
    /// Operator's reason (required with `--force`).
    pub reason: String,
    /// Whether `--force` was given.
    pub force: bool,
    /// Phase before the attempt (`ClusterPhase::name`).
    pub phase_before: String,
    /// `activated`, `activated-forced` or `refused: <why>`.
    pub result: String,
    /// Unresolved findings that need `--force` (critical).
    pub critical: Vec<String>,
    /// Findings reported but not blocking (repair continues once Active).
    pub warnings: Vec<String>,
    /// Counts of the report at the time.
    pub counts: Option<RestoreCounts>,
    /// Objects the report listed; `affected_total` > this when the list
    /// was truncated (the integrity task list then holds every chunk).
    pub affected_listed: u32,
    /// Unreadable chunks counted by verification.
    pub affected_total: u64,
}

impl Record for FinalizeAudit {}

/// What finalize was asked to do.
#[derive(Debug, Clone)]
pub struct FinalizeRequest {
    /// The incarnation the operator looked at; refused if it changed.
    pub incarnation: [u8; 16],
    /// Operator name (audit).
    pub operator: String,
    /// Reason (audit; required with `force`).
    pub reason: String,
    /// Accept critical findings.
    pub force: bool,
    /// Now, Unix milliseconds.
    pub now_ms: u64,
}

/// Outcome of the evaluation of a finalize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeVerdict {
    /// Refusal, if any (`None` = may activate).
    pub refusal: Option<String>,
    /// Findings that need `--force`.
    pub critical: Vec<String>,
    /// Findings that do not block.
    pub warnings: Vec<String>,
    /// Whether the activation depends on `--force`.
    pub forced: bool,
}

/// Decide a finalize from the state it would act on. Refused, whatever
/// `force` says, when there is no report for the current incarnation,
/// verification has not finished (cursor not `Done`, report not
/// finished), a node's listing is missing and was not excluded, or the
/// phase is not `ReadOnly`/`RecoveryRequired`. Critical findings
/// (unreadable chunks, dangling or broken multipart manifests, placement
/// epochs ahead of their group) refuse a finalize without `force`; with
/// `force` they need an operator and a reason, and they stay recorded.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn evaluate_finalize(
    state: &crate::records::ClusterStateRecord,
    cursor: Option<&RestoreCursor>,
    report: Option<&RestoreReport>,
    req: &FinalizeRequest,
) -> FinalizeVerdict {
    use crate::records::ClusterPhase as P;
    let mut v = FinalizeVerdict {
        refusal: None,
        critical: Vec::new(),
        warnings: Vec::new(),
        forced: false,
    };
    let refuse = |mut v: FinalizeVerdict, why: String| {
        v.refusal = Some(why);
        v
    };
    if state.incarnation != req.incarnation {
        return refuse(
            v,
            "the cluster incarnation changed since the request".into(),
        );
    }
    if req.operator.trim().is_empty() {
        return refuse(v, "an operator name is required".into());
    }
    if let Some(r) = report {
        let c = &r.counts;
        for (n, what) in [
            (c.unreadable_chunks, "unreadable chunks"),
            (c.manifest_findings, "multipart manifest findings"),
            (
                c.placement_epoch_mismatch,
                "placement epochs ahead of their group",
            ),
        ] {
            if n > 0 {
                v.critical.push(format!("{n} {what}"));
            }
        }
        let dangling = r
            .affected
            .iter()
            .filter(|a| a.why == "dangling-part")
            .count();
        if dangling > 0 && c.manifest_findings == 0 {
            v.critical
                .push(format!("{dangling} dangling multipart parts"));
        }
        for (n, what) in [
            (c.missing, "missing shards (repair queued)"),
            (c.corrupt, "corrupt shards (repair queued)"),
            (c.degraded_chunks, "degraded chunks (repair queued)"),
            (c.cache_mismatch, "cache copy mismatches"),
            (c.membership_mismatch, "membership mismatches"),
            (
                c.orphan_post_t,
                "post-restore orphans (reclaimed once Active)",
            ),
        ] {
            if n > 0 {
                v.warnings.push(format!("{n} {what}"));
            }
        }
    }
    match state.state {
        P::ReadOnly | P::RecoveryRequired => {}
        other => {
            return refuse(
                v,
                format!(
                    "cluster is {}; finalize needs ReadOnly or RecoveryRequired",
                    other.name()
                ),
            );
        }
    }
    let Some(r) = report else {
        return refuse(
            v,
            "no verification report for this incarnation (run `recovery verify`)".into(),
        );
    };
    if r.incarnation != state.incarnation {
        return refuse(v, "the report belongs to another incarnation".into());
    }
    let done = cursor
        .is_some_and(|c| c.incarnation == state.incarnation && c.phase == RecoveryPhase::Done);
    if !done || r.finished_ms == 0 {
        return refuse(
            v,
            "verification has not finished (not even --force finalizes an unverified cluster)"
                .into(),
        );
    }
    if !r.complete || !r.unanswered_nodes.is_empty() {
        return refuse(
            v,
            format!(
                "verification is incomplete: nodes {:?} did not answer (bring them back, or replace their group memberships, then `recovery verify`)",
                r.unanswered_nodes
            ),
        );
    }
    if !v.critical.is_empty() {
        if !req.force {
            let why = format!(
                "unresolved findings: {} (accept the loss with --force --reason)",
                v.critical.join(", ")
            );
            return refuse(v, why);
        }
        if req.reason.trim().is_empty() {
            return refuse(v, "--force needs --reason".into());
        }
        v.forced = true;
    }
    v
}

/// Finalize in one transaction: re-read the state, the cursor and the
/// report, evaluate, and either refuse or record the acceptance, a
/// [`DataLossRecord`] per unreadable object, and set the cluster
/// `Active`. Every attempt writes a [`FinalizeAudit`]. A concurrent
/// metadata change (a resumed verification, another finalize) conflicts
/// and the evaluation is repeated on the new state.
///
/// # Errors
///
/// Backend errors; a missing cluster state record.
///
/// # Panics
///
/// Never: an accepted verdict implies the report was read.
pub async fn finalize<S: KvStore>(
    store: &S,
    req: &FinalizeRequest,
) -> Result<(FinalizeVerdict, FinalizeAudit)> {
    // One id per attempt, kept across transaction retries.
    let attempt = crate::records::new_gen_id();
    transact(store, (req, attempt), |tx, env| {
        Box::pin(async move {
            let (req, attempt) = *env;
            let mut state: crate::records::ClusterStateRecord =
                read(tx, &keys::cluster_state()).await?.ok_or_else(|| {
                    crate::error::MetaError::InvalidArgument("cluster state record missing".into())
                })?;
            let cursor: Option<RestoreCursor> = read(tx, &keys::restore_cursor()).await?;
            let report: Option<RestoreReport> =
                read(tx, &keys::restore_report(&state.incarnation)).await?;
            let verdict = evaluate_finalize(&state, cursor.as_ref(), report.as_ref(), req);
            let audit = FinalizeAudit {
                incarnation: req.incarnation,
                at_ms: req.now_ms,
                operator: req.operator.clone(),
                reason: req.reason.clone(),
                force: req.force,
                phase_before: state.state.name().to_string(),
                result: match (&verdict.refusal, verdict.forced) {
                    (Some(w), _) => format!("refused: {w}"),
                    (None, true) => "activated-forced".into(),
                    (None, false) => "activated".into(),
                },
                critical: verdict.critical.clone(),
                warnings: verdict.warnings.clone(),
                counts: report.as_ref().map(|r| r.counts.clone()),
                affected_listed: report
                    .as_ref()
                    .map_or(0, |r| u32::try_from(r.affected.len()).unwrap_or(u32::MAX)),
                affected_total: report.as_ref().map_or(0, |r| r.counts.unreadable_chunks),
            };
            write(
                tx,
                &keys::restore_finalize_audit(&req.incarnation, req.now_ms, &attempt),
                &audit,
            )?;
            if verdict.refusal.is_some() {
                return Ok((verdict, audit));
            }
            let mut report = report.expect("evaluated");
            report.accepted = Some(Acceptance {
                at_ms: req.now_ms,
                by: req.operator.clone(),
                forced: verdict.forced,
            });
            for a in &report.affected {
                let rec = DataLossRecord {
                    incarnation: state.incarnation,
                    object: a.clone(),
                    accepted_ms: req.now_ms,
                    accepted_by: req.operator.clone(),
                };
                write(tx, &keys::data_loss(&a.generation, a.chunk_index), &rec)?;
            }
            write(tx, &keys::restore_report(&state.incarnation), &report)?;
            state.state = crate::records::ClusterPhase::Active;
            state.changed_ms = req.now_ms;
            state.changed_by.clone_from(&req.operator);
            write(tx, &keys::cluster_state(), &state)?;
            // Finalizing declares the restored metadata authoritative: the
            // operator hold on the delete fence (set by `recovery begin`)
            // goes with it; the leader opens a new generation (ADR-0023).
            let fence: Option<crate::ops::delete_fence::DeleteFenceRecord> =
                read(tx, &keys::delete_fence()).await?;
            if let Some(mut f) = fence.filter(|f| f.operator_hold) {
                f.operator_hold = false;
                f.changed_ms = req.now_ms;
                f.reason = format!("hold released by recovery finalize ({})", req.operator);
                write(tx, &keys::delete_fence(), &f)?;
            }
            Ok((verdict, audit))
        })
    })
    .await
}

/// Every finalize audit record, oldest first.
///
/// # Errors
///
/// Backend errors.
pub async fn finalize_audits<S: KvStore>(store: &S) -> Result<Vec<FinalizeAudit>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::restore_finalize_audits_range(), usize::MAX, false)
                .await?
            {
                out.push(decode::<FinalizeAudit>(&v)?);
            }
            out.sort_by_key(|a| a.at_ms);
            Ok(out)
        })
    })
    .await
}

/// Every accepted data loss.
///
/// # Errors
///
/// Backend errors.
pub async fn data_losses<S: KvStore>(store: &S) -> Result<Vec<DataLossRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::data_losses_range(), usize::MAX, false)
                .await?
            {
                out.push(decode::<DataLossRecord>(&v)?);
            }
            Ok(out)
        })
    })
    .await
}

/// `ReadOnly` → `Active` for an operator's read-only switch
/// (`admin cluster activate`). Refused while the current incarnation has
/// a restore report that was not accepted: leaving a restore goes through
/// [`finalize`] (and its audit), never around it. Returns the phase after
/// and whether it changed (`Ok(Err(why))` = refused).
///
/// # Errors
///
/// Backend errors; a missing cluster state record.
pub async fn activate_read_only<S: KvStore>(
    store: &S,
    who: &str,
    now_ms: u64,
) -> Result<std::result::Result<crate::records::ClusterPhase, String>> {
    transact(store, (who.to_string(), now_ms), |tx, env| {
        Box::pin(async move {
            let (who, now_ms) = env;
            let mut state: crate::records::ClusterStateRecord = read(tx, &keys::cluster_state())
                .await?
                .ok_or_else(|| crate::error::MetaError::InvalidArgument("cluster state record missing".into()))?;
            if state.state != crate::records::ClusterPhase::ReadOnly {
                return Ok(Err(format!("cluster is {}, not ReadOnly", state.state.name())));
            }
            let report: Option<RestoreReport> = read(tx, &keys::restore_report(&state.incarnation)).await?;
            if report.is_some_and(|r| r.accepted.is_none()) {
                return Ok(Err(
                    "this incarnation has a restore report that was not accepted; use `admin recovery finalize`".into(),
                ));
            }
            state.state = crate::records::ClusterPhase::Active;
            state.changed_ms = *now_ms;
            state.changed_by.clone_from(who);
            write(tx, &keys::cluster_state(), &state)?;
            Ok(Ok(state.state))
        })
    })
    .await
}

/// `recovery verify` (`from_start`: ReadOnly or RecoveryRequired, fresh
/// cursor and report) or `recovery resume` (RecoveryRequired, cursor
/// kept) → `VerificationPending`, in one transaction conditional on the
/// incarnation and the phase: a finalize that committed meanwhile is never
/// undone (its accepted report overwritten, an Active cluster sent back to
/// verification). `Ok(Err(why))` = refused.
///
/// # Errors
///
/// Backend errors; a missing cluster state record.
pub async fn restart_verification<S: KvStore>(
    store: &S,
    incarnation: [u8; 16],
    from_start: bool,
    who: &str,
    now_ms: u64,
) -> Result<std::result::Result<(), String>> {
    use crate::records::ClusterPhase as P;
    transact(store, (who.to_string(), now_ms), |tx, env| {
        Box::pin(async move {
            let (who, now_ms) = env;
            let mut state: crate::records::ClusterStateRecord =
                read(tx, &keys::cluster_state()).await?.ok_or_else(|| {
                    crate::error::MetaError::InvalidArgument("cluster state record missing".into())
                })?;
            if state.incarnation != incarnation {
                return Ok(Err(
                    "the cluster incarnation changed since the request".into()
                ));
            }
            let allowed = if from_start {
                matches!(state.state, P::ReadOnly | P::RecoveryRequired)
            } else {
                state.state == P::RecoveryRequired
            };
            if !allowed {
                return Ok(Err(format!(
                    "cluster is {}; {} needs {}",
                    state.state.name(),
                    if from_start { "verify" } else { "resume" },
                    if from_start {
                        "ReadOnly or RecoveryRequired"
                    } else {
                        "RecoveryRequired"
                    }
                )));
            }
            if from_start {
                let point = state.restore_point_ms().unwrap_or(*now_ms);
                let c = RestoreCursor {
                    incarnation,
                    phase: RecoveryPhase::Shards {
                        group: 0,
                        member: 0,
                        after: None,
                    },
                    restore_point_ms: point,
                    started_ms: *now_ms,
                    updated_ms: *now_ms,
                    unanswered_nodes: Vec::new(),
                    excluded_nodes: Vec::new(),
                };
                let r = RestoreReport {
                    incarnation,
                    restore_point_ms: point,
                    counts: RestoreCounts::default(),
                    affected: Vec::new(),
                    complete: false,
                    unanswered_nodes: Vec::new(),
                    finished_ms: 0,
                    accepted: None,
                };
                write(tx, &keys::restore_cursor(), &c)?;
                write(tx, &keys::restore_report(&incarnation), &r)?;
            }
            state.state = P::VerificationPending;
            state.changed_ms = *now_ms;
            state.changed_by.clone_from(who);
            write(tx, &keys::cluster_state(), &state)?;
            Ok(Ok(()))
        })
    })
    .await
}
