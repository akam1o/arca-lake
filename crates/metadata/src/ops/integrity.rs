//! Integrity state (RFC-008 §8): scrub cursors per group and stripe repair
//! tasks per `(group, chunk)`. Every recipe is one transaction and safe to
//! retry; the controller's fenced store serializes writers.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::{KeyRange, KvStore, Txn};
use crate::ops::{read, write};
use crate::records::{GenId, Record, decode};
use crate::{keys, transact};

/// Corruption classes of RFC-008 §8.4, in table order. The index is used
/// for bounded-cardinality counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Finding {
    /// A current member lacks a shard a live chunk record places there.
    MissingShard,
    /// A payload block CRC failed on the node.
    ChecksumMismatch,
    /// The record is shorter than its header claims.
    TruncatedShard,
    /// The record header did not validate.
    InvalidHeader,
    /// A valid shard that no live generation references.
    UnexpectedShard,
    /// The chunk record's placement epoch is ahead of the group, or the
    /// shard sits on a former member.
    StalePlacementEpoch,
    /// The shard verifies but disagrees with the chunk record (length or
    /// assembled checksum).
    MetadataDataMismatch,
    /// Fewer than `k` verifiable shards for a live chunk.
    UnrecoverableStripe,
}

impl Finding {
    /// Every class, in table order.
    pub const ALL: [Self; 8] = [
        Self::MissingShard,
        Self::ChecksumMismatch,
        Self::TruncatedShard,
        Self::InvalidHeader,
        Self::UnexpectedShard,
        Self::StalePlacementEpoch,
        Self::MetadataDataMismatch,
        Self::UnrecoverableStripe,
    ];

    /// Metric label.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::MissingShard => "missing_shard",
            Self::ChecksumMismatch => "checksum_mismatch",
            Self::TruncatedShard => "truncated_shard",
            Self::InvalidHeader => "invalid_header",
            Self::UnexpectedShard => "unexpected_shard",
            Self::StalePlacementEpoch => "stale_placement_epoch",
            Self::MetadataDataMismatch => "metadata_data_mismatch",
            Self::UnrecoverableStripe => "unrecoverable_stripe",
        }
    }

    /// Index into [`Finding::ALL`].
    #[must_use]
    pub fn index(self) -> usize {
        Self::ALL.iter().position(|f| *f == self).unwrap_or(0)
    }
}

/// Phase of a scrub pass over one group (RFC-008 §8.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScrubPhase {
    /// Deep-verifying the shards of `member`, after chunk `after`.
    Shards {
        /// Member index being scrubbed.
        member: u16,
        /// Last chunk id verified on that member.
        after: Option<[u8; 16]>,
    },
    /// Walking the merged member listings for presence, after `after`.
    Presence {
        /// Last chunk id checked.
        after: Option<[u8; 16]>,
    },
    /// The pass is complete.
    Done,
}

/// Scrub progress of one group (`("al","scrub",group_id)`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrubCursor {
    /// Pass number (increments when a pass completes).
    pub pass: u64,
    /// Where the current pass is.
    pub phase: ScrubPhase,
    /// When the current pass started, Unix milliseconds.
    pub started_ms: u64,
    /// When the last pass finished, Unix milliseconds (0 = never).
    pub finished_ms: u64,
    /// Shards verified in the current pass.
    pub verified: u64,
    /// Bytes read in the current pass.
    pub bytes: u64,
    /// Findings of the current pass by class ([`Finding::ALL`] order).
    pub findings: [u64; 8],
    /// Node or metadata errors in the current pass.
    pub errors: u64,
}

impl Record for ScrubCursor {}

impl ScrubCursor {
    /// A fresh pass starting now.
    #[must_use]
    pub fn start(pass: u64, finished_ms: u64, now_ms: u64) -> Self {
        Self {
            pass,
            phase: ScrubPhase::Shards {
                member: 0,
                after: None,
            },
            started_ms: now_ms,
            finished_ms,
            verified: 0,
            bytes: 0,
            findings: [0; 8],
            errors: 0,
        }
    }

    /// Whether the pass is finished.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.phase == ScrubPhase::Done
    }
}

/// State of a stripe repair task (RFC-008 §8.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairState {
    /// Created; not yet classified.
    Detected,
    /// Classified and planned: rebuild `targets` under `epoch`.
    Planned {
        /// Member indexes to rewrite.
        targets: Vec<u16>,
        /// Group epoch the plan was made under.
        epoch: u64,
    },
    /// Fewer than `k` verifiable shards (I15). Re-checked when due.
    Unrecoverable {
        /// Good shards found.
        have: u16,
        /// Shards needed.
        need: u16,
        /// When first recorded, Unix milliseconds.
        since_ms: u64,
    },
    /// Gave up after the retry budget; retried at the next scrub pass.
    Failed {
        /// Attempts made.
        attempts: u32,
    },
}

/// A stripe repair task (`("al","repair",group_id,chunk_id)`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairTask {
    /// Why it was created.
    pub reason: Finding,
    /// Member indexes the detector believes need a rewrite (a hint; the
    /// task re-classifies).
    pub hint_targets: Vec<u16>,
    /// State.
    pub state: RepairState,
    /// Attempts so far.
    pub attempts: u32,
    /// Not before this time, Unix milliseconds.
    pub next_attempt_ms: u64,
    /// Creation time.
    pub created_ms: u64,
    /// Last change.
    pub updated_ms: u64,
    /// Last error message, if any (no addresses or paths).
    pub last_error: Option<String>,
}

impl Record for RepairTask {}

/// Scrub cursor of a group, if any.
///
/// # Errors
///
/// Backend errors.
pub async fn scrub_cursor<S: KvStore>(store: &S, group_id: u64) -> Result<Option<ScrubCursor>> {
    transact(store, group_id, |tx, g| {
        Box::pin(async move { read(tx, &keys::scrub_cursor(*g)).await })
    })
    .await
}

/// Replace the scrub cursor of a group.
///
/// # Errors
///
/// Backend errors.
pub async fn set_scrub_cursor<S: KvStore>(
    store: &S,
    group_id: u64,
    cursor: &ScrubCursor,
) -> Result<()> {
    transact(store, (group_id, cursor), |tx, env| {
        Box::pin(async move {
            let (g, c) = *env;
            write(tx, &keys::scrub_cursor(g), c)
        })
    })
    .await
}

/// Every scrub cursor.
///
/// # Errors
///
/// Backend errors.
pub async fn scrub_cursors<S: KvStore>(store: &S) -> Result<Vec<(u64, ScrubCursor)>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (k, v) in tx
                .get_range(&keys::scrub_cursors_range(), usize::MAX, false)
                .await?
            {
                if let Some(g) = keys::scrub_group_from(&k) {
                    out.push((g, decode::<ScrubCursor>(&v)?));
                }
            }
            Ok(out)
        })
    })
    .await
}

/// Create a repair task for `(group, chunk)` unless one exists. Returns
/// whether a task was created (a second detection is a no-op, RFC-008
/// §8.5 stage 1).
///
/// # Errors
///
/// Backend errors.
pub async fn enqueue<S: KvStore>(
    store: &S,
    group_id: u64,
    chunk_id: [u8; 16],
    reason: Finding,
    hint_targets: &[u16],
    now_ms: u64,
) -> Result<bool> {
    transact(
        store,
        (group_id, chunk_id, reason, hint_targets, now_ms),
        |tx, env| {
            Box::pin(async move {
                let (group_id, chunk_id, reason, hint_targets, now_ms) = *env;
                let key = keys::repair_task(group_id, &chunk_id);
                if tx.get(&key).await?.is_some() {
                    return Ok(false);
                }
                write(
                    tx,
                    &key,
                    &RepairTask {
                        reason,
                        hint_targets: hint_targets.to_vec(),
                        state: RepairState::Detected,
                        attempts: 0,
                        next_attempt_ms: now_ms,
                        created_ms: now_ms,
                        updated_ms: now_ms,
                        last_error: None,
                    },
                )?;
                Ok(true)
            })
        },
    )
    .await
}

/// A repair task, if any.
///
/// # Errors
///
/// Backend errors.
pub async fn task<S: KvStore>(
    store: &S,
    group_id: u64,
    chunk_id: &[u8; 16],
) -> Result<Option<RepairTask>> {
    transact(store, (group_id, *chunk_id), |tx, env| {
        Box::pin(async move {
            let (g, c) = *env;
            read(tx, &keys::repair_task(g, &c)).await
        })
    })
    .await
}

/// Replace a task (the worker persists state changes with this).
///
/// # Errors
///
/// Backend errors.
pub async fn set_task<S: KvStore>(
    store: &S,
    group_id: u64,
    chunk_id: &[u8; 16],
    task: &RepairTask,
) -> Result<()> {
    transact(store, (group_id, *chunk_id, task), |tx, env| {
        Box::pin(async move {
            let (g, c, t) = *env;
            write(tx, &keys::repair_task(g, &c), t)
        })
    })
    .await
}

/// Remove a task (stage 7, Commit; or the stripe turned out healthy).
///
/// # Errors
///
/// Backend errors.
pub async fn remove_task<S: KvStore>(store: &S, group_id: u64, chunk_id: &[u8; 16]) -> Result<()> {
    transact(store, (group_id, *chunk_id), |tx, env| {
        Box::pin(async move {
            let (g, c) = *env;
            tx.clear(&keys::repair_task(g, &c));
            Ok(())
        })
    })
    .await
}

/// Every task, in key order, at most `limit`.
///
/// # Errors
///
/// Backend errors.
pub async fn tasks<S: KvStore>(
    store: &S,
    limit: usize,
) -> Result<Vec<(u64, [u8; 16], RepairTask)>> {
    transact(store, limit, |tx, limit| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (k, v) in tx
                .get_range(&keys::repair_tasks_range(), *limit, false)
                .await?
            {
                if let Some((g, c)) = keys::repair_task_from(&k) {
                    out.push((g, c, decode::<RepairTask>(&v)?));
                }
            }
            Ok(out)
        })
    })
    .await
}

/// Tasks of one group.
///
/// # Errors
///
/// Backend errors.
pub async fn tasks_of_group<S: KvStore>(
    store: &S,
    group_id: u64,
) -> Result<Vec<([u8; 16], RepairTask)>> {
    transact(store, group_id, |tx, g| {
        Box::pin(async move {
            let range = KeyRange::prefix(&foundationdb_tuple::pack(&("al", "repair", *g)));
            let mut out = Vec::new();
            for (k, v) in tx.get_range(&range, usize::MAX, false).await? {
                if let Some((_, c)) = keys::repair_task_from(&k) {
                    out.push((c, decode::<RepairTask>(&v)?));
                }
            }
            Ok(out)
        })
    })
    .await
}

/// Progress of the background manifest scrub (`("al","mscrub")`): one
/// page of roots per scrub pass, resumed across leaders (R-55). The DR
/// verification pass walks every root in one go instead (RFC-015 §5.3.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestCursor {
    /// Pass number.
    pub pass: u64,
    /// Last root examined in the current pass (`None` = at the start).
    pub after: Option<GenId>,
    /// Whether the current pass is complete.
    pub done: bool,
    /// When the current pass started.
    pub started_ms: u64,
    /// When the last pass finished (0 = never).
    pub finished_ms: u64,
    /// Roots examined in the current pass.
    pub roots: u64,
    /// Findings in the current pass.
    pub findings: u64,
}

impl Record for ManifestCursor {}

/// The manifest cursor, if any.
///
/// # Errors
///
/// Backend errors.
pub async fn manifest_cursor<S: KvStore>(store: &S) -> Result<Option<ManifestCursor>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move { read(tx, &keys::manifest_cursor()).await })
    })
    .await
}

/// Replace the manifest cursor.
///
/// # Errors
///
/// Backend errors.
pub async fn set_manifest_cursor<S: KvStore>(store: &S, c: &ManifestCursor) -> Result<()> {
    transact(store, c, |tx, c| {
        Box::pin(async move { write(tx, &keys::manifest_cursor(), *c) })
    })
    .await
}

/// Open tasks by finding class ([`Finding::ALL`] order): the per-stripe
/// view of what is unresolved now (R-56), as opposed to the cumulative
/// detection counters.
///
/// # Errors
///
/// Backend errors.
pub async fn open_by_class<S: KvStore>(store: &S) -> Result<[u64; 8]> {
    let mut out = [0u64; 8];
    for (_, _, t) in tasks(store, usize::MAX).await? {
        out[t.reason.index()] += 1;
    }
    Ok(out)
}

/// Queue summary for metrics: `(total, unrecoverable, failed)`.
///
/// # Errors
///
/// Backend errors.
pub async fn queue_summary<S: KvStore>(store: &S) -> Result<(u64, u64, u64)> {
    let all = tasks(store, usize::MAX).await?;
    let unrecoverable = all
        .iter()
        .filter(|(_, _, t)| matches!(t.state, RepairState::Unrecoverable { .. }))
        .count() as u64;
    let failed = all
        .iter()
        .filter(|(_, _, t)| matches!(t.state, RepairState::Failed { .. }))
        .count() as u64;
    Ok((all.len() as u64, unrecoverable, failed))
}

// ---- multipart manifest scrub (RFC-008 §8.6) --------------------------------

/// What a manifest check found wrong with one multipart root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestFinding {
    /// A part reference names a generation that does not exist or is not
    /// `Committed`: the byte range is unrecoverable (I15, I-MP1 violated).
    Dangling {
        /// Part number.
        part_number: u32,
    },
    /// The part generation disagrees with its reference (kind, upload,
    /// size, chunk count or checksum), or the chunk records do not cover
    /// the part's size.
    Mismatch {
        /// Part number.
        part_number: u32,
        /// What differs.
        what: String,
    },
    /// The references do not tile the root (offsets, sizes, count).
    Layout {
        /// What differs.
        what: String,
    },
}

/// Result of verifying multipart roots.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManifestReport {
    /// Committed multipart roots examined.
    pub roots: u64,
    /// Part references examined.
    pub parts: u64,
    /// Findings, with the root generation.
    pub findings: Vec<(GenId, ManifestFinding)>,
    /// Cursor for the next page (`None` when every root was examined).
    pub next: Option<GenId>,
}

/// Verify every committed multipart root after `after` (at most `limit`
/// generations scanned per transaction page; the walk continues until the
/// end or `max_roots` roots were examined): each part reference must name
/// a `Committed` generation of kind `Part` for the same upload and part
/// number whose header size, chunk count and checksum equal the
/// reference's and whose chunk records cover exactly its size; the
/// references must tile the root contiguously and their count must equal
/// the root's `part_count`.
///
/// # Errors
///
/// Backend errors.
#[allow(clippy::too_many_lines)]
pub async fn verify_manifests<S: KvStore>(
    store: &S,
    after: Option<GenId>,
    max_roots: usize,
) -> Result<ManifestReport> {
    use crate::records::{GenKind, GenState, GenerationHeader, PartRef};
    let mut report = ManifestReport::default();
    let mut cursor = after;
    loop {
        // One page of generation keys (headers, chunk records, part
        // references, ...): the headers are examined, the last generation
        // seen is the next cursor.
        let (page, last): (Vec<(GenId, GenerationHeader)>, Option<GenId>) =
            transact(store, cursor, |tx, cur| {
                Box::pin(async move {
                    let begin = match cur {
                        Some(g) => keys::gen_range(g).end,
                        None => keys::all_generations_range().begin,
                    };
                    let range = KeyRange::new(begin, keys::all_generations_range().end);
                    let mut out = Vec::new();
                    let mut last = None;
                    for (k, v) in tx.get_range(&range, 512, false).await? {
                        let Some(g) = keys::gen_from_gen_key(&k) else {
                            continue;
                        };
                        last = Some(g);
                        if k == keys::gen_header(&g) {
                            out.push((g, decode::<GenerationHeader>(&v)?));
                        }
                    }
                    Ok((out, last))
                })
            })
            .await?;
        let Some(last) = last else {
            report.next = None;
            return Ok(report);
        };
        for (root, header) in page {
            let GenKind::MultipartRoot {
                upload_id,
                part_count,
            } = &header.kind
            else {
                continue;
            };
            if header.state != GenState::Committed {
                continue;
            }
            report.roots += 1;
            let refs: Vec<PartRef> = transact(store, root, |tx, g| {
                Box::pin(async move {
                    let mut out = Vec::new();
                    for (_, v) in tx
                        .get_range(&keys::part_refs_range(g), usize::MAX, false)
                        .await?
                    {
                        out.push(decode::<PartRef>(&v)?);
                    }
                    Ok(out)
                })
            })
            .await?;
            if refs.len() != *part_count as usize {
                report.findings.push((
                    root,
                    ManifestFinding::Layout {
                        what: format!("{} references, part_count {part_count}", refs.len()),
                    },
                ));
            }
            let mut offset = 0u64;
            for r in &refs {
                report.parts += 1;
                if r.offset != offset {
                    report.findings.push((
                        root,
                        ManifestFinding::Layout {
                            what: format!(
                                "part {} at offset {}, expected {offset}",
                                r.part_number, r.offset
                            ),
                        },
                    ));
                }
                offset = r.offset.saturating_add(r.size);
                let part = transact(store, r.generation, |tx, g| {
                    Box::pin(async move {
                        let header: Option<GenerationHeader> =
                            read(tx, &keys::gen_header(g)).await?;
                        let mut covered = 0u64;
                        let mut chunks = 0u32;
                        for (_, v) in tx
                            .get_range(&keys::gen_chunks_range(g), usize::MAX, false)
                            .await?
                        {
                            let c = decode::<crate::records::ChunkRecord>(&v)?;
                            covered += u64::from(c.len);
                            chunks += 1;
                        }
                        Ok((header, covered, chunks))
                    })
                })
                .await?;
                let (Some(ph), covered, chunks) = part else {
                    report.findings.push((
                        root,
                        ManifestFinding::Dangling {
                            part_number: r.part_number,
                        },
                    ));
                    continue;
                };
                if ph.state != GenState::Committed {
                    report.findings.push((
                        root,
                        ManifestFinding::Dangling {
                            part_number: r.part_number,
                        },
                    ));
                    continue;
                }
                let mut what = Vec::new();
                match &ph.kind {
                    GenKind::Part {
                        upload_id: u,
                        part_number: n,
                    } => {
                        if u != upload_id {
                            what.push("upload id".to_string());
                        }
                        if *n != r.part_number {
                            what.push(format!("part number {n}"));
                        }
                    }
                    other => what.push(format!("kind {other:?}")),
                }
                if ph.size != r.size {
                    what.push(format!("size {} vs reference {}", ph.size, r.size));
                }
                if ph.chunk_count != r.chunk_count {
                    what.push(format!(
                        "chunk_count {} vs reference {}",
                        ph.chunk_count, r.chunk_count
                    ));
                }
                if ph.checksum != r.checksum {
                    what.push("checksum".to_string());
                }
                if chunks != ph.chunk_count {
                    what.push(format!("{chunks} chunk records, header {}", ph.chunk_count));
                }
                if covered != ph.size {
                    what.push(format!(
                        "chunk records cover {covered} bytes, size {}",
                        ph.size
                    ));
                }
                if !what.is_empty() {
                    report.findings.push((
                        root,
                        ManifestFinding::Mismatch {
                            part_number: r.part_number,
                            what: what.join("; "),
                        },
                    ));
                }
            }
            if offset != header.size {
                report.findings.push((
                    root,
                    ManifestFinding::Layout {
                        what: format!("references cover {offset} bytes, root size {}", header.size),
                    },
                ));
            }
            if usize::try_from(report.roots).unwrap_or(usize::MAX) >= max_roots {
                report.next = Some(root);
                return Ok(report);
            }
        }
        cursor = Some(last);
    }
}
