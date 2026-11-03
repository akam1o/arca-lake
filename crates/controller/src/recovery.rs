//! The cluster recovery state machine as driven by the controller leader
//! (RFC-015 §5.3–§5.4, ADR-0016): epoch catch-up while `Restoring`, then
//! the verification pass while `VerificationPending`, resumed from a
//! persisted cursor after a crash or failover, ending in `ReadOnly` or
//! `RecoveryRequired`. Nothing here deletes anything (I6, RFC-015 §5.6).

use std::collections::{HashMap, HashSet};

use arca_lake_common::{split_chunk_id, uuid_v7_unix_ms};
use arca_lake_metadata::ops::integrity::{self, Finding, ManifestFinding};
use arca_lake_metadata::ops::recovery::{
    AffectedObject, MAX_AFFECTED, RecoveryPhase, RestoreCounts, RestoreCursor, RestoreReport,
};
use arca_lake_metadata::ops::{cluster, compat, gc, multipart, recovery};
use arca_lake_metadata::records::{ClusterPhase, GenId, GroupRecord, NodeRecord};
use arca_lake_metadata::{KvStore, MetaError};
use arca_lake_protocol::client::{NodeClients, ShardRef, ShardStatus};
use tracing::{info, warn};

use crate::ControlError;
use crate::integrity::Cancel;
use crate::walk::GroupWalker;

/// Options of the recovery pass.
#[derive(Debug, Clone)]
pub struct RecoveryOptions {
    /// Shards per node RPC.
    pub page: u32,
    /// Payload bytes per `ScrubShards` page.
    pub page_bytes: u64,
    /// Pages examined per pass before yielding to the scheduler.
    pub pages_per_pass: usize,
}

impl Default for RecoveryOptions {
    fn default() -> Self {
        Self {
            page: 256,
            page_bytes: 32 << 20,
            pages_per_pass: 64,
        }
    }
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryPassReport {
    /// Phase before the pass.
    pub phase_before: Option<ClusterPhase>,
    /// Phase after.
    pub phase_after: Option<ClusterPhase>,
    /// Groups whose epoch was raised this pass.
    pub epochs_raised: usize,
    /// Pages examined this pass.
    pages: usize,
    /// Whether the verification reached `Done` this pass.
    pub finished: bool,
}

fn hex(b: &[u8]) -> String {
    compat::hex(b)
}

/// One pass of the recovery state machine. Safe to call in any state:
/// outside `Restoring` and `VerificationPending` it does nothing.
///
/// # Errors
///
/// Metadata or node errors; the cursor makes the next pass resume.
#[allow(clippy::too_many_lines)]
pub async fn recovery_pass<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    opts: &RecoveryOptions,
    cancel: Cancel<'_>,
    who: &str,
    now_ms: u64,
) -> Result<RecoveryPassReport, ControlError> {
    let mut report = RecoveryPassReport::default();
    let Some(state) = compat::cluster_state(meta).await? else {
        return Ok(report);
    };
    report.phase_before = Some(state.state);
    report.phase_after = Some(state.state);
    if !matches!(
        state.state,
        ClusterPhase::Restoring | ClusterPhase::VerificationPending
    ) {
        return Ok(report);
    }
    // The cursor of this incarnation, or a fresh one.
    let mut cursor = match recovery::cursor(meta).await? {
        Some(c) if c.incarnation == state.incarnation => c,
        _ => {
            let phase = if state.state == ClusterPhase::Restoring {
                RecoveryPhase::CatchUp { after_group: None }
            } else {
                RecoveryPhase::Shards {
                    group: 0,
                    member: 0,
                    after: None,
                }
            };
            let c = RestoreCursor {
                incarnation: state.incarnation,
                phase,
                restore_point_ms: state.restored_at_ms.unwrap_or(now_ms),
                started_ms: now_ms,
                updated_ms: now_ms,
                unanswered_nodes: Vec::new(),
                excluded_nodes: Vec::new(),
            };
            recovery::set_cursor(meta, &c).await?;
            c
        }
    };
    let mut rep = match recovery::report(meta, &state.incarnation).await? {
        Some(r) if r.finished_ms == 0 => r,
        Some(r) if r.accepted.is_none() && cursor.phase != RecoveryPhase::Done => r,
        Some(r) => r,
        None => RestoreReport {
            incarnation: state.incarnation,
            restore_point_ms: cursor.restore_point_ms,
            counts: RestoreCounts::default(),
            affected: Vec::new(),
            complete: false,
            unanswered_nodes: Vec::new(),
            finished_ms: 0,
            accepted: None,
        },
    };
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let mut groups = cluster::list_groups(meta).await?;
    groups.sort_by_key(|g| g.id);

    // ---- Restoring: epoch catch-up (§5.4).
    if state.state == ClusterPhase::Restoring {
        if let RecoveryPhase::CatchUp { after_group } = cursor.phase.clone() {
            for g in groups
                .iter()
                .filter(|g| after_group.is_none_or(|a| g.id > a))
            {
                if crate::integrity::is_cancelled(cancel) {
                    break;
                }
                let raised =
                    catch_up_group(meta, clients, &nodes, g, &mut cursor, &mut rep).await?;
                if raised {
                    report.epochs_raised += 1;
                    rep.counts.epochs_raised += 1;
                }
                cursor.phase = RecoveryPhase::CatchUp {
                    after_group: Some(g.id),
                };
                cursor.updated_ms = now_ms;
                recovery::set_cursor(meta, &cursor).await?;
                recovery::set_report(meta, &rep).await?;
            }
            if crate::integrity::is_cancelled(cancel) {
                return Ok(report);
            }
            // Membership mismatch: a node enforcing a group metadata lacks.
            let known: HashSet<u64> = groups.iter().map(|g| g.id).collect();
            for cap in recovery::node_caps(meta).await? {
                for (gid, _) in &cap.group_epochs {
                    if !known.contains(gid) {
                        rep.counts.membership_mismatch += 1;
                    }
                }
            }
            cursor.phase = RecoveryPhase::Shards {
                group: 0,
                member: 0,
                after: None,
            };
            cursor.updated_ms = now_ms;
            recovery::set_cursor(meta, &cursor).await?;
            recovery::set_report(meta, &rep).await?;
            let (_, changed) = compat::transition(
                meta,
                ClusterPhase::Restoring,
                ClusterPhase::VerificationPending,
                who,
                now_ms,
            )
            .await?;
            if changed {
                info!(
                    epochs_raised = report.epochs_raised,
                    "epoch catch-up complete; verifying"
                );
                report.phase_after = Some(ClusterPhase::VerificationPending);
            }
        }
        return Ok(report);
    }

    // ---- VerificationPending: the verification pass, `pages_per_pass`
    // pages at a time.
    let durable: Vec<&GroupRecord> = groups.iter().filter(|g| !g.is_cache()).collect();
    let cache_groups: Vec<&GroupRecord> = groups.iter().filter(|g| g.is_cache()).collect();
    let mut pages = 0usize;
    while pages < opts.pages_per_pass && !crate::integrity::is_cancelled(cancel) {
        match cursor.phase.clone() {
            RecoveryPhase::CatchUp { .. } => {
                // A cursor left in catch-up by an older build: start verifying.
                cursor.phase = RecoveryPhase::Shards {
                    group: 0,
                    member: 0,
                    after: None,
                };
            }
            RecoveryPhase::Shards {
                group,
                member,
                after,
            } => {
                let Some(g) = durable.iter().find(|g| g.id >= group).copied() else {
                    cursor.phase = RecoveryPhase::Presence {
                        group: 0,
                        after: None,
                    };
                    continue;
                };
                let Some(m) = g.members.get(usize::from(member)) else {
                    cursor.phase = RecoveryPhase::Shards {
                        group: g.id + 1,
                        member: 0,
                        after: None,
                    };
                    continue;
                };
                let next_member = RecoveryPhase::Shards {
                    group: g.id,
                    member: member + 1,
                    after: None,
                };
                let Some(node) = nodes.get(&m.node_id) else {
                    note_unanswered(&mut cursor, m.node_id);
                    rep.counts.membership_mismatch += 1;
                    cursor.phase = next_member;
                    continue;
                };
                let after_ref = after.map(|c| ShardRef {
                    group_id: g.id,
                    chunk_id: c,
                    shard_index: u16::MAX,
                });
                let page = match clients
                    .scrub_shards_page(&node.address, g.id, after_ref, opts.page, opts.page_bytes)
                    .await
                {
                    Ok(p) => p,
                    Err(status) => {
                        warn!(group = g.id, member, code = ?status.code(), "verification: node unanswered");
                        note_unanswered(&mut cursor, m.node_id);
                        cursor.phase = next_member;
                        continue;
                    }
                };
                pages += 1;
                let mut last = after;
                for sh in &page.shards {
                    last = Some(sh.shard.chunk_id);
                    classify_shard(
                        meta,
                        g,
                        usize::from(member),
                        sh.shard,
                        sh.status,
                        &cursor,
                        &mut rep,
                    )
                    .await?;
                }
                cursor.phase = match page.next {
                    Some(_) => RecoveryPhase::Shards {
                        group: g.id,
                        member,
                        after: last,
                    },
                    None => next_member,
                };
            }
            RecoveryPhase::Presence { group, after } => {
                let Some(g) = groups.iter().find(|g| g.id >= group) else {
                    cursor.phase = RecoveryPhase::Manifests { after: None };
                    continue;
                };
                pages += 1;
                let done =
                    presence_page(meta, clients, &nodes, g, after, opts, &mut cursor, &mut rep)
                        .await?;
                cursor.phase = if let Some(last) = done {
                    RecoveryPhase::Presence {
                        group: g.id,
                        after: Some(last),
                    }
                } else {
                    // Former members: shards of this group on nodes that
                    // are not members (never deleted here).
                    former_member_shards(clients, &nodes, g, opts.page, &mut rep).await;
                    RecoveryPhase::Presence {
                        group: g.id + 1,
                        after: None,
                    }
                };
            }
            RecoveryPhase::Manifests { after } => {
                pages += 1;
                let r = integrity::verify_manifests(meta, after, 256).await?;
                for (root, f) in &r.findings {
                    rep.counts.manifest_findings += 1;
                    if let ManifestFinding::Dangling { part_number } = f {
                        let (bucket, key) = resolve_object(meta, root).await;
                        push_affected(
                            &mut rep,
                            AffectedObject {
                                bucket,
                                key,
                                generation: *root,
                                chunk_index: *part_number,
                                group: 0,
                                have: 0,
                                need: 0,
                                why: "dangling-part".into(),
                            },
                        );
                    }
                }
                cursor.phase = match r.next {
                    Some(n) => RecoveryPhase::Manifests { after: Some(n) },
                    None => RecoveryPhase::Done,
                };
            }
            RecoveryPhase::Done => break,
        }
        cursor.updated_ms = now_ms;
        recovery::set_cursor(meta, &cursor).await?;
        recovery::set_report(meta, &rep).await?;
    }
    let _ = &cache_groups;
    report.pages = pages;
    if cursor.phase == RecoveryPhase::Done && rep.finished_ms == 0 {
        rep.unanswered_nodes = cursor
            .unanswered_nodes
            .iter()
            .copied()
            .filter(|n| !cursor.excluded_nodes.contains(n))
            .collect();
        rep.complete = rep.unanswered_nodes.is_empty();
        rep.finished_ms = now_ms;
        recovery::set_report(meta, &rep).await?;
        let clean = rep.complete
            && rep.counts.unreadable_chunks == 0
            && !rep.affected.iter().any(|a| a.why == "dangling-part");
        let to = if clean {
            ClusterPhase::ReadOnly
        } else {
            ClusterPhase::RecoveryRequired
        };
        let (_, changed) =
            compat::transition(meta, ClusterPhase::VerificationPending, to, who, now_ms).await?;
        if changed {
            info!(
                phase = to.name(),
                counts = ?rep.counts,
                affected = rep.affected.len(),
                complete = rep.complete,
                "restore verification finished"
            );
            report.phase_after = Some(to);
            report.finished = true;
        }
    }
    Ok(report)
}

fn note_unanswered(cursor: &mut RestoreCursor, node_id: u64) {
    if !cursor.unanswered_nodes.contains(&node_id) {
        cursor.unanswered_nodes.push(node_id);
    }
}

fn push_affected(rep: &mut RestoreReport, a: AffectedObject) {
    if rep.affected.len() < MAX_AFFECTED
        && !rep
            .affected
            .iter()
            .any(|x| x.generation == a.generation && x.chunk_index == a.chunk_index)
    {
        rep.affected.push(a);
    }
}

/// Epoch catch-up of one group (RFC-015 §5.4): the new epoch is one above
/// the highest epoch any member enforces or metadata records, and every
/// reachable member is fenced to it. Returns whether the epoch was raised.
async fn catch_up_group<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    cursor: &mut RestoreCursor,
    rep: &mut RestoreReport,
) -> Result<bool, ControlError> {
    let mut max_member = 0u64;
    let mut addrs = Vec::new();
    for m in &group.members {
        let Some(n) = nodes.get(&m.node_id) else {
            note_unanswered(cursor, m.node_id);
            continue;
        };
        match clients.health(&n.address).await {
            Ok(h) => {
                let e = h
                    .group_epochs
                    .iter()
                    .find(|ge| ge.group_id == group.id)
                    .map_or(0, |ge| ge.epoch);
                if e > group.epoch {
                    rep.counts.membership_mismatch += 1;
                }
                max_member = max_member.max(e);
                addrs.push(n.address.clone());
            }
            Err(_) => note_unanswered(cursor, m.node_id),
        }
    }
    let new_epoch = group.epoch.max(max_member) + 1;
    let id = group.id;
    cluster::update_group(meta, id, move |g| {
        if g.epoch < new_epoch {
            g.epoch = new_epoch;
            true
        } else {
            false
        }
    })
    .await?;
    for addr in &addrs {
        if let Err(e) = clients.update_epoch(addr, id, new_epoch).await {
            warn!(group = id, error = %e, "epoch catch-up: fence failed");
            if let Some(n) = nodes.values().find(|n| &n.address == addr) {
                note_unanswered(cursor, n.id);
            }
        }
    }
    info!(
        group = id,
        from = group.epoch,
        to = new_epoch,
        "epoch caught up"
    );
    Ok(new_epoch > group.epoch)
}

/// Classify one shard a member reported (RFC-015 §5.3.2 orphan classes
/// and §8.4 corruption classes).
#[allow(clippy::too_many_arguments)]
async fn classify_shard<S: KvStore>(
    meta: &S,
    group: &GroupRecord,
    member: usize,
    shard: ShardRef,
    status: ShardStatus,
    cursor: &RestoreCursor,
    rep: &mut RestoreReport,
) -> Result<(), ControlError> {
    let (prefix, index) = split_chunk_id(&shard.chunk_id);
    if !status.is_good() {
        rep.counts.corrupt += 1;
        return Ok(());
    }
    if usize::from(shard.shard_index) != member {
        // A copy under a foreign index: an orphan by time.
        classify_orphan(&prefix, cursor.restore_point_ms, rep);
        return Ok(());
    }
    match gc::attribute_shard(meta, &prefix, index, group.id).await? {
        gc::ShardAttribution::Referenced => {
            rep.counts.matched += 1;
            if let Some(record) = gc::chunk_record_in_group(meta, &prefix, index, group.id).await?
                && record.placement.epoch > group.epoch
            {
                rep.counts.placement_epoch_mismatch += 1;
            }
        }
        gc::ShardAttribution::NoGeneration | gc::ShardAttribution::Unreferenced => {
            classify_orphan(&prefix, cursor.restore_point_ms, rep);
        }
    }
    Ok(())
}

fn classify_orphan(prefix: &[u8; 12], restore_point_ms: u64, rep: &mut RestoreReport) {
    if uuid_v7_unix_ms(prefix) > restore_point_ms {
        rep.counts.orphan_post_t += 1;
    } else {
        rep.counts.orphan_pre_t += 1;
    }
}

/// One page of the presence walk of a group: missing shards, degraded and
/// unreadable chunks (durable groups); copies without a placement (cache
/// groups). Returns the last chunk id when more pages follow.
#[allow(clippy::too_many_arguments)]
async fn presence_page<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    after: Option<[u8; 16]>,
    opts: &RecoveryOptions,
    cursor: &mut RestoreCursor,
    rep: &mut RestoreReport,
) -> Result<Option<[u8; 16]>, ControlError> {
    let mut walker = GroupWalker::new(clients, nodes, group, after, opts.page);
    let k = usize::from(group.data_shards);
    let n = group.members.len();
    let mut seen = 0u32;
    let mut last = None;
    while let Some(chunk) = walker.next().await? {
        last = Some(chunk.chunk_id);
        seen += 1;
        let (prefix, index) = split_chunk_id(&chunk.chunk_id);
        if group.is_cache() {
            if !gc::cache_placement_in_group(meta, &prefix, index, group.id).await? {
                rep.counts.cache_mismatch += 1;
            }
            if seen >= opts.page {
                break;
            }
            continue;
        }
        let Some(_record) = gc::chunk_record_in_group(meta, &prefix, index, group.id).await? else {
            continue;
        };
        let holders: Vec<usize> = chunk.holders().collect();
        let unreachable = walker.unavailable();
        let missing: Vec<u16> = (0..n)
            .filter(|i| !holders.contains(i) && !unreachable.contains(i))
            .map(|i| u16::try_from(i).unwrap_or(u16::MAX))
            .collect();
        rep.counts.missing += missing.len() as u64;
        if holders.len() + unreachable.len() < k {
            rep.counts.unreadable_chunks += 1;
            if let Some(g) = gc::live_generation_with_prefix(meta, &prefix).await? {
                let (bucket, key) = resolve_object(meta, &g).await;
                push_affected(
                    rep,
                    AffectedObject {
                        bucket,
                        key,
                        generation: g,
                        chunk_index: index,
                        group: group.id,
                        have: u16::try_from(holders.len()).unwrap_or(u16::MAX),
                        need: group.data_shards,
                        why: "unreadable".into(),
                    },
                );
            }
            let _ = integrity::enqueue(
                meta,
                group.id,
                chunk.chunk_id,
                Finding::UnrecoverableStripe,
                &missing,
                cursor.updated_ms,
            )
            .await?;
        } else if !missing.is_empty() {
            rep.counts.degraded_chunks += 1;
            let _ = integrity::enqueue(
                meta,
                group.id,
                chunk.chunk_id,
                Finding::MissingShard,
                &missing,
                cursor.updated_ms,
            )
            .await?;
        }
        if seen >= opts.page {
            break;
        }
    }
    for i in walker.unavailable() {
        if let Some(m) = group.members.get(i) {
            note_unanswered(cursor, m.node_id);
        }
    }
    Ok(if seen >= opts.page { last } else { None })
}

/// Count shards of `group` on nodes that are not its members.
async fn former_member_shards(
    clients: &NodeClients,
    nodes: &HashMap<u64, NodeRecord>,
    group: &GroupRecord,
    page: u32,
    rep: &mut RestoreReport,
) {
    let members: HashSet<u64> = group.members.iter().map(|m| m.node_id).collect();
    for n in nodes.values().filter(|n| !members.contains(&n.id)) {
        let mut after = None;
        loop {
            let Ok(p) = clients
                .list_shards_page_with(&n.address, group.id, after, page, true)
                .await
            else {
                break;
            };
            rep.counts.former_member += (p.entries.len() + p.quarantined.len()) as u64;
            match p.next {
                Some(x) => after = Some(x),
                None => break,
            }
        }
    }
}

/// Bucket name and key of a generation (empty strings when gone).
async fn resolve_object<S: KvStore>(meta: &S, generation: &GenId) -> (String, String) {
    let Ok(header) = arca_lake_metadata::ops::object::get_generation(meta, generation).await else {
        return (String::new(), String::new());
    };
    let bucket = match multipart::bucket_of(meta, header.bucket_id).await {
        Ok(b) => b.name,
        Err(MetaError::NoSuchBucket) => format!("#{}", header.bucket_id),
        Err(_) => String::new(),
    };
    (bucket, header.key)
}

/// Resolve a chunk (group, chunk id) to its object for operator listings
/// (`admin integrity status --objects`, R-54): bucket, key, generation,
/// chunk index, and for a part generation the upload it belongs to.
///
/// # Errors
///
/// Metadata errors.
pub async fn resolve_chunk<S: KvStore>(
    meta: &S,
    chunk_id: &[u8; 16],
) -> Result<Option<ResolvedChunk>, ControlError> {
    let (prefix, index) = split_chunk_id(chunk_id);
    let Some(g) = gc::live_generation_with_prefix(meta, &prefix).await? else {
        return Ok(None);
    };
    let header = arca_lake_metadata::ops::object::get_generation(meta, &g).await?;
    let bucket = match multipart::bucket_of(meta, header.bucket_id).await {
        Ok(b) => b.name,
        Err(_) => format!("#{}", header.bucket_id),
    };
    Ok(Some(ResolvedChunk {
        bucket,
        key: header.key.clone(),
        generation: g,
        chunk_index: index,
        kind: format!("{:?}", header.kind),
        version_hint: hex(&g),
    }))
}

/// A chunk resolved to its object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedChunk {
    /// Bucket name (or `#id` when the record is gone).
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Generation.
    pub generation: GenId,
    /// Chunk index within the generation.
    pub chunk_index: u32,
    /// Generation kind (object, part of an upload, multipart root).
    pub kind: String,
    /// Version id hint: the generation in hex (the version id of a
    /// versioned object).
    pub version_hint: String,
}
