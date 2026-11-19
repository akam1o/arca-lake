//! Automatic promotion and demotion between durable tiers (RFC-006 §5.8).
//!
//! Distinct from the read-through cache: this moves the **durable**
//! placement of a generation's chunks, through
//! [`arca_lake_tiering::migrate::migrate_chunk`] (copy, verify, switch,
//! retire). The policy is recency and frequency from the access records
//! the gateway writes; tiers are ordered by configuration (first = lowest).

use std::collections::HashMap;
use std::time::Duration;

use arca_lake_metadata::KvStore;
use arca_lake_metadata::ops::{bg_cursor, cluster, object};
use arca_lake_metadata::records::GenId;
use arca_lake_metadata::records::{GenState, GroupRecord, GroupState, NodeRecord};
use arca_lake_protocol::client::NodeClients;
use arca_lake_tiering::durable::Coders;
use arca_lake_tiering::migrate::{MigrateOutcome, migrate_chunk, migrate_chunk_for_tiering};

use crate::ControlError;

/// The policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieringPolicy {
    /// Durable tiers from lowest (coldest) to highest (hottest).
    pub tiers: Vec<String>,
    /// Promote a generation read at least this many times within
    /// `promote_window`.
    pub promote_reads: u64,
    /// Window for `promote_reads`, measured from the last read.
    pub promote_window: Duration,
    /// Demote a generation not read for this long.
    pub demote_after: Duration,
}

/// What a pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TieringReport {
    /// Committed generations examined.
    pub examined: usize,
    /// Chunks moved to a higher tier.
    pub promoted_chunks: usize,
    /// Chunks moved to a lower tier.
    pub demoted_chunks: usize,
    /// Chunks whose migration was skipped or failed (logged; retried later).
    pub skipped_chunks: usize,
    /// Generations whose records could not be read (logged, passed over;
    /// Phase 3.14).
    pub errors: usize,
    /// The last generation examined, and whether the pass reached the end
    /// (Phase 3.14 cursor).
    pub last: Option<GenId>,
    /// See [`Self::last`].
    pub reached_end: bool,
    /// The pass stopped because the tiering record changed under it
    /// (ADR-0035 §10.1: a move refused at its switch).
    pub policy_changed: bool,
}

/// The tiering record's terms a pass runs under (ADR-0035 §10.1, §10.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyFence {
    /// The record's generation: every move switches only while it holds.
    pub generation: u64,
    /// Demotion counts a generation as touched no earlier than this plus
    /// `margin_ms`.
    pub grace_from_ms: u64,
    /// Counts of records last read before this do not promote.
    pub promotion_from_ms: u64,
    /// Propagation, flush delay and clock skew
    /// ([`arca_lake_metadata::ops::tiering::grace_margin_ms`]).
    pub margin_ms: u64,
}

impl TieringPolicy {
    /// The policy a tiering record holds.
    #[must_use]
    pub fn from_record(r: &arca_lake_metadata::ops::tiering::TieringPolicyRecord) -> Self {
        Self {
            tiers: r.tiers.clone(),
            promote_reads: r.promote_reads,
            promote_window: Duration::from_millis(r.promote_window_ms),
            demote_after: Duration::from_millis(r.demote_after_ms),
        }
    }

    /// This policy as a tiering record holds it.
    #[must_use]
    pub fn to_record(&self) -> arca_lake_metadata::ops::tiering::TieringPolicyRecord {
        arca_lake_metadata::ops::tiering::TieringPolicyRecord {
            tiers: self.tiers.clone(),
            promote_reads: self.promote_reads,
            promote_window_ms: ms(self.promote_window),
            demote_after_ms: ms(self.demote_after),
        }
    }
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// One policy pass over every committed generation.
///
/// # Errors
///
/// Metadata errors. Migration errors of individual chunks are counted,
/// not returned.
pub async fn run_once<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    policy: &TieringPolicy,
    now_ms: u64,
) -> Result<TieringReport, ControlError> {
    let mut report = TieringReport::default();
    sweep(
        meta,
        clients,
        policy,
        now_ms,
        None,
        usize::MAX,
        None,
        &mut report,
    )
    .await?;
    Ok(report)
}

/// A pass over at most `budget` generations (0: every one) from the
/// persisted cursor (Phase 3.14, [`bg_cursor::TIERING`]): a large keyspace
/// is swept over several passes, a new leader resumes where the last pass
/// stopped, and a generation whose records cannot be read is passed over
/// (logged and counted) instead of stopping every pass at it.
///
/// # Errors
///
/// As [`run_once`]; `Fenced` for a former leader.
pub async fn run_once_bounded<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    policy: &TieringPolicy,
    now_ms: u64,
    budget: usize,
) -> Result<TieringReport, ControlError> {
    run_once_bounded_fenced(meta, clients, policy, now_ms, budget, None).await
}

/// [`run_once_bounded`] under a tiering record's terms (ADR-0035): moves
/// switch only while the record stays at `fence.generation` (a refused
/// switch ends the pass, nothing reset), demotion waits for the grace,
/// counts from before the promotion cutoff do not promote.
///
/// # Errors
///
/// As [`run_once_bounded`].
pub async fn run_once_bounded_fenced<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    policy: &TieringPolicy,
    now_ms: u64,
    budget: usize,
    fence: Option<PolicyFence>,
) -> Result<TieringReport, ControlError> {
    let mut cursor = bg_cursor::get(meta, bg_cursor::TIERING).await?;
    let after = GenId::try_from(cursor.after.as_slice()).ok();
    if after.is_none() {
        cursor.sweep_started_ms = now_ms;
    }
    let budget = if budget == 0 { usize::MAX } else { budget };
    // The position is kept even when the pass stops on an error (review):
    // the generations before the one it stopped at are not examined again.
    let mut report = TieringReport::default();
    let result = sweep(
        meta,
        clients,
        policy,
        now_ms,
        after,
        budget,
        fence,
        &mut report,
    )
    .await;
    if let Err(e) = &result
        && crate::process::is_fenced(e)
    {
        return result.map(|()| report);
    }
    if report.reached_end {
        cursor.after.clear();
        cursor.sweeps += 1;
        cursor.last_sweep_ms = now_ms;
    } else if let Some(last) = report.last {
        cursor.after = last.to_vec();
    }
    cursor.last_pass_ms = now_ms;
    cursor.last_pass_items = report.examined as u64;
    cursor.last_pass_errors = report.errors as u64;
    bg_cursor::put(meta, bg_cursor::TIERING, &cursor).await?;
    result.map(|()| report)
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn sweep<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    policy: &TieringPolicy,
    now_ms: u64,
    start: Option<GenId>,
    budget: usize,
    fence: Option<PolicyFence>,
    report: &mut TieringReport,
) -> Result<(), ControlError> {
    // Intents of migrations that crashed: their targets become reclaimable.
    object::expire_migrations(meta, now_ms).await?;
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let groups: HashMap<u64, GroupRecord> = cluster::list_groups(meta)
        .await?
        .into_iter()
        .map(|g| (g.id, g))
        .collect();
    let addr_of = |id: u64| nodes.get(&id).map(|n| n.address.clone());
    let coders = Coders::default();
    let tier_index = |tier: &str| policy.tiers.iter().position(|t| t == tier);

    let mut after = start;
    let mut seen = 0usize;
    loop {
        let page = object::list_generations(meta, after, 256).await?;

        let Some((last, _)) = page.last() else {
            report.reached_end = true;
            break;
        };
        after = Some(*last);
        for (generation, header) in &page {
            if seen >= budget {
                return Ok(());
            }
            seen += 1;
            // Where the cursor stays if this generation stops the pass.
            let before = report.last;
            report.last = Some(*generation);
            if header.state != GenState::Committed {
                continue;
            }
            report.examined += 1;
            let records = async {
                let access = object::access_of(meta, generation).await?;
                let manifest = object::read_manifest_of(meta, generation).await?;
                // A generation never read (or whose reads were all lost)
                // counts as touched when it was written, not at time 0
                // (ADR-0034 decision 5): it is not demoted before
                // `demote_after` from its write.
                let access = access.unwrap_or(object::AccessRecord {
                    last_ms: header.created_ms,
                    count: 0,
                });
                Ok::<_, arca_lake_metadata::MetaError>((access, manifest))
            }
            .await;
            let (access, manifest) = match records {
                Ok(r) => r,
                Err(e @ arca_lake_metadata::MetaError::Codec(_)) => {
                    tracing::warn!(error = %e, "tiering: a generation's records could not be read; passed over until the next sweep");
                    report.errors += 1;
                    continue;
                }
                Err(e) => {
                    report.last = before;
                    return Err(e.into());
                }
            };
            let Some(manifest) = manifest else {
                continue;
            };
            let Some((_, first)) = manifest.first() else {
                continue;
            };
            let Some(current_group) = groups.get(&first.placement.group_id) else {
                continue;
            };
            let Some(current) = tier_index(&current_group.tier) else {
                continue;
            };
            // ADR-0035 §10.4: counts from before the promotion cutoff do not
            // promote; demotion counts a generation as touched no earlier
            // than the grace start plus the margin.
            let (count, touched) = match fence {
                Some(f) => (
                    if access.last_ms < f.promotion_from_ms {
                        0
                    } else {
                        access.count
                    },
                    access
                        .last_ms
                        .max(f.grace_from_ms.saturating_add(f.margin_ms)),
                ),
                None => (access.count, access.last_ms),
            };
            let recent = now_ms.saturating_sub(access.last_ms) <= ms(policy.promote_window);
            let target =
                if count >= policy.promote_reads && recent && current + 1 < policy.tiers.len() {
                    Some((current + 1, true))
                } else if now_ms.saturating_sub(touched) > ms(policy.demote_after) && current > 0 {
                    Some((current - 1, false))
                } else {
                    None
                };
            let Some((target_index, promote)) = target else {
                continue;
            };
            let Some(target_group) = target_group(
                &groups,
                &policy.tiers[target_index],
                current_group,
                generation,
            ) else {
                continue;
            };
            let mut settled = false;
            for (index, record) in &manifest {
                let Some(source) = groups.get(&record.placement.group_id) else {
                    continue;
                };
                let moved = match fence {
                    Some(f) => {
                        migrate_chunk_for_tiering(
                            meta,
                            clients,
                            &coders,
                            &addr_of,
                            source,
                            target_group,
                            generation,
                            *index,
                            record,
                            f.generation,
                            async || {},
                        )
                        .await
                    }
                    None => {
                        migrate_chunk(
                            meta,
                            clients,
                            &coders,
                            &addr_of,
                            source,
                            target_group,
                            generation,
                            *index,
                            record,
                            None,
                        )
                        .await
                    }
                };
                match moved {
                    // ADR-0035 §10.1: the policy changed under the pass. It
                    // ends here and resets nothing (a reset would erase the
                    // reads recorded since).
                    Ok(MigrateOutcome::PolicyChanged) => {
                        tracing::info!("tiering: the tiering record changed, pass stopped");
                        report.skipped_chunks += 1;
                        report.policy_changed = true;
                        // Examined again under the new policy.
                        report.last = before;
                        return Ok(());
                    }
                    Ok(MigrateOutcome::Switched { .. }) => {
                        settled = true;
                        if promote {
                            report.promoted_chunks += 1;
                        } else {
                            report.demoted_chunks += 1;
                        }
                    }
                    Ok(_) => {
                        settled = true;
                        report.skipped_chunks += 1;
                    }
                    // The target is at its reserve (Phase 3.12 liveness
                    // audit): the pass stops, as rebalancing's does. Every
                    // further attempt would write shards it cannot finish,
                    // whose cleanup durable targets retain for their
                    // reclaim retention.
                    Err(arca_lake_tiering::TierError::Node { addr, status })
                        if status.code() == tonic::Code::ResourceExhausted =>
                    {
                        tracing::info!(%addr, "tiering: target at its reserve, pass stopped");
                        report.skipped_chunks += 1;
                        // Passed over (review: a target that stays full must
                        // not hold the rest of the sweep); tried again at
                        // the next sweep.
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, index, "chunk migration failed");
                        report.skipped_chunks += 1;
                    }
                }
            }
            // Only once a chunk was settled (moved, or found there or
            // changed; Phase 3.12 liveness audit): a generation whose every
            // move failed keeps its access record and is tried again on the
            // next pass.
            if settled
                && !object::reset_access_fenced(
                    meta,
                    generation,
                    now_ms,
                    fence.map(|f| f.generation),
                )
                .await?
            {
                // ADR-0035 §10.1: the policy changed after the last switch;
                // the reads recorded since are kept and the pass ends.
                tracing::info!("tiering: the tiering record changed, pass stopped before a reset");
                report.policy_changed = true;
                // Examined again under the new policy.
                report.last = before;
                return Ok(());
            }
        }
        if page.len() < 256 {
            report.reached_end = true;
            break;
        }
    }
    Ok(())
}

/// The group of `tier` a generation moves to: the Active durable groups of
/// the tier with the source group's scheme and shape (any of the tier when
/// none has it), in id order, chosen by the generation id. Taking the
/// first group of a hash map sent every move of a controller process to
/// one group, and the next leader to another (Phase 3.4 soak).
fn target_group<'a>(
    groups: &'a HashMap<u64, GroupRecord>,
    tier: &str,
    source: &GroupRecord,
    generation: &[u8; 16],
) -> Option<&'a GroupRecord> {
    let mut of_tier: Vec<&GroupRecord> = groups
        .values()
        .filter(|g| g.tier == tier && g.state == GroupState::Active && !g.is_cache())
        .collect();
    of_tier.sort_by_key(|g| g.id);
    let same: Vec<&GroupRecord> = of_tier
        .iter()
        .copied()
        .filter(|g| {
            g.scheme == source.scheme
                && g.data_shards == source.data_shards
                && g.parity_shards == source.parity_shards
        })
        .collect();
    let pool = if same.is_empty() { of_tier } else { same };
    if pool.is_empty() {
        return None;
    }
    let salt = u64::from_le_bytes(generation[8..].try_into().unwrap_or_default());
    let i = usize::try_from(salt % pool.len() as u64).unwrap_or(0);
    Some(pool[i])
}
