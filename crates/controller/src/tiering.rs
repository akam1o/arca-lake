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
use arca_lake_metadata::ops::{cluster, object};
use arca_lake_metadata::records::{GenState, GroupRecord, GroupState, NodeRecord};
use arca_lake_protocol::client::NodeClients;
use arca_lake_tiering::durable::Coders;
use arca_lake_tiering::migrate::{MigrateOutcome, migrate_chunk};

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
#[allow(clippy::too_many_lines)]
pub async fn run_once<S: KvStore>(
    meta: &S,
    clients: &NodeClients,
    policy: &TieringPolicy,
    now_ms: u64,
) -> Result<TieringReport, ControlError> {
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
    let mut report = TieringReport::default();

    let mut after = None;
    loop {
        let page = object::list_generations(meta, after, 256).await?;
        let Some((last, _)) = page.last() else { break };
        after = Some(*last);
        for (generation, header) in &page {
            if header.state != GenState::Committed {
                continue;
            }
            report.examined += 1;
            let access = object::access_of(meta, generation)
                .await?
                .unwrap_or_default();
            let Some(manifest) = object::read_manifest_of(meta, generation).await? else {
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
            let recent = now_ms.saturating_sub(access.last_ms) <= ms(policy.promote_window);
            let target = if access.count >= policy.promote_reads
                && recent
                && current + 1 < policy.tiers.len()
            {
                Some((current + 1, true))
            } else if now_ms.saturating_sub(access.last_ms) > ms(policy.demote_after) && current > 0
            {
                Some((current - 1, false))
            } else {
                None
            };
            let Some((target_index, promote)) = target else {
                continue;
            };
            let Some(target_group) = groups.values().find(|g| {
                g.tier == policy.tiers[target_index]
                    && g.state == GroupState::Active
                    && !g.is_cache()
            }) else {
                continue;
            };
            for (index, record) in &manifest {
                let Some(source) = groups.get(&record.placement.group_id) else {
                    continue;
                };
                match migrate_chunk(
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
                {
                    Ok(MigrateOutcome::Switched { .. }) => {
                        if promote {
                            report.promoted_chunks += 1;
                        } else {
                            report.demoted_chunks += 1;
                        }
                    }
                    Ok(_) => report.skipped_chunks += 1,
                    Err(e) => {
                        tracing::warn!(error = %e, index, "chunk migration failed");
                        report.skipped_chunks += 1;
                    }
                }
            }
            object::reset_access(meta, generation, now_ms).await?;
        }
        if page.len() < 256 {
            break;
        }
    }
    Ok(report)
}
