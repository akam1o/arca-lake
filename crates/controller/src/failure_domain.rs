//! A group's required failure domain as the controller applies it
//! (ADR-0038, R-119). One rule for every membership change (auto repair,
//! `replace-member`, drain, a repair's retarget, the dry-run planner, a
//! resumed epoch change): the label is the group's recorded one
//! ([`arca_lake_metadata::ops::group_domain`]), never the controller's
//! `--failure-domain` flag and never inferred from the nodes; a group
//! without a record moves no member; the incoming node and every other
//! member's node carry the label and the incoming value differs from all
//! of theirs. The membership commit checks it again in its transaction.
//!
//! [`audit`] reports what breaks it (groups with two members in one
//! domain, groups without a record, members whose node lacks the label).
//! It only reads: it never moves a member or deletes data.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use arca_lake_metadata::KvStore;
use arca_lake_metadata::keys::SCHEMA_V5;
use arca_lake_metadata::ops::group_domain::{self, GroupDomain, label_value};
use arca_lake_metadata::ops::{cluster, compat, node_lifecycle};
use arca_lake_metadata::records::{GroupMember, GroupRecord, GroupState, NodeRecord};

use crate::ControlError;

/// The label a member move of `g` must keep, or why no member of `g` may
/// move. A one-member group (a `cache-v1` group) has no other member to
/// differ from: its recorded label, else `flag`, only names the label a
/// candidate must carry.
///
/// # Errors
///
/// The refusal for a group of several members without a recorded domain.
pub fn label_for(
    domains: &BTreeMap<u64, GroupDomain>,
    g: &GroupRecord,
    flag: &str,
) -> Result<String, String> {
    match domains.get(&g.id) {
        Some(d) => Ok(d.label.clone()),
        None if g.members.len() <= 1 => Ok(flag.to_string()),
        None => Err(group_domain::unrecorded_reason(g.id)),
    }
}

/// Whether slot `index` of `g` may move to `replacement` under the rule
/// (a resumed epoch change, chosen by whichever release started it).
///
/// # Errors
///
/// Why it may not.
#[allow(clippy::implicit_hasher)]
pub fn check_move(
    domains: &BTreeMap<u64, GroupDomain>,
    nodes: &HashMap<u64, NodeRecord>,
    g: &GroupRecord,
    index: usize,
    replacement: GroupMember,
) -> Result<(), String> {
    if g.members.len() <= 1 {
        return Ok(());
    }
    let label = label_for(domains, g, "")?;
    let Some(incoming) = nodes.get(&replacement.node_id) else {
        return Err(format!(
            "failure domain of group {}: node {} has no node record",
            g.id, replacement.node_id
        ));
    };
    let others: Vec<(u64, Option<&NodeRecord>)> = g
        .members
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != index)
        .map(|(_, m)| (m.node_id, nodes.get(&m.node_id)))
        .collect();
    group_domain::check_replacement(&label, incoming, &others)
        .map_err(|v| format!("failure domain '{label}' of group {}: {v}", g.id))
}

/// One thing [`audit`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finding {
    /// A group of several members without a recorded domain.
    Unrecorded {
        /// Group.
        group: u64,
    },
    /// Members of a group on nodes with the same value of its label.
    SharedDomain {
        /// Group.
        group: u64,
        /// Label.
        label: String,
        /// Shared value.
        value: String,
        /// The nodes sharing it.
        nodes: Vec<u64>,
    },
    /// A member whose node lacks the label (or has no node record): its
    /// domain is unknown.
    MemberUnlabeled {
        /// Group.
        group: u64,
        /// Label.
        label: String,
        /// Node.
        node: u64,
        /// Whether the node record is gone.
        no_record: bool,
    },
}

impl Finding {
    /// The group it is about.
    #[must_use]
    pub fn group(&self) -> u64 {
        match self {
            Self::Unrecorded { group }
            | Self::SharedDomain { group, .. }
            | Self::MemberUnlabeled { group, .. } => *group,
        }
    }

    /// Whether the group's members break its recorded domain (as opposed
    /// to a group whose domain is not recorded).
    #[must_use]
    pub fn is_violation(&self) -> bool {
        !matches!(self, Self::Unrecorded { .. })
    }
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unrecorded { group } => write!(
                f,
                "group {group}: no recorded failure domain; no member is moved until `admin group set-failure-domain --group {group} --label <label>` sets it"
            ),
            Self::SharedDomain {
                group,
                label,
                value,
                nodes,
            } => write!(
                f,
                "group {group}: members on nodes {nodes:?} share '{label}={value}'; the group does not survive the loss of that domain (move one with `admin replace-member`)"
            ),
            Self::MemberUnlabeled {
                group,
                label,
                node,
                no_record,
            } => {
                if *no_record {
                    write!(
                        f,
                        "group {group}: member node {node} has no node record; its '{label}' domain is unknown"
                    )
                } else {
                    write!(
                        f,
                        "group {group}: member node {node} has no '{label}' label; its domain is unknown (re-register it with --label {label}=<value>)"
                    )
                }
            }
        }
    }
}

/// Every finding of `groups` against `nodes` and `domains`.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn audit_groups(
    groups: &[GroupRecord],
    nodes: &HashMap<u64, NodeRecord>,
    domains: &BTreeMap<u64, GroupDomain>,
) -> Vec<Finding> {
    let mut out = Vec::new();
    for g in groups.iter().filter(|g| g.members.len() > 1) {
        let Some(d) = domains.get(&g.id) else {
            out.push(Finding::Unrecorded { group: g.id });
            continue;
        };
        let mut by_value: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
        for m in &g.members {
            match nodes
                .get(&m.node_id)
                .map(|n| label_value(&n.labels, &d.label))
            {
                Some(Some(v)) => by_value.entry(v).or_default().push(m.node_id),
                other => out.push(Finding::MemberUnlabeled {
                    group: g.id,
                    label: d.label.clone(),
                    node: m.node_id,
                    no_record: other.is_none(),
                }),
            }
        }
        for (value, nodes) in by_value.into_iter().filter(|(_, n)| n.len() > 1) {
            out.push(Finding::SharedDomain {
                group: g.id,
                label: d.label.clone(),
                value: value.to_string(),
                nodes,
            });
        }
    }
    out
}

/// Groups of several members without a recorded domain that have a
/// member not `healthy` or are `Repairing`: the dangerous case of an
/// unrecorded group, whose failed member is not replaced (and whose repair
/// does not retarget) until an operator records its domain.
#[must_use]
pub fn unrecorded_at_risk(
    groups: &[GroupRecord],
    domains: &BTreeMap<u64, GroupDomain>,
    healthy: impl Fn(u64) -> bool,
) -> Vec<u64> {
    groups
        .iter()
        .filter(|g| g.members.len() > 1 && !domains.contains_key(&g.id))
        .filter(|g| {
            g.state == GroupState::Repairing || g.members.iter().any(|m| !healthy(m.node_id))
        })
        .map(|g| g.id)
        .collect()
}

/// How long a node may go unseen before [`audit`] counts it down (the
/// controller's default `--unhealthy-after-secs`).
pub const AUDIT_UNSEEN_MS: u64 = 60_000;

/// What [`audit`] read and found.
#[derive(Debug, Clone, Default)]
pub struct Audit {
    /// Findings, by group.
    pub findings: Vec<Finding>,
    /// Groups with a recorded domain whose label differs from the
    /// controller flag given to [`audit`]: `(group, recorded)`.
    pub flag_differs: Vec<(u64, String)>,
    /// The store's `schema_write`.
    pub schema_write: Option<u64>,
    /// [`unrecorded_at_risk`]: unrecorded groups with a member down or a
    /// repair under way.
    pub at_risk: Vec<u64>,
}

impl Audit {
    /// Whether no process that ignores the recorded domains (0.1.0) can
    /// run on the store: from metadata schema 5 (ADR-0038).
    #[must_use]
    pub fn enforced(&self) -> bool {
        self.schema_write.is_some_and(|s| s >= SCHEMA_V5)
    }

    /// Groups whose members break their recorded domain.
    #[must_use]
    pub fn violating_groups(&self) -> usize {
        self.findings
            .iter()
            .filter(|f| f.is_violation())
            .map(Finding::group)
            .collect::<BTreeSet<u64>>()
            .len()
    }

    /// Groups without a recorded domain.
    #[must_use]
    pub fn unrecorded_groups(&self) -> usize {
        self.findings.iter().filter(|f| !f.is_violation()).count()
    }

    /// The `admin status` line on [`Self::at_risk`].
    #[must_use]
    pub fn at_risk_line(&self) -> String {
        format!(
            "WARNING: failure domain unrecorded on groups {:?} with a member down (unseen for {} s, retired or unregistered) or a repair under way: no member of theirs is moved until `admin group set-failure-domain` records the label they were created with",
            self.at_risk,
            AUDIT_UNSEEN_MS / 1000
        )
    }

    /// One line on how far the rule is enforced.
    #[must_use]
    pub fn enforcement(&self) -> String {
        if self.enforced() {
            "failure-domain enforcement: complete (metadata schema 5 or later: no 0.1.0 process can run)".into()
        } else {
            format!(
                "failure-domain enforcement: degraded (metadata schema {} < 5: a 0.1.0 controller may lead and moves members by its --failure-domain flag; every 0.2.0 leader re-checks the groups)",
                self.schema_write
                    .map_or_else(|| "unset".to_string(), |s| s.to_string())
            )
        }
    }
}

/// Read nodes, groups and recorded domains and report every finding
/// (`flag`: the controller's `--failure-domain`, compared with the
/// records; `None` skips the comparison). A member counts as down for
/// [`Audit::at_risk`] when its node is retired, has no record, or was not
/// seen for [`AUDIT_UNSEEN_MS`] before `now_ms`.
///
/// # Errors
///
/// Metadata errors.
pub async fn audit<S: KvStore>(
    meta: &S,
    flag: Option<&str>,
    now_ms: u64,
) -> Result<Audit, ControlError> {
    let nodes: HashMap<u64, NodeRecord> = cluster::list_nodes(meta)
        .await?
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
    let groups = cluster::list_groups(meta).await?;
    let domains = group_domain::list(meta).await?;
    let schema_write = compat::gate(meta).await?.schema_write;
    let retired: BTreeSet<u64> = node_lifecycle::retirements(meta)
        .await?
        .into_iter()
        .map(|r| r.node_id)
        .collect();
    let at_risk = unrecorded_at_risk(&groups, &domains, |id| {
        !retired.contains(&id)
            && nodes
                .get(&id)
                .is_some_and(|n| now_ms.saturating_sub(n.last_seen_ms) <= AUDIT_UNSEEN_MS)
    });
    Ok(Audit {
        at_risk,
        findings: audit_groups(&groups, &nodes, &domains),
        flag_differs: flag
            .map(|f| {
                domains
                    .values()
                    .filter(|d| d.label != f)
                    .map(|d| (d.group, d.label.clone()))
                    .collect()
            })
            .unwrap_or_default(),
        schema_write,
    })
}

/// Why a finalize to `schema` must wait (ADR-0038): from schema 5 no
/// 0.1.0 process runs and the rule is complete only if every group of
/// several members has its domain recorded.
///
/// # Errors
///
/// Metadata errors.
pub async fn finalize_blocker<S: KvStore>(
    meta: &S,
    schema: u64,
) -> Result<Option<String>, ControlError> {
    if schema < SCHEMA_V5 {
        return Ok(None);
    }
    let domains = group_domain::list(meta).await?;
    let missing: Vec<u64> = cluster::list_groups(meta)
        .await?
        .iter()
        .filter(|g| g.members.len() > 1 && !domains.contains_key(&g.id))
        .map(|g| g.id)
        .collect();
    Ok((!missing.is_empty()).then(|| {
        format!(
            "groups {missing:?} have no recorded failure domain: set it with `admin group set-failure-domain` before schema {schema}"
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arca_lake_metadata::ops::group_domain::DomainSource;
    use arca_lake_metadata::records::{GroupRole, GroupState};

    fn node(id: u64, host: Option<&str>) -> NodeRecord {
        NodeRecord {
            id,
            address: format!("n{id}"),
            labels: host
                .map(|h| vec![("host".to_string(), h.to_string())])
                .unwrap_or_default(),
            devices: vec![0],
            registered_ms: 0,
            last_seen_ms: 0,
        }
    }

    fn group(id: u64, members: &[u64]) -> GroupRecord {
        GroupRecord {
            id,
            profile: "p".into(),
            tier: "t".into(),
            scheme: "rs-gf8-v1".into(),
            data_shards: 2,
            parity_shards: 1,
            members: members
                .iter()
                .map(|&node_id| GroupMember {
                    node_id,
                    device_id: 0,
                })
                .collect(),
            epoch: 1,
            state: GroupState::Active,
            repair_cursor: None,
            role: GroupRole::Durable,
        }
    }

    fn recorded(groups: &[u64]) -> BTreeMap<u64, GroupDomain> {
        groups
            .iter()
            .map(|&g| {
                (
                    g,
                    GroupDomain {
                        group: g,
                        label: "host".into(),
                        source: DomainSource::Created,
                        set_ms: 0,
                    },
                )
            })
            .collect()
    }

    /// The audit reports two members in one domain, a member without the
    /// label or node record, and a group without a record; one-member
    /// groups and groups that keep the rule give nothing.
    #[test]
    fn the_audit_reports_each_kind_of_finding() {
        let nodes: HashMap<u64, NodeRecord> = [
            node(1, Some("a")),
            node(2, Some("b")),
            node(3, Some("c")),
            node(4, Some("a")),
            node(5, None),
        ]
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
        let groups = [
            group(1, &[1, 2, 3]),
            group(2, &[1, 2, 4]),
            group(3, &[1, 2, 5]),
            group(4, &[1, 2, 9]),
            group(5, &[1, 2, 3]),
            group(6, &[4]),
        ];
        let f = audit_groups(&groups, &nodes, &recorded(&[1, 2, 3, 4, 6]));
        assert_eq!(
            f,
            vec![
                Finding::SharedDomain {
                    group: 2,
                    label: "host".into(),
                    value: "a".into(),
                    nodes: vec![1, 4]
                },
                Finding::MemberUnlabeled {
                    group: 3,
                    label: "host".into(),
                    node: 5,
                    no_record: false
                },
                Finding::MemberUnlabeled {
                    group: 4,
                    label: "host".into(),
                    node: 9,
                    no_record: true
                },
                Finding::Unrecorded { group: 5 },
            ]
        );
    }

    /// The rule takes the recorded label, refuses a group without one, and
    /// a one-member group only names the label (the flag when unrecorded).
    #[test]
    fn moves_follow_the_recorded_domain_only() {
        let nodes: HashMap<u64, NodeRecord> = [
            node(1, Some("a")),
            node(2, Some("b")),
            node(3, Some("c")),
            node(4, Some("a")),
        ]
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
        let g = group(1, &[1, 2, 3]);
        let to = |node_id| GroupMember {
            node_id,
            device_id: 0,
        };
        let d = recorded(&[1]);
        assert_eq!(label_for(&d, &g, "rack").unwrap(), "host");
        assert!(check_move(&d, &nodes, &g, 0, to(4)).is_ok());
        let e = check_move(&d, &nodes, &g, 1, to(4)).unwrap_err();
        assert!(e.contains("host=a"), "{e}");
        let e = check_move(&BTreeMap::new(), &nodes, &g, 0, to(4)).unwrap_err();
        assert!(e.contains("set-failure-domain"), "{e}");
        assert!(label_for(&BTreeMap::new(), &g, "host").is_err());
        assert_eq!(
            label_for(&BTreeMap::new(), &group(2, &[1]), "host").unwrap(),
            "host"
        );
    }

    /// A group with several findings counts once, wherever they are.
    #[test]
    fn violating_groups_are_counted_once_in_any_order() {
        let unlabeled = |group, node| Finding::MemberUnlabeled {
            group,
            label: "host".into(),
            node,
            no_record: false,
        };
        let a = Audit {
            findings: vec![
                unlabeled(2, 5),
                unlabeled(3, 6),
                Finding::Unrecorded { group: 4 },
                unlabeled(2, 7),
            ],
            ..Audit::default()
        };
        assert_eq!((a.violating_groups(), a.unrecorded_groups()), (2, 1));
    }

    /// Unrecorded groups with a member down or a repair under way are at
    /// risk; recorded groups, healthy ones and one-member groups are not.
    #[test]
    fn unrecorded_groups_with_a_member_down_are_at_risk() {
        let mut repairing = group(4, &[1, 2, 3]);
        repairing.state = GroupState::Repairing;
        let groups = [
            group(1, &[1, 2, 9]),
            group(2, &[1, 2, 3]),
            group(3, &[1, 2, 9]),
            repairing,
            group(5, &[9]),
        ];
        let at_risk = unrecorded_at_risk(&groups, &recorded(&[1]), |id| id != 9);
        assert_eq!(at_risk, vec![3, 4]);
    }

    /// A finalize to schema 5 waits for every group of several members to
    /// be recorded; earlier schemas do not.
    #[tokio::test]
    async fn the_finalize_to_schema_5_waits_for_every_record() {
        let kv = arca_lake_metadata::mem::MemKv::default();
        let g = cluster::create_group(&kv, group(0, &[1, 2, 3]))
            .await
            .unwrap();
        cluster::create_group(&kv, group(0, &[4])).await.unwrap();
        assert_eq!(finalize_blocker(&kv, SCHEMA_V5 - 1).await.unwrap(), None);
        let b = finalize_blocker(&kv, SCHEMA_V5).await.unwrap().unwrap();
        assert!(
            b.contains(&format!("[{}]", g.id)) && b.contains("set-failure-domain"),
            "{b}"
        );
        group_domain::set(&kv, g.id, "host", false, 1)
            .await
            .unwrap();
        assert_eq!(finalize_blocker(&kv, SCHEMA_V5).await.unwrap(), None);
    }
}
