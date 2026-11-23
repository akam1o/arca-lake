//! A group's required failure domain (ADR-0038, R-119): the topology label
//! whose values must differ between the group's members. Recorded when the
//! group is created, or set by an operator for a group created by a
//! release that did not record it (0.1.0); never inferred from the nodes'
//! labels. Under a key of its own (`("al","gdomain",group)`):
//! `GroupRecord`'s encoding is pinned, and a field appended to it would be
//! dropped by a 0.1.0 process rewriting the group (postcard ignores the
//! trailing bytes). Earlier releases never read or rewrite this key.
//!
//! The rule ([`check_replacement`]) is the one every membership change
//! applies: the incoming member's node carries the label, every other
//! member's node does, and the incoming value differs from all of theirs.
//! A member whose node record is gone, or that lacks the label, has an
//! unknown domain: no replacement is admitted (missing information never
//! skips the check). The membership commit of a member move checks it
//! again in its own transaction ([`check_member_in_tx`]).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::{KeyRange, KvStore, Txn};
use crate::ops::{read, write};
use crate::records::{GroupMember, GroupRecord, NodeRecord, Record, decode};
use crate::{MetaError, keys, transact};

/// How a group's domain came to be recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainSource {
    /// By the group's creation (the profile's or the command's label).
    Created,
    /// By an operator (`admin group set-failure-domain`).
    Operator,
}

impl DomainSource {
    /// Short name (reports).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Operator => "operator",
        }
    }
}

/// `("al","gdomain",group)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupDomain {
    /// Group.
    pub group: u64,
    /// Label whose values must be pairwise distinct across the members.
    pub label: String,
    /// Who recorded it.
    pub source: DomainSource,
    /// When, Unix milliseconds.
    pub set_ms: u64,
}

impl Record for GroupDomain {}

/// Why a member may not join a group (or why its domain is unknown).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Violation {
    /// The incoming node lacks the label.
    #[error("node {node} has no '{label}' label")]
    IncomingUnlabeled {
        /// Node.
        node: u64,
        /// Label.
        label: String,
    },
    /// Another member's node record is gone: its domain is unknown.
    #[error("member node {node} has no node record: its '{label}' domain is unknown")]
    MemberUnknown {
        /// Node.
        node: u64,
        /// Label.
        label: String,
    },
    /// Another member's node lacks the label: its domain is unknown.
    #[error("member node {node} has no '{label}' label: its domain is unknown")]
    MemberUnlabeled {
        /// Node.
        node: u64,
        /// Label.
        label: String,
    },
    /// The incoming node shares its domain with another member.
    #[error("node {node} is in '{label}={value}', as member node {other} is")]
    SharesDomain {
        /// Incoming node.
        node: u64,
        /// The member it would share with.
        other: u64,
        /// Label.
        label: String,
        /// Shared value.
        value: String,
    },
}

/// The value of `label` among `labels`.
#[must_use]
pub fn label_value<'a>(labels: &'a [(String, String)], label: &str) -> Option<&'a str> {
    labels
        .iter()
        .find(|(k, _)| k == label)
        .map(|(_, v)| v.as_str())
}

/// Whether `incoming` may take a slot of a group whose other members are
/// on `others` (`None`: no node record), under `label`.
///
/// # Errors
///
/// The first [`Violation`] found.
pub fn check_replacement(
    label: &str,
    incoming: &NodeRecord,
    others: &[(u64, Option<&NodeRecord>)],
) -> std::result::Result<(), Violation> {
    let Some(value) = label_value(&incoming.labels, label) else {
        return Err(Violation::IncomingUnlabeled {
            node: incoming.id,
            label: label.to_string(),
        });
    };
    for (id, n) in others {
        let Some(n) = n else {
            return Err(Violation::MemberUnknown {
                node: *id,
                label: label.to_string(),
            });
        };
        let Some(v) = label_value(&n.labels, label) else {
            return Err(Violation::MemberUnlabeled {
                node: *id,
                label: label.to_string(),
            });
        };
        if v == value {
            return Err(Violation::SharesDomain {
                node: incoming.id,
                other: *id,
                label: label.to_string(),
                value: value.to_string(),
            });
        }
    }
    Ok(())
}

/// Write `rec` in `tx`.
pub(crate) fn put_in_tx<X: Txn>(tx: &mut X, rec: &GroupDomain) -> Result<()> {
    write(tx, &keys::group_domain(rec.group), rec)
}

/// The recorded domain of `group`, if any.
///
/// # Errors
///
/// Backend errors.
pub async fn get<S: KvStore>(store: &S, group: u64) -> Result<Option<GroupDomain>> {
    transact(store, group, |tx, group| {
        Box::pin(async move { read(tx, &keys::group_domain(*group)).await })
    })
    .await
}

/// Every recorded domain, by group.
///
/// # Errors
///
/// Backend errors.
pub async fn list<S: KvStore>(store: &S) -> Result<BTreeMap<u64, GroupDomain>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move { list_in_tx(tx).await })
    })
    .await
}

pub(crate) async fn list_in_tx<X: Txn>(tx: &mut X) -> Result<BTreeMap<u64, GroupDomain>> {
    tx.get_range(
        &KeyRange::prefix(&keys::group_domains_prefix()),
        usize::MAX,
        false,
    )
    .await?
    .into_iter()
    .map(|(_, v)| decode::<GroupDomain>(&v).map(|d| (d.group, d)))
    .collect()
}

/// What [`set`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Set {
    /// Recorded now (`previous`: the label it replaced).
    Recorded {
        /// The label recorded before, if any.
        previous: Option<String>,
    },
    /// The same label was recorded already.
    Unchanged,
}

/// Record `label` as the failure domain of `group` (an operator's
/// decision). A different label already recorded is replaced only with
/// `change`. Members are not moved and nothing is checked against the
/// nodes here: a group whose members already break the rule can be
/// recorded (and then repaired with `replace-member`).
///
/// # Errors
///
/// [`MetaError::NoSuchGroup`]; [`MetaError::InvalidArgument`] for an empty
/// label or another label recorded without `change`; backend errors.
pub async fn set<S: KvStore>(
    store: &S,
    group: u64,
    label: &str,
    change: bool,
    now_ms: u64,
) -> Result<Set> {
    if label.is_empty() || label.contains('=') {
        return Err(MetaError::InvalidArgument(
            "a failure domain is a label name (e.g. host, rack)".into(),
        ));
    }
    transact(store, (group, label, change, now_ms), |tx, env| {
        Box::pin(async move {
            let (group, label, change, now_ms) = *env;
            let _: GroupRecord = read(tx, &keys::group(group))
                .await?
                .ok_or(MetaError::NoSuchGroup(group))?;
            let cur: Option<GroupDomain> = read(tx, &keys::group_domain(group)).await?;
            match cur {
                Some(c) if c.label == label => return Ok(Set::Unchanged),
                Some(c) if !change => {
                    return Err(MetaError::InvalidArgument(format!(
                        "group {group} has failure domain '{}' recorded ({}): pass --change to replace it",
                        c.label,
                        c.source.as_str()
                    )));
                }
                _ => {}
            }
            put_in_tx(
                tx,
                &GroupDomain {
                    group,
                    label: label.to_string(),
                    source: DomainSource::Operator,
                    set_ms: now_ms,
                },
            )?;
            Ok(Set::Recorded {
                previous: cur.map(|c| c.label),
            })
        })
    })
    .await
}

/// Why no member of `group` (which has no recorded domain) may move.
#[must_use]
pub fn unrecorded_reason(group: u64) -> String {
    format!(
        "group {group} has no recorded failure domain (created by a release that did not record it): no member is moved until `admin group set-failure-domain --group {group} --label <label>` sets it"
    )
}

/// In the transaction of a membership change of slot `index` of `g` to
/// `replacement`: the group's recorded domain must admit it (ADR-0038). A
/// group without one refuses every move; a one-member group has no other
/// member to differ from.
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] naming the violation, or the missing
/// record; backend errors.
pub(crate) async fn check_member_in_tx<X: Txn>(
    tx: &mut X,
    g: &GroupRecord,
    index: usize,
    replacement: GroupMember,
) -> Result<()> {
    if g.members.len() <= 1 {
        return Ok(());
    }
    let Some(rec): Option<GroupDomain> = read(tx, &keys::group_domain(g.id)).await? else {
        return Err(MetaError::InvalidArgument(unrecorded_reason(g.id)));
    };
    let label = rec.label;
    let Some(incoming): Option<NodeRecord> = read(tx, &keys::node(replacement.node_id)).await?
    else {
        return Err(MetaError::InvalidArgument(format!(
            "failure domain of group {}: node {} has no node record",
            g.id, replacement.node_id
        )));
    };
    let mut others = Vec::new();
    for (i, m) in g.members.iter().enumerate() {
        if i != index {
            let n: Option<NodeRecord> = read(tx, &keys::node(m.node_id)).await?;
            others.push((m.node_id, n));
        }
    }
    let view: Vec<(u64, Option<&NodeRecord>)> =
        others.iter().map(|(id, n)| (*id, n.as_ref())).collect();
    check_replacement(&label, &incoming, &view).map_err(|v| {
        MetaError::InvalidArgument(format!("failure domain '{label}' of group {}: {v}", g.id))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemKv;
    use crate::records::{GroupRole, GroupState};

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

    #[test]
    fn the_rule_refuses_a_shared_or_unknown_domain() {
        let a = node(1, Some("a"));
        let b = node(2, Some("b"));
        let a2 = node(3, Some("a"));
        let bare = node(4, None);
        assert!(check_replacement("host", &b, &[(1, Some(&a))]).is_ok());
        assert!(matches!(
            check_replacement("host", &a2, &[(1, Some(&a)), (2, Some(&b))]),
            Err(Violation::SharesDomain { other: 1, .. })
        ));
        assert!(matches!(
            check_replacement("host", &bare, &[(1, Some(&a))]),
            Err(Violation::IncomingUnlabeled { node: 4, .. })
        ));
        assert!(matches!(
            check_replacement("host", &b, &[(4, Some(&bare))]),
            Err(Violation::MemberUnlabeled { node: 4, .. })
        ));
        assert!(matches!(
            check_replacement("host", &b, &[(9, None)]),
            Err(Violation::MemberUnknown { node: 9, .. })
        ));
    }

    #[test]
    fn an_operator_sets_a_domain_and_changes_it_only_explicitly() {
        futures::executor::block_on(an_operator_sets_a_domain());
    }

    async fn an_operator_sets_a_domain() {
        let kv = MemKv::default();
        assert!(matches!(
            set(&kv, 1, "host", false, 1).await,
            Err(MetaError::NoSuchGroup(1))
        ));
        let g = crate::ops::cluster::create_group(
            &kv,
            GroupRecord {
                id: 0,
                profile: "p".into(),
                tier: "t".into(),
                scheme: "rs-gf8-v1".into(),
                data_shards: 2,
                parity_shards: 1,
                members: vec![],
                epoch: 0,
                state: GroupState::Active,
                repair_cursor: None,
                role: GroupRole::Durable,
            },
        )
        .await
        .unwrap();
        assert_eq!(get(&kv, g.id).await.unwrap(), None);
        assert!(set(&kv, g.id, "", false, 1).await.is_err());
        assert_eq!(
            set(&kv, g.id, "host", false, 2).await.unwrap(),
            Set::Recorded { previous: None }
        );
        assert_eq!(
            set(&kv, g.id, "host", false, 3).await.unwrap(),
            Set::Unchanged
        );
        let e = set(&kv, g.id, "rack", false, 4).await.unwrap_err();
        assert!(e.to_string().contains("--change"), "{e}");
        assert_eq!(
            set(&kv, g.id, "rack", true, 5).await.unwrap(),
            Set::Recorded {
                previous: Some("host".into())
            }
        );
        let d = get(&kv, g.id).await.unwrap().unwrap();
        assert_eq!(
            (d.label.as_str(), d.source, d.set_ms),
            ("rack", DomainSource::Operator, 5)
        );
        assert_eq!(list(&kv).await.unwrap().len(), 1);
    }

    /// The record's body is pinned (ADR-0038 §3), and the key family lies
    /// outside the group listing a 0.1.0 process decodes as `GroupRecord`.
    #[test]
    fn the_record_and_its_key_stay_out_of_the_way_of_earlier_releases() {
        let d = GroupDomain {
            group: 7,
            label: "host".into(),
            source: DomainSource::Operator,
            set_ms: 300,
        };
        let v = crate::records::encode(&d).unwrap();
        assert_eq!(&v[1..], b"\x07\x04host\x01\xac\x02");
        assert_eq!(decode::<GroupDomain>(&v).unwrap(), d);
        let groups = KeyRange::prefix(&keys::groups_prefix());
        assert!(!groups.contains(&keys::group_domain(7)));
        assert!(!groups.contains(&keys::group_domains_prefix()));
        assert!(!KeyRange::prefix(&keys::group_domains_prefix()).contains(&keys::group(7)));
    }

    /// A group rewrite as every release makes it (`update_group`, the path
    /// of 0.1.0's membership changes and repair cursor) leaves the record.
    #[test]
    fn a_group_rewrite_keeps_the_domain_record() {
        futures::executor::block_on(async {
            let kv = MemKv::default();
            let g = crate::ops::cluster::create_group(
                &kv,
                GroupRecord {
                    id: 0,
                    profile: "p".into(),
                    tier: "t".into(),
                    scheme: "rs-gf8-v1".into(),
                    data_shards: 2,
                    parity_shards: 1,
                    members: vec![],
                    epoch: 0,
                    state: GroupState::Active,
                    repair_cursor: None,
                    role: GroupRole::Durable,
                },
            )
            .await
            .unwrap();
            set(&kv, g.id, "host", false, 2).await.unwrap();
            crate::ops::cluster::update_group(&kv, g.id, |g| {
                g.epoch += 1;
                g.state = GroupState::Repairing;
                true
            })
            .await
            .unwrap();
            let d = get(&kv, g.id).await.unwrap().unwrap();
            assert_eq!((d.label.as_str(), d.set_ms), ("host", 2));
        });
    }
}
