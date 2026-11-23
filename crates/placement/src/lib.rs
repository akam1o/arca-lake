//! Placement logic (RFC-004 §5.3): choosing stripe-group members across
//! failure domains, choosing a replacement member, and choosing a group for
//! a write. Pure functions over plain data; no I/O.

use std::collections::{BTreeMap, BTreeSet};

/// A device the controller may place shards on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    /// Owning node.
    pub node_id: u64,
    /// Device within the node.
    pub device_id: u32,
    /// Topology labels of the node (`host`, `rack`, ...).
    pub labels: BTreeMap<String, String>,
    /// Free capacity estimate.
    pub free_bytes: u64,
    /// Number of groups the device already belongs to.
    pub group_count: u32,
}

/// A chosen member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Member {
    /// Node.
    pub node_id: u64,
    /// Device.
    pub device_id: u32,
}

/// What a group needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSpec {
    /// `k + m`.
    pub total_shards: usize,
    /// Label whose values must be pairwise distinct across members.
    pub failure_domain: String,
}

/// Placement errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    /// Not enough distinct failure domains with a usable device.
    #[error("need {needed} distinct '{label}' domains, found {available}")]
    InsufficientDomains {
        /// Required count.
        needed: usize,
        /// Label name.
        label: String,
        /// Domains available.
        available: usize,
    },
    /// A device lacks the failure-domain label.
    #[error("device {node_id}/{device_id} has no '{label}' label")]
    MissingLabel {
        /// Node.
        node_id: u64,
        /// Device.
        device_id: u32,
        /// Label name.
        label: String,
    },
}

fn domain_of<'a>(d: &'a Device, label: &str) -> Result<&'a str, PlacementError> {
    d.labels
        .get(label)
        .map(String::as_str)
        .ok_or_else(|| PlacementError::MissingLabel {
            node_id: d.node_id,
            device_id: d.device_id,
            label: label.to_string(),
        })
}

/// Rank devices: fewer group memberships first, then more free space, then
/// stable by identity.
fn rank(d: &Device) -> (u32, std::cmp::Reverse<u64>, u64, u32) {
    (
        d.group_count,
        std::cmp::Reverse(d.free_bytes),
        d.node_id,
        d.device_id,
    )
}

/// Choose `spec.total_shards` members with pairwise distinct failure
/// domains, excluding `excluded_domains`. Member order is the shard order.
///
/// # Errors
///
/// [`PlacementError::InsufficientDomains`] when the topology cannot satisfy
/// the spec; [`PlacementError::MissingLabel`] for an unlabeled device.
pub fn choose_group_members(
    devices: &[Device],
    spec: &GroupSpec,
    excluded_domains: &BTreeSet<String>,
) -> Result<Vec<Member>, PlacementError> {
    // Best device per domain.
    let mut best: BTreeMap<&str, &Device> = BTreeMap::new();
    for d in devices {
        let dom = domain_of(d, &spec.failure_domain)?;
        if excluded_domains.contains(dom) {
            continue;
        }
        match best.get(dom) {
            Some(cur) if rank(cur) <= rank(d) => {}
            _ => {
                best.insert(dom, d);
            }
        }
    }
    if best.len() < spec.total_shards {
        return Err(PlacementError::InsufficientDomains {
            needed: spec.total_shards,
            label: spec.failure_domain.clone(),
            available: best.len(),
        });
    }
    let mut candidates: Vec<&Device> = best.into_values().collect();
    candidates.sort_by_key(|d| rank(d));
    Ok(candidates
        .into_iter()
        .take(spec.total_shards)
        .map(|d| Member {
            node_id: d.node_id,
            device_id: d.device_id,
        })
        .collect())
}

/// Choose a replacement for a failed member among `candidates`: a device
/// whose failure domain differs from every remaining member's. The
/// remaining members are given as devices, labels included, so that each
/// one's domain is known (the candidates usually leave them out, and may
/// leave out members that are unhealthy: their domains still count). A
/// candidate without the label is not eligible (its domain cannot be
/// compared), and does not stop the choice among the others (R-119).
///
/// # Errors
///
/// [`PlacementError::InsufficientDomains`] when no eligible candidate is
/// in a free domain; [`PlacementError::MissingLabel`] for a remaining
/// member without the failure-domain label.
pub fn choose_replacement(
    candidates: &[Device],
    spec: &GroupSpec,
    remaining: &[Device],
) -> Result<Member, PlacementError> {
    let mut excluded = BTreeSet::new();
    for d in remaining {
        excluded.insert(domain_of(d, &spec.failure_domain)?.to_string());
    }
    let devices: Vec<Device> = candidates
        .iter()
        .filter(|d| d.labels.contains_key(&spec.failure_domain))
        .cloned()
        .collect();
    let devices = &devices;
    let one = GroupSpec {
        total_shards: 1,
        failure_domain: spec.failure_domain.clone(),
    };
    choose_group_members(devices, &one, &excluded).map(|v| v[0])
}

/// A group as seen by a writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritableGroup {
    /// Group ID.
    pub id: u64,
    /// Smallest free capacity among members (the binding constraint).
    pub min_free_bytes: u64,
}

/// Choose a group for a new chunk: the one with the most binding free
/// capacity, with `salt` breaking ties so that concurrent writers spread.
#[must_use]
pub fn pick_group_for_write(groups: &[WritableGroup], salt: u64) -> Option<u64> {
    if groups.is_empty() {
        return None;
    }
    let max = groups.iter().map(|g| g.min_free_bytes).max()?;
    // Treat groups within 10% of the best as equivalent and rotate.
    let threshold = max - max / 10;
    let eligible: Vec<&WritableGroup> = groups
        .iter()
        .filter(|g| g.min_free_bytes >= threshold)
        .collect();
    let idx = usize::try_from(salt % eligible.len() as u64).unwrap_or(0);
    Some(eligible[idx].id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dev(node: u64, host: &str, rack: &str, free: u64, groups: u32) -> Device {
        Device {
            node_id: node,
            device_id: 0,
            labels: [
                ("host".to_string(), host.to_string()),
                ("rack".to_string(), rack.to_string()),
            ]
            .into(),
            free_bytes: free,
            group_count: groups,
        }
    }

    #[test]
    fn distinct_domains_and_ranking() {
        let devices = vec![
            dev(1, "h1", "r1", 100, 0),
            dev(2, "h2", "r1", 500, 0),
            dev(3, "h3", "r2", 300, 1),
            dev(4, "h4", "r2", 900, 0),
            dev(5, "h1", "r1", 1000, 0), // same host as node 1
        ];
        let spec = GroupSpec {
            total_shards: 3,
            failure_domain: "host".into(),
        };
        let m = choose_group_members(&devices, &spec, &BTreeSet::new()).unwrap();
        let nodes: Vec<u64> = m.iter().map(|m| m.node_id).collect();
        // Lowest group_count first, then most free space: h1 best is node 5.
        assert_eq!(nodes, vec![5, 4, 2]);

        let rack = GroupSpec {
            total_shards: 3,
            failure_domain: "rack".into(),
        };
        let err = choose_group_members(&devices, &rack, &BTreeSet::new()).unwrap_err();
        assert!(
            matches!(
                err,
                PlacementError::InsufficientDomains { available: 2, .. }
            ),
            "{err}"
        );

        let of = |m: Member| {
            devices
                .iter()
                .find(|d| d.node_id == m.node_id)
                .unwrap()
                .clone()
        };
        let repl = choose_replacement(&devices, &spec, &[of(m[1]), of(m[2])]).unwrap();
        assert_eq!(repl.node_id, 5, "replacement must avoid h4 and h2");
    }

    /// Three-host validation of 0.1.0: the candidates leave the remaining
    /// members out (they cannot be chosen again), and their domains must
    /// still be avoided.
    #[test]
    fn a_replacement_avoids_the_domains_of_members_not_among_the_candidates() {
        let a0 = dev(1, "a", "r1", 100, 2);
        let a1 = dev(4, "a", "r1", 100, 1);
        let b0 = dev(2, "b", "r1", 100, 2);
        let c1 = dev(6, "c", "r1", 100, 1);
        let spec = GroupSpec {
            total_shards: 3,
            failure_domain: "host".into(),
        };
        // Group [a1, b1, c1], b1 failed: a0 (fewer free domains, fewer
        // groups) must not be taken; b0 is the only valid choice.
        let repl = choose_replacement(&[a0.clone(), b0], &spec, &[a1.clone(), c1.clone()]).unwrap();
        assert_eq!(repl.node_id, 2);
        assert!(matches!(
            choose_replacement(&[a0], &spec, &[a1, c1]),
            Err(PlacementError::InsufficientDomains { .. })
        ));
    }

    #[test]
    fn a_remaining_member_without_the_label_is_an_error() {
        let mut m = dev(4, "a", "r1", 100, 1);
        m.labels.remove("host");
        let spec = GroupSpec {
            total_shards: 3,
            failure_domain: "host".into(),
        };
        assert!(matches!(
            choose_replacement(&[dev(2, "b", "r1", 100, 0)], &spec, &[m]),
            Err(PlacementError::MissingLabel { .. })
        ));
    }

    /// R-119: an unlabeled candidate used to fail every replacement with
    /// `MissingLabel`; it is now passed over.
    #[test]
    fn an_unlabeled_candidate_is_passed_over() {
        let mut bare = dev(7, "x", "r1", 1000, 0);
        bare.labels.remove("host");
        let spec = GroupSpec {
            total_shards: 3,
            failure_domain: "host".into(),
        };
        let remaining = [dev(1, "a", "r1", 100, 1), dev(2, "b", "r1", 100, 1)];
        let r = choose_replacement(&[bare.clone(), dev(3, "c", "r1", 10, 5)], &spec, &remaining)
            .unwrap();
        assert_eq!(r.node_id, 3);
        assert!(matches!(
            choose_replacement(&[bare], &spec, &remaining),
            Err(PlacementError::InsufficientDomains { .. })
        ));
    }

    #[test]
    fn missing_label_is_explicit() {
        let mut d = dev(1, "h1", "r1", 1, 0);
        d.labels.remove("rack");
        let spec = GroupSpec {
            total_shards: 1,
            failure_domain: "rack".into(),
        };
        assert!(matches!(
            choose_group_members(&[d], &spec, &BTreeSet::new()),
            Err(PlacementError::MissingLabel { .. })
        ));
    }

    #[test]
    fn write_group_selection_rotates_among_equivalent_groups() {
        let groups = vec![
            WritableGroup {
                id: 1,
                min_free_bytes: 1000,
            },
            WritableGroup {
                id: 2,
                min_free_bytes: 950,
            },
            WritableGroup {
                id: 3,
                min_free_bytes: 100,
            },
        ];
        let picks: BTreeSet<u64> = (0..10)
            .map(|s| pick_group_for_write(&groups, s).unwrap())
            .collect();
        assert_eq!(picks, [1, 2].into());
        assert!(pick_group_for_write(&[], 0).is_none());
    }
}
