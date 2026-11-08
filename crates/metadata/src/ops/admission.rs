//! Write admission record (Phase 3.6, R-64): what the controller leader
//! decided each tier may take, read by every gateway on its two-second
//! cluster refresh. One small record written only by the leader (fenced
//! like every controller transaction), read with the cluster state: not a
//! per-request key.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::KvStore;
use crate::ops::{read, write};
use crate::{keys, transact};

/// What one tier admits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TierAdmission {
    /// Tier name.
    pub tier: String,
    /// 0 normal, 1 warning, 2 throttled, 3 write rejected, 4 recovery only.
    pub state: u8,
    /// In `throttled`: logical bytes per second of client data the tier
    /// takes, all gateways together.
    pub rate_bytes_per_s: u64,
    /// Groups not to place new chunks in (a member lacks room).
    pub excluded_groups: Vec<u64>,
    /// Usable free fraction, per mille.
    pub free_permille: u32,
    /// Why.
    pub reason: String,
    /// Room of each group (Phase 3.7): its fullest member's bytes above
    /// the reserve, as last reported. Gateways place where most is left.
    pub group_room: Vec<GroupRoom>,
}

/// Room of one group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GroupRoom {
    /// Group id.
    pub group: u64,
    /// The fullest member's headroom, bytes.
    pub room: u64,
}

/// The admission record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdmissionRecord {
    /// When the leader computed it, unix ms.
    pub computed_ms: u64,
    /// How long gateways may trust it as is, ms.
    pub ttl_ms: u64,
    /// Gateways alive when it was computed (the rate is split over them).
    pub gateways: u32,
    /// Per tier.
    pub tiers: Vec<TierAdmission>,
    /// Fencing token of the leader that computed it (Phase 3.7): a record
    /// of a lower token than one already seen is ignored by gateways.
    pub leader_token: u64,
    /// Publication count within the leader's term.
    pub generation: u64,
    /// The leader had not yet heard every member in its term: the states
    /// are at least those it found published (warm-up after a failover).
    pub warming_up: bool,
    /// Nodes sharing a file system without allotments that fit it (R-80).
    pub capacity_conflicts: Vec<u64>,
    /// Nodes whose file system sharing cannot be checked (no allotment).
    pub capacity_unverified: Vec<u64>,
}

impl crate::records::Record for AdmissionRecord {}

impl AdmissionRecord {
    /// The entry of `tier`, if any.
    #[must_use]
    pub fn tier(&self, tier: &str) -> Option<&TierAdmission> {
        self.tiers.iter().find(|t| t.tier == tier)
    }
}

/// Write the record (the controller leader, through its fenced store).
///
/// # Errors
///
/// Backend errors; `Fenced` when the caller is no longer the leader.
pub async fn publish<S: KvStore>(store: &S, record: &AdmissionRecord) -> Result<()> {
    transact(store, record, |tx, record| {
        Box::pin(async move { write(tx, &keys::admission(), *record) })
    })
    .await
}

/// The record, if any controller has published one.
///
/// # Errors
///
/// Backend errors.
pub async fn current<S: KvStore>(store: &S) -> Result<Option<AdmissionRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move { read(tx, &keys::admission()).await })
    })
    .await
}
