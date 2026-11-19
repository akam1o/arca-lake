//! The tiering record (ADR-0035 option A): whether automatic tiering acts
//! and with which policy, as the cluster's source of truth (not any
//! controller's flags). Written by admin commands, seeded once from a
//! controller's flags, acknowledged by the leader; read by gateways in
//! their two-second refresh together with the leader record, to decide
//! whether reads must be recorded.

use serde::{Deserialize, Serialize};

use crate::error::{MetaError, Result};
use crate::kv::{KvStore, Txn as _};
use crate::ops::leader::LeaderRecord;
use crate::ops::{read, write};
use crate::{keys, transact};

/// First metadata schema whose processes all read this record (ADR-0035
/// §10.1): tiering can be disabled only once `schema_write` reached it,
/// so that no controller ignoring the record can lead while gateways may
/// have stopped recording.
pub const TIERING_RECORD_SCHEMA: u64 = 4;

/// How long a gateway trusts a confirmation that tiering is disabled
/// (ADR-0035 §10.3).
pub const CONFIRMATION_STALE_MS: u64 = 10_000;

/// Gateways' refresh interval the margin assumes (`CLUSTER_REFRESH`).
pub const GATEWAY_REFRESH_MS: u64 = 2_000;

/// How late ADR-0034 writes a read at most (its `max_age` default).
pub const FLUSH_DELAY_MS: u64 = 30_000;

/// The grace margin of ADR-0035 §10.4: the gateways' stale bound plus one
/// refresh, the flush delay, and the clock skew allowed between
/// controllers and gateways.
#[must_use]
pub const fn grace_margin_ms(skew_ms: u64) -> u64 {
    CONFIRMATION_STALE_MS + GATEWAY_REFRESH_MS + FLUSH_DELAY_MS + skew_ms
}

/// The policy (as `arca_lake_controller::tiering::TieringPolicy`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TieringPolicyRecord {
    /// Durable tiers from lowest (coldest) to highest (hottest).
    pub tiers: Vec<String>,
    /// Promote a generation read at least this many times...
    pub promote_reads: u64,
    /// ...within this window, ms.
    pub promote_window_ms: u64,
    /// Demote a generation not read for this long, ms.
    pub demote_after_ms: u64,
}

impl TieringPolicyRecord {
    /// Whether `self` can make more generations eligible for demotion than
    /// `before` (ADR-0035 §10.4): a shorter `demote_after`, or any change
    /// of the tiers.
    #[must_use]
    pub fn widens_demotion_from(&self, before: &Self) -> bool {
        self.demote_after_ms < before.demote_after_ms || self.tiers != before.tiers
    }
}

/// The record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TieringRecord {
    /// Tiering acts.
    pub enabled: bool,
    /// Its policy (kept while disabled).
    pub policy: TieringPolicyRecord,
    /// Incremented by every change (an earlier acknowledgment no longer
    /// matches).
    pub generation: u64,
    /// Changes since the last acknowledgment enabled tiering.
    pub pending_enable: bool,
    /// Changes since the last acknowledgment widened demotion.
    pub pending_widen: bool,
    /// Demotion counts a generation as touched no earlier than this plus
    /// the margin (stamped by the acknowledging leader), unix ms.
    pub grace_from_ms: u64,
    /// Counts of records last read before this do not promote (stamped
    /// when tiering goes from disabled to enabled), unix ms.
    pub promotion_from_ms: u64,
    /// Leader term that acknowledged `applied_generation` (0: none).
    pub applied_term: u64,
    /// Generation acknowledged.
    pub applied_generation: u64,
    /// Cluster incarnation of the acknowledgment (a restore restarts the
    /// grace).
    pub applied_incarnation: [u8; 16],
    /// Time of the last change, unix ms (the writer's clock; shown only).
    pub changed_ms: u64,
}

impl crate::records::Record for TieringRecord {}

impl TieringRecord {
    /// The current generation is acknowledged by the leader of `term`.
    #[must_use]
    pub fn acknowledged_by(&self, term: u64) -> bool {
        self.applied_term == term && self.applied_generation == self.generation && term != 0
    }
}

/// The record, if any.
///
/// # Errors
///
/// Backend and codec errors.
pub async fn current<S: KvStore>(store: &S) -> Result<Option<TieringRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move { read(tx, &keys::tiering()).await })
    })
    .await
}

/// The record and the leader record in one transaction (gateways, ADR-0035
/// §10.2: the acknowledgment and the leader observed together).
///
/// # Errors
///
/// Backend and codec errors.
pub async fn read_with_leader<S: KvStore>(
    store: &S,
) -> Result<(Option<TieringRecord>, Option<LeaderRecord>)> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let t = read(tx, &keys::tiering()).await?;
            let l = read(tx, &keys::leader()).await?;
            Ok((t, l))
        })
    })
    .await
}

/// Write the record from a controller's flags if none exists (one
/// insert-if-absent transaction). Returns whether it was written.
///
/// # Errors
///
/// Backend and codec errors.
pub async fn seed_if_absent<S: KvStore>(
    store: &S,
    policy: &TieringPolicyRecord,
    now_ms: u64,
) -> Result<bool> {
    transact(store, (policy, now_ms), |tx, env| {
        Box::pin(async move {
            let (policy, now_ms) = *env;
            if read::<TieringRecord, _>(tx, &keys::tiering())
                .await?
                .is_some()
            {
                return Ok(false);
            }
            write(
                tx,
                &keys::tiering(),
                &TieringRecord {
                    enabled: true,
                    policy: policy.clone(),
                    generation: 1,
                    pending_enable: true,
                    changed_ms: now_ms,
                    ..TieringRecord::default()
                },
            )?;
            Ok(true)
        })
    })
    .await
}

/// What an admin command sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminChange {
    /// Enable, with this policy (or the record's when `None`).
    Enable(Option<TieringPolicyRecord>),
    /// Disable (refused below [`TIERING_RECORD_SCHEMA`]).
    Disable,
    /// Replace the policy, enabled or not.
    SetPolicy(TieringPolicyRecord),
}

/// Apply an admin change (ADR-0035 §10.1): increments `generation`, notes
/// whether it enables tiering or widens demotion for the acknowledgment.
/// Returns the record written.
///
/// # Errors
///
/// `InvalidArgument` for a disable below [`TIERING_RECORD_SCHEMA`], an
/// enable without any policy, or a policy without tiers; backend errors.
pub async fn admin_change<S: KvStore>(
    store: &S,
    change: &AdminChange,
    now_ms: u64,
) -> Result<TieringRecord> {
    transact(store, (change, now_ms), |tx, env| {
        Box::pin(async move {
            let (change, now_ms) = *env;
            let before = read::<TieringRecord, _>(tx, &keys::tiering()).await?;
            let mut r = before.clone().unwrap_or_default();
            match change {
                AdminChange::Disable => {
                    if before.is_none() {
                        return Err(MetaError::InvalidArgument(
                            "no tiering record: nothing to disable (gateways record reads)".into(),
                        ));
                    }
                    r.enabled = false;
                }
                AdminChange::Enable(policy) => {
                    if let Some(p) = policy {
                        r.pending_widen |= r.enabled && p.widens_demotion_from(&r.policy);
                        r.policy = p.clone();
                    }
                    if r.policy.tiers.is_empty() {
                        return Err(MetaError::InvalidArgument(
                            "no tiering policy: give the tiers and thresholds".into(),
                        ));
                    }
                    r.pending_enable |= !r.enabled || before.is_none();
                    r.enabled = true;
                }
                AdminChange::SetPolicy(p) => {
                    if p.tiers.is_empty() {
                        return Err(MetaError::InvalidArgument("a policy needs tiers".into()));
                    }
                    r.pending_widen |= r.enabled && p.widens_demotion_from(&r.policy);
                    r.policy = p.clone();
                }
            }
            // Any change that leaves tiering disabled (a disable, or a policy
            // set on an absent record) waits for the schema step.
            if !r.enabled {
                let schema = tx
                    .get(&keys::schema_write())
                    .await?
                    .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
                    .map_or(0, u64::from_le_bytes);
                if schema < TIERING_RECORD_SCHEMA {
                    return Err(MetaError::InvalidArgument(format!(
                        "tiering can be disabled once schema {TIERING_RECORD_SCHEMA} is written \
                         (`admin upgrade finalize`); the store writes {schema}: until then a \
                         controller of an earlier release could run tiering from its flags while \
                         gateways stop recording"
                    )));
                }
            }
            r.generation += 1;
            r.changed_ms = now_ms;
            write(tx, &keys::tiering(), &r)?;
            Ok(r)
        })
    })
    .await
}

/// The leader acknowledges the record's current generation (ADR-0035
/// §10.1), if it is still `generation`: writes `(term, generation)`,
/// stamps `grace_from_ms` (enabling, widening, or a new incarnation) and
/// `promotion_from_ms` (enabling, or a new incarnation) with `now_ms`, and
/// clears the pending marks. Returns the record as acknowledged, or
/// `None` when there is no record or its generation moved on.
///
/// # Errors
///
/// Backend and codec errors; `Fenced` through a fenced store when the
/// caller is no longer the leader.
pub async fn acknowledge<S: KvStore>(
    store: &S,
    term: u64,
    incarnation: [u8; 16],
    generation: u64,
    now_ms: u64,
) -> Result<Option<TieringRecord>> {
    transact(store, (term, incarnation, generation, now_ms), |tx, env| {
        Box::pin(async move {
            let (term, incarnation, generation, now_ms) = *env;
            if term == 0 {
                return Ok(None);
            }
            let Some(mut r) = read::<TieringRecord, _>(tx, &keys::tiering()).await? else {
                return Ok(None);
            };
            if r.generation != generation {
                return Ok(None);
            }
            if r.acknowledged_by(term) && r.applied_incarnation == incarnation {
                return Ok(Some(r));
            }
            let restored = r.applied_term != 0 && r.applied_incarnation != incarnation;
            if r.enabled && (r.pending_enable || r.pending_widen || restored) {
                r.grace_from_ms = now_ms;
            }
            if r.enabled && (r.pending_enable || restored) {
                r.promotion_from_ms = now_ms;
            }
            r.pending_enable = false;
            r.pending_widen = false;
            r.applied_term = term;
            r.applied_generation = generation;
            r.applied_incarnation = incarnation;
            write(tx, &keys::tiering(), &r)?;
            Ok(Some(r))
        })
    })
    .await
}

/// Whether a gateway may stop recording reads (ADR-0035 §10.3): only when
/// a combined read completed `age_ms` ago shows tiering disabled and
/// acknowledged by the leader of the leader record, whose lease was
/// unexpired (by more than `skew_ms`) at `read_at_ms`, and the read is
/// fresh. Anything else (no record, enabled, unacknowledged, no or an
/// expired or released lease, a stale read) keeps recording.
#[must_use]
pub fn confirmed_disabled(
    tiering: Option<&TieringRecord>,
    leader: Option<&LeaderRecord>,
    read_at_ms: u64,
    age_ms: u64,
    skew_ms: u64,
) -> bool {
    let (Some(t), Some(l)) = (tiering, leader) else {
        return false;
    };
    !t.enabled
        && t.acknowledged_by(l.token)
        && l.lease_until_ms > read_at_ms.saturating_add(skew_ms)
        && age_ms <= CONFIRMATION_STALE_MS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemKv;

    fn policy(demote_s: u64) -> TieringPolicyRecord {
        TieringPolicyRecord {
            tiers: vec!["default".into(), "fast".into()],
            promote_reads: 5,
            promote_window_ms: 600_000,
            demote_after_ms: demote_s * 1000,
        }
    }

    async fn set_schema(kv: &MemKv, v: u64) {
        let mut tx = kv.begin().await.unwrap();
        tx.set(&keys::schema_write(), &v.to_le_bytes());
        tx.commit().await.unwrap();
    }

    fn leader(token: u64, until: u64) -> LeaderRecord {
        LeaderRecord {
            holder: "ctl".into(),
            token,
            lease_until_ms: until,
        }
    }

    #[test]
    fn seeding_happens_once_and_disabling_waits_for_the_schema() {
        futures::executor::block_on(async {
            let kv = MemKv::default();
            assert!(seed_if_absent(&kv, &policy(1800), 10).await.unwrap());
            assert!(
                !seed_if_absent(&kv, &policy(60), 20).await.unwrap(),
                "seeded twice"
            );
            let r = current(&kv).await.unwrap().unwrap();
            assert_eq!(
                (r.enabled, r.generation, r.policy.demote_after_ms),
                (true, 1, 1_800_000)
            );
            set_schema(&kv, 3).await;
            // Every change that would leave tiering disabled is refused
            // below the schema step: a policy set on an absent record, and a
            // disable; a disable without a record is refused at any schema.
            let empty = MemKv::default();
            set_schema(&empty, TIERING_RECORD_SCHEMA).await;
            assert!(
                admin_change(&empty, &AdminChange::Disable, 5)
                    .await
                    .is_err()
            );
            set_schema(&empty, 3).await;
            assert!(
                admin_change(&empty, &AdminChange::SetPolicy(policy(60)), 5)
                    .await
                    .is_err()
            );
            assert_eq!(current(&empty).await.unwrap(), None);
            let r = admin_change(&empty, &AdminChange::Enable(Some(policy(60))), 6)
                .await
                .unwrap();
            assert!(r.enabled, "enabling is allowed below the schema step");
            let e = admin_change(&kv, &AdminChange::Disable, 30)
                .await
                .unwrap_err();
            assert!(matches!(e, MetaError::InvalidArgument(_)), "{e:?}");
            assert!(current(&kv).await.unwrap().unwrap().enabled);
            set_schema(&kv, TIERING_RECORD_SCHEMA).await;
            let r = admin_change(&kv, &AdminChange::Disable, 40).await.unwrap();
            assert_eq!((r.enabled, r.generation), (false, 2));
        });
    }

    #[test]
    fn the_acknowledgment_names_the_generation_and_stamps_the_grace() {
        futures::executor::block_on(async {
            let kv = MemKv::default();
            let inc = [7u8; 16];
            seed_if_absent(&kv, &policy(1800), 10).await.unwrap();
            // A stale generation is not acknowledged.
            assert_eq!(acknowledge(&kv, 5, inc, 0, 100).await.unwrap(), None);
            let r = acknowledge(&kv, 5, inc, 1, 100).await.unwrap().unwrap();
            assert!(r.acknowledged_by(5));
            assert_eq!(
                (r.grace_from_ms, r.promotion_from_ms),
                (100, 100),
                "enabling stamps both"
            );
            // A change the leader has not applied is not acknowledged.
            set_schema(&kv, TIERING_RECORD_SCHEMA).await;
            let r = admin_change(&kv, &AdminChange::Disable, 200).await.unwrap();
            assert!(
                !r.acknowledged_by(5),
                "an earlier acknowledgment matched a new generation"
            );
            let r = acknowledge(&kv, 5, inc, r.generation, 300)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                (r.grace_from_ms, r.promotion_from_ms),
                (100, 100),
                "disabling stamps nothing"
            );
            // Enabling again: a new grace and promotion cutoff.
            let r = admin_change(&kv, &AdminChange::Enable(None), 400)
                .await
                .unwrap();
            let r = acknowledge(&kv, 5, inc, r.generation, 500)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((r.grace_from_ms, r.promotion_from_ms), (500, 500));
            // A widening change: the grace only.
            let r = admin_change(&kv, &AdminChange::SetPolicy(policy(60)), 600)
                .await
                .unwrap();
            let r = acknowledge(&kv, 5, inc, r.generation, 700)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((r.grace_from_ms, r.promotion_from_ms), (700, 500));
            // A narrowing change: neither.
            let r = admin_change(&kv, &AdminChange::SetPolicy(policy(3600)), 800)
                .await
                .unwrap();
            let r = acknowledge(&kv, 5, inc, r.generation, 900)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((r.grace_from_ms, r.promotion_from_ms), (700, 500));
            // A new leader acknowledges without new stamps; a restore (another
            // incarnation) restarts both.
            let r = acknowledge(&kv, 6, inc, r.generation, 1000)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((r.applied_term, r.grace_from_ms), (6, 700));
            let r = acknowledge(&kv, 7, [8u8; 16], r.generation, 1100)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((r.grace_from_ms, r.promotion_from_ms), (1100, 1100));
        });
    }

    #[test]
    fn a_gateway_stops_recording_only_on_a_fresh_acknowledged_disable() {
        let mut t = TieringRecord {
            enabled: false,
            generation: 3,
            applied_term: 9,
            applied_generation: 3,
            ..TieringRecord::default()
        };
        let l = leader(9, 100_000);
        assert!(confirmed_disabled(Some(&t), Some(&l), 50_000, 0, 1_000));
        assert!(
            !confirmed_disabled(None, Some(&l), 50_000, 0, 1_000),
            "no record"
        );
        assert!(
            !confirmed_disabled(Some(&t), None, 50_000, 0, 1_000),
            "no leader"
        );
        assert!(
            !confirmed_disabled(Some(&t), Some(&leader(10, 100_000)), 50_000, 0, 1_000),
            "new leader"
        );
        assert!(
            !confirmed_disabled(Some(&t), Some(&leader(9, 0)), 50_000, 0, 1_000),
            "released"
        );
        assert!(
            !confirmed_disabled(Some(&t), Some(&leader(9, 50_500)), 50_000, 0, 1_000),
            "lease within the skew"
        );
        assert!(
            !confirmed_disabled(Some(&t), Some(&l), 50_000, CONFIRMATION_STALE_MS + 1, 1_000),
            "stale"
        );
        t.generation = 4;
        assert!(
            !confirmed_disabled(Some(&t), Some(&l), 50_000, 0, 1_000),
            "unacknowledged change"
        );
        t.generation = 3;
        t.enabled = true;
        assert!(
            !confirmed_disabled(Some(&t), Some(&l), 50_000, 0, 1_000),
            "enabled"
        );
    }

    /// A tiering pass's reset is refused once the policy moved on
    /// (ADR-0035 §10.1), and done while it holds.
    #[test]
    fn a_reset_by_a_pass_of_an_earlier_policy_is_refused() {
        futures::executor::block_on(async {
            use crate::ops::object;
            let kv = MemKv::default();
            let g = [5u8; 16];
            let mut tx = kv.begin().await.unwrap();
            tx.set(&keys::gen_header(&g), b"h");
            tx.commit().await.unwrap();
            object::record_access(&kv, &g, 10, 7).await.unwrap();
            seed_if_absent(&kv, &policy(1800), 1).await.unwrap();
            assert!(
                object::reset_access_fenced(&kv, &g, 20, Some(1))
                    .await
                    .unwrap()
            );
            assert_eq!(object::access_of(&kv, &g).await.unwrap().unwrap().count, 0);
            object::record_access(&kv, &g, 30, 3).await.unwrap();
            admin_change(&kv, &AdminChange::SetPolicy(policy(600)), 40)
                .await
                .unwrap();
            assert!(
                !object::reset_access_fenced(&kv, &g, 50, Some(1))
                    .await
                    .unwrap()
            );
            assert_eq!(
                object::access_of(&kv, &g).await.unwrap().unwrap().count,
                3,
                "reads recorded since were erased"
            );
        });
    }

    /// The gate names the schema this build introduced (a later schema
    /// step must not leave it pointing elsewhere).
    #[test]
    fn the_gate_is_the_schema_of_the_tiering_record() {
        assert_eq!(
            TIERING_RECORD_SCHEMA,
            crate::keys::SCHEMA_VERSION - arca_lake_common::compat::BUMP
        );
    }

    #[test]
    fn widening_is_a_shorter_demote_after_or_any_change_of_tiers() {
        let p = policy(1800);
        let mut reordered = p.clone();
        reordered.tiers.reverse();
        assert!(reordered.widens_demotion_from(&p));
        assert!(policy(60).widens_demotion_from(&p));
        assert!(!policy(3600).widens_demotion_from(&p));
        let mut promote_only = p.clone();
        promote_only.promote_reads = 50;
        promote_only.promote_window_ms = 1;
        assert!(!promote_only.widens_demotion_from(&p));
    }

    #[test]
    fn term_zero_never_acknowledges() {
        futures::executor::block_on(async {
            let kv = MemKv::default();
            seed_if_absent(&kv, &policy(1800), 10).await.unwrap();
            assert_eq!(acknowledge(&kv, 0, [1u8; 16], 1, 100).await.unwrap(), None);
            let r = current(&kv).await.unwrap().unwrap();
            assert!(r.pending_enable && r.applied_term == 0);
        });
    }
}
