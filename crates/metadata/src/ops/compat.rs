//! Compatibility and cluster-state records (RFC-014 §5.4–§5.5, RFC-015
//! §5.3, ADR-0016): schema and write gates, the cluster recovery state
//! with its incarnation, process heartbeats, and the upgrade cursor.
//!
//! Every recipe is one transaction. The gate is read-only; the mutating
//! recipes are operator or leader actions and are not themselves guarded
//! (they are what the guard checks against).

use arca_lake_common::compat::{Capabilities, Target};

use crate::error::Result;
use crate::kv::{KvStore, Txn};
use crate::ops::{read, write};
use crate::records::{
    ClusterPhase, ClusterStateRecord, ProcRecord, UpgradeCursor, UpgradeStep, decode,
};
use crate::{MetaError, keys, transact};

/// Lower-case hex of an incarnation.
#[must_use]
pub fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

/// Schema version written by Phase 3.1/3.2 builds, stamped on a store that
/// holds data but no schema key (ADR-0016: those builds wrote version-2
/// records, so `M = 1` would be false).
pub const LEGACY_SCHEMA: u64 = 2;
/// Storage format those builds wrote.
pub const LEGACY_FORMAT: u64 = 1;

fn u64_of(v: Option<Vec<u8>>) -> Option<u64> {
    v.and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
        .map(u64::from_le_bytes)
}

/// Everything the startup gate needs, read in one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gate {
    /// `("al","meta","schema")`, if initialised.
    pub schema: Option<u64>,
    /// `("al","meta","schema_write")`, if initialised.
    pub schema_write: Option<u64>,
    /// `("al","meta","format_write")`, if initialised.
    pub format_write: Option<u64>,
    /// The cluster state record, if initialised.
    pub cluster: Option<ClusterStateRecord>,
    /// Whether any key outside the `("al","meta",…)` subspace exists.
    pub has_data: bool,
    /// Sequence of the incarnation (0: not recorded, an earlier release's
    /// store; nodes then order incarnations as before).
    pub incarnation_seq: u64,
}

/// Read the gate.
///
/// # Errors
///
/// Backend errors.
pub async fn gate<S: KvStore>(store: &S) -> Result<Gate> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let schema = u64_of(tx.get(&keys::schema_version()).await?);
            let schema_write = u64_of(tx.get(&keys::schema_write()).await?);
            let format_write = u64_of(tx.get(&keys::format_write()).await?);
            let cluster = read(tx, &keys::cluster_state()).await?;
            let incarnation_seq = u64_of(tx.get(&keys::incarnation_seq()).await?).unwrap_or(0);
            // A bucket or a generation means the store holds user data
            // written by some release; node and group records alone are a
            // cluster being set up.
            let has_data = !tx
                .get_range(&keys::buckets_range(), 1, false)
                .await?
                .is_empty()
                || !tx
                    .get_range(&keys::all_generations_range(), 1, false)
                    .await?
                    .is_empty();
            Ok(Gate {
                schema,
                schema_write,
                format_write,
                cluster,
                has_data,
                incarnation_seq,
            })
        })
    })
    .await
}

/// Why a process refuses to start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GateError {
    /// The schema is outside this build's window (RFC-014 §5.4).
    #[error("metadata schema M{schema} requires software with M in [{schema}, {}] (this build reads [{min}, {max}])", schema + 1)]
    Schema {
        /// Stored schema.
        schema: u64,
        /// This build's oldest readable schema.
        min: u64,
        /// This build's newest readable schema.
        max: u64,
    },
    /// The store holds data but was never initialised and the caller may
    /// not stamp it.
    #[error(
        "metadata store has data but no schema key; run `admin init` (stamps the legacy versions)"
    )]
    Uninitialised,
}

/// What a starting process may write to an uninitialised store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stamp {
    /// Initialise an empty store; refuse a store with data and no key
    /// (gateways: they are many and start in any order).
    FreshOnly,
    /// Also stamp a store with data and no key at the legacy versions
    /// (controllers and `admin init`).
    Legacy,
}

/// Decide whether a process with `caps` may start on `gate`, initialising
/// or stamping the store as `stamp` allows. Returns the (possibly
/// stamped) gate.
///
/// # Errors
///
/// [`GateError`] wrapped in [`MetaError::InvalidArgument`] when the
/// process must not start; backend errors.
pub async fn check_startup<S: KvStore>(
    store: &S,
    caps: &Capabilities,
    stamp: Stamp,
    who: &str,
    now_ms: u64,
) -> Result<Gate> {
    let mut g = gate(store).await?;
    match g.schema {
        Some(s) if caps.reads_schema(s) => {}
        Some(s) => {
            return Err(MetaError::InvalidArgument(
                GateError::Schema {
                    schema: s,
                    min: caps.schema_min,
                    max: caps.schema_max,
                }
                .to_string(),
            ));
        }
        None if !g.has_data => {
            init(store, caps, who, now_ms).await?;
            g = gate(store).await?;
        }
        None => {
            if stamp != Stamp::Legacy {
                return Err(MetaError::InvalidArgument(
                    GateError::Uninitialised.to_string(),
                ));
            }
            if !caps.reads_schema(LEGACY_SCHEMA) {
                return Err(MetaError::InvalidArgument(
                    GateError::Schema {
                        schema: LEGACY_SCHEMA,
                        min: caps.schema_min,
                        max: caps.schema_max,
                    }
                    .to_string(),
                ));
            }
            stamp_legacy(store, who, now_ms).await?;
            g = gate(store).await?;
        }
    }
    Ok(g)
}

/// Initialise an empty store at this build's versions, `Active`, with a
/// fresh incarnation (`admin init`; `create-groups` on an empty store).
/// Idempotent: an initialised store is returned unchanged.
///
/// # Errors
///
/// Backend errors.
pub async fn init<S: KvStore>(
    store: &S,
    caps: &Capabilities,
    who: &str,
    now_ms: u64,
) -> Result<ClusterStateRecord> {
    let incarnation = *uuid::Uuid::now_v7().as_bytes();
    transact(
        store,
        (
            caps.schema_max,
            caps.format_max,
            incarnation,
            who.to_string(),
            now_ms,
        ),
        |tx, env| {
            Box::pin(async move {
                let (schema, format, incarnation, who, now_ms) = env;
                let existing: Option<ClusterStateRecord> = read(tx, &keys::cluster_state()).await?;
                if let Some(existing) = existing {
                    return Ok(existing);
                }
                if tx.get(&keys::schema_version()).await?.is_none() {
                    tx.set(&keys::schema_version(), &schema.to_le_bytes());
                }
                if tx.get(&keys::schema_write()).await?.is_none() {
                    tx.set(&keys::schema_write(), &schema.to_le_bytes());
                }
                if tx.get(&keys::format_write()).await?.is_none() {
                    tx.set(&keys::format_write(), &format.to_le_bytes());
                }
                let rec = ClusterStateRecord {
                    incarnation: *incarnation,
                    state: ClusterPhase::Active,
                    restored_at_ms: None,
                    restored_from: None,
                    backup_point_ms: None,
                    changed_ms: *now_ms,
                    changed_by: who.clone(),
                };
                if tx.get(&keys::incarnation_seq()).await?.is_none() {
                    tx.set(&keys::incarnation_seq(), &1u64.to_le_bytes());
                }
                if tx.get(&keys::cluster_uid()).await?.is_none() {
                    tx.set(&keys::cluster_uid(), uuid::Uuid::new_v4().as_bytes());
                }
                write(tx, &keys::cluster_state(), &rec)?;
                Ok(rec)
            })
        },
    )
    .await
}

/// Stamp a store that has data but no schema key with the versions the
/// Phase 3.1/3.2 builds wrote (ADR-0016).
///
/// # Errors
///
/// Backend errors.
pub async fn stamp_legacy<S: KvStore>(
    store: &S,
    who: &str,
    now_ms: u64,
) -> Result<ClusterStateRecord> {
    let caps = Capabilities {
        protocol_min: 1,
        protocol_max: 1,
        schema_min: LEGACY_SCHEMA,
        schema_max: LEGACY_SCHEMA,
        format_min: LEGACY_FORMAT,
        format_max: LEGACY_FORMAT,
    };
    init(store, &caps, who, now_ms).await
}

/// The cluster state inside a transaction (also used by the guard).
///
/// # Errors
///
/// Backend or codec errors.
pub async fn cluster_state_in_tx<X: Txn>(tx: &mut X) -> Result<Option<ClusterStateRecord>> {
    read(tx, &keys::cluster_state()).await
}

/// The cluster state.
///
/// # Errors
///
/// Backend errors.
pub async fn cluster_state<S: KvStore>(store: &S) -> Result<Option<ClusterStateRecord>> {
    transact(store, (), |tx, ()| Box::pin(cluster_state_in_tx(tx))).await
}

/// The cluster's uid (Phase 3.12; `None`: initialised by an earlier
/// release and not given one yet, or another value than 16 bytes).
///
/// # Errors
///
/// Backend errors.
pub async fn cluster_uid<S: KvStore>(store: &S) -> Result<Option<[u8; 16]>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            Ok(tx
                .get(&keys::cluster_uid())
                .await?
                .and_then(|v| <[u8; 16]>::try_from(v.as_slice()).ok()))
        })
    })
    .await
}

/// Set the cluster's uid to `uid` if the store has none (Phase 3.12:
/// `admin restore` puts the cluster's uid back on restored metadata of a
/// backup that predates it). Returns the uid the store holds afterwards.
///
/// # Errors
///
/// Backend errors.
pub async fn put_cluster_uid_if_absent<S: KvStore>(store: &S, uid: [u8; 16]) -> Result<[u8; 16]> {
    transact(store, uid, |tx, uid| {
        Box::pin(async move {
            if let Some(v) = tx.get(&keys::cluster_uid()).await?
                && let Ok(held) = <[u8; 16]>::try_from(v.as_slice())
            {
                return Ok(held);
            }
            tx.set(&keys::cluster_uid(), uid);
            Ok(*uid)
        })
    })
    .await
}

/// The cluster's uid, given one now if it has none (a cluster an earlier
/// release initialised). Only on a store holding a cluster state: a
/// cleared store is not given one (it would be another cluster's).
///
/// # Errors
///
/// Backend errors.
pub async fn ensure_cluster_uid<S: KvStore>(store: &S) -> Result<Option<[u8; 16]>> {
    let fresh = *uuid::Uuid::new_v4().as_bytes();
    transact(store, fresh, |tx, fresh| {
        Box::pin(async move {
            if tx.get(&keys::cluster_state()).await?.is_none() {
                return Ok(None);
            }
            if let Some(v) = tx.get(&keys::cluster_uid()).await? {
                return Ok(<[u8; 16]>::try_from(v.as_slice()).ok());
            }
            tx.set(&keys::cluster_uid(), fresh);
            Ok(Some(*fresh))
        })
    })
    .await
}

/// Set the recovery phase, keeping the incarnation (operator transitions
/// `Active` ↔ `ReadOnly`, and the leader's recovery steps).
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] when the record is missing.
pub async fn set_phase<S: KvStore>(
    store: &S,
    phase: ClusterPhase,
    who: &str,
    now_ms: u64,
) -> Result<ClusterStateRecord> {
    transact(store, (phase, who.to_string(), now_ms), |tx, env| {
        Box::pin(async move {
            let (phase, who, now_ms) = env;
            let mut rec: ClusterStateRecord = read(tx, &keys::cluster_state())
                .await?
                .ok_or_else(|| MetaError::InvalidArgument("cluster state record missing".into()))?;
            rec.state = *phase;
            rec.changed_ms = *now_ms;
            rec.changed_by.clone_from(who);
            write(tx, &keys::cluster_state(), &rec)?;
            Ok(rec)
        })
    })
    .await
}

/// Set the phase only if the current phase is `from` (the recovery state
/// machine's transitions). Returns the record and whether it changed.
///
/// # Errors
///
/// Backend errors; a missing record is an error.
pub async fn transition<S: KvStore>(
    store: &S,
    from: ClusterPhase,
    to: ClusterPhase,
    who: &str,
    now_ms: u64,
) -> Result<(ClusterStateRecord, bool)> {
    transact(store, (from, to, who.to_string(), now_ms), |tx, env| {
        Box::pin(async move {
            let (from, to, who, now_ms) = env;
            let mut rec: ClusterStateRecord = read(tx, &keys::cluster_state())
                .await?
                .ok_or_else(|| MetaError::InvalidArgument("cluster state record missing".into()))?;
            if rec.state != *from {
                return Ok((rec, false));
            }
            rec.state = *to;
            rec.changed_ms = *now_ms;
            rec.changed_by.clone_from(who);
            write(tx, &keys::cluster_state(), &rec)?;
            Ok((rec, true))
        })
    })
    .await
}

/// `admin recovery begin`: write a fresh incarnation and `Restoring`
/// (RFC-015 §5.3.1 `restore mark`). Refused unless the cluster is
/// `Active` or `ReadOnly` or `force` is set (a second mark on a cluster
/// already restoring would orphan the first incarnation's catch-up).
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] when refused.
pub async fn mark_restoring<S: KvStore>(
    store: &S,
    restored_from: Option<String>,
    backup_point_ms: Option<u64>,
    who: &str,
    now_ms: u64,
) -> Result<ClusterStateRecord> {
    mark_restoring_above(store, restored_from, backup_point_ms, who, now_ms, 0).await
}

/// [`mark_restoring`] with the new incarnation's sequence above `at_least`
/// (the highest any storage node reported, Phase 3.10).
///
/// # Errors
///
/// Backend errors.
pub async fn mark_restoring_above<S: KvStore>(
    store: &S,
    restored_from: Option<String>,
    backup_point_ms: Option<u64>,
    who: &str,
    now_ms: u64,
    at_least: u64,
) -> Result<ClusterStateRecord> {
    let incarnation = *uuid::Uuid::now_v7().as_bytes();
    transact(
        store,
        (
            incarnation,
            restored_from,
            backup_point_ms,
            who.to_string(),
            now_ms,
            at_least,
        ),
        |tx, env| {
            Box::pin(async move {
                let (incarnation, from, backup_point_ms, who, now_ms, at_least) = env;
                // A managed restore's marker carried by the restored copy (a
                // backup taken while an attempt was paused) belongs to no
                // restore any more: `recovery begin` and the managed mark
                // remove it (re-review).
                tx.clear(&keys::restore_marker());
                let prev: Option<ClusterStateRecord> = read(tx, &keys::cluster_state()).await?;
                let rec = ClusterStateRecord {
                    incarnation: *incarnation,
                    state: ClusterPhase::Restoring,
                    restored_at_ms: Some(*now_ms),
                    restored_from: from.clone(),
                    backup_point_ms: *backup_point_ms,
                    changed_ms: *now_ms,
                    changed_by: who.clone(),
                };
                // Any previous restore cursor and report belong to the old
                // incarnation: the cursor is cleared, reports are kept (keyed by
                // incarnation) for the audit trail.
                tx.clear(&keys::restore_cursor());
                let _ = prev;
                // The incarnation's sequence: above the restored store's and
                // above every node's (`at_least`, collected by `recovery
                // begin`), since the restored counter itself went back.
                let seq = u64_of(tx.get(&keys::incarnation_seq()).await?)
                    .unwrap_or(0)
                    .max(*at_least)
                    .checked_add(1)
                    .ok_or_else(|| {
                        MetaError::InvalidArgument(
                            "incarnation sequence exhausted (a node reported u64::MAX)".into(),
                        )
                    })?;
                tx.set(&keys::incarnation_seq(), &seq.to_le_bytes());
                write(tx, &keys::cluster_state(), &rec)?;
                Ok(rec)
            })
        },
    )
    .await
}

/// Set `schema_write` (finalize step 3) or `format_write` (step 4) or
/// `schema` (step 5). Monotonic: never lowers a value.
///
/// # Errors
///
/// Backend errors.
pub async fn raise_version<S: KvStore>(store: &S, key: VersionKey, value: u64) -> Result<u64> {
    transact(store, (key, value), |tx, env| {
        Box::pin(async move {
            let (key, value) = *env;
            let k = key.key();
            let cur = u64_of(tx.get(&k).await?).unwrap_or(0);
            if value > cur {
                tx.set(&k, &value.to_le_bytes());
                Ok(value)
            } else {
                Ok(cur)
            }
        })
    })
    .await
}

/// Which gate value [`raise_version`] changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VersionKey {
    /// `("al","meta","schema")`.
    Schema,
    /// `("al","meta","schema_write")`.
    SchemaWrite,
    /// `("al","meta","format_write")`.
    FormatWrite,
}

impl VersionKey {
    fn key(self) -> crate::kv::Key {
        match self {
            Self::Schema => keys::schema_version(),
            Self::SchemaWrite => keys::schema_write(),
            Self::FormatWrite => keys::format_write(),
        }
    }
}

// ---- process heartbeats -----------------------------------------------------

/// Write this process's heartbeat (`("al","proc",id)`, RFC-014 §5.5).
///
/// # Errors
///
/// Backend errors.
pub async fn heartbeat<S: KvStore>(store: &S, rec: &ProcRecord) -> Result<()> {
    transact(store, rec, |tx, rec| {
        Box::pin(async move {
            // Not into a store without a cluster state (Phase 3.11 review): a
            // keyspace cleared for a restore stays empty, as `fdbrestore`
            // requires, under processes left running.
            if tx.get(&keys::cluster_state()).await?.is_none() {
                return Ok(());
            }
            write(tx, &keys::proc(&rec.id), *rec)
        })
    })
    .await
}

/// Every heartbeat record.
///
/// # Errors
///
/// Backend errors.
pub async fn procs<S: KvStore>(store: &S) -> Result<Vec<ProcRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move {
            let mut out = Vec::new();
            for (_, v) in tx
                .get_range(&keys::procs_range(), usize::MAX, false)
                .await?
            {
                out.push(decode::<ProcRecord>(&v)?);
            }
            Ok(out)
        })
    })
    .await
}

/// Remove a heartbeat (clean shutdown).
///
/// # Errors
///
/// Backend errors.
pub async fn remove_heartbeat<S: KvStore>(store: &S, id: &str) -> Result<()> {
    transact(store, id.to_string(), |tx, id| {
        Box::pin(async move {
            tx.clear(&keys::proc(id));
            Ok(())
        })
    })
    .await
}

// ---- upgrade cursor ---------------------------------------------------------

/// The upgrade cursor, if any.
///
/// # Errors
///
/// Backend errors.
pub async fn upgrade_cursor<S: KvStore>(store: &S) -> Result<Option<UpgradeCursor>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move { read(tx, &keys::upgrade_cursor()).await })
    })
    .await
}

/// `admin upgrade finalize`: request a finalize to `target`. Refused when
/// a cursor for a different target exists (cancel it first); a cursor for
/// the same target is returned unchanged (idempotent).
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] when refused.
pub async fn request_finalize<S: KvStore>(
    store: &S,
    target: Target,
    ignore_absent: Vec<String>,
    who: &str,
    now_ms: u64,
) -> Result<UpgradeCursor> {
    transact(
        store,
        (target, ignore_absent, who.to_string(), now_ms),
        |tx, env| {
            Box::pin(async move {
                let (target, ignore_absent, who, now_ms) = env;
                let cur: Option<UpgradeCursor> = read(tx, &keys::upgrade_cursor()).await?;
                if let Some(cur) = cur {
                    if cur.target == *target {
                        return Ok(cur);
                    }
                    return Err(MetaError::InvalidArgument(format!(
                        "an upgrade to {:?} is in progress at step {:?}; cancel it first",
                        cur.target, cur.step
                    )));
                }
                let cur = UpgradeCursor {
                    target: *target,
                    step: UpgradeStep::Requested,
                    ignore_absent: ignore_absent.clone(),
                    started_ms: *now_ms,
                    updated_ms: *now_ms,
                    requested_by: who.clone(),
                    by_token: 0,
                    last_error: None,
                    history: vec![format!("{now_ms} requested by {who}")],
                };
                write(tx, &keys::upgrade_cursor(), &cur)?;
                Ok(cur)
            })
        },
    )
    .await
}

/// Advance the cursor from `from` to `to` (compare-and-set on `step`);
/// returns whether it advanced. Each finalize step is one of these.
///
/// # Errors
///
/// Backend errors.
pub async fn advance_upgrade<S: KvStore>(
    store: &S,
    from: UpgradeStep,
    to: UpgradeStep,
    token: u64,
    note: &str,
    now_ms: u64,
) -> Result<bool> {
    advance_upgrade_raising(store, from, to, None, token, note, now_ms).await
}

/// [`advance_upgrade`] that also raises a version gate in the same
/// transaction, only if the cursor is at `from` (Phase 3.5 audit CON-8: a
/// raise in its own transaction could land after a cancellation that saw
/// the cursor before the cut-off, leaving the gate raised and no cursor).
///
/// # Errors
///
/// Backend errors.
pub async fn advance_upgrade_raising<S: KvStore>(
    store: &S,
    from: UpgradeStep,
    to: UpgradeStep,
    raise: Option<(VersionKey, u64)>,
    token: u64,
    note: &str,
    now_ms: u64,
) -> Result<bool> {
    transact(
        store,
        (from, to, raise, token, note.to_string(), now_ms),
        |tx, env| {
            Box::pin(async move {
                let (from, to, raise, token, note, now_ms) = env;
                let cur: Option<UpgradeCursor> = read(tx, &keys::upgrade_cursor()).await?;
                let Some(mut cur) = cur else {
                    return Ok(false);
                };
                if cur.step != *from {
                    return Ok(false);
                }
                if let Some((key, value)) = raise {
                    let k = key.key();
                    let current = u64_of(tx.get(&k).await?).unwrap_or(0);
                    if *value > current {
                        tx.set(&k, &value.to_le_bytes());
                    }
                }
                cur.step = *to;
                cur.updated_ms = *now_ms;
                cur.by_token = *token;
                cur.last_error = None;
                cur.history
                    .push(format!("{now_ms} {from:?} -> {to:?}: {note}"));
                if cur.history.len() > 64 {
                    cur.history.remove(0);
                }
                write(tx, &keys::upgrade_cursor(), &cur)?;
                Ok(true)
            })
        },
    )
    .await
}

/// Record why a step could not be taken (visible in `admin upgrade status`).
///
/// # Errors
///
/// Backend errors.
pub async fn note_upgrade_error<S: KvStore>(store: &S, error: &str, now_ms: u64) -> Result<()> {
    transact(store, (error.to_string(), now_ms), |tx, env| {
        Box::pin(async move {
            let (error, now_ms) = env;
            let cur: Option<UpgradeCursor> = read(tx, &keys::upgrade_cursor()).await?;
            if let Some(mut cur) = cur {
                cur.last_error = Some(error.clone());
                cur.updated_ms = *now_ms;
                write(tx, &keys::upgrade_cursor(), &cur)?;
            }
            Ok(())
        })
    })
    .await
}

/// Remove the cursor (`admin upgrade cancel` before the cut-off, or the
/// last step of finalize).
///
/// # Errors
///
/// [`MetaError::InvalidArgument`] when cancelling past the cut-off.
pub async fn clear_upgrade<S: KvStore>(store: &S, cancel: bool) -> Result<Option<UpgradeCursor>> {
    transact(store, cancel, |tx, cancel| {
        Box::pin(async move {
            let cur: Option<UpgradeCursor> = read(tx, &keys::upgrade_cursor()).await?;
            if let Some(c) = &cur
                && *cancel
                && c.step.past_cut_off()
            {
                return Err(MetaError::InvalidArgument(format!(
                    "upgrade is at {:?}: past the downgrade cut-off (schema_write raised); it cannot be cancelled, only completed",
                    c.step
                )));
            }
            tx.clear(&keys::upgrade_cursor());
            Ok(cur)
        })
    })
    .await
}
