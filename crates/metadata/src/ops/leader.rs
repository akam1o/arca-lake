//! Controller leader election on the metadata store (RFC-011 §5.2).
//!
//! One record, `("al","leader")`, names the holder, a monotonically
//! increasing fencing token, and a lease expiry. Acquisition and renewal are
//! single transactions, so the store's serializable isolation is the only
//! consensus used. The token changes only when the holder changes; it is
//! what [`crate::fence::FencedKv`] checks in every transaction a leader
//! issues, so an old leader whose lease expired cannot commit anything
//! after a new leader took over.
//!
//! Wall-clock time is a parameter: the lease protects liveness (a dead
//! leader is replaced after `lease_ms`); safety comes from the token check,
//! which does not depend on clocks.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::{KvStore, Txn};
use crate::ops::{read, write};
use crate::{keys, transact};

/// The leader record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderRecord {
    /// Holder identity (operator-visible, not trusted for fencing).
    pub holder: String,
    /// Fencing token: increases on every change of holder.
    pub token: u64,
    /// Lease expiry, Unix milliseconds.
    pub lease_until_ms: u64,
}

impl crate::records::Record for LeaderRecord {}

/// A held leadership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leadership {
    /// Holder identity.
    pub holder: String,
    /// Fencing token.
    pub token: u64,
}

/// Current record, if any.
///
/// # Errors
///
/// Backend errors.
pub async fn current<S: KvStore>(store: &S) -> Result<Option<LeaderRecord>> {
    transact(store, (), |tx, ()| {
        Box::pin(async move { read(tx, &keys::leader()).await })
    })
    .await
}

/// Try to become leader: succeeds when there is no record, the lease has
/// expired, or `holder` already holds it (renewal keeps the token).
///
/// # Errors
///
/// Backend errors.
pub async fn try_acquire<S: KvStore>(
    store: &S,
    holder: &str,
    now_ms: u64,
    lease_ms: u64,
) -> Result<Option<Leadership>> {
    transact(store, (holder, now_ms, lease_ms), |tx, env| {
        Box::pin(async move {
            let (holder, now_ms, lease_ms) = *env;
            let current: Option<LeaderRecord> = read(tx, &keys::leader()).await?;
            let token = match &current {
                Some(r) if r.holder == holder => r.token,
                Some(r) if r.lease_until_ms > now_ms => return Ok(None),
                Some(r) => r.token + 1,
                None => 1,
            };
            write(
                tx,
                &keys::leader(),
                &LeaderRecord {
                    holder: holder.to_string(),
                    token,
                    lease_until_ms: now_ms + lease_ms,
                },
            )?;
            Ok(Some(Leadership {
                holder: holder.to_string(),
                token,
            }))
        })
    })
    .await
}

/// Extend the lease; `false` when leadership was lost (holder or token
/// changed), in which case the caller must stop acting as leader.
///
/// # Errors
///
/// Backend errors.
pub async fn renew<S: KvStore>(
    store: &S,
    leadership: &Leadership,
    now_ms: u64,
    lease_ms: u64,
) -> Result<bool> {
    transact(store, (leadership, now_ms, lease_ms), |tx, env| {
        Box::pin(async move {
            let (leadership, now_ms, lease_ms) = *env;
            let Some(mut r) = read::<LeaderRecord, _>(tx, &keys::leader()).await? else {
                return Ok(false);
            };
            if r.holder != leadership.holder || r.token != leadership.token {
                return Ok(false);
            }
            r.lease_until_ms = now_ms + lease_ms;
            write(tx, &keys::leader(), &r)?;
            Ok(true)
        })
    })
    .await
}

/// Give up leadership (graceful shutdown): expires the lease immediately if
/// still held. The token is kept so that a successor increments it.
///
/// # Errors
///
/// Backend errors.
pub async fn release<S: KvStore>(store: &S, leadership: &Leadership) -> Result<bool> {
    transact(store, leadership, |tx, leadership| {
        Box::pin(async move {
            let Some(mut r) = read::<LeaderRecord, _>(tx, &keys::leader()).await? else {
                return Ok(false);
            };
            if r.holder != leadership.holder || r.token != leadership.token {
                return Ok(false);
            }
            r.lease_until_ms = 0;
            write(tx, &keys::leader(), &r)?;
            Ok(true)
        })
    })
    .await
}

/// Inside a transaction: whether `leadership` is still the recorded leader
/// (holder and token match). Reading the record puts it in the
/// transaction's conflict set, so a concurrent change of leader conflicts
/// with any commit that relied on this check.
///
/// # Errors
///
/// Backend errors.
pub async fn check_in_tx<X: Txn>(tx: &mut X, leadership: &Leadership) -> Result<bool> {
    let r: Option<LeaderRecord> = read(tx, &keys::leader()).await?;
    Ok(r.is_some_and(|r| r.holder == leadership.holder && r.token == leadership.token))
}
