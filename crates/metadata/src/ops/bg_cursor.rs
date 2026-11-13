//! Persisted cursors of the leader's background sweeps (Phase 3.14):
//! reconciliation of nodes' shards and tiering. A pass resumes where the
//! last one (this leader's or another's) stopped, does a bounded amount of
//! work and records where it stopped; at the end of the key space it wraps
//! and counts a full sweep. Under `("al","bgcursor",name)`, which earlier
//! releases never read (they sweep from the start every pass, as before).
//!
//! A cursor is a position, never a claim that the work before it was done
//! at any particular time: a stale or lost cursor only moves where the next
//! pass starts. Positions name keys in the sweep's own order; a key gone
//! since is passed over (the next pass starts after it), a key added before
//! it is reached at the next wrap.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kv::KvStore;
use crate::ops::{read, write};
use crate::records::Record;
use crate::{keys, transact};

/// `("al","bgcursor",name)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BackgroundCursor {
    /// Where the next pass starts (after this position; empty: the
    /// beginning). Its meaning is the sweep's.
    pub after: Vec<u8>,
    /// Full sweeps completed (wraps).
    pub sweeps: u64,
    /// When the last full sweep completed, unix ms (0: never).
    pub last_sweep_ms: u64,
    /// When the current sweep began, unix ms.
    pub sweep_started_ms: u64,
    /// When the last pass ended, unix ms.
    pub last_pass_ms: u64,
    /// Items the last pass handled, and of those how many failed (each
    /// failure logged and passed over; retried at the next sweep).
    pub last_pass_items: u64,
    /// See [`Self::last_pass_items`].
    pub last_pass_errors: u64,
}

impl Record for BackgroundCursor {}

/// The cursor `name`, or a fresh one.
///
/// # Errors
///
/// Backend errors.
pub async fn get<S: KvStore>(store: &S, name: &str) -> Result<BackgroundCursor> {
    transact(store, name, |tx, name| {
        Box::pin(async move { Ok(read(tx, &keys::bg_cursor(name)).await?.unwrap_or_default()) })
    })
    .await
}

/// Write the cursor `name`.
///
/// # Errors
///
/// Backend errors; `Fenced` for a former leader.
pub async fn put<S: KvStore>(store: &S, name: &str, c: &BackgroundCursor) -> Result<()> {
    transact(store, (name, c), |tx, (name, c)| {
        Box::pin(async move { write(tx, &keys::bg_cursor(name), *c) })
    })
    .await
}

/// Every cursor, by name (reports).
///
/// # Errors
///
/// Backend errors.
pub async fn list<S: KvStore>(store: &S) -> Result<Vec<(String, BackgroundCursor)>> {
    let mut out = Vec::new();
    for name in NAMES {
        let c = get(store, name).await?;
        if c != BackgroundCursor::default() {
            out.push(((*name).to_string(), c));
        }
    }
    Ok(out)
}

/// The sweeps that keep a cursor.
pub const NAMES: &[&str] = &[RECONCILE, TIERING];
/// Reconciliation: positions are `(group id, node id)`, big-endian.
pub const RECONCILE: &str = "reconcile";
/// Tiering: positions are generation ids.
pub const TIERING: &str = "tiering";

/// A `(group, node)` reconciliation position.
#[must_use]
pub fn pair_position(group: u64, node: u64) -> Vec<u8> {
    let mut v = group.to_be_bytes().to_vec();
    v.extend_from_slice(&node.to_be_bytes());
    v
}

/// The `(group, node)` of a reconciliation position (`None`: the start,
/// or a position of another shape).
#[must_use]
pub fn parse_pair(p: &[u8]) -> Option<(u64, u64)> {
    let g: [u8; 8] = p.get(..8)?.try_into().ok()?;
    let n: [u8; 8] = p.get(8..16)?.try_into().ok()?;
    (p.len() == 16).then(|| (u64::from_be_bytes(g), u64::from_be_bytes(n)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_positions_round_trip_and_order() {
        assert_eq!(parse_pair(&pair_position(3, 9)), Some((3, 9)));
        assert!(pair_position(3, 9) < pair_position(3, 10));
        assert!(pair_position(3, u64::MAX) < pair_position(4, 0));
        assert_eq!(parse_pair(&[]), None);
        assert_eq!(parse_pair(&[1, 2, 3]), None);
    }
}
