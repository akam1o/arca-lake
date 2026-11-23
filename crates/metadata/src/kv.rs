//! Transactional key-value abstraction.
//!
//! The interface is deliberately shaped like FoundationDB's: a transaction
//! reads from a snapshot, buffers writes, and commits atomically; the commit
//! fails with [`KvError::Conflict`] if any key the transaction read was
//! modified by a transaction that committed after the snapshot. Keys are
//! byte strings ordered lexicographically. Backends enforce [`Limits`].

use std::future::Future;
use std::pin::Pin;

use crate::MetaError;

/// A key.
pub type Key = Vec<u8>;
/// A value.
pub type Value = Vec<u8>;

/// Half-open key range `[begin, end)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    /// Inclusive start.
    pub begin: Key,
    /// Exclusive end.
    pub end: Key,
}

impl KeyRange {
    /// Range from `begin` (inclusive) to `end` (exclusive).
    #[must_use]
    pub fn new(begin: Key, end: Key) -> Self {
        Self { begin, end }
    }

    /// All keys that start with `prefix`.
    #[must_use]
    pub fn prefix(prefix: &[u8]) -> Self {
        Self {
            begin: prefix.to_vec(),
            end: strinc(prefix),
        }
    }

    /// The single key `key`.
    #[must_use]
    pub fn point(key: &[u8]) -> Self {
        let mut end = key.to_vec();
        end.push(0);
        Self {
            begin: key.to_vec(),
            end,
        }
    }

    /// Whether `key` lies in the range.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.begin.as_slice() && key < self.end.as_slice()
    }

    /// Whether the range is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.begin >= self.end
    }
}

/// The first key greater than every key that starts with `prefix`.
///
/// Trailing `0xFF` bytes are dropped and the last remaining byte is
/// incremented. A prefix consisting only of `0xFF` bytes has no such key
/// within the keyspace; this returns a key of `0xFF` bytes one longer than
/// the prefix, which is adequate for the layouts used here (all keys begin
/// with a tuple type code below `0xFF`).
#[must_use]
pub fn strinc(prefix: &[u8]) -> Key {
    let mut k: Vec<u8> = prefix
        .iter()
        .copied()
        .rev()
        .skip_while(|&b| b == 0xFF)
        .collect();
    k.reverse();
    match k.last_mut() {
        Some(last) => {
            *last += 1;
            k
        }
        None => vec![0xFF; prefix.len() + 1],
    }
}

/// Size limits enforced by a backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum key length in bytes.
    pub max_key_bytes: usize,
    /// Maximum value length in bytes.
    pub max_value_bytes: usize,
    /// Maximum bytes affected by one transaction (keys and values written,
    /// keys read).
    pub max_transaction_bytes: usize,
}

impl Limits {
    /// FoundationDB's documented limits (Known Limitations, v8.0 docs).
    pub const FOUNDATIONDB: Self = Self {
        max_key_bytes: 10_000,
        max_value_bytes: 100_000,
        max_transaction_bytes: 10_000_000,
    };
}

/// Errors from a key-value backend.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KvError {
    /// The transaction read something that was modified after its snapshot.
    /// Retryable.
    #[error("transaction conflict")]
    Conflict,
    /// A size limit was exceeded.
    #[error("{what} of {actual} bytes exceeds the limit of {limit} bytes")]
    TooLarge {
        /// What was too large: "key", "value", or "transaction".
        what: &'static str,
        /// The limit.
        limit: usize,
        /// The actual size.
        actual: usize,
    },
    /// The commit was sent but its outcome is unknown (timeout, lost
    /// connection, or the backend's own "maybe committed" signal). The
    /// transaction may or may not have been applied; a later read decides.
    #[error("commit outcome unknown: {0}")]
    CommitUnknown(String),
    /// The transaction was fenced: the leadership token it relied on is no
    /// longer current (see `fence::FencedKv`). Never retried.
    #[error("fenced: {0}")]
    Fenced(String),
    /// The cluster is not in a state that admits this write, or its
    /// incarnation changed since the process started (RFC-015 §5.6,
    /// `guard::GuardedKv`). Never retried.
    #[error("cluster state: {0}")]
    ClusterState(String),
    /// The store is locked (FoundationDB `database_locked`, 1038: a restore
    /// or a DR switch is writing it; Phase 3.11). Never retried: a process
    /// that sees it must assume its metadata is being replaced.
    #[error("metadata store locked: {0}")]
    Locked(String),
    /// A record that exists for as long as the store serves this cluster is
    /// missing (the leader record under a leader, the cluster state): the
    /// keyspace was cleared or replaced, as before a restore (Phase 3.11).
    /// Never retried.
    #[error("metadata replaced: {0}")]
    Replaced(String),
    /// Any other backend failure.
    #[error("backend error: {0}")]
    Backend(String),
}

/// A transactional key-value store.
pub trait KvStore: Send + Sync {
    /// Transaction type.
    type Tx: Txn;

    /// Begin a transaction reading from the current snapshot.
    fn begin(&self) -> impl Future<Output = Result<Self::Tx, KvError>> + Send;

    /// Limits this backend enforces.
    fn limits(&self) -> Limits;
}

/// One transaction. Reads see the snapshot plus this transaction's own
/// writes. Nothing is visible to others until [`Txn::commit`] succeeds.
pub trait Txn: Send {
    /// Read one key.
    fn get(&mut self, key: &[u8]) -> impl Future<Output = Result<Option<Value>, KvError>> + Send;

    /// Read up to `limit` key-value pairs in `range`, in key order (or
    /// reverse order from the end if `reverse`).
    fn get_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> impl Future<Output = Result<Vec<(Key, Value)>, KvError>> + Send;

    /// Buffer a write. Size violations surface at commit, as in FoundationDB.
    fn set(&mut self, key: &[u8], value: &[u8]);

    /// Buffer a deletion.
    fn clear(&mut self, key: &[u8]);

    /// Buffer deletion of every key in `range`.
    fn clear_range(&mut self, range: &KeyRange);

    /// Commit. Returns the commit version, which increases monotonically.
    fn commit(self) -> impl Future<Output = Result<u64, KvError>> + Send;

    /// Read one key *without* adding it to the transaction's conflict set
    /// (a snapshot read): the commit does not fail if another transaction
    /// writes the key meanwhile. For values that only guide a decision
    /// which a later pass corrects (cache capacity, RFC-006 §5.4).
    fn get_snapshot(
        &mut self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Value>, KvError>> + Send {
        self.get(key)
    }

    /// Read several keys at this transaction's read version: the same reads
    /// as [`Txn::get`] one after another (the default), which a backend may
    /// issue at once (Phase 4.1, ADR-0033: FoundationDB).
    fn get_many(
        &mut self,
        keys: &[Key],
    ) -> impl Future<Output = Result<Vec<Option<Value>>, KvError>> + Send {
        async move {
            let mut out = Vec::with_capacity(keys.len());
            for k in keys {
                out.push(self.get(k).await?);
            }
            Ok(out)
        }
    }

    /// The first pair of each range (`get_range(range, 1, false)`), as
    /// [`Txn::get_many`].
    fn get_first_many(
        &mut self,
        ranges: &[KeyRange],
    ) -> impl Future<Output = Result<Vec<Option<(Key, Value)>>, KvError>> + Send {
        async move {
            let mut out = Vec::with_capacity(ranges.len());
            for r in ranges {
                out.push(self.get_range(r, 1, false).await?.into_iter().next());
            }
            Ok(out)
        }
    }

    /// Add `delta` to the little-endian `i64` at `key` (missing = 0) as an
    /// atomic mutation: no read conflict, and concurrent adds commute.
    /// Backends without atomic mutations fall back to read-modify-write
    /// (which does conflict).
    fn atomic_add(
        &mut self,
        key: &[u8],
        delta: i64,
    ) -> impl Future<Output = Result<(), KvError>> + Send {
        async move {
            let cur = self
                .get(key)
                .await?
                .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
                .map_or(0, i64::from_le_bytes);
            self.set(key, &cur.wrapping_add(delta).to_le_bytes());
            Ok(())
        }
    }
}

/// Maximum attempts for [`transact`].
pub const MAX_ATTEMPTS: usize = 32;

/// The future a transaction body returns: boxed so that the recipe futures
/// are provably `Send` when awaited inside `async_trait` methods (gateway),
/// which async closures with higher-ranked lifetimes cannot guarantee today.
pub type TxFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, MetaError>> + Send + 'a>>;

/// Run `body` in a transaction, retrying on [`KvError::Conflict`].
///
/// `env` is the body's environment, passed back by reference on every
/// attempt with the same lifetime as the transaction, so that the boxed
/// future may borrow from it (a closure capturing references would need
/// them to be `'static` because of the higher-ranked lifetime).
///
/// `body` must be idempotent with respect to its side effects outside the
/// transaction (it may run several times). Returning any error other than a
/// conflict aborts without retry.
///
/// # Errors
///
/// Returns the body's error, a backend error, or
/// [`MetaError::TooManyRetries`] after [`MAX_ATTEMPTS`] conflicts.
pub async fn transact<S, D, T, F>(store: &S, env: D, body: F) -> Result<T, MetaError>
where
    S: KvStore,
    D: Send + Sync,
    F: for<'a> FnMut(&'a mut S::Tx, &'a D) -> TxFuture<'a, T>,
{
    transact_inner(store, env, body, false, None).await
}

/// What a transaction loop does between a conflict and its next attempt
/// (ADR-0041): the caller's wait, typically a jittered backoff, so that
/// writers contending for one key do not retry in lockstep.
pub trait ConflictBackoff: Send + Sync {
    /// Called after the `conflicts`-th conflict (1-based) of one
    /// transaction, before the next attempt (never after the last).
    fn after_conflict(&self, conflicts: u32) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// [`transact`], waiting on `backoff` (if any) between a conflict and the
/// next attempt. The bound on attempts and the errors are those of
/// [`transact`].
///
/// # Errors
///
/// As [`transact`].
pub async fn transact_with_backoff<S, D, T, F>(
    store: &S,
    env: D,
    body: F,
    backoff: Option<&dyn ConflictBackoff>,
) -> Result<T, MetaError>
where
    S: KvStore,
    D: Send + Sync,
    F: for<'a> FnMut(&'a mut S::Tx, &'a D) -> TxFuture<'a, T>,
{
    transact_inner(store, env, body, false, backoff).await
}

/// Unknown-outcome retries allowed by [`transact_idempotent`].
pub const MAX_UNKNOWN_RETRIES: usize = 3;

/// Like [`transact`], but also re-runs `body` when the commit outcome is
/// unknown ([`KvError::CommitUnknown`]), up to [`MAX_UNKNOWN_RETRIES`] times.
///
/// Only for bodies whose re-execution after an actual commit is harmless
/// (writing the same keys to the same values, such as `begin_put` with a
/// pre-chosen generation ID or appending chunk records). Never use it for a
/// body that publishes, retires, or allocates.
///
/// # Errors
///
/// As [`transact`]; a still-unknown outcome after the retries is returned
/// as [`KvError::CommitUnknown`].
pub async fn transact_idempotent<S, D, T, F>(store: &S, env: D, body: F) -> Result<T, MetaError>
where
    S: KvStore,
    D: Send + Sync,
    F: for<'a> FnMut(&'a mut S::Tx, &'a D) -> TxFuture<'a, T>,
{
    transact_inner(store, env, body, true, None).await
}

/// [`transact_idempotent`], waiting on `backoff` (if any) between a
/// conflict and the next attempt (ADR-0041).
///
/// # Errors
///
/// As [`transact_idempotent`].
pub async fn transact_idempotent_with_backoff<S, D, T, F>(
    store: &S,
    env: D,
    body: F,
    backoff: Option<&dyn ConflictBackoff>,
) -> Result<T, MetaError>
where
    S: KvStore,
    D: Send + Sync,
    F: for<'a> FnMut(&'a mut S::Tx, &'a D) -> TxFuture<'a, T>,
{
    transact_inner(store, env, body, true, backoff).await
}

/// Process-wide counters of [`transact`] outcomes (hot-key analysis).
#[derive(Debug, Default)]
pub struct TransactStats {
    /// Transactions started (attempts, including retries).
    pub attempts: std::sync::atomic::AtomicU64,
    /// Attempts that failed with a conflict and were retried.
    pub conflicts: std::sync::atomic::AtomicU64,
    /// Attempts re-run after an unknown commit outcome.
    pub unknown_retries: std::sync::atomic::AtomicU64,
    /// Transactions given up after [`MAX_ATTEMPTS`] conflicts.
    pub exhausted: std::sync::atomic::AtomicU64,
}

/// A copy of [`TransactStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactStatsSnapshot {
    /// See [`TransactStats::attempts`].
    pub attempts: u64,
    /// See [`TransactStats::conflicts`].
    pub conflicts: u64,
    /// See [`TransactStats::unknown_retries`].
    pub unknown_retries: u64,
    /// See [`TransactStats::exhausted`].
    pub exhausted: u64,
}

static TRANSACT_STATS: TransactStats = TransactStats {
    attempts: std::sync::atomic::AtomicU64::new(0),
    conflicts: std::sync::atomic::AtomicU64::new(0),
    unknown_retries: std::sync::atomic::AtomicU64::new(0),
    exhausted: std::sync::atomic::AtomicU64::new(0),
};

/// Snapshot of the process-wide transaction counters.
#[must_use]
pub fn transact_stats() -> TransactStatsSnapshot {
    use std::sync::atomic::Ordering::Relaxed;
    TransactStatsSnapshot {
        attempts: TRANSACT_STATS.attempts.load(Relaxed),
        conflicts: TRANSACT_STATS.conflicts.load(Relaxed),
        unknown_retries: TRANSACT_STATS.unknown_retries.load(Relaxed),
        exhausted: TRANSACT_STATS.exhausted.load(Relaxed),
    }
}

async fn transact_inner<S, D, T, F>(
    store: &S,
    env: D,
    mut body: F,
    retry_unknown: bool,
    backoff: Option<&dyn ConflictBackoff>,
) -> Result<T, MetaError>
where
    S: KvStore,
    D: Send + Sync,
    F: for<'a> FnMut(&'a mut S::Tx, &'a D) -> TxFuture<'a, T>,
{
    use std::sync::atomic::Ordering::Relaxed;
    let mut unknown = 0;
    let mut conflicts = 0u32;
    for attempt in 1..=MAX_ATTEMPTS {
        TRANSACT_STATS.attempts.fetch_add(1, Relaxed);
        let mut tx = store.begin().await?;
        let conflicted = match body(&mut tx, &env).await {
            Ok(value) => match tx.commit().await {
                Ok(_) => return Ok(value),
                Err(KvError::Conflict) => true,
                Err(KvError::CommitUnknown(_))
                    if retry_unknown && unknown < MAX_UNKNOWN_RETRIES =>
                {
                    unknown += 1;
                    TRANSACT_STATS.unknown_retries.fetch_add(1, Relaxed);
                    false
                }
                Err(e) => return Err(e.into()),
            },
            Err(MetaError::Kv(KvError::Conflict)) => true,
            Err(e) => return Err(e),
        };
        if conflicted {
            TRANSACT_STATS.conflicts.fetch_add(1, Relaxed);
            conflicts += 1;
            if let Some(b) = backoff
                && attempt < MAX_ATTEMPTS
            {
                b.after_conflict(conflicts).await;
            }
        }
    }
    TRANSACT_STATS.exhausted.fetch_add(1, Relaxed);
    Err(MetaError::TooManyRetries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strinc_cases() {
        assert_eq!(strinc(b"abc"), b"abd");
        assert_eq!(strinc(b"ab\xff"), b"ac");
        assert_eq!(strinc(b"\xff\xff"), vec![0xFF, 0xFF, 0xFF]);
        assert_eq!(strinc(b""), vec![0xFF]);
    }

    struct Counting(std::sync::Mutex<Vec<u32>>);

    impl ConflictBackoff for Counting {
        fn after_conflict(&self, conflicts: u32) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            self.0.lock().unwrap().push(conflicts);
            Box::pin(async {})
        }
    }

    #[test]
    fn backoff_runs_between_conflicts_and_not_after_the_last() {
        let store = crate::mem::MemKv::default();
        let backoff = Counting(std::sync::Mutex::new(Vec::new()));
        let r: Result<(), _> = futures::executor::block_on(transact_with_backoff(
            &store,
            (),
            |_, ()| Box::pin(async { Err(MetaError::Kv(KvError::Conflict)) }),
            Some(&backoff),
        ));
        assert!(matches!(r, Err(MetaError::TooManyRetries)), "{r:?}");
        let calls = backoff.0.into_inner().unwrap();
        let expected: Vec<u32> = (1..u32::try_from(MAX_ATTEMPTS).unwrap()).collect();
        assert_eq!(calls, expected);

        let backoff = Counting(std::sync::Mutex::new(Vec::new()));
        let mut first = true;
        let r = futures::executor::block_on(transact_with_backoff(
            &store,
            (),
            move |_, ()| {
                let conflict = std::mem::replace(&mut first, false);
                Box::pin(async move {
                    if conflict {
                        Err(MetaError::Kv(KvError::Conflict))
                    } else {
                        Ok(7)
                    }
                })
            },
            Some(&backoff),
        ));
        assert_eq!(r.unwrap(), 7);
        assert_eq!(backoff.0.into_inner().unwrap(), vec![1]);
    }

    #[test]
    fn ranges() {
        let r = KeyRange::prefix(b"k");
        assert!(r.contains(b"k"));
        assert!(r.contains(b"kzzz"));
        assert!(!r.contains(b"l"));
        let p = KeyRange::point(b"k");
        assert!(p.contains(b"k"));
        assert!(!p.contains(b"k\x01"));
        assert!(!p.is_empty());
    }
}
