//! A [`KvStore`] wrapper that checks the cluster recovery state and
//! incarnation inside every transaction that writes (RFC-015 §5.6,
//! ADR-0016).
//!
//! The cluster-state record is read in the commit of a transaction that
//! buffered a write, so the read is part of the conflict set: a restore
//! that changes the incarnation, or an operator that flips the state,
//! after the read but before this commit makes the commit conflict and
//! retry, where the check then fails with [`KvError::ClusterState`] (never
//! retried). Read-only transactions are not touched. A process keeps the
//! incarnation it started with: after a metadata restore every old process
//! is fenced out of every write until it is restarted on the restored
//! store.

use std::sync::Arc;

use crate::kv::{Key, KeyRange, KvError, KvStore, Limits, Txn, Value};
use crate::ops::compat;
use crate::records::ClusterStateRecord;

/// What a wrapper's transactions do, deciding which states admit them
/// (RFC-015 §5.3 table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpClass {
    /// Client PUT, DELETE, Copy, multipart operations, bucket changes.
    ClientWrite,
    /// GC finish, reconcile delete, eviction, tiering switch, multipart
    /// sweep: anything that removes or moves data.
    Delete,
    /// Repair writes (member replacement, stripe repair, cache repair).
    Repair,
    /// Restore verification and its report and cursor.
    Verification,
    /// Operator commands and the recovery/upgrade state itself.
    Admin,
    /// Low-value bookkeeping that is safe in any state but should not run
    /// while restoring (access counters, scrub cursors, heartbeats).
    Bookkeeping,
}

/// Expectation every write of a wrapper is checked against.
#[derive(Debug, Clone)]
pub struct Expectation {
    /// Incarnation the process started with.
    pub incarnation: [u8; 16],
    /// Operation class of the wrapper.
    pub class: OpClass,
}

/// A guarded store.
pub struct GuardedKv<S> {
    inner: Arc<S>,
    expect: Arc<Expectation>,
}

impl<S: std::fmt::Debug> std::fmt::Debug for GuardedKv<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuardedKv")
            .field("expect", &self.expect)
            .finish_non_exhaustive()
    }
}

impl<S> GuardedKv<S> {
    /// Wrap `inner`: every writing transaction requires the cluster to be
    /// at `incarnation` and in a state that admits `class`.
    pub fn new(inner: Arc<S>, incarnation: [u8; 16], class: OpClass) -> Self {
        Self {
            inner,
            expect: Arc::new(Expectation { incarnation, class }),
        }
    }

    /// The wrapped store.
    pub fn inner(&self) -> &Arc<S> {
        &self.inner
    }

    /// The expectation.
    pub fn expectation(&self) -> &Expectation {
        &self.expect
    }
}

impl<S: KvStore> KvStore for GuardedKv<S> {
    type Tx = GuardedTx<S::Tx>;

    async fn begin(&self) -> Result<Self::Tx, KvError> {
        Ok(GuardedTx {
            inner: self.inner.begin().await?,
            expect: Arc::clone(&self.expect),
            dirty: false,
        })
    }

    fn limits(&self) -> Limits {
        self.inner.limits()
    }
}

/// A transaction that verifies the cluster state at commit when it wrote.
pub struct GuardedTx<T> {
    inner: T,
    expect: Arc<Expectation>,
    dirty: bool,
}

impl<T: std::fmt::Debug> std::fmt::Debug for GuardedTx<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuardedTx")
            .field("dirty", &self.dirty)
            .finish_non_exhaustive()
    }
}

/// Check `state` against `expect`; the error message names the cause.
///
/// # Errors
///
/// [`KvError::ClusterState`] on an incarnation or state mismatch.
pub fn check(state: Option<&ClusterStateRecord>, expect: &Expectation) -> Result<(), KvError> {
    let Some(s) = state else {
        // No record at all: the store was never initialised (or the key was
        // lost). Only administration may write.
        return if expect.class == OpClass::Admin {
            Ok(())
        } else {
            Err(KvError::ClusterState(
                "cluster state record missing; run `admin init`".into(),
            ))
        };
    };
    if s.incarnation != expect.incarnation {
        return Err(KvError::ClusterState(format!(
            "cluster incarnation changed (process started under {}, store is at {}); restart the process",
            compat::hex(&expect.incarnation),
            compat::hex(&s.incarnation)
        )));
    }
    if !s.state.admits(expect.class) {
        return Err(KvError::ClusterState(format!(
            "cluster is {:?}: {:?} operations are not admitted",
            s.state, expect.class
        )));
    }
    Ok(())
}

impl<T: Txn> Txn for GuardedTx<T> {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.inner.get(key).await
    }

    async fn get_snapshot(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.inner.get_snapshot(key).await
    }

    async fn atomic_add(&mut self, key: &[u8], delta: i64) -> Result<(), KvError> {
        self.dirty = true;
        self.inner.atomic_add(key, delta).await
    }

    async fn get_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Key, Value)>, KvError> {
        self.inner.get_range(range, limit, reverse).await
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.dirty = true;
        self.inner.set(key, value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.dirty = true;
        self.inner.clear(key);
    }

    fn clear_range(&mut self, range: &KeyRange) {
        self.dirty = true;
        self.inner.clear_range(range);
    }

    async fn commit(mut self) -> Result<u64, KvError> {
        if self.dirty {
            let state = compat::cluster_state_in_tx(&mut self.inner)
                .await
                .map_err(|e| KvError::Backend(e.to_string()))?;
            check(state.as_ref(), &self.expect)?;
        }
        self.inner.commit().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemKv;
    use crate::records::ClusterPhase;

    #[test]
    fn writes_are_admitted_by_state_and_incarnation_reads_are_free() {
        futures::executor::block_on(async {
            let base = Arc::new(MemKv::default());
            let caps = arca_lake_common::compat::Capabilities {
                protocol_min: 1,
                protocol_max: 1,
                schema_min: 1,
                schema_max: 2,
                format_min: 1,
                format_max: 1,
            };
            let st = compat::init(base.as_ref(), &caps, "test", 1_000)
                .await
                .unwrap();
            let client = GuardedKv::new(Arc::clone(&base), st.incarnation, OpClass::ClientWrite);
            let mut tx = client.begin().await.unwrap();
            tx.set(b"x", b"1");
            tx.commit().await.unwrap();

            compat::set_phase(base.as_ref(), ClusterPhase::ReadOnly, "test", 2_000)
                .await
                .unwrap();
            let mut tx = client.begin().await.unwrap();
            tx.set(b"x", b"2");
            assert!(matches!(tx.commit().await, Err(KvError::ClusterState(_))));
            // A read-only transaction commits regardless of the state.
            let mut tx = client.begin().await.unwrap();
            assert_eq!(tx.get(b"x").await.unwrap(), Some(b"1".to_vec()));
            tx.commit().await.unwrap();
            // Repair is admitted in ReadOnly; deletes are not.
            let repair = GuardedKv::new(Arc::clone(&base), st.incarnation, OpClass::Repair);
            let mut tx = repair.begin().await.unwrap();
            tx.set(b"r", b"1");
            tx.commit().await.unwrap();
            let del = GuardedKv::new(Arc::clone(&base), st.incarnation, OpClass::Delete);
            let mut tx = del.begin().await.unwrap();
            tx.clear(b"x");
            assert!(matches!(tx.commit().await, Err(KvError::ClusterState(_))));

            // A new incarnation fences every old wrapper, whatever its class.
            compat::set_phase(base.as_ref(), ClusterPhase::Active, "test", 3_000)
                .await
                .unwrap();
            let fresh =
                compat::mark_restoring(base.as_ref(), Some("backup".into()), None, "test", 4_000)
                    .await
                    .unwrap();
            assert_ne!(fresh.incarnation, st.incarnation);
            compat::set_phase(base.as_ref(), ClusterPhase::Active, "test", 5_000)
                .await
                .unwrap();
            for class in [OpClass::ClientWrite, OpClass::Repair, OpClass::Admin] {
                let old = GuardedKv::new(Arc::clone(&base), st.incarnation, class);
                let mut tx = old.begin().await.unwrap();
                tx.set(b"y", b"1");
                assert!(
                    matches!(tx.commit().await, Err(KvError::ClusterState(_))),
                    "{class:?}"
                );
            }
            let new = GuardedKv::new(Arc::clone(&base), fresh.incarnation, OpClass::ClientWrite);
            let mut tx = new.begin().await.unwrap();
            tx.set(b"y", b"1");
            tx.commit().await.unwrap();
        });
    }
}
