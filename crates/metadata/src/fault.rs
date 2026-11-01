//! Fault injection for any [`KvStore`]: a wrapper whose transactions can
//! report an unknown commit outcome (after or without applying the
//! commit) or fail outright, selected by the keys the transaction wrote.
//!
//! This reproduces, on the in-memory backend and on FoundationDB alike, the
//! situation RFC-003 §5.8 is about: the client sent a commit and lost the
//! answer. Tests use it to drive the resolution path of the gateway and the
//! controller.

use std::sync::{Arc, Mutex, PoisonError};

use crate::kv::{Key, KeyRange, KvError, KvStore, Limits, Txn, Value};

/// What an injected commit does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFault {
    /// Apply the commit, then report [`KvError::CommitUnknown`] (lost reply).
    UnknownAfterApply,
    /// Drop the transaction, then report [`KvError::CommitUnknown`]
    /// (request never reached the store).
    UnknownWithoutApply,
    /// Drop the transaction and report a backend error (store unavailable).
    Unavailable,
}

type Matcher = dyn Fn(&[Key]) -> bool + Send + Sync;

/// One injection rule: applies to the next `remaining` committing
/// transactions whose written keys satisfy `matcher`.
pub struct FaultRule {
    matcher: Arc<Matcher>,
    fault: CommitFault,
    remaining: usize,
}

/// A store wrapper with injectable commit faults.
pub struct FaultKv<S> {
    inner: S,
    rules: Arc<Mutex<Vec<FaultRule>>>,
    fired: Arc<Mutex<Vec<CommitFault>>>,
}

impl<S: std::fmt::Debug> std::fmt::Debug for FaultKv<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultKv")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<S> FaultKv<S> {
    /// Wrap `inner` with no rules.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            rules: Arc::default(),
            fired: Arc::default(),
        }
    }

    /// The wrapped store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Inject `fault` into the next `times` commits that write a key for
    /// which `matcher` returns true.
    pub fn inject(
        &self,
        matcher: impl Fn(&[Key]) -> bool + Send + Sync + 'static,
        fault: CommitFault,
        times: usize,
    ) {
        self.rules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(FaultRule {
                matcher: Arc::new(matcher),
                fault,
                remaining: times,
            });
    }

    /// Inject `fault` into the next `times` commits that write `key`.
    pub fn inject_on_key(&self, key: Key, fault: CommitFault, times: usize) {
        self.inject(move |written| written.contains(&key), fault, times);
    }

    /// Faults fired so far, in order.
    pub fn fired(&self) -> Vec<CommitFault> {
        self.fired
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Remove every rule.
    pub fn clear_rules(&self) {
        self.rules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

impl<S: KvStore> KvStore for FaultKv<S> {
    type Tx = FaultTx<S::Tx>;

    async fn begin(&self) -> Result<Self::Tx, KvError> {
        Ok(FaultTx {
            inner: self.inner.begin().await?,
            written: Vec::new(),
            rules: Arc::clone(&self.rules),
            fired: Arc::clone(&self.fired),
        })
    }

    fn limits(&self) -> Limits {
        self.inner.limits()
    }
}

/// A transaction that records written keys and consults the rules at commit.
pub struct FaultTx<T> {
    inner: T,
    written: Vec<Key>,
    rules: Arc<Mutex<Vec<FaultRule>>>,
    fired: Arc<Mutex<Vec<CommitFault>>>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for FaultTx<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultTx")
            .field("inner", &self.inner)
            .field("written", &self.written.len())
            .finish_non_exhaustive()
    }
}

impl<T: Txn> Txn for FaultTx<T> {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.inner.get(key).await
    }

    async fn get_snapshot(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.inner.get_snapshot(key).await
    }

    async fn atomic_add(&mut self, key: &[u8], delta: i64) -> Result<(), KvError> {
        self.written.push(key.to_vec());
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
        self.written.push(key.to_vec());
        self.inner.set(key, value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.written.push(key.to_vec());
        self.inner.clear(key);
    }

    fn clear_range(&mut self, range: &KeyRange) {
        self.written.push(range.begin.clone());
        self.inner.clear_range(range);
    }

    async fn commit(self) -> Result<u64, KvError> {
        let fault = {
            let mut rules = self.rules.lock().unwrap_or_else(PoisonError::into_inner);
            let hit = rules
                .iter_mut()
                .find(|r| r.remaining > 0 && (r.matcher)(&self.written));
            hit.map(|r| {
                r.remaining -= 1;
                r.fault
            })
        };
        let Some(fault) = fault else {
            return self.inner.commit().await;
        };
        self.fired
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(fault);
        match fault {
            CommitFault::UnknownAfterApply => {
                self.inner.commit().await?;
                Err(KvError::CommitUnknown(
                    "injected: reply lost after apply".into(),
                ))
            }
            CommitFault::UnknownWithoutApply => Err(KvError::CommitUnknown(
                "injected: request lost before apply".into(),
            )),
            CommitFault::Unavailable => Err(KvError::Backend("injected: store unavailable".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemKv;

    #[test]
    fn faults_fire_once_per_count_and_only_on_matching_keys() {
        futures::executor::block_on(async {
            let kv = FaultKv::new(MemKv::default());
            kv.inject_on_key(b"a".to_vec(), CommitFault::UnknownAfterApply, 1);
            kv.inject_on_key(b"b".to_vec(), CommitFault::UnknownWithoutApply, 1);

            let mut tx = kv.begin().await.unwrap();
            tx.set(b"a", b"1");
            assert!(matches!(tx.commit().await, Err(KvError::CommitUnknown(_))));
            let mut tx = kv.begin().await.unwrap();
            assert_eq!(tx.get(b"a").await.unwrap(), Some(b"1".to_vec()), "applied");

            let mut tx = kv.begin().await.unwrap();
            tx.set(b"b", b"1");
            assert!(matches!(tx.commit().await, Err(KvError::CommitUnknown(_))));
            let mut tx = kv.begin().await.unwrap();
            assert_eq!(tx.get(b"b").await.unwrap(), None, "not applied");

            // Rules are consumed; unrelated keys never fault.
            let mut tx = kv.begin().await.unwrap();
            tx.set(b"a", b"2");
            tx.set(b"c", b"1");
            tx.commit().await.unwrap();
            assert_eq!(
                kv.fired(),
                vec![
                    CommitFault::UnknownAfterApply,
                    CommitFault::UnknownWithoutApply
                ]
            );
        });
    }
}
