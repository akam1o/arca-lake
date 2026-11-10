//! The leader's metadata store with a trip wire (Phase 3.11, R-94(b)).
//!
//! A leader decides deletes in one transaction and sends them afterwards;
//! a batch holds no transaction while it sends. If the metadata is
//! replaced meanwhile (a restore: the keyspace cleared, then written back
//! under a database lock), the next transaction of any of the leader's
//! tasks fails, but the batch would go on sending. This store watches
//! every operation of every task: on a failure that means "this metadata
//! is no longer the one these deletes were decided on" it stops the
//! process's deletes at once ([`NodeClients::set_delete_fence`] to `None`,
//! shared by every task of the term), before the batch's next RPC.
//!
//! - [`KvError::ClusterState`]: a phase or incarnation that no longer
//!   admits the work: deletes stop until a health pass reopens them.
//! - [`KvError::Fenced`]: another leader: deletes stop for the rest of the
//!   term and the leader steps down.
//! - [`KvError::Locked`], [`KvError::Replaced`]: the store is being, or
//!   was, replaced: as for `Fenced`, and the process remembers it (sticky,
//!   across terms) so that its next term closes the fence with an
//!   operator hold instead of reopening on what may be restored metadata.
//!
//! Every trip also closes, on the nodes themselves, the generation the
//! stopped deletes carried ([`Trip::stop`]), so that deletes already in
//! flight are refused when they land.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arca_lake_metadata::kv::{Key, KeyRange, KvError, KvStore, Limits, Txn, Value};
use arca_lake_protocol::client::NodeClients;

/// What a trip does, shared by the store and its transactions.
#[derive(Debug)]
pub struct Trip {
    clients: NodeClients,
    suspect: Arc<AtomicBool>,
    trips: AtomicU64,
    step_down: tokio::sync::Notify,
}

impl Trip {
    /// Stop `clients`' deletes on a trip; `suspect` is set (and stays set
    /// until the process acts on it) when the store looks replaced.
    #[must_use]
    pub fn new(clients: NodeClients, suspect: Arc<AtomicBool>) -> Self {
        Self {
            clients,
            suspect,
            trips: AtomicU64::new(0),
            step_down: tokio::sync::Notify::new(),
        }
    }

    /// React to `e`.
    pub fn observe(&self, e: &KvError) {
        let replaced = match e {
            // The phase or incarnation refuses the work: deletes stop until
            // the leader's health pass reopens them (the existing path).
            KvError::ClusterState(_) => {
                self.stop(false, false);
                return;
            }
            KvError::Fenced(_) => false,
            KvError::Locked(_) | KvError::Replaced(_) => true,
            _ => return,
        };
        // Another leader, or a store being replaced: this term is over.
        // Deletes stay stopped for the rest of it and the leader steps
        // down; a later term starts from what the store says then.
        self.stop(true, true);
        if replaced && !self.suspect.swap(true, Ordering::SeqCst) {
            tracing::error!(error = %e, "metadata store locked or replaced under this leader: deletes stopped");
        }
        self.step_down.notify_one();
    }

    /// Resolves once a trip ended the term.
    pub async fn stepped_down(&self) {
        self.step_down.notified().await;
    }

    /// Stop the process's deletes (`latch`: for the rest of the term), and
    /// with `close_nodes` close the generation they carried on every node
    /// this process knows. A node acknowledges a close only after the deletes it already
    /// admitted finished, and refuses every later one of that generation,
    /// so a delete still in flight (sent before the trip, delayed) is
    /// refused too. No metadata is read or written: it works while the
    /// store is locked or empty. Best effort per node; a node that misses
    /// it closes on its own when its rejoin lease ends.
    pub fn stop(&self, latch: bool, close_nodes: bool) {
        let generation = self.clients.stop_deletes(latch);
        self.trips.fetch_add(1, Ordering::Relaxed);
        let Some(generation) = generation.filter(|_| close_nodes) else {
            return;
        };
        let clients = self.clients.clone();
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                for addr in clients.known_addresses() {
                    if let Err(e) = clients
                        .push_delete_fence_with(
                            &addr,
                            generation,
                            false,
                            arca_lake_protocol::client::FencePush::default(),
                        )
                        .await
                    {
                        tracing::warn!(node = %addr, error = %e, "could not close the delete fence after a trip");
                    }
                }
            });
        }
    }

    /// Trips so far.
    #[must_use]
    pub fn trips(&self) -> u64 {
        self.trips.load(Ordering::Relaxed)
    }
}

/// One look at the store for a leader (the store watch, Phase 3.11): a
/// read-only transaction reading the leader record and the cluster state.
/// Both exist for as long as the store serves this cluster under this
/// leader, so either missing means the keyspace was cleared or replaced;
/// a leader record of another holder or term means it was replaced (a
/// restore from another term) or taken over. A locked store fails the
/// reads themselves. Returns what to [`Trip::observe`], if anything.
pub async fn probe<S: KvStore>(
    store: &S,
    leadership: &arca_lake_metadata::ops::leader::Leadership,
) -> Option<KvError> {
    use arca_lake_metadata::keys;
    let read = async {
        let mut tx = store.begin().await?;
        let leader = tx.get(&keys::leader()).await?.is_some();
        let state = tx.get(&keys::cluster_state()).await?.is_some();
        let ours = leader
            && arca_lake_metadata::ops::leader::check_in_tx(&mut tx, leadership)
                .await
                .map_err(|e| KvError::Backend(e.to_string()))?;
        Ok::<_, KvError>((leader, state, ours))
    };
    match read.await {
        Err(e) => Some(e),
        Ok((false, _, _)) => Some(KvError::Replaced(
            "the leader record is gone: the metadata was cleared or replaced".into(),
        )),
        Ok((_, false, _)) => Some(KvError::Replaced(
            "the cluster state is gone: the metadata was cleared or replaced".into(),
        )),
        Ok((true, true, false)) => Some(KvError::Fenced(format!(
            "the leader record no longer names {} at token {}",
            leadership.holder, leadership.token
        ))),
        Ok((true, true, true)) => None,
    }
}

/// A store whose every failure goes through a [`Trip`].
#[derive(Debug)]
pub struct TripKv<S> {
    inner: Arc<S>,
    trip: Arc<Trip>,
}

impl<S> TripKv<S> {
    /// Wrap `inner`.
    #[must_use]
    pub fn new(inner: Arc<S>, trip: Arc<Trip>) -> Self {
        Self { inner, trip }
    }
}

impl<S: KvStore> KvStore for TripKv<S> {
    type Tx = TripTx<S::Tx>;

    async fn begin(&self) -> Result<Self::Tx, KvError> {
        match self.inner.begin().await {
            Ok(inner) => Ok(TripTx {
                inner,
                trip: Arc::clone(&self.trip),
            }),
            Err(e) => {
                self.trip.observe(&e);
                Err(e)
            }
        }
    }

    fn limits(&self) -> Limits {
        self.inner.limits()
    }
}

/// A transaction of a [`TripKv`].
#[derive(Debug)]
pub struct TripTx<T> {
    inner: T,
    trip: Arc<Trip>,
}

impl<T> TripTx<T> {
    fn seen<R>(&self, r: Result<R, KvError>) -> Result<R, KvError> {
        if let Err(e) = &r {
            self.trip.observe(e);
        }
        r
    }
}

impl<T: Txn> Txn for TripTx<T> {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        let r = self.inner.get(key).await;
        self.seen(r)
    }

    async fn get_snapshot(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        let r = self.inner.get_snapshot(key).await;
        self.seen(r)
    }

    async fn atomic_add(&mut self, key: &[u8], delta: i64) -> Result<(), KvError> {
        let r = self.inner.atomic_add(key, delta).await;
        self.seen(r)
    }

    async fn get_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Key, Value)>, KvError> {
        let r = self.inner.get_range(range, limit, reverse).await;
        self.seen(r)
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.inner.set(key, value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.inner.clear(key);
    }

    fn clear_range(&mut self, range: &KeyRange) {
        self.inner.clear_range(range);
    }

    async fn commit(self) -> Result<u64, KvError> {
        let trip = Arc::clone(&self.trip);
        let r = self.inner.commit().await;
        if let Err(e) = &r {
            trip.observe(e);
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trip() -> (NodeClients, Arc<AtomicBool>, Trip) {
        let clients = NodeClients::new(std::time::Duration::from_secs(1));
        clients.set_delete_fence(Some(7));
        let suspect = Arc::new(AtomicBool::new(false));
        let trip = Trip::new(clients.clone(), Arc::clone(&suspect));
        (clients, suspect, trip)
    }

    #[test]
    fn a_replaced_store_latches_deletes_off_for_the_term() {
        for e in [
            KvError::Locked("1038".into()),
            KvError::Replaced("gone".into()),
        ] {
            let (clients, suspect, trip) = trip();
            trip.observe(&e);
            assert_eq!(clients.delete_fence(), None);
            assert!(suspect.load(Ordering::SeqCst), "{e}");
            // A health pass already under way does not reopen them.
            clients.set_delete_fence(Some(7));
            assert_eq!(clients.delete_fence(), None, "{e}");
            assert_eq!(trip.trips(), 1);
        }
    }

    #[test]
    fn another_leader_latches_without_suspecting_a_restore() {
        let (clients, suspect, trip) = trip();
        trip.observe(&KvError::Fenced("token".into()));
        clients.set_delete_fence(Some(8));
        assert_eq!(clients.delete_fence(), None);
        assert!(!suspect.load(Ordering::SeqCst));
    }

    #[test]
    fn a_phase_change_stops_deletes_until_the_next_health_pass() {
        let (clients, suspect, trip) = trip();
        trip.observe(&KvError::ClusterState("read-only".into()));
        assert_eq!(clients.delete_fence(), None);
        clients.set_delete_fence(Some(8));
        assert_eq!(clients.delete_fence(), Some(8));
        assert!(!suspect.load(Ordering::SeqCst));
    }

    #[test]
    fn the_probe_sees_a_cleared_or_replaced_store() {
        use arca_lake_metadata::mem::MemKv;
        use arca_lake_metadata::ops::leader;
        use arca_lake_metadata::{keys, transact};
        futures::executor::block_on(async {
            let kv = MemKv::default();
            let me = leader::try_acquire(&kv, "a", 1_000, 60_000)
                .await
                .unwrap()
                .unwrap();
            // No cluster state yet: as a cleared store.
            assert!(matches!(probe(&kv, &me).await, Some(KvError::Replaced(_))));
            transact(&kv, (), |tx, ()| {
                Box::pin(async move {
                    tx.set(&keys::cluster_state(), b"x");
                    Ok(())
                })
            })
            .await
            .unwrap();
            assert_eq!(probe(&kv, &me).await, None);
            // A leader record of another term (a restore from another one).
            let other = leader::Leadership {
                holder: me.holder.clone(),
                token: me.token + 1,
            };
            assert!(matches!(probe(&kv, &other).await, Some(KvError::Fenced(_))));
            // Cleared.
            transact(&kv, (), |tx, ()| {
                Box::pin(async move {
                    tx.clear_range(&keys::all_range());
                    Ok(())
                })
            })
            .await
            .unwrap();
            assert!(matches!(probe(&kv, &me).await, Some(KvError::Replaced(_))));
        });
    }

    #[test]
    fn transient_errors_change_nothing() {
        let (clients, suspect, trip) = trip();
        for e in [
            KvError::Conflict,
            KvError::CommitUnknown("timeout".into()),
            KvError::Backend("io".into()),
        ] {
            trip.observe(&e);
        }
        assert_eq!(clients.delete_fence(), Some(7));
        assert!(!suspect.load(Ordering::SeqCst));
        assert_eq!(trip.trips(), 0);
    }
}
