//! Races on the tiering record (ADR-0035 §10.1, §10.5): seeding by two
//! controllers, a seed by a deposed leader against its successor's, a seed
//! against an admin change, and an acknowledgment against an admin change.
//!
//! Each race is forced, not left to timing: a test-only wrapper holds the
//! first commit of a participant after its reads, until the test lets it go,
//! so that the other participant reads and commits in between (or both have
//! read before either commits). The wrapper logs every attempt and commit
//! outcome; the tests assert that the held participant's first commit
//! conflicted and that its retry saw the record the other wrote. Seeding
//! by two leaders at once cannot happen in a deployment (only the leader
//! seeds, through its fenced store); the unfenced race is the metadata
//! operation's own guarantee, the fenced one the deployment's.
//!
//! Runs on the in-memory store always and, with `--features fdb`,
//! `ARCA_FDB_TESTS=1`, `ARCA_FDB_CLUSTER_FILE` and `ARCA_FDB_CLEAR_OK` set
//! to the same disposable cluster file (its keyspace is cleared), on
//! FoundationDB.

#![allow(clippy::too_many_lines, clippy::similar_names)]

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arca_lake_metadata::fence::FencedKv;
use arca_lake_metadata::kv::{Key, KeyRange, KvError, KvStore, Limits, Txn, Value};
use arca_lake_metadata::mem::MemKv;
use arca_lake_metadata::ops::leader;
use arca_lake_metadata::ops::tiering::{
    self, AdminChange, TieringPolicyRecord, TieringRecord, seed_if_absent,
};
use arca_lake_metadata::{MetaError, keys};
use futures::channel::oneshot;

fn run<T>(f: impl Future<Output = T>) -> T {
    futures::executor::block_on(f)
}

/// The events of one race, in order.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    fn push(&self, e: String) {
        println!("  {e}");
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(e);
    }
    fn events(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// A gate on the first commit of one participant: it signals `reached`
/// once its reads are done and waits for `go`.
struct Hold {
    reached: oneshot::Sender<()>,
    go: oneshot::Receiver<()>,
}

/// The test side of a [`Hold`].
struct HoldHandle {
    reached: oneshot::Receiver<()>,
    go: oneshot::Sender<()>,
}

impl HoldHandle {
    async fn reached(&mut self) {
        (&mut self.reached).await.expect("participant gone");
    }
    fn release(self) {
        let _ = self.go.send(());
    }
}

fn hold() -> (Hold, HoldHandle) {
    let (rt, rr) = oneshot::channel();
    let (gt, gr) = oneshot::channel();
    (
        Hold {
            reached: rt,
            go: gr,
        },
        HoldHandle {
            reached: rr,
            go: gt,
        },
    )
}

/// A store whose transactions are logged under `name` and whose first
/// commit may be held.
struct HeldKv<S> {
    inner: Arc<S>,
    name: &'static str,
    log: Log,
    attempts: Arc<AtomicUsize>,
    hold: Arc<Mutex<Option<Hold>>>,
}

impl<S> HeldKv<S> {
    fn new(inner: Arc<S>, name: &'static str, log: &Log, hold: Option<Hold>) -> Self {
        Self {
            inner,
            name,
            log: log.clone(),
            attempts: Arc::new(AtomicUsize::new(0)),
            hold: Arc::new(Mutex::new(hold)),
        }
    }
    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

struct HeldTx<T> {
    inner: T,
    name: &'static str,
    attempt: usize,
    log: Log,
    hold: Option<Hold>,
}

impl<S: KvStore> KvStore for HeldKv<S> {
    type Tx = HeldTx<S::Tx>;

    async fn begin(&self) -> Result<Self::Tx, KvError> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        self.log
            .push(format!("{} attempt {attempt}: begin", self.name));
        Ok(HeldTx {
            inner: self.inner.begin().await?,
            name: self.name,
            attempt,
            log: self.log.clone(),
            hold: self
                .hold
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        })
    }

    fn limits(&self) -> Limits {
        self.inner.limits()
    }
}

impl<T: Txn> Txn for HeldTx<T> {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Value>, KvError> {
        self.inner.get(key).await
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
        self.inner.set(key, value);
    }

    fn clear(&mut self, key: &[u8]) {
        self.inner.clear(key);
    }

    fn clear_range(&mut self, range: &KeyRange) {
        self.inner.clear_range(range);
    }

    async fn commit(self) -> Result<u64, KvError> {
        let Self {
            inner,
            name,
            attempt,
            log,
            hold,
        } = self;
        if let Some(h) = hold {
            log.push(format!("{name} attempt {attempt}: reads done, commit held"));
            let _ = h.reached.send(());
            let _ = h.go.await;
            log.push(format!("{name} attempt {attempt}: commit released"));
        }
        let r = inner.commit().await;
        let outcome = match &r {
            Ok(_) => "committed".to_string(),
            Err(KvError::Conflict) => "conflict (retried)".to_string(),
            Err(e) => format!("error {e}"),
        };
        log.push(format!("{name} attempt {attempt}: {outcome}"));
        r
    }
}

fn policy(promote_reads: u64, demote_s: u64) -> TieringPolicyRecord {
    TieringPolicyRecord {
        tiers: vec!["default".into(), "fast".into()],
        promote_reads,
        promote_window_ms: 600_000,
        demote_after_ms: demote_s * 1000,
    }
}

/// FoundationDB, from an empty keyspace, when configured.
#[cfg(feature = "fdb")]
fn fdb() -> Option<arca_lake_metadata::fdb::FdbKv> {
    // The keyspace is cleared: only a cluster named explicitly, confirmed
    // as disposable by naming it again in ARCA_FDB_CLEAR_OK, and never the
    // system default.
    let (Some(_), Ok(path), Ok(ok)) = (
        std::env::var_os("ARCA_FDB_TESTS"),
        std::env::var("ARCA_FDB_CLUSTER_FILE"),
        std::env::var("ARCA_FDB_CLEAR_OK"),
    ) else {
        eprintln!(
            "ARCA_FDB_TESTS, ARCA_FDB_CLUSTER_FILE or ARCA_FDB_CLEAR_OK not set; the race ran on the in-memory store only"
        );
        return None;
    };
    assert!(
        ok == path && path != "/etc/foundationdb/fdb.cluster",
        "ARCA_FDB_CLEAR_OK must name the disposable cluster file itself (not the system default)"
    );
    let kv = arca_lake_metadata::fdb::FdbKv::open(Some(&path)).expect("open FoundationDB");
    run(kv.clear_range_blocking(&KeyRange::prefix(&foundationdb_tuple::pack(&("al",))))).unwrap();
    Some(kv)
}

/// Runs `race` on the in-memory store and, when configured, FoundationDB.
macro_rules! on_each_store {
    ($race:ident) => {
        println!("{} on the in-memory store:", stringify!($race));
        run($race(Arc::new(MemKv::default())));
        #[cfg(feature = "fdb")]
        if let Some(kv) = fdb() {
            println!("{} on FoundationDB:", stringify!($race));
            run($race(Arc::new(kv)));
        }
    };
}

async fn record<S: KvStore>(kv: &S) -> Option<TieringRecord> {
    tiering::current(kv).await.unwrap()
}

/// Both controllers read "no record" before either commits; one seed
/// commits, the other conflicts, retries, sees the record and writes
/// nothing: the record is the winner's policy, whole.
async fn two_seeds_from_different_flags<S: KvStore + 'static>(kv: Arc<S>) {
    let log = Log::default();
    let (ha, mut a_hold) = hold();
    let (hb, mut b_hold) = hold();
    let a = HeldKv::new(Arc::clone(&kv), "seed-a", &log, Some(ha));
    let b = HeldKv::new(Arc::clone(&kv), "seed-b", &log, Some(hb));
    let (pa, pb) = (policy(5, 1800), policy(9, 60));
    let order = async {
        a_hold.reached().await;
        b_hold.reached().await;
        log.push("both read no record; a released first".into());
        a_hold.release();
        // b goes once a has finished (a's outcome logged).
        while !log
            .events()
            .iter()
            .any(|e| e.starts_with("seed-a attempt 1: committed"))
        {
            yield_now().await;
        }
        b_hold.release();
    };
    let (ra, rb, ()) = futures::join!(
        seed_if_absent(&a, &pa, 10),
        seed_if_absent(&b, &pb, 11),
        order
    );
    assert_eq!((ra.unwrap(), rb.unwrap()), (true, false));
    assert_eq!((a.attempts(), b.attempts()), (1, 2), "{:?}", log.events());
    assert!(
        log.events()
            .contains(&"seed-b attempt 1: conflict (retried)".to_string()),
        "the race did not conflict: {:?}",
        log.events()
    );
    let r = record(kv.as_ref()).await.unwrap();
    assert_eq!((r.enabled, r.generation, &r.policy), (true, 1, &pa));
}

/// The same with equal flags: still one write (generation 1, written once).
async fn two_seeds_from_equal_flags<S: KvStore + 'static>(kv: Arc<S>) {
    let log = Log::default();
    let (ha, mut a_hold) = hold();
    let (hb, mut b_hold) = hold();
    let a = HeldKv::new(Arc::clone(&kv), "seed-a", &log, Some(ha));
    let b = HeldKv::new(Arc::clone(&kv), "seed-b", &log, Some(hb));
    let p = policy(5, 1800);
    let order = async {
        a_hold.reached().await;
        b_hold.reached().await;
        b_hold.release();
        while !log
            .events()
            .iter()
            .any(|e| e.starts_with("seed-b attempt 1: committed"))
        {
            yield_now().await;
        }
        a_hold.release();
    };
    let (ra, rb, ()) = futures::join!(
        seed_if_absent(&a, &p, 20),
        seed_if_absent(&b, &p, 21),
        order
    );
    assert_eq!((ra.unwrap(), rb.unwrap()), (false, true));
    assert_eq!((a.attempts(), b.attempts()), (2, 1), "{:?}", log.events());
    let r = record(kv.as_ref()).await.unwrap();
    assert_eq!((r.generation, r.changed_ms), (1, 21), "written twice");
}

/// A deposed leader read "no record" under its term; its successor takes
/// the lease and seeds; the deposed leader's commit conflicts and its
/// retry is fenced: it never writes. (The deployment's guarantee: only
/// the leader seeds.)
async fn a_deposed_leaders_seed_is_fenced<S: KvStore + 'static>(kv: Arc<S>) {
    let log = Log::default();
    // A cluster state, so that leadership can be taken (initialised store).
    arca_lake_metadata::transact(kv.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.set(&keys::cluster_state(), b"x");
            Ok(())
        })
    })
    .await
    .unwrap();
    let now = arca_lake_metadata_now();
    let old = leader::try_acquire(kv.as_ref(), "ctl-old", now, 300)
        .await
        .unwrap()
        .expect("first leader");
    let (h_old, mut old_hold) = hold();
    let old_store = FencedKv::new(
        Arc::new(HeldKv::new(
            Arc::clone(&kv),
            "old-leader",
            &log,
            Some(h_old),
        )),
        old.clone(),
    );
    let (p_old, p_new) = (policy(5, 1800), policy(9, 60));
    let order = async {
        old_hold.reached().await;
        // The lease runs out; the successor takes it and seeds.
        while arca_lake_metadata_now() <= now + 400 {
            yield_now().await;
        }
        let new = leader::try_acquire(kv.as_ref(), "ctl-new", arca_lake_metadata_now(), 60_000)
            .await
            .unwrap()
            .expect("successor");
        log.push(format!(
            "successor took term {} (old term {})",
            new.token, old.token
        ));
        let new_store = FencedKv::new(
            Arc::new(HeldKv::new(Arc::clone(&kv), "new-leader", &log, None)),
            new,
        );
        assert!(seed_if_absent(&new_store, &p_new, 30).await.unwrap());
        old_hold.release();
    };
    let (r_old, ()) = futures::join!(seed_if_absent(&old_store, &p_old, 31), order);
    log.push(format!("old-leader result: {r_old:?}"));
    match r_old {
        Err(MetaError::Kv(KvError::Fenced(_))) => {}
        other => panic!(
            "the deposed leader's seed returned {other:?}: {:?}",
            log.events()
        ),
    }
    let events = log.events();
    assert!(
        events.contains(&"old-leader attempt 1: conflict (retried)".to_string()),
        "the race did not conflict: {events:?}"
    );
    let r = record(kv.as_ref()).await.unwrap();
    assert_eq!((r.generation, &r.policy), (1, &p_new));
}

/// A seed and an admin enable race both ways: whichever commits first,
/// the other's retry starts from it; the policy is always one of the two,
/// whole, and an admin enable over a seed is a new generation.
async fn a_seed_and_an_admin_enable<S: KvStore + 'static>(kv: Arc<S>) {
    let (p_flags, p_admin) = (policy(5, 1800), policy(7, 3600));
    for admin_first in [true, false] {
        run_clear(&kv).await;
        let log = Log::default();
        let (hs, mut s_hold) = hold();
        let (ha, mut a_hold) = hold();
        let seeder = HeldKv::new(Arc::clone(&kv), "seed", &log, Some(hs));
        let admin = HeldKv::new(Arc::clone(&kv), "admin", &log, Some(ha));
        let change = AdminChange::Enable(Some(p_admin.clone()));
        let order = async {
            s_hold.reached().await;
            a_hold.reached().await;
            let (first, second, first_name) = if admin_first {
                (a_hold, s_hold, "admin")
            } else {
                (s_hold, a_hold, "seed")
            };
            first.release();
            let done = format!("{first_name} attempt 1: committed");
            while !log.events().contains(&done) {
                yield_now().await;
            }
            second.release();
        };
        let (rs, ra, ()) = futures::join!(
            seed_if_absent(&seeder, &p_flags, 40),
            tiering::admin_change(&admin, &change, 41),
            order
        );
        let (seeded, after_admin) = (rs.unwrap(), ra.unwrap());
        let r = record(kv.as_ref()).await.unwrap();
        if admin_first {
            assert!(!seeded, "the seed overwrote the admin's record");
            assert_eq!((r.generation, &r.policy), (1, &p_admin));
            assert_eq!(seeder.attempts(), 2, "{:?}", log.events());
        } else {
            assert!(seeded);
            assert_eq!(
                (r.generation, &r.policy),
                (2, &p_admin),
                "the admin change was lost"
            );
            assert_eq!(after_admin, r);
            assert_eq!(admin.attempts(), 2, "{:?}", log.events());
        }
        assert!(r.enabled);
    }
}

/// An acknowledgment and an admin change race: an acknowledgment that read
/// the earlier generation never names the new one.
async fn an_acknowledgment_and_an_admin_change<S: KvStore + 'static>(kv: Arc<S>) {
    for ack_first in [true, false] {
        run_clear(&kv).await;
        assert!(
            seed_if_absent(kv.as_ref(), &policy(5, 1800), 50)
                .await
                .unwrap()
        );
        let log = Log::default();
        let (hk, mut k_hold) = hold();
        let (ha, mut a_hold) = hold();
        let acker = HeldKv::new(Arc::clone(&kv), "ack", &log, Some(hk));
        let admin = HeldKv::new(Arc::clone(&kv), "admin", &log, Some(ha));
        let change = AdminChange::SetPolicy(policy(9, 600));
        let order = async {
            k_hold.reached().await;
            a_hold.reached().await;
            let (first, second, first_name) = if ack_first {
                (k_hold, a_hold, "ack")
            } else {
                (a_hold, k_hold, "admin")
            };
            first.release();
            let done = format!("{first_name} attempt 1: committed");
            while !log.events().contains(&done) {
                yield_now().await;
            }
            second.release();
        };
        let (rk, ra, ()) = futures::join!(
            tiering::acknowledge(&acker, 3, [1u8; 16], 1, 60),
            tiering::admin_change(&admin, &change, 61),
            order
        );
        let (acked, changed) = (rk.unwrap(), ra.unwrap());
        let r = record(kv.as_ref()).await.unwrap();
        assert_eq!(r.generation, 2);
        assert!(
            !r.acknowledged_by(3),
            "an acknowledgment names a generation it did not read"
        );
        assert_eq!(changed.generation, 2);
        if ack_first {
            assert_eq!(acked.map(|a| a.applied_generation), Some(1));
            assert_eq!(r.applied_generation, 1);
            assert_eq!(admin.attempts(), 2, "{:?}", log.events());
        } else {
            assert_eq!(acked, None, "the acknowledgment was not refused");
            assert_eq!(acker.attempts(), 2, "{:?}", log.events());
        }
    }
}

async fn run_clear<S: KvStore>(kv: &Arc<S>) {
    arca_lake_metadata::transact(kv.as_ref(), (), |tx, ()| {
        Box::pin(async move {
            tx.clear(&keys::tiering());
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Let the other participants run (and come back at once).
async fn yield_now() {
    let mut yielded = false;
    futures::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

fn arca_lake_metadata_now() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

#[test]
fn seeds_from_different_flags_race_and_one_wins_whole() {
    on_each_store!(two_seeds_from_different_flags);
}

#[test]
fn seeds_from_equal_flags_race_and_write_once() {
    on_each_store!(two_seeds_from_equal_flags);
}

#[test]
fn a_deposed_leader_cannot_seed_after_its_successor() {
    on_each_store!(a_deposed_leaders_seed_is_fenced);
}

#[test]
fn a_seed_and_an_admin_enable_race_both_ways() {
    on_each_store!(a_seed_and_an_admin_enable);
}

#[test]
fn an_acknowledgment_never_names_a_generation_it_did_not_read() {
    on_each_store!(an_acknowledgment_and_an_admin_change);
}
