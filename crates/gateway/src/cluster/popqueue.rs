//! Bounded, node-aware queue for asynchronous cache populations
//! (RFC-006 §5.3, Phase 2.3 §5).
//!
//! A GET that misses hands the chunk to [`PopulationQueue::enqueue`] and
//! returns; nothing here is ever on the GET's critical path, and a
//! population that fails only counts. The queue bounds work in three ways:
//!
//! - a **bounded FIFO** (`queue_depth`): when it is full the population is
//!   dropped and counted (`dropped_full`), which is the backpressure
//!   signal to the caller;
//! - **workers** (`workers`): at most that many populations run at once
//!   per gateway;
//! - **per-node permits** (`per_node`): a population writes to every
//!   member node of its cache group and holds one permit per member
//!   while it does, so no node receives more than `per_node` population
//!   streams from this gateway at once. Permits are taken in node order,
//!   so two populations can never deadlock on each other.
//!
//! **Durable reads first**: before taking its permits a population yields
//! while the gateway has shard reads in flight to any of its nodes (up to
//! [`MAX_DEFER_NODE`]) or to any node at all (up to the shorter
//! [`MAX_DEFER_ANY`]), so that a read burst is served before the cache
//! is written and a busy node still cannot starve the cache forever.
//!
//! **Retries**: node errors are retried a bounded number of times with
//! exponential backoff; metadata and corruption errors are not.
//! **Cancellation**: a shutdown stops the workers after their current
//! attempt; queued populations are dropped and counted
//! (`dropped_shutdown`). The dedup marker of every queued or running
//! population is released on every exit path.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arca_lake_protocol::client::NodeClients;
use arca_lake_tiering::TierError;
use arca_lake_tiering::cache::PopulateOutcome;
use tokio::sync::{Semaphore, mpsc};

/// Longest time a population yields to reads in flight on its own nodes.
const MAX_DEFER_NODE: Duration = Duration::from_millis(200);
/// Longest time a population yields to reads in flight anywhere.
const MAX_DEFER_ANY: Duration = Duration::from_millis(50);
/// One yield step while reads are in flight.
const DEFER_STEP: Duration = Duration::from_millis(5);
/// Attempts per population for node errors.
const MAX_ATTEMPTS: u32 = 3;
/// First backoff between attempts (doubles).
const BACKOFF: Duration = Duration::from_millis(100);

/// The future of one population attempt.
pub type AttemptFuture =
    Pin<Box<dyn Future<Output = Result<PopulateOutcome, TierError>> + Send + 'static>>;
/// Builds one attempt; called again on retry.
pub type Attempt = Arc<dyn Fn() -> AttemptFuture + Send + Sync + 'static>;
/// Called exactly once when the population leaves the queue (finished,
/// dropped, or cancelled), with the outcome if it ran.
pub type Done = Box<dyn FnOnce(Option<Result<PopulateOutcome, TierError>>) + Send + 'static>;

/// One queued population.
pub struct Job {
    /// Addresses of the cache group's member nodes.
    pub nodes: Vec<String>,
    /// The attempt builder.
    pub attempt: Attempt,
    /// Completion callback (accounting, dedup release).
    pub done: Done,
}

/// Counters of the queue.
#[derive(Debug, Default)]
pub struct PopQueueStats {
    /// Populations accepted into the queue.
    pub enqueued: AtomicU64,
    /// Populations dropped because the queue was full.
    pub dropped_full: AtomicU64,
    /// Populations dropped at shutdown.
    pub dropped_shutdown: AtomicU64,
    /// Populations that ran to an outcome (any).
    pub completed: AtomicU64,
    /// Attempts repeated after a node error.
    pub retries: AtomicU64,
    /// Populations that yielded at least once to durable reads.
    pub deferred_for_reads: AtomicU64,
    /// Populations waiting in the queue (gauge).
    pub queued: AtomicU64,
    /// Populations holding node permits (gauge).
    pub running: AtomicU64,
}

/// A copy of [`PopQueueStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PopQueueStatsSnapshot {
    /// See [`PopQueueStats::enqueued`].
    pub enqueued: u64,
    /// See [`PopQueueStats::dropped_full`].
    pub dropped_full: u64,
    /// See [`PopQueueStats::dropped_shutdown`].
    pub dropped_shutdown: u64,
    /// See [`PopQueueStats::completed`].
    pub completed: u64,
    /// See [`PopQueueStats::retries`].
    pub retries: u64,
    /// See [`PopQueueStats::deferred_for_reads`].
    pub deferred_for_reads: u64,
    /// See [`PopQueueStats::queued`].
    pub queued: u64,
    /// See [`PopQueueStats::running`].
    pub running: u64,
}

impl PopQueueStats {
    fn snapshot(&self) -> PopQueueStatsSnapshot {
        PopQueueStatsSnapshot {
            enqueued: self.enqueued.load(Relaxed),
            dropped_full: self.dropped_full.load(Relaxed),
            dropped_shutdown: self.dropped_shutdown.load(Relaxed),
            completed: self.completed.load(Relaxed),
            retries: self.retries.load(Relaxed),
            deferred_for_reads: self.deferred_for_reads.load(Relaxed),
            queued: self.queued.load(Relaxed),
            running: self.running.load(Relaxed),
        }
    }
}

struct Shared {
    stats: PopQueueStats,
    per_node: usize,
    permits: Mutex<HashMap<String, Arc<Semaphore>>>,
    clients: NodeClients,
    closed: AtomicBool,
}

impl Shared {
    fn permit_for(&self, addr: &str) -> Arc<Semaphore> {
        let mut map = self
            .permits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            map.entry(addr.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(self.per_node))),
        )
    }
}

/// The queue handle held by the gateway backend.
pub struct PopulationQueue {
    tx: Mutex<Option<mpsc::Sender<Job>>>,
    shared: Arc<Shared>,
    workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl std::fmt::Debug for PopulationQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PopulationQueue")
            .field("per_node", &self.shared.per_node)
            .finish_non_exhaustive()
    }
}

impl PopulationQueue {
    /// Start `workers` workers over a queue of `queue_depth` entries with
    /// `per_node` concurrent populations per node.
    #[must_use]
    pub fn new(clients: NodeClients, workers: usize, queue_depth: usize, per_node: usize) -> Self {
        let (tx, rx) = mpsc::channel::<Job>(queue_depth.max(1));
        let shared = Arc::new(Shared {
            stats: PopQueueStats::default(),
            per_node: per_node.max(1),
            permits: Mutex::new(HashMap::new()),
            clients,
            closed: AtomicBool::new(false),
        });
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let handles = (0..workers.max(1))
            .map(|_| {
                let shared = Arc::clone(&shared);
                let rx = Arc::clone(&rx);
                tokio::spawn(worker(shared, rx))
            })
            .collect();
        Self {
            tx: Mutex::new(Some(tx)),
            shared,
            workers: Mutex::new(handles),
        }
    }

    /// Queue a population. Returns `false` (and calls `done(None)`) when
    /// the queue is full or closed.
    pub fn enqueue(&self, job: Job) -> bool {
        let sender = self
            .tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(sender) = sender else {
            self.shared.stats.dropped_shutdown.fetch_add(1, Relaxed);
            (job.done)(None);
            return false;
        };
        match sender.try_send(job) {
            Ok(()) => {
                self.shared.stats.enqueued.fetch_add(1, Relaxed);
                self.shared.stats.queued.fetch_add(1, Relaxed);
                true
            }
            Err(mpsc::error::TrySendError::Full(job)) => {
                self.shared.stats.dropped_full.fetch_add(1, Relaxed);
                (job.done)(None);
                false
            }
            Err(mpsc::error::TrySendError::Closed(job)) => {
                self.shared.stats.dropped_shutdown.fetch_add(1, Relaxed);
                (job.done)(None);
                false
            }
        }
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> PopQueueStatsSnapshot {
        self.shared.stats.snapshot()
    }

    /// Stop accepting populations, let running attempts finish, drop the
    /// queued ones (counted), and wait for the workers.
    pub async fn shutdown(&self) {
        self.shared.closed.store(true, Relaxed);
        self.tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let handles: Vec<_> = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
            .collect();
        for h in handles {
            let _ = h.await;
        }
    }
}

async fn worker(shared: Arc<Shared>, rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Job>>>) {
    loop {
        let job = {
            let mut guard = rx.lock().await;
            guard.recv().await
        };
        let Some(job) = job else { return };
        shared.stats.queued.fetch_sub(1, Relaxed);
        if shared.closed.load(Relaxed) {
            shared.stats.dropped_shutdown.fetch_add(1, Relaxed);
            (job.done)(None);
            continue;
        }
        run_job(&shared, job).await;
    }
}

async fn run_job(shared: &Shared, job: Job) {
    // Durable reads first: yield while any of the job's nodes is serving
    // reads, within a bound.
    let mut deferred = Duration::ZERO;
    let mut yielded = false;
    loop {
        let on_own_nodes = job
            .nodes
            .iter()
            .any(|n| shared.clients.reads_in_flight(n) > 0);
        let anywhere = shared.clients.reads_in_flight_total() > 0;
        let keep_waiting =
            (on_own_nodes && deferred < MAX_DEFER_NODE) || (anywhere && deferred < MAX_DEFER_ANY);
        if !keep_waiting {
            break;
        }
        yielded = true;
        tokio::time::sleep(DEFER_STEP).await;
        deferred += DEFER_STEP;
    }
    if yielded {
        shared.stats.deferred_for_reads.fetch_add(1, Relaxed);
    }
    // Permits in node order (deadlock-free), one per distinct node.
    let mut nodes = job.nodes.clone();
    nodes.sort();
    nodes.dedup();
    let mut permits = Vec::with_capacity(nodes.len());
    for n in &nodes {
        let sem = shared.permit_for(n);
        match sem.acquire_owned().await {
            Ok(p) => permits.push(p),
            Err(_) => break,
        }
    }
    shared.stats.running.fetch_add(1, Relaxed);
    let mut backoff = BACKOFF;
    let mut result = None;
    for attempt in 1..=MAX_ATTEMPTS {
        let r = (job.attempt)().await;
        let retry = matches!(&r, Err(TierError::Node { .. })) && attempt < MAX_ATTEMPTS;
        result = Some(r);
        if !retry || shared.closed.load(Relaxed) {
            break;
        }
        shared.stats.retries.fetch_add(1, Relaxed);
        tokio::time::sleep(backoff).await;
        backoff *= 2;
    }
    drop(permits);
    shared.stats.running.fetch_sub(1, Relaxed);
    shared.stats.completed.fetch_add(1, Relaxed);
    (job.done)(result);
}
