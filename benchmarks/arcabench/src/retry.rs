//! Bounded retries of refused requests, and pacing after a refused
//! operation (Phase 4.1).
//!
//! A request answered `503` may be sent again under a [`Policy`]:
//! exponential backoff with jitter, a limit on the retries of one request
//! and on the time they may take, a `Retry-After` honoured up to the
//! largest delay, and two limits shared by every worker of the process:
//! retries in flight at once, and retries started per second. A retry
//! that would break a limit within the remaining time is not sent: the
//! request ends refused (`exhausted`). Cancellation ends every wait at
//! once.
//!
//! A closed-loop worker whose operation ended refused waits before its
//! next operation ([`Pacer`]); otherwise a refused operation, answered
//! in microseconds, is followed at once by the next, and a cluster that
//! refuses writes sees the generator's whole concurrency as a stream of
//! requests (Phase 4.1 capacity run, `docs/verification/phase-4.1-adr33.md`).
//!
//! Counters separate logical operations (counted by the caller), the
//! requests they make, and the HTTP attempts those take.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Semaphore, watch};
use tokio::time::Instant;

/// How a request's answer counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Done (any answer that is not a refusal: success or another error).
    Done,
    /// Refused, may be sent again (`503`), with the server's `Retry-After`.
    Retryable(Option<Duration>),
    /// Failed and not to be sent again (a transport error, a timeout).
    NonRetryable,
}

/// How a request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// Its last answer was not a refusal.
    Done,
    /// Refused, and no retry was allowed (none configured).
    Refused,
    /// Refused, and the retry budget or a shared limit ended it.
    Exhausted,
    /// A failure not retried.
    Failed,
    /// Cancelled while waiting to retry.
    Cancelled,
}

/// Jitter of a backoff delay `d`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Jitter {
    /// `d`.
    None,
    /// Uniform in `[0, d]`.
    Full,
    /// Uniform in `[d/2, d]`.
    Equal,
}

/// Retry policy of one request.
#[derive(Debug, Clone)]
pub struct Policy {
    /// Retries of one request at most (0: none).
    pub max_retries: u32,
    /// Time from a request's first refusal within which its retries must
    /// start (zero: no limit beyond `max_retries`).
    pub max_retry_time: Duration,
    /// Delay before the first retry; doubled for each further one.
    pub base: Duration,
    /// Largest delay.
    pub max_delay: Duration,
    pub jitter: Jitter,
    /// Wait at least the server's `Retry-After` (up to `max_delay`).
    pub honor_retry_after: bool,
    /// Seed of the jitter (with a counter: repeatable).
    pub seed: u64,
}

impl Policy {
    /// The delay before retry `n` (1 for the first), with jitter draw `u`
    /// in `[0, 1)`.
    #[must_use]
    pub fn delay(&self, n: u32, u: f64, retry_after: Option<Duration>) -> Duration {
        let exp = self
            .base
            .saturating_mul(1u32 << n.saturating_sub(1).min(20))
            .min(self.max_delay);
        let jittered = match self.jitter {
            Jitter::None => exp,
            Jitter::Full => exp.mul_f64(u),
            Jitter::Equal => exp / 2 + (exp / 2).mul_f64(u),
        };
        match retry_after {
            Some(r) if self.honor_retry_after => jittered.max(r.min(self.max_delay)),
            _ => jittered,
        }
    }
}

/// A per-second token bucket of retries.
#[derive(Debug)]
struct Bucket {
    rate: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
    /// Retry start times (tests: the bound on starts in any window).
    #[cfg(test)]
    starts: Mutex<Vec<Instant>>,
}

impl Bucket {
    fn new(rate: f64, burst: u32) -> Self {
        let burst = f64::from(burst.max(1));
        Self {
            rate,
            burst,
            state: Mutex::new((burst, Instant::now())),
            #[cfg(test)]
            starts: Mutex::new(Vec::new()),
        }
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn note_start(&self) {
        #[cfg(test)]
        self.starts.lock().unwrap().push(Instant::now());
    }

    /// Take a token, waiting until `deadline` at most; `false` if none
    /// comes by then (or on cancellation).
    async fn take(&self, deadline: Option<Instant>, cancel: &Cancel) -> bool {
        loop {
            let wait = {
                let mut s = self.state.lock().unwrap();
                let now = Instant::now();
                s.0 = (s.0 + (now - s.1).as_secs_f64() * self.rate).min(self.burst);
                s.1 = now;
                if s.0 >= 1.0 {
                    s.0 -= 1.0;
                    return true;
                }
                // At least the timer's resolution: a shorter sleep may end
                // with no time passed, and the bucket would never refill.
                Duration::from_secs_f64((1.0 - s.0) / self.rate).max(Duration::from_millis(1))
            };
            if deadline.is_some_and(|d| Instant::now() + wait > d) {
                return false;
            }
            if !cancel.sleep(wait).await {
                return false;
            }
        }
    }
}

/// Cancellation shared by every worker: set once, ends every wait.
#[derive(Debug, Clone)]
pub struct Cancel(watch::Receiver<bool>);

/// The setter of a [`Cancel`].
#[derive(Debug)]
pub struct Canceller(watch::Sender<bool>);

impl Canceller {
    pub fn cancel(&self) {
        let _ = self.0.send(true);
    }
}

/// A cancellation and its setter.
#[must_use]
pub fn cancellation() -> (Canceller, Cancel) {
    let (tx, rx) = watch::channel(false);
    (Canceller(tx), Cancel(rx))
}

impl Cancel {
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }

    /// Resolves once cancelled; never if the setter is gone uncancelled.
    pub async fn cancelled(&self) {
        let mut rx = self.0.clone();
        if rx.wait_for(|c| *c).await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    /// `f`, or `None` if cancelled first (`f` is dropped).
    pub async fn race<T>(&self, f: impl std::future::Future<Output = T>) -> Option<T> {
        if self.is_cancelled() {
            return None;
        }
        tokio::select! {
            v = f => Some(v),
            () = self.cancelled() => None,
        }
    }

    /// Sleep `d`; `false` if cancelled first.
    pub async fn sleep(&self, d: Duration) -> bool {
        if self.is_cancelled() {
            return false;
        }
        tokio::select! {
            () = tokio::time::sleep(d) => !self.is_cancelled(),
            () = self.cancelled() => false,
        }
    }
}

/// Counters of the requests and retries of a process (logical
/// operations are the caller's).
#[derive(Debug, Default)]
pub struct Counters {
    /// Requests made (each sent at least once).
    pub requests: AtomicU64,
    /// HTTP attempts: requests plus retries.
    pub attempts: AtomicU64,
    /// Retries sent.
    pub retries: AtomicU64,
    /// Answers that may be retried (`503`), every attempt.
    pub retryable_failures: AtomicU64,
    /// Failures not retried, every attempt.
    pub non_retryable_failures: AtomicU64,
    /// Requests that ended refused with no retry configured.
    pub refused: AtomicU64,
    /// Requests that ended refused because their budget or a shared limit
    /// allowed no further retry.
    pub exhausted: AtomicU64,
    /// Of those, ended by the retry concurrency limit.
    pub denied_concurrency: AtomicU64,
    /// Of those, ended by the retry rate limit.
    pub denied_rate: AtomicU64,
    /// Requests ended by cancellation while waiting to retry.
    pub cancelled: AtomicU64,
    /// Time a retry waited before it was sent (backoff, rate and
    /// concurrency limits), total and largest, µs.
    pub delay_us: AtomicU64,
    pub delay_us_max: AtomicU64,
    /// Retries in flight now, and at most.
    pub in_flight: AtomicU64,
    pub in_flight_max: AtomicU64,
    /// Pauses of workers after a refused operation, and the time they
    /// waited (ended early by a cancellation: what was waited), µs.
    pub pauses: AtomicU64,
    pub pause_us: AtomicU64,
}

impl Counters {
    /// Every counter, by the name the summary uses (durations in µs).
    #[must_use]
    pub fn values(&self) -> Vec<(&'static str, u64)> {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        vec![
            ("requests", g(&self.requests)),
            ("http_attempts", g(&self.attempts)),
            ("retries", g(&self.retries)),
            ("retryable_failures", g(&self.retryable_failures)),
            ("non_retryable_failures", g(&self.non_retryable_failures)),
            ("refused_no_retry", g(&self.refused)),
            ("retry_exhausted", g(&self.exhausted)),
            ("retry_denied_concurrency", g(&self.denied_concurrency)),
            ("retry_denied_rate", g(&self.denied_rate)),
            ("retry_cancelled", g(&self.cancelled)),
            ("retry_wait_us_total", g(&self.delay_us)),
            ("reject_pauses", g(&self.pauses)),
            ("reject_pause_us_total", g(&self.pause_us)),
        ]
    }

    /// `values` as JSON, minus `since` (a snapshot of `values`: the
    /// counts of a window), with the process-lifetime maxima.
    #[must_use]
    pub fn json_since(&self, since: Option<&[(&'static str, u64)]>) -> serde_json::Value {
        let mut m = serde_json::Map::new();
        for (k, v) in self.values() {
            let base = since
                .and_then(|s| s.iter().find(|(n, _)| *n == k))
                .map_or(0, |(_, b)| *b);
            m.insert(k.into(), v.saturating_sub(base).into());
        }
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        m.insert("retry_wait_us_max".into(), g(&self.delay_us_max).into());
        m.insert(
            "retries_in_flight_max".into(),
            g(&self.in_flight_max).into(),
        );
        serde_json::Value::Object(m)
    }

    #[must_use]
    pub fn json(&self) -> serde_json::Value {
        self.json_since(None)
    }
}

fn add_max(sum: &AtomicU64, max: &AtomicU64, v: u64) {
    sum.fetch_add(v, Ordering::Relaxed);
    max.fetch_max(v, Ordering::Relaxed);
}

/// A policy and the limits shared by every worker of a process.
#[derive(Debug)]
pub struct Retrier {
    pub policy: Policy,
    /// Retries in flight at once (`None`: no limit).
    concurrency: Option<Semaphore>,
    /// Retries started per second (`None`: no limit).
    rate: Option<Bucket>,
    draws: AtomicU64,
    pub counters: Counters,
}

impl Retrier {
    /// `max_concurrency` 0 and `max_rate` 0: no such limit.
    #[must_use]
    pub fn new(policy: Policy, max_concurrency: usize, max_rate: f64, burst: u32) -> Self {
        Self {
            policy,
            concurrency: (max_concurrency > 0).then(|| Semaphore::new(max_concurrency)),
            rate: (max_rate > 0.0).then(|| Bucket::new(max_rate, burst)),
            draws: AtomicU64::new(0),
            counters: Counters::default(),
        }
    }

    /// A jitter draw in `[0, 1)`, repeatable from the seed.
    fn draw(&self) -> f64 {
        let n = self.draws.fetch_add(1, Ordering::Relaxed);
        let mut x = self.policy.seed ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^= x >> 31;
        (x >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Send a request (`attempt`, called once per HTTP attempt) and retry
    /// it while `classify` says it may be and the budget and limits allow.
    /// Returns the last answer (`None` if cancelled before or during the
    /// first attempt) and how the request ended.
    pub async fn run<T, F, Fut>(
        &self,
        cancel: &Cancel,
        mut attempt: F,
        classify: impl Fn(&T) -> Class,
    ) -> (Option<T>, End)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let c = &self.counters;
        // Nothing is sent once cancelled; an attempt in flight when the
        // cancellation comes is abandoned (its outcome is the caller's
        // unknown).
        if cancel.is_cancelled() {
            c.cancelled.fetch_add(1, Ordering::Relaxed);
            return (None, End::Cancelled);
        }
        c.requests.fetch_add(1, Ordering::Relaxed);
        c.attempts.fetch_add(1, Ordering::Relaxed);
        let Some(mut last) = cancel.race(attempt()).await else {
            c.cancelled.fetch_add(1, Ordering::Relaxed);
            return (None, End::Cancelled);
        };
        let mut first_refusal: Option<Instant> = None;
        let mut n = 0u32;
        loop {
            let retry_after = match classify(&last) {
                Class::Done => return (Some(last), End::Done),
                Class::NonRetryable => {
                    c.non_retryable_failures.fetch_add(1, Ordering::Relaxed);
                    return (Some(last), End::Failed);
                }
                Class::Retryable(r) => {
                    c.retryable_failures.fetch_add(1, Ordering::Relaxed);
                    r
                }
            };
            if self.policy.max_retries == 0 {
                c.refused.fetch_add(1, Ordering::Relaxed);
                return (Some(last), End::Refused);
            }
            let start = *first_refusal.get_or_insert_with(Instant::now);
            let deadline =
                (!self.policy.max_retry_time.is_zero()).then(|| start + self.policy.max_retry_time);
            if n >= self.policy.max_retries {
                c.exhausted.fetch_add(1, Ordering::Relaxed);
                return (Some(last), End::Exhausted);
            }
            n += 1;
            let d = self.policy.delay(n, self.draw(), retry_after);
            if deadline.is_some_and(|dl| Instant::now() + d > dl) {
                c.exhausted.fetch_add(1, Ordering::Relaxed);
                return (Some(last), End::Exhausted);
            }
            let waited = Instant::now();
            if !cancel.sleep(d).await {
                c.cancelled.fetch_add(1, Ordering::Relaxed);
                return (Some(last), End::Cancelled);
            }
            // The permit first, then the token, then the send: a token is
            // taken only by a retry about to start, so the bucket bounds
            // retry starts (review: tokens taken before waiting for a permit
            // let blocked retries start together).
            let permit = match &self.concurrency {
                None => None,
                Some(s) => {
                    let wait = async {
                        match deadline {
                            Some(dl) => tokio::time::timeout_at(dl, s.acquire()).await.ok(),
                            None => Some(s.acquire().await),
                        }
                    };
                    let got = tokio::select! {
                        p = wait => p,
                        () = cancel.cancelled() => {
                            c.cancelled.fetch_add(1, Ordering::Relaxed);
                            return (Some(last), End::Cancelled);
                        }
                    };
                    if let Some(Ok(p)) = got {
                        Some(p)
                    } else {
                        c.exhausted.fetch_add(1, Ordering::Relaxed);
                        c.denied_concurrency.fetch_add(1, Ordering::Relaxed);
                        return (Some(last), End::Exhausted);
                    }
                }
            };
            if let Some(b) = &self.rate
                && !b.take(deadline, cancel).await
            {
                if cancel.is_cancelled() {
                    c.cancelled.fetch_add(1, Ordering::Relaxed);
                    return (Some(last), End::Cancelled);
                }
                c.exhausted.fetch_add(1, Ordering::Relaxed);
                c.denied_rate.fetch_add(1, Ordering::Relaxed);
                return (Some(last), End::Exhausted);
            }
            // A wait that ended late (timer, scheduling) does not stretch
            // the budget.
            if deadline.is_some_and(|dl| Instant::now() > dl) {
                c.exhausted.fetch_add(1, Ordering::Relaxed);
                return (Some(last), End::Exhausted);
            }
            add_max(
                &c.delay_us,
                &c.delay_us_max,
                u64::try_from(waited.elapsed().as_micros()).unwrap_or(u64::MAX),
            );
            c.retries.fetch_add(1, Ordering::Relaxed);
            c.attempts.fetch_add(1, Ordering::Relaxed);
            let now = c.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            c.in_flight_max.fetch_max(now, Ordering::Relaxed);
            if let Some(b) = &self.rate {
                b.note_start();
            }
            let r = cancel.race(attempt()).await;
            c.in_flight.fetch_sub(1, Ordering::Relaxed);
            drop(permit);
            let Some(r) = r else {
                c.cancelled.fetch_add(1, Ordering::Relaxed);
                return (Some(last), End::Cancelled);
            };
            last = r;
        }
    }
}

/// Pacing of one closed-loop worker after refused operations: the `k`-th
/// refused operation in a row waits the policy's delay for retry `k`
/// (`Retry-After` included); a completed operation resets it.
#[derive(Debug, Default)]
pub struct Pacer {
    refused_in_a_row: u32,
}

impl Pacer {
    /// After an operation; `refused` when it ended refused or exhausted.
    /// Returns `false` if cancelled while pausing.
    pub async fn after(
        &mut self,
        r: &Retrier,
        refused: bool,
        retry_after: Option<Duration>,
        cancel: &Cancel,
    ) -> bool {
        if !refused {
            self.refused_in_a_row = 0;
            return true;
        }
        self.refused_in_a_row = self.refused_in_a_row.saturating_add(1);
        let d = r.policy.delay(self.refused_in_a_row, r.draw(), retry_after);
        r.counters.pauses.fetch_add(1, Ordering::Relaxed);
        let t0 = Instant::now();
        let slept = cancel.sleep(d).await;
        r.counters.pause_us.fetch_add(
            u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        slept
    }
}

#[cfg(test)]
#[allow(clippy::trivially_copy_pass_by_ref, clippy::float_cmp)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    fn policy(max_retries: u32, max_retry_time_ms: u64) -> Policy {
        Policy {
            max_retries,
            max_retry_time: Duration::from_millis(max_retry_time_ms),
            base: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
            jitter: Jitter::Full,
            honor_retry_after: true,
            seed: 7,
        }
    }

    fn classify(status: &u16) -> Class {
        match *status {
            503 => Class::Retryable(None),
            0 => Class::NonRetryable,
            _ => Class::Done,
        }
    }

    /// A server answering 503 to the first `n` attempts, then 200, in
    /// `latency` of virtual time per attempt.
    fn server(
        n: u32,
        latency: Duration,
    ) -> impl FnMut() -> std::pin::Pin<Box<dyn Future<Output = u16> + Send>> {
        let served = Arc::new(AtomicU32::new(0));
        move || {
            let served = Arc::clone(&served);
            Box::pin(async move {
                tokio::time::sleep(latency).await;
                if served.fetch_add(1, Ordering::Relaxed) < n {
                    503
                } else {
                    200
                }
            })
        }
    }

    #[test]
    fn delays_grow_to_the_cap_with_jitter_inside_their_range() {
        let mut p = policy(10, 0);
        p.jitter = Jitter::None;
        let d: Vec<u64> = (1..=8)
            .map(|n| p.delay(n, 0.5, None).as_millis() as u64)
            .collect();
        assert_eq!(d, vec![100, 200, 400, 800, 1600, 2000, 2000, 2000]);
        p.jitter = Jitter::Full;
        assert_eq!(p.delay(3, 0.0, None), Duration::ZERO);
        assert_eq!(p.delay(3, 0.25, None), Duration::from_millis(100));
        p.jitter = Jitter::Equal;
        assert_eq!(p.delay(3, 0.0, None), Duration::from_millis(200));
        // Retry-After raises the delay, never past the cap; off: ignored.
        assert_eq!(
            p.delay(1, 0.0, Some(Duration::from_secs(1))),
            Duration::from_secs(1)
        );
        assert_eq!(
            p.delay(1, 0.0, Some(Duration::from_secs(30))),
            Duration::from_secs(2)
        );
        p.honor_retry_after = false;
        assert_eq!(
            p.delay(1, 0.0, Some(Duration::from_secs(30))),
            Duration::from_millis(50)
        );
        // Very many retries do not overflow.
        assert_eq!(p.delay(u32::MAX, 1.0, None), Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancellation_whose_setter_is_gone_never_fires() {
        let (c, cancel) = cancellation();
        drop(c);
        assert!(cancel.sleep(Duration::from_secs(5)).await);
        let r = Retrier::new(policy(2, 0), 1, 0.0, 1);
        let (_, end) = r
            .run(&cancel, server(u32::MAX, Duration::ZERO), classify)
            .await;
        assert_eq!(end, End::Exhausted);
        assert_eq!(r.counters.retries.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn jitter_is_repeatable_from_the_seed() {
        let a = Retrier::new(policy(3, 0), 0, 0.0, 1);
        let b = Retrier::new(policy(3, 0), 0, 0.0, 1);
        let da: Vec<f64> = (0..32).map(|_| a.draw()).collect();
        let db: Vec<f64> = (0..32).map(|_| b.draw()).collect();
        assert_eq!(da, db);
        assert!(da.iter().all(|u| (0.0..1.0).contains(u)));
        assert!(da.windows(2).any(|w| w[0] != w[1]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_refused_every_time_ends_after_its_retries() {
        let r = Retrier::new(policy(3, 0), 0, 0.0, 1);
        let (_c, cancel) = cancellation();
        let t0 = Instant::now();
        let (last, end) = r
            .run(&cancel, server(u32::MAX, Duration::ZERO), classify)
            .await;
        assert_eq!((last, end), (Some(503), End::Exhausted));
        let k = &r.counters;
        assert_eq!(k.requests.load(Ordering::Relaxed), 1);
        assert_eq!(k.attempts.load(Ordering::Relaxed), 4);
        assert_eq!(k.retries.load(Ordering::Relaxed), 3);
        assert_eq!(k.retryable_failures.load(Ordering::Relaxed), 4);
        assert_eq!(k.exhausted.load(Ordering::Relaxed), 1);
        // Waited what was counted, at most 100 + 200 + 400 ms.
        let waited = t0.elapsed();
        assert!(waited <= Duration::from_millis(700), "{waited:?}");
        assert_eq!(
            k.delay_us.load(Ordering::Relaxed),
            u64::try_from(waited.as_micros()).unwrap()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_that_succeeds_after_some_refusals_is_done() {
        let r = Retrier::new(policy(5, 0), 0, 0.0, 1);
        let (_c, cancel) = cancellation();
        let (last, end) = r.run(&cancel, server(2, Duration::ZERO), classify).await;
        assert_eq!((last, end), (Some(200), End::Done));
        assert_eq!(r.counters.retries.load(Ordering::Relaxed), 2);
        assert_eq!(r.counters.attempts.load(Ordering::Relaxed), 3);
        assert_eq!(r.counters.exhausted.load(Ordering::Relaxed), 0);
        // No retry configured: the refusal is the answer.
        let none = Retrier::new(policy(0, 0), 0, 0.0, 1);
        let (last, end) = none.run(&cancel, server(1, Duration::ZERO), classify).await;
        assert_eq!((last, end), (Some(503), End::Refused));
        assert_eq!(none.counters.attempts.load(Ordering::Relaxed), 1);
        // A failure that is not a refusal is never retried.
        let (last, end) = r.run(&cancel, || async { 0u16 }, classify).await;
        assert_eq!((last, end), (Some(0), End::Failed));
        assert_eq!(r.counters.non_retryable_failures.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn the_retry_time_budget_ends_a_request_before_it_is_spent() {
        // Up to 100 retries, but 1 s from the first refusal: no retry
        // starts after it, whatever the count allows.
        let mut p = policy(100, 1000);
        p.jitter = Jitter::None;
        let r = Retrier::new(p, 0, 0.0, 1);
        let (_c, cancel) = cancellation();
        let t0 = Instant::now();
        let (_, end) = r
            .run(
                &cancel,
                server(u32::MAX, Duration::from_millis(10)),
                classify,
            )
            .await;
        assert_eq!(end, End::Exhausted);
        // 100 + 200 + 400 ms of delays fit (with 10 ms per attempt); the
        // next 800 ms would not.
        assert_eq!(r.counters.retries.load(Ordering::Relaxed), 3);
        assert!(t0.elapsed() <= Duration::from_millis(1000 + 10));
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_ends_a_wait_at_once() {
        let mut p = policy(10, 0);
        p.jitter = Jitter::None;
        p.base = Duration::from_secs(60);
        p.max_delay = Duration::from_secs(60);
        let r = Arc::new(Retrier::new(p, 0, 0.0, 1));
        let (canceller, cancel) = cancellation();
        let task = {
            let r = Arc::clone(&r);
            tokio::spawn(async move {
                let t0 = Instant::now();
                let out = r
                    .run(&cancel, server(u32::MAX, Duration::ZERO), classify)
                    .await;
                (out, t0.elapsed())
            })
        };
        tokio::time::sleep(Duration::from_secs(1)).await;
        canceller.cancel();
        let ((last, end), took) = task.await.unwrap();
        assert_eq!((last, end), (Some(503), End::Cancelled));
        assert!(took < Duration::from_secs(2), "{took:?}");
        assert_eq!(r.counters.cancelled.load(Ordering::Relaxed), 1);
        assert_eq!(r.counters.retries.load(Ordering::Relaxed), 0);
        // Cancelled before: no wait at all.
        let (_c2, cancel2) = cancellation();
        let (c3, cancel3) = cancellation();
        c3.cancel();
        let t0 = Instant::now();
        let (_, end) = r
            .run(&cancel3, server(u32::MAX, Duration::ZERO), classify)
            .await;
        assert_eq!(end, End::Cancelled);
        assert_eq!(t0.elapsed(), Duration::ZERO);
        drop(cancel2);
        // A pausing worker stops too.
        let (c4, cancel4) = cancellation();
        let mut pacer = Pacer::default();
        let pause = {
            let r = Arc::clone(&r);
            tokio::spawn(async move { pacer.after(&r, true, None, &cancel4).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        c4.cancel();
        assert!(!pause.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn many_clients_retrying_at_once_stay_inside_the_shared_limits() {
        // 64 clients, every attempt refused and taking 50 ms; retries: at
        // most 4 in flight and 20 per second (burst 5).
        let mut p = policy(1000, 5000);
        p.base = Duration::from_millis(10);
        p.max_delay = Duration::from_millis(200);
        let r = Arc::new(Retrier::new(p, 4, 20.0, 5));
        let (_c, cancel) = cancellation();
        let t0 = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..64 {
            let r = Arc::clone(&r);
            let cancel = cancel.clone();
            tasks.push(tokio::spawn(async move {
                r.run(
                    &cancel,
                    server(u32::MAX, Duration::from_millis(50)),
                    classify,
                )
                .await
                .1
            }));
        }
        let mut ends = Vec::new();
        for t in tasks {
            ends.push(t.await.unwrap());
        }
        let k = &r.counters;
        eprintln!("{}", k.json());
        // Every request ended, refused, within its time budget.
        assert!(ends.iter().all(|e| *e == End::Exhausted));
        assert_eq!(k.exhausted.load(Ordering::Relaxed), 64);
        assert!(t0.elapsed() <= Duration::from_millis(5000 + 50 + 50));
        // The limits held: at most 4 retries in flight, and no more retries
        // than the bucket gave over the run.
        assert!(k.in_flight_max.load(Ordering::Relaxed) <= 4);
        let retries = k.retries.load(Ordering::Relaxed);
        let allowed = 5.0 + 20.0 * t0.elapsed().as_secs_f64();
        assert!(
            retries > 0 && retries as f64 <= allowed,
            "{retries} > {allowed}"
        );
        assert_eq!(
            k.attempts.load(Ordering::Relaxed),
            k.requests.load(Ordering::Relaxed) + retries
        );
        assert!(
            k.denied_rate.load(Ordering::Relaxed) + k.denied_concurrency.load(Ordering::Relaxed)
                > 0
        );
        // Starts, not only their total, stay inside the rate.
        let starts = r.rate.as_ref().unwrap().starts.lock().unwrap().clone();
        for (i, t) in starts.iter().enumerate() {
            let within = starts[i..]
                .iter()
                .take_while(|s| **s < *t + Duration::from_secs(1))
                .count();
            assert!(within <= 5 + 20, "{within} starts in a second");
        }
    }

    /// Review: retries waiting for the one permit must not hoard rate
    /// tokens and start together when it frees: in every window of one
    /// second, at most `burst + rate` retries start.
    #[tokio::test(start_paused = true)]
    async fn retry_starts_respect_the_rate_in_every_window_behind_a_blocked_permit() {
        let mut p = policy(1000, 20_000);
        p.base = Duration::from_millis(5);
        p.max_delay = Duration::from_millis(20);
        let r = Arc::new(Retrier::new(p, 1, 10.0, 2));
        let (_c, cancel) = cancellation();
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let r = Arc::clone(&r);
            let cancel = cancel.clone();
            tasks.push(tokio::spawn(async move {
                // Every attempt holds the permit for 30 ms.
                r.run(
                    &cancel,
                    server(u32::MAX, Duration::from_millis(30)),
                    classify,
                )
                .await
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let starts = r.rate.as_ref().unwrap().starts.lock().unwrap().clone();
        assert!(starts.len() > 20, "{}", starts.len());
        for (i, t) in starts.iter().enumerate() {
            let within = starts[i..]
                .iter()
                .take_while(|s| **s < *t + Duration::from_secs(1))
                .count();
            assert!(
                within <= 2 + 10,
                "{within} starts in the second from start {i}"
            );
        }
        assert_eq!(r.counters.in_flight_max.load(Ordering::Relaxed), 1);
    }

    /// Review: an attempt in flight when the run is cancelled is abandoned
    /// at once, and nothing is sent after the cancellation.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_attempt_ends_at_the_cancellation_and_none_follows() {
        let r = Arc::new(Retrier::new(policy(5, 0), 0, 0.0, 1));
        let (canceller, cancel) = cancellation();
        let sent = Arc::new(AtomicU32::new(0));
        let task = {
            let (r, cancel, sent) = (Arc::clone(&r), cancel.clone(), Arc::clone(&sent));
            tokio::spawn(async move {
                let t0 = Instant::now();
                let out = r
                    .run(
                        &cancel,
                        || {
                            sent.fetch_add(1, Ordering::Relaxed);
                            // A response that never comes.
                            std::future::pending::<u16>()
                        },
                        classify,
                    )
                    .await;
                (out, t0.elapsed())
            })
        };
        tokio::time::sleep(Duration::from_secs(3)).await;
        canceller.cancel();
        let ((last, end), took) = task.await.unwrap();
        assert_eq!((last, end), (None, End::Cancelled));
        assert!(took < Duration::from_secs(4));
        let (_, end) = r.run(&cancel, server(0, Duration::ZERO), classify).await;
        assert_eq!(end, End::Cancelled);
        assert_eq!(sent.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_worker_refused_in_a_row_backs_off_and_a_success_resets_it() {
        let mut p = policy(0, 0);
        p.jitter = Jitter::None;
        let r = Retrier::new(p, 0, 0.0, 1);
        let (_c, cancel) = cancellation();
        let mut pacer = Pacer::default();
        let mut waits = Vec::new();
        for refused in [true, true, true, false, true] {
            let t0 = Instant::now();
            assert!(pacer.after(&r, refused, None, &cancel).await);
            waits.push(t0.elapsed().as_millis());
        }
        assert_eq!(waits, vec![100, 200, 400, 0, 100]);
        assert_eq!(r.counters.pauses.load(Ordering::Relaxed), 4);
        // The server's Retry-After is honoured, up to the cap.
        let t0 = Instant::now();
        pacer
            .after(&r, true, Some(Duration::from_secs(33)), &cancel)
            .await;
        assert_eq!(t0.elapsed(), Duration::from_secs(2));
    }
}
