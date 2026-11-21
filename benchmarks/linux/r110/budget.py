"""The run's shared limits (plan §6.1), enforced before every data-plane
HTTP attempt of the test: operations, retries, probes, listing pages and
the final verification all take a permit here.

A permit reserves, under one lock and before anything is sent: one attempt
of the run's total, the attempt's whole body size of the run's byte
budget (counted whether the attempt then succeeds, fails or ends unknown;
a retry is a new attempt and reserves again), a slot of the live set when
the attempt may create a key, then a concurrency slot and a token of the
rate bucket. A limit that would be crossed stops the run (no permit is
ever granted beyond it), and a stopped or expired run grants none, also
to a worker already waiting for its slot or token.

One process per run: the run directory is locked (``flock``) for the
process's lifetime and its state file is created exclusively; a second
process, or a restart, of the same run is refused (no resume: a restart
could not prove what the stopped process had sent).
"""

import fcntl
import json
import math
import os
import threading

from . import common


class Stopped(Exception):
    """The run is stopped (``reason``); no attempt may start."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


class LiveLimit(Exception):
    """The live set is full: this attempt would make one more key live.
    Not a stop: the operation is not made (deletes still are)."""


class RunExists(Exception):
    """The run directory already holds a run (or another process holds
    it)."""


DEFAULTS = {
    "rate_per_s": 20.0,
    "burst": 5,
    "concurrency": 4,
    "max_attempts": 70_000,
    "max_body_bytes": 5 << 30,
    "max_live": 8_000,
    "run_seconds": 7.5 * 3600,
}


def valid_limit(v):
    """A limit is a finite positive number (``NaN`` or ``inf`` would pass
    every comparison against the plan's values and switch the limit off)."""
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v > 0


# The smoke stage's sub-runs (plan §7 stage 2), each with a fixed share of
# the run's attempts and request-body bytes: the main load gets the rest of
# the approved totals, so that the whole run never sends more than they.
SMOKE = {"smoke-hb": {"max_attempts": 400, "max_body_bytes": 128 << 20},
         "smoke-limit": {"max_attempts": 20, "max_body_bytes": 16 << 20},
         "smoke-ops": {"max_attempts": 600, "max_body_bytes": 128 << 20}}


def allocate(limits):
    """``{sub-run or "main": limits}`` from the approved run-wide ``limits``
    (never above ``DEFAULTS``): each smoke sub-run its fixed share, the main
    load what is left of the attempts and the bytes; every other limit (the
    rate, the burst, the concurrency, the live set) as approved for each.
    Raises ``ValueError`` when the smoke shares do not fit with something
    left for the main load. Unused smoke shares are not given back (no
    second count of the same attempts)."""
    approved = dict(DEFAULTS, **limits)
    bad = {k: v for k, v in approved.items() if k not in DEFAULTS or not valid_limit(v) or v > DEFAULTS[k]}
    if bad:
        raise ValueError(f"limits not within the plan's: {bad}")
    out = {}
    for name, share in SMOKE.items():
        out[name] = dict(approved, **share)
    main = dict(approved)
    for k in ("max_attempts", "max_body_bytes"):
        main[k] = approved[k] - sum(share[k] for share in SMOKE.values())
        if main[k] <= 0:
            raise ValueError(f"{k}={approved[k]}: the smoke stage's {approved[k] - main[k]} leave nothing for the run")
    out["main"] = main
    return out


class Permit:
    def __init__(self, budget, kind, body_bytes, live):
        self.budget = budget
        self.kind = kind
        self.body_bytes = body_bytes
        self.live = live
        self.attempt = None
        self.concurrency_wait_s = 0.0
        self.rate_wait_s = 0.0
        self._released = False

    def release(self):
        if not self._released:
            self._released = True
            self.budget._release_slot()


class Budget:
    """The limits of one run. ``limits`` overrides ``DEFAULTS`` (tests use
    small ones); ``stop`` is the run's StopFile (a stop by anyone ends
    permits)."""

    def __init__(self, run_dir, run_id, limits=None, clock=None, stop=None):
        self.clock = clock or common.Clock()
        self.limits = dict(DEFAULTS, **(limits or {}))
        bad = {k: v for k, v in self.limits.items() if k not in DEFAULTS or not valid_limit(v)}
        if bad:
            raise ValueError(f"limits not finite positive numbers: {bad}")
        self.run_id = run_id
        self.stop_file = stop or common.StopFile(os.path.join(run_dir, "stop.json"))
        self._lock = threading.Condition()
        self._cancel = threading.Event()
        self._attempts = 0
        self._body = 0
        self._live = 0
        self._inflight = 0
        self._tokens = float(self.limits["burst"])
        self._last = self.clock.monotonic()
        self._start = self._last
        self._deadline = self._start + self.limits["run_seconds"]
        self._stop_reason = None
        self._lockfd = self._claim(run_dir)

    def _claim(self, run_dir):
        os.makedirs(run_dir, exist_ok=True)
        fd = os.open(os.path.join(run_dir, "run.lock"), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise RunExists(f"{run_dir}: another process holds this run") from None
        state = os.path.join(run_dir, "budget.json")
        try:
            sfd = os.open(state, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            os.close(fd)
            raise RunExists(f"{state}: the run was started before; a run is never resumed") from None
        with os.fdopen(sfd, "w") as f:
            json.dump({"run_id": self.run_id, "limits": self.limits,
                       "started_wall_ms": self.clock.wall_ms()}, f, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        common.fsync_dir(run_dir)
        return fd

    # -- state -------------------------------------------------------------

    def snapshot(self):
        with self._lock:
            return {"attempts": self._attempts, "body_bytes": self._body, "live": self._live,
                    "inflight": self._inflight, "stopped": self._stop_reason,
                    "seconds_left": max(0.0, self._deadline - self.clock.monotonic())}

    def stop(self, reason, source="generator"):
        """Stop the run: no permit after this; waiting workers give up."""
        with self._lock:
            if self._stop_reason is None:
                self._stop_reason = reason
            self._cancel.set()
            self._lock.notify_all()
        self.stop_file.stop(reason, source, self.clock.wall_ms())

    def stopped(self):
        """The reason the run is stopped, if it is (its own stop, the stop
        file, the run deadline)."""
        with self._lock:
            if self._stop_reason is not None:
                return self._stop_reason
        rec = self.stop_file.reason()
        if rec is not None:
            with self._lock:
                if self._stop_reason is None:
                    self._stop_reason = rec.get("reason", "stop-file-unreadable")
                self._cancel.set()
                self._lock.notify_all()
                return self._stop_reason
        if self.clock.monotonic() >= self._deadline:
            self.stop("run-deadline")
            return "run-deadline"
        return None

    # -- live set ------------------------------------------------------------

    def live_release(self):
        """An acknowledged delete of a live key."""
        with self._lock:
            self._live = max(0, self._live - 1)

    def live_count(self):
        with self._lock:
            return self._live

    # -- permits -------------------------------------------------------------

    def acquire(self, kind, body_bytes=0, new_live=False, stage_deadline=None):
        """A permit for one HTTP attempt of ``kind``, or ``Stopped``.
        ``new_live``: the attempt may make a key live that was not (a PUT
        of an absent key); its slot stays taken whatever the outcome (an
        unknown PUT may have stored the key)."""
        reason = self.stopped()
        if reason:
            raise Stopped(reason)
        over = None
        with self._lock:
            if self._stop_reason:
                raise Stopped(self._stop_reason)
            if new_live and self._live + 1 > self.limits["max_live"]:
                raise LiveLimit(f"live set at {self._live}")
            if self._attempts + 1 > self.limits["max_attempts"]:
                over = "attempts-limit"
            elif self._body + body_bytes > self.limits["max_body_bytes"]:
                over = "bytes-limit"
            else:
                # Reserved now: counted even if the attempt is never sent.
                self._attempts += 1
                self._body += body_bytes
                if new_live:
                    self._live += 1
                permit = Permit(self, kind, body_bytes, new_live)
                permit.attempt = self._attempts
        if over:
            self.stop(over)
            raise Stopped(over)
        t0 = self.clock.monotonic()
        try:
            self._take_slot()
            t1 = self.clock.monotonic()
            self._take_token(stage_deadline)
            t2 = self.clock.monotonic()
        except Stopped:
            # Never sent: the attempt and its bytes stay counted (reserved),
            # but a live slot it reserved is given back.
            if new_live:
                self.live_release()
            raise
        permit.concurrency_wait_s = t1 - t0
        permit.rate_wait_s = t2 - t1
        return permit

    def _check(self, stage_deadline):
        if self._stop_reason:
            raise Stopped(self._stop_reason)
        now = self.clock.monotonic()
        if now >= self._deadline:
            self._stop_reason = "run-deadline"
            self._cancel.set()
            self._lock.notify_all()
            raise Stopped("run-deadline")
        if stage_deadline is not None and now >= stage_deadline:
            raise Stopped("stage-deadline")

    def _take_slot(self):
        with self._lock:
            while True:
                self._check(None)
                if self._inflight < self.limits["concurrency"]:
                    self._inflight += 1
                    return
                self._lock.wait(0.05)

    def _release_slot(self):
        with self._lock:
            self._inflight -= 1
            self._lock.notify_all()

    def _take_token(self, stage_deadline):
        try:
            while True:
                with self._lock:
                    self._check(stage_deadline)
                    now = self.clock.monotonic()
                    rate, burst = self.limits["rate_per_s"], self.limits["burst"]
                    self._tokens = min(float(burst), self._tokens + (now - self._last) * rate)
                    self._last = now
                    # (A tolerance of 1e-9 token: float refills can stop just
                    # short of a whole token, and a wait below the clock's
                    # resolution would not move it.)
                    if self._tokens >= 1.0 - 1e-9:
                        self._tokens = max(0.0, self._tokens - 1.0)
                        return
                    wait = max((1.0 - self._tokens) / rate, 1e-6)
                self.clock.sleep(wait, self._cancel)
        except Stopped:
            self._release_slot()
            raise

    def close(self):
        """Release the run's lock (idempotent). The state file stays: the
        run cannot be started again."""
        fd, self._lockfd = self._lockfd, None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
