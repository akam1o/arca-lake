#!/usr/bin/env python3
"""The R-110 load process (plan §6.1, §9 P1): executes the steps the driver
gives it, every HTTP attempt under the run's one budget and in the ledger.

  load.py run --run-dir DIR --endpoint URL [--endpoint URL ...]
              [--limits JSON] [--object-size N] [--bucket NAME]

The run directory must hold ``run.json`` (``run_id``) and is claimed for
the process's lifetime (``budget.Budget``: a run is never resumed). Steps
arrive as ``control/NNNNNN.json`` files (exclusively created by the
driver, executed in order); a step with ``until`` runs until the driver
creates ``control/NNNNNN.done`` or its ``max_seconds`` pass. The process
writes ``status.json`` (atomic) after every step and on exit, and ends on:
a stop (the run's stop file, a limit, a ledger failure), a ``finish``
step, or the run deadline. Its exit code: 0 finished, 3 stopped.

Step kinds: ``create-bucket``; ``put`` / ``delete`` / ``get`` (a list of
keys; ``rate`` per second at most, below the run's bucket);
``put-until-refused`` (new keys until ``refused_for`` consecutive
seconds of refusals, ``max_new`` at most); ``hold`` (one PUT attempt per
``interval`` seconds, never retried, until done); ``probe`` (if ``put``,
a PUT of a new key, and if ``get_key``, a GET of it, every ``interval``
seconds on a fixed schedule, until done); ``start-probe`` /
``stop-probe`` (the baseline-key probe of plan §6.2, in a thread of its
own: a GET of ``key`` every 5 s; 3 failures in a row, or an answer over
5 s, stop the run); ``churn`` (``rate`` PUT of a new key and DELETE of
the oldest live key per second, for ``seconds``); ``list`` (pages);
``verify`` (every key the ledger has, checked against its candidates);
``finish``. Every request of every step goes through ``Load.attempt``.
"""

import argparse
import hashlib
import json
import math
import os
import random
import re
import signal
import sys
import threading
import urllib.parse

from . import budget as budget_mod
from . import common, ledger as ledger_mod, s3

# A 503 is retried at most this many times (plan §6.1): backoff from
# RETRY_BASE_S doubling up to RETRY_MAX_S, full jitter, and no retry starts
# later than RETRY_WINDOW_S after the first refusal.
RETRIES, RETRY_BASE_S, RETRY_MAX_S, RETRY_WINDOW_S = 2, 0.2, 3.2, 10.0


class Load:
    def __init__(self, run_dir, run_id, endpoints, limits=None, clock=None, client=None,
                 bucket="r110", object_size=256 << 10, rng=None, heartbeat=None, heartbeat_id=None):
        self.clock = clock or common.Clock()
        self.run_dir = run_dir
        self.run_id = run_id
        self.bucket = bucket
        self.object_size = object_size
        self.budget = budget_mod.Budget(run_dir, run_id, limits, self.clock)
        self.ledger = ledger_mod.Ledger(os.path.join(run_dir, "ledger.jsonl"), run_id)
        self._client_factory = client or (lambda url: s3.Client(url))
        self.endpoints = list(endpoints)
        self._local = threading.local()
        self._rr = 0
        self._lock = threading.Lock()
        self._key_locks = {}
        self._versions = {}
        self.may_exist = set()
        self.live_order = []
        self.counts = {}
        self.rng = rng or random.Random(1)
        self._closed = False
        self.terminated = False
        # The watchdog's heartbeat, read by the load itself too: if the
        # guard is gone as well, the load still stops within the timeout.
        self._hb = common.Heartbeat(heartbeat, heartbeat_id or run_id, 120.0, self.clock) if heartbeat else None
        self._hb_lock = threading.Lock()
        self._probe = None
        self._probe_end = threading.Event()
        # Keys no step of the load deletes (the baseline probe's object).
        self.protected = set()

    @property
    def stop_reason(self):
        """Why the run stopped (this process, the stop file, a limit)."""
        return self.budget.stopped()

    # -- helpers ---------------------------------------------------------------

    def _client(self):
        c = getattr(self._local, "c", None)
        if c is None:
            with self._lock:
                url = self.endpoints[self._rr % len(self.endpoints)]
                self._rr += 1
            c = self._local.c = self._client_factory(url)
        return c

    def _key_lock(self, key):
        with self._lock:
            return self._key_locks.setdefault(key, threading.Lock())

    def _count(self, name, n=1):
        with self._lock:
            self.counts[name] = self.counts.get(name, 0) + n

    def stop(self, reason):
        self.budget.stop(reason)

    def stopped(self):
        if self.terminated and not self.budget.stopped():
            self.budget.stop("operator", "sigterm")
        if self._hb is not None:
            with self._hb_lock:
                stale = self._hb.check()
            if stale:
                self.budget.stop("heartbeat-lost", "load")
        return self.budget.stopped()

    # -- one operation ------------------------------------------------------------

    def attempt(self, stage, kind, method, key, op_id, attempt_no, version=None, deadline=None,
                op=None, path=None):
        """One HTTP attempt: permit, intent, request, outcome. ``op`` is the
        ledger's name of the operation (default ``method``; ``BUCKET-PUT``
        and ``LIST`` for the bucket's own requests, with their ``path``).
        Returns the result (``ack``, ``refused``, ``unknown``,
        ``not-sent``, ``found``, ``missing``, ``unexpected``) and leaves
        the response in ``last_response()``; or raises ``Stopped`` /
        ``LiveLimit``. Every data-plane request of the load is sent here."""
        if self.stopped():
            raise budget_mod.Stopped(self.stopped())
        op = op or method
        obj_put = op == "PUT"
        size = self.object_size if obj_put else 0
        new_live = obj_put and key not in self.may_exist
        permit = self.budget.acquire(kind, size, new_live, deadline)
        result = None
        try:
            # Checked again after the waits for a slot and a token, just
            # before anything is written or sent.
            reason = self.stopped()
            if reason:
                raise budget_mod.Stopped(reason)
            data = sha = None
            if obj_put:
                data, sha = s3.body(key, version, size)
            rec = {"type": "intent", "op_id": op_id, "attempt": attempt_no, "key": key, "op": op,
                   "stage": stage, "kind": kind, "budget_attempt": permit.attempt,
                   "wall_ms": self.clock.wall_ms()}
            if obj_put:
                rec.update(version=version, sha256=sha)
                with self._lock:
                    self.may_exist.add(key)
            try:
                self.ledger.write(rec)
            except ledger_mod.LedgerFailure:
                self.stop("ledger-failure")
                raise budget_mod.Stopped("ledger-failure") from None
            r = self._client().request(method, path or f"/{self.bucket}/{key}", data)
            self._local.response = r
            result, detail = self._classify(op, r)
            out = {"type": "outcome", "op_id": op_id, "attempt": attempt_no, "status": r.status,
                   "result": result, "wall_ms": self.clock.wall_ms(), "seconds": round(r.seconds, 6),
                   "rate_wait_s": round(permit.rate_wait_s, 6),
                   "concurrency_wait_s": round(permit.concurrency_wait_s, 6)}
            if r.error:
                out["error"] = r.error[:300]
            if detail:
                out["detail"] = detail
            try:
                self.ledger.write(out)
            except ledger_mod.LedgerFailure:
                self.stop("ledger-failure")
                raise budget_mod.Stopped("ledger-failure") from None
        finally:
            permit.release()
            # A new key's live slot back when it cannot have been stored: the
            # attempt never sent, refused (503 SlowDown, before any write) or
            # never connected.
            if new_live and result in (None, "refused", "not-sent"):
                with self._lock:
                    self.may_exist.discard(key)
                self.budget.live_release()
        self._count(f"{kind}-{op}-{result}")
        if r.status >= 500 and r.status != 503:
            self.stop("unexpected-5xx")
        if method == "DELETE" and result == "ack":
            with self._lock:
                was = key in self.may_exist
                self.may_exist.discard(key)
            if was:
                self.budget.live_release()
        if result == "unexpected":
            self.stop("unexpected-response")
        if result in ("mismatch",):
            self.stop("integrity-violation")
        return result

    def last_response(self):
        return getattr(self._local, "response", None)

    @staticmethod
    def _classify(op, r):
        if r.status == 0:
            # Nothing came back: a request that may have been sent is
            # unknown; one never sent (no connection) is not.
            return ("unknown" if r.sent else "not-sent"), None
        if r.slow_down():
            return "refused", None
        if r.status >= 500:
            # Its effect unknown (and any 5xx but 503 stops the run).
            return "unknown", f"status {r.status}"
        if op == "GET":
            if r.status == 200:
                return "found", None
            if r.status == 404:
                return "missing", None
        elif op in ("PUT", "DELETE"):
            if r.status in (200, 204):
                return "ack", None
        elif r.status == 200:
            return "ack", None
        return "unexpected", f"status {r.status}"

    def operation(self, stage, kind, method, key, deadline=None, retries=RETRIES):
        """A logical operation with its retries after a refusal (each a new
        attempt under the budget). Returns the last attempt's result."""
        with self._key_lock(key):
            op_id = self.ledger.next_operation()
            version = None
            if method == "PUT":
                with self._lock:
                    version = self._versions[key] = self._versions.get(key, 0) + 1
            first_refusal = None
            for n in range(retries + 1):
                result = self.attempt(stage, "retry" if n else kind, method, key, op_id, n + 1,
                                      version, deadline)
                if result != "refused" or n == retries:
                    return result
                now = self.clock.monotonic()
                first_refusal = first_refusal or now
                delay = self.rng.uniform(0, min(RETRY_MAX_S, RETRY_BASE_S * 2 ** n))
                if now + delay - first_refusal > RETRY_WINDOW_S:
                    return result
                if self.clock.sleep(delay, self.budget._cancel):
                    raise budget_mod.Stopped(self.stopped() or "stopped")
            return result

    # -- steps ----------------------------------------------------------------

    def _pool(self, items, fn, rate=None, deadline=None, until=None):
        """Run ``fn(item)`` over ``items`` with the run's concurrency, at most
        ``rate`` starts per second. A worker whose operation ended refused
        waits a growing backoff before its next one (plan §6.1)."""
        items = iter(items)
        it_lock = threading.Lock()
        pace = {"next": self.clock.monotonic()}

        def worker():
            refusals = 0
            while True:
                if self.stopped() or (until and until()):
                    return
                if deadline is not None and self.clock.monotonic() >= deadline:
                    return
                with it_lock:
                    try:
                        item = next(items)
                    except StopIteration:
                        return
                    if rate:
                        now = self.clock.monotonic()
                        start = max(now, pace["next"])
                        pace["next"] = start + 1.0 / rate
                        wait = start - now
                    else:
                        wait = 0
                if wait > 0 and self.clock.sleep(wait, self.budget._cancel):
                    return
                try:
                    result = fn(item)
                except budget_mod.Stopped:
                    return
                except budget_mod.LiveLimit:
                    self._count("live-limit")
                    return
                except ledger_mod.LedgerFailure:
                    self.stop("ledger-failure")
                    return
                if result == "refused":
                    refusals += 1
                    delay = self.rng.uniform(0, min(RETRY_MAX_S, RETRY_BASE_S * 2 ** min(refusals, 5)))
                    if self.clock.sleep(delay, self.budget._cancel):
                        return
                else:
                    refusals = 0

        threads = [threading.Thread(target=worker, daemon=True)
                   for _ in range(int(self.budget.limits["concurrency"]))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def step(self, s, done=lambda: False):
        """Execute one step (a dict) and return its summary."""
        kind, stage = s["kind"], s.get("stage", "")
        t0 = self.clock.monotonic()
        deadline = t0 + s["max_seconds"] if s.get("max_seconds") else None
        with self._lock:
            before = dict(self.counts)
        out = {}
        if kind == "create-bucket":
            try:
                out["result"] = self.attempt(stage, "op", "PUT", None, self.ledger.next_operation(), 1,
                                             deadline=deadline, op="BUCKET-PUT", path=f"/{self.bucket}")
                out["status"] = self.last_response().status
            except budget_mod.Stopped:
                out["result"] = "stopped"
        elif kind in ("put", "delete", "get"):
            method = kind.upper()
            if method == "PUT":
                for k in s["keys"]:
                    if k not in self.live_order:
                        self.live_order.append(k)
            keys = s["keys"]
            if method == "DELETE":
                with self._lock:
                    kept = [k for k in keys if k in self.protected]
                if kept:
                    out["protected_not_deleted"] = kept
                keys = [k for k in keys if k not in kept]
            self._pool(keys, lambda k: self.operation(stage, "op", method, k, deadline),
                       s.get("rate"), deadline)
            if method == "DELETE":
                gone = set(s["keys"]) - self.may_exist
                self.live_order = [k for k in self.live_order if k not in gone]
        elif kind == "put-until-refused":
            prefix, max_new = s["prefix"], s["max_new"]
            state = {"refused_since": None, "reached": False}

            def put_new(k):
                r = self.operation(stage, "op", "PUT", k, deadline)
                now = self.clock.monotonic()
                if r == "refused":
                    state["refused_since"] = state["refused_since"] or now
                    if now - state["refused_since"] >= s.get("refused_for", 30):
                        state["reached"] = True
                elif r == "ack":
                    state["refused_since"] = None
                    self.live_order.append(k)
                return r

            self._pool((f"{prefix}{i:06d}" for i in range(max_new)), put_new, s.get("rate"),
                       deadline, until=lambda: state["reached"])
            out["refusal_reached"] = state["reached"]
        elif kind in ("hold", "probe"):
            i = 0
            # A fixed schedule (start + i * interval): any window of n
            # intervals holds n attempts.
            interval = s.get("interval", 1.0)
            while not done() and not self.stopped():
                if deadline is not None and self.clock.monotonic() >= deadline:
                    break
                try:
                    if kind == "hold" or s.get("put"):
                        self.operation(stage, "probe" if kind == "probe" else "op", "PUT",
                                       f"{s['prefix']}{i:06d}", deadline, retries=0)
                    if kind == "probe" and s.get("get_key"):
                        self.operation(stage, "probe", "GET", s["get_key"], deadline, retries=0)
                except (budget_mod.Stopped, budget_mod.LiveLimit):
                    break
                i += 1
                wait = t0 + i * interval - self.clock.monotonic()
                if wait > 0 and self.clock.sleep(wait, self.budget._cancel):
                    break
            out["attempts"] = i
        elif kind == "churn":
            end = t0 + s["seconds"]
            prefix, rate = s["prefix"], s.get("rate", 1.0)
            i = 0
            while self.clock.monotonic() < end and not self.stopped():
                if deadline is not None and self.clock.monotonic() >= deadline:
                    break
                try:
                    k = f"{prefix}{i:06d}"
                    if self.operation(stage, "op", "PUT", k, deadline) == "ack":
                        self.live_order.append(k)
                    with self._lock:
                        # The oldest live key that is not protected.
                        idx = next((j for j, key in enumerate(self.live_order) if key not in self.protected), None)
                        old = self.live_order.pop(idx) if (
                            idx is not None and len(self.live_order) > s.get("min_live", 0)) else None
                    if old is not None:
                        self.operation(stage, "op", "DELETE", old, deadline)
                except (budget_mod.Stopped, budget_mod.LiveLimit):
                    break
                i += 1
                if self.clock.sleep(1.0 / rate, self.budget._cancel):
                    break
            out["iterations"] = i
        elif kind == "list":
            pages = 0
            token = None
            while pages < s.get("max_pages", 100) and not self.stopped():
                q = "?list-type=2" + (f"&continuation-token={urllib.parse.quote(token, safe='')}" if token else "")
                try:
                    res = self.attempt(stage, "list", "GET", None, self.ledger.next_operation(), 1,
                                       deadline=deadline, op="LIST", path=f"/{self.bucket}{q}")
                except budget_mod.Stopped:
                    break
                pages += 1
                text = self.last_response().payload.decode(errors="replace")
                if res != "ack" or "<IsTruncated>true</IsTruncated>" not in text:
                    out["last"] = res
                    break
                m = re.search(r"<NextContinuationToken>([^<]+)</NextContinuationToken>", text)
                if not m:
                    out["error"] = "a truncated page without a continuation token"
                    break
                token = m.group(1)
            out["pages"] = pages
        elif kind == "protect":
            with self._lock:
                self.protected.update(s["keys"])
        elif kind == "start-probe":
            self.start_probe(s["key"], s.get("interval", 5.0))
        elif kind == "stop-probe":
            self.stop_probe()
        elif kind == "verify":
            out.update(self.verify(stage, deadline))
        elif kind == "finish":
            pass
        else:
            raise ValueError(f"unknown step kind {kind!r}")
        with self._lock:
            now = dict(self.counts)
        out["counts"] = {k: v - before.get(k, 0) for k, v in now.items() if v != before.get(k, 0)}
        out["seconds"] = round(self.clock.monotonic() - t0, 3)
        out["deadline_reached"] = bool(deadline is not None and self.clock.monotonic() >= deadline)
        return out

    def verify(self, stage, deadline=None):
        """Read every key the ledger knows and check it against its candidate
        states (plan §8 C5). Each read is an attempt under the budget."""
        records, torn = ledger_mod.read(self.ledger.path)
        state, unresolved = ledger_mod.expected(records)
        results = {"keys": len(state), "unresolved_attempts": len(unresolved), "torn_tail": torn is not None,
                   "ok": 0, "violations": [], "unknown_resolved": 0, "not_checked": 0}
        lock = threading.Lock()

        def check(key):
            cands = state[key]
            with self._key_lock(key):
                op_id = self.ledger.next_operation()
                res = self.attempt(stage, "verify", "GET", key, op_id, 1, deadline=deadline)
                r = self.last_response()
                if res == "found":
                    got = ("body-sha", hashlib.sha256(r.payload).hexdigest())
                elif res == "missing":
                    got = ("absent",)
                else:
                    got = None
                ok = got is not None and any(
                    (c == ledger_mod.ABSENT and got == ("absent",)) or
                    (c[0] == "body" and got[0] == "body-sha" and c[2] == got[1]) for c in cands)
                try:
                    self.ledger.write({"type": "check", "op_id": op_id, "key": key,
                                       "result": "match" if ok else ("unread" if got is None else "mismatch"),
                                       "wall_ms": self.clock.wall_ms()})
                except ledger_mod.LedgerFailure:
                    self.stop("ledger-failure")
                    raise budget_mod.Stopped("ledger-failure") from None
            with lock:
                if got is None:
                    results["not_checked"] += 1
                elif ok:
                    results["ok"] += 1
                    if len(cands) > 1:
                        results["unknown_resolved"] += 1
                else:
                    results["violations"].append({"key": key, "got": list(got),
                                                  "candidates": sorted(list(c) for c in cands)})
            return "ack"

        self._pool(sorted(state), check, None, deadline)
        checked = results["ok"] + len(results["violations"]) + results["not_checked"]
        results["complete"] = checked == len(state) and results["not_checked"] == 0
        if results["violations"]:
            self.stop("integrity-violation")
        return results

    # -- the baseline-key probe (plan §6.2) ------------------------------------

    def start_probe(self, key, interval=5.0, max_seconds=5.0, failures=3, timeout=5.0):
        """A GET of ``key`` every ``interval`` seconds (a fixed schedule), in
        a thread of its own, until the run stops or ``stop_probe``; each
        under the budget and in the ledger. ``failures`` consecutive
        failures (not found, an error, or a response over ``max_seconds``)
        stop the run (``probe``). Requests time out after ``timeout``."""
        if self._probe is not None:
            return
        with self._lock:
            self.protected.add(key)
        self._probe_end.clear()

        def run():
            c = self._client_factory(self.endpoints[0])
            if hasattr(c, "timeout"):
                c.timeout = timeout
            self._local.c = c
            bad, t0, i = 0, self.clock.monotonic(), 0
            while not self._probe_end.is_set() and not self.stopped():
                try:
                    res = self.operation("probe", "probe", "GET", key, retries=0)
                except (budget_mod.Stopped, budget_mod.LiveLimit, ledger_mod.LedgerFailure):
                    return
                r = self.last_response()
                ok = res == "found" and r is not None and r.seconds <= max_seconds
                bad = 0 if ok else bad + 1
                self._count("baseline-probe-" + ("ok" if ok else "failed"))
                if bad >= failures:
                    self.stop("probe")
                    return
                i += 1
                wait = t0 + i * interval - self.clock.monotonic()
                if wait > 0 and self.clock.sleep(wait, self._probe_end):
                    return

        self._probe = threading.Thread(target=run, daemon=True)
        self._probe.start()

    def stop_probe(self):
        self._probe_end.set()
        if self._probe is not None:
            self._probe.join(60)
            self._probe = None

    def close(self):
        if self._closed:
            return
        self._closed = True
        self.stop_probe()
        self.ledger.close()
        self.budget.close()


def write_status(run_dir, status):
    common.atomic_write_json(os.path.join(run_dir, "status.json"), status)


def run(args, clock=None, client=None):
    with open(os.path.join(args.run_dir, "run.json")) as f:
        run_id = json.load(f)["run_id"]
    limits = json.loads(args.limits) if args.limits else None
    load = Load(args.run_dir, run_id, args.endpoint, limits, clock, client, args.bucket, args.object_size,
                heartbeat=args.heartbeat, heartbeat_id=args.heartbeat_id)
    control = os.path.join(args.run_dir, "control")
    os.makedirs(control, exist_ok=True)
    n, steps = 1, []
    rc = 0
    lock = threading.Lock()
    background = {}  # n -> thread of a step running beside the next ones
    running = set()  # the background steps not ended yet
    ending = threading.Event()  # set at the end: background steps stop

    def status(current):
        with lock:
            write_status(args.run_dir, {"run_id": run_id, "step": n, "running": current, "steps": list(steps),
                                        "background": sorted(running), "budget": load.budget.snapshot()})

    def execute(num, s, done_path):
        try:
            out = load.step(s, done=lambda: os.path.exists(done_path) or ending.is_set() or bool(load.stopped()))
        finally:
            with lock:
                running.discard(num)
        with lock:
            steps.append({"n": num, "kind": s["kind"], "stage": s.get("stage"), "result": out})
        status(None)

    # A SIGTERM (the guard's, after its grace) ends the load like a stop:
    # its status is still written. The handler only sets a flag.
    def on_term(*_):
        load.terminated = True

    try:
        signal.signal(signal.SIGTERM, on_term)
    except ValueError:
        pass  # not the main thread (tests)
    try:
        while True:
            if load.stopped():
                rc = 3
                break
            path = os.path.join(control, f"{n:06d}.json")
            if not os.path.exists(path):
                load.clock.sleep(0.2, load.budget._cancel)
                continue
            s = common.read_json(path)
            done_path = os.path.join(control, f"{n:06d}.done")
            status(s)
            if s.get("background"):
                # The same Load (one budget, one ledger, one stop): a step
                # that runs until its done file, beside the steps after it.
                t = threading.Thread(target=execute, args=(n, s, done_path), daemon=True)
                background[n] = t
                with lock:
                    running.add(n)
                t.start()
            else:
                execute(n, s, done_path)
            n += 1
            if s["kind"] == "finish":
                break
        if load.stopped():
            rc = 3
    finally:
        ending.set()
        for t in background.values():
            t.join(120)
        left = [k for k, t in background.items() if t.is_alive()]
        load.stop_probe()
        with lock:
            write_status(args.run_dir, {"run_id": run_id, "step": n, "running": None, "steps": list(steps),
                                        "background_left": left, "budget": load.budget.snapshot(),
                                        "stopped": load.stop_reason, "exit": "stopped" if rc else "finished"})
        load.close()
    return rc


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("command", choices=["run"])
    p.add_argument("--run-dir", required=True)
    p.add_argument("--endpoint", action="append", required=True)
    p.add_argument("--limits", help="JSON overrides of budget.DEFAULTS (never above them)")
    p.add_argument("--bucket", default="r110")
    p.add_argument("--object-size", type=int, default=256 << 10)
    p.add_argument("--heartbeat-id", help="the run id the heartbeat names (default: this run's)")
    p.add_argument("--heartbeat", help="the watchdog's heartbeat file (stop 120 s after the last beat)")
    args = p.parse_args(argv)
    if args.limits:
        over = {k: v for k, v in json.loads(args.limits).items()
                if k not in budget_mod.DEFAULTS or not budget_mod.valid_limit(v) or v > budget_mod.DEFAULTS[k]}
        if over:
            p.error(f"limits above the plan's or not finite positive numbers: {over}")
    if not 0 < args.object_size <= 256 << 10:
        p.error("object size above the plan's 256 KiB")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
