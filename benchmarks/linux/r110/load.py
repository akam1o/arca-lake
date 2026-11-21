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
``interval`` seconds, never retried, until done); ``probe`` (a GET of
``get_key`` and, if ``put``, a PUT of a new key, every ``interval``
seconds, until done); ``churn`` (``rate`` PUT of a new key and DELETE of
the oldest live key per second, for ``seconds``); ``list`` (pages);
``verify`` (every key the ledger has, checked against its candidates);
``finish``.
"""

import argparse
import hashlib
import json
import os
import random
import sys
import threading

from . import budget as budget_mod
from . import common, ledger as ledger_mod, s3

# A 503 is retried at most this many times (plan §6.1): backoff from
# RETRY_BASE_S doubling up to RETRY_MAX_S, full jitter, and no retry starts
# later than RETRY_WINDOW_S after the first refusal.
RETRIES, RETRY_BASE_S, RETRY_MAX_S, RETRY_WINDOW_S = 2, 0.2, 3.2, 10.0


class Load:
    def __init__(self, run_dir, run_id, endpoints, limits=None, clock=None, client=None,
                 bucket="r110", object_size=256 << 10, rng=None, heartbeat=None):
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
        # The watchdog's heartbeat, read by the load itself too: if the
        # guard is gone as well, the load still stops within the timeout.
        self._hb = common.Heartbeat(heartbeat, run_id, 120.0, self.clock) if heartbeat else None
        self._hb_next = 0.0

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
        if self._hb is not None:
            now = self.clock.monotonic()
            if now >= self._hb_next:
                self._hb_next = now + 1.0
                if self._hb.check():
                    self.budget.stop("heartbeat-lost", "load")
        return self.budget.stopped()

    # -- one operation ------------------------------------------------------------

    def attempt(self, stage, kind, method, key, op_id, attempt_no, version=None, deadline=None):
        """One HTTP attempt: permit, intent, request, outcome. Returns the
        result (``ack``, ``refused``, ``unknown``, ``rejected``) or raises
        ``Stopped`` / ``LiveLimit``."""
        if self.stopped():
            raise budget_mod.Stopped(self.stopped())
        size = self.object_size if method == "PUT" else 0
        new_live = method == "PUT" and key not in self.may_exist
        permit = self.budget.acquire(kind, size, new_live, deadline)
        try:
            data = sha = None
            if method == "PUT":
                data, sha = s3.body(key, version, size)
            rec = {"type": "intent", "op_id": op_id, "attempt": attempt_no, "key": key, "op": method,
                   "stage": stage, "kind": kind, "budget_attempt": permit.attempt,
                   "wall_ms": self.clock.wall_ms()}
            if method == "PUT":
                rec.update(version=version, sha256=sha)
                with self._lock:
                    self.may_exist.add(key)
            try:
                self.ledger.write(rec)
            except ledger_mod.LedgerFailure:
                self.stop("ledger-failure")
                raise budget_mod.Stopped("ledger-failure") from None
            path = f"/{self.bucket}/{key}" if key else f"/{self.bucket}"
            r = self._client().request(method, path, data)
            result, detail = self._classify(method, r, key, version)
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
        self._count(f"{kind}-{method}-{result}")
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

    def _classify(self, method, r, key, version):
        if r.status == 0:
            # Nothing came back: a request that may have been sent is
            # unknown; one never sent (no connection) is not.
            return ("unknown" if r.sent else "not-sent"), None
        if method in ("PUT", "DELETE"):
            if r.status in (200, 204):
                return "ack", None
            if r.slow_down():
                return "refused", None
            if r.status >= 500:
                return "unknown", None
            return "unexpected", f"status {r.status}"
        if method == "GET":
            if r.status == 200:
                return "found", None
            if r.status == 404:
                return "missing", None
            if r.slow_down():
                return "refused", None
            return ("unknown" if r.status >= 500 else "unexpected"), f"status {r.status}"
        return ("ack" if r.status < 300 else "unknown" if r.status >= 500 else "unexpected"), None

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
        before = dict(self.counts)
        out = {}
        if kind == "create-bucket":
            permit = self.budget.acquire("op", 0, False, deadline)
            try:
                r = self._client().request("PUT", f"/{self.bucket}")
            finally:
                permit.release()
            out["status"] = r.status
        elif kind in ("put", "delete", "get"):
            method = kind.upper()
            if method == "PUT":
                for k in s["keys"]:
                    if k not in self.live_order:
                        self.live_order.append(k)
            self._pool(s["keys"], lambda k: self.operation(stage, "op", method, k, deadline),
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
                if self.clock.sleep(s.get("interval", 1.0), self.budget._cancel):
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
                    if self.live_order and len(self.live_order) > s.get("min_live", 0):
                        old = self.live_order.pop(0)
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
                try:
                    permit = self.budget.acquire("list", 0, False, deadline)
                except budget_mod.Stopped:
                    break
                try:
                    q = "?list-type=2" + (f"&continuation-token={token}" if token else "")
                    r = self._client().request("GET", f"/{self.bucket}{q}")
                finally:
                    permit.release()
                pages += 1
                text = r.payload.decode(errors="replace")
                if r.status != 200 or "<IsTruncated>true</IsTruncated>" not in text:
                    break
                token = text.split("<NextContinuationToken>")[1].split("<")[0]
            out["pages"] = pages
        elif kind == "verify":
            out.update(self.verify(stage, deadline))
        elif kind == "finish":
            pass
        else:
            raise ValueError(f"unknown step kind {kind!r}")
        out["counts"] = {k: v - before.get(k, 0) for k, v in self.counts.items() if v != before.get(k, 0)}
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
                permit = self.budget.acquire("verify", 0, False, deadline)
                try:
                    self.ledger.write({"type": "intent", "op_id": op_id, "attempt": 1, "key": key,
                                       "op": "GET", "stage": stage, "kind": "verify",
                                       "wall_ms": self.clock.wall_ms()})
                    r = self._client().request("GET", f"/{self.bucket}/{key}")
                    if r.status == 200:
                        got = ("body-sha", hashlib.sha256(r.payload).hexdigest())
                    elif r.status == 404:
                        got = ("absent",)
                    else:
                        got = None
                    ok = got is not None and any(
                        (c == ledger_mod.ABSENT and got == ("absent",)) or
                        (c[0] == "body" and got[0] == "body-sha" and c[2] == got[1]) for c in cands)
                    self.ledger.write({"type": "outcome", "op_id": op_id, "attempt": 1, "status": r.status,
                                       "result": "match" if ok else ("unread" if got is None else "mismatch"),
                                       "wall_ms": self.clock.wall_ms()})
                finally:
                    permit.release()
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

        try:
            self._pool(sorted(state), check, None, deadline)
        except ledger_mod.LedgerFailure:
            self.stop("ledger-failure")
        checked = results["ok"] + len(results["violations"]) + results["not_checked"]
        results["complete"] = checked == len(state) and results["not_checked"] == 0
        if results["violations"]:
            self.stop("integrity-violation")
        return results

    def close(self):
        if self._closed:
            return
        self._closed = True
        self.ledger.close()
        self.budget.close()


def write_status(run_dir, status):
    common.atomic_write_json(os.path.join(run_dir, "status.json"), status)


def run(args, clock=None, client=None):
    with open(os.path.join(args.run_dir, "run.json")) as f:
        run_id = json.load(f)["run_id"]
    limits = json.loads(args.limits) if args.limits else None
    load = Load(args.run_dir, run_id, args.endpoint, limits, clock, client, args.bucket, args.object_size,
                heartbeat=args.heartbeat)
    control = os.path.join(args.run_dir, "control")
    os.makedirs(control, exist_ok=True)
    n, steps = 1, []
    rc = 0
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
            write_status(args.run_dir, {"run_id": run_id, "step": n, "running": s, "steps": steps,
                                        "budget": load.budget.snapshot()})
            out = load.step(s, done=lambda: os.path.exists(done_path) or bool(load.stopped()))
            steps.append({"n": n, "kind": s["kind"], "stage": s.get("stage"), "result": out})
            n += 1
            if s["kind"] == "finish":
                break
    finally:
        write_status(args.run_dir, {"run_id": run_id, "step": n, "running": None, "steps": steps,
                                    "budget": load.budget.snapshot(), "stopped": load.stop_reason,
                                    "exit": "stopped" if rc else "finished"})
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
    p.add_argument("--heartbeat", help="the watchdog's heartbeat file (stop 120 s after the last beat)")
    args = p.parse_args(argv)
    if args.limits:
        over = {k: v for k, v in json.loads(args.limits).items()
                if k not in budget_mod.DEFAULTS or v > budget_mod.DEFAULTS[k]}
        if over:
            p.error(f"limits above the plan's: {over}")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
