"""P1: the shared budget, the load and the ledger (plan §6.1, §9 P1).

Local only: a virtual clock, scripted clients and a local fake S3. Small
limits stand for the plan's; nothing here is a capacity-test result.
"""

import json
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from r110 import budget, common, ledger, s3
from r110.load import Load
from r110.tests import fakes


def windows(times, width):
    """The most attempts in any window of ``width`` seconds."""
    times = sorted(times)
    best, j = 0, 0
    for i, t in enumerate(times):
        while times[j] <= t - width:
            j += 1
        best = max(best, i - j + 1)
    return best


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        # A cleanup, not tearDown: cleanups run last to first, so every
        # load's close (which joins its probe thread) runs before the
        # directory it writes in is removed. tearDown would run first.
        self.addCleanup(self.tmp.cleanup)
        self.dir = self.tmp.name

    def load(self, client, limits=None, clock=None, size=1024):
        self.clock = clock or common.FakeClock()
        lim = dict(budget.DEFAULTS, **(limits or {}))
        ld = Load(self.dir, "run-t", ["http://x:1"], lim, self.clock, client.factory, object_size=size)
        self.addCleanup(ld.close)
        return ld


class RefusalPacing(Base):
    def test_instant_refusals_do_not_raise_the_request_rate(self):
        """Every PUT answered 503 at once, for 120 virtual seconds: the
        cluster sees at most burst + rate in any second and the rate on
        average (new requests and retries together)."""
        clock = common.FakeClock()
        c = fakes.Scripted(clock, fakes.slow_down)
        ld = self.load(c, clock=clock)
        ld.step({"kind": "put", "stage": "4", "keys": [f"k{i}" for i in range(100_000)], "max_seconds": 120})
        times = [t for t, *_ in c.sent]
        self.assertGreater(len(times), 100)
        self.assertLessEqual(windows(times, 1.0), 25)
        self.assertLessEqual(windows(times, 10.0), 205)
        span = max(times) - min(times)
        self.assertLessEqual(len(times), 20 * span + 5)
        # Every attempt sent was reserved; a reservation whose wait ended at
        # the step's deadline is counted and never sent (one per worker).
        reserved = ld.budget.snapshot()["attempts"]
        self.assertGreaterEqual(reserved, len(times))
        self.assertLessEqual(reserved - len(times), 4)
        kinds = ld.counts
        self.assertGreater(kinds.get("retry-PUT-refused", 0), 0, kinds)

    def test_a_refused_worker_waits_before_its_next_operation(self):
        """With the bucket out of the way (10,000/s), the refusal backoff
        alone bounds what refusals at once can make a worker send. One
        worker: the virtual clock moves only by its own waits."""
        clock = common.FakeClock()
        c = fakes.Scripted(clock, fakes.slow_down)
        ld = self.load(c, {"rate_per_s": 10_000, "burst": 50, "concurrency": 1}, clock)
        ld.step({"kind": "put", "keys": [f"k{i}" for i in range(100_000)], "max_seconds": 60})
        # About 3 attempts per 1.9 s with the pause; about 3 per 0.3 s without.
        self.assertLess(len(c.sent), 300, "no pause after a refusal")

    def test_probes_lists_and_verification_take_the_same_budget(self):
        clock = common.FakeClock()
        c = fakes.Scripted(clock, fakes.ok)
        ld = self.load(c, {"max_attempts": 30}, clock)
        ld.step({"kind": "put", "keys": ["a", "b"]})
        ld.step({"kind": "probe", "prefix": "p", "put": True, "get_key": "a", "interval": 1.0, "max_seconds": 3})
        ld.step({"kind": "list", "max_pages": 1})
        before = ld.budget.snapshot()["attempts"]
        ld.step({"kind": "verify"})
        snap = ld.budget.snapshot()
        self.assertEqual(snap["attempts"], len(c.sent))
        self.assertGreater(snap["attempts"], before)
        kinds = {k for k in ld.counts}
        self.assertTrue(any(k.startswith("probe-") for k in kinds), kinds)


class Limits(Base):
    def test_concurrent_workers_never_cross_the_limits(self):
        """Blocking requests (real time) from every worker at once: the
        attempts, the body bytes and the requests in flight never pass
        their limits, and nothing is sent after the limit."""
        c = fakes.Scripted(common.Clock(), fakes.ok, hold=0.01)
        ld = self.load(c, {"max_attempts": 37, "concurrency": 4, "rate_per_s": 10_000, "burst": 50},
                       clock=common.Clock())
        ld.step({"kind": "put", "keys": [f"k{i}" for i in range(500)]})
        self.assertEqual(len(c.sent), 37)
        self.assertLessEqual(c.max_inflight, 4)
        self.assertEqual(ld.stop_reason, "attempts-limit")
        with self.assertRaises(budget.Stopped):
            ld.budget.acquire("op", 0)

    def test_threads_beyond_the_concurrency_wait_for_a_slot(self):
        c = fakes.Scripted(common.Clock(), fakes.ok, hold=0.02)
        ld = self.load(c, {"concurrency": 2, "rate_per_s": 10_000, "burst": 50}, clock=common.Clock())

        def one(i):
            p = ld.budget.acquire("op", 0)
            try:
                c.request("GET", f"/b/{i}")
            finally:
                p.release()

        threads = [threading.Thread(target=one, args=(i,)) for i in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(c.sent), 16)
        self.assertLessEqual(c.max_inflight, 2)

    def test_body_bytes_are_reserved_before_sending(self):
        c = fakes.Scripted(common.Clock(), fakes.slow_down, hold=0.005)
        ld = self.load(c, {"max_body_bytes": 10 * 1024, "rate_per_s": 10_000, "burst": 50},
                       clock=common.Clock(), size=1024)
        ld.step({"kind": "put", "keys": [f"k{i}" for i in range(100)]})
        sent_bytes = sum(n for *_, n in c.sent)
        self.assertEqual(sent_bytes, 10 * 1024)
        self.assertEqual(ld.stop_reason, "bytes-limit")

    def test_the_live_set_limit_refuses_new_keys_not_deletes(self):
        c = fakes.Scripted(common.FakeClock(), fakes.ok)
        ld = self.load(c, {"max_live": 3})
        ld.step({"kind": "put", "keys": ["a", "b", "c", "d", "e"]})
        self.assertEqual(ld.budget.live_count(), 3)
        self.assertGreater(ld.counts.get("live-limit", 0), 0)
        # Which three were stored depends on the workers' order: delete one
        # the ledger shows acknowledged.
        stored = ledger.acknowledged(ledger.read(ld.ledger.path)[0], "PUT")
        self.assertEqual(len(stored), 3)
        ld.step({"kind": "delete", "keys": [stored[0]]})
        self.assertEqual(ld.budget.live_count(), 2)
        self.assertIsNone(ld.stop_reason)

    def test_waiting_workers_send_nothing_after_a_stop(self):
        c = fakes.Scripted(common.Clock(), fakes.ok, hold=0.05)
        ld = self.load(c, {"concurrency": 1, "rate_per_s": 10_000}, clock=common.Clock())
        t = threading.Thread(target=ld.step, args=({"kind": "put", "keys": [f"k{i}" for i in range(1000)]},))
        t.start()
        time.sleep(0.2)
        ld.stop("operator")
        n = len(c.sent)
        t.join(5)
        self.assertFalse(t.is_alive())
        self.assertLessEqual(len(c.sent), n + 1)  # the one in flight at most

    def test_the_run_deadline_ends_permits(self):
        clock = common.FakeClock()
        c = fakes.Scripted(clock, fakes.ok)
        ld = self.load(c, {"run_seconds": 5}, clock)
        ld.step({"kind": "put", "keys": [f"k{i}" for i in range(10_000)]})
        self.assertEqual(ld.stop_reason, "run-deadline")
        # 5 s at 20/s plus the burst at most.
        self.assertLessEqual(len(c.sent), 105)
        self.assertGreater(len(c.sent), 50)
        with self.assertRaises(budget.Stopped):
            ld.budget.acquire("op", 0)

    def test_a_stop_file_written_by_another_process_stops_the_load(self):
        clock = common.FakeClock()
        c = fakes.Scripted(clock, fakes.ok)
        ld = self.load(c, clock=clock)
        common.StopFile(os.path.join(self.dir, "stop.json")).stop("heartbeat-lost", "guard", 1)
        ld.step({"kind": "put", "keys": ["a"]})
        self.assertEqual(c.sent, [])
        self.assertEqual(ld.stop_reason, "heartbeat-lost")


class FullLiveSet(Base):
    def test_a_full_live_set_skips_hold_ticks_and_the_hold_goes_on(self):
        """Each hold attempt is a new key: with the live set full every tick
        is skipped (counted), and the schedule continues to its end."""
        clock = common.FakeClock()
        c = fakes.Scripted(clock, fakes.ok)
        ld = self.load(c, {"max_live": 2}, clock)
        ld.step({"kind": "put", "keys": ["a", "b"]})
        out = ld.step({"kind": "hold", "prefix": "h", "interval": 1.0, "max_seconds": 10})
        self.assertGreaterEqual(out["attempts"], 9)
        self.assertGreaterEqual(ld.counts.get("hold-live-limit", 0), 9)
        self.assertIsNone(ld.stop_reason)


class LoadHeartbeat(Base):
    def test_the_load_stops_by_itself_when_the_heartbeat_goes_stale(self):
        """The guard gone too: the load reads the heartbeat itself."""
        clock = common.FakeClock()
        c = fakes.Scripted(clock, fakes.ok)
        hb = os.path.join(self.dir, "hb.json")
        with open(hb, "w") as f:
            json.dump({"run_id": "run-t", "seq": 1}, f)
        lim = dict(budget.DEFAULTS, concurrency=1)  # one worker: the virtual clock moves only by its waits
        ld = Load(self.dir, "run-t", ["http://x:1"], lim, clock, c.factory, object_size=10, heartbeat=hb)
        self.addCleanup(ld.close)
        ld.step({"kind": "put", "keys": [f"k{i}" for i in range(100_000)], "max_seconds": 600})
        self.assertEqual(ld.stop_reason, "heartbeat-lost")
        self.assertLessEqual(max(t for t, *_ in c.sent), 1000 + 120)


class NoResume(Base):
    def test_a_run_is_never_restarted_with_a_fresh_budget(self):
        c = fakes.Scripted(common.FakeClock(), fakes.ok)
        ld = self.load(c)
        with self.assertRaises(budget.RunExists):
            budget.Budget(self.dir, "run-t", clock=common.FakeClock())
        ld.close()
        with self.assertRaises(budget.RunExists):
            budget.Budget(self.dir, "run-t", clock=common.FakeClock())
        with self.assertRaises(ledger.LedgerFailure):
            ledger.Ledger(os.path.join(self.dir, "ledger.jsonl"), "run-t")


class LedgerSafety(Base):
    def test_an_attempt_without_an_outcome_is_both_states(self):
        recs = [{"type": "intent", "op_id": 1, "attempt": 1, "key": "k", "op": "PUT", "version": 1, "sha256": "a"},
                {"type": "outcome", "op_id": 1, "attempt": 1, "result": "ack"},
                {"type": "intent", "op_id": 2, "attempt": 1, "key": "k", "op": "DELETE"}]
        state, unresolved = ledger.expected(recs)
        self.assertEqual(state["k"], {("body", 1, "a"), ledger.ABSENT})
        self.assertEqual(unresolved, [(2, 1)])

    def test_refused_keeps_unknown_adds_ack_replaces(self):
        recs = [{"type": "intent", "op_id": 1, "attempt": 1, "key": "k", "op": "PUT", "version": 1, "sha256": "a"},
                {"type": "outcome", "op_id": 1, "attempt": 1, "result": "refused"},
                {"type": "intent", "op_id": 1, "attempt": 2, "key": "k", "op": "PUT", "version": 1, "sha256": "a"},
                {"type": "outcome", "op_id": 1, "attempt": 2, "result": "unknown"}]
        self.assertEqual(ledger.expected(recs)[0]["k"], {ledger.ABSENT, ("body", 1, "a")})

    def test_a_torn_tail_is_reported_and_corruption_is_an_error(self):
        p = os.path.join(self.dir, "l")
        lg = ledger.Ledger(p, "r")
        lg.write({"type": "intent", "op_id": 1, "attempt": 1, "key": "k", "op": "PUT", "version": 1, "sha256": "a"})
        lg.close()
        with open(p, "a") as f:
            f.write('0000 {"type":"outc')
        recs, torn = ledger.read(p)
        self.assertEqual(len(recs), 1)
        self.assertIsNotNone(torn)
        with open(p, "w") as f:
            f.write('deadbeef {"type":"intent"}\n' + ledger._line({"a": 1}))
        with self.assertRaises(ledger.LedgerFailure):
            ledger.read(p)

    def test_a_ledger_write_failure_stops_the_load_before_sending(self):
        c = fakes.Scripted(common.FakeClock(), fakes.ok)
        ld = self.load(c)

        class Broken:
            def write(self, _):
                raise OSError(28, "No space left on device")

            def flush(self):
                pass

            def fileno(self):
                return 0

            def close(self):
                pass

        ld.ledger._f = Broken()
        ld.step({"kind": "put", "keys": ["a", "b", "c"]})
        self.assertEqual(c.sent, [])
        self.assertEqual(ld.stop_reason, "ledger-failure")
        self.assertEqual(common.StopFile(os.path.join(self.dir, "stop.json")).reason()["reason"],
                         "ledger-failure")


class AgainstAFakeS3(Base):
    def setUp(self):
        super().setUp()
        self.s3 = fakes.FakeS3()
        self.addCleanup(self.s3.close)

    def real(self, limits=None):
        lim = dict(budget.DEFAULTS, rate_per_s=10_000, burst=50, **(limits or {}))
        ld = Load(self.dir, "run-t", [self.s3.url], lim, common.Clock(), None, object_size=4096)
        self.addCleanup(ld.close)
        return ld

    def test_writes_deletes_and_the_verification(self):
        ld = self.real()
        ld.step({"kind": "create-bucket"})
        ld.step({"kind": "put", "keys": [f"k{i}" for i in range(20)]})
        ld.step({"kind": "delete", "keys": [f"k{i}" for i in range(5)]})
        ld.step({"kind": "put", "keys": ["k6"]})  # a second version
        out = ld.step({"kind": "verify"})
        self.assertTrue(out["complete"], out)
        self.assertEqual((out["ok"], out["violations"]), (20, []))
        data, _ = s3.body("k6", 2, 4096)
        self.assertEqual(self.s3.objects["k6"], data)

    def test_a_wrong_body_is_an_integrity_violation(self):
        ld = self.real()
        ld.step({"kind": "put", "keys": ["a", "b"]})
        self.s3.corrupt.add("b")
        out = ld.step({"kind": "verify"})
        self.assertEqual([v["key"] for v in out["violations"]], ["b"])
        self.assertEqual(ld.stop_reason, "integrity-violation")

    def test_a_deleted_key_that_reappears_is_a_violation(self):
        ld = self.real()
        ld.step({"kind": "put", "keys": ["a"]})
        ld.step({"kind": "delete", "keys": ["a"]})
        self.s3.objects["a"] = s3.body("a", 1, 4096)[0]
        out = ld.step({"kind": "verify"})
        self.assertEqual(len(out["violations"]), 1)

    def test_refused_puts_are_not_expected_to_exist(self):
        ld = self.real()
        self.s3.refuse_puts = True
        ld.step({"kind": "put", "keys": ["a"]})
        self.s3.refuse_puts = False
        out = ld.step({"kind": "verify"})
        self.assertEqual((out["ok"], out["violations"]), (1, []))
        recs, _ = ledger.read(ld.ledger.path)
        self.assertEqual(sum(1 for r in recs if r.get("result") == "refused"), 3)  # 1 + 2 retries


class ReviewFindings(Base):
    """Cases from the independent review of P1."""

    def test_limits_that_are_not_finite_are_refused(self):
        for bad in (float("nan"), float("inf"), -1, 0, True):
            with self.assertRaises(ValueError, msg=bad):
                budget.Budget(os.path.join(self.dir, f"r{bad}"), "r", {"max_attempts": bad}, common.FakeClock())
        from r110 import load
        for text in ('{"rate_per_s": NaN}', '{"max_body_bytes": Infinity}'):
            with self.assertRaises(SystemExit):
                load.main(["run", "--run-dir", self.dir, "--endpoint", "http://x:1", "--limits", text])

    def test_bucket_and_listing_requests_are_in_the_ledger_and_the_budget(self):
        clock = common.FakeClock()
        c = fakes.Scripted(clock, lambda m, path, d: s3.Response(
            200, b"<IsTruncated>true</IsTruncated><NextContinuationToken>a/b+c</NextContinuationToken>"
            if "list-type" in path else b"", sent=True))
        ld = self.load(c, clock=clock)
        ld.step({"kind": "create-bucket"})
        out = ld.step({"kind": "list", "max_pages": 3})
        self.assertEqual(out["pages"], 3)
        recs, _ = ledger.read(ld.ledger.path)
        ops = [r["op"] for r in recs if r["type"] == "intent"]
        self.assertEqual(ops, ["BUCKET-PUT", "LIST", "LIST", "LIST"])
        self.assertEqual(sum(1 for r in recs if r["type"] == "outcome"), 4)
        self.assertEqual(ld.budget.snapshot()["attempts"], len(c.sent))
        self.assertIn("continuation-token=a%2Fb%2Bc", c.sent[-1][2])

    def broken_after(self, ld, n):
        real = ld.ledger._f

        class Breaks:
            count = 0

            def write(self, text):
                Breaks.count += 1
                if Breaks.count > n:
                    raise OSError(28, "No space left on device")
                return real.write(text)

            def flush(self):
                real.flush()

            def fileno(self):
                return real.fileno()

            def close(self):
                real.close()

        ld.ledger._f = Breaks()

    def test_an_outcome_that_cannot_be_written_stops_the_load(self):
        c = fakes.Scripted(common.FakeClock(), fakes.ok)
        ld = self.load(c)
        self.broken_after(ld, 1)  # the intent written, its outcome not
        ld.step({"kind": "put", "keys": ["a"]})  # nothing after it to fail too
        self.assertEqual(len(c.sent), 1)
        self.assertEqual(ld.stop_reason, "ledger-failure")

    def test_a_ledger_failure_during_the_verification_stops_the_run(self):
        c = fakes.Scripted(common.FakeClock(), fakes.ok)
        ld = self.load(c)
        ld.step({"kind": "put", "keys": ["k"]})
        self.broken_after(ld, 2)  # the verify GET's intent and outcome written, its check record not
        out = ld.step({"kind": "verify"})
        self.assertEqual(ld.stop_reason, "ledger-failure")
        self.assertFalse(out["complete"])

    def test_a_5xx_other_than_503_stops_the_run(self):
        clock = common.FakeClock()
        c = fakes.Scripted(clock, lambda *a: s3.Response(500, b"", sent=True))
        ld = self.load(c, clock=clock)
        ld.step({"kind": "put", "keys": [f"k{i}" for i in range(50)]})
        self.assertEqual(ld.stop_reason, "unexpected-5xx")
        self.assertLessEqual(len(c.sent), 4)  # the ones already in flight at most
        self.assertEqual(common.reason_class("unexpected-5xx"), "anomaly")

    def test_a_503_that_is_not_slow_down_is_unknown_and_does_not_stop(self):
        clock = common.FakeClock()
        c = fakes.Scripted(clock, lambda *a: s3.Response(503, b"<Code>ServiceUnavailable</Code>", sent=True))
        ld = self.load(c, clock=clock)
        ld.step({"kind": "put", "keys": ["a"]})
        self.assertIsNone(ld.stop_reason)
        self.assertEqual(ld.counts, {"op-PUT-unknown": 1})

    def test_a_new_key_stopped_in_its_waits_gives_its_live_slot_back(self):
        clock = common.FakeClock()
        b = budget.Budget(os.path.join(self.dir, "b"), "r", {"rate_per_s": 1, "burst": 1}, clock)
        self.addCleanup(b.close)
        b.acquire("op", 0, new_live=True).release()
        with self.assertRaises(budget.Stopped):
            b.acquire("op", 0, new_live=True, stage_deadline=clock.monotonic())  # stage deadline in the token wait
        self.assertEqual(b.live_count(), 1)

    def test_a_late_unknown_write_stays_a_candidate_after_an_acknowledged_one(self):
        recs = [{"type": "intent", "op_id": 1, "attempt": 1, "key": "k", "op": "PUT", "version": 1, "sha256": "a"},
                {"type": "outcome", "op_id": 1, "attempt": 1, "result": "unknown"},
                {"type": "intent", "op_id": 2, "attempt": 1, "key": "k", "op": "DELETE"},
                {"type": "outcome", "op_id": 2, "attempt": 1, "result": "ack"}]
        self.assertEqual(ledger.expected(recs)[0]["k"], {ledger.ABSENT, ("body", 1, "a")})

    def test_a_step_that_fails_stops_the_run(self):
        c = fakes.Scripted(common.FakeClock(), fakes.ok)
        ld = self.load(c)
        with mock.patch.object(ld, "operation", side_effect=RuntimeError("bug")):
            ld.step({"kind": "put", "keys": ["a"]})
        self.assertEqual(ld.stop_reason, "load-error")

    def test_a_refused_new_key_gives_its_live_slot_back(self):
        clock = common.FakeClock()
        refuse = {"on": True}
        c = fakes.Scripted(clock, lambda m, *a: fakes.slow_down() if refuse["on"] else fakes.ok(m))
        ld = self.load(c, {"max_live": 3}, clock)
        ld.step({"kind": "put", "keys": [f"r{i}" for i in range(20)], "max_seconds": 60})
        self.assertEqual(ld.budget.live_count(), 0)
        refuse["on"] = False
        ld.step({"kind": "put", "keys": ["a", "b", "c"]})
        self.assertEqual(ld.budget.live_count(), 3)
        self.assertEqual(ld.counts.get("op-PUT-ack"), 3)

    def test_a_refused_connection_was_not_sent(self):
        import socket
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        port = srv.getsockname()[1]
        srv.close()
        r = s3.Client(f"http://127.0.0.1:{port}", timeout=2).request("PUT", "/b/k", b"x")
        self.assertEqual((r.status, r.sent), (0, False))
        recs = [{"type": "intent", "op_id": 1, "attempt": 1, "key": "k", "op": "PUT", "version": 1, "sha256": "a"},
                {"type": "outcome", "op_id": 1, "attempt": 1, "result": "not-sent"}]
        self.assertEqual(ledger.expected(recs)[0]["k"], {ledger.ABSENT})

    def test_the_rate_wait_is_recorded_apart_from_the_response_time(self):
        clock = common.FakeClock()
        c = fakes.Scripted(clock, lambda m, *a: s3.Response(200, b"", sent=True, seconds=0.01))
        ld = self.load(c, {"concurrency": 1}, clock)
        ld.step({"kind": "put", "keys": [f"k{i}" for i in range(20)]})
        outs = [r for r in ledger.read(ld.ledger.path)[0] if r["type"] == "outcome"]
        self.assertTrue(all(o["seconds"] == 0.01 for o in outs))
        self.assertGreater(max(o["rate_wait_s"] for o in outs), 0.0)
        self.assertEqual(outs[0]["rate_wait_s"], 0.0)  # within the burst

    def test_the_baseline_probe_stops_the_run_after_three_failures(self):
        clock = common.Clock()
        state = {"n": 0}

        def answer(m, path, d):
            state["n"] += 1
            if state["n"] <= 2:
                return s3.Response(200, b"x", sent=True, seconds=0.01)
            return s3.Response(200, b"x", sent=True, seconds=6.0)  # too slow

        c = fakes.Scripted(clock, answer)
        ld = self.load(c, {"rate_per_s": 1000, "burst": 50}, clock)
        ld.start_probe("b0", interval=0.05)
        for _ in range(200):
            if ld.stop_reason:
                break
            time.sleep(0.02)
        # Plan §6.2: one answer over 5 s stops it (two fast, then the slow one).
        self.assertEqual(ld.stop_reason, "probe")
        self.assertEqual(state["n"], 3)

    def test_three_failures_in_a_row_stop_the_run(self):
        clock = common.Clock()
        state = {"n": 0}

        def answer(m, path, d):
            state["n"] += 1
            return s3.Response(200 if state["n"] in (1, 3) else 404, b"x", sent=True, seconds=0.01)

        c = fakes.Scripted(clock, answer)
        ld = self.load(c, {"rate_per_s": 1000, "burst": 50}, clock)
        ld.start_probe("b0", interval=0.05)
        for _ in range(200):
            if ld.stop_reason:
                break
            time.sleep(0.02)
        # found, missing, found, missing, missing, missing: the third in a row.
        self.assertEqual((ld.stop_reason, state["n"]), ("probe", 6))
        self.assertEqual(common.reason_class("probe"), "anomaly")


class Cleanup(unittest.TestCase):
    def test_a_test_directory_is_removed_only_after_the_load_stopped_writing(self):
        """CI failed with "Directory not empty" at the temporary directory's
        cleanup: the probe thread sets the stop reason, then writes the stop
        file; the test sees the reason and returns, and the directory was
        removed (tearDown) before the load was closed (a cleanup) and its
        thread joined. A slow stop-file write (a slow disk) makes that
        window certain: the directory must still hold the finished stop
        file when it is removed."""
        real_link, real_cleanup = common.link_new, tempfile.TemporaryDirectory.cleanup
        seen = []

        def slow_link(path, text):
            time.sleep(0.5)  # the injected slow disk: the write ends after the stop is seen
            return real_link(path, text)

        def cleanup(td):
            seen.append(os.path.exists(os.path.join(td.name, "stop.json")))
            real_cleanup(td)

        result = unittest.TestResult()
        with mock.patch.object(common, "link_new", slow_link), \
                mock.patch.object(tempfile.TemporaryDirectory, "cleanup", cleanup):
            ReviewFindings("test_the_baseline_probe_stops_the_run_after_three_failures").run(result)
        self.assertTrue(result.wasSuccessful(), result.errors + result.failures)
        self.assertEqual(seen, [True], "the directory was removed while the load was still writing in it")


class Process(Base):
    def test_steps_from_the_control_directory_and_the_exit_status(self):
        srv = fakes.FakeS3()
        self.addCleanup(srv.close)
        with open(os.path.join(self.dir, "run.json"), "w") as f:
            json.dump({"run_id": "run-p"}, f)
        ctl = os.path.join(self.dir, "control")
        os.makedirs(ctl)
        for n, s in enumerate([{"kind": "create-bucket"}, {"kind": "put", "keys": ["a"]},
                               {"kind": "verify"}, {"kind": "finish"}], 1):
            with open(os.path.join(ctl, f"{n:06d}.json"), "w") as f:
                json.dump(s, f)
        from r110 import load
        rc = load.main(["run", "--run-dir", self.dir, "--endpoint", srv.url, "--object-size", "512",
                        "--limits", json.dumps({"rate_per_s": 20})])
        self.assertEqual(rc, 0)
        st = common.read_json(os.path.join(self.dir, "status.json"))
        self.assertEqual(st["exit"], "finished")
        self.assertEqual(st["steps"][2]["result"]["ok"], 1)
        # A second process of the same run is refused.
        with self.assertRaises(budget.RunExists):
            load.main(["run", "--run-dir", self.dir, "--endpoint", srv.url])

    def test_limits_above_the_plan_are_refused(self):
        from r110 import load
        with self.assertRaises(SystemExit):
            load.main(["run", "--run-dir", self.dir, "--endpoint", "http://x:1",
                       "--limits", json.dumps({"rate_per_s": 21})])


if __name__ == "__main__":
    unittest.main()
