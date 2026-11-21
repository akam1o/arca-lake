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
        self.dir = self.tmp.name

    def tearDown(self):
        self.tmp.cleanup()

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
        ld.step({"kind": "delete", "keys": ["a"]})
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
