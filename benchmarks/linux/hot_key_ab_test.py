"""Local tests of hot-key-ab.py (no host): rotation, parsing, invalid-run
rules and the judgment of the plan's targets.

  python3 -I benchmarks/linux/hot_key_ab_test.py -v
"""

import importlib.util
import json
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location("hot_key_ab", Path(__file__).resolve().parent / "hot-key-ab.py")
ab = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ab)

GiB = 1 << 30


class Schedule(unittest.TestCase):
    def test_every_cell_has_its_minimum_and_the_order_rotates(self):
        s = ab.schedule()
        self.assertEqual(len(s), 3 * 8 + 2 * 2)
        for a, sc in ab.cells():
            n = sum(1 for x in s if x["arm"] == a and x["scenario"] == sc)
            self.assertEqual(n, ab.MIN_VALID[ab.kind(sc)], (a, sc))
        for sc in ab.CORRECTNESS:
            self.assertFalse([x for x in s if x["scenario"] == sc and x["arm"] == "tag"])
        perf = [x for x in s if x["scenario"] in ab.PERF]
        firsts = {}
        for r in (1, 2, 3):
            rnd = [x for x in perf if x["round"] == r]
            firsts[r] = rnd[0]["scenario"]
            for i in range(0, len(rnd), 2):
                self.assertEqual(rnd[i]["scenario"], rnd[i + 1]["scenario"])
                self.assertEqual(rnd[i]["arm"], "tag" if r % 2 == 1 else "candidate")
        self.assertEqual(firsts, {1: "unv", 2: "ver", 3: "retry"})
        # A correctness round follows each of the first two perf rounds.
        self.assertEqual([x["scenario"] for x in s[8:10]], ["fdbstall", "gwkill"])
        self.assertEqual([x["scenario"] for x in s[18:20]], ["gwkill", "fdbstall"])

    def test_reruns_only_for_short_cells_and_bounded(self):
        recs = [{"arm": a, "scenario": sc, "invalid": False} for a, sc in ab.cells()
                for _ in range(ab.MIN_VALID[ab.kind(sc)])]
        self.assertEqual(ab.reruns_needed(recs, {}), [])
        recs[0]["invalid"] = True
        cell = (recs[0]["arm"], recs[0]["scenario"])
        self.assertEqual(ab.reruns_needed(recs, {}), [cell])
        self.assertEqual(ab.reruns_needed(recs, {f"{cell[0]}/{cell[1]}": ab.MAX_EXTRA_PER_CELL}), [])


PROM_BEFORE = """--- gw0
# TYPE arca_gateway_meta_txn_attempts_total counter
arca_gateway_meta_txn_attempts_total 100
arca_gateway_commit_queue_turns_total 5
--- gw1
arca_gateway_meta_txn_attempts_total 50
"""
PROM_AFTER = """--- gw0
arca_gateway_meta_txn_attempts_total 400
arca_gateway_commit_queue_turns_total 9
--- gw1
arca_gateway_meta_txn_attempts_total 150
"""


class Parsing(unittest.TestCase):
    def test_prometheus_sections_and_deltas(self):
        b, a = ab.parse_prom(PROM_BEFORE), ab.parse_prom(PROM_AFTER)
        self.assertEqual(b["gw0/arca_gateway_meta_txn_attempts_total"], 100)
        self.assertEqual(ab.counter_delta(b, a, "arca_gateway_meta_txn_attempts_total"), 400)
        self.assertEqual(ab.counter_delta(b, a, "arca_gateway_commit_queue_turns_total"), 4)
        # Absent in the tag rebuild: None, not 0.
        self.assertIsNone(ab.counter_delta(b, a, "arca_gateway_commit_queue_conflicts_total"))
        # A restarted gateway (its counter went down): no delta.
        r = ab.parse_prom(PROM_AFTER.replace("150", "10"))
        self.assertIsNone(ab.counter_delta(b, r, "arca_gateway_meta_txn_attempts_total"))

    def test_labelled_metric_sum(self):
        snap = ab.parse_prom('--- n0\narca_node_index_entries{kind="live"} 7\narca_node_index_entries{kind="dead_tracked"} 2\n'
                             '--- n1\narca_node_index_entries{kind="live"} 5\narca_node_reclaim_pending_records 0\n')
        live = {k: v for k, v in snap.items() if 'kind="live"' in k}
        self.assertEqual(ab.metric_sum(live, "arca_node_index_entries"), 12)
        self.assertEqual(ab.metric_sum(snap, "arca_node_reclaim_pending_records"), 0)
        self.assertIsNone(ab.metric_sum(snap, "arca_controller_gc_backlog"))

    def test_fdb_status_counters(self):
        st = {"client": {"database_status": {"available": True, "healthy": True}},
              "cluster": {"workload": {"transactions": {"started": {"counter": 10}, "committed": {"counter": 8},
                                                        "conflicted": {"counter": 2}},
                                       "operations": {"reads": {"counter": 30}, "writes": {"counter": 4}}},
                          "latency_probe": {"commit_seconds": 0.002}}}
        c = ab.fdb_counters(st)
        self.assertTrue(c["available"] and c["healthy"])
        st["cluster"]["workload"]["transactions"]["conflicted"]["counter"] = 12
        d = ab.fdb_delta(c, ab.fdb_counters(st))
        self.assertEqual((d["tx_conflicted"], d["tx_started"], d["tx_rejected"]), (10, 0, None))
        self.assertFalse(ab.fdb_counters({})["healthy"])

    def test_put_outcomes_as_the_checker_classifies_them(self):
        def op(i, att, sent, done, status, code=""):
            return json.dumps({"op": "put", "key": 0, "id": i, "attempt": att, "sent_us": sent, "done_us": done,
                               "status": status, "code": code})
        lines = [
            op(1, 0, 0, 100_000, 200),                       # acked, 100 ms
            op(2, 0, 0, 500_000, 503, "SlowDown"),           # definite failure
            op(3, 0, 0, 500_000, 503, "SlowDown"),           # retried, then acked
            op(3, 1, 600_000, 900_000, 200),
            op(4, 0, 0, 500_000, 503, "SlowDown"),           # retried, then unknown
            op(4, 1, 600_000, 2_000_000, 500, "InternalError"),
            op(5, 0, 0, 1_000_000, 0),                       # transport: unknown
            json.dumps({"op": "get", "key": 0, "id": 1, "sent_us": 10, "done_us": 20, "status": 200}),
        ]
        m = ab.summarize_ops(lines)
        self.assertEqual((m["puts"], m["put_attempts"], m["acked"], m["slowdown"], m["unknown"]), (5, 7, 2, 1, 2))
        self.assertEqual(m["unknown_by_cause"], {"500 InternalError": 1, "0 transport": 1})
        self.assertEqual(m["slowdown_share"], 0.2)
        self.assertEqual(m["duration_s"], 2.0)
        self.assertEqual(m["acked_per_s"], 1.0)
        self.assertEqual(m["ack_p50_ms"], 100.0)
        self.assertEqual(m["ack_p99_ms"], 900.0)
        self.assertEqual(m["all_p99_ms"], 2000.0)
        self.assertEqual(m["gets"], {"total": 1, "ok": 1})

    def test_percentile_nearest_rank(self):
        xs = list(range(1, 101))
        self.assertEqual((ab.percentile(xs, 50), ab.percentile(xs, 95), ab.percentile(xs, 99)), (50, 95, 99))
        self.assertIsNone(ab.percentile([], 99))


def sample(t, busy, procs, mem=4 * GiB, root=10 * GiB):
    total = int(t * 400)
    return {"t": t, "hz": 100, "cpu_total": total, "cpu_idle": int(total * (1 - busy)), "mem_avail": mem,
            "root_free": root, "procs": procs}


class Samples(unittest.TestCase):
    TAG = "v020-perf/runs/x"

    def test_roles_foreign_load_and_busy_cpu(self):
        samples = []
        for t in range(0, 31):
            procs = {"10": {"ticks": t * 150, "start": 1, "rss": (100 + t) << 20,
                            "cmd": f"/r/arca-lake-candidate gateway --cluster-file /x/{self.TAG}/fdb.cluster"},
                     "20": {"ticks": t * 80, "start": 1, "rss": 1 << 20, "cmd": "/usr/bin/stress --cpu 1"},
                     "30": {"ticks": t * 1, "start": 1, "rss": 1 << 20, "cmd": "/usr/sbin/sshd"}}
            samples.append(sample(float(t), 0.95 if t <= 15 else 0.2, procs, mem=(2 if t != 7 else 0.5) * GiB))
        a = ab.analyze_samples(samples, self.TAG)
        self.assertAlmostEqual(a["roles"]["gateway"]["cpu_cores_mean"], 1.5, places=6)
        self.assertEqual(a["roles"]["gateway"]["rss_max"], 130 << 20)
        self.assertEqual(list(a["foreign_heavy"]), ["stress"])
        self.assertEqual(a["cpu_busy_s"], 15)
        self.assertEqual(a["mem_avail_min"], 0.5 * GiB)

    def test_a_restarted_process_is_not_a_negative_delta(self):
        p = lambda ticks, start: {"1": {"ticks": ticks, "start": start, "rss": 0,  # noqa: E731
                                        "cmd": f"x gateway {self.TAG}"}}
        a = ab.analyze_samples([sample(0, 0, p(1000, 1)), sample(1, 0, p(10, 2)), sample(2, 0, p(110, 2))], self.TAG)
        self.assertAlmostEqual(a["roles"]["gateway"]["cpu_cores_mean"], 0.5)


def valid_record(**kw):
    host = {"mem_avail_min": 2 * GiB, "root_free_min": 5 * GiB, "foreign_heavy": {}, "cpu_busy_s": 0, "roles": {}}
    r = {"run_id": "r", "arm": "candidate", "scenario": "unv", "binaries_match": True,
         "fdb_before": {"healthy": True}, "run_window": (100.0, 160.0), "load_rc": 0,
         "processes_exited": [], "hosts": {h: dict(host) for h in "abc"},
         "check": {"rc": 0, "violations": 0, "definite_failures_visible": 0}, "scrub_complete": True}
    r.update(kw)
    return r


class InvalidRuns(unittest.TestCase):
    def test_a_clean_run_is_valid(self):
        self.assertEqual(ab.invalid_reasons(valid_record()), [])
        self.assertEqual(ab.invalid_reasons(valid_record(check={"rc": 2, "violations": 3})), [],
                         "violations make a run fail, not invalid")

    def test_each_rule(self):
        cases = {
            "SHA-256": valid_record(binaries_match=False),
            "not healthy": valid_record(fdb_before={"healthy": False}),
            "fault time outside": valid_record(scenario="gwkill", faults={"times": [110.0, 170.0], "expected": 2}),
            "fault missing": valid_record(scenario="fdbstall", faults={"times": [110.0], "expected": 2}),
            "exited in a fault-free": valid_record(processes_exited=["b:gw1"]),
            "exited unexpectedly": valid_record(scenario="gwkill", faults={"times": [110.0, 130.0], "expected": 2},
                                                processes_exited_unexpected=["a:fdb"]),
            "free memory": valid_record(hosts={**valid_record()["hosts"], "a": {
                **valid_record()["hosts"]["a"], "mem_avail_min": GiB - 1}}),
            "free root": valid_record(hosts={**valid_record()["hosts"], "b": {
                **valid_record()["hosts"]["b"], "root_free_min": 3 * GiB - 1}}),
            "other workload": valid_record(hosts={**valid_record()["hosts"], "b": {
                **valid_record()["hosts"]["b"], "foreign_heavy": {"stress": 12.0}}}),
            "generator host CPU": valid_record(hosts={**valid_record()["hosts"], "c": {
                **valid_record()["hosts"]["c"], "cpu_busy_s": 11}}),
            "no samples": valid_record(hosts={**valid_record()["hosts"], "c": None}),
            "check did not complete": valid_record(check={"rc": 1, "error": "x"}),
            "scrub did not complete": valid_record(scrub_complete=False),
            "generator exited": valid_record(load_rc=1),
            "aborted": valid_record(aborted="RuntimeError('x')"),
        }
        for needle, rec in cases.items():
            why = ab.invalid_reasons(rec)
            self.assertTrue(any(needle in w for w in why), (needle, why))
        # The generator host's CPU rule is about host C only.
        busy_b = valid_record(hosts={**valid_record()["hosts"], "b": {**valid_record()["hosts"]["b"], "cpu_busy_s": 50}})
        self.assertEqual(ab.invalid_reasons(busy_b), [])
        ok_fault = valid_record(scenario="gwkill", faults={"times": [120.0, 140.0], "expected": 2},
                                processes_exited_unexpected=[])
        self.assertEqual(ab.invalid_reasons(ok_fault), [])


def runs(arm, scen, n, **metrics):
    base = {"slowdown_share": 0.55, "acked_per_s": 65.0, "attempts_per_ack": 40.0, "ack_p99_ms": 800.0,
            "unknown": 0}
    base.update(metrics)
    return [{"run_id": f"{arm}-{scen}-{i}", "arm": arm, "scenario": scen, "invalid": False, "correct": True,
             "metrics": dict(base)} for i in range(n)]


def campaign(**over):
    recs = []
    for s in ab.PERF:
        recs += runs("tag", s, 3)
        good = {"slowdown_share": 0.05, "acked_per_s": 120.0, "attempts_per_ack": 8.0, "ack_p99_ms": 900.0}
        if s == "unv16":
            good = {"slowdown_share": 0.0005, "acked_per_s": 490.0, "ack_p99_ms": 300.0}
            recs[-3:] = runs("tag", s, 3, slowdown_share=0.0005, acked_per_s=490.0, ack_p99_ms=250.0)
        good.update(over.get(s, {}))
        recs += runs("candidate", s, 3, **good)
    for s in ab.CORRECTNESS:
        recs += runs("candidate", s, 2, unknown=300)
    return recs


class Judge(unittest.TestCase):
    def test_all_targets_met_adopts(self):
        j = ab.judge(campaign())
        self.assertEqual(j["verdict"], "adopt", [t for t in j["targets"] if t["result"] != "met"])

    def test_one_target_not_met_is_not_adopted(self):
        j = ab.judge(campaign(unv={"acked_per_s": 80.0}))
        self.assertEqual(j["verdict"], "not adopted")
        failed = [t for t in j["targets"] if t["result"] == "not met"]
        self.assertEqual([(t["scenario"], t["target"][:12]) for t in failed], [("unv", "acknowledged")])
        self.assertEqual(ab.judge(campaign(unv16={"ack_p99_ms": 400.0}))["verdict"], "not adopted")
        self.assertEqual(ab.judge(campaign(ver={"unknown": 1}))["verdict"], "not adopted")

    def test_an_incorrect_run_is_not_adopted(self):
        recs = campaign()
        recs[-1]["correct"] = False
        self.assertEqual(ab.judge(recs)["verdict"], "not adopted")

    def test_too_few_valid_runs_is_inconclusive(self):
        recs = campaign()
        recs[0]["invalid"] = True
        j = ab.judge(recs)
        self.assertEqual(j["verdict"], "inconclusive")
        self.assertEqual(j["cells"]["tag/unv"]["valid_runs"], 2)
        self.assertEqual(j["cells"]["tag/unv"]["invalid_runs"], 1)
        recs = [r for r in campaign() if r["scenario"] != "gwkill"]
        self.assertEqual(ab.judge(recs)["verdict"], "inconclusive")

    def test_invalid_runs_do_not_count(self):
        recs = campaign()
        bad = runs("candidate", "unv", 1, slowdown_share=0.9, acked_per_s=1.0)[0]
        bad["invalid"], bad["correct"] = True, False
        self.assertEqual(ab.judge(recs + [bad])["verdict"], "adopt")

    def test_summary_renders(self):
        md = ab.summary_md(ab.judge(campaign()), {"campaign": "x"})
        self.assertIn("Verdict: **adopt**", md)
        self.assertIn("tag rebuild", md)


class CommandLine(unittest.TestCase):
    """The subcommands that need no host, as the driver and the hosts run
    them (`python3 -I`, no HOST_* variables)."""

    SCRIPT = str(Path(__file__).resolve().parent / "hot-key-ab.py")

    def run_script(self, *args):
        import os
        import subprocess
        import sys
        env = {k: v for k, v in os.environ.items() if not k.startswith("HOST_")}
        return subprocess.run([sys.executable, "-I", self.SCRIPT, *args], capture_output=True, text=True,
                              env=env, timeout=60, check=True).stdout

    def test_summarize_log_and_summarize_dir(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "ops.jsonl"
            log.write_text(json.dumps({"op": "put", "key": 0, "id": 1, "sent_us": 0, "done_us": 1000,
                                       "status": 200}) + "\n")
            self.assertEqual(json.loads(self.run_script("summarize-log", str(log)))["acked"], 1)
            (Path(d) / "runs").mkdir()
            for i, r in enumerate(campaign()):
                (Path(d) / "runs" / f"{i:03d}.json").write_text(json.dumps(r))
            self.assertEqual(self.run_script("summarize", d).strip(), "adopt")
            self.assertIn("Verdict: **adopt**", (Path(d) / "summary.md").read_text())

    def test_schedule_and_hosts_required(self):
        self.assertEqual(len(self.run_script("schedule").splitlines()), 28)
        import subprocess
        import sys
        p = subprocess.run([sys.executable, "-I", self.SCRIPT, "run"], capture_output=True, text=True,
                           env={"PATH": "/usr/bin:/bin"}, timeout=60)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("HOST_A, HOST_B and HOST_C are required", p.stderr)


class FakeClock:
    def __init__(self):
        self.now = 1_800_000_000.0

    def time(self):
        return self.now

    def sleep(self, s):
        self.now += max(s, 0.01)

    def strftime(self, fmt, *a):
        import time as real
        return real.strftime(fmt, *a)


class FakeHosts:
    """Answers the driver's remote commands as healthy hosts would, and
    records them (a dry run of one Run through every step)."""

    def __init__(self, clock, build):
        self.clock, self.build, self.cmds, self.signals = clock, build, [], []

    def ssh(self, h, cmd, timeout=120, check=False, stdin=None):
        self.cmds.append((h, cmd))
        t = f"{self.clock.now:.6f}"
        ok = lambda out="": (0, out, "")  # noqa: E731
        if "nohup env" in cmd:
            return ok("")
        if "hostname -I" in cmd:
            return ok({"a": "192.0.2.1", "b": "192.0.2.2", "c": "192.0.2.3"}[h] + "\n")
        if "sha256sum" in cmd:
            arm = "tag" if "arca-lake-tag" in cmd else "candidate"
            return ok((self.build["contend_sha256"] if "contend" in cmd else self.build["arms"][arm]["sha256"]) + "\n")
        if cmd.startswith("date +%s.%N") or cmd == "date +%s.%N":
            return ok(t + "\n")
        if "kill -" in cmd and "date +%s.%N" in cmd and "for p in" not in cmd:
            self.signals.append((h, cmd.split("kill -")[1].split()[0], self.clock.now))
            return ok(t + "\n")
        if "/.owner 2>/dev/null" in cmd:
            return ok(ab.MARKER + "\n")
        if cmd.startswith("test -e"):
            return 1, "", ""
        if "ss -ltnH" in cmd:
            return ok("")
        if "status json" in cmd:
            n = int(self.clock.now)
            return ok(json.dumps({"client": {"database_status": {"available": True, "healthy": True}},
                                  "cluster": {"workload": {"transactions": {"started": {"counter": n},
                                                                            "committed": {"counter": n},
                                                                            "conflicted": {"counter": n // 2}}}}}))
        if " admin " in cmd and cmd.endswith(" status"):
            return ok("group 1 tier default rs 2+1 epoch 1 Active members [1, 2, 3]\ngroup 2 tier x\n")
        if "integrity scrub" in cmd:
            return ok("verified 4 bytes 9 findings [0, 0, 0, 0, 1, 0, 0, 0] enqueued 0 errors 0\n")
        if " admin " in cmd:
            return ok("")
        if "http_code" in cmd:
            return ok("204" if "-X DELETE" in cmd else "200")
        if f"127.0.0.1:{ab.GW_METRICS}" in cmd:
            n = int(self.clock.now)
            return ok("".join(f"--- gw{i}\narca_gateway_meta_txn_attempts_total {n}\n" for i in range(4)))
        if f"127.0.0.1:{ab.NODE_METRICS}" in cmd:
            return ok("--- n0\narca_node_index_entries{kind=\"live\"} 30\narca_node_reclaim_pending_records 0\n")
        if ab.CTL_HTTP in cmd:
            return ok("arca_controller_gc_backlog 0\n")
        if "run.start 2>/dev/null" in cmd:
            self.run_start = self.clock.now
            return ok(t + "\n")
        if "test -s" in cmd:
            return ok("")
        if "run.start" in cmd and "run.end" in cmd:
            return ok(f"{self.run_start:.6f}\n{self.run_start + 61:.6f}\n0\n{{}}\n")
        if "tool.py analyze" in cmd:
            return ok(json.dumps({"roles": {}, "mem_avail_min": 4 * GiB, "root_free_min": 9 * GiB,
                                  "cpu_busy_s": 0, "foreign_heavy": {}, "span_s": 60}))
        if "summarize-log" in cmd:
            return ok(json.dumps(ab.summarize_ops([json.dumps(
                {"op": "put", "key": 0, "id": 1, "sent_us": 0, "done_us": 5, "status": 200})])))
        if "--mode check" in cmd:
            return ok(json.dumps({"mode": "check", "violations": 0, "definite_failures_visible": 0}) + "\n")
        if "pgrep" in cmd:
            return ok("")
        return ok("")


class DryRun(unittest.TestCase):
    BUILD = {"arms": {a: {"commit": f"{a}c0ffee", "sha256": f"{a}-sha"} for a in ab.ARMS},
             "contend_sha256": "contend-sha"}

    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.saved = (ab.ssh, ab.time, ab.subprocess, ab.OUT, dict(ab.HOSTS), dict(ab.ADDR))
        self.clock = FakeClock()
        self.hosts = FakeHosts(self.clock, self.BUILD)
        ab.ssh, ab.time, ab.OUT = self.hosts.ssh, self.clock, Path(self.tmp.name)

        class Sub:
            TimeoutExpired = Exception

            @staticmethod
            def run(*a, **k):
                import subprocess
                return subprocess.CompletedProcess(a, 0, b"" if not k.get("text") else "", "")
        ab.subprocess = Sub
        ab.HOSTS.update({"a": "ha", "b": "hb", "c": "hc"})
        ab.addresses()

    def tearDown(self):
        ab.ssh, ab.time, ab.subprocess, ab.OUT = self.saved[:4]
        ab.HOSTS.clear()
        ab.HOSTS.update(self.saved[4])
        ab.ADDR.clear()
        ab.ADDR.update(self.saved[5])
        self.tmp.cleanup()

    def test_a_fault_free_run_goes_through_every_step_and_is_valid(self):
        r = ab.Run("c1-01-unv-tag", "tag", "unv").execute(self.BUILD)
        self.assertNotIn("aborted", r, r.get("aborted"))
        self.assertEqual(r["invalid_reasons"], [])
        self.assertTrue(r["correct"])
        self.assertEqual(r["metrics"]["acked"], 1)
        self.assertEqual(r["metrics"]["gateway"]["arca_gateway_commit_queue_turns_total"], None)
        self.assertTrue(r["settled"])
        cmds = [c for _, c in self.hosts.cmds]
        # The tag arm's binaries only, on every host that runs one.
        self.assertTrue(any("arca-lake-tag gateway" in c for c in cmds))
        self.assertFalse(any("arca-lake-candidate gateway" in c for c in cmds))
        # Every started process has its pid file in the run directory.
        starts = [c for c in cmds if "nohup env" in c]
        self.assertEqual(len(starts), 1 + 1 + 4 + 4 + 3 + 1)  # fdb ctl nodes gws samplers load
        self.assertTrue(all("v020-perf/runs/c1-01-unv-tag" in c and ".pid" in c for c in starts))
        # The teardown signals only by pid file and command line.
        self.assertTrue(any("grep -qa v020-perf/runs/c1-01-unv-tag" in c and "kill -9" in c for c in cmds))

    def test_a_fault_run_injects_inside_the_run_and_resumes_foundationdb(self):
        r = ab.Run("c1-09-fdbstall-candidate", "candidate", "fdbstall").execute(self.BUILD)
        self.assertNotIn("aborted", r, r.get("aborted"))
        self.assertEqual([s[1] for s in self.hosts.signals], ["STOP", "CONT", "STOP", "CONT"])
        self.assertEqual(len(r["faults"]["times"]), 2)
        self.assertEqual(r["invalid_reasons"], [])
        r = ab.Run("c1-10-gwkill-candidate", "candidate", "gwkill").execute(self.BUILD)
        self.assertNotIn("aborted", r, r.get("aborted"))
        self.assertEqual(r["invalid_reasons"], [])

    def test_a_binary_mismatch_makes_the_run_invalid(self):
        b = json.loads(json.dumps(self.BUILD))
        b["arms"]["tag"]["sha256"] = "other"
        r = ab.Run("c1-01-unv-tag", "tag", "unv").execute(b)
        self.assertTrue(any("SHA-256" in w for w in r["invalid_reasons"]), r["invalid_reasons"])


def load_control():
    """A second instance of the driver with CONTROL=1 (plan §10)."""
    import os
    os.environ["CONTROL"] = "1"
    try:
        spec = importlib.util.spec_from_file_location("hot_key_ab_control",
                                                      Path(__file__).resolve().parent / "hot-key-ab.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        return m
    finally:
        del os.environ["CONTROL"]


class Control(unittest.TestCase):
    cb = load_control()

    def test_arms_scenarios_order_and_budget(self):
        cb = self.cb
        self.assertEqual((cb.ARMS, cb.PERF, cb.CORRECTNESS, cb.BUILDS), (("off", "on"), ("unv", "ver", "unv16"), (),
                                                                         ("candidate",)))
        self.assertEqual(cb.TOTAL_BUDGET_S, 9000)
        s = cb.schedule()
        self.assertEqual(len(s), 3 * 3 * 2)
        self.assertEqual([(x["scenario"], x["arm"]) for x in s[:2]], [("unv", "off"), ("unv", "on")])
        self.assertEqual([(x["scenario"], x["arm"]) for x in s[6:8]], [("ver", "on"), ("ver", "off")])
        for a, sc in cb.cells():
            self.assertEqual(sum(1 for x in s if (x["arm"], x["scenario"]) == (a, sc)), 3)

    def test_one_binary_and_the_flag(self):
        cb = self.cb
        self.assertEqual(cb.binpath("b", "off"), cb.binpath("b", "on"))
        cb.ADDR.update({"a": "192.0.2.1", "b": "192.0.2.2", "c": "192.0.2.3"})
        self.assertTrue(cb.Run("x", "off", "unv").gw_cmd(0).endswith("--commit-queue off"))
        self.assertNotIn("--commit-queue", cb.Run("x", "on", "unv").gw_cmd(0))

    def test_targets_read_as_on_against_off(self):
        cb = self.cb
        recs = []
        for s in cb.PERF:
            off = runs("off", s, 3)
            on = runs("on", s, 3, slowdown_share=0.05, acked_per_s=120.0, attempts_per_ack=8.0, ack_p99_ms=900.0)
            if s == "unv16":
                off = runs("off", s, 3, slowdown_share=0.0005, acked_per_s=490.0, ack_p99_ms=250.0)
                on = runs("on", s, 3, slowdown_share=0.0, acked_per_s=480.0, ack_p99_ms=300.0)
            recs += off + on
        j = cb.judge(recs)
        self.assertEqual(j["verdict"], "adopt", [t for t in j["targets"] if t["result"] != "met"])
        self.assertFalse([t for t in j["targets"] if t["scenario"] in ("retry", "fdbstall", "gwkill")])
        recs[0]["metrics"]["unknown"] = 1  # an off run: the control judges every run
        self.assertEqual(cb.judge(recs)["verdict"], "not adopted")


class Redaction(unittest.TestCase):
    def test_hosts_addresses_and_home_paths(self):
        # Built from parts: the source itself names no account, host or path.
        target, home, mac_home = "op" + "@" + "hostx", "/" + "home/op", "/" + "Users/op"
        t = ab.redact(f"{target} at 192.0.2.7, hostx in {home}/r110-dev and {mac_home}/x",
                      addr={"a": "192.0.2.7"}, hosts={"a": target, "b": "", "c": ""})
        self.assertEqual(t, "<host-a> at <host-a>, <host-a> in /home/<user>/r110-dev and /Users/<user>/x")


if __name__ == "__main__":
    unittest.main()
