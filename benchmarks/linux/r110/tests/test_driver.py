"""P4: the driver's approval gate, configuration checks, stage engine,
evidence, judgement and teardown (plan §7, §8, §10, §11). Nothing here
connects to a host: ``subprocess``, ``socket`` and the cluster are fakes,
and a test fails if anything tries to reach out."""

import copy
import datetime
import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from r110 import common, driver, judge, proc, remote

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXAMPLE = os.path.join(HERE, "example-run.json")
UTC = datetime.timezone.utc
NOW = datetime.datetime(2027, 1, 1, 12, 0, tzinfo=UTC)


def forbid(*_a, **_k):
    raise AssertionError("tried to start a process or connect")


class NoContact:
    """Patches every way out of the process."""

    def __enter__(self):
        self.ps = [mock.patch.object(subprocess, n, forbid) for n in ("run", "Popen", "call", "check_output")]
        self.ps.append(mock.patch.object(socket, "create_connection", forbid))
        self.ps += [mock.patch.object(os, n, forbid) for n in ("mkdir", "makedirs", "system", "execv", "execvp",
                                                                 "fork", "posix_spawn")]
        self.ps.append(mock.patch.object(socket.socket, "connect", forbid))
        for p in self.ps:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self.ps:
            p.stop()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name
        with open(EXAMPLE) as f:
            self.cfg = json.load(f)
        self.cfg["local_dir"] = os.path.join(self.dir, "operator")

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, cfg, name="run.json"):
        p = os.path.join(self.dir, name)
        with open(p, "w") as f:
            json.dump(cfg, f)
        with open(p, "rb") as f:
            return p, hashlib.sha256(f.read()).hexdigest()

    def approval(self, sha, scope=("run",), start="2027-01-01T00:00:00+00:00", end="2027-01-02T00:00:00+00:00",
                 by="approver"):
        p = os.path.join(self.dir, "approval.json")
        with open(p, "w") as f:
            json.dump({"config_sha256": sha, "scope": list(scope), "window_start": start, "window_end": end,
                       "approved_by": by}, f)
        return p


class DryRun(Base):
    def test_check_is_the_default_and_contacts_nothing(self):
        p, _ = self.write(self.cfg)
        before = set(os.listdir(self.dir))
        with NoContact():
            self.assertEqual(driver.main([p]), 0)
            self.assertEqual(driver.main(["check", p]), 0)
        self.assertEqual(set(os.listdir(self.dir)), before, "check created files")

    def test_the_example_is_valid_and_not_approved(self):
        cfg, _ = driver.load_config(EXAMPLE)
        self.assertIn("NOT APPROVED", cfg["_note"])
        self.assertFalse(any(f.startswith("approval") for f in os.listdir(HERE)))


class Configuration(Base):
    def refused(self, change):
        cfg = copy.deepcopy(self.cfg)
        change(cfg)
        with self.assertRaises(driver.ConfigError):
            driver.validate(cfg)

    def test_nothing_looser_than_the_plan(self):
        self.refused(lambda c: c["limits"].update(rate_per_s=25))
        self.refused(lambda c: c["limits"].update(max_attempts=80_000))
        self.refused(lambda c: c["settings"].update(reclaim_retention_secs=0))
        self.refused(lambda c: c["settings"].update(segment_max_bytes=1 << 30))
        self.refused(lambda c: c["settings"].update(allotment_bytes=2 << 30))
        self.refused(lambda c: c["stage_max_s"].update({"5": 7200}))
        self.refused(lambda c: c["stop_rules"]["guard"].update(min_free_gib=1))
        self.refused(lambda c: c["stop_rules"]["guard"].update(heartbeat_timeout_s=600))
        self.refused(lambda c: c["judgement"].update(c3b_ack_fraction=0.9))
        self.refused(lambda c: c["stop_rules"].update(max_test_dir_bytes=100 << 30))

    def test_paths_binary_and_scope(self):
        self.refused(lambda c: c["remote"].update(run_dir="/mnt/data/p41-run/x"))
        self.refused(lambda c: c["remote"].update(run_dir="/tmp"))
        self.refused(lambda c: c["remote"].update(fdb_data=c["remote"]["run_dir"] + "/fdb"))
        self.refused(lambda c: c["remote"].update(run_dir="/other/mount/run"))
        self.refused(lambda c: c["binary"].update(features=["fdb", "fault-injection"]))
        self.refused(lambda c: c["teardown"].update(scope="everything"))
        self.refused(lambda c: c["remote"]["ports"].update(gateway=80))

    def test_tighter_values_are_accepted(self):
        cfg = copy.deepcopy(self.cfg)
        cfg["limits"].update(rate_per_s=10, max_attempts=1000)
        cfg["settings"]["allotment_bytes"] = 768 << 20
        cfg["stop_rules"]["max_test_dir_bytes"] = 4 << 30
        driver.validate(cfg)


class Approval(Base):
    def calls(self):
        made = []
        return made, lambda cfg: made.append(cfg) or FakeCluster(cfg)

    def invoke(self, argv):
        made, factory = self.calls()
        with NoContact():
            rc = driver.main(argv, now=NOW, cluster_factory=factory)
        return rc, made

    def test_no_contact_without_execute_and_a_matching_approval(self):
        p, sha = self.write(self.cfg)
        good = self.approval(sha, scope=("preflight",))
        self.assertEqual(self.invoke(["preflight", p, "--approval", good]), (2, []))  # no --execute
        self.assertEqual(self.invoke(["preflight", p, "--execute"]), (2, []))  # no approval
        self.assertEqual(self.invoke(["preflight", p, "--execute", "--approval", self.approval("0" * 64, ("preflight",))]),
                         (2, []))
        self.assertEqual(self.invoke(["run", p, "--execute", "--evidence", self.dir,
                                   "--approval", self.approval(sha, ("preflight",))]), (2, []))
        self.assertEqual(self.invoke(["preflight", p, "--execute",
                                   "--approval", self.approval(sha, ("preflight",), start="2027-01-02T00:00:00+00:00",
                                                               end="2027-01-03T00:00:00+00:00")]), (2, []))
        self.assertEqual(self.invoke(["preflight", p, "--execute", "--approval",
                                   self.approval(sha, ("preflight",), start="2027-01-01T00:00:00",
                                                 end="2027-01-02T00:00:00")]), (2, []))
        self.assertEqual(self.invoke(["preflight", p, "--execute", "--approval", self.approval(sha, ("preflight",), by="")]),
                         (2, []))

    def test_scope_and_window_are_strict(self):
        p, sha = self.write(self.cfg)
        bad = os.path.join(self.dir, "bad.json")
        for a in ({"scope": "preflight-only"}, {"scope": ["preflight-only"]},
                  {"window_end": "2027-01-03T00:00:01+00:00"}, {"window_end": "2026-12-31T00:00:00+00:00"}):
            base = {"config_sha256": sha, "scope": ["preflight"], "window_start": "2027-01-01T00:00:00+00:00",
                    "window_end": "2027-01-01T23:00:00+00:00", "approved_by": "approver"}
            with open(bad, "w") as f:
                json.dump(dict(base, **a), f)
            self.assertEqual(self.invoke(["preflight", p, "--execute", "--approval", bad]), (2, []), a)

    def test_teardown_needs_an_approval_too(self):
        p, sha = self.write(self.cfg)
        self.assertEqual(self.invoke(["teardown", p, "--execute"]), (2, []))
        self.assertEqual(self.invoke(["teardown", p, "--execute", "--approval", self.approval(sha, ("run",))]),
                         (2, []))

    def test_a_changed_configuration_voids_the_approval(self):
        p, sha = self.write(self.cfg)
        a = self.approval(sha, ("preflight",))
        cfg = copy.deepcopy(self.cfg)
        cfg["limits"]["rate_per_s"] = 10  # tighter, but another configuration
        self.write(cfg)
        self.assertEqual(self.invoke(["preflight", p, "--execute", "--approval", a]), (2, []))

    def test_an_approved_preflight_reaches_the_cluster(self):
        p, sha = self.write(self.cfg)
        rc, made = self.invoke(["preflight", p, "--execute", "--approval", self.approval(sha, ("preflight",))])
        self.assertEqual((rc, len(made)), (0, 1))


# -- the stage engine over a simulated cluster --------------------------------


class FakeCluster:
    """Samples synthesised from the virtual clock and the steps sent; stop
    records and failures injected by the tests."""

    def __init__(self, cfg, clock=None):
        self.cfg, self.clock = cfg, clock or common.FakeClock()
        self.stop = None
        self.stop_at_stage = None
        self.smoke_result = {"stop_layers_exercised": True, "limits_exercised": True, "errors": 0}
        self.fail_collect = False
        self.steps = []
        self.admission = 0
        self.backlog = 0
        self.pending = 0
        self.reclaimed = 0
        self.preflight_ok = True
        self.escalated = None
        self.integrity_calls = []
        self.services_stopped = False
        self._samples = []

    def nodes(self):
        return ["node0", "node1"]

    def gateways(self):
        return ["gateway0"]

    def targets(self):
        return ["gateway0", "controller"] + self.nodes()

    def required_checks(self):
        return ["space"]

    def clock_offset_ms(self):
        return 0

    def escalation(self):
        return self.escalated

    def stop_services(self):
        self.services_stopped = True
        return {"node0.json": "signalled"}

    def preflight(self):
        return {"space": {"ok": self.preflight_ok}}

    def smoke(self):
        return self.smoke_result

    def start_load(self):
        pass

    def sample(self):
        t = self.clock.wall_ms()
        mk = lambda target, vals: {"target": target, "status": "ok", "ended_wall_ms": t, "started_wall_ms": t,  # noqa: E731
                                   "values": {k: {"value": v, "unit": "count"} for k, v in vals.items()}}
        self._samples += [mk("gateway0", {"arca_gateway_admission_state": self.admission}),
                          mk("controller", {"arca_controller_gc_backlog": self.backlog,
                                            "arca_controller_backpressure_state": 0,
                                            "arca_controller_reclaimed_total": self.reclaimed})]
        self._samples += [mk(n, {"arca_node_reclaim_pending_records": self.pending}) for n in self.nodes()]

    def samples(self):
        self.sample()
        return list(self._samples)

    def run_step(self, step, deadline):
        self.steps.append(step)
        st = step.get("stage")
        if self.stop_at_stage == st:
            self.stop = {"reason": self.stop_reason, "source": "guard"}
            return None
        self.clock.advance(5)
        if step["kind"] == "put-until-refused":
            self.admission = 3
            return {"refusal_reached": True}
        if step["kind"] == "verify":
            return {"complete": True, "violations": [], "keys": 10}
        if step["kind"] == "put":
            return {"ok": True, "counts": {"op-PUT-ack": len(step["keys"])}}
        return {"ok": True}

    def send_step(self, step):
        self.steps.append(step)
        return len(self.steps)

    def step_done(self, n):
        pass

    def stop_record(self):
        return self.stop

    def write_stop(self, reason):
        if self.stop is None:
            self.stop = {"reason": reason, "source": "driver"}

    def stage4_keys(self):
        return []

    def integrity(self, kind):
        self.integrity_calls.append(kind)
        return 0

    def ledger_records(self):
        return []

    def collect_logs(self):
        if self.fail_collect:
            raise OSError("collection failed")
        return ""

    def stop_load(self):
        pass


class EngineBase(Base):
    def setUp(self):
        super().setUp()
        self.cfg = driver.validate(self.cfg)
        self.clock = common.FakeClock()

    def go(self, cluster):
        ev = driver.Evidence(os.path.join(self.dir, "evidence"), self.cfg["run_id"])
        res = driver.Run(self.cfg, cluster, ev, self.clock, poll_s=15).execute()
        return res, ev

    def outcomes(self, res):
        return {c["criterion"]: c["outcome"] for c in res["criteria"]}


class Engine(EngineBase):
    def test_a_resource_stop_in_stage_4_is_inconclusive_and_later_criteria_not_run(self):
        c = FakeCluster(self.cfg, self.clock)
        c.stop_at_stage, c.stop_reason = "4", "disk-free"
        res, ev = self.go(c)
        self.assertEqual(res["outcome"], judge.INCONCLUSIVE)
        o = self.outcomes(res)
        self.assertEqual(o["C1"], judge.INCONCLUSIVE)
        self.assertEqual({o[k] for k in ("C2", "C3a", "C3b", "C4", "C5")}, {judge.NOT_RUN})
        for f in ("timeline.json", "result.json", "samples.json"):
            self.assertTrue(os.path.exists(os.path.join(ev.dir, f)), f)
        self.assertEqual(res["stop"]["stage"], "4")

    def test_an_integrity_stop_fails_the_run_whatever_else(self):
        c = FakeCluster(self.cfg, self.clock)
        c.stop_at_stage, c.stop_reason = "3", "integrity-violation"
        res, _ = self.go(c)
        self.assertEqual(res["outcome"], judge.FAILED)

    def test_a_failed_smoke_exercise_ends_the_run_before_any_load(self):
        c = FakeCluster(self.cfg, self.clock)
        c.smoke_result = {"stop_layers_exercised": False, "limits_exercised": True, "errors": 0}
        res, _ = self.go(c)
        self.assertEqual(c.steps, [])
        self.assertEqual(res["stop"]["reason"], "stage-exit")
        self.assertEqual(res["outcome"], judge.NOT_RUN)

    def test_a_failed_preflight_runs_nothing(self):
        c = FakeCluster(self.cfg, self.clock)
        c.preflight_ok = False
        res, _ = self.go(c)
        self.assertEqual((c.steps, res["stop"]["stage"]), ([], "1"))

    def test_a_stage_that_never_meets_its_exit_ends_at_its_maximum(self):
        c = FakeCluster(self.cfg, self.clock)
        c.backlog = 5  # stage 3 waits for 0
        res, ev = self.go(c)
        self.assertEqual((res["stop"]["reason"], res["stop"]["stage"]), ("stage-exit", "3"))
        tl = common.read_json(os.path.join(ev.dir, "timeline.json"))
        self.assertLessEqual(tl["3"]["end_ms"] - tl["3"]["start_ms"], (driver.STAGE_MAX_S["3"] + 30) * 1000)
        # A stop that is not a resource danger: the drain is observed.
        self.assertEqual(tl["drain"]["end_ms"] - tl["drain"]["start_ms"], driver.DRAIN_OBSERVE_S * 1000)
        self.assertFalse(c.services_stopped)

    def test_a_lost_part_of_the_evidence_never_passes(self):
        c = FakeCluster(self.cfg, self.clock)
        c.fail_collect = True
        c.stop_at_stage, c.stop_reason = "4", "memory"
        res, ev = self.go(c)
        self.assertIn("logs: collection failed", res["evidence_failures"])
        self.assertNotEqual(res["outcome"], judge.PASSED)
        self.assertTrue(os.path.exists(os.path.join(ev.dir, "result.json")))

    def test_a_pass_with_lost_evidence_is_inconclusive(self):
        """The judgement alone would pass (forced): losing part of the
        evidence still makes the run Inconclusive."""
        c = FakeCluster(self.cfg, self.clock)
        c.fail_collect = True
        with mock.patch.object(judge, "judge", lambda *a, **k: {"outcome": judge.PASSED, "criteria": []}):
            res, _ = self.go(c)
        self.assertEqual(res["outcome"], judge.INCONCLUSIVE)

    def test_evidence_is_never_overwritten(self):
        driver.Evidence(os.path.join(self.dir, "evidence"), self.cfg["run_id"])
        with self.assertRaises(FileExistsError):
            driver.Evidence(os.path.join(self.dir, "evidence"), self.cfg["run_id"])


# -- judgement -----------------------------------------------------------------


def smp(target, t, labelled=None, **vals):
    vals.update(labelled or {})
    return {"target": target, "status": "ok", "started_wall_ms": t, "ended_wall_ms": t,
            "values": {k: {"value": v, "unit": "count"} for k, v in vals.items()}}


class C1(unittest.TestCase):
    """The refusal from t=0 to t=30 min, samples every 15 s."""

    def build(self, admission=3, refused_rises=False, backlog0=50, gap=False, reclaimed_end=3000):
        S, iv = [], 15_000
        end = 30 * 60_000
        for i, t in enumerate(range(0, end + 1, iv)):
            if gap and 10 * 60_000 < t < 12 * 60_000:
                continue
            frac = t / end
            S.append(smp("gateway0", t, arca_gateway_admission_state=admission if t < end - 60_000 else 3))
            S.append(smp("controller", t, arca_controller_gc_collected_total=int(1000 * frac),
                         arca_controller_gc_backlog=max(0, int(backlog0 * (1 - 2 * frac))),
                         arca_controller_reclaimed_total=int(reclaimed_end * frac),
                         arca_controller_reclaim_refused_total=(1 if refused_rises and t > 600_000 else 0),
                         arca_controller_reclaim_errors_total=0))
            for n in ("node0", "node1"):
                S.append(smp(n, t, arca_node_reclaim_pending_records=0 if frac > 0.9 else 100))
        records = []
        for k in range(1000):
            records += [{"type": "intent", "op_id": k, "attempt": 1, "key": f"b{k}", "op": "DELETE", "stage": "4",
                         "wall_ms": 0},
                        {"type": "outcome", "op_id": k, "attempt": 1, "result": "ack"}]
        tl = {"4": {"start_ms": -60_000, "refusal_start_ms": 0, "refusal_end_ms": end, "reclaimed_before_refusal": 0}}
        return S, records, tl

    def c1(self, **kw):
        S, R, tl = self.build(**kw)
        return judge.c1(S, R, tl, judge.VALUES, ["node0", "node1"])

    def test_pass(self):
        r = self.c1()
        self.assertEqual(r["outcome"], judge.PASSED, r)

    def test_reclaim_refusals_rising_fail(self):
        self.assertEqual(self.c1(refused_rises=True)["outcome"], judge.FAILED)

    def test_too_few_reclaims_fail(self):
        self.assertEqual(self.c1(reclaimed_end=2000)["outcome"], judge.FAILED)

    def test_a_gap_in_the_samples_is_inconclusive(self):
        self.assertEqual(self.c1(gap=True)["outcome"], judge.INCONCLUSIVE)

    def test_admission_recovering_early_is_inconclusive(self):
        self.assertEqual(self.c1(admission=2)["outcome"], judge.INCONCLUSIVE)

    def test_nothing_left_to_collect_at_the_refusal_is_inconclusive(self):
        self.assertEqual(self.c1(backlog0=0)["outcome"], judge.INCONCLUSIVE)

    def test_unknown_reclaims_before_the_refusal_are_inconclusive(self):
        S, R, tl = self.build()
        tl["4"]["reclaimed_before_refusal"] = None
        self.assertEqual(judge.c1(S, R, tl, judge.VALUES, ["node0", "node1"])["outcome"], judge.INCONCLUSIVE)


class C2(unittest.TestCase):
    def build(self, drop_node_baseline=False, du_off=False, gain=1.0, d1_pending=False, gc_backlog=0,
              probes=0):
        v = judge.VALUES
        S, iv = [], 15_000
        start, judge_t, end = 0, 30 * 60_000, 45 * 60_000
        per_node_freed = 1000 * v["shard_bytes_per_object"] // 2  # 1000 deletes over 2 nodes
        for t in range(start, end + 1, iv):
            done = t >= judge_t
            for n in ("node0", "node1"):
                if drop_node_baseline and n == "node1" and t == start:
                    continue
                live = 400_000_000 - (per_node_freed if done else 0)
                avail = 300_000_000 + (int(per_node_freed * gain) if done else 0)
                seg = live + 10_000_000
                S.append(smp(n, t, {
                    'arca_node_capacity_bytes{kind="live"}': live,
                    'arca_node_capacity_bytes{kind="available"}': avail,
                    'arca_node_capacity_bytes{kind="allocated"}': seg,
                    'arca_node_capacity_bytes{kind="reclaimable"}': 10_000_000,
                    'arca_node_device_usage_bytes{kind="segments"}': seg,
                    'arca_node_device_usage_bytes{kind="side_files"}': 1_000_000,
                    "arca_node_reclaim_pending_records": 5 if d1_pending and t == start else 0}))
                S.append(smp(f"{n}-dir", t, allocated_bytes=(seg + 1_000_000) * (1.2 if du_off else 1.0)))
            S.append(smp("controller", t, arca_controller_gc_backlog=gc_backlog if t == start else 0))
            S.append(smp("controller-log", t, reconcile_deleted=0))
        R = []
        for k in range(probes):
            R += [{"type": "intent", "op_id": 5000 + k, "attempt": 1, "key": f"q{k}", "op": "PUT", "stage": "5",
                   "kind": "probe", "wall_ms": 60_000 + k * 10_000},
                  {"type": "outcome", "op_id": 5000 + k, "attempt": 1, "result": "ack"}]
        for k in range(1000):
            R += [{"type": "intent", "op_id": k, "attempt": 1, "key": f"x{k}", "op": "DELETE", "stage": "5", "wall_ms": 1},
                  {"type": "outcome", "op_id": k, "attempt": 1, "result": "ack"}]
        tl = {"5": {"start_ms": start, "c2_judge_ms": judge_t, "end_ms": end}}
        return S, R, tl

    def c2(self, **kw):
        S, R, tl = self.build(**kw)
        return judge.c2(S, R, tl, judge.VALUES, ["node0", "node1"])

    def test_pass(self):
        r = self.c2()
        self.assertEqual(r["outcome"], judge.PASSED, r)

    def test_a_missing_baseline_is_inconclusive(self):
        self.assertEqual(self.c2(drop_node_baseline=True)["outcome"], judge.INCONCLUSIVE)

    def test_du_far_from_the_segments_fails(self):
        self.assertEqual(self.c2(du_off=True)["outcome"], judge.FAILED)

    def test_d1_not_finished_at_the_baseline_is_inconclusive(self):
        self.assertEqual(self.c2(d1_pending=True)["outcome"], judge.INCONCLUSIVE)
        self.assertEqual(self.c2(gc_backlog=3)["outcome"], judge.INCONCLUSIVE)

    def test_probe_puts_raise_the_bar_for_available(self):
        """Available up by exactly half the decrease passes without probes
        and fails once the probes' bound is added."""
        self.assertEqual(self.c2(gain=0.5)["outcome"], judge.PASSED)
        r = self.c2(gain=0.5, probes=200)
        self.assertEqual(r["outcome"], judge.FAILED, r)

    def test_available_not_rising_fails(self):
        self.assertEqual(self.c2(gain=0.2)["outcome"], judge.FAILED)


class Overall(unittest.TestCase):
    def test_a_c5_violation_fails_despite_a_resource_stop(self):
        tl = {"7": {"verify": {"violations": [{"key": "k"}], "complete": True, "keys": 1},
                    "manifest_findings": 0, "scrub_findings": 0}}
        r = judge.judge([], [], tl, {"reason": "memory", "stage": "7"})
        self.assertEqual(r["outcome"], judge.FAILED)

    def test_a_lost_ledger_or_monitor_is_inconclusive_not_failed(self):
        for reason in ("ledger-failure", "heartbeat-lost", "some-new-reason"):
            r = judge.judge([], [], {"4": {"start_ms": 0}}, {"reason": reason, "stage": "4"})
            self.assertEqual(r["outcome"], judge.INCONCLUSIVE, reason)

    def test_a_forbidden_counter_fails(self):
        S = [smp("controller", 1, arca_controller_gc_skipped_referenced_total=1)]
        self.assertEqual(judge.judge(S, [], {})["outcome"], judge.FAILED)

    def test_nothing_started_is_not_run(self):
        r = judge.judge([], [], {})
        self.assertEqual({c["outcome"] for c in r["criteria"]} - {judge.INCONCLUSIVE}, {judge.NOT_RUN})
        self.assertNotEqual(r["outcome"], judge.PASSED)

    def test_c5_incomplete_is_not_a_pass(self):
        tl = {"7": {"verify": {"violations": [], "complete": False, "keys": 1}, "manifest_findings": 0,
                    "scrub_findings": None}}
        self.assertEqual(judge.c5(tl)["outcome"], judge.INCONCLUSIVE)


class ReviewFindingsJudge(unittest.TestCase):
    """Cases from the independent review of P4 (judgement)."""

    def all_pass(self):
        ok = lambda name: lambda *a, **k: judge.verdict(name, judge.PASSED)  # noqa: E731
        return [mock.patch.object(judge, n, ok(n.upper() if n != "c3a" and n != "c3b" else n.capitalize()))
                for n in ("c1", "c2", "c3a", "c3b", "c4", "c5")]

    def test_every_criterion_met_with_a_stop_is_not_a_pass(self):
        ps = self.all_pass()
        for p in ps:
            p.start()
        try:
            self.assertEqual(judge.judge([], [], {})["outcome"], judge.PASSED)
            for reason in ("disk-free", "heartbeat-lost", "smoke-exercise", "operator", "stage-exit"):
                r = judge.judge([], [], {}, {"reason": reason, "stage": "7"})
                self.assertEqual(r["outcome"], judge.INCONCLUSIVE, reason)
        finally:
            for p in ps:
                p.stop()

    def test_protection_counters_not_sampled_over_the_run_cannot_pass(self):
        ps = self.all_pass()
        for p in ps:
            p.start()
        try:
            tl = {"3": {"start_ms": 0, "end_ms": 600_000}}
            S = [smp("controller", t, arca_controller_gc_skipped_referenced_total=0) for t in range(0, 600_001, 15_000)]
            S += [smp(n, t, arca_node_reclaim_damaged=0) for n in ("node0",) for t in range(0, 600_001, 15_000)]
            self.assertEqual(judge.judge(S, [], tl, nodes=("node0",))["outcome"], judge.PASSED)
            gap = [x for x in S if not (x["target"] == "node0" and 100_000 < x["ended_wall_ms"] < 200_000)]
            r = judge.judge(gap, [], tl, nodes=("node0",))
            self.assertEqual(r["outcome"], judge.INCONCLUSIVE)
            self.assertEqual(r["protections_not_sampled"], ["node0 arca_node_reclaim_damaged"])
        finally:
            for p in ps:
                p.stop()

    def c3a(self, probes=60, unknown=0, partial=False):
        t0, hold = 1_000_000, 600_000
        S = []
        for t in range(t0, t0 + hold + 1, 15_000):
            S.append(smp("gateway0", t, arca_gateway_admission_state=0))
            S.append(dict(smp("controller", t, arca_controller_backpressure_state=0),
                          status="partial" if partial else "ok"))
        R = []
        for k in range(probes):
            R.append({"type": "intent", "op_id": k, "attempt": 1, "key": f"q{k}", "op": "PUT", "stage": "5",
                      "kind": "probe", "wall_ms": t0 + 5_000 + k * 9_900})
            if k >= unknown:
                R.append({"type": "outcome", "op_id": k, "attempt": 1, "result": "ack"})
        tl = {"5": {"start_ms": 0, "c3a_window_start_ms": t0}}
        return judge.c3a(S, R, tl, judge.VALUES)["outcome"]

    def test_c3a_needs_the_plans_sixty_probes(self):
        self.assertEqual(self.c3a(), judge.PASSED)
        self.assertEqual(self.c3a(partial=True), judge.PASSED)  # values read in a partial scrape count
        self.assertEqual(self.c3a(probes=1), judge.INCONCLUSIVE)
        self.assertEqual(self.c3a(probes=59), judge.INCONCLUSIVE)

    def test_a_probe_without_an_outcome_is_not_acknowledged(self):
        self.assertEqual(self.c3a(unknown=1), judge.FAILED)


class ReviewFindingsEngine(EngineBase):
    """Cases from the independent review of P4 (stage engine)."""

    def test_a_stop_during_stage_7_is_recorded(self):
        c = FakeCluster(self.cfg, self.clock)
        c.stop_at_stage, c.stop_reason = "7", "memory"
        engine = driver.Run(self.cfg, c, driver.Evidence(os.path.join(self.dir, "e"), self.cfg["run_id"]), self.clock)
        # Straight to stage 7 (the earlier stages' conditions are not modelled).
        engine.stage(driver.stages(self.cfg)[6], engine.s7)
        engine.finish()
        self.assertEqual((engine.result["stop"]["reason"], engine.result["stop"]["stage"]), ("memory", "7"))
        self.assertNotEqual(engine.result["outcome"], judge.PASSED)
        self.assertEqual(c.integrity_calls, [], "a scrub started after the stop")

    def test_a_stop_seen_only_at_the_end_is_recorded(self):
        c = FakeCluster(self.cfg, self.clock)
        engine = driver.Run(self.cfg, c, driver.Evidence(os.path.join(self.dir, "e"), self.cfg["run_id"]), self.clock)
        engine.stage(driver.stages(self.cfg)[6], engine.s7)
        self.assertIsNone(engine.stop)
        c.stop = {"reason": "heartbeat-lost", "source": "guard"}
        engine.finish()
        self.assertEqual((engine.result["stop"]["reason"], engine.result["stop"]["stage"]), ("heartbeat-lost", "7"))

    def test_an_exception_in_a_stage_is_a_stop(self):
        c = FakeCluster(self.cfg, self.clock)

        def boom():
            raise RuntimeError("ssh failed")

        c.smoke = boom
        res, _ = self.go(c)
        self.assertEqual((res["stop"]["reason"], res["stop"]["stage"]), ("driver-error", "2"))
        self.assertIn("ssh failed", res["stop"]["error"])
        self.assertNotEqual(res["outcome"], judge.PASSED)

    def test_a_stale_value_is_not_used(self):
        engine = driver.Run(self.cfg, FakeCluster(self.cfg, self.clock),
                            driver.Evidence(os.path.join(self.dir, "e"), self.cfg["run_id"]), self.clock)
        S = [smp("controller", 0, arca_controller_gc_backlog=0), smp("gateway0", 60_000, arca_gateway_admission_state=0)]
        self.assertIsNone(engine._fresh(S, "controller", "arca_controller_gc_backlog"))
        S.append(smp("controller", 50_000, arca_controller_gc_backlog=0))
        self.assertEqual(engine._fresh(S, "controller", "arca_controller_gc_backlog"), 0)

    def test_the_refusal_is_held_for_d1s_chain(self):
        c = FakeCluster(self.cfg, self.clock)
        res, ev = self.go(c)  # admission stays write rejected: stage 5 never recovers
        tl = common.read_json(os.path.join(ev.dir, "timeline.json"))
        self.assertGreaterEqual(tl["4"]["refusal_end_ms"] - tl["4"]["d1_done_ms"], driver.CHAIN_S * 1000)
        self.assertFalse(tl["4"]["admission_recovered_early"])
        self.assertEqual(res["stop"]["stage"], "5")

    def test_a_resource_rule_during_the_drain_stops_the_services(self):
        c = FakeCluster(self.cfg, self.clock)
        c.stop_at_stage, c.stop_reason = "4", "operator"
        c.escalated = {"reason": "disk-free", "source": "watchdog"}
        res, ev = self.go(c)
        self.assertTrue(c.services_stopped)
        tl = common.read_json(os.path.join(ev.dir, "timeline.json"))
        self.assertEqual(tl["drain"]["escalation"]["reason"], "disk-free")

    def test_no_drain_after_a_resource_stop(self):
        c = FakeCluster(self.cfg, self.clock)
        c.stop_at_stage, c.stop_reason = "4", "disk-free"
        res, ev = self.go(c)
        self.assertNotIn("drain", common.read_json(os.path.join(ev.dir, "timeline.json")))


# -- the hosts' commands, recorded (never run) ----------------------------------


class Recorder:
    def __init__(self, answers=None):
        self.commands = []
        self.answers = answers or []

    def __call__(self, target, command, stdin=None, timeout=None):
        self.commands.append((target, command, stdin))
        for pat, out in self.answers:
            if pat in command or (stdin and pat in stdin):
                return 0, out, ""
        return 0, "", ""


class Commands(Base):
    def cluster(self, answers=None):
        cfg = driver.validate(copy.deepcopy(self.cfg))
        rec = Recorder(answers)
        return remote.SshCluster(cfg, runner=rec), rec, cfg

    def test_teardown_removes_only_marked_run_directories(self):
        c, rec, cfg = self.cluster(answers=[('print("checked %d"', "checked 0\n")])
        out = c.teardown()
        rms = [cmd for _, cmd, _ in rec.commands if "rm " in cmd]
        self.assertEqual(len(rms), 2)
        for cmd, d in zip(rms, (cfg["remote"]["run_dir"], cfg["remote"]["fdb_data"])):
            self.assertIn(f"rm -rf -- {d}", cmd)
            self.assertIn(f"cat {d}/.r110-run", cmd)
            self.assertIn(cfg["run_id"], cmd)
        self.assertNotIn("kept", out)

    def test_teardown_keeps_everything_while_a_test_process_runs(self):
        c, rec, _ = self.cluster(answers=[("proc.matches(json.load", "node0.json\n")])
        out = c.teardown()
        self.assertIn("still running", out["kept"])
        self.assertFalse(any("rm " in cmd for _, cmd, _ in rec.commands))

    def test_no_command_kills_by_name_or_touches_the_system_foundationdb(self):
        c, rec, cfg = self.cluster()
        c.setup = lambda: None
        c.preflight()
        c.teardown()
        c.write_stop("operator")
        for _, cmd, stdin in rec.commands:
            for word in ("pkill", "killall", "kill -9", "systemctl", "reboot", "p41"):
                self.assertNotIn(word, cmd)
            if "/etc/foundationdb/fdb.cluster" in cmd:
                self.assertIn("status minimal", cmd)

    def test_a_smoke_sub_run_cannot_stop_the_main_runs_services(self):
        c, rec, cfg = self.cluster()
        c.start_load("smoke-hb", heartbeat=False)
        main = c.start_load()
        cmds = [cmd for _, cmd, s in rec.commands if s == remote.START]
        self.assertEqual(len(cmds), 2)
        self.assertNotIn("--services", cmds[0])
        self.assertIn("--services", cmds[1])
        self.assertEqual(main, cfg["remote"]["run_dir"] + "/load")
        # The main run's stop record is read from the main load directory only.
        rec.commands.clear()
        c.stop_record()
        self.assertTrue(all("/smoke" not in cmd for _, cmd, _ in rec.commands))

    def test_teardown_keeps_everything_when_the_check_fails(self):
        for answer in ([], [('print("checked %d"', "")], [('print("checked %d"', "unreadable node0.json: x\n")]):
            c, rec, _ = self.cluster(answers=answer)
            if answer == []:
                c.runner = lambda *a: (1, "", "Traceback")
            out = c.teardown()
            self.assertIn("kept", out, answer)
            self.assertFalse(any("rm " in cmd for _, cmd, _ in rec.commands), answer)

    def test_the_liveness_script_itself(self):
        """remote.ALIVE run locally against identity records."""
        svc = os.path.join(self.dir, "services")
        os.mkdir(svc)

        def check():
            r = subprocess.run([sys.executable, "-I", "-", HERE.rsplit("/", 1)[0], svc], input=remote.ALIVE,
                         capture_output=True, text=True, timeout=60)
            return r.returncode, r.stdout.strip()

        self.assertEqual(check(), (0, "checked 0"))
        me = proc.identity(os.getpid())
        with open(os.path.join(svc, "a.json"), "w") as f:
            json.dump(dict(me, start="not-its-start"), f)
        self.assertEqual(check(), (0, "checked 1"))
        with open(os.path.join(svc, "b.json"), "w") as f:
            json.dump(me, f)
        rc, out = check()
        self.assertEqual((rc, out.splitlines()[0]), (1, "alive b.json"))
        os.remove(os.path.join(svc, "b.json"))
        with open(os.path.join(svc, "c.json"), "w") as f:
            f.write("null")
        rc, out = check()
        self.assertEqual(rc, 1)
        self.assertTrue(out.startswith("unreadable c.json"), out)

    def test_a_corrupt_or_missing_ledger_is_an_error_not_empty(self):
        from r110 import ledger as ledger_mod
        c, rec, _ = self.cluster(answers=[("ledger.jsonl", 'deadbeef {"type":"intent"}\n')])
        with self.assertRaises(ledger_mod.LedgerFailure):
            c.ledger_records()
        c, rec, _ = self.cluster()
        c.runner = lambda *a: (1, "", "No such file")
        with self.assertRaises(remote.RemoteError):
            c.ledger_records()

    def test_samples_are_read_incrementally(self):
        line = json.dumps(smp("controller", 1, x=1)) + "\n"
        c, rec, _ = self.cluster(answers=[("samples.jsonl", line + line[:10])])
        self.assertEqual(len(c.samples()), 1)
        self.assertIn("tail -c +1 ", rec.commands[-1][1])
        c.samples()
        self.assertIn(f"tail -c +{len(line) + 1} ", rec.commands[-1][1])

    def test_the_main_load_leaves_the_smoke_stages_budget(self):
        lim = remote.main_limits({})
        used = sum(v["max_attempts"] for v in remote.SMOKE_LIMITS.values())
        self.assertEqual(lim["max_attempts"] + used, 70_000)
        self.assertLessEqual(lim["max_body_bytes"] + sum(v["max_body_bytes"] for v in remote.SMOKE_LIMITS.values()),
                             5 << 30)

    def test_setup_refuses_existing_directories(self):
        c, rec, cfg = self.cluster()
        c.start_watchdog = lambda: None
        c.cluster_services = lambda: None
        c.setup()
        first = rec.commands[0][1]
        self.assertTrue(first.startswith(f"[ ! -e {cfg['remote']['run_dir']} ] && [ ! -e {cfg['remote']['fdb_data']} ]"))
        self.assertIn("exit 1", first)


if __name__ == "__main__":
    unittest.main()
