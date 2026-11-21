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
import tempfile
import unittest
from unittest import mock

from r110 import common, driver, judge, remote

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
        self._samples = []

    def nodes(self):
        return ["node0", "node1"]

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
        return 0

    def ledger_records(self):
        return []

    def collect_logs(self):
        if self.fail_collect:
            raise OSError("collection failed")
        return ""

    def stop_load(self):
        pass


class Engine(Base):
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
        t0 = self.clock.monotonic()
        res, _ = self.go(c)
        self.assertEqual((res["stop"]["reason"], res["stop"]["stage"]), ("stage-exit", "3"))
        self.assertLessEqual(self.clock.monotonic() - t0, driver.STAGE_MAX_S["3"] + 120)

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
        c, rec, cfg = self.cluster()
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
