"""P4: the driver's approval gate, configuration checks, stage engine,
evidence, judgement and teardown (plan §7, §8, §10, §11). Nothing here
connects to a host: ``subprocess``, ``socket`` and the cluster are fakes,
and a test fails if anything tries to reach out."""

import contextlib
import copy
import datetime
import shlex
import hashlib
import io
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
        # A test value (the example leaves it undetermined, as a real run's
        # is until its preflight): never an approved one.
        self.cfg["fdb_memory"] = "3GiB"

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, cfg, name="run.json"):
        p = os.path.join(self.dir, name)
        with open(p, "w") as f:
            json.dump(cfg, f)
        with open(p, "rb") as f:
            return p, hashlib.sha256(f.read()).hexdigest()

    def approval(self, sha, scope=("run",), start="2027-01-01T00:00:00+00:00", end="2027-01-02T00:00:00+00:00",
                 by="approver", **extra):
        p = os.path.join(self.dir, "approval.json")
        # The 0.2.0 campaign's fields for main run 1 (the example's).
        a = {"config_sha256": sha, "scope": list(scope), "window_start": start, "window_end": end,
             "approved_by": by, "campaign": "0.2.0", "campaign_run": 1, "candidate": "interim"}
        a.update(extra)
        with open(p, "w") as f:
            json.dump({k: v for k, v in a.items() if v is not None}, f)
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

    def test_the_system_foundationdb_hosts_are_required(self):
        self.refused(lambda c: c.pop("system_fdb_hosts"))
        self.refused(lambda c: c.update(system_fdb_hosts=[]))
        self.refused(lambda c: c.update(system_fdb_hosts=["nohost"]))

    def test_optional_monitoring_is_only_what_the_configuration_declares(self):
        self.refused(lambda c: c.update(optional_checks=["test_fdb"]))
        self.refused(lambda c: c.update(optional_checks="kernel_log"))
        cfg = copy.deepcopy(self.cfg)
        cfg["optional_checks"] = ["kernel_log", "swap"]
        driver.validate(cfg)

    def test_values_that_are_not_finite_are_refused(self):
        nan = float("nan")
        self.refused(lambda c: c["limits"].update(rate_per_s=nan))
        self.refused(lambda c: c["stage_max_s"].update({"4": nan}))
        self.refused(lambda c: c["stop_rules"]["guard"].update(min_free_gib=nan))
        self.refused(lambda c: c["stop_rules"]["guard"].update(heartbeat_timeout_s=nan))
        self.refused(lambda c: c.pop("retained_record"))
        self.refused(lambda c: c.update(retained_record={"/x": "not-a-digest"}))

    def test_tighter_values_are_accepted(self):
        cfg = copy.deepcopy(self.cfg)
        cfg["limits"].update(rate_per_s=10, max_attempts=5000)
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

    def test_a_run_must_end_inside_the_window(self):
        p, sha = self.write(self.cfg)
        # Starts inside, would end 8 h later: past a window ending in 2 h.
        short = self.approval(sha, ("run",), start="2027-01-01T11:00:00+00:00", end="2027-01-01T14:00:00+00:00")
        self.assertEqual(self.invoke(["run", p, "--execute", "--evidence", self.dir, "--approval", short]), (2, []))

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
        self.incomplete = False
        self.steps = []
        self.admission = 0
        self.backlog = 0
        self.pending = 0
        self.reclaimed = 0
        self.preflight_ok = True
        self.escalated = None
        self.integrity_calls = []
        self.step_waited = False
        self.drop_gateway = False
        self.recover_after = None
        self.polls = 0
        self.services_stopped = False
        self.stop_calls = []
        self.started = ["fdb", "node0", "node1", "controller", "gateway0", "load"]
        self.verdicts = {}
        self.listing_error = None
        self.setup_started = True
        self._samples = []
        # A 0.2.0 binary's (ADR-0037): strict nodes, no unproven write.
        self.strict = 1
        self.unproven = 0

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

    def stop_services(self, names=None):
        """The real result's shape (remote.SshCluster.stop_services)."""
        self.services_stopped = True
        self.stop_calls.append(("stop_services", names))
        if self.listing_error:
            return {"listing": {"error": self.listing_error}, "verdicts": {}}
        names = names or {n + ".json" for n in self.started}
        verdicts = {n: dict(self.verdicts.get(n, {"verdict": "stopped", "confirmed": True}))
                    for n in sorted(names) if n[:-5] in self.started}
        return {"listing": {"exists": True, "started": list(self.started),
                            "records": [n + ".json" for n in self.started]}, "verdicts": verdicts}

    def stop_sentinel(self):
        self.stop_calls.append(("stop_sentinel", None))
        return {"verdict": "stopped", "confirmed": True}

    def stop_sampler(self):
        self.stop_calls.append(("stop_sampler", None))
        return dict(self.verdicts.get("sampler.json", {"verdict": "stopped", "confirmed": True}))

    def stop_watchdog(self):
        self.stop_calls.append(("stop_watchdog", None))
        return {"verdict": "stopped", "returncode": 0}

    def preflight(self):
        return {"space": {"ok": self.preflight_ok}}

    def smoke(self, **kw):
        return self.smoke_result

    def start_load(self, **kw):
        pass

    def sample(self):
        t = self.clock.wall_ms()
        mk = lambda target, vals: {"target": target, "status": "ok", "ended_wall_ms": t, "started_wall_ms": t,  # noqa: E731
                                   "values": {k: {"value": v, "unit": "count"} for k, v in vals.items()}}
        self.polls += 1
        if self.recover_after is not None and self.admission == 3:
            self.recover_after -= 1
            if self.recover_after < 0:
                self.admission = 0
        if not self.drop_gateway:
            self._samples.append(mk("gateway0", {"arca_gateway_admission_state": self.admission}))
        self._samples += [mk("controller", {"arca_controller_gc_backlog": self.backlog,
                                            "arca_controller_backpressure_state": 0,
                                            "arca_controller_reclaimed_total": self.reclaimed,
                                            "arca_controller_nodes_write_ownership_lenient": 0 if self.strict else 1})]
        self._samples += [mk(n, {"arca_node_reclaim_pending_records": self.pending,
                                 "arca_node_write_ownership_strict": self.strict,
                                 "arca_node_writes_unproven_total": self.unproven}) for n in self.nodes()]

    def samples(self):
        self.sample()
        return list(self._samples)

    def run_step(self, step, deadline, check=None):
        self.steps.append(step)
        if check is not None and check():
            return None
        st = step.get("stage")
        if self.stop_at_stage == st:
            # As the real records (watchdog, guard, write_stop): with their time.
            self.stop = {"reason": self.stop_reason, "source": "guard", "wall_ms": self.clock.wall_ms()}
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

    def wait_step(self, n, deadline):
        self.step_waited = True
        return {"attempts": 1}

    def stop_record(self):
        return self.stop

    def write_stop(self, reason):
        if self.stop is None:
            self.stop = {"reason": reason, "source": "driver", "wall_ms": self.clock.wall_ms()}

    def stage4_keys(self):
        return []

    def integrity(self, kind):
        self.integrity_calls.append(kind)
        if kind == "scrub":
            # A 0.2.0 scrub's output: classes and the shards held.
            self.last_scrub_classes = [0] * 8
            self.last_scrub_held = {"retained": 0, "retained_damaged": 0, "in_flight": 0, "unclassified": 0}
            self.last_scrub_errors, self.last_scrub_unread = 0, []
        return 0

    def ledger_records(self):
        return []

    def collect_evidence(self, dest, meta=None, **kw):
        """The real manifest's shape (evidence.collect)."""
        self.stop_calls.append(("collect_evidence", None))
        self.collect_kw = kw
        if self.fail_collect:
            raise OSError("collection failed")
        return {"complete": not self.incomplete, "attempt": 1, "files": [],
                "incomplete": ["run/logs/node0.log: capped"] if self.incomplete else []}

    def stop_load(self):
        self.stop_calls.append(("stop_load", None))
        return {"guard": {"verdict": "stopped"}, "load": {"verdict": "gone"},
                "guard_result": {"reason": "finish", "group_empty": True, "load_reaped": True}}


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
        self.assertEqual(o["C1a"], judge.INCONCLUSIVE)
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
        self.assertNotIn("escalation", tl["drain"], "the drain stopped the services without a rule firing")
        # At the end of the run the services are stopped all the same, the
        # watching being about to end.
        self.assertIn("stop_services", tl["final_stops"])

    def test_a_lost_part_of_the_evidence_never_passes(self):
        c = FakeCluster(self.cfg, self.clock)
        c.fail_collect = True
        c.stop_at_stage, c.stop_reason = "4", "memory"
        res, ev = self.go(c)
        self.assertIn("host_evidence: OSError: collection failed", res["evidence_failures"])
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
    """Plan v2 §8.3. D1's 1,000 deletes end at t = -1 min; the refusal from
    0 to ``refusal_min``; D1's chain over at ``quiet_min``; samples every
    15 s to ``end_min``."""

    def build(self, refusal_min=20, quiet_min=25, end_min=30, reclaimed=3000, refused_rises=False,
              gap=False, refusals=0, reconcile=0, reset=False, no_log=False, lag_s=0, no_errors=False):
        S, iv = [], 15_000
        t_d, t_q, t_e = -60_000, quiet_min * 60_000, end_min * 60_000
        for t in range(t_d, t_e + 1, iv):
            if gap and 3 * 60_000 < t < 5 * 60_000:
                continue
            done = min(1.0, max(0.0, (t - t_d) / (t_q - t_d)))
            # 300: earlier records, reclaimed before D1; lag_s: the
            # controller counts its last pass after the nodes read 0.
            rec = int(300 + reclaimed * min(1.0, max(0.0, (t - t_d - lag_s * 1000) / (t_q - t_d))))
            if reset and t > 8 * 60_000:
                rec -= 300
            S.append(smp("gateway0", t, arca_gateway_admission_state=3 if 0 <= t < refusal_min * 60_000 else 0,
                         arca_gateway_node_capacity_refusals_total=int(refusals * done)))
            S.append(smp("controller", t, arca_controller_gc_collected_total=int(1000 * min(1.0, done * 2)),
                         arca_controller_gc_backlog=0 if t >= t_q else max(1, int(1000 * (1 - done * 2))),
                         arca_controller_reclaimed_total=rec,
                         arca_controller_reclaim_refused_total=(1 if refused_rises and t > 6 * 60_000 else 0),
                         **({} if no_errors else {"arca_controller_reclaim_errors_total": 0})))
            if not no_log:
                S.append(smp("controller-log", t, reconcile_deleted=int(reconcile * done)))
            for n in ("node0", "node1"):
                S.append(smp(n, t, arca_node_reclaim_pending_records=0 if t >= t_q else 100))
        records = []
        for k in range(1000):
            records += [{"type": "intent", "op_id": k, "attempt": 1, "key": f"b{k}", "op": "DELETE", "stage": "4",
                         "wall_ms": -120_000},
                        {"type": "outcome", "op_id": k, "attempt": 1, "result": "ack"}]
        s4 = {"start_ms": -120_000, "d1_done_ms": t_d, "refusal_start_ms": 0,
              "refusal_end_ms": min(refusal_min * 60_000, t_e), "end_ms": t_e,
              "refusal_ended_by": "natural recovery" if refusal_min < quiet_min else "D1's chain over"}
        if quiet_min <= end_min:
            s4["d1_quiet_ms"] = t_q
        return S, records, {"4": s4}

    def judge(self, which, **kw):
        S, R, tl = self.build(**kw)
        return getattr(judge, which)(S, R, tl, judge.VALUES, ["node0", "node1"])

    # -- C1a: progress under refusal -------------------------------------------

    def test_c1a_pass(self):
        r = self.judge("c1a")
        self.assertEqual(r["outcome"], judge.PASSED, r)

    def test_c1a_pass_with_a_natural_recovery(self):
        """The refusal ends by itself after 12 min: progress was shown."""
        r = self.judge("c1a", refusal_min=12)
        self.assertEqual(r["outcome"], judge.PASSED, r)

    def test_c1a_too_short_a_refusal_is_inconclusive(self):
        self.assertEqual(self.judge("c1a", refusal_min=5)["outcome"], judge.INCONCLUSIVE)

    def test_c1a_no_progress_fails(self):
        """Refused past D1's chain (25 min after D1) with no progress."""
        r = self.judge("c1a", reclaimed=50, refusal_min=28, quiet_min=30, end_min=35)
        self.assertEqual(r["outcome"], judge.FAILED, r)

    def test_c1a_no_progress_before_d1_was_due_is_inconclusive(self):
        """Review of #30: a refusal over before D1's reclaim could start
        shows nothing; never a failure."""
        r = self.judge("c1a", reclaimed=50, refusal_min=15)
        self.assertEqual(r["outcome"], judge.INCONCLUSIVE, r)

    def test_c1a_reclaim_refusals_fail_it(self):
        self.assertEqual(self.judge("c1a", refused_rises=True)["outcome"], judge.FAILED)

    def test_c1a_progress_not_attributable_is_inconclusive(self):
        """Enough reclaims, but aborted writes could account for them."""
        r = self.judge("c1a", refusal_min=12, refusals=2000)
        self.assertEqual(r["outcome"], judge.INCONCLUSIVE, r)
        self.assertIn("not attributable", r["reasons"][0])

    def test_c1a_subtracts_the_fills_capacity_refusals(self):
        """Review of #30: writes refused for capacity during the fill (before
        the refusal's start) are reclaimed inside it; they are not D1's."""
        S, R, tl = self.build(refusal_min=12)
        for s in S:
            if s["target"] == "gateway0":
                s["values"]["arca_gateway_node_capacity_refusals_total"] = {
                    "value": 0 if s["ended_wall_ms"] < -30_000 else 400, "unit": "count"}
        r = judge.c1a(S, R, tl, judge.VALUES, ["node0", "node1"])
        self.assertEqual(r["outcome"], judge.INCONCLUSIVE, r)
        self.assertEqual(r["evidence"]["reclaim_node_capacity_refusals"], 400)

    def test_c1a_unsampled_reconcile_counts_are_inconclusive(self):
        self.assertEqual(self.judge("c1a", no_log=True)["outcome"], judge.INCONCLUSIVE)

    def test_c1a_a_gap_is_inconclusive(self):
        self.assertEqual(self.judge("c1a", gap=True)["outcome"], judge.INCONCLUSIVE)

    def test_c1a_a_counter_reset_is_inconclusive(self):
        self.assertEqual(self.judge("c1a", reset=True)["outcome"], judge.INCONCLUSIVE)

    # -- C1b: D1 fully reclaimed by the deadline --------------------------------

    def test_c1b_pass_whether_the_refusal_held_or_not(self):
        for refusal_min in (30, 12):
            r = self.judge("c1b", refusal_min=refusal_min)
            self.assertEqual(r["outcome"], judge.PASSED, (refusal_min, r))

    def test_c1b_too_few_reclaims_at_the_quiet_point_fail(self):
        self.assertEqual(self.judge("c1b", reclaimed=2000)["outcome"], judge.FAILED)

    def test_c1b_not_quiet_by_the_deadline_fails(self):
        r = self.judge("c1b", quiet_min=70, end_min=52)
        self.assertEqual(r["outcome"], judge.FAILED, r)

    def test_c1b_stopped_before_the_deadline_is_inconclusive(self):
        """A stop at 20 min, D1 not yet over: no evidence either way."""
        r = self.judge("c1b", quiet_min=40, end_min=20)
        self.assertEqual(r["outcome"], judge.INCONCLUSIVE, r)

    def test_c1b_quiet_after_the_deadline_fails(self):
        self.assertEqual(self.judge("c1b", quiet_min=55, end_min=60)["outcome"], judge.FAILED)

    def test_c1b_not_attributable_is_inconclusive(self):
        r = self.judge("c1b", reconcile=500)
        self.assertEqual(r["outcome"], judge.INCONCLUSIVE, r)

    def test_c1b_a_reclaim_counted_just_after_the_quiet_point_passes(self):
        """Review of #30: the nodes read 0 pending 15 s before the
        controller counts its last pass."""
        r = self.judge("c1b", lag_s=15)
        self.assertEqual(r["outcome"], judge.PASSED, r)

    def test_c1b_violation_counters_not_sampled_are_inconclusive(self):
        self.assertEqual(self.judge("c1b", no_errors=True)["outcome"], judge.INCONCLUSIVE)

    def test_the_subtractions_reach_the_samples_outside_the_window(self):
        """Review of #30: a refusal counted between the last sample before
        the window and its start is still subtracted."""
        S = [smp("gateway0", 0, arca_gateway_node_capacity_refusals_total=0),
             smp("gateway0", 15_000, arca_gateway_node_capacity_refusals_total=10),
             smp("gateway0", 30_000, arca_gateway_node_capacity_refusals_total=10),
             smp("gateway0", 45_000, arca_gateway_node_capacity_refusals_total=12)]
        k = "arca_gateway_node_capacity_refusals_total"
        self.assertEqual(judge._increase(S, "gateway0", k, 10_000, 40_000, 15_000, 2.0), 0)
        self.assertEqual(judge._increase_outer(S, "gateway0", k, 10_000, 40_000, 15_000, 2.0), 12)
        # In the bound: the reclaims inside, the refusals outside.
        S += [smp("controller", t, arca_controller_reclaimed_total=t // 1000) for t in (0, 15_000, 30_000, 45_000)]
        S += [smp("controller-log", t, reconcile_deleted=0) for t in (0, 15_000, 30_000, 45_000)]
        bound, parts = judge.d1_reclaimed_bound(S, 10_000, 40_000, judge.VALUES)
        self.assertEqual((parts["reclaimed"], parts["node_capacity_refusals"]), (15, 12))
        self.assertEqual(bound, 15 - 36)
        self.assertIsNone(judge._increase_outer(S, "gateway0", k, 10_000, 90_000, 15_000, 2.0))

    def test_c1b_reclaim_errors_fail_it(self):
        self.assertEqual(self.judge("c1b", refused_rises=True)["outcome"], judge.FAILED)

    def test_both_in_the_overall_outcome(self):
        S, R, tl = self.build(reclaimed=2000)
        res = judge.judge(S, R, tl, {"reason": "disk-free", "stage": "5"}, nodes=("node0", "node1"))
        o = {c["criterion"]: c["outcome"] for c in res["criteria"]}
        self.assertEqual(o["C1b"], judge.FAILED)
        self.assertEqual(res["outcome"], judge.FAILED)
        self.assertEqual(res["plan_version"], judge.PLAN_VERSION)

    def test_the_end_of_d1_not_recorded_is_inconclusive(self):
        S, R, tl = self.build()
        del tl["4"]["d1_done_ms"]
        self.assertEqual(judge.c1b(S, R, tl, judge.VALUES, ["node0", "node1"])["outcome"], judge.INCONCLUSIVE)


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

    def c5(self, sc, by, settled="absent", pending=10):
        s7 = {"verify": {"violations": [], "complete": True, "keys": 1}, "manifest_findings": 0,
              "scrub_findings": sc, "scrub_by_class": by, "pending_before_scrub": pending}
        if settled != "absent":
            s7["settled_ms"] = settled
        return judge.c5({"7": s7})["outcome"]

    def test_c5_unexpected_shards_after_the_settle_fail(self):
        """Plan v3: settled, every finding fails C5, unexpected shards too."""
        self.assertEqual(self.c5(5, [0, 0, 0, 0, 5, 0, 0, 0], settled=100), judge.FAILED)

    def test_c5_unexpected_shards_before_the_settle_are_inconclusive_only_alone(self):
        self.assertEqual(self.c5(5, [0, 0, 0, 0, 5, 0, 0, 0], settled=None), judge.INCONCLUSIVE)
        self.assertEqual(self.c5(6, [1, 0, 0, 0, 5, 0, 0, 0], settled=None), judge.FAILED)
        self.assertEqual(self.c5(5, None, settled=None), judge.FAILED, "no breakdown: no excuse")
        self.assertEqual(self.c5(5, [0, 0, 0, 0, 4, 0, 0, 0], settled=None), judge.FAILED, "inconsistent")
        # More unexpected shards than records pending reclaim: not retained
        # records alone (a stuck reclaim or orphans): Failed.
        self.assertEqual(self.c5(11, [0, 0, 0, 0, 11, 0, 0, 0], settled=None), judge.FAILED)
        self.assertEqual(self.c5(5, [0, 0, 0, 0, 5, 0, 0, 0], settled=None, pending=None), judge.FAILED)

    def test_c5_of_a_timeline_without_the_settle_step_is_judged_as_before(self):
        """Run r110-20261122T030322Z (plan v2): its Failed stays Failed."""
        self.assertEqual(self.c5(1809, [0, 0, 0, 0, 1809, 0, 0, 0]), judge.FAILED)
        self.assertEqual(self.c5(0, [0] * 8), judge.PASSED)

    def c5_held(self, sc, by, held, settled=None, pending=10):
        s7 = {"verify": {"violations": [], "complete": True, "keys": 1}, "manifest_findings": 0,
              "scrub_findings": sc, "scrub_by_class": by, "pending_before_scrub": pending,
              "settled_ms": settled, "scrub_held": held}
        return judge.c5({"7": s7})["outcome"]

    def test_c5_with_per_shard_classes_has_no_count_based_excuse(self):
        """0.2.0: the scrub classifies records kept for reclaim itself; an
        unexpected shard is an orphan, before the settle too."""
        held = {"retained": 40, "retained_damaged": 0, "in_flight": 0, "unclassified": 0}
        self.assertEqual(self.c5_held(5, [0, 0, 0, 0, 5, 0, 0, 0], held), judge.FAILED)
        self.assertEqual(self.c5_held(0, [0] * 8, held), judge.PASSED)
        # A damaged record kept for reclaim is counted under its damage class.
        damaged = dict(held, retained_damaged=1)
        self.assertEqual(self.c5_held(1, [0, 1, 0, 0, 0, 0, 0, 0], damaged), judge.FAILED)
        # Shards not classified are never a pass.
        self.assertEqual(self.c5_held(0, [0] * 8, dict(held, unclassified=2)), judge.INCONCLUSIVE)

    def test_c5_incomplete_is_not_a_pass(self):
        tl = {"7": {"verify": {"violations": [], "complete": False, "keys": 1}, "manifest_findings": 0,
                    "scrub_findings": None}}
        self.assertEqual(judge.c5(tl)["outcome"], judge.INCONCLUSIVE)


class ReviewFindingsJudge(unittest.TestCase):
    """Cases from the independent review of P4 (judgement)."""

    def all_pass(self):
        ok = lambda name: lambda *a, **k: judge.verdict(name, judge.PASSED)  # noqa: E731
        return [mock.patch.object(judge, n, ok(n.upper() if n in ("c2", "c4", "c5") else n[:2].upper() + n[2:]))
                for n in ("c1a", "c1b", "c2", "c3a", "c3b", "c4", "c5")]

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

    def test_a_probe_without_an_outcome_is_never_a_pass(self):
        self.assertEqual(self.c3a(unknown=1), judge.INCONCLUSIVE)

    def c3b(self, acks, no_outcome=0, refused=0, unknown=0):
        S = []
        for t in range(0, 600_001, 15_000):
            S.append(smp("gateway0", t, arca_gateway_admission_state=0))
            S.append(smp("controller", t, arca_controller_backpressure_state=0))
        R, k = [], 0
        for result in ["ack"] * acks + [None] * no_outcome + ["refused"] * refused + ["unknown"] * unknown:
            R.append({"type": "intent", "op_id": k, "attempt": 1, "key": f"c{k}", "op": "PUT", "stage": "6",
                      "kind": "op", "wall_ms": 1000 + k})
            if result:
                R.append({"type": "outcome", "op_id": k, "attempt": 1, "result": result})
            k += 1
        tl = {"6": {"start_ms": 0, "end_ms": 600_000, "completed": True}}
        return judge.c3b(S, R, tl, judge.VALUES)["outcome"]

    def test_c3b_attempts_without_an_outcome(self):
        self.assertEqual(self.c3b(1000), judge.PASSED)
        self.assertEqual(self.c3b(980, no_outcome=20), judge.INCONCLUSIVE)
        self.assertEqual(self.c3b(950, no_outcome=5, unknown=45), judge.FAILED)
        self.assertEqual(self.c3b(990, refused=1), judge.FAILED)
        # A refusal fails C3b whatever the open attempts would have been.
        self.assertEqual(self.c3b(980, no_outcome=15, refused=5), judge.FAILED)


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

        def boom(**kw):
            raise RuntimeError("ssh failed")

        c.smoke = boom
        res, _ = self.go(c)
        self.assertEqual((res["stop"]["reason"], res["stop"]["stage"]), ("driver-error", "2"))
        self.assertIn("ssh failed", res["stop"]["error"])
        self.assertNotEqual(res["outcome"], judge.PASSED)

    def test_a_stale_value_is_not_used(self):
        engine = driver.Run(self.cfg, FakeCluster(self.cfg, self.clock),
                            driver.Evidence(os.path.join(self.dir, "e"), self.cfg["run_id"]), self.clock)
        now = engine.host_ms()
        S = [smp("controller", now - 60_000, arca_controller_gc_backlog=0)]
        self.assertIsNone(engine._fresh(S, "controller", "arca_controller_gc_backlog"))
        S.append(smp("controller", now - 10_000, arca_controller_gc_backlog=0))
        self.assertEqual(engine._fresh(S, "controller", "arca_controller_gc_backlog"), 0)

    def test_the_refusal_is_held_for_d1s_chain(self):
        c = FakeCluster(self.cfg, self.clock)
        res, ev = self.go(c)  # admission stays write rejected: stage 5 never recovers
        tl = common.read_json(os.path.join(ev.dir, "timeline.json"))
        self.assertGreaterEqual(tl["4"]["refusal_end_ms"] - tl["4"]["d1_done_ms"], driver.CHAIN_S * 1000)
        self.assertFalse(tl["4"]["admission_recovered_early"])
        self.assertEqual(res["stop"]["stage"], "5")

    def test_a_gap_in_the_gateways_samples_does_not_end_the_hold(self):
        c = FakeCluster(self.cfg, self.clock)
        real = c.run_step

        def run_step(step, deadline, check=None):
            r = real(step, deadline, check)
            if step["kind"] == "put-until-refused":
                c.samples()  # one sample showing the refusal
                c.drop_gateway = True
            return r

        c.run_step = run_step
        res, ev = self.go(c)
        tl = common.read_json(os.path.join(ev.dir, "timeline.json"))
        self.assertFalse(tl["4"]["admission_recovered_early"])
        self.assertGreaterEqual(tl["4"]["refusal_end_ms"] - tl["4"]["d1_done_ms"], driver.CHAIN_S * 1000)

    def stage7(self, c):
        """Stage 7 alone (the earlier stages' conditions are not modelled),
        stage 6 having ended now."""
        engine = driver.Run(self.cfg, c, driver.Evidence(os.path.join(self.dir, "e7"), self.cfg["run_id"]),
                            self.clock)
        engine.tl["6"] = {"end_ms": engine.host_ms()}
        engine.stage(driver.stages(self.cfg)[6], engine.s7)
        return engine

    def test_stage_7_scrubs_only_after_the_background_passes_settled(self):
        """Plan v3: the manifests and the scrub are read after the orphan age
        and two reconcile intervals from stage 6's end, with no GC backlog
        and nothing pending reclaim."""
        c = FakeCluster(self.cfg, self.clock)
        calls = []
        real = c.integrity
        c.integrity = lambda kind: calls.append((kind, self.clock.wall_ms())) or real(kind)
        e = self.stage7(c)
        t7 = e.tl["7"]
        self.assertEqual(t7["settle_after_ms"], e.tl["6"]["end_ms"] + driver.SETTLE7_S * 1000)
        self.assertIsNotNone(t7["settled_ms"])
        self.assertGreaterEqual(t7["settled_ms"], t7["settle_after_ms"])
        self.assertEqual([k for k, _ in calls], ["verify-manifests", "scrub"])
        self.assertTrue(all(t >= t7["settle_after_ms"] for _, t in calls), calls)

    def test_stage_7_not_settled_still_scrubs_and_says_so(self):
        c = FakeCluster(self.cfg, self.clock)
        real = c.sample

        def sample():
            real()
            c._samples[-1]["values"]["arca_node_reclaim_pending_records"] = {"value": 7, "unit": "count"}

        c.sample = sample
        e = self.stage7(c)
        self.assertIn("settled_ms", e.tl["7"])
        self.assertIsNone(e.tl["7"]["settled_ms"])
        self.assertGreaterEqual(e.tl["7"]["pending_before_scrub"], 7)
        self.assertEqual(c.integrity_calls, ["verify-manifests", "scrub"])

    def test_stage_7_with_no_time_left_for_the_wait_still_scrubs(self):
        """The verify ends after the wait's cutoff: no wait, not settled."""
        c = FakeCluster(self.cfg, self.clock)
        real = c.run_step

        def slow(step, deadline, check=None):
            if step.get("kind") == "verify":
                self.clock.sleep(driver.stages(self.cfg)[6]["max_s"] - driver.SCRUB_RESERVE_S + 60)
            return real(step, deadline, check=check)

        c.run_step = slow
        e = self.stage7(c)
        self.assertIsNone(e.tl["7"]["settled_ms"])
        self.assertEqual(c.integrity_calls, ["verify-manifests", "scrub"])

    def test_stage_7_settling_late_but_in_time_is_settled(self):
        c = FakeCluster(self.cfg, self.clock)
        real = c.sample
        t0 = self.clock.monotonic()

        def sample():
            real()
            if self.clock.monotonic() - t0 < driver.SETTLE7_S + 300:
                c._samples[-1]["values"]["arca_node_reclaim_pending_records"] = {"value": 3, "unit": "count"}

        c.sample = sample
        e = self.stage7(c)
        self.assertIsNotNone(e.tl["7"]["settled_ms"])
        self.assertGreaterEqual(e.tl["7"]["settled_ms"] - e.tl["7"]["settle_after_ms"], 250_000)

    def test_a_natural_recovery_ends_the_hold_and_the_run_goes_on(self):
        """Plan v2: admission recovering before D1 matured ends the hold;
        the stage waits for D1's chain to be over, then stage 5 runs."""
        c = FakeCluster(self.cfg, self.clock)
        c.recover_after = 5
        hold_done = []
        real = c.step_done
        c.step_done = lambda n: hold_done.append((n, self.clock.monotonic())) or real(n)
        res, ev = self.go(c)
        tl = common.read_json(os.path.join(ev.dir, "timeline.json"))
        self.assertTrue(tl["4"]["admission_recovered_early"])
        self.assertEqual(tl["4"]["refusal_ended_by"], "natural recovery")
        self.assertGreaterEqual(tl["4"]["d1_quiet_ms"] - tl["4"]["d1_done_ms"], driver.CHAIN_S * 1000)
        self.assertIn("5", tl, "stage 5 did not start")
        self.assertGreaterEqual(tl["5"]["start_ms"], tl["4"]["d1_quiet_ms"])

    def test_not_quiet_by_the_c1b_deadline_ends_the_run_in_stage_4(self):
        c = FakeCluster(self.cfg, self.clock)
        c.recover_after = 5
        real = c.sample

        def sample():
            real()
            if any(s.get("kind") == "put-until-refused" for s in c.steps):  # stage 4 on
                c._samples[-1]["values"]["arca_node_reclaim_pending_records"] = {"value": 7, "unit": "count"}

        c.sample = sample
        res, ev = self.go(c)
        tl = common.read_json(os.path.join(ev.dir, "timeline.json"))
        self.assertEqual((res["stop"]["reason"], res["stop"]["stage"]), ("stage-exit", "4"))
        self.assertNotIn("d1_quiet_ms", tl["4"])
        self.assertNotIn("5", tl)

    def test_c3a_is_judged_only_once_the_probe_step_ended(self):
        c = FakeCluster(self.cfg, self.clock)
        engine = driver.Run(self.cfg, c, driver.Evidence(os.path.join(self.dir, "e"), self.cfg["run_id"]), self.clock)

        def ledger():
            t0 = engine.tl["5"]["c3a_window_start_ms"]
            R = []
            for k in range(61):
                R.append({"type": "intent", "op_id": k, "attempt": 1, "key": f"q{k}", "op": "PUT", "stage": "5",
                          "kind": "probe", "wall_ms": t0 + k * 10_000})
                if k < 60 or c.step_waited:  # the last one in flight until the step ended
                    R.append({"type": "outcome", "op_id": k, "attempt": 1, "result": "ack"})
            return R

        c.ledger_records = ledger
        self.assertTrue(engine.stage(driver.stages(self.cfg)[4], engine.s5))
        self.assertEqual(engine.tl["5"]["c3a"], judge.PASSED)

    def test_a_protection_counter_stops_the_run_at_once(self):
        c = FakeCluster(self.cfg, self.clock)
        real = c.sample

        def sample():
            real()
            if c.admission == 3:  # during stage 4
                c._samples[-1]["values"]["arca_node_reclaim_damaged"] = {"value": 1, "unit": "count"}

        c.sample = sample
        res, _ = self.go(c)
        self.assertEqual((res["stop"]["reason"], res["stop"]["stage"]), ("protection-violation", "4"))
        self.assertEqual(res["outcome"], judge.FAILED)

    def test_a_protection_counter_stops_a_running_step(self):
        """In a step (stage 6's churn), not only in the waits."""
        c = FakeCluster(self.cfg, self.clock)
        real = c.sample

        def sample():
            real()
            if any(s.get("kind") == "churn" for s in c.steps):
                c._samples[-1]["values"]["arca_node_reclaim_damaged"] = {"value": 1, "unit": "count"}

        c.sample = sample
        # Stage 5 never passes in the fake: straight to stage 6's body.
        engine = driver.Run(self.cfg, c, driver.Evidence(os.path.join(self.dir, "e"), self.cfg["run_id"]), self.clock)
        engine.stage(driver.stages(self.cfg)[5], engine.s6)
        self.assertEqual((engine.stop["reason"], engine.stop["stage"]), ("protection-violation", "6"))

    def test_an_interrupt_during_the_end_still_saves_the_evidence(self):
        c = FakeCluster(self.cfg, self.clock)
        c.stop_at_stage, c.stop_reason = "4", "operator"
        calls = {"n": 0}

        def escalation():
            calls["n"] += 1
            raise KeyboardInterrupt

        c.escalation = escalation
        res, ev = self.go(c)
        self.assertTrue(os.path.exists(os.path.join(ev.dir, "result.json")))
        self.assertIn("drain: interrupted (SIGINT): the stops and the evidence follow", res["evidence_failures"])
        self.assertTrue(c.services_stopped)

    def test_recovery_only_is_still_a_refusal(self):
        c = FakeCluster(self.cfg, self.clock)
        real = c.run_step

        def run_step(step, deadline, check=None):
            r = real(step, deadline, check)
            if step["kind"] == "put-until-refused":
                c.admission = 4
            return r

        c.run_step = run_step
        res, ev = self.go(c)
        tl = common.read_json(os.path.join(ev.dir, "timeline.json"))
        self.assertFalse(tl["4"]["admission_recovered_early"])
        self.assertIsNotNone(tl["4"]["refusal_start_ms"])

    def test_an_interrupt_is_an_operator_stop(self):
        c = FakeCluster(self.cfg, self.clock)

        def interrupt(**kw):
            raise KeyboardInterrupt

        c.smoke = interrupt
        res, _ = self.go(c)
        self.assertEqual((res["stop"]["reason"], res["stop"]["stage"]), ("operator", "2"))

    def test_a_resource_rule_during_the_drain_stops_the_services(self):
        c = FakeCluster(self.cfg, self.clock)
        c.stop_at_stage, c.stop_reason = "4", "operator"
        c.escalated = {"reason": "disk-free", "source": "watchdog"}
        res, ev = self.go(c)
        self.assertTrue(c.services_stopped)
        tl = common.read_json(os.path.join(ev.dir, "timeline.json"))
        self.assertEqual(tl["drain"]["escalation"]["reason"], "disk-free")
        # Stopped by the drain when the rule fired, not only at the end.
        self.assertEqual(tl["drain"]["services"]["verdicts"]["node0.json"]["verdict"], "stopped")
        self.assertLess(tl["drain"]["end_ms"] - tl["drain"]["start_ms"], driver.DRAIN_OBSERVE_S * 1000)

    # -- the final stops (finish) ------------------------------------------------

    def passing(self):
        return mock.patch.object(judge, "judge", lambda *a, **k: {"outcome": judge.PASSED, "criteria": []})

    def ended(self, c, failed=False):
        root = tempfile.mkdtemp(dir=self.dir)
        engine = driver.Run(self.cfg, c, driver.Evidence(root, self.cfg["run_id"]), self.clock)
        if failed:
            with mock.patch.object(judge, "judge", lambda *a, **k: {"outcome": judge.FAILED, "criteria": []}):
                engine.finish()
        else:
            with self.passing():
                engine.finish()
        return engine

    def test_all_stops_confirmed_keep_a_pass_and_end_the_watching(self):
        c = FakeCluster(self.cfg, self.clock)
        e = self.ended(c)
        self.assertEqual(e.result["outcome"], judge.PASSED)
        # The host's evidence is read only once every stop has been made.
        self.assertEqual([n for n, _ in c.stop_calls],
                         ["stop_load", "stop_services", "stop_sampler", "stop_sentinel", "stop_watchdog",
                          "collect_evidence"])
        self.assertNotIn("intervention", e.tl["final_stops"])
        self.assertTrue(e.tl["host_evidence"]["complete"])

    def test_an_incomplete_host_evidence_is_never_a_pass(self):
        c = FakeCluster(self.cfg, self.clock)
        c.incomplete = True
        e = self.ended(c)
        self.assertEqual(e.result["outcome"], judge.INCONCLUSIVE)
        self.assertFalse(e.tl["host_evidence"]["complete"])
        self.assertTrue(any(f.startswith("host_evidence: incomplete") for f in e.result["evidence_failures"]))

    def test_a_boot_not_compared_or_a_reboot_gives_gone_verdicts_their_cause(self):
        for boot, cause in (({"rebooted": True}, "host-reboot"), ({"rebooted": None}, "unknown"),
                            (RuntimeError("ssh: lost"), "unknown")):
            c = FakeCluster(self.cfg, self.clock)

            def check(boot=boot):
                if isinstance(boot, Exception):
                    raise boot
                return boot

            c.boot_check = check
            e = self.ended(c)
            self.assertEqual(e.result["outcome"], judge.INCONCLUSIVE, boot)
            self.assertTrue(e.tl["final_stops"]["stop_load"]["load"]["cause"].startswith(cause), boot)
            self.assertNotIn("cause", e.tl["final_stops"]["stop_load"]["guard"])  # stopped, not gone
            self.assertTrue(any(f.startswith(("host_boot: not compared", "the host booted again"))
                                for f in e.result["evidence_failures"]), boot)
        c = FakeCluster(self.cfg, self.clock)
        c.boot_check = lambda: {"rebooted": False}
        e = self.ended(c)
        self.assertEqual(e.result["outcome"], judge.PASSED)
        self.assertNotIn("cause", e.tl["final_stops"]["stop_load"]["load"])

    def test_evidence_with_writers_not_confirmed_stopped_is_not_complete(self):
        c = FakeCluster(self.cfg, self.clock)
        c.verdicts = {"node1.json": {"verdict": "not-stopped", "state": "alive"}}
        e = self.ended(c)
        self.assertFalse(e.tl["host_evidence"]["complete"])
        self.assertEqual(e.tl["host_evidence"]["state"], "complete, writers not confirmed stopped")

    def test_the_collection_limits_reach_the_collection(self):
        c = FakeCluster(self.cfg, self.clock)
        root = tempfile.mkdtemp(dir=self.dir)
        engine = driver.Run(self.cfg, c, driver.Evidence(root, self.cfg["run_id"]), self.clock,
                            collect={"log_cap": 5 << 20, "call_s": 30.0})
        with self.passing():
            engine.finish()
        self.assertEqual(c.collect_kw, {"deadline_s": driver.COLLECT_S, "log_cap": 5 << 20, "call_s": 30.0})

    def test_a_service_not_stopped_is_retried_then_keeps_the_watching(self):
        for verdict in ({"verdict": "not-stopped", "state": "alive"}, {"verdict": "unverified", "reason": "x"}):
            c = FakeCluster(self.cfg, self.clock)
            c.verdicts = {"node1.json": verdict}
            e = self.ended(c)
            self.assertEqual(e.result["outcome"], judge.INCONCLUSIVE, verdict)
            calls = [n for n, _ in c.stop_calls]
            self.assertNotIn("stop_sentinel", calls)
            self.assertNotIn("stop_watchdog", calls)
            retries = [a for n, a in c.stop_calls if n == "stop_services"]
            self.assertGreater(len(retries), 1, "an unconfirmed stop is not retried")
            self.assertEqual(len(retries), 1 + driver.FINAL_STOP_ROUNDS)
            self.assertEqual(retries[1:], [{"node1.json"}] * driver.FINAL_STOP_ROUNDS)
            iv = e.tl["final_stops"]["intervention"]
            self.assertEqual(list(iv["unconfirmed"]), ["node1.json"])
            self.assertIn("kept", iv["watching"])

    def test_a_failed_listing_or_a_missing_record_is_never_a_clean_end(self):
        c = FakeCluster(self.cfg, self.clock)
        c.listing_error = "rc 255: ssh"
        self.assertEqual(self.ended(c).result["outcome"], judge.INCONCLUSIVE)
        self.assertNotIn("stop_sentinel", [n for n, _ in c.stop_calls])
        # A service started but without a verdict (its record gone).
        c = FakeCluster(self.cfg, self.clock)
        real = c.stop_services

        def missing(names=None):
            out = real(names)
            out["verdicts"].pop("node0.json", None)
            return out

        c.stop_services = missing
        e = self.ended(c)
        self.assertEqual(e.result["outcome"], judge.INCONCLUSIVE)
        self.assertIn("node0.json", e.tl["final_stops"]["intervention"]["unconfirmed"])

    def test_an_empty_listing_after_setup_is_not_all_stopped(self):
        c = FakeCluster(self.cfg, self.clock)
        c.started = []
        self.assertEqual(self.ended(c).result["outcome"], judge.INCONCLUSIVE)
        # Nothing was ever set up (stage 1 failed): an empty listing is the truth.
        c = FakeCluster(self.cfg, self.clock)
        c.started, c.setup_started = [], False
        real = c.stop_services
        c.stop_services = lambda names=None: dict(real(names), listing={"exists": False})
        self.assertEqual(self.ended(c).result["outcome"], judge.PASSED)

    def test_not_started_is_accepted_only_as_a_verdict(self):
        c = FakeCluster(self.cfg, self.clock)
        c.verdicts = {"load.json": {"verdict": "not-started"}}
        self.assertEqual(self.ended(c).result["outcome"], judge.PASSED)

    def test_a_load_or_guard_not_confirmed_is_never_a_clean_end(self):
        for change in ({"load": {"verdict": "unverified"}}, {"guard_result": {"group_empty": False}},
                       {"guard": "signalled"}):
            c = FakeCluster(self.cfg, self.clock)
            real = c.stop_load
            c.stop_load = lambda change=change: dict(real(), **change)
            e = self.ended(c)
            self.assertEqual(e.result["outcome"], judge.INCONCLUSIVE, change)
            self.assertNotIn("stop_watchdog", [n for n, _ in c.stop_calls], change)

    def test_a_guard_or_the_sampler_not_accounted_for_keeps_the_watching(self):
        """Review of #27: a smoke guard started.list names without a verdict
        from stop_load (its listing failed), the sampler not stopped, or
        stop_load returning no dict."""
        cases = []
        c = FakeCluster(self.cfg, self.clock)
        c.started = c.started + ["guard-smoke-1"]
        cases.append(("guard-smoke-1.json", c))
        c = FakeCluster(self.cfg, self.clock)
        c.verdicts = {"sampler.json": {"verdict": "not-stopped"}}
        cases.append(("sampler.json", c))
        c = FakeCluster(self.cfg, self.clock)
        c.stop_load = lambda: None
        cases.append(("stop_load", c))
        for key, c in cases:
            e = self.ended(c)
            self.assertEqual(e.result["outcome"], judge.INCONCLUSIVE, key)
            self.assertIn(key, e.tl["final_stops"]["intervention"]["unconfirmed"])
            self.assertNotIn("stop_sentinel", [n for n, _ in c.stop_calls], key)
        # A smoke guard with its verdict is accounted for.
        c = FakeCluster(self.cfg, self.clock)
        c.started = c.started + ["guard-smoke-1"]
        real = c.stop_load
        c.stop_load = lambda: dict(real(), **{"guard-smoke-1.json": {"verdict": "stopped"}})
        self.assertEqual(self.ended(c).result["outcome"], judge.PASSED)

    def test_a_failure_stays_failed_with_unconfirmed_stops(self):
        c = FakeCluster(self.cfg, self.clock)
        c.verdicts = {"node0.json": {"verdict": "not-stopped"}}
        self.assertEqual(self.ended(c, failed=True).result["outcome"], judge.FAILED)

    def test_an_unconfirmed_sentinel_or_watchdog_stop_is_recorded(self):
        c = FakeCluster(self.cfg, self.clock)
        c.stop_sentinel = lambda: {"verdict": "not-stopped"}
        e = self.ended(c)
        self.assertEqual(e.result["outcome"], judge.INCONCLUSIVE)
        self.assertTrue(any("stop_sentinel" in f for f in e.result["evidence_failures"]))

    def test_the_protection_scan_sees_every_new_sample_and_keeps_a_violation(self):
        c = FakeCluster(self.cfg, self.clock)
        engine = driver.Run(self.cfg, c, driver.Evidence(os.path.join(self.dir, "e"), self.cfg["run_id"]), self.clock)
        S = [smp("node0", t, arca_node_reclaim_damaged=0) for t in range(500)]
        self.assertIsNone(engine.protection(S))
        S.append(smp("node1", 500, arca_node_reclaim_damaged=1))
        # 1,000 samples more in the same poll: the violation is not in the last 200.
        S += [smp("node0", 501 + t, arca_node_reclaim_damaged=0) for t in range(1000)]
        self.assertEqual(engine.protection(S), {"target": "node1", "counter": "arca_node_reclaim_damaged"})
        S += [smp("node1", 2000, arca_node_reclaim_damaged=0)]  # the gauge back to 0: still a violation
        self.assertIsNotNone(engine.protection(S))

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
    # The host's answers to the reach, the boot and the inventory: an
    # empty run (nothing on the host to save).
    HOST = [("echo r110-reach", "r110-reach\n"), ("# r110:boot", json.dumps({"boot_id": "b1"}) + "\n"),
            ("# r110:inventory", json.dumps({"files": [], "errors": [], "excluded": [],
                                             "roots": {"run": "absent", "fdb": "absent"}}) + "\n")]

    def cluster(self, answers=None):
        cfg = driver.validate(copy.deepcopy(self.cfg))
        rec = Recorder(list(answers or []) + self.HOST)
        return remote.SshCluster(cfg, runner=rec), rec, cfg

    def test_scrub_lines_of_both_formats_are_parsed(self):
        old = "scrub group 7 verified 9 bytes 1 findings [0, 0, 0, 0, 3, 0, 0, 0] enqueued 0 errors 0\n"
        new = ("scrub group 7 verified 9 bytes 1 findings [0, 0, 0, 0, 0, 0, 0, 0] enqueued 0 errors 0 "
               "retained 3 retained_damaged 0 in_flight 1 unclassified 0\n")
        bad = ("scrub group 7 verified 9 bytes 1 findings [0, 0, 0, 0, 0, 0, 0, 0] enqueued 0 errors 2 "
               "retained 1 retained_damaged 0 in_flight 0 unclassified 2\n")
        # A scrub with errors: incomplete (None), its classes kept (0.2.0
        # plan §4.1: a finding beside an error still fails C5).
        for line, total, classes, held, errors in (
                (old, 3, [0, 0, 0, 0, 3, 0, 0, 0], None, 0),
                (new, 0, [0] * 8, {"retained": 3, "retained_damaged": 0, "in_flight": 1, "unclassified": 0}, 0),
                (bad, None, [0] * 8, {"retained": 1, "retained_damaged": 0, "in_flight": 0, "unclassified": 2}, 2)):
            c, _, _ = self.cluster(answers=[("grep '^group '", "group 7 x\n"), ("integrity scrub", line)])
            self.assertEqual(c.integrity("scrub"), total, line)
            self.assertEqual(c.last_scrub_classes, classes, line)
            self.assertEqual(c.last_scrub_held, held, line)
            self.assertEqual(c.last_scrub_errors, errors, line)

    def test_every_group_is_scrubbed_after_one_with_errors(self):
        """0.2.0 plan §4.1: group 1's errors (an unclassified shard) do not
        hide group 2's checksum finding, nor a group without a result."""
        g1 = ("scrub group 1 verified 9 bytes 1 findings [0, 0, 0, 0, 0, 0, 0, 0] enqueued 0 errors 1 "
              "retained 0 retained_damaged 0 in_flight 0 unclassified 1\n")
        g2 = ("scrub group 2 verified 9 bytes 1 findings [0, 1, 0, 0, 0, 0, 0, 0] enqueued 1 errors 0 "
              "retained 2 retained_damaged 0 in_flight 0 unclassified 0\n")
        c, rec, _ = self.cluster(answers=[("grep '^group '", "group 1 x\ngroup 2 y\ngroup 3 z\n"),
                                          ("scrub --group 1 ", g1), ("scrub --group 2 ", g2),
                                          ("scrub --group 3 ", "error: timed out\n")])
        self.assertIsNone(c.integrity("scrub"))
        self.assertEqual(len([cmd for _, cmd, _ in rec.commands if "integrity scrub" in cmd]), 3)
        self.assertEqual(c.last_scrub_classes, [0, 1, 0, 0, 0, 0, 0, 0])
        self.assertEqual(c.last_scrub_errors, 1)
        self.assertEqual(c.last_scrub_unread, [3])
        self.assertEqual(c.last_scrub_held, {"retained": 2, "retained_damaged": 0, "in_flight": 0,
                                             "unclassified": 1})

    def test_teardown_removes_only_marked_run_directories(self):
        c, rec, cfg = self.cluster(answers=[('print("checked %d"', "checked 0\n"), ("rm -rf --", "removed\n")])
        out = c.teardown(os.path.join(self.dir, "evidence"))
        rms = [cmd for _, cmd, _ in rec.commands if "rm " in cmd]
        self.assertEqual(len(rms), 2)
        for cmd, d in zip(rms, (cfg["remote"]["fdb_data"], cfg["remote"]["run_dir"])):
            self.assertIn(f"rm -rf -- {d}", cmd)
            self.assertIn(f"cat {d}/.r110-run", cmd)
            self.assertIn(cfg["run_id"], cmd)
        self.assertNotIn("kept", out)

    def test_teardown_command_fails_when_a_directory_is_left(self):
        for answer, rc in (("removed\n", 0), ("absent\n", 0), ("kept\n", 1), ("", 1)):
            c, rec, cfg = self.cluster(answers=[('print("checked %d"', "checked 0\n"), ("rm -rf --", answer)])
            out = c.teardown(os.path.join(self.dir, "evidence"))
            self.assertEqual("kept" in out, rc == 1, answer)
            if rc:
                self.assertEqual(out["kept_dirs"], [cfg["remote"]["fdb_data"], cfg["remote"]["run_dir"]])
                self.assertNotIn("parent", out)
            else:
                self.assertIn("rmdir -- " + os.path.dirname(cfg["remote"]["run_dir"]),
                              " ".join(cmd for _, cmd, _ in rec.commands))
            a = os.path.join(self.dir, "approval.json")
            p = os.path.join(self.dir, "run.json")
            with open(p, "w") as f:
                json.dump(self.cfg, f)
            with open(a, "w") as f:
                json.dump({"config_sha256": hashlib.sha256(open(p, "rb").read()).hexdigest(), "scope": ["teardown"],
                           "window_start": "2027-01-01T00:00:00+00:00", "window_end": "2027-01-02T00:00:00+00:00",
                           "approved_by": "approver"}, f)
            made = self.cluster(answers=[('print("checked %d"', "checked 0\n"), ("rm -rf --", answer)])[0]
            with contextlib.redirect_stdout(io.StringIO()):
                got = driver.main(["teardown", p, "--execute", "--approval", a, "--evidence",
                                   os.path.join(self.dir, "ev")], now=NOW, cluster_factory=lambda cfg: made)
            self.assertEqual(got, rc, answer)

    def test_teardown_keeps_everything_while_a_test_process_runs(self):
        c, rec, _ = self.cluster(answers=[("proc.matches(json.load", "node0.json\n")])
        out = c.teardown(os.path.join(self.dir, "evidence"))
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

    def test_teardown_keeps_everything_when_a_stop_is_not_confirmed(self):
        for verdict in ("not-stopped", "unverified"):
            c, rec, _ = self.cluster(answers=[("ls ", "node0.json\n"),
                                              ("stop_identified", json.dumps({"verdict": verdict}) + "\n"),
                                              ('print("checked %d"', "checked 1\n")])
            out = c.teardown(os.path.join(self.dir, "evidence"))
            self.assertIn("not confirmed", out["kept"], verdict)
            self.assertFalse(any("rm " in cmd for _, cmd, _ in rec.commands), verdict)

    def test_teardown_keeps_everything_when_the_check_fails(self):
        for answer in ([], [('print("checked %d"', "")], [('print("checked %d"', "unreadable node0.json: x\n")]):
            c, rec, _ = self.cluster(answers=answer)
            if answer == []:
                c.runner = lambda *a: (1, "", "Traceback")
            out = c.teardown(os.path.join(self.dir, "evidence"))
            self.assertIn("kept", out, answer)
            self.assertFalse(any("rm " in cmd for _, cmd, _ in rec.commands), answer)

    def test_the_liveness_script_itself(self):
        """remote.ALIVE run locally against identity records."""
        svc = os.path.join(self.dir, "services")
        os.mkdir(svc)
        open(os.path.join(svc, "started.list"), "w").close()

        def check():
            r = subprocess.run([sys.executable, "-I", "-", HERE.rsplit("/", 1)[0], svc], input=remote.ALIVE,
                         capture_output=True, text=True, timeout=60)
            return r.returncode, r.stdout.strip()

        self.assertEqual(check(), (0, "checked 0"))
        me = proc.identity(os.getpid())
        # A record of a process gone: a short-lived child of its own session.
        gone = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(30)"], start_new_session=True)
        rec_gone = proc.identity(gone.pid)
        gone.kill()
        gone.wait()
        with open(os.path.join(svc, "a.json"), "w") as f:
            json.dump(rec_gone, f)
        with open(os.path.join(svc, "started.list"), "a") as f:
            f.write("a\n")
        self.assertEqual(check(), (0, "checked 1"))
        with open(os.path.join(svc, "b.json"), "w") as f:
            json.dump(me, f)
        with open(os.path.join(svc, "started.list"), "a") as f:
            f.write("b\n")
        rc, out = check()
        self.assertEqual(rc, 1)
        self.assertTrue(out.splitlines()[0].startswith("alive b"), out)
        os.remove(os.path.join(svc, "b.json"))  # a started process without its record
        rc, out = check()
        self.assertEqual(rc, 1)
        self.assertTrue(out.startswith("unverified b: no readable record"), out)
        with open(os.path.join(svc, "b.json"), "w") as f:
            f.write("null")
        rc, out = check()
        self.assertEqual(rc, 1)
        self.assertTrue(out.startswith("unverified b"), out)
        os.remove(os.path.join(svc, "started.list"))  # the list itself missing
        self.assertEqual(check()[0], 1)

    def test_a_guards_stop_waits_longer_than_the_guards_own_stop(self):
        """The guard, after a TERM: its grace, its load group's TERM and KILL
        waits (20 + 10 s), the reap (5 s); teardown and stop_load must wait
        longer, or they would kill a guard still confirming its load."""
        from r110 import guard
        self.assertGreater(remote.GUARD_STOP_S, guard.GRACE_S + 20 + 10 + 5 + 60)
        c, rec, cfg = self.cluster(answers=[("ls ", "guard.json\n")])
        waits = []
        c._stop = lambda name, wait: waits.append((name, wait)) or {"verdict": "gone"}
        c.stop_load()
        c.teardown()
        self.assertTrue(waits)
        for name, wait in waits:
            if name.startswith("guard"):
                self.assertEqual(wait, remote.GUARD_STOP_S, name)

    def test_a_stops_ssh_outlasts_the_stop(self):
        c, rec, cfg = self.cluster()
        seen = []
        c.runner = lambda target, command, stdin, timeout: seen.append(timeout) or (0, '{"verdict": "gone"}', "")
        c._stop("guard.json", remote.GUARD_STOP_S)
        self.assertGreater(seen[-1], remote.GUARD_STOP_S * 1.5)
        import subprocess as sp

        def slow(*a):
            raise sp.TimeoutExpired("ssh", 1)

        c.runner = slow
        self.assertEqual(c._stop("guard.json", 10)["verdict"], "unverified")

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

    def test_sub_runs_have_their_own_bucket_and_the_guard_alone_watches(self):
        c, rec, cfg = self.cluster()
        c.start_load("smoke-hb", heartbeat=False)
        c.start_load()
        smoke, main = [cmd for _, cmd, s in rec.commands if s == remote.START]
        load_part = smoke.split(" -- ")[-1]
        self.assertIn("r110-smoke-hb", load_part)
        self.assertNotIn("--heartbeat", load_part.split("guard")[-1].split("load import main")[-1])
        self.assertIn("--heartbeat", smoke.split(" -- ")[0])  # the guard's
        self.assertIn("--bucket r110 ", main + " ")

    def test_the_smoke_stage_needs_the_guards_own_stop(self):
        c, rec, cfg = self.cluster()
        c.setup = lambda: None
        sent = []
        c.start_load = lambda sub=None, **k: sub
        c._send = lambda d, n, step: sent.append((d, n, step["kind"]))
        ops_steps = [{"n": i, "result": {}} for i in range(1, 7)]
        ops_steps[2]["result"] = {"counts": {"op-GET-found": 100}}
        ops_steps[4]["result"] = {"complete": True, "violations": []}
        status = {"smoke-hb": {"exit": "stopped", "stopped": "heartbeat-lost"},
                  "smoke-limit": {"exit": "stopped", "stopped": "attempts-limit"},
                  "smoke-ops": {"exit": "finished", "steps": ops_steps}}
        c._wait_exit = lambda d, s: status[d]
        for guard_rec, want in (({"reason": "heartbeat-lost", "source": "guard", "group_empty": True}, True),
                                ({"reason": "heartbeat-lost", "source": "stop-file", "group_empty": True}, False),
                                (None, False)):
            sent.clear()
            c._json = lambda path, g=guard_rec: g
            res = c.smoke()
            self.assertEqual(res["stop_layers_exercised"], want, guard_rec)
            self.assertEqual(res["errors"], 0)
            firsts = {d: k for d, n, k in sent if n == 1}
            self.assertEqual(firsts, {"smoke-hb": "create-bucket", "smoke-limit": "create-bucket",
                                      "smoke-ops": "create-bucket"})

    def test_the_run_wide_limits_are_shared_by_the_smoke_stage_and_the_run(self):
        """F6: the approved totals, not the plan's defaults, are divided."""
        from r110 import budget
        smoke = budget.SMOKE
        for approved in ({}, {"max_attempts": 2000, "max_body_bytes": 1 << 30}):
            alloc = budget.allocate(approved)
            for k in ("max_attempts", "max_body_bytes"):
                total = dict(budget.DEFAULTS, **approved)[k]
                self.assertEqual(sum(alloc[n][k] for n in alloc), total, (approved, k))
            self.assertEqual(alloc["main"]["rate_per_s"], 20)
        self.assertEqual(budget.allocate({})["main"]["max_attempts"], 70_000 - sum(v["max_attempts"] for v in smoke.values()))
        for too_tight in ({"max_attempts": 1020}, {"max_body_bytes": 200 << 20}):
            with self.assertRaises(ValueError):
                budget.allocate(too_tight)
            cfg = copy.deepcopy(self.cfg)
            cfg["limits"] = dict(too_tight)
            with self.assertRaises(driver.ConfigError):
                driver.validate(cfg)

    def test_each_load_gets_its_share_and_the_runs_remaining_time(self):
        c, rec, cfg = self.cluster()
        c.cfg["limits"] = dict(c.cfg["limits"], max_attempts=5000)
        c.start_load(remaining_s=3600)
        c.start_load("smoke-ops", remaining_s=4000)
        main, smoke = [cmd for _, cmd, s in rec.commands if s == remote.START]

        def limits(cmd):
            # START's command, its guard's, then the load's (each quoted once).
            guard = shlex.split(shlex.split(cmd)[-1])
            load = shlex.split(guard[-1])
            return json.loads(load[load.index("--limits") + 1])

        lim_main, lim_smoke = limits(main), limits(smoke)
        self.assertEqual(lim_main["max_attempts"], 5000 - 1020)
        self.assertEqual(lim_smoke["max_attempts"], 600)
        self.assertEqual(lim_main["run_seconds"], 3600)
        self.assertIn("--max-seconds 4200", main)

    def test_the_fdb_memory_limit_reaches_the_command_line(self):
        """F5: the approved value on fdbserver's command line; undetermined
        or malformed refused for anything that runs."""
        c, rec, cfg = self.cluster()
        c.start_sampler = lambda: None
        c.cluster_services()
        fdb = [cmd for _, cmd, s in rec.commands if s == remote.START and "fdbserver -p" in cmd]
        self.assertEqual(len(fdb), 1, [cmd for _, cmd, _ in rec.commands][:5])
        self.assertIn("--memory 3GiB", fdb[0])
        self.assertNotIn("--cache-memory", fdb[0])
        c.cfg["fdb_cache_memory"] = "1GiB"
        rec.commands.clear()
        c.cluster_services()
        fdb = [cmd for _, cmd, s in rec.commands if s == remote.START and "fdbserver -p" in cmd]
        self.assertIn("--memory 3GiB --cache-memory 1GiB", fdb[0])
        # Not above the cache memory (the default 2 GiB, or the one given):
        # fdbserver would not start.
        for mem, cache in (("2GiB", None), ("1GiB", "1GiB"), ("3GiB", "4GiB")):
            cfg2 = copy.deepcopy(self.cfg)
            cfg2.update(fdb_memory=mem, fdb_cache_memory=cache)
            with self.assertRaises(driver.ConfigError, msg=(mem, cache)):
                driver.validate(cfg2)
        for bad in ("2G", "0GiB", "-1GiB", "2 GiB", 2, "nan"):
            cfg2 = copy.deepcopy(self.cfg)
            cfg2["fdb_memory"] = bad
            with self.assertRaises(driver.ConfigError, msg=bad):
                driver.validate(cfg2)
        undetermined = copy.deepcopy(self.cfg)
        undetermined["fdb_memory"] = None
        driver.validate(copy.deepcopy(undetermined))  # check: accepted
        with self.assertRaises(driver.ConfigError):
            driver.validate(undetermined, execute=True)
        p, sha = self.write(undetermined)
        with NoContact():
            # Chosen at the preflight: a preflight runs without it, a run never.
            self.assertEqual(driver.main(["preflight", p, "--execute", "--approval", self.approval(sha, ("preflight",))],
                                         now=NOW, cluster_factory=lambda cfg: FakeCluster(cfg)), 0)
            self.assertEqual(driver.main(["run", p, "--execute", "--evidence", self.dir,
                                          "--approval", self.approval(sha, ("run",))],
                                         now=NOW, cluster_factory=lambda cfg: FakeCluster(cfg)), 2)
            self.assertEqual(driver.main(["check", p]), 0)

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
