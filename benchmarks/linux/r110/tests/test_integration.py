"""The real tools together (no host): the driver's stage code, the control
files, a real ``load.py`` process and a local fake S3. Only the metrics
samples are synthesised (``local.LocalCluster``); every request, the budget,
the ledger and the control directory are the real ones."""

import argparse
import copy
import json
import os
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from r110 import common, driver, guard, judge, ledger, proc, watchdog
from r110.tests import fakes, local
from r110.tests.test_driver import EXAMPLE

FAST = {"rate_per_s": 20, "burst": 5}


def quiet():
    """Samples of a cluster at rest: admission normal, nothing pending."""
    return {"gateway0": {"arca_gateway_admission_state": 0},
            "controller": {"arca_controller_gc_backlog": 0, "arca_controller_backpressure_state": 0,
                           "arca_controller_reclaimed_total": 0},
            "node0": {"arca_node_reclaim_pending_records": 0},
            "node1": {"arca_node_reclaim_pending_records": 0}}


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)  # last: after the loads have ended
        self.dir = self.tmp.name
        self.s3 = fakes.FakeS3()
        self.addCleanup(self.s3.close)

    def load(self, limits=FAST, sub="load"):
        ld = local.LocalLoad(os.path.join(self.dir, sub), self.s3.url, limits=limits)
        self.addCleanup(ld.close)
        return ld


class Stage5Concurrency(Base):
    """F1: the probe PUTs of stage 5 run while its deletes run."""

    def engine(self, ld):
        with open(EXAMPLE) as f:
            cfg = driver.validate(json.load(f))
        cfg = copy.deepcopy(cfg)
        # A small key set (the plan's sizes would take minutes here).
        cfg["settings"].update(l0=40, d1=10, l1=20)
        c = local.LocalCluster(ld, quiet)
        ev = driver.Evidence(os.path.join(self.dir, "evidence"), cfg["run_id"])
        return driver.Run(cfg, c, ev, poll_s=0.1), c

    def short(self):
        """The stage's waits shortened (the logic is the driver's own)."""
        values = dict(judge.VALUES, c3a_hold_s=3, c3a_probe_interval_s=0.2, c2_hold_s=1)
        return [mock.patch.object(driver, "CHAIN_S", 1), mock.patch.object(driver, "SETTLE_S", 0.2),
                mock.patch.dict(judge.VALUES, values)]

    def stage5(self, engine):
        st = copy.deepcopy(driver.stages(engine.cfg)[4])
        st["steps"][0]["interval"] = 0.2
        st["max_s"] = 60
        return st

    def test_probe_puts_continue_while_the_deletes_are_answered(self):
        ld = self.load()
        ld.send({"kind": "create-bucket"})
        n = ld.send({"kind": "put", "keys": [f"b{i:05d}" for i in range(40)]})
        self.assertIsNotNone(ld.result(n))
        self.s3.delete_delay = 0.1  # 10 deletes over about a second
        engine, c = self.engine(ld)
        ps = self.short()
        for p in ps:
            p.start()
        try:
            ok = engine.stage(self.stage5(engine), engine.s5)
        finally:
            for p in ps:
                p.stop()
        recs = ld.records()
        order = [(r["type"], r.get("op"), r.get("kind")) for r in recs]
        ops = {(r["op_id"], r["attempt"]): r["op"] for r in recs if r["type"] == "intent"}
        del_outcomes = [i for i, r in enumerate(recs)
                        if r["type"] == "outcome" and ops[(r["op_id"], r["attempt"])] == "DELETE"]
        probes = [i for i, r in enumerate(recs) if r["type"] == "intent" and r.get("kind") == "probe"]
        self.assertTrue(del_outcomes, order[-20:])
        # Probe PUTs before the first delete's answer and after the last one:
        # the probe step kept running while the deletes were answered.
        self.assertTrue(any(i < del_outcomes[0] for i in probes), "no probe before the deletes")
        self.assertTrue(any(i > del_outcomes[-1] for i in probes), "no probe after the deletes")
        self.assertTrue(any(del_outcomes[0] < i < del_outcomes[-1] for i in probes), "no probe among the deletes")
        # The stage went on to its judgement, and its probes are C3a's.
        self.assertTrue(ok, engine.tl.get("5"))
        self.assertEqual(engine.tl["5"]["c3a"], judge.PASSED)
        st = ld.status()
        self.assertEqual(st.get("background"), [], "a background step still runs")

    def test_a_stop_ends_the_probe_and_the_deletes_and_the_load(self):
        ld = self.load()
        ld.send({"kind": "create-bucket"})
        ld.result(ld.send({"kind": "put", "keys": [f"k{i}" for i in range(20)]}))
        self.s3.delete_delay = 0.2
        probe = ld.send({"kind": "probe", "prefix": "q", "put": True, "interval": 0.1, "background": True,
                         "max_seconds": 60})
        dels = ld.send({"kind": "delete", "keys": [f"k{i}" for i in range(20)], "max_seconds": 60})
        time.sleep(1.0)
        ld.stop("disk-free")
        ld.proc.wait(30)
        st = ld.status()
        self.assertEqual((st["exit"], st["stopped"], st["background_left"]), ("stopped", "disk-free", []))
        done = {s["n"] for s in st["steps"]}
        self.assertTrue({probe, dels} <= done, st["steps"])
        # The process is gone, and nothing more reaches the server.
        n = len(self.s3.requests)
        time.sleep(0.5)
        self.assertEqual(len(self.s3.requests), n)
        self.assertLess(len([d for d in ld.records() if d.get("op") == "DELETE"]), 2 * 20)


class BaselineKey(Base):
    """F2: the baseline probe's object is never deleted by the test."""

    def churn(self, base, extra_put):
        ld = self.load(limits={"rate_per_s": 20, "burst": 5})
        ld.send({"kind": "create-bucket"})
        keys = [f"b{i:05d}" for i in range(10)]
        ld.result(ld.send({"kind": "put", "keys": keys}))
        if extra_put:
            ld.result(ld.send({"kind": "put", "keys": [base]}))
        ld.result(ld.send({"kind": "start-probe", "key": base, "interval": 0.2}))
        # 1 PUT + 1 DELETE per iteration at 20 attempts/s: about 30 deletes,
        # three times round the 10 baseline keys and the churn's own.
        out = ld.result(ld.send({"kind": "churn", "prefix": "c", "seconds": 3, "rate": 10, "min_live": 3}), 60)
        ld.result(ld.send({"kind": "delete", "keys": keys + [base]}))
        recs = ld.records()
        intents = {(r["op_id"], r["attempt"]): r for r in recs if r["type"] == "intent"}
        deleted = {intents[(r["op_id"], r["attempt"])]["key"] for r in recs
                   if r["type"] == "outcome" and r["result"] == "ack"
                   and intents[(r["op_id"], r["attempt"])]["op"] == "DELETE"}
        self.assertTrue(set(keys) - {base} <= deleted, "the churn did not go round the key set")
        self.assertNotIn(base, deleted)
        self.assertIn(base, self.s3.objects)
        gets = [r for k, r in intents.items() if r["key"] == base and r["op"] == "GET"]
        results = {(r["op_id"], r["attempt"]): r["result"] for r in recs if r["type"] == "outcome"}
        self.assertGreater(len(gets), 5)
        self.assertEqual({results.get((g["op_id"], g["attempt"])) for g in gets} - {None}, {"found"})
        self.assertIsNone(ld.status().get("stopped"))
        self.assertGreater(out["iterations"], 10)
        return intents

    def test_churn_through_the_whole_key_set_keeps_the_probed_key(self):
        """The key the probe reads is one of the fill (as the driver chose
        before): the probe protects it from the churn and the deletes."""
        self.churn("b00009", extra_put=False)

    def test_the_dedicated_baseline_object_is_put_like_any_other(self):
        intents = self.churn("base-probe", extra_put=True)
        put = [r for r in intents.values() if r["key"] == "base-probe" and r["op"] == "PUT"]
        # An attempt of the budget, a body in the ledger.
        self.assertEqual(len(put), 1)
        self.assertIsNotNone(put[0].get("budget_attempt"))
        self.assertIsNotNone(put[0].get("sha256"))

    def test_the_stage_definitions_never_delete_the_baseline_key(self):
        with open(EXAMPLE) as f:
            cfg = driver.validate(json.load(f))
        for st in driver.stages(cfg):
            for step in st["steps"]:
                if step["kind"] == "delete" and isinstance(step.get("keys"), list):
                    self.assertNotIn(driver.BASE_KEY, step["keys"])
        put_base = [s for s in driver.stages(cfg)[2]["steps"] if s.get("keys") == [driver.BASE_KEY]]
        self.assertEqual(len(put_base), 1)


# -- F4: the watching never stops before the services do ------------------------


SLEEPER = [sys.executable, "-I", "-c", "import time; time.sleep(300)"]


class Watching(unittest.TestCase):
    """The sentinel, the watchdog and the guards together, with real
    processes standing for the services (each its own session, as
    ``remote.START`` starts them), simulated readings and the watchdog's
    remote writes executed here. Every process is the test's own and is
    killed at the end whatever happened."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = self.tmp.name
        for d in ("services", "alarms", "load", "operator"):
            os.makedirs(os.path.join(self.dir, d))
        self.procs = []
        self.addCleanup(self.kill_all)

    def kill_all(self):
        for p in self.procs:
            if p.poll() is None:
                p.kill()
            p.wait()

    def service(self, name, record=True):
        p = subprocess.Popen(SLEEPER, cwd=self.dir, start_new_session=True)
        self.procs.append(p)
        for _ in range(100):
            ident = proc.identity(p.pid)
            if ident:
                break
            time.sleep(0.05)
        if record:
            with open(os.path.join(self.dir, "services", name + ".json"), "w") as f:
                json.dump(ident, f)
        return p

    def beat(self, seq):
        common.atomic_write_json(os.path.join(self.dir, "hb.json"), {"run_id": "run-w", "seq": seq})

    def sentinel(self, mem=lambda: 8 << 30, free=lambda _p: 100 << 30, timeout=30.0):
        args = argparse.Namespace(run_dir=self.dir, heartbeat=os.path.join(self.dir, "hb.json"), heartbeat_id="run-w",
                                  heartbeat_timeout=timeout, data_dir=self.dir, min_free_gib=3.0, min_mem_mib=1024.0,
                                  max_seconds=120.0, interval=0.1, services=os.path.join(self.dir, "services"),
                                  alarms=os.path.join(self.dir, "alarms"),
                                  stop_file=os.path.join(self.dir, "load", "stop.json"))
        s = guard.Sentinel(args, checks={"mem": mem, "free": free})
        out = {}
        t = threading.Thread(target=lambda: out.setdefault("rc", s.run()), daemon=True)
        t.start()
        return s, t, out

    def result(self, t):
        t.join(90)
        self.assertFalse(t.is_alive(), "the sentinel did not end")
        return common.read_json(os.path.join(self.dir, "sentinel.json"))

    def local_runner(self, readings):
        """The watchdog's ssh: readings for the check, the write scripts run
        here for real (heartbeat, stop file, alarms)."""
        def run(target, script, argv):
            if script == watchdog.CHECK_SCRIPT:
                return 0, json.dumps(readings[target]) + "\n"
            r = subprocess.run([sys.executable, "-I", "-"] + list(argv), input=script, capture_output=True,
                               text=True, timeout=30)
            return r.returncode, r.stdout
        return run

    def test_an_anomaly_then_low_memory_still_stops_the_services(self):
        node = self.service("node0")
        unrelated = self.service("x", record=False)
        readings = {"t2": {"mem_available": 6 << 30, "cpus": 4, "load1": 1.0, "free_root": 20 << 30,
                           "free_data": 20 << 30, "swap_pages": 0, "cpu_iowait_total": [1, 100], "kernel_alerts": [],
                           "test_fdb_available": False}}
        cfg = {"run_id": "run-w", "hosts": {"h2": "t2"}, "load_host": "h2", "remote_run_dir": os.path.join(self.dir, "load"),
               "remote_data_mount": self.dir, "remote_heartbeat": os.path.join(self.dir, "hb.json"),
               "remote_alarms": os.path.join(self.dir, "alarms"), "remote_test_dirs": [self.dir],
               "remote_test_fdb_cluster": "/x/fdb.cluster", "local_dir": os.path.join(self.dir, "operator")}
        w = watchdog.Watchdog(cfg, self.local_runner(readings), common.FakeClock())
        self.beat(1)
        s, t, out = self.sentinel()
        # The test's FoundationDB down for 60 s: the load stops, not the services.
        self.assertEqual(w.run(max_rounds=8), 0)
        first = common.StopFile(os.path.join(self.dir, "load", "stop.json")).reason()
        self.assertEqual(first["reason"], "test-fdb")
        time.sleep(0.5)
        self.assertTrue(t.is_alive(), "the sentinel acted on an anomaly")
        self.assertIsNone(node.poll())
        # Then memory runs low: an alarm, whatever stop came first.
        readings["t2"]["mem_available"] = 512 << 20
        self.assertEqual(w.run(max_rounds=3), 2)
        res = self.result(t)
        self.assertEqual(res["reason"], "memory")
        self.assertEqual(res["services"]["node0"]["verdict"], "stopped")
        self.assertTrue(res["services_confirmed"])
        node.wait(10)
        self.assertIsNone(unrelated.poll(), "a process not of the run was stopped")
        # The first reason is still the run's stop reason.
        self.assertEqual(common.StopFile(os.path.join(self.dir, "load", "stop.json")).reason()["reason"], "test-fdb")
        self.assertTrue(os.listdir(os.path.join(self.dir, "operator", "alarms")))

    def test_a_real_resource_danger_during_the_smoke_stage_stops_the_services(self):
        node = self.service("node0")
        self.beat(1)
        state = {"mem": 8 << 30}
        s, t, out = self.sentinel(mem=lambda: state["mem"])
        time.sleep(0.3)
        self.assertTrue(t.is_alive())
        state["mem"] = 256 << 20
        res = self.result(t)
        self.assertEqual((res["reason"], res["services"]["node0"]["verdict"]), ("memory", "stopped"))
        node.wait(10)

    def test_the_smoke_stages_exercise_touches_only_its_own_load(self):
        node = self.service("node0")
        unrelated = self.service("x", record=False)
        self.beat(1)
        s, t, out = self.sentinel()
        sub = os.path.join(self.dir, "smoke-hb")
        os.makedirs(sub)
        with open(os.path.join(sub, "run.json"), "w") as f:
            json.dump({"run_id": "run-w-smoke-hb"}, f)
        ns = argparse.Namespace(run_dir=sub, heartbeat=os.path.join(sub, "no-heartbeat.json"), heartbeat_id="run-w",
                                heartbeat_timeout=0.5, data_dir=self.dir, min_free_gib=3.0, min_mem_mib=1024.0,
                                max_seconds=60.0, interval=0.1, services=None)
        g = guard.Guard(ns, checks={"free": lambda _p: 100 << 30, "mem": lambda: 8 << 30})
        rc = g.run(SLEEPER)
        res = common.read_json(os.path.join(sub, "guard.json"))
        self.assertEqual((rc, res["reason"], res["group_empty"]), (3, "heartbeat-lost", True))
        self.beat(2)
        time.sleep(0.5)
        self.assertTrue(t.is_alive(), "the main run's sentinel acted on the exercise")
        self.assertIsNone(node.poll())
        self.assertIsNone(unrelated.poll())
        s.terminate()  # the end of the test: the sentinel stops the services
        self.result(t)

    def test_a_lost_driver_and_watchdog_stop_the_services_from_the_host(self):
        node = self.service("node0")
        child = self.service("node1")
        self.beat(1)
        s, t, out = self.sentinel(timeout=1.0)  # no beat after this one
        res = self.result(t)
        self.assertEqual(res["reason"], "heartbeat-lost")
        self.assertTrue(res["services_confirmed"])
        self.assertEqual({res["services"][n]["verdict"] for n in ("node0", "node1")}, {"stopped"})
        node.wait(10)
        child.wait(10)
        self.assertEqual(common.StopFile(os.path.join(self.dir, "load", "stop.json")).reason()["reason"],
                         "heartbeat-lost")

    def test_a_check_that_cannot_be_made_stops_and_confirms(self):
        node = self.service("node0")
        self.beat(1)

        def broken():
            raise OSError("no /proc/meminfo")

        s, t, out = self.sentinel(mem=broken)
        res = self.result(t)
        self.assertEqual((res["reason"], res["services_confirmed"]), ("monitor-failed", True))
        node.wait(10)


# -- F8: the watchdog's own readings -----------------------------------------------


class WatchdogReadings(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = self.tmp.name

    def check(self, args, path_extra=None):
        env = dict(os.environ)
        if path_extra:
            env["PATH"] = path_extra + os.pathsep + env["PATH"]
        r = subprocess.run([sys.executable, "-I", "-", json.dumps(dict({"data_mount": "/", "detail": True}, **args))],
                           input=watchdog.CHECK_SCRIPT, capture_output=True, text=True, timeout=60, env=env)
        return json.loads(r.stdout)

    def test_an_unreadable_test_directory_is_missing_not_zero(self):
        d = os.path.join(self.dir, "t")
        os.makedirs(os.path.join(d, "locked"))
        with open(os.path.join(d, "locked", "f"), "wb") as f:
            f.write(b"x" * 4096)
        self.assertGreater(self.check({"test_dirs": [d]})["test_dir_bytes"], 0)
        os.chmod(os.path.join(d, "locked"), 0)
        self.addCleanup(os.chmod, os.path.join(d, "locked"), 0o755)
        if os.geteuid() == 0:
            self.skipTest("root reads any directory")
        out = self.check({"test_dirs": [d]})
        self.assertIsNone(out["test_dir_bytes"])
        self.assertIn("test_dir_bytes", out["errors"])

    def test_a_missing_test_directory_is_missing(self):
        out = self.check({"test_dirs": [os.path.join(self.dir, "nope")]})
        self.assertIsNone(out["test_dir_bytes"])

    def fake_journalctl(self, text, rc=0):
        b = os.path.join(self.dir, "bin")
        os.makedirs(b, exist_ok=True)
        with open(os.path.join(b, "journalctl"), "w") as f:
            f.write(f"#!/bin/sh\necho {shlex.quote(text)} >&2\nexit {rc}\n")
        os.chmod(os.path.join(b, "journalctl"), 0o755)
        return b

    def test_a_kernel_log_that_cannot_be_read_is_not_no_alert(self):
        for text in ("No journal files were opened due to insufficient permissions.",
                     "Hint: You are currently not seeing messages from other users and the system."):
            out = self.check({}, self.fake_journalctl(text))
            self.assertIsNone(out["kernel_alerts"], text)
        out = self.check({}, self.fake_journalctl("", rc=1))
        self.assertIsNone(out["kernel_alerts"])

    def test_a_readable_empty_kernel_log_is_no_alert(self):
        b = os.path.join(self.dir, "bin2")
        os.makedirs(b)
        with open(os.path.join(b, "journalctl"), "w") as f:
            f.write("#!/bin/sh\nexit 0\n")
        os.chmod(os.path.join(b, "journalctl"), 0o755)
        self.assertEqual(self.check({}, b)["kernel_alerts"], [])

    def test_a_missing_required_reading_holds_the_heartbeat_back(self):
        from r110.tests.test_guard import FakeHosts, healthy
        for missing in ("test_dir_bytes", "kernel_alerts", "swap_pages"):
            readings = {"t1": healthy(), "t2": healthy(load_host=True)}
            readings["t2"][missing] = None
            hosts = FakeHosts(readings)
            cfg = {"run_id": "r", "hosts": {"h1": "t1", "h2": "t2"}, "load_host": "h2", "remote_run_dir": "/r",
                   "remote_data_mount": "/d", "remote_heartbeat": "/r/hb", "remote_test_dirs": ["/r"],
                   "local_dir": os.path.join(self.dir, missing), "limits": {"max_test_dir_bytes": 8 << 30}}
            w = watchdog.Watchdog(cfg, hosts, common.FakeClock())
            self.assertEqual(w.run(max_rounds=3), 2, missing)
            self.assertEqual([a for t, s, a in hosts.writes if s == watchdog.WRITE_HEARTBEAT], [], missing)


if __name__ == "__main__":
    unittest.main()
