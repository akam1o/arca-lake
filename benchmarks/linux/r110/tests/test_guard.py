"""P2: the on-host guard and the watchdog (plan §6.2).

Real child processes in a temporary directory (sleeping Python processes
standing for the load and its children); resource readings are simulated,
nothing is exhausted. No host is contacted: the watchdog's ssh is a fake.
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from r110 import common, guard, proc, watchdog

GIB = 1 << 30

# A "load" that starts a child of its own and sleeps (both in its group).
LOAD = [sys.executable, "-I", "-c",
        "import subprocess,sys,time\n"
        "c=subprocess.Popen([sys.executable,'-I','-c','import time; time.sleep(300)'])\n"
        "open('child.pid','w').write(str(c.pid))\n"
        "time.sleep(300)\n"]
# A load that ignores SIGTERM.
STUBBORN = [sys.executable, "-I", "-c",
            "import signal,time\nsignal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "open('child.pid','w').write('0')\ntime.sleep(300)\n"]
# A load that leaves a child behind and exits at once.
ORPHANING = [sys.executable, "-I", "-c",
             "import subprocess,sys\n"
             "c=subprocess.Popen([sys.executable,'-I','-c','import time; time.sleep(300)'])\n"
             "open('child.pid','w').write(str(c.pid))\n"]


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A zombie is not alive.
    try:
        r = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True)
        return bool(r.stdout.strip()) and not r.stdout.strip().startswith("Z")
    except OSError:
        return True


class GuardBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name
        with open(os.path.join(self.dir, "run.json"), "w") as f:
            json.dump({"run_id": "run-g"}, f)
        self.hb = os.path.join(self.dir, "heartbeat.json")
        self.free = 100 * GIB
        self.mem = 8 * GIB
        self.extra = []

    def tearDown(self):
        for p in self.extra:
            try:
                p.kill()
                p.wait()
            except OSError:
                pass
        self.tmp.cleanup()

    def beat(self, seq, run_id="run-g"):
        with open(self.hb + ".t", "w") as f:
            json.dump({"run_id": run_id, "seq": seq}, f)
        os.replace(self.hb + ".t", self.hb)

    def run_guard(self, load, timeout=2.0, services=None, during=None, max_seconds=60):
        checks = {"free": lambda _p: self.free if not callable(self.free) else self.free(),
                  "mem": (lambda: self.mem) if not callable(self.mem) else self.mem}
        argv = ["run", "--run-dir", self.dir, "--heartbeat", self.hb, "--data-dir", self.dir,
                "--heartbeat-timeout", str(timeout), "--interval", "0.1",
                "--max-seconds", str(max_seconds)]
        if services:
            argv += ["--services", services]
        argv += ["--"] + load
        out = {}
        t = threading.Thread(target=lambda: out.setdefault("rc", guard.main(argv, checks=checks)))
        t.start()
        if during:
            during()
        t.join(90)
        self.assertFalse(t.is_alive(), "the guard did not end")
        return out["rc"], common.read_json(os.path.join(self.dir, "guard.json"))

    def child_pid(self):
        for _ in range(100):
            try:
                return int(open(os.path.join(self.dir, "child.pid")).read())
            except (OSError, ValueError):
                time.sleep(0.05)
        self.fail("the load did not start its child")


class Heartbeats(GuardBase):
    def test_a_lost_heartbeat_stops_the_load_and_its_children_without_network(self):
        rc, res = self.run_guard(LOAD, timeout=1.0)
        self.assertEqual((rc, res["reason"], res["group_empty"]), (3, "heartbeat-lost", True))
        self.assertFalse(alive(self.child_pid()))
        self.assertEqual(common.StopFile(os.path.join(self.dir, "stop.json")).reason()["reason"],
                         "heartbeat-lost")

    def test_only_a_higher_sequence_of_this_run_extends_the_heartbeat(self):
        clock = common.FakeClock()
        h = common.Heartbeat(self.hb, "run-g", 10, clock)
        self.beat(5)
        self.assertIsNone(h.check())
        clock.advance(8)
        self.beat(5)  # the same beat again
        self.assertIsNone(h.check())
        clock.advance(3)
        self.assertEqual(h.check(), "heartbeat-lost", "a repeated seq extended it")
        self.beat(4)  # an older file
        self.assertEqual(h.check(), "heartbeat-lost")
        self.beat(9, run_id="other-run")
        self.assertEqual(h.check(), "heartbeat-lost")
        with open(self.hb, "w") as f:
            f.write('{"run_id": "run-g", "se')  # partial
        self.assertEqual(h.check(), "heartbeat-lost")
        self.beat(6)
        self.assertIsNone(h.check())

    def test_without_a_first_heartbeat_the_load_stops_after_the_timeout(self):
        clock = common.FakeClock()
        h = common.Heartbeat(self.hb, "run-g", 10, clock)
        self.assertIsNone(h.check())
        clock.advance(10)
        self.assertEqual(h.check(), "heartbeat-lost")

    def test_the_watchdogs_stop_file_stops_the_load(self):
        def during():
            self.child_pid()
            common.StopFile(os.path.join(self.dir, "stop.json")).stop("memory", "watchdog", 1)

        rc, res = self.run_guard(LOAD, timeout=60, during=during)
        self.assertEqual((res["reason"], res["source"], res["group_empty"]), ("memory", "stop-file", True))
        self.assertIn("services", res["stopped"])

    def test_looser_limits_than_the_plans_are_refused(self):
        for extra in (["--heartbeat-timeout", "121"], ["--min-free-gib", "2"], ["--min-mem-mib", "512"]):
            with self.assertRaises(SystemExit):
                guard.main(["run", "--run-dir", self.dir, "--heartbeat", self.hb, "--data-dir", self.dir]
                           + extra + ["--", "true"])


class Resources(GuardBase):
    def service(self):
        """A test service and its identity record."""
        p = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(300)"], cwd=self.dir)
        self.extra.append(p)
        for _ in range(50):
            ident = proc.identity(p.pid)
            if ident and ident["cwd"]:
                break
            time.sleep(0.05)
        d = os.path.join(self.dir, "services")
        os.makedirs(d, exist_ok=True)
        return p, ident, d

    def test_low_free_space_stops_the_load_and_the_test_services(self):
        p, ident, d = self.service()
        with open(os.path.join(d, "node0.json"), "w") as f:
            json.dump(ident, f)
        self.beat(1)
        self.free = lambda: 2 * GIB
        rc, res = self.run_guard(LOAD, timeout=60, services=d)
        self.assertEqual(res["reason"], "disk-free")
        self.assertEqual(res["services"]["node0"], "signalled")
        p.wait(10)
        self.assertIsNotNone(p.returncode)

    def test_a_service_record_of_another_process_is_not_signalled(self):
        p, ident, d = self.service()
        stale = dict(ident, start="not-its-start-time")
        with open(os.path.join(d, "node0.json"), "w") as f:
            json.dump(stale, f)
        self.free = lambda: 1 * GIB
        rc, res = self.run_guard(LOAD, timeout=60, services=d)
        self.assertEqual(res["services"]["node0"], "other")
        self.assertIsNone(p.poll(), "a process not ours was signalled")

    def test_a_check_that_cannot_be_made_is_a_breach(self):
        def broken():
            raise OSError("no /proc/meminfo")

        self.mem = broken
        rc, res = self.run_guard(LOAD, timeout=60)
        self.assertEqual(res["reason"], "monitor-failed")

    def test_memory(self):
        self.mem = lambda: 512 << 20
        rc, res = self.run_guard(LOAD, timeout=60)
        self.assertEqual(res["reason"], "memory")

    def test_several_reasons_at_once_keep_the_first(self):
        common.StopFile(os.path.join(self.dir, "stop.json")).stop("integrity-violation", "load", 1)
        self.free = lambda: 1 * GIB
        rc, res = self.run_guard(LOAD, timeout=0.5)
        self.assertEqual(res["reason"], "integrity-violation")
        self.assertEqual(common.StopFile(os.path.join(self.dir, "stop.json")).reason()["reason"],
                         "integrity-violation")
        self.assertNotIn("services", res["stopped"], "an integrity stop keeps the services for evidence")


class Groups(GuardBase):
    def test_a_load_ignoring_term_is_killed(self):
        rc, res = self.run_guard(STUBBORN, timeout=0.5)
        self.assertTrue(res["group_empty"])

    def test_children_left_by_a_load_that_ended_are_stopped(self):
        self.beat(1)
        rc, res = self.run_guard(ORPHANING, timeout=60)
        self.assertIsNone(res["reason"])
        self.assertTrue(res["group_empty"])
        self.assertFalse(alive(self.child_pid()))

    def test_the_guards_deadline_stops_the_load(self):
        self.beat(1)
        rc, res = self.run_guard(LOAD, timeout=60, max_seconds=1)
        self.assertEqual(res["reason"], "run-deadline")

    def test_a_reused_pid_is_not_signalled(self):
        p = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(300)"])
        self.extra.append(p)
        ident = proc.identity(p.pid)
        self.assertEqual(proc.signal_if_same(dict(ident, start="other"), signal.SIGTERM), "other")
        self.assertIsNone(p.poll())
        p.kill()
        p.wait()
        self.assertEqual(proc.signal_if_same(ident, signal.SIGTERM), "gone")


class FakeHosts:
    """The watchdog's ssh: readings per host, scripted unreachable rounds,
    and every remote write recorded."""

    def __init__(self, readings):
        self.readings = readings
        self.down = {}
        self.writes = []
        self.scripts = []

    def __call__(self, target, script, args):
        self.scripts.append(script)
        if self.down.get(target, 0) > 0:
            self.down[target] -= 1
            return 255, ""
        if script == watchdog.CHECK_SCRIPT:
            return 0, json.dumps(self.readings[target]) + "\n"
        self.writes.append((target, script, args))
        return 0, "written\n"


def healthy(load_host=False, system=False):
    r = {"mem_available": 6 * GIB, "cpus": 4, "load1": 1.0, "free_root": 20 * GIB, "free_data": 20 * GIB}
    if load_host:
        r.update(swap_pages=100, cpu_iowait_total=[10, 1000], kernel_alerts=[], test_dir_bytes=GIB,
                 test_fdb_available=True)
    if system:
        r["system_fdb_available"] = True
    return r


class Watchdogs(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = {"run_id": "run-w", "hosts": {"h1": "t1", "h2": "t2", "h3": "t3"}, "load_host": "h2",
                    "system_fdb_hosts": ["h1", "h3"], "remote_run_dir": "/r", "remote_data_mount": "/d",
                    "remote_heartbeat": "/r/heartbeat.json", "remote_test_dirs": ["/r"],
                    "remote_test_fdb_cluster": "/r/fdb.cluster", "local_dir": self.tmp.name,
                    "limits": {"max_test_dir_bytes": 8 * GIB}}
        self.hosts = FakeHosts({"t1": healthy(system=True), "t2": healthy(load_host=True),
                                "t3": healthy(system=True)})

    def tearDown(self):
        self.tmp.cleanup()

    def wd(self, **kw):
        return watchdog.Watchdog(dict(self.cfg, **kw), self.hosts, common.FakeClock())

    def beats(self):
        return [a for t, s, a in self.hosts.writes if s == watchdog.WRITE_HEARTBEAT]

    def stops(self):
        return [json.loads(a[1])["reason"] for t, s, a in self.hosts.writes if s == watchdog.WRITE_STOP]

    def test_healthy_rounds_beat_with_rising_sequence(self):
        w = self.wd()
        self.assertEqual(w.run(max_rounds=3), 0)
        seqs = [json.loads(a[1])["seq"] for a in self.beats()]
        self.assertEqual(len(seqs), 3)
        self.assertEqual(seqs, sorted(set(seqs)))

    def test_memory_low_writes_the_stop_file_on_the_load_host(self):
        self.hosts.readings["t3"]["mem_available"] = 100 << 20
        self.assertEqual(self.wd().run(max_rounds=5), 2)
        self.assertEqual(self.stops(), ["memory"])
        self.assertEqual(self.hosts.writes[-1][0], "t2")

    def test_a_reading_that_cannot_be_made_stops_instead_of_beating(self):
        self.hosts.readings["t2"]["cpu_iowait_total"] = None
        self.assertEqual(self.wd().run(max_rounds=5), 2)
        self.assertEqual(self.stops(), ["monitor-failed"])
        self.assertEqual(self.beats(), [])

    def test_an_optional_check_may_be_missing(self):
        self.hosts.readings["t2"]["kernel_alerts"] = None
        self.assertEqual(self.wd(optional_checks=["kernel_log"]).run(max_rounds=2), 0)
        self.assertEqual(len(self.beats()), 2)

    def test_an_unreachable_host_does_not_beat_and_stops_after_six_rounds(self):
        self.hosts.down["t1"] = 100
        self.assertEqual(self.wd().run(max_rounds=10), 2)
        self.assertEqual(self.beats(), [])
        self.assertEqual(self.stops(), ["host-unreachable"])

    def test_system_foundationdb_unavailable(self):
        self.hosts.readings["t1"]["system_fdb_available"] = False
        self.wd().run(max_rounds=2)
        self.assertEqual(self.stops(), ["system-fdb"])

    def test_swap_activity_kernel_alerts_and_test_directories(self):
        for change, reason in ((("swap_pages", 101), "swap"), (("kernel_alerts", ["Out of memory: Killed"]), "oom"),
                               (("test_dir_bytes", 9 * GIB), "test-dirs")):
            self.setUp()
            w = self.wd()
            w.round()
            self.hosts.readings["t2"][change[0]] = change[1]
            w.run(max_rounds=2)
            self.assertEqual(self.stops(), [reason], change)
            self.tearDown()
        self.setUp()

    def test_the_test_foundationdb_down_for_a_minute(self):
        self.hosts.readings["t2"]["test_fdb_available"] = False
        w = self.wd()
        self.assertEqual(w.run(max_rounds=20), 2)
        self.assertEqual(self.stops(), ["test-fdb"])

    def test_the_watchdog_never_kills_and_refuses_looser_limits(self):
        self.wd().run(max_rounds=2)
        for s in self.hosts.scripts:
            for word in ("os.kill", "killpg", "pkill", "kill -", "signal."):
                self.assertNotIn(word, s)
        with self.assertRaises(ValueError):
            self.wd(limits={"min_free_gib": 2})

    def test_the_check_script_runs_and_reports_missing_readings_as_null(self):
        r = subprocess.run([sys.executable, "-I", "-", json.dumps({"data_mount": "/", "detail": True,
                                                                    "test_dirs": [self.tmp.name]})],
                           input=watchdog.CHECK_SCRIPT, capture_output=True, text=True, timeout=60)
        out = json.loads(r.stdout)
        self.assertIsNotNone(out["free_root"])
        for k in ("mem_available", "swap_pages", "cpu_iowait_total"):
            self.assertIn(k, out)
            if not sys.platform.startswith("linux"):
                self.assertIsNone(out[k], k)


if __name__ == "__main__":
    unittest.main()
