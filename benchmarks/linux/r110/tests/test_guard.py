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
from unittest import mock

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
                "--heartbeat-timeout", str(timeout), "--interval", "0.1", "--grace", "0.3",
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

        d = os.path.join(self.dir, "services")
        os.makedirs(d)
        rc, res = self.run_guard(LOAD, timeout=60, during=during, services=d)
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
        # As remote.START starts one: a session (and process group) of its own.
        p = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(300)"], cwd=self.dir,
                             start_new_session=True)
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
        self.assertEqual(res["services"]["node0"]["verdict"], "stopped")
        p.wait(10)
        self.assertIsNotNone(p.returncode)

    def test_a_service_record_of_another_process_is_not_signalled(self):
        p, ident, d = self.service()
        stale = dict(ident, start="not-its-start-time")
        with open(os.path.join(d, "node0.json"), "w") as f:
            json.dump(stale, f)
        self.free = lambda: 1 * GIB
        rc, res = self.run_guard(LOAD, timeout=60, services=d)
        self.assertEqual(res["services"]["node0"]["verdict"], "unverified")
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
    def test_a_load_that_ends_by_itself_after_the_stop_is_not_signalled(self):
        """The grace: a load that reads the stop file ends on its own (and
        writes its status); its group is then confirmed empty."""
        rc, res = self.run_guard(WATCHING, timeout=0.3)
        self.assertEqual(res["reason"], "heartbeat-lost")
        self.assertEqual(res["load_stop"]["signals"], [])
        self.assertEqual(res["load_exit"], 0)
        self.assertTrue(res["group_empty"])

    def test_a_sub_runs_guard_has_no_services_to_stop(self):
        rc, res = self.run_guard(LOAD, timeout=0.3)
        self.assertNotIn("services", res["stopped"])
        self.assertNotIn("services_confirmed", res)

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


# A load that ends as soon as the run's stop file appears (as the real load
# does), before the guard's next check.
WATCHING = [sys.executable, "-I", "-c",
            "import os,time\nopen('child.pid','w').write('0')\n"
            "while not os.path.exists('stop.json'): time.sleep(0.01)\n"]


class ReviewFindings(GuardBase):
    """Cases from the independent review of P2."""

    def test_the_guard_never_signals_itself(self):
        p, ident, d = Resources.service(self)
        with open(os.path.join(d, "node0.json"), "w") as f:
            json.dump(ident, f)
        me = proc.identity(os.getpid())
        with open(os.path.join(d, "aaa.json"), "w") as f:
            json.dump(me, f)  # sorted first: the guard's own record under another name
        with open(os.path.join(d, "guard.json"), "w") as f:
            json.dump(me, f)
        # A sub-run's guard (another process): a guard is never a service.
        other = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(300)"], cwd=self.dir)
        self.extra.append(other)
        for _ in range(50):
            oid = proc.identity(other.pid)
            if oid and oid["cwd"]:
                break
            time.sleep(0.05)
        with open(os.path.join(d, "guard-smoke-hb.json"), "w") as f:
            json.dump(oid, f)
        self.free = lambda: 1 * GIB
        rc, res = self.run_guard(LOAD, timeout=60, services=d)
        self.assertEqual(res["services"]["aaa"], "skipped: the guard itself")
        self.assertNotIn("guard", res["services"])
        self.assertNotIn("guard-smoke-hb", res["services"])
        self.assertIsNone(other.poll(), "a guard was signalled")
        self.assertEqual(res["services"]["node0"]["verdict"], "stopped")
        p.wait(10)

    def test_a_load_that_ended_on_a_stop_still_gets_the_services_stopped(self):
        p, ident, d = Resources.service(self)
        with open(os.path.join(d, "node0.json"), "w") as f:
            json.dump(ident, f)
        self.beat(1)

        def during():
            self.child_pid()
            common.StopFile(os.path.join(self.dir, "stop.json")).stop("disk-free", "watchdog", 1)

        rc, res = self.run_guard(WATCHING, timeout=60, services=d, during=during)
        self.assertEqual((res["reason"], res["source"]), ("disk-free", "stop-file"))
        self.assertEqual(res["services"]["node0"]["verdict"], "stopped")
        p.wait(10)

    def test_a_terminated_guard_stops_the_load_and_reports(self):
        import argparse
        ns = argparse.Namespace(run_dir=self.dir, heartbeat=self.hb, heartbeat_id=None, heartbeat_timeout=60.0,
                                data_dir=self.dir, min_free_gib=3.0, min_mem_mib=1024.0, max_seconds=60.0,
                                interval=0.1, services=None, grace=0.3)
        self.beat(1)
        g = guard.Guard(ns, checks={"free": lambda _p: self.free, "mem": lambda: self.mem})
        out = {}
        t = threading.Thread(target=lambda: out.setdefault("rc", g.run(LOAD)))
        t.start()
        child = self.child_pid()
        g.terminate()
        t.join(60)
        self.assertEqual(out["rc"], 3)
        res = common.read_json(os.path.join(self.dir, "guard.json"))
        self.assertEqual((res["reason"], res["group_empty"]), ("operator", True))
        self.assertFalse(alive(child))

    def test_limits_that_are_not_finite_are_refused(self):
        for extra in (["--min-free-gib", "nan"], ["--heartbeat-timeout", "nan"], ["--min-mem-mib", "inf"],
                      ["--interval", "nan"], ["--max-seconds", "inf"], ["--interval", "30"]):
            with self.assertRaises(SystemExit, msg=extra):
                guard.main(["run", "--run-dir", self.dir, "--heartbeat", self.hb, "--data-dir", self.dir]
                           + extra + ["--", "true"])

    def test_an_identity_record_that_is_not_one_is_refused(self):
        for rec in (None, {}, {"pid": 1}, {"pid": 1, "start": "x", "cwd": None}):
            with self.assertRaises(ValueError):
                proc.matches(rec)


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


# A service (a session of its own) whose child ignores SIGTERM: stopping the
# leader alone would leave the child.
STUBBORN_CHILD = [sys.executable, "-I", "-c",
                  "import subprocess,sys,time\n"
                  "c=subprocess.Popen([sys.executable,'-I','-c','import signal,time\\n"
                  "signal.signal(signal.SIGTERM, signal.SIG_IGN)\\ntime.sleep(300)'])\n"
                  "open('child.pid','w').write(str(c.pid))\n"
                  "time.sleep(300)\n"]


class F3Stopping(GuardBase):
    """Stopping is confirmed, never assumed (F3): real processes, and the
    errors forced where they cannot be made safely."""

    def spawn(self, argv=None):
        p = subprocess.Popen(argv or [sys.executable, "-I", "-c", "import time; time.sleep(300)"], cwd=self.dir,
                             start_new_session=True)
        self.extra.append(p)
        for _ in range(100):
            ident = proc.identity(p.pid)
            if ident:
                return p, ident
            time.sleep(0.05)
        self.fail("no identity")

    def test_a_childs_stubborn_child_is_stopped_and_confirmed(self):
        p, ident = self.spawn(STUBBORN_CHILD)
        child = self.child_pid()
        res = proc.stop_identified(ident, common.Clock(), term_wait=1.0, kill_wait=5.0)
        self.assertEqual(res["verdict"], "stopped", res)
        self.assertEqual([s for s, _ in res["signals"]], ["SIGTERM", "SIGKILL"])
        p.wait(10)
        for _ in range(50):
            if not alive(child):
                break
            time.sleep(0.1)
        self.assertFalse(alive(child))

    def test_eperm_from_killpg_is_not_a_stop(self):
        p, ident = self.spawn()
        with mock.patch.object(proc.os, "killpg", side_effect=PermissionError(1, "Operation not permitted")):
            res = proc.stop_group(ident["pgid"], common.Clock(), term_wait=0.3, kill_wait=0.3)
        self.assertFalse(res["confirmed"])
        self.assertEqual(res["state"], "alive")
        self.assertEqual(res["signals"], [("SIGTERM", "denied"), ("SIGKILL", "denied")])
        self.assertIsNone(p.poll(), "the process was stopped although EPERM was answered")
        self.assertTrue(proc.group_alive(ident["pgid"]))

    def test_a_process_table_that_cannot_be_read_is_unknown_not_empty(self):
        p, ident = self.spawn()
        with mock.patch.object(proc, "members", side_effect=proc.Unknown("forced")):
            self.assertEqual(proc.group_state(ident["pgid"]), "unknown")
            with self.assertRaises(proc.Unknown):
                proc.group_alive(ident["pgid"])
            res = proc.stop_group(ident["pgid"], common.Clock(), term_wait=0.2, kill_wait=0.2)
            self.assertEqual((res["confirmed"], res["state"]), (False, "unknown"))
            v = proc.stop_identified(ident, common.Clock(), term_wait=0.2, kill_wait=0.2)
            self.assertNotIn(v["verdict"], ("stopped", "gone"))

    def test_an_unreadable_working_directory_sends_nothing(self):
        p, ident = self.spawn()
        with mock.patch.object(proc, "cwd_of", side_effect=proc.Unknown("forced: permission denied")):
            self.assertEqual(proc.check(ident), "unknown")
            self.assertEqual(proc.signal_if_same(ident, signal.SIGTERM), "unknown")
            self.assertEqual(proc.stop_identified(ident, common.Clock())["verdict"], "unverified")
            with self.assertRaises(proc.Unknown):
                proc.matches(ident)
        self.assertIsNone(p.poll())

    def test_a_reused_pid_is_never_signalled_and_never_counted_stopped(self):
        p, ident = self.spawn()
        other = dict(ident, start="not-its-start")
        self.assertEqual(proc.check(other), "other")
        v = proc.stop_identified(other, common.Clock(), term_wait=0.2, kill_wait=0.2)
        self.assertEqual(v["verdict"], "unverified")
        self.assertIsNone(p.poll())

    def test_a_gone_process_with_no_group_left_is_gone(self):
        p, ident = self.spawn()
        p.kill()
        p.wait()
        self.assertEqual(proc.stop_identified(ident, common.Clock())["verdict"], "gone")

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux threads")
    def test_a_process_whose_main_thread_exited_is_alive_until_its_threads_end(self):
        """The main thread gone (SYS_exit: that thread only), a worker
        writing on: the process reads Z in its own stat, but is alive."""
        code = ("import ctypes,threading,time\n"
                "def w():\n"
                "    while True:\n"
                "        open('tick','a').write('x'); time.sleep(0.05)\n"
                "threading.Thread(target=w).start()\n"
                "open('child.pid','w').write('0')\n"
                "time.sleep(0.3)\n"
                "ctypes.CDLL(None).syscall(60, 0)\n")
        p, ident = self.spawn([sys.executable, "-I", "-c", code])
        time.sleep(1.0)
        with open(f"/proc/{p.pid}/stat") as f:
            self.assertIn(f.read().rsplit(")", 1)[1].split()[0], ("Z", "X"))
        self.assertEqual(proc.group_state(ident["pgid"]), "alive")
        self.assertEqual(proc.check(ident), "same")
        res = proc.stop_identified(ident, common.Clock(), term_wait=2.0, kill_wait=2.0)
        self.assertEqual(res["verdict"], "stopped", res)
        self.assertTrue(res["signals"], "confirmed without a signal")
        n = os.path.getsize(os.path.join(self.dir, "tick"))
        time.sleep(0.3)
        self.assertEqual(os.path.getsize(os.path.join(self.dir, "tick")), n, "the worker still writes")

    def test_the_guard_records_its_load_for_teardown(self):
        d = os.path.join(self.dir, "services")
        os.makedirs(d)
        open(os.path.join(d, "started.list"), "w").close()
        seen = {}

        def during():
            for _ in range(100):
                if os.path.exists(os.path.join(d, "load-x.json")):
                    break
                time.sleep(0.05)
            seen["rec"] = common.read_json(os.path.join(d, "load-x.json"))
            seen["check"] = proc.check(seen["rec"])

        argv = ["run", "--run-dir", self.dir, "--heartbeat", self.hb, "--data-dir", self.dir,
                "--heartbeat-timeout", "1", "--interval", "0.1", "--grace", "0.3", "--record-dir", d,
                "--record-name", "load-x", "--", *LOAD]
        out = {}
        t = threading.Thread(target=lambda: out.setdefault("rc", guard.main(
            argv, checks={"free": lambda _p: 100 << 30, "mem": lambda: 8 << 30})))
        t.start()
        during()
        t.join(60)
        self.assertEqual(open(os.path.join(d, "started.list")).read().split(), ["load-x"])
        self.assertEqual(seen["check"], "same")
        self.assertIn("argv", seen["rec"])
        self.assertEqual(proc.check(seen["rec"]), "gone")

    def test_a_load_record_is_never_replaced(self):
        d = os.path.join(self.dir, "services")
        os.makedirs(d)
        open(os.path.join(d, "started.list"), "w").close()
        with open(os.path.join(d, "load.json"), "w") as f:
            json.dump({"pid": 4242, "start": "s", "cwd": "/", "pgid": 4242}, f)
        argv = ["run", "--run-dir", self.dir, "--heartbeat", self.hb, "--data-dir", self.dir, "--heartbeat-timeout",
                "1", "--interval", "0.1", "--grace", "0.3", "--record-dir", d, "--", *LOAD]
        rc = guard.main(argv, checks={"free": lambda _p: 100 << 30, "mem": lambda: 8 << 30})
        res = common.read_json(os.path.join(self.dir, "guard.json"))
        self.assertEqual(res["load_record"], "identity unavailable: stopped")
        self.assertEqual(common.read_json(os.path.join(d, "load.json"))["pid"], 4242)
        self.assertTrue(res["group_empty"])
        self.assertEqual(rc, 3)

    def test_invalid_records(self):
        for rec in (None, {}, {"pid": True, "start": "1", "cwd": "/"}, {"pid": 1, "start": "1", "cwd": "/"},
                    {"pid": 1234, "start": "1", "cwd": "/"}):  # no process group: not an identity
            self.assertEqual(proc.check(rec), "invalid")
            self.assertEqual(proc.stop_identified(rec, common.Clock())["verdict"], "unverified")

    def test_the_guard_reaps_its_load_only_after_the_group_is_stopped(self):
        order = []
        real = proc.stop_group

        def stop_group(pgid, clock, *a, **k):
            # The load (the leader) must still be unreaped here: its zombie
            # or itself is still a member, so the id is reserved.
            order.append(("stop", any(pid == pgid for pid, _ in proc.members(pgid))))
            return real(pgid, clock, *a, **k)

        with mock.patch.object(proc, "stop_group", stop_group):
            rc, res = self.run_guard(LOAD, timeout=0.5)
        self.assertEqual(order, [("stop", True)])
        self.assertTrue(res["group_empty"])
        self.assertFalse(alive(self.child_pid()))

    def test_the_guard_reports_a_group_it_could_not_confirm(self):
        with mock.patch.object(proc.os, "killpg", side_effect=PermissionError(1, "denied")):
            rc, res = self.run_guard(LOAD, timeout=0.3)
        self.assertEqual(rc, 3)
        self.assertFalse(res["group_empty"])
        self.assertFalse(res["load_stop"]["confirmed"])
        # The test's own cleanup: the load and its child, by their pids.
        for pid in (res["load"]["pid"], self.child_pid()):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


FAKE_SSH = """#!/bin/sh
# Stands for OpenSSH: options, the target, then the remaining words joined
# with spaces and given to the remote shell.
while [ $# -gt 0 ]; do
  case "$1" in -o) shift 2 ;; -*) shift ;; *) shift; break ;; esac
done
exec sh -c "$*"
"""


class RealSsh(unittest.TestCase):
    """The watchdog's real ssh_runner, through a stand-in ``ssh`` that
    joins and re-splits the words as OpenSSH and the remote shell do."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        bindir = os.path.join(self.tmp.name, "bin")
        os.mkdir(bindir)
        with open(os.path.join(bindir, "ssh"), "w") as f:
            f.write(FAKE_SSH)
        os.chmod(os.path.join(bindir, "ssh"), 0o755)
        self.env = mock.patch.dict(os.environ, {"PATH": bindir + os.pathsep + os.environ["PATH"]})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def test_the_check_script_gets_its_arguments(self):
        rc, out = watchdog.ssh_runner("t", watchdog.CHECK_SCRIPT,
                                      [json.dumps({"data_mount": "/", "detail": False, "x": "a b 'c'"})])
        self.assertEqual(rc, 0, out)
        self.assertIsNotNone(json.loads(out)["free_root"])

    def test_the_heartbeat_and_the_stop_file_are_written(self):
        hb = os.path.join(self.tmp.name, "hb dir", "hb.json")
        os.mkdir(os.path.dirname(hb))
        rec = json.dumps({"run_id": "r", "seq": 7})
        rc, out = watchdog.ssh_runner("t", watchdog.WRITE_HEARTBEAT, [hb, rec])
        self.assertEqual((rc, json.load(open(hb))), (0, {"run_id": "r", "seq": 7}), out)
        stop = os.path.join(self.tmp.name, "stop.json")
        rc, out = watchdog.ssh_runner("t", watchdog.WRITE_STOP, [stop, json.dumps({"reason": "memory"})])
        self.assertEqual((rc, out.strip(), json.load(open(stop))["reason"]), (0, "written", "memory"))
        rc, out = watchdog.ssh_runner("t", watchdog.WRITE_STOP, [stop, json.dumps({"reason": "load"})])
        self.assertEqual((out.strip(), json.load(open(stop))["reason"]), ("exists", "memory"))


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
        # An anomaly: the load stops, the watching goes on (and beats: the
        # resources are fine), no alarm for the services.
        self.assertEqual(w.run(max_rounds=20), 0)
        self.assertEqual(self.stops(), ["test-fdb"])
        self.assertEqual(len(self.beats()), 20)
        self.assertFalse(os.path.isdir(w.local_alarms) and os.listdir(w.local_alarms))

    def test_the_watchdog_never_kills_and_refuses_looser_limits(self):
        self.wd().run(max_rounds=2)
        for s in self.hosts.scripts:
            for word in ("os.kill", "killpg", "pkill", "kill -", "signal."):
                self.assertNotIn(word, s)
        with self.assertRaises(ValueError):
            self.wd(limits={"min_free_gib": 2})

    def test_a_lost_sentinel_is_a_monitor_stop(self):
        self.hosts.readings["t2"]["sentinel"] = "gone"
        self.wd(remote_sentinel_record="/r/services/sentinel.json").run(max_rounds=2)
        self.assertEqual(self.stops(), ["sentinel-lost"])
        self.setUp()
        self.hosts.readings["t2"]["sentinel"] = None
        self.wd(remote_sentinel_record="/r/services/sentinel.json").run(max_rounds=2)
        self.assertEqual(self.stops(), ["monitor-failed"])

    def test_a_lost_guard_is_a_monitor_stop(self):
        for state, want in (("gone", ["guard-lost"]), (None, ["monitor-failed"])):
            self.setUp()
            self.hosts.readings["t2"]["guard"] = state
            self.wd(remote_guard_record="/r/services/guard.json").run(max_rounds=2)
            self.assertEqual(self.stops(), want, state)
            self.tearDown()
        for state in ("alive", "ended", "absent"):
            self.setUp()
            self.hosts.readings["t2"]["guard"] = state
            self.assertEqual(self.wd(remote_guard_record="/r/services/guard.json").run(max_rounds=2), 0, state)
            self.tearDown()
        self.setUp()
        self.assertEqual(common.reason_class("guard-lost"), "monitor")

    def test_limits_that_are_not_finite_are_refused(self):
        for bad in (float("nan"), float("inf"), -1):
            with self.assertRaises(ValueError):
                self.wd(limits={"min_free_gib": bad})

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
