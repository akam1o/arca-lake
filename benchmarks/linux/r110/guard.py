#!/usr/bin/env python3
"""The on-host guard of the R-110 retest (plan §6.2, layer 2): the parent of
the load process, on the load's host, stopping it without any network.

  guard.py run --run-dir DIR --heartbeat FILE --data-dir PATH
               [--min-free-gib 3] [--min-mem-mib 1024]
               [--heartbeat-timeout 120] [--max-seconds S]
               [--services DIR] -- COMMAND ...

COMMAND (the load) starts in a session of its own: its process group holds
it and anything it starts, and nothing else. Every ``--interval`` (1 s,
monotonic clock) the guard checks, in this order: the run's stop file
(written by the load, the watchdog, the driver or the guard itself); the
heartbeat; the free space of ``--data-dir``; the available memory; its
own deadline. The first breach writes the stop file (the first reason
wins), stops the load's group (TERM, then KILL, until it is empty) and,
for a resource or monitor reason (``common.REASONS``), the run's test
services recorded in ``--services`` (one ``NAME.json`` per process: pid,
start token, working directory, as ``proc.identity`` gives; signalled
only while that identity still holds). A check that cannot be made (a
missing ``/proc/meminfo``, an unreadable file system) is itself a breach
(``monitor-failed``): a guard that cannot see does not let the load run.

The heartbeat is a JSON file ``{"run_id": ..., "seq": N}`` that the
watchdog rewrites when every check of its round passed. It counts only if
it names this run and its ``seq`` is higher than the last one seen; it is
fresh for ``--heartbeat-timeout`` seconds of the guard's monotonic clock
after the guard first saw that ``seq``. File times and wall clocks are not
used: a clock change or a copied old file cannot extend it. Before the
first heartbeat the guard waits the same timeout from its start.

The guard's result (``guard.json`` in the run directory): the reason, what
was stopped, whether the load's group was empty afterwards. Exit code:
the load's when it ended by itself without a breach, else 3.
"""

import argparse
import json
import math
import os
import re
import signal
import subprocess
import sys

from . import common, proc

GIB, MIB = 1 << 30, 1 << 20
KEEP_PREFIXES = ("guard", "sampler", "sentinel")
GRACE_S = 15.0


def free_bytes(path):
    s = os.statvfs(path)
    return s.f_bavail * (s.f_frsize or s.f_bsize)


def mem_available_bytes():
    """``MemAvailable`` of ``/proc/meminfo``; raises where it does not
    exist (a monitor failure, not a pass)."""
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    raise OSError("MemAvailable not in /proc/meminfo")


def stop_services(services_dir, clock):
    """The test's services (plan §6.2: gateway, controller, nodes,
    FoundationDB) by identity, each with its whole process group, until
    stopped or confirmed not stoppable (``proc.stop_identified``). Never the
    guards, the sentinel or the sampler, nor this process or its parent.
    Returns ``(name -> verdict, all confirmed)``; a missing directory is not
    confirmed."""
    out = {}
    if not services_dir or not os.path.isdir(services_dir):
        return out, False
    mine = {os.getpid(), os.getppid()}
    for name in sorted(os.listdir(services_dir), key=lambda n: (n.startswith("fdb"), n)):
        if not name.endswith(".json") or name.startswith(KEEP_PREFIXES):
            continue
        try:
            rec = common.read_json(os.path.join(services_dir, name))
            if not proc.valid(rec):
                raise ValueError(f"not an identity: {rec!r}")
        except (OSError, ValueError) as e:
            out[name[:-5]] = {"verdict": "unverified", "reason": f"unreadable: {e}"}
            continue
        if rec["pid"] in mine:
            out[name[:-5]] = "skipped: the guard itself"
            continue
        out[name[:-5]] = proc.stop_identified(rec, clock)
    verdicts = [v["verdict"] for v in out.values() if isinstance(v, dict)]
    return out, all(v in ("stopped", "gone") for v in verdicts)


class Guard:
    def __init__(self, args, clock=None, checks=None):
        self.args = args
        self.clock = clock or common.Clock()
        with open(os.path.join(args.run_dir, "run.json")) as f:
            self.run_id = json.load(f)["run_id"]
        self.stop_file = common.StopFile(os.path.join(args.run_dir, "stop.json"))
        self.hb = common.Heartbeat(args.heartbeat, args.heartbeat_id or self.run_id, args.heartbeat_timeout,
                                   self.clock)
        self.free = (checks or {}).get("free", free_bytes)
        self.mem = (checks or {}).get("mem", mem_available_bytes)
        self.deadline = self.clock.monotonic() + args.max_seconds
        self.result = {"run_id": self.run_id, "stopped": [], "services": {}}

    def breach(self):
        rec = self.stop_file.reason()
        if rec is not None:
            return rec.get("reason", "stop-file-unreadable"), "stop-file"
        if getattr(self, "terminated", False):
            return "operator", "guard"
        r = self.hb.check()
        if r:
            return r, "guard"
        try:
            if self.free(self.args.data_dir) < self.args.min_free_gib * GIB:
                return "disk-free", "guard"
        except OSError:
            return "monitor-failed", "guard"
        try:
            if self.mem() < self.args.min_mem_mib * MIB:
                return "memory", "guard"
        except (OSError, ValueError):
            return "monitor-failed", "guard"
        if self.clock.monotonic() >= self.deadline:
            return "run-deadline", "guard"
        return None

    def stop_services(self):
        res, confirmed = stop_services(self.args.services, self.clock)
        self.result["services"].update(res)
        self.result["services_confirmed"] = confirmed

    def terminate(self, *_):
        """SIGTERM to the guard: it stops the load's group and writes its
        result like any other stop (never just dies, leaving the load)."""
        self.terminated = True

    def record_load(self, child):
        """The load's identity in the run's records (``--record-dir``), as
        remote.START records a service: listed in started.list before it
        starts (done by the caller), its record written once its identity
        is read. Teardown and the sentinel then see it like any process of
        the run. False if its identity cannot be read."""
        d, name = self.args.record_dir, self.args.record_name
        ident = None
        for _ in range(100):
            try:
                ident = proc.identity(child.pid)
            except proc.Unknown:
                ident = None
            if ident:
                break
            self.clock.sleep(0.05)
        if not ident:
            return False
        ident["argv"] = list(child.args)
        common.atomic_write_json(os.path.join(d, name + ".json"), ident)
        return True

    def run(self, command):
        self.terminated = False
        try:
            signal.signal(signal.SIGTERM, self.terminate)
        except ValueError:
            pass  # not the main thread (tests call terminate() directly)
        if getattr(self.args, "record_dir", None):
            with open(os.path.join(self.args.record_dir, "started.list"), "a") as f:
                f.write(self.args.record_name + "\n")
                f.flush()
                os.fsync(f.fileno())
        child = subprocess.Popen(command, start_new_session=True, cwd=self.args.run_dir)
        if getattr(self.args, "record_dir", None) and not self.record_load(child):
            # Not recordable, so not stoppable by its identity later: stopped now.
            self.terminated = True
            self.result["load_record"] = "identity unavailable: stopped"
        pgid = child.pid
        self.result["load"] = {"pid": child.pid, "command": command}
        reason = source = None
        while True:
            # Exit seen without reaping: the pid (and group id) stays the
            # load's until the guard reaps it below.
            exited = os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG)
            if exited is not None:
                # The load may have ended because of a stop (its own or the
                # watchdog's): the stop file still decides what is stopped.
                rec = self.stop_file.reason()
                if rec is not None:
                    reason, source = rec.get("reason", "stop-file-unreadable"), "stop-file"
                break
            b = self.breach()
            if b:
                reason, source = b
                break
            self.clock.sleep(self.args.interval)
        if reason:
            if source == "guard":
                self.stop_file.stop(reason, "guard", self.clock.wall_ms())
            self.result["stopped"].append("load")
            # The load reads the stop file before each attempt: a short grace
            # lets it end by itself and write its own status, then its
            # group is stopped whatever happened.
            end = self.clock.monotonic() + getattr(self.args, "grace", GRACE_S)
            while self.clock.monotonic() < end and \
                    os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG) is None:
                self.clock.sleep(0.2)
        # The whole group (the load and anything it started), also after the
        # load itself ended: nothing of it may outlive the guard.
        # The load (the group's leader) is reaped only after: until then its
        # id is reserved and the signals can only reach this group.
        stop = proc.stop_group(pgid, self.clock)
        empty = stop["confirmed"]
        self.result["load_stop"] = stop
        try:
            child.wait(timeout=5 if not empty else None)
            self.result["load_reaped"] = True
        except subprocess.TimeoutExpired:
            # Not stoppable from here (the stop is not confirmed): reported,
            # never waited on for ever. The load still sees the stop file
            # and its own heartbeat check.
            self.result["load_reaped"] = False
        if reason and common.reason_class(reason) in ("resource", "monitor") and self.args.services:
            # A sub-run's guard (no services given) has none to stop.
            self.stop_services()
            self.result["stopped"].append("services")
        self.result.update(reason=reason, source=source, load_exit=child.returncode if self.result["load_reaped"] else None,
                           group_empty=empty, wall_ms=self.clock.wall_ms())
        common.atomic_write_json(os.path.join(self.args.run_dir, "guard.json"), self.result)
        if reason or not empty:
            return 3
        return child.returncode


class Sentinel:
    """The run's host-side keeper (plan §6.2), from setup to teardown, apart
    from any load: it watches the data file system's free space, the
    available memory, the watchdog's heartbeat and the watchdog's alarms
    (``ALARMS``: one file per resource or monitor rule that fired, written
    whatever stop came first), and on any of them, on a check it cannot
    make, on its deadline or on SIGTERM, writes the run's stop file (the
    loads stop) and stops the test's services by identity, then writes its
    result (``sentinel.json``) with every verdict. It needs no network, no
    driver and no load: a lost driver, ssh or watchdog leaves the heartbeat
    stale, and it stops the services within the heartbeat timeout."""

    def __init__(self, args, clock=None, checks=None):
        self.args = args
        self.clock = clock or common.Clock()
        self.hb = common.Heartbeat(args.heartbeat, args.heartbeat_id, args.heartbeat_timeout, self.clock)
        self.free = (checks or {}).get("free", free_bytes)
        self.mem = (checks or {}).get("mem", mem_available_bytes)
        self.deadline = self.clock.monotonic() + args.max_seconds
        self.terminated = False
        self.result = {"run_id": args.heartbeat_id, "services": {}}

    def terminate(self, *_):
        self.terminated = True

    def alarm(self):
        d = self.args.alarms
        try:
            names = sorted(os.listdir(d))
        except FileNotFoundError:
            return None
        except OSError as e:
            return {"reason": "monitor-failed", "detail": f"alarms: {e}"}
        for n in names:
            if not n.endswith(".json"):
                continue  # a temporary name is never read

            try:
                rec = common.read_json(os.path.join(d, n))
            except (OSError, ValueError):
                return {"reason": "stop-file-unreadable", "file": n}
            if not isinstance(rec, dict) or not isinstance(rec.get("reason"), str):
                return {"reason": "stop-file-unreadable", "file": n}
            if common.reason_class(rec["reason"]) in ("resource", "monitor"):
                return dict(rec, file=n)
        return None

    def breach(self):
        a = self.alarm()
        if a:
            return a.get("reason", "monitor-failed"), {"source": "alarm", "alarm": a}
        if self.terminated:
            return "operator", {"source": "sigterm"}
        r = self.hb.check()
        if r:
            return r, {"source": "sentinel"}
        try:
            if self.free(self.args.data_dir) < self.args.min_free_gib * GIB:
                return "disk-free", {"source": "sentinel"}
            if self.mem() < self.args.min_mem_mib * MIB:
                return "memory", {"source": "sentinel"}
        except (OSError, ValueError) as e:
            return "monitor-failed", {"source": "sentinel", "error": str(e)[:200]}
        if self.clock.monotonic() >= self.deadline:
            return "run-deadline", {"source": "sentinel"}
        return None

    def run(self):
        try:
            signal.signal(signal.SIGTERM, self.terminate)
        except ValueError:
            pass  # not the main thread (tests call terminate() directly)
        while True:
            b = self.breach()
            if b:
                break
            self.clock.sleep(self.args.interval)
        reason, detail = b
        common.StopFile(self.args.stop_file).stop(reason, "sentinel", self.clock.wall_ms())
        res, confirmed = stop_services(self.args.services, self.clock)
        self.result.update(reason=reason, detail=detail, services=res, services_confirmed=confirmed,
                           wall_ms=self.clock.wall_ms())
        common.atomic_write_json(os.path.join(self.args.run_dir, "sentinel.json"), self.result)
        return 3


def _common_args(p):
    p.add_argument("--run-dir", required=True)
    p.add_argument("--heartbeat", required=True)
    p.add_argument("--data-dir", required=True)
    p.add_argument("--min-free-gib", type=float, default=3.0)
    p.add_argument("--min-mem-mib", type=float, default=1024.0)
    p.add_argument("--heartbeat-timeout", type=float, default=120.0)
    p.add_argument("--max-seconds", type=float, default=7.5 * 3600 + 600)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--services")
    p.add_argument("--heartbeat-id", help="the run id the heartbeat names (default: this run's)")


def _check_limits(p, args, max_seconds):
    nums = (args.heartbeat_timeout, args.min_free_gib, args.min_mem_mib, args.max_seconds, args.interval)
    if not all(math.isfinite(x) and x > 0 for x in nums):
        p.error("every limit a finite positive number")
    if args.heartbeat_timeout > 120 or args.min_free_gib < 3 or args.min_mem_mib < 1024:
        p.error("limits looser than the plan's (heartbeat 120 s, 3 GiB, 1 GiB)")
    if args.interval > 10 or args.max_seconds > max_seconds:
        p.error("check interval above 10 s or deadline above the plan's run time")


# The sentinel outlives the run by the drain's observation and a margin.
SENTINEL_MAX_S = 7.5 * 3600 + 1800 + 1800


def sentinel_main(argv, clock=None, checks=None):
    p = argparse.ArgumentParser(description="the run's host-side keeper")
    _common_args(p)
    p.add_argument("--alarms", required=True)
    p.add_argument("--stop-file", required=True, help="the main load's stop file")
    args = p.parse_args(argv)
    if not args.services or not args.heartbeat_id:
        p.error("--services and --heartbeat-id are required")
    _check_limits(p, args, SENTINEL_MAX_S)
    return Sentinel(args, clock, checks).run()


def main(argv=None, clock=None, checks=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["sentinel"]:
        return sentinel_main(argv[1:], clock, checks)
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("command", choices=["run"])
    _common_args(p)
    p.add_argument("--grace", type=float, default=GRACE_S,
                   help="seconds the load has to end by itself after a stop (at most 60)")
    p.add_argument("--record-dir", help="the run's records (services/): the load's identity is written there")
    p.add_argument("--record-name", default="load")
    if "--" not in argv:
        p.error("the load command follows --")
    i = argv.index("--")
    args = p.parse_args(argv[:i])
    cmd = argv[i + 1:]
    if not cmd:
        p.error("the load command follows --")
    _check_limits(p, args, 7.5 * 3600 + 600)
    if not (math.isfinite(args.grace) and 0 <= args.grace <= 60):
        p.error("--grace: 0 to 60 s")
    if args.record_dir and not re.fullmatch(r"load(-[a-z0-9-]+)?", args.record_name):
        p.error("--record-name: load or load-<sub-run>")
    return Guard(args, clock, checks).run(cmd)


if __name__ == "__main__":
    sys.exit(main())
