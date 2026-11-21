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
import os
import signal
import subprocess
import sys

from . import common, proc

GIB, MIB = 1 << 30, 1 << 20


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
        d = self.args.services
        if not d or not os.path.isdir(d):
            return
        recs = []
        for name in sorted(os.listdir(d)):
            if name.endswith(".json"):
                try:
                    recs.append((name[:-5], common.read_json(os.path.join(d, name))))
                except (OSError, ValueError) as e:
                    self.result["services"][name] = f"unreadable: {e}"
        for name, rec in recs:
            self.result["services"][name] = proc.signal_if_same(rec, signal.SIGTERM)
        end = self.clock.monotonic() + 30
        while self.clock.monotonic() < end and any(proc.matches(r) for _, r in recs):
            self.clock.sleep(0.2)
        for name, rec in recs:
            if proc.matches(rec):
                self.result["services"][name] = "killed: " + proc.signal_if_same(rec, signal.SIGKILL)

    def run(self, command):
        child = subprocess.Popen(command, start_new_session=True, cwd=self.args.run_dir)
        pgid = child.pid
        self.result["load"] = {"pid": child.pid, "command": command}
        reason = source = None
        while True:
            # Exit seen without reaping: the pid (and group id) stays the
            # load's until the guard reaps it below.
            exited = os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG)
            if exited is not None:
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
        # The whole group (the load and anything it started), also after the
        # load itself ended: nothing of it may outlive the guard.
        empty = proc.stop_group(pgid, self.clock, child.poll)
        child.wait()
        if reason and common.reason_class(reason) in ("resource", "monitor"):
            self.stop_services()
            self.result["stopped"].append("services")
        self.result.update(reason=reason, source=source, load_exit=child.returncode,
                           group_empty=empty, wall_ms=self.clock.wall_ms())
        common.atomic_write_json(os.path.join(self.args.run_dir, "guard.json"), self.result)
        if reason or not empty:
            return 3
        return child.returncode


def main(argv=None, clock=None, checks=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("command", choices=["run"])
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
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--" not in argv:
        p.error("the load command follows --")
    i = argv.index("--")
    args = p.parse_args(argv[:i])
    cmd = argv[i + 1:]
    if not cmd:
        p.error("the load command follows --")
    if args.heartbeat_timeout > 120 or args.min_free_gib < 3 or args.min_mem_mib < 1024:
        p.error("limits looser than the plan's (heartbeat 120 s, 3 GiB, 1 GiB)")
    return Guard(args, clock, checks).run(cmd)


if __name__ == "__main__":
    sys.exit(main())
