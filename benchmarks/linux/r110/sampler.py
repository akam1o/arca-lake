#!/usr/bin/env python3
"""The R-110 sampler (plan §2, §9 P3), on the host of the test's processes.

  sampler.py --config FILE --out FILE [--rounds N]

``FILE`` (JSON): ``run_id``; ``interval`` (15 s); ``timeout`` (5 s per
target); ``max_workers`` (4 targets read at once); ``max_out_bytes``
(256 MiB of samples at most; beyond it the sampler records why and
stops: it never grows without bound); ``targets``, each one of

- ``{"name", "kind": "metrics", "url", "families": [...]}``: a Prometheus
  text endpoint; every line whose metric name is one of ``families`` is
  kept (with its labels). A family absent from the answer makes the
  sample ``partial``; no answer, ``error`` (``timeout`` told apart).
- ``{"name", "kind": "dir", "path"}``: the apparent bytes (``st_size``) and
  the allocated bytes (``st_blocks`` × 512) of every file below ``path``.
  A file that disappears while it is listed (compaction deletes segments)
  restarts the walk, at most ``SCAN_ATTEMPTS`` walks, then ``partial``;
  any other error is ``error`` with its errno, never skipped.
- ``{"name", "kind": "log", "path"}``: the controller's log, read from the
  last offset: the reconcile pass lines' ``deleted=`` and ``unreachable=``
  counts. A log shorter than the offset (rotated or replaced) is
  ``partial`` and read again from its start.

Each sample (one JSON line): ``run_id``, ``target``, ``kind``,
``started_wall_ms``, ``ended_wall_ms`` (Unix ms from ``time.time_ns``, not
formatted ``date`` output), ``duration_ms`` (monotonic), ``status``
(``ok``, ``partial``, ``error``, ``timeout``), ``values`` (name ->
``{"value", "unit"}``), ``error``. A value that could not be read is
absent, never 0, and a sample is never reused for a later round.
"""

import argparse
import concurrent.futures
import errno
import json
import os
import re
import sys
import threading
import urllib.error
import urllib.request

from . import common

SCAN_ATTEMPTS = 8
DEFAULTS = {"interval": 15.0, "timeout": 5.0, "max_workers": 4, "max_out_bytes": 256 << 20}
ANSI = re.compile(r"\x1b\[[0-9;]*m")
RECONCILE = re.compile(r"reconcile pass deleted=(\d+) unreachable=(\d+)")
LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)")


def unit_of(name):
    if name.endswith(("_bytes", "_bytes_total")):
        return "bytes"
    if name.endswith(("_seconds", "_seconds_total")):
        return "seconds"
    if name.endswith("_per_second"):
        return "per_second"
    return "count"


def read_metrics(t, timeout):
    try:
        with urllib.request.urlopen(t["url"], timeout=timeout) as r:
            text = r.read().decode(errors="replace")
    except TimeoutError as e:
        return "timeout", {}, f"timeout: {e}"
    except urllib.error.URLError as e:
        if isinstance(e.reason, TimeoutError):
            return "timeout", {}, f"timeout: {e.reason}"
        return "error", {}, f"{type(e).__name__}: {e}"
    except OSError as e:
        return "error", {}, f"{type(e).__name__}: {e}"
    fams = set(t["families"])
    values, seen = {}, set()
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        m = LINE.match(line)
        if not m or m.group(1) not in fams:
            continue
        try:
            v = float(m.group(3))
        except ValueError:
            continue
        seen.add(m.group(1))
        values[m.group(1) + (m.group(2) or "")] = {"value": v, "unit": unit_of(m.group(1))}
    missing = sorted(fams - seen)
    if missing:
        return "partial", values, f"families absent: {missing}"
    return "ok", values, None


def walk_sizes(path, stat=os.lstat):
    """``(apparent, allocated)`` bytes of the files below ``path``. Raises
    ``Vanished`` when files disappeared in every one of the walks."""
    for _ in range(SCAN_ATTEMPTS):
        apparent = allocated = 0
        vanished = False
        for d, dirs, files in os.walk(path, onerror=_raise):
            for n in files:
                try:
                    st = stat(os.path.join(d, n))
                except FileNotFoundError:
                    vanished = True
                    break
                apparent += st.st_size
                allocated += st.st_blocks * 512
            if vanished:
                break
        if not vanished:
            return apparent, allocated
    raise Vanished(f"files vanished during each of {SCAN_ATTEMPTS} walks")


class Vanished(Exception):
    pass


def _raise(e):
    # A directory that vanished while walked restarts the walk like a file;
    # any other error reading a directory is an error, not a skip.
    if isinstance(e, FileNotFoundError):
        raise _DirVanished() from e
    raise e


class _DirVanished(Exception):
    pass


def read_dir(t, stat=os.lstat):
    if not os.path.isdir(t["path"]):
        return "error", {}, f"not a directory: {t['path']}"
    for _ in range(SCAN_ATTEMPTS):
        try:
            a, b = walk_sizes(t["path"], stat)
            return "ok", {"apparent_bytes": {"value": a, "unit": "bytes"},
                          "allocated_bytes": {"value": b, "unit": "bytes"}}, None
        except _DirVanished:
            continue
        except Vanished as e:
            return "partial", {}, str(e)
        except OSError as e:
            return "error", {}, f"{errno.errorcode.get(e.errno, e.errno)}: {e}"
    return "partial", {}, f"directories vanished during each of {SCAN_ATTEMPTS} walks"


class LogReader:
    def __init__(self):
        self.offset = 0
        self.deleted = 0
        self.unreachable = 0
        self.passes = 0

    def read(self, t):
        try:
            size = os.path.getsize(t["path"])
        except OSError as e:
            return "error", {}, f"{type(e).__name__}: {e}"
        status, err = "ok", None
        if size < self.offset:
            status, err = "partial", f"log shorter than the last offset ({size} < {self.offset}): re-read"
            self.offset = 0
        try:
            with open(t["path"], "rb") as f:
                f.seek(self.offset)
                data = f.read()
        except OSError as e:
            return "error", {}, f"{type(e).__name__}: {e}"
        # Only whole lines: a line still being written is read next round.
        end = data.rfind(b"\n") + 1
        self.offset += end
        for line in data[:end].decode(errors="replace").splitlines():
            m = RECONCILE.search(ANSI.sub("", line))
            if m:
                self.passes += 1
                self.deleted += int(m.group(1))
                self.unreachable += int(m.group(2))
        return status, {"reconcile_passes": {"value": self.passes, "unit": "count"},
                        "reconcile_deleted": {"value": self.deleted, "unit": "count"},
                        "reconcile_unreachable": {"value": self.unreachable, "unit": "count"}}, err


class Sampler:
    def __init__(self, cfg, out_path, clock=None):
        self.cfg = dict(DEFAULTS, **cfg)
        self.clock = clock or common.Clock()
        self.out_path = out_path
        self.logs = {t["name"]: LogReader() for t in self.cfg["targets"] if t["kind"] == "log"}
        self.written = os.path.getsize(out_path) if os.path.exists(out_path) else 0
        self.lock = threading.Lock()
        self.stopped = None

    def one(self, t):
        w0, m0 = self.clock.wall_ms(), self.clock.monotonic()
        try:
            if t["kind"] == "metrics":
                status, values, err = read_metrics(t, self.cfg["timeout"])
            elif t["kind"] == "dir":
                status, values, err = read_dir(t)
            elif t["kind"] == "log":
                status, values, err = self.logs[t["name"]].read(t)
            else:
                status, values, err = "error", {}, f"unknown kind {t['kind']!r}"
        except Exception as e:  # noqa: BLE001 - recorded, never hidden
            status, values, err = "error", {}, f"{type(e).__name__}: {e}"
        rec = {"run_id": self.cfg["run_id"], "target": t["name"], "kind": t["kind"],
               "started_wall_ms": w0, "ended_wall_ms": self.clock.wall_ms(),
               "duration_ms": round((self.clock.monotonic() - m0) * 1000, 3),
               "status": status, "values": values}
        if err:
            rec["error"] = err[:500]
        return rec

    def write(self, rec):
        line = json.dumps(rec, sort_keys=True) + "\n"
        with self.lock:
            if self.written + len(line) > self.cfg["max_out_bytes"]:
                self.stopped = "output limit"
                return False
            with open(self.out_path, "a") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            self.written += len(line)
            return True

    def round(self):
        targets = self.cfg["targets"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.cfg["max_workers"]) as ex:
            recs = list(ex.map(self.one, targets))
        for r in recs:
            if not self.write(r):
                break
        return recs

    def run(self, rounds=None):
        n = 0
        while rounds is None or n < rounds:
            t0 = self.clock.monotonic()
            self.round()
            if self.stopped:
                # Recorded once, outside the limit (a few bytes).
                with open(self.out_path, "a") as f:
                    f.write(json.dumps({"run_id": self.cfg["run_id"], "sampler_stopped": self.stopped,
                                        "wall_ms": self.clock.wall_ms()}) + "\n")
                return 2
            n += 1
            self.clock.sleep(max(0.0, self.cfg["interval"] - (self.clock.monotonic() - t0)))
        return 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--config", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--rounds", type=int)
    a = p.parse_args(argv)
    return Sampler(common.read_json(a.config), a.out).run(a.rounds)


if __name__ == "__main__":
    sys.exit(main())
