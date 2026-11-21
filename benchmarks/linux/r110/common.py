"""Clocks, atomic files and stop reasons shared by the R-110 tools.

Times: ``Clock.monotonic()`` (seconds, for intervals and deadlines, never
affected by wall-clock changes) and ``Clock.wall_ms()`` (Unix
milliseconds, for records only). Wall time comes from ``time.time_ns()``,
never from formatted ``date`` output (the test hosts' ``date +%3N`` does
not zero-pad, ADR-0035 report §7.5).
"""

import json
import os
import threading
import time


class Clock:
    """The real clocks."""

    def monotonic(self):
        return time.monotonic()

    def wall_ms(self):
        return time.time_ns() // 1_000_000

    def sleep(self, seconds, cancel=None):
        """Sleep, returning early (True) when ``cancel`` (an Event) is set."""
        if seconds <= 0:
            return bool(cancel and cancel.is_set())
        if cancel is None:
            time.sleep(seconds)
            return False
        return cancel.wait(seconds)


class FakeClock(Clock):
    """A virtual clock for tests: time moves only by ``advance`` or by a
    sleeping thread (which moves it to its own wake-up time). Thread-safe."""

    def __init__(self, start=1000.0, wall_ms=1_800_000_000_000):
        self._t = start
        self._wall0 = wall_ms
        self._start = start
        self._cond = threading.Condition()

    def monotonic(self):
        with self._cond:
            return self._t

    def wall_ms(self):
        with self._cond:
            return self._wall0 + int((self._t - self._start) * 1000)

    def advance(self, seconds):
        with self._cond:
            self._t += seconds
            self._cond.notify_all()

    def sleep(self, seconds, cancel=None):
        if cancel is not None and cancel.is_set():
            return True
        with self._cond:
            target = self._t + max(seconds, 0)
            if self._t < target:
                self._t = target
                self._cond.notify_all()
        return bool(cancel and cancel.is_set())


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_json(path, obj):
    """Write ``obj`` to ``path`` atomically and durably (temporary file,
    fsync, rename, fsync of the directory)."""
    d = os.path.dirname(os.path.abspath(path))
    tmp = f"{path}.tmp-{os.getpid()}-{threading.get_ident()}"
    with open(tmp, "w") as f:
        json.dump(obj, f, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fsync_dir(d)


def read_json(path):
    with open(path) as f:
        return json.load(f)


class StopFile:
    """The run's stop file: the first reason written wins (``O_EXCL``);
    later writers learn the existing reason. Readers poll ``reason()``."""

    def __init__(self, path):
        self.path = path

    def stop(self, reason, source, wall_ms):
        rec = {"reason": reason, "source": source, "wall_ms": wall_ms}
        try:
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w") as f:
            json.dump(rec, f)
            f.flush()
            os.fsync(f.fileno())
        fsync_dir(os.path.dirname(os.path.abspath(self.path)))
        return True

    def reason(self):
        """The stop record, or None. An unreadable or partial file counts
        as a stop (reason ``stop-file-unreadable``): never as running."""
        try:
            with open(self.path) as f:
                text = f.read()
        except FileNotFoundError:
            return None
        except OSError as e:
            return {"reason": "stop-file-unreadable", "detail": str(e)}
        try:
            return json.loads(text)
        except ValueError:
            return {"reason": "stop-file-unreadable", "detail": text[:200]}


class Heartbeat:
    """The watchdog's heartbeat as the guard and the load read it: a JSON
    file ``{"run_id", "seq"}`` that counts only when it names this run and
    its ``seq`` is higher than the last seen; fresh for ``timeout`` seconds
    of the reader's monotonic clock after that (file times and wall clocks
    are not used). Before the first beat: ``timeout`` from the start."""

    def __init__(self, path, run_id, timeout, clock):
        self.path, self.run_id, self.timeout, self.clock = path, run_id, timeout, clock
        self.seq = None
        self.fresh_until = clock.monotonic() + timeout

    def check(self):
        """None when fresh, else the breach reason."""
        try:
            with open(self.path) as f:
                rec = json.load(f)
            if rec.get("run_id") == self.run_id and isinstance(rec.get("seq"), int) \
                    and (self.seq is None or rec["seq"] > self.seq):
                self.seq = rec["seq"]
                self.fresh_until = self.clock.monotonic() + self.timeout
        except (OSError, ValueError, AttributeError):
            pass  # absent, partial or foreign: not a new beat
        return None if self.clock.monotonic() < self.fresh_until else "heartbeat-lost"


# Stop reasons (one vocabulary for the generator, the guard, the watchdog
# and the driver). Classes decide what is stopped (plan §6.2) and how a
# stop is judged (plan §8): ``integrity`` -> Failed; ``resource`` and
# ``monitor`` -> Inconclusive and the test's services stopped too;
# ``limit`` and ``operator`` -> Inconclusive, load only.
REASONS = {
    "integrity-violation": "integrity",
    "protection-violation": "integrity",
    "ledger-failure": "integrity",
    "disk-free": "resource",
    "memory": "resource",
    "swap": "resource",
    "oom": "resource",
    "load": "resource",
    "iowait": "resource",
    "system-fdb": "resource",
    "test-fdb": "resource",
    "test-dirs": "resource",
    "probe": "resource",
    "heartbeat-lost": "monitor",
    "host-unreachable": "monitor",
    "monitor-failed": "monitor",
    "stop-file-unreadable": "monitor",
    "attempts-limit": "limit",
    "bytes-limit": "limit",
    "run-deadline": "limit",
    "stage-deadline": "limit",
    "operator": "operator",
    "smoke-exercise": "exercise",
}


def reason_class(reason):
    """The class of a stop reason; an unknown reason is treated as a
    monitor failure (it stops everything that a monitor loss stops)."""
    return REASONS.get(reason, "monitor")
