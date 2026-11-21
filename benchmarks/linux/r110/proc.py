"""Process identity for the R-110 stop layers (plan §6.2).

A process is identified by its pid *and* its start token (Linux: the
start time in clock ticks since boot, ``/proc/<pid>/stat`` field 22;
elsewhere: ``ps -o lstart=``) and working directory, recorded when it was
started. A signal is sent only after the identity is read again and
matches, so a pid reused by another process is never signalled; the
remaining window between that check and the signal is closed for the
load, which the guard starts as its own child (its pid cannot be reused
before the guard reaps it). Nothing here matches processes by name.
"""

import os
import signal
import subprocess
import sys


def start_token(pid):
    """The start token of ``pid``, or None if it does not exist."""
    if sys.platform.startswith("linux"):
        try:
            with open(f"/proc/{pid}/stat") as f:
                stat = f.read()
        except (FileNotFoundError, ProcessLookupError):
            return None
        # The command may contain spaces and parentheses: fields follow the
        # last ')'. Field 22 overall is the 20th after it.
        return stat.rsplit(")", 1)[1].split()[19]
    r = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True)
    out = r.stdout.strip()
    return out or None


def cwd_of(pid):
    if sys.platform.startswith("linux"):
        try:
            return os.readlink(f"/proc/{pid}/cwd")
        except OSError:
            return None
    r = subprocess.run(["lsof", "-a", "-d", "cwd", "-p", str(pid), "-Fn"], capture_output=True, text=True)
    lines = [ln[1:] for ln in r.stdout.splitlines() if ln.startswith("n")]
    return lines[-1] if lines else None


def identity(pid):
    tok = start_token(pid)
    if tok is None:
        return None
    return {"pid": pid, "start": tok, "cwd": cwd_of(pid)}


def matches(rec):
    """Whether the process ``rec`` (pid, start, cwd) still exists as itself."""
    now = identity(rec["pid"])
    return bool(now and now["start"] == rec["start"] and now["cwd"] == rec["cwd"])


def signal_if_same(rec, sig):
    """Send ``sig`` to ``rec`` only if it is still the same process.
    Returns ``signalled``, ``gone`` or ``other`` (pid reused: nothing sent)."""
    if start_token(rec["pid"]) is None:
        return "gone"
    if not matches(rec):
        return "other"
    try:
        os.kill(rec["pid"], sig)
    except ProcessLookupError:
        return "gone"
    return "signalled"


def group_alive(pgid):
    """Whether the group has a member. ``EPERM`` (macOS answers it for a
    group whose members are all zombies; the group is the guard's own
    session, so no member of another user can be in it) counts as none."""
    try:
        os.killpg(pgid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def stop_group(pgid, clock, reap, term_wait=20.0, kill_wait=10.0):
    """TERM the process group ``pgid``, then KILL it; return whether it is
    empty afterwards. ``reap()`` collects the group leader (the caller's
    child) once it has exited: a zombie leader would otherwise keep the
    group alive, and until it is reaped its id cannot be reused, so the
    signals reach only this group."""
    for sig, wait in ((signal.SIGTERM, term_wait), (signal.SIGKILL, kill_wait)):
        reap()
        if not group_alive(pgid):
            return True
        try:
            os.killpg(pgid, sig)
        except (ProcessLookupError, PermissionError):
            return True
        end = clock.monotonic() + wait
        while clock.monotonic() < end:
            reap()
            if not group_alive(pgid):
                return True
            clock.sleep(0.05)
    reap()
    return not group_alive(pgid)
