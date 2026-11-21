"""Process identity and stopping for the R-110 stop layers (plan §6.2).

A process is identified by its pid *and* its start token (Linux: the start
time in clock ticks since boot, ``/proc/<pid>/stat`` field 22; elsewhere:
``ps -o lstart=``) and working directory, recorded when it was started.
Nothing here matches processes by name, and nothing is signalled on a
guess: every answer distinguishes a process that is gone, the same
process, another one (the pid reused), and one that cannot be checked
(a permission error, an unreadable record). Only the same process is
signalled.

Process groups: every test process is started in a session of its own,
so its process group (id = its pid) holds it and its children. A group
is stopped by signalling its id while it has members, and is stopped
when none of its members is alive (zombies are not alive). A group id is
not reused while any member exists, the leader's zombie included: the
guard does not reap the load (its child) until the group is confirmed
stopped, so its signals can only reach that group. For a service (not the
caller's child) the group is signalled only after its leader's identity
was confirmed in the same call; once the leader is gone, its remaining
members still hold the id, and the group is signalled only while a member
is seen (the remaining window, a member exiting and the id being handed
out again between the look and the signal, needs the kernel to cycle
through its pid space in that interval).
"""

import os
import signal
import subprocess
import sys

LINUX = sys.platform.startswith("linux")


class Unknown(Exception):
    """A process or group that exists, or may exist, and cannot be checked."""


def start_token(pid):
    """The start token of ``pid``; None if no such process. Raises
    ``Unknown`` when it cannot be read."""
    if LINUX:
        try:
            with open(f"/proc/{pid}/stat") as f:
                stat = f.read()
        except (FileNotFoundError, ProcessLookupError):
            return None
        except OSError as e:
            raise Unknown(f"/proc/{pid}/stat: {e}") from None
        # The command may contain spaces and parentheses: fields follow the
        # last ')'. Field 22 overall is the 20th after it.
        return stat.rsplit(")", 1)[1].split()[19]
    try:
        r = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise Unknown(f"ps: {e}") from None
    out = r.stdout.strip()
    if r.returncode != 0 and not out:
        return None  # ps answers 1 for no such process
    return out or None


def cwd_of(pid):
    """The working directory of ``pid``; None if the process is gone.
    Raises ``Unknown`` when it cannot be read (another user's process, a
    failing ``lsof``)."""
    if LINUX:
        try:
            return os.readlink(f"/proc/{pid}/cwd")
        except (FileNotFoundError, ProcessLookupError):
            return None
        except OSError as e:
            raise Unknown(f"/proc/{pid}/cwd: {e}") from None
    try:
        r = subprocess.run(["lsof", "-a", "-d", "cwd", "-p", str(pid), "-Fn"], capture_output=True, text=True,
                           timeout=20)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise Unknown(f"lsof: {e}") from None
    lines = [ln[1:] for ln in r.stdout.splitlines() if ln.startswith("n")]
    if lines:
        return lines[-1]
    if start_token(pid) is None:
        return None
    raise Unknown(f"lsof gave no working directory for {pid}")


def identity(pid):
    """``{"pid", "start", "cwd", "pgid"}`` of a live ``pid``; None if it
    does not exist; raises ``Unknown`` if it cannot be read."""
    tok = start_token(pid)
    if tok is None:
        return None
    cwd = cwd_of(pid)
    if cwd is None:
        return None
    try:
        pgid = os.getpgid(pid)
    except ProcessLookupError:
        return None
    return {"pid": pid, "start": tok, "cwd": cwd, "pgid": pgid}


def valid(rec):
    """A recorded identity: pid, start token, working directory and the
    process group (a record without its group could only guess one)."""
    return isinstance(rec, dict) and isinstance(rec.get("pid"), int) and not isinstance(rec["pid"], bool) \
        and rec["pid"] > 1 and bool(rec.get("start")) and bool(rec.get("cwd")) \
        and isinstance(rec.get("pgid"), int) and not isinstance(rec["pgid"], bool) and rec["pgid"] > 1


def threads_alive(pid):
    """Linux: whether a thread of ``pid`` other than a zombie is left. A
    process whose main thread has exited shows ``Z`` in its own stat while
    its other threads still run (and hold their files). Raises ``Unknown``
    when the task list cannot be read."""
    try:
        tids = os.listdir(f"/proc/{pid}/task")
    except (FileNotFoundError, ProcessLookupError):
        return False
    except OSError as e:
        raise Unknown(f"/proc/{pid}/task: {e}") from None
    for t in tids:
        try:
            with open(f"/proc/{pid}/task/{t}/stat") as f:
                state = f.read().rsplit(")", 1)[1].split()[0]
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as e:
            raise Unknown(f"/proc/{pid}/task/{t}/stat: {e}") from None
        if state not in ("Z", "X"):
            return True
    return False


def check(rec):
    """``same``, ``gone``, ``other`` (the pid now another process's),
    ``unknown`` (it cannot be checked) or ``invalid`` (not an identity)."""
    if not valid(rec):
        return "invalid"
    try:
        tok = start_token(rec["pid"])
        if tok is None:
            return "gone"
        if tok != rec["start"]:
            return "other"
        cwd = cwd_of(rec["pid"])
    except Unknown:
        return "unknown"
    if cwd is None:
        # The working directory is gone with the main thread; the pid and
        # start token still name this process while any thread runs.
        try:
            return "same" if LINUX and threads_alive(rec["pid"]) else "gone"
        except Unknown:
            return "unknown"
    return "same" if cwd == rec["cwd"] else "other"


def matches(rec):
    """Whether ``rec`` is still the same process. Raises ``ValueError``
    for a record that is not an identity and ``Unknown`` when it cannot be
    checked (never a silent no)."""
    c = check(rec)
    if c == "invalid":
        raise ValueError(f"not a process identity: {rec!r}")
    if c == "unknown":
        raise Unknown(f"process {rec['pid']} cannot be checked")
    return c == "same"


def signal_if_same(rec, sig):
    """Send ``sig`` to ``rec`` only if it is still the same process.
    Returns ``signalled``, ``gone``, ``other`` (pid reused: nothing sent),
    ``unknown`` (not checkable: nothing sent) or ``denied`` (EPERM)."""
    if not valid(rec):
        raise ValueError(f"not a process identity: {rec!r}")
    c = check(rec)
    if c != "same":
        return c
    try:
        os.kill(rec["pid"], sig)
    except ProcessLookupError:
        return "gone"
    except PermissionError:
        return "denied"
    return "signalled"


# -- groups ---------------------------------------------------------------------


def members(pgid):
    """``[(pid, state)]`` of the processes in group ``pgid`` (state: a
    one-letter process state, ``Z`` a zombie). Raises ``Unknown`` when the
    process table cannot be read."""
    out = []
    if LINUX:
        try:
            names = os.listdir("/proc")
        except OSError as e:
            raise Unknown(f"/proc: {e}") from None
        for n in names:
            if not n.isdigit():
                continue
            try:
                with open(f"/proc/{n}/stat") as f:
                    rest = f.read().rsplit(")", 1)[1].split()
            except (FileNotFoundError, ProcessLookupError):
                continue  # exited while listed
            except OSError as e:
                raise Unknown(f"/proc/{n}/stat: {e}") from None
            # After ')': state, ppid, pgrp, ...
            if int(rest[2]) == pgid:
                state = rest[0]
                if state in ("Z", "X") and threads_alive(int(n)):
                    state = "T*"  # the main thread gone, others running: alive
                out.append((int(n), state))
        return out
    try:
        r = subprocess.run(["ps", "-A", "-o", "pid=,pgid=,stat="], capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise Unknown(f"ps: {e}") from None
    if r.returncode != 0:
        raise Unknown(f"ps: rc {r.returncode}")
    for line in r.stdout.splitlines():
        f = line.split()
        if len(f) >= 3 and int(f[1]) == pgid:
            out.append((int(f[0]), f[2][0]))
    return out


def group_state(pgid):
    """``empty`` (no member alive: none, or zombies only), ``alive`` or
    ``unknown`` (the table cannot be read)."""
    try:
        ms = members(pgid)
    except Unknown:
        return "unknown"
    return "alive" if any(st not in ("Z", "X") for _, st in ms) else "empty"


def group_alive(pgid):
    """True when a member of ``pgid`` is alive; raises ``Unknown`` when it
    cannot be told (never taken as empty)."""
    st = group_state(pgid)
    if st == "unknown":
        raise Unknown(f"group {pgid} cannot be checked")
    return st == "alive"


def _killpg(pgid, sig):
    try:
        os.killpg(pgid, sig)
        return "signalled"
    except ProcessLookupError:
        return "gone"
    except PermissionError:
        # A member another user owns (Linux), or a group of zombies only
        # (macOS): never taken as stopped; the member scan decides.
        return "denied"


def stop_group(pgid, clock, term_wait=20.0, kill_wait=10.0):
    """TERM the group, then KILL it, until no member is alive. Returns
    ``{"confirmed": bool, "state": final group state, "signals": [...]}``.
    The caller must keep the group id reserved meanwhile (the guard: its
    load, the leader, is not reaped before this returns)."""
    sent = []
    for sig, wait in ((signal.SIGTERM, term_wait), (signal.SIGKILL, kill_wait)):
        st = group_state(pgid)
        if st == "empty":
            return {"confirmed": True, "state": st, "signals": sent}
        sent.append((signal.Signals(sig).name, _killpg(pgid, sig)))
        end = clock.monotonic() + wait
        while clock.monotonic() < end:
            st = group_state(pgid)
            if st == "empty":
                return {"confirmed": True, "state": st, "signals": sent}
            clock.sleep(0.05)
    st = group_state(pgid)
    return {"confirmed": st == "empty", "state": st, "signals": sent}


def stop_identified(rec, clock, term_wait=20.0, kill_wait=10.0):
    """Stop a recorded service (a session leader started by the run) and
    its whole group. Returns ``{"verdict", ...}``:

    - ``stopped``: it was the same process, signalled, and no member of
      its group is alive any more;
    - ``gone``: neither it nor any member of its group is alive;
    - ``not-stopped``: signalled, but a member is still alive;
    - ``unverified``: it could not be identified or checked (an invalid
      record, a reused pid, a permission error, members left of a leader
      gone before this call): nothing was signalled on a guess.
    """
    if not valid(rec):
        return {"verdict": "unverified", "reason": "not a process identity"}
    pgid = rec["pgid"]
    c = check(rec)
    if c == "unknown":
        return {"verdict": "unverified", "reason": "the process cannot be checked"}
    if c in ("gone", "other"):
        st = group_state(pgid)
        if st == "empty" and c == "gone":
            return {"verdict": "gone"}
        # A reused pid (``other``) or members left of a group whose leader
        # is gone: not identifiable here.
        return {"verdict": "unverified", "reason": f"leader {c}, group {st}"}
    if pgid != rec["pid"]:
        # Not a session leader of its own: only the process itself.
        r = signal_if_same(rec, signal.SIGTERM)
        end = clock.monotonic() + term_wait
        while r == "signalled" and clock.monotonic() < end and check(rec) == "same":
            clock.sleep(0.05)
        if check(rec) == "same":
            r = signal_if_same(rec, signal.SIGKILL)
            end = clock.monotonic() + kill_wait
            while clock.monotonic() < end and check(rec) == "same":
                clock.sleep(0.05)
        left = check(rec)
        return {"verdict": "stopped" if left == "gone" else "not-stopped", "signal": r, "left": left}
    res = stop_group(pgid, clock, term_wait, kill_wait)
    verdict = "stopped" if res["confirmed"] else ("unverified" if res["state"] == "unknown" else "not-stopped")
    return dict(res, verdict=verdict)
