"""The outcome ledger (plan §9 P1): every data-plane attempt of the run,
written as it happens, so that integrity (C5) can be judged after any stop.

One JSON record per line, each carrying a CRC-32 of its body; every record
is flushed and ``fsync``ed before the code it describes goes on (an
``intent`` before the request is sent, an ``outcome`` after its answer is
read). A record that cannot be written raises ``LedgerFailure``; the load
then stops (reason ``ledger-failure``) and sends nothing more.

Records:

- ``intent``: run, operation, attempt, key, op (PUT, DELETE, GET, LIST),
  the body's identity (``version`` and ``sha256`` for a PUT), stage, kind
  (``op``, ``retry``, ``probe``, ``list``, ``verify``), the wall time.
- ``outcome``: the same ids, ``status`` (HTTP status or 0), ``result``
  (``ack``, ``refused``, ``unknown``, ``not-sent``, ``missing``,
  ``found``, ``unexpected``), the wall time, the rate and concurrency
  waits, the response time. ``not-sent``: no connection could be made, so
  nothing reached the server.
- ``check``: the verification's verdict on a key (``match``,
  ``mismatch``, ``unread``).

The bucket's own requests are recorded too (op ``BUCKET-PUT``, ``LIST``);
they change no key.

Reading (``read``): a line whose CRC does not match, or that does not
parse, is corruption and an error, except the file's last line when it has
no newline (a write cut by a stop): that torn tail is reported, never
parsed. A torn intent's request was never sent (an intent is durable
before sending); a torn outcome leaves its intent without an outcome,
so that attempt is unknown. An intent without an outcome is
never taken as done or as not done: its key gets both states as
candidates.

Expected state (``expected``): per key, the set of states it may be in,
each ``("absent",)`` or ``("body", version, sha256)``. Operations on one
key are serialised by the load (one at a time per key, in operation-id
order). An acknowledged PUT or DELETE sets the state; a refused one
(``503 SlowDown``, rejected before the write) or a ``not-sent`` one
leaves it; an unknown one
adds its state to the candidates. ``verify`` then reads each key and
checks the body against them.
"""

import json
import os
import threading
import zlib


class LedgerFailure(Exception):
    """The ledger could not be written or read safely."""


def _line(rec):
    body = json.dumps(rec, sort_keys=True, separators=(",", ":"))
    return f"{zlib.crc32(body.encode()):08x} {body}\n"


class Ledger:
    """An append-only ledger file, created exclusively (never reopened)."""

    def __init__(self, path, run_id, opener=open):
        self.path = path
        self.run_id = run_id
        self._lock = threading.Lock()
        self._failed = None
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            raise LedgerFailure(f"{path} exists: a run's ledger is never reopened") from None
        os.close(fd)
        self._f = opener(path, "a")
        self._op = 0

    def next_operation(self):
        with self._lock:
            self._op += 1
            return self._op

    def write(self, rec):
        """Append ``rec`` durably, or raise ``LedgerFailure`` (and keep
        failing: a ledger that failed once is not written again)."""
        rec = dict(rec, run=self.run_id)
        with self._lock:
            if self._failed:
                raise LedgerFailure(self._failed)
            try:
                self._f.write(_line(rec))
                self._f.flush()
                os.fsync(self._f.fileno())
            except (OSError, ValueError) as e:
                self._failed = f"ledger write failed: {e}"
                raise LedgerFailure(self._failed) from e

    def close(self):
        with self._lock:
            try:
                self._f.close()
            except OSError:
                pass


def read(path):
    """``(records, torn_tail)``: the records in order; ``torn_tail`` the
    text of an unterminated last line (None if the file ends cleanly).
    Corruption anywhere else raises ``LedgerFailure``."""
    with open(path, "rb") as f:
        data = f.read()
    lines = data.split(b"\n")
    torn = None
    if lines and lines[-1] != b"":
        torn = lines[-1].decode(errors="replace")
    lines = lines[:-1]
    out = []
    for n, raw in enumerate(lines, 1):
        try:
            text = raw.decode()
            crc, body = text.split(" ", 1)
            if f"{zlib.crc32(body.encode()):08x}" != crc:
                raise ValueError("crc")
            out.append(json.loads(body))
        except (ValueError, UnicodeDecodeError) as e:
            raise LedgerFailure(f"{path}: line {n} corrupt ({e})") from None
    return out, torn


ABSENT = ("absent",)


def expected(records, initial=None):
    """Per key the candidate states after the recorded operations, and the
    list of attempts with no outcome. ``initial``: key -> state before the
    ledger (default absent)."""
    intents, outcomes = {}, {}
    for r in records:
        if r.get("type") == "intent":
            intents[(r["op_id"], r["attempt"])] = r
        elif r.get("type") == "outcome":
            outcomes[(r["op_id"], r["attempt"])] = r
    state = {}
    unresolved = []
    for k in sorted(intents):
        i = intents[k]
        if i["op"] not in ("PUT", "DELETE"):
            continue
        key = i["key"]
        cur = state.setdefault(key, {(initial or {}).get(key, ABSENT)})
        new = ("body", i["version"], i["sha256"]) if i["op"] == "PUT" else ABSENT
        o = outcomes.get(k)
        if o is None:
            unresolved.append(k)
            cur.add(new)
        elif o["result"] == "ack":
            state[key] = {new}
        elif o["result"] in ("refused", "not-sent"):
            pass
        else:
            cur.add(new)
    return state, unresolved


def acknowledged(records, op, stage=None):
    """Keys of the operations ``op`` acknowledged (in ``stage`` if given)."""
    intents = {(r["op_id"], r["attempt"]): r for r in records if r.get("type") == "intent"}
    keys = []
    for r in records:
        if r.get("type") == "outcome" and r["result"] == "ack":
            i = intents.get((r["op_id"], r["attempt"]))
            if i and i["op"] == op and (stage is None or i.get("stage") == stage):
                keys.append(i["key"])
    return keys
