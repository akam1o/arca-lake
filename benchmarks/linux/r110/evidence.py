"""The host-side evidence of an R-110 run, saved on the operator's machine
and verified before anything on the host may be removed (plan §6.2,
"teardown and evidence").

Everything the run wrote on the host that exists only there (the ledgers,
the logs, the services' records and started.list, the stop, guard and
sentinel results, the alarms, the samples, the status and control files,
the markers, the host's boot record, the test FoundationDB's trace logs)
is copied file by file: the host computes each file's sha256, the bytes
come over in chunks (each with its own sha256), are written under a
temporary name, synced, read back and compared with the host's sha256,
and only then take their name. The node stores (``d<N>``) and the test
FoundationDB's data are the cluster's data, not evidence: listed as
excluded, never copied.

The result is a manifest (``private/manifest.json``): the run ID, the
tools' and the binary's sha256, every file saved (size, sha256, the range
saved, verified), every file that could not be (``errors``), the expected
records not found on the host (``missing``), the host's boot record, and
``complete``. Complete means every file found was saved whole and
verified: a capped log, an error, a deadline reached or an unreadable
directory is never complete. A later collection resumes: a file already
saved with the host's current sha256 is not fetched again, one that
changed is fetched again and the earlier copy kept apart.

Logs are capped (per file, and in total): a log over the cap is saved as
its tail and marks the collection incomplete, so that a run is never
removed with a log cut short; the operator raises the cap and collects
again. Collection runs after the safe stops and never delays them.

``private/`` holds the raw evidence (host paths, logs, records);
``public/evidence-summary.json`` only counts, relative paths, sizes and
sha256 (no host name, address, user or absolute path).
"""

import base64
import hashlib
import json
import os
import time

from . import common

CHUNK = 4 << 20
MAX_LOG_FILE = 64 << 20
MAX_LOG_TOTAL = 512 << 20
RETRIES = 2
COLLECT_S = 1800.0
CALL_S = 120

# The records a run with a load is expected to leave (informational: a
# file absent on the host has nothing to save; it is listed as missing).
EXPECTED = ("run/.r110-run", "run/host-boot.json", "run/services/started.list", "run/load/ledger.jsonl",
            "run/load/status.json", "run/load/guard.json", "run/samples.jsonl")

# One JSON line: every file below each root (label, directory, a regular
# expression of top-level names to exclude), with its size and sha256;
# what is excluded; what could not be read.
INVENTORY = r'''# r110:inventory
import hashlib, json, os, re, sys
roots = json.loads(sys.argv[1])
out = {"files": [], "errors": [], "excluded": [], "roots": {}}
for label, top, exclude in roots:
    if not os.path.isdir(top):
        out["roots"][label] = "absent"
        continue
    out["roots"][label] = "present"
    ex = re.compile(exclude) if exclude else None
    def onerror(e, label=label, top=top):
        out["errors"].append({"path": label + "/" + os.path.relpath(e.filename or top, top),
                              "error": "%s: %s" % (type(e).__name__, e.strerror)})
    for d, dirs, files in os.walk(top, onerror=onerror):
        if d == top and ex:
            for x in sorted(dirs):
                if ex.fullmatch(x):
                    out["excluded"].append(label + "/" + x)
            dirs[:] = [x for x in dirs if not ex.fullmatch(x)]
        dirs.sort()
        for n in sorted(files):
            p = os.path.join(d, n)
            rel = label + "/" + os.path.relpath(p, top)
            if os.path.islink(p):
                out["files"].append({"path": rel, "abs": p, "link": os.readlink(p)})
                continue
            try:
                h, size = hashlib.sha256(), 0
                with open(p, "rb") as f:
                    for b in iter(lambda: f.read(1 << 20), b""):
                        h.update(b)
                        size += len(b)
                out["files"].append({"path": rel, "abs": p, "size": size, "sha256": h.hexdigest()})
            except OSError as e:
                out["errors"].append({"path": rel, "error": "%s: %s" % (type(e).__name__, e)})
print(json.dumps(out))
'''

# One file: ``hash PATH CAP`` (its size and sha256 read in one pass, and
# the sha256 of the range saved: the whole file, or its last CAP bytes),
# ``read PATH OFFSET LENGTH`` (the bytes, base64, with their sha256).
FILE = r'''# r110:file
import base64, hashlib, json, os, sys
mode, path = sys.argv[1], sys.argv[2]
try:
    with open(path, "rb") as f:
        if mode == "hash":
            cap = int(sys.argv[3])
            size = os.fstat(f.fileno()).st_size
            start = max(0, size - cap) if cap else 0
            full, part, off = hashlib.sha256(), hashlib.sha256(), 0
            while off < size:
                b = f.read(min(1 << 20, size - off))
                if not b:
                    break
                full.update(b)
                if off + len(b) > start:
                    part.update(b[max(0, start - off):])
                off += len(b)
            print(json.dumps({"size": off, "sha256": full.hexdigest(), "start": start,
                              "range_sha256": part.hexdigest()}))
        else:
            f.seek(int(sys.argv[3]))
            data = f.read(int(sys.argv[4]))
            print(json.dumps({"len": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                              "data": base64.b64encode(data).decode()}))
except OSError as e:
    print(json.dumps({"error": "%s: %s" % (type(e).__name__, e)}))
'''


class Failed(Exception):
    pass


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def tools_sha256(d=None):
    """The sha256 of every tool file (``r110/*.py``), and of their list."""
    d = d or os.path.dirname(os.path.abspath(__file__))
    files = {n: sha256_file(os.path.join(d, n)) for n in sorted(os.listdir(d)) if n.endswith(".py")}
    tree = hashlib.sha256("".join(f"{s}  {n}\n" for n, s in files.items()).encode()).hexdigest()
    return {"tree": tree, "files": files}


def is_log(rel):
    return rel.startswith(("run/logs/", "fdb/l/")) or rel.endswith(".log")


def _safe_rel(rel):
    parts = rel.split("/")
    if not rel or rel.startswith("/") or any(p in ("", ".", "..") for p in parts):
        raise Failed(f"not a relative path: {rel!r}")
    return os.path.join(*parts)


def _json(out, what):
    try:
        return json.loads(out.strip().splitlines()[-1])
    except (ValueError, IndexError):
        raise Failed(f"{what}: no answer ({len(out)} bytes)") from None


def inventory(call, roots, timeout=600):
    """The host's files: ``call(script, *args, timeout=)`` returns stdout
    or raises. Raises ``Failed`` on anything but an inventory."""
    try:
        inv = _json(call(INVENTORY, json.dumps(roots), timeout=timeout), "inventory")
    except Failed:
        raise
    except Exception as e:  # noqa: BLE001 - a lost connection, a timeout: not an inventory
        raise Failed(f"inventory: {type(e).__name__}: {e}"[:300]) from None
    if not isinstance(inv, dict) or not isinstance(inv.get("files"), list):
        raise Failed("inventory: not an inventory")
    return inv


def _fetch(call, f, target, superseded, cap, chunk, timeout):
    """One attempt at one file: saved and verified, or ``Failed``."""
    try:
        h = _json(call(FILE, "hash", f["abs"], cap, timeout=timeout), "hash")
    except Failed:
        raise
    except Exception as e:  # noqa: BLE001
        raise Failed(f"hash: {type(e).__name__}: {e}"[:300]) from None
    if "error" in h:
        raise Failed(h["error"][:300])
    want = h["range_sha256"]
    if os.path.isfile(target) and sha256_file(target) == want:
        return h, "already saved"
    part = target + ".part"
    os.makedirs(os.path.dirname(target), exist_ok=True)
    try:
        with open(part, "wb") as out:
            off = h["start"]
            while off < h["size"]:
                n = min(chunk, h["size"] - off)
                try:
                    r = _json(call(FILE, "read", f["abs"], off, n, timeout=timeout), "read")
                except Failed:
                    raise
                except Exception as e:  # noqa: BLE001 - a connection lost mid-file
                    raise Failed(f"read at {off}: {type(e).__name__}: {e}"[:300]) from None
                if "error" in r:
                    raise Failed(f"read at {off}: {r['error']}"[:300])
                try:
                    data = base64.b64decode(r["data"], validate=True)
                except (KeyError, ValueError) as e:
                    raise Failed(f"read at {off}: undecodable: {e}"[:300]) from None
                if len(data) != r.get("len") or hashlib.sha256(data).hexdigest() != r.get("sha256"):
                    raise Failed(f"read at {off}: sha256 mismatch in transfer")
                if not data:
                    raise Failed(f"read at {off}: the file shrank")
                out.write(data)
                off += len(data)
            out.flush()
            os.fsync(out.fileno())
        got = sha256_file(part)
        if got != want:
            raise Failed(f"sha256 mismatch: saved {got}, host {want}")
        how = "saved"
        if os.path.exists(target):
            # A copy from an earlier collection that the host's file no
            # longer matches: kept apart, never lost.
            os.makedirs(os.path.dirname(superseded), exist_ok=True)
            os.replace(target, superseded)
            how = "changed"
        os.replace(part, target)
        common.fsync_dir(os.path.dirname(target))
        return h, how
    finally:
        if os.path.exists(part):
            os.unlink(part)


def collect(call, roots, dest, run_id, meta=None, log_cap=MAX_LOG_FILE, total_cap=MAX_LOG_TOTAL,
            deadline_s=COLLECT_S, retries=RETRIES, chunk=CHUNK, clock=time.monotonic, wall_ms=None):
    """Save the host's evidence under ``dest`` (``private/host/<label>/...``)
    and write the manifest and the public summary. Returns the manifest.
    Bounded: ``deadline_s`` for the whole collection, ``retries`` per file,
    every call with its own timeout."""
    wall_ms = wall_ms or (lambda: time.time_ns() // 1_000_000)
    end = clock() + deadline_s
    priv = os.path.join(dest, "private")
    host_dir = os.path.join(priv, "host")
    mpath = os.path.join(priv, "manifest.json")
    prev = None
    try:
        prev = common.read_json(mpath)
    except (OSError, ValueError):
        pass
    attempt = (prev or {}).get("attempt", 0) + 1
    man = {"run_id": run_id, "attempt": attempt, "started_wall_ms": wall_ms(), "complete": False,
           "roots": [[label, d] for label, d, _ in roots], "files": [], "missing": [], "errors": [],
           "excluded": [], "incomplete": [], "meta": meta or {}, "tools": tools_sha256(),
           "history": list((prev or {}).get("history", []))}
    try:
        os.makedirs(host_dir, exist_ok=True)
    except OSError as e:
        man["errors"].append({"path": None, "error": f"local directory: {type(e).__name__}: {e}"[:300]})
        return _finish(man, dest, mpath, wall_ms)
    try:
        inv = inventory(call, roots, timeout=max(1, min(600, int(end - clock()))))
    except Failed as e:
        man["errors"].append({"path": None, "error": str(e)})
        return _finish(man, dest, mpath, wall_ms)
    man["host_roots"] = inv.get("roots", {})
    man["excluded"] = inv.get("excluded", [])
    man["errors"] += inv.get("errors", [])
    found = {f["path"] for f in inv["files"]}
    man["missing"] = [{"path": p, "reason": "absent on the host"} for p in EXPECTED
                      if p not in found and inv.get("roots", {}).get(p.split("/")[0]) == "present"]
    logs = 0
    for f in inv["files"]:
        rel = f["path"]
        if "link" in f:
            man["files"].append({"path": rel, "link": f["link"], "verified": True})
            continue
        cls = "log" if is_log(rel) else "record"
        cap = log_cap if cls == "log" else 0
        take = min(f["size"], cap) if cap else f["size"]
        if cls == "log" and logs + take > total_cap:
            man["errors"].append({"path": rel, "error": f"the logs' total cap ({total_cap} bytes) reached"})
            continue
        try:
            target = os.path.join(host_dir, _safe_rel(rel))
        except Failed as e:
            man["errors"].append({"path": rel, "error": str(e)})
            continue
        last = None
        for _ in range(1 + retries):
            if clock() >= end:
                last = f"the collection's deadline ({deadline_s:.0f} s) reached"
                break
            try:
                h, _how = _fetch(call, f, target, os.path.join(priv, "superseded", f"attempt-{attempt}",
                                                               _safe_rel(rel)),
                                 cap, chunk, max(1, min(CALL_S, int(end - clock()))))
                last = None
                break
            except Failed as e:
                last = str(e)
            except OSError as e:  # the operator's side: PermissionError, no space
                last = f"local: {type(e).__name__}: {e}"[:300]
                break
        if last is not None:
            man["errors"].append({"path": rel, "error": last})
            continue
        saved = h["size"] - h["start"]
        entry = {"path": rel, "class": cls, "size": h["size"], "sha256": h["sha256"], "saved_from": h["start"],
                 "saved_bytes": saved, "saved_sha256": h["range_sha256"], "capped": h["start"] > 0,
                 "verified": True}
        if cls == "log":
            logs += saved
        man["files"].append(entry)
        if entry["capped"]:
            man["incomplete"].append(f"{rel}: capped at {cap} bytes (its last bytes saved)")
    return _finish(man, dest, mpath, wall_ms)


def _finish(man, dest, mpath, wall_ms):
    if man["errors"]:
        man["incomplete"].append(f"{len(man['errors'])} file(s) or reads failed")
    man["complete"] = not man["incomplete"] and "host_roots" in man
    man["ended_wall_ms"] = wall_ms()
    man["history"].append({"attempt": man["attempt"], "complete": man["complete"], "errors": len(man["errors"]),
                           "files": len(man["files"]), "ended_wall_ms": man["ended_wall_ms"]})
    try:
        common.atomic_write_json(mpath, man)
        os.makedirs(os.path.join(dest, "public"), exist_ok=True)
        common.atomic_write_json(os.path.join(dest, "public", "evidence-summary.json"), public_summary(man))
    except OSError as e:
        man["complete"] = False
        man["incomplete"].append(f"the manifest could not be written: {type(e).__name__}")
        man["manifest_error"] = f"{type(e).__name__}: {e}"[:300]
    return man


def public_summary(man):
    """The manifest without host paths, error texts or host identities."""
    files = [{k: f[k] for k in ("path", "class", "size", "sha256", "saved_bytes", "saved_sha256", "capped")
              if k in f} for f in man["files"] if "link" not in f]
    boot = (man.get("meta") or {}).get("host_boot") or {}
    return {"run_id": man["run_id"], "attempt": man["attempt"], "complete": man["complete"],
            "files": files, "bytes_saved": sum(f.get("saved_bytes", 0) for f in files),
            "errors": [e.get("path") for e in man["errors"]], "missing": [m["path"] for m in man["missing"]],
            "excluded": man["excluded"], "capped": [f["path"] for f in files if f.get("capped")],
            "host_rebooted": boot.get("rebooted"), "tools_sha256": man["tools"]["tree"],
            "binary_sha256": (man.get("meta") or {}).get("binary_sha256")}


def verify_local(dest, man):
    """Problems of the saved copies against the manifest (re-read now)."""
    host_dir = os.path.join(dest, "private", "host")
    bad = []
    for f in man.get("files", []):
        if "link" in f:
            continue
        p = os.path.join(host_dir, _safe_rel(f["path"]))
        try:
            if sha256_file(p) != f["saved_sha256"]:
                bad.append(f"{f['path']}: the saved copy's sha256 differs")
        except OSError as e:
            bad.append(f"{f['path']}: {type(e).__name__}")
    return bad


def changed_since(man, inv):
    """Files of the host that differ from what the manifest saved: new,
    gone or changed since (a writer still running)."""
    saved = {f["path"]: f for f in man.get("files", [])}
    now = {f["path"]: f for f in inv["files"]}
    out = [f"{p}: new" for p in sorted(set(now) - set(saved))]
    out += [f"{p}: gone from the host" for p in sorted(set(saved) - set(now))]
    for p in sorted(set(now) & set(saved)):
        a, b = saved[p], now[p]
        if "link" in a or "link" in b:
            if a.get("link") != b.get("link"):
                out.append(f"{p}: changed")
        elif (a["size"], a["sha256"]) != (b["size"], b["sha256"]):
            out.append(f"{p}: changed")
    return out + [f"{e['path']}: unreadable" for e in inv.get("errors", [])]
