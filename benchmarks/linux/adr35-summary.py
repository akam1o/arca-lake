#!/usr/bin/env python3
"""Summarise one run of adr35-integration.py for publication.

  adr35-summary.py EVIDENCE_RUN_DIR [--binary TEXT] [--harness SHA] > summary.json

Keeps every check (name, result, detail) and the timing tables of C1, C2
and the leader changes, with times relative to each scenario's first
event; drops addresses (any dotted quad), the process part of leader holders, and host names (hosts are the
letters a, b, c of the harness). The run directory's name is kept so that
the summary can be traced back to the raw logs it came from.
"""

import json
import os
import re
import sys

QUAD = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")
# A controller's leader-record holder, `id@<process>`: the process part masked.
HOLDER = re.compile(r"\b(ctl-[a-z])@[0-9a-f]+")


# The harness's hosts (HOST_A..C, `user@host`) masked when set.
HOSTS = [x for h in ("HOST_A", "HOST_B", "HOST_C") for x in (os.environ.get(h) or "").split("@") if x]


def clean(v):
    if isinstance(v, str):
        v = HOLDER.sub(r"\1#<process>", QUAD.sub("<addr>", v))
        for h in HOSTS:
            v = v.replace(h, "<host>")
        return v
    if isinstance(v, list):
        return [clean(x) for x in v]
    if isinstance(v, dict):
        return {k: clean(x) for k, x in v.items()}
    return v


def load(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def rel_timeline(t):
    if not t:
        return None
    rows = [(e["t_ms"], e["controller"], e["text"][:160]) for e in t["controller"]
            if any(w in e["text"] for w in ("acquired leadership", "released leadership", "lost leadership",
                                             "standing by", "test fault", "chunk migration failed",
                                             "in flight", "flags differ"))]
    rows += [(a["t_ms"], a["controller"], f"acknowledged generation {a['generation']} term {a['term']}")
             for a in t["acks"]]
    for g, v in t["gateways"].items():
        prev = None
        for s, c, raw in v["refreshes"]:
            key = raw.split(" lease ")[0] + (" released" if " lease -" in raw else "")
            if key != prev:
                rows.append((c, g, "observed " + raw))
                prev = key
        for tm, rec, age in v["decisions"]:
            rows.append((tm, g, f"recording {str(rec).lower()} (observation {age} ms old)"))
    rows.sort()
    t0 = rows[0][0] if rows else 0
    return [{"t_s": round((tm - t0) / 1000, 3), "who": who, "what": what} for tm, who, what in rows]


def main():
    d = sys.argv[1]
    args = sys.argv[2:]
    opt = {args[i]: args[i + 1] for i in range(0, len(args) - 1, 2)}
    checks = [json.loads(l) for l in open(os.path.join(d, "checks.jsonl"))]
    events = [json.loads(l) for l in open(os.path.join(d, "events.jsonl"))]
    start = next((e for e in events if e["event"] == "start"), {})
    end = next((e for e in reversed(events) if e["event"] == "end"), {})
    out = {
        "run": os.path.basename(d.rstrip("/")),
        "binary": opt.get("--binary"),
        "harness": opt.get("--harness"),
        "scenarios": start.get("scenarios"),
        "clock_offsets_start": start.get("clocks"),
        "clock_offsets_end": end.get("clocks"),
        "system_foundationdb_available": {"start": start.get("system_fdb"), "end": end.get("system_fdb")},
        "checks_passed": sum(c["ok"] for c in checks),
        "checks_failed": sum(not c["ok"] for c in checks),
        "checks": [{k: v for k, v in c.items() if k != "t_ms"} for c in checks],
        "c1_reflection": load(os.path.join(d, "c1-reflection.json")),
        "c2_stale": load(os.path.join(d, "c2-stale.json")),
        "timelines": {lab: rel_timeline(load(os.path.join(d, f"{lab}-timeline.json")))
                      for lab in ("b1", "b3a", "b2")},
        "events": [e for e in events if e["event"].startswith(("A1", "A2", "B2: demotion", "B2: leader", "B3b",
                                                               "admin", "gets", "load"))],
    }
    print(json.dumps(clean(out), indent=1))


if __name__ == "__main__":
    main()
