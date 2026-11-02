#!/usr/bin/env python3
"""Compare a perf_summary.py document against the baseline recorded for the
same environment label and fail on a regression (Phase 2.3 §8).

  compare.py <summary.json> --env <label> [--record] [--baselines DIR]

Baselines live in benchmarks/baselines/<label>.json and are only ever
compared with runs from the same label; a label names a specific set of
hosts and layout, never "CI". With no baseline for the label the run is
reported and, with --record, stored. Tolerances (relative unless noted):

  throughput (put, get)         -15 %
  latency (p50, p95, p99)       +25 %
  CPU-seconds per GiB           +20 %
  RSS                           +20 %
  hit ratio                     -0.05 absolute
  population chunks per second  -25 %
  metadata transactions per s   ±30 % (either direction is a change)
"""

import json
import os
import sys

RULES = {
    "put_mib_s": ("higher", 0.15),
    "get_warm_mib_s": ("higher", 0.15),
    "get_warm_p50_ms": ("lower", 0.25),
    "get_warm_p95_ms": ("lower", 0.25),
    "get_warm_p99_ms": ("lower", 0.25),
    "cpu_s_per_gib_gateway_host": ("lower", 0.20),
    "cpu_s_per_gib_node_host": ("lower", 0.20),
    "rss_mib": ("lower", 0.20),
    "hit_ratio": ("higher-abs", 0.05),
    "population_chunks_per_s": ("higher", 0.25),
    "meta_txn_per_s": ("either", 0.30),
}


def main():
    argv = sys.argv[1:]
    path = argv[0]
    env = argv[argv.index("--env") + 1]
    record = "--record" in argv
    baselines = argv[argv.index("--baselines") + 1] if "--baselines" in argv else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "baselines"
    )
    cur = json.load(open(path, encoding="utf-8"))
    bpath = os.path.join(baselines, f"{env}.json")
    if not os.path.exists(bpath):
        print(f"no baseline for environment '{env}'" + ("; recording" if record else "; nothing to compare"))
        if record:
            os.makedirs(baselines, exist_ok=True)
            json.dump({"env": env, "metrics": cur}, open(bpath, "w", encoding="utf-8"), indent=2)
        return 0
    base = json.load(open(bpath, encoding="utf-8"))["metrics"]
    print(f"| metric | baseline | current | change | tolerance | result |")
    print(f"| --- | --- | --- | --- | --- | --- |")
    bad = 0
    for k, (direction, tol) in RULES.items():
        b, c = base.get(k), cur.get(k)
        if b is None or c is None:
            continue
        if direction == "higher-abs":
            change = c - b
            ok = change >= -tol
            shown = f"{change:+.3f}"
            tolerance = f"-{tol:.2f} abs"
        else:
            if b == 0:
                continue
            rel = (c - b) / b
            shown = f"{rel:+.1%}"
            tolerance = f"{tol:.0%}"
            ok = {
                "higher": rel >= -tol,
                "lower": rel <= tol,
                "either": abs(rel) <= tol,
            }[direction]
        bad += 0 if ok else 1
        print(f"| {k} | {b:.3g} | {c:.3g} | {shown} | {tolerance} | {'ok' if ok else 'REGRESSION'} |")
    if record and bad == 0:
        json.dump({"env": env, "metrics": cur}, open(bpath, "w", encoding="utf-8"), indent=2)
        print(f"baseline for '{env}' updated")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
