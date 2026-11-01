#!/usr/bin/env python3
"""Cache hit accounting per GET pass (RFC-006 §5.3 "Hit accounting"),
from the gateway counter snapshots of two-host.sh runs.

Usage: summarize_hits.py <results-dir>
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from summarize import metric  # noqa: E402

COUNTERS = [
    ("lookup", "lookup_hits"),
    ("hits", "hits"),
    ("degraded", "degraded_hits"),
    ("fb_unread", "fallback_unreadable"),
    ("fb_corrupt", "fallback_corrupt"),
    ("fb_node", "fallback_node"),
    ("fb_group", "fallback_unknown_group"),
    ("misses", "misses"),
    ("populated", "populated"),
    ("skipped", "populations_skipped"),
    ("dedup", "populations_deduplicated"),
    ("rej_size", "admission_rejected_size"),
    ("rej_thresh", "admission_rejected_threshold"),
    ("rej_cap", "admission_rejected_capacity"),
    ("rej_gen", "admission_rejected_generation"),
    ("fail_node", "population_failures_node"),
    ("fail_meta", "population_failures_meta"),
    ("fail_crc", "population_failures_corrupt"),
]


def main():
    root = sys.argv[1]
    print("| label | pass | " + " | ".join(n for n, _ in COUNTERS) + " | readable hit % |")
    print("| --- | --- | " + " | ".join("---" for _ in COUNTERS) + " | --- |")
    for label in sorted(os.listdir(root)):
        d = os.path.join(root, label)
        if not os.path.isdir(d):
            continue
        passes = sorted((int(m.group(1)) for f in os.listdir(d) if (m := re.match(r"metrics-after-get(\d+)\.txt$", f))))
        for p in passes:
            b = os.path.join(d, f"metrics-before-get{p}.txt")
            a = os.path.join(d, f"metrics-after-get{p}.txt")
            vals = []
            for _, name in COUNTERS:
                x = metric(b, f"arca_gateway_cache_{name}_total")
                y = metric(a, f"arca_gateway_cache_{name}_total")
                vals.append(None if x is None or y is None else y - x)
            by = dict(zip((n for n, _ in COUNTERS), vals))
            hits, misses = by.get("hits"), by.get("misses")
            fb = sum(v for k, v in by.items() if k.startswith("fb_") and v is not None)
            ratio = "n/a"
            if hits is not None and misses is not None and hits + misses + fb > 0:
                ratio = f"{100.0 * hits / (hits + misses + fb):.0f}"
            print(f"| {label} | {p} | " + " | ".join("n/a" if v is None else str(v) for v in vals) + f" | {ratio} |")


if __name__ == "__main__":
    main()
