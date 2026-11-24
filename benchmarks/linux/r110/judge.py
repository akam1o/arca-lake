"""Judgement of the R-110 retest (plan §8), fixed before a run.

Inputs: the sampler's records (``sampler.py``), the ledger's records
(``ledger.py``), the stage timeline the driver kept (stage -> start and
end, wall ms, and the marks of §8 such as the refusal's start), the run's
stop record, and the judgement values (``VALUES``, the approved ones
stored with the run). Each criterion returns ``Passed``, ``Failed``,
``Inconclusive`` or ``Not run`` with its reasons and the evidence it used.
``Passed`` needs every precondition and every sample it relies on; a
missing one makes it ``Inconclusive``, never a pass.

Order of the outcome of the run: an integrity or protection violation
(a stop of class ``integrity``, a C5 failure, a forbidden counter) is
``Failed`` whatever else happened; then a criterion evaluated and not met
is ``Failed``; then a stop of class ``resource``, ``monitor``, ``limit``,
``anomaly`` or ``operator`` before a criterion was judged makes it
``Inconclusive``; a criterion whose stage never started is ``Not run``.
"""

from . import common, ledger as ledger_mod

PASSED, FAILED, INCONCLUSIVE, NOT_RUN = "Passed", "Failed", "Inconclusive", "Not run"

# The plan version these criteria implement (recorded with every result;
# runs judged under v1 are never re-judged under it).
PLAN_VERSION = 3
# The 0.2.0 campaign's amendments of v3 (docs/verification/0.2.0-r110-plan.md
# §4), applied to a timeline the driver marked with ``campaign``.
CAMPAIGN = "0.2.0"

VALUES = {
    "interval_s": 15,
    "max_gap_factor": 2.0,  # a gap above this many intervals breaks a window
    "c1_records_per_object": 3,  # shards of one 2+1 object
    "c2_reclaimable_max_segments": 2,
    "c2_reclaimable_max_fraction": 0.10,
    "c2_available_fraction_of_freed": 0.5,
    "c2_freed_fraction_of_expected": 0.9,
    "c2_hold_s": 600,
    "c2_du_tolerance": 0.05,
    # D1's chain (plan §7 stage 4: the GC grace, a GC interval, the
    # retention and a reclaim interval after D1, about 25 min): C1 is
    # judged on progress only once the refusal was observed that far.
    "d1_chain_s": 1500,
    # Plan v2 §8.3: C1a needs at least this long a refusal, and this share
    # of D1's records reclaimed during it; C1b, all of D1's records
    # reclaimed within this of D1's last delete (twice D1's chain).
    "c1a_min_refusal_s": 600,
    "c1a_min_fraction": 0.1,
    "c1b_deadline_s": 3000,
    "c3a_hold_s": 600,
    "c3a_probe_interval_s": 10,
    "c3b_ack_fraction": 0.99,
    "c4_window_s": 3600,
    "c4_slope_fraction_per_hour": 0.01,
    "c4_range_fraction": 0.10,
    "segment_bytes": 64 << 20,
    "allotment_bytes": 1 << 30,
    "shard_bytes_per_object": 3 * (128 << 10),  # 256 KiB object, 2+1, one chunk
}


def verdict(name, outcome, reasons=(), **evidence):
    return {"criterion": name, "outcome": outcome, "reasons": list(reasons), "evidence": evidence}


# -- samples ------------------------------------------------------------------


def series(samples, target, key, t0=None, t1=None):
    """``[(wall_ms, value)]`` of ``key`` in ``target``'s samples between
    ``t0`` and ``t1`` (inclusive). A value counts when it was read: in an
    ``ok`` sample, or a ``partial`` one (another family absent from the
    same scrape); samples in error, or lacking the key, are left out (the
    coverage checks then see the gap)."""
    out = []
    for s in samples:
        if s.get("target") != target or s.get("status") not in ("ok", "partial"):
            continue
        t = s["ended_wall_ms"]
        if (t0 is not None and t < t0) or (t1 is not None and t > t1):
            continue
        v = s["values"].get(key)
        if v is not None:
            out.append((t, v["value"]))
    return out


def covered(points, t0, t1, interval_ms, factor):
    """Whether ``points`` cover [t0, t1] with no gap above ``factor``
    intervals (the ends included)."""
    if not points:
        return False
    ts = [t0] + [t for t, _ in points] + [t1]
    return all(b - a <= factor * interval_ms for a, b in zip(ts, ts[1:]))


def at(points, t, interval_ms):
    """The value of the sample nearest ``t`` within one interval, or None."""
    best = None
    for pt, v in points:
        d = abs(pt - t)
        if d <= interval_ms and (best is None or d < best[0]):
            best = (d, v)
    return None if best is None else best[1]


def at_or_before(points, t, interval_ms):
    """The last sample at or before ``t``, at most one interval earlier."""
    best = None
    for pt, v in points:
        if t - interval_ms <= pt <= t and (best is None or pt > best[0]):
            best = (pt, v)
    return None if best is None else best[1]


def slope_per_hour(points):
    n = len(points)
    if n < 2:
        return None
    xs = [t / 3_600_000 for t, _ in points]
    ys = [v for _, v in points]
    mx, my = sum(xs) / n, sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den


# -- criteria ---------------------------------------------------------------


def _monotone(points):
    """A counter series with no decrease (a decrease is a restart or reset:
    the increase over it is unknown)."""
    return all(b >= a for (_, a), (_, b) in zip(points, points[1:]))


def _increase(samples, target, key, t0, t1, iv, gap):
    """The increase of a counter over [t0, t1] from samples covering it, or
    None (not covered, or reset)."""
    pts = series(samples, target, key, t0, t1)
    if not covered(pts, t0, t1, iv, gap) or not _monotone(pts):
        return None
    return pts[-1][1] - pts[0][1]


def _increase_outer(samples, target, key, t0, t1, iv, gap):
    """The increase of a counter from its last sample at or before ``t0``
    to its first at or after ``t1`` (each within the gap), or None: an
    upper bound of what happened over [t0, t1]."""
    pts = series(samples, target, key)
    before = [p for p in pts if p[0] <= t0 and t0 - p[0] <= gap * iv]
    after = [p for p in pts if p[0] >= t1 and p[0] - t1 <= gap * iv]
    if not before or not after:
        return None
    a, b = before[-1], after[0]
    inner = [p for p in pts if a[0] <= p[0] <= b[0]]
    if not covered(inner, a[0], b[0], iv, gap) or not _monotone(inner):
        return None
    return b[1] - a[1]


QUEUE_REFUSALS = ("arca_gateway_commit_queue_refused_full_total", "arca_gateway_commit_queue_refused_wait_total")


def _queue_refusals(samples, t0, t1, iv, gap):
    """0.2.0 plan §4.3: PUTs the gateway's commit queue answered SlowDown
    over [t0, t1] (from the samples just outside it, an upper bound), or
    None when not sampled. Such a PUT streamed its body (its shards
    stored) before its turn was refused; its pending generation is
    aborted, and GC retires its shards like an aborted write's."""
    incs = [_increase_outer(samples, "gateway0", k, t0, t1, iv, gap) for k in QUEUE_REFUSALS]
    return None if any(x is None for x in incs) else sum(incs)


def d1_reclaimed_bound(samples, t0, t1, v, sub_from=None, queue=False):
    """A lower bound of D1's records reclaimed over [t0, t1] (plan v2 §8.3):
    the reclaimed records, less those reconciliation's orphans could account
    for (each removed orphan one record), less 3 per shard write a node
    refused for capacity (each marks at most one aborted object, whose at
    most 3 shards GC retires). The reclaims are counted from the samples
    inside the interval (a lower bound), what is subtracted from the
    samples just outside it (an upper bound), so that the bound stays a
    lower bound at the edges. ``sub_from``: the subtractions start there
    instead (an earlier time whose aborted writes or orphans can be
    reclaimed inside the interval). ``queue`` (0.2.0 campaign): also less 3
    per PUT the commit queue refused (``_queue_refusals``). ``(bound,
    parts)``; bound None when a part is not sampled over the interval."""
    s0 = t0 if sub_from is None else min(sub_from, t0)
    iv, gap = v["interval_s"] * 1000, v["max_gap_factor"]
    parts = {"reclaimed": _increase(samples, "controller", "arca_controller_reclaimed_total", t0, t1, iv, gap),
             "reconcile_deleted": _increase_outer(samples, "controller-log", "reconcile_deleted", s0, t1, iv, gap),
             "node_capacity_refusals": _increase_outer(samples, "gateway0",
                                                       "arca_gateway_node_capacity_refusals_total", s0, t1, iv, gap)}
    if queue:
        parts["commit_queue_refusals"] = _queue_refusals(samples, s0, t1, iv, gap)
    if any(x is None for x in parts.values()):
        return None, parts
    return parts["reclaimed"] - parts["reconcile_deleted"] - 3 * parts["node_capacity_refusals"] \
        - 3 * parts.get("commit_queue_refusals", 0), parts


def _d1(records, tl):
    s4 = tl.get("4") or {}
    return len(ledger_mod.acknowledged(records, "DELETE", stage="4")), s4


def c1a(samples, records, tl, v, nodes):
    """C1a, plan v2: GC and reclaim make progress on D1 while writes are
    refused. Over the observed refusal (at least ``c1a_min_refusal_s``;
    no progress fails only on a refusal lasting past D1's chain):
    GC collected generations, D1's reclaimed records (the lower bound)
    reached ``c1a_min_fraction`` of D1's records, and neither reclaim
    refusals nor reclaim errors rose."""
    d1, s4 = _d1(records, tl)
    if not s4 or "refusal_start_ms" not in s4 or s4.get("refusal_start_ms") is None:
        return verdict("C1a", INCONCLUSIVE if s4 else NOT_RUN, ["no refusal formed in stage 4"] if s4 else [])
    t_r, t_e = s4["refusal_start_ms"], s4.get("refusal_end_ms") or s4.get("end_ms")
    if t_e is None:
        return verdict("C1a", INCONCLUSIVE, ["the refusal's end not recorded"])
    if d1 == 0:
        return verdict("C1a", INCONCLUSIVE, ["no acknowledged delete of D1"])
    iv, gap = v["interval_s"] * 1000, v["max_gap_factor"]
    ev = {"d1": d1, "refusal_s": (t_e - t_r) / 1000, "refusal_ended_by": s4.get("refusal_ended_by")}
    reasons = []
    for k in ("arca_controller_reclaim_refused_total", "arca_controller_reclaim_errors_total"):
        inc = _increase(samples, "controller", k, t_r, t_e, iv, gap)
        if inc is None:
            return verdict("C1a", INCONCLUSIVE, [f"{k} not sampled through the refusal"], **ev)
        if inc > 0:
            reasons.append(f"{k} rose during the refusal")
    if reasons:  # an observed violation fails it whatever else
        return verdict("C1a", FAILED, reasons, **ev)
    if (t_e - t_r) < v["c1a_min_refusal_s"] * 1000:
        return verdict("C1a", INCONCLUSIVE, [f"the refusal lasted {(t_e - t_r) / 1000:.0f} s, "
                                             f"{v['c1a_min_refusal_s']} s needed"], **ev)
    collected = _increase(samples, "controller", "arca_controller_gc_collected_total", t_r, t_e, iv, gap)
    # Writes a node refused during the fill, before admission showed the
    # refusal, are reclaimed inside it: subtracted from D1's end on.
    bound, parts = d1_reclaimed_bound(samples, t_r, t_e, v, sub_from=s4.get("d1_done_ms"),
                                      queue=tl.get("campaign") == CAMPAIGN)
    ev.update(gc_collected=collected, d1_reclaimed_at_least=bound, **{f"reclaim_{k}": x for k, x in parts.items()})
    if collected is None or bound is None:
        return verdict("C1a", INCONCLUSIVE, ["GC or reclaim counters not sampled through the refusal"], **ev)
    need = v["c1a_min_fraction"] * v["c1_records_per_object"] * d1
    ev["d1_reclaimed_needed"] = need
    if collected > 0 and bound >= need:
        return verdict("C1a", PASSED, [], **ev)
    if bound < need <= parts["reclaimed"]:
        return verdict("C1a", INCONCLUSIVE, ["reclaim progress not attributable to D1 (orphans or aborted "
                                             "writes in the same counter)"], **ev)
    # No progress shown: a failure only once D1's reclaim was due (its chain
    # after D1's last delete) while writes were still refused; a refusal
    # that ended before shows nothing either way (independent review).
    t_d = s4.get("d1_done_ms")
    if t_d is None or t_e < t_d + v["d1_chain_s"] * 1000:
        return verdict("C1a", INCONCLUSIVE, ["the refusal ended before D1's reclaim was due, with no progress "
                                             "shown"], **ev)
    if collected <= 0:
        reasons.append("no GC collection during the refusal")
    if bound < need:
        reasons.append(f"at least {bound} of D1's records reclaimed during the refusal, {need:.0f} needed")
    return verdict("C1a", FAILED if reasons else PASSED, reasons, **ev)


def c1b(samples, records, tl, v, nodes):
    """C1b, plan v2: D1 is fully reclaimed within ``c1b_deadline_s`` of
    D1's last delete, whether the refusal held or admission recovered: at
    some point by the deadline the GC backlog is 0, every node's pending
    records are 0, and D1's reclaimed records (the lower bound, from D1's
    end) reach all of D1's records."""
    d1, s4 = _d1(records, tl)
    if not s4:
        return verdict("C1b", NOT_RUN)
    t_d = s4.get("d1_done_ms")
    if t_d is None or d1 == 0:
        return verdict("C1b", INCONCLUSIVE, ["D1's deletes not recorded"])
    deadline = t_d + v["c1b_deadline_s"] * 1000
    t_q = s4.get("d1_quiet_ms")  # when the driver saw D1's chain over
    end = s4.get("end_ms")
    iv, gap = v["interval_s"] * 1000, v["max_gap_factor"]
    ev = {"d1": d1, "deadline_ms": deadline, "quiet_ms": t_q}
    reasons, unsampled = [], []
    for k in ("arca_controller_reclaim_refused_total", "arca_controller_reclaim_errors_total"):
        inc = _increase(samples, "controller", k, t_d, min(t_q or end or deadline, deadline), iv, gap)
        if inc is None:
            unsampled.append(k)
        elif inc > 0:
            reasons.append(f"{k} rose after D1")
    if reasons:
        return verdict("C1b", FAILED, reasons, **ev)
    if unsampled:
        return verdict("C1b", INCONCLUSIVE, [f"{k} not sampled after D1" for k in unsampled], **ev)
    if t_q is None:
        if end is not None and end >= deadline:
            # Observed to the deadline without D1's chain over.
            return verdict("C1b", FAILED, ["D1 not fully reclaimed by the deadline"], **ev)
        return verdict("C1b", INCONCLUSIVE, ["stage 4 ended before D1's chain was over or the deadline"], **ev)
    if t_q > deadline:
        return verdict("C1b", FAILED, [f"D1's chain over at {t_q}, after the deadline {deadline}"], **ev)
    if t_q < t_d + v["d1_chain_s"] * 1000:
        return verdict("C1b", INCONCLUSIVE, ["the quiet point recorded before D1's chain time"], **ev)
    # The quiet point itself, read again from the samples (fresh, every node).
    gc = at_or_before(series(samples, "controller", "arca_controller_gc_backlog"), t_q, iv)
    pend = {n: at_or_before(series(samples, n, "arca_node_reclaim_pending_records"), t_q, iv) for n in nodes}
    if gc is None or any(p is None for p in pend.values()):
        return verdict("C1b", INCONCLUSIVE, ["GC backlog or pending records not sampled at the quiet point"], **ev)
    if gc != 0 or any(p != 0 for p in pend.values()):
        return verdict("C1b", INCONCLUSIVE, ["the quiet point not confirmed by the samples"], **ev)
    # The controller counts a reclaim pass after it went over every node: a
    # node can read 0 pending before that pass is counted, so the count is
    # read up to two intervals after the quiet point (independent review).
    bound, parts = d1_reclaimed_bound(samples, t_d, t_q + 2 * iv, v, queue=tl.get("campaign") == CAMPAIGN)
    need = v["c1_records_per_object"] * d1
    ev.update(d1_reclaimed_at_least=bound, d1_reclaimed_needed=need, **{f"reclaim_{k}": x for k, x in parts.items()})
    if bound is None:
        return verdict("C1b", INCONCLUSIVE, ["reclaim counters not sampled from D1 to the quiet point"], **ev)
    if bound >= need:
        return verdict("C1b", PASSED, [], **ev)
    if parts["reclaimed"] >= need:
        return verdict("C1b", INCONCLUSIVE, ["D1's reclaim not attributable (orphans or aborted writes in the "
                                             "same counter)"], **ev)
    # Nothing pending, nothing in the GC backlog, and fewer reclaims than
    # D1's records: retirements lost or never made.
    return verdict("C1b", FAILED, [f"{parts['reclaimed']} records reclaimed by the quiet point, {need} needed"], **ev)


def c2(samples, records, tl, v, nodes):
    """Physical return through compaction in stage 5 (plan §8 C2)."""
    s5 = tl.get("5")
    if not s5:
        return verdict("C2", NOT_RUN)
    t_s, t_j = s5["start_ms"], s5.get("c2_judge_ms")
    if t_j is None:
        return verdict("C2", INCONCLUSIVE, ["the chain of retention, reclaim and compaction did not complete in stage 5"])
    iv = v["interval_s"] * 1000
    hold_end = t_j + v["c2_hold_s"] * 1000
    if hold_end > s5.get("end_ms", 0):
        return verdict("C2", INCONCLUSIVE, ["stage 5 ended before the 10 min hold"])
    deletes = len(ledger_mod.acknowledged(records, "DELETE", stage="5"))
    expected = deletes * v["shard_bytes_per_object"]
    # D1 told apart: its chain must be over at the baseline (no GC backlog,
    # no pending record on any node), else its reclaim would count here.
    gc0 = at_or_before(series(samples, "controller", "arca_controller_gc_backlog"), t_s, iv)
    if gc0 is None:
        return verdict("C2", INCONCLUSIVE, ["no GC backlog sample at stage 5's start"])
    if gc0 > 0:
        return verdict("C2", INCONCLUSIVE, ["D1 still in the GC backlog at stage 5's start"], gc_backlog=gc0)
    for n in nodes:
        p0 = at_or_before(series(samples, n, "arca_node_reclaim_pending_records"), t_s, iv)
        if p0 is None:
            return verdict("C2", INCONCLUSIVE, [f"{n}: no pending-records sample at stage 5's start"])
        if p0 > 0:
            return verdict("C2", INCONCLUSIVE, [f"{n}: D1 records still pending at stage 5's start"])
    # Probe PUTs (acknowledged or unknown) add live bytes during stage 5; a
    # node holds at most one shard of an object, so adding one shard per
    # probe to each node's decrease bounds its freed bytes from above (the
    # stricter threshold).
    probe_puts = sum(1 for r in _attempts(records, "5", "probe", "PUT", t_s, t_j) if r != "refused")
    queued = None
    if tl.get("campaign") == CAMPAIGN:
        # 0.2.0 plan §4.3: a probe the commit queue refused had stored its
        # shards first. With any queue refusal in stage 5 (or none known),
        # every refused probe counts as added too: the stricter bound.
        queued = _queue_refusals(samples, t_s, t_j, iv, v["max_gap_factor"])
        if queued is None or queued > 0:
            probe_puts = len(_attempts(records, "5", "probe", "PUT", t_s, t_j))
    shard = v["shard_bytes_per_object"] // 3
    reasons, per, freed_total = [], {}, 0
    for n in nodes:
        k = lambda kind: f'arca_node_capacity_bytes{{kind="{kind}"}}'  # noqa: E731
        live = series(samples, n, k("live"))
        avail = series(samples, n, k("available"))
        alloc = series(samples, n, k("allocated"))
        recl = series(samples, n, k("reclaimable"))
        seg = series(samples, n, 'arca_node_device_usage_bytes{kind="segments"}')
        side = series(samples, n, 'arca_node_device_usage_bytes{kind="side_files"}')
        du = series(samples, f"{n}-dir", "allocated_bytes")
        # The baseline is taken before stage 5's first delete.
        base_live, base_avail = at_or_before(live, t_s, iv), at_or_before(avail, t_s, iv)
        if base_live is None or base_avail is None:
            return verdict("C2", INCONCLUSIVE, [f"{n}: no baseline sample at stage 5's start"])
        window = [(t, x) for t, x in avail if t_j <= t <= hold_end]
        if not covered(window, t_j, hold_end, iv, v["max_gap_factor"]):
            return verdict("C2", INCONCLUSIVE, [f"{n}: the hold not sampled"])
        live_j = at(live, t_j, iv)
        if live_j is None:
            return verdict("C2", INCONCLUSIVE, [f"{n}: no live sample at the judgement"])
        decrease = base_live - live_j
        freed_total += decrease
        freed = decrease + probe_puts * shard
        info = {"live_decrease": decrease, "freed_bound": freed, "available_gain_min": None}
        gains = []
        for t, a in window:
            gains.append(a - base_avail)
            al, rc = at(alloc, t, iv), at(recl, t, iv)
            if al is None or rc is None:
                return verdict("C2", INCONCLUSIVE, [f"{n}: allocated or reclaimable missing in the hold"])
            limit = max(v["c2_reclaimable_max_segments"] * v["segment_bytes"], v["c2_reclaimable_max_fraction"] * al)
            if rc > limit:
                reasons.append(f"{n}: reclaimable {rc} above {limit:.0f} at {t}")
                break
            sg, sd, d = at(seg, t, iv), at(side, t, iv), at(du, t, iv)
            if None in (sg, sd, d):
                return verdict("C2", INCONCLUSIVE, [f"{n}: segment, side-file or du sample missing in the hold"])
            if abs(d - (sg + sd)) > v["c2_du_tolerance"] * (sg + sd):
                reasons.append(f"{n}: du {d} not within 5 % of segments and side files {sg + sd}")
                break
        info["available_gain_min"] = min(gains)
        if min(gains) < v["c2_available_fraction_of_freed"] * freed:
            reasons.append(f"{n}: available up {min(gains)} at least, half of {freed} freed needed")
        per[n] = info
    if freed_total < v["c2_freed_fraction_of_expected"] * expected:
        reasons.append(f"live bytes down {freed_total}, {expected} expected from {deletes} deletes")
    # Partial-PUT leftovers and orphans removed by reconciliation also free
    # space: recorded as an approximate attribution, never subtracted.
    approx = []
    ref = series(samples, "gateway0", "arca_gateway_node_capacity_refusals_total", t_s, hold_end)
    if ref and ref[-1][1] > ref[0][1]:
        approx.append("PUTs refused by a node after writing shards during stage 5")
    if queued:
        approx.append(f"{queued:.0f} PUTs refused by the commit queue after writing shards during stage 5")
    rec_del = series(samples, "controller-log", "reconcile_deleted", t_s, hold_end)
    if not rec_del:
        approx.append("reconcile counts not sampled")
    elif rec_del[-1][1] > rec_del[0][1]:
        approx.append(f"reconciliation removed {rec_del[-1][1] - rec_del[0][1]} orphans")
    return verdict("C2", FAILED if reasons else PASSED, reasons, deletes=deletes, expected_bytes=expected,
                   live_decrease_bytes=freed_total, probe_puts=probe_puts, nodes=per,
                   approximate_attribution=approx)


def _admission_window(samples, t0, t1, v, gateways):
    """Every configured gateway's admission state and the controller's
    backpressure state over [t0, t1], or None if any is not covered."""
    iv = v["interval_s"] * 1000
    out = {}
    for g in list(gateways) + ["controller"]:
        key = "arca_controller_backpressure_state" if g == "controller" else "arca_gateway_admission_state"
        pts = series(samples, g, key, t0, t1)
        if not covered(pts, t0, t1, iv, v["max_gap_factor"]):
            return None
        out[g] = pts
    return out


def c3a(samples, records, tl, v, gateways=("gateway0",)):
    s5 = tl.get("5")
    if not s5:
        return verdict("C3a", NOT_RUN)
    t0 = s5.get("c3a_window_start_ms")
    if t0 is None:
        return verdict("C3a", INCONCLUSIVE, ["admission never 0 for the hold within stage 5"])
    t1 = t0 + v["c3a_hold_s"] * 1000
    w = _admission_window(samples, t0, t1, v, gateways)
    if w is None:
        return verdict("C3a", INCONCLUSIVE, ["the window not sampled"])
    reasons = [f"{g} not 0 in the window" for g, pts in w.items() if any(x != 0 for _, x in pts)]
    probes = _attempts(records, "5", "probe", "PUT", t0, t1)
    need = int(v["c3a_hold_s"] // v["c3a_probe_interval_s"])
    if len(probes) < need:
        return verdict("C3a", INCONCLUSIVE, [f"{len(probes)} probe PUTs in the window, {need} needed"])
    bad = [p for p in probes if p not in ("ack", "no-outcome")]
    if bad:
        reasons.append(f"{len(bad)} of {len(probes)} probe PUTs not acknowledged")
    elif not reasons and "no-outcome" in probes:
        # A probe cut by a stop proves neither: never a pass, not a failure.
        return verdict("C3a", INCONCLUSIVE, [f"{probes.count('no-outcome')} probe PUTs without an outcome"],
                       probes=len(probes))
    return verdict("C3a", FAILED if reasons else PASSED, reasons, probes=len(probes), window=[t0, t1])


def _attempts(records, stage, kind, op, t0=None, t1=None):
    """The results of the attempts of ``op`` (and ``kind``) in ``stage``
    whose intent falls in [t0, t1]; an intent without an outcome is
    ``no-outcome`` (never left out: it is neither acknowledged nor shown
    to have failed)."""
    outcomes = {(r["op_id"], r["attempt"]): r["result"] for r in records if r.get("type") == "outcome"}
    out = []
    for i in records:
        if i.get("type") != "intent":
            continue
        if i.get("stage") != stage or i["op"] != op or (kind and i.get("kind") != kind):
            continue
        if (t0 is not None and i["wall_ms"] < t0) or (t1 is not None and i["wall_ms"] > t1):
            continue
        out.append(outcomes.get((i["op_id"], i["attempt"]), "no-outcome"))
    return out


def c3b(samples, records, tl, v, gateways=("gateway0",)):
    s6 = tl.get("6")
    if not s6:
        return verdict("C3b", NOT_RUN)
    if not s6.get("completed"):
        return verdict("C3b", INCONCLUSIVE, ["stage 6 did not run its full time"])
    w = _admission_window(samples, s6["start_ms"], s6["end_ms"], v, gateways)
    if w is None:
        return verdict("C3b", INCONCLUSIVE, ["stage 6 not sampled"])
    puts = _attempts(records, "6", "op", "PUT") + _attempts(records, "6", "retry", "PUT")
    if not puts:
        return verdict("C3b", INCONCLUSIVE, ["no PUT in stage 6"])
    ops = _attempts(records, "6", "op", "PUT")
    acked = sum(1 for r in ops if r == "ack")
    open_ = sum(1 for r in ops if r == "no-outcome")
    refused = sum(1 for r in puts if r == "refused")
    reasons = []
    if acked < v["c3b_ack_fraction"] * len(ops):
        if acked + open_ >= v["c3b_ack_fraction"] * len(ops) and not refused:
            # Met or not depending on attempts without an outcome.
            return verdict("C3b", INCONCLUSIVE, [f"{open_} PUTs without an outcome decide the fraction"],
                           puts=len(ops), acked=acked)
        reasons.append(f"{acked} of {len(ops)} PUTs acknowledged")
    if refused:
        reasons.append(f"{refused} PUT attempts refused")
    for g, pts in w.items():
        if any(x > 1 for _, x in pts):
            reasons.append(f"{g} above warning in stage 6")
    return verdict("C3b", FAILED if reasons else PASSED, reasons, puts=len(ops), acked=acked)


def c4(samples, records, tl, v, nodes, gateways=("gateway0",)):
    s6 = tl.get("6")
    if not s6:
        return verdict("C4", NOT_RUN)
    if not s6.get("completed"):
        return verdict("C4", INCONCLUSIVE, ["stage 6 did not run its full time"])
    t1 = s6["end_ms"]
    t0 = t1 - v["c4_window_s"] * 1000
    iv = v["interval_s"] * 1000
    reasons, ev = [], {}
    a = v["allotment_bytes"]
    for n in nodes:
        for key in ("arca_node_reclaim_retained_bytes", 'arca_node_capacity_bytes{kind="allocated"}'):
            pts = series(samples, n, key, t0, t1)
            if not covered(pts, t0, t1, iv, v["max_gap_factor"]):
                return verdict("C4", INCONCLUSIVE, [f"{n}: {key} not sampled over the window"])
            sl = slope_per_hour(pts)
            rng = max(x for _, x in pts) - min(x for _, x in pts)
            ev[f"{n} {key}"] = {"slope_per_hour": sl, "range": rng}
            if sl is None or sl > v["c4_slope_fraction_per_hour"] * a:
                reasons.append(f"{n}: {key} slope {sl} per hour")
            if rng > v["c4_range_fraction"] * a:
                reasons.append(f"{n}: {key} range {rng}")
    w = _admission_window(samples, t0, t1, v, gateways)
    if w is None:
        return verdict("C4", INCONCLUSIVE, ["admission not sampled over the window"], **ev)
    for g, pts in w.items():
        if any(x >= 3 for _, x in pts):
            reasons.append(f"{g} write rejected in the window")
    return verdict("C4", FAILED if reasons else PASSED, reasons, **ev)


def c5(tl):
    s7 = tl.get("7")
    if not s7:
        return verdict("C5", NOT_RUN)
    vr, mf, sc = s7.get("verify"), s7.get("manifest_findings"), s7.get("scrub_findings")
    if vr is None:
        return verdict("C5", INCONCLUSIVE, ["the verification did not run"])
    if vr.get("violations"):
        return verdict("C5", FAILED, [f"{len(vr['violations'])} keys differ from every candidate"],
                       violations=vr["violations"][:20])
    if mf is not None and mf > 0:
        return verdict("C5", FAILED, [f"manifest findings {mf}, scrub findings {sc}"])
    held = s7.get("scrub_held")
    if tl.get("campaign") == CAMPAIGN:
        return _c5_campaign(s7, vr, mf, sc, held)
    if sc is not None and sc > 0 and isinstance(held, dict):
        # 0.2.0 binary: the scrub classifies each shard itself (records kept
        # for reclaim are not findings), so every finding fails, settled or
        # not; no count-based excuse.
        return verdict("C5", FAILED, [f"manifest findings {mf}, scrub findings {sc}"],
                       scrub_by_class=s7.get("scrub_by_class"), scrub_held=held)
    if sc is not None and sc > 0:
        # A binary before 0.2.0 (no per-shard classes printed): the rule of
        # plan v3, kept only to judge such timelines as they were judged.
        by = s7.get("scrub_by_class")
        # Plan v3: a scrub read before the background passes settled lists
        # the records retained for reclaim as unexpected shards (class 4):
        # those alone, no more of them than the records pending reclaim just
        # before the scrub, are Inconclusive there; any other class, or more
        # unexpected shards, fails. Settled (or a timeline without the settle
        # step), every finding fails.
        pend = s7.get("pending_before_scrub")
        if "settled_ms" in s7 and s7["settled_ms"] is None and isinstance(by, list) and len(by) == 8 \
                and sum(by) == sc and sum(by) == by[4] and isinstance(pend, (int, float)) and by[4] <= pend:
            return verdict("C5", INCONCLUSIVE, [f"{sc} unexpected shards, at most the {pend:.0f} records pending "
                                                "reclaim, the scrub read before the background passes settled"],
                           scrub_by_class=by, pending_before_scrub=pend)
        return verdict("C5", FAILED, [f"manifest findings {mf}, scrub findings {sc}"], scrub_by_class=by,
                       pending_before_scrub=pend)
    if not vr.get("complete") or mf is None or sc is None:
        return verdict("C5", INCONCLUSIVE, ["the verification, the manifests or the scrub incomplete"])
    if isinstance(held, dict) and held.get("unclassified", 0) > 0:
        return verdict("C5", INCONCLUSIVE, [f"{held['unclassified']} shards not classified by the scrub"],
                       scrub_held=held)
    return verdict("C5", PASSED, [], keys=vr["keys"], unknown_resolved=vr.get("unknown_resolved"),
                   **({"scrub_held": held} if held is not None else {}))


def _c5_campaign(s7, vr, mf, sc, held):
    """C5's scrub part in the 0.2.0 campaign (0.2.0 plan §4.1): the scrub
    classifies each shard; ``retained`` and ``in_flight`` shards are not
    findings, every finding (an orphan's unexpected shard, any damage,
    damaged retained or in-flight records under their damage class) fails,
    settled or not, also when another group's scrub reported errors; scrub
    errors (``unclassified`` shards among them), a group without a result,
    or a scrub that printed no classes (not a 0.2.0 binary) leave C5
    Inconclusive, never Passed. v3's count rule does not apply."""
    by = s7.get("scrub_by_class")
    found = sum(by) if isinstance(by, list) else sc
    ev = {"scrub_by_class": by, "scrub_held": held, "scrub_errors": s7.get("scrub_errors"),
          "scrub_unread_groups": s7.get("scrub_unread_groups")}
    if found is not None and found > 0:
        return verdict("C5", FAILED, [f"manifest findings {mf}, scrub findings {found}"], **ev)
    if not vr.get("complete") or mf is None or sc is None:
        return verdict("C5", INCONCLUSIVE, ["the verification, the manifests or the scrub incomplete"], **ev)
    if not isinstance(held, dict):
        return verdict("C5", INCONCLUSIVE, ["the scrub printed no per-shard classes (0.2.0 plan §4.1)"], **ev)
    if held.get("unclassified", 0) > 0:
        return verdict("C5", INCONCLUSIVE, [f"{held['unclassified']} shards not classified by the scrub"], **ev)
    return verdict("C5", PASSED, [], keys=vr["keys"], unknown_resolved=vr.get("unknown_resolved"), **ev)


CRITERIA_STAGE = {"C1a": "4", "C1b": "4", "C2": "5", "C3a": "5", "C3b": "6", "C4": "6", "C5": "7"}


def _run_span(tl):
    """[first start, last end] of the stages with load (3 to 7) that ran."""
    ts = [tl[s] for s in ("3", "4", "5", "6", "7") if s in tl and "start_ms" in tl[s]]
    if not ts:
        return None
    return ts[0]["start_ms"], max(t.get("end_ms", t["start_ms"]) for t in ts)


def judge(samples, records, tl, stop=None, values=None, nodes=("node0", "node1", "node2", "node3"),
          gateways=("gateway0",)):
    """Every criterion and the run's outcome (see the module's order)."""
    v = dict(VALUES, **(values or {}))
    res = [c1a(samples, records, tl, v, nodes), c1b(samples, records, tl, v, nodes),
           c2(samples, records, tl, v, nodes),
           c3a(samples, records, tl, v, gateways), c3b(samples, records, tl, v, gateways),
           c4(samples, records, tl, v, nodes, gateways), c5(tl)]
    forbidden, unwatched = [], []
    watch = [("controller", "arca_controller_gc_skipped_referenced_total")] + \
            [(n, "arca_node_reclaim_damaged") for n in nodes]
    # 0.2.0 plan §4.2: a cluster 0.2.0 initialised is strict from its first
    # write (ADR-0037): no node takes a write without proof of its cluster,
    # and every node is strict over the run's stages with load.
    strict = [(n, "arca_node_write_ownership_strict") for n in nodes] if tl.get("campaign") == CAMPAIGN else []
    if strict:
        watch += [(n, "arca_node_writes_unproven_total") for n in nodes]
    span = _run_span(tl)
    for target, k in watch + strict:
        pts = series(samples, target, k)
        if (target, k) in strict:
            if span and any(x != 1 for t, x in pts if span[0] <= t <= span[1]):
                forbidden.append(f"{target} {k} not 1")
        elif any(x > 0 for _, x in pts):
            forbidden.append(f"{target} {k}")
        # A protection counter not sampled over the run cannot show it held.
        if span and not covered([p for p in pts if span[0] <= p[0] <= span[1]], span[0], span[1],
                                v["interval_s"] * 1000, v["max_gap_factor"]):
            unwatched.append(f"{target} {k}")
    cls = common.reason_class(stop["reason"]) if stop else None
    if stop and cls != "integrity":
        stop_stage = stop.get("stage")
        for r in res:
            st = CRITERIA_STAGE[r["criterion"]]
            if r["outcome"] in (PASSED, FAILED):
                continue
            if stop_stage is not None and int(st) > int(stop_stage):
                r["outcome"] = NOT_RUN
                r["reasons"].append(f"stopped in stage {stop_stage}: {stop['reason']}")
            elif r["outcome"] != NOT_RUN:
                r["outcome"] = INCONCLUSIVE
                r["reasons"].append(f"stopped: {stop['reason']}")
    if cls == "integrity" or forbidden or any(r["outcome"] == FAILED and r["criterion"] == "C5" for r in res):
        overall = FAILED
    elif any(r["outcome"] == FAILED for r in res):
        overall = FAILED
    elif all(r["outcome"] == PASSED for r in res) and not stop and not unwatched:
        # A stop that is not an integrity one (plan §8: a stop by the
        # watchdog, the guard or a limit, a lost monitor) makes the run
        # Inconclusive even when every criterion was met before it.
        overall = PASSED
    elif all(r["outcome"] == NOT_RUN for r in res):
        overall = NOT_RUN
    else:
        overall = INCONCLUSIVE
    return {"outcome": overall, "plan_version": PLAN_VERSION, "campaign": tl.get("campaign"), "criteria": res,
            "stop": stop, "forbidden": forbidden,
            "protections_not_sampled": unwatched, "values": v}
