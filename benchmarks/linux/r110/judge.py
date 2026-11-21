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


def c1(samples, records, tl, v, nodes):
    """GC and reclaim under refusal, on the early delete set D1, counted
    from the refusal's start (plan §8 C1)."""
    s4 = tl.get("4")
    if not s4 or "refusal_start_ms" not in s4:
        return verdict("C1", INCONCLUSIVE, ["no refusal formed in stage 4"])
    t_r, t_e = s4["refusal_start_ms"], s4.get("refusal_end_ms") or s4.get("end_ms")
    if t_e is None:
        return verdict("C1", INCONCLUSIVE, ["the refusal's end not recorded"])
    iv = v["interval_s"] * 1000
    d1 = len(ledger_mod.acknowledged(records, "DELETE", stage="4"))
    if d1 == 0:
        return verdict("C1", INCONCLUSIVE, ["no acknowledged delete of D1"])
    gw = series(samples, "gateway0", "arca_gateway_admission_state", t_r, t_e)
    if not covered(gw, t_r, t_e, iv, v["max_gap_factor"]):
        return verdict("C1", INCONCLUSIVE, ["the admission state not sampled through the refusal"])
    ctl = {k: series(samples, "controller", k, t_r, t_e) for k in (
        "arca_controller_gc_collected_total", "arca_controller_gc_backlog",
        "arca_controller_reclaimed_total", "arca_controller_reclaim_refused_total",
        "arca_controller_reclaim_errors_total")}
    for k, pts in ctl.items():
        if not covered(pts, t_r, t_e, iv, v["max_gap_factor"]):
            return verdict("C1", INCONCLUSIVE, [f"{k} not sampled through the refusal"])
    reasons, ev = [], {"d1": d1, "refusal_s": (t_e - t_r) / 1000}
    # Observed violations fail C1 whenever they happened, matured or not.
    for k in ("arca_controller_reclaim_refused_total", "arca_controller_reclaim_errors_total"):
        pts = ctl[k]
        if pts[-1][1] != pts[0][1]:
            reasons.append(f"{k} rose during the refusal")
    if reasons:
        return verdict("C1", FAILED, reasons, **ev)
    # Progress is judged only on an observation that reached D1's maturity
    # (its chain after D1's last delete); one cut short (a stop, a lost
    # monitor, the stage's end) is Inconclusive, never Failed for progress
    # it had no time to show.
    d1_done = s4.get("d1_done_ms")
    if d1_done is None:
        return verdict("C1", INCONCLUSIVE, ["the end of D1's deletes not recorded"], **ev)
    matured_at = d1_done + v["d1_chain_s"] * 1000
    ev["matured_at_ms"] = matured_at
    if t_e < matured_at:
        return verdict("C1", INCONCLUSIVE, [f"the refusal observed until {t_e}, D1 matures at {matured_at}"], **ev)
    if any(s != 3 for t, s in gw if t <= matured_at):
        return verdict("C1", INCONCLUSIVE, ["admission left write rejected before D1 matured"], **ev)
    backlog0 = ctl["arca_controller_gc_backlog"][0][1]
    if backlog0 == 0:
        return verdict("C1", INCONCLUSIVE, ["no D1 generation left to collect at the refusal's start"], **ev)
    collected = ctl["arca_controller_gc_collected_total"][-1][1] - ctl["arca_controller_gc_collected_total"][0][1]
    reclaimed = ctl["arca_controller_reclaimed_total"][-1][1] - ctl["arca_controller_reclaimed_total"][0][1]
    ev.update(backlog_at_refusal=backlog0, gc_collected=collected, reclaimed=reclaimed)
    before = s4.get("reclaimed_before_refusal")
    if before is None:
        return verdict("C1", INCONCLUSIVE, ["the reclaims before the refusal not known"], **ev)
    for n in nodes:
        pend = series(samples, n, "arca_node_reclaim_pending_records", t_r, t_e)
        if not covered(pend, t_r, t_e, iv, v["max_gap_factor"]):
            return verdict("C1", INCONCLUSIVE, [f"{n}: pending records not sampled through the refusal"], **ev)
        if pend[-1][1] != 0:
            reasons.append(f"{n}: pending records not back to 0 at the end of the refusal")
    if collected <= 0:
        reasons.append("no GC collection after the refusal's start")
    if min(b for _, b in ctl["arca_controller_gc_backlog"]) != 0:
        reasons.append("the GC backlog did not reach 0 during the refusal")
    need = v["c1_records_per_object"] * d1 - before
    ev["reclaimed_needed"] = need
    if reclaimed < need:
        reasons.append(f"reclaimed {reclaimed} records after the refusal's start, {need} needed")
    approx = [k for k in ("arca_gateway_node_capacity_refusals_total",)
              if (lambda p: p and p[-1][1] > p[0][1])(series(samples, "gateway0", k, t_r, t_e))]
    if tl.get("reconcile_deleted_during_refusal"):
        approx.append("reconcile removed orphans")
    ev["approximate_attribution"] = approx
    return verdict("C1", FAILED if reasons else PASSED, reasons, **ev)


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
    if mf is not None and mf > 0 or sc is not None and sc > 0:
        return verdict("C5", FAILED, [f"manifest findings {mf}, scrub findings {sc}"])
    if not vr.get("complete") or mf is None or sc is None:
        return verdict("C5", INCONCLUSIVE, ["the verification, the manifests or the scrub incomplete"])
    return verdict("C5", PASSED, [], keys=vr["keys"], unknown_resolved=vr.get("unknown_resolved"))


CRITERIA_STAGE = {"C1": "4", "C2": "5", "C3a": "5", "C3b": "6", "C4": "6", "C5": "7"}


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
    res = [c1(samples, records, tl, v, nodes), c2(samples, records, tl, v, nodes),
           c3a(samples, records, tl, v, gateways), c3b(samples, records, tl, v, gateways),
           c4(samples, records, tl, v, nodes, gateways), c5(tl)]
    forbidden, unwatched = [], []
    watch = [("controller", "arca_controller_gc_skipped_referenced_total")] + \
            [(n, "arca_node_reclaim_damaged") for n in nodes]
    span = _run_span(tl)
    for target, k in watch:
        pts = series(samples, target, k)
        if any(x > 0 for _, x in pts):
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
    return {"outcome": overall, "criteria": res, "stop": stop, "forbidden": forbidden,
            "protections_not_sampled": unwatched, "values": v}
