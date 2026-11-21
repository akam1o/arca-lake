#!/usr/bin/env python3
"""The R-110 retest driver (plan §7, §9 P4): checks a run configuration,
and only with an approval runs the stages, keeps the evidence and judges.

  driver.py check CONFIG                      (the default: local only)
  driver.py preflight CONFIG --approval FILE --execute
  driver.py run CONFIG --approval FILE --execute --evidence DIR
  driver.py teardown CONFIG --approval FILE --execute

``check`` validates CONFIG against the plan (nothing looser than its
values: limits, stage maxima, settings, stop rules, judgement values;
paths, ports, the binary's features) and prints the stages and their
steps. It never connects anywhere, starts a process or creates a
directory outside ``--evidence`` (which it does not use).

Every other command contacts the hosts, so it needs ``--execute`` and an
approval file (JSON, written by the approver, never by this tool):
``config_sha256`` (the sha256 of CONFIG's bytes: any change of the
configuration voids the approval), ``scope`` (``preflight``, ``run``,
``teardown``), ``window_start`` and ``window_end`` (ISO 8601 UTC; the
command is refused outside), ``approved_by``. The example configuration
in this directory is not approved.

Stages (plan §7), each with its start condition, exit condition and
maximum time; a stage that ends any other way ends the run. The smoke
stage exercises the stop layers in a sub-run of its own (its own run
directory, budget and stop file): an intentional stop there never
touches the main run's budget or stop state. Any stop of the main run
(its stop file, by the load, the guard, the watchdog or this driver) is
recorded with its stage; the evidence collected so far is saved; the
judgement (``judge.py``) is computed from what exists. A run never
overwrites another's evidence: its directory is created exclusively.
"""

import argparse
import datetime
import hashlib
import json
import os
import re
import sys

from . import budget, common, judge

# The plan's values (docs/verification/phase-4.1-r110-retest-plan.md).
STAGE_MAX_S = {"1": 1800, "2": 1800, "3": 3600, "4": 3600, "5": 3600, "6": 6000, "7": 3600}
RUN_MAX_S = 7.5 * 3600
SETTINGS = {
    "nodes": 4,
    "allotment_bytes": 1 << 30,          # at most; 768 MiB allowed (plan §4.2)
    "segment_max_bytes": 64 << 20,
    "reclaim_retention_secs": 600,
    "reclaim_skew_secs": 60,
    "chunk_bytes": 1 << 20,
    "admission_max_chunk_bytes": 1 << 20,
    "object_size": 256 << 10,
    "l0": 4000,
    "d1": 1000,
    "stage4_max_new": 5000,
    "l1": 2000,
    "churn_seconds": 5400,
    "churn_rate": 1.0,
}
STOP_RULES = {"guard": {"min_free_gib": 3.0, "min_mem_mib": 1024.0, "heartbeat_timeout_s": 120.0},
              "preflight_free_gib": 12.2, "preflight_root_free_gib": 3.0}
SCOPES = ("preflight", "run", "teardown")
MAX_WINDOW_H = 24


class ConfigError(Exception):
    pass


class NotApproved(Exception):
    pass


def check_path(name, p, depth=3):
    if not isinstance(p, str) or not p.startswith("/") or os.path.normpath(p) != p \
            or p.count("/") < depth or not re.fullmatch(r"[A-Za-z0-9/._-]+", p):
        raise ConfigError(f"{name}={p!r}: an absolute, normalised path {depth} levels deep")
    if any(part.startswith("p41") for part in p.split("/")):
        raise ConfigError(f"{name}: a directory of the retained cluster")


def validate(cfg):
    """Raise ``ConfigError`` for anything missing or looser than the plan."""
    for k in ("run_id", "hosts", "load_host", "remote", "binary", "limits", "settings", "stage_max_s",
              "stop_rules", "judgement", "teardown"):
        if k not in cfg:
            raise ConfigError(f"{k} missing")
    if not re.fullmatch(r"r110-[0-9]{8}T[0-9]{6}Z", cfg["run_id"]):
        raise ConfigError("run_id: r110-YYYYMMDDTHHMMSSZ")
    if cfg["load_host"] not in cfg["hosts"]:
        raise ConfigError("load_host not among the hosts")
    r = cfg["remote"]
    for k in ("run_dir", "fdb_data"):
        check_path(k, r.get(k))
    check_path("data_mount", r.get("data_mount"), depth=2)
    check_path("binary_path", r.get("binary_path"))
    if r["run_dir"] == r["fdb_data"] or r["run_dir"].startswith(r["fdb_data"] + "/") \
            or r["fdb_data"].startswith(r["run_dir"] + "/"):
        raise ConfigError("run_dir and fdb_data overlap")
    if not (r["run_dir"].startswith(r["data_mount"] + "/") and r["fdb_data"].startswith(r["data_mount"] + "/")):
        raise ConfigError("the run's directories must be below the data mount")
    ports = r.get("ports", {})
    if not ports or not all(isinstance(v, int) and 1024 < v < 65536 for v in ports.values()) \
            or len(set(ports.values())) != len(ports):
        raise ConfigError("ports: distinct unprivileged integers")
    b = cfg["binary"]
    if b.get("features") != ["fdb"]:
        raise ConfigError("binary features: exactly [\"fdb\"] (a release build without fault-injection)")
    if not re.fullmatch(r"[0-9a-f]{40}", b.get("commit", "")) or not re.fullmatch(r"[0-9a-f]{64}", b.get("sha256", "")):
        raise ConfigError("binary: the full commit and sha256")
    for k, v in cfg["limits"].items():
        if k not in budget.DEFAULTS or v > budget.DEFAULTS[k]:
            raise ConfigError(f"limit {k}={v} above the plan's {budget.DEFAULTS.get(k)}")
    for k, v in budget.DEFAULTS.items():
        cfg["limits"].setdefault(k, v)
    s = cfg["settings"]
    for k, v in SETTINGS.items():
        got = s.get(k)
        if k == "allotment_bytes":
            if got not in (1 << 30, 768 << 20):
                raise ConfigError("allotment_bytes: 1 GiB, or 768 MiB (plan §4.2)")
        elif got != v:
            raise ConfigError(f"setting {k}={got}: the plan's is {v}")
    for k, v in STAGE_MAX_S.items():
        if cfg["stage_max_s"].get(k, v) > v:
            raise ConfigError(f"stage {k} maximum above the plan's {v} s")
    g = cfg["stop_rules"].get("guard", {})
    pg = STOP_RULES["guard"]
    if g.get("min_free_gib", 0) < pg["min_free_gib"] or g.get("min_mem_mib", 0) < pg["min_mem_mib"] \
            or g.get("heartbeat_timeout_s", 1e9) > pg["heartbeat_timeout_s"]:
        raise ConfigError("guard stop rules looser than the plan's")
    for k, v in cfg["judgement"].items():
        if judge.VALUES.get(k) != v:
            raise ConfigError(f"judgement value {k}={v}: the plan's is {judge.VALUES.get(k)}")
    cap = s["nodes"] * s["allotment_bytes"] + (3 << 30)
    if not 0 < cfg["stop_rules"].get("max_test_dir_bytes", 0) <= cap:
        raise ConfigError(f"stop_rules.max_test_dir_bytes: at most the allotments plus 3 GiB ({cap})")
    check_path("local_dir", cfg.get("local_dir"), depth=2)
    if cfg["teardown"] != {"scope": "run-directories-and-processes"}:
        raise ConfigError("teardown scope: the run's own directories and processes only")
    return cfg


def load_config(path):
    with open(path, "rb") as f:
        raw = f.read()
    return validate(json.loads(raw)), hashlib.sha256(raw).hexdigest()


def check_approval(path, config_sha, scope, now):
    """Raise ``NotApproved`` unless the approval names this configuration,
    this scope and a window containing ``now`` (an aware datetime)."""
    if not path:
        raise NotApproved("no approval file")
    try:
        a = common.read_json(path)
    except (OSError, ValueError) as e:
        raise NotApproved(f"approval unreadable: {e}") from None
    if a.get("config_sha256") != config_sha:
        raise NotApproved("the approval names another configuration")
    sc = a.get("scope")
    if not isinstance(sc, list) or not all(isinstance(x, str) and x in SCOPES for x in sc):
        raise NotApproved(f"scope: a list of {SCOPES}")
    if scope not in sc:
        raise NotApproved(f"the approval does not cover {scope}")
    if not a.get("approved_by"):
        raise NotApproved("approved_by missing")
    try:
        w0 = datetime.datetime.fromisoformat(a["window_start"])
        w1 = datetime.datetime.fromisoformat(a["window_end"])
    except (KeyError, ValueError) as e:
        raise NotApproved(f"window: {e}") from None
    if w0.tzinfo is None or w1.tzinfo is None:
        raise NotApproved("window times need a time zone")
    if not w0 < w1 or w1 - w0 > datetime.timedelta(hours=MAX_WINDOW_H):
        raise NotApproved(f"the window must end after it starts, within {MAX_WINDOW_H} h")
    if not w0 <= now <= w1:
        raise NotApproved(f"now ({now.isoformat()}) is outside the approved window")
    return a


def stages(cfg):
    """The stages and the load steps each sends (a pure description)."""
    s, mx = cfg["settings"], cfg["stage_max_s"]
    l0 = [f"b{i:05d}" for i in range(s["l0"])]
    d1 = l0[: s["d1"]]
    more = l0[s["d1"]: s["d1"] + (s["l0"] - s["d1"] - s["l1"])]
    return [
        {"stage": "1", "name": "preflight", "max_s": mx.get("1", STAGE_MAX_S["1"]), "steps": []},
        {"stage": "2", "name": "smoke", "max_s": mx.get("2", STAGE_MAX_S["2"]), "steps": []},
        {"stage": "3", "name": "fill and baseline", "max_s": mx.get("3", STAGE_MAX_S["3"]),
         "steps": [{"kind": "create-bucket"}, {"kind": "put", "keys": l0, "rate": 20}]},
        {"stage": "4", "name": "pressure", "max_s": mx.get("4", STAGE_MAX_S["4"]),
         "steps": [{"kind": "delete", "keys": d1, "rate": 20},
                   {"kind": "put-until-refused", "prefix": "p", "max_new": s["stage4_max_new"], "rate": 10,
                    "refused_for": 30},
                   {"kind": "hold", "prefix": "h", "interval": 1.0, "until": "driver"}]},
        {"stage": "5", "name": "drain and recovery", "max_s": mx.get("5", STAGE_MAX_S["5"]),
         "steps": [{"kind": "probe", "prefix": "q", "put": True, "interval": 10.0,
                    "until": "driver", "concurrent_with": "deletes"},
                   {"kind": "delete", "keys": "stage-4 keys + " + str(len(more)) + " baseline", "rate": 20}]},
        {"stage": "6", "name": "steady churn", "max_s": mx.get("6", STAGE_MAX_S["6"]),
         "steps": [{"kind": "churn", "prefix": "c", "seconds": s["churn_seconds"], "rate": s["churn_rate"],
                    "min_live": s["l1"]}]},
        {"stage": "7", "name": "integrity and evidence", "max_s": mx.get("7", STAGE_MAX_S["7"]),
         "steps": [{"kind": "verify"}, {"kind": "finish"}]},
    ]


class Evidence:
    """The run's evidence directory, created exclusively (never reused)."""

    def __init__(self, root, run_id):
        os.makedirs(root, exist_ok=True)
        self.dir = os.path.join(root, run_id)
        os.mkdir(self.dir)  # FileExistsError: a run's evidence is never overwritten
        self.events = os.path.join(self.dir, "events.jsonl")
        self.failures = []

    def event(self, wall_ms, what, **kw):
        with open(self.events, "a") as f:
            f.write(json.dumps(dict(kw, wall_ms=wall_ms, event=what), sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def save(self, name, obj):
        try:
            common.atomic_write_json(os.path.join(self.dir, name), obj)
            return True
        except OSError as e:
            self.failures.append(f"{name}: {e}")
            return False


# Plan §7: D1's chain (GC grace 600 s, a GC interval, the retention 600 s,
# a reclaim interval: about 25 min) before the GC and reclaim gauges can
# mean "done"; before it, 0 pending records is only "not retired yet".
CHAIN_S = 1500
SETTLE_S = 180  # three compaction passes after the last reclaim (§8 C2)
DRAIN_OBSERVE_S = 1800  # §6.2: observe the drain after a stop that is not a resource danger


class Run:
    """The stage engine over a ``cluster`` (an object with the methods used
    below; ``SshCluster`` for hosts, a fake in tests). Polls every
    ``poll_s``; every wait has its stage's maximum. Marks of the timeline
    are on the load host's clock (the samples' and the ledger's): the
    driver's wall clock plus the offset measured at each stage's start."""

    def __init__(self, cfg, cluster, evidence, clock=None, poll_s=15.0):
        self.cfg, self.c, self.ev = cfg, cluster, evidence
        self.clock = clock or common.Clock()
        self.poll_s = poll_s
        self.tl = {"clock_offsets": []}
        self.stop = None
        self.current = None
        self.offset_ms = 0
        self.iv_ms = judge.VALUES["interval_s"] * 1000
        self.start = self.clock.monotonic()

    def host_ms(self):
        return self.clock.wall_ms() + self.offset_ms

    def _offset(self, stage):
        self.offset_ms = int(self.c.clock_offset_ms())
        self.tl["clock_offsets"].append({"stage": stage, "offset_ms": self.offset_ms})

    def _stopped(self, stage):
        if self.stop:
            return True
        rec = self.c.stop_record()
        if rec:
            self.stop = dict(rec, stage=stage)
            self.ev.event(self.clock.wall_ms(), "stop", **self.stop)
            return True
        if self.clock.monotonic() - self.start >= RUN_MAX_S:
            self._stop(stage, "run-deadline")
            return True
        return False

    def _stop(self, stage, reason, **detail):
        try:
            self.c.write_stop(reason)
            rec = self.c.stop_record() or {"reason": reason}
        except Exception as e:  # noqa: BLE001 - the stop is recorded whatever happens
            self.ev.failures.append(f"write_stop: {e}")
            rec = {"reason": reason}
        self.stop = dict(rec, stage=stage, **detail)
        self.ev.event(self.clock.wall_ms(), "stop", **self.stop)

    def wait(self, stage, deadline, cond):
        """Poll ``cond(samples)`` until true, a stop, or the deadline."""
        while True:
            if self._stopped(stage):
                return False
            v = cond(self.c.samples())
            if v:
                return v
            if self.clock.monotonic() >= deadline:
                return False
            self.clock.sleep(self.poll_s)

    def stage(self, st, body):
        self.current = st["stage"]
        self._offset(st["stage"])
        self.tl[st["stage"]] = {"start_ms": self.host_ms(), "name": st["name"]}
        self.ev.event(self.clock.wall_ms(), "stage start", stage=st["stage"])
        deadline = self.clock.monotonic() + st["max_s"]
        try:
            ok = body(st, deadline)
        finally:
            self.tl[st["stage"]]["end_ms"] = self.host_ms()
        self.ev.event(self.clock.wall_ms(), "stage end", stage=st["stage"], ok=bool(ok))
        if not ok and not self.stop:
            # Its exit condition not met by its maximum (or a precondition
            # not formed): the run ends here.
            self._stop(st["stage"], "stage-exit", stage_name=st["name"])
        return ok

    # -- samples: fresh values only ---------------------------------------------

    def _fresh(self, samples, target, key):
        """``target``'s latest value of ``key``, or None when it is missing
        or older than two intervals behind the newest sample (a target
        whose scrapes now fail never shows its last good value)."""
        newest = max((x.get("ended_wall_ms", 0) for x in samples), default=None)
        for x in reversed(samples):
            if x.get("target") == target and x.get("status") in ("ok", "partial") and key in x["values"]:
                if newest - x["ended_wall_ms"] > 2 * self.iv_ms:
                    return None
                return x["values"][key]["value"]
        return None

    def _all_nodes(self, samples, key, pred):
        vals = [self._fresh(samples, n, key) for n in self.c.nodes()]
        return all(v is not None and pred(v) for v in vals)

    def _complete(self, samples):
        """Every sampler target's latest sample fresh and read (plan §7
        stage 3: samples complete)."""
        newest = max((x.get("ended_wall_ms", 0) for x in samples), default=None)
        last = {}
        for x in samples:
            last[x.get("target")] = x
        for t in self.c.targets():
            x = last.get(t)
            if x is None or x.get("status") not in ("ok", "partial") or newest - x["ended_wall_ms"] > 2 * self.iv_ms:
                return False
        return True

    def _quiet(self, s):
        return self._fresh(s, "controller", "arca_controller_gc_backlog") == 0 and \
            self._all_nodes(s, "arca_node_reclaim_pending_records", lambda v: v == 0)

    def _admission(self, s, state):
        return all(self._fresh(s, g, "arca_gateway_admission_state") == state for g in self.c.gateways()) and \
            (state != 0 or self._fresh(s, "controller", "arca_controller_backpressure_state") == 0)

    # -- stage bodies ---------------------------------------------------------

    def s1(self, st, deadline):
        checks = self.c.preflight()
        self.tl["1"]["checks"] = checks
        missing = sorted(set(self.c.required_checks()) - set(checks))
        self.tl["1"]["missing_checks"] = missing
        return bool(checks) and not missing and all(c.get("ok") for c in checks.values())

    def s2(self, st, deadline):
        r = self.c.smoke()  # its own sub-runs: their stops are not this run's
        self.tl["2"]["smoke"] = r
        return bool(r.get("stop_layers_exercised")) and bool(r.get("limits_exercised")) and r.get("errors") == 0

    def s3(self, st, deadline):
        self.c.start_load()
        bucket, fill = st["steps"]
        if not self.c.run_step(dict(bucket, stage="3"), deadline) or self._stopped("3"):
            return False
        r = self.c.run_step(dict(fill, stage="3"), deadline)
        if not r or self._stopped("3"):
            return False
        counts = r.get("counts", {})
        acked = counts.get("op-PUT-ack", 0) + counts.get("retry-PUT-ack", 0)
        self.tl["3"]["l0_acknowledged"] = acked
        if acked != len(fill["keys"]):
            return False
        # The baseline-key probe (plan §6.2), from here to the end.
        if not self.c.run_step({"kind": "start-probe", "key": fill["keys"][-1], "interval": 5.0, "stage": "3"},
                               deadline):
            return False
        return bool(self.wait("3", deadline, lambda s: self._quiet(s) and self._admission(s, 0) and self._complete(s)))

    def s4(self, st, deadline):
        d1, fill, hold = st["steps"]
        tl = self.tl["4"]
        if not self.c.run_step(dict(d1, stage="4"), deadline) or self._stopped("4"):
            return False
        d1_done = self.clock.monotonic()
        tl["d1_done_ms"] = self.host_ms()
        tl["fill_start_ms"] = self.host_ms()
        r = self.c.run_step(dict(fill, stage="4"), deadline)
        if not r or not r.get("refusal_reached") or self._stopped("4"):
            return False
        if not self.wait("4", deadline, lambda s: self._admission(s, 3)):
            return False
        samples = self.c.samples()
        # The refusal's start: the first sample showing write rejected after
        # the fill began (host clock), not the driver's poll time.
        first = [x["ended_wall_ms"] for x in samples
                 if x.get("target") in self.c.gateways() and x.get("status") in ("ok", "partial")
                 and x["ended_wall_ms"] >= tl["fill_start_ms"]
                 and x["values"].get("arca_gateway_admission_state", {}).get("value") == 3]
        tl["refusal_start_ms"] = min(first) if first else None
        if tl["refusal_start_ms"] is None:
            return False
        ctl = judge.series(samples, "controller", "arca_controller_reclaimed_total")
        at_d1 = judge.at_or_before(ctl, tl["d1_done_ms"], self.iv_ms)
        at_r = judge.at_or_before(ctl, tl["refusal_start_ms"], self.iv_ms)
        # D1's reclaims before the refusal do not count for C1 (plan §8).
        tl["reclaimed_before_refusal"] = (at_r - at_d1) if at_r is not None and at_d1 is not None else None
        n = self.c.send_step(dict(hold, stage="4", max_seconds=max(1, deadline - self.clock.monotonic())))
        done = self.wait("4", deadline, lambda s: not self._admission(s, 3) or (
            self.clock.monotonic() >= d1_done + CHAIN_S and self._quiet(s)))
        tl["refusal_end_ms"] = self.host_ms()
        tl["admission_recovered_early"] = bool(done) and self.clock.monotonic() < d1_done + CHAIN_S
        self.c.step_done(n)
        return bool(done)

    def s5(self, st, deadline):
        probe, _ = st["steps"]
        tl = self.tl["5"]
        n = self.c.send_step(dict(probe, stage="5", max_seconds=max(1, deadline - self.clock.monotonic())))
        try:
            s = self.cfg["settings"]
            keys = self.c.stage4_keys() + [f"b{i:05d}" for i in range(s["d1"], s["l0"] - s["l1"])]
            if not self.c.run_step({"kind": "delete", "keys": keys, "rate": 20, "stage": "5"}, deadline) \
                    or self._stopped("5"):
                return False
            dels_done = self.clock.monotonic()
            # C2: the chain of the deletes, then three compaction passes.
            if self.wait("5", deadline, lambda x: self.clock.monotonic() >= dels_done + CHAIN_S and self._quiet(x)):
                settle = self.clock.monotonic() + SETTLE_S
                if self.wait("5", deadline, lambda x: self.clock.monotonic() >= settle):
                    tl["c2_judge_ms"] = self.host_ms()
                    c2_end = self.clock.monotonic() + judge.VALUES["c2_hold_s"]
            # C3a: an uninterrupted window of the hold; a flap restarts it.
            hold = judge.VALUES["c3a_hold_s"]
            win = None
            while True:
                if self._stopped("5") or self.clock.monotonic() >= deadline:
                    break
                ok = self._admission(self.c.samples(), 0)
                now = self.clock.monotonic()
                if ok and win is None:
                    win = now
                    tl["c3a_window_start_ms"] = self.host_ms()
                elif not ok and win is not None:
                    tl.setdefault("c3a_flaps", 0)
                    tl["c3a_flaps"] += 1
                    win = None
                    tl.pop("c3a_window_start_ms", None)
                if win is not None and now - win >= hold:
                    break
                self.clock.sleep(self.poll_s)
            if win is None or self.clock.monotonic() - win < hold:
                return False
            if "c2_judge_ms" in tl and not self.wait("5", deadline, lambda x: self.clock.monotonic() >= c2_end):
                return False
        finally:
            self.c.step_done(n)
        tl["end_ms"] = self.host_ms()
        # Stage 6 starts only when C3a passed (plan §7).
        r = judge.c3a(self.c.samples(), self.c.ledger_records(), self.tl, judge.VALUES, self.c.gateways())
        tl["c3a"] = r["outcome"]
        return r["outcome"] == judge.PASSED

    def s6(self, st, deadline):
        (churn,) = st["steps"]
        r = self.c.run_step(dict(churn, stage="6"), deadline)
        self.tl["6"]["completed"] = bool(r) and not r.get("deadline_reached") and not self._stopped("6")
        return self.tl["6"]["completed"]

    def s7(self, st, deadline):
        r = self.c.run_step({"kind": "verify", "stage": "7"}, deadline)
        self.tl["7"]["verify"] = r
        if self._stopped("7"):
            return False
        self.tl["7"]["manifest_findings"] = self.c.integrity("verify-manifests")
        if self._stopped("7"):
            return False
        self.tl["7"]["scrub_findings"] = self.c.integrity("scrub")
        if self._stopped("7"):
            return False
        self.c.run_step({"kind": "finish", "stage": "7"}, deadline)
        return r is not None and r.get("complete") is not None and not self._stopped("7")

    def execute(self):
        bodies = {"1": self.s1, "2": self.s2, "3": self.s3, "4": self.s4, "5": self.s5, "6": self.s6, "7": self.s7}
        try:
            for st in stages(self.cfg):
                if self._stopped(st["stage"]):
                    break
                try:
                    ok = self.stage(st, bodies[st["stage"]])
                except Exception as e:  # noqa: BLE001 - a failure of the driver is a stop
                    self._stop(st["stage"], "driver-error", error=f"{type(e).__name__}: {e}"[:500])
                    break
                if not ok:
                    break
        finally:
            self.finish()
        return self.result

    def drain(self):
        """Plan §6.2: after a stop that is not a resource danger, keep the
        samplers up to 30 min to observe the drain; a resource or monitor
        rule firing meanwhile (the watchdog keeps watching) stops the test's
        services too."""
        d = self.tl["drain"] = {"start_ms": self.host_ms()}
        end = self.clock.monotonic() + DRAIN_OBSERVE_S
        while self.clock.monotonic() < end:
            rec = self.c.escalation()
            if rec and common.reason_class(rec.get("reason")) in ("resource", "monitor"):
                d["escalation"] = rec
                d["services"] = self.c.stop_services()
                break
            self.clock.sleep(self.poll_s)
        d["end_ms"] = self.host_ms()

    def finish(self):
        if self.stop is None:
            try:
                rec = self.c.stop_record()
                if rec:
                    self.stop = dict(rec, stage=self.current)
            except Exception as e:  # noqa: BLE001
                self.ev.failures.append(f"stop_record: {e}")
                self.stop = {"reason": "monitor-failed", "stage": self.current, "detail": str(e)[:300]}
        cls = common.reason_class(self.stop["reason"]) if self.stop else None
        if self.stop and cls not in ("resource", "monitor") and self.stop.get("stage") not in (None, "1", "2"):
            try:
                self.drain()
            except Exception as e:  # noqa: BLE001
                self.ev.failures.append(f"drain: {e}")
        try:
            self.c.stop_load()
        except Exception as e:  # noqa: BLE001 - evidence first
            self.ev.failures.append(f"stop_load: {e}")
        parts = {}
        for name, f in (("samples", self.c.samples), ("ledger", self.c.ledger_records),
                        ("logs", self.c.collect_logs)):
            try:
                parts[name] = f()
            except Exception as e:  # noqa: BLE001
                self.ev.failures.append(f"{name}: {e}")
                parts[name] = None
        self.ev.save("timeline.json", self.tl)
        for name in ("samples", "ledger"):
            if parts[name] is not None:
                self.ev.save(f"{name}.json", parts[name])
        if parts["logs"] is not None:
            self.ev.save("logs.json", {"tail": parts["logs"]})
        self.result = judge.judge(parts["samples"] or [], parts["ledger"] or [], self.tl, self.stop,
                                  self.cfg["judgement"], tuple(self.c.nodes()), tuple(self.c.gateways()))
        if self.ev.failures:
            self.result["evidence_failures"] = list(self.ev.failures)
            if self.result["outcome"] == judge.PASSED:
                self.result["outcome"] = judge.INCONCLUSIVE
        self.ev.save("result.json", self.result)


def main(argv=None, now=None, cluster_factory=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("command", nargs="?", default="check", choices=["check", "preflight", "run", "teardown"])
    p.add_argument("config")
    p.add_argument("--approval")
    p.add_argument("--execute", action="store_true")
    p.add_argument("--evidence")
    a = p.parse_args(argv)
    try:
        cfg, sha = load_config(a.config)
    except (ConfigError, OSError, ValueError) as e:
        print(f"configuration refused: {e}", file=sys.stderr)
        return 2
    if a.command == "check":
        print(json.dumps({"run_id": cfg["run_id"], "config_sha256": sha,
                          "stages": [{k: v for k, v in s.items() if k != "steps"} | {
                              "steps": [st["kind"] for st in s["steps"]]} for s in stages(cfg)]}, indent=1))
        print("check only: nothing contacted. Connecting needs an approval and --execute.", file=sys.stderr)
        return 0
    if not a.execute:
        print(f"{a.command} contacts the hosts: --execute and an approval are required", file=sys.stderr)
        return 2
    now = now or datetime.datetime.now(datetime.timezone.utc)
    try:
        check_approval(a.approval, sha, a.command, now)
    except NotApproved as e:
        print(f"not approved: {e}", file=sys.stderr)
        return 2
    if cluster_factory is None:
        from . import remote
        cluster_factory = remote.SshCluster
    cluster = cluster_factory(cfg)
    if a.command == "preflight":
        print(json.dumps(cluster.preflight(), indent=1))
        return 0
    if a.command == "teardown":
        print(json.dumps(cluster.teardown(), indent=1))
        return 0
    if not a.evidence:
        print("run needs --evidence", file=sys.stderr)
        return 2
    ev = Evidence(a.evidence, cfg["run_id"])
    ev.save("config.json", {"config": cfg, "config_sha256": sha})
    res = Run(cfg, cluster, ev).execute()
    print(json.dumps({"outcome": res["outcome"], "evidence": ev.dir}))
    return 0 if res["outcome"] == judge.PASSED else 1


if __name__ == "__main__":
    sys.exit(main())
