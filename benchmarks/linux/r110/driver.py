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


class ConfigError(Exception):
    pass


class NotApproved(Exception):
    pass


def check_path(name, p, depth=3):
    if not isinstance(p, str) or not p.startswith("/") or os.path.normpath(p) != p \
            or p.count("/") < depth or not re.fullmatch(r"[A-Za-z0-9/._-]+", p):
        raise ConfigError(f"{name}={p!r}: an absolute, normalised path {depth} levels deep")
    if "p41" in p:
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
    if not isinstance(cfg.get("local_dir"), str) or not cfg["local_dir"].startswith("/"):
        raise ConfigError("local_dir: an absolute path on this machine")
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
    if scope not in a.get("scope", []):
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
         "steps": [{"kind": "probe", "prefix": "q", "put": True, "get_key": l0[-1], "interval": 10.0,
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


class Run:
    """The stage engine over a ``cluster`` (an object with the methods used
    below; ``SshCluster`` for hosts, a fake in tests). Polls every
    ``poll_s``; every wait has its stage's maximum."""

    def __init__(self, cfg, cluster, evidence, clock=None, poll_s=15.0):
        self.cfg, self.c, self.ev = cfg, cluster, evidence
        self.clock = clock or common.Clock()
        self.poll_s = poll_s
        self.tl = {}
        self.stop = None
        self.start = self.clock.monotonic()

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
        self.c.write_stop(reason)
        rec = self.c.stop_record() or {"reason": reason}
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
        self.tl[st["stage"]] = {"start_ms": self.clock.wall_ms(), "name": st["name"]}
        self.ev.event(self.clock.wall_ms(), "stage start", stage=st["stage"])
        deadline = self.clock.monotonic() + st["max_s"]
        try:
            ok = body(st, deadline)
        finally:
            self.tl[st["stage"]]["end_ms"] = self.clock.wall_ms()
        self.ev.event(self.clock.wall_ms(), "stage end", stage=st["stage"], ok=bool(ok))
        if not ok and not self.stop:
            # Its exit condition not met by its maximum (or a precondition
            # not formed): the run ends here.
            self._stop(st["stage"], "stage-exit", stage_name=st["name"])
        return ok

    # -- stage bodies ---------------------------------------------------------

    def s1(self, st, deadline):
        checks = self.c.preflight()
        self.tl["1"]["checks"] = checks
        return bool(checks) and all(c.get("ok") for c in checks.values())

    def s2(self, st, deadline):
        r = self.c.smoke()  # its own sub-run: its stops are not this run's
        self.tl["2"]["smoke"] = r
        return bool(r.get("stop_layers_exercised")) and bool(r.get("limits_exercised")) and r.get("errors") == 0

    def _all_nodes(self, samples, key, pred):
        nodes = self.c.nodes()
        last = {}
        for s in samples:
            if s.get("target") in nodes and s.get("status") == "ok" and key in s["values"]:
                last[s["target"]] = s["values"][key]["value"]
        return len(last) == len(nodes) and all(pred(v) for v in last.values())

    def _last(self, samples, target, key):
        for s in reversed(samples):
            if s.get("target") == target and s.get("status") == "ok" and key in s["values"]:
                return s["values"][key]["value"]
        return None

    def s3(self, st, deadline):
        self.c.start_load()
        for step in st["steps"]:
            if not self.c.run_step(dict(step, stage="3"), deadline) or self._stopped("3"):
                return False
        return self.wait("3", deadline, lambda s: self._last(s, "controller", "arca_controller_gc_backlog") == 0
                         and self._all_nodes(s, "arca_node_reclaim_pending_records", lambda v: v == 0))

    def s4(self, st, deadline):
        d1, fill, hold = st["steps"]
        self.tl["4"]["reclaimed_at_d1"] = self._last(self.c.samples(), "controller", "arca_controller_reclaimed_total")
        if not self.c.run_step(dict(d1, stage="4"), deadline) or self._stopped("4"):
            return False
        r = self.c.run_step(dict(fill, stage="4"), deadline)
        if not r or not r.get("refusal_reached") or self._stopped("4"):
            return False
        t = self.wait("4", deadline, lambda s: self._last(s, "gateway0", "arca_gateway_admission_state") == 3)
        if not t:
            return False
        tl = self.tl["4"]
        tl["refusal_start_ms"] = self.clock.wall_ms()
        # D1's reclaims before the refusal do not count for C1 (plan §8).
        now_r = self._last(self.c.samples(), "controller", "arca_controller_reclaimed_total")
        tl["reclaimed_before_refusal"] = (now_r - tl["reclaimed_at_d1"]) if (
            now_r is not None and tl.get("reclaimed_at_d1") is not None) else None
        n = self.c.send_step(dict(hold, stage="4", max_seconds=max(1, deadline - self.clock.monotonic())))
        done = self.wait("4", deadline, lambda s: (
            self._last(s, "gateway0", "arca_gateway_admission_state") != 3 or
            (self._last(s, "controller", "arca_controller_gc_backlog") == 0 and
             self._all_nodes(s, "arca_node_reclaim_pending_records", lambda v: v == 0))))
        tl["refusal_end_ms"] = self.clock.wall_ms()
        self.c.step_done(n)
        return bool(done)

    def s5(self, st, deadline):
        probe, dels = st["steps"]
        n = self.c.send_step(dict(probe, stage="5", max_seconds=max(1, deadline - self.clock.monotonic())))
        keys = self.c.stage4_keys() + [f"b{i:05d}" for i in range(self.cfg["settings"]["d1"],
                                                                 self.cfg["settings"]["l0"] - self.cfg["settings"]["l1"])]
        ok = self.c.run_step({"kind": "delete", "keys": keys, "rate": 20, "stage": "5"}, deadline)
        if not ok or self._stopped("5"):
            self.c.step_done(n)
            return False
        tl = self.tl["5"]
        chain = self.wait("5", deadline, lambda s: self._last(s, "controller", "arca_controller_gc_backlog") == 0
                          and self._all_nodes(s, "arca_node_reclaim_pending_records", lambda v: v == 0))
        if chain:
            # Three compaction passes after the last reclaim (plan §8 C2).
            settle = self.clock.monotonic() + 180
            if self.wait("5", min(deadline, settle + 1), lambda s: self.clock.monotonic() >= settle):
                tl["c2_judge_ms"] = self.clock.wall_ms()
        adm = self.wait("5", deadline, lambda s: self._last(s, "gateway0", "arca_gateway_admission_state") == 0
                        and self._last(s, "controller", "arca_controller_backpressure_state") == 0)
        if adm:
            tl["c3a_window_start_ms"] = self.clock.wall_ms()
        hold_end = self.clock.monotonic() + judge.VALUES["c3a_hold_s"]
        ok = bool(chain and adm) and self.wait("5", min(deadline, hold_end + 1),
                                               lambda s: self.clock.monotonic() >= hold_end)
        self.c.step_done(n)
        return ok

    def s6(self, st, deadline):
        (churn,) = st["steps"]
        r = self.c.run_step(dict(churn, stage="6"), deadline)
        self.tl["6"]["completed"] = bool(r) and not r.get("deadline_reached") and not self._stopped("6")
        return self.tl["6"]["completed"]

    def s7(self, st, deadline):
        r = self.c.run_step({"kind": "verify", "stage": "7"}, deadline)
        self.tl["7"]["verify"] = r
        self.tl["7"]["manifest_findings"] = self.c.integrity("verify-manifests")
        self.tl["7"]["scrub_findings"] = self.c.integrity("scrub")
        self.c.run_step({"kind": "finish", "stage": "7"}, deadline)
        return r is not None and r.get("complete") is not None

    def execute(self):
        bodies = {"1": self.s1, "2": self.s2, "3": self.s3, "4": self.s4, "5": self.s5, "6": self.s6, "7": self.s7}
        try:
            for st in stages(self.cfg):
                if self._stopped(st["stage"]) and st["stage"] != "7":
                    break
                if not self.stage(st, bodies[st["stage"]]):
                    break
        finally:
            self.finish()
        return self.result

    def finish(self):
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
        if parts["samples"] is not None:
            self.ev.save("samples.json", parts["samples"])
        if parts["ledger"] is not None:
            self.ev.save("ledger.json", parts["ledger"])
        self.result = judge.judge(parts["samples"] or [], parts["ledger"] or [], self.tl, self.stop,
                                  self.cfg["judgement"])
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
