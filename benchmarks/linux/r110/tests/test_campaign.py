"""The 0.2.0 campaign's amendments (docs/verification/0.2.0-r110-plan.md):
its run IDs, configurations and approvals (§3), the scrub's classes in C5
(§4.1), strict write ownership (§4.2), the commit queue's refusals in the
attribution (§4.3), the preflight of every host (§5). Nothing here
connects to a host."""

import contextlib
import copy
import io
import json
import os
import unittest
from unittest import mock

from r110 import common, driver, judge, remote, watchdog
from r110.tests import test_guard as tg
from r110.tests import test_driver as td

ZERO = "0" * 64


class Configuration(td.Base):
    def refused(self, change, legacy=False):
        cfg = copy.deepcopy(self.cfg)
        change(cfg)
        with self.assertRaises(driver.ConfigError):
            driver.validate(cfg, legacy=legacy)

    def test_the_example_is_main_run_1_of_the_campaign(self):
        self.assertEqual(driver.campaign_run(self.cfg["run_id"]), 1)
        self.assertEqual(self.cfg["campaign"], "0.2.0")
        self.assertEqual(set(self.cfg["system_fdb_hosts"]), {"host1", "host2"})

    def test_run_ids_of_the_campaign_only(self):
        # The 0.1.0 campaign's format, a fourth main run, no number.
        self.refused(lambda c: c.update(run_id="r110-20261122T082126Z"))
        self.refused(lambda c: c.update(run_id="r110-020-4-20270101T000000Z"))
        self.refused(lambda c: c.update(run_id="r110-020-0-20270101T000000Z"))
        self.refused(lambda c: c.update(run_id="r110-020-20270101T000000Z"))
        self.refused(lambda c: c.update(run_id="r110-020-1"))
        for n in (1, 2, 3):
            cfg = copy.deepcopy(self.cfg)
            cfg["run_id"] = f"r110-020-{n}-20270101T000000Z"
            driver.validate(cfg)

    def test_a_campaign_run_names_the_campaign_and_the_archive(self):
        self.refused(lambda c: c.pop("campaign"))
        self.refused(lambda c: c.update(campaign="0.1.0"))
        self.refused(lambda c: c["binary"].pop("archive_sha256"))
        self.refused(lambda c: c["binary"].update(archive_sha256="not-a-digest"))

    def test_the_retained_directories_of_every_host(self):
        self.refused(lambda c: c["retained_record"].pop("host3"))
        self.refused(lambda c: c.update(retained_record={"/mnt/data/x": ZERO}))
        self.refused(lambda c: c["retained_record"].update(host1={"/x": "not-a-digest"}))
        cfg = copy.deepcopy(self.cfg)
        cfg["retained_record"]["host3"] = {}  # listed at the preflight: none there
        driver.validate(cfg)

    def test_a_run_of_the_earlier_campaign_is_only_collected_and_torn_down(self):
        cfg = copy.deepcopy(self.cfg)
        cfg["run_id"] = "r110-20261122T162239Z"
        cfg.pop("campaign")
        cfg["binary"].pop("archive_sha256")
        cfg["retained_record"] = {"/mnt/data/x": ZERO}
        driver.validate(cfg, legacy=True)
        with self.assertRaises(driver.ConfigError):
            driver.validate(cfg)
        p, sha = self.write(cfg)
        a = self.approval(sha, ("preflight", "run", "collect", "teardown"), campaign=None, campaign_run=None,
                          candidate=None)
        for cmd in ("preflight", "run", "collect", "teardown"):
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc, made = td.Approval.invoke(self, [cmd, p, "--evidence", self.dir, "--approval", a])
            self.assertEqual((rc, made), (2, []), cmd)
            # Refused as a configuration for a preflight or a run; taken (and
            # stopped only by the missing --execute) for the others.
            self.assertEqual("configuration refused" in err.getvalue(), cmd in ("preflight", "run"), err.getvalue())

    def calls(self):
        return td.Approval.calls(self)


RID = "r110-020-{}-2027010{}T000000Z"


class Approval(td.Base):
    """§3: the run-number ledger in the evidence root, the rerun record, the
    candidate rules, the archive named by the approval."""

    def calls(self):
        return td.Approval.calls(self)

    def invoke(self, argv):
        return td.Approval.invoke(self, argv)

    def made(self, n, candidate="interim", connected=True, day=1, record=None):
        """An earlier attempt of main run ``n`` in the evidence root."""
        d = os.path.join(self.dir, RID.format(n, day))
        os.makedirs(d)
        with open(os.path.join(d, "approval.json"), "w") as f:
            json.dump({"candidate": candidate, "archive_sha256": ZERO}, f)
        if connected:
            open(os.path.join(d, "events.jsonl"), "w").close()
        if record is not None:
            with open(os.path.join(d, "rerun-record.json"), "w") as f:
                json.dump(record, f)
        return os.path.basename(d)

    def preflight(self, n, **extra):
        cfg = copy.deepcopy(self.cfg)
        cfg["run_id"] = RID.format(n, 9)
        p, sha = self.write(cfg)
        return self.invoke(["preflight", p, "--execute",
                            "--approval", self.approval(sha, ("preflight",), **dict({"campaign_run": n}, **extra))])

    def rerun(self, prev):
        return {"previous_run_id": prev, "cause": "C5 Failed: 2 orphans", "change": "none (a fix in the candidate)",
                "hypothesis": "the orphans were the bug's"}

    def test_an_approval_of_the_earlier_campaign_is_refused(self):
        self.assertEqual(self.preflight(1, campaign=None, candidate=None), (2, []))
        self.assertEqual(self.preflight(1, campaign="0.1.0"), (2, []))

    def test_the_approval_names_this_main_run_and_this_archive(self):
        cfg = copy.deepcopy(self.cfg)
        p, sha = self.write(cfg)
        self.assertEqual(self.invoke(["preflight", p, "--execute",
                                      "--approval", self.approval(sha, ("preflight",), campaign_run=2)]), (2, []))
        self.assertEqual(self.preflight(1, candidate="maybe"), (2, []))
        self.assertEqual(self.preflight(1, archive_sha256="1" * 64), (2, []))
        self.assertEqual(self.preflight(1, archive_sha256=None), (2, []))
        self.assertEqual(self.preflight(1)[0], 0)

    def test_a_main_run_already_made_is_refused(self):
        self.made(1, connected=False)  # never reached a host: not counted
        self.assertEqual(self.preflight(1)[0], 0)
        self.made(1, day=2)
        self.assertEqual(self.preflight(1), (2, []))

    def test_a_later_run_needs_the_previous_run_and_its_rerun_record(self):
        self.assertEqual(self.preflight(2, rerun=self.rerun(RID.format(1, 1))), (2, []))  # no run 1
        prev = self.made(1)
        rr = self.rerun(prev)
        self.assertEqual(self.preflight(2, rerun=rr), (2, []))  # no record with run 1's evidence
        with open(os.path.join(self.dir, prev, "rerun-record.json"), "w") as f:
            json.dump(dict(rr, cause="another cause"), f)
        self.assertEqual(self.preflight(2, rerun=rr), (2, []))  # the record differs
        with open(os.path.join(self.dir, prev, "rerun-record.json"), "w") as f:
            json.dump(rr, f)
        self.assertEqual(self.preflight(2), (2, []))
        for k in rr:
            self.assertEqual(self.preflight(2, rerun=dict(rr, **{k: " "})), (2, []), k)
        self.assertEqual(self.preflight(2, rerun=dict(rr, previous_run_id=RID.format(1, 5))), (2, []))
        # An attempt of run 1 that never reached a host is not run 1, even
        # with a record.
        never = self.made(1, connected=False, day=6)
        with open(os.path.join(self.dir, never, "rerun-record.json"), "w") as f:
            json.dump(self.rerun(never), f)
        self.assertEqual(self.preflight(2, rerun=self.rerun(never)), (2, []))
        self.assertEqual(self.preflight(2, rerun=rr)[0], 0)

    def test_interim_never_leaves_no_run_for_the_final_candidate(self):
        p1 = self.made(1)
        p2 = self.made(2, record=self.rerun(p1))
        with open(os.path.join(self.dir, p2, "rerun-record.json"), "w") as f:
            json.dump(self.rerun(p2), f)
        self.assertEqual(self.preflight(3, rerun=self.rerun(p2)), (2, []))
        self.assertEqual(self.preflight(3, rerun=self.rerun(p2), candidate="final")[0], 0)

    def test_no_interim_run_after_a_final_one(self):
        p1 = self.made(1, candidate="final", record=None)
        with open(os.path.join(self.dir, p1, "rerun-record.json"), "w") as f:
            json.dump(self.rerun(p1), f)
        self.assertEqual(self.preflight(2, rerun=self.rerun(p1)), (2, []))
        self.assertEqual(self.preflight(2, rerun=self.rerun(p1), candidate="final")[0], 0)
        # An earlier approval that cannot be read counts as possibly final.
        os.remove(os.path.join(self.dir, p1, "approval.json"))
        self.assertEqual(self.preflight(2, rerun=self.rerun(p1)), (2, []))

    def test_a_run_starts_from_its_preflight_boot_ids(self):
        """§4.5: no preflight record, no run; with it, the watchdog's seed."""
        cfg = copy.deepcopy(self.cfg)
        p, sha = self.write(cfg)
        made = []
        argv = ["run", p, "--execute", "--evidence", self.dir, "--approval", self.approval(sha, ("run",))]

        class Stub:
            def __init__(self, cfg, cluster, ev, collect=None):
                made.append(cluster)

            def execute(self):
                return {"outcome": judge.INCONCLUSIVE}

        with mock.patch.object(driver, "Run", Stub):
            self.assertEqual(driver.main(argv, now=td.NOW, cluster_factory=td.FakeCluster), 2)
            self.assertEqual(made, [])
            rec = {"run_id": cfg["run_id"], "checks": {f"host {h}": {"boot_id": f"b-{h}"} for h in cfg["hosts"]}}
            with open(driver.preflight_record_path(self.dir, cfg), "w") as f:
                json.dump(dict(rec, checks=dict(rec["checks"], **{"host host3": {}})), f)
            self.assertEqual(driver.main(argv, now=td.NOW, cluster_factory=td.FakeCluster), 2)
            with open(driver.preflight_record_path(self.dir, cfg), "w") as f:
                json.dump(rec, f)
            self.assertEqual(driver.main(argv, now=td.NOW, cluster_factory=td.FakeCluster), 1)
        self.assertEqual(made[0].boot_seed, {h: f"b-{h}" for h in cfg["hosts"]})
        saved = json.load(open(os.path.join(self.dir, cfg["run_id"], "approval.json")))
        self.assertEqual(saved["candidate"], "interim")
        # The attempt is made once its run started: the ledger counts it.
        self.assertFalse(driver.campaign_ledger(self.dir)[1][0]["connected"])  # the stub contacted nothing

    def test_the_preflight_saves_its_record(self):
        p, sha = self.write(self.cfg)
        rc, made = self.invoke(["preflight", p, "--execute", "--approval", self.approval(sha, ("preflight",))])
        self.assertEqual(rc, 0)
        rec = json.load(open(driver.preflight_record_path(self.dir, self.cfg)))
        self.assertEqual((rec["run_id"], rec["config_sha256"]), (self.cfg["run_id"], sha))


def s7(sc, by, held, errors=0, unread=(), settled=None, pending=10, complete=True, unclassed=()):
    return {"campaign": "0.2.0",
            "7": {"verify": {"violations": [], "complete": complete, "keys": 1}, "manifest_findings": 0,
                  "scrub_findings": sc, "scrub_by_class": by, "scrub_held": held, "scrub_errors": errors,
                  "scrub_unread_groups": list(unread), "settled_ms": settled, "pending_before_scrub": pending,
                  "scrub_groups_without_classes": list(unclassed)}}


HELD = {"retained": 4, "retained_damaged": 0, "in_flight": 0, "unclassified": 0}


class ScrubClasses(unittest.TestCase):
    """§4.1: retained and in-flight shards are not findings; every finding
    fails; errors and missing classes are never a pass."""

    def c5(self, *a, **k):
        return judge.c5(s7(*a, **k))

    def test_retained_shards_are_not_findings(self):
        r = self.c5(0, [0] * 8, HELD)
        self.assertEqual(r["outcome"], judge.PASSED, r)
        self.assertEqual(r["evidence"]["scrub_held"], HELD)

    def test_shards_in_flight_after_the_settle_are_inconclusive(self):
        """Nothing is written, moved or collected then: not a clean pass,
        not a finding either."""
        r = self.c5(0, [0] * 8, dict(HELD, in_flight=3), settled=100)
        self.assertEqual(r["outcome"], judge.INCONCLUSIVE, r)
        self.assertIn("in flight", r["reasons"][0])

    def test_retained_shards_are_capped_at_the_records_pending(self):
        self.assertEqual(self.c5(0, [0] * 8, dict(HELD, retained=10), pending=10)["outcome"], judge.PASSED)
        self.assertEqual(self.c5(0, [0] * 8, dict(HELD, retained=11), pending=10)["outcome"], judge.INCONCLUSIVE)
        self.assertEqual(self.c5(0, [0] * 8, HELD, pending=None)["outcome"], judge.INCONCLUSIVE)
        self.assertEqual(self.c5(0, [0] * 8, dict(HELD, retained=0), pending=None)["outcome"], judge.PASSED)

    def test_every_group_prints_its_classes(self):
        self.assertEqual(self.c5(0, [0] * 8, HELD, unclassed=[2])["outcome"], judge.INCONCLUSIVE)
        tl = s7(0, [0] * 8, HELD)
        del tl["7"]["scrub_groups_without_classes"]
        self.assertEqual(judge.c5(tl)["outcome"], judge.INCONCLUSIVE)

    def test_every_finding_fails_settled_or_not(self):
        for settled in (None, 100):
            self.assertEqual(self.c5(5, [0, 0, 0, 0, 5, 0, 0, 0], HELD, settled=settled)["outcome"], judge.FAILED)
        # A damaged record kept for reclaim: under its damage class.
        self.assertEqual(self.c5(1, [0, 1, 0, 0, 0, 0, 0, 0], dict(HELD, retained_damaged=1))["outcome"],
                         judge.FAILED)

    def test_a_finding_beside_another_groups_errors_fails(self):
        r = self.c5(None, [0, 1, 0, 0, 0, 0, 0, 0], dict(HELD, unclassified=1), errors=1)
        self.assertEqual(r["outcome"], judge.FAILED, r)
        r = self.c5(None, [0, 0, 0, 0, 2, 0, 0, 0], HELD, unread=[3])
        self.assertEqual(r["outcome"], judge.FAILED, r)

    def test_errors_unread_groups_and_unclassified_shards_are_inconclusive(self):
        for kw in ({"errors": 2, "held": dict(HELD, unclassified=2)}, {"unread": [2]}):
            held = kw.pop("held", HELD)
            r = self.c5(None, [0] * 8, held, **kw)
            self.assertEqual(r["outcome"], judge.INCONCLUSIVE, (kw, r))
        self.assertEqual(self.c5(0, [0] * 8, dict(HELD, unclassified=1))["outcome"], judge.INCONCLUSIVE)

    def test_no_per_shard_classes_is_never_a_pass_nor_excused(self):
        """A scrub without the classes (not a 0.2.0 binary): v3's count rule
        is not applied in the campaign."""
        self.assertEqual(self.c5(0, [0] * 8, None)["outcome"], judge.INCONCLUSIVE)
        # Unexpected shards no more than the records pending, unsettled:
        # Inconclusive under v3, Failed in the campaign.
        tl = s7(5, [0, 0, 0, 0, 5, 0, 0, 0], None)
        self.assertEqual(judge.c5(tl)["outcome"], judge.FAILED)
        del tl["campaign"]
        self.assertEqual(judge.c5(tl)["outcome"], judge.INCONCLUSIVE)

    def test_a_key_that_differs_still_fails_first(self):
        tl = s7(0, [0] * 8, HELD)
        tl["7"]["verify"]["violations"] = [{"key": "k"}]
        self.assertEqual(judge.c5(tl)["outcome"], judge.FAILED)


class WriteOwnership(unittest.TestCase):
    """§4.2: strict from the first write; no unproven write."""

    def run_tl(self, campaign=True):
        tl = {"3": {"start_ms": 0, "end_ms": 60_000}}
        if campaign:
            tl["campaign"] = "0.2.0"
        return tl

    def samples(self, strict=1, unproven=0, strict_at=None):
        S = []
        for t in range(0, 60_001, 15_000):
            for n in ("node0", "node1"):
                S.append(td.smp(n, t, arca_node_write_ownership_strict=strict if strict_at is None or t >= strict_at
                                else 0, arca_node_writes_unproven_total=unproven if t >= 30_000 else 0,
                                arca_node_reclaim_damaged=0))
            S.append(td.smp("controller", t, arca_controller_gc_skipped_referenced_total=0))
        return S

    def judge(self, S, tl):
        return judge.judge(S, [], tl, nodes=("node0", "node1"))

    def test_strict_nodes_without_unproven_writes_are_not_forbidden(self):
        r = self.judge(self.samples(), self.run_tl())
        self.assertEqual(r["forbidden"], [])
        self.assertEqual(r["protections_not_sampled"], [])

    def test_an_unproven_write_fails_the_run(self):
        r = self.judge(self.samples(unproven=1), self.run_tl())
        self.assertEqual(r["outcome"], judge.FAILED)
        self.assertIn("node0 arca_node_writes_unproven_total", r["forbidden"])

    def test_a_node_not_strict_during_the_run_fails_it(self):
        r = self.judge(self.samples(strict=0), self.run_tl())
        self.assertEqual(r["outcome"], judge.FAILED)
        self.assertIn("node1 arca_node_write_ownership_strict not 1", r["forbidden"])
        # Before the stages with load (setup, smoke): not judged.
        S = self.samples(strict_at=0) + [td.smp("node0", -30_000, arca_node_write_ownership_strict=0)]
        self.assertEqual(self.judge(S, self.run_tl())["forbidden"], [])

    def test_the_counters_unsampled_are_not_a_pass(self):
        S = [s for s in self.samples() if s["target"] == "controller"]
        S += [td.smp(n, t, arca_node_reclaim_damaged=0) for t in range(0, 60_001, 15_000) for n in ("node0", "node1")]
        r = self.judge(S, self.run_tl())
        self.assertIn("node0 arca_node_writes_unproven_total", r["protections_not_sampled"])
        self.assertIn("node0 arca_node_write_ownership_strict", r["protections_not_sampled"])

    def test_a_timeline_of_the_earlier_campaign_is_judged_as_before(self):
        r = self.judge(self.samples(strict=0, unproven=1), self.run_tl(campaign=False))
        self.assertEqual(r["forbidden"], [])


class Engine(td.EngineBase):
    def test_the_timeline_is_marked_with_the_campaign(self):
        res, ev = self.go(td.FakeCluster(self.cfg, self.clock))
        with open(os.path.join(ev.dir, "timeline.json")) as f:
            tl = json.load(f)
        self.assertEqual(tl["campaign"], "0.2.0")
        self.assertTrue(tl["3"]["write_ownership_strict"])

    def test_the_attempt_is_marked_made_before_its_first_contact(self):
        """§3: the ledger counts an attempt by its events; the first is
        written before the cluster is asked anything."""
        c = td.FakeCluster(self.cfg, self.clock)
        seen = []
        real = c.clock_offset_ms
        c.clock_offset_ms = lambda: seen.append(os.path.exists(os.path.join(ev_dir[0], "events.jsonl"))) or real()
        ev_dir = [os.path.join(self.dir, "evidence", self.cfg["run_id"])]
        self.go(c)
        self.assertTrue(seen and seen[0], "contacted before the attempt was recorded")
        with open(os.path.join(ev_dir[0], "events.jsonl")) as f:
            self.assertEqual(json.loads(f.readline())["event"], "contact")
        self.assertTrue(driver.campaign_ledger(os.path.join(self.dir, "evidence"))[1][0]["connected"])

    def test_nodes_not_strict_before_the_fill_end_the_run_before_any_write(self):
        c = td.FakeCluster(self.cfg, self.clock)
        c.strict = 0
        res, _ = self.go(c)
        self.assertEqual(c.steps, [])
        self.assertEqual((res["stop"]["reason"], res["stop"]["stage"]), ("stage-exit", "3"))
        self.assertNotEqual(res["outcome"], judge.PASSED)

    def test_an_unproven_write_stops_the_run_at_once(self):
        c = td.FakeCluster(self.cfg, self.clock)
        real = c.sample

        def sample():
            real()
            if c.admission == 3:  # during stage 4
                c.unproven = 1

        c.sample = sample
        res, _ = self.go(c)
        self.assertEqual((res["stop"]["reason"], res["stop"]["stage"]), ("protection-violation", "4"))
        self.assertEqual(res["stop"]["counter"], "arca_node_writes_unproven_total")
        self.assertEqual(res["outcome"], judge.FAILED)


class CommitQueue(unittest.TestCase):
    """§4.3: a PUT the commit queue refused (SlowDown before any commit
    attempt) had stored its shards: not D1's reclaims, not freed bytes."""

    QUEUE = {"arca_gateway_commit_queue_refused_full_total": 0, "arca_gateway_commit_queue_refused_wait_total": 0}

    def c1(self, refused_wait=0, sampled=True, **kw):
        S, R, tl = td.C1.build(self, **kw)
        tl["campaign"] = "0.2.0"
        for s in S:
            if s["target"] == "gateway0" and sampled:
                q = dict(self.QUEUE)
                if s["ended_wall_ms"] >= 0:
                    q["arca_gateway_commit_queue_refused_wait_total"] = refused_wait
                s["values"].update({k: {"value": v, "unit": "count"} for k, v in q.items()})
        return S, R, tl

    def test_queue_refusals_are_subtracted_from_d1s_reclaims(self):
        S, R, tl = self.c1()
        self.assertEqual(judge.c1b(S, R, tl, judge.VALUES, ["node0", "node1"])["outcome"], judge.PASSED)
        S, R, tl = self.c1(refused_wait=10)
        r = judge.c1b(S, R, tl, judge.VALUES, ["node0", "node1"])
        self.assertEqual(r["evidence"]["reclaim_commit_queue_refusals"], 10)
        self.assertEqual(r["outcome"], judge.INCONCLUSIVE, r)
        r = judge.c1a(S, R, tl, judge.VALUES, ["node0", "node1"])
        self.assertEqual(r["evidence"]["reclaim_commit_queue_refusals"], 10)

    def test_queue_counters_not_sampled_give_no_bound(self):
        S, R, tl = self.c1(sampled=False)
        r = judge.c1b(S, R, tl, judge.VALUES, ["node0", "node1"])
        self.assertEqual(r["outcome"], judge.INCONCLUSIVE, r)
        del tl["campaign"]
        self.assertEqual(judge.c1b(S, R, tl, judge.VALUES, ["node0", "node1"])["outcome"], judge.PASSED)

    def c2(self, refused, queued):
        S, R, tl = td.C2.build(self, gain=0.5)
        tl["campaign"] = "0.2.0"
        for t in range(0, 45 * 60_000 + 1, 15_000):
            S.append(td.smp("gateway0", t, **dict(self.QUEUE, arca_gateway_commit_queue_refused_wait_total=queued
                                                   if t > 60_000 else 0)))
        for k in range(refused):
            R += [{"type": "intent", "op_id": 9000 + k, "attempt": 1, "key": f"q{k}", "op": "PUT", "stage": "5",
                   "kind": "probe", "wall_ms": 60_000 + k * 10_000},
                  {"type": "outcome", "op_id": 9000 + k, "attempt": 1, "result": "refused"}]
        return judge.c2(S, R, tl, judge.VALUES, ["node0", "node1"])

    def test_refused_probes_count_as_added_once_the_queue_refused(self):
        """Available up by exactly half the decrease: refused probes do not
        raise the bar without queue refusals, and do with them."""
        self.assertEqual(self.c2(refused=200, queued=0)["outcome"], judge.PASSED)
        r = self.c2(refused=200, queued=1)
        self.assertEqual(r["outcome"], judge.FAILED, r)
        self.assertIn("commit queue", " ".join(r["evidence"]["approximate_attribution"]))


class Preflight(td.Base):
    HOST = td.Commands.HOST
    cluster = td.Commands.cluster

    def test_the_retained_directories_of_every_host_are_compared(self):
        c, rec, cfg = self.cluster(answers=[("find /mnt/data/retained-cluster-placeholder", ZERO + "\n")])
        r = c._retained()
        self.assertTrue(r["ok"], r)
        targets = {t for t, cmd, _ in rec.commands if "find " in cmd}
        self.assertEqual(targets, set(cfg["hosts"].values()))
        c, _, _ = self.cluster(answers=[("find /mnt/data/retained-cluster-placeholder", "1" * 64 + "\n")])
        self.assertFalse(c._retained()["ok"])

    def host_check(self, free_data):
        out = json.dumps({"cpus": 4, "mem_available": 6 << 30, "load1": 0.5, "free_root": 20 << 30,
                          "free_data": free_data, "boot_id": "b", "swaps": []})
        c, rec, cfg = self.cluster(answers=[("MemAvailable", out + "\n"),
                                            ("df -B1", f"{100 << 30}\n{100 << 30}\n")])
        res = c.preflight()
        cmds = [cmd for _, cmd, stdin in rec.commands if stdin == remote.HOST_CHECK]
        self.assertTrue(cmds and all(cmd.endswith(cfg["remote"]["data_mount"]) for cmd in cmds), cmds)
        return [res[f"host {h}"]["ok"] for h in cfg["hosts"]]

    def test_every_hosts_data_mount_keeps_the_floor(self):
        self.assertEqual(self.host_check(20 << 30), [True] * 3)
        self.assertEqual(self.host_check(None), [True] * 3)  # no data mount there
        self.assertEqual(self.host_check(2 << 30), [False] * 3)


class Reboot(unittest.TestCase):
    """§4.5: the watchdog starts from the preflight's boot IDs."""

    def test_a_reboot_before_the_first_round_is_seen(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            hosts = tg.FakeHosts({"t1": tg.healthy(system=True), "t2": tg.healthy(load_host=True)})
            cfg = {"run_id": "r", "hosts": {"h1": "t1", "h2": "t2"}, "load_host": "h2", "system_fdb_hosts": ["h1"],
                   "remote_run_dir": "/r", "remote_data_mount": "/d", "remote_heartbeat": "/r/hb.json",
                   "remote_test_dirs": ["/r"], "remote_test_fdb_cluster": "/r/fdb.cluster", "local_dir": d,
                   "limits": {"max_test_dir_bytes": 8 << 30}}
            self.assertEqual(watchdog.Watchdog(dict(cfg, boot_ids={"h1": "boot-1", "h2": "boot-1"}), hosts,
                                               common.FakeClock()).run(max_rounds=2), 0)
            w = watchdog.Watchdog(dict(cfg, boot_ids={"h1": "boot-0"}), hosts, common.FakeClock())
            self.assertEqual(w.run(max_rounds=2), 2)
            self.assertIn("host-reboot", [json.loads(a[1])["reason"] for t, sc, a in hosts.writes
                                           if sc == watchdog.WRITE_STOP])



class PreflightArchive(td.Base):
    HOST = td.Commands.HOST
    cluster = td.Commands.cluster

    def test_the_run_passes_the_seed_to_the_watchdog(self):
        c, _, cfg = self.cluster()
        c.boot_seed = {"host1": "b1"}
        with mock.patch.object(remote.subprocess, "Popen") as popen:
            c.start_watchdog()
        with open(os.path.join(cfg["local_dir"], "watchdog.json")) as f:
            self.assertEqual(json.load(f)["boot_ids"], {"host1": "b1"})
        self.assertEqual(popen.call_count, 1)

    def archive(self, out):
        c, rec, cfg = self.cluster(answers=[("mktemp -d", out)])
        r = c._archive()
        cmd = [x for _, x, _ in rec.commands if "mktemp -d" in x][0]
        self.assertIn("tar -xzf " + cfg["remote"]["archive_path"], cmd)
        self.assertIn("rm -rf --", cmd)
        return r["ok"]

    def test_the_extracted_binary_is_the_approved_one(self):
        self.assertTrue(self.archive(f"{ZERO}\n{ZERO}\n"))
        self.assertFalse(self.archive(f"{'1' * 64}\n{ZERO}\n"))  # another archive
        self.assertFalse(self.archive(f"{ZERO}\n{'2' * 64}\n"))  # another binary in it
        self.assertFalse(self.archive(f"{ZERO}\n{ZERO}\n{ZERO}\n"))  # two binaries
        self.assertFalse(self.archive(f"{ZERO}\n"))  # none extracted
        c, _, _ = self.cluster()
        self.assertIn("archive", c.required_checks())

    def test_a_p41_directory_not_in_the_record_fails(self):
        place = "/mnt/data/retained-cluster-placeholder"
        c, _, _ = self.cluster(answers=[("find " + place, ZERO + "\n"), ("/p41-*", "/mnt/data/p41-x\n")])
        r = c._retained()
        self.assertFalse(r["ok"])
        self.assertEqual(set(r["unrecorded"]), {"host1", "host2", "host3"})
        c, _, cfg = self.cluster(answers=[("find ", ZERO + "\n"), ("/p41-*", "/mnt/data/p41-x\n")])
        for h in c.cfg["retained_record"]:
            c.cfg["retained_record"][h] = {"/mnt/data/p41-x": ZERO}
        self.assertTrue(c._retained()["ok"])

    def test_an_all_empty_record_is_valid_and_checked_at_the_preflight(self):
        cfg = copy.deepcopy(self.cfg)
        cfg["retained_record"] = {h: {} for h in cfg["hosts"]}
        driver.validate(cfg)
        c = remote.SshCluster(driver.validate(cfg), runner=td.Recorder([("/p41-*", "/mnt/data/p41-y\n")]))
        self.assertFalse(c._retained()["ok"])
        c = remote.SshCluster(driver.validate(cfg), runner=td.Recorder())
        self.assertTrue(c._retained()["ok"])

    def test_a_group_without_its_classes_line_is_recorded(self):
        g1 = ("scrub group 1 verified 9 bytes 1 findings [0, 0, 0, 0, 0, 0, 0, 0] enqueued 0 errors 0 "
              "retained 0 retained_damaged 0 in_flight 0 unclassified 0\n")
        g2 = "scrub group 2 verified 9 bytes 1 findings [0, 0, 0, 0, 0, 0, 0, 0] enqueued 0 errors 0\n"
        c, _, _ = self.cluster(answers=[("grep '^group '", "group 1 x\ngroup 2 y\n"),
                                        ("scrub --group 1 ", g1), ("scrub --group 2 ", g2)])
        self.assertEqual(c.integrity("scrub"), 0)
        self.assertEqual(c.last_scrub_unclassed, [2])


if __name__ == "__main__":
    unittest.main()
