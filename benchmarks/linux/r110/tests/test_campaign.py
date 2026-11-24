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

from r110 import driver, judge, remote
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
        self.refused(lambda c: c.update(retained_record={h: {} for h in c["hosts"]}))
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


class Approval(td.Base):
    def calls(self):
        return td.Approval.calls(self)

    def invoke(self, argv):
        return td.Approval.invoke(self, argv)

    def run_cfg(self, n):
        cfg = copy.deepcopy(self.cfg)
        cfg["run_id"] = f"r110-020-{n}-20270101T000000Z"
        return self.write(cfg)

    def preflight(self, n, **extra):
        p, sha = self.run_cfg(n)
        return self.invoke(["preflight", p, "--execute",
                            "--approval", self.approval(sha, ("preflight",), **extra)])

    def test_an_approval_of_the_earlier_campaign_is_refused(self):
        self.assertEqual(self.preflight(1, campaign=None, campaign_run=None, candidate=None), (2, []))
        self.assertEqual(self.preflight(1, campaign="0.1.0"), (2, []))

    def test_the_approval_names_this_main_run(self):
        self.assertEqual(self.preflight(1, campaign_run=2), (2, []))
        self.assertEqual(self.preflight(1, candidate="maybe"), (2, []))
        self.assertEqual(self.preflight(1)[0], 0)

    def test_a_later_run_records_the_cause_the_change_and_the_hypothesis(self):
        rerun = {"previous_run_id": "r110-020-1-20270101T000000Z", "cause": "C5 Failed: 2 orphans",
                 "change": "none (a product fix in the candidate)", "hypothesis": "the orphans were the bug's"}
        self.assertEqual(self.preflight(2, campaign_run=2), (2, []))
        for k in rerun:
            self.assertEqual(self.preflight(2, campaign_run=2, rerun=dict(rerun, **{k: " "})), (2, []), k)
        self.assertEqual(self.preflight(2, campaign_run=2,
                                        rerun=dict(rerun, previous_run_id="r110-020-2-20270101T000000Z")), (2, []))
        self.assertEqual(self.preflight(2, campaign_run=2, rerun=rerun)[0], 0)

    def test_the_last_run_is_kept_for_the_final_candidate(self):
        rerun = {"previous_run_id": "r110-020-2-20270101T000000Z", "cause": "c", "change": "c", "hypothesis": "h"}
        self.assertEqual(self.preflight(3, campaign_run=3, rerun=rerun), (2, []))
        self.assertEqual(self.preflight(3, campaign_run=3, rerun=rerun, candidate="final")[0], 0)


def s7(sc, by, held, errors=0, unread=(), settled=None, pending=10, complete=True):
    return {"campaign": "0.2.0",
            "7": {"verify": {"violations": [], "complete": complete, "keys": 1}, "manifest_findings": 0,
                  "scrub_findings": sc, "scrub_by_class": by, "scrub_held": held, "scrub_errors": errors,
                  "scrub_unread_groups": list(unread), "settled_ms": settled, "pending_before_scrub": pending}}


HELD = {"retained": 40, "retained_damaged": 0, "in_flight": 3, "unclassified": 0}


class ScrubClasses(unittest.TestCase):
    """§4.1: retained and in-flight shards are not findings; every finding
    fails; errors and missing classes are never a pass."""

    def c5(self, *a, **k):
        return judge.c5(s7(*a, **k))

    def test_retained_and_in_flight_shards_are_not_findings(self):
        r = self.c5(0, [0] * 8, HELD)
        self.assertEqual(r["outcome"], judge.PASSED, r)
        self.assertEqual(r["evidence"]["scrub_held"], HELD)

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


if __name__ == "__main__":
    unittest.main()
