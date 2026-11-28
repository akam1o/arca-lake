"""Local tests of three-host-validate.py and three-host-client.py (no
host): the judged-run and binary preflight rules, output parsing, the
ledger rules of the refused writes, and the digests the client sends.

  python3 -I benchmarks/linux/three_host_test.py -v
"""

import argparse
import hashlib
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, HERE / file)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


drv = load("three_host_validate", "three-host-validate.py")
cli = load("three_host_client", "three-host-client.py")

SHA = "9efed01c6cceecc06a46fcd38fdbfc4d9e4c574223f9d953d171e306857517b6"


SOURCE = "a" * 40


def facts(version="arca-lake 0.2.0", sha=SHA, v010=drv.V010_SHA256, v010_version="arca-lake 0.1.0"):
    return {h: {"version": version, "sha256": sha, "v010_version": v010_version, "v010_sha256": v010,
                "record": {"kind": "release-candidate", "source_sha": SOURCE, "binary_sha256": sha,
                           "archive_sha256": "4" * 64},
                "archive_sha256": drv.V010_ARCHIVE_SHA256, "archive_commit": drv.V010_COMMIT,
                "archive_binary_sha256": drv.V010_SHA256, "free_bytes": 20 << 30, "on_root_fs": h != "a"}
            for h in "abc"}


def problems(f, scenario="run", judged=True, budget=drv.PUT_BUDGET_MIB):
    return drv.preflight_problems(f, scenario, judged, "arca-lake 0.2.0", SOURCE, budget)


class JudgedRun(unittest.TestCase):
    def test_a_judged_run_refuses_the_debugging_switches(self):
        base = {"JUDGED": "1", "SOURCE_SHA": "abc"}
        self.assertEqual(drv.judged_refusal(base), [])
        for var in ("CONTINUE_ON_FAIL", "STOP_AFTER"):
            why = drv.judged_refusal({**base, var: "1"})
            self.assertEqual(len(why), 1)
            self.assertIn(var, why[0])
        self.assertIn("SOURCE_SHA", drv.judged_refusal({"JUDGED": "1"})[0])

    def test_a_run_not_judged_may_use_them(self):
        self.assertEqual(drv.judged_refusal({"CONTINUE_ON_FAIL": "1", "STOP_AFTER": "2c-multipart"}), [])

    def test_a_judged_run_needs_the_expected_version_everywhere(self):
        self.assertEqual(problems(facts()), [])
        f = facts()
        f["b"]["version"] = "arca-lake 0.1.0"
        why = problems(f)
        self.assertEqual(len(why), 1)
        self.assertIn("host b", why[0])
        # A rehearsal of a build whose version is not bumped yet.
        self.assertEqual(problems(facts(version="arca-lake 0.1.0"), judged=False), [])

    def test_a_judged_run_needs_the_build_record_of_source_sha_and_this_binary(self):
        for field, value in (("source_sha", "b" * 40), ("binary_sha256", "0" * 64)):
            f = facts()
            f["c"]["record"][field] = value
            why = problems(f)
            self.assertEqual(len(why), 1, why)
            self.assertIn("build record", why[0])
            self.assertEqual(problems(f, judged=False), [])
        f = facts()
        f["a"]["record"] = None
        why = problems(f)
        self.assertTrue(any("host a" in w and "install-candidate" in w for w in why), why)

    def test_a_judged_run_needs_the_release_candidate_archive(self):
        f = facts()
        f["b"]["record"]["kind"] = "rehearsal-build"
        why = problems(f)
        self.assertEqual(len(why), 1, why)
        self.assertIn("install-candidate", why[0])
        self.assertEqual(problems(f, judged=False), [])
        f = facts()
        f["c"]["record"]["archive_sha256"] = "5" * 64
        self.assertIn("candidate archive differs", problems(f)[0])

    def test_every_run_needs_one_binary_on_every_host(self):
        f = facts()
        f["c"]["sha256"] = "0" * 64
        self.assertTrue(problems(f, judged=False))
        f = facts()
        for h in "abc":
            f[h]["sha256"] = None
        self.assertTrue(problems(f, judged=False))
        f = facts()
        f["b"] = {"error": "ssh failed"}
        self.assertTrue(problems(f, judged=False))

    def test_the_upgrade_needs_the_published_0_1_0_and_its_archive(self):
        self.assertEqual(problems(facts(), "upgrade"), [])
        self.assertTrue(problems(facts(v010="1" * 64), "upgrade", judged=False))
        self.assertTrue(problems(facts(v010_version="arca-lake 0.2.0"), "upgrade", judged=False))
        for field, value in (("archive_sha256", None), ("archive_sha256", "2" * 64), ("archive_commit", "c" * 40),
                             ("archive_binary_sha256", "3" * 64)):
            f = facts()
            f["b"][field] = value
            why = problems(f, "upgrade", judged=False)
            self.assertEqual(len(why), 1, (field, why))
            self.assertIn("archive", why[0])
        # The main scenario does not use it.
        self.assertEqual(problems(facts(v010="", v010_version=""), "run"), [])

    def test_free_space_and_host_a_root(self):
        need = (drv.PUT_BUDGET_MIB << 20) + drv.FREE_MARGIN
        f = facts()
        f["c"]["free_bytes"] = need - 1
        why = problems(f, judged=False)
        self.assertEqual(len(why), 1)
        self.assertIn("host c", why[0])
        f["c"]["free_bytes"] = need
        self.assertEqual(problems(f, judged=False), [])
        f["a"]["on_root_fs"] = True
        self.assertIn("root file system", problems(f, judged=False)[0])

    def test_the_driver_and_the_client_share_the_put_budget(self):
        self.assertEqual(drv.PUT_BUDGET_MIB, cli.PUT_BUDGET_MIB)


class Remote(unittest.TestCase):
    """The scripts run on the hosts with `python3 -I`, here on a fake
    archive."""

    def archive(self, d, binary=b"not the published binary", commit=drv.V010_COMMIT):
        import io
        import tarfile
        path = os.path.join(d, drv.V010_ARCHIVE)
        with tarfile.open(path, "w:gz") as t:
            for name, data in (("arca-lake", binary), ("BUILD-INFO.txt", f"source commit: {commit} (HEAD)\n".encode())):
                info = tarfile.TarInfo(f"arca-lake-0.1.0-x86_64-unknown-linux-gnu/{name}")
                info.size = len(data)
                t.addfile(info, io.BytesIO(data))
        return path

    def test_facts_read_the_archive_without_extracting_it(self):
        import subprocess
        import sys
        with tempfile.TemporaryDirectory() as d:
            path = self.archive(d)
            p = subprocess.run([sys.executable, "-I", "-", d, os.path.join(d, "none"), os.path.join(d, "none"), path],
                               input=drv.FACTS_SCRIPT, capture_output=True, text=True, check=True)
            f = json.loads(p.stdout)
            self.assertEqual(f["archive_binary_sha256"], hashlib.sha256(b"not the published binary").hexdigest())
            self.assertEqual(f["archive_commit"], drv.V010_COMMIT)
            self.assertIsNone(f["sha256"])
            self.assertIsNone(f["record"])
            self.assertGreater(f["free_bytes"], 0)
            self.assertEqual(sorted(os.listdir(d)), [drv.V010_ARCHIVE])
            # And the preflight refuses it.
            g = facts()
            g["a"].update({k: f[k] for k in ("archive_sha256", "archive_commit", "archive_binary_sha256")})
            self.assertTrue(problems(g, "upgrade", judged=False))

    def test_the_install_refuses_a_binary_that_is_not_the_published_one(self):
        import subprocess
        import sys
        with tempfile.TemporaryDirectory() as d:
            path = self.archive(d)
            dest = os.path.join(d, "arca-lake-0.1.0")
            p = subprocess.run([sys.executable, "-I", "-c", drv.INSTALL_V010, path, dest], capture_output=True, text=True)
            self.assertNotEqual(p.returncode, 0)
            self.assertIn("not the published binary", p.stderr)
            self.assertFalse(os.path.exists(dest) or os.path.exists(dest + ".new"))


class Candidate(unittest.TestCase):
    """install-candidate's host script on a fake package.sh archive."""

    def archive(self, d, commit=SOURCE, version="arca-lake 0.2.0"):
        import io
        import tarfile
        path = os.path.join(d, "arca-lake-0.2.0-x86_64-unknown-linux-gnu.tar.gz")
        files = (("arca-lake", f"#!/bin/sh\necho '{version}'\n".encode(), 0o755),
                 ("BUILD-INFO.txt", f"version: 0.2.0\nsource commit: {commit} (HEAD verified, tree clean)\n".encode(), 0o644))
        with tarfile.open(path, "w:gz") as t:
            for name, data, mode in files:
                info = tarfile.TarInfo(f"arca-lake-0.2.0-x86_64-unknown-linux-gnu/{name}")
                info.size, info.mode = len(data), mode
                t.addfile(info, io.BytesIO(data))
        with open(path, "rb") as f:
            return path, hashlib.sha256(f.read()).hexdigest()

    def install(self, d, path, expected, sha=SOURCE):
        import subprocess
        import sys
        work = os.path.join(d, "candidate")
        os.mkdir(work)
        os.mkdir(os.path.join(d, "bin"))
        dest = os.path.join(d, "bin", "arca-lake")
        p = subprocess.run([sys.executable, "-I", "-c", drv.CANDIDATE_SCRIPT, path, expected, sha, "arca-lake 0.2.0",
                            work, dest], capture_output=True, text=True)
        return p, dest

    def test_the_candidate_is_installed_with_its_record(self):
        with tempfile.TemporaryDirectory() as d:
            path, digest = self.archive(d)
            p, dest = self.install(d, path, digest)
            self.assertEqual(p.returncode, 0, p.stderr)
            with open(os.path.join(d, "bin", "build-record.json")) as f:
                rec = json.load(f)
            with open(dest, "rb") as f:
                self.assertEqual(rec["binary_sha256"], hashlib.sha256(f.read()).hexdigest())
            self.assertEqual((rec["kind"], rec["archive_sha256"], rec["source_sha"], rec["version"]),
                             ("release-candidate", digest, SOURCE, "arca-lake 0.2.0"))
            # And a judged preflight takes it.
            g = facts(sha=rec["binary_sha256"])
            for h in "abc":
                g[h]["record"] = rec
            self.assertEqual(problems(g), [])

    def test_a_wrong_digest_commit_or_version_installs_nothing(self):
        for case in ("digest", "commit", "version"):
            with tempfile.TemporaryDirectory() as d:
                path, digest = self.archive(d, commit="b" * 40 if case == "commit" else SOURCE,
                                            version="arca-lake 0.1.0" if case == "version" else "arca-lake 0.2.0")
                p, dest = self.install(d, path, "0" * 64 if case == "digest" else digest)
                self.assertNotEqual(p.returncode, 0, case)
                self.assertIn("install-candidate:", p.stderr, case)
                self.assertFalse(os.path.exists(dest), case)
                self.assertFalse(os.path.exists(os.path.join(d, "bin", "build-record.json")), case)


class LoadWrapper(unittest.TestCase):
    def test_the_client_pid_and_its_exit_status_are_recorded(self):
        import subprocess
        import time
        with tempfile.TemporaryDirectory() as d:
            saved = drv.CLIENT, drv.R
            fake = os.path.join(d, "fake-client.sh")
            with open(fake, "w") as f:
                f.write("#!/bin/sh\nsleep 0.3\nexit 3\n")
            os.chmod(fake, 0o755)
            drv.CLIENT, drv.R = f"sh {fake}", d
            try:
                inner = drv.load_command("s9", "x")
            finally:
                drv.CLIENT, drv.R = saved
            w = subprocess.Popen(["sh", "-c", inner])
            for _ in range(50):
                if os.path.exists(os.path.join(d, "client-s9-py.pid")):
                    break
                time.sleep(0.02)
            with open(os.path.join(d, "client-s9-py.pid")) as f:
                py = int(f.read())
            self.assertNotEqual(py, w.pid)  # the client itself, not its wrapper
            w.wait(5)
            with open(os.path.join(d, "client-s9.rc")) as f:
                self.assertEqual(f.read().strip(), "3")


class Checks(unittest.TestCase):
    def recs(self, n_b, n_c, extra=()):
        r = [{"gw": "gw-b", "outcome": "ack", "t0": 1, "t1": 2}] * n_b + \
            [{"gw": "gw-c", "outcome": "ok", "t0": 1, "t1": 2}] * n_c
        return r + list(extra)

    def test_a_load_phase_needs_a_clean_exit_and_acknowledged_requests_on_each_gateway(self):
        n = drv.MIN_PHASE_ACKED
        self.assertTrue(drv.load_check("p", "0\n", "", self.recs(n, n))["ok"])
        for rc, log, recs in (("3\n", "", self.recs(n, n)), ("", "", self.recs(n, n)),
                              ("0\n", "worker failed:\nTraceback (most recent call last):", self.recs(n, n)),
                              ("0\n", "", self.recs(n, n - 1)),
                              ("0\n", "", self.recs(n, n, [{"gw": "gw-b", "outcome": "local-error", "t0": 1, "t1": 2}])),
                              ("0\n", "", self.recs(0, n))):
            self.assertFalse(drv.load_check("p", rc, log, recs)["ok"], (rc, log, len(recs)))
        # Errors and refusals are not acknowledgements.
        bad = [{"gw": g, "outcome": o, "t0": 1, "t1": 2} for g in ("gw-b", "gw-c") for o in ("error", "refused")] * n
        self.assertFalse(drv.load_check("p", "0", "", bad)["ok"])

    def test_a_window_needs_acknowledged_requests_inside_it(self):
        drv.CHECKS.clear()
        recs = self.recs(3, 3) + [{"gw": "gw-c", "outcome": "ok", "t0": 5, "t1": 6}] * 9
        self.assertTrue(drv.need_window("w", recs, 0, 3))
        self.assertFalse(drv.need_window("w", recs, 4, 7))  # gw-b has none there
        self.assertTrue(drv.need_window("w", recs, 4, 7, gws=("gw-c",)))
        self.assertEqual([c["ok"] for c in drv.CHECKS], [True, False, True])
        drv.CHECKS.clear()

    def test_a_failed_check_fails_a_passing_step(self):
        ok = {"check": "x", "ok": True}
        bad = {"check": "y", "ok": False}
        self.assertEqual(drv.judge_checks("PASS", {"a": 1}, []), ("PASS", {"a": 1}))
        v, ev = drv.judge_checks("PASS", {"a": 1}, [ok])
        self.assertEqual((v, ev["load_and_window_checks"]), ("PASS", [ok]))
        self.assertEqual(drv.judge_checks("PASS", {}, [ok, bad])[0], "FAIL")
        self.assertEqual(drv.judge_checks("FAIL", {}, [ok])[0], "FAIL")


class Layout(unittest.TestCase):
    def test_the_new_node_has_the_same_allotment_as_the_others(self):
        # c2-j1: a smaller c2 kept its group out of placement's 10 % band.
        saved = dict(drv.ADDR)
        drv.ADDR.update({"a": "192.0.2.1", "b": "192.0.2.2", "c": "192.0.2.3"})
        try:
            self.assertIn(f"--capacity-bytes {drv.CAPACITY} ", drv.node_cmd("c2"))
            self.assertIn(f"--capacity-bytes {drv.CAPACITY} ", drv.node_cmd("a0"))
        finally:
            drv.ADDR.clear()
            drv.ADDR.update(saved)
        src = (Path(__file__).resolve().parent / "three-host-validate.py").read_text()
        self.assertIn('start("c", "n-c2", node_cmd("c2"))', src)

    def test_upgrade_restart_windows_last_25_s(self):
        # c2-uj1: an 11.6 s window after a controller restart fell into a
        # pause of the paced load.
        self.assertEqual(drv.RESTART_WINDOW_S, 25)
        self.assertEqual(drv.MIN_WINDOW_ACKED, 3)
        src = (Path(__file__).resolve().parent / "three-host-validate.py").read_text()
        u2 = src[src.index("def u2():"):src.index("def u3():")]
        u3 = src[src.index("def u3():"):src.index("def u4():")]
        self.assertIn("10 + RESTART_WINDOW_S - (ts(\"c\") - t)", u2)
        self.assertIn("RESTART_WINDOW_S - (ts(\"c\") - t)", u3)
        self.assertNotIn("time.sleep(15)", u2)
        self.assertNotIn("time.sleep(10)\n            st = status()", u3)

    def test_the_driver_commit_is_recorded(self):
        self.assertRegex(drv.driver_sha(), r"^([0-9a-f]{40}(-dirty)?)?$")


class Parsing(unittest.TestCase):
    def test_verify_object_lists_only_chunks_not_whole(self):
        head = "object v3h/k00/001 version null generation 01a2 multipart false chunks 2 verdict "
        self.assertEqual(drv.parse_verify_object(head + "Healthy\nmode read-only (nothing was changed)\n",
                                                 "k00/001"), set())
        out = (head + "Degraded\n  part None chunk 1 id 01a2 group 4 good 2/3 need 2 affected "
               "[2@node6:Unreachable]\n  part Some(2) chunk 0 id 01a3 group 2 good 2/3 need 2 affected []\n")
        self.assertEqual(drv.parse_verify_object(out, "k00/001"), {2, 4})
        self.assertIsNone(drv.parse_verify_object("error: object not found\n", "k00/001"))
        self.assertIsNone(drv.parse_verify_object(head + "Healthy\n", "k00/0011"))

    def test_scrub_lines_of_both_releases(self):
        line = "scrub group 2 verified 955 bytes 987077568 findings [0, 2, 0, 0, 1, 0, 0, 0] enqueued 2 errors 0"
        r = drv.parse_scrub(line)
        self.assertEqual(r["findings"]["ChecksumMismatch"], 2)
        self.assertEqual(r["findings"]["UnexpectedShard"], 1)
        self.assertNotIn("held", r)
        r = drv.parse_scrub(line + " retained 810 retained_damaged 0 in_flight 33 unclassified 4")
        self.assertEqual(r["held"], {"retained": 810, "retained_damaged": 0, "in_flight": 33, "unclassified": 4})
        self.assertIsNone(drv.parse_scrub("error: group 9 unknown"))

    def test_acked_in_takes_each_keys_last_write(self):
        recs = [
            {"op": "PUT", "key": "a", "outcome": "ack", "t0": 5},
            {"op": "PUT", "key": "b", "outcome": "ack", "t0": 5},
            {"op": "DELETE", "key": "b", "outcome": "ack", "t0": 6},
            {"op": "PUT", "key": "c", "outcome": "error", "t0": 5},
            {"op": "MPU", "key": "d", "outcome": "ack", "t0": 7},
            {"op": "PUT", "key": "e", "outcome": "ack", "t0": 1},
            {"op": "GET", "key": "e", "outcome": "ok", "t0": 5},
        ]
        self.assertEqual(drv.acked_in(recs, 4, 10), ["a", "d"])


class Client(unittest.TestCase):
    def runner(self, d):
        args = argparse.Namespace(gateways="gw-x=127.0.0.1:1", state=os.path.join(d, "state.json"),
                                  ledger=os.path.join(d, "ledger.jsonl"), phase="t", put_rate_mib=0, timeout=5,
                                  put_budget_mib=cli.PUT_BUDGET_MIB)
        return cli.Runner(args)

    def test_digests(self):
        self.assertEqual(cli.b64_crc32(b"123456789"), "y/Q5Jg==")  # CRC-32 check value 0xCBF43926
        self.assertEqual(cli.b64_md5(b""), "1B2M2Y8AsgTpgAmY7PhCfg==")  # MD5 d41d8cd9...
        x = cli.delete_xml(["auth/gw-b-del1", "auth/gw-b-del2"])
        self.assertIn(b"<Object><Key>auth/gw-b-del2</Key></Object>", x)
        self.assertTrue(x.startswith(b"<?xml"))

    def test_a_refused_put_leaves_the_key_and_a_taken_or_unknown_one_is_recorded(self):
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d)
            old = ["0" * 64, 10]
            body = b"new body"
            new = [hashlib.sha256(body).hexdigest(), len(body)]
            for res, want in [((400, None, 0, "http", None, None), [old]),
                              ((500, None, 0, "http", None, None), [old, new]),
                              ((None, None, 0, "unknown", "reset", None), [old, new]),
                              ((None, None, 0, "refused", "connect", None), [old]),
                              ((200, None, 0, "http", None, None), [new])]:
                r.state.keys["k"] = [old]
                r.note_refused_put("k", res, body)
                self.assertEqual(r.state.keys["k"], want, res)
            r.ledger.close()

    def test_delete_objects_outcomes(self):
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d)
            body = ["1" * 64, 5]
            for res, want in [((400, None, 0, "http", None, None), [body]),
                              ((503, None, 0, "http", None, None), [body, cli.ABSENT]),
                              ((None, None, 0, "refused", "connect", None), [body]),
                              ((200, None, 0, "http", None, None), [cli.ABSENT])]:
                r.state.keys = {"x": [body], "y": [body]}
                r.note_delete_objects(["x", "y"], res)
                self.assertEqual(r.state.keys, {"x": want, "y": want}, res)
            r.ledger.close()

    def test_a_check_records_its_verdict(self):
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d)
            gw = ("gw-x", "127.0.0.1", 1)
            cases = []
            err = {"body": b"<Error><Code>BadDigest</Code></Error>", "headers": {}}
            self.assertTrue(r.expect(cases, "md5", gw, (400, None, 0, "http", None, err), {400}, {"BadDigest"}))
            self.assertFalse(r.expect(cases, "md5", gw, (500, None, 0, "http", None, err), {400}, {"BadDigest"}))
            self.assertFalse(r.expect(cases, "md5", gw, (None, None, 0, "unknown", "x", None), {400}))
            r.ledger.close()
            with open(r.args.ledger) as f:
                outcomes = [json.loads(l)["outcome"] for l in f]
            self.assertEqual(outcomes, ["ok", "VIOLATION", "VIOLATION"])
            self.assertEqual([c["ok"] for c in cases], [True, False, False])

    def test_a_signing_failure_is_a_local_error_and_changes_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d)
            saved = cli.sign_headers, cli.CREDS

            def broken(*a):
                raise RuntimeError("no botocore")

            cli.sign_headers, cli.CREDS = broken, ("AK", "SK")
            try:
                res = cli.call(("gw-x", "127.0.0.1", 1), "GET", "/v3h/k")
                self.assertEqual(res[3], "local")
                r.state.keys["k"] = [["0" * 64, 1]]
                self.assertEqual(r.do_put(0, ("gw-x", "127.0.0.1", 1), "k", 10), "local-error")
                self.assertEqual(r.do_delete(0, ("gw-x", "127.0.0.1", 1), "k"), "local-error")
                self.assertEqual(r.do_get(0, ("gw-x", "127.0.0.1", 1), "k"), "local-error")
                self.assertEqual(r.state.keys["k"], [["0" * 64, 1]])
            finally:
                cli.sign_headers, cli.CREDS = saved
            r.ledger.close()

    def test_the_put_budget_ends_the_run_with_a_failure(self):
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d)
            r.args.put_budget_mib = 1
            self.assertFalse(r.budget_spent(1 << 20))
            r.state.put_bytes = 1
            self.assertTrue(r.budget_spent(1 << 20))
            self.assertTrue(r.stop.is_set())
            self.assertIn("put budget", r.failures[0])
            r.ledger.close()

    def test_a_worker_exception_stops_the_run_with_a_failure(self):
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d)

            def boom():
                raise ValueError("bad")

            import contextlib
            import io
            with contextlib.redirect_stderr(io.StringIO()) as err:
                r.guarded(boom)
            self.assertTrue(r.stop.is_set())
            self.assertIn("ValueError: bad", r.failures[0])
            self.assertIn("Traceback", err.getvalue())
            r.ledger.close()

    @unittest.skipUnless(importlib.util.find_spec("botocore"), "botocore not installed (the client host has it)")
    def test_requests_are_signed_over_the_payload(self):
        body = b"payload"
        h = cli.sign_headers("PUT", "127.0.0.1", 9600, "/v3h/k", "", body, ("AKIAEXAMPLE", "secret"),
                             {"Content-MD5": cli.b64_md5(body)})
        self.assertTrue(h["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/"))
        self.assertIn("/us-east-1/s3/aws4_request", h["Authorization"])
        self.assertIn("content-md5", h["Authorization"])
        self.assertEqual(h["X-Amz-Content-SHA256"], hashlib.sha256(body).hexdigest())


if __name__ == "__main__":
    unittest.main()
