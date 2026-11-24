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


def facts(version="arca-lake 0.2.0", sha=SHA, v010=drv.V010_SHA256, v010_version="arca-lake 0.1.0"):
    return {h: {"version": version, "sha256": sha, "v010_version": v010_version, "v010_sha256": v010}
            for h in "abc"}


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
        self.assertEqual(drv.preflight_problems(facts(), "run", True, "arca-lake 0.2.0"), [])
        f = facts()
        f["b"]["version"] = "arca-lake 0.1.0"
        why = drv.preflight_problems(f, "run", True, "arca-lake 0.2.0")
        self.assertEqual(len(why), 1)
        self.assertIn("host b", why[0])
        # A rehearsal of a build whose version is not bumped yet.
        self.assertEqual(drv.preflight_problems(facts(version="arca-lake 0.1.0"), "run", False,
                                                "arca-lake 0.2.0"), [])

    def test_every_run_needs_one_binary_on_every_host(self):
        f = facts()
        f["c"]["sha256"] = "0" * 64
        self.assertTrue(drv.preflight_problems(f, "run", False, "arca-lake 0.2.0"))
        f = facts()
        for h in "abc":
            f[h]["sha256"] = "sha256sum: no such file"
        self.assertTrue(drv.preflight_problems(f, "run", False, "arca-lake 0.2.0"))

    def test_the_upgrade_needs_the_published_0_1_0(self):
        self.assertEqual(drv.preflight_problems(facts(), "upgrade", True, "arca-lake 0.2.0"), [])
        self.assertTrue(drv.preflight_problems(facts(v010="1" * 64), "upgrade", False, "arca-lake 0.2.0"))
        self.assertTrue(drv.preflight_problems(facts(v010_version="arca-lake 0.2.0"), "upgrade", False,
                                               "arca-lake 0.2.0"))
        # The main scenario does not use it.
        self.assertEqual(drv.preflight_problems(facts(v010=""), "run", True, "arca-lake 0.2.0"), [])


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
                                  ledger=os.path.join(d, "ledger.jsonl"), phase="t", put_rate_mib=0)
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
            outcomes = [json.loads(l)["outcome"] for l in open(r.args.ledger)]
            self.assertEqual(outcomes, ["ok", "VIOLATION", "VIOLATION"])
            self.assertEqual([c["ok"] for c in cases], [True, False, False])

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
