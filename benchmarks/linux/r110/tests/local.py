"""Local stand-ins that run the real tools: a load process (``load.py``)
driven through its control directory against a local fake S3, and a
cluster object for ``driver.Run`` built on it. Only the samples are
synthesised; the load, its budget, its ledger and the control files are
the real ones."""

import json
import os
import subprocess
import sys
import time

from r110 import common, ledger as ledger_mod

HERE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class LocalLoad:
    """A real ``load.py run`` process in ``run_dir``."""

    def __init__(self, run_dir, endpoint, run_id="run-local", limits=None, object_size=1024, bucket="r110",
                 heartbeat=None):
        self.dir = run_dir
        os.makedirs(os.path.join(run_dir, "control"), exist_ok=True)
        with open(os.path.join(run_dir, "run.json"), "w") as f:
            json.dump({"run_id": run_id}, f)
        code = f"import sys; sys.path.insert(0, {HERE!r}); from r110.load import main; sys.exit(main())"
        argv = [sys.executable, "-I", "-c", code, "run", "--run-dir", run_dir, "--endpoint", endpoint,
                "--object-size", str(object_size), "--bucket", bucket,
                "--limits", json.dumps(limits or {})]
        if heartbeat:
            argv += ["--heartbeat", heartbeat]
        self.proc = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.n = 0

    def send(self, step):
        self.n += 1
        p = os.path.join(self.dir, "control", f"{self.n:06d}.json")
        with open(p + ".tmp", "w") as f:
            json.dump(step, f)
        os.replace(p + ".tmp", p)
        return self.n

    def done(self, n):
        open(os.path.join(self.dir, "control", f"{n:06d}.done"), "w").close()

    def status(self):
        try:
            return common.read_json(os.path.join(self.dir, "status.json"))
        except (OSError, ValueError):
            return None

    def result(self, n, timeout=60):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            st = self.status()
            if st:
                for s in st.get("steps", []):
                    if s["n"] == n:
                        return s["result"]
                if st.get("exit"):
                    return None
            time.sleep(0.05)
        return None

    def records(self):
        return ledger_mod.read(os.path.join(self.dir, "ledger.jsonl"))[0]

    def stop(self, reason="operator"):
        common.StopFile(os.path.join(self.dir, "stop.json")).stop(reason, "test", 0)

    def close(self, timeout=60):
        if self.proc.poll() is None:
            self.send({"kind": "finish"})
            try:
                self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        return self.proc.returncode


class LocalCluster:
    """``driver.Run``'s cluster over a ``LocalLoad``: the steps, the stop
    record and the ledger are the real load's; the samples are made by
    ``sample()`` (a callable returning ``{target: {name: value}}``)."""

    def __init__(self, load, sample, nodes=("node0", "node1")):
        self.load, self.sample, self._nodes = load, sample, list(nodes)
        self._samples = []

    def nodes(self):
        return list(self._nodes)

    def gateways(self):
        return ["gateway0"]

    def targets(self):
        return ["gateway0", "controller"] + self._nodes

    def clock_offset_ms(self):
        return 0

    def samples(self):
        t = time.time_ns() // 1_000_000
        for target, vals in self.sample().items():
            self._samples.append({"target": target, "status": "ok", "started_wall_ms": t, "ended_wall_ms": t,
                                  "values": {k: {"value": v, "unit": "count"} for k, v in vals.items()}})
        return list(self._samples)

    def send_step(self, step):
        return self.load.send(step)

    def step_done(self, n):
        self.load.done(n)

    def run_step(self, step, deadline, check=None):
        n = self.load.send(dict(step, max_seconds=max(1, int(deadline - time.monotonic()))))
        return self.load.result(n, timeout=max(1, deadline - time.monotonic()))

    def wait_step(self, n, deadline):
        return self.load.result(n, timeout=max(1, deadline - time.monotonic()))

    def stop_record(self):
        return common.StopFile(os.path.join(self.load.dir, "stop.json")).reason()

    def write_stop(self, reason):
        common.StopFile(os.path.join(self.load.dir, "stop.json")).stop(reason, "driver", 0)

    def ledger_records(self):
        return self.load.records()

    def stage4_keys(self):
        return sorted(set(ledger_mod.acknowledged(self.ledger_records(), "PUT", stage="4")))
