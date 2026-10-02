from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock
import zipfile

from scripts import bench_rl_scale as bench

TEST_TIER = "contract"


SCRIPT = Path(bench.__file__)


def sample_rows(values, *, image="img", node="n1"):
    return [
        {"ok": True, "image": image, "node": node, "sandbox_id": f"s{index}",
         "time_to_first_command_seconds": value, "create_seconds": value / 2,
         "first_exec_seconds": value / 2, "completion_seconds": value + index,
         "queued_seconds": float(index)}
        for index, value in enumerate(values)
    ]


class PureComputationTests(unittest.TestCase):
    def test_nearest_rank_percentiles(self):
        summary = bench.latency_summary(range(1, 101))
        self.assertEqual({key: summary[key] for key in ("n", "p50", "p95", "p99", "max", "min")},
                         {"n": 100, "p50": 50, "p95": 95, "p99": 99, "max": 100, "min": 1})
        self.assertEqual(bench.latency_summary([3.0])["p99"], 3.0)
        self.assertEqual(bench.latency_summary([2, 1])["p50"], 1)
        self.assertRaises(ValueError, bench.percentile, [], 0.5)
        self.assertRaises(ValueError, bench.percentile, [1], 0)
        summary = bench.latency_summary([])  # an empty summary has null percentiles
        self.assertEqual(summary["n"], 0)
        self.assertTrue(all(summary[key] is None for key in ("p50", "p95", "p99", "max")))

    def test_round_robin_assignment_and_duplicate_detection(self):
        self.assertEqual(bench.assign_images(5, ["a", "b"]), ["a", "b", "a", "b", "a"])
        self.assertRaises(ValueError, bench.assign_images, 1, [])
        self.assertEqual(bench.duplicate_images(["a", "b", "a", "c", "b"]), ["a", "b"])
        self.assertEqual(bench.duplicate_images(["a", "b"]), [])

    def test_resident_binning_and_density_limit(self):
        samples = (
            [{"resident": 16, "ok": True, "seconds": 0.1}] * 10
            + [{"resident": 32, "ok": True, "seconds": 0.2}] * 10
            + [{"resident": 48, "ok": True, "seconds": 2.0}] * 10
            + [{"resident": 64, "ok": True, "seconds": 0.1}] * 10
        )
        bins = bench.bin_by_resident(samples, 16)
        self.assertEqual([(row["resident_low"], row["resident_high"]) for row in bins],
                         [(1, 16), (17, 32), (33, 48), (49, 64)])
        verdict = bench.density_at_limit(bins, 1.0)
        # A later passing bin never hides the first violation.
        self.assertEqual(verdict["max_resident_within_limit"], 32)
        self.assertEqual(verdict["first_violating_bin"]["resident_max_observed"], 48)
        self.assertEqual(verdict["first_violating_bin"]["p99"], 2.0)
        samples = [{"resident": 4, "ok": True, "seconds": 0.1},  # failures violate their bin
                   {"resident": 4, "ok": False, "seconds": 0.1}]
        verdict = bench.density_at_limit(bench.bin_by_resident(samples, 4), 1.0)
        self.assertIsNone(verdict["max_resident_within_limit"])
        self.assertEqual(verdict["first_violating_bin"]["n_failed"], 1)
        self.assertRaises(ValueError, bench.bin_by_resident,
                          [{"resident": 0, "ok": True, "seconds": 1}], 4)

    def test_rate_summary_window_and_timeline(self):
        offsets = [0.5, 1.2, 1.4, 2.1, 2.2, 2.3, 3.5]
        summary = bench.rate_summary(offsets, start=1.0, end=3.0, bin_seconds=1.0)
        self.assertEqual(summary["completed_in_window"], 5)
        self.assertEqual(summary["per_second"], 2.5)
        self.assertEqual([row["completed"] for row in summary["timeline"]], [1, 2, 3, 1])
        self.assertIsNone(bench.rate_summary([], start=2, end=2, bin_seconds=1)["per_second"])
        self.assertRaises(ValueError, bench.rate_summary, [], start=0, end=1, bin_seconds=0)
        rows = [{"ok": True, "node": "a" if index % 2 else "b", "completion_seconds": 1 + index * 0.5,
                 "create_seconds": 0.4} for index in range(6)]
        section = bench.rate_section(rows, window_seconds=60, warmup_seconds=1,
                                     bin_seconds=1, counted_event="create",
                                     cap_reached_seconds=2.0, max_sandboxes=6, concurrency=2)
        # The cap ends the sustained window at the last completion (3.5 s).
        self.assertEqual(section["cluster"]["window_end_seconds"], 3.5)
        self.assertEqual(section["cluster"]["completed_in_window"], 6)
        self.assertEqual(set(section["per_node"]), {"a", "b"})
        self.assertEqual(section["status"], "measured")

    def test_burst_section_reports_all_ready_and_worst_wait(self):
        rows = sample_rows([1.0, 3.0, 2.0])
        section = bench.burst_section(rows, images=["img"], concurrency=2)
        self.assertTrue(section["all_ready"])
        self.assertEqual(section["all_ready_seconds"], 4.0)
        self.assertEqual(section["worst_single_wait_seconds"], 3.0)
        rows.append({"ok": False, "image": "img", "sandbox_id": "bad", "completion_seconds": 9})
        section = bench.burst_section(rows, images=["img"], concurrency=2)
        self.assertFalse(section["all_ready"])
        self.assertIsNone(section["all_ready_seconds"])
        self.assertEqual(section["last_ready_seconds"], 4.0)
        self.assertEqual(section["per_image"]["img"], {"n": 4, "succeeded": 3})

    def test_record_helpers_origin_redaction_and_sdk_park_detection(self):
        self.assertEqual(bench.node_identity({"node_id": "n1"}), "n1")
        self.assertEqual(bench.node_identity({"sandbox": {"node": {"job_id": 12}}}), "12")
        self.assertEqual(
            bench.node_identity({"sandbox": {"status": {"node": {"node_id": "deep"}}}}), "deep")
        self.assertIsNone(bench.node_identity({"sandbox": {}}))
        self.assertIsNone(bench.node_identity(None))
        self.assertEqual(bench.record_id({"spec": {"id": "x"}}), "x")
        self.assertEqual(bench.record_id({"id": "y"}), "y")
        self.assertEqual(bench.record_state({"status": {"state": "deleted"}}), "deleted")
        self.assertEqual(bench.gateway_origin("https://user:secret@gw.example:8443/api/?t=1#x"),
                         "https://gw.example:8443/api")
        self.assertEqual(bench.gateway_origin("http://127.0.0.1:8090"), "http://127.0.0.1:8090")
        bench._SECRETS.add("top-secret-token")
        try:
            text = bench.safe_error(RuntimeError("bad token top-secret-token at /_relay/abc/x"))
        finally:
            bench._SECRETS.discard("top-secret-token")
        self.assertNotIn("top-secret-token", text)
        self.assertNotIn("/_relay/abc", text)
        self.assertIsNone(bench.sdk_park_wake_methods(SimpleNamespace(health=lambda: {})))
        client = SimpleNamespace(park_sandbox=lambda sid: None, wake_sandbox=lambda sid: None)
        self.assertEqual(bench.sdk_park_wake_methods(client), ("park_sandbox", "wake_sandbox"))
        self.assertIsNone(bench.sdk_park_wake_methods(SimpleNamespace(park_sandbox=lambda s: 1)))

    def test_rollout_selection_sampling_and_image_resolution(self):
        attach = {"task_source_cached": True, "prepared_reference": "reg/p"}
        entries = [{"family": "ScaleSWE", "image": "up/s", "cached_kind": "source", **attach},
                   {"family": "SWE-smith", "image": "up/m", "upstream_rows": 9,
                    "cached_kind": "complete_recipe", **attach},
                   {"family": "TMax", "image": "tmax:t", "cached_kind": "foundation",
                    "remaining_build_work": ["context_transfer"], "prepared_reference": "reg/f"}]
        with tempfile.TemporaryDirectory() as root:
            selectors = Path(root) / bench.SELECTORS_NAME  # an unpacked directory
            selectors.write_text(json.dumps({"images": entries}))
            with zipfile.ZipFile(Path(root) / "s.zip", "w") as archive:
                archive.write(selectors, "sel/" + bench.SELECTORS_NAME)
            loaded, provenance = bench.load_selection(Path(root) / "s.zip")
            self.assertEqual((bench.load_selection(Path(root))[0], provenance["upstream_rows"]),
                             (loaded, 11))
        rows = bench.sample_tasks(loaded, count=11, seed=1, weighting="rows")
        self.assertEqual(sum(e["family"] == "SWE-smith" for e in rows), 9)  # no replacement
        self.assertEqual(rows, bench.sample_tasks(loaded, count=11, seed=1, weighting="rows"))
        self.assertEqual(bench.sample_tasks(loaded, count=1, seed=1, weighting="uniform",
                                            families=["tmax"]), [loaded[2]])
        self.assertRaises(ValueError, bench.sample_tasks, loaded, count=4, seed=1, weighting="uniform")
        self.assertEqual([bench.resolve_image(e, "auto")["resolution"] for e in loaded],
                         ["import_alias", "prepared_reference", "prepared_reference_fallback"])
        self.assertEqual(bench.resolve_image(loaded[0], "prepared")["reference"], "reg/p")

    def test_heartbeat_snapshot_and_environment_io_deltas(self):
        def node(name, epoch, hits, age=1.0, cache=0):
            return {"node_id": name, "node_epoch": epoch, "capabilities": ["sandbox"],
                    "received_at": datetime.fromtimestamp(100 - age, timezone.utc).isoformat(),
                    "runtime_metrics": {"environment_io": dict(hits=hits, misses=1, cached_bytes=cache)}}
        snapshot = lambda *nodes: bench.fleet_snapshot(nodes, now=100, fresh_seconds=120)  # noqa: E731
        before = snapshot(node("a", 1, 5), node("b", 1, 9, age=500), {})
        after = snapshot(node("a", 1, 8, cache=7), node("b", 2, 4), node("c", 1, 2))
        self.assertEqual((before["sandbox_nodes_stored"], before["sandbox_nodes_fresh"]), (2, 1))
        deltas = bench.environment_io_deltas(before, after)
        self.assertEqual(deltas["per_node"]["a"]["deltas"], {"hits": 3, "misses": 0})
        self.assertTrue(deltas["per_node"]["b"]["counted_from_zero"])  # new node_epoch
        self.assertEqual(deltas["totals"], {"hits": 9, "misses": 2})
        self.assertEqual(deltas["per_node"]["a"]["gauges"]["cached_bytes"], {"before": 0, "after": 7})


class ReportSchemaTests(unittest.TestCase):
    def finished(self, scenario="cold", rows=None):
        report = bench.new_report("rlbench-test", scenario, {"images": ["img"]})
        if scenario == "cold":
            report["metrics"]["cold_time_to_first_command"] = bench.first_command_section(
                rows if rows is not None else sample_rows([1.0, 2.0]))
        return bench.finalize_report(report)

    def test_new_report_carries_every_metric(self):
        report = self.finished()
        self.assertEqual(bench.validate_report(report), [])
        self.assertEqual(tuple(report["metrics"]), bench.REPORT_METRIC_KEYS)
        legacy = {key: value for key, value in report["metrics"].items() if key != "rollout"}
        self.assertEqual(bench.validate_report({**report, "metrics": legacy}), [])  # pre-rollout
        self.assertTrue(report["ok"])
        for key in ("bytes_fetched_share", "idle_pss_uss", "page_sharing_ratio"):
            section = report["metrics"][key]
            self.assertEqual(section["status"], "external")
            self.assertIsNone(section["value"])
            self.assertTrue(section["source"])
        self.assertIn("spike_rl_scale.py --probe s2", report["metrics"]["page_sharing_ratio"]["source"])
        self.assertEqual(report["metrics"]["fork"]["status"], "unsupported")
        self.assertEqual(report["metrics"]["warm_time_to_first_command"], {"status": "not_run"})

    def test_failed_or_missing_scenario_metric_fails_the_run(self):
        report = self.finished(rows=[{"ok": False, "image": "img", "sandbox_id": "x"}])
        self.assertEqual(report["metrics"]["cold_time_to_first_command"]["status"], "failed")
        self.assertFalse(report["ok"])
        self.assertEqual(report["status"], "failed")
        report = bench.finalize_report(bench.new_report("rlbench-test", "warm", {}))
        self.assertFalse(report["ok"])
        report = bench.finalize_report(bench.new_report("rlbench-test", "warm", {}),
                                       interrupted=True)
        self.assertEqual(report["status"], "interrupted")

    def test_validator_rejects_schema_violations(self):
        report = self.finished()
        broken = json.loads(json.dumps(report))
        broken["schema"] = "other/v0"
        del broken["metrics"]["fork"]
        broken["metrics"]["page_sharing_ratio"]["value"] = 0.5
        summary = broken["metrics"]["cold_time_to_first_command"]["time_to_first_command"]
        summary["p95"] = summary["max"] + 1
        broken["metrics"]["extra"] = {"status": "measured"}
        problems = "\n".join(bench.validate_report(broken))
        for expected in ("schema must be", "metrics missing: fork", "page_sharing_ratio is node-side",
                         "p50 <= p95 <= p99 <= max", "unknown metrics: extra"):
            self.assertIn(expected, problems)
        self.assertEqual(bench.validate_report([]), ["report must be a JSON object"])
        empty = json.loads(json.dumps(report))
        empty["metrics"]["cold_time_to_first_command"]["create"]["p50"] = 1.0
        empty["metrics"]["cold_time_to_first_command"]["create"]["n"] = 0
        self.assertIn("empty summary", "\n".join(bench.validate_report(empty)))

    def test_merge_combines_distinct_metrics_and_rejects_conflicts(self):
        cold = self.finished()
        warm = bench.new_report("rlbench-warm", "warm", {})
        warm["metrics"]["warm_time_to_first_command"] = bench.first_command_section(
            sample_rows([0.5]))
        bench.finalize_report(warm)
        merged = bench.merge_reports([("cold.json", cold), ("warm.json", warm)])
        self.assertEqual(bench.validate_report(merged), [])
        self.assertTrue(merged["ok"])
        self.assertEqual(merged["scenario"], "merged")
        self.assertEqual(merged["metrics"]["warm_time_to_first_command"]["merged_from"], "warm.json")
        self.assertEqual(len(merged["conditions"]["merged_from"]), 2)
        with self.assertRaisesRegex(ValueError, "several inputs"):
            bench.merge_reports([("a", cold), ("b", cold)])
        with self.assertRaisesRegex(ValueError, "not a valid"):
            bench.merge_reports([("bad", {"schema": "x"})])
        failed = self.finished(rows=[{"ok": False, "image": "img", "sandbox_id": "x"}])
        self.assertFalse(bench.merge_reports([("failed.json", failed)])["ok"])  # propagated


class ArgumentTests(unittest.TestCase):
    def parse(self, *argv):
        return bench.parse_args(list(argv))

    def parse_error(self, *argv):
        with redirect_stderr(io.StringIO()) as stderr, self.assertRaises(SystemExit) as raised:
            bench.parse_args(list(argv))
        self.assertEqual(raised.exception.code, 2)
        return stderr.getvalue()

    def test_live_scenarios_parse_with_defaults(self):
        args = self.parse("cold", "--output", "r.json", "--image", "a", "--image", "b")
        self.assertEqual(args.images, ["a", "b"])
        self.assertEqual(args.first_command_argv, ["true"])
        self.assertEqual(args.sandbox_command_argv, ["sleep", "1800"])
        args = self.parse("warm", "--output", "r.json", "--image", "a", "--repeats", "3",
                          "--sandbox-command", "", "--label", "k=v=w")
        self.assertEqual((args.images, args.repeats, args.sandbox_command_argv), (["a"], 3, []))
        self.assertEqual(args.labels, {"k": "v=w"})
        args = self.parse("density", "--output", "r.json", "--image", "a",
                          "--tool-command", "python3 -c 'print(1)'")
        self.assertEqual(args.tool_command_argv, ["python3", "-c", "print(1)"])
        args = self.parse("burst", "--output", "r.json", "--image", "a", "--sandboxes", "9")
        self.assertEqual((args.sandboxes, args.concurrency), (9, 32))
        self.assertEqual(self.parse("park", "--output", "r.json", "--image", "a").cycles, 5)
        with tempfile.TemporaryDirectory() as root:
            (listing := Path(root) / "images.txt").write_text("# cold set\nx\n\ny\n")
            args = self.parse("rate", "--output", "r.json", "--images-file", str(listing))
            self.assertEqual(args.images, ["x", "y"])

    def test_invalid_arguments_are_refused(self):
        self.assertIn("distinct", self.parse_error("cold", "--output", "r", "--image", "a",
                                                   "--image", "a"))
        self.assertIn("at least one", self.parse_error("burst", "--output", "r"))
        self.assertIn("--step", self.parse_error("density", "--output", "r", "--image", "a",
                                                 "--step", "8", "--max-resident", "4"))
        self.assertIn("warmup", self.parse_error("rate", "--output", "r", "--image", "a",
                                                 "--window-seconds", "5", "--warmup-seconds", "5"))
        self.assertIn("KEY=VALUE", self.parse_error("warm", "--output", "r", "--image", "a",
                                                    "--label", "novalue"))
        self.assertIn("empty", self.parse_error("warm", "--output", "r", "--image", "a",
                                                "--first-command", " "))
        self.assertIn("run-id", self.parse_error("warm", "--output", "r", "--image", "a",
                                                 "--run-id", "bad id"))
        self.parse_error("warm", "--output", "r", "--image", "a", "--repeats", "0")
        self.parse_error("warm", "--image", "a")
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(  # think-mode credentials
                os.environ, {"UCLOUD_RELAY_WORKER_TOKEN": ""}):
            (images := Path(root) / "i.txt").write_text("a\n")
            argv = ("rollout", "--output", "r", "--selection", str(images), "--fleet-state", "zero",
                    "--tasks", "1", "--think-mode")
            self.assertIn("relay-worker-token", self.parse_error(*argv, "relay"))
            self.assertIn("container", self.parse_error(*argv, "relay", "--profile", "linux_host",
                                                        "--relay-worker-token-file", "t"))
            self.assertIn("--parkable", self.parse_error(*argv, "park", "--operator-token-file", "t"))
            args = self.parse(*argv, "relay", "--relay-worker-token-file", "t")
            self.assertEqual((args.profile, args.parkable), ("container", True))

    def test_help_does_not_need_the_sdk(self):
        for argv in ([], ["cold"], ["density"], ["merge"]):
            completed = subprocess.run([sys.executable, str(SCRIPT), *argv, "--help"],
                                       capture_output=True, text=True, timeout=60)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("usage:", completed.stdout)
        probe = ("import sys; import scripts.bench_rl_scale; "
                 "assert 'ucloud_sandboxes_sdk' not in sys.modules")
        completed = subprocess.run([sys.executable, "-c", probe], cwd=SCRIPT.parents[1],
                                   capture_output=True, text=True, timeout=60)
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_validate_command(self):
        report = bench.finalize_report(bench.new_report("rlbench-x", "park", {}))
        with tempfile.TemporaryDirectory() as root:
            good = Path(root) / "good.json"
            good.write_text(json.dumps(report))
            bad = Path(root) / "bad.json"
            bad.write_text("{}")
            with redirect_stdout(io.StringIO()):
                self.assertEqual(bench.main(["validate", str(good)]), 0)
                self.assertEqual(bench.main(["validate", str(good), str(bad)]), 1)


class FakeApiError(RuntimeError):
    def __init__(self, message, *, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class FakeSdk:
    __name__ = "fake_sdk"
    __version__ = "0.0-test"
    SandboxApiError = FakeApiError

    Image = SimpleNamespace(from_registry=lambda ref: SimpleNamespace(reference=ref, kind="registry"),
                            from_name=lambda ref: SimpleNamespace(reference=ref, kind="name"))
    SandboxSpec = SandboxSecuritySpec = staticmethod(lambda **fields: SimpleNamespace(**fields))
    http_tunnel_url = staticmethod(lambda base, rid, path, registration_token:
                                   f"{base}/tunnels/{rid}/_relay/{registration_token}/{path}")


class FakeClient:
    def __init__(self, *, fail_images=(), preexisting=(), flaky_delete=0, root=None):
        self.lock = threading.Lock()
        self.live = {sid: {"spec": {"id": sid}} for sid in preexisting}
        self.created, self.execs, self.deleted, self.agents, self.state = [], [], [], {}, {}
        self.fail_images = set(fail_images)
        self.flaky_delete = flaky_delete
        self.root = root

    def start_agent(self, sid, command, env, script):
        """Run the uploaded agent program as this sandbox's managed job."""
        (path := Path(self.root) / f"{sid}.py").write_text(script)
        log = (Path(self.root) / f"{sid}.log").open("w+b")
        process = subprocess.Popen([sys.executable, str(path)], env={**os.environ, **env},
                                   stdout=log, cwd=self.root)
        self.agents[sid], process.log = process, log

        def refresh():
            code = process.poll()
            job.record = SimpleNamespace(state="running" if code is None else "exited", signal=0,
                                         exit_code=code, terminal=code is not None, success=code == 0)
            return job.record
        job = SimpleNamespace(job_id="agent-" + sid, refresh=refresh, logs=lambda stream, limit:
                              (log.seek(0), SimpleNamespace(data=log.read()))[1])
        return refresh() and job

    def health(self):
        return {"ok": True, "service": "control-plane", "version": "9.9.9"}

    def list_sandboxes(self):
        with self.lock:
            return [dict(record) for record in self.live.values()]

    def create_sandbox(self, spec, *, request_timeout_seconds=None):
        with self.lock:
            self.created.append(spec)
            if spec.image.reference in self.fail_images:
                # Server-side success with a client-visible failure must be cleaned up.
                self.live[spec.id] = {"spec": {"id": spec.id}}
                raise FakeApiError("create timed out", status_code=504)
            self.live[spec.id] = {"spec": {"id": spec.id}}
        files = {}
        return SimpleNamespace(id=spec.id, record={"spec": {"id": spec.id}}, create_response={
            "sandbox": {"spec": {"id": spec.id}}, "node_id": "node-a",
            "timings": {"total_ms": 1.0}}, upload_file=files.__setitem__,
            start_agent=lambda command, env: self.start_agent(spec.id, command, env,
                                                               files[bench.AGENT_PATH]))

    def start_exec(self, sandbox_id, command):
        with self.lock:
            if sandbox_id not in self.live:
                raise FakeApiError("missing", status_code=404)
            self.execs.append((sandbox_id, tuple(command)))
        result = SimpleNamespace(success=True, status="exited", exit_code=0, stderr="")
        return SimpleNamespace(wait=lambda timeout_seconds=None: result)

    def delete_sandbox(self, sandbox_id):
        with self.lock:
            if self.flaky_delete:
                self.flaky_delete -= 1
                raise FakeApiError("busy", status_code=503)
            if self.live.pop(sandbox_id, None) is None:
                raise FakeApiError("not found", status_code=404)
            self.deleted.append(sandbox_id)
            if (agent := self.agents.pop(sandbox_id, None)) is not None:
                agent.kill()
                agent.wait()
                agent.log.close()
        return {}


class FakeRelay:
    """The relay worker API in process; its tunnel over localhost HTTP holds each
    agent call until the benchmark commits a response."""

    def __init__(self, on_commit=lambda: None):
        self.queues, self.unregistered, self.on_commit = {}, [], on_commit
        relay = self

        class Tunnel(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                request = SimpleNamespace(request_id=self.headers["X-UCloud-Relay-Request-Id"],
                                          delivery_count=1, created_at=time.time(), body=body,
                                          accepted_notified_at=None, reply=b"", done=threading.Event())
                relay.queues[self.path.split("/")[2]].put(request)
                request.done.wait(30)
                self.send_response(200 if request.reply else 410)
                self.send_header("Content-Length", str(len(request.reply)))
                self.end_headers()
                self.wfile.write(request.reply)

            def log_message(self, *_):
                pass
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Tunnel)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def register_agent_rollout(self, rid, handle, metadata):
        self.queues[rid] = queue.Queue()
        return {"rollout": {"rollout_id": rid, "registration_token": "secret-registration"}}

    def poll(self, rid, **_):
        try:
            return SimpleNamespace(requests=[self.queues[rid].get(timeout=0.05)])
        except queue.Empty:
            return SimpleNamespace(requests=[])

    def renew_request(self, request, **_):
        request.accepted_notified_at = time.time()
        return request

    def commit_response_bytes_to(self, request, body, **_):
        self.on_commit()
        request.reply = body
        request.done.set()
        return {"ok": True, "committed": True, "delivery_status": "released"}

    def unregister_rollout(self, rid, registration_token):
        self.unregistered.append((rid, registration_token))


class FakeOperator:
    """Heartbeats, status inventory and park/wake; the first park is busy."""

    def __init__(self, client):
        self.client, self.calls = client, []

    def nodes(self):
        return [{"node_id": "n", "node_epoch": 1, "capabilities": ["sandbox"],
                 "received_at": bench.utc_now(), "used_resources": {"memory_mb": 512},
                 "runtime_metrics": {"memory_available_mb": 900, "resident_wait": {
                     "pauses": len(self.calls), "paused_sandboxes": 0, "reason": "x"}}}]

    def statuses(self, sandbox_id=None):
        with self.client.lock:
            return [{"id": sid, "spec": {"id": sid}, "generation": 1, "node": {"node_id": "n"},
                     "state": self.client.state.get(sid, "running")}
                    for sid in self.client.live if sandbox_id in (None, sid)]

    def park(self, sid, operation_id):
        self.calls.append(operation_id)
        if len(self.calls) == 1:
            raise bench.OperatorError("busy", status_code=503)
        self.client.state[sid] = "parked"

    def wake(self, sid, operation_id, generation):
        self.calls.append((operation_id, generation))
        self.client.state[sid] = "running"


class LiveRunTests(unittest.TestCase):
    def run_scenario(self, client, *argv, operator=None, relay=None):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "report.json"
            args = bench.parse_args([*argv, "--output", str(output),
                                     "--gateway-url", "http://gw.test:8090",
                                     "--cleanup-timeout-seconds", "5"])
            with redirect_stdout(io.StringIO()):
                report = bench.run_live(args, sdk=FakeSdk, client=client, operator=operator,
                                        relay=relay)
            persisted = json.loads(output.read_text())
        self.assertEqual(persisted["run_id"], report["run_id"])
        self.assertEqual(bench.validate_report(persisted), [])
        return persisted

    def test_cold_run_measures_and_deletes_every_sandbox_through_busy_deletes(self):
        client = FakeClient(flaky_delete=2)  # transient delete failures are retried
        report = self.run_scenario(client, "cold", "--image", "a", "--image", "b")
        self.assertTrue(report["ok"], report["errors"])
        section = report["metrics"]["cold_time_to_first_command"]
        self.assertEqual(section["n_succeeded"], 2)
        self.assertEqual(section["time_to_first_command"]["n"], 2)
        self.assertEqual(client.live, {})
        self.assertEqual(report["conditions"]["gateway_version"], "9.9.9")
        self.assertEqual(report["conditions"]["sdk"]["version"], "0.0-test")
        self.assertEqual(report["cleanup"]["remaining_owned_ids"], [])
        self.assertTrue(all(command == ("true",) for _, command in client.execs))

    def test_burst_density_rate_and_warm_runs_are_valid_and_clean(self):
        client = FakeClient()
        report = self.run_scenario(client, "burst", "--image", "a", "--image", "b",
                                   "--sandboxes", "5", "--concurrency", "3")
        burst = report["metrics"]["burst_completion"]
        self.assertTrue(burst["all_ready"])
        self.assertEqual(burst["per_image"], {"a": {"n": 3, "succeeded": 3},
                                              "b": {"n": 2, "succeeded": 2}})
        self.assertEqual(client.live, {})

        report = self.run_scenario(client, "density", "--image", "a", "--step", "2",
                                   "--max-resident", "4", "--probes-per-bin", "4",
                                   "--create-concurrency", "2", "--probe-concurrency", "2")
        density = report["metrics"]["density_at_latency"]
        self.assertTrue(report["ok"], report["errors"])
        self.assertEqual([row["resident_max_observed"] for row in density["bins"]], [2, 4])
        self.assertEqual(density["max_resident_within_limit"], 4)
        self.assertEqual(density["stop_reason"], "max resident reached")
        self.assertEqual(client.live, {})

        report = self.run_scenario(client, "rate", "--image", "a", "--window-seconds", "0.3",
                                   "--warmup-seconds", "0", "--max-sandboxes", "12",
                                   "--concurrency", "3")
        rate = report["metrics"]["creation_rate"]
        self.assertEqual(rate["n_attempted"], 12)
        self.assertIsNotNone(rate["cap_reached_seconds"])
        self.assertEqual(rate["status"], "measured")
        self.assertEqual(client.live, {})

        report = self.run_scenario(client, "warm", "--image", "a", "--repeats", "3")
        warm = report["metrics"]["warm_time_to_first_command"]
        self.assertEqual(warm["n_succeeded"], 3)
        self.assertTrue(warm["priming"]["ok"])
        self.assertEqual(client.live, {})

    def test_failed_create_is_recorded_and_still_deleted(self):
        client = FakeClient(fail_images={"bad"})
        report = self.run_scenario(client, "burst", "--image", "good", "--image", "bad",
                                   "--sandboxes", "4")
        self.assertFalse(report["ok"])
        burst = report["metrics"]["burst_completion"]
        self.assertEqual(burst["n_failed"], 2)
        self.assertFalse(burst["all_ready"])
        self.assertEqual(client.live, {})
        self.assertEqual(report["cleanup_errors"], [])

    def test_occupied_fleet_is_refused_before_any_create(self):
        client = FakeClient(preexisting={"someone-else"})
        report = self.run_scenario(client, "cold", "--image", "a")
        self.assertFalse(report["ok"])
        self.assertIn("idle fleet", report["errors"][0])
        self.assertEqual(client.created, [])
        self.assertIn("someone-else", client.live)
        self.assertEqual(report["conditions"]["preexisting_sandboxes"], 1)

    def test_park_is_unsupported_without_sdk_methods(self):
        client = FakeClient()
        report = self.run_scenario(client, "park", "--image", "a")
        section = report["metrics"]["pause_resume"]
        self.assertEqual(section["status"], "unsupported")
        self.assertIn("park/wake", section["reason"])
        self.assertIsNone(section["bytes_written"])
        self.assertEqual(client.created, [])
        self.assertTrue(report["ok"], report["errors"])

    def test_rollout_issues_all_creates_at_once_and_reports_per_family(self):
        client, occupied = FakeClient(fail_images={"bad:1"}), FakeClient()
        create, barrier = client.create_sandbox, threading.Barrier(4, timeout=10)  # all 4 at once
        client.create_sandbox = lambda spec, **kw: (barrier.wait(), create(spec, **kw))[1]
        fresh = {"node_id": "n", "capabilities": ["sandbox"], "received_at": bench.utc_now()}
        with tempfile.TemporaryDirectory() as root:
            (images := Path(root) / "images.txt").write_text("TMax\tg1\nSWE-smith\tg2\nTMax\tbad:1\nx\n")
            argv = ("rollout", "--selection", str(images), "--fleet-state", "zero",
                    "--sampling", "uniform", "--think-seconds", "0:0", "--turns", "3")
            report = self.run_scenario(client, *argv, "--tasks", "4", "--heartbeat-settle-seconds",
                                       "0", operator=SimpleNamespace(nodes=lambda: []))
            refused = self.run_scenario(occupied, *argv, "--tasks", "1",
                                        operator=SimpleNamespace(nodes=lambda: [fresh]))
        section = report["metrics"]["rollout"]
        self.assertEqual((section["n_succeeded"], section["arrival"]["max_concurrent_creates"]), (3, 4))
        self.assertEqual(section["failures_by_error_code"]["http_504"]["by_phase"], {"create": 1})
        self.assertEqual(set(section["per_family"]), {"TMax", "SWE-smith", "unknown"})
        self.assertEqual(section["turns"]["by_kind"]["grep"]["all"]["n"], 3)
        self.assertEqual((section["node_io"]["status"], report["conditions"]["rollout"]
                          ["fleet_state"]["verified"]), ("measured", True))
        self.assertEqual(({spec.profile for spec in client.created}, client.live), ({"linux_host"}, {}))
        self.assertIn("--fleet-state zero", refused["errors"][0])
        self.assertEqual(occupied.created, [])

    def rollout(self, client, *argv, **kw):
        (images := Path(client.root) / "images.txt").write_text("SWE-smith\tg1\nTMax\tg2\n")
        return self.run_scenario(client, "rollout", "--selection", str(images), "--turns", "2",
                                 "--fleet-state", "warm-empty", "--sampling", "uniform", "--tasks",
                                 "2", "--turn-mix", "grep", "--heartbeat-settle-seconds", "0",
                                 *argv, **kw)

    @mock.patch.dict(os.environ, {"UCLOUD_RELAY_WORKER_TOKEN": "worker"})
    @mock.patch.object(bench, "AGENT_CHECK_SECONDS", 0.05)
    def test_rollout_relay_mode_answers_agent_calls_and_releases_rollouts(self):
        with tempfile.TemporaryDirectory() as root:
            client, relay = FakeClient(root=root), FakeRelay()
            argv = ("--think-mode", "relay", "--sandbox-relay-url", relay.url, "--think-seconds")
            report = self.rollout(client, *argv, "0.05:0.05", "--turn-command",
                                  "grep=echo ucloud-bench grep_lines=7", relay=relay)
            failed = self.rollout(FakeClient(root=root), *argv, "0:0", "--turn-command",
                                  "grep=exit 5", relay=relay)
        section, think = report["metrics"]["rollout"], report["metrics"]["rollout"]["think"]
        self.assertTrue(report["ok"], report["errors"])
        self.assertEqual((think["n_ok"], think["model_wait_accepted"],
                          think["relay_call_overhead"]["n"]), (4, 4, 4))
        self.assertEqual(section["samples"][0]["turns"][1]["markers"]["grep_lines"], "7")
        self.assertTrue(all(spec.managed_process and spec.parkable for spec in client.created))
        self.assertLessEqual({(spec.id, "secret-registration") for spec in client.created},
                             set(relay.unregistered))
        self.assertNotIn("secret-registration", json.dumps(report))
        self.assertEqual(failed["metrics"]["rollout"]["failures_by_phase"],
                         {"turn": {"turn_grep_exit_5": 2}})
        self.assertEqual((client.live, client.agents), ({}, {}))

    @mock.patch.dict(os.environ, {"UCLOUD_RELAY_WORKER_TOKEN": "worker"})
    def test_rollout_relay_interrupt_releases_rollouts_and_sandboxes(self):
        if threading.current_thread() is not threading.main_thread():
            self.skipTest("run_live installs its SIGTERM handler only on the main thread")
        fired = []

        def interrupt():
            if not fired and signal.getsignal(signal.SIGTERM) is bench._raise_interrupt:
                fired.append(os.kill(os.getpid(), signal.SIGTERM))
        with tempfile.TemporaryDirectory() as root:
            client, relay = FakeClient(root=root), FakeRelay(interrupt)
            report = self.rollout(client, "--think-mode", "relay", "--sandbox-relay-url", relay.url,
                                  "--think-seconds", "0:0", relay=relay)
        self.assertEqual(report["status"], "interrupted")
        self.assertEqual({rid for rid, _ in relay.unregistered}, {spec.id for spec in client.created})
        self.assertEqual((client.live, client.agents,
                          report["cleanup"]["relay_registrations"]["remaining"]), ({}, {}, []))

    def test_rollout_park_mode_parks_wakes_and_reports_density(self):
        with tempfile.TemporaryDirectory() as root:
            client = FakeClient(root=root)
            (token := Path(root) / "operator").write_text("x")
            report = self.rollout(client, "--think-mode", "park", "--parkable",
                                  "--operator-token-file", str(token), "--think-seconds", "0.2:0.2",
                                  "--status-poll-seconds", "0.01", "--node-poll-seconds", "0.02",
                                  operator=(operator := FakeOperator(client)))
        section = report["metrics"]["rollout"]
        think, density = section["think"], section["density"]
        self.assertTrue(report["ok"], report["errors"])
        self.assertEqual((think["park"]["n"], think["wake"]["n"], think["retry_codes"],
                          think["n_parked_observed"]), (4, 4, {"http_503": 1}, 4))
        self.assertEqual([call[1] for call in operator.calls if isinstance(call, tuple)], [1] * 4)
        self.assertEqual((density["peak_live_sandboxes"]["driver"], density["peak_per_node"]
                          ["node_id"], density["at_peak"]["paused"]), (2, "n", 0))
        self.assertEqual(density["timeline"][0]["nodes"][0]["memory_mb"]["committed"], 512)
        self.assertGreater(sum(sample["resident_wait_deltas"].get("pauses", 0)
                               for sample in density["timeline"]), 0)

    def test_existing_output_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "report.json"
            output.write_text("previous")
            args = bench.parse_args(["cold", "--image", "a", "--output", str(output),
                                     "--gateway-url", "http://gw.test"])
            with self.assertRaises(FileExistsError):
                bench.run_live(args, sdk=FakeSdk, client=FakeClient())
            self.assertEqual(output.read_text(), "previous")


if __name__ == "__main__":
    unittest.main()
