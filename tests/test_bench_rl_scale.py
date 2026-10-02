from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest

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
        self.assertEqual(
            {key: summary[key] for key in ("n", "p50", "p95", "p99", "max", "min")},
            {"n": 100, "p50": 50, "p95": 95, "p99": 99, "max": 100, "min": 1},
        )
        self.assertEqual(bench.latency_summary([3.0])["p99"], 3.0)
        self.assertEqual(bench.latency_summary([2, 1])["p50"], 1)
        with self.assertRaises(ValueError):
            bench.percentile([], 0.5)
        with self.assertRaises(ValueError):
            bench.percentile([1], 0)

    def test_empty_summary_has_null_percentiles(self):
        summary = bench.latency_summary([])
        self.assertEqual(summary["n"], 0)
        self.assertTrue(all(summary[key] is None for key in ("p50", "p95", "p99", "max")))

    def test_round_robin_assignment_and_duplicate_detection(self):
        self.assertEqual(bench.assign_images(5, ["a", "b"]), ["a", "b", "a", "b", "a"])
        with self.assertRaises(ValueError):
            bench.assign_images(1, [])
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

    def test_failed_tool_calls_violate_their_bin(self):
        samples = [{"resident": 4, "ok": True, "seconds": 0.1},
                   {"resident": 4, "ok": False, "seconds": 0.1}]
        verdict = bench.density_at_limit(bench.bin_by_resident(samples, 4), 1.0)
        self.assertIsNone(verdict["max_resident_within_limit"])
        self.assertEqual(verdict["first_violating_bin"]["n_failed"], 1)
        with self.assertRaises(ValueError):
            bench.bin_by_resident([{"resident": 0, "ok": True, "seconds": 1}], 4)

    def test_rate_summary_window_and_timeline(self):
        offsets = [0.5, 1.2, 1.4, 2.1, 2.2, 2.3, 3.5]
        summary = bench.rate_summary(offsets, start=1.0, end=3.0, bin_seconds=1.0)
        self.assertEqual(summary["completed_in_window"], 5)
        self.assertEqual(summary["per_second"], 2.5)
        self.assertEqual([row["completed"] for row in summary["timeline"]], [1, 2, 3, 1])
        self.assertIsNone(bench.rate_summary([], start=2, end=2, bin_seconds=1)["per_second"])
        with self.assertRaises(ValueError):
            bench.rate_summary([], start=0, end=1, bin_seconds=0)

    def test_rate_section_groups_nodes_and_ends_at_cap(self):
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

    def test_node_identity_and_record_helpers(self):
        self.assertEqual(bench.node_identity({"node_id": "n1"}), "n1")
        self.assertEqual(bench.node_identity({"sandbox": {"node": {"job_id": 12}}}), "12")
        self.assertEqual(
            bench.node_identity({"sandbox": {"status": {"node": {"node_id": "deep"}}}}), "deep")
        self.assertIsNone(bench.node_identity({"sandbox": {}}))
        self.assertIsNone(bench.node_identity(None))
        self.assertEqual(bench.record_id({"spec": {"id": "x"}}), "x")
        self.assertEqual(bench.record_id({"id": "y"}), "y")
        self.assertEqual(bench.record_state({"status": {"state": "deleted"}}), "deleted")

    def test_gateway_origin_drops_credentials_and_query(self):
        self.assertEqual(bench.gateway_origin("https://user:secret@gw.example:8443/api/?t=1#x"),
                         "https://gw.example:8443/api")
        self.assertEqual(bench.gateway_origin("http://127.0.0.1:8090"), "http://127.0.0.1:8090")

    def test_safe_error_redacts_registered_secrets(self):
        bench._SECRETS.add("top-secret-token")
        try:
            text = bench.safe_error(RuntimeError("bad token top-secret-token at /_relay/abc/x"))
        finally:
            bench._SECRETS.discard("top-secret-token")
        self.assertNotIn("top-secret-token", text)
        self.assertNotIn("/_relay/abc", text)

    def test_sdk_park_wake_detection(self):
        self.assertIsNone(bench.sdk_park_wake_methods(SimpleNamespace(health=lambda: {})))
        client = SimpleNamespace(park_sandbox=lambda sid: None, wake_sandbox=lambda sid: None)
        self.assertEqual(bench.sdk_park_wake_methods(client), ("park_sandbox", "wake_sandbox"))
        self.assertIsNone(bench.sdk_park_wake_methods(SimpleNamespace(park_sandbox=lambda s: 1)))


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
        self.assertEqual(tuple(report["metrics"]), bench.METRIC_KEYS)
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

    def test_merge_propagates_failed_inputs(self):
        failed = self.finished(rows=[{"ok": False, "image": "img", "sandbox_id": "x"}])
        merged = bench.merge_reports([("failed.json", failed)])
        self.assertFalse(merged["ok"])


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
        args = self.parse("park", "--output", "r.json", "--image", "a")
        self.assertEqual(args.cycles, 5)

    def test_images_file_and_environment_defaults(self):
        with tempfile.TemporaryDirectory() as root:
            listing = Path(root) / "images.txt"
            listing.write_text("# cold set\nx\n\ny\n")
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

    class Image:
        @staticmethod
        def from_registry(reference):
            return SimpleNamespace(reference=reference, kind="registry")

        @staticmethod
        def from_name(reference):
            return SimpleNamespace(reference=reference, kind="name")

    @staticmethod
    def SandboxSpec(**fields):
        return SimpleNamespace(**fields)


class FakeClient:
    def __init__(self, *, fail_images=(), preexisting=(), flaky_delete=0):
        self.lock = threading.Lock()
        self.live = {sid: {"spec": {"id": sid}} for sid in preexisting}
        self.created = []
        self.execs = []
        self.deleted = []
        self.fail_images = set(fail_images)
        self.flaky_delete = flaky_delete

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
        return SimpleNamespace(id=spec.id, create_response={
            "sandbox": {"spec": {"id": spec.id}}, "node_id": "node-a",
            "timings": {"total_ms": 1.0}})

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
        return {}


class LiveRunTests(unittest.TestCase):
    def run_scenario(self, client, *argv):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "report.json"
            args = bench.parse_args([*argv, "--output", str(output),
                                     "--gateway-url", "http://gw.test:8090",
                                     "--cleanup-timeout-seconds", "5"])
            with redirect_stdout(io.StringIO()):
                report = bench.run_live(args, sdk=FakeSdk, client=client)
            persisted = json.loads(output.read_text())
        self.assertEqual(persisted["run_id"], report["run_id"])
        self.assertEqual(bench.validate_report(persisted), [])
        return persisted

    def test_cold_run_measures_and_deletes_every_sandbox(self):
        client = FakeClient()
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

    def test_transient_delete_failures_are_retried(self):
        client = FakeClient(flaky_delete=2)
        report = self.run_scenario(client, "cold", "--image", "a")
        self.assertTrue(report["ok"], report["errors"])
        self.assertEqual(client.live, {})

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
