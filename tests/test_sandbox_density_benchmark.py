from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from scripts import benchmark_sandbox_density as benchmark
from ucloud_sandboxes.sandbox import SandboxOperation, SandboxSpec
from ucloud_sandboxes.sandbox_exec import SandboxExecSpec


class FakeNode:
    def __init__(self, *, fail_create=False, corrupt_wake=False, fail_delete=False):
        self.records = {}
        self.states = {}
        self.fail_create = fail_create
        self.corrupt_wake = corrupt_wake
        self.fail_delete = fail_delete
        self.created = 0
        self.deleted = []

    def call(self, path, *, method="GET", payload=None, headers=None, timeout=None):
        if path == "/v1/heartbeat":
            return {
                "heartbeat": {
                    "runtime_metrics": {
                        "cpu_count": 32,
                        "storage_ublk_active_devices": len(self.records),
                        "storage_ublk_idle_devices": 16,
                        "storage_error_volumes": 0,
                        "storage_hard_reserved_mb": len(self.records) * 5184,
                    },
                    "capabilities": [
                        "direct-runsc-v1",
                        "hibernate-local-v2",
                        "storage-native-v1",
                    ],
                    "active_sandboxes": len(self.records),
                    "active_sandbox_creates": 0,
                    "inventory_complete": True,
                    "deployment_id": "test-deployment",
                    "init_version": "test-init",
                }
            }
        if path == "/v1/images/pull":
            return {"image": {"id": "sha256:" + "a" * 64}}
        if path == "/v1/sandboxes":
            if method == "GET":
                return {"sandboxes": [dict(record) for record in self.records.values()]}
            raw = dict(payload)
            operation = SandboxOperation.from_dict(raw.pop("_ucloud_operation", None))
            spec = SandboxSpec.from_dict(raw)
            operation.validate_spec(spec)
            self.created += 1
            record = {
                "id": spec.id,
                "state": "running",
                "generation": operation.generation,
            }
            self.records[spec.id] = record
            self.states[spec.id] = {
                "nonce": spec.id,
                "pid": 10,
                "resident_bytes": int(spec.command[-2]) * 1024 * 1024,
                "dirty_mb": int(spec.command[-1]),
                "dirty_page_count": int(spec.command[-1]) * 256,
                "counter": 0,
            }
            if self.fail_create and self.created == 2:
                raise TimeoutError("create response lost after provisioning")
            return {"sandbox": dict(record)}
        sandbox_id = path.split("/")[3]
        if method == "DELETE":
            if self.fail_delete:
                raise RuntimeError("delete failed")
            if headers["X-UCloud-Sandbox-Generation"] != str(
                self.records[sandbox_id]["generation"]
            ):
                raise AssertionError("incorrect delete generation fence")
            self.deleted.append(sandbox_id)
            del self.records[sandbox_id]
            return {}
        record = self.records[sandbox_id]
        if path.endswith("/park"):
            if set(payload) != {"operation_id"}:
                raise AssertionError("incorrect direct node park contract")
            record["state"] = "parked"
        elif path.endswith("/wake"):
            if payload["generation"] != record["generation"]:
                raise AssertionError("incorrect wake generation")
            record["state"] = "running"
            if self.corrupt_wake:
                self.states[sandbox_id]["nonce"] = "restarted"
        else:
            raise AssertionError(path)
        return {"sandbox": dict(record)}

    def probe(self, sandbox_id, *, act=False, cpu_ms=100):
        state = self.states[sandbox_id]
        if act:
            state["counter"] += 1
        return dict(state)


def args(*extra):
    return benchmark.parse_args(
        [
            "--node-url",
            "http://127.0.0.1:8090",
            "--node-token-file",
            "/tmp/unused-token",
            "--output",
            "/tmp/unused-result",
            "--count",
            "4",
            "--cycles",
            "2",
            "--cleanup-timeout-seconds",
            "0.01",
            *extra,
        ]
    )


class SandboxDensityBenchmarkTests(unittest.TestCase):
    def test_probe_sends_the_current_node_exec_schema(self):
        api = benchmark.NodeApi("http://127.0.0.1:8090", "test-token")
        observed = []

        session = {"id": "test-exec", "status": "exited", "exit_code": 0}

        def call(path, *, method="GET", payload=None, timeout=None):
            if method == "POST":
                spec = SandboxExecSpec.from_dict(payload, sandbox_id="test-sandbox")
                observed.append(spec)
                return {"session": session}
            self.assertEqual(urlsplit(path).path, "/v1/exec/test-exec/events")
            return {
                "session": session,
                "events": [
                    {"sequence": 1, "stream": "stdout", "data": '{"counter": 1}\n'},
                    {"sequence": 2, "stream": "exit", "exit_code": 0},
                ],
            }

        with patch.object(api, "call", side_effect=call):
            self.assertEqual(api.probe("test-sandbox", act=True), {"counter": 1})
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0].command[:2], ("python3", "-c"))

    def test_probe_long_polls_and_drains_output_after_terminal_status_race(self):
        api = benchmark.NodeApi("http://127.0.0.1:8090", "test-token", timeout=90)
        running = {"id": "test-exec", "status": "running", "exit_code": None}
        terminal = {**running, "status": "exited", "exit_code": 0}
        start_timings = {"start_ms": 12.5, "manager": {"exec_lease": 2.5}}
        batches = [
            {
                "session": running,
                "events": [
                    {"sequence": 1, "stream": "status", "data": "started"},
                    {"sequence": 2, "stream": "stdout", "data": '{"counter":'},
                ],
            },
            {"session": running, "events": []},
            # The session became terminal after this page was snapshotted.
            {
                "session": terminal,
                "events": [
                    {"sequence": 3, "stream": "stdout", "data": " 1}\n"},
                ],
            },
            {
                "session": terminal,
                "events": [
                    {"sequence": 4, "stream": "exit", "exit_code": 0},
                ],
            },
        ]
        cursors, waits = [], []

        def call(path, *, method="GET", payload=None, timeout=None):
            if method == "POST":
                return {"session": running, "timings": start_timings}
            parsed = urlsplit(path)
            self.assertEqual(parsed.path, "/v1/exec/test-exec/events")
            query = parse_qs(parsed.query)
            cursors.append(int(query["after"][0]))
            waits.append(float(query["wait_seconds"][0]))
            self.assertLessEqual(waits[-1], 30)
            self.assertLess(waits[-1], timeout)
            return batches.pop(0)

        with (
            patch.object(api, "call", side_effect=call),
            patch.object(
                benchmark.time, "sleep", side_effect=AssertionError("polling sleep")
            ),
        ):
            state = api.probe("test-sandbox", act=True)
        self.assertEqual(
            state, {"counter": 1, "node_exec_start_timings": start_timings}
        )
        self.assertEqual(cursors, [0, 2, 2, 3])
        self.assertTrue(all(wait > 0 for wait in waits[:3]))
        self.assertEqual(waits[3], 0)
        self.assertFalse(batches)

    def test_probe_preserves_process_and_workload_failures(self):
        for exit_code, stdout, stderr, expected in (
            (1, "", "probe failed", "workload exec failed: probe failed"),
            (
                0,
                '{"error": "memory corrupt"}',
                "",
                "workload validation failed: memory corrupt",
            ),
        ):
            with self.subTest(exit_code=exit_code):
                api = benchmark.NodeApi("http://127.0.0.1:8090", "test-token")
                session = {
                    "id": "test-exec",
                    "status": "failed" if exit_code else "exited",
                    "exit_code": exit_code,
                }
                with patch.object(
                    api,
                    "call",
                    side_effect=[
                        {"session": session},
                        {
                            "session": session,
                            "events": [
                                {"sequence": 1, "stream": "stdout", "data": stdout},
                                {"sequence": 2, "stream": "stderr", "data": stderr},
                                {
                                    "sequence": 3,
                                    "stream": "exit",
                                    "exit_code": exit_code,
                                },
                            ],
                        },
                    ],
                ):
                    with self.assertRaisesRegex(RuntimeError, expected):
                        api.probe("test-sandbox")

    def test_probe_drains_when_empty_long_poll_races_with_completion(self):
        api = benchmark.NodeApi("http://127.0.0.1:8090", "test-token")
        running = {"id": "test-exec", "status": "running", "exit_code": None}
        terminal = {**running, "status": "exited", "exit_code": 0}
        with patch.object(
            api,
            "call",
            side_effect=[
                {"session": running},
                {"session": terminal, "events": []},
                {
                    "session": terminal,
                    "events": [
                        {"sequence": 1, "stream": "stdout", "data": '{"counter": 1}'},
                        {"sequence": 2, "stream": "exit", "exit_code": 0},
                    ],
                },
            ],
        ) as calls:
            self.assertEqual(api.probe("test-sandbox"), {"counter": 1})
        self.assertIn("wait_seconds=0.0", calls.call_args_list[-1].args[0])
        with patch.object(
            api,
            "call",
            side_effect=[
                {"session": terminal},
                {"session": terminal, "events": []},
            ],
        ):
            with self.assertRaisesRegex(RuntimeError, "without its terminal event"):
                api.probe("test-sandbox")

    def test_probe_rejects_lost_events_and_enforces_completion_deadline(self):
        api = benchmark.NodeApi("http://127.0.0.1:8090", "test-token", timeout=1)
        session = {"id": "test-exec", "status": "running", "exit_code": None}
        with patch.object(
            api,
            "call",
            side_effect=[
                {"session": session},
                {
                    "session": session,
                    "events": [
                        {"sequence": 2, "stream": "stdout", "data": '{"counter": 1}'},
                    ],
                },
            ],
        ):
            with self.assertRaisesRegex(RuntimeError, "event history has a gap"):
                api.probe("test-sandbox")
        with patch.object(api, "call", return_value={"session": session}) as calls:
            with patch.object(benchmark.time, "monotonic", side_effect=[0, 2]):
                with self.assertRaisesRegex(TimeoutError, "completion deadline"):
                    api.probe("test-sandbox")
            self.assertEqual(calls.call_count, 1)

    def test_lifecycle_uses_real_node_contract_and_preserves_every_process(self):
        api = FakeNode()
        result = benchmark.run(args(), api)
        self.assertEqual(result["status"], "passed", result)
        self.assertEqual(api.created, 4)
        self.assertEqual(len(api.deleted), 4)
        self.assertFalse(result["remaining_owned_ids"])
        wakes = [p for p in result["phases"] if p["name"].startswith("wake_")]
        self.assertEqual(len(wakes), 2)
        self.assertTrue(all(p["service"]["count"] == 4 for p in wakes))
        self.assertTrue(
            all(p["completion"]["max_ms"] <= p["makespan_ms"] for p in wakes)
        )
        self.assertEqual(result["image_pull"]["image"]["id"], "sha256:" + "a" * 64)
        self.assertEqual(
            result["snapshots"]["baseline"]["deployment_id"], "test-deployment"
        )
        self.assertEqual(result["snapshots"]["baseline"]["init_version"], "test-init")

    def test_uncertain_create_response_still_cleans_up_every_owned_sandbox(self):
        api = FakeNode(fail_create=True)
        result = benchmark.run(args(), api)
        self.assertEqual(result["status"], "failed")
        self.assertIn("create response lost", result["errors"][0])
        self.assertEqual(len(api.deleted), 4)
        self.assertFalse(api.records)

    def test_restarted_process_cannot_pass_wake(self):
        api = FakeNode(corrupt_wake=True)
        result = benchmark.run(args(), api)
        self.assertEqual(result["status"], "failed")
        self.assertIn("replaced the process", result["errors"][0])
        self.assertFalse(api.records)

    def test_latency_and_cleanup_failures_are_failed_results(self):
        result = benchmark.run(args("--park-p95-ms", "0.000001"), FakeNode())
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["slo_violations"])
        result = benchmark.run(args(), FakeNode(fail_delete=True))
        self.assertEqual(result["status"], "failed")
        self.assertEqual(len(result["remaining_owned_ids"]), 4)
        self.assertTrue(result["cleanup_errors"])

    def test_latency_failures_survive_a_later_failed_wake(self):
        result = benchmark.run(
            args("--park-p95-ms", "0.000001"), FakeNode(corrupt_wake=True)
        )
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["errors"])
        self.assertEqual(
            result["slo_violations"],
            ["park_0: service p95 exceeds 1e-06 ms"],
        )

    def test_queue_completion_latency_is_an_independent_slo(self):
        result = benchmark.run(args("--completion-p95-ms", "0.000001"), FakeNode())
        self.assertEqual(result["status"], "failed")
        self.assertTrue(
            any("completion p95" in item for item in result["slo_violations"])
        )
        self.assertFalse(
            any("service p95" in item for item in result["slo_violations"])
        )

    def test_cleanup_waits_for_create_that_was_not_yet_in_inventory(self):
        class LateNode(FakeNode):
            heartbeat_reads = 0

            def call(self, path, **kwargs):
                result = super().call(path, **kwargs)
                if path == "/v1/heartbeat":
                    self.heartbeat_reads += 1
                    if self.heartbeat_reads == 1:
                        self.records["late"] = {
                            "id": "late",
                            "generation": 1,
                            "state": "running",
                        }
                        result["heartbeat"]["active_sandbox_creates"] = 1
                return result

        api = LateNode()
        evidence = {"snapshots": {}, "cleanup_errors": []}
        with patch.object(benchmark.time, "sleep"):
            benchmark.cleanup_owned_sandboxes(
                api, ["late"], evidence, timeout_seconds=1
            )
        self.assertEqual(api.deleted, ["late"])
        self.assertFalse(evidence["cleanup_errors"])
        self.assertFalse(evidence["remaining_owned_ids"])

    def test_cleanup_bounds_parallel_deletes_and_drains_before_quiescence(self):
        class BlockingNode(FakeNode):
            def __init__(self):
                super().__init__()
                self.lock = threading.Lock()
                self.release = threading.Event()
                self.first_batch = threading.Event()
                self.active = 0
                self.maximum_active = 0

            def call(self, path, **kwargs):
                if kwargs.get("method") == "DELETE":
                    with self.lock:
                        self.active += 1
                        self.maximum_active = max(self.maximum_active, self.active)
                        if self.active == 8:
                            self.first_batch.set()
                    try:
                        if not self.release.wait(5):
                            raise TimeoutError("test did not release deletion")
                        return super().call(path, **kwargs)
                    finally:
                        with self.lock:
                            self.active -= 1
                with self.lock:
                    if self.active:
                        raise AssertionError(
                            "quiescence sampled before deletes drained"
                        )
                return super().call(path, **kwargs)

        api = BlockingNode()
        ids = [f"owned-{index}" for index in range(128)]
        for sandbox_id in ids:
            api.records[sandbox_id] = {
                "id": sandbox_id,
                "generation": 7,
                "state": "running",
            }
        evidence = {"snapshots": {}, "cleanup_errors": []}
        errors = []

        def cleanup():
            try:
                benchmark.cleanup_owned_sandboxes(
                    api, ids, evidence, timeout_seconds=10
                )
            except Exception as exc:
                errors.append(exc)

        worker = threading.Thread(target=cleanup)
        worker.start()
        try:
            self.assertTrue(api.first_batch.wait(3), "deletes did not run in parallel")
            self.assertEqual(api.maximum_active, 8)
            self.assertFalse(evidence.get("snapshots", {}).get("final"))
        finally:
            api.release.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertFalse(errors)
        self.assertEqual(api.maximum_active, 8)
        self.assertEqual(set(api.deleted), set(ids))
        self.assertFalse(api.records)
        self.assertFalse(evidence["remaining_owned_ids"])
        self.assertFalse(evidence["cleanup_errors"])

    def test_cleanup_failed_delete_does_not_abandon_other_parallel_deletes(self):
        class FailingNode(FakeNode):
            def call(self, path, **kwargs):
                if kwargs.get("method") == "DELETE" and path.endswith("/owned-0"):
                    raise RuntimeError("one delete failed")
                return super().call(path, **kwargs)

        api = FailingNode()
        ids = [f"owned-{index}" for index in range(16)]
        for sandbox_id in ids:
            api.records[sandbox_id] = {
                "id": sandbox_id,
                "generation": 7,
                "state": "running",
            }
        evidence = {"snapshots": {}, "cleanup_errors": []}
        with self.assertRaisesRegex(TimeoutError, "quiescence"):
            benchmark.cleanup_owned_sandboxes(api, ids, evidence, timeout_seconds=0.1)
        self.assertEqual(set(api.deleted), set(ids) - {"owned-0"})
        self.assertEqual(evidence["remaining_owned_ids"], ["owned-0"])
        self.assertEqual(list(api.records), ["owned-0"])

    def test_storage_owners_errors_and_missing_metrics_prevent_idle_preflight(self):
        base = FakeNode().call("/v1/heartbeat")["heartbeat"]
        benchmark.check_node(base, {"sandboxes": []}, 32)
        for key in (
            "storage_ublk_active_devices",
            "storage_error_volumes",
            "storage_hard_reserved_mb",
        ):
            for value in (1, None, "0", False):
                with self.subTest(key=key, value=value):
                    heartbeat = {
                        **base,
                        "runtime_metrics": {**base["runtime_metrics"], key: value},
                    }
                    with self.assertRaisesRegex(RuntimeError, key):
                        benchmark.check_node(heartbeat, {"sandboxes": []}, 32)
        for key in ("storage_ublk_active_devices", "storage_error_volumes"):
            with self.subTest(missing=key):
                metrics = dict(base["runtime_metrics"])
                del metrics[key]
                with self.assertRaisesRegex(RuntimeError, key):
                    benchmark.check_node(
                        {**base, "runtime_metrics": metrics}, {"sandboxes": []}, 32
                    )

    def test_cleanup_waits_for_retired_storage_after_inventory_is_empty(self):
        class RetiredNode(FakeNode):
            heartbeat_reads = 0

            def call(self, path, **kwargs):
                result = super().call(path, **kwargs)
                if path == "/v1/heartbeat":
                    self.heartbeat_reads += 1
                    if self.heartbeat_reads == 1:
                        result["heartbeat"]["runtime_metrics"].update(
                            {
                                "storage_ublk_active_devices": 1,
                                "storage_error_volumes": 1,
                                "storage_hard_reserved_mb": 5184,
                            }
                        )
                return result

        api = RetiredNode()
        evidence = {"snapshots": {}, "cleanup_errors": []}
        with patch.object(benchmark.time, "sleep"):
            benchmark.cleanup_owned_sandboxes(api, [], evidence, timeout_seconds=1)
        self.assertEqual(api.heartbeat_reads, 2)
        self.assertFalse(evidence["cleanup_errors"])
        self.assertEqual(
            evidence["snapshots"]["final"]["runtime_metrics"][
                "storage_ublk_idle_devices"
            ],
            16,
        )

    def test_cleanup_cannot_pass_with_retired_or_unknown_storage_owners(self):
        for key, value in (
            ("storage_ublk_active_devices", 1),
            ("storage_ublk_active_devices", None),
            ("storage_error_volumes", 1),
            ("storage_error_volumes", None),
            ("storage_hard_reserved_mb", 1),
        ):
            with self.subTest(key=key, value=value):
                api = FakeNode()
                original = api.call

                def call(path, **kwargs):
                    result = original(path, **kwargs)
                    if path == "/v1/heartbeat":
                        metrics = result["heartbeat"]["runtime_metrics"]
                        if value is None:
                            del metrics[key]
                        else:
                            metrics[key] = value
                    return result

                api.call = call
                evidence = {"snapshots": {}, "cleanup_errors": []}
                with self.assertRaisesRegex(TimeoutError, key):
                    benchmark.cleanup_owned_sandboxes(
                        api, [], evidence, timeout_seconds=0.01
                    )
                self.assertFalse(evidence["remaining_owned_ids"])

    def test_unknown_or_running_creates_cannot_report_successful_cleanup(self):
        for active in (None, 1):
            with self.subTest(active=active):
                api = FakeNode()
                original = api.call

                def call(path, **kwargs):
                    result = original(path, **kwargs)
                    if path == "/v1/heartbeat":
                        result["heartbeat"]["active_sandbox_creates"] = active
                    return result

                api.call = call
                result = benchmark.run(args(), api)
                self.assertEqual(result["status"], "failed")
                self.assertIn("quiescence", result["cleanup_errors"][-1])

    def test_interruption_persists_owned_ids_and_cleanup_result(self):
        class InterruptedNode(FakeNode):
            def probe(self, sandbox_id, **kwargs):
                raise KeyboardInterrupt()

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "evidence.json"
            api = InterruptedNode()
            result = benchmark.run(
                args(),
                api,
                persist=lambda value: benchmark.persist_evidence(path, value),
            )
            observed = json.loads(path.read_text())
        self.assertEqual(observed, result)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["interrupted"])
        self.assertEqual(len(result["owned_ids"]), 4)
        self.assertFalse(api.records)
        self.assertFalse(result["cleanup_errors"])

    def test_busy_node_fails_without_touching_others(self):
        api = FakeNode()
        api.records["other-user"] = {
            "id": "other-user",
            "generation": 7,
            "state": "running",
        }
        result = benchmark.run(args(), api)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(api.created, 0)
        self.assertEqual(api.deleted, [])
        self.assertIn("other-user", api.records)

    def test_wrong_host_shape_and_invalid_working_set_fail(self):
        api = FakeNode()
        result = benchmark.run(args("--expected-cpus", "64"), api)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(api.created, 0)
        with self.assertRaises(SystemExit):
            args("--resident-mb", "1024")
        with self.assertRaises(SystemExit):
            args("--cpus", "nan")

    def test_dirty_working_set_defaults_to_resident_and_rejects_invalid_sizes(self):
        defaults = args()
        self.assertEqual(defaults.dirty_mb, 256)
        self.assertEqual(defaults.dirty_page_count, 65_536)
        self.assertEqual(args("--resident-mb", "128").dirty_mb, 128)
        subset = args("--dirty-mb", "16")
        self.assertEqual(subset.dirty_mb, 16)
        self.assertEqual(subset.dirty_page_count, 4096)
        for size in ("0", "-1", "257", "1.5"):
            with self.subTest(size=size), self.assertRaises(SystemExit):
                args("--dirty-mb", size)

    def test_evidence_records_resolved_dirty_profile_and_phase_utc_times(self):
        result = benchmark.run(args("--dirty-mb", "16"), FakeNode())
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["configuration"]["resident_mb"], 256)
        self.assertEqual(result["configuration"]["dirty_mb"], 16)
        self.assertEqual(result["configuration"]["dirty_page_count"], 4096)
        for phase in result["phases"]:
            begin = datetime.fromisoformat(phase["started_at"])
            finish = datetime.fromisoformat(phase["finished_at"])
            self.assertEqual(begin.utcoffset(), timezone.utc.utcoffset(begin))
            self.assertEqual(finish.utcoffset(), timezone.utc.utcoffset(finish))
            self.assertLessEqual(begin, finish)
            self.assertGreaterEqual(phase["makespan_ms"], 0)
            for sample in phase["samples"]:
                state = sample["result"]
                if "resident_bytes" in state:
                    self.assertEqual(state["dirty_mb"], 16)
                    self.assertEqual(state["dirty_page_count"], 4096)

    def test_incorrect_guest_dirty_profile_fails_qualification(self):
        class WrongProfileNode(FakeNode):
            def probe(self, sandbox_id, **kwargs):
                return {**super().probe(sandbox_id, **kwargs), "dirty_page_count": 1}

        result = benchmark.run(args("--dirty-mb", "16"), WrongProfileNode())
        self.assertEqual(result["status"], "failed")
        self.assertIn("dirty working set size mismatch", result["errors"][0])
        self.assertFalse(result["remaining_owned_ids"])

    def test_auto_parked_residents_cannot_claim_simultaneous_running_density(self):
        class IdleParkingNode(FakeNode):
            def __init__(self, *, after_wake):
                super().__init__()
                self.after_wake = after_wake
                self.wakes = 0
                self.idled = False

            def call(self, path, **kwargs):
                result = super().call(path, **kwargs)
                if path.endswith("/wake"):
                    self.wakes += 1
                if path == "/v1/heartbeat" and len(self.records) == 4:
                    if not self.idled and (not self.after_wake or self.wakes == 4):
                        next(iter(self.records.values()))["state"] = "parked"
                        self.idled = True
                    result["heartbeat"]["active_sandboxes"] = sum(
                        record["state"] == "running" for record in self.records.values()
                    )
                return result

        for after_wake in (False, True):
            with self.subTest(after_wake=after_wake):
                api = IdleParkingNode(after_wake=after_wake)
                result = benchmark.run(args(), api)
                self.assertEqual(result["status"], "failed")
                self.assertIn("simultaneously running", result["errors"][0])
                names = [phase["name"] for phase in result["phases"]]
                self.assertEqual("wake_0" in names, after_wake)
                self.assertNotIn("act_1", names)
                self.assertFalse(api.records)
                self.assertFalse(result["cleanup_errors"])

    def test_running_density_requires_exact_complete_active_inventory(self):
        heartbeat = {
            "active_sandboxes": 2,
            "active_sandbox_creates": 0,
            "inventory_complete": True,
        }
        inventory = {
            "sandboxes": [{"id": item, "state": "running"} for item in ("a", "b")]
        }
        benchmark.check_running_residents(heartbeat, inventory, ["a", "b"])
        for change, message in (
            ({"active_sandboxes": 1}, "active sandbox count"),
            ({"active_sandboxes": None}, "active sandbox count"),
            ({"inventory_complete": False}, "incomplete"),
            ({"inventory_complete": None}, "incomplete"),
            ({"active_sandbox_creates": 1}, "quiescence"),
            ({"active_sandbox_creates": None}, "quiescence"),
        ):
            with (
                self.subTest(change=change),
                self.assertRaisesRegex(RuntimeError, message),
            ):
                benchmark.check_running_residents(
                    {**heartbeat, **change}, inventory, ["a", "b"]
                )
        with self.assertRaisesRegex(RuntimeError, "target count"):
            benchmark.check_running_residents(
                heartbeat,
                {"sandboxes": [*inventory["sandboxes"], inventory["sandboxes"][0]]},
                ["a", "b"],
            )

    def test_guest_fixture_exercises_memory_sqlite_socket_and_persistent_files(self):
        # Exercise the real guest protocol on the host; no gVisor claim is made.
        with tempfile.TemporaryDirectory(prefix="density-fixture-", dir="/tmp") as tmp:
            code = benchmark.WORKLOAD.replace("/workspace/density", tmp)
            with subprocess.Popen(
                [sys.executable, "-c", code, "1"],
                stderr=subprocess.PIPE,
            ) as process:
                try:
                    probe = benchmark.PROBE.replace("/workspace/density", tmp)

                    def invoke(op):
                        completed = subprocess.run(
                            [
                                sys.executable,
                                "-c",
                                probe,
                                json.dumps({"op": op, "cpu_ms": 1}),
                            ],
                            capture_output=True,
                            text=True,
                            timeout=10,
                            check=True,
                        )
                        return json.loads(completed.stdout)

                    before = invoke("status")
                    after = invoke("act")
                    benchmark.check_identity(before, after, advanced=True)
                    self.assertEqual(after["resident_bytes"], 1024 * 1024)
                    self.assertEqual(after["dirty_mb"], 1)
                    self.assertEqual(after["dirty_page_count"], 256)
                    self.assertEqual(after["dirty_start_page"], 0)
                    self.assertEqual(after["files"], 64)
                    timings = after["guest_timings_ms"]
                    self.assertEqual(
                        set(timings),
                        {
                            "memory_verify",
                            "persistent_verify",
                            "memory_dirty_and_hash",
                            "files_write_read",
                            "sqlite_commit",
                            "cpu_work",
                            "total",
                        },
                    )
                    self.assertTrue(all(value >= 0 for value in timings.values()))
                    self.assertGreaterEqual(
                        timings["total"],
                        sum(value for key, value in timings.items() if key != "total"),
                    )
                    self.assertNotEqual(before["memory_sha256"], after["memory_sha256"])
                    self.assertEqual(len(list(Path(tmp).glob("source-*"))), 64)
                    with sqlite3.connect(Path(tmp, "state.sqlite")) as database:
                        database.execute("update state set counter = 99")
                        database.commit()
                    self.assertIn("SQLite", invoke("act")["error"])
                    with sqlite3.connect(Path(tmp, "state.sqlite")) as database:
                        database.execute("update state set counter = 1")
                        database.commit()
                    Path(tmp, "source-0").write_bytes(b"corrupted checkpoint")
                    self.assertIn("persisted file", invoke("act")["error"])
                finally:
                    process.terminate()
                    process.wait(timeout=5)

    def test_guest_rotates_dirty_subset_and_hashes_all_resident_memory(self):
        with tempfile.TemporaryDirectory(prefix="density-fixture-", dir="/tmp") as tmp:
            code = benchmark.WORKLOAD.replace("/workspace/density", tmp).replace(
                "timings['total'] =",
                "(root / 'test-memory.bin').write_bytes(memory)\n        timings['total'] =",
            )
            with subprocess.Popen([sys.executable, "-c", code, "5", "2"]) as process:
                try:

                    def invoke(op):
                        completed = subprocess.run(
                            [
                                sys.executable,
                                "-c",
                                benchmark.PROBE.replace("/workspace/density", tmp),
                                json.dumps({"op": op, "cpu_ms": 1}),
                            ],
                            capture_output=True,
                            text=True,
                            timeout=10,
                            check=True,
                        )
                        return json.loads(completed.stdout)

                    state = invoke("status")
                    memory_path = Path(tmp, "test-memory.bin")
                    before = memory_path.read_bytes()
                    self.assertEqual(len(before), 5 * 1024 * 1024)
                    self.assertEqual(
                        state["memory_sha256"], hashlib.sha256(before).hexdigest()
                    )
                    self.assertIsNone(state["dirty_start_page"])
                    for start_page in (0, 512, 1024, 256):
                        after = invoke("act")
                        benchmark.check_identity(state, after, advanced=True)
                        self.assertEqual(after["dirty_mb"], 2)
                        self.assertEqual(after["dirty_page_count"], 512)
                        self.assertEqual(after["dirty_start_page"], start_page)
                        expected = bytearray(before)
                        for page in range(512):
                            offset = ((start_page + page) % 1280) * 4096
                            expected[offset] = (expected[offset] + 1) % 256
                        observed = memory_path.read_bytes()
                        self.assertEqual(observed, expected)
                        self.assertEqual(
                            after["memory_sha256"], hashlib.sha256(observed).hexdigest()
                        )
                        state, before = after, observed
                finally:
                    process.terminate()
                    process.wait(timeout=5)

    def test_guest_checks_memory_bytes_outside_the_dirty_subset(self):
        with tempfile.TemporaryDirectory(prefix="density-fixture-", dir="/tmp") as tmp:
            code = benchmark.WORKLOAD.replace("/workspace/density", tmp).replace(
                "server.listen(16)",
                "server.listen(16)\nmemory[2 * 1024 * 1024 - 123] ^= 1",
            )
            with subprocess.Popen([sys.executable, "-c", code, "2", "1"]) as process:
                try:
                    result = subprocess.run(
                        [
                            sys.executable,
                            "-c",
                            benchmark.PROBE.replace("/workspace/density", tmp),
                            json.dumps({"op": "act", "cpu_ms": 1}),
                        ],
                        capture_output=True,
                        text=True,
                        timeout=10,
                        check=True,
                    )
                    self.assertEqual(
                        json.loads(result.stdout)["error"], "resident memory corrupted"
                    )
                finally:
                    process.terminate()
                    process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
