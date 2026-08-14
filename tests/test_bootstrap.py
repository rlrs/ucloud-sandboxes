import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Event, Lock
from time import monotonic
import unittest

from ucloud_sandboxes import cli
from ucloud_sandboxes.bootstrap import (
    VmBootstrapIntent,
    VmBootstrapRecord,
    build_vm_bootstrap_intents,
    mark_bootstrap_access_refresh,
)
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.metrics import MetricsStore
from ucloud_sandboxes.models import InstancePhase, SandboxNode, ProviderInstance
from ucloud_sandboxes.providers.base import InstanceBootstrapAccess
from ucloud_sandboxes.vm_init import VmInitOptions


def bootstrap_node(job_id: str) -> SandboxNode:
    instance = ProviderInstance(
        id=job_id,
        name=job_id,
        application_name="vm-ubuntu",
        application_version="24.04",
        product_id="cpu",
        product_category="cpu",
        state="RUNNING",
        phase=InstancePhase.RUNNING,
    )
    return SandboxNode(
        job=instance,
        heartbeat=None,
        active_sandboxes=0,
        heartbeat_fresh=False,
    )


def unavailable_access(instance: ProviderInstance) -> InstanceBootstrapAccess:
    return InstanceBootstrapAccess(
        instance=instance,
        command=None,
        runnable=False,
        reason="not ready",
        refresh_recommended=True,
    )


def bootstrap_options(node: SandboxNode, _role: str) -> VmInitOptions:
    return VmInitOptions(
        job_id=node.job_id,
        heartbeat_url="http://gateway/v1/nodes/heartbeat",
    )


class BootstrapRetryTests(unittest.TestCase):
    def test_provider_access_refreshes_are_bounded_and_rotate_fairly(self) -> None:
        nodes = [bootstrap_node(f"job-{index}") for index in range(5)]
        refreshed: list[str] = []

        def refresh(instance: ProviderInstance) -> InstanceBootstrapAccess:
            refreshed.append(instance.id)
            return unavailable_access(instance)

        first_now = datetime(2026, 7, 10, tzinfo=timezone.utc)
        first = build_vm_bootstrap_intents(
            nodes,
            {},
            retry_seconds=30,
            max_per_cycle=2,
            options_for_node=bootstrap_options,
            access_for_instance=unavailable_access,
            refresh_access_for_instance=refresh,
            max_access_refreshes=99,
            now=first_now,
        )
        self.assertEqual(refreshed, ["job-0", "job-1"])
        self.assertEqual(sum(item.access_refreshed_at is not None for item in first), 2)

        records: dict[str, VmBootstrapRecord] = {}
        for intent in first:
            records = mark_bootstrap_access_refresh(records, intent)
        refreshed.clear()
        second = build_vm_bootstrap_intents(
            nodes,
            records,
            retry_seconds=30,
            max_per_cycle=2,
            options_for_node=bootstrap_options,
            access_for_instance=unavailable_access,
            refresh_access_for_instance=refresh,
            max_access_refreshes=2,
            now=first_now + timedelta(seconds=1),
        )
        self.assertEqual(refreshed, ["job-2", "job-3"])
        self.assertEqual(
            sum(item.access_refreshed_at is not None for item in second), 2
        )

    def test_successful_init_is_not_replayed_for_a_stale_heartbeat(self) -> None:
        job = ProviderInstance(
            id="job-1",
            name="ucloud-sandbox-node-1",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu",
            product_category="cpu",
            state="RUNNING",
            phase=InstancePhase.RUNNING,
            raw={"id": "job-1"},
        )
        node = SandboxNode(
            job=job,
            heartbeat=None,
            active_sandboxes=0,
            heartbeat_fresh=False,
        )
        access = InstanceBootstrapAccess(
            instance=job,
            command="ssh job-1",
            runnable=True,
            reason="ready",
        )

        intents = build_vm_bootstrap_intents(
            [node],
            {
                "job-1": VmBootstrapRecord(
                    job_id="job-1",
                    status="succeeded",
                    attempts=1,
                )
            },
            retry_seconds=30,
            max_per_cycle=1,
            options_for_node=lambda _node, _role: VmInitOptions(
                job_id="job-1",
                heartbeat_url="http://gateway/v1/nodes/heartbeat",
            ),
            access_for_instance=lambda _instance: access,
        )

        self.assertEqual(len(intents), 1)
        self.assertFalse(intents[0].runnable)
        self.assertIn("previously succeeded", intents[0].reason)

    def test_fast_failure_retry_is_not_blocked_by_slow_peer(self) -> None:
        slow_release = Event()
        fast_finished = Event()
        retry_started = Event()
        attempts_lock = Lock()
        fast_attempts = 0

        def intent(job_id: str) -> VmBootstrapIntent:
            job = ProviderInstance(
                id=job_id,
                name=job_id,
                application_name="vm-ubuntu",
                application_version="24.04",
                product_id="cpu",
                product_category="cpu",
                state="RUNNING",
                phase=InstancePhase.RUNNING,
            )
            return VmBootstrapIntent(
                job_id=job_id,
                node_id=f"node-{job_id}",
                role="sandbox",
                access=InstanceBootstrapAccess(
                    instance=job,
                    command=f"ssh {job_id}",
                    runnable=True,
                    reason="ready",
                ),
                options=VmInitOptions(
                    job_id=job_id,
                    heartbeat_url="http://gateway/v1/nodes/heartbeat",
                ),
                runnable=True,
                reason="ready",
            )

        def fake_execute(
            bootstrap_intent,
            _args,
            *,
            attempt_count,
            assert_provider_fence,
            attempt_started_perf,
            telemetry,
            trace_context,
        ):
            del attempt_started_perf, telemetry, trace_context
            assert_provider_fence()
            nonlocal fast_attempts
            if bootstrap_intent.job_id == "slow":
                slow_release.wait(timeout=2)
                return cli._VmBootstrapAttemptResult(
                    result={
                        "jobId": "slow",
                        "status": "succeeded",
                        "durationMs": 20,
                    },
                    status="succeeded",
                    returncode=0,
                )
            with attempts_lock:
                fast_attempts += 1
                current_attempt = fast_attempts
            if current_attempt == 1:
                fast_finished.set()
                return cli._VmBootstrapAttemptResult(
                    result={
                        "jobId": "fast",
                        "status": "failed",
                        "durationMs": 7,
                    },
                    status="failed",
                    returncode=255,
                    error="SSH not ready",
                    retry_delay_seconds=0,
                )
            self.assertEqual(attempt_count, 2)
            retry_started.set()
            return cli._VmBootstrapAttemptResult(
                result={
                    "jobId": "fast",
                    "status": "succeeded",
                    "durationMs": 5,
                },
                status="succeeded",
                returncode=0,
            )

        original_execute = cli._execute_vm_bootstrap_attempt
        cli._execute_vm_bootstrap_attempt = fake_execute
        coordinator = None
        try:
            with TemporaryDirectory() as raw_dir:
                store = ControlStateStore(Path(raw_dir) / "control-state.sqlite")
                metrics = MetricsStore(Path(raw_dir) / "metrics.sqlite")
                coordinator = cli._VmBootstrapCoordinator(2, metrics)
                records: dict[str, VmBootstrapRecord] = {}
                fence_checks = 0

                def assert_fence() -> None:
                    nonlocal fence_checks
                    fence_checks += 1

                records, _ = coordinator.submit(
                    intent("slow"),
                    argparse.Namespace(),
                    records,
                    store,
                    assert_provider_fence=assert_fence,
                )
                fast_intent = intent("fast")
                records, _ = coordinator.submit(
                    fast_intent,
                    argparse.Namespace(),
                    records,
                    store,
                    assert_provider_fence=assert_fence,
                )
                self.assertTrue(fast_finished.wait(timeout=1))

                records, completed = coordinator.collect_completed(
                    records,
                    store,
                    active_job_ids={"slow", "fast"},
                )
                self.assertEqual([item["jobId"] for item in completed], ["fast"])
                self.assertEqual(coordinator.available_slots, 1)
                self.assertIn("slow", coordinator.in_flight_job_ids)
                self.assertEqual(
                    store.load_bootstrap_records()["fast"].status,
                    "failed",
                )
                events = metrics.load_events()
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0].data["job_id"], "fast")
                self.assertEqual(events[0].data["duration_ms"], 7)

                wait_started = monotonic()
                coordinator.wait_for_activity(1.0)
                self.assertLess(monotonic() - wait_started, 0.2)

                records, scheduled = coordinator.submit(
                    fast_intent,
                    argparse.Namespace(),
                    records,
                    store,
                    assert_provider_fence=assert_fence,
                )
                self.assertEqual(scheduled["attempts"], 2)
                self.assertEqual(
                    store.load_bootstrap_records()["fast"].status,
                    "attempting",
                )
                self.assertTrue(retry_started.wait(timeout=1))
                self.assertFalse(slow_release.is_set())
                self.assertGreaterEqual(fence_checks, 6)
        finally:
            slow_release.set()
            if coordinator is not None:
                coordinator.shutdown()
            cli._execute_vm_bootstrap_attempt = original_execute

    def test_store_roundtrips_exact_bootstrap_records(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "control-state.sqlite"
            store = ControlStateStore(path)
            record = VmBootstrapRecord(
                job_id="job-1",
                node_id="node-1",
                role="sandbox",
                status="failed",
                attempts=2,
                last_attempt_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
                last_error="not ready",
                retry_delay_seconds=4,
            )

            store.save_bootstrap_records({"job-1": record})
            loaded = store.load_bootstrap_records()

            self.assertEqual(loaded, {"job-1": record})
            with self.assertRaisesRegex(ValueError, "key does not match"):
                store.save_bootstrap_records({"different-job": record})

    def test_store_rejects_legacy_json_and_corrupt_bootstrap_rows(self) -> None:
        with self.assertRaisesRegex(ValueError, "current schema"):
            VmBootstrapRecord.from_dict({"job_id": "job-1", "status": "failed"})
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            legacy = root / "legacy.json"
            legacy.write_text('{"version":1,"jobs":{}}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unreadable"):
                ControlStateStore(legacy)

            path = root / "control-state.sqlite"
            store = ControlStateStore(path)
            record = VmBootstrapRecord(job_id="job-1", attempts=1)
            store.save_bootstrap_records({"job-1": record})
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "UPDATE control_records SET payload = ' ' || payload "
                    "WHERE namespace = 'bootstrap' AND record_id = ?",
                    ("job-1",),
                )
            with self.assertRaisesRegex(ValueError, "invalid bootstrap control-state"):
                store.load_bootstrap_records()


if __name__ == "__main__":
    unittest.main()
