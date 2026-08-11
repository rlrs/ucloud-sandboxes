import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock
from time import monotonic
import unittest

from ucloud_sandboxes import cli
from ucloud_sandboxes.bootstrap import (
    VmBootstrapIntent,
    VmBootstrapRecord,
    VmBootstrapStore,
    build_vm_bootstrap_intents,
    mark_bootstrap_access_refresh,
)
from ucloud_sandboxes.cli import _bootstrap_retry_delay_seconds
from ucloud_sandboxes.metrics import MetricsStore
from ucloud_sandboxes.models import InstancePhase, SandboxNode, ProviderInstance
from ucloud_sandboxes.providers.base import InstanceBootstrapAccess
from ucloud_sandboxes.vm_init import VmInitOptions


class BootstrapRetryTests(unittest.TestCase):
    def test_excluded_nodes_do_not_consume_provider_refresh_budget(self) -> None:
        def node(job_id: str) -> SandboxNode:
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

        nodes = [node(f"job-{index}") for index in range(3)]
        refreshed: list[str] = []

        def unavailable(instance: ProviderInstance) -> InstanceBootstrapAccess:
            return InstanceBootstrapAccess(
                instance=instance,
                command=None,
                runnable=False,
                reason="not ready",
                refresh_recommended=True,
            )

        def refresh(instance: ProviderInstance) -> InstanceBootstrapAccess:
            refreshed.append(instance.id)
            return unavailable(instance)

        def options(current: SandboxNode, _role: str) -> VmInitOptions:
            return VmInitOptions(
                job_id=current.job_id,
                heartbeat_url="http://gateway/v1/nodes/heartbeat",
            )
        intents = build_vm_bootstrap_intents(
            nodes,
            {},
            retry_seconds=30,
            max_per_cycle=2,
            options_for_node=options,
            access_for_instance=unavailable,
            refresh_access_for_instance=refresh,
            max_access_refreshes=2,
            excluded_job_ids={"job-0"},
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )

        self.assertEqual(refreshed, ["job-1", "job-2"])
        self.assertEqual([intent.job_id for intent in intents], ["job-1", "job-2"])

    def test_provider_access_refreshes_are_bounded_and_rotate_fairly(self) -> None:
        def node(job_id: str) -> SandboxNode:
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
            return SandboxNode(
                job=job,
                heartbeat=None,
                active_sandboxes=0,
                heartbeat_fresh=False,
            )

        nodes = [node(f"job-{index}") for index in range(5)]
        refreshed: list[str] = []

        def unavailable(instance: ProviderInstance) -> InstanceBootstrapAccess:
            return InstanceBootstrapAccess(
                instance=instance,
                command=None,
                runnable=False,
                reason="not ready",
                refresh_recommended=True,
            )

        def refresh(instance: ProviderInstance) -> InstanceBootstrapAccess:
            refreshed.append(instance.id)
            return unavailable(instance)

        def options(current: SandboxNode, _role: str) -> VmInitOptions:
            return VmInitOptions(
                job_id=current.job_id,
                heartbeat_url="http://gateway/v1/nodes/heartbeat",
            )
        first_now = datetime(2026, 7, 10, tzinfo=timezone.utc)
        first = build_vm_bootstrap_intents(
            nodes,
            {},
            retry_seconds=30,
            max_per_cycle=2,
            options_for_node=options,
            access_for_instance=unavailable,
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
            options_for_node=options,
            access_for_instance=unavailable,
            refresh_access_for_instance=refresh,
            max_access_refreshes=2,
            now=first_now + timedelta(seconds=1),
        )
        self.assertEqual(refreshed, ["job-2", "job-3"])
        self.assertEqual(sum(item.access_refreshed_at is not None for item in second), 2)

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
        ):
            del attempt_started_perf
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
                store = VmBootstrapStore(Path(raw_dir) / "bootstrap.json")
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
                self.assertEqual(store.load()["fast"].status, "failed")
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
                self.assertEqual(store.load()["fast"].status, "attempting")
                self.assertTrue(retry_started.wait(timeout=1))
                self.assertFalse(slow_release.is_set())
                self.assertGreaterEqual(fence_checks, 6)
        finally:
            slow_release.set()
            if coordinator is not None:
                coordinator.shutdown()
            cli._execute_vm_bootstrap_attempt = original_execute

    def test_transient_retry_delay_overrides_normal_backoff(self) -> None:
        attempted_at = datetime(2026, 7, 10, tzinfo=timezone.utc)
        record = VmBootstrapRecord(
            job_id="job-1",
            status="failed",
            last_attempt_at=attempted_at,
            retry_delay_seconds=1,
        )

        self.assertFalse(
            record.retry_due(
                now=attempted_at + timedelta(milliseconds=999),
                retry_seconds=30,
            )
        )
        self.assertTrue(
            record.retry_due(
                now=attempted_at + timedelta(seconds=1),
                retry_seconds=30,
            )
        )

    def test_record_requires_current_schema(self) -> None:
        with self.assertRaisesRegex(ValueError, "current schema"):
            VmBootstrapRecord.from_dict(
                {
                    "job_id": "job-1",
                    "status": "failed",
                }
            )

    def test_store_roundtrip_uses_versioned_schema(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "bootstrap.json"
            store = VmBootstrapStore(path)
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

            store.save({"job-1": record})
            raw = json.loads(path.read_text(encoding="utf-8"))
            loaded = store.load()

            self.assertEqual(set(raw), {"version", "jobs"})
            self.assertEqual(raw["version"], 1)
            self.assertEqual(loaded, {"job-1": record})
            with self.assertRaisesRegex(ValueError, "embedded job_id"):
                store.save({"different-job": record})

    def test_store_rejects_invalid_envelopes_and_records(self) -> None:
        valid_record = VmBootstrapRecord(
            job_id="job-1",
            node_id="node-1",
            role="sandbox",
            status="failed",
            attempts=2,
            last_attempt_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
            last_error="not ready",
            retry_delay_seconds=4,
        ).to_dict()
        invalid_payloads: tuple[object, ...] = (
            [],
            {"jobs": {}},
            {"version": 1, "jobs": {}, "extra": True},
            {"version": True, "jobs": {}},
            {"version": 2, "jobs": {}},
            {"version": 1, "jobs": []},
            {
                "version": 1,
                "jobs": {"different-job": valid_record},
            },
            {
                "version": 1,
                "jobs": {"job-1": {**valid_record, "attempts": True}},
            },
            {
                "version": 1,
                "jobs": {"job-1": {**valid_record, "attempts": "2"}},
            },
            {
                "version": 1,
                "jobs": {"job-1": {**valid_record, "attempts": -1}},
            },
            {
                "version": 1,
                "jobs": {
                    "job-1": {**valid_record, "last_attempt_at": "not-a-time"}
                },
            },
            {
                "version": 1,
                "jobs": {
                    "job-1": {
                        **valid_record,
                        "last_attempt_at": "2026-07-10T00:00:00",
                    }
                },
            },
            {
                "version": 1,
                "jobs": {
                    "job-1": {**valid_record, "retry_delay_seconds": True}
                },
            },
            {
                "version": 1,
                "jobs": {"job-1": {**valid_record, "node_id": 1}},
            },
        )

        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "bootstrap.json"
            store = VmBootstrapStore(path)
            for payload in invalid_payloads:
                with self.subTest(payload=payload):
                    path.write_text(json.dumps(payload), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        store.load()

    def test_ssh_failures_use_bounded_exponential_retry(self) -> None:
        delays = [
            _bootstrap_retry_delay_seconds(
                255,
                attempt_count=attempt,
                configured_retry_seconds=30,
            )
            for attempt in range(1, 8)
        ]

        self.assertEqual(delays, [1, 2, 4, 8, 16, 30, 30])
        self.assertIsNone(
            _bootstrap_retry_delay_seconds(
                17,
                attempt_count=1,
                configured_retry_seconds=30,
            )
        )


if __name__ == "__main__":
    unittest.main()
