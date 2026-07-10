import argparse
from datetime import datetime, timedelta, timezone
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
)
from ucloud_sandboxes.cli import _bootstrap_retry_delay_seconds
from ucloud_sandboxes.metrics import MetricsStore
from ucloud_sandboxes.models import VmJob
from ucloud_sandboxes.vm_init import VmInitOptions, VmInitPlan


class BootstrapRetryTests(unittest.TestCase):
    def test_fast_failure_retry_is_not_blocked_by_slow_peer(self) -> None:
        slow_release = Event()
        fast_finished = Event()
        retry_started = Event()
        attempts_lock = Lock()
        fast_attempts = 0

        def intent(job_id: str) -> VmBootstrapIntent:
            job = VmJob(
                id=job_id,
                project_id="project-1",
                name=job_id,
                application_name="vm-ubuntu",
                application_version="24.04",
                product_id="cpu",
                product_category="cpu",
                state="RUNNING",
            )
            return VmBootstrapIntent(
                job_id=job_id,
                node_id=f"node-{job_id}",
                role="sandbox",
                plan=VmInitPlan(
                    job=job,
                    ssh_command=f"ssh {job_id}",
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
                metrics = MetricsStore(Path(raw_dir) / "metrics.jsonl")
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

    def test_legacy_record_uses_configured_backoff(self) -> None:
        attempted_at = datetime(2026, 7, 10, tzinfo=timezone.utc)
        record = VmBootstrapRecord.from_dict(
            {
                "jobId": "job-1",
                "status": "failed",
                "lastAttemptAt": attempted_at.isoformat(),
            }
        )

        self.assertFalse(
            record.retry_due(
                now=attempted_at + timedelta(seconds=29),
                retry_seconds=30,
            )
        )
        self.assertTrue(
            record.retry_due(
                now=attempted_at + timedelta(seconds=30),
                retry_seconds=30,
            )
        )

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
