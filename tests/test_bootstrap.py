from datetime import datetime, timedelta, timezone
import unittest

from ucloud_sandboxes.bootstrap import VmBootstrapRecord
from ucloud_sandboxes.cli import _bootstrap_retry_delay_seconds


class BootstrapRetryTests(unittest.TestCase):
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
