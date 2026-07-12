from datetime import datetime, timedelta, timezone
from pathlib import Path
import stat
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.autoscaler_state import (
    DEPLOYMENT_LABEL,
    PROVIDER_OPERATION_LABEL,
    AutoscalerStateStore,
    OperationConflictError,
    stable_provider_operation_id,
)


NOW = datetime(2026, 7, 9, 12, 0, tzinfo=timezone.utc)


def create_request(intent_key: str, deployment_id: str = "prod-a") -> dict:
    operation_id = stable_provider_operation_id(deployment_id, "create", intent_key)
    return {
        "type": "bulk",
        "items": [
            {
                "labels": {
                    PROVIDER_OPERATION_LABEL: operation_id,
                    DEPLOYMENT_LABEL: deployment_id,
                }
            }
        ],
    }


def provider_job(operation_id: str, job_id: str, deployment_id: str = "prod-a") -> dict:
    return {
        "id": job_id,
        "specification": {
            "labels": {
                PROVIDER_OPERATION_LABEL: operation_id,
                DEPLOYMENT_LABEL: deployment_id,
            }
        },
    }


class AutoscalerStateTests(unittest.TestCase):
    def test_process_lock_has_one_local_holder_and_releases(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = AutoscalerStateStore(Path(raw_dir) / "autoscaler.sqlite")
            first = store.process_lock()
            second = store.process_lock()

            self.assertTrue(first.acquire())
            self.assertFalse(second.acquire())
            first.release()
            self.assertTrue(second.acquire())
            second.release()

    def test_database_and_lock_files_are_owner_only(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "autoscaler.sqlite"
            store = AutoscalerStateStore(path)
            lock = store.process_lock()
            self.assertTrue(lock.acquire())
            lock.release()

            for candidate in (path, path.with_name(path.name + ".lock")):
                self.assertEqual(stat.S_IMODE(candidate.stat().st_mode), 0o600)

    def test_drain_desired_state_survives_restart_and_cancellation(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "autoscaler.sqlite"
            store = AutoscalerStateStore(path)
            intent = store.prepare_drain_intent(
                deployment_id="prod-a", job_id="job-1", role="sandbox", now=NOW
            )
            repeated = AutoscalerStateStore(path).prepare_drain_intent(
                deployment_id="prod-a", job_id="job-1", role="sandbox", now=NOW
            )
            self.assertEqual(repeated.token, intent.token)

            canceling = store.begin_drain_cancellation(
                deployment_id="prod-a", job_id="job-1", now=NOW
            )
            self.assertEqual(canceling.state, "canceling")
            self.assertEqual(canceling.token, intent.token)
            store.retire_drain_intent(
                deployment_id="prod-a", job_id="job-1", reason="canceled"
            )
            self.assertEqual(store.pending_drain_intents(deployment_id="prod-a"), [])

            reincarnated = store.prepare_drain_intent(
                deployment_id="prod-a", job_id="job-1", role="sandbox", now=NOW
            )
            self.assertNotEqual(reincarnated.token, intent.token)

    def test_operation_is_stable_and_conflicting_reuse_is_rejected(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = AutoscalerStateStore(Path(raw_dir) / "autoscaler.sqlite")
            request = create_request("sandbox:seed")
            operation = store.prepare_operation(
                intent_key="sandbox:seed",
                kind="create",
                deployment_id="prod-a",
                role="sandbox",
                request=request,
                now=NOW,
            )
            repeated = AutoscalerStateStore(store.path).prepare_operation(
                intent_key="sandbox:seed",
                kind="create",
                deployment_id="prod-a",
                role="sandbox",
                request=request,
                now=NOW,
            )
            self.assertEqual(repeated, operation)
            with self.assertRaises(OperationConflictError):
                store.prepare_operation(
                    intent_key="sandbox:seed",
                    kind="create",
                    deployment_id="prod-a",
                    role="builder",
                    request=request,
                    now=NOW,
                )

    def test_uncertain_create_recovers_by_indexed_provider_labels(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = AutoscalerStateStore(Path(raw_dir) / "autoscaler.sqlite")
            operation = store.prepare_operation(
                intent_key="sandbox:seed",
                kind="create",
                deployment_id="prod-a",
                role="sandbox",
                request=create_request("sandbox:seed"),
                now=NOW,
            )
            store.begin_provider_call(operation.operation_id, now=NOW)
            store.mark_operation_uncertain(
                operation.operation_id, error="connection dropped", now=NOW
            )
            self.assertEqual(store.submittable_operations(), [])

            recovery = AutoscalerStateStore(store.path).recover_uncertain_creates(
                [provider_job(operation.operation_id, "job-1")], now=NOW
            )
            self.assertEqual(recovery[0].status, "recovered")
            accepted = store.get_operation(operation.operation_id)
            self.assertEqual(accepted.state, "accepted")
            self.assertEqual(accepted.target_job_ids, ("job-1",))

            store.confirm_visible_creates(["job-1"], now=NOW)
            self.assertEqual(store.get_operation(operation.operation_id).state, "settled")

    def test_duplicate_create_label_correlation_fails_closed(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = AutoscalerStateStore(Path(raw_dir) / "autoscaler.sqlite")
            operation = store.prepare_operation(
                intent_key="sandbox:seed",
                kind="create",
                deployment_id="prod-a",
                request=create_request("sandbox:seed"),
            )
            store.begin_provider_call(operation.operation_id)

            result = store.recover_uncertain_creates(
                [
                    provider_job(operation.operation_id, "job-1"),
                    provider_job(operation.operation_id, "job-2"),
                ]
            )

            self.assertEqual(result[0].status, "conflict")
            self.assertEqual(store.get_operation(operation.operation_id).state, "uncertain")

    def test_uncertain_stop_retries_same_record_or_recovers_from_final(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = AutoscalerStateStore(Path(raw_dir) / "autoscaler.sqlite")
            operation = store.prepare_operation(
                intent_key="sandbox:job-1:drain-1",
                kind="stop",
                deployment_id="prod-a",
                role="sandbox",
                request={"type": "bulk", "items": [{"id": "job-1"}]},
                target_job_ids=("job-1",),
            )
            store.begin_provider_call(operation.operation_id)
            retry = store.recover_uncertain_stops([])
            self.assertEqual(retry[0].status, "retry")
            self.assertEqual(store.get_operation(operation.operation_id).state, "prepared")

            store.begin_provider_call(operation.operation_id)
            recovered = store.recover_uncertain_stops(["job-1"])
            self.assertEqual(recovered[0].status, "recovered")
            self.assertEqual(store.get_operation(operation.operation_id).state, "accepted")

    def test_resume_operation_waits_retries_and_settles_from_inventory(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = AutoscalerStateStore(Path(raw_dir) / "autoscaler.sqlite")
            operation = store.prepare_operation(
                intent_key="sandbox:job-1:2026-07-12T15:44:47Z",
                kind="resume",
                deployment_id="prod-a",
                role="sandbox",
                request={"type": "bulk", "items": [{"id": "job-1"}]},
                target_job_ids=("job-1",),
                now=NOW,
            )
            store.begin_provider_call(operation.operation_id, now=NOW)
            store.mark_operation_accepted(
                operation.operation_id,
                response={"responses": [{}]},
                target_job_ids=("job-1",),
                now=NOW,
            )

            waiting = store.reconcile_resume_operations(
                {"job-1": "SUSPENDED"}, now=NOW + timedelta(seconds=29)
            )
            self.assertEqual(waiting[0].status, "waiting")
            self.assertEqual(store.get_operation(operation.operation_id).state, "accepted")

            retry = store.reconcile_resume_operations(
                {"job-1": "SUSPENDED"}, now=NOW + timedelta(seconds=30)
            )
            self.assertEqual(retry[0].status, "retry")
            self.assertEqual(store.get_operation(operation.operation_id).state, "prepared")

            store.begin_provider_call(operation.operation_id, now=NOW + timedelta(seconds=31))
            recovered = store.reconcile_resume_operations(
                {"job-1": "RUNNING"}, now=NOW + timedelta(seconds=32)
            )
            self.assertEqual(recovered[0].status, "recovered")
            self.assertEqual(store.get_operation(operation.operation_id).state, "settled")

    def test_resume_operation_fails_if_job_becomes_final(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = AutoscalerStateStore(Path(raw_dir) / "autoscaler.sqlite")
            operation = store.prepare_operation(
                intent_key="builder:job-1:started",
                kind="resume",
                deployment_id="prod-a",
                role="builder",
                request={"type": "bulk", "items": [{"id": "job-1"}]},
                target_job_ids=("job-1",),
                now=NOW,
            )
            store.begin_provider_call(operation.operation_id, now=NOW)

            result = store.reconcile_resume_operations(
                {"job-1": "FAILURE"}, now=NOW + timedelta(seconds=1)
            )

            self.assertEqual(result[0].status, "failed")
            self.assertEqual(store.get_operation(operation.operation_id).state, "failed")

    def test_prepared_resume_settles_without_call_if_job_is_already_running(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = AutoscalerStateStore(Path(raw_dir) / "autoscaler.sqlite")
            operation = store.prepare_operation(
                intent_key="sandbox:job-1:started",
                kind="resume",
                deployment_id="prod-a",
                role="sandbox",
                request={"type": "bulk", "items": [{"id": "job-1"}]},
                target_job_ids=("job-1",),
                now=NOW,
            )

            result = store.reconcile_resume_operations(
                {"job-1": "RUNNING"}, now=NOW + timedelta(seconds=1)
            )

            self.assertEqual(result[0].status, "recovered")
            self.assertEqual(store.get_operation(operation.operation_id).state, "settled")

    def test_slot_incarnation_does_not_scan_or_reuse_terminal_operation(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = AutoscalerStateStore(Path(raw_dir) / "autoscaler.sqlite")
            first_key = store.allocate_operation_intent_key(
                deployment_id="prod-a", kind="create", base_key="sandbox:seed"
            )
            operation = store.prepare_operation(
                intent_key=first_key,
                kind="create",
                deployment_id="prod-a",
                request=create_request(first_key),
            )
            store.begin_provider_call(operation.operation_id)
            store.mark_operation_accepted(
                operation.operation_id,
                response={"responses": [{"id": "job-1"}]},
                target_job_ids=("job-1",),
            )
            store.confirm_visible_creates(["job-1"])

            second_key = store.allocate_operation_intent_key(
                deployment_id="prod-a", kind="create", base_key="sandbox:seed"
            )
            self.assertEqual(second_key, "sandbox:seed#2")
            self.assertEqual(store.compact_terminal_history(keep=0), 1)
            self.assertEqual(store.list_operations(), [])


if __name__ == "__main__":
    unittest.main()
