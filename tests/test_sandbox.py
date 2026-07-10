from pathlib import Path
from dataclasses import replace
from datetime import timedelta
from tempfile import TemporaryDirectory
from threading import Barrier, Lock, Thread
import hashlib
import json
import multiprocessing
import sys
import time
import unittest

from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.sandbox import (
    CommandResult,
    DockerGvisorRuntime,
    RecordingExecutor,
    SandboxFileTooLargeError,
    SandboxCapacityUnavailableError,
    SandboxConflictError,
    SandboxAdmissionClosedError,
    SandboxFilesystemSpec,
    SandboxManager,
    SandboxOperation,
    SandboxSecuritySpec,
    SandboxSpec,
    SandboxStore,
    SandboxStaleOperationError,
    SANDBOX_GENERATION_LABEL,
    SANDBOX_OPERATION_ID_LABEL,
    SANDBOX_SPEC_HASH_LABEL,
    sandbox_spec_fingerprint,
)


class SandboxRuntimeTests(unittest.TestCase):
    def test_drain_persists_replays_and_requires_matching_undrain_token(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            manager = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(dry_run=True),
            )

            drained = manager.configure_drain(
                "drain-1",
                True,
                active_build_count=lambda: 0,
            )
            replay = manager.configure_drain(
                "drain-1",
                True,
                active_build_count=lambda: 0,
            )

            self.assertTrue(drained.ready)
            self.assertFalse(drained.drain.admission_open)
            self.assertEqual(replay.activity.activity_revision, drained.activity.activity_revision)
            persisted = SandboxStore(path).load_state().drain
            self.assertTrue(persisted.draining)
            self.assertEqual(persisted.token, "drain-1")
            with self.assertRaises(SandboxConflictError):
                manager.configure_drain(
                    "other-drain",
                    True,
                    active_build_count=lambda: 0,
                )
            with self.assertRaises(SandboxAdmissionClosedError):
                manager.create(
                    SandboxSpec(id="blocked", image="busybox", memory_mb=128)
                )
            with self.assertRaises(SandboxConflictError):
                manager.configure_drain(
                    "other-drain",
                    False,
                    active_build_count=lambda: 0,
                )

            opened = manager.configure_drain(
                "drain-1",
                False,
                active_build_count=lambda: 0,
            )
            opened_replay = manager.configure_drain(
                "drain-1",
                False,
                active_build_count=lambda: 0,
            )
            self.assertTrue(opened.drain.admission_open)
            self.assertFalse(opened.drain.draining)
            self.assertEqual(
                opened_replay.activity.activity_revision,
                opened.activity.activity_revision,
            )
            record, _result = manager.create(
                SandboxSpec(id="accepted", image="busybox", memory_mb=128)
            )
            self.assertEqual(record.spec.id, "accepted")

    def test_drain_waits_for_existing_work_then_reacknowledges_revision(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                DockerGvisorRuntime(dry_run=True),
            )
            spec = SandboxSpec(id="existing", image="busybox", memory_mb=128)
            existing, _result = manager.create(spec)

            draining = manager.configure_drain(
                "drain-existing",
                True,
                active_build_count=lambda: 0,
            )

            self.assertFalse(draining.ready)
            self.assertEqual(draining.drain.drain_activity_epoch, 0)
            replay, _result = manager.create(spec)
            self.assertEqual(replay, existing)
            with self.assertRaises(SandboxAdmissionClosedError):
                manager.create(
                    SandboxSpec(id="new", image="busybox", memory_mb=128)
                )
            manager.delete(spec.id)
            ready = manager.heartbeat_snapshot(active_build_count=lambda: 0)
            self.assertTrue(ready.ready)
            self.assertEqual(
                ready.drain.drain_activity_epoch,
                ready.activity.activity_revision,
            )

    def test_multiprocess_create_cannot_enter_after_drain_ack(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            manager = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(dry_run=True),
            )
            drained = manager.configure_drain(
                "drain-process",
                True,
                active_build_count=lambda: 0,
            )
            self.assertTrue(drained.ready)
            context = multiprocessing.get_context("spawn")
            results = context.Queue()
            processes = [
                context.Process(
                    target=_multiprocess_create_after_drain,
                    args=(str(path), index, results),
                )
                for index in range(4)
            ]
            for process in processes:
                process.start()
            outcomes = [results.get(timeout=10) for _process in processes]
            for process in processes:
                process.join(timeout=10)

            self.assertEqual([process.exitcode for process in processes], [0] * 4)
            self.assertEqual(outcomes, ["closed"] * 4)
            self.assertEqual(SandboxStore(path).load(), {})
    def test_runtime_command_carries_operation_identity_labels(self) -> None:
        spec = SandboxSpec(id="versioned", image="busybox", memory_mb=128)
        operation = _create_operation(spec, generation=7, operation_id="create-7")

        argv = DockerGvisorRuntime(dry_run=True).create_command(
            spec,
            operation=operation,
        )

        self.assertIn(f"{SANDBOX_GENERATION_LABEL}=7", argv)
        self.assertIn(f"{SANDBOX_OPERATION_ID_LABEL}=create-7", argv)
        self.assertIn(f"{SANDBOX_SPEC_HASH_LABEL}={operation.spec_hash}", argv)

    def test_generation_replay_conflict_and_tombstone_fencing(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))
            spec = SandboxSpec(id="versioned", image="busybox", memory_mb=128)
            create_one = _create_operation(spec, 1, "create-1")

            first, _result = manager.create(spec, operation=create_one)
            replay, _result = manager.create(spec, operation=create_one)

            self.assertEqual(first.operation_id, "create-1")
            self.assertEqual(first.generation, 1)
            self.assertEqual(replay, first)
            with self.assertRaises(SandboxConflictError):
                manager.create(
                    spec,
                    operation=_create_operation(spec, 1, "other-create"),
                )
            changed_spec = replace(spec, image="alpine")
            with self.assertRaises(SandboxConflictError):
                manager.create(
                    changed_spec,
                    operation=_create_operation(changed_spec, 1, "create-1"),
                )
            with self.assertRaises(SandboxStaleOperationError):
                manager.create(
                    spec,
                    operation=_create_operation(spec, 0, "stale-create"),
                )
            with self.assertRaises(SandboxConflictError):
                manager.create(
                    spec,
                    operation=_create_operation(spec, 2, "create-2"),
                )

            deleted, _result = manager.delete(
                spec.id,
                generation=1,
                operation_id="delete-1",
            )
            replay_delete, _result = manager.delete(
                spec.id,
                generation=1,
                operation_id="delete-1",
            )

            self.assertIsNotNone(deleted)
            assert deleted is not None
            self.assertEqual(deleted.spec, first.spec)
            self.assertEqual(deleted.generation, first.generation)
            self.assertEqual(deleted.operation_id, first.operation_id)
            self.assertEqual(deleted.state, "deleting")
            self.assertEqual(deleted.delete_operation_id, "delete-1")
            self.assertIsNone(replay_delete)
            state = store.load_state()
            self.assertEqual(state.records, {})
            self.assertEqual(state.tombstones[spec.id].generation, 1)
            self.assertEqual(state.tombstones[spec.id].operation_id, "delete-1")
            with self.assertRaises(SandboxStaleOperationError):
                manager.create(spec, operation=create_one)
            with self.assertRaises(SandboxConflictError):
                manager.delete(
                    spec.id,
                    generation=1,
                    operation_id="different-delete",
                )
            with self.assertRaises(SandboxStaleOperationError):
                manager.create(spec)
            with self.assertRaises(SandboxStaleOperationError):
                manager.delete(spec.id)

            create_two = _create_operation(spec, 2, "create-2")
            second, _result = manager.create(spec, operation=create_two)
            self.assertEqual(second.generation, 2)
            delayed_delete, _result = manager.delete(
                spec.id,
                generation=1,
                operation_id="delete-1",
            )
            self.assertIsNone(delayed_delete)
            self.assertEqual(store.load()[spec.id].generation, 2)
            with self.assertRaises(SandboxConflictError):
                manager.delete(
                    spec.id,
                    generation=3,
                    operation_id="delete-3",
                )
            self.assertEqual(store.load()[spec.id].generation, 2)

    def test_delete_of_absent_generation_persists_fence(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))
            spec = SandboxSpec(id="canceled", image="busybox", memory_mb=128)

            deleted, result = manager.delete(
                spec.id,
                generation=4,
                operation_id="cancel-4",
            )

            self.assertIsNone(deleted)
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(store.load_state().tombstones[spec.id].generation, 4)
            with self.assertRaises(SandboxStaleOperationError):
                manager.create(
                    spec,
                    operation=_create_operation(spec, 4, "create-4"),
                )

    def test_ttl_tombstone_accepts_later_explicit_delete_replay(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))
            spec = SandboxSpec(
                id="expired",
                image="busybox",
                memory_mb=128,
                ttl_seconds=1,
            )
            record, _result = manager.create(
                spec,
                operation=_create_operation(spec, 5, "create-5"),
            )
            manager.cleanup_expired(now=record.created_at + timedelta(seconds=2))

            ttl_tombstone = store.load_state().tombstones[spec.id]
            self.assertEqual(ttl_tombstone.operation_id, "ttl:create-5")
            absent, _result = manager.delete(
                spec.id,
                generation=5,
                operation_id="delete-5",
            )
            replay, _result = manager.delete(
                spec.id,
                generation=5,
                operation_id="delete-5",
            )

            self.assertIsNone(absent)
            self.assertIsNone(replay)
            self.assertEqual(
                store.load_state().tombstones[spec.id].operation_id,
                "delete-5",
            )
            with self.assertRaises(SandboxConflictError):
                manager.delete(
                    spec.id,
                    generation=5,
                    operation_id="other-delete-5",
                )

    def test_legacy_delete_is_replayable_but_fences_id_reuse(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))
            spec = SandboxSpec(id="legacy", image="busybox", memory_mb=128)
            manager.create(spec)

            deleted, _result = manager.delete(spec.id)
            replay, _result = manager.delete(spec.id)

            self.assertIsNotNone(deleted)
            self.assertIsNone(replay)
            self.assertEqual(store.load_state().tombstones[spec.id].generation, 0)
            with self.assertRaises(SandboxStaleOperationError):
                manager.create(spec)

    def test_create_retry_recovers_runtime_side_effect_from_labels(self) -> None:
        with TemporaryDirectory() as raw_dir:
            spec = SandboxSpec(id="recovered", image="busybox", memory_mb=128)
            operation = _create_operation(spec, 3, "create-3")
            executor = CrashRecoveryExecutor(spec.id, operation)
            runtime = DockerGvisorRuntime(executor=executor)
            runtime.create(spec, operation=operation)
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                runtime,
            )

            record, result = manager.create(spec, operation=operation)

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(record.generation, 3)
            self.assertEqual(record.operation_id, "create-3")
            self.assertEqual(record.spec_hash, operation.spec_hash)
            self.assertEqual(manager.store.load()[spec.id], record)

    def test_create_persists_intent_before_ambiguous_runtime_failure(self) -> None:
        class AmbiguousCreateExecutor:
            def __init__(self, spec: SandboxSpec, operation: SandboxOperation) -> None:
                self.spec = spec
                self.operation = operation
                self.created = False
                self.create_calls = 0

            def run(self, argv, *, input=None):
                del input
                if len(argv) > 1 and argv[1] == "run":
                    self.create_calls += 1
                    self.created = True
                    return CommandResult(
                        argv=argv,
                        exit_code=1,
                        stderr="docker response was lost after create",
                    )
                if len(argv) > 1 and argv[1] == "inspect" and self.created:
                    return CommandResult(
                        argv=argv,
                        exit_code=0,
                        stdout=json.dumps(
                            {
                                "ucloud-sandboxes.managed": "true",
                                "ucloud-sandboxes.sandbox-id": self.spec.id,
                                SANDBOX_GENERATION_LABEL: str(self.operation.generation),
                                SANDBOX_OPERATION_ID_LABEL: self.operation.operation_id,
                                SANDBOX_SPEC_HASH_LABEL: self.operation.spec_hash,
                            }
                        ),
                    )
                return CommandResult(
                    argv=argv,
                    exit_code=1,
                    stderr="No such container",
                )

        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            spec = SandboxSpec(id="ambiguous", image="busybox", memory_mb=128)
            operation = _create_operation(spec, 7, "create-7")
            executor = AmbiguousCreateExecutor(spec, operation)
            manager = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(executor=executor),
            )

            with self.assertRaisesRegex(RuntimeError, "response was lost"):
                manager.create(spec, operation=operation)

            intent = SandboxStore(path).load()[spec.id]
            self.assertEqual(intent.state, "planned")
            self.assertEqual(intent.operation_id, operation.operation_id)
            restarted = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(executor=executor),
            )
            recovered, result, timings = restarted.create_with_timings(
                spec,
                operation=operation,
            )

            self.assertEqual(recovered.state, "running")
            self.assertEqual(result.argv, ())
            self.assertTrue(timings["idempotent"])
            self.assertEqual(timings["recovered"], "container")
            self.assertEqual(executor.create_calls, 1)

    def test_create_replay_resumes_planned_intent_when_runtime_is_absent(self) -> None:
        class FailBeforeCreateExecutor:
            def __init__(self) -> None:
                self.create_calls = 0

            def run(self, argv, *, input=None):
                del input
                if len(argv) > 1 and argv[1] == "inspect":
                    return CommandResult(
                        argv=argv,
                        exit_code=1,
                        stderr="No such container",
                    )
                if len(argv) > 1 and argv[1] == "run":
                    self.create_calls += 1
                    if self.create_calls == 1:
                        return CommandResult(
                            argv=argv,
                            exit_code=1,
                            stderr="daemon unavailable before create",
                        )
                    return CommandResult(argv=argv, exit_code=0)
                return CommandResult(argv=argv, exit_code=0)

        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            spec = SandboxSpec(id="resumed", image="busybox", memory_mb=128)
            operation = _create_operation(spec, 8, "create-8")
            executor = FailBeforeCreateExecutor()
            manager = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(executor=executor),
            )
            with self.assertRaisesRegex(RuntimeError, "daemon unavailable"):
                manager.create(spec, operation=operation)
            self.assertEqual(SandboxStore(path).load()[spec.id].state, "planned")

            restarted = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(executor=executor),
            )
            recovered, _result, timings = restarted.create_with_timings(
                spec,
                operation=operation,
            )

            self.assertEqual(recovered.state, "running")
            self.assertTrue(timings["idempotent"])
            self.assertEqual(executor.create_calls, 2)

    def test_delete_replay_completes_durable_intent_after_runtime_crash_window(self) -> None:
        class FailSecondSaveStore(SandboxStore):
            def __init__(self, path: Path) -> None:
                super().__init__(path)
                self.calls = 0

            def save_state(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 2:
                    raise RuntimeError("crash after runtime delete")
                return super().save_state(*args, **kwargs)

        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            executor = RecordingExecutor()
            runtime = DockerGvisorRuntime(executor=executor)
            spec = SandboxSpec(id="delete-crash", image="busybox", memory_mb=128)
            create_operation = _create_operation(spec, 4, "create-4")
            SandboxManager(SandboxStore(path), runtime).create(
                spec,
                operation=create_operation,
            )

            crashing = SandboxManager(FailSecondSaveStore(path), runtime)
            with self.assertRaisesRegex(RuntimeError, "crash after runtime delete"):
                crashing.delete(
                    spec.id,
                    generation=4,
                    operation_id="delete-4",
                )

            deleting = SandboxStore(path).load()[spec.id]
            self.assertEqual(deleting.state, "deleting")
            self.assertEqual(deleting.delete_operation_id, "delete-4")
            restarted = SandboxManager(SandboxStore(path), runtime)
            with self.assertRaisesRegex(SandboxConflictError, "being deleted"):
                restarted.create(spec, operation=create_operation)

            deleted, _result = restarted.delete(
                spec.id,
                generation=4,
                operation_id="delete-4",
            )
            final_state = SandboxStore(path).load_state()

            self.assertIsNotNone(deleted)
            self.assertNotIn(spec.id, final_state.records)
            self.assertEqual(final_state.tombstones[spec.id].generation, 4)
            self.assertEqual(
                final_state.tombstones[spec.id].operation_id,
                "delete-4",
            )

    def test_multiprocess_delayed_create_cannot_cross_tombstone(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            manager = SandboxManager(
                SandboxStore(path),
                DockerGvisorRuntime(dry_run=True),
            )
            spec = SandboxSpec(id="raced", image="busybox", memory_mb=128)
            manager.create(spec, operation=_create_operation(spec, 1, "create-1"))
            manager.delete(spec.id, generation=1, operation_id="delete-1")
            context = multiprocessing.get_context("spawn")
            start = context.Event()
            results = context.Queue()
            generations = [1, 1, 1, 2]
            processes = [
                context.Process(
                    target=_multiprocess_versioned_create,
                    args=(
                        str(path),
                        generation,
                        f"create-{generation}-{index}",
                        start,
                        results,
                    ),
                )
                for index, generation in enumerate(generations)
            ]
            for process in processes:
                process.start()
            start.set()
            outcomes = [results.get(timeout=10) for _process in processes]
            for process in processes:
                process.join(timeout=10)

            self.assertEqual([process.exitcode for process in processes], [0] * 4)
            self.assertEqual(outcomes.count("created:2"), 1)
            self.assertEqual(outcomes.count("stale:1"), 3)
            record = SandboxStore(path).load()[spec.id]
            self.assertEqual(record.generation, 2)
    def test_delete_treats_missing_container_as_idempotent_success(self) -> None:
        executor = RecordingExecutor(
            exit_code=1,
            stderr="Error response from daemon: No such container: missing",
        )
        runtime = DockerGvisorRuntime(executor=executor)

        result = runtime.delete("missing")

        self.assertEqual(result.exit_code, 0)

    def test_delete_surfaces_transient_runtime_failure(self) -> None:
        executor = RecordingExecutor(exit_code=1, stderr="daemon unavailable")
        runtime = DockerGvisorRuntime(executor=executor)

        with self.assertRaisesRegex(RuntimeError, "daemon unavailable"):
            runtime.delete("sandbox")

    def test_activity_snapshot_uses_one_store_load(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = CountingSandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))
            manager.create(SandboxSpec(id="one", image="busybox", memory_mb=128))
            store.load_count = 0

            snapshot = manager.activity_snapshot()

            self.assertEqual(store.load_count, 1)
            self.assertEqual(snapshot.used_resources.memory_mb, 0)
            self.assertEqual(snapshot.reserved_resources.memory_mb, 128)
            self.assertEqual([record.spec.id for record in snapshot.records], ["one"])
            self.assertEqual(snapshot.activity_revision, 1)

    def test_capacity_admission_counts_planned_records_and_preserves_replay(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            manager = SandboxManager(
                store,
                DockerGvisorRuntime(dry_run=True),
                effective_capacity=ResourceQuantity(memory_mb=128),
            )
            spec = SandboxSpec(id="fills-node", image="busybox", memory_mb=128)

            created, _result = manager.create(spec)
            replayed, _result = manager.create(spec)
            with self.assertRaisesRegex(
                SandboxCapacityUnavailableError,
                "exhausted memory_mb",
            ):
                manager.create(
                    SandboxSpec(id="over-capacity", image="busybox", memory_mb=1)
                )

            self.assertEqual(replayed, created)
            records, revision = store.load_with_revision()
            self.assertEqual(list(records), ["fills-node"])
            self.assertEqual(revision, 1)

    def test_zero_capacity_dimensions_remain_unbounded(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True),
                effective_capacity=ResourceQuantity(),
            )

            manager.create(
                SandboxSpec(
                    id="legacy-unbounded",
                    image="busybox",
                    cpus=32.0,
                    memory_mb=1_000_000,
                    disk_mb=1_000_000,
                )
            )

    def test_store_revision_is_persisted_and_monotonic(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            store = SandboxStore(path)
            manager = SandboxManager(store, DockerGvisorRuntime(dry_run=True))

            self.assertEqual(store.load_with_revision(), ({}, 0))
            manager.create(SandboxSpec(id="one", image="busybox", memory_mb=128))
            _records, first_revision = store.load_with_revision()
            store.delete("missing")
            records, second_revision = store.load_with_revision()

            self.assertEqual(list(records), ["one"])
            self.assertEqual(first_revision, 1)
            self.assertEqual(second_revision, 2)
            self.assertEqual(json.loads(path.read_text())["revision"], 2)

    def test_multiprocess_creates_do_not_lose_updates_or_ssh_ports(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            context = multiprocessing.get_context("spawn")
            process_count = 3
            records_per_process = 5
            processes = [
                context.Process(
                    target=_multiprocess_sandbox_writer,
                    args=(str(path), worker, records_per_process),
                )
                for worker in range(process_count)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=15)

            self.assertEqual([process.exitcode for process in processes], [0, 0, 0])
            records, revision = SandboxStore(path).load_with_revision()
            ports = [record.spec.ssh.host_port for record in records.values()]
            expected = process_count * records_per_process
            self.assertEqual(len(records), expected)
            self.assertEqual(revision, expected)
            self.assertEqual(len(set(ports)), expected)
            self.assertEqual(
                list(path.parent.glob(f".{path.name}.*.tmp")),
                [],
            )

    def test_multiprocess_capacity_admission_allows_only_one_winner(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "sandboxes.json"
            context = multiprocessing.get_context("spawn")
            start = context.Event()
            results = context.Queue()
            processes = [
                context.Process(
                    target=_multiprocess_capacity_create,
                    args=(str(path), index, start, results),
                )
                for index in range(2)
            ]
            for process in processes:
                process.start()
            start.set()
            for process in processes:
                process.join(timeout=15)

            self.assertEqual([process.exitcode for process in processes], [0, 0])
            outcomes = sorted(results.get(timeout=2) for _process in processes)
            self.assertEqual(outcomes, ["capacity", "created"])
            records, revision = SandboxStore(path).load_with_revision()
            self.assertEqual(len(records), 1)
            self.assertEqual(revision, 1)

    def test_concurrent_ssh_creates_allocate_distinct_ports(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manager = SandboxManager(
                SandboxStore(Path(raw_dir) / "sandboxes.json"),
                SlowCreateRuntime(),
                ssh_port_range=(23000, 23001),
            )
            barrier = Barrier(2)
            results: list[int] = []
            errors: list[BaseException] = []
            result_lock = Lock()

            def create(sandbox_id: str) -> None:
                try:
                    barrier.wait()
                    record, _result = manager.create(
                        SandboxSpec.from_dict(
                            {
                                "id": sandbox_id,
                                "image": "sandbox-ssh:latest",
                                "memory_mb": 128,
                                "network": "bridge",
                                "ssh": True,
                            }
                        )
                    )
                    assert record.spec.ssh.host_port is not None
                    with result_lock:
                        results.append(record.spec.ssh.host_port)
                except BaseException as exc:
                    with result_lock:
                        errors.append(exc)

            threads = [Thread(target=create, args=(f"ssh-{index}",)) for index in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=2)

            self.assertEqual(errors, [])
            self.assertEqual(sorted(results), [23000, 23001])

    def test_builds_docker_gvisor_run_command(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True)
        spec = SandboxSpec(
            id="abc-123",
            image="python:3.12-slim",
            command=("python", "-c", "print('ok')"),
            env={"B": "2", "A": "1"},
            memory_mb=512,
            cpus=1.5,
            disk_mb=2048,
            labels={"purpose": "test"},
        )

        argv = runtime.create_command(spec)

        self.assertEqual(
            argv[:7],
            (
                "docker",
                "run",
                "-d",
                "--name",
                "ucloud-sandbox-abc-123",
                "--runtime",
                "runsc",
            ),
        )
        self.assertIn("--network", argv)
        self.assertIn("none", argv)
        self.assertIn("--memory", argv)
        self.assertIn("512m", argv)
        self.assertIn("--cpus", argv)
        self.assertIn("1.5", argv)
        self.assertIn("--storage-opt", argv)
        self.assertIn("size=2048m", argv)
        self.assertIn("--init", argv)
        self.assertIn("--user", argv)
        self.assertIn("1000:1000", argv)
        self.assertIn("--security-opt", argv)
        self.assertIn("no-new-privileges", argv)
        self.assertIn("--cap-drop", argv)
        self.assertIn("ALL", argv)
        self.assertIn("--pids-limit", argv)
        self.assertIn("256", argv)
        self.assertIn("--tmpfs", argv)
        self.assertIn("/tmp:rw,nosuid,nodev,size=64m", argv)
        self.assertIn("/run:rw,nosuid,nodev,size=16m", argv)
        self.assertIn("-e", argv)
        self.assertIn("A=1", argv)
        self.assertIn("B=2", argv)
        self.assertEqual(argv[-4:], ("python:3.12-slim", "python", "-c", "print('ok')"))

    def test_disk_request_requires_validated_storage_quota_support(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)
        spec = SandboxSpec(
            id="disk",
            image="busybox",
            disk_mb=2048,
        )

        with self.assertRaises(ValueError):
            runtime.create_command(spec)

    def test_tmpfs_workspace_requires_validated_runtime_support(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)
        spec = SandboxSpec(
            id="tmpfs",
            image="busybox",
            disk_mb=2048,
            filesystem=SandboxFilesystemSpec(enforce_disk_quota=True),
        )

        with self.assertRaises(ValueError):
            runtime.create_command(spec)

    def test_can_request_tmpfs_workspace_on_validated_runtime(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True, allow_tmpfs_workspace=True)
        spec = SandboxSpec(
            id="tmpfs",
            image="busybox",
            disk_mb=2048,
            filesystem=SandboxFilesystemSpec(enforce_disk_quota=True),
        )

        argv = runtime.create_command(spec)

        self.assertNotIn("--storage-opt", argv)
        self.assertIn("--read-only", argv)
        self.assertIn("--tmpfs", argv)
        self.assertIn("/workspace:rw,nosuid,nodev,size=2048m", argv)
        self.assertIn("/tmp:rw,nosuid,nodev,size=64m", argv)
        self.assertIn("/run:rw,nosuid,nodev,size=16m", argv)
        self.assertIn("--workdir", argv)
        self.assertIn("/workspace", argv)

    def test_compatibility_security_profile_can_opt_out_of_hardening(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)
        spec = SandboxSpec(
            id="compat",
            image="busybox",
            memory_mb=128,
            security=SandboxSecuritySpec(
                user=None,
                cap_drop=(),
                no_new_privileges=False,
                pids_limit=None,
                init=False,
            ),
        )

        argv = runtime.create_command(spec)

        self.assertNotIn("--user", argv)
        self.assertNotIn("--security-opt", argv)
        self.assertNotIn("--cap-drop", argv)
        self.assertNotIn("--pids-limit", argv)
        self.assertNotIn("--init", argv)

    def test_linux_host_profile_uses_vm_like_entrypoint_and_defaults(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True)
        spec = SandboxSpec.from_dict(
            {
                "id": "linux-host",
                "image": "ubuntu:24.04",
                "memory_mb": 512,
                "disk_mb": 2048,
                "profile": "linux_host",
                "network": "bridge",
                "command": ["sleep", "infinity"],
                "ssh": {
                    "enabled": True,
                    "host_port": 23000,
                    "authorized_keys": ["ssh-ed25519 AAAA test"],
                },
                "linux_host": {"enable_cron": True},
            }
        )

        argv = runtime.create_command(spec)

        self.assertIsNone(spec.security.user)
        self.assertEqual(spec.security.cap_drop, ())
        self.assertFalse(spec.security.no_new_privileges)
        self.assertIsNone(spec.security.pids_limit)
        self.assertIn("--init", argv)
        self.assertNotIn("--user", argv)
        self.assertNotIn("--cap-drop", argv)
        self.assertNotIn("--security-opt", argv)
        self.assertNotIn("--pids-limit", argv)
        self.assertIn("UCLOUD_SANDBOX_PROFILE=linux_host", argv)
        self.assertIn("UCLOUD_SANDBOX_ENABLE_CRON=1", argv)
        self.assertIn("UCLOUD_SANDBOX_ENABLE_SSHD=1", argv)
        self.assertIn("UCLOUD_SANDBOX_SSH_PORT=22", argv)
        paths_env = next(
            item
            for item in argv
            if item.startswith("UCLOUD_SANDBOX_LINUX_HOST_PATHS=")
        )
        self.assertIn("/var/spool/cron", paths_env)
        self.assertIn("--entrypoint", argv)
        self.assertIn("/bin/sh", argv)
        image_index = argv.index("ubuntu:24.04")
        self.assertEqual(argv[image_index + 1], "-lc")
        script = argv[image_index + 2]
        self.assertIn("/usr/local/bin/service", script)
        self.assertIn("ssh-keygen -A", script)
        self.assertEqual(argv[-2:], ("sleep", "infinity"))

    def test_linux_host_profile_round_trips_from_dict(self) -> None:
        spec = SandboxSpec.from_dict(
            {
                "id": "linux-host",
                "image": "ubuntu:24.04",
                "memory_mb": 512,
                "profile": "linux_host",
                "linux_host": {
                    "enable_cron": True,
                    "enable_sshd": True,
                    "keep_alive": False,
                    "writable_paths": ["/tests", "/logs/verifier"],
                },
            }
        )

        raw = spec.to_dict()
        round_tripped = SandboxSpec.from_dict(raw)

        self.assertEqual(raw["profile"], "linux_host")
        self.assertEqual(raw["linux_host"]["writable_paths"], ["/tests", "/logs/verifier"])
        self.assertTrue(round_tripped.linux_host.enable_cron)
        self.assertTrue(round_tripped.linux_host.enable_sshd)
        self.assertFalse(round_tripped.linux_host.keep_alive)
        self.assertEqual(
            round_tripped.linux_host.writable_paths,
            ("/tests", "/logs/verifier"),
        )

    def test_rejects_unknown_sandbox_profile(self) -> None:
        spec = SandboxSpec(
            id="bad-profile",
            image="busybox",
            profile="vm",
            memory_mb=128,
        )

        with self.assertRaisesRegex(ValueError, "profile must be one of"):
            spec.validate()

    def test_rejects_invalid_sandbox_id(self) -> None:
        with self.assertRaises(ValueError):
            SandboxSpec(id="../bad", image="busybox").validate()

    def test_rejects_missing_resource_request(self) -> None:
        with self.assertRaisesRegex(ValueError, "resources are required"):
            SandboxSpec(id="no-resources", image="busybox").validate()

    def test_manager_records_planned_sandbox_in_dry_run_mode(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            executor = RecordingExecutor()
            runtime = DockerGvisorRuntime(executor=executor, dry_run=True)
            manager = SandboxManager(store, runtime)
            spec = SandboxSpec(
                id="one",
                image="busybox",
                command=("true",),
                memory_mb=128,
            )

            record, result = manager.create(spec)

            self.assertEqual(record.state, "planned")
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(executor.commands, [])
            self.assertEqual(len(manager.list()), 1)

    def test_manager_create_is_idempotent_for_same_spec(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            executor = RecordingExecutor()
            runtime = DockerGvisorRuntime(executor=executor, allow_storage_opt_quota=True)
            manager = SandboxManager(store, runtime)
            spec = SandboxSpec(
                id="same",
                image="busybox",
                cpus=1.0,
                memory_mb=128,
                disk_mb=512,
                labels={"sample": "one"},
            )

            first, _first_result = manager.create(spec)
            second, second_result, timings = manager.create_with_timings(spec)

            self.assertEqual(first.spec.id, second.spec.id)
            self.assertEqual(second_result.argv, ())
            self.assertTrue(timings["idempotent"])
            self.assertEqual(timings["recovered"], "store")
            self.assertEqual(len(executor.commands), 1)

    def test_manager_create_conflicts_for_same_id_different_spec(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True)
            manager = SandboxManager(store, runtime)
            manager.create(
                SandboxSpec(
                    id="same",
                    image="busybox",
                    cpus=1.0,
                    memory_mb=128,
                    disk_mb=512,
                )
            )

            with self.assertRaises(SandboxConflictError):
                manager.create(
                    SandboxSpec(
                        id="same",
                        image="python:3.12-slim",
                        cpus=1.0,
                        memory_mb=128,
                        disk_mb=512,
                    )
                )

    def test_manager_recovers_managed_container_after_conflict_without_store_record(self) -> None:
        class ConflictExecutor:
            def __init__(self, spec: SandboxSpec) -> None:
                self.spec = spec
                self.commands = []

            def run(self, argv, *, input=None):
                self.commands.append(argv)
                if len(argv) > 1 and argv[1] == "run":
                    return CommandResult(
                        argv=argv,
                        exit_code=1,
                        stderr=(
                            "Conflict. The container name "
                            "\"/ucloud-sandbox-recovered\" is already in use"
                        ),
                    )
                labels = {
                    "ucloud-sandboxes.managed": "true",
                    "ucloud-sandboxes.sandbox-id": self.spec.id,
                    "ucloud-sandboxes.spec-sha256": sandbox_spec_fingerprint(self.spec),
                }
                return CommandResult(
                    argv=argv,
                    exit_code=0,
                    stdout=__import__("json").dumps(labels),
                )

        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            spec = SandboxSpec(
                id="recovered",
                image="busybox",
                cpus=1.0,
                memory_mb=128,
                disk_mb=512,
            )
            executor = ConflictExecutor(spec)
            runtime = DockerGvisorRuntime(executor=executor, allow_storage_opt_quota=True)
            manager = SandboxManager(store, runtime)

            record, result, timings = manager.create_with_timings(spec)

            self.assertEqual(record.spec.id, "recovered")
            self.assertEqual(result.argv, ())
            self.assertTrue(timings["idempotent"])
            self.assertEqual(timings["recovered"], "container")
            self.assertEqual(store.load()["recovered"].spec.id, "recovered")

    def test_manager_recovers_container_with_legacy_default_profile_fingerprint(
        self,
    ) -> None:
        class LegacyFingerprintConflictExecutor:
            def __init__(self, spec: SandboxSpec) -> None:
                raw = spec.to_dict()
                raw.pop("profile", None)
                raw.pop("linux_host", None)
                self.legacy_fingerprint = hashlib.sha256(
                    json.dumps(raw, sort_keys=True, separators=(",", ":")).encode(
                        "utf-8"
                    )
                ).hexdigest()
                self.commands = []

            def run(self, argv, *, input=None):
                self.commands.append(argv)
                if len(argv) > 1 and argv[1] == "run":
                    return CommandResult(
                        argv=argv,
                        exit_code=1,
                        stderr=(
                            "Conflict. The container name "
                            "\"/ucloud-sandbox-legacy\" is already in use"
                        ),
                    )
                labels = {
                    "ucloud-sandboxes.managed": "true",
                    "ucloud-sandboxes.sandbox-id": "legacy",
                    "ucloud-sandboxes.spec-sha256": self.legacy_fingerprint,
                }
                return CommandResult(
                    argv=argv,
                    exit_code=0,
                    stdout=json.dumps(labels),
                )

        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            spec = SandboxSpec(
                id="legacy",
                image="busybox",
                cpus=1.0,
                memory_mb=128,
                disk_mb=512,
            )
            executor = LegacyFingerprintConflictExecutor(spec)
            runtime = DockerGvisorRuntime(executor=executor, allow_storage_opt_quota=True)
            manager = SandboxManager(store, runtime)

            record, _result, timings = manager.create_with_timings(spec)

        self.assertEqual(record.spec.id, "legacy")
        self.assertTrue(timings["idempotent"])
        self.assertEqual(timings["recovered"], "container")

    def test_manager_sums_requested_resources(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True)
            manager = SandboxManager(store, runtime)
            manager.create(
                SandboxSpec(
                    id="one",
                    image="busybox",
                    cpus=0.5,
                    memory_mb=256,
                    disk_mb=1024,
                )
            )
            manager.create(
                SandboxSpec(
                    id="two",
                    image="busybox",
                    cpus=1.0,
                    memory_mb=512,
                    disk_mb=2048,
                )
            )

            resources = manager.requested_resources()

            self.assertEqual(resources.vcpu, 1.5)
            self.assertEqual(resources.memory_mb, 768)
            self.assertEqual(resources.disk_mb, 3072)

    def test_manager_cleans_up_expired_sandboxes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            executor = RecordingExecutor()
            runtime = DockerGvisorRuntime(executor=executor, dry_run=True)
            manager = SandboxManager(store, runtime)
            manager.create(
                SandboxSpec(
                    id="short",
                    image="busybox",
                    ttl_seconds=1,
                    memory_mb=128,
                )
            )

            expired = manager.cleanup_expired()

            self.assertEqual(expired, [])
            records = store.load()
            record = records["short"]
            expired = manager.cleanup_expired(
                now=record.created_at.replace(microsecond=0)
            )
            self.assertEqual(expired, [])
            expired = manager.cleanup_expired(
                now=record.created_at.replace(microsecond=0) + timedelta(seconds=2)
            )

            self.assertEqual([record.spec.id for record in expired], ["short"])
            self.assertEqual(store.load(), {})

    def test_ssh_enabled_sandbox_gets_port_and_publish_flag(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(dry_run=True)
            manager = SandboxManager(
                store,
                runtime,
                ssh_port_range=(23000, 23001),
            )
            spec = SandboxSpec.from_dict(
                {
                    "id": "ssh-one",
                    "image": "sandbox-ssh:latest",
                    "memory_mb": 128,
                    "network": "bridge",
                    "ssh": {
                        "enabled": True,
                        "user": "sandbox",
                        "authorized_keys": ["ssh-ed25519 AAAA test"],
                    },
                }
            )

            record, result = manager.create(spec)

            self.assertEqual(record.spec.ssh.host_port, 23000)
            self.assertIn("-p", result.argv)
            self.assertIn("127.0.0.1:23000:22", result.argv)
            self.assertEqual(
                record.to_dict()["ssh"]["command"],
                "ssh -p 23000 sandbox@127.0.0.1",
            )

    def test_ssh_requires_bridge_network(self) -> None:
        spec = SandboxSpec.from_dict(
            {
                "id": "bad-ssh",
                "image": "sandbox-ssh:latest",
                "memory_mb": 128,
                "ssh": {"enabled": True, "host_port": 23000},
            }
        )

        with self.assertRaises(ValueError):
            spec.validate()

    def test_builds_docker_exec_command(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)

        argv = runtime.exec_command(
            "abc-123",
            ("python", "-c", "print('ok')"),
            env={"B": "2", "A": "1"},
            working_dir="/workspace",
            interactive=True,
        )

        self.assertEqual(
            argv,
            (
                "docker",
                "exec",
                "-i",
                "-w",
                "/workspace",
                "-e",
                "A=1",
                "-e",
                "B=2",
                "ucloud-sandbox-abc-123",
                "python",
                "-c",
                "print('ok')",
            ),
        )

    def test_builds_docker_file_copy_commands(self) -> None:
        with TemporaryDirectory() as raw_dir:
            source = Path(raw_dir) / "payload.txt"
            target = Path(raw_dir) / "download.txt"
            source.write_bytes(b"hello")
            runtime = DockerGvisorRuntime(dry_run=True)

            upload = runtime.copy_to_container("abc-123", source, "/workspace/payload.txt")
            download = runtime.copy_from_container(
                "abc-123",
                "/workspace/payload.txt",
                target,
            )

        self.assertEqual(
            upload.argv,
            (
                "docker",
                "cp",
                str(source),
                "ucloud-sandbox-abc-123:/workspace/payload.txt",
            ),
        )
        self.assertEqual(
            download.argv,
            (
                "docker",
                "cp",
                "ucloud-sandbox-abc-123:/workspace/payload.txt",
                str(target),
            ),
        )

    def test_streams_file_upload_and_download_through_exec(self) -> None:
        executor = RecordingExecutor(stdout_bytes=b"downloaded bytes\n")
        runtime = DockerGvisorRuntime(executor=executor)

        upload = runtime.write_file_to_container(
            "abc-123",
            "/workspace/payload.txt",
            b"uploaded bytes\n",
            owner="1000:1000",
        )
        content, download = runtime.read_file_from_container(
            "abc-123",
            "/workspace/payload.txt",
        )

        self.assertEqual(executor.inputs[0], b"uploaded bytes\n")
        self.assertIsNone(executor.inputs[1])
        self.assertEqual(content, b"downloaded bytes\n")
        self.assertEqual(
            upload.argv[:9],
            (
                "docker",
                "exec",
                "-i",
                "-e",
                "UCLOUD_SANDBOX_FILE=/workspace/payload.txt",
                "-e",
                "UCLOUD_SANDBOX_OWNER=1000:1000",
                "-u",
                "0",
            ),
        )
        self.assertEqual(
            download.argv[:6],
            (
                "docker",
                "exec",
                "-e",
                "UCLOUD_SANDBOX_FILE=/workspace/payload.txt",
                "-u",
                "0",
            ),
        )

    def test_file_download_preserves_exact_limit_and_rejects_limit_plus_one(self) -> None:
        exact = b"\x00\xffbinary"
        runtime = DockerGvisorRuntime(executor=RecordingExecutor(stdout_bytes=exact))

        content, _ = runtime.read_file_from_container(
            "abc-123",
            "/workspace/payload.bin",
            max_bytes=len(exact),
        )

        self.assertEqual(content, exact)
        oversized = DockerGvisorRuntime(
            executor=RecordingExecutor(stdout_bytes=exact + b"!")
        )
        with self.assertRaisesRegex(SandboxFileTooLargeError, "download limit"):
            oversized.read_file_from_container(
                "abc-123",
                "/workspace/payload.bin",
                max_bytes=len(exact),
            )

    def test_bounded_command_output_does_not_retain_unbounded_diagnostics(self) -> None:
        from ucloud_sandboxes.sandbox import SubprocessExecutor

        result = SubprocessExecutor().run_bounded_stdout(
            (
                sys.executable,
                "-c",
                "import os; os.write(2, b'e' * 100000); os.write(1, b'x' * 100000)",
            ),
            max_stdout_bytes=8,
            max_stderr_bytes=32,
        )

        self.assertEqual(result.stdout_bytes, b"x" * 9)
        self.assertEqual(result.stderr_bytes, b"e" * 32)

    def test_container_file_copy_rejects_directory_paths(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)

        with TemporaryDirectory() as raw_dir:
            source = Path(raw_dir) / "payload.txt"
            source.write_bytes(b"hello")
            with self.assertRaises(ValueError):
                runtime.copy_to_container("abc-123", source, "/workspace/")


class CountingSandboxStore(SandboxStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.load_count = 0

    def load_state(self):
        self.load_count += 1
        return super().load_state()


class SlowCreateRuntime(DockerGvisorRuntime):
    def __init__(self) -> None:
        super().__init__(dry_run=True)

    def create(
        self,
        spec: SandboxSpec,
        operation=None,
    ) -> CommandResult:
        time.sleep(0.05)
        return super().create(spec, operation=operation)


class CrashRecoveryExecutor:
    def __init__(self, sandbox_id: str, operation: SandboxOperation) -> None:
        self.sandbox_id = sandbox_id
        self.operation = operation
        self.create_calls = 0

    def run(self, argv: tuple[str, ...], *, input: bytes | None = None) -> CommandResult:
        del input
        if len(argv) > 1 and argv[1] == "run":
            self.create_calls += 1
            if self.create_calls == 1:
                return CommandResult(argv=argv, exit_code=0)
            return CommandResult(
                argv=argv,
                exit_code=1,
                stderr=(
                    "Conflict. The container name is already in use by container name"
                ),
            )
        if len(argv) > 1 and argv[1] == "inspect":
            return CommandResult(
                argv=argv,
                exit_code=0,
                stdout=json.dumps(
                    {
                        "ucloud-sandboxes.managed": "true",
                        "ucloud-sandboxes.sandbox-id": self.sandbox_id,
                        SANDBOX_GENERATION_LABEL: str(self.operation.generation),
                        SANDBOX_OPERATION_ID_LABEL: self.operation.operation_id,
                        SANDBOX_SPEC_HASH_LABEL: self.operation.spec_hash,
                    }
                ),
            )
        return CommandResult(argv=argv, exit_code=0)


def _create_operation(
    spec: SandboxSpec,
    generation: int,
    operation_id: str,
) -> SandboxOperation:
    return SandboxOperation(
        operation_id=operation_id,
        generation=generation,
        kind="create",
        spec_hash=sandbox_spec_fingerprint(spec),
    )


def _multiprocess_sandbox_writer(path: str, worker: int, count: int) -> None:
    manager = SandboxManager(
        SandboxStore(Path(path)),
        DockerGvisorRuntime(dry_run=True),
        ssh_port_range=(24000, 24999),
    )
    for index in range(count):
        manager.create(
            SandboxSpec.from_dict(
                {
                    "id": f"worker-{worker}-{index}",
                    "image": "busybox",
                    "memory_mb": 64,
                    "network": "bridge",
                    "ssh": True,
                }
            )
        )


def _multiprocess_versioned_create(
    path: str,
    generation: int,
    operation_id: str,
    start,
    results,
) -> None:
    spec = SandboxSpec(id="raced", image="busybox", memory_mb=128)
    manager = SandboxManager(
        SandboxStore(Path(path)),
        DockerGvisorRuntime(dry_run=True),
    )
    start.wait(10)
    try:
        record, _result = manager.create(
            spec,
            operation=_create_operation(spec, generation, operation_id),
        )
    except SandboxStaleOperationError:
        results.put(f"stale:{generation}")
    except SandboxConflictError:
        results.put(f"conflict:{generation}")
    else:
        results.put(f"created:{record.generation}")


def _multiprocess_create_after_drain(path: str, index: int, results) -> None:
    manager = SandboxManager(
        SandboxStore(Path(path)),
        DockerGvisorRuntime(dry_run=True),
    )
    try:
        manager.create(
            SandboxSpec(
                id=f"blocked-{index}",
                image="busybox",
                memory_mb=64,
            )
        )
    except SandboxAdmissionClosedError:
        results.put("closed")
    else:
        results.put("created")


def _multiprocess_capacity_create(path: str, index: int, start, results) -> None:
    manager = SandboxManager(
        SandboxStore(Path(path)),
        DockerGvisorRuntime(dry_run=True),
        effective_capacity=ResourceQuantity(memory_mb=128),
    )
    start.wait(10)
    try:
        manager.create(
            SandboxSpec(
                id=f"capacity-{index}",
                image="busybox",
                memory_mb=128,
            )
        )
    except SandboxCapacityUnavailableError:
        results.put("capacity")
    else:
        results.put("created")


if __name__ == "__main__":
    unittest.main()
