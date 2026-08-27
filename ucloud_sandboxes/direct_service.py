from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json
import logging
import subprocess
import threading
import time
from typing import Callable, Iterator, Sequence
from uuid import uuid4

from .direct_provisioner import DirectSandboxProvisioner
from .storage_native_migration import (
    StorageNativeSandboxManifest,
    StorageNativeMigration,
)
from .managed_process import (
    MANAGED_PROCESS_BINARY,
    MAX_LOG_READ_BYTES,
    ManagedProcessError,
    ManagedProcessLogChunk,
    ManagedProcessRecord,
    ManagedProcessStart,
    control_request_bytes,
    parse_control_response,
)
from .direct_registry import DirectSandboxRegistration
from .direct_warden import DirectWardenError
from .hibernation import HibernationState
from .models import NodeRuntimeMetrics, ResourceQuantity
from .sandbox import (
    OPERATION_ID_RE,
    SandboxAdmissionClosedError,
    SandboxCapacityUnavailableError,
    SandboxConflictError,
    SandboxFileTooLargeError,
    SandboxOperation,
    SandboxRecord,
    SandboxSpec,
    validate_container_path,
)
from .telemetry import Telemetry


_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class DirectExecResult:
    argv: tuple[str, ...]
    exit_code: int
    stdout: bytes
    stderr: bytes


@dataclass(frozen=True)
class DirectSandboxInventoryItem:
    registration: DirectSandboxRegistration
    record: SandboxRecord


@dataclass(frozen=True)
class DirectServiceInventorySnapshot:
    """Lifecycle records and their owners from one registry snapshot."""

    items: tuple[DirectSandboxInventoryItem, ...]
    activity_revision: int

    @property
    def records(self) -> tuple[SandboxRecord, ...]:
        return tuple(item.record for item in self.items)


@dataclass
class _LifecycleLockEntry:
    lock: threading.Lock
    users: int = 0


class DirectProcessRunner:
    """Run a fenced runsc command without buffering unbounded output in RAM."""

    def run(
        self,
        argv: Sequence[str],
        *,
        input_bytes: bytes | None,
        timeout_seconds: float | None,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
    ) -> DirectExecResult:
        command = tuple(str(item) for item in argv)
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE if input_bytes is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        output = bytearray()
        error = bytearray()
        overflow = threading.Event()

        def pump(stream, target: bytearray, limit: int) -> None:
            while True:
                chunk = stream.read(64 * 1024)
                if not chunk:
                    return
                remaining = limit + 1 - len(target)
                if remaining > 0:
                    target.extend(chunk[:remaining])
                if len(target) > limit:
                    overflow.set()
                    try:
                        process.terminate()
                    except ProcessLookupError:
                        pass
                    return

        readers = [
            threading.Thread(
                target=pump,
                args=(process.stdout, output, max_stdout_bytes),
                daemon=True,
            ),
            threading.Thread(
                target=pump,
                args=(process.stderr, error, max_stderr_bytes),
                daemon=True,
            ),
        ]
        for reader in readers:
            reader.start()
        writer: threading.Thread | None = None
        if input_bytes is not None:
            assert process.stdin is not None

            def write_input() -> None:
                try:
                    process.stdin.write(input_bytes)
                except BrokenPipeError:
                    pass
                finally:
                    process.stdin.close()

            writer = threading.Thread(target=write_input, daemon=True)
            writer.start()
        try:
            exit_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            for reader in readers:
                reader.join(timeout=2)
            if writer is not None:
                writer.join(timeout=2)
            assert process.stdout is not None and process.stderr is not None
            process.stdout.close()
            process.stderr.close()
            raise DirectWardenError("direct sandbox exec timed out") from exc
        for reader in readers:
            reader.join(timeout=2)
        if writer is not None:
            writer.join(timeout=2)
        assert process.stdout is not None and process.stderr is not None
        process.stdout.close()
        process.stderr.close()
        if overflow.is_set():
            raise SandboxFileTooLargeError(
                "direct sandbox exec exceeded its bounded output allowance"
            )
        return DirectExecResult(
            argv=command,
            exit_code=exit_code,
            stdout=bytes(output),
            stderr=bytes(error),
        )


class DirectSandboxService:
    """Product-facing service owned by the single direct runtime daemon."""

    def __init__(
        self,
        provisioner: DirectSandboxProvisioner,
        *,
        process_runner: DirectProcessRunner | None = None,
        max_concurrent_restores: int = 8,
        idle_park_seconds: float = 0.0,
        deletion_reconcile_interval_seconds: float = 5.0,
        image_reconcile_interval_seconds: float = 300.0,
        telemetry: Telemetry | None = None,
    ) -> None:
        if max_concurrent_restores < 1:
            raise ValueError("max_concurrent_restores must be positive")
        if idle_park_seconds < 0:
            raise ValueError("idle_park_seconds cannot be negative")
        if deletion_reconcile_interval_seconds <= 0:
            raise ValueError("deletion_reconcile_interval_seconds must be positive")
        if image_reconcile_interval_seconds <= 0:
            raise ValueError("image_reconcile_interval_seconds must be positive")
        self.provisioner = provisioner
        self.warden = provisioner.warden
        self.process_runner = process_runner or DirectProcessRunner()
        self.telemetry = telemetry or Telemetry.disabled("direct-sandbox-service")
        self._restore_slots = threading.Semaphore(max_concurrent_restores)
        self._active_capacity: ResourceQuantity | None = None
        self._runtime_metrics_provider: (
            Callable[[], NodeRuntimeMetrics | None] | None
        ) = None
        self._active_reservations: dict[tuple[str, int], ResourceQuantity] = {}
        self._active_exec_reservations: dict[
            str, tuple[tuple[str, int], ResourceQuantity]
        ] = {}
        # Cover transient work that deliberately precedes a durable registry
        # record, notably cold rootfs materialization. A per-process
        # nanosecond seed prevents a restarted draining Warden from reusing a
        # predecessor's drain proof by accident.
        self._activity_epoch = time.time_ns()
        self._capacity_guard = threading.Lock()
        self._locks: dict[tuple[str, int], _LifecycleLockEntry] = {}
        self._locks_guard = threading.Lock()
        self._admission_open = True
        self._idle_park_seconds = float(idle_park_seconds)
        self._deletion_reconcile_interval_seconds = float(
            deletion_reconcile_interval_seconds
        )
        self._image_reconcile_interval_seconds = float(image_reconcile_interval_seconds)
        self._next_image_reconcile = (
            time.monotonic() + self._image_reconcile_interval_seconds
        )
        self._last_activity: dict[tuple[str, int], float] = {}
        self._activity_guard = threading.Lock()
        self._stop_event = threading.Event()
        self._parking_thread: threading.Thread | None = None
        self._network_thread: threading.Thread | None = None
        self._deletion_thread: threading.Thread | None = None
        self._publication_threads: dict[tuple[str, int], threading.Thread] = {}
        self._publication_errors: dict[tuple[str, int], BaseException] = {}
        self._publication_guard = threading.Lock()
        self._published_snapshots: dict[
            tuple[str, int], StorageNativeMigration
        ] = {}
        self._published_snapshots_guard = threading.Lock()
        self._snapshot_hydration_thread: threading.Thread | None = None

    def configure_active_capacity(
        self,
        capacity: ResourceQuantity,
        *,
        runtime_metrics_provider: Callable[[], NodeRuntimeMetrics | None],
    ) -> None:
        """Install lifecycle capacity and the live-pressure admission source."""

        if not capacity.is_valid:
            raise ValueError("direct active capacity cannot be negative")
        with self._capacity_guard:
            self._active_capacity = ResourceQuantity(
                vcpu=capacity.vcpu,
                memory_mb=capacity.memory_mb,
            )
            self._runtime_metrics_provider = runtime_metrics_provider

    def start(self) -> tuple[SandboxRecord, ...]:
        results = self.provisioner.start()
        records = tuple(self._record(item) for item in results)
        self._stop_event.clear()
        self._next_image_reconcile = (
            time.monotonic() + self._image_reconcile_interval_seconds
        )
        now = time.monotonic()
        with self._activity_guard:
            self._last_activity = {
                (record.spec.id, record.generation): now for record in records
            }
        if self._idle_park_seconds > 0 and (
            self._parking_thread is None or not self._parking_thread.is_alive()
        ):
            self._parking_thread = threading.Thread(
                target=self._idle_parking_loop,
                name="ucloud-direct-idle-parker",
                daemon=True,
            )
            self._parking_thread.start()
        if self._deletion_thread is None or not self._deletion_thread.is_alive():
            self._deletion_thread = threading.Thread(
                target=self._deletion_reconciliation_loop,
                name="ucloud-direct-deletion-reconciler",
                daemon=True,
            )
            self._deletion_thread.start()
        if (
            self._snapshot_hydration_thread is None
            or not self._snapshot_hydration_thread.is_alive()
        ):
            self._snapshot_hydration_thread = threading.Thread(
                target=self._hydrate_published_snapshot_cache,
                args=(records,),
                name="ucloud-published-snapshot-cache-hydrator",
                daemon=True,
            )
            self._snapshot_hydration_thread.start()
        network_manager = self.provisioner.network_manager
        if network_manager is not None:
            if network_manager.has_dynamic_tcp_egress and (
                self._network_thread is None or not self._network_thread.is_alive()
            ):
                self._network_thread = threading.Thread(
                    target=self._network_reconciliation_loop,
                    name="ucloud-direct-network-reconciler",
                    daemon=True,
                )
                self._network_thread.start()
        return records

    def stop(self) -> None:
        self._stop_event.set()
        deletion_thread = self._deletion_thread
        if deletion_thread is not None:
            deletion_thread.join(
                timeout=max(2.0, self._deletion_reconcile_interval_seconds * 2)
            )
        self._deletion_thread = None
        thread = self._parking_thread
        if thread is not None:
            thread.join(timeout=max(2.0, self._idle_park_seconds * 2))
        self._parking_thread = None
        network_thread = self._network_thread
        if network_thread is not None:
            interval = self.provisioner.network_manager.resolve_interval_seconds
            network_thread.join(timeout=max(2.0, interval * 2))
        self._network_thread = None
        snapshot_hydration_thread = self._snapshot_hydration_thread
        if snapshot_hydration_thread is not None:
            snapshot_hydration_thread.join(timeout=2.0)
        self._snapshot_hydration_thread = None

    def _hydrate_published_snapshot_cache(
        self,
        records: tuple[SandboxRecord, ...],
    ) -> None:
        """Recover completed publications once without taxing heartbeats."""

        for record in records:
            if self._stop_event.is_set():
                return
            if record.state.lower() != HibernationState.PARKED.value:
                continue
            try:
                registration = self._require_registration(record.spec.id)
                storage = self.warden._storage_record(
                    registration.to_direct_sandbox()
                )
                if storage.state.value != "published":
                    continue
                self.describe_storage_native_snapshot(record.spec.id)
            except (RuntimeError, ValueError):
                # This is a best-effort cache warmup. Lifecycle operations and
                # the explicit descriptor endpoint retain their own errors.
                continue

    def cached_storage_native_snapshot(
        self,
        sandbox_id: str,
        generation: int,
    ) -> StorageNativeMigration | None:
        with self._published_snapshots_guard:
            return self._published_snapshots.get((sandbox_id, generation))

    def _remember_published_snapshot(
        self,
        snapshot: StorageNativeMigration,
    ) -> StorageNativeMigration:
        if not isinstance(snapshot, StorageNativeMigration):
            # Keep lifecycle identity tests and instrumented substitutes able
            # to wrap the descriptor builder without requiring cache internals.
            return snapshot
        key = (
            snapshot.manifest.sandbox_id,
            snapshot.manifest.sandbox_generation,
        )
        with self._published_snapshots_guard:
            self._published_snapshots[key] = snapshot
        return snapshot

    def _forget_published_snapshot(self, sandbox_id: str, generation: int) -> None:
        with self._published_snapshots_guard:
            self._published_snapshots.pop((sandbox_id, generation), None)

    def _deletion_reconciliation_loop(self) -> None:
        while not self._stop_event.wait(self._deletion_reconcile_interval_seconds):
            failures: list[tuple[str, Exception]] = []
            deleted = 0
            now = time.monotonic()
            image_reconcile_pending = (
                self.provisioner.image_cache_reconciliation_pending
            )
            if image_reconcile_pending or now >= self._next_image_reconcile:
                try:
                    if image_reconcile_pending:
                        self.provisioner.reconcile_image_cache_if_pending()
                    else:
                        # A periodic pass is required even without a prior
                        # failure so crash-orphaned private tags are eventually
                        # collected on long-lived nodes.
                        self.provisioner.reconcile_image_cache()
                except Exception as exc:
                    _LOG.warning(
                        "could not reconcile the direct rootfs cache: %s",
                        exc,
                    )
                finally:
                    self._next_image_reconcile = (
                        time.monotonic() + self._image_reconcile_interval_seconds
                    )
            try:
                registrations = self.provisioner.registry.list()
            except Exception as exc:
                _LOG.warning(
                    "could not read durable sandbox deletions for reconciliation: %s",
                    exc,
                )
                continue
            for registration in registrations:
                if registration.phase != "deleting":
                    continue
                try:
                    self.delete(
                        registration.sandbox_id,
                        generation=registration.sandbox_generation,
                    )
                    deleted += 1
                except Exception as exc:
                    failures.append((registration.sandbox_id, exc))
            if deleted:
                _LOG.info("reconciled %d durable sandbox deletion(s)", deleted)
            if failures:
                first_id, first_error = failures[0]
                _LOG.warning(
                    "could not reconcile %d durable sandbox deletion(s); "
                    "first=%s: %s",
                    len(failures),
                    first_id,
                    first_error,
                )

    def _network_reconciliation_loop(self) -> None:
        network_manager = self.provisioner.network_manager
        assert network_manager is not None
        while not self._stop_event.wait(network_manager.resolve_interval_seconds):
            try:
                network_manager.refresh_tcp_egress()
            except Exception:
                # Keep the last exact /32 rules and retry. The initial
                # reconciliation is synchronous and fails node startup if the
                # endpoint has never resolved.
                continue

    def create(
        self,
        spec: SandboxSpec,
        *,
        operation: SandboxOperation,
    ) -> SandboxRecord:
        operation.validate_spec(spec)
        with self._lock(spec.id, operation.generation):
            with self._reserve_active_capacity(
                spec.id,
                operation.generation,
                spec.requested_resources(),
            ):
                registration = self.provisioner.create(
                    spec=spec,
                    sandbox_generation=operation.generation,
                    operation_id=operation.operation_id,
                )
            self._forget_published_snapshot(spec.id, operation.generation)
            self.mark_activity(spec.id, operation.generation)
            return self._record(registration)

    def get(self, sandbox_id: str) -> SandboxRecord | None:
        registration = self.provisioner.registry.get(sandbox_id)
        return None if registration is None else self._record(registration)

    def list(self) -> tuple[SandboxRecord, ...]:
        return tuple(
            self._record(item)
            for item in self.provisioner.registry.list()
            if item.phase != "deleting"
        )

    def list_snapshot(self) -> tuple[SandboxRecord, ...]:
        """Return inventory without waiting for a sandbox lifecycle fence."""

        return self.inventory_snapshot().records

    def inventory_snapshot(self) -> DirectServiceInventorySnapshot:
        """Build one indexed heartbeat view from one durable registry read."""

        registry = self.provisioner.registry.snapshot()
        return DirectServiceInventorySnapshot(
            items=tuple(
                DirectSandboxInventoryItem(item, self._record_snapshot(item))
                for item in registry.records
                if item.phase != "deleting"
            ),
            activity_revision=registry.activity_revision,
        )

    def try_delete(self, sandbox_id: str, *, generation: int) -> bool:
        """Delete only when no lifecycle operation currently owns the sandbox."""

        key = (sandbox_id, generation)
        with self._try_lock(*key) as acquired:
            if not acquired:
                return False
            registration = self.provisioner.registry.get(sandbox_id)
            if registration is None:
                deleted = True
            elif registration.sandbox_generation != generation:
                return False
            else:
                self.provisioner.delete(sandbox_id)
                deleted = True
        assert deleted
        with self._activity_guard:
            self._last_activity.pop(key, None)
        self._forget_published_snapshot(*key)
        return True

    def delete(
        self,
        sandbox_id: str,
        *,
        generation: int,
    ) -> None:
        if generation <= 0:
            raise ValueError("delete generation must be positive")
        registration = self.provisioner.registry.get(sandbox_id)
        if registration is None:
            return
        if registration.sandbox_generation != generation:
            raise DirectWardenError("delete generation does not own direct sandbox")
        key = (sandbox_id, registration.sandbox_generation)
        with self._lock(*key):
            self.provisioner.delete(sandbox_id)
        with self._activity_guard:
            self._last_activity.pop(key, None)
        self._forget_published_snapshot(*key)

    def evict_published(
        self,
        sandbox_id: str,
        *,
        generation: int,
        snapshot_manifest_digest: str,
    ) -> None:
        """Remove only a parked incarnation already durable in the Registry."""

        if generation <= 0:
            raise ValueError("eviction generation must be positive")
        digest = snapshot_manifest_digest.strip()
        if (
            not digest.startswith("sha256:")
            or len(digest) != len("sha256:") + 64
            or any(character not in "0123456789abcdef" for character in digest[7:])
        ):
            raise ValueError("eviction snapshot manifest digest is invalid")
        registration = self.provisioner.registry.get(sandbox_id)
        if registration is None:
            return
        if registration.sandbox_generation != generation:
            raise DirectWardenError(
                "eviction generation does not own the direct sandbox"
            )
        key = (sandbox_id, generation)
        with self._lock(*key):
            registration = self.provisioner.registry.get(sandbox_id)
            if registration is None:
                return
            if registration.sandbox_generation != generation:
                raise DirectWardenError(
                    "eviction generation does not own the direct sandbox"
                )
            if registration.phase == "deleting":
                self.provisioner.delete(sandbox_id)
            else:
                if registration.phase != "owned":
                    raise DirectWardenError(
                        "direct sandbox is busy with another ownership transition"
                    )
                sandbox = registration.to_direct_sandbox()
                lifecycle = self.warden.inspect(sandbox)
                if lifecycle is None or lifecycle.state != HibernationState.PARKED:
                    raise DirectWardenError(
                        "only a parked sandbox can be evicted from its worker"
                    )
                storage = self.warden._storage_record(sandbox)
                if (
                    storage.state.value != "published"
                    or storage.published_manifest_digest != digest
                ):
                    raise DirectWardenError(
                        "worker publication does not match the durable route"
                    )
                self.provisioner.delete(sandbox_id)
        with self._activity_guard:
            self._last_activity.pop(key, None)
        self._forget_published_snapshot(*key)

    def park(
        self,
        sandbox_id: str,
        *,
        operation_id: str,
        background: bool = False,
    ) -> SandboxRecord:
        if not OPERATION_ID_RE.fullmatch(operation_id):
            raise ValueError("park operation id is invalid")
        registration = self._require_registration(sandbox_id)
        with self._lock(sandbox_id, registration.sandbox_generation):
            sandbox = registration.to_direct_sandbox()
            record = self.warden.inspect(sandbox)
            if record is None:
                raise DirectWardenError("direct sandbox has no lifecycle journal")
            if (
                record.state == HibernationState.RUNNING
                and not self.warden.running_process_alive(sandbox)
            ):
                record = self.warden.reconcile(sandbox)
            if record.state == HibernationState.PARKED:
                if background:
                    self._start_storage_publication(
                        registration,
                        operation_id=f"{operation_id}:publish",
                    )
                return self._record(registration)
            if record.state != HibernationState.RUNNING:
                record = self.warden.reconcile(sandbox)
            if record.state != HibernationState.RUNNING:
                raise DirectWardenError(
                    f"direct sandbox cannot park from {record.state.value}"
                )
            with self.telemetry.span(
                "sandbox.park",
                attributes={
                    "sandbox.id": sandbox_id,
                    "sandbox.generation": registration.sandbox_generation,
                    "sandbox.park.background_publication": background,
                },
            ):
                self._forget_published_snapshot(
                    sandbox_id,
                    registration.sandbox_generation,
                )
                self.warden.park(
                    sandbox,
                    operation_id=operation_id,
                )
            if background:
                self._start_storage_publication(
                    registration,
                    operation_id=f"{operation_id}:publish",
                )
            return self._record(registration)

    def storage_native_publication_pending(self, sandbox_id: str) -> bool:
        registration = self._require_registration(sandbox_id)
        key = (sandbox_id, registration.sandbox_generation)
        with self._publication_guard:
            thread = self._publication_threads.get(key)
            return bool(thread is not None and thread.is_alive())

    def _start_storage_publication(
        self,
        registration: DirectSandboxRegistration,
        *,
        operation_id: str,
    ) -> None:
        key = (
            registration.sandbox_id,
            registration.sandbox_generation,
        )
        with self._publication_guard:
            existing = self._publication_threads.get(key)
            if existing is not None and existing.is_alive():
                return
            self._publication_errors.pop(key, None)
            trace_context = self.telemetry.current_trace_headers()

            def publish() -> None:
                with self.telemetry.span(
                    "sandbox.storage_publication.background",
                    attributes={
                        "sandbox.id": registration.sandbox_id,
                        "sandbox.generation": registration.sandbox_generation,
                    },
                    parent_context=self.telemetry.extracted_context(trace_context),
                ) as span:
                    try:
                        self.warden.publish_storage_snapshot(
                            registration.to_direct_sandbox(),
                            operation_id=operation_id,
                        )
                        self.describe_storage_native_snapshot(
                            registration.sandbox_id
                        )
                    except BaseException as exc:
                        with self._publication_guard:
                            self._publication_errors[key] = exc
                        span.set_error(exc)

            thread = threading.Thread(
                target=publish,
                name=(
                    "ucloud-storage-publisher-"
                    f"{registration.sandbox_id}-{registration.sandbox_generation}"
                ),
                daemon=True,
            )
            self._publication_threads[key] = thread
            thread.start()

    def wake(
        self,
        sandbox_id: str,
        *,
        generation: int,
        operation_id: str,
    ) -> SandboxRecord:
        if generation <= 0:
            raise ValueError("wake generation must be positive")
        if not OPERATION_ID_RE.fullmatch(operation_id):
            raise ValueError("wake operation id is invalid")
        registration = self._require_registration(sandbox_id)
        if registration.sandbox_generation != generation:
            raise DirectWardenError("wake generation does not own direct sandbox")
        with self._lock(sandbox_id, registration.sandbox_generation):
            sandbox = registration.to_direct_sandbox()
            record = self.warden.inspect(sandbox)
            if record is None:
                raise DirectWardenError("direct sandbox has no lifecycle journal")
            if (
                record.state == HibernationState.RUNNING
                and not self.warden.running_process_alive(sandbox)
            ):
                record = self.warden.reconcile(sandbox)
            if record.state == HibernationState.RUNNING:
                return self._record(registration)
            if record.state != HibernationState.PARKED:
                record = self.warden.reconcile(sandbox)
            if record.state == HibernationState.PARKED:
                timings: dict[str, float] = {}
                with self.telemetry.span(
                    "sandbox.wake",
                    attributes={
                        "sandbox.id": sandbox_id,
                        "sandbox.generation": generation,
                    },
                ) as span:
                    with self.telemetry.span("sandbox.wake.ensure_network"):
                        self.provisioner.ensure_network(registration)
                    record = self.warden.resume(
                        sandbox,
                        operation_id=operation_id,
                        timings=timings,
                    )
                    span.add_event("sandbox.wake.timings", timings)
            if record.state != HibernationState.RUNNING:
                raise DirectWardenError(
                    f"direct sandbox cannot wake from {record.state.value}"
                )
            self._forget_published_snapshot(sandbox_id, generation)
            return self._record(registration)

    def prepare_storage_native_move(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
    ) -> StorageNativeMigration:
        """Publish a parked source and fence its portable metadata."""

        existing = self.provisioner.registry.get(sandbox_id)
        if existing is not None and existing.phase == "moving_out":
            if existing.migration_id != migration_id or not existing.migration_sha256:
                raise DirectWardenError("source does not own this prepared migration")
            migration = self.provisioner.storage_migrations.load(migration_id)
            if migration.sha256 != existing.migration_sha256:
                raise DirectWardenError(
                    "prepared storage-native migration changed identity"
                )
            return migration
        registration = self._require_registration(sandbox_id)
        with self._lock(sandbox_id, registration.sandbox_generation):
            migration = self._storage_native_snapshot_locked(registration)
            self.provisioner.storage_migrations.save(migration_id, migration)
            self.provisioner.registry.begin_move_out(
                sandbox_id,
                expected_revision=registration.revision,
                migration_id=migration_id,
                migration_sha256=migration.sha256,
            )
            return migration

    def describe_storage_native_snapshot(
        self,
        sandbox_id: str,
    ) -> StorageNativeMigration:
        """Return a complete portable descriptor for durable parked authority."""

        registration = self._require_registration(sandbox_id)
        with self._lock(sandbox_id, registration.sandbox_generation):
            return self._remember_published_snapshot(
                self._storage_native_snapshot_locked(registration)
            )

    def publish_parked(
        self,
        sandbox_id: str,
        *,
        generation: int,
        create_operation_id: str,
        spec_hash: str,
    ) -> StorageNativeMigration:
        """Publish one exact parked incarnation without changing worker ownership."""

        if generation <= 0:
            raise ValueError("publication generation must be positive")
        if not OPERATION_ID_RE.fullmatch(create_operation_id):
            raise ValueError("publication create operation id is invalid")
        if len(spec_hash) != 64 or any(
            character not in "0123456789abcdef" for character in spec_hash
        ):
            raise ValueError("publication spec digest is invalid")
        registration = self._require_registration(sandbox_id)
        if (
            registration.sandbox_generation != generation
            or registration.operation_id != create_operation_id
            or registration.spec_sha256 != spec_hash
        ):
            raise DirectWardenError(
                "publication identity does not own the direct sandbox"
            )
        with self._lock(sandbox_id, generation):
            registration = self._require_registration(sandbox_id)
            if (
                registration.sandbox_generation != generation
                or registration.operation_id != create_operation_id
                or registration.spec_sha256 != spec_hash
            ):
                raise DirectWardenError(
                    "publication identity does not own the direct sandbox"
                )
            if registration.phase != "owned":
                raise DirectWardenError(
                    "direct sandbox is busy with another ownership transition"
                )
            return self._remember_published_snapshot(
                self._storage_native_snapshot_locked(registration)
            )

    def _storage_native_snapshot_locked(
        self,
        registration: DirectSandboxRegistration,
    ) -> StorageNativeMigration:
        sandbox = registration.to_direct_sandbox()
        lifecycle = self.warden.inspect(sandbox)
        if lifecycle is None or lifecycle.state != HibernationState.PARKED:
            raise DirectWardenError(
                "only an already parked sandbox has a storage snapshot"
            )
        local_manifest = self.warden.load_parked_manifest(sandbox)
        source_guest_ip: str | None = None
        if registration.spec.network != "none":
            network_manager = self.provisioner.network_manager
            if network_manager is None:
                raise DirectWardenError(
                    "networked snapshot has no source network manager"
                )
            network_lease = network_manager.lease(
                registration.sandbox_id,
                registration.sandbox_generation,
            )
            if network_lease is None:
                raise DirectWardenError(
                    "networked snapshot has no durable source network lease"
                )
            source_guest_ip = network_lease.guest_ip
        portable = StorageNativeSandboxManifest.from_local(
            registration,
            local_manifest,
            source_guest_ip=source_guest_ip,
        )
        storage = self.warden._storage_record(sandbox)
        if storage.state.value != "published":
            storage = self.warden.publish_storage_snapshot(
                sandbox,
                operation_id=(
                    "snapshot:" f"{lifecycle.hibernation_generation}:publish"
                ),
            )
        if storage.state.value != "published":
            raise DirectWardenError(
                "storage-native publication did not return durable authority"
            )
        return StorageNativeMigration(
            manifest=portable,
            publication=storage.publication(),
        )

    def abort_move(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> SandboxRecord:
        # The gateway durably records a migration before asking the source to
        # prepare it.  If that prepare response is lost, the gateway still has
        # the migration id but cannot know the snapshot digest that the worker
        # fenced locally.  Cancellation must therefore be able to recover that
        # digest from the exact moving-out registration.  The migration id is
        # always validated and matched; supplied digests retain the strict
        # identity check used by every later migration phase.
        if not OPERATION_ID_RE.fullmatch(migration_id):
            raise ValueError("migration id is invalid")
        if migration_sha256:
            self._validate_migration_identity(migration_id, migration_sha256)
        registration = self.provisioner.registry.get(sandbox_id)
        if registration is None:
            raise DirectWardenError("migration source is absent")
        with self._lock(sandbox_id, registration.sandbox_generation):
            if registration.phase == "owned":
                return self._record(registration)
            if not migration_sha256:
                if (
                    registration.phase != "moving_out"
                    or registration.migration_id != migration_id
                ):
                    raise DirectWardenError(
                        "source does not own this prepared migration"
                    )
                migration_sha256 = registration.migration_sha256
                self._validate_migration_identity(migration_id, migration_sha256)
            registration = self.provisioner.registry.abort_move_out(
                sandbox_id,
                expected_revision=registration.revision,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
            self.provisioner.storage_migrations.discard(migration_id)
            return self._record(registration)

    def activate_import(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> SandboxRecord:
        self._validate_migration_identity(migration_id, migration_sha256)
        registration = self.provisioner.registry.get(sandbox_id)
        if registration is None:
            raise DirectWardenError("migration destination is absent")
        with self._lock(sandbox_id, registration.sandbox_generation):
            registration = self.provisioner.activate_import(
                sandbox_id,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
            self.mark_activity(sandbox_id, registration.sandbox_generation)
            return self._record(registration)

    def abort_import(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> None:
        self._validate_migration_identity(migration_id, migration_sha256)
        registration = self.provisioner.registry.get(sandbox_id)
        if registration is None:
            return
        key = (sandbox_id, registration.sandbox_generation)
        with self._lock(*key):
            self.provisioner.abort_import(
                sandbox_id,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
        with self._activity_guard:
            self._last_activity.pop(key, None)
        self.provisioner.storage_migrations.discard(migration_id)

    def stage_storage_native_import(
        self,
        migration: StorageNativeMigration,
        *,
        migration_id: str,
    ) -> tuple[SandboxRecord, StorageNativeMigration]:
        registration, stored = self.provisioner.stage_storage_native_import(
            migration,
            migration_id=migration_id,
        )
        return self._record(registration), stored

    def finalize_moved_source(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> None:
        self._validate_migration_identity(migration_id, migration_sha256)
        registration = self.provisioner.registry.get(sandbox_id)
        if registration is None:
            return
        key = (sandbox_id, registration.sandbox_generation)
        with self._lock(*key):
            self.provisioner.finalize_moved_source(
                sandbox_id,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
        with self._activity_guard:
            self._last_activity.pop(key, None)
        self.provisioner.storage_migrations.discard(migration_id)

    @staticmethod
    def _validate_migration_identity(
        migration_id: str,
        migration_sha256: str,
    ) -> None:
        if not OPERATION_ID_RE.fullmatch(migration_id):
            raise ValueError("migration id is invalid")
        if len(migration_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in migration_sha256
        ):
            raise ValueError("migration digest is invalid")

    def exec(
        self,
        sandbox_id: str,
        argv: Sequence[str],
        *,
        input_bytes: bytes | None = None,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        user: str | None = None,
        timeout_seconds: float | None = None,
        max_stdout_bytes: int = 256 * 1024 * 1024,
        max_stderr_bytes: int = 16 * 1024 * 1024,
    ) -> DirectExecResult:
        if max_stdout_bytes < 1 or max_stderr_bytes < 1:
            raise ValueError("direct exec output limits must be positive")
        registration = self._require_registration(sandbox_id)
        with self._lock(sandbox_id, registration.sandbox_generation):
            self.mark_activity(sandbox_id, registration.sandbox_generation)
            sandbox = registration.to_direct_sandbox()
            self._ensure_running(sandbox)
            token = self.acquire_exec_capacity(
                sandbox_id,
                registration.sandbox_generation,
            )
            try:
                with self.warden.exec_lease(
                    sandbox,
                    argv,
                    env=env,
                    working_dir=working_dir,
                    user=user,
                ) as command:
                    result = self.process_runner.run(
                        command,
                        input_bytes=input_bytes,
                        timeout_seconds=timeout_seconds,
                        max_stdout_bytes=max_stdout_bytes,
                        max_stderr_bytes=max_stderr_bytes,
                    )
            finally:
                self.release_exec_capacity(token)
            self.mark_activity(sandbox_id, registration.sandbox_generation)
            return result

    def start_managed_process(
        self,
        sandbox_id: str,
        spec: ManagedProcessStart,
    ) -> ManagedProcessRecord:
        registration = self._require_managed_registration(sandbox_id)
        uid, gid = self._managed_workload_credentials(registration)
        payload = spec.control_payload(uid=uid, gid=gid)
        raw = self._managed_control(registration, payload, retry_not_ready=True)
        return ManagedProcessRecord.from_control_response(
            raw,
            sandbox_id=sandbox_id,
            sandbox_generation=registration.sandbox_generation,
        )

    @staticmethod
    def _managed_workload_credentials(
        registration: DirectSandboxRegistration,
    ) -> tuple[int, int]:
        try:
            config = json.loads(
                (Path(registration.bundle) / "config.json").read_text(encoding="utf-8")
            )
            annotations = config["annotations"]
            uid = int(annotations["dev.ucloud-sandboxes.managed-process.uid"])
            gid = int(annotations["dev.ucloud-sandboxes.managed-process.gid"])
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ManagedProcessError(
                "managed process workload credentials are unavailable"
            ) from exc
        if not 0 <= uid <= (2**32 - 1) or not 0 <= gid <= (2**32 - 1):
            raise ManagedProcessError(
                "managed process workload credentials are invalid"
            )
        return uid, gid

    def managed_process_status(
        self,
        sandbox_id: str,
        job_id: str,
    ) -> ManagedProcessRecord:
        registration = self._require_managed_registration(sandbox_id)
        raw = self._managed_control(
            registration,
            {
                "version": 1,
                "action": "status",
                "job_id": job_id,
            },
        )
        return ManagedProcessRecord.from_control_response(
            raw,
            sandbox_id=sandbox_id,
            sandbox_generation=registration.sandbox_generation,
        )

    def managed_process_logs(
        self,
        sandbox_id: str,
        job_id: str,
        *,
        stream: str,
        offset: int,
        limit: int,
    ) -> ManagedProcessLogChunk:
        registration = self._require_managed_registration(sandbox_id)
        if stream not in {"stdout", "stderr"}:
            raise ValueError("managed process stream must be stdout or stderr")
        if offset < 0:
            raise ValueError("managed process log offset cannot be negative")
        if not 1 <= limit <= MAX_LOG_READ_BYTES:
            raise ValueError(
                f"managed process log limit must be in [1, {MAX_LOG_READ_BYTES}]"
            )
        raw = self._managed_control(
            registration,
            {
                "version": 1,
                "action": "logs",
                "job_id": job_id,
                "stream": stream,
                "offset": offset,
                "limit": limit,
            },
            max_stdout_bytes=(limit * 2) + 4096,
        )
        return ManagedProcessLogChunk.from_control_response(raw)

    def signal_managed_process(
        self,
        sandbox_id: str,
        job_id: str,
        *,
        signal: int,
    ) -> ManagedProcessRecord:
        registration = self._require_managed_registration(sandbox_id)
        raw = self._managed_control(
            registration,
            {
                "version": 1,
                "action": "signal",
                "job_id": job_id,
                "signal": signal,
            },
        )
        return ManagedProcessRecord.from_control_response(
            raw,
            sandbox_id=sandbox_id,
            sandbox_generation=registration.sandbox_generation,
        )

    def _require_managed_registration(self, sandbox_id: str):
        registration = self._require_registration(sandbox_id)
        if not registration.spec.managed_process:
            raise ManagedProcessError("sandbox was not created in managed_process mode")
        return registration

    def _managed_control(
        self,
        registration,
        payload: dict[str, object],
        *,
        retry_not_ready: bool = False,
        max_stdout_bytes: int = 2 * 1024 * 1024,
    ) -> dict[str, object]:
        with self._lock(
            registration.sandbox_id,
            registration.sandbox_generation,
        ):
            sandbox = registration.to_direct_sandbox()
            lifecycle = self.warden.inspect(sandbox)
            if lifecycle is None:
                raise ManagedProcessError(
                    "managed process sandbox has no lifecycle journal"
                )
            if lifecycle.state == HibernationState.PARKED:
                raise ManagedProcessError(
                    "managed process is suspended; status is served by the gateway"
                )
            self._ensure_running(sandbox)
            attempts = 50 if retry_not_ready else 1
            last_error = ""
            for attempt in range(attempts):
                with self.warden.exec_lease(
                    sandbox,
                    (MANAGED_PROCESS_BINARY, "ctl", "--timeout", "10s"),
                    user="0:0",
                ) as command:
                    result = self.process_runner.run(
                        command,
                        input_bytes=control_request_bytes(payload),
                        timeout_seconds=15,
                        max_stdout_bytes=max_stdout_bytes,
                        max_stderr_bytes=64 * 1024,
                    )
                if result.exit_code == 0:
                    self.mark_activity(
                        registration.sandbox_id,
                        registration.sandbox_generation,
                    )
                    return parse_control_response(result.stdout)
                try:
                    failed_response = parse_control_response(result.stdout)
                    last_error = str(failed_response.get("error") or "").strip()
                except ManagedProcessError:
                    last_error = ""
                if not last_error:
                    last_error = result.stderr.decode("utf-8", errors="replace").strip()
                if not retry_not_ready or attempt + 1 >= attempts:
                    break
                time.sleep(0.02)
            raise ManagedProcessError(
                last_error or "managed process control exchange failed"
            )

    def mark_activity(self, sandbox_id: str, generation: int) -> None:
        with self._activity_guard:
            self._last_activity[(sandbox_id, generation)] = time.monotonic()

    def _idle_parking_loop(self) -> None:
        interval = min(1.0, max(0.05, self._idle_park_seconds / 4))
        while not self._stop_event.wait(interval):
            now = time.monotonic()
            for registration in self.provisioner.registry.list():
                if registration.phase != "owned" or not registration.spec.parkable:
                    continue
                key = (
                    registration.sandbox_id,
                    registration.sandbox_generation,
                )
                with self._activity_guard:
                    last_activity = self._last_activity.setdefault(key, now)
                if now - last_activity < self._idle_park_seconds:
                    continue
                with self._capacity_guard:
                    if any(
                        active_key == key
                        for active_key, _resources in (
                            self._active_exec_reservations.values()
                        )
                    ):
                        continue
                try:
                    with self._try_lock(*key) as acquired:
                        if not acquired:
                            continue
                        lifecycle = self.warden.inspect(
                            registration.to_direct_sandbox()
                        )
                        if (
                            lifecycle is not None
                            and lifecycle.state == HibernationState.RUNNING
                        ):
                            self.warden.park(
                                registration.to_direct_sandbox(),
                                operation_id=f"idle-park:{uuid4().hex}",
                            )
                except DirectWardenError:
                    # Reconciliation and node health expose persistent failures;
                    # one failed background park must not kill the daemon.
                    continue

    def read_file(
        self,
        sandbox_id: str,
        path: str,
        *,
        max_bytes: int,
    ) -> bytes:
        validate_container_path("sandbox file path", path)
        result = self.exec(
            sandbox_id,
            ("/bin/cat", "--", path),
            max_stdout_bytes=max_bytes,
            max_stderr_bytes=64 * 1024,
        )
        if result.exit_code != 0:
            raise DirectWardenError(
                f"sandbox file read failed with exit {result.exit_code}"
            )
        return result.stdout

    def write_file(
        self,
        sandbox_id: str,
        path: str,
        payload: bytes,
    ) -> None:
        validate_container_path("sandbox file path", path)
        script = (
            "set -eu; target=$1; dir=${target%/*}; "
            'mkdir -p -- "$dir"; '
            'tmp=$(mktemp "$dir/.ucloud-write.XXXXXX"); '
            "trap 'rm -f -- \"$tmp\"' EXIT HUP INT TERM; "
            'cat >"$tmp"; chmod 0600 "$tmp"; mv -f -- "$tmp" "$target"; '
            "trap - EXIT HUP INT TERM"
        )
        result = self.exec(
            sandbox_id,
            ("/bin/sh", "-c", script, "ucloud-write", path),
            input_bytes=payload,
            max_stdout_bytes=64 * 1024,
            max_stderr_bytes=64 * 1024,
        )
        if result.exit_code != 0:
            raise DirectWardenError(
                f"sandbox file write failed with exit {result.exit_code}"
            )

    def close_admission(self) -> None:
        # Once this returns, every admitted create is present in the transient
        # reservation snapshot or has already finished.
        with self._capacity_guard:
            self._admission_open = False

    def open_admission(self) -> None:
        with self._capacity_guard:
            self._admission_open = True

    @property
    def admission_open(self) -> bool:
        with self._capacity_guard:
            return self._admission_open

    def _ensure_running(self, sandbox) -> None:
        self.ensure_running_with_timings(sandbox)

    def ensure_running_with_timings(self, sandbox) -> dict[str, float]:
        started = time.monotonic()
        phase = started
        record = self.warden.inspect(sandbox)
        timings = {"inspect": (time.monotonic() - phase) * 1000}
        if record is None:
            raise DirectWardenError("direct sandbox has no lifecycle journal")
        if (
            record.state == HibernationState.RUNNING
            and not self.warden.running_process_alive(sandbox)
        ):
            phase = time.monotonic()
            record = self.warden.reconcile(sandbox)
            timings["reconcile_dead_sentry"] = (time.monotonic() - phase) * 1000
        if record.state not in {
            HibernationState.RUNNING,
            HibernationState.PARKED,
        }:
            phase = time.monotonic()
            record = self.warden.reconcile(sandbox)
            timings["reconcile"] = (time.monotonic() - phase) * 1000
        if record.state == HibernationState.PARKED:
            registration = self._require_registration(sandbox.sandbox_id)
            with self._reserve_active_capacity(
                sandbox.sandbox_id,
                registration.sandbox_generation,
                ResourceQuantity(
                    vcpu=registration.spec.cpus or 0,
                    memory_mb=registration.spec.memory_mb or 0,
                ),
            ):
                phase = time.monotonic()
                self._restore_slots.acquire()
                timings["restore_queue"] = (time.monotonic() - phase) * 1000
                try:
                    registration = self._require_registration(sandbox.sandbox_id)
                    phase = time.monotonic()
                    self.provisioner.ensure_network(registration)
                    timings["restore_network"] = (time.monotonic() - phase) * 1000
                    phase = time.monotonic()
                    warden_timings: dict[str, float] = {}
                    record = self.warden.resume(
                        sandbox,
                        operation_id=f"wake:{uuid4().hex}",
                        timings=warden_timings,
                    )
                    timings["restore"] = (time.monotonic() - phase) * 1000
                    timings.update(
                        {
                            f"restore_{name}": elapsed_ms
                            for name, elapsed_ms in warden_timings.items()
                        }
                    )
                finally:
                    self._restore_slots.release()
        if record.state != HibernationState.RUNNING:
            raise DirectWardenError(
                f"direct sandbox cannot accept traffic in {record.state.value}"
            )
        timings["total"] = (time.monotonic() - started) * 1000
        return timings

    @contextmanager
    def _reserve_active_capacity(
        self,
        sandbox_id: str,
        generation: int,
        requested: ResourceQuantity,
    ):
        key = (sandbox_id, generation)
        with self._capacity_guard:
            if not self._admission_open:
                raise SandboxAdmissionClosedError("direct node admission is closed")
            capacity = self._active_capacity
            if capacity is not None:
                used = ResourceQuantity()
                for candidate_key, reservation in self._active_reservations.items():
                    if candidate_key != key:
                        used = used + reservation
                for _active_key, reservation in (
                    self._active_exec_reservations.values()
                ):
                    used = used + reservation
                available = ResourceQuantity(
                    vcpu=max(0.0, capacity.vcpu - used.vcpu),
                    memory_mb=max(0, capacity.memory_mb - used.memory_mb),
                )
                active_requested = ResourceQuantity(
                    vcpu=requested.vcpu,
                    memory_mb=requested.memory_mb,
                )
                if not active_requested.fits_within(available):
                    raise SandboxCapacityUnavailableError(
                        "direct node has insufficient active CPU or memory "
                        "admission headroom; retry on another node"
                    )
                self._require_dynamic_headroom(active_requested)
            self._active_reservations[key] = requested
            self._activity_epoch += 1
        try:
            yield
        finally:
            with self._capacity_guard:
                self._active_reservations.pop(key, None)
                self._activity_epoch += 1

    def acquire_exec_capacity(self, sandbox_id: str, generation: int) -> str:
        """Admit an exec from live node pressure and track its lifetime."""

        registration = self._require_registration(sandbox_id)
        if registration.sandbox_generation != generation:
            raise SandboxConflictError(
                "exec generation does not own direct sandbox"
            )
        token = f"exec:{uuid4().hex}"
        key = (sandbox_id, generation)
        # Sampling host CPU currently spans a short /proc/stat interval. Do it
        # outside the capacity guard so unrelated exec starts can sample in
        # parallel instead of turning admission into a serialized hot path.
        with self._capacity_guard:
            metrics_provider = (
                self._runtime_metrics_provider
                if self._active_capacity is not None
                else None
            )
        metrics = metrics_provider() if metrics_provider is not None else None
        with self._capacity_guard:
            if not self._admission_open:
                raise SandboxAdmissionClosedError("direct node admission is closed")
            if self._active_capacity is not None:
                # A sandbox's configured limits bound that sandbox; they do not
                # describe what its next command will consume. Charging the
                # complete shape here artificially capped a 32-vCPU worker at
                # eight concurrent execs from 4-vCPU sandboxes even when the
                # host was mostly idle. Admit from current host CPU, load,
                # memory and PSI instead. ExecSessionManager retains the hard
                # session-count backstop for admission bursts between samples.
                self._require_dynamic_headroom_for_metrics(
                    ResourceQuantity(),
                    metrics,
                )
            # Keep a zero-resource lease so drain fencing, idle parking and
            # active-operation telemetry still cover the full exec lifetime.
            self._active_exec_reservations[token] = (key, ResourceQuantity())
            self._activity_epoch += 1
        return token

    def release_exec_capacity(self, token: str) -> None:
        with self._capacity_guard:
            if self._active_exec_reservations.pop(token, None) is not None:
                self._activity_epoch += 1

    def active_reservations_snapshot(
        self,
    ) -> tuple[dict[tuple[str, ...], ResourceQuantity], int]:
        """Return admitted transient work and its drain-fencing epoch."""

        with self._capacity_guard:
            reservations: dict[tuple[str, ...], ResourceQuantity] = dict(
                self._active_reservations
            )
            for token, (key, resources) in self._active_exec_reservations.items():
                reservations[(*key, token)] = resources
            return reservations, self._activity_epoch

    def _require_dynamic_headroom(self, requested: ResourceQuantity) -> None:
        provider = self._runtime_metrics_provider
        metrics = provider() if provider is not None else None
        self._require_dynamic_headroom_for_metrics(requested, metrics)

    @staticmethod
    def _require_dynamic_headroom_for_metrics(
        requested: ResourceQuantity,
        metrics: NodeRuntimeMetrics | None,
    ) -> None:
        if metrics is None:
            raise SandboxCapacityUnavailableError(
                "direct node has no fresh runtime metrics for dynamic admission"
            )
        if metrics.cpu_percent is not None and metrics.cpu_percent >= 90.0:
            raise SandboxCapacityUnavailableError(
                "direct node CPU pressure blocks active admission"
            )
        if (
            metrics.cpu_count > 0
            and metrics.load_average_1m is not None
            and metrics.load_average_1m >= metrics.cpu_count * 1.25
        ):
            raise SandboxCapacityUnavailableError(
                "direct node CPU load blocks active admission"
            )
        if (
            metrics.memory_psi_full_avg10 is not None
            and metrics.memory_psi_full_avg10 >= 10.0
        ):
            raise SandboxCapacityUnavailableError(
                "direct node memory pressure blocks active admission"
            )
        available_memory_mb = metrics.memory_available_mb
        if metrics.swap_total_mb > 0:
            available_memory_mb += metrics.swap_free_mb
        if available_memory_mb < max(2048, requested.memory_mb):
            raise SandboxCapacityUnavailableError(
                "direct node has insufficient live memory headroom"
            )

    def _require_registration(
        self,
        sandbox_id: str,
    ) -> DirectSandboxRegistration:
        registration = self.provisioner.registry.get(sandbox_id)
        if registration is None or registration.phase != "owned":
            raise DirectWardenError("direct sandbox is unavailable")
        return registration

    def _retain_lock(
        self,
        sandbox_id: str,
        generation: int,
    ) -> tuple[tuple[str, int], _LifecycleLockEntry]:
        key = (sandbox_id, generation)
        with self._locks_guard:
            entry = self._locks.setdefault(key, _LifecycleLockEntry(threading.Lock()))
            entry.users += 1
            return key, entry

    def _release_lock_entry(
        self,
        key: tuple[str, int],
        entry: _LifecycleLockEntry,
    ) -> None:
        with self._locks_guard:
            entry.users -= 1
            if entry.users < 0:
                raise RuntimeError("direct lifecycle lock reference underflow")
            if entry.users == 0 and self._locks.get(key) is entry:
                self._locks.pop(key)

    @contextmanager
    def _lock(self, sandbox_id: str, generation: int) -> Iterator[None]:
        key, entry = self._retain_lock(sandbox_id, generation)
        acquired = False
        try:
            entry.lock.acquire()
            acquired = True
            yield
        finally:
            if acquired:
                entry.lock.release()
            self._release_lock_entry(key, entry)

    @contextmanager
    def _try_lock(self, sandbox_id: str, generation: int) -> Iterator[bool]:
        key, entry = self._retain_lock(sandbox_id, generation)
        acquired = False
        try:
            acquired = entry.lock.acquire(blocking=False)
            yield acquired
        finally:
            if acquired:
                entry.lock.release()
            self._release_lock_entry(key, entry)

    def _record(self, registration: DirectSandboxRegistration) -> SandboxRecord:
        state = registration.phase
        if registration.phase == "owned":
            sandbox = registration.to_direct_sandbox()
            lifecycle = self.warden.inspect(sandbox)
            if (
                lifecycle is not None
                and lifecycle.state == HibernationState.RUNNING
                and not self.warden.running_process_alive(sandbox)
            ):
                lifecycle = self.warden.reconcile(sandbox)
            state = lifecycle.state.value if lifecycle is not None else "unavailable"
        return self._record_with_state(registration, state)

    def _record_snapshot(
        self,
        registration: DirectSandboxRegistration,
    ) -> SandboxRecord:
        state = registration.phase
        if registration.phase == "owned":
            lifecycle = self.warden.inspect_snapshot(registration.to_direct_sandbox())
            state = lifecycle.state.value if lifecycle is not None else "unavailable"
        return self._record_with_state(registration, state)

    @staticmethod
    def _record_with_state(
        registration: DirectSandboxRegistration,
        state: str,
    ) -> SandboxRecord:
        created_at = datetime.fromtimestamp(
            registration.created_ns / 1_000_000_000,
            tz=timezone.utc,
        )
        updated_at = datetime.fromtimestamp(
            registration.updated_ns / 1_000_000_000,
            tz=timezone.utc,
        )
        return SandboxRecord(
            spec=registration.spec,
            container_name=registration.container_id,
            state=state,
            created_at=created_at,
            updated_at=updated_at,
            generation=registration.sandbox_generation,
            operation_id=registration.operation_id,
            spec_hash=registration.spec_sha256,
        )
