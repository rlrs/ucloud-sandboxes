from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import threading
import time
from typing import Sequence
from uuid import uuid4
import re

from .direct_provisioner import DirectSandboxProvisioner
from .direct_migration import DirectMigrationArchive
from .direct_registry import DirectSandboxRegistration
from .direct_warden import DirectWardenError
from .hibernation import HibernationState
from .models import ResourceQuantity
from .sandbox import (
    SandboxAdmissionClosedError,
    SandboxFileTooLargeError,
    SandboxOperation,
    SandboxRecord,
    SandboxSpec,
    validate_container_path,
)


@dataclass(frozen=True)
class DirectExecResult:
    argv: tuple[str, ...]
    exit_code: int
    stdout: bytes
    stderr: bytes


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
    ) -> None:
        if max_concurrent_restores < 1:
            raise ValueError("max_concurrent_restores must be positive")
        if idle_park_seconds < 0:
            raise ValueError("idle_park_seconds cannot be negative")
        self.provisioner = provisioner
        self.warden = provisioner.warden
        self.process_runner = process_runner or DirectProcessRunner()
        self._restore_slots = threading.Semaphore(max_concurrent_restores)
        self._active_capacity: ResourceQuantity | None = None
        self._restore_reservations: set[tuple[str, int]] = set()
        self._capacity_guard = threading.Lock()
        self._locks: dict[tuple[str, int], threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self._admission_open = True
        self._idle_park_seconds = float(idle_park_seconds)
        self._last_activity: dict[tuple[str, int], float] = {}
        self._activity_guard = threading.Lock()
        self._stop_event = threading.Event()
        self._parking_thread: threading.Thread | None = None

    def configure_active_capacity(self, capacity: ResourceQuantity) -> None:
        """Install the hard CPU/RAM wake-admission ceiling for this node."""

        if not capacity.is_valid:
            raise ValueError("direct active capacity cannot be negative")
        with self._capacity_guard:
            self._active_capacity = ResourceQuantity(
                vcpu=capacity.vcpu,
                memory_mb=capacity.memory_mb,
            )

    def start(self) -> tuple[SandboxRecord, ...]:
        results = self.provisioner.start()
        records = tuple(self._record(item.registration) for item in results)
        now = time.monotonic()
        with self._activity_guard:
            self._last_activity = {
                (record.spec.id, record.generation): now for record in records
            }
        if (
            self._idle_park_seconds > 0
            and (
                self._parking_thread is None
                or not self._parking_thread.is_alive()
            )
        ):
            self._stop_event.clear()
            self._parking_thread = threading.Thread(
                target=self._idle_parking_loop,
                name="ucloud-direct-idle-parker",
                daemon=True,
            )
            self._parking_thread.start()
        return records

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._parking_thread
        if thread is not None:
            thread.join(timeout=max(2.0, self._idle_park_seconds * 2))
        self._parking_thread = None

    def create(
        self,
        spec: SandboxSpec,
        *,
        operation: SandboxOperation | None = None,
    ) -> SandboxRecord:
        if not self._admission_open:
            raise SandboxAdmissionClosedError("direct node admission is closed")
        operation = operation or SandboxOperation.legacy_create(spec)
        operation.validate_spec(spec)
        with self._lock(spec.id, operation.generation):
            result = self.provisioner.create(
                spec=spec,
                sandbox_generation=operation.generation,
                operation_id=operation.operation_id or "legacy-create",
            )
            self.mark_activity(spec.id, operation.generation)
            return self._record(result.registration)

    def get(self, sandbox_id: str) -> SandboxRecord | None:
        registration = self.provisioner.registry.get(sandbox_id)
        return None if registration is None else self._record(registration)

    def list(self) -> tuple[SandboxRecord, ...]:
        return tuple(
            self._record(item)
            for item in self.provisioner.registry.list()
            if item.phase != "deleting"
        )

    def delete(
        self,
        sandbox_id: str,
        *,
        generation: int | None = None,
    ) -> None:
        registration = self.provisioner.registry.get(sandbox_id)
        if registration is None:
            return
        if generation is not None and registration.sandbox_generation != generation:
            raise DirectWardenError("delete generation does not own direct sandbox")
        key = (sandbox_id, registration.sandbox_generation)
        with self._lock(*key):
            self.provisioner.delete(sandbox_id)
        with self._locks_guard:
            self._locks.pop(key, None)
        with self._activity_guard:
            self._last_activity.pop(key, None)

    def park(
        self, sandbox_id: str, *, operation_id: str | None = None
    ) -> SandboxRecord:
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
                return self._record(registration)
            if record.state != HibernationState.RUNNING:
                record = self.warden.reconcile(sandbox)
            if record.state != HibernationState.RUNNING:
                raise DirectWardenError(
                    f"direct sandbox cannot park from {record.state.value}"
                )
            self.warden.park(
                sandbox,
                operation_id=operation_id or f"park:{uuid4().hex}",
            )
            return self._record(registration)

    def wake(
        self,
        sandbox_id: str,
        *,
        generation: int | None = None,
        operation_id: str | None = None,
    ) -> SandboxRecord:
        registration = self._require_registration(sandbox_id)
        if generation is not None and registration.sandbox_generation != generation:
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
                self.provisioner.ensure_network(registration)
                record = self.warden.resume(
                    sandbox,
                    operation_id=operation_id or f"wake:{uuid4().hex}",
                )
            if record.state != HibernationState.RUNNING:
                raise DirectWardenError(
                    f"direct sandbox cannot wake from {record.state.value}"
                )
            return self._record(registration)

    def prepare_move(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        archive_path: Path | None = None,
    ) -> DirectMigrationArchive:
        """Freeze a parked source behind a migration fence and export it."""
        existing = self.provisioner.registry.get(sandbox_id)
        if existing is not None and existing.phase == "moving_out":
            return self.prepared_move_archive(
                sandbox_id,
                migration_id=migration_id,
            )
        registration = self._require_registration(sandbox_id)
        with self._lock(sandbox_id, registration.sandbox_generation):
            archive_path = archive_path or self.migration_archive_path(migration_id)
            lifecycle = self.warden.inspect(registration.to_direct_sandbox())
            if lifecycle is None or lifecycle.state != HibernationState.PARKED:
                raise DirectWardenError(
                    "only an already parked sandbox can begin migration"
                )
            local_manifest = self.warden.artifacts.load_complete(
                sandbox_id=registration.sandbox_id,
                sandbox_generation=registration.sandbox_generation,
                hibernation_generation=lifecycle.hibernation_generation,
            )
            source_guest_ip: str | None = None
            if registration.spec.network != "none":
                network_manager = self.provisioner.network_manager
                if network_manager is None:
                    raise DirectWardenError(
                        "networked migration has no source network manager"
                    )
                network_lease = network_manager.lease(
                    registration.sandbox_id,
                    registration.sandbox_generation,
                )
                if network_lease is None:
                    raise DirectWardenError(
                        "networked migration has no durable source network lease"
                    )
                source_guest_ip = network_lease.guest_ip
            exported = self.provisioner.migration_archives.export(
                registration=registration,
                local_manifest=local_manifest,
                runtime_identity=self.provisioner.identity,
                writable_incarnation=Path(registration.quota_path),
                archive_path=archive_path,
                source_guest_ip=source_guest_ip,
            )
            self.provisioner.registry.begin_move_out(
                sandbox_id,
                expected_revision=registration.revision,
                migration_id=migration_id,
                migration_sha256=exported.sha256,
            )
            return exported

    def migration_archive_path(self, migration_id: str) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", migration_id):
            raise ValueError("migration id contains unsupported characters")
        root = self.provisioner.registry.path.parent / "migration-archives"
        return root / f"{migration_id}.tar"

    def prepared_move_archive(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
    ) -> DirectMigrationArchive:
        registration = self.provisioner.registry.get(sandbox_id)
        if (
            registration is None
            or registration.phase != "moving_out"
            or registration.migration_id != migration_id
            or not registration.migration_sha256
        ):
            raise DirectWardenError("source does not own this prepared migration")
        path = self.migration_archive_path(migration_id)
        manifest = self.provisioner.migration_archives.inspect(
            path,
            expected_sha256=registration.migration_sha256,
        )
        info = path.lstat()
        return DirectMigrationArchive(
            path=path,
            sha256=registration.migration_sha256,
            physical_bytes=info.st_blocks * 512,
            elapsed_ms=0.0,
            manifest=manifest,
        )

    def abort_move(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> SandboxRecord:
        registration = self.provisioner.registry.get(sandbox_id)
        if registration is None:
            raise DirectWardenError("migration source is absent")
        with self._lock(sandbox_id, registration.sandbox_generation):
            if registration.phase == "owned":
                return self._record(registration)
            if (
                registration.phase == "moving_out"
                and registration.migration_id == migration_id
                and not migration_sha256
            ):
                migration_sha256 = registration.migration_sha256
            registration = self.provisioner.registry.abort_move_out(
                sandbox_id,
                expected_revision=registration.revision,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
            self._discard_migration_archive(migration_id)
            return self._record(registration)

    def activate_import(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> SandboxRecord:
        registration = self.provisioner.registry.get(sandbox_id)
        if registration is None:
            raise DirectWardenError("migration destination is absent")
        with self._lock(sandbox_id, registration.sandbox_generation):
            result = self.provisioner.activate_import(
                sandbox_id,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
            self.mark_activity(sandbox_id, registration.sandbox_generation)
            return self._record(result.registration)

    def abort_import(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> None:
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
        with self._locks_guard:
            self._locks.pop(key, None)
        with self._activity_guard:
            self._last_activity.pop(key, None)

    def stage_import(
        self,
        archive_path: Path,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> SandboxRecord:
        result = self.provisioner.stage_import(
            archive_path,
            expected_sha256=migration_sha256,
            migration_id=migration_id,
        )
        return self._record(result.registration)

    def finalize_moved_source(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> None:
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
        with self._locks_guard:
            self._locks.pop(key, None)
        with self._activity_guard:
            self._last_activity.pop(key, None)
        self._discard_migration_archive(migration_id)

    def _discard_migration_archive(self, migration_id: str) -> None:
        path = self.migration_archive_path(migration_id)
        try:
            path.unlink()
        except FileNotFoundError:
            return

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
            self.mark_activity(sandbox_id, registration.sandbox_generation)
            return result

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
                lock = self._lock(*key)
                if not lock.acquire(blocking=False):
                    continue
                try:
                    lifecycle = self.warden.inspect(registration.to_direct_sandbox())
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
                finally:
                    lock.release()

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
        self._admission_open = False

    def open_admission(self) -> None:
        self._admission_open = True

    @property
    def admission_open(self) -> bool:
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
            timings["reconcile_dead_sentry"] = (
                time.monotonic() - phase
            ) * 1000
        if record.state not in {
            HibernationState.RUNNING,
            HibernationState.PARKED,
        }:
            phase = time.monotonic()
            record = self.warden.reconcile(sandbox)
            timings["reconcile"] = (time.monotonic() - phase) * 1000
        if record.state == HibernationState.PARKED:
            with self._reserve_restore_capacity(sandbox.sandbox_id):
                phase = time.monotonic()
                self._restore_slots.acquire()
                timings["restore_queue"] = (time.monotonic() - phase) * 1000
                try:
                    registration = self._require_registration(sandbox.sandbox_id)
                    phase = time.monotonic()
                    self.provisioner.ensure_network(registration)
                    timings["restore_network"] = (
                        time.monotonic() - phase
                    ) * 1000
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
    def _reserve_restore_capacity(self, sandbox_id: str):
        registration = self._require_registration(sandbox_id)
        key = (sandbox_id, registration.sandbox_generation)
        with self._capacity_guard:
            capacity = self._active_capacity
            if capacity is not None:
                used = ResourceQuantity()
                for candidate in self.provisioner.registry.list():
                    if candidate.phase != "owned":
                        continue
                    candidate_key = (
                        candidate.sandbox_id,
                        candidate.sandbox_generation,
                    )
                    running = candidate_key in self._restore_reservations
                    if not running:
                        lifecycle = self.warden.inspect(candidate.to_direct_sandbox())
                        running = bool(
                            lifecycle is not None
                            and lifecycle.state == HibernationState.RUNNING
                        )
                    if not running:
                        continue
                    used = used + ResourceQuantity(
                        vcpu=candidate.spec.cpus or 0,
                        memory_mb=candidate.spec.memory_mb or 0,
                    )
                requested = ResourceQuantity(
                    vcpu=registration.spec.cpus or 0,
                    memory_mb=registration.spec.memory_mb or 0,
                )
                available = ResourceQuantity(
                    vcpu=max(0.0, capacity.vcpu - used.vcpu),
                    memory_mb=max(0, capacity.memory_mb - used.memory_mb),
                )
                if not requested.fits_within(available):
                    raise DirectWardenError(
                        "direct node has insufficient active CPU or memory "
                        "capacity; relocate the parked sandbox and retry"
                    )
            self._restore_reservations.add(key)
        try:
            yield
        finally:
            with self._capacity_guard:
                self._restore_reservations.discard(key)

    def _require_registration(
        self,
        sandbox_id: str,
    ) -> DirectSandboxRegistration:
        registration = self.provisioner.registry.get(sandbox_id)
        if registration is None or registration.phase != "owned":
            raise DirectWardenError("direct sandbox is unavailable")
        return registration

    def _lock(self, sandbox_id: str, generation: int) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(
                (sandbox_id, generation),
                threading.Lock(),
            )

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
