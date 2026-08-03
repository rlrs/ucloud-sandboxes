from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import tempfile
from threading import local, Lock, RLock
import time
from typing import Iterator

from .direct_service import DirectSandboxService
from .managed_process import (
    ManagedProcessLogChunk,
    ManagedProcessRecord,
    ManagedProcessStart,
)
from .models import ResourceQuantity
from .sandbox import (
    CommandResult,
    NodeDrainSnapshot,
    NodeDrainState,
    OPERATION_ID_RE,
    SandboxActivitySnapshot,
    SandboxAdmissionClosedError,
    SandboxConflictError,
    SandboxForkUnsupportedError,
    SandboxLifecycleCoordinator,
    SandboxOperation,
    SandboxRecord,
    SandboxSpec,
)


class DirectNodeStateStore:
    """Small crash-durable node state that is independent of sandbox ownership."""

    VERSION = 1

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("direct node state path must be absolute")
        self.path = path
        self._lock = Lock()

    def load_drain(self) -> NodeDrainState:
        with self._lock:
            if not self.path.exists():
                return NodeDrainState()
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict) or set(raw) != {"version", "drain"}:
                raise ValueError("direct node state has an invalid schema")
            if raw["version"] != self.VERSION:
                raise ValueError("direct node state has an unsupported version")
            return NodeDrainState.from_dict(raw["drain"])

    def save_drain(self, drain: NodeDrainState) -> None:
        payload = {
            "drain": drain.to_dict(),
            "version": self.VERSION,
        }
        encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
        with self._lock:
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            descriptor, raw_temporary = tempfile.mkstemp(
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                dir=self.path.parent,
            )
            temporary = Path(raw_temporary)
            try:
                os.fchmod(descriptor, 0o600)
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(encoded)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self.path)
                directory = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass


class DirectExecRuntimeAdapter:
    dry_run = False
    fork_enabled = False
    hibernate_enabled = True

    def __init__(self, owner: DirectNodeManagerAdapter) -> None:
        self.owner = owner

    def exec_command(
        self,
        sandbox_id: str,
        command: tuple[str, ...],
        *,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        interactive: bool = True,
        tty: bool = False,
        user: str | None = None,
    ) -> tuple[str, ...]:
        del interactive
        if tty:
            raise ValueError("direct runtime TTY exec is not yet qualified")
        start_lock = self.owner._acquire_exec_start(sandbox_id)
        try:
            registration = self.owner.service._require_registration(sandbox_id)
            lease = self.owner.service.warden.exec_lease(
                registration.to_direct_sandbox(),
                command,
                env=env,
                working_dir=working_dir,
                user=user,
            )
            started = time.monotonic()
            try:
                argv = lease.__enter__()
            except Exception:
                raise
            self.owner._record_exec_start_timing(
                "exec_lease",
                (time.monotonic() - started) * 1000,
            )
            try:
                self.owner._attach_exec_lease(sandbox_id, lease)
            except Exception:
                lease.__exit__(None, None, None)
                raise
            return argv
        except Exception:
            self.owner._release_exec_start_lock(sandbox_id, start_lock)
            raise

    def exec_started(self, sandbox_id: str) -> None:
        """Release the start fence once runsc exec owns the child request.

        The fence closes the build/spawn race with park and delete. Keeping it
        for the child lifetime would make forced deletion impossible: the
        runsc client only exits after deletion, while deletion would wait for
        the client to release this fence.
        """

        self.owner._release_exec_start(sandbox_id)

    def exec_start_failed(self, sandbox_id: str) -> None:
        self.owner._release_exec_start(sandbox_id)


class DirectLifecycleAdapter:
    """Bridge existing exec-session streaming onto direct lifecycle ownership."""

    def __init__(self, owner: DirectNodeManagerAdapter) -> None:
        self.owner = owner
        self._coordinator = SandboxLifecycleCoordinator()

    def acquire_shared(self, sandbox_id: str) -> None:
        self._coordinator.acquire_shared(sandbox_id)
        try:
            registration = self.owner.service._require_registration(sandbox_id)
            with self.owner.service._lock(
                sandbox_id,
                registration.sandbox_generation,
            ):
                self.owner.service.mark_activity(
                    sandbox_id,
                    registration.sandbox_generation,
                )
                timings = self.owner.service.ensure_running_with_timings(
                    registration.to_direct_sandbox()
                )
            self.owner._set_exec_start_timings(timings)
        except Exception:
            self._coordinator.release_shared(sandbox_id)
            raise

    def release_shared(self, sandbox_id: str) -> None:
        registration = self.owner.service.provisioner.registry.get(sandbox_id)
        if registration is not None:
            self.owner.service.mark_activity(
                sandbox_id,
                registration.sandbox_generation,
            )
        self._coordinator.release_shared(sandbox_id)

    @contextmanager
    def shared(self, sandbox_id: str) -> Iterator[None]:
        self.acquire_shared(sandbox_id)
        try:
            yield
        finally:
            self.release_shared(sandbox_id)

    @contextmanager
    def exclusive(
        self,
        sandbox_id: str,
        *,
        allow_shared: bool = False,
    ) -> Iterator[None]:
        with self._coordinator.exclusive(
            sandbox_id,
            allow_shared=allow_shared,
        ):
            yield


class DirectNodeManagerAdapter:
    """Temporary wire-compatible facade while the legacy manager is removed."""

    def __init__(
        self,
        service: DirectSandboxService,
        *,
        state_store: DirectNodeStateStore | None = None,
    ) -> None:
        self.service = service
        self.lifecycle = DirectLifecycleAdapter(self)
        self.runtime = DirectExecRuntimeAdapter(self)
        self._exec_leases: dict[str, object] = {}
        self._exec_start_locks: dict[str, Lock] = {}
        self._exec_start_users: dict[str, int] = {}
        self._activity_guard = Lock()
        self._exec_start_state = local()
        self._drain_guard = RLock()
        self._state_store = state_store or DirectNodeStateStore(
            service.provisioner.registry.path.parent / "direct-node-state.json"
        )
        self._drain = self._state_store.load_drain()
        if self._drain.draining:
            self.service.close_admission()
        else:
            self.service.open_admission()

    def create_with_timings(
        self,
        spec: SandboxSpec,
        *,
        operation: SandboxOperation | None = None,
    ) -> tuple[SandboxRecord, CommandResult, dict[str, object]]:
        existing = self.service.get(spec.id)
        started = time.monotonic()
        record = self.service.create(spec, operation=operation)
        return (
            record,
            CommandResult(("direct-warden", "create", spec.id), 0),
            {
                "idempotent": existing is not None and existing == record,
                "total_ms": max(0, int((time.monotonic() - started) * 1000)),
            },
        )

    def delete(
        self,
        sandbox_id: str,
        *,
        generation: int = 0,
        operation_id: str = "",
    ) -> tuple[SandboxRecord | None, CommandResult]:
        del operation_id
        record = self.service.get(sandbox_id)
        if record is not None and record.generation != generation:
            raise SandboxConflictError("delete generation does not own direct sandbox")
        # Deletion is a hard revocation boundary. It closes new activity but is
        # allowed to sever attached execs, matching the legacy runtime contract.
        with self.lifecycle.exclusive(sandbox_id, allow_shared=True):
            self.service.delete(
                sandbox_id,
                generation=generation if record else None,
            )
        return record, CommandResult(("direct-warden", "delete", sandbox_id), 0)

    def get(self, sandbox_id: str) -> SandboxRecord | None:
        return self.service.get(sandbox_id)

    def list(self) -> list[SandboxRecord]:
        self.cleanup_expired(blocking=False)
        return list(self.service.list_snapshot())

    def park(
        self,
        sandbox_id: str,
        *,
        operation_id: str | None = None,
        background: bool = False,
    ) -> SandboxRecord:
        with self.lifecycle.exclusive(sandbox_id):
            return self.service.park(
                sandbox_id,
                operation_id=operation_id,
                background=background,
            )

    def wake(
        self,
        sandbox_id: str,
        *,
        generation: int | None = None,
        operation_id: str | None = None,
    ) -> SandboxRecord:
        with self.lifecycle.exclusive(sandbox_id):
            return self.service.wake(
                sandbox_id,
                generation=generation,
                operation_id=operation_id,
            )

    def start_managed_process(
        self,
        sandbox_id: str,
        spec: ManagedProcessStart,
    ) -> ManagedProcessRecord:
        with self.lifecycle.shared(sandbox_id):
            return self.service.start_managed_process(sandbox_id, spec)

    def managed_process_status(
        self,
        sandbox_id: str,
        job_id: str,
    ) -> ManagedProcessRecord:
        with self.lifecycle.shared(sandbox_id):
            return self.service.managed_process_status(sandbox_id, job_id)

    def managed_process_logs(
        self,
        sandbox_id: str,
        job_id: str,
        *,
        stream: str,
        offset: int,
        limit: int,
    ) -> ManagedProcessLogChunk:
        with self.lifecycle.shared(sandbox_id):
            return self.service.managed_process_logs(
                sandbox_id,
                job_id,
                stream=stream,
                offset=offset,
                limit=limit,
            )

    def signal_managed_process(
        self,
        sandbox_id: str,
        job_id: str,
        *,
        signal: int,
    ) -> ManagedProcessRecord:
        with self.lifecycle.shared(sandbox_id):
            return self.service.signal_managed_process(
                sandbox_id,
                job_id,
                signal=signal,
            )

    def require_activity_sandbox(self, sandbox_id: str) -> SandboxRecord:
        record = self.service.get(sandbox_id)
        if record is None:
            raise ValueError(f"sandbox not found: {sandbox_id}")
        return record

    def consume_exec_start_timings(self) -> dict[str, float]:
        timings = dict(getattr(self._exec_start_state, "timings", {}))
        self._exec_start_state.timings = {}
        return timings

    def _set_exec_start_timings(self, timings: dict[str, float]) -> None:
        self._exec_start_state.timings = dict(timings)

    def _record_exec_start_timing(self, name: str, value: float) -> None:
        timings = dict(getattr(self._exec_start_state, "timings", {}))
        timings[name] = value
        self._exec_start_state.timings = timings

    def upload_file(
        self,
        sandbox_id: str,
        path: str,
        content: bytes,
    ) -> CommandResult:
        self.service.write_file(sandbox_id, path, content)
        return CommandResult(("direct-warden", "write", sandbox_id, path), 0)

    def download_file(
        self,
        sandbox_id: str,
        path: str,
        *,
        max_bytes: int,
    ) -> tuple[bytes, CommandResult]:
        content = self.service.read_file(sandbox_id, path, max_bytes=max_bytes)
        return content, CommandResult(
            ("direct-warden", "read", sandbox_id, path),
            0,
            stdout_bytes=content,
        )

    def fork_with_timings(self, *args, **kwargs):
        del args, kwargs
        raise SandboxForkUnsupportedError("fork is deferred from the direct runtime")

    def fork_many_with_timings(self, *args, **kwargs):
        del args, kwargs
        raise SandboxForkUnsupportedError("fork is deferred from the direct runtime")

    def snapshot(self, *args, **kwargs):
        del args, kwargs
        raise RuntimeError("image snapshot is not implemented by the direct runtime")

    def cleanup_expired(self, *, blocking: bool = True) -> list[SandboxRecord]:
        records = self.service.list() if blocking else self.service.list_snapshot()
        expired = [record for record in records if record.is_expired()]
        for record in expired:
            if blocking:
                self.service.delete(record.spec.id, generation=record.generation)
            else:
                self.service.try_delete(
                    record.spec.id,
                    generation=record.generation,
                )
        return expired

    def configure_drain(
        self,
        token: str,
        draining: bool,
        *,
        active_build_count,
    ) -> NodeDrainSnapshot:
        token = token.strip()
        if not token or not OPERATION_ID_RE.fullmatch(token):
            raise ValueError("drain token contains unsupported characters")
        with self._drain_guard:
            current = self._drain
            if draining:
                if current.draining and current.token != token:
                    raise SandboxConflictError("node is draining with another token")
                if not current.draining:
                    self.service.close_admission()
                    self._drain = NodeDrainState(
                        draining=True,
                        token=token,
                        drain_activity_epoch=0,
                        admission_open=False,
                    )
                    self._state_store.save_drain(self._drain)
            else:
                if current.draining and current.token != token:
                    raise SandboxConflictError("node is not draining with this token")
                if not current.draining and current.token != token:
                    raise SandboxConflictError("node is not draining with this token")
                if current.draining:
                    self.service.open_admission()
                    self._drain = NodeDrainState(
                        draining=False,
                        token=token,
                        admission_open=True,
                    )
                    self._state_store.save_drain(self._drain)
        return self.heartbeat_snapshot(active_build_count=active_build_count)

    @contextmanager
    def image_operation(self, image_manager):
        """Admit image pull/materialization atomically against node drain."""

        with self._drain_guard:
            if self._drain.draining or not self.service.admission_open:
                raise SandboxAdmissionClosedError(
                    "direct node admission is closed"
                )
            operation = image_manager.image_operation()
            operation.__enter__()
        try:
            yield
        finally:
            operation.__exit__(None, None, None)

    def heartbeat_snapshot(self, *, active_build_count) -> NodeDrainSnapshot:
        self.cleanup_expired(blocking=False)
        # Drain admission and the empty proof share this lock. A heartbeat
        # that began just before drain therefore cannot publish an empty proof
        # from observations taken before admission closed.
        with self._drain_guard:
            return self._heartbeat_snapshot_locked(
                active_build_count=active_build_count,
            )

    def _heartbeat_snapshot_locked(self, *, active_build_count) -> NodeDrainSnapshot:
        # Read transient operations before durable inventory. During drain,
        # admission has already been atomically closed, so this ordering cannot
        # miss a create transitioning from pre-registry work into the registry.
        active_reservations, transient_epoch = (
            self.service.active_reservations_snapshot()
        )
        records = self.service.list_snapshot()
        registered_keys = {
            (record.spec.id, record.generation) for record in records
        }
        used = ResourceQuantity()
        reserved = ResourceQuantity()
        for record in records:
            registration = self.service.provisioner.registry.get(record.spec.id)
            quota_disk = (
                registration.quota_total_mb
                if registration is not None and registration.quota_total_mb is not None
                else record.spec.disk_mb or 0
            )
            disk_charged = True
            if getattr(self.service.warden, "storage", None) is not None:
                if registration is None:
                    raise RuntimeError(
                        "direct activity has no storage-native registration"
                    )
                # Planned and quota-ready registrations are valid, durable
                # create reservations but do not own a runsc sandbox yet.
                # They must remain visible in heartbeat capacity accounting
                # without making the entire node heartbeat fail while a cold
                # image is materialized.
                if registration.has_direct_sandbox:
                    storage = self.service.warden._storage_record(
                        registration.to_direct_sandbox()
                    )
                    disk_charged = storage.get("state") != "published"
            resources = ResourceQuantity(
                disk_mb=quota_disk if disk_charged else 0
            )
            if record.state == "running":
                # Direct-runtime CPU and memory limits bound an individual
                # sandbox; they are not permanent node reservations. Actual
                # host consumption and pressure are reported separately in
                # runtime_metrics. Disk remains additive and hard.
                used = used + resources
            elif record.state not in {"parked"}:
                reserved = reserved + ResourceQuantity(
                    vcpu=record.spec.cpus or 0,
                    memory_mb=record.spec.memory_mb or 0,
                    disk_mb=quota_disk if disk_charged else 0,
                )
            else:
                used = used + resources
        for key, resources in active_reservations.items():
            if key in registered_keys:
                continue
            # The gateway route owns the exact hard-disk reservation before a
            # node registry record exists. Only transient CPU/RAM are added
            # here, avoiding duplicate disk charging during placement.
            reserved = reserved + ResourceQuantity(
                vcpu=resources.vcpu,
                memory_mb=resources.memory_mb,
            )
        revision = max(
            (item.revision for item in self.service.provisioner.registry.list()),
            default=0,
        ) + transient_epoch
        activity = SandboxActivitySnapshot(
            records=records,
            active_sandboxes=sum(record.state == "running" for record in records),
            used_resources=used,
            reserved_resources=reserved,
            activity_revision=revision,
            active_operations=len(active_reservations),
        )
        build_count = max(0, active_build_count())
        drain = self._drain
        if (
            drain.draining
            and not records
            and activity.active_operations == 0
            and build_count == 0
            and drain.drain_activity_epoch != revision
        ):
            drain = replace(drain, drain_activity_epoch=revision)
            self._drain = drain
            self._state_store.save_drain(drain)
        return NodeDrainSnapshot(activity, drain, build_count)

    def _attach_exec_lease(self, sandbox_id: str, lease: object) -> None:
        with self._activity_guard:
            if sandbox_id in self._exec_leases:
                raise RuntimeError("direct sandbox already has an exec lease")
            self._exec_leases[sandbox_id] = lease

    def _acquire_exec_start(self, sandbox_id: str) -> Lock:
        with self._activity_guard:
            lock = self._exec_start_locks.setdefault(sandbox_id, Lock())
            self._exec_start_users[sandbox_id] = (
                self._exec_start_users.get(sandbox_id, 0) + 1
            )
        lock.acquire()
        return lock

    def _release_exec_start_lock(self, sandbox_id: str, lock: Lock) -> None:
        lock.release()
        with self._activity_guard:
            users = self._exec_start_users.get(sandbox_id, 0) - 1
            if users <= 0:
                self._exec_start_users.pop(sandbox_id, None)
                if self._exec_start_locks.get(sandbox_id) is lock:
                    self._exec_start_locks.pop(sandbox_id, None)
            else:
                self._exec_start_users[sandbox_id] = users

    def _pop_exec_lease(self, sandbox_id: str):
        with self._activity_guard:
            return self._exec_leases.pop(sandbox_id, None)

    def _release_exec_start(self, sandbox_id: str) -> None:
        lease = self._pop_exec_lease(sandbox_id)
        if lease is None:
            return
        with self._activity_guard:
            lock = self._exec_start_locks.get(sandbox_id)
        if lock is None:
            raise RuntimeError("direct exec start lock is unavailable")
        try:
            lease.__exit__(None, None, None)
        finally:
            self._release_exec_start_lock(sandbox_id, lock)
