from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from threading import Event, Lock, RLock, Thread, local
from typing import Iterator
from uuid import uuid4

from .direct_service import DirectSandboxService
from .managed_process import (
    ManagedProcessLogChunk,
    ManagedProcessRecord,
    ManagedProcessStart,
)
from .models import ResourceQuantity
from .sandbox import (
    OPERATION_ID_RE,
    NodeDrainSnapshot,
    NodeDrainState,
    SandboxActivitySnapshot,
    SandboxAdmissionClosedError,
    SandboxBusyError,
    SandboxConflictError,
    SandboxLifecycleCoordinator,
    SandboxOperation,
    SandboxSnapshotPublicationPendingError,
    SandboxRecord,
    SandboxSpec,
    _atomic_write_json,
)


class NodeStateStore:
    """Small crash-durable node state that is independent of sandbox ownership."""

    VERSION = 1

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("node state path must be absolute")
        self.path = path
        self._lock = Lock()

    def load_drain(self) -> NodeDrainState:
        with self._lock:
            if not self.path.exists():
                return NodeDrainState()
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict) or set(raw) != {"version", "drain"}:
                raise ValueError("node state has an invalid schema")
            if raw["version"] != self.VERSION:
                raise ValueError("node state has an unsupported version")
            return NodeDrainState.from_dict(raw["drain"])

    def save_drain(self, drain: NodeDrainState) -> None:
        with self._lock:
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            _atomic_write_json(
                self.path,
                {"drain": drain.to_dict(), "version": self.VERSION},
            )


class BuilderNodeRuntime:
    """Durable drain and admission state for an image-builder node."""

    def __init__(self, state_store: NodeStateStore) -> None:
        self._state_store = state_store
        self._drain_guard = RLock()
        self._drain = state_store.load_drain()

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
            if current.draining and current.token != token:
                raise SandboxConflictError("node is draining with another token")
            if not draining and current.token != token:
                raise SandboxConflictError("node is not draining with this token")
            self._drain = NodeDrainState(
                draining=draining,
                token=token,
                drain_activity_epoch=0,
                admission_open=not draining,
            )
            self._state_store.save_drain(self._drain)
            return self._heartbeat_snapshot_locked(
                active_build_count=active_build_count,
            )

    @contextmanager
    def image_operation(self, image_manager):
        with self._drain_guard:
            if self._drain.draining:
                raise SandboxAdmissionClosedError("builder node admission is closed")
            operation = image_manager.image_operation()
            operation.__enter__()
        try:
            yield
        finally:
            operation.__exit__(None, None, None)

    def heartbeat_snapshot(self, *, active_build_count) -> NodeDrainSnapshot:
        with self._drain_guard:
            return self._heartbeat_snapshot_locked(
                active_build_count=active_build_count,
            )

    def _heartbeat_snapshot_locked(self, *, active_build_count) -> NodeDrainSnapshot:
        activity = SandboxActivitySnapshot(
            records=(),
            active_sandboxes=0,
            used_resources=ResourceQuantity(),
            reserved_resources=ResourceQuantity(),
            activity_revision=0,
        )
        return NodeDrainSnapshot(
            activity=activity,
            drain=self._drain,
            active_image_builds=max(0, active_build_count()),
        )


class DirectExecRuntime:
    def __init__(self, owner: DirectNodeRuntime) -> None:
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


class DirectLifecycle:
    def __init__(self, owner: DirectNodeRuntime) -> None:
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
        join_transition: bool = False,
        transition_timeout_seconds: float | None = None,
    ) -> Iterator[None]:
        with self._coordinator.exclusive(
            sandbox_id,
            allow_shared=allow_shared,
            join_transition=join_transition,
            transition_timeout_seconds=transition_timeout_seconds,
        ):
            yield


class DirectNodeRuntime:
    """Node-level orchestration for the direct sandbox service."""

    def __init__(
        self,
        service: DirectSandboxService,
    ) -> None:
        self.service = service
        self.lifecycle = DirectLifecycle(self)
        self.runtime = DirectExecRuntime(self)
        self._exec_leases: dict[str, object] = {}
        self._exec_start_locks: dict[str, Lock] = {}
        self._exec_start_users: dict[str, int] = {}
        self._activity_guard = Lock()
        self._exec_start_state = local()
        self._drain_guard = RLock()
        self._registry = service.provisioner.registry
        self._drain = self._registry.load_drain()
        if self._drain.draining:
            self.service.close_admission()
        else:
            self.service.open_admission()
        self._background_stop = Event()
        self._idle_parking_thread: Thread | None = None

    def start(self) -> None:
        idle_seconds = self.service.idle_park_seconds
        if idle_seconds <= 0 or (
            self._idle_parking_thread is not None
            and self._idle_parking_thread.is_alive()
        ):
            return
        self._background_stop.clear()
        self._idle_parking_thread = Thread(
            target=self._idle_parking_loop,
            name="ucloud-direct-idle-parker",
            daemon=True,
        )
        self._idle_parking_thread.start()

    def stop(self) -> None:
        self._background_stop.set()
        thread = self._idle_parking_thread
        if thread is not None:
            thread.join(timeout=max(2.0, self.service.idle_park_seconds * 2))
        self._idle_parking_thread = None

    def _idle_parking_loop(self) -> None:
        idle_seconds = self.service.idle_park_seconds
        interval = min(1.0, max(0.05, idle_seconds / 4))
        while not self._background_stop.wait(interval):
            now = time.monotonic()
            for registration in self.service.provisioner.registry.list():
                # Managed agents own their park points through the SDK/relay
                # model-wait protocol. Local request inactivity is not evidence
                # that their primary process is idle.
                if (
                    registration.phase != "owned"
                    or not registration.spec.parkable
                    or registration.spec.managed_process
                ):
                    continue
                if (
                    self.service.idle_for_seconds(
                        registration.sandbox_id,
                        registration.sandbox_generation,
                        now=now,
                    )
                    < idle_seconds
                ):
                    continue
                record = self.service.get(registration.sandbox_id)
                if record is None or record.state != "running":
                    continue
                try:
                    self.park(
                        registration.sandbox_id,
                        operation_id=f"idle-park:{uuid4().hex}",
                        background=True,
                    )
                except (RuntimeError, ValueError):
                    # The normal lifecycle fence rejects concurrent activity.
                    # Persistent failures remain visible through node health and
                    # lifecycle reconciliation; one rejected timer tick is safe.
                    continue

    def create_with_timings(
        self,
        spec: SandboxSpec,
        *,
        operation: SandboxOperation,
    ) -> tuple[SandboxRecord, dict[str, object]]:
        existing = self.service.get(spec.id)
        started = time.monotonic()
        record = self.service.create(spec, operation=operation)
        return (
            record,
            {
                "idempotent": existing is not None and existing == record,
                "total_ms": max(0, int((time.monotonic() - started) * 1000)),
            },
        )

    def delete(
        self,
        sandbox_id: str,
        *,
        generation: int,
        operation_id: str,
    ) -> SandboxRecord | None:
        if generation <= 0:
            raise ValueError("delete generation must be positive")
        if not isinstance(operation_id, str) or not OPERATION_ID_RE.fullmatch(
            operation_id
        ):
            raise ValueError("delete operation id is invalid")
        record = self.service.get(sandbox_id)
        if record is not None and record.generation != generation:
            raise SandboxConflictError("delete generation does not own direct sandbox")
        # Deletion is a hard revocation boundary. It closes new activity but is
        # allowed to sever attached exec sessions during sandbox deletion.
        with self.lifecycle.exclusive(sandbox_id, allow_shared=True):
            self.service.delete(
                sandbox_id,
                generation=generation,
            )
        return record

    def get(self, sandbox_id: str) -> SandboxRecord | None:
        return self.service.get(sandbox_id)

    def list(self) -> list[SandboxRecord]:
        self.cleanup_expired(blocking=False)
        return list(self.service.list_snapshot())

    def park(
        self,
        sandbox_id: str,
        *,
        operation_id: str,
        background: bool = False,
    ) -> SandboxRecord:
        if not isinstance(operation_id, str) or not OPERATION_ID_RE.fullmatch(
            operation_id
        ):
            raise ValueError("park operation id is invalid")
        try:
            # Join a concurrent park/wake and then re-evaluate the stable
            # runtime state. This makes exact replays and crossed lifecycle
            # calls idempotent without weakening the attached-activity fence.
            with self.lifecycle.exclusive(
                sandbox_id,
                join_transition=True,
                transition_timeout_seconds=60.0,
            ):
                return self.service.park(
                    sandbox_id,
                    operation_id=operation_id,
                    background=background,
                )
        except SandboxBusyError as exc:
            raise SandboxBusyError(
                "sandbox has active exec/file activity that cannot survive park: "
                f"{sandbox_id}; launch a long-lived agent in a managed_process "
                "sandbox through the SDK start_agent() API"
            ) from exc

    def wake(
        self,
        sandbox_id: str,
        *,
        generation: int,
        operation_id: str,
    ) -> SandboxRecord:
        if generation <= 0:
            raise ValueError("wake generation must be positive")
        if not isinstance(operation_id, str) or not OPERATION_ID_RE.fullmatch(
            operation_id
        ):
            raise ValueError("wake operation id is invalid")
        # Waking an already-running sandbox is a successful no-op. Attached
        # activity is proof that the current runtime is live, not a reason to
        # reject that idempotent result. We still take the exclusive transition
        # fence so a concurrent park completes first and is then re-evaluated.
        with self.lifecycle.exclusive(
            sandbox_id,
            allow_shared=True,
            join_transition=True,
            transition_timeout_seconds=60.0,
        ):
            if self.service.storage_native_publication_pending(sandbox_id):
                raise SandboxSnapshotPublicationPendingError(
                    "parked snapshot publication is still in progress"
                )
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

    def acquire_exec_capacity(self, sandbox_id: str) -> str:
        registration = self.service._require_registration(sandbox_id)
        return self.service.acquire_exec_capacity(
            sandbox_id,
            registration.sandbox_generation,
        )

    def release_exec_capacity(self, token: str) -> None:
        self.service.release_exec_capacity(token)

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
    ) -> None:
        self.service.write_file(sandbox_id, path, content)

    def download_file(
        self,
        sandbox_id: str,
        path: str,
        *,
        max_bytes: int,
    ) -> bytes:
        return self.service.read_file(sandbox_id, path, max_bytes=max_bytes)

    def cleanup_expired(self, *, blocking: bool = True) -> list[SandboxRecord]:
        records = self.service.list() if blocking else self.service.list_snapshot()
        return self._cleanup_expired_records(records, blocking=blocking)

    def _cleanup_expired_records(
        self,
        records: tuple[SandboxRecord, ...] | list[SandboxRecord],
        *,
        blocking: bool,
    ) -> list[SandboxRecord]:
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
                    self._registry.save_drain(self._drain)
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
                    self._registry.save_drain(self._drain)
        return self.heartbeat_snapshot(active_build_count=active_build_count)

    @contextmanager
    def image_operation(self, image_manager):
        """Admit image pull/materialization atomically against node drain."""

        with self._drain_guard:
            if self._drain.draining or not self.service.admission_open:
                raise SandboxAdmissionClosedError("direct node admission is closed")
            operation = image_manager.image_operation()
            operation.__enter__()
        try:
            yield
        finally:
            operation.__exit__(None, None, None)

    def heartbeat_snapshot(self, *, active_build_count) -> NodeDrainSnapshot:
        # Drain admission and the empty proof share this lock. A heartbeat
        # that began just before drain therefore cannot publish an empty proof
        # from observations taken before admission closed.
        with self._drain_guard:
            snapshot = self._heartbeat_snapshot_locked(
                active_build_count=active_build_count,
            )
        # Expiry is deliberately reconciled after publishing this conservative
        # snapshot. Deleting before the empty proof would require a second
        # registry parse; counting an expired sandbox for one extra heartbeat is
        # safe, while publishing an inventory assembled before drain admission
        # closed is not.
        self._cleanup_expired_records(snapshot.activity.records, blocking=False)
        return snapshot

    def _heartbeat_snapshot_locked(self, *, active_build_count) -> NodeDrainSnapshot:
        # Read transient operations before durable inventory. During drain,
        # admission has already been atomically closed, so this ordering cannot
        # miss a create transitioning from pre-registry work into the registry.
        active_reservations, transient_epoch = (
            self.service.active_reservations_snapshot()
        )
        inventory = self.service.inventory_snapshot()
        records = inventory.records
        registered_keys = {(record.spec.id, record.generation) for record in records}
        direct_sandboxes = tuple(
            item.registration.to_direct_sandbox()
            for item in inventory.items
            if item.registration.has_direct_sandbox
        )
        storage_records = self.service.warden.storage_records_snapshot(direct_sandboxes)
        used = ResourceQuantity()
        reserved = ResourceQuantity()
        for item in inventory.items:
            record = item.record
            registration = item.registration
            quota_disk = (
                registration.quota_total_mb
                if registration.quota_total_mb is not None
                else record.spec.disk_mb or 0
            )
            disk_charged = True
            # Planned and quota-ready registrations are valid, durable create
            # reservations but do not own a runsc sandbox yet. They remain
            # visible in heartbeat capacity accounting while a cold image is
            # materialized.
            if registration.has_direct_sandbox:
                storage = storage_records[registration.memory_directory]
                disk_charged = storage.state.value != "published"
            resources = ResourceQuantity(disk_mb=quota_disk if disk_charged else 0)
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
        revision = inventory.activity_revision + transient_epoch
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
            self._registry.save_drain(drain)
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
