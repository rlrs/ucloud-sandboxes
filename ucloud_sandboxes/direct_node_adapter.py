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
from .models import ResourceQuantity
from .sandbox import (
    CommandResult,
    NodeDrainSnapshot,
    NodeDrainState,
    OPERATION_ID_RE,
    SandboxActivitySnapshot,
    SandboxConflictError,
    SandboxForkUnsupportedError,
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
        encoded = (
            json.dumps(payload, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
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
        self.owner._attach_exec_lease(sandbox_id, lease)
        return argv


class DirectLifecycleAdapter:
    """Bridge existing exec-session streaming onto direct lifecycle ownership."""

    def __init__(self, owner: DirectNodeManagerAdapter) -> None:
        self.owner = owner

    def acquire_shared(self, sandbox_id: str) -> None:
        registration = self.owner.service._require_registration(sandbox_id)
        lock = self.owner.service._lock(
            sandbox_id,
            registration.sandbox_generation,
        )
        lock.acquire()
        try:
            self.owner.service.mark_activity(
                sandbox_id,
                registration.sandbox_generation,
            )
            timings = self.owner.service.ensure_running_with_timings(
                registration.to_direct_sandbox()
            )
            self.owner._set_exec_start_timings(timings)
            self.owner._attach_activity_lock(sandbox_id, lock)
        except Exception:
            lock.release()
            raise

    def release_shared(self, sandbox_id: str) -> None:
        lease = self.owner._pop_exec_lease(sandbox_id)
        try:
            if lease is not None:
                lease.__exit__(None, None, None)
        finally:
            lock = self.owner._pop_activity_lock(sandbox_id)
            if lock is not None:
                registration = self.owner.service.provisioner.registry.get(
                    sandbox_id
                )
                if registration is not None:
                    self.owner.service.mark_activity(
                        sandbox_id,
                        registration.sandbox_generation,
                    )
                lock.release()

    @contextmanager
    def shared(self, sandbox_id: str) -> Iterator[None]:
        self.acquire_shared(sandbox_id)
        try:
            yield
        finally:
            self.release_shared(sandbox_id)


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
        self._activity_locks: dict[str, Lock] = {}
        self._exec_leases: dict[str, object] = {}
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
            raise SandboxConflictError(
                "delete generation does not own direct sandbox"
            )
        self.service.delete(sandbox_id, generation=generation if record else None)
        return record, CommandResult(("direct-warden", "delete", sandbox_id), 0)

    def get(self, sandbox_id: str) -> SandboxRecord | None:
        return self.service.get(sandbox_id)

    def list(self) -> list[SandboxRecord]:
        self.cleanup_expired()
        return list(self.service.list())

    def park(
        self,
        sandbox_id: str,
        *,
        operation_id: str | None = None,
    ) -> SandboxRecord:
        return self.service.park(sandbox_id, operation_id=operation_id)

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
        raise SandboxForkUnsupportedError(
            "fork is deferred from the direct runtime"
        )

    def fork_many_with_timings(self, *args, **kwargs):
        del args, kwargs
        raise SandboxForkUnsupportedError(
            "fork is deferred from the direct runtime"
        )

    def snapshot(self, *args, **kwargs):
        del args, kwargs
        raise RuntimeError("image snapshot is not implemented by the direct runtime")

    def cleanup_expired(self) -> list[SandboxRecord]:
        expired = [record for record in self.service.list() if record.is_expired()]
        for record in expired:
            self.service.delete(record.spec.id, generation=record.generation)
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

    def heartbeat_snapshot(self, *, active_build_count) -> NodeDrainSnapshot:
        self.cleanup_expired()
        records = self.service.list()
        used = ResourceQuantity()
        reserved = ResourceQuantity()
        for record in records:
            registration = self.service.provisioner.registry.get(record.spec.id)
            quota_disk = (
                registration.quota_total_mb
                if registration is not None and registration.quota_total_mb is not None
                else record.spec.disk_mb or 0
            )
            resources = ResourceQuantity(disk_mb=quota_disk)
            if record.state == "running":
                resources = replace(
                    resources,
                    vcpu=record.spec.cpus or 0,
                    memory_mb=record.spec.memory_mb or 0,
                )
                used = used + resources
            elif record.state not in {"parked"}:
                reserved = reserved + ResourceQuantity(
                    vcpu=record.spec.cpus or 0,
                    memory_mb=record.spec.memory_mb or 0,
                    disk_mb=quota_disk,
                )
            else:
                used = used + resources
        revision = max(
            (
                item.revision
                for item in self.service.provisioner.registry.list()
            ),
            default=0,
        )
        activity = SandboxActivitySnapshot(
            records=records,
            active_sandboxes=sum(record.state == "running" for record in records),
            used_resources=used,
            reserved_resources=reserved,
            activity_revision=revision,
        )
        build_count = max(0, active_build_count())
        with self._drain_guard:
            drain = self._drain
            if (
                drain.draining
                and not records
                and build_count == 0
                and drain.drain_activity_epoch != revision
            ):
                drain = replace(drain, drain_activity_epoch=revision)
                self._drain = drain
                self._state_store.save_drain(drain)
        return NodeDrainSnapshot(activity, drain, build_count)

    def _attach_activity_lock(self, sandbox_id: str, lock: Lock) -> None:
        with self._activity_guard:
            if sandbox_id in self._activity_locks:
                raise RuntimeError("direct sandbox already has attached activity")
            self._activity_locks[sandbox_id] = lock

    def _pop_activity_lock(self, sandbox_id: str) -> Lock | None:
        with self._activity_guard:
            return self._activity_locks.pop(sandbox_id, None)

    def _attach_exec_lease(self, sandbox_id: str, lease: object) -> None:
        with self._activity_guard:
            if sandbox_id in self._exec_leases:
                raise RuntimeError("direct sandbox already has an exec lease")
            self._exec_leases[sandbox_id] = lease

    def _pop_exec_lease(self, sandbox_id: str):
        with self._activity_guard:
            return self._exec_leases.pop(sandbox_id, None)
