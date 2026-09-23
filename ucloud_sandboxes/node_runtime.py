from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
import time
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from pathlib import Path
from threading import Event, Lock, RLock, Thread, local
from typing import BinaryIO, Iterator
from uuid import uuid4

from .background_io import PressureSampler
from .direct_service import DirectSandboxService
from .direct_registry import DirectRegistryConflictError
from .warm_park import WarmParkDeferred, WarmParkPolicy
from .transition_admission import MemoryDemand
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
    SandboxExecAdmissionDeferredError,
    SandboxLifecycleCoordinator,
    SandboxOperation,
    SandboxRecord,
    SandboxSpec,
    _atomic_write_json,
    compose_activity_revision,
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

    def is_idle(self, sandbox_id: str) -> bool:
        return self._coordinator.is_idle(sandbox_id)

    def acquire_shared(self, sandbox_id: str) -> None:
        # A parked guest can race a tool request with a local deferred park.
        # Queue before accepting exec/file activity, then re-read registration
        # and restore under its generation lock. No command has been launched,
        # so a bounded wait expiring is safe for the gateway to reschedule.
        try:
            self._coordinator.acquire_shared(
                sandbox_id,
                join_transition=True,
                transition_timeout_seconds=getattr(self.owner.service, "admission_wait_seconds", 30.0),
            )
        except SandboxBusyError as exc:
            raise SandboxExecAdmissionDeferredError(str(exc)) from exc
        try:
            registration = self.owner.service._require_registration(sandbox_id)
            with self.owner.service._request_lock(
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
        try:
            registration = self.owner.service.provisioner.registry.get(sandbox_id)
            if registration is not None:
                self.owner.service.mark_activity(
                    sandbox_id,
                    registration.sandbox_generation,
                )
        finally:
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
        memory_backing_root = getattr(
            getattr(getattr(service, "warden", None), "config", None),
            "application_memory_root", None,
        )
        self._warm_parks = WarmParkPolicy(
            PressureSampler(memory_backing_root=memory_backing_root).sample,
            demand=getattr(service, "warm_park_demand", lambda: MemoryDemand()),
        )
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
        self._relay_parking_thread: Thread | None = None
        self._relay_parking_guard = Lock()
        self._deferred_relay_parks: dict[tuple, dict] = {}
        # Own one executor for local lifecycle rechecks, not one per request.
        # CPU parallelism bounds Python/kernel orchestration; reclaim byte
        # credits independently decide which checkpoints resources need.
        self._relay_park_workers = max(1, os.cpu_count() or 1)
        self._relay_park_executor = None
        self._relay_park_tasks = {}

    def start(self) -> None:
        self._background_stop.clear()
        if self._relay_parking_thread is None or not self._relay_parking_thread.is_alive():
            self._relay_parking_thread = Thread(
                target=self._relay_parking_loop,
                name="ucloud-direct-relay-parker", daemon=True,
            )
            self._relay_parking_thread.start()
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
        thread = self._relay_parking_thread
        if thread is not None:
            thread.join(timeout=2.0)
        self._relay_parking_thread = None
        with self._relay_parking_guard:
            executor, self._relay_park_executor = self._relay_park_executor, None
        if executor is not None:
            # Do not cancel an in-flight checkpoint mid-transaction. Queued
            # rechecks may be cancelled; their durable relay intent survives.
            executor.shutdown(wait=False, cancel_futures=True)

    def resident_wait_snapshot(self):
        return {**self._warm_parks.snapshot(), **self.service.resident_demand_snapshot()}

    def _relay_parking_loop(self) -> None:
        next_sample = 0.0
        while not self._background_stop.wait(0.25):
            if time.monotonic() >= next_sample:
                try:
                    sampler = getattr(self.service, "refresh_resident_memory", None)
                    if sampler is not None:
                        sampler()
                except (OSError, RuntimeError, ValueError):
                    pass  # Missing observations carry no projected reclaim credit.
                next_sample = time.monotonic() + 1.0
            self._recheck_relay_parks()

    def _recheck_relay_parks(self) -> None:
        # The relay retains durable intents. Local scheduling never changes
        # their generation/activity fences or grants lifecycle authority.
        with self._relay_parking_guard:
            for key, task in tuple(self._relay_park_tasks.items()):
                if task.done():
                    self._relay_park_tasks.pop(key)
            pending = tuple(self._deferred_relay_parks.items())
        for key, entry in pending:
            if self._background_stop.is_set():
                return
            sample = self._resident_wait_memory_sample(key)
            entry['memory_bytes'] = sample.current_bytes if sample is not None else 0
            ram_bytes = self.service.resident_memory_ram_bytes(key[0], key[1], sample)
            if time.monotonic() < entry['retry_at'] or not self._warm_parks.ready(
                key, memory_bytes=entry['memory_bytes'], ram_bytes=ram_bytes,
                application_file_bytes=self._resident_application_file_bytes(key, sample),
            ):
                continue
            with self._relay_parking_guard:
                if self._background_stop.is_set():
                    return
                if key in self._relay_park_tasks:
                    continue
                if len(self._relay_park_tasks) >= self._relay_park_workers:
                    return  # No unbounded executor queue or blocked HTTP caller.
                if self._deferred_relay_parks.get(key) is not entry:
                    continue
                if self._relay_park_executor is None:
                    self._relay_park_executor = ThreadPoolExecutor(
                        max_workers=self._relay_park_workers,
                        thread_name_prefix="resident-reclaim",
                    )
                self._relay_park_tasks[key] = self._relay_park_executor.submit(
                    self._recheck_relay_park, key, entry,
                )

    def _recheck_relay_park(self, key, entry) -> None:
        if self._background_stop.is_set():
            return
        with self._relay_parking_guard:
            if self._deferred_relay_parks.get(key) is not entry:
                return
        try:
            # Re-evaluate the byte budget after scheduling. A wake can win
            # while this task waits for a CPU worker, or headroom may recover.
            record, _ = self.park_with_activity_revision(
                key[0], generation=key[1], relay_request_id=key[2],
                operation_id=entry['operation_id'], background=entry['background'],
            )
        except WarmParkDeferred:
            return
        except (SandboxConflictError, DirectRegistryConflictError):
            record = None  # A durable wake/deletion/replacement wins.
            self._warm_parks.forget(key)
        except (RuntimeError, ValueError):
            entry['retry_at'] = time.monotonic() + 1.0
            return
        if record is not None and record.state == 'running':
            entry['retry_at'] = time.monotonic() + 1.0
            return
        with self._relay_parking_guard:
            if self._deferred_relay_parks.get(key) is entry:
                self._deferred_relay_parks.pop(key, None)

    def _idle_parking_loop(self) -> None:
        idle_seconds = self.service.idle_park_seconds
        interval = min(1.0, max(0.05, idle_seconds / 4))
        registry_revision = None
        candidates = ()
        while not self._background_stop.wait(interval):
            now = time.monotonic()
            registry = self.service.provisioner.registry
            if registry.activity_revision() != registry_revision:
                snapshot = registry.snapshot()
                # Managed agents own their park points through the SDK/relay
                # model-wait protocol. Local request inactivity is not evidence
                # that their primary process is idle.
                candidates = tuple(
                    registration for registration in snapshot.records
                    if registration.phase == 'owned' and registration.spec.parkable
                    and not registration.spec.managed_process
                )
                registry_revision = snapshot.activity_revision
            for registration in candidates:
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
        existing = self.service.get_snapshot(spec.id)
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
        with self._relay_parking_guard:
            for key in tuple(self._deferred_relay_parks):
                if key[:2] == (sandbox_id, generation):
                    self._deferred_relay_parks.pop(key, None)
                    self._warm_parks.forget(key)
        self._warm_parks.forget_incarnation(sandbox_id, generation)
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
        record, _activity_revision = self.park_with_activity_revision(
            sandbox_id,
            operation_id=operation_id,
            background=background,
        )
        return record

    def park_with_activity_revision(
        self,
        sandbox_id: str,
        *,
        operation_id: str,
        background: bool = False,
        relay_request_id: str | None = None,
        generation: int | None = None,
        resource_phase: dict | None = None,
    ) -> tuple[SandboxRecord, int]:
        if not isinstance(operation_id, str) or not OPERATION_ID_RE.fullmatch(
            operation_id
        ):
            raise ValueError("park operation id is invalid")
        if relay_request_id is not None and generation is None:
            raise ValueError("relay park requires generation")
        if relay_request_id is not None and self.service.provisioner.registry.relay_wake_fence(
            sandbox_id, generation, relay_request_id,
        ):
            raise SandboxConflictError("relay park was superseded by durable wake")
        if relay_request_id is not None:
            observe_wait = getattr(self.service, "observe_managed_wait", None)
            if observe_wait is not None:
                observe_wait(sandbox_id, generation, relay_request_id)
        key = (sandbox_id, generation, relay_request_id)
        if resource_phase is not None:
            if relay_request_id is None or generation is None:
                raise ValueError("resource phase requires a generation-bound relay park")
            self._warm_parks.observe_phase(key, resource_phase)
        memory_bytes = 0
        ram_bytes = None
        application_file_bytes = 0
        if relay_request_id is not None:
            sample = self._resident_wait_memory_sample(key)
            if sample is not None:
                memory_bytes = sample.current_bytes
            ram_bytes = self.service.resident_memory_ram_bytes(sandbox_id, generation, sample)
            application_file_bytes = self._resident_application_file_bytes(key, sample)
        snapshot = self.service.get_snapshot(sandbox_id) if hasattr(self.service, 'get_snapshot') else None
        delay = (
            self._warm_parks.defer(key, memory_bytes=memory_bytes, ram_bytes=ram_bytes,
                                   application_file_bytes=application_file_bytes, blocking=False)
            if relay_request_id is not None and (snapshot is None or snapshot.state == 'running')
            else nullcontext(None)
        )
        try:
            with delay as cancelled:
                if relay_request_id is not None and self._reclaim_wait_cache(key, sample, cancelled):
                    # Keep the same live wait. Actual MemAvailable decides
                    # whether another reclaim/park is needed after settling.
                    raise WarmParkDeferred(0.25)
                if relay_request_id is not None and not self._warm_parks.checkpoint_ready(key):
                    if cancelled is None or not cancelled.is_set():
                        raise WarmParkDeferred(0.25)
                # Join a concurrent park/wake and then re-evaluate the stable
                # runtime state. This makes exact replays and crossed lifecycle
                # calls idempotent without weakening the attached-activity fence.
                with self.lifecycle.exclusive(
                    sandbox_id,
                    join_transition=True,
                    transition_timeout_seconds=60.0,
                ):
                    if relay_request_id is not None:
                        if cancelled is not None and cancelled.is_set():
                            raise SandboxConflictError("relay park was superseded by wake")
                        if self.service.provisioner.registry.relay_wake_fence(sandbox_id, generation, relay_request_id):
                            raise SandboxConflictError("relay park was superseded by durable wake")
                    record = self.service.park(
                        sandbox_id,
                        operation_id=operation_id,
                        background=background,
                    )
                    if relay_request_id is not None and record.state == "parked":
                        self._warm_parks.parked(key)
                    activity_revision = self.service.advance_lifecycle_activity_revision()
                    return record, activity_revision
        except WarmParkDeferred as exc:
            thread = self._relay_parking_thread
            if thread is not None and thread.is_alive() and not self._background_stop.is_set():
                with self._relay_parking_guard:
                    if self._warm_parks.waiting(key):
                        self._deferred_relay_parks.setdefault(key, {
                            'memory_bytes': memory_bytes, 'operation_id': operation_id,
                            'background': background, 'retry_at': 0.0,
                        })
                # Pressure is checked locally every 250 ms. The durable retry
                # is a recovery backstop, not the resource scheduling timer.
                raise WarmParkDeferred(30.0) from exc
            raise
        except SandboxBusyError as exc:
            raise SandboxBusyError(
                "sandbox has active exec/file activity that cannot survive park: "
                f"{sandbox_id}; launch a long-lived agent in a managed_process "
                "sandbox through the SDK start_agent() API"
            ) from exc

    def _resident_wait_memory_sample(self, key):
        sample = (
            self.service.resident_memory_sample(key[0], key[1])
            if hasattr(self.service, 'resident_memory_sample') else None
        )
        if sample is None or not self._warm_parks.observed_after_wait(
            key, sample.sampled_at,
        ):
            return None
        return sample

    def _resident_application_file_bytes(self, key, sample):
        if sample is None or not getattr(
            self.service, 'resident_application_reclaim_enabled', lambda *_: False
        )(key[0], key[1]):
            return 0
        return max(0, min(sample.current_bytes,
                          sample.file_bytes - sample.shared_memory_bytes))

    def _reclaim_wait_cache(self, key, sample, cancelled):
        reclaim = getattr(self.service, "reclaim_resident_wait", None)
        if reclaim is None or sample is None or not self.lifecycle.is_idle(key[0]):
            return False
        file_backed = getattr(self.service, "resident_application_reclaim_enabled", lambda *_: False)(key[0], key[1])
        target = self._warm_parks.cache_reclaim_target(
            key, sample, application_file_backed=file_backed,
        )
        if not target:
            return False
        try:
            result = reclaim(
                key[0],
                generation=key[1],
                relay_request_id=key[2],
                target_bytes=target,
                is_wait_current=lambda: (
                    (cancelled is None or not cancelled.is_set())
                    and self.lifecycle.is_idle(key[0])
                    and self._warm_parks.cache_reclaim_still_needed(
                        key, application_file_backed=file_backed,
                    )
                ),
            )
        except (RuntimeError, ValueError, OSError):
            self._warm_parks.record_cache_reclaim(key, sample, None)
            # Cache reclaim is optional. A real deficit still progresses
            # through the canonical, fully fenced checkpoint path below.
            return False
        self._warm_parks.record_cache_reclaim(key, sample, result)
        return result.reclaimed_bytes >= 16 * 1024**2

    def wake(
        self,
        sandbox_id: str,
        *,
        generation: int,
        operation_id: str,
    ) -> SandboxRecord:
        record, _activity_revision = self.wake_with_activity_revision(
            sandbox_id,
            generation=generation,
            operation_id=operation_id,
        )
        return record

    def wake_with_activity_revision(
        self,
        sandbox_id: str,
        *,
        generation: int,
        operation_id: str,
        relay_request_id: str | None = None,
    ) -> tuple[SandboxRecord, int]:
        if generation <= 0:
            raise ValueError("wake generation must be positive")
        if not isinstance(operation_id, str) or not OPERATION_ID_RE.fullmatch(
            operation_id
        ):
            raise ValueError("wake operation id is invalid")
        # Keep the current safe wait reclaimable until growth is admitted.
        # Cancelling/fencing every wait before a response burst obtains memory
        # would leave only non-reclaimable queued continuations on a full node.
        if relay_request_id is not None:
            self._warm_parks.response_ready((sandbox_id, generation, relay_request_id))
            continuation = getattr(self.service, "admit_managed_continuation", None)
            if continuation is not None:
                continuation(sandbox_id, generation, relay_request_id)
        wake_observation = None
        if relay_request_id is not None:
            wake_observation = self._warm_parks.wake((sandbox_id, generation, relay_request_id))
            with self._relay_parking_guard:
                self._deferred_relay_parks.pop((sandbox_id, generation, relay_request_id), None)
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
            if relay_request_id is not None:
                self.service.provisioner.registry.relay_wake_fence(
                    sandbox_id, generation, relay_request_id, record=True,
                )
            # Local wake takes precedence over background publication. The
            # storage journal supersedes/fences the upload while retaining the
            # sealed checkpoint; an uploader thread is not an admission limit.
            record = self.service.wake(
                sandbox_id,
                generation=generation,
                operation_id=operation_id,
            )
            activity_revision = self.service.advance_lifecycle_activity_revision()
            if relay_request_id is not None:
                self._warm_parks.record_wake((sandbox_id, generation, relay_request_id), wake_observation)
            return record, activity_revision

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
        *,
        expected_generation: int | None = None,
    ) -> None:
        self.service.write_file(sandbox_id, path, content, expected_generation=expected_generation)

    def upload_file_from_file(
        self, sandbox_id: str, path: str, source: BinaryIO, size: int,
        *, expected_generation: int,
    ) -> None:
        self.service.write_file_from_file(
            sandbox_id, path, source, size, expected_generation=expected_generation,
        )

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
        transient = self.service.activity_snapshot()
        inventory = self.service.inventory_snapshot()
        records = inventory.records
        registered_keys = {(record.spec.id, record.generation) for record in records}
        direct_sandboxes = tuple(
            item.registration.to_direct_sandbox()
            for item in inventory.items
            if item.registration.has_direct_sandbox
        )
        storage_records = self.service.warden.storage_records_snapshot(direct_sandboxes)
        storage_dependencies: dict[str, dict] = {}
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
            storage_dependencies[record.spec.id] = {}
            charged_disk_mb = quota_disk
            # Planned and quota-ready registrations are valid, durable create
            # reservations but do not own a runsc sandbox yet. They remain
            # visible in heartbeat capacity accounting while a cold image is
            # materialized.
            if registration.has_direct_sandbox:
                storage = storage_records[registration.workspace_volume_id]
                if storage.state.value == "published":
                    # Publication releases the workspace volume only. Split
                    # checkpoint memory retains its local quota until deletion.
                    memory = registration.memory_reference
                    charged_disk_mb = ((memory.quota_bytes + 1024**2 - 1) // 1024**2
                                       if memory is not None else 0)
                if storage.published_layers:
                    storage_dependencies[record.spec.id] = (
                        storage.dependency_publication().to_dict()
                    )
            resources = ResourceQuantity(disk_mb=charged_disk_mb)
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
                    disk_mb=charged_disk_mb,
                )
            else:
                used = used + resources
        for key, resources in transient.resource_reservations.items():
            if key in registered_keys:
                continue
            # The gateway route owns the exact hard-disk reservation before a
            # node registry record exists. Only transient CPU/RAM are added
            # here, avoiding duplicate disk charging during placement.
            reserved = reserved + ResourceQuantity(
                vcpu=resources.vcpu,
                memory_mb=resources.memory_mb,
            )
        revision = compose_activity_revision(
            durable_revision=inventory.activity_revision,
            transient_revision=transient.activity_revision,
        )
        activity = SandboxActivitySnapshot(
            records=records,
            active_sandboxes=sum(record.state == "running" for record in records),
            used_resources=used,
            reserved_resources=reserved,
            activity_revision=revision,
            active_operations=transient.active_operations,
            active_sandbox_creates=transient.active_sandbox_creates,
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
        return NodeDrainSnapshot(activity, drain, build_count, storage_dependencies)

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
