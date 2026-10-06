"""The agent's half of runtime/noded's creates (docs/rust-node-daemon-plan.md, phase 1).

The daemon runs a create's I/O itself, but admission stays here, with its one
owner: ``admit`` holds exactly what ``DirectSandboxService.create`` holds
(transition demand, a startup slot, active capacity and the per-(id, gen)
lifecycle lock) under a token, and ``finish`` builds the record and releases
it. Heartbeats and drain therefore see a held admission as an in-flight create.

Each held admission lives in its own thread, inside ``create_admission``, so
the thread-bound parts of today's create (the startup slot's reentrancy flag
and the lifecycle lock's holder list) stay on one thread. A token is released
unfinished when it expires, or when a request names a new daemon session: a
restarted daemon finishes nothing its predecessor admitted.
"""

from __future__ import annotations

from contextvars import copy_context
from dataclasses import dataclass, field
import logging
import secrets
import threading
import time
from typing import Any, Callable

from . import phase_timings
from .admission import CREATE_ADMISSION_WAIT
from .sandbox import SandboxCapacityUnavailableError, SandboxConflictError, SandboxOperation, SandboxSpec

NODED_SESSION_HEADER = "X-UCloud-Noded-Session"
CREATE_TOKEN_EXPIRY_SECONDS = 600.0
_LOG = logging.getLogger(__name__)


class CreateTokenUnknownError(LookupError):
    """The token was never issued, already finished, expired or abandoned."""


@dataclass(eq=False)
class _Held:
    token: str
    spec: SandboxSpec
    operation: SandboxOperation
    session: str | None
    existing: Any
    deadline: float
    phases: dict[str, int] = field(default_factory=dict)
    admitted: threading.Event = field(default_factory=threading.Event)
    admission_error: BaseException | None = None
    outcome: Callable[[], Any] | None = None
    delivered: threading.Event = field(default_factory=threading.Event)
    done: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    error: BaseException | None = None


class CreateHandoff:
    def __init__(self, service: Any, *, expiry_seconds: float = CREATE_TOKEN_EXPIRY_SECONDS) -> None:
        self.service = service
        self.expiry_seconds = float(expiry_seconds)
        self._guard = threading.Lock()
        self._held: dict[str, _Held] = {}
        self._session: str | None = None

    def observe_session(self, session: str | None) -> None:
        """Release every token another daemon session was handed."""
        if not session:
            return
        with self._guard:
            if session == self._session:
                return
            self._session = session
            stale = [held for held in self._held.values() if held.session != session]
            for held in stale:
                del self._held[held.token]
        for held in stale:
            _LOG.warning("released create admission %s of a previous noded session", held.spec.id)
            self._deliver(held, None)

    def admit(self, spec: SandboxSpec, operation: SandboxOperation, *, wait: float | None,
              session: str | None) -> dict[str, Any]:
        """Hold a create's admission; return its token and what the daemon
        creates from: Python's canonical spec and its layout decisions."""
        operation.validate_spec(spec)
        provisioner = self.service.provisioner
        with phase_timings.recording() as phases:
            # Before admission is held: on a busy node an invalid spec gets its
            # 400 instead of waiting for a slot and a 503.
            with phase_timings.phase("validate_spec"):
                provisioner.validate_spec(spec)
            split, initial_claim = provisioner.create_layout(spec)
            existing = self.service.get_snapshot(spec.id)
            held = _Held(token=secrets.token_hex(16), spec=spec, operation=operation, session=session or None,
                         existing=existing, deadline=time.monotonic() + self.expiry_seconds, phases=phases)
            # The holder records its admission phases into this same dict.
            threading.Thread(target=copy_context().run, args=(self._hold, held, wait), daemon=True,
                             name=f"ucloud-create-admission-{spec.id}").start()
        held.admitted.wait()
        if held.admission_error is not None:
            raise held.admission_error
        with self._guard:
            superseded = held.session is not None and self._session not in (None, held.session)
            if not superseded:
                self._held[held.token] = held
        if superseded:
            self._deliver(held, None)
            raise SandboxConflictError("the noded session that requested this create has ended")
        return {
            "token": held.token,
            "existing": None if existing is None else existing.to_dict(),
            "spec": spec.to_dict(),
            "requested_resources": spec.requested_resources().to_dict(),
            "initial_claim": None if initial_claim is None else {
                "workspace_mb": initial_claim.workspace_mb, "memory_mb": initial_claim.memory_mb},
            "split": split,
        }

    def finish_created(self, token: str, *, runtime_started: bool = True) -> tuple[Any, bool, dict[str, int]]:
        """Record the daemon's committed create; return (record, idempotent,
        the phases Python timed for it). Only a runtime the daemon started is
        adopted; a replay of an owned sandbox keeps its claim as it is."""
        held = self._take(token)

        def created():
            registry = self.service.provisioner.registry
            # The daemon committed in another process: a fresh read, so the
            # heartbeat sees the row before the in-flight claim goes.
            registration = registry.get(held.spec.id, fresh=True)
            if registration is None or (registration.sandbox_generation, registration.operation_id) != (
                    held.operation.generation, held.operation.operation_id):
                raise RuntimeError("the finished create is not this operation's registration")
            if registration.has_direct_sandbox and runtime_started:
                # Growth monitor, workspace claim, memory placement: what
                # warden.create records in this process.
                self.service.warden.adopt_created(registration.to_direct_sandbox())
            return self.service.created_record(registration)

        record = self._run(held, created)
        return record, held.existing is not None and held.existing == record, dict(held.phases)

    def finish_capacity_rejected(self, token: str, message: str) -> None:
        """Roll back a create the daemon could not fit, as create does today."""
        held = self._take(token)

        def rejected():
            # The rollback reads what the daemon committed in another process.
            self.service.provisioner.registry.get(held.spec.id, fresh=True)
            self.service.roll_back_capacity_rejection(held.spec.id, held.operation.generation)
            raise SandboxCapacityUnavailableError(message)

        self._run(held, rejected)

    def finish_failed(self, token: str) -> None:
        self._run(self._take(token), lambda: None)

    def close(self) -> None:
        with self._guard:
            held, self._held = list(self._held.values()), {}
        for item in held:
            self._deliver(item, None)

    @property
    def held_count(self) -> int:
        with self._guard:
            return len(self._held)

    def _take(self, token: str) -> _Held:
        with self._guard:
            held = self._held.pop(token, None)
        if held is None:
            raise CreateTokenUnknownError("create admission token is unknown or expired")
        return held

    def _run(self, held: _Held, outcome: Callable[[], Any]) -> Any:
        self._deliver(held, outcome)
        held.done.wait()
        if held.error is not None:
            raise held.error
        return held.result

    @staticmethod
    def _deliver(held: _Held, outcome: Callable[[], Any] | None) -> None:
        held.outcome = outcome
        held.delivered.set()

    def _hold(self, held: _Held, wait: float | None) -> None:
        reset = CREATE_ADMISSION_WAIT.set(wait)
        try:
            with self.service.create_admission(held.spec, operation=held.operation):
                held.admitted.set()
                while not held.delivered.wait(max(0.05, held.deadline - time.monotonic())):
                    with self._guard:
                        expired = self._held.pop(held.token, None) is held
                    if expired:
                        _LOG.warning("create admission %s expired unfinished", held.spec.id)
                        return
                    # A finish took it just now; its outcome is on the way.
                if held.outcome is not None:
                    held.result = held.outcome()
        except BaseException as exc:
            if not held.admitted.is_set():
                held.admission_error = exc
            else:
                held.error = exc
        finally:
            CREATE_ADMISSION_WAIT.reset(reset)
            # Released before the response: the activity epoch follows it.
            held.admitted.set()
            held.done.set()


def create_config(service: Any, *, node_epoch: str, rust_creates_enabled: bool,
                  rust_execs_enabled: bool = False, exec_sessions: Any = None) -> dict[str, Any]:
    """The agent's effective create configuration, read from the assembled node.

    Python stays the single source of truth for flags and assembly checks;
    runtime/noded reads this instead of parsing the agent's command line.
    ``exec`` is phase 2a's: what noded needs to run execs itself, fenced by
    ucloud_sandboxes/exec_fence.py; ``exec_sessions`` is the ExecSessionManager.
    """
    from .direct_network import NETWORK_MTU
    from .environment_manifest import HOST_EROFS_ABI
    from .resource_admission import PHYSICAL_MEMORY_FLOOR_MB

    provisioner = service.provisioner
    warden = provisioner.warden
    config = warden.config
    overlays = provisioner.overlays
    store = overlays.image_store
    network = provisioner.network_manager
    allowed = network.allowed_tcp_egress if network is not None else ()
    backing = warden.memory_backing
    policy = provisioner.disk_claim_policy

    def path(value):
        return None if value is None else str(value)

    environment = None
    if getattr(store, "backend_abi", None) == HOST_EROFS_ABI:
        environment = {"registry_url": store.registry.client.base_url,
                       "registry_repository": store.registry.repository,
                       "backend_socket": str(store.backend.path), "rafs": bool(store.rafs)}
    return {
        "state_root": str(provisioner.registry.path.parent),
        "registry_path": str(provisioner.registry.path),
        "image_cache_root": str(store.root),
        "volume_mount_root": str(overlays.writable_root),
        "storage_native_socket": str(warden.storage.socket_path),
        "runsc": str(config.runsc),
        "runtime_root": str(config.runtime_root),
        "bundle_root": str(config.bundle_root),
        "journal_root": str(config.journal_root),
        "network": config.network,
        "network_mtu": NETWORK_MTU,
        "direct_network_allow_tcp": [{"ip": item.address, "port": item.port}
                                     for item in allowed if not item.is_dynamic],
        "dns_named_egress": any(item.is_dynamic for item in allowed),
        "relays_configured": bool(network is not None and network.relays),
        "split_memory_backing": backing is not None,
        "memory_backing_hard_capacity_bytes": backing.hard_capacity_bytes if backing is not None else 0,
        "application_memory_root": path(getattr(config, "application_memory_root", None)),
        "reflink_memory_restore": bool(getattr(config, "reflink_memory_restore", False)),
        "workspace_initial_grant_mb": policy.workspace_grant_mb,
        "demonstrated_memory": policy.demonstrated_memory,
        "init_binary": path(provisioner.oci.init_binary),
        "managed_init_binary": path(provisioner.oci.managed_init_binary),
        "environment": environment,
        "runtime_compatibility_sha256": provisioner.runtime_compatibility_sha256,
        "node_epoch": node_epoch,
        "rust_creates_enabled": bool(rust_creates_enabled),
        "exec": {
            "runsc": str(config.runsc),
            "runtime_root": str(config.runtime_root),
            "warden_locks_dir": str(config.runtime_root / "warden-locks"),
            "warden_paused_dir": str(config.runtime_root / "warden-paused"),
            # The physical floor applies to execs only with active capacity
            # (direct_service._active_admission_guard).
            "active_capacity_configured": getattr(service, "_active_capacity", None) is not None,
            "memory_floor_mib": PHYSICAL_MEMORY_FLOOR_MB,
            "sessions": None if exec_sessions is None else {
                "max_sessions": exec_sessions.max_sessions,
                "max_events_per_session": exec_sessions.max_events_per_session,
                "completed_retention_seconds": exec_sessions.completed_retention_seconds,
                "delivered_grace_seconds": exec_sessions.delivered_grace_seconds,
                "output_idle_timeout_seconds": exec_sessions.output_idle_timeout_seconds,
            },
            "admission_wait_seconds": service.admission_wait_seconds,
            "rust_execs_enabled": bool(rust_execs_enabled),
        },
    }
