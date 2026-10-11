"""Node admission behind one interface (docs/admission-port-3c.md, step 3c.3).

``DirectSandboxService`` admits through ``service.admission``:

- ``LocalAdmission`` is the agent's own ledger, slot pools and managed growth:
  the code the service has always run, moved here call site by call site and
  otherwise unchanged. The ledger's state and its building blocks
  (``_transitions``, the pools, ``_active_admission_guard``, the growth cost
  functions) stay on the service, where exec admission still uses them.
- ``NodedAdmission`` is runtime/noded's admission service on its internal
  socket (``sandbox.direct_node_rust_admission``), so the node has exactly
  one ledger. Python still quotes restore costs (decision E), holds the
  lifecycle locks, runs the ``_try_lock``/``prepare_restore_memory`` probe of
  a relay continuation and revalidates a restore's owner and checkpoint after
  its grant.

The operations, by caller (§1.4):

- ``startup``: a create's demand, startup slot and capacity (``create``, and
  noded's forwarded creates through the create handoff);
- ``restore``: a wake's demand, restore slot and capacity, inside
  ``_unlocked_for_admission``;
- ``observe``: a capture (never queued or refused);
- ``publication``: a background publication's lane, or None;
- ``cancel``: delete and try_delete (queued waiters, and the growth intent);
- ``close`` / ``open``: the drain gate (synchronous, before the drain row);
- ``growth`` / ``growth_batch``: the durable growth writers (park, wait,
  activate, terminal, bind; local waits);
- ``launch`` / ``continuation``: managed launch and relay continuation
  admission;
- ``activity_snapshot`` and the other readers: heartbeats and metrics.

Every refusal NodedAdmission raises is the exception the local code raises
for the same refusal, with noded's message, so client answers stay byte for
byte the same (I25). A request that reached noded but got no answer
(``NodedLost``) was not dispatched: it becomes the safe-retry error (I10).
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
import logging
import threading
import time
from typing import Any, Callable, Iterator

from . import phase_timings
from .admission import CREATE_ADMISSION_WAIT
from .direct_registry import (
    DirectRegistryConflictError,
    ManagedGrowthIntent,
    ManagedPrimaryOwnedError,
)
from .direct_warden import DirectWardenError
from .managed_process import ManagedGenerationMismatchError, ManagedProcessError
from .models import ResourceQuantity
from .noded_client import NodedError, NodedLink, NodedLost, NodedRefused, NodedUnavailable
from .sandbox import (
    SandboxAdmissionClosedError,
    SandboxCapacityUnavailableError,
    SandboxConflictError,
    SandboxRestoreBusyError,
    SandboxStartupBusyError,
)
from .transition_admission import MemoryDemand, TransitionCost, TransitionKind

_LOG = logging.getLogger(__name__)

# noded's messages for the refusals Python raised with other exception types.
STARTUP_BUSY = "node startup admission wait deadline exceeded"
RESTORE_LOST = "queued restore generation lost incarnation ownership"
GROWTH_LOST = "queued primary growth lost incarnation"
CONTINUATION_LOST = "managed continuation lost incarnation"
# Beyond a blocking call's own wait: transport, and noded's last pass.
_RPC_MARGIN_SECONDS = 10.0
_RELEASE_ATTEMPTS = 3
_PUBLICATION_PROVENANCE = "bounded-storage-backend-stream"


class NodedSnapshotUnavailable(RuntimeError):
    """noded's transient snapshot cannot be read now (recovering, restarted
    or down). A heartbeat skips its sample rather than report no claims."""


class LocalAdmission:
    """The agent's own admission: the ledger on ``DirectSandboxService``."""

    remote = False

    def __init__(self, service) -> None:
        self.service = service

    @contextmanager
    def startup(self, owner, requested: ResourceQuantity, *, managed: bool) -> Iterator[None]:
        service = self.service
        with ExitStack() as admitted:
            # Same nesting order as one compound with-statement; each entry is
            # timed because slot, pressure and lock waits are distinct causes.
            admitted.enter_context(service._startup_demand(*owner, requested, managed=managed))
            startup_cost = service._startup_cost(owner, requested, managed=managed)
            with phase_timings.phase("startup_admission"):
                admitted.enter_context(service.startup_admission(owner=owner))
            with phase_timings.phase("active_capacity"):
                admitted.enter_context(service._reserve_active_capacity(*owner, requested, cost=startup_cost))
            yield

    @contextmanager
    def restore(self, owner, requested: ResourceQuantity, cost: TransitionCost, *, deadline: float,
                validate_owner: Callable[[], None]) -> Iterator[None]:
        service = self.service
        # Publish demand before releasing lifecycle ownership. Deletion/drain
        # can now cancel a queued wake; no parked guest has been activated yet.
        with service._transition_demand(owner, cost), ExitStack() as stack:
            try:
                with service._unlocked_for_admission(owner):
                    stack.enter_context(service._restore_slot(owner=owner, deadline=deadline))
                    stack.enter_context(service._reserve_active_capacity(*owner, requested,
                                                                         cost=cost, deadline=deadline,
                                                                         validate_owner=validate_owner))
            except SandboxCapacityUnavailableError as exc:
                raise SandboxRestoreBusyError(str(exc)) from exc
            yield

    @contextmanager
    def observe(self, owner, cost: TransitionCost) -> Iterator[None]:
        with self.service._observe_transition(owner, cost):
            yield

    def publication(self, owner) -> Callable[[], None] | None:
        """A publication lane and its claim; returns their release, or None."""
        service = self.service
        with service._capacity_guard:
            # Keep one maintenance operation progressing through foreground
            # bursts, but don't launch a whole upload wave ahead of wakes.
            if (service._transitions.foreground_waiting
                    and service._transitions.active_count(TransitionKind.PUBLICATION)):
                return None
        if not service._publication_slots.acquire(blocking=False):
            return None
        with service._capacity_guard:
            publication_claim = service._transitions.claim(owner, TransitionCost(
                TransitionKind.PUBLICATION, None, provenance=_PUBLICATION_PROVENANCE))

        def release() -> None:
            with service._capacity_guard:
                service._transitions.release(publication_claim)
                service._admission_changed.notify_all()
            service._publication_slots.release()

        return release

    def cancel(self, owner) -> None:
        service = self.service
        service._restore_slots.cancel_waiters(owner)
        service._startup_slots.cancel_waiters(owner)
        service._forget_growth(owner)

    def close(self, drain_token: str | None = None) -> None:
        service = self.service
        # Once this returns, every admitted create is present in the transient
        # reservation snapshot or has already finished.
        with service._capacity_guard:
            service._admission_open = False
            service._admission_changed.notify_all()
        service._startup_slots.cancel_waiters()
        service._restore_slots.cancel_waiters()

    def open(self, drain_token: str | None = None) -> None:
        service = self.service
        with service._capacity_guard:
            service._admission_open = True

    def growth(self, key, **kwargs):
        return self.service._record_growth_intent(key, **kwargs)

    def growth_batch(self, items):
        return self.service._record_local_wait_growth(items)

    def launch(self, registration, job_id: str, launch_sha256: str):
        return self.service._admit_local_managed_launch(registration, job_id, launch_sha256)

    def continuation(self, registration, request_id: str) -> None:
        try:
            self.service._admit_managed_growth(registration, request_id=request_id, startup=False)
        except SandboxCapacityUnavailableError as exc:
            # No continuation has been acknowledged or runtime restored.
            # Use the existing activation retry contract without asserting
            # that this still-resident safe wait is physically parked.
            raise SandboxStartupBusyError(str(exc)) from exc

    def activity_snapshot(self):
        return self.service._local_activity_snapshot()

    def transition_snapshot(self):
        service = self.service
        with service._capacity_guard:
            return service._transitions.snapshot()

    def resident_demand(self):
        return self.service._local_resident_demand_snapshot()

    def warm_park_demand(self) -> MemoryDemand:
        return self.service._local_warm_park_demand()

    def reclaim_demand(self) -> tuple[bool, MemoryDemand]:
        return self.service._local_reclaim_demand()


def _owner(owner) -> dict[str, Any]:
    return {"sandbox_id": owner[0], "generation": int(owner[1])}


def _shape(requested: ResourceQuantity) -> dict[str, Any]:
    return {"vcpu": float(requested.vcpu or 0), "memory_mb": int(requested.memory_mb or 0)}


def _cost(cost: TransitionCost) -> dict[str, Any]:
    return {
        "memory_bytes": cost.memory_bytes,
        "ram_backing_bytes": cost.ram_backing_bytes,
        "read_bytes": cost.read_bytes,
        "write_bytes": cost.write_bytes,
        "provenance": cost.provenance or "unknown",
    }


def _intent(raw) -> ManagedGrowthIntent | None:
    if raw is None:
        return None
    return ManagedGrowthIntent(raw["sandbox_id"], int(raw["generation"]), raw["job_id"], raw["launch_sha256"],
                               int(raw["memory_bytes"]), raw["phase"], raw["request_id"])


class NodedAdmission:
    """noded's admission service over its internal socket.

    Every claim this agent holds is a noded token; each one is remembered
    with its owner, kind, cost and shape until released, so a hello to a
    restarted noded reasserts them (§3.5). Refusals noded answers with
    ``agent_hello_required`` or ``admission_recovering`` are retried once,
    after a hello.
    """

    remote = True

    def __init__(self, service, link: NodedLink) -> None:
        self.service = service
        self.link = link
        self.client = link.client
        link.reassert = self.live_claims
        link.on_hello = self._after_hello
        self._guard = threading.Lock()
        self._claims: dict[str, dict[str, Any]] = {}
        # Growth events the agent's start could not deliver (noded was not up
        # yet): sent after the next hello.
        self._pending: list[dict[str, Any]] = []
        # The largest transient revision noded reported: the agent's lifecycle
        # answers compose it, as heartbeats do, so neither runs backwards.
        self._noded_revision: int | None = None

    # -- transport ------------------------------------------------------------

    def _call(self, method: str, path: str, body: Any = None, *, timeout: float | None = None) -> dict[str, Any]:
        for attempt in (0, 1):
            try:
                return self.client.call(method, path, body, timeout=timeout)
            except NodedRefused as exc:
                if attempt or exc.error_code not in ("agent_hello_required", "admission_recovering"):
                    raise
                self.link.hello_now()
        raise AssertionError("unreachable")

    def _see_revision(self, revision) -> None:
        if isinstance(revision, int) and not isinstance(revision, bool):
            with self._guard:
                if self._noded_revision is None or revision > self._noded_revision:
                    self._noded_revision = revision

    def live_claims(self) -> list[dict[str, Any]]:
        """``hello``'s ``reassert``: every claim this agent holds."""
        with self._guard:
            return [dict(item, agent_token=token) for token, item in self._claims.items()]

    def _after_hello(self, hello: dict[str, Any]) -> None:
        # The agent writes the drain row: a noded that disagrees follows it.
        local_open = self.service._admission_open
        if hello.get("admission_open") is not None and hello["admission_open"] != local_open:
            path = "open" if local_open else "close"
            answer = self.client.call("POST", f"/internal/v1/admission/{path}", {"drain_token": "agent-hello"})
            self._see_revision(answer.get("transient_revision"))
        with self._guard:
            pending, self._pending = self._pending, []
        if pending:
            try:
                self.client.call("POST", "/internal/v1/growth/events", {"items": pending})
            except NodedError:
                with self._guard:
                    self._pending = pending + self._pending
                raise
        try:
            self._see_revision(self.client.call("GET", "/internal/v1/admission/snapshot").get("transient_revision"))
        except NodedError:
            pass  # Seeded by the next heartbeat instead.

    def _hold(self, token: str, owner, kind: str, cost: TransitionCost, requested: ResourceQuantity | None) -> None:
        with self._guard:
            self._claims[token] = {
                "owner": _owner(owner), "kind": kind, "cost": _cost(cost),
                "requested": None if requested is None else _shape(requested),
            }

    def _release(self, token: str) -> None:
        with self._guard:
            self._claims.pop(token, None)
        for attempt in range(_RELEASE_ATTEMPTS):
            try:
                self.client.call("POST", f"/internal/v1/admission/{token}/release", {})
                return
            except NodedRefused as exc:
                if exc.error_code == "admission_token_unknown":
                    return  # Released already, or noded restarted without it.
                _LOG.warning("noded refused to release admission %s: %s", token, exc)
                return
            except (NodedUnavailable, NodedLost) as exc:
                if attempt + 1 == _RELEASE_ATTEMPTS:
                    _LOG.warning("could not release noded admission %s: %s", token, exc)
                    return
                time.sleep(0.05 * (attempt + 1))
            except NodedError as exc:
                _LOG.warning("could not release noded admission %s: %s", token, exc)
                return

    @contextmanager
    def _held(self, token: str) -> Iterator[None]:
        try:
            yield
        finally:
            self._release(token)

    # -- refusals ---------------------------------------------------------------

    @staticmethod
    def _retry_error(context: str, message: str) -> RuntimeError:
        """The safe retry for a refusal that granted nothing (I10)."""
        if context == "restore":
            return SandboxRestoreBusyError(message)
        return SandboxStartupBusyError(message)

    def _refused(self, exc: NodedError, context: str, *, validate: Callable[[], None] | None = None) -> BaseException:
        """The exception the local code raises for the refusal noded answered."""
        if not isinstance(exc, NodedRefused):
            # Nothing reached noded, or no answer did: never dispatched.
            return self._retry_error(context, f"node admission is unavailable: {exc}")
        message = str(exc.payload.get("error") or exc)
        code = exc.error_code
        if code == "node_admission_closed":
            return SandboxAdmissionClosedError(message)
        if code == "node_active_admission_deferred":
            if message == STARTUP_BUSY:
                return SandboxStartupBusyError(message)
            return SandboxCapacityUnavailableError(message)
        if code in ("node_restore_busy", "node_startup_busy"):
            return self._retry_error(context, message)
        if code == "primary_already_owned":
            return ManagedPrimaryOwnedError(str(exc.payload.get("job_id") or ""))
        if code in ("generation_mismatch", "sandbox_unavailable"):
            if validate is not None:
                validate()  # The local check raises exactly what it always did.
            if message == RESTORE_LOST or code == "sandbox_unavailable":
                return DirectWardenError(message)
            if message in (GROWTH_LOST, CONTINUATION_LOST):
                return SandboxConflictError(message)
            if " is generation " in message:
                return ManagedGenerationMismatchError(message)
            return DirectRegistryConflictError(message)
        if code == "not_managed_process":
            return ManagedProcessError(message)
        if exc.retryable:
            return self._retry_error(context, message)
        return RuntimeError(message)

    # -- creates, restores, captures, publications ---------------------------------

    @contextmanager
    def startup(self, owner, requested: ResourceQuantity, *, managed: bool) -> Iterator[None]:
        service = self.service
        wait = CREATE_ADMISSION_WAIT.get()
        wait = service.admission_wait_seconds if wait is None else min(wait, service.admission_wait_seconds)
        body = {"owner": _owner(owner), "requested": _shape(requested), "managed": bool(managed),
                "wait_seconds": float(wait)}
        try:
            answer = self._call("POST", "/internal/v1/admission/startup", body,
                                timeout=wait + _RPC_MARGIN_SECONDS)
        except NodedError as exc:
            raise self._refused(exc, "startup") from exc
        token = answer["token"]
        cost = service._startup_cost(owner, requested, managed=managed)
        self._hold(token, owner, TransitionKind.STARTUP.value, cost, requested)
        phases = answer.get("phases") or {}
        phase_timings.add("startup_admission", phases.get("startup_admission_ms", 0))
        phase_timings.add("active_capacity", phases.get("active_capacity_ms", 0))
        with self._held(token):
            yield

    @contextmanager
    def restore(self, owner, requested: ResourceQuantity, cost: TransitionCost, *, deadline: float,
                validate_owner: Callable[[], None]) -> Iterator[None]:
        body = {"owner": _owner(owner), "requested": _shape(requested), "cost": _cost(cost)}
        with self.service._unlocked_for_admission(owner):
            budget = max(0.0, deadline - time.monotonic())
            body["deadline_ms"] = budget * 1000
            try:
                answer = self._call("POST", "/internal/v1/admission/restore", body,
                                    timeout=budget + _RPC_MARGIN_SECONDS)
            except NodedError as exc:
                raise self._refused(exc, "restore", validate=validate_owner) from exc
            token = answer["token"]
            self._hold(token, owner, TransitionKind.RESTORE.value, cost, requested)
        with self._held(token):
            yield

    @contextmanager
    def observe(self, owner, cost: TransitionCost) -> Iterator[None]:
        # Never queued or refused (I16); a capture never waits for noded either.
        token = None
        try:
            token = self._call("POST", "/internal/v1/admission/observe",
                               {"owner": _owner(owner), "kind": "capture", "cost": _cost(cost)})["token"]
        except NodedError as exc:
            _LOG.warning("capture of %s is not observed by noded: %s", owner[0], exc)
        if token is None:
            yield
            return
        self._hold(token, owner, TransitionKind.CAPTURE.value, cost, None)
        with self._held(token):
            yield

    def publication(self, owner) -> Callable[[], None] | None:
        try:
            token = self._call("POST", "/internal/v1/admission/publication", {"owner": _owner(owner)})["token"]
        except NodedRefused as exc:
            if exc.error_code != "publication_deferred":
                _LOG.warning("noded refused a publication lane for %s: %s", owner[0], exc)
            return None
        except NodedError as exc:
            _LOG.warning("no publication lane for %s: %s", owner[0], exc)
            return None
        cost = TransitionCost(TransitionKind.PUBLICATION, None, provenance=_PUBLICATION_PROVENANCE)
        self._hold(token, owner, TransitionKind.PUBLICATION.value, cost, None)
        return lambda: self._release(token)

    def cancel(self, owner) -> None:
        """Delete: wake the owner's queued waiters and retire its growth intent.
        The registration is already gone, so a failure is only logged."""
        try:
            self._call("POST", "/internal/v1/admission/cancel", {"owner": _owner(owner)})
            self._call("POST", "/internal/v1/growth/events",
                       {"items": [{"action": "forget", "owner": _owner(owner)}]})
        except NodedError as exc:
            _LOG.warning("noded did not retire deleted %s: %s", owner[0], exc)

    def close(self, drain_token: str | None = None, *, strict: bool = True) -> None:
        """Close noded's gate, then the agent's (its execs). Synchronous: once
        this returns, noded holds every admitted create in its snapshot or has
        released it (I18). ``strict=False`` (agent start): a noded that is not
        up yet reads the drain row itself and is aligned at the hello."""
        self._gate("close", drain_token, strict=strict)
        service = self.service
        with service._capacity_guard:
            service._admission_open = False
            service._admission_changed.notify_all()

    def open(self, drain_token: str | None = None, *, strict: bool = True) -> None:
        self._gate("open", drain_token, strict=strict)
        service = self.service
        with service._capacity_guard:
            service._admission_open = True

    def _gate(self, path: str, drain_token: str | None, *, strict: bool) -> None:
        try:
            answer = self._call("POST", f"/internal/v1/admission/{path}", {"drain_token": drain_token or "agent"})
        except NodedError as exc:
            if strict:
                raise RuntimeError(f"noded did not {path} node admission: {exc}") from exc
            _LOG.warning("noded did not %s node admission yet: %s", path, exc)
            return
        self._see_revision(answer.get("transient_revision"))

    # -- managed growth ------------------------------------------------------------

    def _events(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        answer = self._call("POST", "/internal/v1/growth/events", {"items": items})
        results = answer.get("results")
        if not isinstance(results, list) or len(results) != len(items):
            raise RuntimeError("noded's growth events answer is outside its contract")
        return results

    def _event_error(self, item: dict[str, Any], result: dict[str, Any]) -> Exception:
        message, code = str(result.get("error") or "growth bookkeeping failed"), result.get("error_code")
        if code == "sandbox_unavailable":
            return DirectWardenError(message)
        if code == "primary_already_owned":
            owner = item["owner"]
            current = next((intent for intent in self.service.provisioner.registry.growth_intents()
                            if (intent.sandbox_id, intent.generation) == (owner["sandbox_id"], owner["generation"])),
                           None)
            return ManagedPrimaryOwnedError(current.job_id if current is not None else "")
        if code == "growth_conflict":
            return DirectRegistryConflictError(message)
        if code == "invalid_request":
            return ValueError(message)
        return RuntimeError(message)

    @staticmethod
    def _event(key, action: str, *, job_id: str = "", launch_sha256: str = "", request_id: str = "") -> dict[str, Any]:
        item: dict[str, Any] = {"action": action, "owner": _owner(key)}
        if request_id:
            item["request_id"] = request_id
        if job_id:
            item["job_id"] = job_id
        if launch_sha256:
            item["launch_sha256"] = launch_sha256
        return item

    def growth(self, key, *, action: str, job_id: str = "", launch_sha256: str = "", request_id: str = ""):
        """One durable growth transition, committed by noded; its intent."""
        item = self._event(key, action, job_id=job_id, launch_sha256=launch_sha256, request_id=request_id)
        try:
            [result] = self._events([item])
        except NodedError as exc:
            raise RuntimeError(f"noded did not record the managed {action}: {exc}") from exc
        if "error_code" in result:
            raise self._event_error(item, result)
        return _intent(result.get("intent"))

    def observe_parked(self, key) -> None:
        """A parked managed record found at agent start: re-observed once
        noded answers, as noded may start after the agent."""
        item = self._event(key, "park")
        try:
            [result] = self._events([item])
        except NodedError:
            with self._guard:
                self._pending.append(item)
            return
        if "error_code" in result:
            _LOG.warning("noded did not re-observe parked %s: %s", key[0], result.get("error"))

    def growth_batch(self, items):
        """``record_local_wait_growth``: ``(method, key, request_id)`` items in one commit."""
        actions = {"observe_managed_wait": "wait", "resume_managed_continuation": "activate"}
        events = [self._event(key, actions[method], request_id=request_id) for method, key, request_id in items]
        try:
            results = self._events(events)
        except NodedError as exc:
            return [RuntimeError(f"noded did not record local wait growth: {exc}")] * len(items)
        return [self._event_error(event, result) if "error_code" in result else None
                for event, result in zip(events, results)]

    def launch(self, registration, job_id: str, launch_sha256: str):
        """The agent's own managed start (the requests noded forwards to it).
        Returns an intent whose ``job_id`` is empty for an imported primary,
        which the caller binds after its dispatch."""
        service = self.service
        key = (registration.sandbox_id, registration.sandbox_generation)
        budget = service.admission_wait_seconds
        body = {"owner": _owner(key), "job_id": job_id, "launch_sha256": launch_sha256, "deadline_ms": budget * 1000}
        try:
            answer = self._call("POST", "/internal/v1/growth/launch", body, timeout=budget + _RPC_MARGIN_SECONDS)
        except NodedError as exc:
            raise self._refused(exc, "launch") from exc
        bound = answer.get("job_id") or ""
        return ManagedGrowthIntent(key[0], key[1], bound, launch_sha256 if bound else "",
                                   int(registration.spec.memory_mb * 1024**2), "active", "")

    def continuation(self, registration, request_id: str) -> None:
        service = self.service
        key = (registration.sandbox_id, registration.sandbox_generation)
        budget = service.admission_wait_seconds
        body = {"owner": _owner(key), "request_id": request_id, "deadline_ms": budget * 1000}

        def validate_owner():
            current = service._require_registration(key[0])
            if current.sandbox_generation != key[1] or current.phase != "owned":
                raise SandboxConflictError(GROWTH_LOST)

        try:
            self._call("POST", "/internal/v1/growth/continuation", body, timeout=budget + _RPC_MARGIN_SECONDS)
        except NodedError as exc:
            raise self._refused(exc, "continuation", validate=validate_owner) from exc

    # -- readers -----------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """noded's transient view, or NodedSnapshotUnavailable."""
        try:
            answer = self._call("GET", "/internal/v1/admission/snapshot")
        except NodedError as exc:
            raise NodedSnapshotUnavailable(f"noded's admission snapshot is unavailable: {exc}") from exc
        self._see_revision(answer.get("transient_revision"))
        return answer

    def confirm_session(self, session: str) -> None:
        """A heartbeat read noded's snapshot under ``session`` and then the
        registry: abort it if noded restarted in between (§5, risk 1)."""
        try:
            current = self.client.ping().get("noded_session")
        except NodedError as exc:
            raise NodedSnapshotUnavailable(f"noded's session cannot be confirmed: {exc}") from exc
        if current != session:
            raise NodedSnapshotUnavailable("noded restarted while the heartbeat was sampled")

    def activity_snapshot(self):
        """§3.6: noded's snapshot first, then this agent's exec leases."""
        from .direct_service import DirectServiceActivitySnapshot

        snapshot = self.snapshot()
        service = self.service
        with service._capacity_guard:
            exec_operations = len(service._active_exec_reservations)
            agent_epoch = service._activity_epoch
        reservations = {
            (item["owner"]["sandbox_id"], int(item["owner"]["generation"])):
                ResourceQuantity(vcpu=item["vcpu"], memory_mb=item["memory_mb"])
            for item in snapshot["resource_reservations"]
        }
        return DirectServiceActivitySnapshot(
            resource_reservations=reservations,
            active_sandbox_creates=len(snapshot["resource_reservations"]),
            active_exec_operations=exec_operations,
            activity_revision=agent_epoch + int(snapshot["transient_revision"]),
            noded_session=snapshot["noded_session"],
        )

    def noded_revision(self) -> int:
        """The largest transient revision noded reported (read once if none
        was yet): the noded addend of the agent's lifecycle revisions."""
        with self._guard:
            noded = self._noded_revision
        if noded is None:
            try:
                self.snapshot()
            except NodedSnapshotUnavailable:
                pass
            with self._guard:
                noded = self._noded_revision or 0
        return noded

    def transition_snapshot(self):
        return self.snapshot()["transitions"]

    def resident_demand(self):
        return self.snapshot()["resident_demand"]

    def slots(self):
        """Each pool's ``{capacity, available, waiting}``."""
        return self.snapshot()["slots"]

    def warm_park_demand(self) -> MemoryDemand:
        if not self.service._admission_open:
            return MemoryDemand(1 << 63, 1 << 63)
        return self._next_demand(self.resident_demand())

    def reclaim_demand(self) -> tuple[bool, MemoryDemand]:
        snapshot = self.snapshot()
        if not snapshot["admission_open"]:
            return False, MemoryDemand()
        return True, self._next_demand(snapshot["resident_demand"])

    @staticmethod
    def _next_demand(demand) -> MemoryDemand:
        # next_memory_demands = admitted + pending (demand_snapshot's split).
        return MemoryDemand(
            demand["admitted_demand_bytes"] + demand["pending_demand_bytes"],
            demand["admitted_ram_backing_bytes"] + demand["pending_ram_backing_bytes"],
        )
