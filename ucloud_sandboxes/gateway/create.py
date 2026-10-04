"""Power-of-k create placement (C4.3 phase 1, docs/c43-placement-wiring-plan.md).

A create samples k capable workers from the shared heartbeats, plus incoming
migrations' reservations, and charges this process's in-flight overlay. It
writes the route intent and lets the worker's admission decide: no fleet route
scan and no capacity transaction. A definite reject moves the intent to the
next candidate. An ambiguous answer keeps it, so a retry replays the same
incarnation on the same worker.
"""

from __future__ import annotations

from dataclasses import replace
from http import HTTPStatus
import random
from typing import Any, Protocol
from uuid import uuid4

from ..admission import ADMISSION_WAIT_HEADER
from ..control_state import ControlStateStore, detached_heartbeat
from ..metrics import MetricsStore, record_sandbox_scheduled
from ..models import NodeHeartbeat, utc_now
from ..placement_choice import (
    DEFAULT_K, FleetView, InflightOverlay, PlacementRequest, PowerOfKChooser, SandboxIncarnation,
)
from ..routing import (
    PendingSandboxDemand, RoutingStore, SandboxRoute, SandboxRouteAllocation,
    SandboxRouteConflictError,
)
from ..sandbox import SandboxSpec, sandbox_spec_fingerprint
from ..telemetry import Telemetry
from ..worker_receipts import (
    _route_with_sandbox_record, _sandbox_create_request_body, _sandbox_record_matches_route,
)
from .exchange import Exchange
from .node_rpc import ProxiedResponse
from .placement import _sandbox_required_capabilities, migration_reservations
from .registry_refs import RegistryImageReferenceUnavailable, RegistryReferences

# Creation includes quota allocation, rootfs preparation, networking, and
# runsc startup. Those idempotent lifecycle operations can legitimately queue
# behind other creates on a dense direct node.
SANDBOX_CREATE_PROXY_TIMEOUT_SECONDS = 10 * 60
# A candidate with another behind it waits this long for a startup slot and
# memory before rejecting; the last candidate waits the node's full time, so
# a busy fleet does not turn all k away at once.
SHORT_ADMISSION_WAIT_SECONDS = 1.0
# Rounds of k candidates; a second round resamples without the first.
CREATE_ROUNDS = 2


class CreateExchange(Exchange, Protocol):
    """The handler's create steps this use case shares with ranked placement."""

    def _ensure_image_for_create(self, heartbeat: NodeHeartbeat, image: str,
                                 environment_root: str | None = None) -> ProxiedResponse | None: ...
    def _send_existing_sandbox_response(self, route: SandboxRoute, spec: SandboxSpec, *, status: HTTPStatus,
                                        pending: PendingSandboxDemand | None = None) -> bool: ...
    def _retry_sandbox_create_on_assigned_node(self, route: SandboxRoute, spec: SandboxSpec) -> None: ...
    def _confirm_sandbox_observation(self, route: SandboxRoute, confirm: Any = None) -> SandboxRoute | None: ...
    def _write_create_in_progress_response(self, sandbox_id: str) -> None: ...
    def _write_no_ready_node(self, demand: Any, error_code: str) -> None: ...
    def _write_image_pull_failed(self, image_response: ProxiedResponse) -> None: ...
    def _write_invalid_create_confirmation(self) -> None: ...


class CreatePlacement:
    """One overlay and chooser per process, shared by every request thread.

    ``api_processes`` counts the processes placing creates on this fleet.
    """

    def __init__(
        self, routing_store: RoutingStore, heartbeats: ControlStateStore, *,
        heartbeat_ttl_seconds: int, registry_refs: RegistryReferences,
        metrics_store: MetricsStore | None, telemetry: Telemetry,
        target_creates_per_node: int, api_processes: int, k: int = DEFAULT_K,
        rng: random.Random | None = None,
    ) -> None:
        self.routing_store, self.heartbeats = routing_store, heartbeats
        self.heartbeat_ttl_seconds, self.registry_refs = heartbeat_ttl_seconds, registry_refs
        self.metrics_store, self.telemetry = metrics_store, telemetry
        self.overlay = InflightOverlay()
        self.chooser = PowerOfKChooser(
            self.overlay, rng=rng or random.Random(), k=k,
            target_creates_per_node=target_creates_per_node, api_processes=api_processes,
        )

    def view(self) -> FleetView:
        return FleetView.ready(
            self.heartbeats.load_heartbeats(shared=True).values(), now=utc_now(),
            ttl_seconds=self.heartbeat_ttl_seconds,
            routes=migration_reservations(self.routing_store, self.routing_store.get_sandbox_readonly),
        )

    def create(
        self, ex: CreateExchange, spec: SandboxSpec, root: Any, *,
        excluded_job_ids: tuple[str, ...] = (), last_failure_reason: str = "",
    ) -> None:
        spec_dict = spec.to_dict()
        request = PlacementRequest(
            spec.requested_resources(), capabilities=_sandbox_required_capabilities(spec_dict),
            image=spec.image, spec=spec_dict,
        )
        spec_hash = sandbox_spec_fingerprint(spec)
        excluded, reason, rejections = set(excluded_job_ids), last_failure_reason, 0
        route: SandboxRoute | None = None
        pending: PendingSandboxDemand | None = None
        root.set_attribute("sandbox.placement.mode", "power_of_k")
        for _round in range(CREATE_ROUNDS):
            with self.telemetry.span("gateway.sandbox_select_node", attributes={
                "container.image.name": spec.image,
            }) as span:
                candidates = self.chooser.choose(
                    self.view(), replace(request, excluded_job_ids=frozenset(excluded)),
                )
                span.set_attribute("candidate_count", len(candidates))
            if not candidates:
                break
            for index, shared in enumerate(candidates):
                excluded.add(shared.job_id)
                heartbeat = detached_heartbeat(shared)
                allocation = SandboxRouteAllocation(
                    sandbox_id=spec.id, node_id=heartbeat.node_id, job_id=heartbeat.job_id,
                    node_url=heartbeat.node_url or "", resources=request.resources,
                    spec=dict(spec_dict), node_epoch=heartbeat.node_epoch,
                    activity_epoch=heartbeat.activity_epoch,
                )
                operation_id = f"create-{uuid4().hex}"
                if route is None:
                    try:
                        route, pending = self.routing_store.reserve_create_intent(
                            allocation, spec_hash=spec_hash, create_operation_id=operation_id,
                        )
                    except SandboxRouteConflictError:
                        root.status = "error"
                        root.set_attribute("outcome", "concurrent_spec_conflict")
                        ex._write_json({
                            "error": f"sandbox already exists with different spec: {spec.id}",
                        }, status=HTTPStatus.CONFLICT)
                        return
                    if route.create_operation_id != operation_id:
                        root.set_attribute("outcome", "concurrent_route_won")
                        ex._retry_sandbox_create_on_assigned_node(route, spec)
                        return
                    self._ensure_reference(spec, route)
                    root.set_attribute("reserved_route", True)
                else:
                    moved = self.routing_store.retarget_create_intent(
                        route, allocation, create_operation_id=operation_id,
                    )
                    if moved is None:
                        root.status = "error"
                        root.set_attribute("outcome", "route_changed_during_reselect")
                        ex._write_create_in_progress_response(spec.id)
                        return
                    route, previous = moved, route
                    try:
                        self._ensure_reference(spec, route)
                    finally:
                        self.registry_refs.release_route_reference(previous)
                self.overlay.reserve(shared, _incarnation(route), request)
                rejected = self._attempt(
                    ex, spec, route, heartbeat, pending, root,
                    last=index == len(candidates) - 1,
                )
                if rejected is None:
                    return
                self.overlay.release(route.job_id, _incarnation(route))
                reason, rejections = rejected, rejections + 1
                root.set_attribute("rejected_jobs", rejections)
        demand_fence: dict[str, Any] = {}
        if route is not None:
            removed = self.routing_store.delete_sandbox_if_current(
                spec.id, generation=route.generation, create_operation_id=route.create_operation_id,
            )
            if removed is None:
                root.status = "error"
                root.set_attribute("outcome", "route_changed_during_reselect")
                ex._write_create_in_progress_response(spec.id)
                return
            self.registry_refs.release_route_reference(removed)
            demand_fence = {"generation": route.generation, "operation_id": route.create_operation_id,
                            "spec_hash": route.spec_hash}
        _pending, demand = self.routing_store.upsert_pending_with_demand(
            spec.id, request.resources, failure_reason=reason, **demand_fence,
        )
        root.status = "error"
        root.set_attribute("outcome", "queued_no_ready_node" if route is None else "rejected_by_all")
        root.set_attribute("pending_resources", demand.pending_resources.to_dict())
        ex._write_no_ready_node(demand, reason or "no_ready_node")

    def _attempt(
        self, ex: CreateExchange, spec: SandboxSpec, route: SandboxRoute,
        heartbeat: NodeHeartbeat, pending: PendingSandboxDemand | None, root: Any, *, last: bool,
    ) -> str | None:
        """Dispatch to one candidate: a definite reject's reason, else None
        once a response has been written."""

        with self.telemetry.span("gateway.sandbox_ensure_image", attributes={
            "node.id": heartbeat.node_id, "container.image.name": spec.image,
        }) as span:
            image_response = ex._ensure_image_for_create(heartbeat, spec.image, spec.environment_root)
            span.set_attribute("pulled", image_response is not None)
            if image_response is not None:
                span.set_attribute("status_code", int(image_response.status))
        if image_response is not None:
            if image_response.json().get("error_code") == "image_warmup_pending":
                # Retain the assigned generation while its pull continues.
                root.set_attribute("outcome", "image_warmup_pending")
                ex._send_proxied_response(image_response)
                return None
            if image_response.status >= 400:
                rejected = _node_create_rejection_reason(image_response)
                if rejected is not None:
                    return rejected  # No create was dispatched.
                self._abandon(spec, route, f"image_pull_http_{image_response.status}")
                root.status = "error"
                root.set_attribute("outcome", "image_pull_failed")
                ex._write_image_pull_failed(image_response)
                return None
        wait = None if last else SHORT_ADMISSION_WAIT_SECONDS
        with self.telemetry.span("gateway.sandbox_proxy_create", attributes={
            "node.id": heartbeat.node_id, "admission_wait_seconds": wait or 0.0,
        }) as span:
            response = ex._proxy_request(
                heartbeat.node_url or "", "/v1/sandboxes", method="POST",
                body=_sandbox_create_request_body(spec, route),
                timeout_seconds=SANDBOX_CREATE_PROXY_TIMEOUT_SECONDS,
                extra_headers=None if wait is None else {ADMISSION_WAIT_HEADER: f"{wait:g}"},
            )
            span.set_attribute("status_code", int(response.status))
            payload = response.json()
            if isinstance(payload.get("timings"), dict):
                span.add_event("node.timings", payload["timings"])
        if _is_duplicate_sandbox_response(response, spec.id) and ex._send_existing_sandbox_response(
            route, spec, status=HTTPStatus.CREATED, pending=pending,
        ):
            root.set_attribute("outcome", "recovered_duplicate")
            return None
        if 200 <= response.status < 300:
            record = payload.get("sandbox")
            if not isinstance(record, dict) or not _sandbox_record_matches_route(record, route, spec):
                root.status = "error"
                root.set_attribute("outcome", "invalid_create_confirmation")
                ex._write_invalid_create_confirmation()
                return None
            confirmed = ex._confirm_sandbox_observation(
                _route_with_sandbox_record(route, record), self.routing_store.confirm_create,
            )
            if confirmed is None:
                return None
            record_sandbox_scheduled(
                self.metrics_store, sandbox_id=spec.id, route=confirmed,
                resources=spec.requested_resources(), pending=pending,
            )
            self.registry_refs.record_image_used(spec.image)
            root.set_attribute("outcome", "scheduled")
            root.set_attribute("node_id", heartbeat.node_id)
            ex._send_proxied_response(response)
            return None
        rejected = _node_create_rejection_reason(response)
        if rejected is not None:
            return rejected
        root.status = "error"
        root.set_attribute("outcome", "node_create_failed")
        root.set_attribute("status_code", int(response.status))
        if _node_create_may_still_be_running(response):
            root.set_attribute("kept_durable_route", True)
        else:
            self.overlay.release(route.job_id, _incarnation(route))
            removed = self.routing_store.delete_sandbox_if_current(
                spec.id, generation=route.generation, create_operation_id=route.create_operation_id,
            )
            if removed is not None:
                self.registry_refs.release_route_reference(removed)
        ex._send_proxied_response(response)
        return None

    def _ensure_reference(self, spec: SandboxSpec, route: SandboxRoute) -> None:
        try:
            self.registry_refs.ensure_route_reference(route, touch=True)
        except RegistryImageReferenceUnavailable:
            # Nothing was dispatched for this incarnation: fail closed and
            # keep the demand. A retry allocates a new route incarnation.
            self._abandon(spec, route, "registry_lease_unavailable")
            raise

    def _abandon(self, spec: SandboxSpec, route: SandboxRoute, reason: str) -> None:
        self.overlay.release(route.job_id, _incarnation(route))
        removed = self.routing_store.delete_sandbox_if_current(
            spec.id, generation=route.generation, create_operation_id=route.create_operation_id,
        )
        if removed is not None:
            self.registry_refs.release_route_reference(removed)
            self.routing_store.upsert_pending(
                spec.id, spec.requested_resources(), generation=route.generation,
                operation_id=route.create_operation_id, spec_hash=route.spec_hash,
                failure_reason=reason,
            )


def _incarnation(route: SandboxRoute) -> SandboxIncarnation:
    return SandboxIncarnation(
        route.sandbox_id, route.generation, route.spec_hash, route.create_operation_id,
    )


def _is_duplicate_sandbox_response(response: ProxiedResponse, sandbox_id: str) -> bool:
    if response.status not in {HTTPStatus.BAD_REQUEST, HTTPStatus.CONFLICT}:
        return False
    error_message = str(response.json().get("error") or "").lower()
    return "already exists" in error_message and sandbox_id.lower() in error_message


def _node_create_may_still_be_running(response: ProxiedResponse) -> bool:
    if response.transport_error_kind == "dns":
        # DNS lookup failed before an HTTP connection could be established, so
        # the node cannot have received or persisted this create operation.
        return False
    return response.status in {408, 425, 429, 500, 502, 503, 504}


def _node_create_definitively_rejected(response: ProxiedResponse) -> bool:
    """An explicit pre-provisioning rejection is safe to place elsewhere."""

    return _node_create_rejection_reason(response) is not None


def _node_create_rejection_reason(response: ProxiedResponse) -> str | None:
    if response.status != HTTPStatus.SERVICE_UNAVAILABLE:
        return None
    payload = response.json()
    error_code = str(payload.get("error_code") or "")
    if error_code in {
        "node_admission_closed",
        "node_active_admission_deferred",
    }:
        return error_code
    return None
