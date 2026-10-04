"""C3.2 group create (docs/c43-placement-wiring-plan.md, "C3.2 (implemented)").

``POST /v1/sandboxes:batch`` creates ``count`` ordinary sandboxes
``<group>-<i:04d>`` of one spec, resolved once; GET lists them and DELETE
deletes them. Members pack onto few workers, so each worker attaches the image
once: per worker one ensure, one intent transaction and one confirm. Rejected
members are re-planned elsewhere for GROUP_ROUNDS rounds; the rest become
pending demand with a retryable answer, and a repeat places only those.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import contextvars
from dataclasses import dataclass, replace
import hashlib
from http import HTTPStatus
import json
import re
from typing import Any, Callable, Protocol
from urllib.parse import unquote
from uuid import uuid4

from ..control_state import detached_heartbeat
from ..metrics import record_sandbox_scheduled
from ..models import NodeHeartbeat
from ..placement_choice import FleetView, PlacementRequest
from ..routing import (
    PendingSandboxDemand, SandboxGroup, SandboxGroupDeletedError, SandboxRoute, SandboxRouteAllocation,
)
from ..sandbox import SandboxSpec, sandbox_spec_fingerprint
from ..worker_receipts import (
    _route_with_sandbox_record, _sandbox_create_request_body, _sandbox_record_matches_route,
)
from .create import (
    SANDBOX_CREATE_PROXY_TIMEOUT_SECONDS, CreateExchange, CreatePlacement, _incarnation,
    _is_duplicate_sandbox_response, _node_create_may_still_be_running, _node_create_rejection_reason,
)
from .fleet import _heartbeat_has_image
from .placement import _sandbox_required_capabilities

GROUP_PATH = "/v1/sandboxes:batch"
# A member id appends "-dddd", within the 64 characters of a sandbox id.
GROUP_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,58}$")
MAX_GROUP_SIZE = 512
GROUP_ROUNDS = 4
MAX_GROUP_DISPATCH = 32  # Member POSTs one group has in flight, over all its workers.
MEMBER_DELETE_CONCURRENCY = 8
UNFINISHED = frozenset({"pending", "creating"})


def group_budget(target_creates_per_node: int) -> int:
    """Members packed onto one worker: twice its create concurrency keeps its
    admission queue short; past 32 one attach no longer pays for the wait."""
    return min(32, 2 * target_creates_per_node)


def group_id_from_path(path: str) -> str | None:
    group_id = unquote(path[len(GROUP_PATH) + 1:]) if path.startswith(GROUP_PATH + "/") else ""
    return group_id if GROUP_ID_RE.match(group_id) else None


@dataclass(frozen=True)
class GroupRequest:
    group_id: str
    count: int
    template: dict[str, Any]
    policy: str = "pack"

    @property
    def request_hash(self) -> str:
        """Identifies a retry: the client's spec and count, not the resolution."""
        raw = json.dumps([self.count, self.template], sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def member(self) -> SandboxSpec:
        return SandboxSpec.from_dict({**self.template, "id": f"{self.group_id}-0000"})


def parse_group_request(raw: object) -> GroupRequest:
    if not isinstance(raw, dict) or set(raw) - {"group_id", "count", "spec", "placement"}:
        raise ValueError("group payload is an object of group_id, count, spec and placement")
    group_id, count, template = raw.get("group_id"), raw.get("count"), raw.get("spec")
    if not isinstance(group_id, str) or not GROUP_ID_RE.match(group_id):
        raise ValueError("group_id must be 1-59 characters of [A-Za-z0-9_.-], starting alphanumeric")
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= MAX_GROUP_SIZE:
        raise ValueError(f"count must be an integer from 1 to {MAX_GROUP_SIZE}")
    if not isinstance(template, dict) or "id" in template:
        raise ValueError("spec must be a sandbox spec object without id")
    if raw.get("placement", "pack") not in ("pack", "spread"):
        raise ValueError("placement must be pack or spread")
    request = GroupRequest(group_id, count, dict(template), raw.get("placement", "pack"))
    spec = request.member()
    if spec.environment_root is not None:
        raise ValueError("environment_root is set by the gateway, not by clients")
    spec.validate()
    return request


class GroupExchange(CreateExchange, Protocol):
    def _sandbox_record_on_node(self, node_url: str, sandbox_id: str) -> dict[str, Any] | None: ...
    def _loopback_delete(self, sandbox_id: str) -> tuple[int, dict[str, Any]]: ...


@dataclass(eq=False)
class _Member:
    spec: SandboxSpec
    route: SandboxRoute | None = None  # Its intent, once it has one.
    pending: PendingSandboxDemand | None = None

    @property
    def id(self) -> str:
        return self.spec.id


def _result(member_id: str, status: str, route: SandboxRoute | None = None, **extra: Any) -> dict[str, Any]:
    owner = {"generation": route.generation, "node_id": route.node_id} if route is not None else {}
    return {"id": member_id, "status": status, **owner, **extra}


def _conflict(member_id: str) -> dict[str, Any]:
    return _result(member_id, "conflict", error="sandbox already exists with a different spec")


class GroupCreate:
    """Shares the per-process overlay and chooser of the single create path."""

    def __init__(self, creates: CreatePlacement, *, target_creates_per_node: int) -> None:
        self.creates, self.store = creates, creates.routing_store
        self.budget = group_budget(target_creates_per_node)

    def create(
        self, ex: GroupExchange, group: SandboxGroup, policy: str, root: Any,
    ) -> tuple[int, dict[str, Any], dict[str, str]]:
        specs = [SandboxSpec.from_dict({**group.spec, "id": member}) for member in group.member_ids()]
        routes = self.store.sandbox_routes_by_id_readonly([spec.id for spec in specs])
        results: dict[str, dict[str, Any]] = {}
        unplaced: list[_Member] = []
        replays: dict[str, list[_Member]] = {}
        for spec in specs:
            route = routes.get(spec.id)
            if route is None and spec.id not in group.placed:
                unplaced.append(_Member(spec))
            elif route is None or route.delete_operation_id:  # Deleted since the group confirmed it.
                results[spec.id] = _result(spec.id, "deleted")
            elif route.spec_hash != sandbox_spec_fingerprint(spec):
                results[spec.id] = _conflict(spec.id)
            elif route.state.lower() in {"creating", "unknown"}:
                # An ambiguous earlier attempt: replay that incarnation where it is.
                replays.setdefault(route.job_id, []).append(_Member(spec, route))
            else:
                results[spec.id] = _result(spec.id, route.state.lower(), route)
        spec_dict = specs[0].to_dict()
        request = PlacementRequest(
            specs[0].requested_resources(), capabilities=_sandbox_required_capabilities(spec_dict),
            image=specs[0].image, spec=spec_dict,
        )
        root.set_attribute("sandbox.group.unplaced", len(unplaced))
        excluded: set[str] = set()
        reason = ""
        view = self.creates.view()
        for round_index in range(GROUP_ROUNDS):
            batches: list[tuple[NodeHeartbeat, list[_Member]]] = []
            ready = {node.job_id: node for node in view.nodes}
            for job_id, members in replays.items() if round_index == 0 else ():
                if job_id in ready:
                    batches.append((ready[job_id], members))
                else:
                    results.update({m.id: _result(m.id, "creating", m.route) for m in members})
            try:
                planned, unplaced = self._plan(view, request, unplaced, excluded, policy, group, results)
            except SandboxGroupDeletedError:
                for member in unplaced:  # Intents written before the delete are its to remove.
                    if member.route is not None:
                        self.creates.overlay.release(member.route.job_id, _incarnation(member.route))
                results.update({m.id: _result(m.id, "deleted") for m in unplaced})
                planned, unplaced = [], []
            if not batches + planned:
                break
            for outcome, rejected in self._run(ex, group, request, batches + planned):
                results.update(outcome)
                for member, reason in rejected:
                    excluded.add(member.route.job_id)
                    unplaced.append(member)
            root.set_attribute("sandbox.group.rounds", round_index + 1)
            if not unplaced:
                break
            view = self.creates.view()
        for member in unplaced:
            results[member.id] = self._leave_pending(member, reason or "no_ready_node")
        members = [results[spec.id] for spec in specs]
        counts: dict[str, int] = {}
        for item in members:
            counts[item["status"]] = counts.get(item["status"], 0) + 1
        created = any(item.get("sandbox") for item in members)
        if created:
            self.creates.registry_refs.record_image_used(request.image)
        root.set_attribute("sandbox.group.counts", json.dumps(counts, sort_keys=True))
        payload: dict[str, Any] = {
            "group": {"id": group.group_id, "count": group.count, "state": group.state,
                      "image": request.image, "placement": policy},
            "sandboxes": members, "counts": counts,
        }
        if counts.get("conflict") or counts.get("failed"):
            status = HTTPStatus.CONFLICT if counts.get("conflict") else HTTPStatus.BAD_GATEWAY
            return status, {**payload, "retryable": False}, {}
        if any(counts.get(status) for status in UNFINISHED):
            return HTTPStatus.SERVICE_UNAVAILABLE, {
                **payload, "error": "some group members are not placed yet; repeat the request",
                "error_code": reason or "sandbox_group_incomplete", "retryable": True,
            }, {"Retry-After": "1", "X-UCloud-Sandbox-Retryable": "true"}
        return (HTTPStatus.CREATED if created else HTTPStatus.OK), payload, {}

    def _plan(
        self, view: FleetView, request: PlacementRequest, members: list[_Member],
        excluded: set[str], policy: str, group: SandboxGroup, results: dict[str, dict[str, Any]],
    ) -> tuple[list[tuple[NodeHeartbeat, list[_Member]]], list[_Member]]:
        """Pack ``members`` and write each worker's intents in one transaction.
        Returns the batches and the members the view fits nowhere."""

        if not members:
            return [], []
        require_digest = self.creates.registry_refs.requires_digest_identity(request.image)
        resident = frozenset(
            node.job_id for node in view.nodes
            if _heartbeat_has_image(node, request.image, require_digest=require_digest)
        )
        slices = self.creates.chooser.plan_group(
            view, replace(
                request, excluded_job_ids=frozenset(excluded),
                residency={node.node_id: 1.0 for node in view.nodes if node.job_id in resident},
            ), len(members), per_node_budget=self.budget, policy=policy, include_job_ids=resident,
        )
        batches, remaining = [], list(members)
        for item in slices:
            chosen, remaining = remaining[:item.count], remaining[item.count:]
            heartbeat = detached_heartbeat(item.heartbeat)
            fresh = [member for member in chosen if member.route is None]
            moving = [member for member in chosen if member.route is not None]
            operations = {member.id: f"create-{uuid4().hex}" for member in chosen}
            placed: list[_Member] = []
            for member, intent in zip(fresh, self.store.reserve_create_intents(
                [self._allocation(member.spec, heartbeat) for member in fresh],
                spec_hashes=[sandbox_spec_fingerprint(member.spec) for member in fresh],
                operation_ids=[operations[member.id] for member in fresh], group_id=group.group_id,
            ) if fresh else ()):
                if intent is None:
                    results[member.id] = _conflict(member.id)
                elif intent[0].create_operation_id != operations[member.id]:
                    # Another create holds this id; a repeat of the request replays it.
                    results[member.id] = _result(member.id, "creating", intent[0])
                else:
                    member.route, member.pending = intent
                    self.creates._ensure_reference(member.spec, member.route)
                    placed.append(member)
            for member, route in zip(moving, self.store.retarget_create_intents(
                [(member.route, self._allocation(member.spec, heartbeat), operations[member.id])
                 for member in moving], group_id=group.group_id,
            ) if moving else ()):
                if route is None:  # Deleted or answered since: a repeat reports it.
                    results[member.id] = _result(member.id, "creating", member.route)
                    continue
                previous, member.route = member.route, route
                try:
                    self.creates._ensure_reference(member.spec, route)
                finally:
                    self.creates.registry_refs.release_route_reference(previous)
                placed.append(member)
            for member in placed:
                self.creates.overlay.reserve(item.heartbeat, _incarnation(member.route), request)
            if placed:
                batches.append((item.heartbeat, placed))
        return batches, remaining

    @staticmethod
    def _allocation(spec: SandboxSpec, heartbeat: NodeHeartbeat) -> SandboxRouteAllocation:
        return SandboxRouteAllocation(
            sandbox_id=spec.id, node_id=heartbeat.node_id, job_id=heartbeat.job_id,
            node_url=heartbeat.node_url or "", resources=spec.requested_resources(),
            spec=spec.to_dict(), node_epoch=heartbeat.node_epoch,
            activity_epoch=heartbeat.activity_epoch,
        )

    def _run(self, ex: GroupExchange, group: SandboxGroup, request: PlacementRequest,
             batches: list[tuple[NodeHeartbeat, list[_Member]]]) -> list[Any]:
        """Each worker's batch on its own thread; member POSTs share one pool."""

        width = min(MAX_GROUP_DISPATCH, sum(len(members) for _node, members in batches))
        with ThreadPoolExecutor(len(batches)) as workers, ThreadPoolExecutor(width) as posts:
            return [future.result() for future in [
                workers.submit(_in_context(self._run_batch), ex, group, request, shared, members, posts)
                for shared, members in batches
            ]]

    def _run_batch(
        self, ex: GroupExchange, group: SandboxGroup, request: PlacementRequest,
        shared: NodeHeartbeat, members: list[_Member], posts: ThreadPoolExecutor,
    ) -> tuple[dict[str, dict[str, Any]], list[tuple[_Member, str]]]:
        """One worker's members: their results, and the definitely rejected."""

        heartbeat, node_url = detached_heartbeat(shared), shared.node_url or ""
        results: dict[str, dict[str, Any]] = {}
        rejected: list[tuple[_Member, str]] = []
        image_response = ex._ensure_image_for_create(heartbeat, request.image)  # Once per worker.
        if image_response is not None and image_response.json().get("error_code") == "image_warmup_pending":
            # The pull continues; the members keep their incarnations here.
            return {m.id: _result(m.id, "creating", m.route, error_code="image_warmup_pending")
                    for m in members}, []
        if image_response is not None and image_response.status >= 400:
            reason = _node_create_rejection_reason(image_response)
            for member in members:
                if reason is not None:
                    self.creates.overlay.release(member.route.job_id, _incarnation(member.route))
                    rejected.append((member, reason))
                else:
                    self.creates._abandon(member.spec, member.route, f"image_pull_http_{image_response.status}")
                    results[member.id] = _result(member.id, "failed", error_code="image_pull_failed",
                                                 pull=image_response.json())
            return results, rejected
        responses = [posts.submit(
            _in_context(ex._proxy_request), node_url, "/v1/sandboxes", method="POST",
            body=_sandbox_create_request_body(member.spec, member.route),
            timeout_seconds=SANDBOX_CREATE_PROXY_TIMEOUT_SECONDS,
        ) for member in members]
        accepted: list[tuple[_Member, dict[str, Any]]] = []
        for member, future in zip(members, responses):
            response, route = future.result(), member.route
            duplicate = _is_duplicate_sandbox_response(response, member.id)
            record = (ex._sandbox_record_on_node(node_url, member.id) if duplicate
                      else response.json().get("sandbox") if 200 <= response.status < 300 else None)
            reason = _node_create_rejection_reason(response)
            if isinstance(record, dict) and _sandbox_record_matches_route(record, route, member.spec):
                accepted.append((member, record))
            elif reason is not None:
                self.creates.overlay.release(route.job_id, _incarnation(route))
                rejected.append((member, reason))
            elif duplicate or 200 <= response.status < 300 or _node_create_may_still_be_running(response):
                # The worker may hold this incarnation: keep its route and charge.
                results[member.id] = _result(member.id, "creating", route,
                                             error_code=str(response.json().get("error_code") or ""))
            else:
                self.creates.overlay.release(route.job_id, _incarnation(route))
                removed = self.store.delete_sandbox_if_current(
                    member.id, generation=route.generation, create_operation_id=route.create_operation_id)
                if removed is not None:
                    self.creates.registry_refs.release_route_reference(removed)
                results[member.id] = _result(member.id, "failed", status_code=int(response.status),
                                             error=response.json().get("error") or "node create failed")
        confirmed = self.store.confirm_creates(
            [_route_with_sandbox_record(member.route, record) for member, record in accepted],
            group_id=group.group_id,
        ) if accepted else []
        for (member, record), route in zip(accepted, confirmed):
            if route is None:  # Deleted while it was being created.
                results[member.id] = _result(member.id, "deleted")
                continue
            record_sandbox_scheduled(
                self.creates.metrics_store, sandbox_id=member.id, route=route,
                resources=member.spec.requested_resources(), pending=member.pending,
            )
            results[member.id] = _result(member.id, route.state.lower(), route, sandbox=record)
        return results, rejected

    def _leave_pending(self, member: _Member, reason: str) -> dict[str, Any]:
        """No worker took it: drop its intent, if any, and queue fenced demand."""

        if member.route is None:
            self.store.upsert_pending(member.id, member.spec.requested_resources(), failure_reason=reason)
        elif self.creates._abandon(member.spec, member.route, reason) is None:
            return _result(member.id, "creating", member.route)  # Its route changed meanwhile.
        return _result(member.id, "pending", error_code=reason)

    def status(self, group_id: str) -> tuple[int, dict[str, Any]]:
        group = self.store.sandbox_group(group_id)
        if group is None:
            return HTTPStatus.NOT_FOUND, {"error": "sandbox group not found"}
        return HTTPStatus.OK, self._members(group)

    def _members(self, group: SandboxGroup) -> dict[str, Any]:
        routes = self.store.sandbox_routes_by_id_readonly(group.member_ids())
        absent = "pending" if group.state == "active" else "deleted"
        return {
            "group": {"id": group.group_id, "count": group.count, "state": group.state,
                      "image": group.spec.get("image", "")},
            "sandboxes": [
                _result(member, routes[member].state.lower(), routes[member]) if member in routes
                else _result(member, "deleted" if member in group.placed else absent)
                for member in group.member_ids()
            ],
        }

    def delete(self, ex: GroupExchange, group_id: str) -> tuple[int, dict[str, Any], dict[str, str]]:
        """Refuse the group further intents, then delete each member through
        the gateway's own single-sandbox delete."""

        group = self.store.delete_sandbox_group(group_id)
        if group is None:
            return HTTPStatus.NOT_FOUND, {"error": "sandbox group not found"}, {}
        routed = sorted(self.store.sandbox_routes_by_id_readonly(group.member_ids()))
        for member in set(group.member_ids()) - set(routed):
            self.store.clear_pending(member)
        with ThreadPoolExecutor(MEMBER_DELETE_CONCURRENCY) as pool:
            answers = list(pool.map(_in_context(ex._loopback_delete), routed))
        outcomes = [{"id": member, "status_code": status, **({"error": body.get("error")} if status >= 300 else {})}
                    for member, (status, body) in zip(routed, answers)]
        payload = {**self._members(group), "deleted": outcomes}
        if any(item["status_code"] >= 300 and item["status_code"] != HTTPStatus.NOT_FOUND for item in outcomes):
            return HTTPStatus.SERVICE_UNAVAILABLE, {**payload, "retryable": True}, {
                "Retry-After": "1", "X-UCloud-Sandbox-Retryable": "true"}
        return HTTPStatus.OK, payload, {}


def _in_context(function: Callable[..., Any]) -> Callable[..., Any]:
    """Run on a pool thread in a copy of the caller's context: its trace span
    and, on the placement worker, the durable command claim."""

    context = contextvars.copy_context()
    return lambda *args, **kwargs: context.copy().run(function, *args, **kwargs)
