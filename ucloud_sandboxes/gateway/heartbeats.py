"""Authenticated worker heartbeats: identity, receipt and route reconciliation."""

from __future__ import annotations

from dataclasses import dataclass, replace
from http import HTTPStatus
import sqlite3
from typing import Any
from urllib.parse import urlparse

from ..control_state import QUARANTINE_REASON, ControlStateStore
from ..metrics import MetricsStore, record_node_heartbeat
from ..models import NodeHeartbeat, SandboxInventoryEntry, utc_now
from ..registry import HeartbeatIdentityError, heartbeat_from_dict, heartbeat_to_dict
from ..routing import RoutingStore, SandboxRoute, route_with_inventory_snapshot
from .image_resolution import RegistryLayerMetadataCache
from .registry_refs import (
    RegistryImageReferenceUnavailable, RegistryReferences, _portable_snapshot_for_route,
)


@dataclass(frozen=True)
class HeartbeatOutcome:
    """The response to one heartbeat; ``accepted`` means new state persisted."""

    status: int
    payload: dict[str, Any]
    headers: dict[str, str] | None = None
    accepted: bool = False


class HeartbeatIngest:
    """Persist a worker's heartbeat and reconcile the routes it proves.

    Holds stores and configuration only, so every request thread shares one
    instance. The caller has already authenticated the worker channel.
    """

    def __init__(
        self, *, store: ControlStateStore, routing_store: RoutingStore,
        metrics_store: MetricsStore, deployment_id: str, registry_refs: RegistryReferences,
        layer_cache: RegistryLayerMetadataCache | None,
    ) -> None:
        self.store = store
        self.routing_store = routing_store
        self.metrics_store = metrics_store
        self.deployment_id = deployment_id
        self.registry_refs = registry_refs
        self.layer_cache = layer_cache

    def receive(self, raw: Any) -> HeartbeatOutcome:
        """Validate, persist and reconcile one decoded heartbeat body.

        The receipt is durable before any reconciliation; a reconcile failure
        raises, and the worker's next heartbeat replays it.
        """
        if not isinstance(raw, dict):
            return _rejected(HTTPStatus.BAD_REQUEST, "heartbeat payload must be a JSON object")
        try:
            heartbeat = heartbeat_from_dict(raw)
        except (TypeError, ValueError, OverflowError):
            heartbeat = None
        if heartbeat is None:
            return _rejected(HTTPStatus.BAD_REQUEST, "invalid heartbeat payload")
        if heartbeat.deployment_id != self.deployment_id:
            return HeartbeatOutcome(HTTPStatus.FORBIDDEN, {
                "error": "heartbeat deployment_id does not match this gateway",
                "expected_deployment_id": self.deployment_id,
            })
        identity_error = self.identity_error(heartbeat)
        if identity_error is not None:
            return _rejected(HTTPStatus.FORBIDDEN, identity_error)
        received_at = utc_now()
        # The sender controls neither freshness nor the idle-grace clock. Keep
        # its timestamp as reported_at for diagnostics while recording the
        # gateway-controlled receipt time used for freshness.
        heartbeat = replace(
            heartbeat, node_url=_canonical_node_url(heartbeat.node_url), updated_at=received_at,
            reported_at=heartbeat.reported_at or heartbeat.updated_at, received_at=received_at,
            idle_since=None,
        )
        try:
            receipt = self.store.receive_heartbeat(heartbeat)
        except HeartbeatIdentityError as exc:
            return _rejected(HTTPStatus.FORBIDDEN, str(exc))
        except ValueError as exc:
            # The store preserves SQLite's cause when wrapping storage errors.
            # A heartbeat can safely retry after lock contention, including an
            # ambiguous receipt. Do not turn corruption or I/O errors into busy.
            cause = exc.__cause__
            if not isinstance(cause, sqlite3.OperationalError) or not str(cause).startswith(
                ("database is locked", "database table is locked")
            ):
                raise
            return HeartbeatOutcome(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "heartbeat storage is temporarily busy",
                 "error_code": "heartbeat_storage_busy", "retryable": True},
                {"Retry-After": "1", "X-UCloud-Retryable": "true"},
            )
        stored = receipt.stored
        if receipt.accepted:
            self._apply(stored, first=receipt.previous is None)
        return HeartbeatOutcome(
            HTTPStatus.OK, {"ok": True, "node": heartbeat_to_dict(stored)},
            accepted=receipt.accepted,
        )

    def _apply(self, heartbeat: NodeHeartbeat, *, first: bool) -> None:
        record_node_heartbeat(self.metrics_store, heartbeat, first=first)
        if self.layer_cache is not None:
            self.layer_cache.hydrate_async(heartbeat.cached_images)
        # Authenticated boot identity, unlike provider readiness, proves
        # that a previous guest process namespace no longer exists. Replay
        # cleanup after every heartbeat so a routing-store failure cannot
        # strand old routes after the new epoch was already persisted.
        for retired_epoch in heartbeat.retired_node_epochs:
            for route in self.routing_store.delete_sandboxes_for_jobs_with_error(
                (heartbeat.job_id,), terminal_error="node_lost", retired_node_epoch=retired_epoch,
            ):
                self.registry_refs.release_route_reference(route)
        if (
            not heartbeat.inventory_complete
            or not heartbeat.node_url
            or heartbeat.labels.get(QUARANTINE_REASON)
        ):
            return
        reconciled_inventory: list[SandboxInventoryEntry] = []
        prepared_snapshot_routes: list[SandboxRoute] = []
        for item in heartbeat.inventory:
            if not item.storage_snapshot:
                reconciled_inventory.append(item)
                continue
            route = self.routing_store.get_sandbox_readonly(item.sandbox_id)
            try:
                if route is None:
                    raise ValueError("inventory snapshot has no assigned route")
                candidate = route_with_inventory_snapshot(route, item)
                snapshot = _portable_snapshot_for_route(candidate)
                # The permanent Registry reference must be durable
                # before the portable route becomes durable.
                self.registry_refs.ensure_snapshot_reference(
                    candidate, repository=snapshot.reference.repository,
                    tag=snapshot.reference.tag, digest=snapshot.reference.manifest_digest,
                )
                prepared_snapshot_routes.append(candidate)
                reconciled_inventory.append(item)
            except (RegistryImageReferenceUnavailable, ValueError) as exc:
                self.metrics_store.append("sandbox_snapshot_inventory_error", {
                    "sandbox_id": item.sandbox_id, "generation": item.generation,
                    "node_id": heartbeat.node_id, "error": str(exc),
                })
                reconciled_inventory.append(replace(
                    item, storage_schema="", snapshot_manifest_digest="",
                    snapshot_repository="", snapshot_tag="", storage_snapshot={},
                ))
        removed_routes, stale_snapshot_routes = self.reconcile_inventory(
            heartbeat, reconciled_inventory, prepared_snapshot_routes,
        )
        for route in stale_snapshot_routes:
            self.registry_refs.release_snapshot_reference(route)
        for route in removed_routes:
            self.metrics_store.append("sandbox_inventory_absent", {
                "sandbox_id": route.sandbox_id, "generation": route.generation,
                "job_id": route.job_id, "node_epoch": route.node_epoch,
                "route_activity_epoch": route.activity_epoch,
                "inventory_activity_epoch": heartbeat.activity_epoch,
                "route_updated_at": route.updated_at,
                "inventory_received_at": heartbeat.freshness_at.isoformat(),
            })
            self.registry_refs.release_route_reference(route)

    def reconcile_inventory(
        self, heartbeat: NodeHeartbeat, inventory: list[SandboxInventoryEntry],
        prepared_snapshot_routes: list[SandboxRoute],
    ) -> tuple[list[SandboxRoute], list[SandboxRoute]]:
        """Reconcile inventory without leaking pre-acquired snapshot owners."""

        try:
            return self.routing_store.reconcile_sandboxes_for_node(
                heartbeat.node_url or "", inventory, node_id=heartbeat.node_id,
                job_id=heartbeat.job_id,
                reported_sandbox_ids=(item.sandbox_id for item in inventory),
                observed_at=heartbeat.freshness_at.isoformat(), node_epoch=heartbeat.node_epoch,
                activity_epoch=heartbeat.activity_epoch, inventory_complete=True,
                allow_node_epoch_adoption=False,
            )
        finally:
            # A failed SQLite commit is ambiguous. A successful read-back tells
            # us whether each candidate became durable; if read-back itself
            # fails, retaining the Registry owner is the data-safe outcome.
            for prepared_route in prepared_snapshot_routes:
                try:
                    current = self.routing_store.get_sandbox_readonly(prepared_route.sandbox_id)
                except BaseException:
                    continue
                self.registry_refs.release_snapshot_reference(prepared_route, keep_route=current)

    def identity_error(self, heartbeat: NodeHeartbeat) -> str | None:
        node_url = _canonical_node_url(heartbeat.node_url)
        if node_url is None:
            return "heartbeat node_url must be an absolute HTTP(S) origin"
        if not heartbeat.node_id or not heartbeat.job_id or not heartbeat.node_epoch:
            return "heartbeat node_id, job_id, and node_epoch are required"
        for route_node, route_job, route_url in self.routing_store.assigned_node_identities(
            node_id=heartbeat.node_id, job_id=heartbeat.job_id, node_url=node_url,
        ):
            same_job = bool(route_job) and route_job == heartbeat.job_id
            same_node = bool(route_node) and route_node == heartbeat.node_id
            same_node_url = _canonical_node_url(route_url) == node_url
            if not same_job and not same_node and not same_node_url:
                continue
            if not same_job or not same_node or not same_node_url:
                return "heartbeat identity conflicts with an assigned route"
        return None


def _rejected(status: int, error: str) -> HeartbeatOutcome:
    return HeartbeatOutcome(status, {"error": error})


def _canonical_node_url(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlparse(value.strip())
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        return None
    try:
        parsed.port
    except ValueError:
        return None
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
