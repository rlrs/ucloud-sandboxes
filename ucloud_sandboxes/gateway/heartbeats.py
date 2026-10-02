"""Authenticated worker heartbeats: identity, receipt and route reconciliation."""

from __future__ import annotations

from dataclasses import dataclass, replace
from hashlib import sha256
from http import HTTPStatus
from http.client import HTTPException
import logging
import sqlite3
from threading import Event, Lock, Thread
import time
from typing import Any, Callable
from urllib.parse import urlparse

from ..control_state import QUARANTINE_REASON, ControlStateStore
from ..metrics import MetricsStore, record_node_epoch_retired, record_node_heartbeat
from ..models import NodeHeartbeat, SandboxInventoryEntry, utc_now
from ..registry import HeartbeatIdentityError, heartbeat_from_dict, heartbeat_to_dict
from ..routing import RoutingStore, SandboxRoute, route_with_inventory_snapshot
from ..worker_receipts import _record_generation
from .image_resolution import RegistryLayerMetadataCache
from .registry_refs import (
    RegistryImageReferenceUnavailable, RegistryReferences, _portable_snapshot_for_route,
)

# A pull stands in for a late push on a request thread: one bounded worker RPC,
# and at most one per worker boot per interval however many requests wait.
# Each unanswered pull doubles the interval, up to the cap, so retries against
# a dead worker do not hold request threads on its timeouts.
PULL_TIMEOUT_SECONDS = 2.0
PULL_INTERVAL_SECONDS = 2.0
PULL_BACKOFF_MAX_SECONDS = 32.0
_UNANSWERED = frozenset({"unreachable", "identity_mismatch"})


class _Pull(Event):
    finished, unanswered = float("-inf"), 0

    def due(self, now: float) -> bool:
        return self.is_set() and now - self.finished >= min(
            PULL_INTERVAL_SECONDS * 2 ** self.unanswered, PULL_BACKOFF_MAX_SECONDS)


@dataclass(frozen=True)
class HeartbeatOutcome:
    """The response to one heartbeat; ``accepted`` means new state persisted."""

    status: int
    payload: dict[str, Any]
    headers: dict[str, str] | None = None
    accepted: bool = False


_LOG = logging.getLogger(__name__)
REBOOT_REAP_BATCH = 32
REBOOT_REAP_TIMEOUT_SECONDS = 60.0
# (node_url, sandbox_id, generation, operation_id) -> (HTTP status, JSON body).
WorkerDelete = Callable[[str, str, int, str], tuple[int, dict[str, Any]]]


@dataclass(frozen=True)
class RebootReap:
    """One generation-fenced worker delete that a retired boot still owes."""

    sandbox_id: str
    generation: int
    operation_id: str
    route: SandboxRoute | None = None  # A recorded delete, removed once confirmed.


class RebootReaper:
    """Deliver retired-boot deletes off the heartbeat thread, one pass per job.

    Every heartbeat derives the work from durable routing state, so a crash or
    a failed call only waits for the next one. The worker fences each delete
    by generation, so a replay never touches a newer incarnation.
    """

    def __init__(
        self, *, routing_store: RoutingStore, registry_refs: RegistryReferences,
        metrics_store: MetricsStore, delete_on_worker: WorkerDelete,
    ) -> None:
        self.routing_store = routing_store
        self.registry_refs = registry_refs
        self.metrics_store = metrics_store
        self.delete_on_worker = delete_on_worker
        self._guard = Lock()
        self._active: dict[str, Thread] = {}
        self._closed = False

    def schedule(self, job_id: str, node_url: str, reaps: list[RebootReap]) -> None:
        with self._guard:
            if not reaps or self._closed or job_id in self._active:
                return
            thread = self._active[job_id] = Thread(
                target=self._run, args=(job_id, node_url, reaps[:REBOOT_REAP_BATCH]),
                name="ucloud-reboot-reaper", daemon=True,
            )
        try:
            thread.start()
        except BaseException:
            with self._guard:
                self._active.pop(job_id, None)
            raise

    def close(self, timeout: float = 10.0) -> None:
        """Start no more passes; wait at most ``timeout`` for those in flight."""
        with self._guard:
            self._closed = True
            threads = tuple(self._active.values())
        deadline = time.monotonic() + timeout
        for thread in threads:
            thread.join(max(0.0, deadline - time.monotonic()))

    def _run(self, job_id: str, node_url: str, reaps: list[RebootReap]) -> None:
        try:
            for reap in reaps:
                if self._closed:
                    return
                try:
                    status, payload = self.delete_on_worker(
                        node_url, reap.sandbox_id, reap.generation, reap.operation_id)
                    confirmed = 200 <= status < 300 and _record_generation(
                        payload.get("deleted")) in {None, reap.generation}
                    if confirmed and reap.route is not None:
                        # The same commit as a client-driven delete of this intent.
                        removed = self.routing_store.delete_sandbox_if_current(
                            reap.sandbox_id, generation=reap.generation,
                            create_operation_id=reap.route.create_operation_id,
                            delete_operation_id=reap.operation_id)
                        if removed is not None:
                            self.registry_refs.release_route_reference(removed)
                    self.metrics_store.append("sandbox_reboot_reap", {
                        "sandbox_id": reap.sandbox_id, "generation": reap.generation,
                        "job_id": job_id, "client_delete": reap.route is not None,
                        "status": status, "confirmed": confirmed})
                except Exception:
                    _LOG.exception("could not reap %s of a retired boot", reap.sandbox_id)
        finally:
            with self._guard:
                self._active.pop(job_id, None)


def _reap_operation_id(job_id: str, sandbox_id: str, generation: int) -> str:
    # Stable, so every replay is the same delete intent.
    return "reboot-reap-" + sha256(f"{job_id}\0{sandbox_id}\0{generation}".encode()).hexdigest()[:32]


class HeartbeatIngest:
    """Persist a worker's heartbeat and reconcile the routes it proves.

    Every request thread shares one instance: stores, configuration and the
    in-flight pulls. A pushed heartbeat arrives on the authenticated worker
    channel; a pulled one on the gateway's own node-control channel.
    """

    def __init__(
        self, *, store: ControlStateStore, routing_store: RoutingStore,
        metrics_store: MetricsStore, deployment_id: str, registry_refs: RegistryReferences,
        layer_cache: RegistryLayerMetadataCache | None,
        reaper: RebootReaper | None = None,
    ) -> None:
        self.store = store
        self.routing_store = routing_store
        self.metrics_store = metrics_store
        self.deployment_id = deployment_id
        self.registry_refs = registry_refs
        self.layer_cache = layer_cache
        # None only for handlers built without worker access (tests).
        self.reaper = reaper
        self._pull_guard = Lock()
        self._pulls: dict[tuple[str, str], _Pull] = {}

    def refresh(
        self, previous: NodeHeartbeat, read_worker: Callable[[str], Any],
    ) -> NodeHeartbeat | None:
        """Pull ``previous``'s worker once; return the stored header of its boot.

        Silence is never loss: a request asks the worker before it answers
        that the worker is unreachable. Concurrent callers for one boot share
        one pull and wait for it at most a second past its timeout; until the
        next is due they read the store without pulling. A sample counts only
        from the stored node, job, deployment, agent version and URL, through
        the push's own ingest: it reconciles routes, and a new epoch retires
        the old boot (a retired one is refused). None means the caller's boot
        is no longer the stored one.
        """
        if not previous.node_url:
            return previous
        key, now = (previous.job_id, previous.node_epoch), time.monotonic()
        with self._pull_guard:
            pull = self._pulls.get(key)
            lead = pull is None or pull.due(now)
            if lead:
                unanswered = pull.unanswered if pull is not None else 0
                self._pulls = {k: p for k, p in self._pulls.items()
                               if not p.is_set() or now - p.finished < 2 * PULL_BACKOFF_MAX_SECONDS}
                pull = self._pulls[key] = _Pull()
        if not lead:
            pull.wait(PULL_TIMEOUT_SECONDS + 1)
        else:
            outcome = "error"
            try:
                outcome = self._pull(previous, read_worker)
            finally:
                pull.unanswered = unanswered + 1 if outcome in _UNANSWERED else 0
                pull.finished = time.monotonic()
                pull.set()
            if self.metrics_store is not None:
                self.metrics_store.append("node_heartbeat_pull", {
                    "node_id": previous.node_id, "job_id": previous.job_id,
                    "node_epoch": previous.node_epoch, "outcome": outcome,
                    "receipt_age_seconds": round((utc_now() - previous.freshness_at).total_seconds(), 3),
                })
        current = self.store.get_heartbeat(previous.job_id, include_inventory=False)
        return current if current is not None and current.node_epoch == previous.node_epoch else None

    def _pull(self, previous: NodeHeartbeat, read_worker: Callable[[str], Any]) -> str:
        try:
            raw = read_worker(previous.node_url or "")
            sample = heartbeat_from_dict(raw) if isinstance(raw, dict) else None
        except (OSError, HTTPException, ValueError, TypeError, OverflowError):
            sample = None
        if sample is None:
            return "unreachable"
        def identity(heartbeat: NodeHeartbeat) -> tuple:
            return (heartbeat.node_id, heartbeat.job_id, heartbeat.deployment_id,
                    heartbeat.agent_version, _canonical_node_url(heartbeat.node_url))
        if identity(sample) != identity(previous):
            return "identity_mismatch"
        # The worker serves its own labels; its sender merges the provider
        # labels over them, so keep the stored ones a pull cannot see.
        if not self._accept(replace(sample, labels={**previous.labels, **sample.labels})).accepted:
            return "rejected"
        return "refreshed" if sample.node_epoch == previous.node_epoch else "epoch_changed"

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
        return self._accept(heartbeat)

    def _accept(self, heartbeat: NodeHeartbeat) -> HeartbeatOutcome:
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
            record_node_epoch_retired(self.metrics_store, receipt.previous, stored)
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
        # retirement after every heartbeat so a routing-store failure cannot
        # strand old routes after the new epoch was already persisted.
        if heartbeat.retired_node_epochs:
            retirement = self.routing_store.retire_node_epochs(
                heartbeat.job_id, heartbeat.retired_node_epochs,
                node_epoch=heartbeat.node_epoch, activity_epoch=heartbeat.activity_epoch,
                inventory=heartbeat.inventory if heartbeat.inventory_complete else None,
                observed_at=heartbeat.freshness_at.isoformat(),
            )
            for route in retirement.lost:
                self.registry_refs.release_route_reference(route)
            self._reap_retired_boots(heartbeat, retirement.pending_deletes)
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

    def _reap_retired_boots(
        self, heartbeat: NodeHeartbeat, pending_deletes: tuple[SandboxRoute, ...],
    ) -> None:
        """Owed old-boot deletes: recorded intents, and new-boot entries a reboot lost."""
        if self.reaper is None or not heartbeat.node_url:
            return
        reaps = [
            RebootReap(route.sandbox_id, route.generation, route.delete_operation_id, route)
            for route in pending_deletes
        ]
        if heartbeat.inventory_complete:
            reaps.extend(
                RebootReap(sandbox_id, generation,
                           _reap_operation_id(heartbeat.job_id, sandbox_id, generation))
                for sandbox_id, generation in sorted(self.routing_store.reboot_lost_incarnations(
                    heartbeat.job_id, {(item.sandbox_id, item.generation) for item in heartbeat.inventory},
                ))
            )
        self.reaper.schedule(heartbeat.job_id, heartbeat.node_url, reaps)

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
