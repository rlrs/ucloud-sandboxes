from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from http import HTTPStatus
import fcntl
import hashlib
import hmac
import json
import math
from pathlib import Path
import sqlite3
import socket
from threading import BoundedSemaphore, Event, RLock, Thread
import time
from typing import Any, Callable
from urllib import error, request
from urllib.parse import parse_qs, quote, unquote, urlparse
from uuid import uuid4

import urllib3
from urllib3.exceptions import HTTPError as Urllib3HTTPError

from .capabilities import (
    DISK_QUOTA_CAPABILITY,
    HIBERNATE_LOCAL_CAPABILITY,
    MANAGED_PRIMARY_CAPABILITY,
    STORAGE_NATIVE_CAPABILITY,
    STORAGE_NATIVE_MIGRATION_CAPABILITY,
    has_capability,
)
from .build_context_store import (
    BuildContextBlobStore,
    BuildContextHttpHandler,
    build_context_digest_from_path,
)
from .dashboard import dashboard_asset
from .deployment import agent_version_is_schedulable, service_health
from .storage_native_migration import (
    STORAGE_NATIVE_MIGRATION_SCHEMA,
    StorageNativeMigration,
)
from .hibernation import hibernation_disk_reservation_mb
from .http_server import (
    DEFAULT_MAX_JSON_BODY_BYTES,
    HighBacklogThreadingHTTPServer,
    traced_http_request,
)
from .http_contract import SandboxHttpRoute, match_sandbox_http_route
from .image_inventory_cache import ImageInventoryCache, ImageInventorySnapshot
from .images import (
    DockerImageRuntime,
    ImageBuildSpec,
    ImageManager,
    ImageRecord,
    ImageStore,
    image_id_from_tag,
    uploaded_build_context_reference,
)
from .managed_registry import (
    RegistryClient,
    RegistryManifestLayers,
    RegistryRequestError,
    RegistryUsageStore,
    RegistryUsageStateError,
    canonical_image_digest_ref,
    digest_protection_tag,
    image_ref_with_manifest_digest,
    manifest_digest_from_image_ref,
    normalize_manifest_digest,
    registry_host_from_image_ref,
    registry_repository_tag_from_image_ref,
    registry_summary,
)
from .managed_process import ManagedProcessRecord
from .metrics import (
    GatewayBusySampler,
    MetricsStore,
    build_metrics_snapshot,
    record_node_heartbeat,
    record_sandbox_pending_deleted,
    record_sandbox_scheduled,
)
from .telemetry import Telemetry
from .models import (
    NodeHeartbeat,
    ResourceQuantity,
    SandboxInventoryEntry,
    ScalePolicy,
    parse_iso_datetime,
    sandbox_route_state_from_observation,
    utc_now,
)
from .program_scheduler import (
    WakeNodeCandidate,
    node_pressure_score,
    plan_shadow_wake_queue,
)
from .resource_admission import node_accepts_dynamic_request
from .control_state import ControlStateStore
from .registry import (
    HeartbeatIdentityError,
    heartbeat_from_dict,
    heartbeat_to_dict,
)
from .routing import (
    ExecRoute,
    MAX_PREPARED_CAPACITY_COUNT,
    PendingImageWarmup,
    PendingSandboxDemand,
    ProgramRequestState,
    RoutingStore,
    SandboxRoute,
    SandboxRouteAllocation,
    SandboxRouteConflictError,
    is_portable_parked_route,
    is_worker_detachable_parked_route,
    route_with_inventory_snapshot,
)
from .sandbox import SandboxSpec, sandbox_spec_fingerprint, sandbox_specs_match


_IMAGE_PULL_LOCKS_GUARD = RLock()
_IMAGE_PULL_LOCKS: dict[tuple[str, str], RLock] = {}
_IMAGE_WARMUP_TASKS_GUARD = RLock()
_IMAGE_WARMUP_TASKS: set[tuple[str, str]] = set()
_GATEWAY_SCHEDULING_LOCK = RLock()
_MIGRATION_OPERATION_LOCKS_GUARD = RLock()
_MIGRATION_OPERATION_LOCKS: dict[str, tuple[RLock, int]] = {}
_REGISTRY_LEASE_COORDINATION_LOCK = RLock()
REGISTRY_IMAGE_LEASE_TTL_SECONDS = 60 * 60
DEFAULT_MAX_CONCURRENT_SANDBOX_CREATES = 32
DEFAULT_MAX_GATEWAY_HTTP_REQUEST_THREADS = 2048
SANDBOX_CREATE_BUSY_RETRY_AFTER_SECONDS = 2
SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS = 5
SANDBOX_PLACEMENT_LOCK_WAIT_SECONDS = 0.25
# Build execution is asynchronous. This timeout only covers proxying the build
# context and enqueueing the build on a builder node.
IMAGE_BUILD_PROXY_TIMEOUT_SECONDS = 30 * 60
IMAGE_PULL_PROXY_TIMEOUT_SECONDS = 30 * 60
IMAGE_PULL_RETRY_ATTEMPTS = 3
IMAGE_PULL_RETRY_BASE_DELAY_SECONDS = 0.25
# Creation includes quota allocation, rootfs preparation, networking, and
# runsc startup. Those idempotent lifecycle operations can legitimately queue
# behind other creates on a dense direct node.
SANDBOX_CREATE_PROXY_TIMEOUT_SECONDS = 10 * 60
DEFAULT_PROXY_TIMEOUT_SECONDS = 60
DEFAULT_MAX_PROXY_BODY_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_PROXY_RESPONSE_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_PROXY_ERROR_BYTES = 1024 * 1024
PROXY_STREAM_CHUNK_BYTES = 64 * 1024
DEFAULT_MAX_BUILD_CONTEXT_STORE_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_MAX_BUILD_CONTEXT_ENTRIES = 128
DEFAULT_MAX_BUILD_CONTEXT_AGE_SECONDS = 24 * 60 * 60
NODE_RECONCILE_PROXY_TIMEOUT_SECONDS = 5
NODE_RECOVERY_PROXY_TIMEOUT_SECONDS = 5
SANDBOX_GENERATION_HEADER = "X-UCloud-Sandbox-Generation"
SANDBOX_OPERATION_ID_HEADER = "X-UCloud-Sandbox-Operation-Id"
SANDBOX_TRANSPORT_RESET_HEADER = "X-UCloud-Sandbox-Transport-Reset"
SANDBOX_TRANSPORT_EPOCH_HEADER = "X-UCloud-Sandbox-Transport-Epoch"
IMAGE_REFERENCE_KIND_HEADER = "X-UCloud-Image-Reference-Kind"
MANAGED_REGISTRY_DIGEST_PROTECTION_UNAVAILABLE_ERROR_CODE = (
    "managed_registry_digest_protection_unavailable"
)
TRANSIENT_IMAGE_RESOLUTION_ERROR_CODES = frozenset(
    {
        "image_inventory_incomplete",
        MANAGED_REGISTRY_DIGEST_PROTECTION_UNAVAILABLE_ERROR_CODE,
    }
)
REGISTRY_METRICS_TIMEOUT_SECONDS = 1.5
DEFAULT_METRICS_EVENT_LIMIT = 500
FULL_METRICS_EVENT_LIMIT = 10000
METRICS_RESPONSE_CACHE_TTL_SECONDS = 1.0
REGISTRY_STATUS_CACHE_TTL_SECONDS = 30.0
REGISTRY_LAYER_METADATA_TIMEOUT_SECONDS = 2.0
REGISTRY_LAYER_METADATA_CACHE_MAX_ENTRIES = 4096
REGISTRY_MANIFEST_CACHE_MAX_ENTRIES = 4096
REGISTRY_IMMUTABLE_MANIFEST_CACHE_TTL_SECONDS = 5 * 60.0
REGISTRY_MUTABLE_MANIFEST_CACHE_TTL_SECONDS = 5.0
IMAGE_INVENTORY_CACHE_TTL_SECONDS = 5.0
NODE_HTTP_POOL_CONNECTIONS_PER_ORIGIN = 128
NODE_HTTP_POOL_ORIGINS = 64
# Treat each additional distinct cold image like 256 MiB of missing transfer.
# For the observed ~1.1 GiB shared TMax base this spreads after roughly four
# concurrent related pulls instead of concentrating an entire burst on one node.
COLD_PULL_PRESSURE_PENALTY_BYTES = 256 * 1024 * 1024


def _migration_pending_demand_id(sandbox_id: str) -> str:
    return f"__migration__:{sandbox_id}"


def _wake_pending_demand_id(sandbox_id: str) -> str:
    return f"__wake__:{sandbox_id}"


def _sandbox_required_capabilities(spec: dict[str, Any]) -> tuple[str, ...]:
    if not bool(spec.get("parkable")):
        return ()
    return tuple(
        capability
        for capability in (
            HIBERNATE_LOCAL_CAPABILITY,
            DISK_QUOTA_CAPABILITY,
            MANAGED_PRIMARY_CAPABILITY if bool(spec.get("managed_process")) else "",
        )
        if capability
    )


def _sandbox_supports_managed_lifecycle(spec: dict[str, Any]) -> bool:
    """Return whether request-bound relay park/wake is valid for this spec."""

    try:
        parsed = SandboxSpec.from_dict(spec)
    except (TypeError, ValueError):
        return False
    return parsed.parkable and parsed.managed_process


def _sandbox_request_wakes(path: str, method: str) -> bool:
    route = match_sandbox_http_route(method, path)
    return bool(route is not None and route.wakes)


def _sandbox_transport_epoch(
    route: SandboxRoute,
    migrations: list[Any],
) -> str:
    """Hash every committed route handoff for this sandbox incarnation."""

    committed = sorted(
        migration.migration_id
        for migration in migrations
        if migration.sandbox_id == route.sandbox_id
        and migration.generation == route.generation
        and migration.create_operation_id == route.create_operation_id
        and migration.phase in {"routed", "activated", "complete"}
    )
    return hashlib.sha256(
        "\0".join(
            (
                route.sandbox_id,
                str(route.generation),
                route.create_operation_id,
                *committed,
            )
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class NodePlacementState:
    """Per-node route accounting reused throughout one placement decision."""

    available_resources: ResourceQuantity
    inflight_image_identities: frozenset[str]
    projected_image_identities: frozenset[str]
    active_creates: int


@dataclass(frozen=True)
class PlacementReservation:
    reservation_id: str
    node_id: str
    job_id: str
    node_url: str
    resources: ResourceQuantity
    image: str
    state: str = "creating"


PlacementRecord = SandboxRoute | PlacementReservation


@dataclass(frozen=True)
class PlacementRouteIndex:
    """Route lookup tables built once for a gateway placement decision."""

    by_node_id: dict[str, tuple[PlacementRecord, ...]]
    by_job_id: dict[str, tuple[PlacementRecord, ...]]
    by_node_url: dict[str, tuple[PlacementRecord, ...]]

    def routes_for(self, heartbeat: NodeHeartbeat) -> list[PlacementRecord]:
        matches: list[PlacementRecord] = []
        seen: set[int] = set()
        keys = (
            self.by_node_id.get(heartbeat.node_id, ()),
            self.by_job_id.get(heartbeat.job_id, ()),
            self.by_node_url.get((heartbeat.node_url or "").rstrip("/"), ()),
        )
        for routes in keys:
            for route in routes:
                identity = id(route)
                if identity in seen:
                    continue
                seen.add(identity)
                matches.append(route)
        return matches


class RegistryLayerMetadataCache:
    """Bounded immutable-manifest cache used by placement scoring."""

    def __init__(
        self,
        registry_url: str,
        *,
        registry_worker_url: str | None = None,
        max_entries: int = 4096,
    ) -> None:
        self.registry_url = registry_url.rstrip("/")
        self.registry_worker_url = (registry_worker_url or "").rstrip("/")
        self.max_entries = max(1, int(max_entries))
        self._lock = RLock()
        self._records: OrderedDict[str, RegistryManifestLayers] = OrderedDict()
        self._loading: dict[str, Event] = {}

    def get(
        self,
        image_ref: str,
        *,
        load: bool = False,
    ) -> RegistryManifestLayers | None:
        coordinates = self._coordinates(image_ref)
        if coordinates is None:
            return None
        key, repository, digest = coordinates
        waiter: Event | None = None
        with self._lock:
            if key in self._records:
                record = self._records.pop(key)
                self._records[key] = record
                return record
            if key in self._loading:
                if load:
                    waiter = self._loading[key]
                else:
                    return None
            elif not load:
                return None
            else:
                self._loading[key] = Event()
        if waiter is not None:
            waiter.wait(REGISTRY_LAYER_METADATA_TIMEOUT_SECONDS)
            with self._lock:
                return self._records.get(key)
        return self._load_one(key, repository, digest)

    def hydrate_async(self, image_refs: tuple[str, ...]) -> None:
        pending: list[tuple[str, str, str]] = []
        with self._lock:
            for image_ref in image_refs:
                coordinates = self._coordinates(image_ref)
                if coordinates is None:
                    continue
                key, repository, digest = coordinates
                if key in self._records or key in self._loading:
                    continue
                self._loading[key] = Event()
                pending.append((key, repository, digest))
        if not pending:
            return
        Thread(
            target=self._hydrate,
            args=(tuple(pending),),
            daemon=True,
            name="registry-layer-metadata",
        ).start()

    def _hydrate(self, pending: tuple[tuple[str, str, str], ...]) -> None:
        for key, repository, digest in pending:
            self._load_one(key, repository, digest)

    def _load_one(
        self,
        key: str,
        repository: str,
        digest: str,
    ) -> RegistryManifestLayers | None:
        record: RegistryManifestLayers | None = None
        try:
            record = RegistryClient(
                self.registry_url,
                timeout_seconds=REGISTRY_LAYER_METADATA_TIMEOUT_SECONDS,
            ).manifest_layers(repository, digest)
        except (OSError, RegistryRequestError, ValueError):
            record = None
        finally:
            waiter: Event | None = None
            with self._lock:
                waiter = self._loading.pop(key, None)
                if record is not None:
                    self._records[key] = record
                    while len(self._records) > self.max_entries:
                        self._records.popitem(last=False)
            if waiter is not None:
                waiter.set()
        return record

    def _coordinates(self, image_ref: str) -> tuple[str, str, str] | None:
        coordinates = _managed_registry_image_coordinates(
            image_ref,
            self.registry_url,
            self.registry_worker_url,
        )
        digest = manifest_digest_from_image_ref(image_ref)
        if coordinates is None or not digest:
            return None
        repository, _tag = coordinates
        key = canonical_image_digest_ref(image_ref)
        if not key:
            return None
        return key, repository, digest


@dataclass(frozen=True)
class RegistryManifestResolution:
    digest: str
    expires_at: float


class RegistryManifestResolutionCache:
    """Bound repeated verification/protection work for managed manifests."""

    def __init__(self, *, max_entries: int = 4096) -> None:
        self.max_entries = max(1, int(max_entries))
        self._lock = RLock()
        self._records: OrderedDict[tuple[str, str], RegistryManifestResolution] = (
            OrderedDict()
        )

    def get(self, repository: str, reference: str) -> str:
        key = (repository, reference)
        now = time.monotonic()
        with self._lock:
            record = self._records.get(key)
            if record is None:
                return ""
            if record.expires_at <= now:
                self._records.pop(key, None)
                return ""
            self._records.move_to_end(key)
            return record.digest

    def put(self, repository: str, reference: str, digest: str) -> None:
        normalized = normalize_manifest_digest(digest)
        if not normalized:
            return
        immutable = bool(normalize_manifest_digest(reference))
        ttl_seconds = (
            REGISTRY_IMMUTABLE_MANIFEST_CACHE_TTL_SECONDS
            if immutable
            else REGISTRY_MUTABLE_MANIFEST_CACHE_TTL_SECONDS
        )
        key = (repository, reference)
        with self._lock:
            self._records[key] = RegistryManifestResolution(
                digest=normalized,
                expires_at=time.monotonic() + ttl_seconds,
            )
            self._records.move_to_end(key)
            while len(self._records) > self.max_entries:
                self._records.popitem(last=False)


class GatewaySchedulingBusyError(RuntimeError):
    """Placement serialization is occupied and the caller should retry."""


class SandboxShapeUnschedulableError(ValueError):
    def __init__(
        self,
        requested: ResourceQuantity,
        maximum: ResourceQuantity,
    ) -> None:
        super().__init__("sandbox resources exceed the schedulable node shape")
        self.requested = requested
        self.maximum = maximum


class RegistryImageReferenceUnavailable(RuntimeError):
    pass


class ProxyResponseTooLargeError(RuntimeError):
    pass


class ProxiedResponse:
    def __init__(
        self,
        status: int,
        headers: Any,
        body: bytes,
        *,
        transport_error_kind: str = "",
    ) -> None:
        self.status = status
        self.headers = headers
        self.body = body
        self.transport_error_kind = transport_error_kind

    def json(self) -> dict[str, Any]:
        try:
            decoded = json.loads(self.body.decode("utf-8")) if self.body else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}
        return decoded if isinstance(decoded, dict) else {}


_NODE_HTTP_POOL = urllib3.PoolManager(
    num_pools=NODE_HTTP_POOL_ORIGINS,
    maxsize=NODE_HTTP_POOL_CONNECTIONS_PER_ORIGIN,
    block=True,
    retries=False,
)


def _open_node_request(
    req: request.Request,
    *,
    timeout: float,
    authenticated: bool = False,
) -> Any:
    # Authenticated node calls must never carry the deployment credential to a
    # redirect target selected by a compromised node endpoint.
    if authenticated:
        try:
            return _NODE_HTTP_POOL.request(
                req.get_method(),
                req.full_url,
                body=req.data,
                headers=dict(req.header_items()),
                redirect=False,
                retries=False,
                preload_content=False,
                pool_timeout=timeout,
                timeout=urllib3.Timeout(connect=timeout, read=timeout),
            )
        except Urllib3HTTPError as exc:
            raise error.URLError(exc) from exc
    return request.urlopen(req, timeout=timeout)


class ControlPlaneHandler(BuildContextHttpHandler):
    store: ControlStateStore
    routing_store: RoutingStore
    gateway_bearer_token: str
    sandbox_api_token: str
    heartbeat_bearer_token: str
    node_control_bearer_token: str
    deployment_id: str
    heartbeat_ttl_seconds: int
    image_manager: ImageManager
    build_context_store: BuildContextBlobStore
    metrics_store: MetricsStore
    registry_url: str | None
    registry_worker_url: str | None = None
    registry_status_cache: dict[str, Any] | None
    registry_status_cache_at: float
    registry_status_lock: RLock
    registry_manifest_cache: RegistryManifestResolutionCache | None = None
    image_inventory_cache = ImageInventoryCache(
        ttl_seconds=IMAGE_INVENTORY_CACHE_TTL_SECONDS
    )
    metrics_response_cache: bytes | None
    metrics_response_cache_at: float
    metrics_response_lock: RLock
    registry_layer_cache: RegistryLayerMetadataCache | None
    registry_usage_store: RegistryUsageStore | None
    sandbox_create_limiter: BoundedSemaphore | None
    sandbox_create_busy_sampler: GatewayBusySampler
    max_concurrent_sandbox_creates: int
    create_target_concurrency_per_node: int
    max_sandbox_resources: ResourceQuantity
    server_version = "ucloud-sandboxes-control-plane/0.1"

    @traced_http_request
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if self.path == "/healthz":
            health = service_health("control-plane")
            registry_usage_error = self._registry_usage_health_error()
            if registry_usage_error:
                health["ok"] = False
                health["registry_usage"] = {
                    "ok": False,
                    "error": registry_usage_error,
                }
                self._write_json(health, status=HTTPStatus.SERVICE_UNAVAILABLE)
            else:
                self._write_json(health)
            return
        asset = dashboard_asset(parsed.path)
        if asset is not None:
            self._write_bytes(
                asset.body,
                asset.content_type,
                headers={
                    "Cache-Control": "no-store",
                    "Content-Security-Policy": (
                        "default-src 'self'; "
                        "connect-src 'self'; "
                        "script-src 'self'; "
                        "style-src 'self'; "
                        "object-src 'none'; "
                        "base-uri 'none'; "
                        "frame-ancestors 'none'"
                    ),
                },
            )
            return
        if not self._check_authorized():
            return
        context_digest = build_context_digest_from_path(parsed.path)
        if context_digest is not None:
            try:
                size = self.build_context_store.size(context_digest)
            except (FileNotFoundError, ValueError):
                self._write_json(
                    {"error": "build context not found"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return
            self._write_json(
                {"digest": context_digest, "size": size, "deduplicated": True}
            )
            return
        if parsed.path == "/v1/nodes":
            nodes = [
                heartbeat_to_dict(heartbeat)
                for heartbeat in self.store.load_heartbeats().values()
            ]
            self._write_json({"nodes": nodes})
            return
        if parsed.path == "/v1/demand":
            try:
                demand_payload = self._demand_payload()
            except sqlite3.DatabaseError as exc:
                self._write_routing_store_unavailable(exc)
                return
            self._write_json(demand_payload)
            return
        if parsed.path == "/v1/metrics":
            try:
                body = self._metrics_response_bytes(
                    full=_truthy_query_param(parsed, "full"),
                    refresh_registry=_truthy_query_param(parsed, "refresh_registry"),
                )
            except sqlite3.DatabaseError as exc:
                self._write_routing_store_unavailable(exc)
                return
            self._write_bytes(
                body,
                "application/json",
                headers={"Cache-Control": "no-store"},
            )
            return
        if parsed.path == "/v1/registry":
            self._write_json({"registry": self._registry_status()})
            return
        if self._route_to_nodes(parsed.path):
            return
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    @traced_http_request
    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/v1/nodes/heartbeat":
            if not self._check_heartbeat_authorized():
                return
        else:
            if not self._check_authorized():
                return
            if self._route_to_nodes(parsed.path):
                return
            self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return

        try:
            raw = self._read_json_body()
        except ValueError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        if not isinstance(raw, dict):
            self._write_json(
                {"error": "heartbeat payload must be a JSON object"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        try:
            heartbeat = heartbeat_from_dict(raw)
        except (TypeError, ValueError, OverflowError):
            heartbeat = None
        if heartbeat is None:
            self._write_json(
                {"error": "invalid heartbeat payload"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        if heartbeat.deployment_id != self.deployment_id:
            self._write_json(
                {
                    "error": "heartbeat deployment_id does not match this gateway",
                    "expected_deployment_id": self.deployment_id,
                },
                status=HTTPStatus.FORBIDDEN,
            )
            return

        identity_error = self._heartbeat_identity_error(heartbeat)
        if identity_error is not None:
            self._write_json(
                {"error": identity_error},
                status=HTTPStatus.FORBIDDEN,
            )
            return

        received_at = utc_now()
        reported_at = heartbeat.reported_at or heartbeat.updated_at
        # The sender controls neither freshness nor the idle-grace clock. Keep
        # its timestamp as reported_at for diagnostics while recording the
        # gateway-controlled receipt time used for freshness.
        heartbeat = replace(
            heartbeat,
            node_url=_canonical_node_url(heartbeat.node_url),
            updated_at=received_at,
            reported_at=reported_at,
            received_at=received_at,
            idle_since=None,
        )

        try:
            receipt = self.store.receive_heartbeat(heartbeat)
        except HeartbeatIdentityError as exc:
            self._write_json(
                {"error": str(exc)},
                status=HTTPStatus.FORBIDDEN,
            )
            return
        stored_heartbeat = receipt.stored
        if receipt.accepted:
            record_node_heartbeat(
                self.metrics_store,
                stored_heartbeat,
                first=receipt.previous is None,
            )
            if self.registry_layer_cache is not None:
                self.registry_layer_cache.hydrate_async(stored_heartbeat.cached_images)
            if stored_heartbeat.inventory_complete and stored_heartbeat.node_url:
                reconciled_inventory: list[SandboxInventoryEntry] = []
                prepared_snapshot_routes: list[SandboxRoute] = []
                for item in stored_heartbeat.inventory:
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
                        self._ensure_registry_snapshot_reference(
                            candidate,
                            repository=snapshot.publication.repository,
                            tag=snapshot.publication.tag,
                            digest=snapshot.publication.manifest_digest,
                        )
                        prepared_snapshot_routes.append(candidate)
                        reconciled_inventory.append(item)
                    except (RegistryImageReferenceUnavailable, ValueError) as exc:
                        self.metrics_store.append(
                            "sandbox_snapshot_inventory_error",
                            {
                                "sandbox_id": item.sandbox_id,
                                "generation": item.generation,
                                "node_id": stored_heartbeat.node_id,
                                "error": str(exc),
                            },
                        )
                        reconciled_inventory.append(
                            replace(
                                item,
                                storage_schema="",
                                snapshot_manifest_digest="",
                                snapshot_repository="",
                                snapshot_tag="",
                                storage_snapshot={},
                            )
                        )
                removed_routes, stale_snapshot_routes = (
                    self._reconcile_heartbeat_inventory(
                        stored_heartbeat,
                        reconciled_inventory,
                        prepared_snapshot_routes,
                    )
                )
                for route in stale_snapshot_routes:
                    self._release_registry_snapshot_reference(route)
                for route in removed_routes:
                    self._release_registry_route_reference(route)
            self._schedule_image_warmups()
        self._write_json({"ok": True, "node": heartbeat_to_dict(stored_heartbeat)})

    def _reconcile_heartbeat_inventory(
        self,
        heartbeat: NodeHeartbeat,
        inventory: list[SandboxInventoryEntry],
        prepared_snapshot_routes: list[SandboxRoute],
    ) -> tuple[list[SandboxRoute], list[SandboxRoute]]:
        """Reconcile inventory without leaking pre-acquired snapshot owners."""

        try:
            return self.routing_store.reconcile_sandboxes_for_node(
                heartbeat.node_url or "",
                inventory,
                node_id=heartbeat.node_id,
                job_id=heartbeat.job_id,
                reported_sandbox_ids=(item.sandbox_id for item in inventory),
                observed_at=heartbeat.freshness_at.isoformat(),
                node_epoch=heartbeat.node_epoch,
                activity_epoch=heartbeat.activity_epoch,
                inventory_complete=True,
            )
        finally:
            # A failed SQLite commit is ambiguous. A successful read-back tells
            # us whether each candidate became durable; if read-back itself
            # fails, retaining the Registry owner is the data-safe outcome.
            for prepared_route in prepared_snapshot_routes:
                try:
                    current = self.routing_store.get_sandbox_readonly(
                        prepared_route.sandbox_id
                    )
                except BaseException:
                    continue
                self._release_registry_snapshot_reference(
                    prepared_route,
                    keep_route=current,
                )

    def _heartbeat_identity_error(self, heartbeat: NodeHeartbeat) -> str | None:
        node_url = _canonical_node_url(heartbeat.node_url)
        if node_url is None:
            return "heartbeat node_url must be an absolute HTTP(S) origin"
        if not heartbeat.node_id or not heartbeat.job_id or not heartbeat.node_epoch:
            return "heartbeat node_id, job_id, and node_epoch are required"

        for route in self.routing_store.sandbox_routes_matching_node_identity(
            node_id=heartbeat.node_id,
            job_id=heartbeat.job_id,
            node_url=node_url,
        ):
            same_job = bool(route.job_id) and route.job_id == heartbeat.job_id
            same_node = bool(route.node_id) and route.node_id == heartbeat.node_id
            same_node_url = _canonical_node_url(route.node_url) == node_url
            if not same_job and not same_node and not same_node_url:
                continue
            if not same_job or not same_node or not same_node_url:
                return "heartbeat identity conflicts with an assigned route"
        return None

    @traced_http_request
    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        if not self._check_authorized():
            return
        context_digest = build_context_digest_from_path(parsed.path)
        if context_digest is not None:
            self._store_build_context(context_digest)
            return
        if self._route_to_nodes(parsed.path):
            return
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    @traced_http_request
    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        if not self._check_authorized():
            return
        if self._route_to_nodes(parsed.path):
            return
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def _route_to_nodes(self, path: str) -> bool:
        try:
            return self._route_to_nodes_unchecked(path)
        except sqlite3.DatabaseError as exc:
            self._write_routing_store_unavailable(exc)
            return True
        except RegistryImageReferenceUnavailable as exc:
            self._write_registry_lease_unavailable(exc)
            return True

    def _route_to_nodes_unchecked(self, path: str) -> bool:
        if path == "/v1/sandboxes" and self.command == "GET":
            if _truthy_query_param(urlparse(self.path), "refresh"):
                self._list_sandboxes_across_nodes()
            else:
                self._list_sandboxes_from_cache()
            return True
        if path == "/v1/sandboxes" and self.command == "POST":
            self._create_sandbox_on_node()
            return True
        if path == "/v1/capacity/prepare" and self.command == "GET":
            self._list_prepared_capacity()
            return True
        if path == "/v1/capacity/prepare" and self.command == "POST":
            self._prepare_capacity()
            return True
        prepare_id = _prepare_id_from_path(path)
        if prepare_id is not None and self.command == "DELETE":
            self._delete_prepared_capacity(prepare_id)
            return True
        if path == "/v1/builders/prepare" and self.command == "GET":
            self._list_prepared_builders()
            return True
        if path == "/v1/builders/prepare" and self.command == "POST":
            self._prepare_builder()
            return True
        builder_prepare_id = _builder_prepare_id_from_path(path)
        if builder_prepare_id is not None and self.command == "DELETE":
            self._delete_prepared_builder(builder_prepare_id)
            return True
        if path == "/v1/images" and self.command == "GET":
            self._list_images_across_nodes()
            return True
        if path == "/v1/images/builds" and self.command == "GET":
            self._list_image_builds_across_nodes()
            return True
        build_key = _image_build_key_from_path(path)
        if build_key is not None and self.command == "GET":
            self._get_image_build(build_key)
            return True
        if path == "/v1/images/build" and self.command == "POST":
            self._route_image_build()
            return True
        if path == "/v1/images/pull" and self.command == "POST":
            self._route_image_pull()
            return True
        migration_sandbox_id = _sandbox_migration_id_from_path(path)
        if migration_sandbox_id is not None and self.command == "POST":
            self._migrate_sandbox_on_node(migration_sandbox_id)
            return True
        if migration_sandbox_id is not None and self.command == "DELETE":
            self._cancel_sandbox_migration(migration_sandbox_id)
            return True
        detach_sandbox_id = _sandbox_detach_id_from_path(path)
        if detach_sandbox_id is not None and self.command == "POST":
            self._detach_sandbox_from_worker(detach_sandbox_id)
            return True
        sandbox_id = _sandbox_id_from_path(path)
        if sandbox_id is not None:
            self._route_sandbox_request(sandbox_id, path)
            return True
        session_id = _exec_session_id_from_path(path)
        if session_id is not None:
            self._route_exec_request(session_id)
            return True
        return False

    def _detach_sandbox_from_worker(self, sandbox_id: str) -> None:
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict) or raw:
                raise ValueError("sandbox detach payload must be an empty object")
            route = self.routing_store.get_sandbox_readonly(sandbox_id)
            if route is None:
                self._write_json(
                    {"error": "sandbox route not found"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return
            if route.worker_state == "detached":
                self._write_json({"ok": True, "sandbox": route.to_dict()})
                return
            if route.worker_state == "attached" and not is_portable_parked_route(route):
                if not is_worker_detachable_parked_route(route):
                    raise SandboxRouteConflictError(
                        "only a parked sandbox can detach from a worker"
                    )
                route, publication_error = self._publish_route_for_detach(route)
                if route is None:
                    self._write_json(
                        {
                            "error": publication_error
                            or "parked snapshot publication is incomplete",
                            "retryable": True,
                        },
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        headers={
                            "Retry-After": str(
                                SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS
                            ),
                            "X-UCloud-Sandbox-Retryable": "true",
                        },
                    )
                    return
            if not is_portable_parked_route(route):
                raise SandboxRouteConflictError(
                    "only a fully published parked sandbox can detach from a worker"
                )
            _portable_snapshot_for_route(route)
            self._ensure_registry_route_reference(route, touch=True)
            fenced = self.routing_store.begin_sandbox_detach(route)
            if fenced is None:
                raise SandboxRouteConflictError(
                    "sandbox route changed before worker detach began"
                )
            detached, error_message = self._finish_sandbox_detach(fenced)
            if detached is None:
                self._write_json(
                    {
                        "error": error_message or "worker detach is incomplete",
                        "retryable": True,
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers={
                        "Retry-After": str(
                            SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS
                        ),
                        "X-UCloud-Sandbox-Retryable": "true",
                    },
                )
                return
            self._write_json({"ok": True, "sandbox": detached.to_dict()})
        except RegistryImageReferenceUnavailable as exc:
            self._write_registry_lease_unavailable(exc)
        except (SandboxRouteConflictError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)

    def _publish_route_for_detach(
        self,
        route: SandboxRoute,
    ) -> tuple[SandboxRoute | None, str]:
        response = self._proxy_request(
            route.node_url,
            (f"/v1/sandboxes/{quote(route.sandbox_id, safe='')}/publish-parked"),
            method="POST",
            body=json.dumps(
                {
                    "generation": route.generation,
                    "create_operation_id": route.create_operation_id,
                    "spec_hash": route.spec_hash,
                },
                separators=(",", ":"),
            ).encode("utf-8"),
            extra_headers={"Content-Type": "application/json"},
            timeout_seconds=3600,
        )
        if response.status >= 400:
            error_message = str(response.json().get("error") or "").strip()
            return (
                None,
                error_message
                or f"worker parked publication returned HTTP {response.status}",
            )
        try:
            payload = response.json()
            candidate = _route_with_snapshot_payload(route, payload)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            return None, f"worker returned invalid parked publication: {exc}"
        self._ensure_registry_snapshot_reference(
            route,
            repository=candidate.snapshot_repository,
            tag=candidate.snapshot_tag,
            digest=candidate.snapshot_manifest_digest,
        )
        try:
            stored = self.routing_store.set_sandbox_state_if_current(
                route,
                expected_states={"parked"},
                state="parked",
                storage_schema=candidate.storage_schema,
                snapshot_manifest_digest=candidate.snapshot_manifest_digest,
                snapshot_repository=candidate.snapshot_repository,
                snapshot_tag=candidate.snapshot_tag,
                storage_snapshot=candidate.storage_snapshot,
            )
        except BaseException:
            # Commit acknowledgement can fail after the candidate became
            # durable. Reconcile against a successful read-back and otherwise
            # retain the candidate reference conservatively.
            try:
                current = self.routing_store.get_sandbox_readonly(route.sandbox_id)
            except BaseException:
                raise
            self._release_registry_snapshot_reference(
                candidate,
                keep_route=current,
            )
            raise
        if stored is None:
            self._release_registry_snapshot_reference(
                candidate,
                keep_route=self.routing_store.get_sandbox_readonly(route.sandbox_id),
            )
            return None, "sandbox route changed while its parked snapshot published"
        self._release_registry_snapshot_reference(route, keep_route=stored)
        return stored, ""

    def _finish_sandbox_detach(
        self,
        route: SandboxRoute,
    ) -> tuple[SandboxRoute | None, str]:
        current = self.routing_store.get_sandbox_readonly(route.sandbox_id)
        if current is None:
            return None, "sandbox route disappeared during worker detach"
        if current.worker_state == "detached":
            return current, ""
        if current.worker_state != "detaching" or not is_portable_parked_route(current):
            return None, "sandbox route changed during worker detach"
        heartbeat = self._heartbeat_for_route(
            job_id=current.job_id,
        )
        if _heartbeat_proves_route_absent(
            heartbeat,
            sandbox_id=current.sandbox_id,
            route_created_at=current.created_at,
            route_updated_at=current.updated_at,
            heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
        ):
            completed = self.routing_store.complete_sandbox_detach(current)
            return completed, "" if completed is not None else "detach fence changed"
        response = self._proxy_request(
            current.node_url,
            (f"/v1/sandboxes/{quote(current.sandbox_id, safe='')}/evict-published"),
            method="POST",
            body=json.dumps(
                {
                    "generation": current.generation,
                    "snapshot_manifest_digest": current.snapshot_manifest_digest,
                },
                separators=(",", ":"),
            ).encode("utf-8"),
            extra_headers={"Content-Type": "application/json"},
        )
        if response.status >= 400:
            error_message = str(response.json().get("error") or "").strip()
            return (
                None,
                error_message
                or f"worker published eviction returned HTTP {response.status}",
            )
        completed = self.routing_store.complete_sandbox_detach(current)
        if completed is None:
            return None, "sandbox route changed before worker detach committed"
        return completed, ""

    def _migrate_sandbox_on_node(self, sandbox_id: str) -> None:
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("migration payload must be a JSON object")
            migration_id = str(
                raw.get("migration_id") or f"migration-{uuid4().hex}"
            ).strip()
            requested_destination = str(raw.get("destination_node_id") or "").strip()
            migration = self.routing_store.get_sandbox_migration(migration_id)
            if migration is not None and migration.sandbox_id != sandbox_id:
                raise SandboxRouteConflictError(
                    "migration id belongs to another sandbox"
                )
            if migration is None:
                with (
                    _GATEWAY_SCHEDULING_LOCK,
                    _gateway_placement_lock(self.routing_store.path),
                ):
                    migration = self.routing_store.get_sandbox_migration(migration_id)
                    if migration is not None and migration.sandbox_id != sandbox_id:
                        raise SandboxRouteConflictError(
                            "migration id belongs to another sandbox"
                        )
                    if migration is None:
                        source = self.routing_store.get_sandbox_readonly(sandbox_id)
                        if source is None:
                            self._write_json(
                                {"error": "sandbox route not found"},
                                status=HTTPStatus.NOT_FOUND,
                            )
                            return
                        destination = self._select_migration_destination(
                            source,
                            requested_node_id=requested_destination,
                        )
                        if destination is None:
                            _pending, demand = (
                                self.routing_store.upsert_pending_with_demand(
                                    _migration_pending_demand_id(sandbox_id),
                                    ResourceQuantity(disk_mb=source.resources.disk_mb),
                                    failure_reason=(
                                        "migration_destination_unavailable"
                                    ),
                                )
                            )
                            self._write_json(
                                {
                                    "error": (
                                        "no ready destination has disk capacity "
                                        "for parked sandbox migration"
                                    ),
                                    "retryable": True,
                                    "pending_resources": (
                                        demand.pending_resources.to_dict()
                                    ),
                                },
                                status=HTTPStatus.SERVICE_UNAVAILABLE,
                            )
                            return
                        # Persist the destination reservation before any image
                        # pull or transfer. Placement can then continue safely
                        # while the long-running migration work executes.
                        migration = self.routing_store.begin_sandbox_migration(
                            source,
                            migration_id=migration_id,
                            destination_node_id=destination.node_id,
                            destination_job_id=destination.job_id,
                            destination_node_url=destination.node_url or "",
                        )
            assert migration is not None
            self.routing_store.clear_pending(_migration_pending_demand_id(sandbox_id))
            migration_timings_ms: dict[str, float] = {}
            migration = self._prepare_and_advance_sandbox_migration(
                migration,
                timings_ms=migration_timings_ms,
            )
            if migration is None:
                return
        except (SandboxRouteConflictError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except sqlite3.DatabaseError as exc:
            self._write_routing_store_unavailable(exc)
            return
        if migration.phase != "complete":
            self._write_json(
                {
                    "error": migration.error or "sandbox migration is incomplete",
                    "migration": migration.to_dict(),
                    "retryable": True,
                    "timings_ms": migration_timings_ms,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        route = self.routing_store.get_sandbox_readonly(sandbox_id)
        self._write_json(
            {
                "migration": migration.to_dict(),
                "sandbox": route.to_dict() if route is not None else None,
                "timings_ms": migration_timings_ms,
            }
        )

    def _select_migration_destination(
        self,
        source: SandboxRoute,
        *,
        requested_node_id: str,
        require_active_resources: bool = False,
    ) -> NodeHeartbeat | None:
        routes = self.routing_store.sandbox_routes_readonly()
        active_migrations = self.routing_store.sandbox_migrations(active_only=True)
        ready_heartbeats = self._ready_sandbox_heartbeats()
        source_heartbeat = next(
            (
                heartbeat
                for heartbeat in ready_heartbeats
                if heartbeat.node_id == source.node_id
                or (
                    source.node_url
                    and heartbeat.node_url
                    and heartbeat.node_url.rstrip("/") == source.node_url.rstrip("/")
                )
            ),
            None,
        )
        source_storage_native = bool(
            source_heartbeat is not None
            and STORAGE_NATIVE_CAPABILITY in source_heartbeat.capabilities
            and STORAGE_NATIVE_MIGRATION_CAPABILITY in source_heartbeat.capabilities
        )
        source_is_attached = source.worker_state == "attached"
        if source_is_attached:
            if not source_storage_native:
                return None
        elif source.worker_state != "detached" or not is_portable_parked_route(source):
            return None
        required_destination_capabilities = _sandbox_required_capabilities(source.spec)
        reservations: dict[str, int] = {}
        routes_by_id = {route.sandbox_id: route for route in routes}
        for migration in active_migrations:
            if migration.phase in {"routed", "activated"}:
                continue
            route = routes_by_id.get(migration.sandbox_id)
            if route is None:
                continue
            reservations[migration.destination_node_id] = (
                reservations.get(migration.destination_node_id, 0)
                + route.resources.disk_mb
            )
        candidates: list[NodeHeartbeat] = []
        for heartbeat in ready_heartbeats:
            if (
                (source_is_attached and heartbeat.node_id == source.node_id)
                or STORAGE_NATIVE_CAPABILITY not in heartbeat.capabilities
                or STORAGE_NATIVE_MIGRATION_CAPABILITY not in heartbeat.capabilities
                or any(
                    capability not in heartbeat.capabilities
                    for capability in required_destination_capabilities
                )
                or (requested_node_id and heartbeat.node_id != requested_node_id)
            ):
                continue
            available = _node_available_resources(heartbeat, routes)
            available = replace(
                available,
                disk_mb=max(
                    0,
                    available.disk_mb - reservations.get(heartbeat.node_id, 0),
                ),
            )
            requested = (
                source.resources
                if require_active_resources
                else ResourceQuantity(disk_mb=source.resources.disk_mb)
            )
            if requested.disk_mb <= 0 or not _node_can_fit_available(
                heartbeat,
                requested,
                available,
            ):
                continue
            candidates.append(heartbeat)
        if not candidates:
            return None
        image = str(source.spec.get("image") or "")
        return min(
            candidates,
            key=lambda heartbeat: (
                0 if _heartbeat_has_image(heartbeat, image) else 1,
                reservations.get(heartbeat.node_id, 0),
                -_node_available_resources(heartbeat, routes).disk_mb,
                heartbeat.node_id,
            ),
        )

    def _prepare_migration_destination_image(
        self,
        source: SandboxRoute,
        destination: NodeHeartbeat,
    ) -> bool:
        image = str(source.spec.get("image") or "").strip()
        image_response = self._ensure_image_on_node(destination, image)
        if image_response is None or image_response.status < 400:
            return True
        image_error = image_response.json()
        self._write_json(
            {
                "error": ("migration destination could not prepare the sandbox image"),
                "retryable": True,
                "image": image,
                "node_id": destination.node_id,
                "node_error": image_error.get("error") or image_error,
            },
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            headers={
                "Retry-After": str(SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS),
                "X-UCloud-Sandbox-Retryable": "true",
            },
        )
        return False

    def _prepare_and_advance_sandbox_migration(
        self,
        migration,
        *,
        timings_ms: dict[str, float] | None = None,
        wake_on_complete: bool = False,
    ):
        """Run network-heavy migration work outside global placement locks."""

        with _migration_operation_lock(migration.migration_id):
            current = self.routing_store.get_sandbox_migration(migration.migration_id)
            if current is None:
                self._write_json(
                    {"error": "sandbox migration disappeared", "retryable": True},
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers={
                        "Retry-After": str(
                            SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS
                        ),
                        "X-UCloud-Sandbox-Retryable": "true",
                    },
                )
                return None
            if current.phase == "complete":
                return (
                    self.routing_store.complete_sandbox_migration(
                        current.migration_id,
                        wake_destination=wake_on_complete,
                    )
                    or current
                )
            if current.phase == "planned":
                source = self.routing_store.get_sandbox_readonly(current.sandbox_id)
                destination = next(
                    (
                        heartbeat
                        for heartbeat in self._ready_sandbox_heartbeats()
                        if heartbeat.node_id == current.destination_node_id
                        and heartbeat.job_id == current.destination_job_id
                        and (heartbeat.node_url or "").rstrip("/")
                        == current.destination_node_url.rstrip("/")
                    ),
                    None,
                )
                if source is None or destination is None:
                    self._write_json(
                        {
                            "error": "migration source or destination is unavailable",
                            "retryable": True,
                            "migration": current.to_dict(),
                        },
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        headers={
                            "Retry-After": str(
                                SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS
                            ),
                            "X-UCloud-Sandbox-Retryable": "true",
                        },
                    )
                    return None
                if not self._prepare_migration_destination_image(
                    source,
                    destination,
                ):
                    return None
            return self._advance_sandbox_migration(
                current,
                timings_ms=timings_ms,
                wake_on_complete=wake_on_complete,
            )

    def _advance_sandbox_migration(
        self,
        migration,
        *,
        timings_ms: dict[str, float] | None = None,
        wake_on_complete: bool = False,
    ):
        advance_started = time.monotonic()
        measured = timings_ms if timings_ms is not None else {}
        if migration.phase == "planned":
            source_route = self.routing_store.get_sandbox_readonly(migration.sandbox_id)
            route_snapshot: StorageNativeMigration | None = None
            if source_route is not None and is_portable_parked_route(source_route):
                try:
                    route_snapshot = _portable_snapshot_for_route(source_route)
                except ValueError:
                    route_snapshot = None
            phase_started = time.monotonic()
            if source_route is None:
                return self._record_migration_error(
                    migration,
                    error_message="migration source route disappeared",
                )
            if source_route.worker_state == "detached":
                if route_snapshot is None:
                    return self._record_migration_error(
                        migration,
                        error_message="detached route has no valid published snapshot",
                    )
                migration = (
                    self.routing_store.advance_sandbox_migration(
                        migration.migration_id,
                        expected_phases={"planned"},
                        phase="prepared",
                        storage_schema=STORAGE_NATIVE_MIGRATION_SCHEMA,
                        snapshot_sha256=route_snapshot.sha256,
                        storage_snapshot=route_snapshot.to_dict(),
                        source_fenced=False,
                        error="",
                    )
                    or migration
                )
            elif source_route.worker_state == "attached":
                response = self._proxy_request(
                    migration.source_node_url,
                    (
                        f"/v1/sandboxes/{quote(migration.sandbox_id, safe='')}"
                        "/migration/prepare"
                    ),
                    method="POST",
                    body=json.dumps(
                        {
                            "migration_id": migration.migration_id,
                            "format": STORAGE_NATIVE_MIGRATION_SCHEMA,
                        }
                    ).encode("utf-8"),
                    extra_headers={"Content-Type": "application/json"},
                    timeout_seconds=3600,
                )
                if response.status >= 400:
                    return self._record_migration_error(migration, response)
                prepared = response.json().get("migration")
                if not isinstance(prepared, dict):
                    return self._record_migration_error(
                        migration,
                        error_message="source returned invalid migration metadata",
                    )
                storage_schema = str(prepared.get("storage_schema") or "")
                try:
                    if storage_schema != STORAGE_NATIVE_MIGRATION_SCHEMA:
                        raise ValueError("unsupported migration storage schema")
                    storage_snapshot = StorageNativeMigration.from_dict(
                        prepared.get("storage_snapshot")
                    )
                    snapshot_sha256 = str(prepared.get("snapshot_sha256") or "")
                    if storage_snapshot.sha256 != snapshot_sha256:
                        raise ValueError(
                            "source snapshot digest does not match metadata"
                        )
                except ValueError as exc:
                    return self._record_migration_error(
                        migration,
                        error_message=f"source returned invalid snapshot: {exc}",
                    )
                migration = (
                    self.routing_store.advance_sandbox_migration(
                        migration.migration_id,
                        expected_phases={"planned"},
                        phase="prepared",
                        storage_schema=storage_schema,
                        snapshot_sha256=snapshot_sha256,
                        storage_snapshot=storage_snapshot.to_dict(),
                        source_fenced=True,
                        error="",
                    )
                    or migration
                )
            else:
                return self._record_migration_error(
                    migration,
                    error_message="migration source is still detaching",
                )
            measured["prepare_export"] = _precise_elapsed_ms(phase_started)
        if migration.phase == "prepared":
            phase_started = time.monotonic()
            if migration.storage_schema != STORAGE_NATIVE_MIGRATION_SCHEMA:
                return self._record_migration_error(
                    migration,
                    error_message="migration is missing storage-native metadata",
                )
            import_payload = {
                "migration_id": migration.migration_id,
                "sandbox_id": migration.sandbox_id,
                "snapshot_sha256": migration.snapshot_sha256,
                "storage_schema": migration.storage_schema,
                "storage_snapshot": migration.storage_snapshot,
            }
            response = self._proxy_request(
                migration.destination_node_url,
                "/v1/migrations/import",
                method="POST",
                body=json.dumps(import_payload).encode("utf-8"),
                extra_headers={"Content-Type": "application/json"},
                timeout_seconds=3600,
            )
            measured["transfer_and_stage"] = _precise_elapsed_ms(phase_started)
            if response.status >= 400:
                return self._record_migration_error(migration, response)
            destination_snapshot: dict[str, Any] | None = None
            try:
                response_body = response.json()
                if (
                    str(response_body.get("storage_schema") or "")
                    != STORAGE_NATIVE_MIGRATION_SCHEMA
                ):
                    raise ValueError("destination changed the storage schema")
                parsed_destination = StorageNativeMigration.from_dict(
                    response_body.get("storage_snapshot")
                )
                if (
                    parsed_destination.manifest
                    != StorageNativeMigration.from_dict(
                        migration.storage_snapshot
                    ).manifest
                ):
                    raise ValueError("destination changed portable migration metadata")
                destination_snapshot = parsed_destination.to_dict()
            except ValueError as exc:
                return self._record_migration_error(
                    migration,
                    error_message=f"destination returned invalid snapshot: {exc}",
                )
            migration = (
                self.routing_store.advance_sandbox_migration(
                    migration.migration_id,
                    expected_phases={"prepared"},
                    phase="staged",
                    storage_snapshot=destination_snapshot,
                    error="",
                )
                or migration
            )
        if migration.phase == "staged":
            phase_started = time.monotonic()
            source_route = self.routing_store.get_sandbox_readonly(migration.sandbox_id)
            destination_route: SandboxRoute | None = None
            try:
                destination_snapshot = StorageNativeMigration.from_dict(
                    migration.storage_snapshot
                )
                destination_publication = destination_snapshot.publication
                if source_route is None:
                    raise ValueError("source route disappeared")
                destination_route = replace(
                    source_route,
                    node_id=migration.destination_node_id,
                    job_id=migration.destination_job_id,
                    node_url=migration.destination_node_url,
                    storage_schema=migration.storage_schema,
                    snapshot_manifest_digest=(destination_publication.manifest_digest),
                    snapshot_repository=destination_publication.repository,
                    snapshot_tag=destination_publication.tag,
                    storage_snapshot=destination_snapshot.to_dict(),
                )
                # Both the image and portable snapshot must be protected under
                # the destination owner before routing can point at it.
                self._ensure_registry_route_reference(
                    destination_route,
                    touch=True,
                )
            except (
                RegistryImageReferenceUnavailable,
                ValueError,
            ) as exc:
                if destination_route is not None and source_route is not None:
                    self._release_registry_route_reference(
                        destination_route,
                        keep_route=source_route,
                    )
                return self._record_migration_error(
                    migration,
                    error_message=(
                        f"destination registry references could not be persisted: {exc}"
                    ),
                )
            try:
                routed = self.routing_store.route_sandbox_migration(
                    migration.migration_id
                )
            except BaseException:
                # A SQLite commit error is ambiguous: the destination route
                # may already be durable. Release only after a read-back proves
                # which owner still needs protection; a failed read leaks
                # conservatively instead of risking live image/snapshot data.
                try:
                    current_route = self.routing_store.get_sandbox_readonly(
                        migration.sandbox_id
                    )
                except BaseException:
                    raise
                self._release_registry_route_reference(
                    destination_route,
                    keep_route=current_route,
                )
                raise
            measured["route_commit"] = _precise_elapsed_ms(phase_started)
            if routed is None:
                self._release_registry_route_reference(
                    destination_route,
                    keep_route=source_route,
                )
                return self._record_migration_error(
                    migration,
                    error_message="sandbox route changed before migration commit",
                )
            migration, destination_route = routed
            self._release_registry_route_reference(
                source_route,
                keep_route=destination_route,
            )
        if migration.phase == "routed":
            phase_started = time.monotonic()
            response = self._proxy_request(
                migration.destination_node_url,
                (
                    f"/v1/sandboxes/{quote(migration.sandbox_id, safe='')}"
                    "/migration/activate"
                ),
                method="POST",
                body=json.dumps(
                    {
                        "snapshot_sha256": migration.snapshot_sha256,
                        "migration_id": migration.migration_id,
                    }
                ).encode("utf-8"),
                extra_headers={"Content-Type": "application/json"},
                timeout_seconds=3600,
            )
            measured["activate_destination"] = _precise_elapsed_ms(phase_started)
            if response.status >= 400:
                return self._record_migration_error(migration, response)
            migration = (
                self.routing_store.advance_sandbox_migration(
                    migration.migration_id,
                    expected_phases={"routed"},
                    phase="activated",
                    error="",
                )
                or migration
            )
        if migration.phase == "activated":
            phase_started = time.monotonic()
            if not migration.source_fenced:
                measured["finalize_source"] = 0.0
                migration = (
                    self.routing_store.complete_sandbox_migration(
                        migration.migration_id,
                        wake_destination=wake_on_complete,
                    )
                    or migration
                )
                measured["protocol_total"] = _precise_elapsed_ms(advance_started)
                return migration
            response = self._proxy_request(
                migration.source_node_url,
                (
                    f"/v1/sandboxes/{quote(migration.sandbox_id, safe='')}"
                    "/migration/finalize"
                ),
                method="POST",
                body=json.dumps(
                    {
                        "snapshot_sha256": migration.snapshot_sha256,
                        "migration_id": migration.migration_id,
                    }
                ).encode("utf-8"),
                extra_headers={"Content-Type": "application/json"},
                timeout_seconds=3600,
            )
            measured["finalize_source"] = _precise_elapsed_ms(phase_started)
            if response.status >= 400:
                return self._record_migration_error(migration, response)
            migration = (
                self.routing_store.complete_sandbox_migration(
                    migration.migration_id,
                    wake_destination=wake_on_complete,
                )
                or migration
            )
        measured["protocol_total"] = _precise_elapsed_ms(advance_started)
        return migration

    def _record_migration_error(
        self,
        migration,
        response: ProxiedResponse | None = None,
        *,
        error_message: str = "",
    ):
        detail = error_message
        if response is not None:
            payload = response.json()
            detail = str(payload.get("error") or "").strip()
            if not detail:
                detail = f"node migration request returned HTTP {response.status}"
        return (
            self.routing_store.advance_sandbox_migration(
                migration.migration_id,
                expected_phases={migration.phase},
                phase=migration.phase,
                error=detail,
            )
            or migration
        )

    def _cancel_sandbox_migration(self, sandbox_id: str) -> None:
        query = parse_qs(urlparse(self.path).query, keep_blank_values=True)
        migration_id = (query.get("migration_id") or [""])[0].strip()
        migration = self.routing_store.get_sandbox_migration(migration_id)
        if migration is None or migration.sandbox_id != sandbox_id:
            self._write_json(
                {"error": "sandbox migration not found"},
                status=HTTPStatus.NOT_FOUND,
            )
            return
        if migration.phase in {"routed", "activated"}:
            self._write_json(
                {
                    "error": (
                        "migration routing is already committed; retry the "
                        "migration to finish it"
                    )
                },
                status=HTTPStatus.CONFLICT,
            )
            return
        if migration.phase == "complete":
            self._write_json({"migration": migration.to_dict()})
            return
        migration, error_message = self._abort_sandbox_migration(migration)
        if error_message:
            self._write_json(
                {
                    "error": error_message,
                    "migration": migration.to_dict(),
                    "retryable": True,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self._write_json({"migration": migration.to_dict()})

    def _abort_sandbox_migration(self, migration):
        """Roll back an uncommitted migration using its durable journal."""

        payload = json.dumps(
            {
                "snapshot_sha256": migration.snapshot_sha256,
                "migration_id": migration.migration_id,
            }
        ).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if migration.phase in {"prepared", "staged"}:
            destination = self._proxy_request(
                migration.destination_node_url,
                (
                    f"/v1/sandboxes/{quote(migration.sandbox_id, safe='')}"
                    "/migration/abort-import"
                ),
                method="POST",
                body=payload,
                extra_headers=headers,
                timeout_seconds=3600,
            )
            if destination.status >= 400:
                migration = self._record_migration_error(migration, destination)
                return migration, migration.error
        source = self._proxy_request(
            migration.source_node_url,
            (f"/v1/sandboxes/{quote(migration.sandbox_id, safe='')}/migration/abort"),
            method="POST",
            body=payload,
            extra_headers=headers,
            timeout_seconds=3600,
        )
        if source.status >= 400:
            migration = self._record_migration_error(migration, source)
            return migration, migration.error
        migration = (
            self.routing_store.advance_sandbox_migration(
                migration.migration_id,
                expected_phases={migration.phase},
                phase="complete",
                error="cancelled before route commit",
            )
            or migration
        )
        return migration, ""

    def _resolve_sandbox_migrations_for_delete(self, sandbox_id: str) -> str:
        """Finish or roll back migration journals before replaying deletion."""

        active = [
            migration
            for migration in self.routing_store.sandbox_migrations(active_only=True)
            if migration.sandbox_id == sandbox_id
        ]
        for migration in active:
            if migration.phase in {"routed", "activated"}:
                migration = self._advance_sandbox_migration(migration)
                if migration.phase != "complete":
                    return (
                        migration.error or "committed sandbox migration is incomplete"
                    )
                continue
            migration, error_message = self._abort_sandbox_migration(migration)
            if error_message:
                return error_message
        return ""

    def _registry_usage_health_error(self) -> str:
        store = self.registry_usage_store
        if store is None:
            return ""
        try:
            # RegistryUsageStore is SQLite-backed. Opening it as the JSON file
            # used by the retired implementation made healthy gateways report
            # 503 whenever managed-registry accounting was configured.
            store.snapshot()
        except (OSError, sqlite3.DatabaseError, RegistryUsageStateError, ValueError):
            return "state file is unavailable"
        return ""

    def _write_routing_store_unavailable(self, _exc: sqlite3.DatabaseError) -> None:
        self._write_json(
            {
                "error": "routing state unavailable",
                "retryable": True,
            },
            status=HTTPStatus.SERVICE_UNAVAILABLE,
        )

    def _demand_payload(self) -> dict[str, Any]:
        demand = self.routing_store.pending_demand()
        pending_image_builds = self.routing_store.pending_image_build_count()
        prepared_builders = self.routing_store.prepared_builders()
        prepared_builder_count = sum(item.count for item in prepared_builders)
        return {
            "pending_resources": demand.pending_resources.to_dict(),
            "suppressed_pending_resources": (
                demand.suppressed_pending_resources.to_dict()
            ),
            "pending_count": demand.pending_count,
            "suppressed_pending_count": demand.suppressed_pending_count,
            "prepared_resources": demand.prepared_resources.to_dict(),
            "desired_resources": demand.desired_resources.to_dict(),
            "oldest_pending_seconds": demand.oldest_pending_seconds,
            "pending_image_builds": pending_image_builds,
            "prepared_builder_count": prepared_builder_count,
            "desired_builders": max(
                1 if pending_image_builds > 0 else 0,
                prepared_builder_count,
            ),
            "pending": [
                item.to_dict() for item in self.routing_store.pending_sandboxes()
            ],
            "prepared": [
                item.to_dict() for item in self.routing_store.prepared_capacity()
            ],
            "prepared_builders": [item.to_dict() for item in prepared_builders],
            "image_warmups": [
                item.to_dict() for item in self.routing_store.image_warmups()
            ],
        }

    def _metrics_response_bytes(
        self,
        *,
        full: bool,
        refresh_registry: bool,
    ) -> bytes:
        cacheable = not full and not refresh_registry
        handler_cls = type(self)
        with handler_cls.metrics_response_lock:
            now = time.monotonic()
            if (
                cacheable
                and handler_cls.metrics_response_cache is not None
                and now - handler_cls.metrics_response_cache_at
                <= METRICS_RESPONSE_CACHE_TTL_SECONDS
            ):
                return handler_cls.metrics_response_cache

            exec_session_count = 0
            load_metrics = getattr(self.routing_store, "load_metrics", None)
            if load_metrics is None:
                routing_state = self.routing_store.load()
                exec_session_count = len(routing_state.exec_sessions)
            else:
                routing_state, exec_session_count = load_metrics()
            events = self.metrics_store.load_events(
                max_events=(
                    FULL_METRICS_EVENT_LIMIT if full else DEFAULT_METRICS_EVENT_LIMIT
                )
            )
            # High-rate heartbeats must not crowd the sparse provisioning
            # and autoscaler records out of the dashboard snapshot.
            supplemental = self.metrics_store.load_events(
                max_events=2_000 if full else 500,
                kinds=(
                    "vm_submitted",
                    "node_first_heartbeat",
                    "sandbox_scheduled",
                    "autoscaler_cycle",
                ),
                since_seconds=7 * 24 * 60 * 60,
            )
            keyed = {
                (
                    event.timestamp,
                    event.kind,
                    json.dumps(event.data, sort_keys=True),
                ): event
                for event in [*events, *supplemental]
            }
            events = sorted(
                keyed.values(),
                key=lambda event: event.timestamp,
            )
            snapshot = build_metrics_snapshot(
                self.store.load_heartbeats(),
                routing_state,
                events,
                heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
                exec_session_count=exec_session_count,
                program_requests=self.routing_store.program_requests_readonly(),
            )
            snapshot["telemetry"] = (
                self.telemetry.health()
                if self.telemetry is not None
                else {"enabled": False}
            )
            builds = self._cached_image_build_records()
            active_builds = [
                build
                for build in builds
                if build.get("status") not in {"succeeded", "failed"}
            ]
            failed_builds = [
                build for build in builds if build.get("status") == "failed"
            ]
            active_build_count = max(
                len(active_builds),
                int(
                    snapshot.get("resources", {})
                    .get("fresh", {})
                    .get("active_image_builds")
                    or 0
                ),
            )
            snapshot.setdefault("images", {}).update(
                {
                    "active_builds": active_build_count,
                    "failed_builds": len(failed_builds),
                    "builds": builds,
                }
            )
            snapshot["registry"] = self._registry_status_cached(
                force_refresh=full or refresh_registry
            )
            body = json.dumps(
                snapshot,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            if cacheable:
                cached_registry = dict(snapshot.get("registry") or {})
                cached_registry["cached"] = True
                snapshot["registry"] = cached_registry
                handler_cls.metrics_response_cache = json.dumps(
                    snapshot,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                handler_cls.metrics_response_cache_at = time.monotonic()
            return body

    def _list_prepared_capacity(self) -> None:
        self._write_json(
            {
                "prepared": [
                    item.to_dict() for item in self.routing_store.prepared_capacity()
                ],
                "demand": self._demand_payload(),
            }
        )

    def _prepare_capacity(self) -> None:
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("prepare payload must be a JSON object")
            unsupported = sorted(
                set(raw)
                - {
                    "count",
                    "cpus",
                    "disk_mb",
                    "id",
                    "image",
                    "memory_mb",
                    "parkable",
                    "ttl_seconds",
                }
            )
            if unsupported:
                raise ValueError(
                    "unsupported prepare fields: " + ", ".join(unsupported)
                )
            prepare_id = str(raw.get("id") or f"prep-{uuid4().hex[:16]}").strip()
            if not prepare_id or "/" in prepare_id:
                raise ValueError("prepare id must be non-empty and cannot contain '/'.")
            count = _strict_positive_integer(raw.get("count", 1), "count")
            if count > MAX_PREPARED_CAPACITY_COUNT:
                raise ValueError(f"count cannot exceed {MAX_PREPARED_CAPACITY_COUNT}.")
            ttl_seconds = _strict_positive_integer(
                raw.get("ttl_seconds", 900),
                "ttl_seconds",
            )
            resources = _prepared_resources_from_payload(raw)
            image = str(raw.get("image") or "").strip()
            if count <= 0:
                raise ValueError("count must be positive.")
            if ttl_seconds <= 0:
                raise ValueError("ttl_seconds must be positive.")
            _validate_prepared_resources(resources)
        except (TypeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        if image:
            image, image_error = self._resolve_request_image_reference(image)
            if image_error is not None:
                self._write_image_resolution_error(image_error)
                return

        item = self.routing_store.upsert_prepared_capacity(
            prepare_id,
            resources,
            count=count,
            ttl_seconds=ttl_seconds,
            image=image,
        )
        warmup = (
            self.routing_store.upsert_image_warmup(
                prepare_id,
                image,
                resources,
                count=count,
                ttl_seconds=ttl_seconds,
            )
            if image
            else None
        )
        warmup_summary = self._schedule_image_warmups() if warmup is not None else None
        payload = {
            "prepare": item.to_dict(),
            "demand": self._demand_payload(),
        }
        if warmup is not None:
            payload["image_warmup"] = warmup.to_dict()
        if warmup_summary is not None:
            payload["image_prewarm"] = warmup_summary
        self._write_json(
            payload,
            status=HTTPStatus.CREATED,
        )

    def _delete_prepared_capacity(self, prepare_id: str) -> None:
        deleted = self.routing_store.delete_prepared_capacity(prepare_id)
        self._write_json(
            {
                "ok": True,
                "deleted": deleted.to_dict() if deleted is not None else None,
                "demand": self._demand_payload(),
            }
        )

    def _list_prepared_builders(self) -> None:
        self._write_json(
            {
                "prepared_builders": [
                    item.to_dict() for item in self.routing_store.prepared_builders()
                ],
                "demand": self._demand_payload(),
            }
        )

    def _prepare_builder(self) -> None:
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("builder prepare payload must be a JSON object")
            unsupported = sorted(set(raw) - {"count", "id", "ttl_seconds"})
            if unsupported:
                raise ValueError(
                    "unsupported builder prepare fields: " + ", ".join(unsupported)
                )
            prepare_id = str(
                raw.get("id") or f"builder-prep-{uuid4().hex[:16]}"
            ).strip()
            if not prepare_id or "/" in prepare_id:
                raise ValueError("prepare id must be non-empty and cannot contain '/'.")
            count = _strict_positive_integer(raw.get("count", 1), "count")
            ttl_seconds = _strict_positive_integer(
                raw.get("ttl_seconds", 900),
                "ttl_seconds",
            )
            if count <= 0:
                raise ValueError("count must be positive.")
            if ttl_seconds <= 0:
                raise ValueError("ttl_seconds must be positive.")
        except (TypeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        item = self.routing_store.upsert_prepared_builder(
            prepare_id,
            count=count,
            ttl_seconds=ttl_seconds,
        )
        self._write_json(
            {
                "prepare": item.to_dict(),
                "demand": self._demand_payload(),
            },
            status=HTTPStatus.CREATED,
        )

    def _delete_prepared_builder(self, prepare_id: str) -> None:
        deleted = self.routing_store.delete_prepared_builder(prepare_id)
        self._write_json(
            {
                "ok": True,
                "deleted": deleted.to_dict() if deleted is not None else None,
                "demand": self._demand_payload(),
            }
        )

    def _list_sandboxes_from_cache(self) -> None:
        heartbeats = self.store.load_heartbeats()
        heartbeats_by_node_id = {
            heartbeat.node_id: heartbeat for heartbeat in heartbeats.values()
        }
        sandboxes = [
            _route_only_sandbox_record(
                route,
                heartbeats_by_node_id.get(route.node_id),
                heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
            )
            for route in self.routing_store.sandbox_routes_readonly()
        ]
        self._write_json(
            {
                "sandboxes": sandboxes,
                "cached": True,
                "refresh_supported": True,
            }
        )

    def _list_sandboxes_across_nodes(self) -> None:
        sandboxes: list[dict[str, Any]] = []
        observed_ids: set[str] = set()
        reconciled_node_urls: set[str] = set()
        heartbeats = self._ready_sandbox_heartbeats()
        heartbeats_by_node_id = {
            heartbeat.node_id: heartbeat for heartbeat in heartbeats
        }
        for heartbeat in heartbeats:
            observed_at = utc_now().isoformat()
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/sandboxes",
                method="GET",
                timeout_seconds=NODE_RECONCILE_PROXY_TIMEOUT_SECONDS,
            )
            if response.status >= 400:
                continue
            reconciled_node_urls.add((heartbeat.node_url or "").rstrip("/"))
            payload = response.json()
            raw_sandboxes = payload.get("sandboxes")
            if not isinstance(raw_sandboxes, list):
                continue
            observations: list[SandboxInventoryEntry] = []
            reported_ids: set[str] = set()
            records_by_id: dict[str, dict[str, Any]] = {}
            for record in raw_sandboxes:
                if not isinstance(record, dict):
                    continue
                spec = record.get("spec")
                sandbox_id = spec.get("id") if isinstance(spec, dict) else None
                if isinstance(sandbox_id, str) and sandbox_id:
                    reported_ids.add(sandbox_id)
                    try:
                        observation = _sandbox_inventory_from_record(record)
                    except (TypeError, ValueError):
                        # Protect a known route from absence reconciliation, but do
                        # not publish malformed node state as a gateway record.
                        continue
                    observed_ids.add(sandbox_id)
                    observations.append(observation)
                    records_by_id[sandbox_id] = record
            removed_routes, stale_snapshot_routes = (
                self.routing_store.reconcile_sandboxes_for_node(
                    heartbeat.node_url or "",
                    observations,
                    node_id=heartbeat.node_id,
                    job_id=heartbeat.job_id,
                    reported_sandbox_ids=reported_ids,
                    observed_at=observed_at,
                    node_epoch=heartbeat.node_epoch,
                    activity_epoch=heartbeat.activity_epoch,
                    allow_node_epoch_adoption=False,
                )
            )
            for route in stale_snapshot_routes:
                self._release_registry_snapshot_reference(route)
            for route in removed_routes:
                self._release_registry_route_reference(route)
            for sandbox_id in reported_ids:
                stored_route = self.routing_store.get_sandbox_readonly(sandbox_id)
                record = records_by_id.get(sandbox_id)
                if stored_route is None or record is None:
                    continue
                if _route_targets_node(stored_route, heartbeat):
                    try:
                        # The record was sampled after this heartbeat revision,
                        # not after whichever route happens to be current when
                        # the network response arrives. Preserve that fence so
                        # a delayed RUNNING/PARKED record cannot inherit and
                        # overwrite a newer lifecycle revision.
                        observed_route = replace(
                            stored_route,
                            node_epoch=heartbeat.node_epoch,
                            activity_epoch=heartbeat.activity_epoch,
                        )
                        confirmed = _route_with_sandbox_record(
                            observed_route,
                            record,
                        )
                    except (TypeError, ValueError):
                        continue
                    if (
                        confirmed.generation == stored_route.generation
                        and confirmed.create_operation_id
                        == stored_route.create_operation_id
                        and confirmed.spec_hash == stored_route.spec_hash
                    ):
                        try:
                            self._ensure_registry_route_reference(
                                confirmed,
                                touch=True,
                            )
                        except RegistryImageReferenceUnavailable:
                            current_route = self.routing_store.get_sandbox_readonly(
                                sandbox_id
                            )
                            self._release_registry_route_reference(
                                confirmed,
                                keep_route=current_route,
                            )
                            continue
                        try:
                            stored_route = self.routing_store.upsert_sandbox(
                                confirmed,
                                allow_node_epoch_adoption=False,
                            )
                        except BaseException:
                            # A failed commit acknowledgement is ambiguous. Keep
                            # only the owner required by durable route read-back;
                            # if read-back fails, retain it conservatively.
                            try:
                                current_route = self.routing_store.get_sandbox_readonly(
                                    sandbox_id
                                )
                            except BaseException:
                                raise
                            self._release_registry_route_reference(
                                confirmed,
                                keep_route=current_route,
                            )
                            raise
                        self._release_registry_route_reference(
                            confirmed,
                            keep_route=stored_route,
                        )
                sandboxes.append(_enrich_sandbox_record(record, heartbeat))
                self._ensure_registry_route_reference(stored_route, touch=True)
        for route in self.routing_store.sandbox_routes_readonly():
            if route.sandbox_id in observed_ids:
                continue
            if route.node_url.rstrip("/") in reconciled_node_urls:
                continue
            sandboxes.append(
                _route_only_sandbox_record(
                    route,
                    heartbeats_by_node_id.get(route.node_id),
                    heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
                )
            )
        self._write_json({"sandboxes": sandboxes, "cached": False})

    def _list_images_across_nodes(self) -> None:
        snapshot = self._cached_raw_image_inventory_across_nodes()
        self._write_json(
            {
                "images": self._enrich_image_inventory_records(snapshot.records),
                "complete": snapshot.complete,
            }
        )

    def _cached_raw_image_inventory_across_nodes(self) -> ImageInventorySnapshot:
        return type(self).image_inventory_cache.get_or_load(
            self._load_raw_image_inventory_across_nodes
        )

    def _invalidate_image_inventory_cache(self) -> None:
        type(self).image_inventory_cache.invalidate()

    def _load_raw_image_inventory_across_nodes(self) -> ImageInventorySnapshot:
        images: list[dict[str, Any]] = []
        for record in sorted(self.image_manager.list(), key=lambda item: item.id):
            raw = record.to_dict()
            raw["location"] = "control-plane"
            images.append(raw)
        complete = True
        unobserved_references: set[str] = set()
        for heartbeat in self._ready_heartbeats():
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/images",
                method="GET",
            )
            if response.status >= 400:
                complete = False
                if heartbeat.cached_images_known:
                    unobserved_references.update(heartbeat.cached_images)
                continue
            payload = response.json()
            raw_images = payload.get("images")
            if not isinstance(raw_images, list):
                complete = False
                if heartbeat.cached_images_known:
                    unobserved_references.update(heartbeat.cached_images)
                continue
            for record in raw_images:
                if not isinstance(record, dict):
                    complete = False
                    if heartbeat.cached_images_known:
                        unobserved_references.update(heartbeat.cached_images)
                    continue
                raw = dict(record)
                raw["node"] = _node_metadata(heartbeat)
                images.append(raw)
        return ImageInventorySnapshot.from_records(
            images,
            complete=complete,
            unobserved_references=unobserved_references,
        )

    def _enrich_image_inventory_records(
        self,
        records: tuple[dict[str, Any], ...],
        *,
        image_id: str | None = None,
    ) -> list[dict[str, Any]]:
        images: list[dict[str, Any]] = []
        for raw in records:
            if image_id is not None and raw.get("id") != image_id:
                continue
            record = dict(raw)
            if self._image_record_missing_registry_manifest(record):
                if record.get("location") == "control-plane":
                    tag = str(record.get("tag") or "")
                    if tag:
                        self.image_manager.store.delete_by_tags([tag])
                continue
            images.append(self._image_record_with_registry_digest(record))
        return images

    def _list_image_builds_across_nodes(self) -> None:
        self._write_json({"builds": self._image_build_records_across_nodes()})

    def _get_image_build(self, build_key: str) -> None:
        matches = [
            build
            for build in self._image_build_records_across_nodes()
            if build.get("build_id") == build_key or build.get("image_id") == build_key
        ]
        if not matches:
            self._write_json(
                {"error": "image build not found"},
                status=HTTPStatus.NOT_FOUND,
            )
            return
        selected = sorted(
            matches,
            key=lambda item: (
                str(item.get("created_at") or ""),
                str(item.get("build_id") or ""),
            ),
        )[-1]
        selected_image_id = str(selected.get("image_id") or "")
        if selected_image_id and _image_build_response_terminal({"build": selected}):
            self.routing_store.clear_pending_image_build(selected_image_id)
        self._record_successful_build_image(selected)
        self._write_json({"build": selected})

    def _image_build_records_across_nodes(self) -> list[dict[str, Any]]:
        builds = self._cached_image_build_records()
        for heartbeat in self._ready_heartbeats():
            if "image-build" not in heartbeat.capabilities:
                continue
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/images/builds",
                method="GET",
                timeout_seconds=NODE_RECONCILE_PROXY_TIMEOUT_SECONDS,
            )
            if response.status >= 400:
                continue
            raw_builds = response.json().get("builds")
            if not isinstance(raw_builds, list):
                continue
            for record in raw_builds:
                if isinstance(record, dict):
                    enriched = dict(record)
                    enriched["location"] = heartbeat.node_id
                    enriched["node"] = _node_metadata(heartbeat)
                    self._record_successful_build_image(enriched)
                    builds.append(enriched)
        return builds

    def _cached_image_build_records(self) -> list[dict[str, Any]]:
        builds: list[dict[str, Any]] = []
        for record in sorted(
            self.image_manager.list_builds(),
            key=lambda item: (item.created_at, item.build_id),
        ):
            enriched = record.to_dict()
            enriched["location"] = "control-plane"
            builds.append(enriched)
        return builds

    def _registry_status(self) -> dict[str, Any]:
        if not self.registry_url:
            return {
                "configured": False,
                "ok": False,
                "url": "",
                "repository_count": 0,
                "scanned_repository_count": 0,
                "scanned_tag_count": 0,
                "visible_tag_count": 0,
                "catalog_truncated": False,
                "repositories": [],
            }
        client = RegistryClient(
            self.registry_url,
            timeout_seconds=REGISTRY_METRICS_TIMEOUT_SECONDS,
        )
        try:
            return registry_summary(client)
        except Exception as exc:
            return {
                "configured": True,
                "ok": False,
                "url": self.registry_url,
                "repository_count": 0,
                "scanned_repository_count": 0,
                "scanned_tag_count": 0,
                "visible_tag_count": 0,
                "catalog_truncated": False,
                "repositories": [],
                "error": str(exc),
            }

    def _registry_status_cached(self, *, force_refresh: bool = False) -> dict[str, Any]:
        now = time.monotonic()
        handler_cls = type(self)
        with handler_cls.registry_status_lock:
            cached = handler_cls.registry_status_cache
            if (
                not force_refresh
                and cached is not None
                and now - handler_cls.registry_status_cache_at
                <= REGISTRY_STATUS_CACHE_TTL_SECONDS
            ):
                result = dict(cached)
                result["cached"] = True
                return result
            result = self._registry_status()
            result["cached"] = False
            handler_cls.registry_status_cache = dict(result)
            handler_cls.registry_status_cache_at = now
            return result

    def _record_successful_build_image(self, build: dict[str, Any]) -> None:
        if build.get("status") != "succeeded":
            return
        raw_image = build.get("image")
        if not isinstance(raw_image, dict) or not _image_record_available_to_sandboxes(
            raw_image
        ):
            return
        raw_image = self._image_record_with_registry_digest(raw_image)
        build["image"] = raw_image
        try:
            self.image_manager.store.upsert(ImageRecord.from_dict(raw_image))
        except ValueError:
            pass
        self._invalidate_image_inventory_cache()

    def _managed_registry_manifest_digest(self, image_ref: str) -> str:
        try:
            digest = self._resolve_and_protect_managed_manifest(image_ref)
        except (OSError, ValueError, RegistryRequestError):
            return ""
        existing = manifest_digest_from_image_ref(image_ref)
        if existing and digest != existing:
            return ""
        return digest

    def _resolve_and_protect_managed_manifest(self, image_ref: str) -> str:
        existing = manifest_digest_from_image_ref(image_ref)
        if not self.registry_url:
            return existing
        coordinates = _managed_registry_image_coordinates(
            image_ref,
            self.registry_url,
            self.registry_worker_url or "",
        )
        if coordinates is None:
            return existing
        repository, image_tag = coordinates
        reference = existing or image_tag
        cache = self.registry_manifest_cache
        if cache is not None:
            cached = cache.get(repository, reference)
            if cached:
                return cached
        client = RegistryClient(self.registry_url)
        digest = normalize_manifest_digest(
            client.manifest_digest(repository, reference)
        )
        if not digest or (existing and digest != existing):
            return ""
        client.ensure_digest_protection_tag(repository, digest)
        if cache is not None:
            cache.put(repository, reference, digest)
            cache.put(repository, digest, digest)
        return digest

    def _image_record_with_registry_digest(
        self,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        updated = dict(record)
        existing = normalize_manifest_digest(str(record.get("manifest_digest") or ""))
        tag = str(record.get("tag") or "")
        digest = self._managed_registry_manifest_digest(
            image_ref_with_manifest_digest(tag, existing) if existing else tag
        )
        managed_record = bool(
            self.registry_url
            and _managed_registry_image_coordinates(
                tag,
                self.registry_url,
                self.registry_worker_url or "",
            )
            is not None
        )
        if not digest and not managed_record:
            digest = existing
        if digest:
            updated["manifest_digest"] = digest
        elif managed_record:
            # Never advertise an unprotected managed digest retained in a
            # builder/node response from before protection was established.
            updated["manifest_digest"] = ""
        return updated

    def _create_sandbox_on_node(self) -> None:
        limiter = self.sandbox_create_limiter
        limiter_acquired = False
        parsed_ok = False
        try:
            if limiter is not None and not limiter.acquire(blocking=False):
                self.sandbox_create_busy_sampler.record(
                    max_concurrent_sandbox_creates=(
                        self.max_concurrent_sandbox_creates
                    ),
                )
                self._write_json(
                    {
                        "error": "gateway is busy creating sandboxes; retry shortly",
                        "retryable": True,
                        "max_concurrent_sandbox_creates": (
                            self.max_concurrent_sandbox_creates
                        ),
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers={
                        "Retry-After": str(SANDBOX_CREATE_BUSY_RETRY_AFTER_SECONDS),
                        "X-UCloud-Sandbox-Retryable": "true",
                    },
                )
                return
            limiter_acquired = limiter is not None
            body = self._read_raw_body(max_bytes=DEFAULT_MAX_JSON_BODY_BYTES)
            raw = json.loads(body.decode("utf-8")) if body else None
            if not isinstance(raw, dict):
                raise ValueError("sandbox payload must be a JSON object")
            spec = SandboxSpec.from_dict(raw)
            spec.validate()
            requested = spec.requested_resources()
            if not requested.fits_within(self.max_sandbox_resources):
                raise SandboxShapeUnschedulableError(
                    requested,
                    self.max_sandbox_resources,
                )
            parsed_ok = True
        except SandboxShapeUnschedulableError as exc:
            self._write_json(
                {
                    "error": str(exc),
                    "error_code": "sandbox_shape_unschedulable",
                    "retryable": False,
                    "requested_resources": exc.requested.to_dict(),
                    "maximum_resources": exc.maximum.to_dict(),
                },
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
            return
        except (json.JSONDecodeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        finally:
            if limiter_acquired and not parsed_ok:
                limiter.release()

        try:
            self._create_sandbox_on_node_locked(spec)
        finally:
            if limiter_acquired:
                limiter.release()
        return

    def _create_sandbox_on_node_locked(
        self,
        spec: SandboxSpec,
        *,
        excluded_job_ids: tuple[str, ...] = (),
        last_failure_reason: str = "",
        image_resolved: bool = False,
    ) -> None:
        requested = spec.requested_resources()
        with self.telemetry.span(
            "gateway.sandbox_create",
            attributes={
                "sandbox.id": spec.id,
                "container.image.name": spec.image,
                "sandbox.request.vcpu": requested.vcpu,
                "sandbox.request.memory_mb": requested.memory_mb,
                "sandbox.request.disk_mb": requested.disk_mb,
                "sandbox.placement.excluded_jobs": len(excluded_job_ids),
            },
        ) as root:
            if not image_resolved:
                with self.telemetry.span(
                    "gateway.sandbox_resolve_image",
                    attributes={"container.image.name": spec.image},
                ) as span:
                    resolved_image, image_error = self._resolve_request_image_reference(
                        spec.image
                    )
                    span.set_attribute("resolved_image", resolved_image)
                    if image_error is not None:
                        span.status = "error"
                        root.status = "error"
                        root.set_attribute("outcome", "image_reference_unavailable")
                        self._write_image_resolution_error(image_error)
                        return
                    if resolved_image != spec.image:
                        spec = replace(spec, image=resolved_image)
                        root.set_attribute("resolved_image", resolved_image)

            with self.telemetry.span(
                "gateway.sandbox_existing_route_check",
            ) as span:
                existing = self.routing_store.get_sandbox_readonly(spec.id)
                span.set_attribute("existing_route", existing is not None)
                if existing is not None:
                    requested_hash = sandbox_spec_fingerprint(spec)
                    existing_spec_matches = True
                    if existing.spec:
                        try:
                            existing_spec_matches = sandbox_specs_match(
                                SandboxSpec.from_dict(existing.spec), spec
                            )
                        except (TypeError, ValueError):
                            existing_spec_matches = False
                    if (
                        existing.spec_hash and existing.spec_hash != requested_hash
                    ) or not existing_spec_matches:
                        root.status = "error"
                        root.set_attribute("outcome", "generation_spec_conflict")
                        self._write_json(
                            {
                                "error": (
                                    f"sandbox already exists with different spec: {spec.id}"
                                )
                            },
                            status=HTTPStatus.CONFLICT,
                        )
                        return
                    if self._send_existing_sandbox_response(
                        existing,
                        spec,
                        status=HTTPStatus.OK,
                    ):
                        root.set_attribute("outcome", "recovered_existing")
                        return
                    if (
                        existing.spec_hash == requested_hash
                        and existing.state.lower() in {"creating", "unknown"}
                    ):
                        root.set_attribute("outcome", "retry_same_generation")
                        self._retry_sandbox_create_on_assigned_node(existing, spec)
                        return
                    # Age and aggregate active counts cannot fence a delayed
                    # create. Only generation-aware complete inventory or a
                    # successful same-generation delete may remove this route.
                    root.status = "error"
                    root.set_attribute("outcome", "route_pending")
                    self._write_create_in_progress_response(spec.id)
                    return

            if existing is not None:
                root.status = "error"
                root.set_attribute("outcome", "duplicate")
                self._write_json(
                    {"error": f"sandbox already exists: {spec.id}"},
                    status=HTTPStatus.CONFLICT,
                )
                return

            if self.registry_layer_cache is not None:
                with self.telemetry.span(
                    "gateway.sandbox_resolve_layers",
                    attributes={"container.image.name": spec.image},
                ) as span:
                    # Layer overlap only improves placement scoring; it is not
                    # part of image identity or admission correctness.  A cold
                    # metadata lookup can take the full registry timeout, so
                    # never put it in the create critical path.  This request
                    # uses any already-cached manifest while a later request
                    # benefits from the asynchronous hydration.
                    manifest = self.registry_layer_cache.get(spec.image)
                    if manifest is None:
                        self.registry_layer_cache.hydrate_async((spec.image,))
                    span.set_attribute("available", manifest is not None)
                    if manifest is not None:
                        span.set_attribute("layer_count", len(manifest.layers))
                        span.set_attribute("compressed_bytes", manifest.total_size)

            with self.telemetry.span(
                "gateway.sandbox_select_node",
                attributes={"container.image.name": spec.image},
            ) as span:
                pending_before = None
                try:
                    placement = self._select_and_reserve_node(
                        spec.id,
                        spec.requested_resources(),
                        image=spec.image,
                        spec=spec.to_dict(),
                        spec_hash=sandbox_spec_fingerprint(spec),
                        excluded_job_ids=excluded_job_ids,
                    )
                except GatewaySchedulingBusyError:
                    root.status = "error"
                    root.set_attribute("outcome", "placement_busy")
                    self._write_json(
                        {
                            "error": (
                                "gateway is busy reserving sandbox placement; "
                                "retry shortly"
                            ),
                            "retryable": True,
                        },
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        headers={
                            "Retry-After": str(SANDBOX_CREATE_BUSY_RETRY_AFTER_SECONDS),
                            "X-UCloud-Sandbox-Retryable": "true",
                        },
                    )
                    return
                except SandboxRouteConflictError:
                    root.status = "error"
                    root.set_attribute("outcome", "concurrent_spec_conflict")
                    self._write_json(
                        {
                            "error": (
                                f"sandbox already exists with different spec: {spec.id}"
                            )
                        },
                        status=HTTPStatus.CONFLICT,
                    )
                    return
                heartbeat = placement[0] if placement is not None else None
                route = placement[1] if placement is not None else None
                pending_before = placement[2] if placement is not None else None
                span.set_attribute(
                    "selected_node_id", heartbeat.node_id if heartbeat else ""
                )
                span.set_attribute(
                    "selected_job_id", heartbeat.job_id if heartbeat else ""
                )
            if heartbeat is None:
                _pending, demand = self.routing_store.upsert_pending_with_demand(
                    spec.id,
                    spec.requested_resources(),
                    failure_reason=last_failure_reason,
                )
                root.status = "error"
                root.set_attribute("outcome", "queued_no_ready_node")
                root.set_attribute(
                    "pending_resources", demand.pending_resources.to_dict()
                )
                self._write_json(
                    {
                        "error": "no ready node has resources for sandbox request",
                        "error_code": last_failure_reason or "no_ready_node",
                        "retryable": True,
                        "pending_resources": demand.pending_resources.to_dict(),
                        "oldest_pending_seconds": demand.oldest_pending_seconds,
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers={
                        "Retry-After": str(SANDBOX_CREATE_BUSY_RETRY_AFTER_SECONDS),
                        "X-UCloud-Sandbox-Retryable": "true",
                    },
                )
                return

            assert route is not None
            if route.node_url.rstrip("/") != (heartbeat.node_url or "").rstrip("/"):
                root.set_attribute("outcome", "concurrent_route_won")
                self._retry_sandbox_create_on_assigned_node(route, spec)
                return
            try:
                self._ensure_registry_route_reference(route, touch=True)
            except RegistryImageReferenceUnavailable:
                # No node pull/create has been dispatched yet, so remove
                # the provisional route, retain the accepted demand, and
                # fail closed.  A retry allocates a new route incarnation.
                removed = self.routing_store.delete_sandbox_if_current(
                    spec.id,
                    generation=route.generation,
                    create_operation_id=route.create_operation_id,
                )
                if removed is not None:
                    self._release_registry_route_reference(removed)
                self._persist_failed_sandbox_demand(
                    spec,
                    route,
                    failure_reason="registry_lease_unavailable",
                )
                raise
            root.set_attribute("reserved_route", True)
            with self.telemetry.span(
                "gateway.sandbox_ensure_image",
                attributes={
                    "node.id": heartbeat.node_id,
                    "container.image.name": spec.image,
                },
            ) as span:
                initial_cache_hit = self._node_has_image(heartbeat, spec.image)
                image_response = self._ensure_image_on_node(heartbeat, spec.image)
                span.set_attribute("cache_hit", image_response is None)
                span.set_attribute("initial_cache_hit", initial_cache_hit)
                span.set_attribute(
                    "waited_for_peer_pull",
                    not initial_cache_hit and image_response is None,
                )
                span.set_attribute("pulled", image_response is not None)
                if image_response is not None:
                    span.set_attribute("status_code", int(image_response.status))
                    pull_payload = image_response.json()
                    pull_timings = pull_payload.get("timings")
                    if isinstance(pull_timings, dict):
                        span.add_event("node.timings", pull_timings)
                    if image_response.status >= 400:
                        span.status = "error"
                        span.set_attribute(
                            "error_code",
                            str(pull_payload.get("error_code") or ""),
                        )
            if image_response is not None and image_response.status >= 400:
                removed = self.routing_store.delete_sandbox_if_current(
                    spec.id,
                    generation=route.generation,
                    create_operation_id=route.create_operation_id,
                )
                if removed is not None:
                    self._release_registry_route_reference(removed)
                    self._persist_failed_sandbox_demand(
                        spec,
                        removed,
                        failure_reason=f"image_pull_http_{image_response.status}",
                    )
                root.status = "error"
                root.set_attribute("outcome", "image_pull_failed")
                self._write_json(
                    {
                        "error": (
                            "image is not available on selected sandbox node; pull failed. "
                            "For gateway-managed images, resubmit the build by image id "
                            "before creating sandboxes."
                        ),
                        "pull": image_response.json(),
                    },
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return

            refreshed_heartbeat = self._heartbeat_for_route(
                job_id=route.job_id,
            )
            refreshed_available = (
                _node_available_resources(
                    refreshed_heartbeat,
                    self._placement_routes(),
                )
                if refreshed_heartbeat is not None
                else ResourceQuantity()
            )
            refreshed_available = replace(
                refreshed_available,
                # This request already owns its durable route reservation.
                disk_mb=refreshed_available.disk_mb + requested.disk_mb,
            )
            pressure_changed = not bool(
                refreshed_heartbeat is not None
                and refreshed_heartbeat.node_url
                and refreshed_heartbeat.is_fresh(utc_now(), self.heartbeat_ttl_seconds)
                and not refreshed_heartbeat.draining
                and refreshed_heartbeat.admission_open
                and _node_can_fit_available(
                    refreshed_heartbeat,
                    requested,
                    refreshed_available,
                )
            )
            if pressure_changed:
                failure_reason = "node_actual_pressure_changed"
                removed = self.routing_store.delete_sandbox_if_current(
                    spec.id,
                    generation=route.generation,
                    create_operation_id=route.create_operation_id,
                )
                if removed is None:
                    root.status = "error"
                    root.set_attribute("outcome", "route_changed_during_reselect")
                    self._write_create_in_progress_response(spec.id)
                    return
                self._release_registry_route_reference(removed)
                _pending, demand = self.routing_store.upsert_pending_with_demand(
                    spec.id,
                    requested,
                    generation=route.generation,
                    operation_id=route.create_operation_id,
                    spec_hash=route.spec_hash,
                    failure_reason=failure_reason,
                )
                next_excluded = tuple(dict.fromkeys((*excluded_job_ids, route.job_id)))
                if self._sandbox_create_alternate_available(
                    spec,
                    excluded_job_ids=next_excluded,
                ):
                    root.set_attribute("outcome", "reselect_after_pressure_change")
                    root.set_attribute("rejected_job_id", route.job_id)
                    self._create_sandbox_on_node_locked(
                        spec,
                        excluded_job_ids=next_excluded,
                        last_failure_reason=failure_reason,
                        image_resolved=True,
                    )
                    return
                root.status = "error"
                root.set_attribute("outcome", failure_reason)
                self._write_json(
                    {
                        "error": (
                            "selected node became busy while preparing the image; "
                            "retry shortly"
                        ),
                        "error_code": failure_reason,
                        "retryable": True,
                        "pending_resources": demand.pending_resources.to_dict(),
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers={
                        "Retry-After": "1",
                        "X-UCloud-Sandbox-Retryable": "true",
                    },
                )
                return
            assert refreshed_heartbeat is not None
            heartbeat = refreshed_heartbeat

            with self.telemetry.span(
                "gateway.sandbox_proxy_create",
                attributes={"node.id": heartbeat.node_id},
            ) as span:
                response = self._proxy_request(
                    heartbeat.node_url or "",
                    "/v1/sandboxes",
                    method="POST",
                    body=_sandbox_create_request_body(spec, route),
                    timeout_seconds=SANDBOX_CREATE_PROXY_TIMEOUT_SECONDS,
                )
                span.set_attribute("status_code", int(response.status))
                response_payload = response.json()
                node_timings = response_payload.get("timings")
                if isinstance(node_timings, dict):
                    span.add_event("node.timings", node_timings)
            if _is_duplicate_sandbox_response(response, spec.id):
                if self._send_existing_sandbox_response(
                    route,
                    spec,
                    status=HTTPStatus.CREATED,
                    pending=pending_before,
                ):
                    root.set_attribute("outcome", "recovered_duplicate")
                    return
            if 200 <= response.status < 300:
                record = response_payload.get("sandbox")
                if isinstance(record, dict) and _sandbox_record_matches_route(
                    record, route, spec
                ):
                    route = _route_with_sandbox_record(route, record)
                else:
                    root.status = "error"
                    root.set_attribute("outcome", "invalid_create_confirmation")
                    self._write_json(
                        {
                            "error": (
                                "node create response did not confirm the assigned "
                                "sandbox generation and spec hash"
                            ),
                            "retryable": True,
                        },
                        status=HTTPStatus.BAD_GATEWAY,
                    )
                    return
                self.routing_store.upsert_sandbox(route)
                record_sandbox_scheduled(
                    self.metrics_store,
                    sandbox_id=spec.id,
                    route=route,
                    resources=spec.requested_resources(),
                    pending=pending_before,
                )
                self._record_registry_image_used(spec.image)
                root.set_attribute("outcome", "scheduled")
                root.set_attribute("node_id", heartbeat.node_id)
            else:
                rejection_reason = _node_create_rejection_reason(response)
                if rejection_reason is not None:
                    removed = self.routing_store.delete_sandbox_if_current(
                        spec.id,
                        generation=route.generation,
                        create_operation_id=route.create_operation_id,
                    )
                    if removed is not None:
                        self._release_registry_route_reference(removed)
                        self._persist_failed_sandbox_demand(
                            spec,
                            route,
                            failure_reason=rejection_reason,
                        )
                    next_excluded = tuple(
                        dict.fromkeys((*excluded_job_ids, route.job_id))
                    )
                    if removed is not None and self._sandbox_create_alternate_available(
                        spec,
                        excluded_job_ids=next_excluded,
                    ):
                        root.set_attribute("outcome", "reselect_after_node_rejection")
                        root.set_attribute("rejection_reason", rejection_reason)
                        root.set_attribute("rejected_job_id", route.job_id)
                        self._create_sandbox_on_node_locked(
                            spec,
                            excluded_job_ids=next_excluded,
                            last_failure_reason=rejection_reason,
                            image_resolved=True,
                        )
                        return
                root.status = "error"
                root.set_attribute("outcome", "node_create_failed")
                root.set_attribute("status_code", int(response.status))
                if _node_create_may_still_be_running(
                    response
                ) and not _node_create_definitively_rejected(response):
                    root.set_attribute("kept_durable_route", True)
                elif rejection_reason is None:
                    removed = self.routing_store.delete_sandbox_if_current(
                        spec.id,
                        generation=route.generation,
                        create_operation_id=route.create_operation_id,
                    )
                    if removed is not None:
                        self._release_registry_route_reference(removed)
            self._send_proxied_response(response)

    def _persist_failed_sandbox_demand(
        self,
        spec: SandboxSpec,
        route: SandboxRoute,
        *,
        failure_reason: str,
    ) -> None:
        self.routing_store.upsert_pending(
            spec.id,
            spec.requested_resources(),
            generation=route.generation,
            operation_id=route.create_operation_id,
            spec_hash=route.spec_hash,
            failure_reason=failure_reason,
        )

    def _send_existing_sandbox_response(
        self,
        route: SandboxRoute,
        spec: SandboxSpec,
        *,
        status: HTTPStatus,
        pending: PendingSandboxDemand | None = None,
    ) -> bool:
        if not self._route_worker_is_fresh(route):
            return False
        record = self._sandbox_record_on_node(route.node_url, spec.id)
        if (
            record is None
            or not _sandbox_record_matches_route(record, route, spec)
            or not _sandbox_record_is_ready(record)
        ):
            return False
        route = _route_with_sandbox_record(route, record)
        self.routing_store.upsert_sandbox(route)
        route = self.routing_store.get_sandbox_readonly(spec.id) or route
        self._ensure_registry_route_reference(route, touch=True)
        if pending is not None:
            record_sandbox_scheduled(
                self.metrics_store,
                sandbox_id=spec.id,
                route=route,
                resources=spec.requested_resources(),
                pending=pending,
            )
        self._record_registry_image_used(spec.image)
        self._write_json({"sandbox": record, "recovered": True}, status=status)
        return True

    def _retry_sandbox_create_on_assigned_node(
        self,
        route: SandboxRoute,
        spec: SandboxSpec,
    ) -> None:
        """Replay an ambiguous create without changing its node or identity."""

        if route.spec_hash != sandbox_spec_fingerprint(spec):
            self._write_json(
                {"error": f"sandbox already exists with different spec: {spec.id}"},
                status=HTTPStatus.CONFLICT,
            )
            return
        if not self._route_worker_is_fresh(route):
            self._write_route_worker_unreachable(route)
            return
        self._ensure_registry_route_reference(route, touch=True)
        response = self._proxy_request(
            route.node_url,
            "/v1/sandboxes",
            method="POST",
            body=_sandbox_create_request_body(spec, route),
            timeout_seconds=SANDBOX_CREATE_PROXY_TIMEOUT_SECONDS,
        )
        payload = response.json()
        record = payload.get("sandbox")
        if 200 <= response.status < 300:
            if not isinstance(record, dict) or not _sandbox_record_matches_route(
                record, route, spec
            ):
                self._write_json(
                    {
                        "error": (
                            "node create response did not confirm the assigned "
                            "sandbox generation and spec hash"
                        ),
                        "retryable": True,
                    },
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return
            stored = self.routing_store.upsert_sandbox(
                _route_with_sandbox_record(route, record)
            )
            self._ensure_registry_route_reference(stored, touch=True)
            self._record_registry_image_used(spec.image)
            self._write_json(
                {"sandbox": record, "recovered": True},
                status=HTTPStatus.OK,
            )
            return
        if _is_duplicate_sandbox_response(response, spec.id) and (
            self._send_existing_sandbox_response(
                route,
                spec,
                status=HTTPStatus.OK,
            )
        ):
            return
        rejection_reason = _node_create_rejection_reason(response)
        if rejection_reason is not None:
            removed = self.routing_store.delete_sandbox_if_current(
                spec.id,
                generation=route.generation,
                create_operation_id=route.create_operation_id,
            )
            if removed is not None:
                self._release_registry_route_reference(removed)
                self._persist_failed_sandbox_demand(
                    spec,
                    route,
                    failure_reason=rejection_reason,
                )
                excluded_job_ids = (route.job_id,)
                if self._sandbox_create_alternate_available(
                    spec,
                    excluded_job_ids=excluded_job_ids,
                ):
                    self._create_sandbox_on_node_locked(
                        spec,
                        excluded_job_ids=excluded_job_ids,
                        last_failure_reason=rejection_reason,
                        image_resolved=True,
                    )
                    return
        # Ambiguous failures retain the identity fence for another identical
        # replay. A closed admission gate is synchronous and definitive, so its
        # route was removed above and the request can be placed elsewhere.
        self._send_proxied_response(response)

    def _record_registry_image_used(self, image_ref: str) -> None:
        if self.registry_usage_store is None:
            return
        if _private_registry_image_coordinates(image_ref) is None:
            return
        try:
            self.registry_usage_store.touch_image(image_ref)
        except (OSError, ValueError):
            return

    def _ensure_registry_image_lease(
        self,
        image_ref: str,
        owner: str,
        *,
        touch: bool,
    ) -> None:
        store = self.registry_usage_store
        if store is None:
            return
        try:
            _persist_registry_image_protection(
                store,
                image_ref,
                owner,
                touch=touch,
                persistent=False,
            )
        except (OSError, TypeError, ValueError) as exc:
            raise RegistryImageReferenceUnavailable(
                "registry image-use state could not be persisted"
            ) from exc

    def _ensure_registry_route_reference(
        self,
        route: SandboxRoute,
        *,
        touch: bool,
    ) -> None:
        image_ref = str(route.spec.get("image") or "")
        store = self.registry_usage_store
        if store is None:
            return
        if image_ref:
            try:
                _persist_registry_image_protection(
                    store,
                    image_ref,
                    _registry_route_reference_owner(
                        route,
                        deployment_id=self.deployment_id,
                        route_generation=route.generation,
                    ),
                    touch=touch,
                    persistent=True,
                )
            except (OSError, TypeError, ValueError) as exc:
                raise RegistryImageReferenceUnavailable(
                    "registry route image reference could not be persisted"
                ) from exc
        if (
            route.snapshot_repository
            and route.snapshot_tag
            and route.snapshot_manifest_digest
        ):
            self._ensure_registry_snapshot_reference(
                route,
                repository=route.snapshot_repository,
                tag=route.snapshot_tag,
                digest=route.snapshot_manifest_digest,
            )

    def _protect_registry_image_build_target(
        self,
        spec: ImageBuildSpec,
        *,
        push: bool,
    ) -> None:
        if (
            not push
            or self.registry_usage_store is None
            or not self.registry_url
            or _managed_registry_image_coordinates(
                spec.tag,
                self.registry_url,
                self.registry_worker_url or "",
            )
            is None
        ):
            return
        try:
            touched = self.registry_usage_store.touch_image(spec.tag)
            if touched is None:
                raise ValueError("registry image-build target could not be recorded")
        except (OSError, TypeError, ValueError) as exc:
            raise RegistryImageReferenceUnavailable(
                "registry image-build target could not be protected"
            ) from exc

    def _release_registry_route_reference(
        self,
        route: SandboxRoute,
        *,
        keep_route: SandboxRoute | None = None,
    ) -> None:
        store = self.registry_usage_store
        if store is not None:
            release_registry_route_references(
                store,
                route,
                deployment_id=self.deployment_id,
                keep_route=keep_route,
            )

    def _ensure_registry_snapshot_reference(
        self,
        route: SandboxRoute,
        *,
        repository: str,
        tag: str,
        digest: str,
    ) -> None:
        store = self.registry_usage_store
        if store is None:
            return
        if not repository or not tag or not digest:
            raise RegistryImageReferenceUnavailable(
                "snapshot registry identity is incomplete"
            )
        try:
            owner = _registry_snapshot_reference_owner(
                route,
                deployment_id=self.deployment_id,
            )
            with _REGISTRY_LEASE_COORDINATION_LOCK:
                store.acquire_reference(
                    repository,
                    tag,
                    owner,
                    digest=digest,
                )
        except (OSError, TypeError, ValueError) as exc:
            raise RegistryImageReferenceUnavailable(
                "snapshot registry reference could not be persisted"
            ) from exc

    def _release_registry_snapshot_reference(
        self,
        route: SandboxRoute,
        *,
        keep_route: SandboxRoute | None = None,
    ) -> None:
        store = self.registry_usage_store
        if store is not None:
            release_registry_snapshot_reference(
                store,
                route,
                deployment_id=self.deployment_id,
                keep_route=keep_route,
            )

    def _write_registry_lease_unavailable(
        self,
        _exc: RegistryImageReferenceUnavailable,
    ) -> None:
        self._write_json(
            {
                "error": "registry image-use state is unavailable",
                "retryable": True,
            },
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            headers={"Retry-After": "2"},
        )

    def _write_create_in_progress_response(self, sandbox_id: str) -> None:
        self._write_json(
            {
                "error": "sandbox creation is already in progress",
                "retryable": True,
                "sandbox_id": sandbox_id,
            },
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            headers={
                "Retry-After": str(SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS),
                "X-UCloud-Sandbox-Retryable": "true",
            },
        )

    def _sandbox_record_on_node(
        self,
        node_url: str,
        sandbox_id: str,
    ) -> dict[str, Any] | None:
        response = self._proxy_request(
            node_url,
            "/v1/sandboxes",
            method="GET",
            timeout_seconds=NODE_RECOVERY_PROXY_TIMEOUT_SECONDS,
        )
        if response.status >= 400:
            return None
        raw_sandboxes = response.json().get("sandboxes")
        if not isinstance(raw_sandboxes, list):
            return None
        for record in raw_sandboxes:
            if not isinstance(record, dict):
                continue
            spec = record.get("spec")
            existing_id = spec.get("id") if isinstance(spec, dict) else None
            if existing_id == sandbox_id:
                return record
        return None

    def _route_image_build(self) -> None:
        try:
            body = self._read_raw_body(max_bytes=self.max_json_body_bytes)
            raw = json.loads(body.decode("utf-8")) if body else None
            if not isinstance(raw, dict):
                raise ValueError("image build payload must be a JSON object")
            context_reference = uploaded_build_context_reference(
                raw, self.build_context_store
            )
            spec = ImageBuildSpec.from_dict(raw)
            push = bool(raw.get("push", False))
            build_registry_url = self.registry_worker_url or ""
            if not spec.tag.strip():
                if not str(raw.get("id") or "").strip():
                    raise ValueError("gateway-managed image builds require an image id")
                spec = replace(
                    spec,
                    tag=_managed_registry_build_tag(spec.id, build_registry_url),
                )
                push = True
            elif self.registry_url and self.registry_worker_url:
                spec = replace(
                    spec,
                    tag=_managed_registry_worker_reference(
                        spec.tag,
                        self.registry_url,
                        self.registry_worker_url,
                    ),
                )
            spec.validate()
            raw = dict(raw)
            raw["tag"] = spec.tag
            raw["push"] = push
            body = json.dumps(raw, separators=(",", ":")).encode("utf-8")
            with self.telemetry.span(
                "gateway.image_build",
                attributes={
                    "image.id": spec.id,
                    "container.image.name": spec.tag,
                    "image.push": push,
                },
            ) as root:
                with self.telemetry.span(
                    "gateway.image_build_select_builder",
                ) as span:
                    heartbeat = self._select_builder_node()
                    span.set_attribute(
                        "selected_node_id", heartbeat.node_id if heartbeat else ""
                    )
                    span.set_attribute(
                        "selected_job_id", heartbeat.job_id if heartbeat else ""
                    )
                if heartbeat is None:
                    self.routing_store.upsert_pending_image_build(spec.id, spec.tag)
                    pending_builds = self.routing_store.pending_image_build_count()
                    root.status = "error"
                    root.set_attribute("outcome", "queued_no_builder")
                    root.set_attribute("pending_image_builds", pending_builds)
                    self._write_json(
                        {
                            "error": "no ready builder node is available",
                            "pending_image_builds": pending_builds,
                        },
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                    return
                with self.telemetry.span(
                    "gateway.image_build_enqueue",
                    attributes={"node.id": heartbeat.node_id},
                ):
                    self.routing_store.upsert_pending_image_build(spec.id, spec.tag)
                with self.telemetry.span(
                    "gateway.image_build_context_sync",
                    attributes={"node.id": heartbeat.node_id},
                ) as span:
                    context_response = self._ensure_node_build_context(
                        heartbeat.node_url or "", context_reference
                    )
                    span.set_attribute("status_code", int(context_response.status))
                    context_payload = context_response.json()
                    if "deduplicated" in context_payload:
                        span.set_attribute(
                            "deduplicated",
                            bool(context_payload["deduplicated"]),
                        )
                if not 200 <= context_response.status < 300:
                    root.status = "error"
                    root.set_attribute("outcome", "context_proxy_failed")
                    root.set_attribute("status_code", int(context_response.status))
                    self._send_proxied_response(context_response)
                    return
                self._protect_registry_image_build_target(
                    spec,
                    push=push,
                )
                with self.telemetry.span(
                    "gateway.image_build_proxy_builder",
                    attributes={"node.id": heartbeat.node_id},
                ) as span:
                    response = self._proxy_request(
                        heartbeat.node_url or "",
                        "/v1/images/build",
                        method="POST",
                        body=body,
                        timeout_seconds=IMAGE_BUILD_PROXY_TIMEOUT_SECONDS,
                    )
                    span.set_attribute("status_code", int(response.status))
                    response_payload = response.json()
                    raw_image = response_payload.get("image")
                    if isinstance(
                        raw_image, dict
                    ) and _image_record_available_to_sandboxes(raw_image):
                        raw_image = self._image_record_with_registry_digest(raw_image)
                        response_payload["image"] = raw_image
                        raw_build = response_payload.get("build")
                        if isinstance(raw_build, dict):
                            raw_build["image"] = raw_image
                        response.body = json.dumps(response_payload).encode("utf-8")
                    node_timings = response_payload.get("timings")
                    if isinstance(node_timings, dict):
                        span.add_event("node.timings", node_timings)
                accepted_build_response = 200 <= response.status < 300
                terminal_build_response = _image_build_response_terminal(
                    response_payload
                ) or (
                    not 200 <= response.status < 300
                    and response.status < 500
                    and response.status not in {408, 425, 429}
                )
                if accepted_build_response or terminal_build_response:
                    self.routing_store.clear_pending_image_build(spec.id)
                if 200 <= response.status < 300:
                    raw_image = response_payload.get("image")
                    if isinstance(
                        raw_image, dict
                    ) and _image_record_available_to_sandboxes(raw_image):
                        try:
                            self.image_manager.store.upsert(
                                ImageRecord.from_dict(raw_image)
                            )
                        except ValueError:
                            pass
                        self._invalidate_image_inventory_cache()
                if 200 <= response.status < 300:
                    root.set_attribute("outcome", "builder_completed")
                    root.set_attribute("node_id", heartbeat.node_id)
                else:
                    root.status = "error"
                    root.set_attribute("outcome", "builder_failed")
                    root.set_attribute("status_code", int(response.status))
                self._send_proxied_response(response)
                return
        except (json.JSONDecodeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except RegistryImageReferenceUnavailable as exc:
            self._write_registry_lease_unavailable(exc)
            return
        except RuntimeError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

    def _ensure_node_build_context(
        self,
        node_url: str,
        reference: tuple[str, int],
    ) -> ProxiedResponse:
        digest, size = reference
        path = f"/v1/image-contexts/{quote(digest, safe=':')}"
        probe = self._proxy_request(node_url, path, method="GET")
        if 200 <= probe.status < 300:
            payload = probe.json()
            if payload.get("digest") == digest and payload.get("size") == size:
                return probe
        elif probe.status != HTTPStatus.NOT_FOUND:
            return probe

        try:
            with self.build_context_store.open(digest) as archive:
                return self._proxy_request(
                    node_url,
                    path,
                    method="PUT",
                    body=archive,
                    extra_headers={
                        "Content-Type": "application/gzip",
                        "Content-Length": str(size),
                    },
                    timeout_seconds=IMAGE_BUILD_PROXY_TIMEOUT_SECONDS,
                )
        except FileNotFoundError:
            return ProxiedResponse(
                HTTPStatus.BAD_REQUEST,
                {"Content-Type": "application/json"},
                json.dumps(
                    {"error": f"build context {digest!r} has not been uploaded"}
                ).encode("utf-8"),
            )

    def _route_image_pull(self) -> None:
        try:
            body = self._read_raw_body(max_bytes=self.max_json_body_bytes)
            raw = json.loads(body.decode("utf-8")) if body else None
            if not isinstance(raw, dict):
                raise ValueError("image pull payload must be a JSON object")
            unsupported = sorted(
                set(raw)
                - {
                    "count",
                    "cpus",
                    "disk_mb",
                    "id",
                    "image",
                    "memory_mb",
                    "sandbox_nodes_only",
                }
            )
            if unsupported:
                raise ValueError(
                    "unsupported image pull fields: " + ", ".join(unsupported)
                )
            image = str(raw.get("image") or "")
            if not image.strip():
                raise ValueError("image is required.")
            count = _strict_positive_integer(raw.get("count", 1), "count")
            resources = _prepared_resources_from_payload(raw)
            sandbox_nodes_only = raw.get("sandbox_nodes_only", True)
            if not isinstance(sandbox_nodes_only, bool):
                raise ValueError("sandbox_nodes_only must be a boolean.")
            if count <= 0:
                raise ValueError("count must be positive.")
        except (json.JSONDecodeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        image, image_error = self._resolve_request_image_reference(image)
        if image_error is not None:
            self._write_image_resolution_error(image_error)
            return

        self._ensure_registry_image_lease(
            image,
            _registry_operation_lease_owner(
                "image-pull",
                {
                    "image": image,
                    "image_id": str(raw.get("id") or "").strip(),
                    "count": count,
                    "resources": resources.to_dict(),
                    "sandbox_nodes_only": sandbox_nodes_only,
                },
            ),
            touch=True,
        )
        result = self._warm_image_on_ready_nodes(
            image,
            count=count,
            resources=resources,
            sandbox_nodes_only=sandbox_nodes_only,
            image_id=str(raw.get("id") or "").strip(),
        )
        if result["ready"] <= 0:
            error_message = (
                "image pull failed on ready image-cache nodes"
                if result["failed"]
                else "no ready image-cache node is available"
            )
            self._write_json(
                {
                    "error": error_message,
                    "image": image,
                    "result": result,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self._write_json(result, status=HTTPStatus.OK)

    def _route_sandbox_request(self, sandbox_id: str, path: str) -> None:
        route = self.routing_store.get_sandbox(sandbox_id)
        if route is None:
            if self.command == "DELETE":
                pending_before = self.routing_store.load().pending.get(sandbox_id)
                self.routing_store.delete_sandbox(sandbox_id)
                record_sandbox_pending_deleted(
                    self.metrics_store,
                    sandbox_id=sandbox_id,
                    pending=pending_before,
                )
                self._write_json({"ok": True, "deleted": False})
                return
            self._write_json(
                {"error": "sandbox route not found"}, status=HTTPStatus.NOT_FOUND
            )
            return

        if self.command != "DELETE" and route.delete_operation_id:
            self._write_json(
                {
                    "error": "sandbox deletion is in progress",
                    "error_code": "sandbox_delete_pending",
                    "retryable": False,
                },
                status=HTTPStatus.CONFLICT,
            )
            return

        sandbox_http_route = match_sandbox_http_route(self.command, path)
        request_wakes = bool(
            sandbox_http_route is not None and sandbox_http_route.wakes
        )

        # Placement is durable before provisioning begins. Do not forward
        # tool traffic into a registration that is still planned, quota-ready,
        # or preparing its rootfs. Reconcile a completed node record first;
        # otherwise keep the caller on the retryable create boundary.
        if self.command != "DELETE":
            route = self._reconcile_routable_sandbox(route)
            if route is None:
                return

        try:
            body = (
                self._read_raw_body(max_bytes=DEFAULT_MAX_PROXY_BODY_BYTES)
                if self.command in {"POST", "PUT", "PATCH"}
                else None
            )
        except ValueError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        transport_reset = False
        lifecycle_payload: dict[str, Any] = {}
        lifecycle_action = (
            sandbox_http_route.action
            if sandbox_http_route is not None
            and sandbox_http_route.action in {"park", "wake"}
            else ""
        )
        if lifecycle_action:
            parsed_lifecycle = self._parse_lifecycle_request(
                route,
                lifecycle_action,
                body,
            )
            if parsed_lifecycle is None:
                return
            lifecycle_payload = parsed_lifecycle

        if (
            sandbox_http_route is not None
            and sandbox_http_route.action == "job_status"
            and (route.state or "unknown").lower()
            in {"parking", "parked", "moving", "restoring", "waking"}
        ):
            self._serve_cached_job_status(
                route,
                sandbox_http_route,
                missing_status=HTTPStatus.NOT_FOUND,
            )
            return

        self._prepare_program_lifecycle(route, lifecycle_action, lifecycle_payload)

        implicit_wake = bool(
            not lifecycle_action
            and request_wakes
            and (route.state or "unknown").lower() in {"parked", "waking"}
        )
        if (route.state or "unknown").lower() == "parked" and request_wakes:
            placement = self._prepare_wake_placement(route)
            if placement is None:
                return
            route, transport_reset = placement

        if lifecycle_action == "wake" and lifecycle_payload.get("request_id"):
            self._record_program_request_transition(
                route,
                lifecycle_payload,
                state="waking",
            )

        if self.command == "DELETE":
            prepared_delete = self._prepare_delete_route(route)
            if prepared_delete is None:
                return
            route = prepared_delete

        if not self._route_worker_is_fresh(route):
            if self._serve_cached_job_status(route, sandbox_http_route):
                return
            self._write_route_worker_unreachable(route)
            return

        if implicit_wake:
            completed_wake = self._perform_implicit_wake(route)
            if completed_wake is None:
                return
            route = completed_wake

        extra_headers = (
            {
                SANDBOX_GENERATION_HEADER: str(route.generation),
                SANDBOX_OPERATION_ID_HEADER: route.delete_operation_id,
            }
            if self.command == "DELETE"
            else None
        )
        # Only downloads need a streamed response. Upload acknowledgements are
        # small JSON responses and follow the same buffered proxy path as every
        # other mutating sandbox request, which carries the already-read body.
        if (
            sandbox_http_route is not None
            and sandbox_http_route.action == "files"
            and self.command == "GET"
        ):
            self._stream_proxy_request(
                route.node_url,
                self.path,
                method=self.command,
                extra_headers=extra_headers,
            )
            return
        proxy_body = body
        if lifecycle_action:
            proxy_body = self._lifecycle_proxy_body(
                route,
                lifecycle_action,
                lifecycle_payload,
            )
            if proxy_body is None:
                return
        response = self._proxy_request(
            route.node_url,
            self.path,
            method=self.command,
            body=proxy_body,
            extra_headers=extra_headers,
        )
        if self._serve_cached_transition_job_status(
            route,
            sandbox_http_route,
            response,
        ):
            return
        if not self._record_successful_sandbox_proxy_state(
            route,
            sandbox_http_route,
            response,
        ):
            return
        if lifecycle_action == "park" and response.status == HTTPStatus.ACCEPTED:
            self._send_proxied_response(response)
            return
        if lifecycle_action:
            lifecycle_route = self._handle_lifecycle_proxy_response(
                route,
                lifecycle_action,
                lifecycle_payload,
                response,
            )
            if lifecycle_route is None:
                return
            route = lifecycle_route
        if self.command == "DELETE" and 200 <= response.status < 300:
            if not self._commit_successful_worker_delete(route, response):
                return
        response_headers: dict[str, str] | None = None
        if lifecycle_action:
            response_headers = {
                SANDBOX_TRANSPORT_EPOCH_HEADER: _sandbox_transport_epoch(
                    route,
                    self.routing_store.sandbox_migrations(active_only=False),
                )
            }
            if transport_reset:
                response_headers[SANDBOX_TRANSPORT_RESET_HEADER] = "true"
        self._send_proxied_response(response, extra_headers=response_headers)

    def _reconcile_routable_sandbox(
        self,
        route: SandboxRoute,
    ) -> SandboxRoute | None:
        """Promote a completed create before forwarding sandbox traffic."""

        if route.state.lower() not in {
            "creating",
            "unknown",
            "planned",
            "quota_ready",
            "rootfs_ready",
        }:
            return route
        try:
            routed_spec = SandboxSpec.from_dict(route.spec)
        except (TypeError, ValueError):
            routed_spec = None
        if not self._route_worker_is_fresh(route):
            self._write_create_in_progress_response(route.sandbox_id)
            return None
        record = self._sandbox_record_on_node(route.node_url, route.sandbox_id)
        if (
            routed_spec is None
            or record is None
            or not _sandbox_record_matches_route(record, route, routed_spec)
            or not _sandbox_record_is_ready(record)
        ):
            self._write_create_in_progress_response(route.sandbox_id)
            return None
        return self.routing_store.upsert_sandbox(
            _route_with_sandbox_record(route, record)
        )

    def _prepare_program_lifecycle(
        self,
        route: SandboxRoute,
        action: str,
        payload: dict[str, Any],
    ) -> None:
        request_id = str(payload.get("request_id") or "").strip()
        if not action or not request_id:
            return
        program, became_ready = self._record_program_request_transition(
            route,
            payload,
            state=("model_wait" if action == "park" else "ready_to_wake"),
        )
        if action != "wake" or program is None or not became_ready:
            return
        self._record_program_wake_shadow_plan(
            payload,
            program,
            self._placement_routes(),
        )

    def _prepare_wake_placement(
        self,
        route: SandboxRoute,
    ) -> tuple[SandboxRoute, bool] | None:
        current = self.routing_store.get_sandbox_readonly(route.sandbox_id)
        if current is None:
            self._write_json(
                {"error": "sandbox route not found"},
                status=HTTPStatus.NOT_FOUND,
            )
            return None
        if (current.state or "unknown").lower() != "parked":
            return current, False
        previous_owner = current.node_id, current.job_id, current.node_url
        placed = self._ensure_parked_sandbox_wake_placement(current)
        if placed is None:
            return None
        new_owner = placed.node_id, placed.job_id, placed.node_url
        return placed, previous_owner != new_owner

    def _lifecycle_proxy_body(
        self,
        route: SandboxRoute,
        action: str,
        payload: dict[str, Any],
    ) -> bytes | None:
        node_payload: dict[str, Any] = {
            "operation_id": str(payload["operation_id"]).strip(),
        }
        if action == "wake":
            node_payload["generation"] = route.generation
        elif "background" in payload:
            if not isinstance(payload["background"], bool):
                self._write_json(
                    {"error": "park background must be a boolean"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return None
            node_payload["background"] = payload["background"]
        return json.dumps(node_payload, separators=(",", ":")).encode("utf-8")

    def _parse_lifecycle_request(
        self,
        route: SandboxRoute,
        action: str,
        body: bytes | None,
    ) -> dict[str, Any] | None:
        """Parse and authorize one explicit lifecycle request."""

        try:
            payload = json.loads((body or b"{}").decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("sandbox lifecycle payload must be an object")
            request_id = str(payload.get("request_id") or "").strip()
            rollout_id = str(payload.get("rollout_id") or "").strip()
            if bool(request_id) != bool(rollout_id):
                raise ValueError(
                    "program lifecycle requires both request_id and rollout_id"
                )
            generation = payload.get("generation")
            operation_id = str(payload.get("operation_id") or "").strip()
            if not operation_id:
                raise ValueError("sandbox lifecycle operation_id is required")
            if action == "wake" and generation is None:
                raise ValueError("wake generation is required")
            if generation is not None and int(generation) != route.generation:
                self._write_json(
                    {
                        "error": (
                            f"{action} generation does not own the current "
                            "sandbox route"
                        ),
                        "retryable": False,
                    },
                    status=HTTPStatus.CONFLICT,
                )
                return None
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return None
        if request_id and not _sandbox_supports_managed_lifecycle(route.spec):
            self._write_json(
                {
                    "error": (
                        f"request-bound {action} requires a parkable "
                        "managed_process sandbox"
                    ),
                    "retryable": False,
                },
                status=HTTPStatus.CONFLICT,
            )
            return None
        return payload

    def _prepare_delete_route(self, route: SandboxRoute) -> SandboxRoute | None:
        """Resolve detach/migration state before proxying a durable delete."""

        sandbox_id = route.sandbox_id
        route = self.routing_store.prepare_sandbox_delete(sandbox_id) or route
        if route.worker_state != "detached":
            migration_error = self._resolve_sandbox_migrations_for_delete(sandbox_id)
            if migration_error:
                self._write_json(
                    {"error": migration_error, "retryable": True},
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers={"X-UCloud-Sandbox-Retryable": "true"},
                )
                return None
        current = self.routing_store.get_sandbox_readonly(sandbox_id)
        if current is None:
            self._write_json({"deleted": None})
            return None
        route = current
        if route.worker_state == "detaching":
            detached, error_message = self._finish_sandbox_detach(route)
            if detached is None:
                self._write_json(
                    {
                        "error": error_message or "worker detach is incomplete",
                        "retryable": True,
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers={"X-UCloud-Sandbox-Retryable": "true"},
                )
                return None
            route = detached
        if route.worker_state != "detached":
            return route
        route = self.routing_store.prepare_sandbox_delete(sandbox_id) or route
        removed = self.routing_store.delete_sandbox_if_current(
            sandbox_id,
            generation=route.generation,
            delete_operation_id=route.delete_operation_id,
        )
        if removed is not None:
            self._release_registry_route_reference(removed)
        self._write_json(
            {"deleted": removed.to_dict() if removed is not None else None}
        )
        return None

    def _serve_cached_job_status(
        self,
        route: SandboxRoute,
        http_route: SandboxHttpRoute | None,
        *,
        missing_status: HTTPStatus | None = None,
    ) -> bool:
        if http_route is None or http_route.action != "job_status":
            return False
        cached = self.routing_store.get_managed_process(
            route.sandbox_id,
            http_route.job_id,
            sandbox_generation=route.generation,
        )
        if cached is None:
            if missing_status is None:
                return False
            self._write_json(
                {"error": "managed process state is not available"},
                status=missing_status,
            )
            return True
        self._write_json({"job": cached.to_dict()})
        return True

    def _perform_implicit_wake(self, route: SandboxRoute) -> SandboxRoute | None:
        response = self._proxy_request(
            route.node_url,
            f"/v1/sandboxes/{quote(route.sandbox_id, safe='')}/wake",
            method="POST",
            body=json.dumps(
                {
                    "generation": route.generation,
                    "operation_id": f"activity-wake:{uuid4().hex}",
                },
                separators=(",", ":"),
            ).encode("utf-8"),
            extra_headers={"Content-Type": "application/json"},
        )
        if response.status < 300:
            return self._commit_successful_wake(route, response)
        if str(response.json().get("lifecycle_state") or "").lower() == "parked":
            self.routing_store.set_sandbox_state_if_current(
                route,
                expected_states={"waking"},
                state="parked",
            )
        self._send_proxied_response(response)
        return None

    def _serve_cached_transition_job_status(
        self,
        route: SandboxRoute,
        http_route: SandboxHttpRoute | None,
        response: ProxiedResponse,
    ) -> bool:
        return bool(
            http_route is not None
            and http_route.action == "job_status"
            and response.status in {HTTPStatus.BAD_REQUEST, HTTPStatus.CONFLICT}
            and "lifecycle transition is in progress"
            in str(response.json().get("error") or "").lower()
            and self._serve_cached_job_status(route, http_route)
        )

    def _record_successful_sandbox_proxy_state(
        self,
        route: SandboxRoute,
        http_route: SandboxHttpRoute | None,
        response: ProxiedResponse,
    ) -> bool:
        """Project successful node responses into the gateway-owned ledgers."""

        if http_route is None or not 200 <= response.status < 300:
            return True
        if http_route.action in {"job_create", "job_status", "job_signal"}:
            try:
                managed_record = ManagedProcessRecord.from_dict(
                    response.json().get("job")
                )
                if http_route.job_id and managed_record.job_id != http_route.job_id:
                    raise ValueError("node returned another managed process")
                self.routing_store.upsert_managed_process(route, managed_record)
            except (SandboxRouteConflictError, TypeError, ValueError) as exc:
                self._write_json(
                    {"error": f"invalid managed process state from node: {exc}"},
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return False
        if http_route.action == "exec":
            session = response.json().get("session")
            session_id = session.get("id") if isinstance(session, dict) else None
            if isinstance(session_id, str) and session_id:
                self.routing_store.upsert_exec(
                    ExecRoute(
                        session_id=session_id,
                        sandbox_id=route.sandbox_id,
                        node_id=route.node_id,
                        job_id=route.job_id,
                        node_url=route.node_url,
                    )
                )
        return True

    def _handle_lifecycle_proxy_response(
        self,
        route: SandboxRoute,
        action: str,
        payload: dict[str, Any],
        response: ProxiedResponse,
    ) -> SandboxRoute | None:
        if response.status >= 300:
            if action == "wake":
                try:
                    failed_state = str(
                        response.json().get("lifecycle_state") or ""
                    ).lower()
                except (TypeError, ValueError, json.JSONDecodeError):
                    failed_state = ""
                if failed_state == "parked":
                    rolled_back = self.routing_store.set_sandbox_state_if_current(
                        route,
                        expected_states={"waking"},
                        state="parked",
                    )
                    if rolled_back is not None:
                        route = rolled_back
            self._record_program_request_transition(
                route,
                payload,
                state=("waking" if action == "wake" else "model_wait"),
                last_error=_lifecycle_proxy_error(response),
            )
            return route
        if not 200 <= response.status < 300:
            return route
        updated = (
            self._commit_successful_wake(route, response)
            if action == "wake"
            else self._commit_successful_park(route, response)
        )
        if updated is None:
            return None
        self._record_completed_program_lifecycle(updated, action, payload)
        return updated

    def _commit_successful_park(
        self,
        route: SandboxRoute,
        response: ProxiedResponse,
    ) -> SandboxRoute | None:
        """Commit one fenced park and compensate any uncommitted reference."""

        lifecycle_response = response.json()
        try:
            node_epoch, activity_epoch = self._lifecycle_response_fence(route, response)
        except ValueError as exc:
            self._write_json(
                {"error": f"invalid node lifecycle response: {exc}"},
                status=HTTPStatus.BAD_GATEWAY,
            )
            return None
        candidate: SandboxRoute | None = None
        supplied_schema = str(lifecycle_response.get("storage_schema") or "")
        supplied_manifest = str(
            lifecycle_response.get("snapshot_manifest_digest") or ""
        )
        if supplied_schema or supplied_manifest:
            try:
                candidate = _route_with_snapshot_payload(route, lifecycle_response)
            except ValueError:
                self._write_json(
                    {"error": "node returned invalid durable park metadata"},
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return None
            try:
                self._ensure_registry_snapshot_reference(
                    candidate,
                    repository=candidate.snapshot_repository,
                    tag=candidate.snapshot_tag,
                    digest=candidate.snapshot_manifest_digest,
                )
            except RegistryImageReferenceUnavailable as exc:
                current = self.routing_store.get_sandbox_readonly(route.sandbox_id)
                self._release_registry_snapshot_reference(
                    candidate,
                    keep_route=current,
                )
                self._write_registry_lease_unavailable(exc)
                return None
        try:
            updated = self.routing_store.set_sandbox_state_if_current(
                route,
                expected_states={"running", "waking", "parked"},
                state="parked",
                node_epoch=node_epoch,
                activity_epoch=activity_epoch,
                storage_schema=(candidate.storage_schema if candidate else None),
                snapshot_manifest_digest=(
                    candidate.snapshot_manifest_digest if candidate else None
                ),
                snapshot_repository=(candidate.snapshot_repository if candidate else None),
                snapshot_tag=(candidate.snapshot_tag if candidate else None),
                storage_snapshot=(dict(candidate.storage_snapshot) if candidate else None),
            )
        except BaseException:
            self._release_failed_lifecycle_commit(route, candidate)
            raise
        if updated is None:
            self._release_failed_lifecycle_commit(route, candidate)
            self._write_json(
                {
                    "error": "sandbox route changed while committing lifecycle state",
                    "retryable": True,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                headers={"X-UCloud-Sandbox-Retryable": "true"},
            )
            return None
        self._release_registry_snapshot_reference(route, keep_route=updated)
        return updated

    def _release_failed_lifecycle_commit(
        self,
        route: SandboxRoute,
        candidate: SandboxRoute | None,
    ) -> None:
        current = self.routing_store.get_sandbox_readonly(route.sandbox_id)
        if candidate is not None:
            self._release_registry_snapshot_reference(candidate, keep_route=current)
        self._release_registry_snapshot_reference(route, keep_route=current)

    def _record_completed_program_lifecycle(
        self,
        route: SandboxRoute,
        action: str,
        payload: dict[str, Any],
    ) -> None:
        if not payload.get("request_id"):
            return
        if action == "park":
            self._record_program_request_transition(
                route,
                payload,
                state="model_wait",
                parked_at=utc_now().isoformat(),
                clear_error=True,
            )
            return
        _program, changed = self._record_program_request_transition(
            route,
            payload,
            state="acting",
            clear_error=True,
        )
        if not changed:
            return
        self.metrics_store.append(
            "program_wake_actual",
            {
                "request_id": str(payload.get("request_id") or "").strip(),
                "rollout_id": str(payload.get("rollout_id") or "").strip(),
                "sandbox_id": route.sandbox_id,
                "sandbox_generation": route.generation,
                "node_id": route.node_id,
                "job_id": route.job_id,
            },
        )

    def _commit_successful_worker_delete(
        self,
        route: SandboxRoute,
        response: ProxiedResponse,
    ) -> bool:
        deleted = response.json().get("deleted")
        response_generation = _record_generation(deleted)
        if (
            isinstance(deleted, dict)
            and response_generation is not None
            and response_generation != route.generation
        ):
            self._write_json(
                {
                    "error": "node delete response confirmed a different generation",
                    "retryable": True,
                },
                status=HTTPStatus.BAD_GATEWAY,
            )
            return False
        removed = self.routing_store.delete_sandbox_if_current(
            route.sandbox_id,
            generation=route.generation,
            delete_operation_id=route.delete_operation_id,
        )
        if removed is not None:
            self._release_registry_route_reference(removed)
        return True

    def _record_program_request_transition(
        self,
        route: SandboxRoute,
        lifecycle_payload: dict[str, Any],
        *,
        state: str,
        parked_at: str | None = None,
        last_error: str = "",
        clear_error: bool = False,
    ) -> tuple[ProgramRequestState | None, bool]:
        request_id = str(lifecycle_payload.get("request_id") or "").strip()
        rollout_id = str(lifecycle_payload.get("rollout_id") or "").strip()
        if not request_id or not rollout_id:
            return None, False
        accepted_at = ""
        try:
            raw_created_at = float(lifecycle_payload.get("request_created_at"))
            if math.isfinite(raw_created_at) and raw_created_at >= 0:
                accepted_at = datetime.fromtimestamp(
                    raw_created_at,
                    tz=timezone.utc,
                ).isoformat()
        except (TypeError, ValueError, OverflowError, OSError):
            pass
        try:
            program, changed = (
                self.routing_store.upsert_program_request_transition_with_change(
                    route,
                    request_id=request_id,
                    rollout_id=rollout_id,
                    state=state,
                    accepted_at=accepted_at or None,
                    parked_at=parked_at,
                    last_error=last_error,
                    clear_error=clear_error,
                )
            )
        except (OSError, sqlite3.Error, ValueError, SandboxRouteConflictError) as exc:
            self.metrics_store.append(
                "program_state_projection_error",
                {
                    "request_id": request_id,
                    "rollout_id": rollout_id,
                    "sandbox_id": route.sandbox_id,
                    "sandbox_generation": route.generation,
                    "state": state,
                    "error": str(exc),
                },
            )
            return None, False
        if changed:
            self.metrics_store.append(
                "program_state_transition",
                program.to_dict(),
            )
        return program, changed

    def _record_program_wake_shadow_plan(
        self,
        lifecycle_payload: dict[str, Any],
        program: ProgramRequestState,
        routes: list[PlacementRecord],
    ) -> None:
        """Observe every response-ready event without changing wake behavior."""

        request_id = str(lifecycle_payload.get("request_id") or "").strip()
        if not request_id:
            return
        try:
            plan = plan_shadow_wake_queue(
                [program],
                [route for route in routes if isinstance(route, SandboxRoute)],
                [
                    WakeNodeCandidate(
                        node_id=heartbeat.node_id,
                        job_id=heartbeat.job_id,
                        available=_node_available_resources(heartbeat, routes),
                        total=heartbeat.total_resources,
                        pressure=node_pressure_score(heartbeat),
                        heartbeat=heartbeat,
                    )
                    for heartbeat in self._ready_sandbox_heartbeats()
                    if heartbeat.admission_open
                    and agent_version_is_schedulable(heartbeat.agent_version)
                ],
            )
            placements = plan.get("placements")
            unplaced = plan.get("unplaced")
            decision = next(
                (
                    item
                    for item in (
                        [
                            *(placements if isinstance(placements, list) else []),
                            *(unplaced if isinstance(unplaced, list) else []),
                        ]
                    )
                    if isinstance(item, dict) and item.get("request_id") == request_id
                ),
                None,
            )
            self.metrics_store.append(
                "program_wake_shadow_plan",
                {
                    "request_id": request_id,
                    "rollout_id": str(
                        lifecycle_payload.get("rollout_id") or ""
                    ).strip(),
                    "queued": plan.get("queued", 0),
                    "placed": plan.get("placed", 0),
                    "unplaced_count": plan.get("unplaced_count", 0),
                    "decision": decision,
                },
            )
        except (OSError, sqlite3.Error, ValueError) as exc:
            self.metrics_store.append(
                "program_wake_shadow_plan_error",
                {
                    "request_id": request_id,
                    "error": str(exc),
                },
            )

    def _ensure_parked_sandbox_wake_placement(
        self,
        route: SandboxRoute,
    ) -> SandboxRoute | None:
        """Reserve wake placement briefly, then relocate without global locks."""

        if route.worker_state == "detaching":
            detached, error_message = self._finish_sandbox_detach(route)
            if detached is None:
                self._write_json(
                    {
                        "error": error_message or "worker detach is incomplete",
                        "retryable": True,
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers={
                        "Retry-After": str(
                            SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS
                        ),
                        "X-UCloud-Sandbox-Retryable": "true",
                    },
                )
                return None
            route = detached

        with _GATEWAY_SCHEDULING_LOCK, _gateway_placement_lock(self.routing_store.path):
            current = self.routing_store.get_sandbox_readonly(route.sandbox_id)
            if current is None:
                self._write_json(
                    {"error": "sandbox route not found"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return None
            if (current.state or "unknown").lower() in {"waking", "running"}:
                return current
            if (current.state or "unknown").lower() != "parked":
                self._write_json(
                    {
                        "error": "sandbox route changed during wake admission",
                        "retryable": True,
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers={
                        "Retry-After": str(
                            SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS
                        ),
                        "X-UCloud-Sandbox-Retryable": "true",
                    },
                )
                return None
            route = current
            routes = self._placement_routes()
            source_heartbeat = self._heartbeat_for_route(
                job_id=route.job_id,
            )
            active_request = ResourceQuantity(
                vcpu=route.resources.vcpu,
                memory_mb=route.resources.memory_mb,
            )
            ready_source_ids = {
                heartbeat.node_id for heartbeat in self._ready_sandbox_heartbeats()
            }
            if (
                route.worker_state == "attached"
                and source_heartbeat is not None
                and source_heartbeat.node_id in ready_source_ids
                and _node_can_fit_available(
                    source_heartbeat,
                    active_request,
                    _node_available_resources(source_heartbeat, routes),
                )
            ):
                self.routing_store.clear_pending(
                    _wake_pending_demand_id(route.sandbox_id)
                )
                return self._mark_sandbox_waking(route)

            if route.worker_state == "attached" and not is_portable_parked_route(route):
                # Background park publication is deliberately asynchronous.
                # Do not turn a transiently busy local source into a blocking
                # migration/EnsurePublished call. The node heartbeat will
                # attach the portable descriptor as soon as publication
                # completes; until then SDK retries are the safe wake fence.
                _pending, demand = self.routing_store.upsert_pending_with_demand(
                    _wake_pending_demand_id(route.sandbox_id),
                    route.resources,
                    failure_reason="wake_snapshot_publication_pending",
                )
                self._write_json(
                    {
                        "error": "parked snapshot publication is still in progress",
                        "error_code": "snapshot_publication_pending",
                        "retryable": True,
                        "pending_resources": demand.pending_resources.to_dict(),
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers={
                        "Retry-After": "1",
                        "X-UCloud-Sandbox-Retryable": "true",
                    },
                )
                return None

            active_migration = next(
                (
                    migration
                    for migration in self.routing_store.sandbox_migrations(
                        active_only=True
                    )
                    if migration.sandbox_id == route.sandbox_id
                ),
                None,
            )
            if active_migration is None:
                destination = self._select_migration_destination(
                    route,
                    requested_node_id="",
                    require_active_resources=True,
                )
                if destination is None:
                    _pending, demand = self.routing_store.upsert_pending_with_demand(
                        _wake_pending_demand_id(route.sandbox_id),
                        route.resources,
                        failure_reason="wake_destination_unavailable",
                    )
                    self._write_json(
                        {
                            "error": (
                                "parked sandbox has no node with active CPU, memory, "
                                "and disk capacity"
                            ),
                            "retryable": True,
                            "pending_resources": demand.pending_resources.to_dict(),
                        },
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        headers={
                            "Retry-After": str(
                                SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS
                            ),
                            "X-UCloud-Sandbox-Retryable": "true",
                        },
                    )
                    return None
                active_migration = self.routing_store.begin_sandbox_migration(
                    route,
                    migration_id=f"wake-{uuid4().hex}",
                    destination_node_id=destination.node_id,
                    destination_job_id=destination.job_id,
                    destination_node_url=destination.node_url or "",
                )

        # The planned migration now reserves the destination shape in normal
        # placement. Clear capacity demand and release both placement locks
        # before image pulls, export, transfer, import, or activation.
        self.routing_store.clear_pending(_wake_pending_demand_id(route.sandbox_id))
        migration = self._prepare_and_advance_sandbox_migration(
            active_migration,
            wake_on_complete=True,
        )
        if migration is None:
            return None
        if migration.phase != "complete":
            self._write_json(
                {
                    "error": (
                        migration.error or "parked sandbox relocation is incomplete"
                    ),
                    "migration": migration.to_dict(),
                    "retryable": True,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                headers={
                    "Retry-After": str(SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS),
                    "X-UCloud-Sandbox-Retryable": "true",
                },
            )
            return None
        destination_route = self.routing_store.get_sandbox_readonly(route.sandbox_id)
        return (
            self._mark_sandbox_waking(destination_route)
            if destination_route is not None
            else None
        )

    def _mark_sandbox_waking(
        self,
        route: SandboxRoute,
    ) -> SandboxRoute | None:
        waking = self.routing_store.set_sandbox_state_if_current(
            route,
            expected_states={"parked"},
            state="waking",
        )
        if waking is not None:
            return waking
        current = self.routing_store.get_sandbox_readonly(route.sandbox_id)
        if current is not None and (current.state or "unknown").lower() in {
            "waking",
            "running",
        }:
            return current
        self._write_json(
            {
                "error": "sandbox route changed during wake admission",
                "retryable": True,
            },
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            headers={
                "Retry-After": str(SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS),
                "X-UCloud-Sandbox-Retryable": "true",
            },
        )
        return None

    def _lifecycle_response_fence(
        self,
        route: SandboxRoute,
        response: ProxiedResponse,
    ) -> tuple[str, int]:
        payload = response.json()
        node_epoch = payload.get("node_epoch")
        activity_epoch = payload.get("activity_epoch")
        if not isinstance(node_epoch, str) or not node_epoch.strip():
            raise ValueError("node_epoch is required")
        node_epoch = node_epoch.strip()
        if isinstance(activity_epoch, bool) or not isinstance(activity_epoch, int):
            raise ValueError("activity_epoch must be an integer")
        if activity_epoch < 0:
            raise ValueError("activity_epoch must be non-negative")
        heartbeat = self._heartbeat_for_route(job_id=route.job_id)
        if (
            heartbeat is None
            or heartbeat.node_id != route.node_id
            or (heartbeat.node_url or "").rstrip("/") != route.node_url.rstrip("/")
            or heartbeat.node_epoch != node_epoch
        ):
            raise ValueError("node epoch does not match the routed worker")
        if route.node_epoch and route.node_epoch != node_epoch:
            raise ValueError("node epoch does not match the sandbox route")
        if activity_epoch <= route.activity_epoch:
            raise ValueError("activity_epoch does not postdate the sandbox route")
        if activity_epoch < heartbeat.activity_epoch:
            raise ValueError("activity_epoch predates the accepted worker heartbeat")
        return node_epoch, activity_epoch

    def _commit_successful_wake(
        self,
        route: SandboxRoute,
        response: ProxiedResponse,
    ) -> SandboxRoute | None:
        """Commit one proven wake and retire its obsolete snapshot authority."""

        try:
            node_epoch, activity_epoch = self._lifecycle_response_fence(
                route,
                response,
            )
        except ValueError as exc:
            self._write_json(
                {"error": f"invalid node lifecycle response: {exc}"},
                status=HTTPStatus.BAD_GATEWAY,
            )
            return None
        try:
            updated = self.routing_store.set_sandbox_state_if_current(
                route,
                # A same-epoch heartbeat sampled before the wake can restore
                # ``parked`` while the node response is in flight. The newer
                # activity proof is authoritative for that exact owner.
                expected_states={"parked", "waking", "running"},
                state="running",
                node_epoch=node_epoch,
                activity_epoch=activity_epoch,
                # Live writes make the resumed snapshot stale. Retain only
                # the storage schema needed by the current worker.
                storage_schema=route.storage_schema,
                snapshot_manifest_digest="",
                snapshot_repository="",
                snapshot_tag="",
                storage_snapshot={},
            )
        except BaseException:
            try:
                current_route = self.routing_store.get_sandbox_readonly(
                    route.sandbox_id
                )
            except BaseException:
                raise
            self._release_registry_snapshot_reference(
                route,
                keep_route=current_route,
            )
            raise
        if updated is None:
            current_route = self.routing_store.get_sandbox_readonly(route.sandbox_id)
            self._release_registry_snapshot_reference(
                route,
                keep_route=current_route,
            )
            self._write_json(
                {
                    "error": "sandbox route changed while committing wake",
                    "retryable": True,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                headers={"X-UCloud-Sandbox-Retryable": "true"},
            )
            return None
        self._release_registry_snapshot_reference(
            route,
            keep_route=updated,
        )
        return updated

    def _route_exec_request(self, session_id: str) -> None:
        route = self.routing_store.get_exec(session_id)
        if route is None:
            self._write_json(
                {
                    "error": "exec route not found",
                    "retryable": False,
                },
                status=HTTPStatus.NOT_FOUND,
            )
            return
        if self._exec_route_is_proven_stale(route):
            self.routing_store.delete_exec(route.session_id)
            self._write_json(
                {
                    "error": "exec route is stale",
                    "sandbox_id": route.sandbox_id,
                    "retryable": False,
                },
                status=HTTPStatus.NOT_FOUND,
            )
            return
        try:
            body = (
                self._read_raw_body(max_bytes=DEFAULT_MAX_PROXY_BODY_BYTES)
                if self.command in {"POST", "PUT", "PATCH"}
                else None
            )
        except ValueError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        response = self._proxy_request(
            route.node_url,
            self.path,
            method=self.command,
            body=body,
        )
        self._send_proxied_response(response)

    def _exec_route_is_proven_stale(self, route: ExecRoute) -> bool:
        heartbeat = self._heartbeat_for_route(
            job_id=route.job_id,
        )
        return _heartbeat_proves_route_absent(
            heartbeat,
            sandbox_id=route.sandbox_id,
            route_created_at=route.created_at,
            route_updated_at=route.updated_at,
            heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
        )

    def _heartbeat_for_route(
        self,
        *,
        job_id: str,
    ) -> NodeHeartbeat | None:
        # Every persisted sandbox and exec route has a non-empty immutable job
        # binding. An exact miss means that worker heartbeat is unavailable;
        # scanning unrelated node inventories cannot make the route current.
        return self.store.get_heartbeat(job_id)

    def _route_worker_is_fresh(self, route: SandboxRoute) -> bool:
        heartbeat = self._heartbeat_for_route(
            job_id=route.job_id,
        )
        return bool(
            heartbeat is not None
            and heartbeat.node_url
            and heartbeat.is_fresh(utc_now(), self.heartbeat_ttl_seconds)
        )

    def _write_route_worker_unreachable(self, route: SandboxRoute) -> None:
        self._write_json(
            {
                "error": "sandbox worker heartbeat is stale or unavailable",
                "error_code": "sandbox_worker_unreachable",
                "retryable": True,
                "node_id": route.node_id,
                "job_id": route.job_id,
            },
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            headers={
                "Retry-After": "1",
                "X-UCloud-Sandbox-Retryable": "true",
            },
        )

    def _select_node(
        self,
        requested: ResourceQuantity,
        *,
        image: str | None = None,
        required_capabilities: tuple[str, ...] = (),
        excluded_job_ids: tuple[str, ...] = (),
    ) -> NodeHeartbeat | None:
        routes = self._placement_routes()
        route_index = _placement_route_index(routes)
        excluded_jobs = frozenset(excluded_job_ids)
        candidate_states: list[tuple[NodeHeartbeat, NodePlacementState]] = []
        for heartbeat in self._ready_sandbox_heartbeats():
            if heartbeat.job_id in excluded_jobs:
                continue
            if not heartbeat.admission_open:
                continue
            if not agent_version_is_schedulable(heartbeat.agent_version):
                continue
            if not all(
                has_capability(heartbeat.capabilities, capability)
                for capability in required_capabilities
            ):
                continue
            placement_state = _node_placement_state(
                heartbeat,
                route_index.routes_for(heartbeat),
            )
            if not _node_can_fit_available(
                heartbeat,
                requested,
                placement_state.available_resources,
            ):
                continue
            candidate_states.append((heartbeat, placement_state))
        if not candidate_states:
            return None
        candidates = [heartbeat for heartbeat, _state in candidate_states]
        image_node_ids = self._nodes_with_image(
            image or "",
            candidates,
            probe_uncached=False,
        )
        image_identity = (
            canonical_image_digest_ref(image or "") or (image or "").strip()
        )
        inflight_image_node_ids = {
            heartbeat.node_id
            for heartbeat, state in candidate_states
            if image_identity and image_identity in state.inflight_image_identities
        }
        cached_nodes_have_headroom = any(
            heartbeat.node_id in image_node_ids
            and state.active_creates < self.create_target_concurrency_per_node
            for heartbeat, state in candidate_states
        )
        inflight_nodes_have_headroom = any(
            heartbeat.node_id in inflight_image_node_ids
            and state.active_creates < self.create_target_concurrency_per_node
            for heartbeat, state in candidate_states
        )
        if image_node_ids and cached_nodes_have_headroom:
            candidate_states = [
                (heartbeat, state)
                for heartbeat, state in candidate_states
                if heartbeat.node_id in image_node_ids
            ]
        elif inflight_image_node_ids and inflight_nodes_have_headroom:
            # Follow an in-flight copy of the exact immutable image instead
            # of transferring the same layers to another node.
            candidate_states = [
                (heartbeat, state)
                for heartbeat, state in candidate_states
                if heartbeat.node_id in inflight_image_node_ids
            ]
        spread_cold_image = bool(
            image
            and not (
                (image_node_ids and cached_nodes_have_headroom)
                or (inflight_image_node_ids and inflight_nodes_have_headroom)
            )
        )
        layer_cache = getattr(self, "registry_layer_cache", None)
        target_manifest = (
            layer_cache.get(image or "") if layer_cache is not None else None
        )
        return min(
            candidate_states,
            key=lambda item: (
                _cold_image_placement_cost_for_state(
                    item[1],
                    target_manifest,
                    layer_cache,
                    spread_cold_image=spread_cold_image,
                ),
                item[1].active_creates,
                _resource_slack(
                    item[1].available_resources,
                    requested,
                ),
                item[0].node_id,
            ),
        )[0]

    def _sandbox_create_alternate_available(
        self,
        spec: SandboxSpec,
        *,
        excluded_job_ids: tuple[str, ...],
    ) -> bool:
        return (
            self._select_node(
                spec.requested_resources(),
                image=spec.image,
                required_capabilities=_sandbox_required_capabilities(spec.to_dict()),
                excluded_job_ids=excluded_job_ids,
            )
            is not None
        )

    def _placement_routes(self) -> list[PlacementRecord]:
        """Include in-flight destination imports in normal node admission."""

        routes = list(self.routing_store.sandbox_routes_readonly())
        routes_by_id = {route.sandbox_id: route for route in routes}
        for migration in self.routing_store.sandbox_migrations(active_only=True):
            source = routes_by_id.get(migration.sandbox_id)
            if source is None:
                continue
            # Before route commit the destination may already be allocating
            # quota and restoring metadata, while its heartbeat still has no
            # observation. Reserve the complete shape. After route commit the
            # parked route owns disk itself, but a wake relocation still needs
            # its CPU/RAM reservation through activation. Completion can then
            # atomically turn that parked route into ``waking``.
            reservation = source.resources
            if migration.phase in {"routed", "activated"}:
                reservation = ResourceQuantity(
                    vcpu=source.resources.vcpu,
                    memory_mb=source.resources.memory_mb,
                )
            routes.append(
                PlacementReservation(
                    reservation_id=migration.migration_id,
                    node_id=migration.destination_node_id,
                    job_id=migration.destination_job_id,
                    node_url=migration.destination_node_url,
                    resources=reservation,
                    image=str(source.spec.get("image") or ""),
                )
            )
        return routes

    def _select_and_reserve_node(
        self,
        sandbox_id: str,
        requested: ResourceQuantity,
        *,
        image: str | None = None,
        spec: dict[str, Any],
        spec_hash: str,
        excluded_job_ids: tuple[str, ...] = (),
    ) -> (
        tuple[
            NodeHeartbeat,
            SandboxRoute,
            PendingSandboxDemand | None,
        ]
        | None
    ):
        if not _GATEWAY_SCHEDULING_LOCK.acquire(
            timeout=SANDBOX_PLACEMENT_LOCK_WAIT_SECONDS
        ):
            raise GatewaySchedulingBusyError(
                "sandbox placement is already being reserved"
            )
        try:
            with _gateway_placement_lock(self.routing_store.path, blocking=False):
                heartbeat = self._select_node(
                    requested,
                    image=image,
                    required_capabilities=_sandbox_required_capabilities(spec),
                    excluded_job_ids=excluded_job_ids,
                )
                if heartbeat is None:
                    return None
                route, pending = (
                    self.routing_store.allocate_sandbox_create_with_pending(
                        SandboxRouteAllocation(
                            sandbox_id=sandbox_id,
                            node_id=heartbeat.node_id,
                            job_id=heartbeat.job_id,
                            node_url=heartbeat.node_url or "",
                            resources=requested,
                            spec=dict(spec),
                            node_epoch=heartbeat.node_epoch,
                            activity_epoch=heartbeat.activity_epoch,
                        ),
                        spec_hash=spec_hash,
                    )
                )
                return heartbeat, route, pending
        finally:
            _GATEWAY_SCHEDULING_LOCK.release()

    def _write_image_resolution_error(self, payload: dict[str, Any]) -> None:
        transient = payload.get("error_code") in TRANSIENT_IMAGE_RESOLUTION_ERROR_CODES
        self._write_json(
            payload,
            status=HTTPStatus.SERVICE_UNAVAILABLE
            if transient
            else HTTPStatus.BAD_REQUEST,
            headers=(
                {"Retry-After": "1", "X-UCloud-Sandbox-Retryable": "true"}
                if transient
                else None
            ),
        )

    def _resolve_request_image_reference(
        self,
        image: str,
    ) -> tuple[str, dict[str, Any] | None]:
        try:
            reference_kind = _image_reference_kind_from_headers(self.headers)
        except ValueError as exc:
            return image, {
                "error": str(exc),
                "error_code": "invalid_image_reference_kind",
                "retryable": False,
            }
        return self._resolve_sandbox_image_reference(
            image,
            reference_kind=reference_kind,
        )

    def _resolve_sandbox_image_reference(
        self,
        image: str,
        *,
        reference_kind: str = "auto",
    ) -> tuple[str, dict[str, Any] | None]:
        if reference_kind not in {"auto", "name", "registry"}:
            raise ValueError(f"unsupported image reference kind: {reference_kind!r}")
        existing_digest = manifest_digest_from_image_ref(image)
        if existing_digest:
            protected_digest = self._managed_registry_manifest_digest(image)
            if (
                self.registry_url
                and _managed_registry_image_coordinates(
                    image,
                    self.registry_url,
                    self.registry_worker_url or "",
                )
                is not None
                and protected_digest != existing_digest
            ):
                return image, {
                    "error": "managed registry digest protection is unavailable",
                    "error_code": (
                        MANAGED_REGISTRY_DIGEST_PROTECTION_UNAVAILABLE_ERROR_CODE
                    ),
                    "retryable": True,
                    "image": image,
                }
            return self._managed_registry_worker_reference(image), None
        if reference_kind != "name":
            direct_digest = self._managed_registry_manifest_digest(image)
            if direct_digest:
                return self._managed_registry_worker_reference(
                    image_ref_with_manifest_digest(image, direct_digest)
                ), None
            if reference_kind == "registry":
                return image, None
            if not _looks_like_image_id_reference(image):
                return image, None
        inventory = self._cached_raw_image_inventory_across_nodes()
        matches = self._enrich_image_inventory_records(
            inventory.records,
            image_id=image,
        )
        if not matches:
            if reference_kind == "name":
                if not inventory.complete:
                    return image, _incomplete_image_inventory_error(image)
                return image, {
                    "error": f"gateway image id was not found: {image}",
                    "error_code": "image_id_not_found",
                    "retryable": False,
                    "image_id": image,
                }
            if (
                not inventory.complete
                and image in inventory.unobserved_references
                and self._is_known_successful_gateway_image_id(image)
            ):
                return image, _incomplete_image_inventory_error(image)
            return image, None
        available = [
            record
            for record in matches
            if _image_record_available_to_sandboxes(record)
            and isinstance(record.get("tag"), str)
            and record.get("tag")
        ]
        if available:
            selected = sorted(
                available,
                key=lambda record: (
                    0 if record.get("location") == "control-plane" else 1,
                    str(record.get("tag") or ""),
                ),
            )[0]
            selected_tag = str(selected["tag"])
            digest = normalize_manifest_digest(
                str(selected.get("manifest_digest") or "")
            )
            if (
                not digest
                and self.registry_url
                and _managed_registry_image_coordinates(
                    selected_tag,
                    self.registry_url,
                    self.registry_worker_url or "",
                )
                is not None
            ):
                return image, {
                    "error": "managed registry digest protection is unavailable",
                    "error_code": (
                        MANAGED_REGISTRY_DIGEST_PROTECTION_UNAVAILABLE_ERROR_CODE
                    ),
                    "retryable": True,
                    "image_id": image,
                }
            if digest and selected.get("location") == "control-plane":
                try:
                    self.image_manager.store.upsert(ImageRecord.from_dict(selected))
                except ValueError:
                    pass
            return self._managed_registry_worker_reference(
                image_ref_with_manifest_digest(selected_tag, digest)
            ), None
        return image, {
            "error": (
                "image id exists, but it is not available to sandbox nodes; "
                "resubmit the gateway-managed build, then create the sandbox "
                "with that image id"
            ),
            "image_id": image,
            "matches": [_image_record_summary(record) for record in matches],
        }

    def _is_known_successful_gateway_image_id(self, image: str) -> bool:
        get_build = getattr(self.image_manager, "get_build", None)
        if not callable(get_build):
            return False
        try:
            build = get_build(image)
        except (OSError, TypeError, ValueError):
            return False
        return bool(
            build is not None
            and getattr(build, "image_id", None) == image
            and getattr(build, "status", None) == "succeeded"
        )

    def _managed_registry_worker_reference(self, image_ref: str) -> str:
        if not self.registry_url:
            return image_ref
        return _managed_registry_worker_reference(
            image_ref,
            self.registry_url,
            self.registry_worker_url or "",
        )

    def _image_record_missing_registry_manifest(self, record: dict[str, Any]) -> bool:
        tag = str(record.get("tag") or "")
        if not self.registry_url or not _image_record_requires_registry_manifest(
            record,
            self.registry_url,
            self.registry_worker_url or "",
        ):
            return False
        parsed = registry_repository_tag_from_image_ref(tag)
        if parsed is None:
            return False
        try:
            recorded_digest = normalize_manifest_digest(
                str(record.get("manifest_digest") or "")
            )
            resolved_digest = self._resolve_and_protect_managed_manifest(
                image_ref_with_manifest_digest(tag, recorded_digest)
                if recorded_digest
                else tag
            )
            if recorded_digest:
                return normalize_manifest_digest(resolved_digest) != recorded_digest
            normalized_digest = normalize_manifest_digest(resolved_digest)
            if normalized_digest:
                record["manifest_digest"] = normalized_digest
                return False
            return True
        except RegistryRequestError as exc:
            return exc.status_code == 404
        except (OSError, ValueError):
            return False

    def _image_cache_candidates(
        self,
        *,
        resources: ResourceQuantity,
        sandbox_nodes_only: bool,
    ) -> list[NodeHeartbeat]:
        routes = list(self.routing_store.sandbox_routes_readonly())
        candidates = []
        for heartbeat in self._ready_heartbeats():
            if "image-cache" not in heartbeat.capabilities:
                continue
            if not agent_version_is_schedulable(heartbeat.agent_version):
                continue
            if sandbox_nodes_only and "sandbox" not in heartbeat.capabilities:
                continue
            if _has_resource_values(resources) and "sandbox" in heartbeat.capabilities:
                if not _node_can_fit(heartbeat, resources, routes):
                    continue
            candidates.append(heartbeat)
        return sorted(
            candidates,
            key=lambda heartbeat: (
                0 if "sandbox" in heartbeat.capabilities else 1,
                -heartbeat.free_resources.disk_mb,
                -heartbeat.free_resources.memory_mb,
                -heartbeat.free_resources.vcpu,
                heartbeat.node_id,
            ),
        )

    def _select_builder_node(self) -> NodeHeartbeat | None:
        candidates = [
            heartbeat
            for heartbeat in self._ready_heartbeats()
            if "image-build" in heartbeat.capabilities
            and "sandbox" not in heartbeat.capabilities
            and agent_version_is_schedulable(heartbeat.agent_version)
        ]
        if not candidates:
            return None
        return sorted(
            candidates,
            key=lambda heartbeat: (
                -heartbeat.free_resources.disk_mb,
                -heartbeat.free_resources.memory_mb,
                -heartbeat.free_resources.vcpu,
                heartbeat.node_id,
            ),
        )[0]

    def _nodes_with_image(
        self,
        image: str,
        heartbeats: list[NodeHeartbeat],
        *,
        image_id: str = "",
        use_heartbeat_cache: bool = True,
        probe_uncached: bool = True,
    ) -> set[str]:
        if not image.strip() and not image_id.strip():
            return set()
        image_keys = _requested_image_cache_keys(
            image,
            image_id,
            require_digest=self._managed_image_requires_digest_cache_identity(image),
        )
        node_ids: set[str] = set()
        for heartbeat in heartbeats:
            if use_heartbeat_cache and heartbeat.cached_images_known:
                if image_keys.intersection(heartbeat.cached_images):
                    node_ids.add(heartbeat.node_id)
                continue
            if not probe_uncached:
                continue
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/images",
                method="GET",
            )
            if response.status >= 400:
                continue
            raw_images = response.json().get("images")
            if not isinstance(raw_images, list):
                continue
            for record in raw_images:
                if not isinstance(record, dict):
                    continue
                if image_keys.intersection(_image_record_cache_keys(record)):
                    node_ids.add(heartbeat.node_id)
                    break
        return node_ids

    def _schedule_image_warmups(self) -> dict[str, Any]:
        warmups = self.routing_store.image_warmups()
        if not warmups:
            return {"scheduled": 0, "completed": 0, "warmups": []}
        heartbeats = self._ready_sandbox_heartbeats()
        summaries: list[dict[str, Any]] = []
        scheduled = 0
        completed = 0
        for warmup in warmups:
            summary = self._schedule_image_warmup(warmup, heartbeats)
            scheduled += int(summary.get("scheduled", 0))
            completed += 1 if summary.get("completed") else 0
            summaries.append(summary)
        return {
            "scheduled": scheduled,
            "completed": completed,
            "warmups": summaries,
        }

    def _schedule_image_warmup(
        self,
        warmup: PendingImageWarmup,
        heartbeats: list[NodeHeartbeat],
    ) -> dict[str, Any]:
        ready_units = 0
        projected_units = 0
        scheduled = 0
        scheduled_nodes: list[str] = []
        warmed_node_ids = set(warmup.warmed_node_ids)
        candidate_heartbeats = [
            heartbeat
            for heartbeat in heartbeats
            if _warmup_node_units(heartbeat, warmup.resources) > 0
            and agent_version_is_schedulable(heartbeat.agent_version)
        ]
        for heartbeat in candidate_heartbeats:
            if _heartbeat_has_image(
                heartbeat,
                warmup.image,
                warmup.image_id,
                require_digest=self._managed_image_requires_digest_cache_identity(
                    warmup.image
                ),
            ):
                warmed_node_ids.add(heartbeat.node_id)
                self.routing_store.mark_image_warmup_node(
                    warmup.warmup_id,
                    heartbeat.node_id,
                )
        for heartbeat in candidate_heartbeats:
            if heartbeat.node_id in warmed_node_ids:
                ready_units += _warmup_node_units(heartbeat, warmup.resources)
        projected_units = ready_units
        if ready_units >= warmup.count:
            self.routing_store.delete_image_warmup(warmup.warmup_id)
            return {
                "warmup_id": warmup.warmup_id,
                "image": warmup.image,
                "requested": warmup.count,
                "ready": ready_units,
                "projected": projected_units,
                "scheduled": 0,
                "scheduled_nodes": [],
                "completed": True,
            }
        for heartbeat in candidate_heartbeats:
            if projected_units >= warmup.count:
                break
            if heartbeat.node_id in warmed_node_ids:
                continue
            if self._start_image_warmup_task(warmup, heartbeat):
                node_units = _warmup_node_units(heartbeat, warmup.resources)
                projected_units += node_units
                scheduled += 1
                scheduled_nodes.append(heartbeat.node_id)
        return {
            "warmup_id": warmup.warmup_id,
            "image": warmup.image,
            "requested": warmup.count,
            "ready": ready_units,
            "projected": projected_units,
            "scheduled": scheduled,
            "scheduled_nodes": scheduled_nodes,
            "completed": False,
        }

    def _start_image_warmup_task(
        self,
        warmup: PendingImageWarmup,
        heartbeat: NodeHeartbeat,
    ) -> bool:
        node_url = heartbeat.node_url or ""
        if not node_url:
            return False
        key = (warmup.warmup_id, heartbeat.node_id)
        try:
            self._ensure_registry_image_lease(
                warmup.image,
                _registry_operation_lease_owner(
                    "image-warmup",
                    {
                        "warmup_id": warmup.warmup_id,
                        "image_id": warmup.image_id,
                        "node_id": heartbeat.node_id,
                        "job_id": heartbeat.job_id,
                    },
                ),
                touch=True,
            )
        except RegistryImageReferenceUnavailable:
            # No pull thread is started when the lifetime fence is unavailable.
            return False
        with _IMAGE_WARMUP_TASKS_GUARD:
            if key in _IMAGE_WARMUP_TASKS:
                return False
            _IMAGE_WARMUP_TASKS.add(key)
        thread = Thread(
            target=_run_image_warmup_task,
            args=(
                self.routing_store,
                warmup,
                heartbeat,
                key,
                self.node_control_bearer_token,
            ),
            daemon=True,
            name=f"image-warmup-{warmup.warmup_id[:16]}-{heartbeat.node_id[:16]}",
        )
        thread.start()
        return True

    def _node_has_image(
        self,
        heartbeat: NodeHeartbeat,
        image: str,
        *,
        image_id: str = "",
        use_heartbeat_cache: bool = True,
    ) -> bool:
        if not image.strip() and not image_id.strip():
            return False
        image_keys = _requested_image_cache_keys(
            image,
            image_id,
            require_digest=self._managed_image_requires_digest_cache_identity(image),
        )
        if use_heartbeat_cache and heartbeat.cached_images_known:
            return bool(image_keys.intersection(heartbeat.cached_images))
        return heartbeat.node_id in self._nodes_with_image(
            image,
            [heartbeat],
            image_id=image_id,
            use_heartbeat_cache=use_heartbeat_cache,
        )

    def _managed_image_requires_digest_cache_identity(self, image: str) -> bool:
        return bool(
            self.registry_url
            and _managed_registry_image_coordinates(
                image,
                self.registry_url,
                self.registry_worker_url or "",
            )
            is not None
        )

    def _ensure_image_on_node(
        self,
        heartbeat: NodeHeartbeat,
        image: str,
    ) -> ProxiedResponse | None:
        node_url = heartbeat.node_url or ""
        if not image.strip() or self._node_has_image(heartbeat, image):
            return None
        with _image_pull_lock(node_url, image):
            if self._node_has_image(heartbeat, image, use_heartbeat_cache=False):
                return None
            return self._pull_image_on_node(heartbeat, image)

    def _warm_image_on_ready_nodes(
        self,
        image: str,
        *,
        count: int,
        resources: ResourceQuantity,
        sandbox_nodes_only: bool,
        image_id: str = "",
    ) -> dict[str, Any]:
        image = image.strip()
        image_id = image_id.strip()
        requested = max(1, count)
        candidates = self._image_cache_candidates(
            resources=resources,
            sandbox_nodes_only=sandbox_nodes_only,
        )
        cache_hits: list[dict[str, Any]] = []
        pulled: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        selected_image: dict[str, Any] | None = None
        for heartbeat in candidates:
            if len(cache_hits) + len(pulled) >= requested:
                break
            if self._node_has_image(heartbeat, image, image_id=image_id):
                hit = {
                    "node": _node_metadata(heartbeat),
                    "image": {
                        "id": image_id or image_id_from_tag(image),
                        "tag": image,
                    },
                }
                cache_hits.append(hit)
                selected_image = selected_image or hit["image"]
                continue
            response = self._pull_image_on_node(heartbeat, image, image_id=image_id)
            payload = response.json()
            raw_image = payload.get("image")
            image_record = (
                dict(raw_image)
                if isinstance(raw_image, dict)
                else {"id": image_id or image_id_from_tag(image), "tag": image}
            )
            item = {
                "node": _node_metadata(heartbeat),
                "status": int(response.status),
                "image": image_record,
            }
            if 200 <= response.status < 300:
                pulled.append(item)
                selected_image = selected_image or image_record
            else:
                item["error"] = payload.get("error") or payload
                failed.append(item)
        ready = len(cache_hits) + len(pulled)
        return {
            "image": selected_image
            or {"id": image_id or image_id_from_tag(image), "tag": image},
            "image_ref": image,
            "requested": requested,
            "ready": ready,
            "cache_hits": cache_hits,
            "pulled": pulled,
            "failed": failed,
        }

    def _pull_image_on_node(
        self,
        heartbeat: NodeHeartbeat,
        image: str,
        *,
        image_id: str = "",
    ) -> ProxiedResponse:
        payload: dict[str, Any] = {"image": image}
        if image_id:
            payload["id"] = image_id
        response: ProxiedResponse | None = None
        for attempt in range(IMAGE_PULL_RETRY_ATTEMPTS):
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/images/pull",
                method="POST",
                body=json.dumps(payload).encode("utf-8"),
                timeout_seconds=IMAGE_PULL_PROXY_TIMEOUT_SECONDS,
            )
            if not _retryable_image_pull_response(response):
                if 200 <= response.status < 300:
                    self._invalidate_image_inventory_cache()
                return response
            if attempt + 1 < IMAGE_PULL_RETRY_ATTEMPTS:
                time.sleep(IMAGE_PULL_RETRY_BASE_DELAY_SECONDS * (2**attempt))
        assert response is not None
        if 200 <= response.status < 300:
            self._invalidate_image_inventory_cache()
        return response

    def _ready_heartbeats(self) -> list[NodeHeartbeat]:
        now = utc_now()
        return [
            heartbeat
            for heartbeat in self.store.load_heartbeats().values()
            if heartbeat.node_url
            and not heartbeat.draining
            and heartbeat.is_fresh(now, self.heartbeat_ttl_seconds)
        ]

    def _ready_sandbox_heartbeats(self) -> list[NodeHeartbeat]:
        return [
            heartbeat
            for heartbeat in self._ready_heartbeats()
            if "sandbox" in heartbeat.capabilities
        ]

    def _proxy_request(
        self,
        node_url: str,
        path: str,
        *,
        method: str,
        body: Any = None,
        timeout_seconds: float = DEFAULT_PROXY_TIMEOUT_SECONDS,
        extra_headers: dict[str, str] | None = None,
    ) -> ProxiedResponse:
        proxied = self._build_proxy_request(
            node_url,
            path,
            method=method,
            body=body,
            extra_headers=extra_headers,
        )
        try:
            with _open_node_request(
                proxied,
                timeout=timeout_seconds,
                authenticated=True,
            ) as response:
                try:
                    response_body = _read_bounded_proxy_body(
                        response,
                        max_bytes=DEFAULT_MAX_PROXY_RESPONSE_BYTES,
                    )
                except ProxyResponseTooLargeError:
                    return _proxy_response_too_large(DEFAULT_MAX_PROXY_RESPONSE_BYTES)
                return ProxiedResponse(
                    response.status,
                    response.headers,
                    response_body,
                )
        except error.HTTPError as exc:
            try:
                response_body = _read_bounded_proxy_body(
                    exc,
                    max_bytes=DEFAULT_MAX_PROXY_ERROR_BYTES,
                )
            except ProxyResponseTooLargeError:
                return _proxy_response_too_large(DEFAULT_MAX_PROXY_ERROR_BYTES)
            return ProxiedResponse(exc.code, exc.headers, response_body)
        except error.URLError as exc:
            return _node_transport_error_response(exc.reason)
        except OSError as exc:
            return _node_transport_error_response(exc)

    def _build_proxy_request(
        self,
        node_url: str,
        path: str,
        *,
        method: str,
        body: Any = None,
        extra_headers: dict[str, str] | None = None,
    ) -> request.Request:
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower()
            not in {
                "host",
                "content-length",
                "connection",
                "authorization",
                "proxy-authorization",
                "x-ucloud-sandbox-token",
            }
        }
        headers.update(extra_headers or {})
        # Public gateway credentials are never node credentials. Override any
        # caller-provided auth header with the private control-plane credential.
        for key in list(headers):
            if key.lower() in {
                "authorization",
                "proxy-authorization",
                "x-ucloud-sandbox-token",
            }:
                del headers[key]
        headers["Authorization"] = f"Bearer {self.node_control_bearer_token}"
        if self.telemetry is not None:
            self.telemetry.inject(headers)
        return request.Request(
            node_url.rstrip("/") + path,
            data=body,
            method=method,
            headers=headers,
        )

    def _stream_proxy_request(
        self,
        node_url: str,
        path: str,
        *,
        method: str,
        body: Any = None,
        timeout_seconds: float = DEFAULT_PROXY_TIMEOUT_SECONDS,
        extra_headers: dict[str, str] | None = None,
        on_success: Callable[[], None] | None = None,
    ) -> None:
        proxied = self._build_proxy_request(
            node_url,
            path,
            method=method,
            body=body,
            extra_headers=extra_headers,
        )
        try:
            response = _open_node_request(
                proxied,
                timeout=timeout_seconds,
                authenticated=True,
            )
        except error.HTTPError as exc:
            try:
                response_body = _read_bounded_proxy_body(
                    exc,
                    max_bytes=DEFAULT_MAX_PROXY_ERROR_BYTES,
                )
            except ProxyResponseTooLargeError:
                proxied_error = _proxy_response_too_large(DEFAULT_MAX_PROXY_ERROR_BYTES)
            else:
                proxied_error = ProxiedResponse(exc.code, exc.headers, response_body)
            self._send_proxied_response(proxied_error)
            return
        except error.URLError as exc:
            self._send_proxied_response(_node_transport_error_response(exc.reason))
            return
        except OSError as exc:
            self._send_proxied_response(_node_transport_error_response(exc))
            return

        with response:
            if response.status >= 400:
                try:
                    response_body = _read_bounded_proxy_body(
                        response,
                        max_bytes=DEFAULT_MAX_PROXY_ERROR_BYTES,
                    )
                except ProxyResponseTooLargeError:
                    proxied_error = _proxy_response_too_large(
                        DEFAULT_MAX_PROXY_ERROR_BYTES
                    )
                else:
                    proxied_error = ProxiedResponse(
                        response.status,
                        response.headers,
                        response_body,
                    )
                self._send_proxied_response(proxied_error)
                return
            try:
                content_length = _proxy_content_length(response.headers)
            except ValueError as exc:
                self._write_json(
                    {"error": f"invalid upstream node response: {exc}"},
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return
            if on_success is not None:
                on_success()
            self.send_response(response.status)
            self._copy_streaming_response_headers(
                response.headers,
                content_length=content_length,
            )
            self.end_headers()
            while chunk := response.read(PROXY_STREAM_CHUNK_BYTES):
                self.wfile.write(chunk)

    def _send_proxied_response(
        self,
        response: ProxiedResponse,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        structured_error = _structured_proxy_error(response)
        if structured_error is not None:
            self._write_json(structured_error, status=response.status)
            return
        self.send_response(response.status)
        self._copy_response_headers(
            response.headers,
            len(response.body),
            extra_headers=extra_headers,
        )
        self.end_headers()
        self.wfile.write(response.body)

    def _copy_response_headers(
        self,
        headers: Any,
        content_length: int,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        overridden = {key.lower() for key in (extra_headers or {})}
        for key, value in headers.items():
            if key.lower() in {
                "connection",
                "transfer-encoding",
                "content-length",
                *overridden,
            }:
                continue
            self.send_header(key, value)
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(content_length))

    def _copy_streaming_response_headers(
        self,
        headers: Any,
        *,
        content_length: int | None,
    ) -> None:
        for key, value in headers.items():
            if key.lower() in {"connection", "transfer-encoding", "content-length"}:
                continue
            self.send_header(key, value)
        if content_length is None:
            self.close_connection = True
        else:
            self.send_header("Content-Length", str(content_length))

    def _check_authorized(self) -> bool:
        if self._token_matches(
            self.gateway_bearer_token,
            allow_ucloud_sandbox_header=True,
        ):
            return True
        if self._token_matches(
            self.sandbox_api_token,
            allow_ucloud_sandbox_header=True,
        ):
            if _is_sdk_api_request(self.command, urlparse(self.path).path):
                return True
            self._write_json(
                {"error": "sandbox API key is not authorized for this endpoint"},
                status=HTTPStatus.FORBIDDEN,
            )
            return False
        return self._write_unauthorized()

    def _check_heartbeat_authorized(self) -> bool:
        if self._token_matches(
            self.heartbeat_bearer_token,
            allow_ucloud_sandbox_header=False,
        ):
            return True
        return self._write_unauthorized()

    def _token_matches(
        self,
        expected: str,
        *,
        allow_ucloud_sandbox_header: bool,
    ) -> bool:
        authorization = self.headers.get("Authorization") or ""
        prefix = "Bearer "
        bearer = (
            authorization[len(prefix) :] if authorization.startswith(prefix) else ""
        )
        if bearer and hmac.compare_digest(bearer, expected):
            return True
        if allow_ucloud_sandbox_header:
            public_link_token = self.headers.get("X-UCloud-Sandbox-Token") or ""
            if public_link_token and hmac.compare_digest(public_link_token, expected):
                return True
        return False

    def _write_unauthorized(self) -> bool:
        self._write_json(
            {"error": "unauthorized"},
            status=HTTPStatus.UNAUTHORIZED,
            headers={"WWW-Authenticate": "Bearer"},
        )
        return False


def build_server(
    host: str,
    port: int,
    control_state_file: Path,
    *,
    gateway_bearer_token: str,
    sandbox_api_token: str,
    heartbeat_bearer_token: str,
    node_control_bearer_token: str,
    deployment_id: str,
    routing_file: Path,
    image_file: Path,
    metrics_file: Path,
    heartbeat_ttl_seconds: int = 120,
    registry_url: str | None = None,
    registry_worker_url: str | None = None,
    registry_usage_file: Path | None = None,
    max_concurrent_sandbox_creates: int = DEFAULT_MAX_CONCURRENT_SANDBOX_CREATES,
    create_target_concurrency_per_node: int = (
        ScalePolicy().create_target_concurrency_per_node
    ),
    max_http_request_threads: int = DEFAULT_MAX_GATEWAY_HTTP_REQUEST_THREADS,
    build_context_store_dir: Path | None = None,
    max_sandbox_resources: ResourceQuantity | None = None,
    telemetry: Telemetry | None = None,
) -> HighBacklogThreadingHTTPServer:
    credentials = {
        "gateway bearer token": gateway_bearer_token.strip(),
        "sandbox API token": sandbox_api_token.strip(),
        "heartbeat bearer token": heartbeat_bearer_token.strip(),
        "node control bearer token": node_control_bearer_token.strip(),
    }
    for label, credential in credentials.items():
        if not credential.strip():
            raise ValueError(f"{label} cannot be empty")
    if len(set(credentials.values())) != len(credentials):
        raise ValueError(
            "gateway, sandbox API, heartbeat, and node control tokens must be distinct"
        )
    gateway_bearer_token = credentials["gateway bearer token"]
    sandbox_api_token = credentials["sandbox API token"]
    heartbeat_bearer_token = credentials["heartbeat bearer token"]
    node_control_bearer_token = credentials["node control bearer token"]
    deployment_id = deployment_id.strip()
    if not deployment_id:
        raise ValueError("deployment id cannot be empty")
    if create_target_concurrency_per_node < 1:
        raise ValueError("create target concurrency per node must be positive")
    resolved_telemetry = telemetry or Telemetry.disabled("ucloud-sandbox-gateway")
    store = ControlStateStore(control_state_file)
    routing_store = RoutingStore(routing_file)
    metrics_store = MetricsStore(metrics_file)
    registry_usage_store = (
        RegistryUsageStore(registry_usage_file)
        if registry_usage_file is not None
        else None
    )
    image_manager = ImageManager(
        ImageStore(image_file),
        DockerImageRuntime(dry_run=True),
        telemetry=resolved_telemetry,
    )
    build_context_store = BuildContextBlobStore(
        build_context_store_dir or image_file.parent / f"{image_file.stem}-contexts",
        max_blob_bytes=DEFAULT_MAX_PROXY_BODY_BYTES,
        max_total_bytes=DEFAULT_MAX_BUILD_CONTEXT_STORE_BYTES,
        max_entries=DEFAULT_MAX_BUILD_CONTEXT_ENTRIES,
        max_age_seconds=DEFAULT_MAX_BUILD_CONTEXT_AGE_SECONDS,
    )

    class BoundHandler(ControlPlaneHandler):
        pass

    BoundHandler.store = store
    BoundHandler.routing_store = routing_store
    BoundHandler.gateway_bearer_token = gateway_bearer_token
    BoundHandler.sandbox_api_token = sandbox_api_token
    BoundHandler.heartbeat_bearer_token = heartbeat_bearer_token
    BoundHandler.node_control_bearer_token = node_control_bearer_token
    BoundHandler.deployment_id = deployment_id
    BoundHandler.heartbeat_ttl_seconds = heartbeat_ttl_seconds
    BoundHandler.image_manager = image_manager
    BoundHandler.build_context_store = build_context_store
    BoundHandler.metrics_store = metrics_store
    BoundHandler.registry_url = registry_url
    BoundHandler.registry_worker_url = registry_worker_url
    BoundHandler.registry_status_cache = None
    BoundHandler.registry_status_cache_at = 0.0
    BoundHandler.registry_status_lock = RLock()
    BoundHandler.registry_manifest_cache = (
        RegistryManifestResolutionCache(
            max_entries=REGISTRY_MANIFEST_CACHE_MAX_ENTRIES,
        )
        if registry_url
        else None
    )
    BoundHandler.image_inventory_cache = ImageInventoryCache(
        ttl_seconds=IMAGE_INVENTORY_CACHE_TTL_SECONDS
    )
    BoundHandler.metrics_response_cache = None
    BoundHandler.metrics_response_cache_at = 0.0
    BoundHandler.metrics_response_lock = RLock()
    BoundHandler.registry_layer_cache = (
        RegistryLayerMetadataCache(
            registry_url,
            registry_worker_url=registry_worker_url,
            max_entries=REGISTRY_LAYER_METADATA_CACHE_MAX_ENTRIES,
        )
        if registry_url
        else None
    )
    BoundHandler.registry_usage_store = registry_usage_store
    BoundHandler.max_concurrent_sandbox_creates = max(
        0,
        int(max_concurrent_sandbox_creates),
    )
    BoundHandler.create_target_concurrency_per_node = int(
        create_target_concurrency_per_node
    )
    BoundHandler.max_sandbox_resources = (
        max_sandbox_resources or ScalePolicy().default_node_resources
    )
    BoundHandler.sandbox_create_limiter = (
        BoundedSemaphore(BoundHandler.max_concurrent_sandbox_creates)
        if BoundHandler.max_concurrent_sandbox_creates > 0
        else None
    )
    BoundHandler.sandbox_create_busy_sampler = GatewayBusySampler(metrics_store)
    BoundHandler.telemetry = resolved_telemetry
    return HighBacklogThreadingHTTPServer(
        (host, port),
        BoundHandler,
        max_request_threads=max_http_request_threads,
    )


def _collection_id_from_path(path: str, prefix: str) -> str | None:
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix) :]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


def _is_sdk_api_request(method: str, path: str) -> bool:
    """Return whether the least-privileged public SDK key may use a route."""

    method = method.upper()
    exact_routes = {
        ("GET", "/v1/sandboxes"),
        ("POST", "/v1/sandboxes"),
        ("GET", "/v1/capacity/prepare"),
        ("POST", "/v1/capacity/prepare"),
        ("GET", "/v1/builders/prepare"),
        ("POST", "/v1/builders/prepare"),
        ("GET", "/v1/images"),
        ("GET", "/v1/images/builds"),
        ("POST", "/v1/images/build"),
        ("POST", "/v1/images/pull"),
    }
    if (method, path) in exact_routes:
        return True
    for prefix, methods in (
        ("/v1/capacity/prepare/", {"DELETE"}),
        ("/v1/builders/prepare/", {"DELETE"}),
        ("/v1/images/builds/", {"GET"}),
        ("/v1/image-contexts/", {"GET", "PUT"}),
    ):
        if method in methods and _single_encoded_path_segment(path, prefix):
            return True

    sandbox_route = match_sandbox_http_route(method, path)
    if sandbox_route is not None:
        return sandbox_route.sdk_public

    exec_parts = _encoded_path_parts(path, "/v1/exec/")
    if exec_parts is None:
        return False
    if len(exec_parts) == 1:
        return method == "GET"
    if len(exec_parts) != 2:
        return False
    if exec_parts[1] == "events":
        return method == "GET"
    if exec_parts[1] in {"stdin", "close-stdin", "signal"}:
        return method == "POST"
    return False


def _single_encoded_path_segment(path: str, prefix: str) -> bool:
    parts = _encoded_path_parts(path, prefix)
    return parts is not None and len(parts) == 1


def _encoded_path_parts(path: str, prefix: str) -> list[str] | None:
    if not path.startswith(prefix):
        return None
    raw = path[len(prefix) :]
    if not raw:
        return None
    parts = raw.split("/")
    if any(not part or "/" in unquote(part) for part in parts):
        return None
    return parts


def _sandbox_id_from_path(path: str) -> str | None:
    return _collection_id_from_path(path, "/v1/sandboxes/")


def _sandbox_migration_id_from_path(path: str) -> str | None:
    prefix = "/v1/sandboxes/"
    suffix = "/migration"
    if not path.startswith(prefix) or not path.endswith(suffix):
        return None
    encoded = path[len(prefix) : -len(suffix)]
    sandbox_id = unquote(encoded)
    if not sandbox_id or "/" in sandbox_id:
        return None
    return sandbox_id


def _sandbox_detach_id_from_path(path: str) -> str | None:
    prefix = "/v1/sandboxes/"
    suffix = "/detach"
    if not path.startswith(prefix) or not path.endswith(suffix):
        return None
    encoded = path[len(prefix) : -len(suffix)]
    sandbox_id = unquote(encoded)
    if not sandbox_id or "/" in sandbox_id:
        return None
    return sandbox_id


def _image_build_key_from_path(path: str) -> str | None:
    return _collection_id_from_path(path, "/v1/images/builds/")


def _exec_session_id_from_path(path: str) -> str | None:
    return _collection_id_from_path(path, "/v1/exec/")


def _prepare_id_from_path(path: str) -> str | None:
    return _collection_id_from_path(path, "/v1/capacity/prepare/")


def _builder_prepare_id_from_path(path: str) -> str | None:
    return _collection_id_from_path(path, "/v1/builders/prepare/")


def _truthy_query_param(parsed: Any, name: str) -> bool:
    values = parse_qs(str(getattr(parsed, "query", ""))).get(name, [])
    return any(
        str(value).lower() in {"1", "true", "yes", "on", "full"} for value in values
    )


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


def _prepared_resources_from_payload(raw: dict[str, Any]) -> ResourceQuantity:
    resources = {
        "vcpu": raw.get("cpus", 0),
        "memory_mb": raw.get("memory_mb", 0),
        "disk_mb": raw.get("disk_mb", 0),
    }
    vcpu = resources["vcpu"]
    if (
        isinstance(vcpu, bool)
        or not isinstance(vcpu, (int, float))
        or not math.isfinite(float(vcpu))
        or vcpu < 0
    ):
        raise ValueError("cpus must be non-negative and finite.")
    for label in ("memory_mb", "disk_mb"):
        value = resources[label]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{label} must be a non-negative integer.")
    prepared = ResourceQuantity.from_dict(resources)
    parkable = raw.get("parkable", False)
    if not isinstance(parkable, bool):
        raise ValueError("parkable must be a boolean.")
    if not parkable:
        return prepared
    if prepared.memory_mb <= 0:
        raise ValueError("parkable prepared capacity requires memory_mb.")
    if prepared.disk_mb <= 0:
        raise ValueError("parkable prepared capacity requires disk_mb.")
    return replace(
        prepared,
        disk_mb=hibernation_disk_reservation_mb(
            memory_mb=prepared.memory_mb,
            writable_disk_mb=prepared.disk_mb,
        ),
    )


def _strict_positive_integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer.")
    return value


def _validate_prepared_resources(resources: ResourceQuantity) -> None:
    if resources.vcpu < 0:
        raise ValueError("vcpu must be non-negative.")
    if resources.memory_mb < 0:
        raise ValueError("memory_mb must be non-negative.")
    if resources.disk_mb < 0:
        raise ValueError("disk_mb must be non-negative.")
    if resources == ResourceQuantity():
        raise ValueError("prepared capacity resources are required.")


def _sandbox_inventory_from_record(record: dict[str, Any]) -> SandboxInventoryEntry:
    spec = record.get("spec")
    if not isinstance(spec, dict):
        raise ValueError("sandbox record is missing its spec")
    parsed_spec = SandboxSpec.from_dict(spec)
    parsed_spec.validate()
    generation = _record_generation(record)
    operation_id = record.get("operation_id")
    spec_hash = record.get("spec_hash")
    state = record.get("state")
    if generation is None:
        raise ValueError("sandbox record generation must be positive")
    if spec_hash != sandbox_spec_fingerprint(parsed_spec):
        raise ValueError("sandbox record spec_hash does not match its spec")
    if not isinstance(state, str) or not state.strip():
        raise ValueError("sandbox record state is required")
    return SandboxInventoryEntry(
        sandbox_id=parsed_spec.id,
        generation=generation,
        operation_id=operation_id,
        spec_hash=spec_hash,
        state=state.strip(),
        resources=parsed_spec.requested_resources(),
    )


def _portable_snapshot_for_route(route: SandboxRoute) -> StorageNativeMigration:
    if not is_portable_parked_route(route):
        raise ValueError("sandbox route is not a fully published park")
    snapshot = StorageNativeMigration.from_dict(route.storage_snapshot)
    manifest = snapshot.manifest
    publication = snapshot.publication
    if (
        manifest.sandbox_id != route.sandbox_id
        or manifest.sandbox_generation != route.generation
        or manifest.create_operation_id != route.create_operation_id
        or sandbox_spec_fingerprint(manifest.spec) != route.spec_hash
        or publication.manifest_digest != route.snapshot_manifest_digest
        or publication.repository != route.snapshot_repository
        or publication.tag != route.snapshot_tag
    ):
        raise ValueError("published snapshot does not match its sandbox route")
    return snapshot


def _route_with_snapshot_payload(
    route: SandboxRoute,
    payload: dict[str, Any],
    *,
    observation: SandboxInventoryEntry | None = None,
) -> SandboxRoute:
    """Apply the one canonical worker-publication validation path."""

    snapshot = StorageNativeMigration.from_dict(payload.get("storage_snapshot"))
    if snapshot.sha256 != str(payload.get("snapshot_sha256") or ""):
        raise ValueError("worker snapshot digest does not match its descriptor")
    base = observation or SandboxInventoryEntry(
        sandbox_id=route.sandbox_id,
        generation=route.generation,
        operation_id=route.create_operation_id,
        spec_hash=route.spec_hash,
        state="parked",
        resources=route.resources,
    )
    item = replace(
        base,
        storage_schema=str(payload.get("storage_schema") or ""),
        snapshot_manifest_digest=str(payload.get("snapshot_manifest_digest") or ""),
        snapshot_repository=str(payload.get("snapshot_repository") or ""),
        snapshot_tag=str(payload.get("snapshot_tag") or ""),
        storage_snapshot=snapshot.to_dict(),
    )
    return route_with_inventory_snapshot(route, item)


def _route_with_sandbox_record(
    route: SandboxRoute,
    record: dict[str, Any],
) -> SandboxRoute:
    observation = _sandbox_inventory_from_record(record)
    if observation.sandbox_id != route.sandbox_id:
        raise ValueError("sandbox record id does not match its route")
    route_state = observation.route_state
    if route_state is None:
        raise ValueError(f"sandbox record state is not routable: {observation.state!r}")
    storage_schema = str(record.get("storage_schema") or "")
    storage_snapshot: dict[str, Any] = {}
    snapshot_manifest_digest = ""
    snapshot_repository = ""
    snapshot_tag = ""
    if storage_schema:
        validated = _route_with_snapshot_payload(
            route,
            record,
            observation=observation,
        )
        storage_schema = validated.storage_schema
        storage_snapshot = dict(validated.storage_snapshot)
        snapshot_manifest_digest = validated.snapshot_manifest_digest
        snapshot_repository = validated.snapshot_repository
        snapshot_tag = validated.snapshot_tag
    elif route_state == "parked":
        storage_schema = route.storage_schema
        storage_snapshot = dict(route.storage_snapshot)
        snapshot_manifest_digest = route.snapshot_manifest_digest
        snapshot_repository = route.snapshot_repository
        snapshot_tag = route.snapshot_tag
    return replace(
        route,
        resources=observation.resources,
        spec=dict(record["spec"]),
        state=route_state,
        generation=observation.generation,
        create_operation_id=observation.operation_id,
        spec_hash=observation.spec_hash,
        delete_operation_id=route.delete_operation_id,
        node_epoch=route.node_epoch,
        activity_epoch=route.activity_epoch,
        storage_schema=storage_schema,
        snapshot_manifest_digest=snapshot_manifest_digest,
        snapshot_repository=snapshot_repository,
        snapshot_tag=snapshot_tag,
        storage_snapshot=storage_snapshot,
    )


def _sandbox_record_is_ready(
    record: dict[str, Any],
) -> bool:
    """Return true only for externally usable lifecycle states."""

    return sandbox_route_state_from_observation(record.get("state")) is not None


def _is_duplicate_sandbox_response(response: ProxiedResponse, sandbox_id: str) -> bool:
    if response.status not in {HTTPStatus.BAD_REQUEST, HTTPStatus.CONFLICT}:
        return False
    error_message = str(response.json().get("error") or "").lower()
    return "already exists" in error_message and sandbox_id.lower() in error_message


def _sandbox_record_matches_spec(
    record: dict[str, Any], requested: SandboxSpec
) -> bool:
    raw_spec = record.get("spec")
    if not isinstance(raw_spec, dict):
        return False
    try:
        existing = SandboxSpec.from_dict(raw_spec)
    except (TypeError, ValueError):
        return False
    return sandbox_specs_match(existing, requested)


def _record_generation(record: object) -> int | None:
    if not isinstance(record, dict):
        return None
    try:
        generation = int(record.get("generation"))
    except (TypeError, ValueError, OverflowError):
        return None
    return generation if generation > 0 else None


def _sandbox_record_matches_route(
    record: dict[str, Any],
    route: SandboxRoute,
    requested: SandboxSpec,
) -> bool:
    if not _sandbox_record_matches_spec(record, requested):
        return False
    try:
        confirmed = _route_with_sandbox_record(route, record)
    except (TypeError, ValueError):
        return False
    return (
        confirmed.generation == route.generation
        and confirmed.create_operation_id == route.create_operation_id
        and confirmed.spec_hash == route.spec_hash
        and route.spec_hash == sandbox_spec_fingerprint(requested)
    )


def _sandbox_create_request_body(spec: SandboxSpec, route: SandboxRoute) -> bytes:
    payload = spec.to_dict()
    payload["_ucloud_operation"] = {
        "operation_id": route.create_operation_id,
        "generation": route.generation,
        "kind": "create",
        "spec_hash": route.spec_hash,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _enrich_sandbox_record(
    record: dict[str, Any],
    heartbeat: NodeHeartbeat,
) -> dict[str, Any]:
    enriched = dict(record)
    enriched["node"] = _node_metadata(heartbeat)
    return enriched


def _route_only_sandbox_record(
    route: SandboxRoute,
    heartbeat: NodeHeartbeat | None,
    *,
    heartbeat_ttl_seconds: int = 120,
) -> dict[str, Any]:
    spec = dict(route.spec)
    if spec.get("id") != route.sandbox_id:
        raise ValueError("sandbox route spec does not match its id")
    image = str(spec.get("image") or "")
    labels = spec.get("labels")
    labels = dict(labels) if isinstance(labels, dict) else {}
    node_fresh = heartbeat is not None and heartbeat.is_fresh(
        utc_now(), heartbeat_ttl_seconds
    )
    cached_state = route.state or "unknown"
    route_absent = _heartbeat_proves_route_absent(
        heartbeat,
        sandbox_id=route.sandbox_id,
        route_created_at=route.created_at,
        route_updated_at=route.updated_at,
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
    )
    visible_state = (
        "parked"
        if route.worker_state == "detached" and is_portable_parked_route(route)
        else cached_state
        if cached_state == "creating" or (node_fresh and not route_absent)
        else "unknown"
    )
    record: dict[str, Any] = {
        "id": route.sandbox_id,
        "state": visible_state,
        "cached_state": cached_state,
        "cached": True,
        "route_only": visible_state != "running",
        "spec": spec,
        "resources": route.resources.to_dict(),
        "labels": {str(key): str(value) for key, value in labels.items()},
        "node": {
            "node_id": route.node_id,
            "job_id": route.job_id,
            "node_url": route.node_url,
            "fresh": node_fresh,
            "attached": route.worker_state == "attached",
        },
        "created_at": route.created_at,
        "updated_at": route.updated_at,
    }
    if image:
        record["image"] = image
    if heartbeat is not None:
        node = _node_metadata(heartbeat)
        node["fresh"] = node_fresh
        node["attached"] = route.worker_state == "attached"
        record["node"] = node
    return record


def _node_metadata(heartbeat: NodeHeartbeat) -> dict[str, Any]:
    return {
        "node_id": heartbeat.node_id,
        "job_id": heartbeat.job_id,
        "node_url": heartbeat.node_url or "",
        "active_sandboxes": heartbeat.active_sandboxes,
    }


def _heartbeat_proves_route_absent(
    heartbeat: NodeHeartbeat | None,
    *,
    sandbox_id: str | None = None,
    route_created_at: str,
    route_updated_at: str,
    heartbeat_ttl_seconds: int,
) -> bool:
    if heartbeat is None:
        return False
    if not heartbeat.is_fresh(utc_now(), heartbeat_ttl_seconds):
        return False
    if (
        sandbox_id is not None
        and heartbeat.inventory_complete
        and any(item.sandbox_id == sandbox_id for item in heartbeat.inventory)
    ):
        # Parked sandboxes consume no active CPU/RAM and therefore correctly
        # report active_sandboxes=0. A complete inventory entry is stronger
        # evidence than that aggregate counter.
        return False
    if heartbeat.active_sandboxes != 0:
        return False
    route_reference = parse_iso_datetime(route_updated_at) or parse_iso_datetime(
        route_created_at
    )
    return route_reference is None or heartbeat.freshness_at >= route_reference


def _node_can_fit(
    heartbeat: NodeHeartbeat,
    requested: ResourceQuantity,
    routes: list[PlacementRecord],
) -> bool:
    return _node_can_fit_available(
        heartbeat,
        requested,
        _node_available_resources(heartbeat, routes),
    )


def _node_can_fit_available(
    heartbeat: NodeHeartbeat,
    requested: ResourceQuantity,
    available: ResourceQuantity,
) -> bool:
    return node_accepts_dynamic_request(heartbeat, requested, available)


def _node_placement_state(
    heartbeat: NodeHeartbeat,
    node_routes: list[PlacementRecord],
) -> NodePlacementState:
    inflight_images = frozenset(
        identity
        for route in node_routes
        if route.state.lower() in {"creating", "unknown"}
        and (identity := _route_image_identity(route))
        and not _heartbeat_has_image(heartbeat, identity)
    )
    projected_images = set(heartbeat.cached_images)
    projected_images.update(
        identity
        for route in node_routes
        if route.state.lower() in {"creating", "unknown", "running"}
        and (identity := _route_image_identity(route))
    )
    return NodePlacementState(
        available_resources=_node_available_resources(heartbeat, node_routes),
        inflight_image_identities=inflight_images,
        projected_image_identities=frozenset(projected_images),
        active_creates=max(
            heartbeat.active_sandbox_creates,
            sum(
                route.state.lower()
                in {
                    "creating",
                    "planned",
                    "quota_ready",
                    "rootfs_ready",
                    "unknown",
                }
                for route in node_routes
            ),
        ),
    )


def _node_available_resources(
    heartbeat: NodeHeartbeat,
    routes: list[PlacementRecord],
) -> ResourceQuantity:
    route_reservations = _node_reserved_route_resources(heartbeat, routes)
    free = heartbeat.free_resources
    disk_mb = max(0, free.disk_mb - route_reservations.disk_mb)
    metrics = heartbeat.runtime_metrics
    if (
        STORAGE_NATIVE_CAPABILITY in heartbeat.capabilities
        and metrics is not None
        and metrics.storage_ublk_max_devices > 0
    ):
        reserved_device_slots = _node_reserved_storage_device_slots(
            heartbeat,
            routes,
        )
        if (
            metrics.storage_ublk_active_devices + reserved_device_slots
            >= metrics.storage_ublk_max_devices
        ):
            disk_mb = 0
    return ResourceQuantity(
        vcpu=max(0.0, free.vcpu - route_reservations.vcpu),
        memory_mb=max(0, free.memory_mb - route_reservations.memory_mb),
        disk_mb=disk_mb,
    )


def _node_reserved_storage_device_slots(
    heartbeat: NodeHeartbeat,
    routes: list[PlacementRecord],
) -> int:
    """Count assigned volumes not yet represented by backend ownership metrics."""

    inventory_identities = {
        (
            item.sandbox_id,
            item.generation,
            item.spec_hash,
            item.operation_id,
        )
        for item in heartbeat.inventory
    }
    seen: set[tuple[str, ...]] = set()
    reserved = 0
    for route in routes:
        if not _route_targets_node(route, heartbeat):
            continue
        identity = _placement_identity(route)
        if identity in seen:
            continue
        seen.add(identity)
        if isinstance(route, SandboxRoute):
            if route.worker_state == "detached":
                continue
            if (
                route.sandbox_id,
                route.generation,
                route.spec_hash,
                route.create_operation_id,
            ) in inventory_identities:
                continue
            if route.resources.disk_mb > 0:
                reserved += 1
        elif route.resources.disk_mb > 0:
            reserved += 1
    return reserved


def _node_reserved_route_resources(
    heartbeat: NodeHeartbeat,
    routes: list[PlacementRecord],
) -> ResourceQuantity:
    resources = ResourceQuantity()
    seen_routes: set[tuple[str, ...]] = set()
    inventory_by_identity: dict[tuple[str, int, str, str], Any] = {}
    for item in heartbeat.inventory:
        inventory_by_identity.setdefault(
            (
                item.sandbox_id,
                item.generation,
                item.spec_hash,
                item.operation_id,
            ),
            item,
        )
    for route in routes:
        if not _route_targets_node(route, heartbeat):
            continue
        identity = _placement_identity(route)
        if identity in seen_routes:
            continue
        seen_routes.add(identity)
        if isinstance(route, PlacementReservation):
            resources = resources + route.resources
            continue
        if route.worker_state == "detached" and is_portable_parked_route(route):
            continue
        matching_inventory = inventory_by_identity.get(
            (
                route.sandbox_id,
                route.generation,
                route.spec_hash,
                route.create_operation_id,
            )
        )
        if matching_inventory is not None:
            if (
                route.state.lower() == "waking"
                and (matching_inventory.state or "unknown").lower() == "parked"
            ):
                storage_disk = (
                    route.resources.disk_mb
                    if route.storage_schema == STORAGE_NATIVE_MIGRATION_SCHEMA
                    and bool(route.snapshot_manifest_digest)
                    else 0
                )
                # Published parked inventory does not charge active disk, so
                # waking must reserve the attached writable volume.
                resources = resources + ResourceQuantity(
                    vcpu=route.resources.vcpu,
                    memory_mb=route.resources.memory_mb,
                    disk_mb=storage_disk,
                )
            continue
        if (
            route.state.lower() == "parked"
            and route.storage_schema == STORAGE_NATIVE_MIGRATION_SCHEMA
            and route.snapshot_manifest_digest
        ):
            continue
        resources = resources + route.resources
    return resources


def _placement_route_index(routes: list[PlacementRecord]) -> PlacementRouteIndex:
    by_node_id: dict[str, list[PlacementRecord]] = {}
    by_job_id: dict[str, list[PlacementRecord]] = {}
    by_node_url: dict[str, list[PlacementRecord]] = {}
    for route in routes:
        if route.node_id:
            by_node_id.setdefault(route.node_id, []).append(route)
        if route.job_id:
            by_job_id.setdefault(route.job_id, []).append(route)
        if route.node_url:
            by_node_url.setdefault(route.node_url.rstrip("/"), []).append(route)
    return PlacementRouteIndex(
        by_node_id={key: tuple(value) for key, value in by_node_id.items()},
        by_job_id={key: tuple(value) for key, value in by_job_id.items()},
        by_node_url={key: tuple(value) for key, value in by_node_url.items()},
    )


def _route_targets_node(route: PlacementRecord, heartbeat: NodeHeartbeat) -> bool:
    return bool(
        (route.node_id and route.node_id == heartbeat.node_id)
        or (route.job_id and route.job_id == heartbeat.job_id)
        or (
            route.node_url
            and heartbeat.node_url
            and route.node_url.rstrip("/") == heartbeat.node_url.rstrip("/")
        )
    )


def _route_image_identity(route: PlacementRecord) -> str:
    image = (
        str(route.spec.get("image") or "")
        if isinstance(route, SandboxRoute)
        else route.image
    ).strip()
    return canonical_image_digest_ref(image) or image


def _placement_identity(route: PlacementRecord) -> tuple[str, ...]:
    if isinstance(route, SandboxRoute):
        return (
            "sandbox",
            route.sandbox_id,
            str(route.generation),
            route.create_operation_id,
        )
    return ("migration", route.reservation_id)


def _cold_image_placement_cost_for_state(
    state: NodePlacementState,
    target_manifest: RegistryManifestLayers | None,
    layer_cache: RegistryLayerMetadataCache | None,
    *,
    spread_cold_image: bool,
) -> tuple[int, int]:
    if not spread_cold_image:
        return (0, 0)
    pressure = max(len(state.inflight_image_identities), state.active_creates)
    if target_manifest is None or layer_cache is None:
        return (1, pressure)
    available_layers: set[str] = set()
    for image_ref in state.projected_image_identities:
        manifest = layer_cache.get(image_ref)
        if manifest is not None:
            available_layers.update(layer.digest for layer in manifest.layers)
    missing_bytes = sum(
        layer.size
        for layer in target_manifest.layers
        if layer.digest not in available_layers
    )
    return (
        0,
        missing_bytes + pressure * COLD_PULL_PRESSURE_PENALTY_BYTES,
    )


def _image_pull_lock(node_url: str, image: str) -> RLock:
    key = (node_url.rstrip("/"), image)
    with _IMAGE_PULL_LOCKS_GUARD:
        lock = _IMAGE_PULL_LOCKS.get(key)
        if lock is None:
            lock = RLock()
            _IMAGE_PULL_LOCKS[key] = lock
        return lock


@contextmanager
def _migration_operation_lock(migration_id: str):
    """Serialize one migration without retaining an unbounded keyed-lock cache."""

    key = migration_id.strip()
    with _MIGRATION_OPERATION_LOCKS_GUARD:
        lock, users = _MIGRATION_OPERATION_LOCKS.get(key, (RLock(), 0))
        _MIGRATION_OPERATION_LOCKS[key] = (lock, users + 1)
    try:
        with lock:
            yield
    finally:
        with _MIGRATION_OPERATION_LOCKS_GUARD:
            current = _MIGRATION_OPERATION_LOCKS.get(key)
            if current is not None and current[0] is lock:
                remaining = current[1] - 1
                if remaining <= 0:
                    _MIGRATION_OPERATION_LOCKS.pop(key, None)
                else:
                    _MIGRATION_OPERATION_LOCKS[key] = (lock, remaining)


@contextmanager
def _gateway_placement_lock(route_path: Path, *, blocking: bool = True):
    """Serialize route accounting and intent persistence across gateways."""

    lock_path = route_path.with_name(route_path.name + ".placement.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        operation = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
        try:
            fcntl.flock(lock_file.fileno(), operation)
        except BlockingIOError as exc:
            raise GatewaySchedulingBusyError(
                "sandbox placement is reserved by another gateway process"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _private_registry_image_coordinates(
    image_ref: str,
) -> tuple[str, str] | None:
    # A host-qualified reference is the strongest signal currently available
    # that this request depends on a registry rather than a public shorthand
    # such as ``ubuntu:latest``. Repositories not present in the managed
    # registry are harmless: their leases never match a prune candidate.
    if not registry_host_from_image_ref(image_ref):
        return None
    return registry_repository_tag_from_image_ref(image_ref)


def _managed_registry_image_coordinates(
    image_ref: str,
    registry_url: str,
    registry_worker_url: str = "",
) -> tuple[str, str] | None:
    """Return coordinates only when the tag targets this managed registry."""

    image_host = registry_host_from_image_ref(image_ref).lower()
    if not image_host:
        return None
    allowed_hosts: set[str] = set()
    for configured_url in (registry_url, registry_worker_url):
        configured_host = urlparse(configured_url).netloc.lower()
        if configured_host:
            allowed_hosts.add(configured_host)
    if image_host not in allowed_hosts:
        return None
    return registry_repository_tag_from_image_ref(image_ref)


def _managed_registry_build_tag(image_id: str, registry_worker_url: str) -> str:
    """Allocate a stable internal tag without exposing registry naming to clients."""

    host = urlparse(registry_worker_url).netloc
    if not host:
        raise ValueError("gateway-managed image builds require a worker registry URL")
    component = "".join(
        character.lower() if character.isalnum() else "-"
        for character in image_id.strip()
    ).strip("-")
    component = component[:40].rstrip("-") or "image"
    suffix = hashlib.sha256(image_id.encode("utf-8")).hexdigest()[:12]
    return f"{host}/ucloud-managed/{component}-{suffix}:latest"


def _managed_registry_worker_reference(
    image_ref: str,
    registry_url: str,
    registry_worker_url: str,
) -> str:
    """Rewrite a managed image reference for worker transport."""

    if not registry_worker_url:
        return image_ref
    coordinates = _managed_registry_image_coordinates(
        image_ref,
        registry_url,
        registry_worker_url,
    )
    worker_host = urlparse(registry_worker_url).netloc
    if coordinates is None or not worker_host:
        return image_ref
    repository, tag = coordinates
    rewritten = f"{worker_host}/{repository}:{tag}"
    digest = manifest_digest_from_image_ref(image_ref)
    return image_ref_with_manifest_digest(rewritten, digest) if digest else rewritten


def _persist_registry_image_protection(
    store: RegistryUsageStore,
    image_ref: str,
    owner: str,
    *,
    touch: bool,
    persistent: bool,
    now: Any | None = None,
    ttl_seconds: float = REGISTRY_IMAGE_LEASE_TTL_SECONDS,
) -> bool:
    """Persist either a durable reference or a finite transient lease."""

    coordinates = _private_registry_image_coordinates(image_ref)
    if coordinates is None:
        return False
    repository, tag = coordinates
    digest = manifest_digest_from_image_ref(image_ref)
    with _REGISTRY_LEASE_COORDINATION_LOCK:
        if touch:
            usage_refs = [image_ref]
            if digest:
                usage_refs.append(f"{repository}:{digest_protection_tag(digest)}")
            touched = store.touch_images(usage_refs, when=now)
            if len(touched) != len(usage_refs):
                raise ValueError("private-registry image could not be recorded")
        timestamp = now or utc_now()
        snapshot = store.snapshot(now=timestamp)
        existing = snapshot.leases.get((repository, tag, owner))
        digest_matches = not digest or (
            existing is not None and existing.digest == digest
        )
        if existing is not None and not existing.expires_at and digest_matches:
            return True
        if persistent:
            store.acquire_reference(
                repository,
                tag,
                owner,
                digest=digest,
                now=timestamp,
            )
            return True
        ttl_seconds = float(ttl_seconds)
        if existing is not None:
            existing_expiry = parse_iso_datetime(existing.expires_at)
            if existing_expiry is not None:
                remaining = max(
                    0.0,
                    (existing_expiry - timestamp).total_seconds(),
                )
                # Heartbeats arrive far more frequently than the lease TTL.
                # Renew only after half the lifetime has elapsed to avoid an
                # fsync/generation bump on every node report.
                if remaining >= ttl_seconds / 2 and digest_matches:
                    return True
                # Never replace an existing lease with an earlier deadline,
                # including leases created with a longer TTL.
                ttl_seconds = max(ttl_seconds, remaining)
        store.acquire_lease(
            repository,
            tag,
            owner,
            ttl_seconds=ttl_seconds,
            digest=digest,
            now=timestamp,
        )
    return True


def _registry_route_reference_owner(
    route: SandboxRoute,
    *,
    deployment_id: str,
    route_generation: int | str | None = None,
) -> str:
    """Return a restart-stable, generation-specific route incarnation owner."""

    effective_generation = (
        route.generation if route_generation is None else route_generation
    )

    identity = {
        "kind": "sandbox-route",
        "version": 1,
        "deployment_id": deployment_id,
        "sandbox_id": route.sandbox_id,
        "node_id": route.node_id,
        "job_id": route.job_id,
        "route_generation": (
            str(effective_generation) if effective_generation is not None else ""
        ),
        "route_created_at": route.created_at,
        "image": str(route.spec.get("image") or ""),
    }
    return _registry_operation_lease_owner("sandbox-route", identity)


def _registry_snapshot_reference_owner(
    route: SandboxRoute,
    *,
    deployment_id: str,
) -> str:
    return _registry_operation_lease_owner(
        "sandbox-snapshot",
        {
            "version": 1,
            "deployment_id": deployment_id,
            "sandbox_id": route.sandbox_id,
            "generation": route.generation,
            "create_operation_id": route.create_operation_id,
            "node_id": route.node_id,
            "job_id": route.job_id,
        },
    )


def _registry_route_image_reference_key(
    route: SandboxRoute,
    *,
    deployment_id: str,
) -> tuple[str, str, str] | None:
    coordinates = _private_registry_image_coordinates(
        str(route.spec.get("image") or "")
    )
    if coordinates is None:
        return None
    repository, tag = coordinates
    return (
        repository,
        tag,
        _registry_route_reference_owner(
            route,
            deployment_id=deployment_id,
            route_generation=route.generation,
        ),
    )


def _registry_snapshot_reference_key(
    route: SandboxRoute,
    *,
    deployment_id: str,
) -> tuple[str, str, str] | None:
    if not route.snapshot_repository or not route.snapshot_tag:
        return None
    return (
        route.snapshot_repository,
        route.snapshot_tag,
        _registry_snapshot_reference_owner(route, deployment_id=deployment_id),
    )


def _registry_route_reference_keys(
    route: SandboxRoute,
    *,
    deployment_id: str,
) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        reference
        for reference in (
            _registry_route_image_reference_key(route, deployment_id=deployment_id),
            _registry_snapshot_reference_key(route, deployment_id=deployment_id),
        )
        if reference is not None
    )


def _release_registry_reference_keys(
    store: RegistryUsageStore,
    references: set[tuple[str, str, str]],
) -> None:
    for repository, tag, owner in sorted(references):
        try:
            with _REGISTRY_LEASE_COORDINATION_LOCK:
                store.release_lease(repository, tag, owner)
        except (AttributeError, OSError, TypeError, ValueError):
            # A leaked durable reference is conservative. Explicit
            # reconciliation may remove it after proving the owner terminal.
            continue


def release_registry_snapshot_reference(
    store: RegistryUsageStore,
    route: SandboxRoute,
    *,
    deployment_id: str,
    keep_route: SandboxRoute | None = None,
) -> None:
    """Release one route's durable snapshot owner, if present."""

    reference = _registry_snapshot_reference_key(
        route,
        deployment_id=deployment_id,
    )
    references = {reference} if reference is not None else set()
    if keep_route is not None:
        keep_reference = _registry_snapshot_reference_key(
            keep_route,
            deployment_id=deployment_id,
        )
        if keep_reference is not None:
            references.discard(keep_reference)
    _release_registry_reference_keys(store, references)


def release_registry_route_references(
    store: RegistryUsageStore,
    route: SandboxRoute,
    *,
    deployment_id: str,
    keep_route: SandboxRoute | None = None,
) -> None:
    """Release route owners that are not shared by a successor route.

    ``keep_route`` makes migration transition and rollback safe even when a
    detached sandbox is re-adopted by the same node and therefore retains one
    or both deterministic Registry owner keys.
    """

    references = set(_registry_route_reference_keys(route, deployment_id=deployment_id))
    if keep_route is not None:
        references.difference_update(
            _registry_route_reference_keys(
                keep_route,
                deployment_id=deployment_id,
            )
        )
    _release_registry_reference_keys(store, references)


def _registry_operation_lease_owner(kind: str, identity: object) -> str:
    encoded = json.dumps(
        {"kind": kind, "identity": identity},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return f"{kind}:v1:{digest}"


def _run_image_warmup_task(
    routing_store: RoutingStore,
    warmup: PendingImageWarmup,
    heartbeat: NodeHeartbeat,
    task_key: tuple[str, str],
    node_control_bearer_token: str,
) -> None:
    try:
        node_url = heartbeat.node_url or ""
        if not node_url:
            return
        payload: dict[str, Any] = {"image": warmup.image}
        if warmup.image_id:
            payload["id"] = warmup.image_id
        with _image_pull_lock(node_url, warmup.image):
            req = request.Request(
                node_url.rstrip("/") + "/v1/images/pull",
                data=json.dumps(payload).encode("utf-8"),
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {node_control_bearer_token}",
                },
            )
            try:
                with _open_node_request(
                    req,
                    timeout=IMAGE_PULL_PROXY_TIMEOUT_SECONDS,
                    authenticated=True,
                ) as response:
                    status = int(response.status)
                    response.read()
            except error.HTTPError as exc:
                status = int(exc.code)
                exc.read()
            except (error.URLError, OSError):
                return
        if 200 <= status < 300:
            updated = routing_store.mark_image_warmup_node(
                warmup.warmup_id,
                heartbeat.node_id,
                expected_image=warmup.image,
                expected_image_id=warmup.image_id,
            )
            if (
                updated is not None
                and _warmup_node_units(heartbeat, updated.resources) >= updated.count
            ):
                routing_store.delete_image_warmup(updated.warmup_id)
    finally:
        with _IMAGE_WARMUP_TASKS_GUARD:
            _IMAGE_WARMUP_TASKS.discard(task_key)


def _heartbeat_has_image(
    heartbeat: NodeHeartbeat,
    image: str,
    image_id: str = "",
    *,
    require_digest: bool = False,
) -> bool:
    if not heartbeat.cached_images_known:
        return False
    image_keys = _requested_image_cache_keys(
        image,
        image_id,
        require_digest=require_digest,
    )
    return bool(image_keys.intersection(heartbeat.cached_images))


def _requested_image_cache_keys(
    image: str,
    image_id: str = "",
    *,
    require_digest: bool = False,
) -> set[str]:
    """Return only cache identities that prove the requested image is present."""

    digest_ref = canonical_image_digest_ref(image)
    if digest_ref:
        return {image.strip(), digest_ref}
    # A mutable host-qualified tag can move independently of a node heartbeat.
    # It must be resolved to a digest (or pulled again) before it is a cache hit.
    if require_digest and registry_host_from_image_ref(image):
        return set()
    return {item for item in (image, image_id, image_id_from_tag(image)) if item}


def _image_record_cache_keys(record: dict[str, Any]) -> set[str]:
    tag = str(record.get("tag") or "")
    image_id = str(record.get("id") or "")
    digest = normalize_manifest_digest(str(record.get("manifest_digest") or ""))
    digest_ref = canonical_image_digest_ref(tag, digest)
    keys = {item for item in (tag, image_id, digest_ref) if item}
    return keys


def _warmup_node_units(
    heartbeat: NodeHeartbeat,
    resources: ResourceQuantity,
) -> int:
    free = heartbeat.free_resources
    units: list[int] = []
    if resources.vcpu > 0:
        units.append(int(free.vcpu // resources.vcpu))
    if resources.memory_mb > 0:
        units.append(free.memory_mb // resources.memory_mb)
    if resources.disk_mb > 0:
        units.append(free.disk_mb // resources.disk_mb)
    if not units:
        return 0
    return max(0, min(units))


def _node_create_may_still_be_running(response: ProxiedResponse) -> bool:
    if response.transport_error_kind == "dns":
        # DNS lookup failed before an HTTP connection could be established, so
        # the node cannot have received or persisted this create operation.
        return False
    return response.status in {408, 425, 429, 500, 502, 503, 504}


def _retryable_image_pull_response(response: ProxiedResponse) -> bool:
    if response.status != HTTPStatus.SERVICE_UNAVAILABLE:
        return False
    payload = response.json()
    return bool(
        payload.get("retryable") is True
        and payload.get("error_code") == "image_pull_failed"
    )


def _precise_elapsed_ms(started: float) -> float:
    return round(max(0.0, (time.monotonic() - started) * 1000), 3)


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


def _node_transport_error_response(reason: object) -> ProxiedResponse:
    message = str(reason)
    lowered = message.lower()
    if isinstance(reason, socket.gaierror) or any(
        marker in lowered
        for marker in (
            "name resolution",
            "name or service not known",
            "nodename nor servname provided",
        )
    ):
        status = HTTPStatus.SERVICE_UNAVAILABLE
        code = "node_dns_unavailable"
        error_message = (
            "sandbox node DNS is temporarily unavailable; its UCloud VM may be "
            "suspended and resuming"
        )
        kind = "dns"
    elif isinstance(reason, (TimeoutError, socket.timeout)) or "timed out" in lowered:
        status = HTTPStatus.GATEWAY_TIMEOUT
        code = "node_request_timeout"
        error_message = "sandbox node request timed out"
        kind = "timeout"
    else:
        status = HTTPStatus.BAD_GATEWAY
        code = "node_transport_error"
        error_message = f"sandbox node request failed: {message}"
        kind = "transport"
    body = json.dumps(
        {
            "error": error_message,
            "code": code,
            "retryable": True,
        }
    ).encode("utf-8")
    return ProxiedResponse(
        status,
        {"Content-Type": "application/json"},
        body,
        transport_error_kind=kind,
    )


def _proxy_response_too_large(max_bytes: int) -> ProxiedResponse:
    body = json.dumps(
        {
            "error": "upstream sandbox node response exceeded the gateway limit",
            "max_bytes": max_bytes,
            "retryable": False,
        }
    ).encode("utf-8")
    return ProxiedResponse(
        HTTPStatus.BAD_GATEWAY,
        {"Content-Type": "application/json"},
        body,
    )


def _proxy_content_length(headers: Any) -> int | None:
    raw = _header_value(headers, "Content-Length").strip()
    if not raw:
        return None
    try:
        length = int(raw)
    except ValueError as exc:
        raise ValueError("invalid Content-Length") from exc
    if length < 0:
        raise ValueError("negative Content-Length")
    return length


def _read_bounded_proxy_body(response: Any, *, max_bytes: int) -> bytes:
    content_length = _proxy_content_length(response.headers)
    if content_length is not None and content_length > max_bytes:
        raise ProxyResponseTooLargeError(
            f"upstream response exceeds the {max_bytes} byte limit"
        )
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = response.read(min(PROXY_STREAM_CHUNK_BYTES, max_bytes + 1 - total))
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise ProxyResponseTooLargeError(
                f"upstream response exceeds the {max_bytes} byte limit"
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _image_build_response_terminal(payload: dict[str, Any]) -> bool:
    build = payload.get("build")
    if not isinstance(build, dict):
        return "image" in payload
    return str(build.get("status") or "").lower() in {"succeeded", "failed"}


def _structured_proxy_error(response: ProxiedResponse) -> dict[str, Any] | None:
    if response.status < 400 or _response_looks_json(response):
        return None
    preview = response.body[:500].decode("utf-8", errors="replace").strip()
    return {
        "error": "upstream sandbox node returned a non-JSON error response",
        "status": int(response.status),
        "retryable": response.status in {408, 425, 429, 500, 502, 503, 504},
        "upstream_content_type": _header_value(response.headers, "Content-Type"),
        "upstream_body_preview": preview,
    }


def _lifecycle_proxy_error(response: ProxiedResponse) -> str:
    detail = ""
    try:
        payload = response.json()
        if isinstance(payload, dict):
            detail = str(payload.get("error") or "").strip()
    except (TypeError, ValueError, json.JSONDecodeError):
        pass
    if not detail:
        detail = response.body[:500].decode("utf-8", errors="replace").strip()
    prefix = f"HTTP {int(response.status)}"
    return f"{prefix}: {detail}" if detail else prefix


def _response_looks_json(response: ProxiedResponse) -> bool:
    content_type = _header_value(response.headers, "Content-Type").lower()
    if "json" in content_type:
        return True
    stripped = response.body.lstrip()
    return stripped.startswith(b"{") or stripped.startswith(b"[")


def _header_value(headers: Any, key: str) -> str:
    try:
        value = headers.get(key, "")
    except AttributeError:
        value = ""
    return str(value or "")


def _image_reference_kind_from_headers(headers: Any) -> str:
    raw = _header_value(headers, IMAGE_REFERENCE_KIND_HEADER).strip().lower()
    if not raw:
        return "auto"
    if raw not in {"auto", "name", "registry"}:
        raise ValueError(
            f"{IMAGE_REFERENCE_KIND_HEADER} must be 'auto', 'name', or 'registry'"
        )
    return raw


def _incomplete_image_inventory_error(image: str) -> dict[str, Any]:
    return {
        "error": (
            "image inventory is temporarily incomplete; image id could not be resolved"
        ),
        "error_code": "image_inventory_incomplete",
        "retryable": True,
        "image_id": image,
    }


def _looks_like_image_id_reference(image: str) -> bool:
    return (
        bool(image.strip())
        and "/" not in image
        and ":" not in image
        and "@" not in image
    )


def _image_record_available_to_sandboxes(record: dict[str, Any]) -> bool:
    return bool(
        record.get("available_to_sandboxes")
        or record.get("pushed")
        or record.get("source") == "registry"
    )


def _image_record_requires_registry_manifest(
    record: dict[str, Any],
    registry_url: str,
    registry_worker_url: str = "",
) -> bool:
    if not _image_record_available_to_sandboxes(record):
        return False
    source = str(record.get("source") or "")
    if not source.startswith("build:"):
        return False
    host = registry_host_from_image_ref(str(record.get("tag") or ""))
    if not host:
        return False
    allowed: set[str] = set()
    for configured_url in (registry_url, registry_worker_url):
        configured = urlparse(configured_url).netloc
        if configured:
            allowed.add(configured)
    return host in allowed


def _image_record_summary(record: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "id": record.get("id"),
        "tag": record.get("tag"),
        "source": record.get("source"),
        "pushed": bool(record.get("pushed")),
        "available_to_sandboxes": _image_record_available_to_sandboxes(record),
    }
    if record.get("manifest_digest"):
        summary["manifest_digest"] = record.get("manifest_digest")
    node = record.get("node")
    if isinstance(node, dict):
        summary["node"] = {
            "node_id": node.get("node_id"),
            "job_id": node.get("job_id"),
        }
    if record.get("location"):
        summary["location"] = record.get("location")
    return summary


def _resource_slack(
    free: ResourceQuantity, requested: ResourceQuantity
) -> tuple[float, int, int]:
    return (
        max(0.0, free.vcpu - requested.vcpu),
        max(0, free.memory_mb - requested.memory_mb),
        max(0, free.disk_mb - requested.disk_mb),
    )


def _has_resource_values(resources: ResourceQuantity) -> bool:
    return resources.vcpu > 0 or resources.memory_mb > 0 or resources.disk_mb > 0
