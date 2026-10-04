from __future__ import annotations

import asyncio
from collections import OrderedDict
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from datetime import datetime, timezone
from http import HTTPStatus
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
from threading import RLock, Thread
import time
from typing import Any, Callable
from urllib import error, request
from urllib.parse import parse_qs, quote, urlparse
from uuid import uuid4

from .registry_disk import (
    REGISTRY_DISK_PRESSURE_ERROR_CODE,
    REGISTRY_DISK_RETRY_AFTER_SECONDS,
    RegistryDiskMonitor,
    registry_disk_pressure_payload,
)
from .image_import import (
    IMPORT_RETRY_AFTER_SECONDS,
    ImageImportSubmitter,
    import_build_context,
    import_image_id,
)
from .placement_accounting import (
    PlacementReservation as PlacementReservation,
    PlacementRecord as PlacementRecord,
    PlacementRouteIndex as PlacementRouteIndex,
    _node_available_resources as _node_available_resources,
    _node_has_storage_device_capacity as _node_has_storage_device_capacity,
    _node_reserved_storage_device_slots as _node_reserved_storage_device_slots,
    _node_reserved_route_resources as _node_reserved_route_resources,
    _placement_route_index as _placement_route_index,
    _route_targets_node as _route_targets_node,
    _placement_identity as _placement_identity,
)

from .worker_receipts import (
    _record_generation as _record_generation,
    _route_with_sandbox_record as _route_with_sandbox_record,
    _sandbox_create_request_body as _sandbox_create_request_body,
    _sandbox_inventory_from_record as _sandbox_inventory_from_record,
    _sandbox_record_matches_route as _sandbox_record_matches_route,
    _sandbox_record_matches_spec as _sandbox_record_matches_spec,
)

from .admission import FairCapacity
from .capabilities import (
    REQUEST_BODY_KEEPALIVE_CAPABILITY,
    STORAGE_NATIVE_CAPABILITY,
    STORAGE_NATIVE_MIGRATION_CAPABILITY,
    SPLIT_CHECKPOINT_CAPABILITY,
    HOST_EROFS_CAPABILITY,
    RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX,
    RUNTIME_CPU_CAPABILITY_PREFIX,
    RESOURCE_PHASE_CAPABILITY,
)
from .build_admission import BUILD_ADMISSION_CAPACITY_LABEL, build_admission_capacity
from .build_context_store import (
    BuildContextBlobStore,
    BuildContextHttpHandler,
    build_context_digest_from_path,
)
from .dashboard import dashboard_asset
from .deployment import agent_version_is_schedulable, service_health
from .storage_native_migration import (
    STORAGE_NATIVE_MIGRATION_SCHEMA,
    SUPPORTED_STORAGE_NATIVE_MIGRATION_SCHEMAS,
    SPLIT_MIGRATION_SCHEMA,
    StorageNativeMigration,
)
# Patch node transport via node_rpc; its pools, limits and opener are not imported here.
from .gateway import node_rpc
from .gateway.auth import _is_sdk_api_request, _token_matches
from .gateway.create import (
    SANDBOX_CREATE_PROXY_TIMEOUT_SECONDS, _is_duplicate_sandbox_response,
    _node_create_definitively_rejected, _node_create_may_still_be_running,
    _node_create_rejection_reason,
)
from .gateway.fleet import _heartbeat_has_image, _node_metadata, _requested_image_cache_keys
from .gateway.groups import GROUP_PATH, group_id_from_path, parse_group_request
from .gateway.heartbeats import PULL_TIMEOUT_SECONDS
from .gateway.image_resolution import (
    TRANSIENT_IMAGE_RESOLUTION_ERROR_CODES, _image_record_available_to_sandboxes,
    _image_reference_kind_from_headers,
)
from .gateway.node_rpc import (
    DEFAULT_MAX_PROXY_ERROR_BYTES, DEFAULT_PROXY_TIMEOUT_SECONDS, NODE_CONNECT_TIMEOUT_SECONDS,
    PROXY_STREAM_CHUNK_BYTES, ProxiedResponse, ProxyResponseTooLargeError, _async_proxy_response,
    _node_request_headers, _node_transport_error_response, _proxy_content_length,
    _proxy_response_too_large, _read_bounded_proxy_body, _structured_proxy_error,
)
from .gateway.placement import (
    GatewaySchedulingBusyError, _has_resource_values, _node_can_fit, _node_can_fit_available,
    _sandbox_required_capabilities,
)
from .gateway.registry_refs import (
    RegistryImageReferenceUnavailable, _managed_registry_build_tag, _managed_registry_worker_reference,
    _portable_snapshot_for_route, _registry_operation_lease_owner,
)
from .gateway.request_parsing import (
    _builder_prepare_id_from_path, _exec_session_id_from_path, _image_build_key_from_path,
    _prepare_id_from_path, _prepared_resources_from_payload, _sandbox_detach_id_from_path,
    _sandbox_id_from_path, _sandbox_migration_id_from_path, _strict_positive_integer,
    _truthy_query_param, _validate_prepared_resources,
)
from .gateway.heartbeats import REBOOT_REAP_TIMEOUT_SECONDS
from .gateway.services import GatewayServices, build_services
from .host_locks import HOST_LOCKS
from .http_server import (
    DEFAULT_MAX_JSON_BODY_BYTES,
    HighBacklogThreadingHTTPServer,
    RequestBodyStream,
    TRANSFER_CHUNK_BYTES,
    traced_http_request,
)
from .http_contract import SandboxHttpRoute, match_sandbox_http_route
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
    RegistryUsageStore,
    canonical_image_digest_ref,
    manifest_digest_from_image_ref,
    normalize_manifest_digest,
)
from .managed_process import ManagedProcessRecord
from .metrics import (
    BufferedMetricsStore,
    GatewayBusySampler,
    MetricsStore,
    build_metrics_snapshot,
    record_sandbox_pending_deleted,
    record_sandbox_scheduled,
)
from .telemetry import Telemetry
from .models import (
    NodeHeartbeat,
    ResourceQuantity,
    SandboxInventoryEntry,
    ScalePolicy,
    is_soft_drained,
    sandbox_route_state_from_observation,
    utc_now,
)
from .control_state import ControlStateStore
from .wake_admission import WakeAdmission
from .wake_placement import (
    WakePlaced, WakePlacement, WakePlacementPorts, WakePlacementStopped, WakeUnavailable,
)
from .lifecycle_commit import (
    InvalidLifecycleReceipt, LifecycleCommitter, LifecycleRouteChanged,
    SnapshotReferences, route_with_snapshot_payload as _route_with_snapshot_payload,
)
from .exec_routing import (ExecRoutingService, ExecRouteUnavailable,
    heartbeat_proves_route_absent as _heartbeat_proves_route_absent)
from .exec_session_routes import EXEC_SESSION_PREFIX_HEADER, ExecSessionRoutes
from .registry import (
    heartbeat_from_dict,
    heartbeat_to_dict,
)
from .routing import (
    open_routing_store,
    cold_offload_fence,
    ExecRoute,
    MAX_PREPARED_CAPACITY_COUNT,
    PendingImageWarmup,
    PendingSandboxDemand,
    ProgramRequestState,
    RoutingStore,
    SandboxGroup,
    SandboxRoute,
    SandboxRouteConflictError,
    is_portable_parked_route,
    is_worker_detachable_parked_route,
)
from .consolidation import can_consolidate_wake, consolidation_rank, observed_memory_mb
from .sandbox import SandboxSpec, sandbox_spec_fingerprint, sandbox_specs_match


_BUILDER_DISPATCH_GUARD = RLock()
_BUILDER_DISPATCH_COUNTS: dict[str, int] = {}
_BUILDER_DISPATCH_INFLIGHT: dict[str, int] = {}
_IMAGE_PULL_LOCKS_GUARD = RLock()
_IMAGE_PULL_LOCKS: dict[tuple[str, str], RLock] = {}
_IMAGE_WARMUP_TASKS_GUARD = RLock()
_IMAGE_WARMUP_TASKS: set[tuple[str, str]] = set()
DEFAULT_MAX_CONCURRENT_SANDBOX_CREATES = 0
DEFAULT_MAX_GATEWAY_HTTP_REQUEST_THREADS = 2048
SANDBOX_CREATE_BUSY_RETRY_AFTER_SECONDS = 2
SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS = 5
# Build execution is asynchronous. This timeout only covers proxying the build
# context and enqueueing the build on a builder node.
IMAGE_BUILD_PROXY_TIMEOUT_SECONDS = 30 * 60
IMAGE_PULL_PROXY_TIMEOUT_SECONDS = 30 * 60
IMAGE_PULL_RETRY_ATTEMPTS = 3
IMAGE_PULL_RETRY_BASE_DELAY_SECONDS = 0.25
SANDBOX_IMAGE_WAIT_SECONDS = 2.0
# On immutable-environment workers a create's "pull" is the image's attach
# (resolve and mount its components): seconds, not an OCI pull's minutes. A
# create waits for it, up to the admission wait, instead of polling the
# durable queue every 2 s (a warm 512 burst deferred ~100 creates/10 s so).
ENVIRONMENT_ATTACH_WAIT_SECONDS = 30.0
MAX_BACKGROUND_CREATE_IMAGE_PULLS = 32
DEFAULT_MAX_PROXY_BODY_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_BUILD_CONTEXT_STORE_BYTES = 2 * 1024 * 1024 * 1024
_BUILD_CONTEXT_PROBE_MIN_BYTES = 1024 * 1024
# Contexts are usually tiny (a Dockerfile); a harness with hundreds of task
# images must not evict the one it was just told exists. Bytes still bound it.
DEFAULT_MAX_BUILD_CONTEXT_ENTRIES = 8192
DEFAULT_MAX_BUILD_CONTEXT_AGE_SECONDS = 24 * 60 * 60
NODE_RECONCILE_PROXY_TIMEOUT_SECONDS = 5
NODE_RECOVERY_PROXY_TIMEOUT_SECONDS = 5
SANDBOX_GENERATION_HEADER = "X-UCloud-Sandbox-Generation"
SANDBOX_OPERATION_ID_HEADER = "X-UCloud-Sandbox-Operation-Id"
SANDBOX_TRANSPORT_RESET_HEADER = "X-UCloud-Sandbox-Transport-Reset"
SANDBOX_TRANSPORT_EPOCH_HEADER = "X-UCloud-Sandbox-Transport-Epoch"
DEFAULT_METRICS_EVENT_LIMIT = 500
FULL_METRICS_EVENT_LIMIT = 10000
METRICS_RESPONSE_CACHE_TTL_SECONDS = 1.0


def _migration_pending_demand_id(sandbox_id: str) -> str:
    return f"__migration__:{sandbox_id}"


def _is_warm_wake_route(route: SandboxRoute | None, generation: int | None = None) -> bool:
    """A resident owner needs no placement; its worker fences the wake itself."""
    return bool(
        route is not None
        and not route.delete_operation_id
        and route.worker_state == "attached"
        and (route.state or "").lower() in {"running", "waking"}
        and (generation is None or route.generation == generation)
    )


def _sandbox_supports_managed_lifecycle(spec: dict[str, Any]) -> bool:
    """Return whether request-bound relay park/wake is valid for this spec."""

    try:
        parsed = SandboxSpec.from_dict(spec)
    except (TypeError, ValueError):
        return False
    return parsed.parkable and parsed.managed_process


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


class SandboxShapeUnschedulableError(ValueError):
    def __init__(
        self,
        requested: ResourceQuantity,
        maximum: ResourceQuantity,
    ) -> None:
        super().__init__("sandbox resources exceed the schedulable node shape")
        self.requested = requested
        self.maximum = maximum


class ImageBuildLookupUnavailableError(RuntimeError):
    """A failed status observation cannot establish that a build is absent."""

    def __init__(self, upstream_status: int = HTTPStatus.SERVICE_UNAVAILABLE) -> None:
        super().__init__("image build status is temporarily unavailable")
        self.status = (
            upstream_status
            if upstream_status in {408, 429, 500, 502, 503, 504}
            else HTTPStatus.BAD_GATEWAY
        )


class CreateImagePullTasks:
    """Share cold pulls without retaining HTTP admission slots indefinitely."""

    def __init__(self, wait_seconds: float | None = None, *, bounded: bool = True) -> None:
        self.lock = RLock()
        self.tasks: dict[tuple[str, ...], Future[ProxiedResponse | None]] = {}
        self.wait_seconds = wait_seconds
        # Awaited attaches always have a waiting create, so creates in flight
        # bound them; only pulls that outlive their 2 s callers need a cap.
        self.bounded = bounded

    def run(
        self, key: tuple[str, ...], pull: Callable[[], ProxiedResponse | None]
    ) -> ProxiedResponse | None:
        with self.lock:
            task = self.tasks.get(key)
            if task is None:
                # Completed results need no durable cache: the node inventory
                # is authoritative, including after eviction or a node restart.
                self.tasks = {k: v for k, v in self.tasks.items() if not v.done()}
                if self.bounded and len(self.tasks) >= MAX_BACKGROUND_CREATE_IMAGE_PULLS:
                    return _create_image_pull_pending_response()
                task = Future()
                self.tasks[key] = task

                def work() -> None:
                    try:
                        task.set_result(pull())
                    except BaseException as exc:
                        task.set_exception(exc)

                try:
                    Thread(target=work, daemon=True, name="create-image-pull").start()
                except BaseException:
                    del self.tasks[key]
                    raise
        try:
            return task.result(timeout=self.wait_seconds or SANDBOX_IMAGE_WAIT_SECONDS)
        except FutureTimeoutError:
            if task.done():
                return task.result()
            return _create_image_pull_pending_response()
        finally:
            with self.lock:
                if task.done() and self.tasks.get(key) is task:
                    del self.tasks[key]


class _LocalWakeBatcher:
    """Coalesce waiting local admissions without a timer or a concurrency cap."""

    def __init__(self):
        self.lock = RLock()
        self.pending = []
        self.running = False

    def reserve(self, handler, route):
        future = Future()
        with self.lock:
            self.pending.append((handler, route, future))
            if not self.running:
                self.running = True
                Thread(target=self._drain, name="local-wake-admission", daemon=True).start()
        observation = (handler.telemetry.span("gateway.wake.await_admission")
                       if handler.telemetry is not None else nullcontext())
        with observation:
            return future.result()

    def _drain(self):
        while True:
            with self.lock:
                if not self.pending:
                    self.running = False
                    return
                leader = self.pending[0][0]
            batch = []
            try:
                # Gather after acquiring placement, so callers that arrived
                # while another reservation held it share this inventory read
                # and durable commit. Creates and migrations use the same lock.
                if leader.routing_store.distributed:
                    with self.lock:
                        batch, self.pending = self.pending, []
                    results = leader.services.placement.atomic(lambda: leader._reserve_local_wake_batch(batch),worker_id=batch[0][1].job_id)
                else:
                    with leader.services.placement.reservation() as span:
                        with self.lock:
                            batch, self.pending = self.pending, []
                        span.set_attribute("gateway.wake.batch_size", len(batch))
                        results = leader._reserve_local_wake_batch(batch)
                for (_, _, future), result in zip(batch, results, strict=True):
                    future.set_result(result)
            except BaseException as exc:
                if not batch:
                    with self.lock:
                        batch, self.pending = self.pending, []
                for _, _, future in batch:
                    future.set_exception(exc)


_LOCAL_WAKE_BATCHERS_LOCK = RLock()
_LOCAL_WAKE_BATCHERS: dict[tuple[Path,str], _LocalWakeBatcher] = {}


def _local_wake_batcher(path: Path, *, owner: str = '') -> _LocalWakeBatcher:
    with _LOCAL_WAKE_BATCHERS_LOCK:
        return _LOCAL_WAKE_BATCHERS.setdefault((path.resolve(),owner), _LocalWakeBatcher())


def _create_image_pull_pending_response() -> ProxiedResponse:
    return ProxiedResponse(
        HTTPStatus.SERVICE_UNAVAILABLE,
        {
            "Content-Type": "application/json",
            "Retry-After": "2",
            "X-UCloud-Sandbox-Retryable": "true",
        },
        json.dumps(
            {
                "error": "sandbox image preparation is still in progress",
                "error_code": "image_warmup_pending",
                "retryable": True,
            }
        ).encode("utf-8"),
    )




class ControlPlaneHandler(BuildContextHttpHandler):
    routing_store: RoutingStore
    gateway_bearer_token: str
    sandbox_api_token: str
    heartbeat_bearer_token: str
    node_control_bearer_token: str
    image_manager: ImageManager
    build_context_store: BuildContextBlobStore
    metrics_store: MetricsStore
    image_build_owners: OrderedDict[str, tuple[str, str, str]] = OrderedDict()
    image_build_owners_lock = RLock()
    image_build_metrics_seen: OrderedDict[tuple[str, str], None] = OrderedDict()
    metrics_response_cache: bytes | None
    metrics_response_cache_at: float
    metrics_response_lock: RLock
    fleet_response_lock: RLock
    fleet_response_future: Future | None
    fleet_status_futures: dict[tuple[str, ...], Future]
    sandbox_create_limiter: FairCapacity | None
    upload_memory_limiter: FairCapacity
    admission_wait_seconds = 30.0
    create_image_pull_tasks: CreateImagePullTasks
    sandbox_create_busy_sampler: GatewayBusySampler
    max_concurrent_sandbox_creates: int
    max_sandbox_resources: ResourceQuantity
    services: GatewayServices
    wake_consolidation_policy: ScalePolicy = ScalePolicy()
    wake_consolidation_next_at: float = 0.0
    server_version = "ucloud-sandboxes-control-plane/0.1"
    routing_write_process = None
    dispatch_environment_roots = False
    create_placement = "ranked"

    @traced_http_request
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if self.path == "/healthz":
            health = service_health("control-plane")
            writer_error = (self.routing_write_process.health_error()
                            if self.routing_write_process is not None else "")
            if writer_error:
                health.update(ok=False, routing_writer={"ok": False, "error": writer_error})
                self._write_json(health, status=HTTPStatus.SERVICE_UNAVAILABLE)
                return
            registry_usage_error = self.services.registry_refs.usage_health_error()
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
                size = self.build_context_store.size_and_touch(context_digest)
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
                for heartbeat in self.services.fleet.store.load_heartbeats().values()
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
            self._write_json({"registry": self.services.images.registry_status()})
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
        outcome = self.services.heartbeats.receive(raw)
        if outcome.accepted:
            self._schedule_image_warmups()
        self._write_json(outcome.payload, status=outcome.status, headers=outcome.headers)

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

    def _read_raw_body(self, *, max_bytes):
        cached = getattr(self, '_placement_request_body', None)
        if cached is not None:
            self._placement_request_body = None
            if len(cached)>max_bytes:
                raise ValueError('placement request exceeds body budget')
            return cached
        return super()._read_raw_body(max_bytes=max_bytes)

    def _defer_placement(self,kind,sandbox_id,path,body):
        headers={key:self.headers[key] for key in
            ('X-UCloud-Image-Reference-Kind','traceparent','tracestate') if key in self.headers}
        client=self.placement_queue
        self.close_connection=True
        self.server.defer_proxy_response(self.request,
            trace_headers=self.telemetry.current_trace_headers(),telemetry=self.telemetry,
            response_provider=lambda:client.response(kind,sandbox_id,path,headers,body))

    def _route_to_nodes(self, path: str) -> bool:
        from .routing import PlacementCommandRejected
        try:
            if getattr(self,'placement_worker',False) and self.command=='POST':
                action=match_sandbox_http_route(self.command,path)
                if path in ('/v1/sandboxes',GROUP_PATH) or (action and action.action=='wake'):
                    body=self._read_raw_body(max_bytes=DEFAULT_MAX_JSON_BODY_BYTES)
                    self._placement_request_body=body
                    with self.routing_store.command_execution(
                        self.headers.get("X-UCloud-Placement-Command"),self.headers.get("X-UCloud-Placement-Claim"),path,body):
                        return self._route_to_nodes_unchecked(path)
            return self._route_to_nodes_unchecked(path)
        except PlacementCommandRejected as exc:
            self._write_json({'error':str(exc),'error_code':'placement_command_rejected','retryable':False},status=409)
            return True
        except sqlite3.DatabaseError as exc:
            self._write_routing_store_unavailable(exc)
            return True
        except RegistryImageReferenceUnavailable as exc:
            self._write_registry_lease_unavailable(exc)
            return True

    def _route_to_nodes_unchecked(self, path: str) -> bool:
        if path == "/v1/sandboxes" and self.command == "GET":
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query, keep_blank_values=True)
            view = query.get("view", ["full"])
            if len(view) != 1 or view[0] not in {"full", "status"}:
                self._write_json({"error": "view must be full or status"}, status=400)
            elif view[0] == "status":
                from .fleet_reader import status_ids
                try:
                    ids = status_ids(query.get("id", []))
                    if _truthy_query_param(parsed, "refresh"):
                        raise ValueError("status view does not support refresh")
                except ValueError as exc:
                    self._write_json({"error": str(exc)}, status=400)
                else:
                    self._list_sandbox_statuses(ids)
            elif "id" in query:
                self._write_json({"error": "id filters require view=status"}, status=400)
            elif _truthy_query_param(parsed, "refresh"):
                self._list_sandboxes_across_nodes()
            else:
                self._list_sandboxes_from_cache()
            return True
        if path == "/v1/sandboxes" and self.command == "POST":
            self._create_sandbox_on_node()
            return True
        if path == GROUP_PATH and self.command == "POST":
            self._create_sandbox_group()
            return True
        group_id = group_id_from_path(path)
        if group_id is not None and self.command in {"GET", "DELETE"}:
            if self.command == "GET":
                status, payload, headers = (*self.services.groups.status(group_id), {})
            else:
                status, payload, headers = self.services.groups.delete(self, group_id)
            self._write_json(payload, status=status, headers=headers)
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
            self._write_json(self.services.images.inventory(self))
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
            if not isinstance(raw, dict) or (raw and set(raw) != {"if_cold"}):
                raise ValueError("sandbox detach payload must be empty or contain if_cold")
            require_cold = bool(raw)
            if require_cold and not isinstance(raw["if_cold"], dict):
                raise ValueError("conditional detach requires an owner observation")
            route = self.routing_store.get_sandbox_readonly(sandbox_id)
            if route is None:
                self._write_missing_sandbox_route(sandbox_id)
                return
            if require_cold:
                expected = cold_offload_fence(route)
                if raw["if_cold"] != expected or any(
                    type(raw["if_cold"][key]) is not type(value) for key, value in expected.items()
                ):
                    raise SandboxRouteConflictError("cold offload observation no longer owns the snapshot")
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
            self.services.registry_refs.ensure_route_reference(route, touch=True)
            fenced = self.routing_store.begin_sandbox_detach(route, require_cold=require_cold)
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
        self.services.registry_refs.ensure_snapshot_reference(
            candidate,
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
            self.services.registry_refs.release_snapshot_reference(
                candidate,
                keep_route=current,
            )
            raise
        if stored is None:
            self.services.registry_refs.release_snapshot_reference(
                candidate,
                keep_route=self.routing_store.get_sandbox_readonly(route.sandbox_id),
            )
            return None, "sandbox route changed while its parked snapshot published"
        self.services.registry_refs.release_snapshot_reference(route, keep_route=stored)
        return stored, ""

    def _finish_sandbox_detach(
        self,
        route: SandboxRoute,
    ) -> tuple[SandboxRoute | None, str]:
        current = self.routing_store.get_sandbox_readonly(route.sandbox_id)
        if current is None:
            return None, "sandbox route disappeared during worker detach"
        if not WakeAdmission.same_incarnation(current, route) or current.delete_operation_id:
            return None, "sandbox route changed during worker detach"
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
            heartbeat_ttl_seconds=self.services.fleet.heartbeat_ttl_seconds,
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
            # An autoscaler drain move is optional work: it never buys capacity.
            soft_drain = raw.get("soft_drain") is True
            migration = self.routing_store.get_sandbox_migration(migration_id)
            if migration is not None and migration.sandbox_id != sandbox_id:
                raise SandboxRouteConflictError(
                    "migration id belongs to another sandbox"
                )
            if migration is None and soft_drain:
                # The gateway learns a worker's publication from its next
                # heartbeat; agents that wake every few seconds are rarely seen
                # as published parks. Ask the owner now, outside the placement
                # lock (the worker reuses an existing publication).
                source = self.routing_store.get_sandbox_readonly(sandbox_id)
                if (
                    source is not None
                    and source.state.lower() == "parked"
                    and source.worker_state == "attached"
                    and not source.delete_operation_id
                    and not is_portable_parked_route(source)
                ):
                    published, error = self._publish_route_for_detach(source)
                    if published is None:
                        self._write_wake_unavailable(WakeUnavailable(
                            error or "sandbox is no longer a parked sandbox",
                            error_code="migration_source_not_parked", retry_after=1,
                        ))
                        return
            if migration is None:
                def reserve_migration():
                    existing = self.routing_store.get_sandbox_migration(migration_id)
                    if existing is not None:
                        if existing.sandbox_id != sandbox_id:
                            raise SandboxRouteConflictError('migration id belongs to another sandbox')
                        return existing
                    source = self.routing_store.get_sandbox_readonly(sandbox_id)
                    if source is None:
                        return WakeUnavailable('sandbox route not found',missing_sandbox_id=sandbox_id)
                    if soft_drain and not is_portable_parked_route(source):
                        return WakeUnavailable('sandbox is no longer a published park',
                            error_code='migration_source_not_parked',retry_after=1)
                    destination = self._select_migration_destination(
                        source,requested_node_id=requested_destination)
                    if destination is None and soft_drain:
                        return WakeUnavailable('no ready destination for soft-drain move',
                            error_code='migration_destination_unavailable',retry_after=1)
                    if destination is None:
                        _,demand = self.routing_store.upsert_pending_with_demand(
                            _migration_pending_demand_id(sandbox_id),
                            ResourceQuantity(disk_mb=source.resources.disk_mb),
                            failure_reason='migration_destination_unavailable')
                        return WakeUnavailable('no ready destination has disk capacity for parked sandbox migration',
                            error_code='migration_destination_unavailable',retry_after=1,
                            pending_resources=demand.pending_resources)
                    return self.routing_store.begin_sandbox_migration(source,migration_id=migration_id,
                        destination_node_id=destination.node_id,destination_job_id=destination.job_id,
                        destination_node_url=destination.node_url or '')
                reserved = self.services.placement.atomic(reserve_migration)
                if isinstance(reserved,WakeUnavailable):
                    self._write_wake_unavailable(reserved)
                    return
                migration = reserved
            assert migration is not None
            self.routing_store.clear_pending(_migration_pending_demand_id(sandbox_id))
            migration_timings_ms: dict[str, float] = {}
            migration = self._prepare_and_advance_sandbox_migration(
                migration,
                timings_ms=migration_timings_ms,
            )
            if migration is None:
                return
        except WakePlacementStopped as stopped:
            self._write_wake_unavailable(stopped.outcome)
            return
        except (SandboxRouteConflictError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except sqlite3.DatabaseError as exc:
            self._write_routing_store_unavailable(exc)
            return
        if migration.phase != "complete":
            aborted = False
            if soft_drain and migration.phase in {"planned", "prepared", "staged"}:
                # A drain move is optional: roll it back at once so the source
                # is never left fenced (unable to wake) by a failed import.
                migration, abort_error = self._abort_sandbox_migration(migration)
                aborted = not abort_error
            self._write_json(
                {
                    "error": migration.error or "sandbox migration is incomplete",
                    "migration": migration.to_dict(),
                    "retryable": True,
                    "aborted": aborted,
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
        consolidation_source: NodeHeartbeat | None = None,
    ) -> NodeHeartbeat | None:
        if consolidation_source is not None and (
            time.monotonic() < self.wake_consolidation_next_at
            or not is_portable_parked_route(source)
        ):
            return None
        routes = self.services.placement.routes()
        active_migrations = self.routing_store.sandbox_migrations(active_only=True)
        if consolidation_source is not None and active_migrations:
            return None
        ready_heartbeats = self.services.fleet.ready_sandbox_heartbeats()
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
        if source_heartbeat is None and source.worker_state == "attached":
            # Closed admission/draining excludes a destination, not a source.
            # Busy workers must still be able to export their parked state.
            owner = self._heartbeat_for_route(job_id=source.job_id)
            if (
                owner is not None and owner.node_id == source.node_id
                and owner.is_fresh(utc_now(), self.services.fleet.heartbeat_ttl_seconds)
            ):
                source_heartbeat = owner
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
        try:
            runtime_capability = _migration_runtime_capability(source, source_heartbeat)
        except ValueError:
            return None
        source_cpu = _migration_cpu_capability(source, source_heartbeat)
        required_destination_capabilities = _sandbox_required_capabilities(source.spec)
        if runtime_capability is not None:
            required_destination_capabilities = (*required_destination_capabilities, runtime_capability)
        if (source.storage_schema == SPLIT_MIGRATION_SCHEMA or (source_is_attached
                and source_heartbeat is not None
                and SPLIT_CHECKPOINT_CAPABILITY in source_heartbeat.capabilities)):
            required_destination_capabilities = (*required_destination_capabilities, SPLIT_CHECKPOINT_CAPABILITY)
        reservations: dict[str, int] = {}
        routes_by_id = {
            route.sandbox_id: route for route in routes if isinstance(route, SandboxRoute)
        }
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
        route_index = _placement_route_index(routes)
        available_by_node: dict[str, ResourceQuantity] = {}
        candidates: list[NodeHeartbeat] = []
        for heartbeat in ready_heartbeats:
            if (
                # A former owner, or any worker still registering this id,
                # would refuse the import as a registration conflict.
                heartbeat.node_id == source.node_id
                or any(item.sandbox_id == source.sandbox_id for item in heartbeat.inventory)
                or STORAGE_NATIVE_CAPABILITY not in heartbeat.capabilities
                or STORAGE_NATIVE_MIGRATION_CAPABILITY not in heartbeat.capabilities
                # A legacy attached source cannot prove compatibility with
                # the new EROFS adapter. Published parks instead carry the
                # exact import fingerprint, including their rootfs ABI.
                or (runtime_capability is None and HOST_EROFS_CAPABILITY in heartbeat.capabilities)
                or any(
                    capability not in heartbeat.capabilities
                    for capability in required_destination_capabilities
                )
                or (requested_node_id and heartbeat.node_id != requested_node_id)
                or not _cpu_compatible(heartbeat, source_cpu)
            ):
                continue
            node_routes = route_index.routes_for(heartbeat)
            available = _node_available_resources(heartbeat, node_routes)
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
            if consolidation_source is not None:
                if not can_consolidate_wake(
                    consolidation_source,
                    heartbeat,
                    source.resources,
                    self.wake_consolidation_policy,
                    now=utc_now(),
                    observed_memory_mb=observed_memory_mb(
                        source, (consolidation_source,), now=utc_now(),
                        max_age_seconds=self.wake_consolidation_policy.live_pressure_window_seconds),
                ) or not _heartbeat_has_image(
                    heartbeat, str(source.spec.get("image") or "")
                ):
                    continue
                # A fresh CPU sample cannot account for in-flight admission.
                if any(
                    route.node_id == heartbeat.node_id
                    and route.worker_state != "detached"
                    and route.state in {"creating", "waking", "unknown"}
                    for route in node_routes
                ):
                    continue
            available_by_node[heartbeat.node_id] = available
            candidates.append(heartbeat)
        undrained = [item for item in candidates if not is_soft_drained(item)]
        # Moves (plain migrations) and optional consolidation never target a
        # soft-drained worker. A wake falls back to it rather than failing.
        if undrained or consolidation_source is not None or not require_active_resources:
            candidates = undrained
        if not candidates:
            return None
        if consolidation_source is not None:
            return min(candidates, key=consolidation_rank)
        image = str(source.spec.get("image") or "")
        return min(
            candidates,
            key=lambda heartbeat: (
                0 if _heartbeat_has_image(heartbeat, image) else 1,
                reservations.get(heartbeat.node_id, 0),
                -available_by_node[heartbeat.node_id].disk_mb,
                heartbeat.node_id,
            ),
        )

    def _prepare_migration_destination_image(self, source: SandboxRoute, destination: NodeHeartbeat) -> None:
        image = str(source.spec.get("image") or "").strip()
        response = self._ensure_image_on_node(destination, image)
        if response is not None and response.status >= 400:
            error = response.json()
            raise WakePlacementStopped(WakeUnavailable(
                "migration destination could not prepare the sandbox image",
                details={"image": image, "node_id": destination.node_id,
                         "node_error": error.get("error") or error}))

    def _prepare_and_advance_sandbox_migration(
        self, migration, *, timings_ms: dict[str, float] | None = None,
        wake_on_complete: bool = False,
    ):
        """Existing migration authority, with domain failures rather than HTTP writes."""
        with _migration_operation_lock(migration.migration_id):
            current = self.routing_store.get_sandbox_migration(migration.migration_id)
            if current is None:
                raise WakePlacementStopped(WakeUnavailable("sandbox migration disappeared"))
            if current.phase == "complete":
                return self.routing_store.complete_sandbox_migration(
                    current.migration_id, wake_destination=wake_on_complete) or current
            if current.phase == "planned":
                source = self.routing_store.get_sandbox_readonly(current.sandbox_id)
                destination = next((heartbeat for heartbeat in self.services.fleet.ready_sandbox_heartbeats()
                    if heartbeat.node_id == current.destination_node_id
                    and heartbeat.job_id == current.destination_job_id
                    and (heartbeat.node_url or "").rstrip("/") == current.destination_node_url.rstrip("/")), None)
                if source is None or destination is None:
                    raise WakePlacementStopped(WakeUnavailable(
                        "migration source or destination is unavailable", migration=current))
                self._prepare_migration_destination_image(source, destination)
            return self._advance_sandbox_migration(
                current, timings_ms=timings_ms, wake_on_complete=wake_on_complete)

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
                        storage_schema=route_snapshot.schema,
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
                    if storage_schema not in SUPPORTED_STORAGE_NATIVE_MIGRATION_SCHEMAS:
                        raise ValueError("unsupported migration storage schema")
                    storage_snapshot = StorageNativeMigration.from_dict(
                        prepared.get("storage_snapshot")
                    )
                    snapshot_sha256 = str(prepared.get("snapshot_sha256") or "")
                    if storage_snapshot.schema != storage_schema:
                        raise ValueError("source schema does not match snapshot descriptor")
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
            if migration.storage_schema not in SUPPORTED_STORAGE_NATIVE_MIGRATION_SCHEMAS:
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
                    != migration.storage_schema
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
                destination_publication = destination_snapshot.reference
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
                self.services.registry_refs.ensure_route_reference(
                    destination_route,
                    touch=True,
                )
            except (
                RegistryImageReferenceUnavailable,
                ValueError,
            ) as exc:
                if destination_route is not None and source_route is not None:
                    self.services.registry_refs.release_route_reference(
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
                self.services.registry_refs.release_route_reference(
                    destination_route,
                    keep_route=current_route,
                )
                raise
            measured["route_commit"] = _precise_elapsed_ms(phase_started)
            if routed is None:
                self.services.registry_refs.release_route_reference(
                    destination_route,
                    keep_route=source_route,
                )
                return self._record_migration_error(
                    migration,
                    error_message="sandbox route changed before migration commit",
                )
            migration, destination_route = routed
            self.services.registry_refs.release_route_reference(
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
                self.services.fleet.store.load_heartbeats(),
                routing_state,
                events,
                heartbeat_ttl_seconds=self.services.fleet.heartbeat_ttl_seconds,
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
            snapshot["registry"] = self.services.images.registry_status_cached(
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
            # Start an import early; creates wait for it, preparation does not.
            image, _ = self._external_image_import(image, wait=False)

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
        # Concurrent public polls need the same observation. Share the scan
        # and JSON encoding, not merely a lock that makes every waiter rescan.
        # There is no TTL: a request after completion takes a new snapshot.
        handler_cls = type(self)
        with handler_cls.fleet_response_lock:
            future = handler_cls.fleet_response_future
            owner = future is None
            if owner:
                future = Future()
                handler_cls.fleet_response_future = future
        if owner:
            try:
                future.set_result(self._sandbox_list_response())
            except BaseException as exc:
                future.set_exception(exc)
                raise
            finally:
                with handler_cls.fleet_response_lock:
                    handler_cls.fleet_response_future = None
        self._write_bytes(future.result(), "application/json")

    def _sandbox_list_response(self) -> bytes:
        reader = getattr(type(self), "fleet_snapshot_reader", None)
        if reader is not None:
            return reader.read()
        fleet = self.services.fleet
        return _sandbox_list_bytes(fleet.store, self.routing_store, fleet.heartbeat_ttl_seconds)

    def _list_sandbox_statuses(self, sandbox_ids: tuple[str, ...]) -> None:
        # Coalesce identical in-flight projections only; full reads and other
        # filters must never receive this response. Completion retains no TTL.
        handler_cls = type(self)
        with handler_cls.fleet_response_lock:
            future = handler_cls.fleet_status_futures.get(sandbox_ids)
            owner = future is None
            if owner:
                future = Future()
                handler_cls.fleet_status_futures[sandbox_ids] = future
        if owner:
            try:
                reader = getattr(handler_cls, "fleet_snapshot_reader", None)
                payload = (reader.read_status(sandbox_ids) if reader is not None else
                           _sandbox_list_bytes(self.services.fleet.store, self.routing_store,
                               self.services.fleet.heartbeat_ttl_seconds, status_only=True,
                               sandbox_ids=sandbox_ids))
                future.set_result(payload)
            except BaseException as exc:
                future.set_exception(exc)
                raise
            finally:
                with handler_cls.fleet_response_lock:
                    del handler_cls.fleet_status_futures[sandbox_ids]
        self._write_bytes(future.result(), "application/json")

    def _list_sandboxes_across_nodes(self) -> None:
        sandboxes: list[dict[str, Any]] = []
        observed_ids: set[str] = set()
        reconciled_node_urls: set[str] = set()
        heartbeats = self.services.fleet.ready_sandbox_heartbeats()
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
                )
            )
            for route in stale_snapshot_routes:
                self.services.registry_refs.release_snapshot_reference(route)
            for route in removed_routes:
                self.services.registry_refs.release_route_reference(route)
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
                            self.services.registry_refs.ensure_route_reference(
                                confirmed,
                                touch=True,
                            )
                        except RegistryImageReferenceUnavailable:
                            current_route = self.routing_store.get_sandbox_readonly(
                                sandbox_id
                            )
                            self.services.registry_refs.release_route_reference(
                                confirmed,
                                keep_route=current_route,
                            )
                            continue
                        try:
                            stored_route = self.routing_store.confirm_sandbox_observation(
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
                            self.services.registry_refs.release_route_reference(
                                confirmed,
                                keep_route=current_route,
                            )
                            raise
                        self.services.registry_refs.release_route_reference(
                            confirmed,
                            keep_route=stored_route,
                        )
                if stored_route is None:
                    continue
                sandboxes.append(_enrich_sandbox_record(record, heartbeat))
                self.services.registry_refs.ensure_route_reference(stored_route, touch=True)
        for route in self.routing_store.sandbox_routes_readonly():
            if route.sandbox_id in observed_ids:
                continue
            if route.node_url.rstrip("/") in reconciled_node_urls:
                continue
            sandboxes.append(
                _route_only_sandbox_record(
                    route,
                    heartbeats_by_node_id.get(route.node_id),
                    heartbeat_ttl_seconds=self.services.fleet.heartbeat_ttl_seconds,
                )
            )
        self._write_json({"sandboxes": sandboxes, "cached": False})

    def _list_image_builds_across_nodes(self) -> None:
        self._write_json({"builds": self._image_build_records_across_nodes()})

    def _get_image_build(self, build_key: str) -> None:
        try:
            builds = self._image_build_records_for_key(build_key)
        except ImageBuildLookupUnavailableError as exc:
            self._write_json(
                {"error": str(exc), "error_code": "image_build_status_unavailable",
                 "retryable": True},
                status=exc.status,
                headers={"Retry-After": "2", "X-UCloud-Sandbox-Retryable": "true"},
            )
            return
        matches = [
            build
            for build in builds
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

    def _image_build_records_for_key(self, build_key: str) -> list[dict[str, Any]]:
        """Read one build, using an incarnation-checked owner hint for exact IDs.

        Image names deliberately search all builders: a later build can own the
        same name elsewhere. Hints are disposable and never cache build state.
        Only explicit 404 responses establish absence. A failed known-owner
        probe is retried without fanning out to unrelated overloaded builders.
        """
        builders = [
            h for h in self.services.fleet.ready_heartbeats() if "image-build" in h.capabilities
        ]
        with self.image_build_owners_lock:
            owner = self.image_build_owners.get(build_key)

        def identity(heartbeat: NodeHeartbeat) -> tuple[str, str, str]:
            return (heartbeat.job_id, heartbeat.node_id, heartbeat.node_epoch)

        def fetch(heartbeat: NodeHeartbeat) -> dict[str, Any] | None:
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/images/builds/" + quote(build_key, safe=""),
                method="GET",
                timeout_seconds=NODE_RECONCILE_PROXY_TIMEOUT_SECONDS,
            )
            if response.status == HTTPStatus.NOT_FOUND:
                return None
            if not 200 <= response.status < 300:
                raise ImageBuildLookupUnavailableError(response.status)
            raw = response.json().get("build")
            if not isinstance(raw, dict) or build_key not in (
                raw.get("build_id"), raw.get("image_id")
            ):
                raise ImageBuildLookupUnavailableError(HTTPStatus.BAD_GATEWAY)
            build = dict(raw)
            build.update(location=heartbeat.node_id, node=_node_metadata(heartbeat))
            if raw.get("build_id") == build_key:
                with self.image_build_owners_lock:
                    self.image_build_owners[build_key] = identity(heartbeat)
                    self.image_build_owners.move_to_end(build_key)
                    while len(self.image_build_owners) > 4096:
                        self.image_build_owners.popitem(last=False)
            return build

        def cached_matches() -> list[dict[str, Any]]:
            return [
                b for b in self._cached_image_build_records()
                if build_key in (b.get("build_id"), b.get("image_id"))
            ]

        def terminal_exact_match() -> list[dict[str, Any]]:
            return [
                b for b in cached_matches()
                if b.get("build_id") == build_key
                and _image_build_response_terminal({"build": b})
            ]

        if owner is not None and not any(
            identity(heartbeat)[:2] == owner[:2] for heartbeat in builders
        ):
            # A missing/stale heartbeat does not prove the owner's build gone.
            # A new incarnation of the same node may still recover its record.
            terminal = terminal_exact_match()
            if terminal:
                return terminal
            raise ImageBuildLookupUnavailableError()

        tried = None
        for heartbeat in builders:
            if identity(heartbeat) == owner:
                tried = identity(heartbeat)
                try:
                    build = fetch(heartbeat)
                except ImageBuildLookupUnavailableError:
                    terminal = terminal_exact_match()
                    if terminal:
                        return terminal
                    raise
                if build is not None and build.get("build_id") == build_key:
                    return [build]
                break
        builds = cached_matches()
        exact = [b for b in builds if b.get("build_id") == build_key]
        if exact:
            return exact
        failure = None
        for heartbeat in builders:
            if identity(heartbeat) == tried:
                continue
            try:
                build = fetch(heartbeat)
            except ImageBuildLookupUnavailableError as exc:
                failure = failure or exc
                continue
            if build is not None:
                builds.append(build)
                if build.get("build_id") == build_key:
                    return [build]
        if failure is not None:
            # Partial image-name results cannot identify the latest build;
            # partial exact-ID discovery cannot establish a negative result.
            raise failure
        return builds

    def _image_build_records_across_nodes(self) -> list[dict[str, Any]]:
        builds = self._cached_image_build_records()
        for heartbeat in self.services.fleet.ready_heartbeats():
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

    def _record_terminal_build_metrics(self, build: dict[str, Any]) -> None:
        """Retain observed terminal timings after an ephemeral builder exits."""
        if build.get("status") not in {"succeeded", "failed"} or not build.get("build_id"):
            return
        history = getattr(self, "build_history", None)
        if history is not None:
            try:
                history.record(build)
            except (OSError, sqlite3.Error, ValueError):
                # Retry on a subsequent observation even if metrics already
                # accepted this result. Never fail a successful client poll.
                pass
        store = getattr(self, "metrics_store", None)
        if store is None:
            return
        key = (str(build["build_id"]), str(build.get("updated_at", "")))
        with self.image_build_owners_lock:
            if key in self.image_build_metrics_seen:
                return
            timings = build.get("timings") or {}
            if not isinstance(timings, dict):
                return
            summary = {name: build.get(name, "") for name in (
                "build_id", "image_id", "status", "created_at", "started_at", "finished_at", "location")}
            summary["timings"] = {
                "total_ms": timings.get("total_ms"),
                **{name: timings[name] for name in ("preparation_ms", "queue_wait_ms", "end_to_end_ms")
                   if type(timings.get(name)) in {int, float}},
                **{name: {k: v for k, v in (timings.get(name) or {}).items()
                          if type(v) in {int, float}}
                   for name in ("phases", "environment") if isinstance(timings.get(name), dict)},
            }
            try:
                store.append("image_build_completed", summary)
            except (OSError, sqlite3.Error, ValueError):
                # Observability must not turn a successful build into a retry.
                return
            self.image_build_metrics_seen[key] = None
            while len(self.image_build_metrics_seen) > 4096:
                self.image_build_metrics_seen.popitem(last=False)

    def _record_successful_build_image(self, build: dict[str, Any]) -> None:
        self._record_terminal_build_metrics(build)
        if build.get("status") != "succeeded":
            return
        raw_image = build.get("image")
        if not isinstance(raw_image, dict) or not _image_record_available_to_sandboxes(
            raw_image
        ):
            return
        raw_image = self.services.images.record_with_digest(raw_image)
        build["image"] = raw_image
        try:
            changed = self.image_manager.store.upsert_if_changed(
                ImageRecord.from_dict(raw_image)
            )
        except ValueError:
            return
        if changed:
            self.services.images.invalidate_inventory()

    @contextmanager
    def _startup_request_admission(self, *, creating: bool = False, weight: int = 1):
        # Queue before reading a body. Uploads reserve bytes independently of
        # creates; wakes and streamed/control requests do not use either lane.
        limiter = (self.sandbox_create_limiter if creating else self.upload_memory_limiter)
        if limiter is not None and not limiter.acquire(
            timeout=self.admission_wait_seconds, weight=weight
        ):
            if creating:
                self.sandbox_create_busy_sampler.record(
                    max_concurrent_sandbox_creates=self.max_concurrent_sandbox_creates,
                )
            # An unread request body must never become a second request on a
            # reused reverse-proxy connection.
            self.close_connection = True
            self._write_json(
                {
                    "error": "gateway admission wait deadline exceeded",
                    "error_code": "gateway_startup_busy",
                    "retryable": True,
                    "max_concurrent_sandbox_creates": self.max_concurrent_sandbox_creates,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                headers={"Retry-After": "1", "X-UCloud-Sandbox-Retryable": "true"},
            )
            yield False
            return
        try:
            yield True
        finally:
            if limiter is not None:
                limiter.release(weight=weight)

    def _create_sandbox_on_node(self) -> None:
        with self._startup_request_admission(creating=True) as admitted:
            if admitted:
                self._create_sandbox_admitted()

    def _create_sandbox_admitted(self) -> None:
        try:
            body = self._read_raw_body(max_bytes=DEFAULT_MAX_JSON_BODY_BYTES)
            raw = json.loads(body.decode("utf-8")) if body else None
            if not isinstance(raw, dict):
                raise ValueError("sandbox payload must be a JSON object")
            spec = SandboxSpec.from_dict(raw)
            if spec.environment_root is not None:
                raise ValueError("environment_root is set by the gateway, not by clients")
            spec.validate()
            requested = spec.requested_resources()
            if not requested.fits_within(self.max_sandbox_resources):
                raise SandboxShapeUnschedulableError(
                    requested,
                    self.max_sandbox_resources,
                )
        except SandboxShapeUnschedulableError as exc:
            self._write_shape_unschedulable(exc)
            return
        except (json.JSONDecodeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        if getattr(self,'placement_queue',None) is not None:
            self._defer_placement('create',spec.id,'/v1/sandboxes',body)
            return
        self._create_sandbox_on_node_locked(spec)

    def _write_shape_unschedulable(self, exc: SandboxShapeUnschedulableError) -> None:
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

    def _create_sandbox_group(self) -> None:
        """C3.2: ``count`` sandboxes of one spec (gateway/groups.py)."""
        try:
            body = self._read_raw_body(max_bytes=DEFAULT_MAX_JSON_BODY_BYTES)
            group = parse_group_request(json.loads(body.decode("utf-8")) if body else None)
            requested = group.member().requested_resources()
            if not requested.fits_within(self.max_sandbox_resources):
                raise SandboxShapeUnschedulableError(requested, self.max_sandbox_resources)
        except SandboxShapeUnschedulableError as exc:
            self._write_shape_unschedulable(exc)
            return
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if self.create_placement != "power_of_k":
            self._write_json({
                "error": "group create requires gateway_create_placement power_of_k",
                "error_code": "sandbox_group_create_unavailable", "retryable": False,
            }, status=HTTPStatus.NOT_IMPLEMENTED)
            return
        if getattr(self, "placement_queue", None) is not None:
            # As a create: the one placement process places it, durably.
            self._defer_placement("group", group.group_id, GROUP_PATH, body)
            return
        limiter = self.sandbox_create_limiter
        weight = min(group.count, limiter.capacity) if limiter is not None else 1
        with self._startup_request_admission(creating=True, weight=weight) as admitted:
            if admitted:
                self._create_sandbox_group_admitted(group)

    def _create_sandbox_group_admitted(self, group_request: Any) -> None:
        with self.telemetry.span("gateway.sandbox_group_create", attributes={
            "sandbox.group.id": group_request.group_id, "sandbox.group.count": group_request.count,
        }) as root:
            group = self.routing_store.sandbox_group(group_request.group_id)
            if group is None:
                # Resolved once: every retry places members from the stored spec.
                spec = self._resolve_create_spec(group_request.member(), root)
                if spec is None:
                    return
                warmup = self._active_image_warmup_for_image(spec.image, spec.requested_resources())
                if warmup is not None:
                    self._write_image_warmup_pending(warmup)
                    return
                template = {key: value for key, value in spec.to_dict().items() if key != "id"}
                group = self.routing_store.ensure_sandbox_group(SandboxGroup(
                    group_request.group_id, group_request.request_hash, template, group_request.count))
            if group.request_hash != group_request.request_hash or group.state != "active":
                root.status = "error"
                self._write_json({
                    "error": f"sandbox group {group_request.group_id} " + (
                        "exists with a different request" if group.state == "active" else "was deleted"),
                    "error_code": "sandbox_group_conflict" if group.state == "active" else "sandbox_group_deleted",
                    "retryable": False,
                }, status=HTTPStatus.CONFLICT)
                return
            status, payload, headers = self.services.groups.create(self, group, group_request.policy, root)
            self._write_json(payload, status=status, headers=headers)

    def _loopback_delete(self, sandbox_id: str) -> tuple[int, dict[str, Any]]:
        """A group member's delete through this gateway's own public path."""
        deletion = request.Request(
            self.loopback_origin() + f"/v1/sandboxes/{quote(sandbox_id, safe='')}", method="DELETE",
            headers={"Authorization": f"Bearer {self.gateway_bearer_token}"},
        )
        try:
            with request.urlopen(deletion, timeout=DEFAULT_PROXY_TIMEOUT_SECONDS) as response:
                status, raw = response.status, response.read()
        except error.HTTPError as exc:
            status, raw = exc.code, exc.read()
            exc.close()
        except OSError as exc:
            return HTTPStatus.SERVICE_UNAVAILABLE, {"error": f"loopback delete failed: {exc}"}
        try:
            payload = json.loads(raw) if raw else {}
        except ValueError:
            payload = {}
        return status, payload if isinstance(payload, dict) else {}

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
                resolved = self._resolve_create_spec(spec, root)
                if resolved is None:
                    return
                spec = resolved

            with self.telemetry.span(
                "gateway.sandbox_existing_route_check",
            ) as span:
                existing = self.routing_store.get_sandbox_readonly(spec.id)
                span.set_attribute("existing_route", existing is not None)
                if existing is not None:
                    if existing.spec and spec.environment_root != existing.spec.get("environment_root"):
                        # A retry keeps the root its route pinned, even across a switch.
                        spec = replace(spec, environment_root=existing.spec.get("environment_root"))
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

            active_warmup = self._active_image_warmup_for_image(
                spec.image,
                requested,
            )
            if active_warmup is not None:
                # A capacity preparation already owns this pull. Keep create
                # requests out of node placement and the per-node pull lock
                # until that work completes; the SDK retries this explicit,
                # short-lived admission response.
                root.set_attribute("outcome", "image_warmup_pending")
                root.set_attribute("image.warmup.id", active_warmup.warmup_id)
                self._write_image_warmup_pending(active_warmup)
                return

            if self.create_placement == "power_of_k":
                self.services.creates.create(
                    self, spec, root, excluded_job_ids=excluded_job_ids,
                    last_failure_reason=last_failure_reason,
                )
                return
            layer_cache = self.services.placement.layer_cache
            if layer_cache is not None:
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
                    manifest = layer_cache.get(spec.image)
                    if manifest is None:
                        layer_cache.hydrate_async((spec.image,))
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
                    placement = self.services.placement.select_and_reserve(
                        spec.id,
                        spec.requested_resources(),
                        image=spec.image,
                        spec=spec.to_dict(),
                        spec_hash=sandbox_spec_fingerprint(spec),
                        excluded_job_ids=excluded_job_ids,
                        lock_timeout=self.admission_wait_seconds,
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
                            "error_code": "gateway_placement_busy",
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
                self._write_no_ready_node(demand, last_failure_reason or "no_ready_node")
                return

            assert route is not None
            if route.node_url.rstrip("/") != (heartbeat.node_url or "").rstrip("/"):
                root.set_attribute("outcome", "concurrent_route_won")
                self._retry_sandbox_create_on_assigned_node(route, spec)
                return
            try:
                self.services.registry_refs.ensure_route_reference(route, touch=True)
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
                    self.services.registry_refs.release_route_reference(removed)
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
                initial_cache_hit = _heartbeat_has_image(
                    heartbeat,
                    spec.image,
                    require_digest=self.services.registry_refs.requires_digest_identity(
                        spec.image
                    ),
                )
                image_response = self._ensure_image_for_create(heartbeat, spec.image)
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
            if (
                image_response is not None
                and image_response.json().get("error_code") == "image_warmup_pending"
            ):
                # Retain the assigned generation while its pull continues.
                # Retrying must never move an ambiguous create to another node.
                root.set_attribute("outcome", "image_warmup_pending")
                self._send_proxied_response(image_response)
                return
            if image_response is not None and image_response.status >= 400:
                rejection_reason = _node_create_rejection_reason(image_response)
                removed = self.routing_store.delete_sandbox_if_current(
                    spec.id,
                    generation=route.generation,
                    create_operation_id=route.create_operation_id,
                )
                if removed is not None:
                    self.services.registry_refs.release_route_reference(removed)
                    self._persist_failed_sandbox_demand(
                        spec,
                        removed,
                        failure_reason=(rejection_reason or f"image_pull_http_{image_response.status}"),
                    )
                if rejection_reason is not None:
                    # No create has been dispatched: a draining node's image
                    # admission rejection is safe to place elsewhere. Preserve
                    # retryable demand when the next worker is still starting.
                    if removed is None:
                        self._write_create_in_progress_response(spec.id)
                        return
                    next_excluded = tuple(dict.fromkeys((*excluded_job_ids, route.job_id)))
                    if self.services.placement.alternate_available(spec, excluded_job_ids=next_excluded):
                        root.set_attribute("outcome", "reselect_after_image_admission_rejection")
                        self._create_sandbox_on_node_locked(
                            spec, excluded_job_ids=next_excluded,
                            last_failure_reason=rejection_reason, image_resolved=True,
                        )
                    else:
                        self._send_proxied_response(image_response)
                    return
                root.status = "error"
                root.set_attribute("outcome", "image_pull_failed")
                self._write_image_pull_failed(image_response)
                return

            refreshed_heartbeat = self._heartbeat_for_route(
                job_id=route.job_id,
            )
            refreshed_available = (
                _node_available_resources(
                    refreshed_heartbeat,
                    self.services.placement.routes_for_node(refreshed_heartbeat),
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
                and refreshed_heartbeat.is_fresh(utc_now(), self.services.fleet.heartbeat_ttl_seconds)
                and not refreshed_heartbeat.draining
                and refreshed_heartbeat.admission_open
                and _node_can_fit_available(
                    refreshed_heartbeat,
                    requested,
                    refreshed_available,
                    check_cpu=False,
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
                self.services.registry_refs.release_route_reference(removed)
                _pending, demand = self.routing_store.upsert_pending_with_demand(
                    spec.id,
                    requested,
                    generation=route.generation,
                    operation_id=route.create_operation_id,
                    spec_hash=route.spec_hash,
                    failure_reason=failure_reason,
                )
                next_excluded = tuple(dict.fromkeys((*excluded_job_ids, route.job_id)))
                if self.services.placement.alternate_available(
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
                    self._write_invalid_create_confirmation()
                    return
                confirmed=self._confirm_sandbox_observation(route)
                if confirmed is None:
                    return
                route=confirmed
                record_sandbox_scheduled(
                    self.metrics_store,
                    sandbox_id=spec.id,
                    route=route,
                    resources=spec.requested_resources(),
                    pending=pending_before,
                )
                self.services.registry_refs.record_image_used(spec.image)
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
                        self.services.registry_refs.release_route_reference(removed)
                        self._persist_failed_sandbox_demand(
                            spec,
                            route,
                            failure_reason=rejection_reason,
                        )
                    next_excluded = tuple(
                        dict.fromkeys((*excluded_job_ids, route.job_id))
                    )
                    if removed is not None and self.services.placement.alternate_available(
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
                        self.services.registry_refs.release_route_reference(removed)
            self._send_proxied_response(response)

    def _write_image_warmup_pending(self, warmup: Any) -> None:
        self._write_json(
            {
                "error": "prepared image warmup is still in progress",
                "error_code": "image_warmup_pending",
                "retryable": True,
                "warmup_id": warmup.warmup_id,
            },
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            headers={"Retry-After": "1", "X-UCloud-Sandbox-Retryable": "true"},
        )

    def _resolve_create_spec(self, spec: SandboxSpec, root: Any) -> SandboxSpec | None:
        """Image reference, external import and root dispatch, once per create
        or group; None once an error response is written."""
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
                return None
            if resolved_image != spec.image:
                spec = replace(spec, image=resolved_image)
                root.set_attribute("resolved_image", resolved_image)
        imported_image, import_error = self._external_image_import(
            spec.image, wait=True,
        )
        if import_error is not None:
            root.set_attribute("outcome", str(import_error.get("error_code")))
            self._write_image_import_error(import_error)
            return None
        if imported_image != spec.image:
            spec = replace(spec, image=imported_image)
            root.set_attribute("imported_image", imported_image)
        if self.dispatch_environment_roots and spec.environment_root is None:
            # Chunk store M2: pin the root for the sandbox's life (plan §3.1).
            dispatched = self.services.registry_refs.dependency_resolver.root(spec.image)
            if dispatched is not None:
                spec = replace(spec, environment_root=dispatched)
                root.set_attribute("environment_root", dispatched)
        return spec

    def _write_no_ready_node(self, demand: Any, error_code: str) -> None:
        self._write_json(
            {
                "error": "no ready node has resources for sandbox request",
                "error_code": error_code,
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

    def _write_image_pull_failed(self, image_response: ProxiedResponse) -> None:
        self._write_json({
            "error": (
                "image is not available on selected sandbox node; pull failed. "
                "For gateway-managed images, resubmit the build by image id "
                "before creating sandboxes."
            ),
            "pull": image_response.json(),
        }, status=HTTPStatus.BAD_GATEWAY)

    def _write_invalid_create_confirmation(self) -> None:
        self._write_json({
            "error": "node create response did not confirm the assigned sandbox generation and spec hash",
            "retryable": True,
        }, status=HTTPStatus.BAD_GATEWAY)

    def _confirm_sandbox_observation(self,route,confirm=None):
        confirmed=(confirm or self.routing_store.confirm_sandbox_observation)(route)
        if confirmed is None:
            self._write_json({'error':'sandbox ownership ended before worker confirmation',
                'error_code':'sandbox_observation_superseded','retryable':False},status=HTTPStatus.GONE)
        return confirmed

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
        route=self._confirm_sandbox_observation(route)
        if route is None:
            return True
        self.services.registry_refs.ensure_route_reference(route, touch=True)
        if pending is not None:
            record_sandbox_scheduled(
                self.metrics_store,
                sandbox_id=spec.id,
                route=route,
                resources=spec.requested_resources(),
                pending=pending,
            )
        self.services.registry_refs.record_image_used(spec.image)
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
        if not self._route_worker_is_fresh(route, pull=True):
            self._write_route_worker_unreachable(route)
            return
        self.services.registry_refs.ensure_route_reference(route, touch=True)
        heartbeat = self._heartbeat_for_route(job_id=route.job_id)
        if heartbeat is None:
            self._write_route_worker_unreachable(route)
            return
        image_response = self._ensure_image_for_create(heartbeat, spec.image)
        if image_response is not None and image_response.status >= 400:
            self._send_proxied_response(image_response)
            return
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
                self._write_invalid_create_confirmation()
                return
            stored = self._confirm_sandbox_observation(_route_with_sandbox_record(route, record))
            if stored is None:
                return
            self.services.registry_refs.ensure_route_reference(stored, touch=True)
            self.services.registry_refs.record_image_used(spec.image)
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
                self.services.registry_refs.release_route_reference(removed)
                self._persist_failed_sandbox_demand(
                    spec,
                    route,
                    failure_reason=rejection_reason,
                )
                excluded_job_ids = (route.job_id,)
                if self.create_placement == "power_of_k" or self.services.placement.alternate_available(
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
            f"/v1/sandboxes?sandbox_id={quote(sandbox_id, safe='')}",
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
        reserved_job_id = ""
        try:
            body = self._read_raw_body(max_bytes=self.max_json_body_bytes)
            if self._write_registry_disk_pressure("image builds"):
                return
            raw = json.loads(body.decode("utf-8")) if body else None
            if not isinstance(raw, dict):
                raise ValueError("image build payload must be a JSON object")
            context_reference = uploaded_build_context_reference(
                raw, self.build_context_store
            )
            if context_reference is not None:
                # Keep it recent while this build is copied to a builder.
                self.build_context_store.touch(context_reference[0])
            spec = ImageBuildSpec.from_dict(raw)
            push = bool(raw.get("push", False))
            refs = self.services.registry_refs
            if not spec.tag.strip():
                if not str(raw.get("id") or "").strip():
                    raise ValueError("gateway-managed image builds require an image id")
                spec = replace(
                    spec,
                    tag=_managed_registry_build_tag(spec.id, refs.registry_worker_url or ""),
                )
                push = True
            elif refs.registry_url and refs.registry_worker_url:
                spec = replace(
                    spec,
                    tag=_managed_registry_worker_reference(
                        spec.tag,
                        refs.registry_url,
                        refs.registry_worker_url,
                    ),
                )
            spec.validate()
            raw = dict(raw)
            raw["tag"] = spec.tag
            raw["push"] = push
            prepared_resolution = None
            prepared_catalog = getattr(self, "prepared_image_catalog", None)
            if prepared_catalog is not None:
                from .prepared_images import resolve_build
                raw, prepared_resolution = resolve_build(
                    prepared_catalog, self.build_context_store, raw, spec,
                    protect=lambda reference: self.services.registry_refs.ensure_image_lease(
                        reference, _registry_operation_lease_owner("prepared-build", {
                            "id": spec.id, "context": raw["context_archive_digest"],
                        }), touch=True,
                    ),
                )
                context_reference = uploaded_build_context_reference(raw, self.build_context_store)
            body = json.dumps(raw, separators=(",", ":")).encode("utf-8")
            with _builder_image_dispatch_lock(spec.id):
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
                        heartbeat = self._select_builder_node(image_id=spec.id, reserve=True)
                        reserved_job_id = heartbeat.job_id if heartbeat else ""
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
                                "error": "no ready builder execution slot is available",
                                "error_code": "builder_not_ready",
                                "retryable": True,
                                "pending_image_builds": pending_builds,
                            },
                            status=HTTPStatus.SERVICE_UNAVAILABLE,
                            headers={"Retry-After": "2", "X-UCloud-Sandbox-Retryable": "true"},
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
                    self.services.registry_refs.protect_build_target(
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
                        if prepared_resolution and 200 <= response.status < 300:
                            response_payload["prepared"] = prepared_resolution
                            response.body = json.dumps(response_payload).encode("utf-8")
                        raw_image = response_payload.get("image")
                        if isinstance(
                            raw_image, dict
                        ) and _image_record_available_to_sandboxes(raw_image):
                            raw_image = self.services.images.record_with_digest(raw_image)
                            response_payload["image"] = raw_image
                            raw_build = response_payload.get("build")
                            if isinstance(raw_build, dict):
                                raw_build["image"] = raw_image
                            response.body = json.dumps(response_payload).encode("utf-8")
                        node_timings = response_payload.get("timings")
                        if isinstance(node_timings, dict):
                            span.add_event("node.timings", node_timings)
                    if isinstance(response_payload.get("build"), dict):
                        self._record_terminal_build_metrics(response_payload["build"])
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
                            self.services.images.invalidate_inventory()
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

        finally:
            if reserved_job_id:
                with _BUILDER_DISPATCH_GUARD:
                    _BUILDER_DISPATCH_INFLIGHT[reserved_job_id] -= 1

    def _ensure_node_build_context(
        self,
        node_url: str,
        reference: tuple[str, int],
    ) -> ProxiedResponse:
        digest, size = reference
        path = f"/v1/image-contexts/{quote(digest, safe=':')}"
        # A small context is re-sent rather than probed: the builder's
        # least-recently-used store could evict a probed context before the
        # build starts (older builders do not refresh it on probe), while an
        # identical upload only refreshes it.
        if size > _BUILD_CONTEXT_PROBE_MIN_BYTES:
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

        self.services.registry_refs.ensure_image_lease(
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
        action = match_sandbox_http_route(self.command, path)
        if action and action.action=='delete' and self.routing_store.distributed:
            self.routing_store.cancel_create_commands(sandbox_id)
        if action and action.action=='wake' and getattr(self,'placement_queue',None) is not None:
            try:
                body=self._read_raw_body(max_bytes=DEFAULT_MAX_JSON_BODY_BYTES)
                raw=json.loads(body)
                if not isinstance(raw,dict) or not raw.get('operation_id') or int(raw.get('generation',0))<1:
                    raise ValueError('wake requires operation_id and generation')
            except (ValueError,TypeError) as exc:
                self._write_json({'error':str(exc)},status=400)
                return
            if not self._warm_wake_route(sandbox_id,int(raw['generation'])):
                self._prefetched_route = None
                self._defer_placement('wake',sandbox_id,path,body)
                return
            # A running owner already holds its capacity: there is no placement
            # to serialize. Forward the worker-fenced wake directly instead of
            # paying the durable queue round trip. Admission re-reads the route
            # and returns to the queue if it parked in between.
            self._placement_request_body=body
            self._warm_wake_fallback=(path,body)
        try:
            weight = max(1, int(self.headers.get("Content-Length", "0")))
            if weight > DEFAULT_MAX_PROXY_BODY_BYTES:
                raise ValueError("request exceeds gateway body limit")
        except ValueError as exc:
            self.close_connection = True
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        buffered_bulk = self.command in {"POST", "PUT", "PATCH"} and (
            weight > 64 * 1024
            or (action is not None and action.action == "files")
        ) and not (self.command == "PUT" and action is not None and action.action == "files")
        if buffered_bulk:
            with self._startup_request_admission(weight=weight) as admitted:
                if admitted:
                    self._route_sandbox_request_admitted(sandbox_id, path)
        else:
            self._route_sandbox_request_admitted(sandbox_id, path)

    def _warm_wake_route(self, sandbox_id: str, generation: int) -> bool:
        try:
            route = self.routing_store.get_sandbox_readonly(sandbox_id)
        except sqlite3.DatabaseError:
            return False  # The durable queue owns availability errors.
        # The admitted path runs immediately (a wake body is never buffered
        # behind admission), so it reuses this read instead of repeating it.
        self._prefetched_route = (sandbox_id, route)
        return _is_warm_wake_route(route, generation)

    def _write_missing_sandbox_route(self, sandbox_id: str) -> None:
        loss = self.routing_store.get_sandbox_loss(sandbox_id)
        if loss is not None:
            self._write_json(
                {
                    "error": "sandbox worker was lost; this sandbox incarnation cannot resume",
                    "error_code": "node_lost",
                    "reason": loss["reason"],
                    "retryable": False,
                    "sandbox_id": sandbox_id,
                    "sandbox_generation": loss["generation"],
                    "lost_at": loss["lost_at"],
                },
                status=HTTPStatus.GONE,
            )
            return
        self._write_json(
            {"error": "sandbox route not found"}, status=HTTPStatus.NOT_FOUND
        )

    def _write_absent_route(self, sandbox_id: str) -> None:
        if self.command == "DELETE":
            pending_before = self.routing_store.get_pending(sandbox_id)
            self.routing_store.delete_sandbox(sandbox_id)
            record_sandbox_pending_deleted(
                self.metrics_store,
                sandbox_id=sandbox_id,
                pending=pending_before,
            )
            self._write_json({"ok": True, "deleted": False})
            return
        self._write_missing_sandbox_route(sandbox_id)

    def _route_sandbox_request_admitted(self, sandbox_id: str, path: str) -> None:
        prefetched, self._prefetched_route = getattr(self, "_prefetched_route", None), None
        if prefetched is not None and prefetched[0] == sandbox_id:
            route = prefetched[1]
        else:
            route = self.routing_store.get_sandbox(sandbox_id)
        fallback, self._warm_wake_fallback = getattr(self, "_warm_wake_fallback", None), None
        if fallback is not None and not _is_warm_wake_route(route):
            # Placement belongs to the durable queue; never reserve it here.
            self._placement_request_body = None
            self._defer_placement("wake", sandbox_id, *fallback)
            return
        if route is None:
            self._write_absent_route(sandbox_id)
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
            if self.command == "PUT" and sandbox_http_route is not None and sandbox_http_route.action == "files":
                length = self._request_content_length(max_bytes=DEFAULT_MAX_PROXY_BODY_BYTES)
                # One transfer chunk is already the streaming adapter's memory
                # bound. Buffer small tools/signals within that same bound so
                # they use the canonical pooled RPC transport; larger uploads
                # still reach the worker before the entire body arrives.
                body = (
                    self._read_raw_body(max_bytes=TRANSFER_CHUNK_BYTES)
                    if length <= TRANSFER_CHUNK_BYTES
                    else RequestBodyStream(self.rfile, length)
                )
            else:
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

        if (lifecycle_action == "wake" and lifecycle_payload.get("request_id")
                and not self._program_wake_started):
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

        if not self._route_worker_is_fresh(route, pull=True):
            if self.routing_store.get_sandbox_readonly(sandbox_id) is None:
                # The pull proved a new boot, whose ingest retired this route.
                self._write_absent_route(sandbox_id)
                return
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
        if self.command == "PUT" and sandbox_http_route is not None and sandbox_http_route.action == "files":
            extra_headers = {
                "Content-Length": str(body.length if isinstance(body, RequestBodyStream) else len(body)),
                SANDBOX_GENERATION_HEADER: str(route.generation),
            }
        if sandbox_http_route is not None and sandbox_http_route.action == "exec":
            exec_routing = getattr(self, "exec_routing", None)
            prefix = exec_routing.signed_prefix(route) if exec_routing is not None else None
            if prefix is not None:
                # Capable workers name the session under this signed route, so
                # later polls need no durable exec route. Others ignore it.
                extra_headers = {EXEC_SESSION_PREFIX_HEADER: prefix}
        # Downloads stream their response; uploads stream the request body and
        # receive a small JSON acknowledgement after the worker commits it.
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
        if (self.command == "PUT" and sandbox_http_route is not None
                and sandbox_http_route.action == "files" and isinstance(proxy_body, bytes)
                and len(proxy_body) <= TRANSFER_CHUNK_BYTES
                and self._defer_node_response(route.node_url, self.path, method="PUT",
                                             body=proxy_body, extra_headers=extra_headers)):
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
                    self.routing_store.sandbox_migrations(
                        active_only=False, sandbox_id=route.sandbox_id,
                    ),
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
        return self._confirm_sandbox_observation(_route_with_sandbox_record(route, record))

    def _prepare_program_lifecycle(
        self,
        route: SandboxRoute,
        action: str,
        payload: dict[str, Any],
    ) -> None:
        self._program_wake_started = False
        self._warm_program_wake_observation = None
        request_id = str(payload.get("request_id") or "").strip()
        if not action or not request_id:
            return
        if action == "wake" and route.state.lower() == "running" and payload.get("durable_lifecycle"):
            # PostgreSQL has already committed this response and wake intent.
            # A running owner needs no placement demand. Persist the local
            # observational timestamps with the outcome, rather than make a
            # warm wake wait for an extra SQLite commit before contacting it.
            self._warm_program_wake_observation = (
                request_id, route.sandbox_id, route.generation, utc_now().isoformat(),
            )
            self._program_wake_started = True
            return
        warm_wake = action == "wake" and route.state.lower() in {"running", "waking"}
        # Warm work has no placement wait between response-ready and dispatch.
        # Persist both timestamps in one transition instead of queueing twice.
        transition = {"response_ready": True} if warm_wake else {}
        program, _changed = self._record_program_request_transition(
            route,
            payload,
            state=("waking" if warm_wake else "model_wait" if action == "park" else "ready_to_wake"),
            **transition,
        )
        self._program_wake_started = warm_wake and program is not None

    def _prepare_wake_placement(self, route: SandboxRoute) -> tuple[SandboxRoute, bool] | None:
        outcome = self._wake_placement().place(route)
        if isinstance(outcome, WakePlaced):
            return outcome.route, outcome.owner_changed
        self._write_wake_unavailable(outcome)
        return None

    def _lifecycle_proxy_body(
        self,
        route: SandboxRoute,
        action: str,
        payload: dict[str, Any],
    ) -> bytes | None:
        node_payload: dict[str, Any] = {
            "operation_id": str(payload["operation_id"]).strip(),
        }
        if payload.get("durable_lifecycle"):
            from .capabilities import RELAY_WAKE_FENCE_CAPABILITY
            # Only capabilities are read; skip copying the full inventory.
            owner = self._heartbeat_for_route(
                job_id=route.job_id, include_inventory=False,
            )
            if owner is None or RELAY_WAKE_FENCE_CAPABILITY not in owner.capabilities:
                self._write_json(
                    {"error": "worker upgrade required for durable relay lifecycle", "retryable": True},
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers={"X-UCloud-Sandbox-Retryable": "true"},
                )
                return None
            node_payload["relay_request_id"] = payload["request_id"]
            node_payload["generation"] = route.generation
            if (action == "park" and payload.get("resource_phase") is not None
                    and RESOURCE_PHASE_CAPABILITY in owner.capabilities):
                node_payload["resource_phase"] = payload["resource_phase"]
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
            if "durable_lifecycle" in payload and (payload["durable_lifecycle"] is not True or not request_id):
                raise ValueError("durable lifecycle requires a request binding")
            if payload.get("resource_phase") is not None:
                if action != "park" or not payload.get("durable_lifecycle"):
                    raise ValueError("resource phase requires a durable relay park")
                from .relay_phase import transport_phase
                payload["resource_phase"] = transport_phase(payload["resource_phase"])
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
        if action == "park" and payload.get("durable_lifecycle"):
            program = self.routing_store.program_request_readonly(request_id)
            if (program is not None and program.sandbox_id == route.sandbox_id
                    and program.sandbox_generation == route.generation
                    and program.rollout_id == rollout_id and program.state != "model_wait"):
                self._write_json({"skipped": True, "reason": "model_result_ready"})
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
            self.services.registry_refs.release_route_reference(removed)
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
            return self._commit_lifecycle_response(route, "wake", response)
        if str(response.json().get("lifecycle_state") or "").lower() == "parked":
            self.routing_store.set_sandbox_state_if_current(
                route,
                expected_states={"waking"},
                state="parked",
            )
        if response.transport_error_kind:
            # Only the internal wake was attempted. The caller's exec, upload
            # or job mutation has not been dispatched, even if the wake reply
            # timed out after resume began. Certify that boundary for existing
            # SDKs without claiming the sandbox is still parked.
            self._write_json(
                {
                    "error": "sandbox wake is unavailable; requested operation has not started",
                    "error_code": "node_restore_busy",
                    "cause_code": response.json().get("code", "node_transport_error"),
                    "retryable": True,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                headers={"Retry-After": "1", "X-UCloud-Sandbox-Retryable": "true"},
            )
            return None
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
            exec_routing = getattr(self, "exec_routing", None)
            if (
                isinstance(session_id, str)
                and session_id
                and not (
                    exec_routing is not None
                    and exec_routing.is_signed_for(session_id, route)
                )
            ):
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
            if action == "park" and response.status == HTTPStatus.CONFLICT:
                try:
                    deferred = response.json()
                except (TypeError, ValueError):
                    deferred = {}
                if (
                    isinstance(deferred, dict)
                    and deferred.get("error_code") == "park_deferred"
                    and deferred.get("retryable") is True
                ):
                    # Retention is expected scheduling, not a failed park.
                    # Keep model_wait without another durable error write.
                    return route
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
        updated = self._commit_lifecycle_response(
            route, action, response, lifecycle_payload=payload if action == "wake" else None,
        )
        if updated is None:
            return None
        if action != "wake":
            self._record_completed_program_lifecycle(updated, action, payload)
        return updated

    def _commit_lifecycle_response(
        self,
        route: SandboxRoute,
        action: str,
        response: ProxiedResponse,
        *, lifecycle_payload: dict[str, Any] | None = None,
    ) -> SandboxRoute | None:
        """Decode worker HTTP evidence and translate domain commit outcomes."""
        commit = LifecycleCommitter(
            self.routing_store,
            heartbeat=lambda job_id: self._heartbeat_for_route(
                job_id=job_id, include_inventory=False),
            snapshots=SnapshotReferences(
                protect=lambda candidate: self.services.registry_refs.ensure_snapshot_reference(
                    candidate, repository=candidate.snapshot_repository,
                    tag=candidate.snapshot_tag, digest=candidate.snapshot_manifest_digest),
                release=self.services.registry_refs.release_snapshot_reference,
            ),
        )
        try:
            try:
                receipt = response.json()
            except (ValueError, TypeError) as exc:
                raise InvalidLifecycleReceipt("invalid node lifecycle response: expected JSON object") from exc
            if not isinstance(receipt, dict):
                raise InvalidLifecycleReceipt("invalid node lifecycle response: expected JSON object")
            if action == "wake":
                outcome = commit.wake(route, receipt, program_transition=(
                    self._program_request_transition_args(
                        route, lifecycle_payload, state="acting", clear_error=True,
                    ) if lifecycle_payload is not None else None))
            else:
                outcome = commit.park(route, receipt)
        except InvalidLifecycleReceipt as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_GATEWAY)
            return None
        except LifecycleRouteChanged as exc:
            self._write_json({"error": str(exc), "retryable": True},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
                headers={"X-UCloud-Sandbox-Retryable": "true"})
            return None
        except RegistryImageReferenceUnavailable as exc:
            self._write_registry_lease_unavailable(exc)
            return None
        if action == "wake" and lifecycle_payload is not None:
            self._record_completed_program_lifecycle(
                outcome.route, action, lifecycle_payload,
                committed=outcome.program_transition)
        return outcome.route

    def _record_completed_program_lifecycle(
        self,
        route: SandboxRoute,
        action: str,
        payload: dict[str, Any],
        *, committed=None,
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
        if committed is None:
            _program, changed = self._record_program_request_transition(
                route, payload, state="acting", clear_error=True,
            )
        else:
            _program, changed = committed
            if changed:
                self.metrics_store.append("program_state_transition", _program.to_dict())
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
            self.services.registry_refs.release_route_reference(removed)
        return True

    def _program_request_transition_args(
        self,
        route: SandboxRoute,
        lifecycle_payload: dict[str, Any],
        *,
        state: str,
        parked_at: str | None = None,
        response_ready: bool = False,
        last_error: str = "",
        clear_error: bool = False,
    ) -> dict[str, Any] | None:
        request_id = str(lifecycle_payload.get("request_id") or "").strip()
        rollout_id = str(lifecycle_payload.get("rollout_id") or "").strip()
        if not request_id or not rollout_id:
            return None
        observed = getattr(self, "_warm_program_wake_observation", None)
        warm_started_at = (
            observed[3] if observed is not None
            and observed[:3] == (request_id, route.sandbox_id, route.generation)
            and state in {"waking", "acting"} else None
        )
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
        return dict(
            request_id=request_id, rollout_id=rollout_id, state=state,
            accepted_at=accepted_at or None, parked_at=parked_at,
            response_ready_at=warm_started_at or (utc_now().isoformat() if response_ready else None),
            **({"wake_started_at": warm_started_at} if warm_started_at else {}),
            last_error=last_error, clear_error=clear_error,
        )

    def _record_program_request_transition(
        self,
        route: SandboxRoute,
        lifecycle_payload: dict[str, Any],
        *,
        state: str,
        parked_at: str | None = None,
        response_ready: bool = False,
        last_error: str = "",
        clear_error: bool = False,
    ) -> tuple[ProgramRequestState | None, bool]:
        args = self._program_request_transition_args(
            route, lifecycle_payload, state=state, parked_at=parked_at,
            response_ready=response_ready, last_error=last_error, clear_error=clear_error,
        )
        if args is None:
            return None, False
        try:
            program, changed = self.routing_store.upsert_program_request_transition_with_change(route, **args)
        except (OSError, sqlite3.Error, ValueError, SandboxRouteConflictError) as exc:
            self.metrics_store.append(
                "program_state_projection_error",
                {
                    "request_id": args["request_id"],
                    "rollout_id": args["rollout_id"],
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

    def _read_worker_heartbeat(self, node_url: str) -> Any:
        response = self._proxy_request(
            node_url, "/v1/heartbeat", method="GET", timeout_seconds=PULL_TIMEOUT_SECONDS)
        return response.json().get("heartbeat") if response.status == HTTPStatus.OK else None

    def _refresh_worker(self, heartbeat: NodeHeartbeat) -> NodeHeartbeat | None:
        return self.services.heartbeats.refresh(heartbeat, self._read_worker_heartbeat)

    def _refresh_wake_capacity(self, route: SandboxRoute) -> None:
        # A blocked owner may be stale, or fresh but full: only a live sample
        # can admit locally. The pull is shared with every other caller.
        owner = self.services.fleet.store.get_heartbeat(route.job_id, include_inventory=False)
        if owner is not None:
            self._refresh_worker(owner)

    def _request_wake_publication(self, route: SandboxRoute) -> dict[str, Any] | None:
        response = self._proxy_request(
            route.node_url, f"/v1/sandboxes/{quote(route.sandbox_id, safe='')}/snapshot/publish",
            method="POST", body=json.dumps({"generation": route.generation}).encode(),
            timeout_seconds=2.0)
        return response.json() if response.status < 400 else None

    @staticmethod
    def _decode_wake_publication(route: SandboxRoute, payload: dict[str, Any]) -> SandboxRoute | None:
        record = payload.get("sandbox")
        if not isinstance(record, dict) or record.get("state") != "parked":
            return None
        if not _sandbox_record_matches_route(record, route, SandboxSpec.from_dict(route.spec)):
            return None
        return _route_with_sandbox_record(route, record)

    def _observe_wake_consolidation(self, route, migration) -> None:
        type(self).wake_consolidation_next_at = time.monotonic() + 60
        if migration is not None:
            self.metrics_store.append("sandbox_wake_consolidation", {
                "sandbox_id": route.sandbox_id, "migration_id": migration.migration_id,
                "source_job_id": route.job_id, "destination_job_id": migration.destination_job_id})

    def _wake_placement(self) -> WakePlacement:
        placement = self.services.placement
        return WakePlacement(self.routing_store, self._wake_admission(), WakePlacementPorts(
            reservation=placement.reservation,
            owner=lambda job_id: self._heartbeat_for_route(job_id=job_id),
            occupants=placement.routes_for_node,
            destination=lambda route, **options: self._select_migration_destination(
                route, requested_node_id="", require_active_resources=True, **options),
            reserve_local=self._reserve_local_wake,
            finish_detach=self._finish_sandbox_detach,
            advance_migration=self._prepare_and_advance_sandbox_migration,
            refresh_capacity=self._refresh_wake_capacity,
            publish=self._request_wake_publication,
            decode_publication=self._decode_wake_publication,
            observe_consolidation=self._observe_wake_consolidation,
            atomic=placement.atomic if self.routing_store.distributed else None,
        ))

    def _reserve_local_wake(self, route):
        if self.routing_store.distributed:
            return _local_wake_batcher(self.routing_store.path,owner=route.job_id).reserve(self,route)
        return _local_wake_batcher(self.routing_store.path).reserve(self, route)

    def _write_wake_unavailable(self, outcome: WakeUnavailable) -> None:
        if outcome.missing_sandbox_id:
            self._write_missing_sandbox_route(outcome.missing_sandbox_id)
            return
        payload = {"error": outcome.message, "retryable": True, **outcome.details}
        if outcome.error_code:
            payload["error_code"] = outcome.error_code
        if outcome.pending_resources is not None:
            payload["pending_resources"] = outcome.pending_resources.to_dict()
        if outcome.migration is not None:
            payload["migration"] = outcome.migration.to_dict()
        self._write_json(payload, status=HTTPStatus.SERVICE_UNAVAILABLE, headers={
            "Retry-After": str(outcome.retry_after), "X-UCloud-Sandbox-Retryable": "true"})

    def _wake_admission(self) -> WakeAdmission:
        return WakeAdmission(
            self.routing_store,
            read_owner=self.services.fleet.store.get_heartbeat,
            read_placement=self.services.placement.routes_for_node,
            can_admit=lambda owner, routes, requested: (
                _node_has_storage_device_capacity(owner, routes)
                and _node_can_fit_available(
                    owner, requested, _node_available_resources(owner, routes),
                )
            ),
            heartbeat_ttl_seconds=self.services.fleet.heartbeat_ttl_seconds,
            consolidation_enabled=self.wake_consolidation_policy.parked_wake_consolidation_enabled,
        )

    def _reserve_local_wake_batch(self, batch):
        decisions = self._wake_admission().reserve_batch([route for _, route, _ in batch])
        return [decision.route for decision in decisions]

    def _route_exec_request(self, session_id: str) -> None:
        try:
            route, heartbeat = self.exec_routing.resolve(session_id, refresh=self._refresh_worker)
        except ExecRouteUnavailable as exc:
            self._write_json(exc.payload, status=exc.status, headers=exc.headers)
            return
        self._pooled_node_body_origin = (
            heartbeat.node_url.rstrip("/")
            if REQUEST_BODY_KEEPALIVE_CAPABILITY in heartbeat.capabilities else None
        )
        try:
            body = (
                self._read_raw_body(max_bytes=DEFAULT_MAX_PROXY_BODY_BYTES)
                if self.command in {"POST", "PUT", "PATCH"}
                else None
            )
        except ValueError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if (self.command == "GET" and urlparse(self.path).path.endswith("/events")
                and self.async_responses is not None):
            # This route has one transport selected at server bootstrap. After
            # parsing/auth/authoritative resolution, only asynchronous I/O remains.
            if self.headers.get("Transfer-Encoding") or self.headers.get("Content-Length", "0") != "0":
                self._write_json({"error": "event polls must not contain a request body"},
                                 status=HTTPStatus.BAD_REQUEST)
                return
            self._defer_node_response(route.node_url, self.path, method="GET", event_poll=True)
            return
        response = self._proxy_request(
            route.node_url,
            self.path,
            method=self.command,
            body=body,
        )
        self._send_proxied_response(response)

    def _defer_node_response(self, node_url, path, *, method, body=None,
                             extra_headers=None, event_poll=False) -> bool:
        """Detach only an authorized RPC with no gateway response projection.

        Buffered upload bytes move from the bounded HTTP handler to the existing
        memory budget. If that budget is occupied, retain the handler's bounded
        synchronous path; never queue/replay a mutation after dispatch.
        """
        if self.async_responses is None:
            return False
        weight = max(1, len(body)) if body is not None else 0
        limiter = self.upload_memory_limiter
        if weight and not limiter.acquire(blocking=False, weight=weight):
            return False
        release = (lambda: limiter.release(weight=weight)) if weight else None
        try:
            proxied = self._build_proxy_request(
                node_url, path, method=method, body=body, extra_headers=extra_headers,
            )
            headers = _node_request_headers(proxied, allow_body_keep_alive=(
                getattr(self, "_pooled_node_body_origin", None) == node_url.rstrip("/")
            ))
            self.server.defer_proxy_response(
                self.request, url=proxied.full_url, headers=headers,
                trace_headers=self.telemetry.current_trace_headers(), telemetry=self.telemetry,
                method=method, body=body, event_poll=event_poll, release=release,
            )
            self.close_connection = True
            return True
        except BaseException as exc:
            if release is not None:
                release()
            if not isinstance(exc, (OSError, error.URLError, RuntimeError)):
                raise
            self._send_proxied_response(_node_transport_error_response(exc))
            return True

    def _heartbeat_for_route(self, *, job_id: str, include_inventory: bool = True) -> NodeHeartbeat | None:
        # Last lookup wins: the next node RPC of this request may keep its
        # connection after a framed body only for the worker just resolved.
        fleet = self.services.fleet
        heartbeat = fleet.heartbeat_for_route(job_id=job_id, include_inventory=include_inventory)
        self._pooled_node_body_origin = fleet.body_keepalive_origin(heartbeat)
        return heartbeat

    def _route_worker_is_fresh(self, route: SandboxRoute | ExecRoute, *, pull: bool = False) -> bool:
        heartbeat = self._heartbeat_for_route(
            job_id=route.job_id,
            include_inventory=False,
        )
        fleet = self.services.fleet
        if pull and heartbeat is not None and not heartbeat.is_fresh(utc_now(), fleet.heartbeat_ttl_seconds):
            # Silence is never loss: every sandbox_worker_unreachable answer
            # follows one shared pull of the worker (HeartbeatIngest.refresh).
            heartbeat = self._refresh_worker(heartbeat)
            self._pooled_node_body_origin = fleet.body_keepalive_origin(heartbeat)
        return bool(
            heartbeat is not None
            and heartbeat.node_url
            and heartbeat.is_fresh(utc_now(), fleet.heartbeat_ttl_seconds)
        )

    def _write_route_worker_unreachable(self, route: SandboxRoute | ExecRoute) -> None:
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

    def _external_image_import(
        self, image: str, *, wait: bool,
    ) -> tuple[str, dict[str, Any] | None]:
        """Map an external image to its imported managed copy (docs/image-import.md).

        Only fleets whose workers run immutable environments import; their
        workers cannot run an image without a signed attachment. Returns the
        pinned managed reference once the import is published. Otherwise it
        submits the import and, when ``wait`` is set, returns a pending error.
        """

        submitter = getattr(self, "image_import_submitter", None)
        image = image.strip()
        if submitter is None or not image:
            return image, None
        images = self.services.images
        if images.is_managed(image):
            return image, None
        import_id = import_image_id(image)
        resolved, resolution_error = images.resolve(self, import_id, reference_kind="name")
        if resolution_error is None and manifest_digest_from_image_ref(resolved):
            return resolved, None
        pressure = images.disk_refusal()
        if pressure is not None:
            # The import would push its layers and environment into a full
            # registry; the create retries once retention has freed space.
            return image, (
                registry_disk_pressure_payload(pressure, action="image imports")
                | {"image": image, "import_id": import_id}
                if wait else None
            )
        failure = self._image_import_failure(import_id)
        submitter.ensure_submitted(import_id, image)
        if not wait:
            return image, None
        if failure:
            return image, {
                "error": f"importing {image} failed: {failure}",
                "error_code": "image_import_failed",
                "retryable": False,
                "image": image,
                "import_id": import_id,
            }
        return image, {
            "error": f"image {image} is being imported for immutable workers",
            "error_code": "image_import_pending",
            "retryable": True,
            "image": image,
            "import_id": import_id,
        }

    def _image_import_failure(self, import_id: str) -> str:
        try:
            records = self._image_build_records_for_key(import_id)
        except Exception:
            return ""
        if not records:
            return ""
        latest = sorted(
            records,
            key=lambda item: (str(item.get("created_at") or ""), str(item.get("build_id") or "")),
        )[-1]
        if str(latest.get("status") or "") != "failed":
            return ""
        return str(latest.get("error") or latest.get("log_tail") or "build failed")[-500:]

    def _write_image_import_error(self, payload: dict[str, Any]) -> None:
        retryable = payload.get("retryable") is True
        retry_after = (
            REGISTRY_DISK_RETRY_AFTER_SECONDS
            if payload.get("error_code") == REGISTRY_DISK_PRESSURE_ERROR_CODE
            else IMPORT_RETRY_AFTER_SECONDS
        )
        self._write_json(
            payload,
            status=HTTPStatus.SERVICE_UNAVAILABLE if retryable else HTTPStatus.BAD_REQUEST,
            headers=(
                {"Retry-After": str(retry_after), "X-UCloud-Sandbox-Retryable": "true"}
                if retryable else None
            ),
        )

    def _write_registry_disk_pressure(self, action: str) -> bool:
        """Refuse a registry write before dispatch; True when refused."""

        pressure = self.services.images.disk_refusal()
        if pressure is None:
            return False
        self._write_json(
            registry_disk_pressure_payload(pressure, action=action),
            status=HTTPStatus.SERVICE_UNAVAILABLE,
            headers={
                "Retry-After": str(REGISTRY_DISK_RETRY_AFTER_SECONDS),
                "X-UCloud-Sandbox-Retryable": "true",
            },
        )
        return True

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

    def _resolve_request_image_reference(self, image: str) -> tuple[str, dict[str, Any] | None]:
        """Resolve with the reference kind this request's header selects."""
        try:
            reference_kind = _image_reference_kind_from_headers(self.headers)
        except ValueError as exc:
            return image, {
                "error": str(exc),
                "error_code": "invalid_image_reference_kind",
                "retryable": False,
            }
        return self.services.images.resolve(self, image, reference_kind=reference_kind)

    def _image_cache_candidates(
        self,
        *,
        resources: ResourceQuantity,
        sandbox_nodes_only: bool,
    ) -> list[NodeHeartbeat]:
        routes = list(self.routing_store.sandbox_routes_readonly())
        candidates = []
        for heartbeat in self.services.fleet.ready_heartbeats():
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

    def _select_builder_node(
        self, *, image_id: str = "", reserve: bool = False,
    ) -> NodeHeartbeat | None:
        candidates = [
            heartbeat
            for heartbeat in self.services.fleet.ready_heartbeats()
            if "image-build" in heartbeat.capabilities
            and "sandbox" not in heartbeat.capabilities
            and agent_version_is_schedulable(heartbeat.agent_version)
        ]
        if not candidates:
            return None
        with _BUILDER_DISPATCH_GUARD:
            live_ids = {h.job_id for h in candidates}
            for job_id in list(_BUILDER_DISPATCH_COUNTS):
                if job_id not in live_ids and not _BUILDER_DISPATCH_INFLIGHT.get(job_id):
                    _BUILDER_DISPATCH_COUNTS.pop(job_id, None)
                    _BUILDER_DISPATCH_INFLIGHT.pop(job_id, None)
            baseline = {
                h.job_id: _BUILDER_DISPATCH_COUNTS.get(h.job_id, 0)
                - _BUILDER_DISPATCH_INFLIGHT.get(h.job_id, 0)
                for h in candidates
            }
        # A retry must reach the active build's owner even when that node is
        # busier than its peers. Probe before balancing new work so node-local
        # build deduplication and conflicting-spec checks still apply.
        if image_id:
            for heartbeat in candidates:
                response = self._proxy_request(
                    heartbeat.node_url or "",
                    f"/v1/images/builds/{quote(image_id, safe='')}",
                    method="GET",
                    timeout_seconds=NODE_RECONCILE_PROXY_TIMEOUT_SECONDS,
                )
                if response.status == HTTPStatus.NOT_FOUND:
                    continue
                if response.status != HTTPStatus.OK:
                    return None
                build = response.json().get("build")
                if not isinstance(build, dict) or build.get("status") not in {
                    "running",
                    "succeeded",
                    "failed",
                }:
                    return None
                if (build["status"] == "running"
                        or build.get("admission_phase") in {"preparing_solving", "finishing"}):
                    # Joining/conflicting with an existing build is allowed
                    # even at capacity, including terminal cleanup ownership;
                    # only new work needs another slot.
                    return _reserve_builder_candidate(
                        [heartbeat], baseline, reserve=reserve, allow_full=True,
                    )
        # Periodic heartbeats can lag an entire burst, including on a one-node
        # pool. Keep new work pending at the gateway until a live slot exists;
        # accepted local queues otherwise strand work when peers become free.
        refreshed = []
        for heartbeat in candidates:
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/heartbeat",
                method="GET",
                timeout_seconds=NODE_RECONCILE_PROXY_TIMEOUT_SECONDS,
            )
            raw = response.json().get("heartbeat")
            if response.status != HTTPStatus.OK or not isinstance(raw, dict):
                continue
            try:
                current = heartbeat_from_dict(raw)
            except (ValueError, TypeError):
                continue
            if (
                current is None
                or current.job_id != heartbeat.job_id
                or current.node_id != heartbeat.node_id
                or current.deployment_id != heartbeat.deployment_id
                or current.node_epoch != heartbeat.node_epoch
                or current.draining
                or not current.admission_open
            ):
                continue
            # Keep gateway-owned metadata (for example quarantine labels),
            # but take this scheduling hint from the same live sample as the
            # admitted count. A legacy live response must clear a stale hint.
            labels = dict(heartbeat.labels)
            labels.pop(BUILD_ADMISSION_CAPACITY_LABEL, None)
            if BUILD_ADMISSION_CAPACITY_LABEL in current.labels:
                labels[BUILD_ADMISSION_CAPACITY_LABEL] = current.labels[
                    BUILD_ADMISSION_CAPACITY_LABEL
                ]
            refreshed.append(
                replace(
                    heartbeat,
                    active_image_builds=current.active_image_builds,
                    physical_disk_free_mb=current.physical_disk_free_mb,
                    labels=labels,
                )
            )
        return _reserve_builder_candidate(refreshed, baseline, reserve=reserve)

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
        node_ids = (
            self.services.fleet.nodes_with_cached_image(image, heartbeats, image_id=image_id)
            if use_heartbeat_cache else set()
        )
        uncached = [
            heartbeat for heartbeat in heartbeats
            if not (use_heartbeat_cache and heartbeat.cached_images_known)
        ]
        if not probe_uncached or not uncached:
            return node_ids
        # Only workers whose heartbeat cache is unknown cost a node RPC.
        image_keys = _requested_image_cache_keys(
            image, image_id, require_digest=self.services.registry_refs.requires_digest_identity(image),
        )
        for heartbeat in uncached:
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
        heartbeats = self.services.fleet.ready_sandbox_heartbeats()
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

    def _active_image_warmup_for_image(
        self,
        image: str,
        requested: ResourceQuantity,
    ) -> PendingImageWarmup | None:
        requested_keys = _requested_image_cache_keys(
            image,
            "",
            require_digest=self.services.registry_refs.requires_digest_identity(image),
        )
        if not requested_keys:
            return None
        with _IMAGE_WARMUP_TASKS_GUARD:
            active_warmup_ids = {warmup_id for warmup_id, _node_id in _IMAGE_WARMUP_TASKS}
        if not active_warmup_ids:
            return None
        matching_warmup: PendingImageWarmup | None = None
        for warmup in self.routing_store.image_warmups():
            if warmup.warmup_id not in active_warmup_ids:
                continue
            warmup_keys = _requested_image_cache_keys(
                warmup.image,
                warmup.image_id,
                require_digest=self.services.registry_refs.requires_digest_identity(
                    warmup.image
                ),
            )
            if requested_keys.intersection(warmup_keys):
                matching_warmup = warmup
                break
        if matching_warmup is None:
            return None

        routes = self.services.placement.routes()
        for heartbeat in self.services.fleet.ready_sandbox_heartbeats():
            if not _heartbeat_has_image(
                heartbeat,
                image,
                require_digest=self.services.registry_refs.requires_digest_identity(
                    image
                ),
            ):
                continue
            available = _node_available_resources(heartbeat, routes)
            if _node_can_fit_available(heartbeat, requested, available):
                return None
        return matching_warmup

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
                require_digest=self.services.registry_refs.requires_digest_identity(
                    warmup.image
                ),
            ):
                if heartbeat.node_id not in warmed_node_ids:
                    self.routing_store.mark_image_warmup_node(
                        warmup.warmup_id,
                        heartbeat.node_id,
                        expected_image=warmup.image,
                        expected_image_id=warmup.image_id,
                    )
                    warmed_node_ids.add(heartbeat.node_id)
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
            self.services.registry_refs.ensure_image_lease(
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
            require_digest=self.services.registry_refs.requires_digest_identity(image),
        )
        if use_heartbeat_cache and heartbeat.cached_images_known:
            return bool(image_keys.intersection(heartbeat.cached_images))
        return heartbeat.node_id in self._nodes_with_image(
            image,
            [heartbeat],
            image_id=image_id,
            use_heartbeat_cache=use_heartbeat_cache,
        )

    def _ensure_image_for_create(
        self, heartbeat: NodeHeartbeat, image: str
    ) -> ProxiedResponse | None:
        if not image.strip() or _heartbeat_has_image(
            heartbeat,
            image,
            require_digest=self.services.registry_refs.requires_digest_identity(image),
        ):
            return None

        def pull() -> ProxiedResponse | None:
            # The route can be canceled while this task runs. Protect the
            # registry image independently of that route's lifetime.
            self.services.registry_refs.ensure_image_lease(
                image,
                _registry_operation_lease_owner("create-image-pull", key),
                touch=True,
            )
            return self._ensure_image_on_node(heartbeat, image)

        key = (
            heartbeat.job_id,
            heartbeat.node_epoch,
            heartbeat.node_url or "",
            image,
        )
        return self.create_image_pull_tasks.run(key, pull)

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
                    self.services.images.invalidate_inventory()
                return response
            if attempt + 1 < IMAGE_PULL_RETRY_ATTEMPTS:
                time.sleep(IMAGE_PULL_RETRY_BASE_DELAY_SECONDS * (2**attempt))
        assert response is not None
        if 200 <= response.status < 300:
            self.services.images.invalidate_inventory()
        return response

    def _proxy_request(
        self, node_url: str, path: str, *, method: str, body: Any = None,
        timeout_seconds: float = DEFAULT_PROXY_TIMEOUT_SECONDS,
        extra_headers: dict[str, str] | None = None,
    ) -> ProxiedResponse:
        # Transport flags stay on this request: the body-reuse origin it last
        # resolved, and whether a streamed upload consumed the request body.
        def upload_consumed() -> None:
            self._request_body_consumed = True

        return node_rpc.proxy(
            self._build_proxy_request(
                node_url, path, method=method, body=body, extra_headers=extra_headers,
            ),
            node_url, path, method=method, body=body, timeout_seconds=timeout_seconds,
            telemetry=self.telemetry, on_upload_consumed=upload_consumed,
            allow_body_keep_alive=(
                getattr(self, "_pooled_node_body_origin", None) == node_url.rstrip("/")
            ),
        )

    def _build_proxy_request(
        self, node_url: str, path: str, *, method: str, body: Any = None,
        extra_headers: dict[str, str] | None = None,
    ) -> request.Request:
        return node_rpc.build_request(
            node_url, path, method=method, body=body, forwarded_headers=self.headers,
            extra_headers=extra_headers, node_token=self.node_control_bearer_token,
            telemetry=self.telemetry,
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
            response = node_rpc._open_node_request(
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
        if _token_matches(self.headers, self.gateway_bearer_token, allow_ucloud_sandbox_header=True):
            return True
        if _token_matches(self.headers, self.sandbox_api_token, allow_ucloud_sandbox_header=True):
            if _is_sdk_api_request(self.command, urlparse(self.path).path):
                return True
            self._write_json(
                {"error": "sandbox API key is not authorized for this endpoint"},
                status=HTTPStatus.FORBIDDEN,
            )
            return False
        return self._write_unauthorized()

    def _check_heartbeat_authorized(self) -> bool:
        if _token_matches(self.headers, self.heartbeat_bearer_token, allow_ucloud_sandbox_header=False):
            return True
        return self._write_unauthorized()

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
    isolate_fleet_reads: bool = False,
    isolate_routing_writes: bool = False,
    async_proxy_responses: bool = True,
    queue_placement: bool = False,
    placement_worker: bool = False,
    registry_url: str | None = None,
    registry_worker_url: str | None = None,
    registry_usage_file: Path | None = None,
    environment_registry: object | None = None,
    dispatch_environment_roots: bool = False,
    import_external_images: bool = False,
    create_placement: str = "ranked",
    registry_disk_monitor: RegistryDiskMonitor | None = None,
    max_concurrent_sandbox_creates: int = DEFAULT_MAX_CONCURRENT_SANDBOX_CREATES,
    create_target_concurrency_per_node: int = (
        ScalePolicy().create_target_concurrency_per_node
    ),
    max_http_request_threads: int = DEFAULT_MAX_GATEWAY_HTTP_REQUEST_THREADS,
    build_context_store_dir: Path | None = None,
    max_sandbox_resources: ResourceQuantity | None = None,
    wake_consolidation_policy: ScalePolicy | None = None,
    telemetry: Telemetry | None = None,
    process_count: int = 1,
    reuse_port: bool = False,
) -> HighBacklogThreadingHTTPServer:
    """One gateway process; ``process_count`` replicas share budgets and port."""
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
    # Validate persisted worker state before creating threads or binding HTTP.
    # Otherwise /healthz can succeed while every fleet request fails decoding.
    store.load_heartbeats()
    routing_store = open_routing_store(routing_file)
    if routing_store.distributed:
        routing_store.telemetry = resolved_telemetry
    from .routing_writer import RoutingWriteProcess
    routing_writer = RoutingWriteProcess(routing_store) if isolate_routing_writes and not routing_store.distributed else None
    if routing_writer is not None:
        routing_store = routing_writer
    metrics_store = BufferedMetricsStore(metrics_file)
    from .build_history import BuildHistoryStore
    build_history = BuildHistoryStore(metrics_file.with_name("build-history.sqlite"))
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

    BoundHandler.wake_consolidation_policy = wake_consolidation_policy or ScalePolicy()
    BoundHandler.wake_consolidation_next_at = 0.0
    BoundHandler.exec_routing = ExecRoutingService(
        store,
        routing_store,
        heartbeat_ttl_seconds,
        session_routes=ExecSessionRoutes(gateway_bearer_token),
    )
    # UCloud ingress owns public client connection reuse. Do not let its idle
    # upstream HTTP/1.1 pool consume gateway request threads between requests.
    # Private gateway-to-worker clients retain pooled keep-alives.
    BoundHandler.allow_http_keep_alive = False
    BoundHandler.routing_store = routing_store
    BoundHandler.placement_worker = placement_worker
    if placement_worker and not routing_store.distributed:
        raise ValueError('placement worker requires PostgreSQL routing')
    placement_queue=None
    if queue_placement and routing_store.distributed:
        if not async_proxy_responses or placement_worker:
            raise ValueError('queued placement requires asynchronous public responses')
        from .shared_control.placement_queue import (
            IsolatedPlacementResponses, PlacementQueue, PlacementQueueClient)
        from .shared_control.database import postgres_transaction_observer, process_pool_share
        placement_queue=IsolatedPlacementResponses(
            PlacementQueueClient(PlacementQueue(routing_store.pool.conninfo,
                deployment_id,schema=routing_store.schema,
                max_connections=process_pool_share(16),
                observe=postgres_transaction_observer(resolved_telemetry))),
            on_loop_started=(
                (lambda loop: resolved_telemetry.observe_event_loop_lag(loop, "placement-queue-io"))
                if resolved_telemetry.enabled else None))
    BoundHandler.placement_queue=placement_queue
    BoundHandler.routing_write_process = routing_writer
    BoundHandler.gateway_bearer_token = gateway_bearer_token
    BoundHandler.sandbox_api_token = sandbox_api_token
    BoundHandler.heartbeat_bearer_token = heartbeat_bearer_token
    BoundHandler.node_control_bearer_token = node_control_bearer_token
    BoundHandler.image_manager = image_manager
    BoundHandler.image_build_owners = OrderedDict()
    BoundHandler.image_build_metrics_seen = OrderedDict()
    BoundHandler.image_build_owners_lock = RLock()
    BoundHandler.build_context_store = build_context_store
    from .prepared_images import PreparedImageCatalog, catalog_path
    BoundHandler.prepared_image_catalog = PreparedImageCatalog(catalog_path(image_file))
    BoundHandler.metrics_store = metrics_store
    BoundHandler.build_history = build_history
    BoundHandler.metrics_response_cache = None
    BoundHandler.metrics_response_cache_at = 0.0
    BoundHandler.metrics_response_lock = RLock()
    BoundHandler.fleet_response_lock = RLock()
    BoundHandler.fleet_response_future = None
    BoundHandler.fleet_status_futures = {}
    from .fleet_reader import FleetSnapshotReader
    fleet_reader = (FleetSnapshotReader(control_state_file, routing_file, heartbeat_ttl_seconds)
                    if isolate_fleet_reads else None)
    BoundHandler.fleet_snapshot_reader = fleet_reader
    loopback = ["127.0.0.1" if host in {"", "0.0.0.0", "::"} else host, port]
    BoundHandler.loopback_origin = staticmethod(lambda: f"http://{loopback[0]}:{loopback[1]}")
    BoundHandler.image_import_submitter = (
        ImageImportSubmitter(_loopback_image_import(
            BoundHandler.build_context_store,
            gateway_bearer_token,
            lambda: f"http://{loopback[0]}:{loopback[1]}",
        ))
        if import_external_images else None
    )
    dependency_resolver = None
    if environment_registry is not None:
        from .environment_dependencies import EnvironmentDependencyResolver
        from .gateway.image_roots import ImageRootsStore, roots_path
        dependency_resolver = EnvironmentDependencyResolver(
            environment_registry, image_roots=ImageRootsStore(roots_path(image_file)))
    BoundHandler.dispatch_environment_roots = bool(dispatch_environment_roots and dependency_resolver is not None)
    BoundHandler.services = build_services(
        store=store, routing_store=routing_store, metrics_store=metrics_store,
        telemetry=resolved_telemetry, heartbeat_ttl_seconds=heartbeat_ttl_seconds,
        registry_url=registry_url, registry_worker_url=registry_worker_url,
        registry_usage_store=registry_usage_store, registry_disk_monitor=registry_disk_monitor,
        image_manager=image_manager, deployment_id=deployment_id,
        dependency_resolver=dependency_resolver,
        create_target_concurrency_per_node=int(create_target_concurrency_per_node),
        delete_on_worker=_worker_delete(node_control_bearer_token, resolved_telemetry),
        api_processes=max(1, int(process_count)),
    )
    if create_placement not in {"ranked", "power_of_k"}:
        raise ValueError("create placement must be ranked or power_of_k")
    BoundHandler.create_placement = create_placement
    BoundHandler.max_concurrent_sandbox_creates = max(
        0,
        int(max_concurrent_sandbox_creates),
    )
    BoundHandler.max_sandbox_resources = (
        max_sandbox_resources or ScalePolicy().default_node_resources
    )
    BoundHandler.sandbox_create_limiter = (
        FairCapacity(BoundHandler.max_concurrent_sandbox_creates)
        if BoundHandler.max_concurrent_sandbox_creates > 0
        else None
    )
    # Replicas divide the host's buffered-upload memory budget.
    BoundHandler.upload_memory_limiter = FairCapacity(
        max(1, DEFAULT_MAX_PROXY_BODY_BYTES // max(1, process_count))
    )
    BoundHandler.sandbox_create_busy_sampler = GatewayBusySampler(metrics_store)
    BoundHandler.create_image_pull_tasks = (
        CreateImagePullTasks(ENVIRONMENT_ATTACH_WAIT_SECONDS, bounded=False) if import_external_images
        else CreateImagePullTasks())
    BoundHandler.telemetry = resolved_telemetry
    from .gateway_response_proxy import AsyncGatewayResponses
    async_responses = (AsyncGatewayResponses(
        response_policy=_async_proxy_response,
        response_limit=node_rpc.DEFAULT_MAX_PROXY_RESPONSE_BYTES,
        timeout=DEFAULT_PROXY_TIMEOUT_SECONDS,
        connect_timeout=NODE_CONNECT_TIMEOUT_SECONDS,
    ) if async_proxy_responses else None)
    BoundHandler.async_responses = async_responses
    if resolved_telemetry.enabled:
        from .node_http_async import node_http_pool
        node_http_pool.call_soon(lambda: resolved_telemetry.observe_event_loop_lag(
            asyncio.get_running_loop(), "node-http-io"))
    class GatewayHTTPServer(HighBacklogThreadingHTTPServer):
        def __init__(self, *args, **kwargs):
            self._detached_proxy_requests = {}
            super().__init__(*args, **kwargs)

        def defer_proxy_response(self, request, **kwargs):
            with self._overload_lock:
                self._detached_proxy_requests[request] = kwargs

        def shutdown_request(self, request):
            with self._overload_lock:
                deferred = self._detached_proxy_requests.pop(request, None)
                if deferred is not None:
                    self._consumed_requests.discard(request)
            if deferred is not None:
                # finish_request has closed handler reader/writer wrappers.
                # Transfer the original descriptor now: no duplicated fd and no
                # overlap between blocking parser and asynchronous socket I/O.
                try:
                    async_responses.start(request, **deferred)
                except (OSError, RuntimeError):
                    pass  # start owns/cleans the socket even on submission failure.
                return
            super().shutdown_request(request)

        def server_close(self):
            if async_responses is not None:
                async_responses.close()
            try:
                super().server_close()
            finally:
                if BoundHandler.services.heartbeats.reaper is not None:
                    BoundHandler.services.heartbeats.reaper.close()
                if fleet_reader is not None:
                    fleet_reader.close()
                if routing_writer is not None:
                    routing_writer.close()
                if placement_queue is not None:
                    from .node_http_async import node_http_pool
                    node_http_pool.submit(placement_queue.close()).result(timeout=10)
                metrics_store.close()

    GatewayHTTPServer.reuse_port = bool(reuse_port)
    try:
        server = GatewayHTTPServer(
            (host, port), BoundHandler, max_request_threads=max_http_request_threads,
        )
        loopback[1] = server.server_address[1]
        return server
    except BaseException:
        if routing_writer is not None:
            routing_writer.close()
        metrics_store.close()
        raise


def _loopback_image_import(build_context_store, gateway_bearer_token, base_url):
    """Submit an image import as an ordinary managed build through this gateway."""

    def submit(import_id: str, image: str) -> None:
        archive = import_build_context(image)
        digest = "sha256:" + hashlib.sha256(archive).hexdigest()
        from io import BytesIO
        build_context_store.put_with_status(digest, BytesIO(archive), content_length=len(archive))
        payload = {
            "id": import_id,
            "context_path": ".",
            "dockerfile": "Dockerfile",
            "context_archive_digest": digest,
            "context_archive_format": "tar.gz",
            "context_archive_size": len(archive),
            "labels": {"org.ucloud.import-source": image[:512]},
        }
        submission = request.Request(
            base_url() + "/v1/images/build",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {gateway_bearer_token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with request.urlopen(submission, timeout=300) as response:
                response.read()
        except error.HTTPError as exc:
            body = exc.read()[:500]
            exc.close()
            # No builder yet: the build is queued and the autoscaler boots one;
            # the next create retry resubmits.
            if exc.code != HTTPStatus.SERVICE_UNAVAILABLE:
                raise RuntimeError(f"import build rejected ({exc.code}): {body!r}") from exc

    return submit


def _sandbox_list_bytes(store, routing_store, heartbeat_ttl_seconds, *, renderer=None,
                        status_only=False, sandbox_ids=()) -> bytes:
    from .fleet_reader import FleetResponseRenderer

    # Rendering only reads heartbeat data; retain inventory for absence checks
    # without copying every sandbox descriptor on each fleet-list request.
    heartbeats = store.load_heartbeats(shared=True)
    heartbeats_by_node_id = {
        heartbeat.node_id: heartbeat for heartbeat in heartbeats.values()
    }
    rows = (routing_store.sandbox_status_rows_readonly(sandbox_ids) if status_only else
            routing_store._sandbox_route_rows_readonly(background=True))
    return (renderer or FleetResponseRenderer(status_only=status_only)).render(
        rows,
        heartbeats_by_node_id, heartbeat_ttl_seconds,
    )


def _migration_runtime_capability(
    route: SandboxRoute, owner: NodeHeartbeat | None,
) -> str | None:
    """Use the same immutable runtime identity that import already validates.

    A detached checkpoint retains this identity without its former worker.
    Only an attached, pre-capability Docker source keeps legacy eligibility.
    Runtime fingerprints deliberately exclude node-local paths and addresses.
    """
    if route.storage_snapshot:
        snapshot = _portable_snapshot_for_route(route)
        return RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX + snapshot.manifest.runtime.node_compatibility_sha256
    if route.worker_state != "attached" or owner is None:
        raise ValueError("migration source runtime is unavailable")
    advertised = [value for value in owner.capabilities
                  if value.startswith(RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX)]
    if advertised:
        if len(advertised) != 1 or not re.fullmatch(
            r"[0-9a-f]{64}", advertised[0][len(RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX):],
        ):
            raise ValueError("migration source advertises an invalid runtime")
        return advertised[0]
    if HOST_EROFS_CAPABILITY in owner.capabilities:
        raise ValueError("immutable environment source lacks its runtime identity")
    return None


def _migration_cpu_capability(
    route: SandboxRoute, owner: NodeHeartbeat | None,
) -> str | None:
    """The CPU feature identity a destination's import will require, if known."""

    if route.storage_snapshot:
        try:
            snapshot = _portable_snapshot_for_route(route)
        except ValueError:
            return None
        return RUNTIME_CPU_CAPABILITY_PREFIX + snapshot.manifest.runtime.cpu_features_sha256
    if owner is None:
        return None
    advertised = [value for value in owner.capabilities
                  if value.startswith(RUNTIME_CPU_CAPABILITY_PREFIX)]
    return advertised[0] if len(advertised) == 1 else None


def _cpu_compatible(destination: NodeHeartbeat, required: str | None) -> bool:
    # Workers that predate the CPU capability stay eligible; their import
    # still validates the full fingerprint.
    if required is None:
        return True
    advertised = [value for value in destination.capabilities
                  if value.startswith(RUNTIME_CPU_CAPABILITY_PREFIX)]
    return not advertised or required in advertised




def _sandbox_record_is_ready(
    record: dict[str, Any],
) -> bool:
    """Return true only for externally usable lifecycle states."""

    return sandbox_route_state_from_observation(record.get("state")) is not None










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



def _reserve_builder_candidate(
    candidates: list[NodeHeartbeat], baseline: dict[str, int], *, reserve: bool,
    allow_full: bool = False,
) -> NodeHeartbeat | None:
    # Account for dispatches committed since the live-load sample began, even
    # if their HTTP response already returned. A stale simultaneous sample
    # must not make every request choose the same previously idle builder.
    with _BUILDER_DISPATCH_GUARD:
        def load(heartbeat: NodeHeartbeat):
            additions = (
                _BUILDER_DISPATCH_COUNTS.get(heartbeat.job_id, 0)
                - baseline.get(heartbeat.job_id, 0)
                if reserve else 0
            )
            return heartbeat.active_image_builds + max(0, additions)

        eligible = [heartbeat for heartbeat in candidates
                    if allow_full or load(heartbeat) < build_admission_capacity(heartbeat.labels)]
        if not eligible:
            return None

        def rank(heartbeat: NodeHeartbeat):
            # Pipeline admission includes solves waiting for publication; it
            # is a hard safety ceiling, not a throughput target. Consolidate
            # a trickle up to two builds, then use idle peers before queuing
            # more output behind the two publication workers. Once every
            # peer has work, prefer the least loaded. Legacy builders retain
            # their original packing behavior. Dispatch reservations count
            # here as well, so a simultaneous burst cannot defeat spreading.
            active = load(heartbeat)
            pipelined = BUILD_ADMISSION_CAPACITY_LABEL in heartbeat.labels
            saturated = pipelined and active >= 2
            return (
                int(saturated),
                active if saturated else -active,
                consolidation_rank(heartbeat),
                -heartbeat.physical_disk_free_mb,
                heartbeat.node_id,
            )
        selected = min(eligible, key=rank)
        if reserve:
            job_id = selected.job_id
            _BUILDER_DISPATCH_COUNTS[job_id] = _BUILDER_DISPATCH_COUNTS.get(job_id, 0) + 1
            _BUILDER_DISPATCH_INFLIGHT[job_id] = _BUILDER_DISPATCH_INFLIGHT.get(job_id, 0) + 1
        return selected


def _builder_image_dispatch_lock(image_id: str):
    """Serialize one image submission across every gateway process on the host.

    The running-build probe, builder choice and dispatch must be one step, or
    two processes both see "not running" and push the same managed tag.
    """
    return HOST_LOCKS.hold("image-dispatch", image_id.strip())


def _image_pull_lock(node_url: str, image: str) -> RLock:
    key = (node_url.rstrip("/"), image)
    with _IMAGE_PULL_LOCKS_GUARD:
        lock = _IMAGE_PULL_LOCKS.get(key)
        if lock is None:
            lock = RLock()
            _IMAGE_PULL_LOCKS[key] = lock
        return lock


def _migration_operation_lock(migration_id: str):
    """Serialize one migration's worker export and stage calls across processes.

    Phase commits are compare-and-set in PostgreSQL, but the worker calls between
    them are not; the public gateways and the placement worker all advance
    migrations.
    """
    return HOST_LOCKS.hold("migration", migration_id.strip())


def _worker_delete(node_token: str, telemetry: Telemetry):
    """The fenced worker DELETE a gateway issues on its own, outside a request."""

    def delete(node_url: str, sandbox_id: str, generation: int, operation_id: str):
        path = f"/v1/sandboxes/{quote(sandbox_id, safe='')}"
        response = node_rpc.proxy(
            node_rpc.build_request(
                node_url, path, method="DELETE", body=None, forwarded_headers={},
                extra_headers={SANDBOX_GENERATION_HEADER: str(generation),
                               SANDBOX_OPERATION_ID_HEADER: operation_id},
                node_token=node_token, telemetry=telemetry,
            ),
            node_url, path, method="DELETE", body=None,
            timeout_seconds=REBOOT_REAP_TIMEOUT_SECONDS, telemetry=telemetry,
            allow_body_keep_alive=False, on_upload_consumed=lambda: None,
        )
        return response.status, response.json()

    return delete


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
                with node_rpc._open_node_request(
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


def _image_build_response_terminal(payload: dict[str, Any]) -> bool:
    build = payload.get("build")
    if not isinstance(build, dict):
        return "image" in payload
    return str(build.get("status") or "").lower() in {"succeeded", "failed"}


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

