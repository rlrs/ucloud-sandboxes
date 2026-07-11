from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
import fcntl
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import sqlite3
from threading import BoundedSemaphore, RLock, Thread
import time
from typing import Any
from urllib import error, request
from urllib.parse import parse_qs, quote, unquote, urlparse
from uuid import uuid4

from .capabilities import (
    DISK_QUOTA_CAPABILITY,
    FORK_LOCAL_CAPABILITY,
    has_capability,
)
from .build_context_store import BuildContextBlobStore, ContentLengthReader
from .dashboard import dashboard_asset
from .deployment import agent_version_is_compatible, service_health
from .http_server import HighBacklogThreadingHTTPServer
from .images import (
    DockerImageRuntime,
    ImageBuildSpec,
    ImageManager,
    ImageRecord,
    ImageStore,
    image_id_from_tag,
    uploaded_build_context,
    uploaded_build_context_reference,
)
from .managed_registry import (
    RegistryClient,
    RegistryRequestError,
    RegistryUsageStore,
    canonical_image_digest_ref,
    digest_protection_tag,
    image_ref_with_manifest_digest,
    manifest_digest_from_image_ref,
    normalize_manifest_digest,
    registry_host_from_image_ref,
    registry_repository_tag_from_image_ref,
    registry_summary,
)
from .metrics import (
    MetricsStore,
    build_metrics_snapshot,
    record_node_heartbeat,
    record_sandbox_pending_deleted,
    record_sandbox_scheduled,
    trace_span,
)
from .models import NodeHeartbeat, ResourceQuantity, parse_iso_datetime, utc_now
from .registry import HeartbeatStore, heartbeat_from_dict, heartbeat_to_dict
from .routing import (
    ExecRoute,
    PendingImageWarmup,
    PendingSandboxDemand,
    RoutingStore,
    SandboxRoute,
    SandboxRouteConflictError,
)
from .sandbox import (
    FORK_REQUEST_TIMEOUT_SECONDS,
    MAX_FORK_FANOUT,
    SandboxSpec,
    sandbox_fork_target,
    sandbox_spec_fingerprint,
    sandbox_specs_match,
)
from .sandbox_exec import exec_session_route


_IMAGE_PULL_LOCKS_GUARD = RLock()
_IMAGE_PULL_LOCKS: dict[tuple[str, str], RLock] = {}
_IMAGE_WARMUP_TASKS_GUARD = RLock()
_IMAGE_WARMUP_TASKS: set[tuple[str, str]] = set()
_GATEWAY_SCHEDULING_LOCK = RLock()
_REGISTRY_LEASE_COORDINATION_LOCK = RLock()
REGISTRY_IMAGE_LEASE_TTL_SECONDS = 60 * 60
DEFAULT_MAX_CONCURRENT_SANDBOX_CREATES = 32
FORK_PROXY_TIMEOUT_SECONDS = FORK_REQUEST_TIMEOUT_SECONDS
SANDBOX_CREATE_BUSY_RETRY_AFTER_SECONDS = 2
SANDBOX_CREATE_IN_PROGRESS_RETRY_AFTER_SECONDS = 5
SANDBOX_PLACEMENT_LOCK_WAIT_SECONDS = 0.25
# Build execution is asynchronous. This timeout only covers proxying the build
# context and enqueueing the build on a builder node.
IMAGE_BUILD_PROXY_TIMEOUT_SECONDS = 30 * 60
IMAGE_PULL_PROXY_TIMEOUT_SECONDS = 30 * 60
DEFAULT_PROXY_TIMEOUT_SECONDS = 60
DEFAULT_MAX_JSON_BODY_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_PROXY_BODY_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_BUILD_CONTEXT_STORE_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_MAX_BUILD_CONTEXT_ENTRIES = 128
DEFAULT_MAX_BUILD_CONTEXT_AGE_SECONDS = 24 * 60 * 60
NODE_RECONCILE_PROXY_TIMEOUT_SECONDS = 5
NODE_RECOVERY_PROXY_TIMEOUT_SECONDS = 5
NODE_DISCOVERY_TOTAL_TIMEOUT_SECONDS = 2.0
SANDBOX_GENERATION_HEADER = "X-UCloud-Sandbox-Generation"
SANDBOX_OPERATION_ID_HEADER = "X-UCloud-Sandbox-Operation-Id"
REGISTRY_METRICS_TIMEOUT_SECONDS = 1.5
DEFAULT_METRICS_EVENT_LIMIT = 2000
FULL_METRICS_EVENT_LIMIT = 10000
REGISTRY_STATUS_CACHE_TTL_SECONDS = 30.0


class RequestBodyTooLargeError(ValueError):
    pass


class GatewaySchedulingBusyError(RuntimeError):
    """Placement serialization is occupied and the caller should retry."""


class RegistryImageReferenceUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class _RegistryImageBuildReference:
    image_id: str
    tag: str
    owner: str


class ProxiedResponse:
    def __init__(self, status: int, headers: Any, body: bytes) -> None:
        self.status = status
        self.headers = headers
        self.body = body

    def json(self) -> dict[str, Any]:
        try:
            decoded = json.loads(self.body.decode("utf-8")) if self.body else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}
        return decoded if isinstance(decoded, dict) else {}


class _RejectNodeRedirects(request.HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        return None


def _open_node_request(
    req: request.Request,
    *,
    timeout: float,
    authenticated: bool = False,
) -> Any:
    # Authenticated node calls must never carry the deployment credential to a
    # redirect target selected by a compromised node endpoint.
    if authenticated:
        return request.build_opener(_RejectNodeRedirects()).open(req, timeout=timeout)
    return request.urlopen(req, timeout=timeout)


class ControlPlaneHandler(BaseHTTPRequestHandler):
    store: HeartbeatStore
    routing_store: RoutingStore | None
    upstream_node_url: str | None
    gateway_bearer_token: str | None
    heartbeat_bearer_token: str | None
    node_control_bearer_token: str | None
    deployment_id: str
    heartbeat_ttl_seconds: int
    image_manager: ImageManager | None
    build_context_store: BuildContextBlobStore
    local_image_builds_enabled: bool
    metrics_store: MetricsStore | None
    registry_url: str | None
    registry_status_cache: dict[str, Any] | None
    registry_status_cache_at: float
    registry_status_lock: RLock
    registry_usage_store: RegistryUsageStore | None
    sandbox_create_limiter: BoundedSemaphore | None
    max_concurrent_sandbox_creates: int
    server_version = "ucloud-sandboxes-control-plane/0.1"

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
        context_digest = _build_context_digest_from_path(parsed.path)
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
                heartbeat_to_dict(heartbeat) for heartbeat in self.store.load().values()
            ]
            self._write_json({"nodes": nodes})
            return
        if parsed.path == "/v1/demand" and self.routing_store is not None:
            try:
                demand_payload = self._demand_payload()
            except sqlite3.DatabaseError as exc:
                self._write_routing_store_unavailable(exc)
                return
            self._write_json(demand_payload)
            return
        if parsed.path == "/v1/metrics":
            try:
                routing_state = (
                    self.routing_store.load()
                    if self.routing_store is not None
                    else None
                )
            except sqlite3.DatabaseError as exc:
                self._write_routing_store_unavailable(exc)
                return
            full = _truthy_query_param(parsed, "full") or _truthy_query_param(
                parsed, "detail"
            )
            events = (
                self.metrics_store.load_events(
                    max_events=FULL_METRICS_EVENT_LIMIT
                    if full
                    else DEFAULT_METRICS_EVENT_LIMIT
                )
                if self.metrics_store is not None
                else []
            )
            snapshot = build_metrics_snapshot(
                self.store.load(),
                routing_state,
                events,
                heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
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
                force_refresh=full or _truthy_query_param(parsed, "refresh_registry")
            )
            self._write_json(snapshot)
            return
        if parsed.path == "/v1/registry":
            self._write_json({"registry": self._registry_status()})
            return
        if self._route_to_nodes(parsed.path):
            return
        if self._proxy_to_node():
            return
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

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
            if self._proxy_to_node():
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

        if self.deployment_id and heartbeat.deployment_id != self.deployment_id:
            self._write_json(
                {
                    "error": "heartbeat deployment_id does not match this gateway",
                    "expected_deployment_id": self.deployment_id,
                },
                status=HTTPStatus.FORBIDDEN,
            )
            return

        received_at = utc_now()
        reported_at = heartbeat.reported_at or heartbeat.updated_at
        previous = self.store.load().get(heartbeat.job_id)
        if heartbeat.active_workloads > 0:
            idle_since = None
        elif previous is not None and previous.active_workloads == 0:
            idle_since = previous.idle_since or previous.freshness_at
        else:
            idle_since = received_at
        # The sender controls neither freshness nor the idle-grace clock. Keep
        # its timestamp as reported_at for diagnostics while overwriting the
        # legacy timestamp for old readers and recording the explicit receipt.
        heartbeat = replace(
            heartbeat,
            updated_at=received_at,
            reported_at=reported_at,
            received_at=received_at,
            idle_since=idle_since,
        )

        heartbeats = self.store.upsert(heartbeat)
        stored_heartbeat = heartbeats.get(heartbeat.job_id, heartbeat)
        record_node_heartbeat(self.metrics_store, stored_heartbeat)
        if (
            self.routing_store is not None
            and stored_heartbeat.inventory_complete
            and stored_heartbeat.node_url
        ):
            inventory_routes = [
                SandboxRoute(
                    sandbox_id=item.sandbox_id,
                    node_id=stored_heartbeat.node_id,
                    job_id=stored_heartbeat.job_id,
                    node_url=stored_heartbeat.node_url,
                    resources=item.resources,
                    state=item.state or "running",
                    generation=item.generation,
                    create_operation_id=item.operation_id,
                    spec_hash=item.spec_hash,
                    node_epoch=stored_heartbeat.node_epoch,
                    activity_epoch=stored_heartbeat.activity_epoch,
                )
                for item in stored_heartbeat.inventory
            ]
            self.routing_store.reconcile_sandboxes_for_node(
                stored_heartbeat.node_url,
                inventory_routes,
                observed_at=stored_heartbeat.freshness_at.isoformat(),
                node_epoch=stored_heartbeat.node_epoch,
                activity_epoch=stored_heartbeat.activity_epoch,
                inventory_complete=True,
            )
        self._schedule_image_warmups()
        self._write_json({"ok": True, "node": heartbeat_to_dict(stored_heartbeat)})

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        if not self._check_authorized():
            return
        context_digest = _build_context_digest_from_path(parsed.path)
        if context_digest is not None:
            self._store_build_context(context_digest)
            return
        if self._route_to_nodes(parsed.path):
            return
        if self._proxy_to_node():
            return
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def _store_build_context(self, digest: str) -> None:
        content_type = (self.headers.get("Content-Type") or "").split(";", 1)[0]
        if content_type.strip().lower() != "application/gzip":
            self.close_connection = True
            self._write_json(
                {"error": "build contexts require Content-Type: application/gzip"},
                status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
            )
            return
        try:
            length = self._request_content_length(
                max_bytes=self.build_context_store.max_blob_bytes
            )
            result = self.build_context_store.put_with_status(
                digest,
                ContentLengthReader(self.rfile, length),
                content_length=length,
            )
            self.build_context_store.gc(protected=(digest,))
        except RequestBodyTooLargeError as exc:
            self.close_connection = True
            self._write_json(
                {"error": str(exc)}, status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE
            )
            return
        except (OSError, ValueError) as exc:
            self.close_connection = True
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._write_json(
            {"digest": digest, "size": length, "deduplicated": result.deduplicated},
            status=HTTPStatus.OK if result.deduplicated else HTTPStatus.CREATED,
        )

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        if not self._check_authorized():
            return
        if self._route_to_nodes(parsed.path):
            return
        if self._proxy_to_node():
            return
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def _read_json_body(self) -> object:
        raw = self._read_raw_body().decode("utf-8")
        if not raw:
            raise ValueError("empty request body")
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON: {exc}") from exc

    def _read_raw_body(
        self,
        *,
        max_bytes: int = DEFAULT_MAX_JSON_BODY_BYTES,
    ) -> bytes:
        length = self._request_content_length(max_bytes=max_bytes)
        body = self.rfile.read(length)
        if len(body) != length:
            raise ValueError("request body ended before Content-Length bytes were read")
        return body

    def _request_content_length(self, *, max_bytes: int | None = None) -> int:
        if self.headers.get("Transfer-Encoding"):
            raise ValueError("Transfer-Encoding is not supported; use Content-Length")
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            raise ValueError("Content-Length header is required")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc
        if length < 0:
            raise ValueError("Content-Length cannot be negative")
        if max_bytes is not None and length > max_bytes:
            raise RequestBodyTooLargeError(
                f"request body exceeds the {max_bytes} byte limit"
            )
        return length

    def _write_json(
        self,
        payload: dict[str, Any],
        *,
        status: HTTPStatus = HTTPStatus.OK,
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _write_bytes(
        self,
        body: bytes,
        content_type: str,
        *,
        status: HTTPStatus = HTTPStatus.OK,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _route_to_nodes(self, path: str) -> bool:
        if self.routing_store is None:
            return False
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
        fork_source_id = _sandbox_fork_source_from_path(path)
        if fork_source_id is not None and self.command == "POST":
            self._fork_sandbox_on_node(fork_source_id)
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
        sandbox_id = _sandbox_id_from_path(path)
        if sandbox_id is not None:
            self._route_sandbox_request(sandbox_id, path)
            return True
        session_id = _exec_session_id_from_path(path)
        if session_id is not None:
            self._route_exec_request(session_id, path)
            return True
        return False

    def _registry_usage_health_error(self) -> str:
        store = self.registry_usage_store
        if store is None:
            return ""
        path = store.path
        try:
            if not path.exists():
                if not path.parent.is_dir() or not os.access(path.parent, os.W_OK):
                    return "state directory is not writable"
                return ""
            with path.open("r+", encoding="utf-8") as file:
                payload = json.load(file)
            if not isinstance(payload, dict):
                return "state file is invalid"
            if not isinstance(payload.get("images", []), list) or not isinstance(
                payload.get("leases", []), list
            ):
                return "state file is invalid"
        except (OSError, ValueError, json.JSONDecodeError):
            return "state file is unavailable"
        return ""

    def _write_routing_store_unavailable(self, exc: sqlite3.DatabaseError) -> None:
        self._write_json(
            {
                "error": "routing state unavailable",
                "retryable": True,
                "details": str(exc),
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
            prepare_id = str(
                raw.get("id")
                or raw.get("prepare_id")
                or raw.get("prepareId")
                or f"prep-{uuid4().hex[:16]}"
            ).strip()
            if not prepare_id or "/" in prepare_id:
                raise ValueError("prepare id must be non-empty and cannot contain '/'.")
            count = int(_payload_value(raw, "count", "sandboxes", default=1))
            ttl_seconds = int(
                _payload_value(raw, "ttl_seconds", "ttlSeconds", default=900)
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
            image, image_error = self._resolve_sandbox_image_reference(image)
            if image_error is not None:
                self._write_json(image_error, status=HTTPStatus.BAD_REQUEST)
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
            prepare_id = str(
                raw.get("id")
                or raw.get("prepare_id")
                or raw.get("prepareId")
                or f"builder-prep-{uuid4().hex[:16]}"
            ).strip()
            if not prepare_id or "/" in prepare_id:
                raise ValueError("prepare id must be non-empty and cannot contain '/'.")
            count = int(_payload_value(raw, "count", "builders", default=1))
            ttl_seconds = int(
                _payload_value(raw, "ttl_seconds", "ttlSeconds", default=900)
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
        heartbeats = self.store.load()
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
            routes: list[SandboxRoute] = []
            for record in raw_sandboxes:
                if not isinstance(record, dict):
                    continue
                spec = record.get("spec")
                sandbox_id = spec.get("id") if isinstance(spec, dict) else None
                if isinstance(sandbox_id, str) and sandbox_id:
                    observed_ids.add(sandbox_id)
                    routes.append(
                        _route_with_sandbox_record(
                            _sandbox_route_from_heartbeat(
                                heartbeat,
                                sandbox_id,
                                spec,
                                state=_sandbox_record_state(record, default="running"),
                            ),
                            record,
                        )
                    )
                    sandboxes.append(_enrich_sandbox_record(record, heartbeat))
            self.routing_store.reconcile_sandboxes_for_node(
                heartbeat.node_url or "",
                routes,
                observed_at=observed_at,
            )
            for observed_route in routes:
                stored_route = (
                    self.routing_store.get_sandbox_readonly(observed_route.sandbox_id)
                    or observed_route
                )
                self._ensure_registry_route_reference(stored_route, touch=True)
        if self.routing_store is not None:
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
        self._write_json({"images": self._image_records_across_nodes()})

    def _image_records_across_nodes(
        self,
        *,
        image_id: str | None = None,
    ) -> list[dict[str, Any]]:
        images: list[dict[str, Any]] = []
        if self.image_manager is not None:
            for record in sorted(self.image_manager.list(), key=lambda item: item.id):
                if image_id is not None and record.id != image_id:
                    continue
                enriched = record.to_dict()
                enriched["location"] = "control-plane"
                if self._image_record_missing_registry_manifest(enriched):
                    self.image_manager.store.delete_by_tags([record.tag])
                    continue
                enriched = self._image_record_with_registry_digest(enriched)
                images.append(enriched)
        for heartbeat in self._ready_heartbeats():
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/images",
                method="GET",
            )
            if response.status >= 400:
                continue
            payload = response.json()
            raw_images = payload.get("images")
            if not isinstance(raw_images, list):
                continue
            for record in raw_images:
                if isinstance(record, dict):
                    if image_id is not None and record.get("id") != image_id:
                        continue
                    enriched = dict(record)
                    enriched["node"] = _node_metadata(heartbeat)
                    if self._image_record_missing_registry_manifest(enriched):
                        continue
                    enriched = self._image_record_with_registry_digest(enriched)
                    images.append(enriched)
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
        if (
            self.routing_store is not None
            and selected_image_id
            and _image_build_response_terminal({"build": selected})
        ):
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
        if self.image_manager is not None:
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
        if self.image_manager is None or build.get("status") != "succeeded":
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

    def _managed_registry_manifest_digest(self, image_ref: str) -> str:
        existing = manifest_digest_from_image_ref(image_ref)
        if not self.registry_url:
            return existing
        coordinates = _managed_registry_image_coordinates(image_ref, self.registry_url)
        if coordinates is None:
            return existing
        repository, image_tag = coordinates
        client = RegistryClient(self.registry_url)
        try:
            digest = existing or normalize_manifest_digest(
                client.manifest_digest(repository, image_tag)
            )
            if not digest:
                return ""
            client.ensure_digest_protection_tag(repository, digest)
        except (OSError, ValueError, RegistryRequestError):
            return ""
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
            and _managed_registry_image_coordinates(tag, self.registry_url) is not None
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
                trace_id = _request_trace_id(self, "sandbox-create", "admission")
                with trace_span(
                    self.metrics_store,
                    trace_id,
                    "gateway.sandbox_create",
                    attributes={
                        "outcome": "gateway_busy",
                        "max_concurrent_sandbox_creates": (
                            self.max_concurrent_sandbox_creates
                        ),
                    },
                ) as root:
                    root.status = "error"
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
            parsed_ok = True
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
    ) -> None:
        trace_id = _request_trace_id(self, "sandbox-create", spec.id)
        with trace_span(
            self.metrics_store,
            trace_id,
            "gateway.sandbox_create",
            attributes={
                "sandbox_id": spec.id,
                "image": spec.image,
                "resources": spec.requested_resources().to_dict(),
            },
        ) as root:
            with trace_span(
                self.metrics_store,
                trace_id,
                "gateway.sandbox_resolve_image",
                parent_span_id=root.span_id,
                attributes={"image": spec.image},
            ) as span:
                resolved_image, image_error = self._resolve_sandbox_image_reference(
                    spec.image
                )
                span.set_attribute("resolved_image", resolved_image)
                if image_error is not None:
                    span.status = "error"
                    root.status = "error"
                    root.set_attribute("outcome", "image_reference_unavailable")
                    self._write_json(image_error, status=HTTPStatus.BAD_REQUEST)
                    return
                if resolved_image != spec.image:
                    spec = replace(spec, image=resolved_image)
                    root.set_attribute("resolved_image", resolved_image)

            with trace_span(
                self.metrics_store,
                trace_id,
                "gateway.sandbox_existing_route_check",
                parent_span_id=root.span_id,
            ) as span:
                existing = self.routing_store.get_sandbox_readonly(spec.id)
                if existing is None:
                    existing = self._discover_sandbox_route(spec.id)
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
                        existing.generation > 0
                        and existing.create_operation_id
                        and existing.spec_hash == requested_hash
                        and (existing.state or "unknown").lower()
                        in {"creating", "unknown"}
                    ):
                        root.set_attribute("outcome", "retry_same_generation")
                        self._retry_sandbox_create_on_assigned_node(existing, spec)
                        return
                    # Age and aggregate active counts cannot fence a delayed
                    # create. Only generation-aware complete inventory or a
                    # successful same-generation delete may remove this route.
                    if existing is not None:
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

            with trace_span(
                self.metrics_store,
                trace_id,
                "gateway.sandbox_select_node",
                parent_span_id=root.span_id,
                attributes={"image": spec.image},
            ) as span:
                pending_before = self.routing_store.get_pending(spec.id)
                try:
                    placement = self._select_and_reserve_node(
                        spec.id,
                        spec.requested_resources(),
                        image=spec.image,
                        spec=spec.to_dict(),
                        spec_hash=sandbox_spec_fingerprint(spec),
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
                            "Retry-After": str(
                                SANDBOX_CREATE_BUSY_RETRY_AFTER_SECONDS
                            ),
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
                span.set_attribute(
                    "selected_node_id", heartbeat.node_id if heartbeat else ""
                )
                span.set_attribute(
                    "selected_job_id", heartbeat.job_id if heartbeat else ""
                )
            if heartbeat is None:
                self.routing_store.upsert_pending(spec.id, spec.requested_resources())
                demand = self.routing_store.pending_demand()
                root.status = "error"
                root.set_attribute("outcome", "queued_no_ready_node")
                root.set_attribute(
                    "pending_resources", demand.pending_resources.to_dict()
                )
                self._write_json(
                    {
                        "error": "no ready node has resources for sandbox request",
                        "pending_resources": demand.pending_resources.to_dict(),
                        "oldest_pending_seconds": demand.oldest_pending_seconds,
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
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
                self.routing_store.delete_sandbox_if_current(
                    spec.id,
                    generation=route.generation,
                    create_operation_id=route.create_operation_id,
                )
                self._persist_failed_sandbox_demand(
                    spec,
                    route,
                    failure_reason="registry_lease_unavailable",
                )
                raise
            root.set_attribute("reserved_route", True)
            with trace_span(
                self.metrics_store,
                trace_id,
                "gateway.sandbox_ensure_image",
                parent_span_id=root.span_id,
                attributes={
                    "node_id": heartbeat.node_id,
                    "image": spec.image,
                },
            ) as span:
                image_response = self._ensure_image_on_node(heartbeat, spec.image)
                span.set_attribute("cache_hit", image_response is None)
                if image_response is not None:
                    span.set_attribute("status_code", int(image_response.status))
            if image_response is not None and image_response.status >= 400:
                self._release_registry_route_reference(route)
                self.routing_store.delete_sandbox_if_current(
                    spec.id,
                    generation=route.generation,
                    create_operation_id=route.create_operation_id,
                )
                self._persist_failed_sandbox_demand(
                    spec,
                    route,
                    failure_reason=f"image_pull_http_{image_response.status}",
                )
                root.status = "error"
                root.set_attribute("outcome", "image_pull_failed")
                self._write_json(
                    {
                        "error": (
                            "image is not available on selected sandbox node; pull failed. "
                            "For images built by the UCloud builder, build with push=true "
                            "and a pullable registry tag before creating sandboxes."
                        ),
                        "pull": image_response.json(),
                    },
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return

            with trace_span(
                self.metrics_store,
                trace_id,
                "gateway.sandbox_proxy_create",
                parent_span_id=root.span_id,
                attributes={"node_id": heartbeat.node_id},
            ) as span:
                response = self._proxy_request(
                    heartbeat.node_url or "",
                    "/v1/sandboxes",
                    method="POST",
                    body=_sandbox_create_request_body(spec, route),
                )
                span.set_attribute("status_code", int(response.status))
                response_payload = response.json()
                node_timings = response_payload.get("timings")
                if isinstance(node_timings, dict):
                    span.set_attribute("node_timings", node_timings)
                    root.set_attribute("node_timings", node_timings)
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
                root.status = "error"
                root.set_attribute("outcome", "node_create_failed")
                root.set_attribute("status_code", int(response.status))
                if _node_create_may_still_be_running(response):
                    root.set_attribute("kept_durable_route", True)
                else:
                    self._release_registry_route_reference(route)
                    self.routing_store.delete_sandbox_if_current(
                        spec.id,
                        generation=route.generation,
                        create_operation_id=route.create_operation_id,
                    )
            self._send_proxied_response(response)

    def _fork_sandbox_on_node(self, source_sandbox_id: str) -> None:
        limiter = self.sandbox_create_limiter
        limiter_acquired = False
        if limiter is not None and not limiter.acquire(blocking=False):
            self._write_json(
                {
                    "error": "gateway is busy creating or forking sandboxes; retry shortly",
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
        try:
            self._fork_sandbox_on_node_locked(source_sandbox_id)
        finally:
            if limiter_acquired:
                limiter.release()

    def _fork_sandbox_on_node_locked(self, source_sandbox_id: str) -> None:
        try:
            body = self._read_raw_body(max_bytes=DEFAULT_MAX_JSON_BODY_BYTES)
            raw = json.loads(body.decode("utf-8")) if body else None
            if not isinstance(raw, dict):
                raise ValueError("fork payload must be a JSON object")
        except (json.JSONDecodeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        source_route = self.routing_store.get_sandbox_readonly(source_sandbox_id)
        if source_route is None:
            source_route = self._discover_sandbox_route(source_sandbox_id)
        if source_route is None:
            self._write_json(
                {"error": "source sandbox route not found"},
                status=HTTPStatus.NOT_FOUND,
            )
            return
        if (source_route.state or "unknown").lower() != "running":
            self._write_json(
                {
                    "error": "source sandbox is not confirmed running",
                    "retryable": True,
                    "sandbox_id": source_sandbox_id,
                },
                status=HTTPStatus.CONFLICT,
            )
            return
        if not source_route.spec:
            source_record = self._sandbox_record_on_node(
                source_route.node_url,
                source_sandbox_id,
            )
            if source_record is not None:
                source_route = _route_with_sandbox_record(
                    source_route,
                    source_record,
                )
                source_route = self.routing_store.upsert_sandbox(source_route)
        try:
            source_spec = SandboxSpec.from_dict(source_route.spec)
            source_spec.validate()
            if not source_route.spec_hash:
                raise ValueError(
                    "source route does not carry an exact current spec identity"
                )
            batch = _public_fork_request_is_batch(raw)
            if batch:
                raw_targets = raw.get("sandboxes")
                if not isinstance(raw_targets, list) or not raw_targets:
                    raise ValueError("sandboxes must be a non-empty JSON array")
                if len(raw_targets) > MAX_FORK_FANOUT:
                    raise ValueError(
                        f"fork fan-out cannot exceed {MAX_FORK_FANOUT} sandboxes"
                    )
                if not all(isinstance(item, dict) for item in raw_targets):
                    raise ValueError("each fork sandbox must be a JSON object")
                targets = tuple(
                    sandbox_fork_target(source_spec, item) for item in raw_targets
                )
                if len({target.id for target in targets}) != len(targets):
                    raise ValueError("fork fan-out target ids must be unique")
            else:
                targets = (sandbox_fork_target(source_spec, raw),)
        except (TypeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        heartbeat = self._heartbeat_for_route(
            node_id=source_route.node_id,
            job_id=source_route.job_id,
            node_url=source_route.node_url,
        )
        if (
            heartbeat is None
            or not heartbeat.node_url
            or not heartbeat.is_fresh(utc_now(), self.heartbeat_ttl_seconds)
            or heartbeat.draining
            or not agent_version_is_compatible(heartbeat.agent_version)
        ):
            self._write_json(
                {
                    "error": "source sandbox node is not ready for a local fork",
                    "retryable": True,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        if not has_capability(heartbeat.capabilities, FORK_LOCAL_CAPABILITY):
            self._write_json(
                {
                    "error": "source sandbox node does not support live local fork",
                    "capability": FORK_LOCAL_CAPABILITY,
                },
                status=HTTPStatus.NOT_IMPLEMENTED,
            )
            return

        if (
            _sandbox_fork_request_body_upper_bound(
                source_route,
                targets,
                batch=batch,
            )
            > DEFAULT_MAX_JSON_BODY_BYTES
        ):
            self._write_json(
                {
                    "error": ("expanded fork request exceeds the node JSON body limit"),
                    "max_bytes": DEFAULT_MAX_JSON_BODY_BYTES,
                    "intent_persisted": False,
                },
                status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            )
            return

        try:
            reservations = self._reserve_forks_on_source_node(
                source_route,
                heartbeat,
                targets,
            )
        except SandboxRouteConflictError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        if reservations is None:
            required = ResourceQuantity()
            for target in targets:
                required += target.requested_resources()
            self._write_json(
                {
                    "error": (
                        "source sandbox node lacks capacity for fork children"
                        if batch
                        else "source sandbox node lacks capacity for fork child"
                    ),
                    "retryable": True,
                    "required_resources": required.to_dict(),
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        routes = tuple(route for route, _created in reservations)
        created_routes = tuple(route for route, created in reservations if created)

        referenced_created_routes: list[SandboxRoute] = []
        try:
            for route, created in reservations:
                self._ensure_registry_route_reference(route, touch=True)
                if created:
                    referenced_created_routes.append(route)
        except RegistryImageReferenceUnavailable:
            for route in referenced_created_routes:
                self._release_registry_route_reference(route)
            for route in created_routes:
                self.routing_store.delete_sandbox_if_current(
                    route.sandbox_id,
                    generation=route.generation,
                    create_operation_id=route.create_operation_id,
                )
            raise

        response = self._proxy_request(
            heartbeat.node_url or "",
            f"/v1/sandboxes/{quote(source_sandbox_id, safe='')}/forks",
            method="POST",
            timeout_seconds=FORK_PROXY_TIMEOUT_SECONDS,
            body=(
                _sandbox_fork_batch_request_body(source_route, targets, routes)
                if batch
                else _sandbox_fork_request_body(source_route, targets[0], routes[0])
            ),
        )
        payload = response.json()
        if 200 <= response.status < 300:
            if batch:
                records = payload.get("sandboxes")
                forks = payload.get("forks")
                valid_response = _sandbox_fork_batch_result_matches_routes(
                    payload,
                    records,
                    forks,
                    routes,
                    targets,
                    source_route,
                )
            else:
                record = payload.get("sandbox")
                records = [record]
                valid_response = (
                    isinstance(record, dict)
                    and _sandbox_fork_record_matches_route(
                        record,
                        routes[0],
                        targets[0],
                        source_route,
                    )
                    and _sandbox_fork_result_matches_record(payload, record)
                )
            if not valid_response:
                # A success response with an invalid confirmation is ambiguous:
                # keep the exact route identity so a retry can safely replay it.
                self._write_json(
                    {
                        "error": (
                            "node fork response did not confirm the assigned child "
                            "generation and source identity"
                        ),
                        "retryable": True,
                    },
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return
            finalization_conflict = False
            for route, target, record in zip(routes, targets, records, strict=True):
                stored_route = self.routing_store.finalize_sandbox_create(
                    _route_with_sandbox_record(route, record)
                )
                if stored_route is None:
                    finalization_conflict = True
                    continue
                record_sandbox_scheduled(
                    self.metrics_store,
                    sandbox_id=target.id,
                    route=stored_route,
                    resources=target.requested_resources(),
                    pending=None,
                )
                self._record_registry_image_used(target.image)
            if finalization_conflict:
                self._write_json(
                    {
                        "error": (
                            "fork completed while a child route was concurrently "
                            "deleted"
                        ),
                        "intent_persisted": True,
                    },
                    status=HTTPStatus.CONFLICT,
                )
                return
        else:
            intent_states = _node_fork_intent_states(payload, routes)
            for route, intent_persisted in zip(routes, intent_states, strict=True):
                if intent_persisted is not False:
                    continue
                current = self.routing_store.get_sandbox_readonly(route.sandbox_id)
                if (
                    current is None
                    or (current.state or "unknown").lower() != "creating"
                ):
                    continue
                removed = self.routing_store.delete_sandbox_if_current(
                    route.sandbox_id,
                    generation=route.generation,
                    create_operation_id=route.create_operation_id,
                )
                if removed is not None:
                    self._release_registry_route_reference(removed)
        self._send_proxied_response(response)

    def _reserve_forks_on_source_node(
        self,
        source_route: SandboxRoute,
        heartbeat: NodeHeartbeat,
        targets: tuple[SandboxSpec, ...],
    ) -> tuple[tuple[SandboxRoute, bool], ...] | None:
        if not targets:
            raise ValueError("at least one fork target is required")
        if len({target.id for target in targets}) != len(targets):
            raise ValueError("fork fan-out target ids must be unique")
        with _GATEWAY_SCHEDULING_LOCK, _gateway_placement_lock(self.routing_store.path):
            current_source = self.routing_store.get_sandbox_readonly(
                source_route.sandbox_id
            )
            if (
                current_source is None
                or current_source.generation != source_route.generation
                or current_source.spec_hash != source_route.spec_hash
                or current_source.node_id != source_route.node_id
            ):
                raise SandboxRouteConflictError(
                    "source sandbox route changed before fork reservation"
                )

            current_routes = list(self.routing_store.sandbox_routes_readonly())
            requests: list[tuple[SandboxRoute, str, str]] = []
            now = utc_now().isoformat()
            for target in targets:
                target_hash = sandbox_spec_fingerprint(target)
                operation_id = _sandbox_fork_operation_id(source_route, target_hash)
                existing = self.routing_store.get_sandbox_readonly(target.id)
                if existing is not None:
                    if (
                        existing.spec_hash != target_hash
                        or existing.create_operation_id != operation_id
                        or existing.node_id != heartbeat.node_id
                    ):
                        raise SandboxRouteConflictError(
                            "fork child already exists with another identity: "
                            f"{target.id}"
                        )
                    candidate = existing
                else:
                    if not _node_can_fit(
                        heartbeat, target.requested_resources(), current_routes
                    ):
                        return None
                    candidate = SandboxRoute(
                        sandbox_id=target.id,
                        node_id=heartbeat.node_id,
                        job_id=heartbeat.job_id,
                        node_url=heartbeat.node_url or "",
                        resources=target.requested_resources(),
                        spec=target.to_dict(),
                        state="creating",
                        node_epoch=heartbeat.node_epoch,
                        activity_epoch=heartbeat.activity_epoch,
                        created_at=now,
                        updated_at=now,
                    )
                    # Account for every earlier child before admitting the next.
                    current_routes.append(candidate)
                requests.append((candidate, target_hash, operation_id))
            return self.routing_store.allocate_sandbox_creates(requests)

    def _reserve_fork_on_source_node(
        self,
        source_route: SandboxRoute,
        heartbeat: NodeHeartbeat,
        target: SandboxSpec,
    ) -> tuple[SandboxRoute, bool] | None:
        reservations = self._reserve_forks_on_source_node(
            source_route, heartbeat, (target,)
        )
        return reservations[0] if reservations is not None else None

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
        record = self._sandbox_record_on_node(route.node_url, spec.id)
        if record is None or not _sandbox_record_matches_route(record, route, spec):
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

        if (
            route.generation <= 0
            or not route.create_operation_id
            or route.spec_hash != sandbox_spec_fingerprint(spec)
        ):
            self._write_json(
                {"error": f"sandbox already exists with different spec: {spec.id}"},
                status=HTTPStatus.CONFLICT,
            )
            return
        self._ensure_registry_route_reference(route, touch=True)
        response = self._proxy_request(
            route.node_url,
            "/v1/sandboxes",
            method="POST",
            body=_sandbox_create_request_body(spec, route),
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
        # The original create may have completed even when this replay did not.
        # Keep the durable route for another identical replay or delete fence.
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
        if not image_ref:
            return
        store = self.registry_usage_store
        if store is None:
            return
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

    def _begin_registry_image_build_reference(
        self,
        spec: ImageBuildSpec,
        *,
        push: bool,
    ) -> _RegistryImageBuildReference | None:
        if (
            not push
            or self.registry_usage_store is None
            or not self.registry_url
            or _managed_registry_image_coordinates(spec.tag, self.registry_url) is None
        ):
            return None
        owner = _registry_operation_lease_owner(
            "image-build",
            {
                "version": 1,
                "deployment_id": self.deployment_id,
                "operation_id": uuid4().hex,
                "image_id": spec.id,
                "tag": spec.tag,
            },
        )
        try:
            _persist_registry_image_protection(
                self.registry_usage_store,
                spec.tag,
                owner,
                touch=True,
                persistent=True,
            )
        except (OSError, TypeError, ValueError) as exc:
            raise RegistryImageReferenceUnavailable(
                "registry image-build reference could not be persisted"
            ) from exc
        return _RegistryImageBuildReference(spec.id, spec.tag, owner)

    def _release_registry_image_reference(
        self,
        image_ref: str,
        owner: str,
    ) -> None:
        coordinates = _private_registry_image_coordinates(image_ref)
        store = self.registry_usage_store
        if store is None or coordinates is None:
            return
        repository, tag = coordinates
        try:
            with _REGISTRY_LEASE_COORDINATION_LOCK:
                store.release_lease(
                    repository,
                    tag,
                    owner,
                )
        except (OSError, TypeError, ValueError):
            # A leaked durable reference is conservative. Explicit
            # reconciliation may remove it after proving the owner terminal.
            return

    def _release_registry_route_reference(self, route: SandboxRoute) -> None:
        self._release_registry_image_reference(
            str(route.spec.get("image") or ""),
            _registry_route_reference_owner(
                route,
                deployment_id=self.deployment_id,
                route_generation=route.generation,
            ),
        )

    def _release_registry_image_build_reference(
        self,
        reference: _RegistryImageBuildReference | None,
    ) -> None:
        if reference is not None:
            self._release_registry_image_reference(reference.tag, reference.owner)

    def _write_registry_lease_unavailable(
        self,
        exc: RegistryImageReferenceUnavailable,
    ) -> None:
        self._write_json(
            {
                "error": "registry image-use state is unavailable",
                "retryable": True,
                "details": str(exc),
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
            body = self._read_raw_body()
            raw = json.loads(body.decode("utf-8")) if body else None
            if not isinstance(raw, dict):
                raise ValueError("image build payload must be a JSON object")
            context_reference = uploaded_build_context_reference(
                raw, self.build_context_store
            )
            spec = ImageBuildSpec.from_dict(raw)
            spec.validate()
            push = bool(raw.get("push", False))
            trace_id = _request_trace_id(self, "image-build", spec.id)
            with trace_span(
                self.metrics_store,
                trace_id,
                "gateway.image_build",
                attributes={
                    "image_id": spec.id,
                    "tag": spec.tag,
                    "push": push,
                    "local_build_enabled": self.local_image_builds_enabled,
                },
            ) as root:
                if self.local_image_builds_enabled and self.image_manager is not None:
                    build_reference = self._begin_registry_image_build_reference(
                        spec,
                        push=push,
                    )
                    with trace_span(
                        self.metrics_store,
                        trace_id,
                        "gateway.image_build_local",
                        parent_span_id=root.span_id,
                    ) as span:
                        try:
                            with uploaded_build_context(
                                raw, self.build_context_store
                            ) as context_path:
                                build_spec = spec
                                span.set_attribute(
                                    "uploaded_context", context_path is not None
                                )
                                if context_path is not None:
                                    build_spec = ImageBuildSpec(
                                        id=spec.id,
                                        tag=spec.tag,
                                        context_path=str(context_path),
                                        dockerfile=spec.dockerfile,
                                        build_args=spec.build_args,
                                        labels=spec.labels,
                                    )
                                record, result = self.image_manager.build(build_spec)
                                push_result = (
                                    self.image_manager.runtime.push(build_spec.tag)
                                    if push
                                    else None
                                )
                                if push_result is not None:
                                    record = self.image_manager.mark_pushed(
                                        record.id,
                                        manifest_digest=(
                                            self._managed_registry_manifest_digest(
                                                build_spec.tag
                                            )
                                        ),
                                    )
                        finally:
                            self._release_registry_image_build_reference(
                                build_reference
                            )
                    if self.routing_store is not None:
                        self.routing_store.clear_pending_image_build(spec.id)
                    root.set_attribute("outcome", "built_locally")
                    payload: dict[str, Any] = {
                        "image": record.to_dict(),
                        "command": list(result.argv),
                        "exitCode": result.exit_code,
                        "location": "control-plane",
                    }
                    if push_result is not None:
                        payload["pushCommand"] = list(push_result.argv)
                        payload["pushExitCode"] = push_result.exit_code
                    self._write_json(
                        payload,
                        status=HTTPStatus.CREATED,
                    )
                    return
                with trace_span(
                    self.metrics_store,
                    trace_id,
                    "gateway.image_build_select_builder",
                    parent_span_id=root.span_id,
                ) as span:
                    heartbeat = self._select_builder_node()
                    span.set_attribute(
                        "selected_node_id", heartbeat.node_id if heartbeat else ""
                    )
                    span.set_attribute(
                        "selected_job_id", heartbeat.job_id if heartbeat else ""
                    )
                if heartbeat is None:
                    if self.routing_store is not None:
                        self.routing_store.upsert_pending_image_build(spec.id, spec.tag)
                        pending_builds = self.routing_store.pending_image_build_count()
                    else:
                        pending_builds = 0
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
                with trace_span(
                    self.metrics_store,
                    trace_id,
                    "gateway.image_build_enqueue",
                    parent_span_id=root.span_id,
                    attributes={"node_id": heartbeat.node_id},
                ):
                    if self.routing_store is not None:
                        self.routing_store.upsert_pending_image_build(spec.id, spec.tag)
                if context_reference is not None:
                    with trace_span(
                        self.metrics_store,
                        trace_id,
                        "gateway.image_build_context_sync",
                        parent_span_id=root.span_id,
                        attributes={"node_id": heartbeat.node_id},
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
                build_reference = self._begin_registry_image_build_reference(
                    spec,
                    push=push,
                )
                with trace_span(
                    self.metrics_store,
                    trace_id,
                    "gateway.image_build_proxy_builder",
                    parent_span_id=root.span_id,
                    attributes={"node_id": heartbeat.node_id},
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
                        span.set_attribute("node_timings", node_timings)
                        root.set_attribute("node_timings", node_timings)
                accepted_build_response = 200 <= response.status < 300
                terminal_build_response = _image_build_response_terminal(
                    response_payload
                ) or (
                    not 200 <= response.status < 300
                    and response.status < 500
                    and response.status not in {408, 425, 429}
                )
                if (
                    accepted_build_response or terminal_build_response
                ) and self.routing_store is not None:
                    self.routing_store.clear_pending_image_build(spec.id)
                if terminal_build_response:
                    self._release_registry_image_build_reference(build_reference)
                if 200 <= response.status < 300 and self.image_manager is not None:
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
            body = self._read_raw_body()
            raw = json.loads(body.decode("utf-8")) if body else None
            if not isinstance(raw, dict):
                raise ValueError("image pull payload must be a JSON object")
            image = str(raw.get("image") or "")
            if not image.strip():
                raise ValueError("image is required.")
            count = int(raw.get("count") or 1)
            resources = _prepared_resources_from_payload(raw)
            sandbox_nodes_only = bool(
                raw.get("sandbox_nodes_only", raw.get("sandboxNodesOnly", True))
            )
            if count <= 0:
                raise ValueError("count must be positive.")
        except (json.JSONDecodeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        image, image_error = self._resolve_sandbox_image_reference(image)
        if image_error is not None:
            self._write_json(image_error, status=HTTPStatus.BAD_REQUEST)
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
            route = self._discover_sandbox_route(sandbox_id)
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

        try:
            body = (
                self._read_raw_body(max_bytes=DEFAULT_MAX_PROXY_BODY_BYTES)
                if self.command in {"POST", "PUT", "PATCH"}
                else None
            )
        except ValueError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        extra_headers: dict[str, str] | None = None
        if self.command == "DELETE" and route.generation > 0:
            route = self.routing_store.prepare_sandbox_delete(sandbox_id) or route
            extra_headers = {
                SANDBOX_GENERATION_HEADER: str(route.generation),
                SANDBOX_OPERATION_ID_HEADER: route.delete_operation_id,
            }
        response = self._proxy_request(
            route.node_url,
            self.path,
            method=self.command,
            body=body,
            extra_headers=extra_headers,
        )
        if (
            self.command == "POST"
            and path.endswith("/exec")
            and 200 <= response.status < 300
        ):
            session = response.json().get("session")
            session_id = session.get("id") if isinstance(session, dict) else None
            if isinstance(session_id, str) and session_id:
                session_route = exec_session_route(session_id)
                if session_route is not None and (
                    session_route.sandbox_id != sandbox_id
                    or session_route.node_id != route.node_id
                    or session_route.job_id != route.job_id
                ):
                    self._write_json(
                        {"error": "node returned an exec session for another route"},
                        status=HTTPStatus.BAD_GATEWAY,
                    )
                    return
                if session_route is None:
                    self.routing_store.upsert_exec(
                        ExecRoute(
                            session_id=session_id,
                            sandbox_id=sandbox_id,
                            node_id=route.node_id,
                            job_id=route.job_id,
                            node_url=route.node_url,
                        )
                    )
        if self.command == "DELETE" and 200 <= response.status < 300:
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
                return
            removed = self.routing_store.delete_sandbox_if_current(
                sandbox_id,
                generation=route.generation,
                delete_operation_id=route.delete_operation_id,
            )
            if removed is not None:
                self._release_registry_route_reference(removed)
        self._send_proxied_response(response)

    def _route_exec_request(self, session_id: str, path: str) -> None:
        session_route = exec_session_route(session_id)
        route = None
        if session_route is not None:
            heartbeat = self._heartbeat_for_route(
                node_id=session_route.node_id,
                job_id=session_route.job_id,
                node_url="",
            )
            if heartbeat is not None and heartbeat.node_url:
                route = ExecRoute(
                    session_id=session_id,
                    sandbox_id=session_route.sandbox_id,
                    node_id=session_route.node_id,
                    job_id=session_route.job_id,
                    node_url=heartbeat.node_url,
                )
            else:
                # A rolling-upgrade gateway may have persisted the mapping
                # before it understood routable session ids.
                route = self.routing_store.get_exec(session_id)
        else:
            route = self.routing_store.get_exec(session_id)
        if route is None:
            self._write_json(
                {"error": "exec route not found"}, status=HTTPStatus.NOT_FOUND
            )
            return
        if session_route is None and self._exec_route_is_proven_stale(route):
            self.routing_store.delete_sandbox(route.sandbox_id)
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

    def _discover_sandbox_route(self, sandbox_id: str) -> SandboxRoute | None:
        heartbeats = self._ready_heartbeats()
        for heartbeat in heartbeats:
            if not heartbeat.inventory_complete:
                continue
            item = next(
                (item for item in heartbeat.inventory if item.sandbox_id == sandbox_id),
                None,
            )
            if item is None:
                continue
            # Normally heartbeat reconciliation already persisted this. This
            # reconstruction closes the narrow receipt/create race without a
            # network round trip; the subsequent exact GET recovers full spec.
            route = SandboxRoute(
                sandbox_id=sandbox_id,
                node_id=heartbeat.node_id,
                job_id=heartbeat.job_id,
                node_url=heartbeat.node_url or "",
                resources=item.resources,
                state=item.state or "running",
                generation=item.generation,
                create_operation_id=item.operation_id,
                spec_hash=item.spec_hash,
                node_epoch=heartbeat.node_epoch,
                activity_epoch=heartbeat.activity_epoch,
            )
            self.routing_store.upsert_sandbox(route)
            return self.routing_store.get_sandbox_readonly(sandbox_id) or route

        deadline = time.monotonic() + NODE_DISCOVERY_TOTAL_TIMEOUT_SECONDS
        for heartbeat in heartbeats:
            if heartbeat.inventory_complete:
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/sandboxes",
                method="GET",
                timeout_seconds=min(
                    NODE_RECOVERY_PROXY_TIMEOUT_SECONDS,
                    remaining,
                ),
            )
            if response.status >= 400:
                continue
            raw_sandboxes = response.json().get("sandboxes")
            if not isinstance(raw_sandboxes, list):
                continue
            for record in raw_sandboxes:
                spec = record.get("spec") if isinstance(record, dict) else None
                if isinstance(spec, dict) and spec.get("id") == sandbox_id:
                    route = _route_with_sandbox_record(
                        _sandbox_route_from_heartbeat(
                            heartbeat,
                            sandbox_id,
                            spec,
                            state=_sandbox_record_state(record, default="running"),
                        ),
                        record,
                    )
                    self.routing_store.upsert_sandbox(route)
                    route = self.routing_store.get_sandbox_readonly(sandbox_id) or route
                    self._ensure_registry_route_reference(route, touch=True)
                    return route
        return None

    def _sandbox_route_is_proven_stale(self, route: SandboxRoute) -> bool:
        if (route.state or "unknown").lower() in {"creating", "unknown"}:
            return False
        heartbeat = self._heartbeat_for_route(
            node_id=route.node_id,
            job_id=route.job_id,
            node_url=route.node_url,
        )
        return _heartbeat_proves_route_absent(
            heartbeat,
            route_created_at=route.created_at,
            route_updated_at=route.updated_at,
            heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
        )

    def _exec_route_is_proven_stale(self, route: ExecRoute) -> bool:
        heartbeat = self._heartbeat_for_route(
            node_id=route.node_id,
            job_id=route.job_id,
            node_url=route.node_url,
        )
        return _heartbeat_proves_route_absent(
            heartbeat,
            route_created_at=route.created_at,
            route_updated_at=route.updated_at,
            heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
        )

    def _heartbeat_for_route(
        self,
        *,
        node_id: str,
        job_id: str,
        node_url: str,
    ) -> NodeHeartbeat | None:
        normalized_node_url = node_url.rstrip("/")
        for heartbeat in self.store.load().values():
            if heartbeat.node_id == node_id or heartbeat.job_id == job_id:
                return heartbeat
            if (
                heartbeat.node_url
                and heartbeat.node_url.rstrip("/") == normalized_node_url
            ):
                return heartbeat
        return None

    def _select_node(
        self,
        requested: ResourceQuantity,
        *,
        image: str | None = None,
        required_capabilities: tuple[str, ...] = (),
    ) -> NodeHeartbeat | None:
        routes = list(self.routing_store.sandbox_routes_readonly())
        candidates = [
            heartbeat
            for heartbeat in self._ready_sandbox_heartbeats()
            if agent_version_is_compatible(heartbeat.agent_version)
            and all(
                has_capability(heartbeat.capabilities, capability)
                for capability in required_capabilities
            )
            and _node_can_fit(heartbeat, requested, routes)
        ]
        if not candidates:
            return None
        image_node_ids = self._nodes_with_image(
            image or "",
            candidates,
            probe_uncached=False,
        )
        if image_node_ids:
            candidates = [
                heartbeat
                for heartbeat in candidates
                if heartbeat.node_id in image_node_ids
            ]
        return sorted(
            candidates,
            key=lambda heartbeat: (
                _resource_slack(
                    _node_available_resources(heartbeat, routes), requested
                ),
                heartbeat.node_id,
            ),
        )[0]

    def _select_and_reserve_node(
        self,
        sandbox_id: str,
        requested: ResourceQuantity,
        *,
        image: str | None = None,
        spec: dict[str, Any],
        spec_hash: str,
    ) -> tuple[NodeHeartbeat, SandboxRoute] | None:
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
                    required_capabilities=(
                        (FORK_LOCAL_CAPABILITY, DISK_QUOTA_CAPABILITY)
                        if bool(spec.get("forkable"))
                        else ()
                    ),
                )
                if heartbeat is None:
                    return None
                now = utc_now()
                route = self.routing_store.allocate_sandbox_create(
                    SandboxRoute(
                        sandbox_id=sandbox_id,
                        node_id=heartbeat.node_id,
                        job_id=heartbeat.job_id,
                        node_url=heartbeat.node_url or "",
                        resources=requested,
                        spec=dict(spec),
                        state="creating",
                        node_epoch=heartbeat.node_epoch,
                        activity_epoch=heartbeat.activity_epoch,
                        created_at=now.isoformat(),
                        updated_at=now.isoformat(),
                    ),
                    spec_hash=spec_hash,
                )
                return heartbeat, route
        finally:
            _GATEWAY_SCHEDULING_LOCK.release()

    def _resolve_sandbox_image_reference(
        self,
        image: str,
    ) -> tuple[str, dict[str, Any] | None]:
        existing_digest = manifest_digest_from_image_ref(image)
        if existing_digest:
            protected_digest = self._managed_registry_manifest_digest(image)
            if (
                self.registry_url
                and _managed_registry_image_coordinates(image, self.registry_url)
                is not None
                and protected_digest != existing_digest
            ):
                return image, {
                    "error": "managed registry digest protection is unavailable",
                    "retryable": True,
                    "image": image,
                }
            return image, None
        direct_digest = self._managed_registry_manifest_digest(image)
        if direct_digest:
            return image_ref_with_manifest_digest(image, direct_digest), None
        if not _looks_like_image_id_reference(image):
            return image, None
        matches = self._image_records_across_nodes(image_id=image)
        if not matches:
            return image, None
        available = [
            record
            for record in matches
            if _image_record_available_to_sandboxes(record)
            and isinstance(record.get("tag"), str)
            and record.get("tag")
            and not self._image_record_missing_registry_manifest(record)
        ]
        if available:
            selected = sorted(
                available,
                key=lambda record: (
                    0 if record.get("location") == "control-plane" else 1,
                    str(record.get("tag") or ""),
                ),
            )[0]
            selected = self._image_record_with_registry_digest(selected)
            selected_tag = str(selected["tag"])
            digest = normalize_manifest_digest(
                str(selected.get("manifest_digest") or "")
            )
            if (
                not digest
                and self.registry_url
                and _managed_registry_image_coordinates(selected_tag, self.registry_url)
                is not None
            ):
                return image, {
                    "error": "managed registry digest protection is unavailable",
                    "retryable": True,
                    "image_id": image,
                }
            if (
                digest
                and selected.get("location") == "control-plane"
                and self.image_manager is not None
            ):
                try:
                    self.image_manager.store.upsert(ImageRecord.from_dict(selected))
                except ValueError:
                    pass
            return image_ref_with_manifest_digest(selected_tag, digest), None
        return image, {
            "error": (
                "image id exists, but it is not available to sandbox nodes; "
                "build with push=true and a pullable registry tag, then create "
                "the sandbox with that image id or registry tag"
            ),
            "image_id": image,
            "matches": [_image_record_summary(record) for record in matches],
        }

    def _image_record_missing_registry_manifest(self, record: dict[str, Any]) -> bool:
        tag = str(record.get("tag") or "")
        if not self.registry_url or not _image_record_requires_registry_manifest(
            record,
            self.registry_url,
        ):
            return False
        parsed = registry_repository_tag_from_image_ref(tag)
        if parsed is None:
            return False
        repository, image_tag = parsed
        try:
            recorded_digest = normalize_manifest_digest(
                str(record.get("manifest_digest") or "")
            )
            resolved_digest = RegistryClient(self.registry_url).manifest_digest(
                repository,
                recorded_digest or image_tag,
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

    def _select_capable_node(self, capability: str) -> NodeHeartbeat | None:
        candidates = [
            heartbeat
            for heartbeat in self._ready_heartbeats()
            if capability in heartbeat.capabilities
            and agent_version_is_compatible(heartbeat.agent_version)
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

    def _image_cache_candidates(
        self,
        *,
        resources: ResourceQuantity,
        sandbox_nodes_only: bool,
    ) -> list[NodeHeartbeat]:
        routes = (
            list(self.routing_store.sandbox_routes_readonly())
            if self.routing_store is not None
            else []
        )
        candidates = []
        for heartbeat in self._ready_heartbeats():
            if "image-cache" not in heartbeat.capabilities:
                continue
            if not agent_version_is_compatible(heartbeat.agent_version):
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
            and agent_version_is_compatible(heartbeat.agent_version)
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
        if self.routing_store is None:
            return {"scheduled": 0, "completed": 0, "warmups": []}
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
            and agent_version_is_compatible(heartbeat.agent_version)
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
            and _managed_registry_image_coordinates(image, self.registry_url)
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
            return self._proxy_request(
                node_url,
                "/v1/images/pull",
                method="POST",
                body=json.dumps({"image": image}).encode("utf-8"),
                timeout_seconds=IMAGE_PULL_PROXY_TIMEOUT_SECONDS,
            )

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
        return self._proxy_request(
            heartbeat.node_url or "",
            "/v1/images/pull",
            method="POST",
            body=json.dumps(payload).encode("utf-8"),
            timeout_seconds=IMAGE_PULL_PROXY_TIMEOUT_SECONDS,
        )

    def _ready_heartbeats(self) -> list[NodeHeartbeat]:
        now = utc_now()
        return [
            heartbeat
            for heartbeat in self.store.load().values()
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

    def _proxy_to_node(self) -> bool:
        if self.upstream_node_url is None:
            return False
        if not _is_node_api_path(self.path):
            return False

        body = None
        is_sandbox_create = (
            self.command == "POST" and urlparse(self.path).path == "/v1/sandboxes"
        )
        limiter = self.sandbox_create_limiter if is_sandbox_create else None
        limiter_acquired = False
        try:
            if limiter is not None and not limiter.acquire(blocking=False):
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
                return True
            limiter_acquired = limiter is not None
            if self.command in {"POST", "PUT", "PATCH"}:
                try:
                    body = self._read_raw_body(
                        max_bytes=(
                            DEFAULT_MAX_JSON_BODY_BYTES
                            if is_sandbox_create
                            else DEFAULT_MAX_PROXY_BODY_BYTES
                        )
                    )
                except ValueError as exc:
                    self._write_json(
                        {"error": str(exc)},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return True
            response = self._proxy_request(
                self.upstream_node_url,
                self.path,
                method=self.command,
                body=body,
            )
            self._send_proxied_response(response)
        except OSError as exc:
            self._write_json(
                {"error": f"upstream node request failed: {exc}"},
                status=HTTPStatus.BAD_GATEWAY,
            )
        finally:
            if limiter_acquired:
                limiter.release()
        return True

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
        if self.node_control_bearer_token is not None:
            headers["Authorization"] = f"Bearer {self.node_control_bearer_token}"
        proxied = request.Request(
            node_url.rstrip("/") + path,
            data=body,
            method=method,
            headers=headers,
        )
        try:
            with _open_node_request(
                proxied,
                timeout=timeout_seconds,
                authenticated=self.node_control_bearer_token is not None,
            ) as response:
                return ProxiedResponse(
                    response.status, response.headers, response.read()
                )
        except error.HTTPError as exc:
            return ProxiedResponse(exc.code, exc.headers, exc.read())
        except error.URLError as exc:
            body = json.dumps({"error": f"node request failed: {exc.reason}"}).encode(
                "utf-8"
            )
            return ProxiedResponse(HTTPStatus.BAD_GATEWAY, {}, body)
        except OSError as exc:
            body = json.dumps({"error": f"node request failed: {exc}"}).encode("utf-8")
            return ProxiedResponse(HTTPStatus.BAD_GATEWAY, {}, body)

    def _send_proxied_response(self, response: ProxiedResponse) -> None:
        structured_error = _structured_proxy_error(response)
        if structured_error is not None:
            self._write_json(structured_error, status=response.status)
            return
        self.send_response(response.status)
        self._copy_response_headers(response.headers, len(response.body))
        self.end_headers()
        self.wfile.write(response.body)

    def _copy_response_headers(self, headers: Any, content_length: int) -> None:
        for key, value in headers.items():
            if key.lower() in {"connection", "transfer-encoding", "content-length"}:
                continue
            self.send_header(key, value)
        self.send_header("Content-Length", str(content_length))

    def _check_authorized(self) -> bool:
        if self.gateway_bearer_token is None:
            return True
        if self._token_matches(
            self.gateway_bearer_token,
            allow_ucloud_sandbox_header=True,
        ):
            return True
        return self._write_unauthorized()

    def _check_heartbeat_authorized(self) -> bool:
        if self.heartbeat_bearer_token is None:
            # Compatibility mode: deployments that omit a distinct heartbeat
            # credential retain the historical gateway-token behavior.
            return self._check_authorized()
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
        body = json.dumps({"error": "unauthorized"}, indent=2).encode("utf-8")
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("Content-Type", "application/json")
        self.send_header("WWW-Authenticate", "Bearer")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        return False


def build_server(
    host: str,
    port: int,
    heartbeat_file: Path,
    *,
    routing_file: Path | None = None,
    upstream_node_url: str | None = None,
    gateway_bearer_token: str | None = None,
    heartbeat_bearer_token: str | None = None,
    node_control_bearer_token: str | None = None,
    deployment_id: str | None = None,
    heartbeat_ttl_seconds: int = 120,
    image_file: Path | None = None,
    image_runtime: DockerImageRuntime | None = None,
    local_image_builds_enabled: bool | None = None,
    metrics_file: Path | None = None,
    registry_url: str | None = None,
    registry_usage_file: Path | None = None,
    max_concurrent_sandbox_creates: int = DEFAULT_MAX_CONCURRENT_SANDBOX_CREATES,
    build_context_store_dir: Path | None = None,
) -> HighBacklogThreadingHTTPServer:
    if node_control_bearer_token is not None and not node_control_bearer_token.strip():
        raise ValueError("node control bearer token cannot be empty")
    store = HeartbeatStore(heartbeat_file)
    routing_store = RoutingStore(routing_file) if routing_file is not None else None
    metrics_store = MetricsStore(metrics_file) if metrics_file is not None else None
    registry_usage_store = (
        RegistryUsageStore(registry_usage_file)
        if registry_usage_file is not None
        else None
    )
    image_manager = (
        ImageManager(
            ImageStore(image_file),
            image_runtime or DockerImageRuntime(dry_run=True),
        )
        if image_file is not None
        else None
    )
    context_path_source = image_file or heartbeat_file
    build_context_store = BuildContextBlobStore(
        build_context_store_dir
        or context_path_source.parent / f"{context_path_source.stem}-contexts",
        max_blob_bytes=DEFAULT_MAX_PROXY_BODY_BYTES,
        max_total_bytes=DEFAULT_MAX_BUILD_CONTEXT_STORE_BYTES,
        max_entries=DEFAULT_MAX_BUILD_CONTEXT_ENTRIES,
        max_age_seconds=DEFAULT_MAX_BUILD_CONTEXT_AGE_SECONDS,
    )

    class BoundHandler(ControlPlaneHandler):
        pass

    BoundHandler.store = store
    BoundHandler.routing_store = routing_store
    BoundHandler.upstream_node_url = upstream_node_url
    BoundHandler.gateway_bearer_token = gateway_bearer_token
    BoundHandler.heartbeat_bearer_token = heartbeat_bearer_token
    BoundHandler.node_control_bearer_token = node_control_bearer_token
    BoundHandler.deployment_id = (deployment_id or "").strip()
    BoundHandler.heartbeat_ttl_seconds = heartbeat_ttl_seconds
    BoundHandler.image_manager = image_manager
    BoundHandler.build_context_store = build_context_store
    BoundHandler.local_image_builds_enabled = (
        image_runtime is not None
        if local_image_builds_enabled is None
        else local_image_builds_enabled
    )
    BoundHandler.metrics_store = metrics_store
    BoundHandler.registry_url = registry_url
    BoundHandler.registry_status_cache = None
    BoundHandler.registry_status_cache_at = 0.0
    BoundHandler.registry_status_lock = RLock()
    BoundHandler.registry_usage_store = registry_usage_store
    BoundHandler.max_concurrent_sandbox_creates = max(
        0,
        int(max_concurrent_sandbox_creates),
    )
    BoundHandler.sandbox_create_limiter = (
        BoundedSemaphore(BoundHandler.max_concurrent_sandbox_creates)
        if BoundHandler.max_concurrent_sandbox_creates > 0
        else None
    )
    return HighBacklogThreadingHTTPServer((host, port), BoundHandler)


def _is_node_api_path(path: str) -> bool:
    return path == "/v1/images" or path.startswith(
        (
            "/v1/sandboxes",
            "/v1/exec",
            "/v1/images/",
        )
    )


def _request_trace_id(
    handler: ControlPlaneHandler,
    operation: str,
    object_id: str,
) -> str:
    header = str(handler.headers.get("X-Trace-Id") or "").strip()
    if header and len(header) <= 128 and all(ch not in header for ch in "\r\n"):
        return header
    safe_operation = _safe_trace_component(operation)
    safe_object = _safe_trace_component(object_id)[:48]
    return f"{safe_operation}-{safe_object}-{uuid4().hex[:12]}"


def _safe_trace_component(value: str) -> str:
    cleaned = "".join(
        ch if ch.isalnum() or ch in "._-" else "-" for ch in value.strip()
    ).strip("-")
    return cleaned or "trace"


def _sandbox_id_from_path(path: str) -> str | None:
    prefix = "/v1/sandboxes/"
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix) :]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


def _sandbox_fork_source_from_path(path: str) -> str | None:
    prefix = "/v1/sandboxes/"
    suffix = "/forks"
    if not path.startswith(prefix) or not path.endswith(suffix):
        return None
    encoded = path[len(prefix) : -len(suffix)]
    source_id = unquote(encoded)
    if not source_id or "/" in source_id:
        return None
    return source_id


def _build_context_digest_from_path(path: str) -> str | None:
    prefix = "/v1/image-contexts/"
    if not path.startswith(prefix):
        return None
    digest = unquote(path[len(prefix) :])
    return digest if digest and "/" not in digest else None


def _image_build_key_from_path(path: str) -> str | None:
    prefix = "/v1/images/builds/"
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix) :]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


def _exec_session_id_from_path(path: str) -> str | None:
    prefix = "/v1/exec/"
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix) :]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


def _prepare_id_from_path(path: str) -> str | None:
    prefix = "/v1/capacity/prepare/"
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix) :]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


def _builder_prepare_id_from_path(path: str) -> str | None:
    prefix = "/v1/builders/prepare/"
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix) :]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


def _truthy_query_param(parsed: Any, name: str) -> bool:
    values = parse_qs(str(getattr(parsed, "query", ""))).get(name, [])
    return any(
        str(value).lower() in {"1", "true", "yes", "on", "full"} for value in values
    )


def _payload_value(raw: dict[str, Any], *keys: str, default: Any) -> Any:
    for key in keys:
        if raw.get(key) is not None:
            return raw[key]
    return default


def _prepared_resources_from_payload(raw: dict[str, Any]) -> ResourceQuantity:
    resources: dict[str, Any] = {}
    nested = raw.get("resources")
    if isinstance(nested, dict):
        resources.update(nested)
    if raw.get("cpus") is not None:
        resources["vcpu"] = raw.get("cpus")
    for key in ("vcpu", "cpu", "memory_mb", "memoryMb", "disk_mb", "diskMb"):
        if raw.get(key) is not None:
            resources[key] = raw.get(key)
    for label, aliases in (
        ("vcpu", ("vcpu", "cpu")),
        ("memory_mb", ("memory_mb", "memoryMb")),
        ("disk_mb", ("disk_mb", "diskMb")),
    ):
        value = next((resources[key] for key in aliases if key in resources), None)
        if value is None:
            continue
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{label} must be numeric.") from exc
        if not math.isfinite(parsed) or parsed < 0:
            raise ValueError(f"{label} must be non-negative and finite.")
    return ResourceQuantity.from_dict(resources)


def _validate_prepared_resources(resources: ResourceQuantity) -> None:
    if resources.vcpu < 0:
        raise ValueError("vcpu must be non-negative.")
    if resources.memory_mb < 0:
        raise ValueError("memory_mb must be non-negative.")
    if resources.disk_mb < 0:
        raise ValueError("disk_mb must be non-negative.")
    if resources == ResourceQuantity():
        raise ValueError("prepared capacity resources are required.")


def _sandbox_route_from_heartbeat(
    heartbeat: NodeHeartbeat,
    sandbox_id: str,
    spec: dict[str, Any] | None,
    *,
    state: str = "unknown",
) -> SandboxRoute:
    stored_spec = dict(spec) if isinstance(spec, dict) else {}
    resources = (
        SandboxSpec.from_dict(stored_spec).requested_resources()
        if stored_spec
        else ResourceQuantity()
    )
    return SandboxRoute(
        sandbox_id=sandbox_id,
        node_id=heartbeat.node_id,
        job_id=heartbeat.job_id,
        node_url=heartbeat.node_url or "",
        resources=resources,
        spec=stored_spec,
        state=state,
        node_epoch=heartbeat.node_epoch,
        activity_epoch=heartbeat.activity_epoch,
    )


def _route_with_sandbox_record(
    route: SandboxRoute,
    record: dict[str, Any],
) -> SandboxRoute:
    spec = record.get("spec")
    spec = dict(spec) if isinstance(spec, dict) else dict(route.spec)
    generation = _record_generation(record)
    return SandboxRoute(
        sandbox_id=route.sandbox_id,
        node_id=route.node_id,
        job_id=route.job_id,
        node_url=route.node_url,
        resources=route.resources,
        spec=spec,
        state=_sandbox_record_state(record, default="running"),
        generation=route.generation if generation is None else generation,
        create_operation_id=str(
            record.get("operation_id") or route.create_operation_id
        ),
        spec_hash=str(record.get("spec_hash") or route.spec_hash),
        delete_operation_id=route.delete_operation_id,
        node_epoch=route.node_epoch,
        activity_epoch=route.activity_epoch,
        created_at=route.created_at,
        updated_at=route.updated_at,
    )


def _sandbox_record_state(record: dict[str, Any], *, default: str) -> str:
    for key in ("state", "status"):
        raw = record.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return default


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
    return generation if generation >= 0 else None


def _sandbox_record_matches_route(
    record: dict[str, Any],
    route: SandboxRoute,
    requested: SandboxSpec,
) -> bool:
    if not _sandbox_record_matches_spec(record, requested):
        return False
    if route.generation <= 0:
        return (_record_generation(record) or 0) == 0
    return (
        _record_generation(record) == route.generation
        and str(record.get("operation_id") or "") == route.create_operation_id
        and str(record.get("spec_hash") or "") == route.spec_hash
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


def _sandbox_fork_operation_id(
    source_route: SandboxRoute,
    target_spec_hash: str,
) -> str:
    identity = "\0".join(
        (
            source_route.sandbox_id,
            str(source_route.generation),
            source_route.spec_hash,
            target_spec_hash,
        )
    )
    return "fork-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:40]


def _public_fork_request_is_batch(raw: dict[str, Any]) -> bool:
    batch = "sandboxes" in raw
    if "sandbox" in raw and "target" in raw:
        raise ValueError("fork payload cannot contain both sandbox and target")
    single = "sandbox" in raw or "target" in raw
    if batch and single:
        raise ValueError("fork payload cannot contain both sandbox and sandboxes")
    reserved = sorted(key for key in raw if str(key).startswith("_ucloud_"))
    if reserved:
        raise ValueError(
            "public fork payload cannot contain internal fencing fields: "
            + ", ".join(reserved)
        )
    return batch


def _sandbox_fork_request_body_upper_bound(
    source_route: SandboxRoute,
    targets: tuple[SandboxSpec, ...],
    *,
    batch: bool,
) -> int:
    """Size the expanded internal request before persisting reservations."""

    preview_routes = tuple(
        SandboxRoute(
            sandbox_id=target.id,
            node_id=source_route.node_id,
            job_id=source_route.job_id,
            node_url=source_route.node_url,
            resources=target.requested_resources(),
            spec=target.to_dict(),
            state="creating",
            # SQLite generations are signed 64-bit integers.  Using the
            # largest value makes this a safe serialization upper bound.
            generation=(2**63) - 1,
            create_operation_id=_sandbox_fork_operation_id(
                source_route,
                sandbox_spec_fingerprint(target),
            ),
            spec_hash=sandbox_spec_fingerprint(target),
        )
        for target in targets
    )
    body = (
        _sandbox_fork_batch_request_body(source_route, targets, preview_routes)
        if batch
        else _sandbox_fork_request_body(
            source_route,
            targets[0],
            preview_routes[0],
        )
    )
    return len(body)


def _sandbox_fork_request_body(
    source_route: SandboxRoute,
    target: SandboxSpec,
    child_route: SandboxRoute,
) -> bytes:
    payload = {
        "sandbox": target.to_dict(),
        "_ucloud_operation": {
            "operation_id": child_route.create_operation_id,
            "generation": child_route.generation,
            "kind": "create",
            "spec_hash": child_route.spec_hash,
        },
        "_ucloud_source": {
            "generation": source_route.generation,
            "spec_hash": source_route.spec_hash,
        },
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sandbox_fork_batch_request_body(
    source_route: SandboxRoute,
    targets: tuple[SandboxSpec, ...],
    child_routes: tuple[SandboxRoute, ...],
) -> bytes:
    if not targets or len(targets) != len(child_routes):
        raise ValueError("fork batch requires one child route per sandbox")
    payload = {
        "sandboxes": [target.to_dict() for target in targets],
        "_ucloud_operations": [
            {
                "operation_id": route.create_operation_id,
                "generation": route.generation,
                "kind": "create",
                "spec_hash": route.spec_hash,
            }
            for route in child_routes
        ],
        "_ucloud_source": {
            "generation": source_route.generation,
            "spec_hash": source_route.spec_hash,
        },
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sandbox_fork_record_matches_route(
    record: dict[str, Any],
    child_route: SandboxRoute,
    target: SandboxSpec,
    source_route: SandboxRoute,
) -> bool:
    try:
        source_generation = int(record.get("source_generation"))
    except (TypeError, ValueError, OverflowError):
        return False
    fork_nonce = str(record.get("fork_nonce") or "")
    return (
        _sandbox_record_matches_route(record, child_route, target)
        and str(record.get("state") or "") == "running"
        and str(record.get("creation_kind") or "") == "restore"
        and str(record.get("source_sandbox_id") or "") == source_route.sandbox_id
        and source_generation == source_route.generation
        and bool(str(record.get("checkpoint_id") or ""))
        and len(fork_nonce) == 64
        and all(character in "0123456789abcdef" for character in fork_nonce)
    )


def _sandbox_fork_result_matches_record(
    payload: dict[str, Any],
    record: dict[str, Any],
) -> bool:
    fork = payload.get("fork")
    timings = payload.get("timings")
    if not isinstance(fork, dict) or not isinstance(timings, dict):
        return False
    commands = fork.get("commands")
    return (
        payload.get("intent_persisted") is True
        and str(record.get("state") or "") == "running"
        and str(fork.get("checkpoint_id") or "")
        == str(record.get("checkpoint_id") or "")
        and fork.get("restored") is True
        and isinstance(commands, list)
        and all(
            isinstance(command, list)
            and all(isinstance(argument, str) for argument in command)
            for command in commands
        )
    )


def _sandbox_fork_batch_result_matches_routes(
    payload: dict[str, Any],
    records: object,
    forks: object,
    child_routes: tuple[SandboxRoute, ...],
    targets: tuple[SandboxSpec, ...],
    source_route: SandboxRoute,
) -> bool:
    timings = payload.get("timings")
    if (
        not isinstance(records, list)
        or not isinstance(forks, list)
        or not isinstance(timings, dict)
        or len(records) != len(child_routes)
        or len(forks) != len(child_routes)
        or len(targets) != len(child_routes)
    ):
        return False
    checkpoint_ids: set[str] = set()
    fork_nonces: set[str] = set()
    for record, fork, child_route, target in zip(
        records, forks, child_routes, targets, strict=True
    ):
        if (
            not isinstance(record, dict)
            or not isinstance(fork, dict)
            or str(fork.get("sandbox_id") or "") != target.id
            or not _sandbox_fork_record_matches_route(
                record, child_route, target, source_route
            )
            or not _sandbox_fork_result_matches_record(
                {
                    "fork": fork,
                    "timings": timings,
                    "intent_persisted": payload.get("intent_persisted"),
                },
                record,
            )
        ):
            return False
        checkpoint_ids.add(str(record.get("checkpoint_id") or ""))
        fork_nonces.add(str(record.get("fork_nonce") or ""))
    return (
        len(checkpoint_ids) == 1
        and "" not in checkpoint_ids
        and len(fork_nonces) == 1
        and "" not in fork_nonces
    )


def _enrich_sandbox_record(
    record: dict[str, Any],
    heartbeat: NodeHeartbeat,
) -> dict[str, Any]:
    enriched = dict(record)
    spec = enriched.get("spec")
    spec = spec if isinstance(spec, dict) else {}
    sandbox_id = str(
        enriched.get("id") or enriched.get("sandbox_id") or spec.get("id") or ""
    )
    image = str(enriched.get("image") or spec.get("image") or "")
    labels = enriched.get("labels")
    if not isinstance(labels, dict):
        raw_labels = spec.get("labels")
        labels = dict(raw_labels) if isinstance(raw_labels, dict) else {}
    if sandbox_id:
        enriched["id"] = sandbox_id
        enriched["sandbox_id"] = sandbox_id
    if "name" not in enriched and enriched.get("container_name"):
        enriched["name"] = enriched["container_name"]
    if image:
        enriched["image"] = image
    enriched["labels"] = {str(key): str(value) for key, value in labels.items()}
    enriched["node"] = _node_metadata(heartbeat)
    return enriched


def _route_only_sandbox_record(
    route: SandboxRoute,
    heartbeat: NodeHeartbeat | None,
    *,
    heartbeat_ttl_seconds: int = 120,
) -> dict[str, Any]:
    spec = dict(route.spec)
    if not spec:
        spec = {
            "id": route.sandbox_id,
            "resources": route.resources.to_dict(),
        }
    spec.setdefault("id", route.sandbox_id)
    image = str(spec.get("image") or "")
    labels = spec.get("labels")
    labels = dict(labels) if isinstance(labels, dict) else {}
    node_fresh = heartbeat is not None and heartbeat.is_fresh(
        utc_now(), heartbeat_ttl_seconds
    )
    cached_state = route.state or "unknown"
    route_absent = _heartbeat_proves_route_absent(
        heartbeat,
        route_created_at=route.created_at,
        route_updated_at=route.updated_at,
        heartbeat_ttl_seconds=heartbeat_ttl_seconds,
    )
    visible_state = (
        cached_state
        if cached_state == "creating" or (node_fresh and not route_absent)
        else "unknown"
    )
    record: dict[str, Any] = {
        "id": route.sandbox_id,
        "sandbox_id": route.sandbox_id,
        "state": visible_state,
        "status": visible_state,
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
        },
        "created_at": route.created_at,
        "updated_at": route.updated_at,
    }
    if image:
        record["image"] = image
    if heartbeat is not None:
        node = _node_metadata(heartbeat)
        node["fresh"] = node_fresh
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
    route_created_at: str,
    route_updated_at: str,
    heartbeat_ttl_seconds: int,
) -> bool:
    if heartbeat is None:
        return False
    if not heartbeat.is_fresh(utc_now(), heartbeat_ttl_seconds):
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
    routes: list[SandboxRoute],
) -> bool:
    if not _has_resource_values(requested):
        return False
    if requested.disk_mb > 0 and not has_capability(
        heartbeat.capabilities,
        DISK_QUOTA_CAPABILITY,
    ):
        return False
    return requested.fits_within(_node_available_resources(heartbeat, routes))


def _node_available_resources(
    heartbeat: NodeHeartbeat,
    routes: list[SandboxRoute],
) -> ResourceQuantity:
    route_reservations = _node_reserved_route_resources(heartbeat, routes)
    effective = heartbeat.effective_resources
    accounted_used = ResourceQuantity(
        vcpu=(
            heartbeat.used_resources.vcpu
            + heartbeat.reserved_resources.vcpu
            + heartbeat.build_reserved_resources.vcpu
            + route_reservations.vcpu
        ),
        memory_mb=(
            heartbeat.used_resources.memory_mb
            + heartbeat.reserved_resources.memory_mb
            + heartbeat.build_reserved_resources.memory_mb
            + route_reservations.memory_mb
        ),
        disk_mb=(
            heartbeat.used_resources.disk_mb
            + heartbeat.reserved_resources.disk_mb
            + heartbeat.build_reserved_resources.disk_mb
            + route_reservations.disk_mb
        ),
    )
    return ResourceQuantity(
        vcpu=max(0.0, effective.vcpu - accounted_used.vcpu),
        memory_mb=max(0, effective.memory_mb - accounted_used.memory_mb),
        disk_mb=max(0, effective.disk_mb - accounted_used.disk_mb),
    )


def _node_reserved_route_resources(
    heartbeat: NodeHeartbeat,
    routes: list[SandboxRoute],
) -> ResourceQuantity:
    resources = ResourceQuantity()
    node_url = heartbeat.node_url or ""
    seen_routes: set[tuple[str, int, str]] = set()
    for route in routes:
        if route.node_id != heartbeat.node_id and route.node_url != node_url:
            continue
        identity = (
            route.sandbox_id,
            route.generation,
            route.create_operation_id,
        )
        if identity in seen_routes:
            continue
        seen_routes.add(identity)
        if any(
            item.sandbox_id == route.sandbox_id
            and item.generation == route.generation
            and (
                route.generation == 0
                or (
                    item.spec_hash == route.spec_hash
                    and item.operation_id == route.create_operation_id
                )
            )
            for item in heartbeat.inventory
        ):
            continue
        resources = resources + route.resources
    return resources


def _image_pull_lock(node_url: str, image: str) -> RLock:
    key = (node_url.rstrip("/"), image)
    with _IMAGE_PULL_LOCKS_GUARD:
        lock = _IMAGE_PULL_LOCKS.get(key)
        if lock is None:
            lock = RLock()
            _IMAGE_PULL_LOCKS[key] = lock
        return lock


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
) -> tuple[str, str] | None:
    """Return coordinates only when the tag targets this managed registry."""

    image_host = registry_host_from_image_ref(image_ref).lower()
    if not image_host:
        return None
    configured_host = urlparse(registry_url).netloc.lower()
    allowed_hosts = {
        "ucloud-sandbox-registry:5000",
        "localhost:5000",
        "127.0.0.1:5000",
    }
    if configured_host:
        allowed_hosts.add(configured_host)
    if image_host not in allowed_hosts:
        return None
    return registry_repository_tag_from_image_ref(image_ref)


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
            touch_many = getattr(store, "touch_images", None)
            if callable(touch_many):
                touched = touch_many(usage_refs, when=now)
            else:
                # Compatibility for test/custom stores implementing the older
                # single-reference protocol.
                touched = tuple(
                    item
                    for item in (store.touch_image(ref, when=now) for ref in usage_refs)
                    if item is not None
                )
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
    node_control_bearer_token: str | None = None,
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
                    **(
                        {"Authorization": (f"Bearer {node_control_bearer_token}")}
                        if node_control_bearer_token is not None
                        else {}
                    ),
                },
            )
            try:
                with _open_node_request(
                    req,
                    timeout=IMAGE_PULL_PROXY_TIMEOUT_SECONDS,
                    authenticated=node_control_bearer_token is not None,
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
    return response.status in {408, 425, 429, 500, 502, 503, 504}


def _node_fork_intent_states(
    payload: dict[str, Any],
    routes: tuple[SandboxRoute, ...],
) -> tuple[bool | None, ...]:
    """Read exact per-child intent state, falling back to the batch signal."""

    top_level = payload.get("intent_persisted")
    fallback = top_level if isinstance(top_level, bool) else None
    raw_intents = payload.get("intents")
    if not isinstance(raw_intents, list):
        return tuple(fallback for _route in routes)
    parsed: dict[str, bool | None] = {}
    for item in raw_intents:
        if not isinstance(item, dict):
            return tuple(fallback for _route in routes)
        sandbox_id = str(item.get("sandbox_id") or "")
        if not sandbox_id or sandbox_id in parsed:
            return tuple(fallback for _route in routes)
        value = item.get("intent_persisted")
        if value is not None and not isinstance(value, bool):
            return tuple(fallback for _route in routes)
        parsed[sandbox_id] = value
    if set(parsed) != {route.sandbox_id for route in routes}:
        return tuple(fallback for _route in routes)
    return tuple(parsed[route.sandbox_id] for route in routes)


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
) -> bool:
    if not _image_record_available_to_sandboxes(record):
        return False
    source = str(record.get("source") or "")
    if not source.startswith("build:"):
        return False
    host = registry_host_from_image_ref(str(record.get("tag") or ""))
    if not host:
        return False
    allowed = {
        "ucloud-sandbox-registry:5000",
        "localhost:5000",
        "127.0.0.1:5000",
    }
    configured = urlparse(registry_url).netloc
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
