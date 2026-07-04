from __future__ import annotations

from dataclasses import replace
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from threading import RLock
from typing import Any
from urllib import error, request
from urllib.parse import unquote, urlparse
from uuid import uuid4

from .dashboard import dashboard_asset
from .deployment import agent_version_is_compatible, service_health
from .images import (
    DockerImageRuntime,
    ImageBuildSpec,
    ImageManager,
    ImageRecord,
    ImageStore,
    image_id_from_tag,
    uploaded_build_context,
)
from .managed_registry import RegistryClient, registry_summary
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
from .routing import ExecRoute, PendingSandboxDemand, RoutingStore, SandboxRoute
from .sandbox import SandboxSpec, sandbox_specs_match


_IMAGE_PULL_LOCKS_GUARD = RLock()
_IMAGE_PULL_LOCKS: dict[tuple[str, str], RLock] = {}
_GATEWAY_SANDBOX_CREATE_LOCKS_GUARD = RLock()
_GATEWAY_SANDBOX_CREATE_LOCKS: dict[str, RLock] = {}
# Build execution is asynchronous. This timeout only covers proxying the build
# context and enqueueing the build on a builder node.
IMAGE_BUILD_PROXY_TIMEOUT_SECONDS = 30 * 60
DEFAULT_PROXY_TIMEOUT_SECONDS = 60
REGISTRY_METRICS_TIMEOUT_SECONDS = 1.5


class ProxiedResponse:
    def __init__(self, status: int, headers: Any, body: bytes) -> None:
        self.status = status
        self.headers = headers
        self.body = body

    def json(self) -> dict[str, Any]:
        try:
            decoded = json.loads(self.body.decode("utf-8")) if self.body else {}
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}


class ControlPlaneHandler(BaseHTTPRequestHandler):
    store: HeartbeatStore
    routing_store: RoutingStore | None
    upstream_node_url: str | None
    gateway_bearer_token: str | None
    heartbeat_ttl_seconds: int
    image_manager: ImageManager | None
    local_image_builds_enabled: bool
    metrics_store: MetricsStore | None
    registry_url: str | None
    server_version = "ucloud-sandboxes-control-plane/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if self.path == "/healthz":
            self._write_json(service_health("control-plane"))
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
        if parsed.path == "/v1/nodes":
            nodes = [
                heartbeat_to_dict(heartbeat)
                for heartbeat in self.store.load().values()
            ]
            self._write_json({"nodes": nodes})
            return
        if parsed.path == "/v1/demand" and self.routing_store is not None:
            self._write_json(self._demand_payload())
            return
        if parsed.path == "/v1/metrics":
            routing_state = self.routing_store.load() if self.routing_store is not None else None
            events = (
                self.metrics_store.load_events(max_events=10000)
                if self.metrics_store is not None
                else []
            )
            snapshot = build_metrics_snapshot(
                self.store.load(),
                routing_state,
                events,
                heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
            )
            builds = self._image_build_records_across_nodes()
            active_builds = [
                build for build in builds
                if build.get("status") not in {"succeeded", "failed"}
            ]
            failed_builds = [
                build for build in builds
                if build.get("status") == "failed"
            ]
            snapshot.setdefault("images", {}).update(
                {
                    "active_builds": len(active_builds),
                    "failed_builds": len(failed_builds),
                    "builds": builds,
                }
            )
            snapshot["registry"] = self._registry_status()
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
        if not self._check_authorized():
            return
        if parsed.path != "/v1/nodes/heartbeat":
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

        heartbeat = heartbeat_from_dict(raw)
        if heartbeat is None:
            self._write_json(
                {"error": "invalid heartbeat payload"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        heartbeats = self.store.upsert(heartbeat)
        stored_heartbeat = heartbeats.get(heartbeat.job_id, heartbeat)
        record_node_heartbeat(self.metrics_store, stored_heartbeat)
        self._write_json({"ok": True, "node": heartbeat_to_dict(stored_heartbeat)})

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        if not self._check_authorized():
            return
        if self._route_to_nodes(parsed.path):
            return
        if self._proxy_to_node():
            return
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

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

    def _read_raw_body(self) -> bytes:
        length_header = self.headers.get("Content-Length", "0")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc
        return self.rfile.read(max(0, length))

    def _write_json(
        self,
        payload: dict[str, Any],
        *,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
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
        if path == "/v1/sandboxes" and self.command == "GET":
            self._list_sandboxes_across_nodes()
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
        sandbox_id = _sandbox_id_from_path(path)
        if sandbox_id is not None:
            self._route_sandbox_request(sandbox_id, path)
            return True
        session_id = _exec_session_id_from_path(path)
        if session_id is not None:
            self._route_exec_request(session_id, path)
            return True
        return False

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
                item.to_dict()
                for item in self.routing_store.pending_sandboxes()
            ],
            "prepared": [
                item.to_dict() for item in self.routing_store.prepared_capacity()
            ],
            "prepared_builders": [item.to_dict() for item in prepared_builders],
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

        item = self.routing_store.upsert_prepared_capacity(
            prepare_id,
            resources,
            count=count,
            ttl_seconds=ttl_seconds,
            image=image,
        )
        prewarm = (
            self._warm_image_on_ready_nodes(
                image,
                count=count,
                resources=resources,
                sandbox_nodes_only=True,
            )
            if image
            else None
        )
        payload = {
            "prepare": item.to_dict(),
            "demand": self._demand_payload(),
        }
        if prewarm is not None:
            payload["image_prewarm"] = prewarm
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

    def _list_sandboxes_across_nodes(self) -> None:
        sandboxes: list[dict[str, Any]] = []
        for heartbeat in self._ready_sandbox_heartbeats():
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/sandboxes",
                method="GET",
            )
            if response.status >= 400:
                continue
            payload = response.json()
            raw_sandboxes = payload.get("sandboxes")
            if not isinstance(raw_sandboxes, list):
                continue
            for record in raw_sandboxes:
                if not isinstance(record, dict):
                    continue
                spec = record.get("spec")
                sandbox_id = spec.get("id") if isinstance(spec, dict) else None
                if isinstance(sandbox_id, str) and sandbox_id:
                    self.routing_store.upsert_sandbox(
                        _sandbox_route_from_heartbeat(heartbeat, sandbox_id, spec)
                    )
                    sandboxes.append(_enrich_sandbox_record(record, heartbeat))
        self._write_json({"sandboxes": sandboxes})

    def _list_images_across_nodes(self) -> None:
        self._write_json({"images": self._image_records_across_nodes()})

    def _image_records_across_nodes(self) -> list[dict[str, Any]]:
        images: list[dict[str, Any]] = []
        if self.image_manager is not None:
            for record in sorted(self.image_manager.list(), key=lambda item: item.id):
                enriched = record.to_dict()
                enriched["location"] = "control-plane"
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
                    enriched = dict(record)
                    enriched["node"] = _node_metadata(heartbeat)
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
        self._record_successful_build_image(selected)
        self._write_json({"build": selected})

    def _image_build_records_across_nodes(self) -> list[dict[str, Any]]:
        builds: list[dict[str, Any]] = []
        if self.image_manager is not None:
            for record in sorted(
                self.image_manager.list_builds(),
                key=lambda item: (item.created_at, item.build_id),
            ):
                enriched = record.to_dict()
                enriched["location"] = "control-plane"
                builds.append(enriched)
        for heartbeat in self._ready_heartbeats():
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/images/builds",
                method="GET",
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

    def _record_successful_build_image(self, build: dict[str, Any]) -> None:
        if self.image_manager is None or build.get("status") != "succeeded":
            return
        raw_image = build.get("image")
        if (
            not isinstance(raw_image, dict)
            or not _image_record_available_to_sandboxes(raw_image)
        ):
            return
        try:
            self.image_manager.store.upsert(ImageRecord.from_dict(raw_image))
        except ValueError:
            pass

    def _create_sandbox_on_node(self) -> None:
        try:
            body = self._read_raw_body()
            raw = json.loads(body.decode("utf-8")) if body else None
            if not isinstance(raw, dict):
                raise ValueError("sandbox payload must be a JSON object")
            spec = SandboxSpec.from_dict(raw)
            spec.validate()
        except (json.JSONDecodeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        with _gateway_sandbox_create_lock(spec.id):
            self._create_sandbox_on_node_locked(body, raw, spec)
        return

    def _create_sandbox_on_node_locked(
        self,
        body: bytes,
        raw: dict[str, Any],
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
                resolved_image, image_error = self._resolve_sandbox_image_reference(spec.image)
                span.set_attribute("resolved_image", resolved_image)
                if image_error is not None:
                    span.status = "error"
                    root.status = "error"
                    root.set_attribute("outcome", "image_reference_unavailable")
                    self._write_json(image_error, status=HTTPStatus.BAD_REQUEST)
                    return
                if resolved_image != spec.image:
                    spec = replace(spec, image=resolved_image)
                    raw = dict(raw)
                    raw["image"] = resolved_image
                    body = json.dumps(raw).encode("utf-8")
                    root.set_attribute("resolved_image", resolved_image)

            with trace_span(
                self.metrics_store,
                trace_id,
                "gateway.sandbox_existing_route_check",
                parent_span_id=root.span_id,
            ) as span:
                existing = self.routing_store.get_sandbox(spec.id)
                span.set_attribute("existing_route", existing is not None)
                if existing is not None:
                    if self._send_existing_sandbox_response(
                        existing,
                        spec,
                        status=HTTPStatus.OK,
                    ):
                        root.set_attribute("outcome", "recovered_existing")
                        return
                    self.routing_store.delete_sandbox(spec.id)
                    existing = None
                    span.set_attribute("deleted_stale_route", True)

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
                heartbeat = self._select_node(spec.requested_resources(), image=spec.image)
                span.set_attribute("selected_node_id", heartbeat.node_id if heartbeat else "")
                span.set_attribute("selected_job_id", heartbeat.job_id if heartbeat else "")
            if heartbeat is None:
                self.routing_store.upsert_pending(spec.id, spec.requested_resources())
                demand = self.routing_store.pending_demand()
                root.status = "error"
                root.set_attribute("outcome", "queued_no_ready_node")
                root.set_attribute("pending_resources", demand.pending_resources.to_dict())
                self._write_json(
                    {
                        "error": "no ready node has resources for sandbox request",
                        "pending_resources": demand.pending_resources.to_dict(),
                        "oldest_pending_seconds": demand.oldest_pending_seconds,
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return

            pending_before = self.routing_store.load().pending.get(spec.id)
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
                    body=body,
                )
                span.set_attribute("status_code", int(response.status))
                response_payload = response.json()
                node_timings = response_payload.get("timings")
                if isinstance(node_timings, dict):
                    span.set_attribute("node_timings", node_timings)
                    root.set_attribute("node_timings", node_timings)
            if _is_duplicate_sandbox_response(response, spec.id):
                route = _sandbox_route_from_heartbeat(heartbeat, spec.id, spec.to_dict())
                if self._send_existing_sandbox_response(
                    route,
                    spec,
                    status=HTTPStatus.CREATED,
                    pending=pending_before,
                ):
                    root.set_attribute("outcome", "recovered_duplicate")
                    return
            if 200 <= response.status < 300:
                route = _sandbox_route_from_heartbeat(heartbeat, spec.id, spec.to_dict())
                self.routing_store.upsert_sandbox(route)
                record_sandbox_scheduled(
                    self.metrics_store,
                    sandbox_id=spec.id,
                    route=route,
                    resources=spec.requested_resources(),
                    pending=pending_before,
                )
                root.set_attribute("outcome", "scheduled")
                root.set_attribute("node_id", heartbeat.node_id)
            else:
                root.status = "error"
                root.set_attribute("outcome", "node_create_failed")
                root.set_attribute("status_code", int(response.status))
            self._send_proxied_response(response)

    def _send_existing_sandbox_response(
        self,
        route: SandboxRoute,
        spec: SandboxSpec,
        *,
        status: HTTPStatus,
        pending: PendingSandboxDemand | None = None,
    ) -> bool:
        record = self._sandbox_record_on_node(route.node_url, spec.id)
        if record is None or not _sandbox_record_matches_spec(record, spec):
            return False
        self.routing_store.upsert_sandbox(route)
        if pending is not None:
            record_sandbox_scheduled(
                self.metrics_store,
                sandbox_id=spec.id,
                route=route,
                resources=spec.requested_resources(),
                pending=pending,
            )
        self._write_json({"sandbox": record, "recovered": True}, status=status)
        return True

    def _sandbox_record_on_node(
        self,
        node_url: str,
        sandbox_id: str,
    ) -> dict[str, Any] | None:
        response = self._proxy_request(
            node_url,
            "/v1/sandboxes",
            method="GET",
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
                    with trace_span(
                        self.metrics_store,
                        trace_id,
                        "gateway.image_build_local",
                        parent_span_id=root.span_id,
                    ) as span:
                        with uploaded_build_context(raw) as context_path:
                            build_spec = spec
                            span.set_attribute("uploaded_context", context_path is not None)
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
                            push_result = self.image_manager.runtime.push(build_spec.tag) if push else None
                            if push_result is not None:
                                record = self.image_manager.mark_pushed(record.id)
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
                    span.set_attribute("selected_node_id", heartbeat.node_id if heartbeat else "")
                    span.set_attribute("selected_job_id", heartbeat.job_id if heartbeat else "")
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
                    node_timings = response_payload.get("timings")
                    if isinstance(node_timings, dict):
                        span.set_attribute("node_timings", node_timings)
                        root.set_attribute("node_timings", node_timings)
                if 200 <= response.status < 300 and self.routing_store is not None:
                    self.routing_store.clear_pending_image_build(spec.id)
                if 200 <= response.status < 300 and self.image_manager is not None:
                    raw_image = response_payload.get("image")
                    if isinstance(raw_image, dict) and _image_record_available_to_sandboxes(raw_image):
                        try:
                            self.image_manager.store.upsert(ImageRecord.from_dict(raw_image))
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
        except RuntimeError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

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
            sandbox_nodes_only = bool(raw.get("sandbox_nodes_only", raw.get("sandboxNodesOnly", True)))
            if count <= 0:
                raise ValueError("count must be positive.")
        except (json.JSONDecodeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        result = self._warm_image_on_ready_nodes(
            image,
            count=count,
            resources=resources,
            sandbox_nodes_only=sandbox_nodes_only,
            image_id=str(raw.get("id") or "").strip(),
        )
        if result["ready"] <= 0:
            self._write_json(
                {
                    "error": "no ready image-cache node is available",
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
            self._write_json({"error": "sandbox route not found"}, status=HTTPStatus.NOT_FOUND)
            return

        try:
            body = self._read_raw_body() if self.command in {"POST", "PUT", "PATCH"} else None
        except ValueError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        response = self._proxy_request(
            route.node_url,
            self.path,
            method=self.command,
            body=body,
        )
        if self.command == "POST" and path.endswith("/exec") and 200 <= response.status < 300:
            session = response.json().get("session")
            session_id = session.get("id") if isinstance(session, dict) else None
            if isinstance(session_id, str) and session_id:
                self.routing_store.upsert_exec(
                    ExecRoute(
                        session_id=session_id,
                        sandbox_id=sandbox_id,
                        node_id=route.node_id,
                        job_id=route.job_id,
                        node_url=route.node_url,
                    )
                )
        if self.command == "DELETE" and 200 <= response.status < 500:
            self.routing_store.delete_sandbox(sandbox_id)
        self._send_proxied_response(response)

    def _route_exec_request(self, session_id: str, path: str) -> None:
        route = self.routing_store.get_exec(session_id)
        if route is None:
            self._write_json({"error": "exec route not found"}, status=HTTPStatus.NOT_FOUND)
            return
        try:
            body = self._read_raw_body() if self.command in {"POST", "PUT", "PATCH"} else None
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
        for heartbeat in self._ready_heartbeats():
            response = self._proxy_request(
                heartbeat.node_url or "",
                "/v1/sandboxes",
                method="GET",
            )
            if response.status >= 400:
                continue
            raw_sandboxes = response.json().get("sandboxes")
            if not isinstance(raw_sandboxes, list):
                continue
            for record in raw_sandboxes:
                spec = record.get("spec") if isinstance(record, dict) else None
                if isinstance(spec, dict) and spec.get("id") == sandbox_id:
                    route = _sandbox_route_from_heartbeat(heartbeat, sandbox_id, spec)
                    self.routing_store.upsert_sandbox(route)
                    return route
        return None

    def _select_node(
        self,
        requested: ResourceQuantity,
        *,
        image: str | None = None,
    ) -> NodeHeartbeat | None:
        routes = list(self.routing_store.load().sandboxes.values())
        candidates = [
            heartbeat
            for heartbeat in self._ready_sandbox_heartbeats()
            if agent_version_is_compatible(heartbeat.agent_version)
            and _node_can_fit(heartbeat, requested, routes)
        ]
        if not candidates:
            return None
        image_node_ids = self._nodes_with_image(image or "", candidates)
        if image_node_ids:
            candidates = [
                heartbeat for heartbeat in candidates if heartbeat.node_id in image_node_ids
            ]
        return sorted(
            candidates,
            key=lambda heartbeat: (
                _resource_slack(heartbeat.free_resources, requested),
                heartbeat.node_id,
            ),
        )[0]

    def _resolve_sandbox_image_reference(
        self,
        image: str,
    ) -> tuple[str, dict[str, Any] | None]:
        if not _looks_like_image_id_reference(image):
            return image, None
        matches = [
            record
            for record in self._image_records_across_nodes()
            if record.get("id") == image
        ]
        if not matches:
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
            return str(selected["tag"]), None
        return image, {
            "error": (
                "image id exists, but it is not available to sandbox nodes; "
                "build with push=true and a pullable registry tag, then create "
                "the sandbox with that image id or registry tag"
            ),
            "image_id": image,
            "matches": [_image_record_summary(record) for record in matches],
        }

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
            list(self.routing_store.load().sandboxes.values())
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
    ) -> set[str]:
        if not image.strip() and not image_id.strip():
            return set()
        image_keys = {item for item in (image, image_id, image_id_from_tag(image)) if item}
        node_ids: set[str] = set()
        for heartbeat in heartbeats:
            if use_heartbeat_cache and heartbeat.cached_images_known:
                if image_keys.intersection(heartbeat.cached_images):
                    node_ids.add(heartbeat.node_id)
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
                if record.get("tag") in image_keys or record.get("id") in image_keys:
                    node_ids.add(heartbeat.node_id)
                    break
        return node_ids

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
        image_keys = {item for item in (image, image_id, image_id_from_tag(image)) if item}
        if use_heartbeat_cache and heartbeat.cached_images_known:
            return bool(image_keys.intersection(heartbeat.cached_images))
        return heartbeat.node_id in self._nodes_with_image(
            image,
            [heartbeat],
            image_id=image_id,
            use_heartbeat_cache=use_heartbeat_cache,
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
            "image": selected_image or {"id": image_id or image_id_from_tag(image), "tag": image},
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
        if self.command in {"POST", "PUT", "PATCH"}:
            length_header = self.headers.get("Content-Length", "0")
            try:
                length = int(length_header)
            except ValueError:
                self._write_json(
                    {"error": "invalid Content-Length"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return True
            body = self.rfile.read(max(0, length))

        try:
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
        return True

    def _proxy_request(
        self,
        node_url: str,
        path: str,
        *,
        method: str,
        body: bytes | None = None,
        timeout_seconds: float = DEFAULT_PROXY_TIMEOUT_SECONDS,
    ) -> ProxiedResponse:
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in {"host", "content-length", "connection"}
        }
        proxied = request.Request(
            node_url.rstrip("/") + path,
            data=body,
            method=method,
            headers=headers,
        )
        try:
            with request.urlopen(proxied, timeout=timeout_seconds) as response:
                return ProxiedResponse(response.status, response.headers, response.read())
        except error.HTTPError as exc:
            return ProxiedResponse(exc.code, exc.headers, exc.read())
        except error.URLError as exc:
            body = json.dumps(
                {"error": f"node request failed: {exc.reason}"}
            ).encode("utf-8")
            return ProxiedResponse(HTTPStatus.BAD_GATEWAY, {}, body)
        except OSError as exc:
            body = json.dumps(
                {"error": f"node request failed: {exc}"}
            ).encode("utf-8")
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
        expected = f"Bearer {self.gateway_bearer_token}"
        if self.headers.get("Authorization") == expected:
            return True
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
    heartbeat_ttl_seconds: int = 120,
    image_file: Path | None = None,
    image_runtime: DockerImageRuntime | None = None,
    local_image_builds_enabled: bool | None = None,
    metrics_file: Path | None = None,
    registry_url: str | None = None,
) -> ThreadingHTTPServer:
    store = HeartbeatStore(heartbeat_file)
    routing_store = RoutingStore(routing_file) if routing_file is not None else None
    metrics_store = MetricsStore(metrics_file) if metrics_file is not None else None
    image_manager = (
        ImageManager(
            ImageStore(image_file),
            image_runtime or DockerImageRuntime(dry_run=True),
        )
        if image_file is not None
        else None
    )

    class BoundHandler(ControlPlaneHandler):
        pass

    BoundHandler.store = store
    BoundHandler.routing_store = routing_store
    BoundHandler.upstream_node_url = upstream_node_url
    BoundHandler.gateway_bearer_token = gateway_bearer_token
    BoundHandler.heartbeat_ttl_seconds = heartbeat_ttl_seconds
    BoundHandler.image_manager = image_manager
    BoundHandler.local_image_builds_enabled = (
        image_runtime is not None
        if local_image_builds_enabled is None
        else local_image_builds_enabled
    )
    BoundHandler.metrics_store = metrics_store
    BoundHandler.registry_url = registry_url
    return ThreadingHTTPServer((host, port), BoundHandler)


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
        ch if ch.isalnum() or ch in "._-" else "-"
        for ch in value.strip()
    ).strip("-")
    return cleaned or "trace"


def _sandbox_id_from_path(path: str) -> str | None:
    prefix = "/v1/sandboxes/"
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix):]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


def _image_build_key_from_path(path: str) -> str | None:
    prefix = "/v1/images/builds/"
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix):]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


def _exec_session_id_from_path(path: str) -> str | None:
    prefix = "/v1/exec/"
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix):]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


def _prepare_id_from_path(path: str) -> str | None:
    prefix = "/v1/capacity/prepare/"
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix):]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


def _builder_prepare_id_from_path(path: str) -> str | None:
    prefix = "/v1/builders/prepare/"
    if not path.startswith(prefix):
        return None
    rest = path[len(prefix):]
    if not rest:
        return None
    return unquote(rest.split("/", 1)[0])


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
) -> SandboxRoute:
    resources = (
        SandboxSpec.from_dict(spec).requested_resources()
        if isinstance(spec, dict)
        else ResourceQuantity()
    )
    return SandboxRoute(
        sandbox_id=sandbox_id,
        node_id=heartbeat.node_id,
        job_id=heartbeat.job_id,
        node_url=heartbeat.node_url or "",
        resources=resources,
    )


def _is_duplicate_sandbox_response(response: ProxiedResponse, sandbox_id: str) -> bool:
    if response.status not in {HTTPStatus.BAD_REQUEST, HTTPStatus.CONFLICT}:
        return False
    error_message = str(response.json().get("error") or "").lower()
    return "already exists" in error_message and sandbox_id.lower() in error_message


def _sandbox_record_matches_spec(record: dict[str, Any], requested: SandboxSpec) -> bool:
    raw_spec = record.get("spec")
    if not isinstance(raw_spec, dict):
        return False
    try:
        existing = SandboxSpec.from_dict(raw_spec)
    except (TypeError, ValueError):
        return False
    return sandbox_specs_match(existing, requested)


def _enrich_sandbox_record(
    record: dict[str, Any],
    heartbeat: NodeHeartbeat,
) -> dict[str, Any]:
    enriched = dict(record)
    spec = enriched.get("spec")
    spec = spec if isinstance(spec, dict) else {}
    sandbox_id = str(
        enriched.get("id")
        or enriched.get("sandbox_id")
        or spec.get("id")
        or ""
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


def _node_metadata(heartbeat: NodeHeartbeat) -> dict[str, str]:
    return {
        "node_id": heartbeat.node_id,
        "job_id": heartbeat.job_id,
        "node_url": heartbeat.node_url or "",
    }


def _node_can_fit(
    heartbeat: NodeHeartbeat,
    requested: ResourceQuantity,
    routes: list[SandboxRoute],
) -> bool:
    if not _has_resource_values(requested):
        return False
    recent_route_resources = ResourceQuantity()
    for route in routes:
        if route.node_url != heartbeat.node_url:
            continue
        route_created_at = parse_iso_datetime(route.created_at)
        if route_created_at is not None and route_created_at <= heartbeat.updated_at:
            continue
        recent_route_resources = recent_route_resources + route.resources

    adjusted_free = ResourceQuantity(
        vcpu=max(0.0, heartbeat.free_resources.vcpu - recent_route_resources.vcpu),
        memory_mb=max(
            0,
            heartbeat.free_resources.memory_mb - recent_route_resources.memory_mb,
        ),
        disk_mb=max(0, heartbeat.free_resources.disk_mb - recent_route_resources.disk_mb),
    )
    return requested.fits_within(adjusted_free)


def _image_pull_lock(node_url: str, image: str) -> RLock:
    key = (node_url.rstrip("/"), image)
    with _IMAGE_PULL_LOCKS_GUARD:
        lock = _IMAGE_PULL_LOCKS.get(key)
        if lock is None:
            lock = RLock()
            _IMAGE_PULL_LOCKS[key] = lock
        return lock


def _gateway_sandbox_create_lock(sandbox_id: str) -> RLock:
    with _GATEWAY_SANDBOX_CREATE_LOCKS_GUARD:
        lock = _GATEWAY_SANDBOX_CREATE_LOCKS.get(sandbox_id)
        if lock is None:
            lock = RLock()
            _GATEWAY_SANDBOX_CREATE_LOCKS[sandbox_id] = lock
        return lock


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
    return bool(image.strip()) and "/" not in image and ":" not in image and "@" not in image


def _image_record_available_to_sandboxes(record: dict[str, Any]) -> bool:
    return bool(
        record.get("available_to_sandboxes")
        or record.get("pushed")
        or record.get("source") == "registry"
    )


def _image_record_summary(record: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "id": record.get("id"),
        "tag": record.get("tag"),
        "source": record.get("source"),
        "pushed": bool(record.get("pushed")),
        "available_to_sandboxes": _image_record_available_to_sandboxes(record),
    }
    node = record.get("node")
    if isinstance(node, dict):
        summary["node"] = {
            "node_id": node.get("node_id"),
            "job_id": node.get("job_id"),
        }
    if record.get("location"):
        summary["location"] = record.get("location")
    return summary


def _resource_slack(free: ResourceQuantity, requested: ResourceQuantity) -> tuple[float, int, int]:
    return (
        max(0.0, free.vcpu - requested.vcpu),
        max(0, free.memory_mb - requested.memory_mb),
        max(0, free.disk_mb - requested.disk_mb),
    )


def _has_resource_values(resources: ResourceQuantity) -> bool:
    return resources.vcpu > 0 or resources.memory_mb > 0 or resources.disk_mb > 0
