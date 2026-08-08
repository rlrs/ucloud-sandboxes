from __future__ import annotations

from dataclasses import replace
import base64
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
import hmac
import json
from pathlib import Path
import shutil
import time
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlparse
from uuid import uuid4

from .agent import build_heartbeat
from .build_context_store import BuildContextBlobStore, ContentLengthReader
from .deployment import service_health
from .storage_native_migration import (
    STORAGE_NATIVE_MIGRATION_SCHEMA,
    StorageNativeMigration,
)
from .http_server import HighBacklogThreadingHTTPServer
from .images import (
    DockerImageRuntime,
    ImageBuildSpec,
    ImageManager,
    ImageStore,
    materialize_uploaded_build_context,
)
from .node_runtime import BuilderNodeRuntime, DirectNodeRuntime, NodeStateStore
from .registry import heartbeat_to_dict
from .models import NodeRuntimeMetrics, ResourceQuantity, SandboxInventoryEntry, utc_now
from .runtime_metrics import sample_node_runtime_metrics
from .capabilities import (
    DISK_QUOTA_CAPABILITY,
    DYNAMIC_ACTIVE_ADMISSION_CAPABILITY,
    HIBERNATE_LOCAL_CAPABILITY,
    MANAGED_PRIMARY_CAPABILITY,
    STORAGE_NATIVE_CAPABILITY,
    STORAGE_NATIVE_MIGRATION_CAPABILITY,
)
from .managed_process import ManagedProcessError, ManagedProcessStart
from .sandbox import (
    SandboxAdmissionClosedError,
    SandboxBusyError,
    SandboxCapacityUnavailableError,
    SandboxConflictError,
    SandboxFileTooLargeError,
    SandboxOperation,
    SandboxRecord,
    SandboxSpec,
    sandbox_spec_fingerprint,
)
from .sandbox_exec import ExecSessionManager, SandboxExecSpec


DEFAULT_MAX_JSON_BODY_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_FILE_BODY_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_BUILD_CONTEXT_STORE_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_MAX_BUILD_CONTEXT_ENTRIES = 128
DEFAULT_MAX_BUILD_CONTEXT_AGE_SECONDS = 24 * 60 * 60
# Public node API headers for a generation-fenced DELETE operation.
SANDBOX_GENERATION_HEADER = "X-UCloud-Sandbox-Generation"
SANDBOX_OPERATION_ID_HEADER = "X-UCloud-Sandbox-Operation-Id"


class RequestBodyTooLargeError(ValueError):
    pass


class NodeAgentHandler(BaseHTTPRequestHandler):
    manager: Any
    exec_manager: ExecSessionManager | None
    image_manager: ImageManager
    build_context_store: BuildContextBlobStore
    job_id: str
    node_id: str
    node_url: str | None
    agent_version: str
    deployment_id: str
    init_version: str
    total_resources: ResourceQuantity
    cpu_overcommit: float
    memory_overcommit: float
    disk_overcommit: float
    capabilities: tuple[str, ...]
    image_builds_enabled: bool
    sandboxes_enabled: bool
    runtime_metrics_provider: Callable[[], NodeRuntimeMetrics | None]
    node_epoch: str
    physical_disk_path: Path
    image_materializer: Callable[[str], object] | None = None
    rootfs_metrics_provider: Callable[[], dict[str, int]] | None = None
    node_control_bearer_token: str
    max_json_body_bytes = DEFAULT_MAX_JSON_BODY_BYTES
    max_file_body_bytes = DEFAULT_MAX_FILE_BODY_BYTES
    server_version = "ucloud-sandboxes-node-agent/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self._write_json(service_health("node-agent"))
            return
        if not self._check_node_control_authorized():
            return
        if parsed.path == "/v1/heartbeat":
            node_snapshot = self.manager.heartbeat_snapshot(
                active_build_count=(
                    self.image_manager.active_build_count
                    if self.image_builds_enabled
                    else lambda: 0
                )
            )
            activity = node_snapshot.activity
            physical_disk_total_mb, physical_disk_free_mb = _physical_disk_usage_mb(
                self.physical_disk_path
            )
            inventory = tuple(
                SandboxInventoryEntry(
                    sandbox_id=record.spec.id,
                    generation=record.generation,
                    operation_id=record.operation_id,
                    spec_hash=record.spec_hash or sandbox_spec_fingerprint(record.spec),
                    state=record.state,
                    resources=record.spec.requested_resources(),
                )
                for record in activity.records
            )
            self._write_json(
                {
                    "heartbeat": heartbeat_to_dict(
                        build_heartbeat(
                            job_id=self.job_id,
                            node_id=self.node_id,
                            node_url=self.node_url,
                            agent_version=self.agent_version,
                            deployment_id=self.deployment_id,
                            init_version=self.init_version,
                            active_sandboxes=activity.active_sandboxes,
                            active_image_builds=node_snapshot.active_image_builds,
                            active_sandbox_creates=activity.active_operations,
                            draining=node_snapshot.drain.draining,
                            capabilities=self.capabilities,
                            total_resources=self.total_resources,
                            used_resources=activity.used_resources,
                            cpu_overcommit=self.cpu_overcommit,
                            memory_overcommit=self.memory_overcommit,
                            disk_overcommit=self.disk_overcommit,
                            cached_images=_cached_image_refs(self.image_manager),
                            runtime_metrics=self._runtime_metrics_snapshot(),
                            node_epoch=self.node_epoch,
                            activity_epoch=activity.activity_revision,
                            inventory=inventory,
                            inventory_complete=True,
                            reserved_resources=activity.reserved_resources,
                            physical_disk_total_mb=physical_disk_total_mb,
                            physical_disk_free_mb=physical_disk_free_mb,
                            drain_token=(
                                node_snapshot.drain.token
                                if node_snapshot.drain.draining
                                else ""
                            ),
                            drain_activity_epoch=(
                                node_snapshot.drain.drain_activity_epoch
                            ),
                            admission_open=node_snapshot.drain.admission_open,
                        )
                    )
                }
            )
            return
        if not self.sandboxes_enabled and (
            parsed.path.startswith("/v1/sandboxes")
            or parsed.path.startswith("/v1/exec")
        ):
            self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        if parsed.path == "/v1/sandboxes":
            records = sorted(
                self.manager.list(),
                key=lambda item: item.spec.id,
            )
            self._write_json(
                {
                    "sandboxes": [
                        self._sandbox_inventory_payload(record) for record in records
                    ]
                }
            )
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith("/files"):
            self._download_file(parsed)
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith("/ssh"):
            self._sandbox_ssh(parsed.path)
            return
        managed_path = _managed_process_path(parsed.path)
        if managed_path is not None and managed_path[0] == "status":
            self._managed_process_status(managed_path[1], managed_path[2])
            return
        if managed_path is not None and managed_path[0].startswith("logs:"):
            self._managed_process_logs(
                managed_path[1],
                managed_path[2],
                managed_path[0].split(":", 1)[1],
                parsed,
            )
            return
        if parsed.path.startswith("/v1/exec/") and parsed.path.endswith("/events"):
            self._exec_events(parsed)
            return
        if parsed.path.startswith("/v1/exec/"):
            self._exec_session(parsed.path)
            return
        if parsed.path == "/v1/images":
            self._write_json(
                {
                    "images": [
                        record.to_dict()
                        for record in sorted(
                            self.image_manager.list(),
                            key=lambda item: item.id,
                        )
                    ]
                }
            )
            return
        context_digest = _build_context_digest_from_path(parsed.path)
        if context_digest is not None and self.image_builds_enabled:
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
        if parsed.path == "/v1/images/builds":
            self._write_json(
                {
                    "builds": [
                        record.to_dict()
                        for record in sorted(
                            self.image_manager.list_builds(),
                            key=lambda item: (item.created_at, item.build_id),
                        )
                    ]
                }
            )
            return
        build_key = _image_build_key_from_path(parsed.path)
        if build_key is not None:
            record = self.image_manager.get_build(build_key)
            if record is None:
                self._write_json(
                    {"error": "image build not found"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return
            self._write_json({"build": record.to_dict()})
            return
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        if not self._check_node_control_authorized():
            return
        parsed = urlparse(self.path)
        if parsed.path == "/v1/drain":
            self._configure_drain()
            return
        if not self.sandboxes_enabled and parsed.path not in {
            "/v1/images/build",
            "/v1/images/pull",
        }:
            self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        if parsed.path == "/v1/sandboxes":
            self._create_sandbox()
            return
        if parsed.path == "/v1/migrations/import":
            self._import_migration()
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith(
            "/migration/prepare"
        ):
            self._prepare_migration(parsed.path)
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith(
            "/migration/activate"
        ):
            self._activate_migration(parsed.path)
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith(
            "/migration/finalize"
        ):
            self._finalize_migration(parsed.path)
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith(
            "/migration/abort"
        ):
            self._abort_migration(parsed.path)
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith(
            "/migration/abort-import"
        ):
            self._abort_import(parsed.path)
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith("/park"):
            self._park_sandbox(parsed.path)
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith("/wake"):
            self._wake_sandbox(parsed.path)
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith("/exec"):
            self._start_exec(parsed.path)
            return
        managed_path = _managed_process_path(parsed.path)
        if managed_path is not None and managed_path[0] == "collection":
            self._start_managed_process(managed_path[1])
            return
        if managed_path is not None and managed_path[0] == "signal":
            self._signal_managed_process(managed_path[1], managed_path[2])
            return
        if parsed.path.startswith("/v1/exec/") and parsed.path.endswith("/stdin"):
            self._write_exec_stdin(parsed.path)
            return
        if parsed.path.startswith("/v1/exec/") and parsed.path.endswith("/close-stdin"):
            self._close_exec_stdin(parsed.path)
            return
        if parsed.path == "/v1/images/build":
            self._build_image()
            return
        if parsed.path == "/v1/images/pull":
            self._pull_image()
            return
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def _configure_drain(self) -> None:
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("drain payload must be a JSON object")
            if set(raw) != {"draining", "token"}:
                raise ValueError("drain payload has an invalid schema")
            token = raw.get("token")
            if not isinstance(token, str):
                raise ValueError("drain token must be a string")
            token = token.strip()
            draining = raw.get("draining")
            if not isinstance(draining, bool):
                raise ValueError("draining must be a boolean")
            snapshot = self.manager.configure_drain(
                token,
                draining,
                active_build_count=(
                    self.image_manager.active_build_count
                    if self.image_builds_enabled
                    else lambda: 0
                ),
            )
        except SandboxConflictError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json(
            {
                "drain": {
                    "token": snapshot.drain.token,
                    "draining": snapshot.drain.draining,
                    "admission_open": snapshot.drain.admission_open,
                    "drain_activity_epoch": (snapshot.drain.drain_activity_epoch),
                    "activity_epoch": snapshot.activity.activity_revision,
                    "active_sandboxes": snapshot.activity.active_sandboxes,
                    "reserved_resources": (
                        snapshot.activity.reserved_resources.to_dict()
                    ),
                    "active_image_builds": snapshot.active_image_builds,
                    "ready": snapshot.ready,
                }
            }
        )

    def do_PUT(self) -> None:
        if not self._check_node_control_authorized():
            return
        parsed = urlparse(self.path)
        context_digest = _build_context_digest_from_path(parsed.path)
        if context_digest is not None and self.image_builds_enabled:
            self._store_build_context(context_digest)
            return
        if not self.sandboxes_enabled:
            self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith("/files"):
            self._upload_file(parsed)
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

    def _create_sandbox(self) -> None:
        started = time.monotonic()
        phases: dict[str, int] = {}
        try:
            phase = time.monotonic()
            raw = self._read_json_body()
            phases["read_request_ms"] = _elapsed_ms(phase)
            if not isinstance(raw, dict):
                raise ValueError("sandbox payload must be a JSON object")
            phase = time.monotonic()
            spec_raw = dict(raw)
            operation = SandboxOperation.from_dict(
                spec_raw.pop("_ucloud_operation", None)
            )
            spec = SandboxSpec.from_dict(spec_raw)
            phases["parse_spec_ms"] = _elapsed_ms(phase)
            phase = time.monotonic()
            record, result, manager_timings = self.manager.create_with_timings(
                spec,
                operation=operation,
            )
            phases["manager_create_ms"] = _elapsed_ms(phase)
        except SandboxConflictError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except SandboxAdmissionClosedError as exc:
            self._write_json(
                {
                    "error": str(exc),
                    "error_code": "node_admission_closed",
                    "retryable": True,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        except SandboxCapacityUnavailableError as exc:
            self._write_json(
                {
                    "error": str(exc),
                    "error_code": "node_active_admission_deferred",
                    "retryable": True,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        status = (
            HTTPStatus.OK if manager_timings.get("idempotent") else HTTPStatus.CREATED
        )
        self._write_json(
            {
                "sandbox": record.to_dict(),
                "command": list(result.argv),
                "exit_code": result.exit_code,
                "timings": {
                    "total_ms": _elapsed_ms(started),
                    "phases": phases,
                    "manager": manager_timings,
                },
            },
            status=status,
        )

    def _start_exec(self, path: str) -> None:
        started = time.monotonic()
        prefix = "/v1/sandboxes/"
        suffix = "/exec"
        sandbox_id = unquote(path[len(prefix) : -len(suffix)])
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("exec payload must be a JSON object")
            spec = SandboxExecSpec.from_dict(raw, sandbox_id=sandbox_id)
            session = self.exec_manager.start(spec)
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        manager_timings = self.manager.consume_exec_start_timings()
        self._write_json(
            {
                "session": session.to_dict(),
                "timings": {
                    "manager": manager_timings,
                    "start_ms": _elapsed_ms(started),
                },
            },
            status=HTTPStatus.CREATED,
        )

    def _start_managed_process(self, sandbox_id: str) -> None:
        try:
            spec = ManagedProcessStart.from_dict(self._read_json_body())
            record = self.manager.start_managed_process(sandbox_id, spec)
        except ManagedProcessError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json({"job": record.to_dict()}, status=HTTPStatus.CREATED)

    def _managed_process_status(self, sandbox_id: str, job_id: str) -> None:
        try:
            record = self.manager.managed_process_status(sandbox_id, job_id)
        except ManagedProcessError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json({"job": record.to_dict()})

    def _managed_process_logs(
        self,
        sandbox_id: str,
        job_id: str,
        stream: str,
        parsed: Any,
    ) -> None:
        query = parse_qs(parsed.query, keep_blank_values=True)
        try:
            chunk = self.manager.managed_process_logs(
                sandbox_id,
                job_id,
                stream=stream,
                offset=int((query.get("offset") or ["0"])[0]),
                limit=int((query.get("limit") or [str(1024 * 1024)])[0]),
            )
        except ManagedProcessError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json(
            {
                "stream": chunk.stream,
                "offset": chunk.offset,
                "next_offset": chunk.next_offset,
                "data": base64.b64encode(chunk.data).decode("ascii"),
                "eof": chunk.eof,
            }
        )

    def _signal_managed_process(self, sandbox_id: str, job_id: str) -> None:
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("signal payload must be a JSON object")
            record = self.manager.signal_managed_process(
                sandbox_id,
                job_id,
                signal=int(raw.get("signal") or 15),
            )
        except ManagedProcessError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json({"job": record.to_dict()})

    def _park_sandbox(self, path: str) -> None:
        sandbox_id = _sandbox_id_from_path(path, suffix="/park")
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("park payload must be a JSON object")
            if set(raw) not in ({"operation_id"}, {"background", "operation_id"}):
                raise ValueError("park payload has an invalid schema")
            operation_id = raw.get("operation_id")
            if not isinstance(operation_id, str) or not operation_id.strip():
                raise ValueError("operation_id must be a nonempty string")
            operation_id = operation_id.strip()
            background = raw.get("background", False)
            if type(background) is not bool:
                raise ValueError("background must be a boolean")
            record = self.manager.park(
                sandbox_id,
                operation_id=operation_id,
                background=background,
            )
        except SandboxConflictError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except RuntimeError as exc:
            payload = {"error": str(exc)}
            try:
                current = self.manager.get(sandbox_id)
                if current is not None:
                    payload["lifecycle_state"] = current.state
            except (RuntimeError, ValueError):
                pass
            self._write_json(payload, status=HTTPStatus.SERVICE_UNAVAILABLE)
            return
        except ValueError as exc:
            self._write_exception(exc)
            return
        payload: dict[str, Any] = {"sandbox": record.to_dict()}
        try:
            service = self.manager.service
            warden = service.warden
            if background and service.storage_native_publication_pending(sandbox_id):
                payload["publication"] = "pending"
                self._write_json(payload, status=HTTPStatus.ACCEPTED)
                return
            registration = service._require_registration(sandbox_id)
            storage_record = warden._storage_record(registration.to_direct_sandbox())
            if storage_record.get("state") != "published":
                self._write_json(payload)
                return
            snapshot = service.describe_storage_native_snapshot(sandbox_id)
            payload["storage_schema"] = "storage-native-v1"
            payload["snapshot_sha256"] = snapshot.sha256
            payload["storage_snapshot"] = snapshot.to_dict()
            payload["snapshot_manifest_digest"] = snapshot.publication.manifest_digest
            payload["snapshot_repository"] = snapshot.publication.repository
            payload["snapshot_tag"] = snapshot.publication.tag
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json(payload)

    def _sandbox_inventory_payload(self, record: Any) -> dict[str, Any]:
        payload = record.to_dict()
        if str(payload.get("state") or "").lower() != "parked":
            return payload
        service = self.manager.service
        warden = service.warden
        try:
            registration = service._require_registration(record.spec.id)
            storage_record = warden._storage_record(registration.to_direct_sandbox())
            if storage_record.get("state") != "published":
                return payload
            snapshot = service.describe_storage_native_snapshot(record.spec.id)
        except (RuntimeError, ValueError):
            return payload
        payload.update(
            {
                "snapshot_manifest_digest": (snapshot.publication.manifest_digest),
                "snapshot_repository": snapshot.publication.repository,
                "snapshot_sha256": snapshot.sha256,
                "snapshot_tag": snapshot.publication.tag,
                "storage_schema": STORAGE_NATIVE_MIGRATION_SCHEMA,
                "storage_snapshot": snapshot.to_dict(),
            }
        )
        return payload

    def _wake_sandbox(self, path: str) -> None:
        sandbox_id = _sandbox_id_from_path(path, suffix="/wake")
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("wake payload must be a JSON object")
            if set(raw) != {"generation", "operation_id"}:
                raise ValueError("wake payload has an invalid schema")
            operation_id = raw.get("operation_id")
            if not isinstance(operation_id, str) or not operation_id.strip():
                raise ValueError("operation_id must be a nonempty string")
            operation_id = operation_id.strip()
            generation_raw = raw.get("generation")
            if isinstance(generation_raw, bool) or not isinstance(generation_raw, int):
                raise ValueError("generation must be an integer")
            generation = generation_raw
            if generation <= 0:
                raise ValueError("generation must be positive")
            record = self.manager.wake(
                sandbox_id,
                generation=generation,
                operation_id=operation_id,
            )
        except SandboxConflictError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except RuntimeError as exc:
            payload = {"error": str(exc)}
            try:
                current = self.manager.get(sandbox_id)
                if current is not None:
                    payload["lifecycle_state"] = current.state
            except (RuntimeError, ValueError):
                pass
            self._write_json(payload, status=HTTPStatus.SERVICE_UNAVAILABLE)
            return
        except ValueError as exc:
            self._write_exception(exc)
            return
        self._write_json({"sandbox": record.to_dict()})

    def _prepare_migration(self, path: str) -> None:
        sandbox_id = _sandbox_id_from_path(path, suffix="/migration/prepare")
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("migration payload must be a JSON object")
            migration_id = str(raw.get("migration_id") or "").strip()
            requested_format = str(raw.get("format") or "").strip()
            service = self._direct_service()
            if requested_format not in {"", STORAGE_NATIVE_MIGRATION_SCHEMA}:
                raise ValueError("unsupported migration storage schema")
            migration = service.prepare_storage_native_move(
                sandbox_id,
                migration_id=migration_id,
            )
            self._write_json(
                {
                    "migration": {
                        "migration_id": migration_id,
                        "sandbox_id": sandbox_id,
                        "snapshot_sha256": migration.sha256,
                        "storage_schema": STORAGE_NATIVE_MIGRATION_SCHEMA,
                        "storage_snapshot": migration.to_dict(),
                    }
                }
            )
            return
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return

    def _import_migration(self) -> None:
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("migration payload must be a JSON object")
            sandbox_id = str(raw.get("sandbox_id") or "").strip()
            migration_id = str(raw.get("migration_id") or "").strip()
            storage_schema = str(raw.get("storage_schema") or "").strip()
            if storage_schema != STORAGE_NATIVE_MIGRATION_SCHEMA:
                raise ValueError("unsupported migration storage schema")
            migration = StorageNativeMigration.from_dict(raw.get("storage_snapshot"))
            if migration.manifest.sandbox_id != sandbox_id:
                raise ValueError("storage-native migration belongs to another sandbox")
            expected_sha256 = str(raw.get("snapshot_sha256") or "").strip()
            if migration.sha256 != expected_sha256:
                raise ValueError("storage-native migration digest does not match")
            result, destination = self._direct_service().stage_storage_native_import(
                migration,
                migration_id=migration_id,
            )
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json(
            {
                "sandbox": result.to_dict(),
                "storage_schema": STORAGE_NATIVE_MIGRATION_SCHEMA,
                "storage_snapshot": destination.to_dict(),
            },
            status=HTTPStatus.CREATED,
        )

    def _activate_migration(self, path: str) -> None:
        self._complete_migration_action(path, action="activate")

    def _finalize_migration(self, path: str) -> None:
        self._complete_migration_action(path, action="finalize")

    def _abort_migration(self, path: str) -> None:
        self._complete_migration_action(path, action="abort")

    def _abort_import(self, path: str) -> None:
        self._complete_migration_action(path, action="abort-import")

    def _complete_migration_action(self, path: str, *, action: str) -> None:
        sandbox_id = _sandbox_id_from_path(path, suffix=f"/migration/{action}")
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("migration payload must be a JSON object")
            migration_id = str(raw.get("migration_id") or "").strip()
            migration_sha256 = str(raw.get("snapshot_sha256") or "").strip()
            service = self._direct_service()
            if action == "activate":
                record = service.activate_import(
                    sandbox_id,
                    migration_id=migration_id,
                    migration_sha256=migration_sha256,
                )
            elif action == "abort":
                record = service.abort_move(
                    sandbox_id,
                    migration_id=migration_id,
                    migration_sha256=migration_sha256,
                )
            elif action == "abort-import":
                service.abort_import(
                    sandbox_id,
                    migration_id=migration_id,
                    migration_sha256=migration_sha256,
                )
                record = None
            else:
                service.finalize_moved_source(
                    sandbox_id,
                    migration_id=migration_id,
                    migration_sha256=migration_sha256,
                )
                record = None
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        payload: dict[str, Any] = {"ok": True}
        if record is not None:
            payload["sandbox"] = record.to_dict()
        self._write_json(payload)

    def _direct_service(self) -> Any:
        return self.manager.service

    def _exec_session(self, path: str) -> None:
        session_id = self._exec_session_id_from_path(path)
        session = self.exec_manager.get(session_id)
        if session is None:
            self._write_json(
                {"error": "exec session not found"}, status=HTTPStatus.NOT_FOUND
            )
            return
        self._write_json({"session": session.to_dict()})

    def _exec_events(self, parsed: Any) -> None:
        session_id = self._exec_session_id_from_path(parsed.path, suffix="/events")
        query = parse_qs(parsed.query)
        after = _int_query(query, "after", 0)
        limit = _int_query(query, "limit", 100)
        wait_seconds = min(
            30.0, max(0.0, float((query.get("wait_seconds") or ["0"])[0]))
        )
        try:
            events = self.exec_manager.events_after(
                session_id,
                after=after,
                limit=limit,
                wait_seconds=wait_seconds,
            )
        except ValueError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            return
        session = self.exec_manager.get(session_id)
        self._write_json(
            {
                "session": session.to_dict() if session is not None else None,
                "events": [event.to_dict() for event in events],
            }
        )

    def _write_exec_stdin(self, path: str) -> None:
        session_id = self._exec_session_id_from_path(path, suffix="/stdin")
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("stdin payload must be a JSON object")
            data = str(raw.get("data") or "")
            session = self.exec_manager.write_stdin(session_id, data)
            if raw.get("eof"):
                session = self.exec_manager.close_stdin(session_id)
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json({"session": session.to_dict()})

    def _close_exec_stdin(self, path: str) -> None:
        session_id = self._exec_session_id_from_path(path, suffix="/close-stdin")
        try:
            session = self.exec_manager.close_stdin(session_id)
        except ValueError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._write_json({"session": session.to_dict()})

    def _sandbox_ssh(self, path: str) -> None:
        prefix = "/v1/sandboxes/"
        suffix = "/ssh"
        sandbox_id = unquote(path[len(prefix) : -len(suffix)])
        try:
            with self.manager.lifecycle.shared(sandbox_id):
                record = self.manager.require_activity_sandbox(sandbox_id)
        except SandboxBusyError as exc:
            self._write_json(
                {"error": str(exc), "retryable": True},
                status=HTTPStatus.CONFLICT,
            )
            return
        except ValueError:
            self._write_json(
                {"error": "sandbox not found"}, status=HTTPStatus.NOT_FOUND
            )
            return
        ssh = record.to_dict().get("ssh")
        if not ssh:
            self._write_json(
                {"error": "sandbox ssh is not enabled"}, status=HTTPStatus.BAD_REQUEST
            )
            return
        self._write_json({"sandbox_id": sandbox_id, "ssh": ssh})

    def _upload_file(self, parsed: Any) -> None:
        sandbox_id = _sandbox_id_from_path(parsed.path, suffix="/files")
        container_path = _file_path_from_query(parsed)
        if container_path is None:
            self._write_json(
                {"error": "path query parameter is required"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        try:
            content = self._read_raw_body(max_bytes=self.max_file_body_bytes)
            result = self.manager.upload_file(sandbox_id, container_path, content)
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json(
            {
                "ok": True,
                "sandbox_id": sandbox_id,
                "path": container_path,
                "size": len(content),
                "command": list(result.argv),
                "exit_code": result.exit_code,
            }
        )

    def _download_file(self, parsed: Any) -> None:
        sandbox_id = _sandbox_id_from_path(parsed.path, suffix="/files")
        container_path = _file_path_from_query(parsed)
        if container_path is None:
            self._write_json(
                {"error": "path query parameter is required"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        try:
            content, result = self.manager.download_file(
                sandbox_id,
                container_path,
                max_bytes=self.max_file_body_bytes,
            )
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_bytes(
            content,
            "application/octet-stream",
            headers={
                "X-Sandbox-Id": sandbox_id,
                "X-Sandbox-Path": container_path,
                "X-Docker-Command": json.dumps(list(result.argv)),
                "X-Docker-Exit-Code": str(result.exit_code),
            },
        )

    def _build_image(self) -> None:
        if not self.image_builds_enabled:
            self._write_json(
                {"error": "image builds are disabled on this node"},
                status=HTTPStatus.FORBIDDEN,
            )
            return
        started = time.monotonic()
        phases: dict[str, int] = {}
        materialized_context = None
        cleanup_transferred = False
        try:
            phase = time.monotonic()
            raw = self._read_json_body()
            phases["read_request_ms"] = _elapsed_ms(phase)
            if not isinstance(raw, dict):
                raise ValueError("image build payload must be a JSON object")
            push = bool(raw.get("push", False))
            wait = bool(raw.get("wait", True))
            phase = time.monotonic()
            materialized_context = materialize_uploaded_build_context(
                raw, self.build_context_store
            )
            phases["materialize_context_ms"] = _elapsed_ms(phase)
            phase = time.monotonic()
            spec = ImageBuildSpec.from_dict(raw)
            if materialized_context is not None:
                spec = ImageBuildSpec(
                    id=spec.id,
                    tag=spec.tag,
                    context_path=str(materialized_context.path),
                    dockerfile=spec.dockerfile,
                    build_args=spec.build_args,
                    labels=spec.labels,
                )
            phases["parse_spec_ms"] = _elapsed_ms(phase)
            phase = time.monotonic()
            with self.manager.image_operation(self.image_manager):
                build, build_started = self.image_manager.start_build(
                    spec,
                    push=push,
                    cleanup=(
                        materialized_context.cleanup
                        if materialized_context is not None
                        else None
                    ),
                )
            cleanup_transferred = materialized_context is not None
            phases["start_build_ms"] = _elapsed_ms(phase)
            if wait:
                phase = time.monotonic()
                build = self.image_manager.wait_for_build(build.build_id) or build
                phases["wait_for_build_ms"] = _elapsed_ms(phase)
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        finally:
            if materialized_context is not None and not cleanup_transferred:
                materialized_context.cleanup()
        timings = {
            "total_ms": _elapsed_ms(started),
            "phases": phases,
            "build": build.timings,
        }
        if not wait:
            self._write_json(
                {
                    "build": build.to_dict(),
                    "started": build_started,
                    "timings": timings,
                },
                status=HTTPStatus.ACCEPTED,
            )
            return
        if build.status != "succeeded":
            self._write_json(
                {
                    "error": build.error or f"image build {build.status}",
                    "build": build.to_dict(),
                    "timings": timings,
                },
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        image_record = self.image_manager.get_image(build.image_id)
        payload: dict[str, Any] = {
            "build": build.to_dict(),
            "image": image_record.to_dict()
            if image_record is not None
            else build.image,
            "command": list(build.command),
            "exit_code": build.exit_code,
            "timings": timings,
        }
        if build.push_command:
            payload["push_command"] = list(build.push_command)
            payload["push_exit_code"] = build.push_exit_code
        self._write_json(payload, status=HTTPStatus.CREATED)

    def _pull_image(self) -> None:
        started = time.monotonic()
        failed_phase = "read_request"
        pull_queue_ms = 0
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("image pull payload must be a JSON object")
            image = str(raw.get("image") or "")
            image_id = str(raw["id"]) if raw.get("id") else None
            with self.manager.image_operation(self.image_manager):
                failed_phase = "docker_pull"
                with self.image_manager.pull_slot() as pull_admission:
                    pull_queue_ms = int(pull_admission["queue_wait_ms"])
                    pull_started = time.monotonic()
                    record, result = self.image_manager.pull(image, image_id=image_id)
                pull_finished = time.monotonic()
                materialize_ms: int | None = None
                if self.image_materializer is not None:
                    failed_phase = "rootfs_materialize"
                    try:
                        self.image_materializer(record.tag)
                    except Exception as exc:
                        # A direct node must not advertise a pulled image as ready
                        # when its immutable rootfs export is still unavailable.
                        self.image_manager.store.delete_by_tags((record.tag,))
                        raise RuntimeError(
                            f"image rootfs materialization failed: {exc}"
                        ) from exc
                    materialize_ms = int(
                        max(0.0, time.monotonic() - pull_finished) * 1000
                    )
        except RuntimeError as exc:
            self._write_json(
                {
                    "error": str(exc),
                    "error_code": "image_pull_failed",
                    "retryable": True,
                    "timings": {
                        "total_ms": _elapsed_ms(started),
                        "pull_queue_ms": pull_queue_ms,
                        "failed_phase": failed_phase,
                    },
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        except ValueError as exc:
            self._write_json(
                {
                    "error": str(exc),
                    "timings": {
                        "total_ms": _elapsed_ms(started),
                        "failed_phase": failed_phase,
                    },
                },
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        timings = {
            "pull_queue_ms": pull_queue_ms,
            "docker_pull_ms": int(max(0.0, pull_finished - pull_started) * 1000),
        }
        if materialize_ms is not None:
            timings["rootfs_materialize_ms"] = materialize_ms
        self._write_json(
            {
                "image": record.to_dict(),
                "command": list(result.argv),
                "exit_code": result.exit_code,
                "timings": timings,
            },
            status=HTTPStatus.CREATED,
        )

    def _runtime_metrics_snapshot(self) -> NodeRuntimeMetrics | None:
        metrics = self.runtime_metrics_provider()
        pull_snapshot = self.image_manager.pull_operation_snapshot()
        if metrics is None:
            metrics = NodeRuntimeMetrics(collected_at=utc_now())
        metrics = replace(
            metrics,
            image_pull_active_operations=max(
                0, int(pull_snapshot.get("active_operations") or 0)
            ),
            image_pull_waiting_operations=max(
                0, int(pull_snapshot.get("waiting_operations") or 0)
            ),
            image_pull_max_concurrent_operations=max(
                0, int(pull_snapshot.get("max_concurrent_operations") or 0)
            ),
        )
        if self.rootfs_metrics_provider is None:
            return metrics
        snapshot = self.rootfs_metrics_provider()
        return replace(
            metrics,
            image_materialization_active_operations=max(
                0, int(snapshot.get("active_operations") or 0)
            ),
            image_materialization_waiting_operations=max(
                0, int(snapshot.get("waiting_operations") or 0)
            ),
            image_materialization_max_concurrent_operations=max(
                0, int(snapshot.get("max_concurrent_operations") or 0)
            ),
        )

    def _exec_session_id_from_path(self, path: str, *, suffix: str = "") -> str:
        prefix = "/v1/exec/"
        if suffix:
            return unquote(path[len(prefix) : -len(suffix)])
        return unquote(path[len(prefix) :])

    def do_DELETE(self) -> None:
        if not self._check_node_control_authorized():
            return
        if not self.sandboxes_enabled:
            self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        parsed = urlparse(self.path)
        prefix = "/v1/sandboxes/"
        if not parsed.path.startswith(prefix):
            self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        sandbox_id = unquote(parsed.path[len(prefix) :])
        if not sandbox_id:
            self._write_json(
                {"error": "sandbox id is required"}, status=HTTPStatus.BAD_REQUEST
            )
            return
        try:
            generation, operation_id = self._delete_operation_headers()
            record, result = self.manager.delete(
                sandbox_id,
                generation=generation,
                operation_id=operation_id,
            )
        except SandboxConflictError as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        payload: dict[str, Any] = {
            "deleted": record.to_dict() if record is not None else None,
            "command": list(result.argv),
            "exit_code": result.exit_code,
        }
        self._write_json(payload)

    def _delete_operation_headers(self) -> tuple[int, str]:
        generation_header = self.headers.get(SANDBOX_GENERATION_HEADER)
        operation_id_header = self.headers.get(SANDBOX_OPERATION_ID_HEADER)
        if generation_header is None or operation_id_header is None:
            raise ValueError(
                f"{SANDBOX_GENERATION_HEADER} and {SANDBOX_OPERATION_ID_HEADER} "
                "must be supplied together"
            )
        try:
            generation = int(generation_header)
        except ValueError as exc:
            raise ValueError(f"{SANDBOX_GENERATION_HEADER} must be an integer") from exc
        operation_id = operation_id_header.strip()
        if generation < 1:
            raise ValueError(f"{SANDBOX_GENERATION_HEADER} must be positive")
        if not operation_id:
            raise ValueError(f"{SANDBOX_OPERATION_ID_HEADER} cannot be empty")
        return generation, operation_id

    def _check_node_control_authorized(self) -> bool:
        expected = self.node_control_bearer_token
        authorization = self.headers.get("Authorization") or ""
        prefix = "Bearer "
        supplied = (
            authorization[len(prefix) :] if authorization.startswith(prefix) else ""
        )
        if supplied and hmac.compare_digest(supplied, expected):
            return True
        body = json.dumps({"error": "unauthorized"}).encode("utf-8")
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("Content-Type", "application/json")
        self.send_header("WWW-Authenticate", "Bearer")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        return False

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def _read_json_body(self) -> object:
        raw = self._read_raw_body(max_bytes=self.max_json_body_bytes).decode("utf-8")
        if not raw:
            raise ValueError("empty request body")
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON: {exc}") from exc

    def _read_raw_body(self, *, max_bytes: int | None = None) -> bytes:
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

    def _write_exception(self, exc: RuntimeError | ValueError) -> None:
        if isinstance(exc, (RequestBodyTooLargeError, SandboxFileTooLargeError)):
            status = HTTPStatus.REQUEST_ENTITY_TOO_LARGE
        elif isinstance(exc, RuntimeError):
            status = HTTPStatus.SERVICE_UNAVAILABLE
        else:
            status = HTTPStatus.BAD_REQUEST
        self._write_json({"error": str(exc)}, status=status)

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


def build_builder_node_agent_server(
    host: str,
    port: int,
    *,
    state_file: Path,
    image_file: Path,
    job_id: str,
    node_id: str,
    node_url: str | None = None,
    agent_version: str = "",
    deployment_id: str,
    init_version: str = "",
    total_resources: ResourceQuantity | None = None,
    image_runtime: DockerImageRuntime,
    node_control_bearer_token: str,
    runtime_metrics_provider: Callable[[], NodeRuntimeMetrics | None] | None = None,
    max_json_body_bytes: int = DEFAULT_MAX_JSON_BODY_BYTES,
    max_file_body_bytes: int = DEFAULT_MAX_FILE_BODY_BYTES,
    max_active_image_builds: int = 4,
    max_concurrent_image_pulls: int = 8,
    physical_disk_path: Path | None = None,
    build_context_store_dir: Path | None = None,
) -> HighBacklogThreadingHTTPServer:
    token = node_control_bearer_token.strip()
    if not token:
        raise ValueError("node control bearer token cannot be empty")
    if not deployment_id.strip():
        raise ValueError("deployment_id cannot be empty")
    if (
        max_json_body_bytes < 1
        or max_file_body_bytes < 1
        or max_active_image_builds < 1
        or max_concurrent_image_pulls < 1
    ):
        raise ValueError("node-agent request and build limits must be positive")
    resources = total_resources or ResourceQuantity()
    if not resources.is_valid:
        raise ValueError("total_resources cannot contain negative or non-finite values")

    runtime = BuilderNodeRuntime(NodeStateStore(state_file))
    image_manager = ImageManager(
        ImageStore(image_file),
        image_runtime,
        max_active_builds=max_active_image_builds,
        max_concurrent_pulls=max_concurrent_image_pulls,
    )
    context_store = BuildContextBlobStore(
        build_context_store_dir or image_file.parent / f"{image_file.stem}-contexts",
        max_blob_bytes=max_file_body_bytes,
        max_total_bytes=DEFAULT_MAX_BUILD_CONTEXT_STORE_BYTES,
        max_entries=DEFAULT_MAX_BUILD_CONTEXT_ENTRIES,
        max_age_seconds=DEFAULT_MAX_BUILD_CONTEXT_AGE_SECONDS,
    )

    class BuilderHandler(NodeAgentHandler):
        pass

    BuilderHandler.manager = runtime
    BuilderHandler.exec_manager = None
    BuilderHandler.image_manager = image_manager
    BuilderHandler.build_context_store = context_store
    BuilderHandler.job_id = job_id
    BuilderHandler.node_id = node_id
    BuilderHandler.node_url = node_url
    BuilderHandler.agent_version = agent_version
    BuilderHandler.deployment_id = deployment_id
    BuilderHandler.init_version = init_version
    BuilderHandler.total_resources = resources
    BuilderHandler.cpu_overcommit = 1.0
    BuilderHandler.memory_overcommit = 1.0
    BuilderHandler.disk_overcommit = 1.0
    BuilderHandler.capabilities = ("image-cache", "image-build")
    BuilderHandler.image_builds_enabled = True
    BuilderHandler.sandboxes_enabled = False
    BuilderHandler.node_epoch = uuid4().hex
    BuilderHandler.physical_disk_path = physical_disk_path or image_file.parent
    BuilderHandler.node_control_bearer_token = token
    BuilderHandler.max_json_body_bytes = max_json_body_bytes
    BuilderHandler.max_file_body_bytes = max_file_body_bytes
    BuilderHandler.runtime_metrics_provider = staticmethod(
        runtime_metrics_provider or sample_node_runtime_metrics
    )
    return HighBacklogThreadingHTTPServer((host, port), BuilderHandler)


def build_direct_node_agent_server(
    host: str,
    port: int,
    *,
    service: Any,
    image_file: Path,
    job_id: str,
    node_id: str,
    node_url: str | None = None,
    agent_version: str = "",
    deployment_id: str,
    init_version: str = "",
    total_resources: ResourceQuantity | None = None,
    cpu_overcommit: float = 1.0,
    memory_overcommit: float = 1.0,
    image_runtime: DockerImageRuntime,
    node_control_bearer_token: str,
    max_json_body_bytes: int = DEFAULT_MAX_JSON_BODY_BYTES,
    max_file_body_bytes: int = DEFAULT_MAX_FILE_BODY_BYTES,
    runtime_metrics_provider: Callable[[], NodeRuntimeMetrics | None] | None = None,
    max_concurrent_image_pulls: int = 8,
) -> HighBacklogThreadingHTTPServer:
    """Serve a sandbox node with direct runsc and storage-native ownership."""
    node_control_bearer_token = node_control_bearer_token.strip()
    if not node_control_bearer_token:
        raise ValueError("node control bearer token cannot be empty")
    if not deployment_id.strip():
        raise ValueError("deployment_id cannot be empty")
    if max_concurrent_image_pulls < 1:
        raise ValueError("max concurrent image pulls must be positive")
    configured_resources = total_resources or ResourceQuantity()
    if not configured_resources.is_valid:
        raise ValueError("total_resources cannot contain negative values")
    if cpu_overcommit != 1.0 or memory_overcommit != 1.0:
        raise ValueError(
            "direct node CPU and memory overcommit factors must be exactly 1.0"
        )
    if service.warden.storage is None:
        raise ValueError("direct node requires storage-native ownership")
    host_runtime_metrics = runtime_metrics_provider or sample_node_runtime_metrics
    if configured_resources.vcpu > 0 or configured_resources.memory_mb > 0:
        service.configure_active_capacity(
            configured_resources.scaled(
                cpu=cpu_overcommit,
                memory=memory_overcommit,
                disk=0.0,
            ),
            dynamic=True,
            runtime_metrics_provider=host_runtime_metrics,
        )
    service.start()
    manager = DirectNodeRuntime(service)
    exec_manager = ExecSessionManager(
        manager,
        route_node_id=node_id,
        route_job_id=job_id,
    )
    image_manager = ImageManager(
        ImageStore(image_file),
        image_runtime,
        max_concurrent_pulls=max_concurrent_image_pulls,
    )
    build_context_store = BuildContextBlobStore(
        image_file.parent / f"{image_file.stem}-contexts",
        max_blob_bytes=max_file_body_bytes,
        max_total_bytes=DEFAULT_MAX_BUILD_CONTEXT_STORE_BYTES,
        max_entries=DEFAULT_MAX_BUILD_CONTEXT_ENTRIES,
        max_age_seconds=DEFAULT_MAX_BUILD_CONTEXT_AGE_SECONDS,
    )

    class DirectBoundHandler(NodeAgentHandler):
        pass

    DirectBoundHandler.manager = manager
    DirectBoundHandler.exec_manager = exec_manager
    DirectBoundHandler.image_manager = image_manager
    DirectBoundHandler.build_context_store = build_context_store
    DirectBoundHandler.job_id = job_id
    DirectBoundHandler.node_id = node_id
    DirectBoundHandler.node_url = node_url
    DirectBoundHandler.agent_version = agent_version
    DirectBoundHandler.deployment_id = deployment_id
    DirectBoundHandler.init_version = init_version
    DirectBoundHandler.total_resources = configured_resources
    DirectBoundHandler.cpu_overcommit = cpu_overcommit
    DirectBoundHandler.memory_overcommit = memory_overcommit
    DirectBoundHandler.disk_overcommit = 1.0
    direct_capabilities = [
        "sandbox",
        "image-cache",
        DISK_QUOTA_CAPABILITY,
        HIBERNATE_LOCAL_CAPABILITY,
        "direct-runsc-v1",
    ]
    if service.provisioner.oci.managed_init_binary is not None:
        direct_capabilities.append(MANAGED_PRIMARY_CAPABILITY)
    if service.dynamic_active_admission_enabled:
        direct_capabilities.append(DYNAMIC_ACTIVE_ADMISSION_CAPABILITY)
    direct_capabilities.extend(
        (
            STORAGE_NATIVE_CAPABILITY,
            STORAGE_NATIVE_MIGRATION_CAPABILITY,
        )
    )
    DirectBoundHandler.capabilities = tuple(direct_capabilities)
    DirectBoundHandler.image_builds_enabled = False
    DirectBoundHandler.sandboxes_enabled = True
    DirectBoundHandler.node_epoch = uuid4().hex
    DirectBoundHandler.physical_disk_path = service.provisioner.overlays.writable_root
    DirectBoundHandler.image_materializer = staticmethod(
        service.provisioner.image_store.materialize
    )
    DirectBoundHandler.rootfs_metrics_provider = staticmethod(
        service.provisioner.image_store.operation_snapshot
    )
    DirectBoundHandler.node_control_bearer_token = node_control_bearer_token
    DirectBoundHandler.max_json_body_bytes = max_json_body_bytes
    DirectBoundHandler.max_file_body_bytes = max_file_body_bytes

    def direct_runtime_metrics() -> NodeRuntimeMetrics | None:
        metrics = host_runtime_metrics()
        storage = service.warden.storage
        if metrics is None:
            return metrics
        try:
            raw = storage.get_metrics()
        except (OSError, RuntimeError):
            return metrics
        mib = 1024 * 1024
        return replace(
            metrics,
            storage_hard_capacity_mb=(int(raw.get("hard_capacity_bytes", 0)) // mib),
            storage_hard_reserved_mb=(int(raw.get("hard_reserved_bytes", 0)) // mib),
            storage_cache_mb=int(raw.get("cache_bytes", 0)) // mib,
            storage_active_operations=int(raw.get("active_operations", 0)),
            storage_waiting_operations=int(raw.get("waiting_operations", 0)),
            storage_max_concurrent_operations=int(
                raw.get("max_concurrent_operations", 0)
            ),
            storage_published_volumes=int(raw.get("published_volumes", 0)),
            storage_error_volumes=int(raw.get("error_volumes", 0)),
            storage_device_pool_enabled=bool(raw.get("device_pool_enabled", False)),
            storage_device_pool_low_watermark=int(
                raw.get("device_pool_low_watermark", 0)
            ),
            storage_device_pool_high_watermark=int(
                raw.get("device_pool_high_watermark", 0)
            ),
            storage_device_pool_idle_devices=int(
                raw.get("device_pool_idle_devices", 0)
            ),
            storage_ublk_active_devices=int(raw.get("ublk_active_devices", 0)),
            storage_ublk_live_devices=int(raw.get("ublk_live_devices", 0)),
            storage_device_pool_acquires=int(raw.get("device_pool_acquires", 0)),
            storage_device_pool_reused_acquires=int(
                raw.get("device_pool_reused_acquires", 0)
            ),
            storage_device_pool_new_acquires=int(
                raw.get("device_pool_new_acquires", 0)
            ),
            storage_device_pool_releases=int(raw.get("device_pool_releases", 0)),
            storage_device_pool_discards=int(raw.get("device_pool_discards", 0)),
        )

    DirectBoundHandler.runtime_metrics_provider = staticmethod(direct_runtime_metrics)

    class DirectServiceHTTPServer(HighBacklogThreadingHTTPServer):
        def server_close(self) -> None:
            try:
                service.stop()
            finally:
                super().server_close()

    return DirectServiceHTTPServer((host, port), DirectBoundHandler)


def sandbox_record_to_dict(record: SandboxRecord) -> dict[str, Any]:
    return record.to_dict()


def _cached_image_refs(image_manager: ImageManager) -> tuple[str, ...]:
    refs: list[str] = []
    for record in image_manager.list():
        refs.append(record.id)
        if record.tag:
            refs.append(record.tag)
        if record.digest_ref:
            refs.append(record.digest_ref)
    return tuple(dict.fromkeys(refs))


def _default_physical_disk_path(sandbox_file: Path) -> Path:
    docker_quota_root = Path("/var/lib/ucloud-sandboxes/docker-xfs")
    if docker_quota_root.exists():
        return docker_quota_root
    return sandbox_file.parent


def _physical_disk_usage_mb(path: Path) -> tuple[int, int]:
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    try:
        usage = shutil.disk_usage(candidate)
    except OSError:
        return 0, 0
    divisor = 1024 * 1024
    return usage.total // divisor, usage.free // divisor


def _int_query(query: dict[str, list[str]], key: str, default: int) -> int:
    try:
        return int((query.get(key) or [str(default)])[0])
    except ValueError:
        return default


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.monotonic() - started) * 1000))


def _sandbox_id_from_path(path: str, *, suffix: str = "") -> str:
    prefix = "/v1/sandboxes/"
    if suffix:
        return unquote(path[len(prefix) : -len(suffix)])
    return unquote(path[len(prefix) :])


def _managed_process_path(path: str) -> tuple[str, str, str] | None:
    parts = [unquote(item) for item in path.split("/") if item]
    if len(parts) < 4 or parts[:2] != ["v1", "sandboxes"] or parts[3] != "jobs":
        return None
    sandbox_id = parts[2]
    if len(parts) == 4:
        return "collection", sandbox_id, ""
    job_id = parts[4]
    if len(parts) == 5:
        return "status", sandbox_id, job_id
    if len(parts) == 6 and parts[5] == "signal":
        return "signal", sandbox_id, job_id
    if len(parts) == 7 and parts[5] == "logs" and parts[6] in {"stdout", "stderr"}:
        return f"logs:{parts[6]}", sandbox_id, job_id
    return None


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
    key = unquote(path[len(prefix) :])
    return key or None


def _file_path_from_query(parsed: Any) -> str | None:
    value = (parse_qs(parsed.query).get("path") or [""])[0]
    value = value.strip()
    return value or None
