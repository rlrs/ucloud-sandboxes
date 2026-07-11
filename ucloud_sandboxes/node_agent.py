from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
import hmac
import json
import math
from pathlib import Path
import shutil
import time
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlparse
from uuid import uuid4

from .agent import build_heartbeat
from .build_context_store import BuildContextBlobStore, ContentLengthReader
from .deployment import service_health
from .http_server import HighBacklogThreadingHTTPServer
from .images import (
    DockerImageRuntime,
    ImageBuildSpec,
    ImageManager,
    ImageStore,
    image_id_from_tag,
    materialize_uploaded_build_context,
)
from .registry import heartbeat_to_dict
from .models import NodeRuntimeMetrics, ResourceQuantity, SandboxInventoryEntry
from .runtime_metrics import sample_node_runtime_metrics
from .capabilities import FORK_LOCAL_CAPABILITY, merge_capabilities
from .sandbox import (
    MAX_FORK_FANOUT,
    SandboxBusyError,
    DockerGvisorRuntime,
    SandboxCapacityUnavailableError,
    SandboxConflictError,
    SandboxFileTooLargeError,
    SandboxForkRuntimeResult,
    SandboxForkUnsupportedError,
    SandboxManager,
    SandboxOperation,
    SandboxRecord,
    SandboxSpec,
    SandboxStore,
    sandbox_fork_target,
    sandbox_spec_fingerprint,
)
from .sandbox_exec import ExecSessionManager, SandboxExecSpec


DEFAULT_MAX_JSON_BODY_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_FILE_BODY_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_BUILD_CONTEXT_STORE_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_MAX_BUILD_CONTEXT_ENTRIES = 128
DEFAULT_MAX_BUILD_CONTEXT_AGE_SECONDS = 24 * 60 * 60
# Public node API headers for a versioned DELETE operation.  Callers must send
# both together; omitting both selects the legacy generation-zero operation.
SANDBOX_GENERATION_HEADER = "X-UCloud-Sandbox-Generation"
SANDBOX_OPERATION_ID_HEADER = "X-UCloud-Sandbox-Operation-Id"


class RequestBodyTooLargeError(ValueError):
    pass


class NodeAgentHandler(BaseHTTPRequestHandler):
    manager: SandboxManager
    exec_manager: ExecSessionManager
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
    runtime_metrics_provider: Callable[[], NodeRuntimeMetrics | None]
    node_epoch: str
    physical_disk_path: Path
    node_control_bearer_token: str | None = None
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
                            draining=node_snapshot.drain.draining,
                            capabilities=self.capabilities,
                            total_resources=self.total_resources,
                            used_resources=activity.used_resources,
                            cpu_overcommit=self.cpu_overcommit,
                            memory_overcommit=self.memory_overcommit,
                            disk_overcommit=self.disk_overcommit,
                            cached_images=_cached_image_refs(self.image_manager),
                            runtime_metrics=self.runtime_metrics_provider(),
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
        if parsed.path == "/v1/sandboxes":
            self._write_json(
                {
                    "sandboxes": [
                        record.to_dict()
                        for record in sorted(
                            self.manager.list(),
                            key=lambda item: item.spec.id,
                        )
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
        if parsed.path == "/v1/sandboxes":
            self._create_sandbox()
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith("/forks"):
            self._fork_sandbox(parsed.path)
            return
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith("/exec"):
            self._start_exec(parsed.path)
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
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith("/snapshot"):
            self._snapshot_sandbox(parsed.path)
            return
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def _configure_drain(self) -> None:
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("drain payload must be a JSON object")
            token = str(raw.get("token") or "").strip()
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
                    "drain_activity_epoch": (
                        snapshot.drain.drain_activity_epoch
                    ),
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
        if context_digest is not None:
            self._store_build_context(context_digest)
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
            spec = SandboxSpec.from_dict(raw)
            operation_raw = raw.get("_ucloud_operation")
            operation = (
                SandboxOperation.from_dict(operation_raw)
                if operation_raw is not None
                else None
            )
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
        except SandboxCapacityUnavailableError as exc:
            self._write_json(
                {"error": str(exc)},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        status = (
            HTTPStatus.OK
            if manager_timings.get("idempotent")
            else HTTPStatus.CREATED
        )
        self._write_json(
            {
                "sandbox": record.to_dict(),
                "command": list(result.argv),
                "exitCode": result.exit_code,
                "timings": {
                    "total_ms": _elapsed_ms(started),
                    "phases": phases,
                    "manager": manager_timings,
                },
            },
            status=status,
        )

    def _start_exec(self, path: str) -> None:
        prefix = "/v1/sandboxes/"
        suffix = "/exec"
        sandbox_id = unquote(path[len(prefix):-len(suffix)])
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("exec payload must be a JSON object")
            spec = SandboxExecSpec.from_dict(raw, sandbox_id=sandbox_id)
            session = self.exec_manager.start(spec)
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json({"session": session.to_dict()}, status=HTTPStatus.CREATED)

    def _fork_sandbox(self, path: str) -> None:
        started = time.monotonic()
        phases: dict[str, int] = {}
        source_sandbox_id = _sandbox_id_from_path(path, suffix="/forks")
        target: SandboxSpec | None = None
        operation: SandboxOperation | None = None
        targets: tuple[SandboxSpec, ...] = ()
        operations: tuple[SandboxOperation, ...] = ()
        batch = False
        source_generation_for_intent: int | None = None
        try:
            phase = time.monotonic()
            raw = self._read_json_body()
            phases["read_request_ms"] = _elapsed_ms(phase)
            if not isinstance(raw, dict):
                raise ValueError("fork payload must be a JSON object")

            phase = time.monotonic()
            batch = _fork_request_is_batch(raw)
            source_generation, source_spec_hash = _fork_source_envelope(raw)
            source_generation_for_intent = source_generation
            raw_targets: list[dict[str, Any]] = []
            if batch:
                raw_targets_value = raw.get("sandboxes")
                raw_operations = raw.get("_ucloud_operations")
                if not isinstance(raw_targets_value, list) or not raw_targets_value:
                    raise ValueError("sandboxes must be a non-empty JSON array")
                if len(raw_targets_value) > MAX_FORK_FANOUT:
                    raise ValueError(
                        f"fork fan-out cannot exceed {MAX_FORK_FANOUT} sandboxes"
                    )
                if not all(isinstance(item, dict) for item in raw_targets_value):
                    raise ValueError("each fork sandbox must be a JSON object")
                raw_targets = list(raw_targets_value)
                if (
                    not isinstance(raw_operations, list)
                    or len(raw_operations) != len(raw_targets)
                ):
                    raise ValueError(
                        "_ucloud_operations must contain one operation per sandbox"
                    )
                operations = tuple(
                    SandboxOperation.from_dict(item) for item in raw_operations
                )
                targets = tuple(_fork_wire_target(item) for item in raw_targets)
            else:
                operation = SandboxOperation.from_dict(raw.get("_ucloud_operation"))
                operations = (operation,)
                target = _fork_wire_target(raw)
                targets = (target,)

            try:
                source = self.manager.get(source_sandbox_id)
            except (OSError, RuntimeError, ValueError) as exc:
                self._write_json(
                    {
                        "error": f"sandbox store unavailable during fork: {exc}",
                        "retryable": True,
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return
            if source is None:
                payload = _fork_request_error_payload(
                    f"source sandbox not found: {source_sandbox_id}",
                    self.manager,
                    targets,
                    operations,
                    batch=batch,
                    source_sandbox_id=source_sandbox_id,
                    source_generation=source_generation_for_intent,
                )
                self._write_json(
                    payload,
                    status=HTTPStatus.NOT_FOUND,
                )
                return
            if batch:
                targets = tuple(
                    sandbox_fork_target(source.spec, item) for item in raw_targets
                )
            else:
                target = sandbox_fork_target(source.spec, raw)
                targets = (target,)
            phases["parse_fork_ms"] = _elapsed_ms(phase)

            phase = time.monotonic()
            if batch:
                records, results, manager_timings = (
                    self.manager.fork_many_with_timings(
                        source_sandbox_id,
                        targets,
                        operations=operations,
                        source_generation=source_generation,
                        source_spec_hash=source_spec_hash,
                    )
                )
            else:
                record, result, manager_timings = self.manager.fork_with_timings(
                    source_sandbox_id,
                    target,
                    operation=operation,
                    source_generation=source_generation,
                    source_spec_hash=source_spec_hash,
                )
                records, results = (record,), (result,)
            phases["manager_fork_ms"] = _elapsed_ms(phase)
        except SandboxBusyError as exc:
            self._write_json(
                _fork_request_error_payload(
                    str(exc),
                    self.manager,
                    targets,
                    operations,
                    batch=batch,
                    source_sandbox_id=source_sandbox_id,
                    source_generation=source_generation_for_intent,
                    retryable=True,
                ),
                status=HTTPStatus.CONFLICT,
            )
            return
        except SandboxConflictError as exc:
            self._write_json(
                _fork_request_error_payload(
                    str(exc),
                    self.manager,
                    targets,
                    operations,
                    batch=batch,
                    source_sandbox_id=source_sandbox_id,
                    source_generation=source_generation_for_intent,
                ),
                status=HTTPStatus.CONFLICT,
            )
            return
        except SandboxCapacityUnavailableError as exc:
            self._write_json(
                _fork_request_error_payload(
                    str(exc),
                    self.manager,
                    targets,
                    operations,
                    batch=batch,
                    source_sandbox_id=source_sandbox_id,
                    source_generation=source_generation_for_intent,
                    retryable=True,
                ),
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        except SandboxForkUnsupportedError as exc:
            self._write_json(
                _fork_request_error_payload(
                    str(exc),
                    self.manager,
                    targets,
                    operations,
                    batch=batch,
                    source_sandbox_id=source_sandbox_id,
                    source_generation=source_generation_for_intent,
                    capability="fork-local-v1",
                ),
                status=HTTPStatus.NOT_IMPLEMENTED,
            )
            return
        except (RequestBodyTooLargeError, SandboxFileTooLargeError) as exc:
            self._write_json(
                _fork_request_error_payload(
                    str(exc),
                    self.manager,
                    targets,
                    operations,
                    batch=batch,
                    source_sandbox_id=source_sandbox_id,
                    source_generation=source_generation_for_intent,
                ),
                status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            )
            return
        except RuntimeError as exc:
            self._write_json(
                _fork_request_error_payload(
                    str(exc),
                    self.manager,
                    targets,
                    operations,
                    batch=batch,
                    source_sandbox_id=source_sandbox_id,
                    source_generation=source_generation_for_intent,
                    retryable=True,
                ),
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        except ValueError as exc:
            payload = _fork_request_error_payload(
                str(exc),
                self.manager,
                targets,
                operations,
                batch=batch,
                source_sandbox_id=source_sandbox_id,
                source_generation=source_generation_for_intent,
            )
            store_ambiguous = "intent_persisted" not in payload
            if store_ambiguous:
                payload["retryable"] = True
            self._write_json(
                payload,
                status=(
                    HTTPStatus.SERVICE_UNAVAILABLE
                    if store_ambiguous
                    else HTTPStatus.BAD_REQUEST
                ),
            )
            return

        response_payload: dict[str, Any] = {
            "intent_persisted": True,
            "timings": {
                "total_ms": _elapsed_ms(started),
                "phases": phases,
                "manager": manager_timings,
            },
        }
        if batch:
            response_payload["sandboxes"] = [record.to_dict() for record in records]
            response_payload["forks"] = [
                {
                    "sandbox_id": record.spec.id,
                    **_fork_result_payload(result),
                }
                for record, result in zip(records, results, strict=True)
            ]
        else:
            response_payload["sandbox"] = records[0].to_dict()
            response_payload["fork"] = _fork_result_payload(results[0])
        self._write_json(
            response_payload,
            status=(
                HTTPStatus.OK
                if manager_timings.get("idempotent")
                else HTTPStatus.CREATED
            ),
        )

    def _exec_session(self, path: str) -> None:
        session_id = self._exec_session_id_from_path(path)
        session = self.exec_manager.get(session_id)
        if session is None:
            self._write_json({"error": "exec session not found"}, status=HTTPStatus.NOT_FOUND)
            return
        self._write_json({"session": session.to_dict()})

    def _exec_events(self, parsed: Any) -> None:
        session_id = self._exec_session_id_from_path(parsed.path, suffix="/events")
        query = parse_qs(parsed.query)
        after = _int_query(query, "after", 0)
        limit = _int_query(query, "limit", 100)
        wait_seconds = min(30.0, max(0.0, float((query.get("wait_seconds") or ["0"])[0])))
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
        sandbox_id = unquote(path[len(prefix):-len(suffix)])
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
            self._write_json({"error": "sandbox not found"}, status=HTTPStatus.NOT_FOUND)
            return
        ssh = record.to_dict().get("ssh")
        if not ssh:
            self._write_json({"error": "sandbox ssh is not enabled"}, status=HTTPStatus.BAD_REQUEST)
            return
        self._write_json({"sandboxId": sandbox_id, "ssh": ssh})

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
                "sandboxId": sandbox_id,
                "path": container_path,
                "size": len(content),
                "command": list(result.argv),
                "exitCode": result.exit_code,
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
            "image": image_record.to_dict() if image_record is not None else build.image,
            "command": list(build.command),
            "exitCode": build.exit_code,
            "timings": timings,
        }
        if build.push_command:
            payload["pushCommand"] = list(build.push_command)
            payload["pushExitCode"] = build.push_exit_code
        self._write_json(payload, status=HTTPStatus.CREATED)

    def _pull_image(self) -> None:
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("image pull payload must be a JSON object")
            image = str(raw.get("image") or "")
            image_id = str(raw["id"]) if raw.get("id") else None
            record, result = self.image_manager.pull(image, image_id=image_id)
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json(
            {
                "image": record.to_dict(),
                "command": list(result.argv),
                "exitCode": result.exit_code,
            },
            status=HTTPStatus.CREATED,
        )

    def _snapshot_sandbox(self, path: str) -> None:
        if not self.image_builds_enabled:
            self._write_json(
                {"error": "snapshots are disabled on this node"},
                status=HTTPStatus.FORBIDDEN,
            )
            return
        prefix = "/v1/sandboxes/"
        suffix = "/snapshot"
        sandbox_id = unquote(path[len(prefix):-len(suffix)])
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("snapshot payload must be a JSON object")
            image = str(raw.get("image") or "")
            image_id = str(raw.get("id") or image_id_from_tag(image))
            result = self.manager.snapshot(sandbox_id, image)
            record = self.image_manager.record_snapshot(
                image_id=image_id,
                image=image,
                sandbox_id=sandbox_id,
                dry_run=self.manager.runtime.dry_run,
            )
        except (RuntimeError, ValueError) as exc:
            self._write_exception(exc)
            return
        self._write_json(
            {
                "image": record.to_dict(),
                "command": list(result.argv),
                "exitCode": result.exit_code,
            },
            status=HTTPStatus.CREATED,
        )

    def _exec_session_id_from_path(self, path: str, *, suffix: str = "") -> str:
        prefix = "/v1/exec/"
        if suffix:
            return unquote(path[len(prefix):-len(suffix)])
        return unquote(path[len(prefix):])

    def do_DELETE(self) -> None:
        if not self._check_node_control_authorized():
            return
        parsed = urlparse(self.path)
        prefix = "/v1/sandboxes/"
        if not parsed.path.startswith(prefix):
            self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        sandbox_id = unquote(parsed.path[len(prefix):])
        if not sandbox_id:
            self._write_json({"error": "sandbox id is required"}, status=HTTPStatus.BAD_REQUEST)
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
            "exitCode": result.exit_code,
        }
        self._write_json(payload)

    def _delete_operation_headers(self) -> tuple[int, str]:
        generation_header = self.headers.get(SANDBOX_GENERATION_HEADER)
        operation_id_header = self.headers.get(SANDBOX_OPERATION_ID_HEADER)
        if generation_header is None and operation_id_header is None:
            return 0, ""
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
        if generation < 0:
            raise ValueError(f"{SANDBOX_GENERATION_HEADER} cannot be negative")
        if not operation_id:
            raise ValueError(f"{SANDBOX_OPERATION_ID_HEADER} cannot be empty")
        return generation, operation_id

    def _check_node_control_authorized(self) -> bool:
        expected = self.node_control_bearer_token
        if expected is None:
            return True
        authorization = self.headers.get("Authorization") or ""
        prefix = "Bearer "
        supplied = (
            authorization[len(prefix) :]
            if authorization.startswith(prefix)
            else ""
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


def build_node_agent_server(
    host: str,
    port: int,
    *,
    sandbox_file: Path,
    image_file: Path,
    job_id: str,
    node_id: str,
    node_url: str | None = None,
    agent_version: str = "",
    deployment_id: str = "",
    init_version: str = "",
    total_resources: ResourceQuantity | None = None,
    cpu_overcommit: float = 1.0,
    memory_overcommit: float = 1.0,
    disk_overcommit: float = 1.0,
    runtime: DockerGvisorRuntime | None = None,
    image_runtime: DockerImageRuntime | None = None,
    ssh_port_range: tuple[int, int] | None = (22000, 22999),
    image_builds_enabled: bool = False,
    extra_capabilities: tuple[str, ...] = (),
    runtime_metrics_provider: Callable[[], NodeRuntimeMetrics | None] | None = None,
    max_json_body_bytes: int = DEFAULT_MAX_JSON_BODY_BYTES,
    max_file_body_bytes: int = DEFAULT_MAX_FILE_BODY_BYTES,
    max_active_image_builds: int = 4,
    physical_disk_path: Path | None = None,
    node_control_bearer_token: str | None = None,
    build_context_store_dir: Path | None = None,
) -> HighBacklogThreadingHTTPServer:
    if node_control_bearer_token is not None and not node_control_bearer_token.strip():
        raise ValueError("node control bearer token cannot be empty")
    if (
        max_json_body_bytes < 1
        or max_file_body_bytes < 1
        or max_active_image_builds < 1
    ):
        raise ValueError("node-agent request and build limits must be positive")
    configured_resources = total_resources or ResourceQuantity()
    if not configured_resources.is_valid:
        raise ValueError("total_resources cannot contain negative or non-finite values")
    overcommit = {
        "cpu_overcommit": cpu_overcommit,
        "memory_overcommit": memory_overcommit,
        "disk_overcommit": disk_overcommit,
    }
    for name, factor in overcommit.items():
        if not math.isfinite(factor) or factor < 0:
            raise ValueError(f"{name} must be finite and non-negative")
    manager = SandboxManager(
        SandboxStore(sandbox_file),
        runtime or DockerGvisorRuntime(dry_run=True),
        ssh_port_range=ssh_port_range,
        effective_capacity=configured_resources.scaled(
            cpu=cpu_overcommit,
            memory=memory_overcommit,
            disk=disk_overcommit,
        ),
    )
    manager.reconcile_checkpoint_storage()
    exec_manager = ExecSessionManager(
        manager,
        route_node_id=node_id,
        route_job_id=job_id,
    )
    image_manager = ImageManager(
        ImageStore(image_file),
        image_runtime or DockerImageRuntime(dry_run=True),
        max_active_builds=max_active_image_builds,
        admission_store=manager.store,
    )
    build_context_store = BuildContextBlobStore(
        build_context_store_dir
        or image_file.parent / f"{image_file.stem}-contexts",
        max_blob_bytes=max_file_body_bytes,
        max_total_bytes=DEFAULT_MAX_BUILD_CONTEXT_STORE_BYTES,
        max_entries=DEFAULT_MAX_BUILD_CONTEXT_ENTRIES,
        max_age_seconds=DEFAULT_MAX_BUILD_CONTEXT_AGE_SECONDS,
    )

    class BoundHandler(NodeAgentHandler):
        pass

    BoundHandler.manager = manager
    BoundHandler.exec_manager = exec_manager
    BoundHandler.image_manager = image_manager
    BoundHandler.build_context_store = build_context_store
    BoundHandler.job_id = job_id
    BoundHandler.node_id = node_id
    BoundHandler.node_url = node_url
    BoundHandler.agent_version = agent_version
    BoundHandler.deployment_id = deployment_id
    BoundHandler.init_version = init_version
    BoundHandler.total_resources = configured_resources
    BoundHandler.cpu_overcommit = cpu_overcommit
    BoundHandler.memory_overcommit = memory_overcommit
    BoundHandler.disk_overcommit = disk_overcommit
    capabilities = ["image-cache"] if image_builds_enabled else ["sandbox", "image-cache"]
    if image_builds_enabled:
        capabilities.extend(["image-build", "snapshot"])
    merged_capabilities = merge_capabilities(tuple(capabilities), extra_capabilities)
    if image_builds_enabled or not manager.runtime.fork_enabled:
        merged_capabilities = tuple(
            capability
            for capability in merged_capabilities
            if capability != FORK_LOCAL_CAPABILITY
        )
    BoundHandler.capabilities = merged_capabilities
    BoundHandler.image_builds_enabled = image_builds_enabled
    BoundHandler.node_epoch = uuid4().hex
    BoundHandler.physical_disk_path = physical_disk_path or _default_physical_disk_path(
        sandbox_file
    )
    BoundHandler.node_control_bearer_token = node_control_bearer_token
    BoundHandler.max_json_body_bytes = max_json_body_bytes
    BoundHandler.max_file_body_bytes = max_file_body_bytes
    BoundHandler.runtime_metrics_provider = staticmethod(
        runtime_metrics_provider or sample_node_runtime_metrics
    )
    return HighBacklogThreadingHTTPServer((host, port), BoundHandler)


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


def _fork_source_envelope(raw: dict[str, Any]) -> tuple[int, str]:
    source_raw = raw.get("_ucloud_source")
    if not isinstance(source_raw, dict):
        raise ValueError("_ucloud_source must be a JSON object")
    try:
        generation = int(source_raw.get("generation"))
    except (TypeError, ValueError) as exc:
        raise ValueError("source generation must be an integer") from exc
    if generation < 0:
        raise ValueError("source generation cannot be negative")
    spec_hash = str(source_raw.get("spec_hash") or "").strip()
    if not spec_hash:
        raise ValueError("source spec_hash is required")
    return generation, spec_hash


def _fork_request_is_batch(raw: dict[str, Any]) -> bool:
    """Validate the mutually-exclusive single and fan-out wire shapes."""

    batch = "sandboxes" in raw
    if "sandbox" in raw and "target" in raw:
        raise ValueError("fork payload cannot contain both sandbox and target")
    single = "sandbox" in raw or "target" in raw
    if batch and single:
        raise ValueError("fork payload cannot contain both sandbox and sandboxes")
    if batch and "_ucloud_operation" in raw:
        raise ValueError(
            "batch fork payload cannot contain singular _ucloud_operation"
        )
    if not batch and "_ucloud_operations" in raw:
        raise ValueError(
            "single fork payload cannot contain plural _ucloud_operations"
        )
    return batch


def _fork_wire_target(raw: object) -> SandboxSpec:
    """Parse the gateway's full target spec without consulting the source."""

    if not isinstance(raw, dict):
        raise ValueError("fork sandbox must be a JSON object")
    target_raw = raw.get("sandbox", raw.get("target", raw))
    if not isinstance(target_raw, dict) or not target_raw.get("image"):
        raise ValueError("node fork requests require a complete sandbox spec")
    target = SandboxSpec.from_dict(target_raw)
    target.validate()
    return target


def _fork_intent_persisted(
    manager: SandboxManager,
    target: SandboxSpec | None,
    operation: SandboxOperation | None,
    *,
    source_sandbox_id: str,
    source_generation: int | None,
) -> bool | None:
    """Return whether this exact fork has a durable destination intent.

    ``None`` is deliberately reserved for an unreadable/ambiguous store.  The
    gateway must retain its reservation in that case, just as it does for an
    interrupted node request.
    """

    if target is None or operation is None or source_generation is None:
        return False
    try:
        record = manager.get(target.id)
    except (OSError, RuntimeError, ValueError):
        return None
    if record is None:
        return False
    return (
        record.generation == operation.generation
        and record.operation_id == operation.operation_id
        and record.spec_hash == operation.spec_hash
        and record.creation_kind == "restore"
        and record.source_sandbox_id == source_sandbox_id
        and record.source_generation == source_generation
        and bool(record.checkpoint_id)
        and len(record.fork_nonce) == 64
        and all(character in "0123456789abcdef" for character in record.fork_nonce)
        and record.state in {"restoring", "running"}
    )


def _fork_error_payload(
    error: str,
    manager: SandboxManager,
    target: SandboxSpec | None,
    operation: SandboxOperation | None,
    *,
    source_sandbox_id: str,
    source_generation: int | None,
    **fields: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"error": error, **fields}
    persisted = _fork_intent_persisted(
        manager,
        target,
        operation,
        source_sandbox_id=source_sandbox_id,
        source_generation=source_generation,
    )
    if persisted is not None:
        payload["intent_persisted"] = persisted
    return payload


def _fork_request_error_payload(
    error: str,
    manager: SandboxManager,
    targets: tuple[SandboxSpec, ...],
    operations: tuple[SandboxOperation, ...],
    *,
    batch: bool,
    source_sandbox_id: str,
    source_generation: int | None,
    **fields: Any,
) -> dict[str, Any]:
    if not batch:
        target = targets[0] if targets else None
        operation = operations[0] if operations else None
        return _fork_error_payload(
            error,
            manager,
            target,
            operation,
            source_sandbox_id=source_sandbox_id,
            source_generation=source_generation,
            **fields,
        )

    payload: dict[str, Any] = {"error": error, **fields}
    if not targets or len(targets) != len(operations):
        payload["intent_persisted"] = False
        return payload
    persisted = tuple(
        _fork_intent_persisted(
            manager,
            target,
            operation,
            source_sandbox_id=source_sandbox_id,
            source_generation=source_generation,
        )
        for target, operation in zip(targets, operations, strict=True)
    )
    payload["intents"] = [
        {
            "sandbox_id": target.id,
            "intent_persisted": value,
        }
        for target, value in zip(targets, persisted, strict=True)
    ]
    if all(value is True for value in persisted):
        payload["intent_persisted"] = True
    elif all(value is False for value in persisted):
        payload["intent_persisted"] = False
    # A partial/unreadable set is ambiguous. Omitting the signal makes the
    # gateway retain every reservation for safe exact replay.
    return payload


def _fork_result_payload(result: SandboxForkRuntimeResult) -> dict[str, Any]:
    return {
        "checkpoint_id": result.checkpoint_id,
        "restored": result.restored,
        # Runtime argv can contain restore-time environment values. Keep the
        # stable response shape without reflecting secrets.
        "commands": [],
    }


def _sandbox_id_from_path(path: str, *, suffix: str = "") -> str:
    prefix = "/v1/sandboxes/"
    if suffix:
        return unquote(path[len(prefix):-len(suffix)])
    return unquote(path[len(prefix):])


def _build_context_digest_from_path(path: str) -> str | None:
    prefix = "/v1/image-contexts/"
    if not path.startswith(prefix):
        return None
    digest = unquote(path[len(prefix):])
    return digest if digest and "/" not in digest else None


def _image_build_key_from_path(path: str) -> str | None:
    prefix = "/v1/images/builds/"
    if not path.startswith(prefix):
        return None
    key = unquote(path[len(prefix):])
    return key or None


def _file_path_from_query(parsed: Any) -> str | None:
    value = (parse_qs(parsed.query).get("path") or [""])[0]
    value = value.strip()
    return value or None
