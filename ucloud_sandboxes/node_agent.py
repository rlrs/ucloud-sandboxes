from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import time
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlparse

from .agent import build_heartbeat
from .images import (
    DockerImageRuntime,
    ImageBuildSpec,
    ImageManager,
    ImageStore,
    image_id_from_tag,
    uploaded_build_context,
)
from .registry import heartbeat_to_dict
from .models import NodeRuntimeMetrics, ResourceQuantity
from .runtime_metrics import sample_node_runtime_metrics
from .capabilities import merge_capabilities
from .sandbox import (
    DockerGvisorRuntime,
    SandboxManager,
    SandboxRecord,
    SandboxSpec,
    SandboxStore,
)
from .sandbox_exec import ExecSessionManager, SandboxExecSpec


class NodeAgentHandler(BaseHTTPRequestHandler):
    manager: SandboxManager
    exec_manager: ExecSessionManager
    image_manager: ImageManager
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
    server_version = "ucloud-sandboxes-node-agent/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self._write_json({"ok": True})
            return
        if parsed.path == "/v1/heartbeat":
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
                            active_sandboxes=self.manager.active_count(),
                            capabilities=self.capabilities,
                            total_resources=self.total_resources,
                            used_resources=self.manager.requested_resources(),
                            cpu_overcommit=self.cpu_overcommit,
                            memory_overcommit=self.memory_overcommit,
                            disk_overcommit=self.disk_overcommit,
                            runtime_metrics=self.runtime_metrics_provider(),
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
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/v1/sandboxes":
            self._create_sandbox()
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

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path.startswith("/v1/sandboxes/") and parsed.path.endswith("/files"):
            self._upload_file(parsed)
            return
        self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def _create_sandbox(self) -> None:
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("sandbox payload must be a JSON object")
            spec = SandboxSpec.from_dict(raw)
            record, result = self.manager.create(spec)
        except (RuntimeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._write_json(
            {
                "sandbox": record.to_dict(),
                "command": list(result.argv),
                "exitCode": result.exit_code,
            },
            status=HTTPStatus.CREATED,
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
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._write_json({"session": session.to_dict()}, status=HTTPStatus.CREATED)

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
        deadline = time.monotonic() + wait_seconds
        try:
            while True:
                events = self.exec_manager.drain_events(
                    session_id,
                    after=after,
                    limit=limit,
                )
                session = self.exec_manager.get(session_id)
                if events or wait_seconds <= 0 or time.monotonic() >= deadline:
                    break
                if session is not None and session.status in {"exited", "failed"}:
                    break
                time.sleep(0.05)
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
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
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
        record = self.manager.get(sandbox_id)
        if record is None:
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
            content = self._read_raw_body()
            result = self.manager.upload_file(sandbox_id, container_path, content)
        except (RuntimeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
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
            content, result = self.manager.download_file(sandbox_id, container_path)
        except (RuntimeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
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
        try:
            raw = self._read_json_body()
            if not isinstance(raw, dict):
                raise ValueError("image build payload must be a JSON object")
            push = bool(raw.get("push", False))
            with uploaded_build_context(raw) as context_path:
                spec = ImageBuildSpec.from_dict(raw)
                if context_path is not None:
                    spec = ImageBuildSpec(
                        id=spec.id,
                        tag=spec.tag,
                        context_path=str(context_path),
                        dockerfile=spec.dockerfile,
                        build_args=spec.build_args,
                        labels=spec.labels,
                    )
                record, result = self.image_manager.build(spec)
                push_result = self.image_manager.runtime.push(spec.tag) if push else None
        except (RuntimeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        payload: dict[str, Any] = {
            "image": record.to_dict(),
            "command": list(result.argv),
            "exitCode": result.exit_code,
        }
        if push_result is not None:
            payload["pushCommand"] = list(push_result.argv)
            payload["pushExitCode"] = push_result.exit_code
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
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
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
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
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
            record, result = self.manager.delete(sandbox_id)
        except (RuntimeError, ValueError) as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        payload: dict[str, Any] = {
            "deleted": record.to_dict() if record is not None else None,
            "command": list(result.argv),
            "exitCode": result.exit_code,
        }
        self._write_json(payload)

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
) -> ThreadingHTTPServer:
    manager = SandboxManager(
        SandboxStore(sandbox_file),
        runtime or DockerGvisorRuntime(dry_run=True),
        ssh_port_range=ssh_port_range,
    )
    exec_manager = ExecSessionManager(manager)
    image_manager = ImageManager(
        ImageStore(image_file),
        image_runtime or DockerImageRuntime(dry_run=True),
    )

    class BoundHandler(NodeAgentHandler):
        pass

    BoundHandler.manager = manager
    BoundHandler.exec_manager = exec_manager
    BoundHandler.image_manager = image_manager
    BoundHandler.job_id = job_id
    BoundHandler.node_id = node_id
    BoundHandler.node_url = node_url
    BoundHandler.agent_version = agent_version
    BoundHandler.deployment_id = deployment_id
    BoundHandler.init_version = init_version
    BoundHandler.total_resources = total_resources or ResourceQuantity()
    BoundHandler.cpu_overcommit = cpu_overcommit
    BoundHandler.memory_overcommit = memory_overcommit
    BoundHandler.disk_overcommit = disk_overcommit
    capabilities = ["image-cache"] if image_builds_enabled else ["sandbox", "image-cache"]
    if image_builds_enabled:
        capabilities.extend(["image-build", "snapshot"])
    BoundHandler.capabilities = merge_capabilities(tuple(capabilities), extra_capabilities)
    BoundHandler.image_builds_enabled = image_builds_enabled
    BoundHandler.runtime_metrics_provider = staticmethod(
        runtime_metrics_provider or sample_node_runtime_metrics
    )
    return ThreadingHTTPServer((host, port), BoundHandler)


def sandbox_record_to_dict(record: SandboxRecord) -> dict[str, Any]:
    return record.to_dict()


def _int_query(query: dict[str, list[str]], key: str, default: int) -> int:
    try:
        return int((query.get(key) or [str(default)])[0])
    except ValueError:
        return default


def _sandbox_id_from_path(path: str, *, suffix: str = "") -> str:
    prefix = "/v1/sandboxes/"
    if suffix:
        return unquote(path[len(prefix):-len(suffix)])
    return unquote(path[len(prefix):])


def _file_path_from_query(parsed: Any) -> str | None:
    value = (parse_qs(parsed.query).get("path") or [""])[0]
    value = value.strip()
    return value or None
