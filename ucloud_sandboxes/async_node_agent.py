from __future__ import annotations

import asyncio
import hmac
from pathlib import Path
from typing import Any

from aiohttp import WSMsgType, web

from .async_exec import (
    AsyncExecSessionManager,
    STDERR_STREAM_ID,
    STDOUT_STREAM_ID,
)
from .deployment import service_health
from .images import DockerImageRuntime, ImageManager, ImageStore
from .models import ResourceQuantity
from .node_agent import SANDBOX_GENERATION_HEADER, SANDBOX_OPERATION_ID_HEADER
from .sandbox import (
    DockerGvisorRuntime,
    SandboxCapacityUnavailableError,
    SandboxConflictError,
    SandboxManager,
    SandboxOperation,
    SandboxSpec,
    SandboxStore,
)
from .sandbox_exec import SandboxExecSpec


SANDBOX_MANAGER_KEY = web.AppKey("sandbox_manager", SandboxManager)
IMAGE_MANAGER_KEY = web.AppKey("image_manager", ImageManager)
EXEC_MANAGER_KEY = web.AppKey("exec_manager", AsyncExecSessionManager)


async def create_sandbox(request: web.Request) -> web.Response:
    manager = sandbox_manager(request)
    try:
        raw = await request.json()
        if not isinstance(raw, dict):
            raise ValueError("sandbox payload must be a JSON object")
        spec = SandboxSpec.from_dict(raw)
        operation_raw = raw.get("_ucloud_operation")
        operation = (
            SandboxOperation.from_dict(operation_raw)
            if operation_raw is not None
            else None
        )
        record, result, timings = await asyncio.to_thread(
            manager.create_with_timings,
            spec,
            operation=operation,
        )
    except SandboxConflictError as exc:
        raise web.HTTPConflict(text=str(exc)) from exc
    except SandboxCapacityUnavailableError as exc:
        raise web.HTTPServiceUnavailable(text=str(exc)) from exc
    except RuntimeError as exc:
        raise web.HTTPServiceUnavailable(text=str(exc)) from exc
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    return web.json_response(
        {
            "sandbox": record.to_dict(),
            "command": list(result.argv),
            "exitCode": result.exit_code,
        },
        status=200 if timings.get("idempotent") else 201,
    )


async def delete_sandbox(request: web.Request) -> web.Response:
    generation_header = request.headers.get(SANDBOX_GENERATION_HEADER)
    operation_id_header = request.headers.get(SANDBOX_OPERATION_ID_HEADER)
    try:
        if generation_header is None and operation_id_header is None:
            generation, operation_id = 0, ""
        elif generation_header is None or operation_id_header is None:
            raise ValueError(
                f"{SANDBOX_GENERATION_HEADER} and {SANDBOX_OPERATION_ID_HEADER} "
                "must be supplied together"
            )
        else:
            try:
                generation = int(generation_header)
            except ValueError as exc:
                raise ValueError(
                    f"{SANDBOX_GENERATION_HEADER} must be an integer"
                ) from exc
            operation_id = operation_id_header.strip()
            if generation < 0:
                raise ValueError(f"{SANDBOX_GENERATION_HEADER} cannot be negative")
            if not operation_id:
                raise ValueError(f"{SANDBOX_OPERATION_ID_HEADER} cannot be empty")
        record, result = await asyncio.to_thread(
            sandbox_manager(request).delete,
            request.match_info["sandbox_id"],
            generation=generation,
            operation_id=operation_id,
        )
    except SandboxConflictError as exc:
        raise web.HTTPConflict(text=str(exc)) from exc
    except RuntimeError as exc:
        raise web.HTTPServiceUnavailable(text=str(exc)) from exc
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    return web.json_response(
        {
            "deleted": record.to_dict() if record is not None else None,
            "command": list(result.argv),
            "exitCode": result.exit_code,
        }
    )


async def configure_drain(request: web.Request) -> web.Response:
    try:
        raw = await request.json()
        if not isinstance(raw, dict):
            raise ValueError("drain payload must be a JSON object")
        token = str(raw.get("token") or "").strip()
        draining = raw.get("draining")
        if not isinstance(draining, bool):
            raise ValueError("draining must be a boolean")
        snapshot = await asyncio.to_thread(
            sandbox_manager(request).configure_drain,
            token,
            draining,
            active_build_count=image_manager(request).active_build_count,
        )
    except SandboxConflictError as exc:
        raise web.HTTPConflict(text=str(exc)) from exc
    except RuntimeError as exc:
        raise web.HTTPServiceUnavailable(text=str(exc)) from exc
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    return web.json_response(
        {
            "drain": {
                "token": snapshot.drain.token,
                "draining": snapshot.drain.draining,
                "admission_open": snapshot.drain.admission_open,
                "drain_activity_epoch": snapshot.drain.drain_activity_epoch,
                "activity_epoch": snapshot.activity.activity_revision,
                "active_sandboxes": snapshot.activity.active_sandboxes,
                "reserved_resources": snapshot.activity.reserved_resources.to_dict(),
                "active_image_builds": snapshot.active_image_builds,
                "ready": snapshot.ready,
            }
        }
    )


async def list_sandboxes(request: web.Request) -> web.Response:
    manager = sandbox_manager(request)
    records = await asyncio.to_thread(manager.list)
    return web.json_response(
        {
            "sandboxes": [
                record.to_dict()
                for record in sorted(records, key=lambda item: item.spec.id)
            ]
        }
    )


async def start_exec(request: web.Request) -> web.Response:
    exec_manager = async_exec_manager(request)
    sandbox_id = request.match_info["sandbox_id"]
    try:
        raw = await request.json()
        if not isinstance(raw, dict):
            raise ValueError("exec payload must be a JSON object")
        spec = SandboxExecSpec.from_dict(raw, sandbox_id=sandbox_id)
        session = await exec_manager.start(spec)
    except (RuntimeError, ValueError) as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    return web.json_response({"session": session.to_dict()}, status=201)


async def get_exec_session(request: web.Request) -> web.Response:
    session = async_exec_manager(request).get(request.match_info["session_id"])
    if session is None:
        raise web.HTTPNotFound(text="exec session not found")
    return web.json_response({"session": session.to_dict()})


async def exec_events(request: web.Request) -> web.Response:
    manager = async_exec_manager(request)
    try:
        events = await manager.events_after(
            request.match_info["session_id"],
            after=int(request.query.get("after", "0")),
            limit=int(request.query.get("limit", "100")),
            wait_seconds=float(request.query.get("wait_seconds", "0")),
        )
    except ValueError as exc:
        raise web.HTTPNotFound(text=str(exc)) from exc
    session = manager.get(request.match_info["session_id"])
    return web.json_response(
        {
            "session": session.to_dict() if session is not None else None,
            "events": [event.to_dict() for event in events],
        }
    )


async def exec_websocket(request: web.Request) -> web.WebSocketResponse:
    manager = async_exec_manager(request)
    session_id = request.match_info["session_id"]
    session = manager.get(session_id)
    if session is None:
        raise web.HTTPNotFound(text="exec session not found")

    ws = web.WebSocketResponse(heartbeat=30.0, max_msg_size=16 * 1024 * 1024)
    await ws.prepare(request)
    await ws.send_json({"type": "session", "session": session.to_dict()})

    async def send_events() -> None:
        while True:
            event = await manager.next_output_event(session_id)
            if event.stream in {"stdout", "stderr"}:
                await ws.send_bytes(event.binary_frame())
            else:
                await ws.send_json(
                    {
                        "type": event.stream,
                        "sequence": event.sequence,
                        "data": event.data.decode("utf-8", errors="replace"),
                        "exit_code": event.exit_code,
                    }
                )
            if event.stream == "exit":
                await ws.close()
                return

    async def receive_stdin() -> None:
        async for message in ws:
            if message.type == WSMsgType.BINARY:
                await manager.write_stdin(session_id, message.data)
            elif message.type == WSMsgType.TEXT:
                data = message.json()
                if data.get("type") == "stdin":
                    await manager.write_stdin(
                        session_id,
                        str(data.get("data") or "").encode("utf-8"),
                    )
                elif data.get("type") in {"close_stdin", "eof"}:
                    await manager.close_stdin(session_id)
            elif message.type == WSMsgType.ERROR:
                return

    sender = asyncio.create_task(send_events())
    receiver = asyncio.create_task(receive_stdin())
    done, pending = await asyncio.wait(
        {sender, receiver},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    for task in done:
        if not task.cancelled():
            task.result()
    return ws


async def sandbox_ssh(request: web.Request) -> web.Response:
    record = await asyncio.to_thread(
        sandbox_manager(request).get,
        request.match_info["sandbox_id"],
    )
    if record is None:
        raise web.HTTPNotFound(text="sandbox not found")
    ssh = record.to_dict().get("ssh")
    if not ssh:
        raise web.HTTPBadRequest(text="sandbox ssh is not enabled")
    return web.json_response({"sandboxId": request.match_info["sandbox_id"], "ssh": ssh})


async def sandbox_ssh_websocket(request: web.Request) -> web.WebSocketResponse:
    record = await asyncio.to_thread(
        sandbox_manager(request).get,
        request.match_info["sandbox_id"],
    )
    if record is None:
        raise web.HTTPNotFound(text="sandbox not found")
    if not record.spec.ssh.enabled or record.spec.ssh.host_port is None:
        raise web.HTTPBadRequest(text="sandbox ssh is not enabled")

    ws = web.WebSocketResponse(heartbeat=30.0, max_msg_size=16 * 1024 * 1024)
    await ws.prepare(request)
    try:
        reader, writer = await asyncio.open_connection(
            record.spec.ssh.host,
            record.spec.ssh.host_port,
        )
    except OSError as exc:
        await ws.send_json({"type": "error", "message": str(exc)})
        await ws.close()
        return ws

    async def socket_to_ws() -> None:
        try:
            while True:
                chunk = await reader.read(16 * 1024)
                if not chunk:
                    await ws.close()
                    return
                await ws.send_bytes(chunk)
        finally:
            writer.close()
            await writer.wait_closed()

    async def ws_to_socket() -> None:
        async for message in ws:
            if message.type == WSMsgType.BINARY:
                writer.write(message.data)
                await writer.drain()
            elif message.type == WSMsgType.TEXT and message.data == "close":
                writer.write_eof()
                await writer.drain()
            elif message.type == WSMsgType.ERROR:
                return

    sender = asyncio.create_task(socket_to_ws())
    receiver = asyncio.create_task(ws_to_socket())
    done, pending = await asyncio.wait(
        {sender, receiver},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    for task in done:
        if not task.cancelled():
            task.result()
    return ws


async def healthz(_request: web.Request) -> web.Response:
    return web.json_response(service_health("async-node-agent"))


def _node_control_auth_middleware(
    expected: str | None,
) -> web.middleware:
    @web.middleware
    async def middleware(
        request: web.Request,
        handler: Any,
    ) -> web.StreamResponse:
        if request.path == "/healthz" or expected is None:
            return await handler(request)
        authorization = request.headers.get("Authorization", "")
        prefix = "Bearer "
        supplied = (
            authorization[len(prefix) :]
            if authorization.startswith(prefix)
            else ""
        )
        if supplied and hmac.compare_digest(supplied, expected):
            return await handler(request)
        raise web.HTTPUnauthorized(
            text="unauthorized",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return middleware


def create_async_node_agent_app(
    *,
    sandbox_file: Path,
    image_file: Path,
    runtime: DockerGvisorRuntime | None = None,
    image_runtime: DockerImageRuntime | None = None,
    ssh_port_range: tuple[int, int] | None = (22000, 22999),
    total_resources: ResourceQuantity | None = None,
    node_control_bearer_token: str | None = None,
) -> web.Application:
    if node_control_bearer_token is not None and not node_control_bearer_token.strip():
        raise ValueError("node control bearer token cannot be empty")
    manager = SandboxManager(
        SandboxStore(sandbox_file),
        runtime or DockerGvisorRuntime(dry_run=True),
        ssh_port_range=ssh_port_range,
        effective_capacity=total_resources,
    )
    app = web.Application(
        middlewares=[_node_control_auth_middleware(node_control_bearer_token)]
    )
    app[SANDBOX_MANAGER_KEY] = manager
    app[IMAGE_MANAGER_KEY] = ImageManager(
        ImageStore(image_file),
        image_runtime or DockerImageRuntime(dry_run=True),
        admission_store=manager.store,
    )
    app[EXEC_MANAGER_KEY] = AsyncExecSessionManager(manager)
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/v1/sandboxes", list_sandboxes)
    app.router.add_post("/v1/sandboxes", create_sandbox)
    app.router.add_delete("/v1/sandboxes/{sandbox_id}", delete_sandbox)
    app.router.add_post("/v1/drain", configure_drain)
    app.router.add_post("/v1/sandboxes/{sandbox_id}/exec", start_exec)
    app.router.add_get("/v1/exec/{session_id}", get_exec_session)
    app.router.add_get("/v1/exec/{session_id}/events", exec_events)
    app.router.add_get("/v1/exec/{session_id}/ws", exec_websocket)
    app.router.add_get("/v1/sandboxes/{sandbox_id}/ssh", sandbox_ssh)
    app.router.add_get("/v1/sandboxes/{sandbox_id}/ssh/ws", sandbox_ssh_websocket)
    return app


def sandbox_manager(request: web.Request) -> SandboxManager:
    return request.app[SANDBOX_MANAGER_KEY]


def async_exec_manager(request: web.Request) -> AsyncExecSessionManager:
    return request.app[EXEC_MANAGER_KEY]


def image_manager(request: web.Request) -> ImageManager:
    return request.app[IMAGE_MANAGER_KEY]
