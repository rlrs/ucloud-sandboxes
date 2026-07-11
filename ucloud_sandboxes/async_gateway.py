from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import json
from typing import Any, AsyncIterator
from urllib.parse import quote

from aiohttp import ClientSession, ClientTimeout, ContentTypeError, WSMsgType

from .async_exec import STREAM_NAMES
from .gateway import _fork_success_error
from .sandbox import FORK_REQUEST_TIMEOUT_SECONDS, MAX_FORK_FANOUT
from .sandbox_exec import SandboxExecSpec


class AsyncGatewayError(RuntimeError):
    pass


class AsyncNodeGatewayClient:
    """Async gateway client; fork methods target the public control plane."""

    def __init__(
        self,
        node_url: str,
        *,
        session: ClientSession | None = None,
        node_control_bearer_token: str | None = None,
    ) -> None:
        if (
            node_control_bearer_token is not None
            and not node_control_bearer_token.strip()
        ):
            raise ValueError("node control bearer token cannot be empty")
        self.node_url = node_url.rstrip("/")
        self._session = session
        self._owned_session: ClientSession | None = None
        self._node_control_headers = (
            {"Authorization": f"Bearer {node_control_bearer_token}"}
            if node_control_bearer_token is not None
            else {}
        )

    async def fork_sandbox(
        self,
        source_sandbox_id: str,
        sandbox: dict[str, Any],
        *,
        timeout_seconds: float = FORK_REQUEST_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        """Fork one sandbox through the public control plane."""
        payload = {"sandbox": dict(sandbox)}
        response = await self._post_fork(
            source_sandbox_id,
            payload,
            timeout_seconds=timeout_seconds,
        )
        requested_id = str(sandbox.get("id") or "")
        error_message = _fork_success_error(
            response,
            source_sandbox_id=source_sandbox_id,
            requested_ids=(requested_id,),
            batch=False,
        )
        if error_message is not None:
            raise AsyncGatewayError(error_message)
        return response

    async def fork_sandboxes(
        self,
        source_sandbox_id: str,
        sandboxes: Sequence[dict[str, Any]],
        *,
        timeout_seconds: float = FORK_REQUEST_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        """Fork several sandboxes through the public control plane."""
        requested = tuple(dict(sandbox) for sandbox in sandboxes)
        if not 1 <= len(requested) <= MAX_FORK_FANOUT:
            raise ValueError(f"fork batch size must be in [1, {MAX_FORK_FANOUT}]")
        payload = {"sandboxes": list(requested)}
        response = await self._post_fork(
            source_sandbox_id,
            payload,
            timeout_seconds=timeout_seconds,
        )
        error_message = _fork_success_error(
            response,
            source_sandbox_id=source_sandbox_id,
            requested_ids=tuple(str(item.get("id") or "") for item in requested),
            batch=True,
        )
        if error_message is not None:
            raise AsyncGatewayError(error_message)
        return response

    async def _post_fork(
        self,
        source_sandbox_id: str,
        payload: dict[str, Any],
        *,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        encoded_source_id = quote(source_sandbox_id, safe="")
        async with (await self._client()).post(
            f"{self.node_url}/v1/sandboxes/{encoded_source_id}/forks",
            json=payload,
            headers=self._node_control_headers,
            allow_redirects=False,
            timeout=ClientTimeout(total=timeout_seconds),
        ) as response:
            if not 200 <= response.status < 300:
                raise AsyncGatewayError(
                    f"node-agent request failed ({response.status}): "
                    f"{await response.text()}"
                )
            try:
                raw = await response.json()
            except (ContentTypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AsyncGatewayError(
                    "node-agent returned an invalid JSON fork payload."
                ) from exc
        if not isinstance(raw, dict):
            raise AsyncGatewayError("node-agent returned a non-object fork payload.")
        return raw

    async def __aenter__(self) -> "AsyncNodeGatewayClient":
        await self._client()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owned_session is not None:
            await self._owned_session.close()
            self._owned_session = None

    async def start_exec(
        self,
        sandbox_id: str,
        spec: SandboxExecSpec,
    ) -> dict[str, Any]:
        payload = spec.to_dict()
        payload.pop("sandbox_id", None)
        async with (await self._client()).post(
            f"{self.node_url}/v1/sandboxes/{sandbox_id}/exec",
            json=payload,
            headers=self._node_control_headers,
            allow_redirects=False,
        ) as response:
            if response.status >= 400:
                raise AsyncGatewayError(await response.text())
            raw = await response.json()
        if not isinstance(raw, dict):
            raise AsyncGatewayError("node-agent returned a non-object exec payload.")
        return raw

    async def open_exec_stream(
        self,
        sandbox_id: str,
        spec: SandboxExecSpec,
    ) -> "ExecWebSocketStream":
        started = await self.start_exec(sandbox_id, spec)
        session = started.get("session")
        if not isinstance(session, dict) or not isinstance(session.get("id"), str):
            raise AsyncGatewayError("node-agent returned an invalid exec session.")
        ws = await (await self._client()).ws_connect(
            f"{self.node_url}/v1/exec/{session['id']}/ws",
            heartbeat=30.0,
            max_msg_size=16 * 1024 * 1024,
            headers=self._node_control_headers,
        )
        return ExecWebSocketStream(session["id"], ws)

    async def get_ssh_target(self, sandbox_id: str) -> dict[str, Any]:
        async with (await self._client()).get(
            f"{self.node_url}/v1/sandboxes/{sandbox_id}/ssh",
            headers=self._node_control_headers,
            allow_redirects=False,
        ) as response:
            if response.status >= 400:
                raise AsyncGatewayError(await response.text())
            raw = await response.json()
        if not isinstance(raw, dict):
            raise AsyncGatewayError("node-agent returned a non-object SSH payload.")
        return raw

    async def open_ssh_stream(self, sandbox_id: str) -> "TcpWebSocketStream":
        ws = await (await self._client()).ws_connect(
            f"{self.node_url}/v1/sandboxes/{sandbox_id}/ssh/ws",
            heartbeat=30.0,
            max_msg_size=16 * 1024 * 1024,
            headers=self._node_control_headers,
        )
        return TcpWebSocketStream(ws)

    async def _client(self) -> ClientSession:
        if self._session is not None:
            return self._session
        if self._owned_session is None:
            self._owned_session = ClientSession()
        return self._owned_session


@dataclass
class ExecWebSocketStream:
    session_id: str
    ws: Any

    async def __aenter__(self) -> "ExecWebSocketStream":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def write_stdin(self, data: bytes) -> None:
        await self.ws.send_bytes(data)

    async def close_stdin(self) -> None:
        await self.ws.send_json({"type": "close_stdin"})

    async def close(self) -> None:
        await self.ws.close()

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        async for message in self.ws:
            if message.type == WSMsgType.BINARY:
                if not message.data:
                    continue
                stream_id = message.data[0]
                yield {
                    "type": STREAM_NAMES.get(stream_id, "unknown"),
                    "data": message.data[1:],
                }
            elif message.type == WSMsgType.TEXT:
                payload = message.json()
                if isinstance(payload, dict):
                    yield payload
                    if payload.get("type") == "exit":
                        return
            elif message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED}:
                return
            elif message.type == WSMsgType.ERROR:
                raise AsyncGatewayError(f"websocket failed: {self.ws.exception()}")


@dataclass
class TcpWebSocketStream:
    ws: Any

    async def __aenter__(self) -> "TcpWebSocketStream":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def write(self, data: bytes) -> None:
        await self.ws.send_bytes(data)

    async def close(self) -> None:
        await self.ws.close()

    async def chunks(self) -> AsyncIterator[bytes]:
        async for message in self.ws:
            if message.type == WSMsgType.BINARY:
                yield message.data
            elif message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED}:
                return
            elif message.type == WSMsgType.ERROR:
                raise AsyncGatewayError(f"websocket failed: {self.ws.exception()}")
