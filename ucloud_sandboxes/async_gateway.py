from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator

from aiohttp import ClientSession, WSMsgType

from .async_exec import STREAM_NAMES
from .sandbox_exec import SandboxExecSpec


class AsyncGatewayError(RuntimeError):
    pass


class AsyncNodeGatewayClient:
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
