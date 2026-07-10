from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator
from urllib import error, request

from .sandbox_exec import SandboxExecSpec


MAX_GATEWAY_RESPONSE_BYTES = 16 * 1024 * 1024


class _RejectNodeRedirects(request.HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        return None


class GatewayError(RuntimeError):
    pass


class NodeGatewayClient:
    """Async-capable client for the VM node-agent sandbox routing API."""

    def __init__(
        self,
        node_url: str,
        *,
        timeout_seconds: float = 30.0,
        node_control_bearer_token: str | None = None,
    ) -> None:
        if (
            node_control_bearer_token is not None
            and not node_control_bearer_token.strip()
        ):
            raise ValueError("node control bearer token cannot be empty")
        self.node_url = node_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._node_control_headers = (
            {"Authorization": f"Bearer {node_control_bearer_token}"}
            if node_control_bearer_token is not None
            else {}
        )

    async def start_exec(
        self,
        sandbox_id: str,
        spec: SandboxExecSpec,
    ) -> "RemoteExecHandle":
        payload = spec.to_dict()
        payload.pop("sandbox_id", None)
        response = await self._request_json(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/exec",
            payload=payload,
        )
        session = response.get("session")
        if not isinstance(session, dict) or not isinstance(session.get("id"), str):
            raise GatewayError("node-agent returned an invalid exec session payload.")
        return RemoteExecHandle(self, session["id"])

    async def get_exec_session(self, session_id: str) -> dict[str, Any]:
        return await self._request_json("GET", f"/v1/exec/{session_id}")

    async def read_exec_events(
        self,
        session_id: str,
        *,
        after: int = 0,
        limit: int = 100,
        wait_seconds: float = 0.0,
    ) -> dict[str, Any]:
        query = (
            f"?after={max(0, after)}&limit={max(1, limit)}"
            f"&wait_seconds={max(0.0, wait_seconds):g}"
        )
        return await self._request_json("GET", f"/v1/exec/{session_id}/events{query}")

    async def write_exec_stdin(
        self,
        session_id: str,
        data: str,
        *,
        eof: bool = False,
    ) -> dict[str, Any]:
        return await self._request_json(
            "POST",
            f"/v1/exec/{session_id}/stdin",
            payload={"data": data, "eof": eof},
        )

    async def close_exec_stdin(self, session_id: str) -> dict[str, Any]:
        return await self._request_json("POST", f"/v1/exec/{session_id}/close-stdin")

    async def get_ssh_target(self, sandbox_id: str) -> dict[str, Any]:
        return await self._request_json("GET", f"/v1/sandboxes/{sandbox_id}/ssh")

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._request_json_sync,
            method,
            path,
            payload,
        )

    def _request_json_sync(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = dict(self._node_control_headers)
        if payload is not None:
            headers["Content-Type"] = "application/json"
        req = request.Request(
            self.node_url + path,
            data=body,
            method=method,
            headers=headers,
        )
        try:
            with request.build_opener(_RejectNodeRedirects()).open(
                req,
                timeout=self.timeout_seconds,
            ) as response:
                raw_bytes = response.read(MAX_GATEWAY_RESPONSE_BYTES + 1)
                if len(raw_bytes) > MAX_GATEWAY_RESPONSE_BYTES:
                    raise GatewayError("node-agent response exceeds the 16 MiB limit")
                raw = raw_bytes.decode("utf-8")
                decoded = json.loads(raw) if raw else {}
        except error.HTTPError as exc:
            raw_bytes = exc.read(MAX_GATEWAY_RESPONSE_BYTES + 1)
            if len(raw_bytes) > MAX_GATEWAY_RESPONSE_BYTES:
                raise GatewayError(
                    "node-agent error response exceeds the 16 MiB limit"
                ) from exc
            raw = raw_bytes.decode("utf-8", errors="replace")
            try:
                decoded = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                decoded = {"error": raw}
            raise GatewayError(f"node-agent request failed ({exc.code}): {decoded}") from exc
        except (OSError, json.JSONDecodeError) as exc:
            raise GatewayError(f"node-agent request failed: {exc}") from exc
        if not isinstance(decoded, dict):
            raise GatewayError("node-agent returned a non-object JSON payload.")
        return decoded


class RemoteExecHandle:
    def __init__(self, client: NodeGatewayClient, session_id: str) -> None:
        self.client = client
        self.session_id = session_id
        self._last_sequence = 0

    async def write_stdin(self, data: str) -> None:
        await self.client.write_exec_stdin(self.session_id, data)

    async def close_stdin(self) -> None:
        await self.client.close_exec_stdin(self.session_id)

    async def events(
        self,
        *,
        wait_seconds: float = 30.0,
        limit: int = 100,
    ) -> AsyncIterator[dict[str, Any]]:
        while True:
            payload = await self.client.read_exec_events(
                self.session_id,
                after=self._last_sequence,
                limit=limit,
                wait_seconds=wait_seconds,
            )
            raw_events = payload.get("events")
            events = raw_events if isinstance(raw_events, list) else []
            for event in events:
                if not isinstance(event, dict):
                    continue
                sequence = int(event.get("sequence") or 0)
                self._last_sequence = max(self._last_sequence, sequence)
                yield event
            session = payload.get("session")
            if (
                isinstance(session, dict)
                and session.get("status") in {"exited", "failed"}
                and not events
            ):
                return
