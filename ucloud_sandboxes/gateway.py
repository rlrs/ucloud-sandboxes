from __future__ import annotations

import asyncio
from collections.abc import Sequence
import json
from typing import Any, AsyncIterator
from urllib import error, request
from urllib.parse import quote

from .sandbox import FORK_REQUEST_TIMEOUT_SECONDS, MAX_FORK_FANOUT
from .sandbox_exec import SandboxExecSpec


MAX_GATEWAY_RESPONSE_BYTES = 16 * 1024 * 1024


class _RejectNodeRedirects(request.HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        return None


class GatewayError(RuntimeError):
    pass


def _fork_success_error(
    response: dict[str, Any],
    *,
    source_sandbox_id: str,
    requested_ids: tuple[str, ...],
    batch: bool,
) -> str | None:
    if not requested_ids or any(not sandbox_id for sandbox_id in requested_ids):
        return "fork request requires a non-empty sandbox id"
    if response.get("intent_persisted") is not True:
        return "control plane returned a fork success without durable intent"
    if not isinstance(response.get("timings"), dict):
        return "control plane returned a fork success without timings"
    records_raw = response.get("sandboxes") if batch else [response.get("sandbox")]
    forks_raw = response.get("forks") if batch else [response.get("fork")]
    if (
        not isinstance(records_raw, list)
        or not isinstance(forks_raw, list)
        or len(records_raw) != len(requested_ids)
        or len(forks_raw) != len(requested_ids)
        or not all(isinstance(item, dict) for item in records_raw)
        or not all(isinstance(item, dict) for item in forks_raw)
    ):
        return "control plane returned an invalid fork response shape"

    checkpoint_ids: set[str] = set()
    fork_nonces: set[str] = set()
    for requested_id, record, fork in zip(
        requested_ids, records_raw, forks_raw, strict=True
    ):
        record_id = str(record.get("id") or record.get("sandbox_id") or "")
        checkpoint_id = str(record.get("checkpoint_id") or "")
        fork_nonce = str(record.get("fork_nonce") or "")
        commands = fork.get("commands")
        if (
            record_id != requested_id
            or str(record.get("state") or "") != "running"
            or str(record.get("creation_kind") or "") != "restore"
            or str(record.get("source_sandbox_id") or "") != source_sandbox_id
            or not checkpoint_id
            or len(fork_nonce) != 64
            or any(character not in "0123456789abcdef" for character in fork_nonce)
            or str(fork.get("checkpoint_id") or "") != checkpoint_id
            or fork.get("restored") is not True
            or not isinstance(commands, list)
            or any(
                not isinstance(command, list)
                or any(not isinstance(argument, str) for argument in command)
                for command in commands
            )
            or (
                batch
                and str(fork.get("sandbox_id") or "") != requested_id
            )
        ):
            return "control plane returned inconsistent fork confirmation"
        checkpoint_ids.add(checkpoint_id)
        fork_nonces.add(fork_nonce)
    if batch and (len(checkpoint_ids) != 1 or len(fork_nonces) != 1):
        return "control plane returned children from different fork instants"
    return None


class NodeGatewayClient:
    """Client for gateway APIs (the historical class name is retained).

    Fork methods must point at the public control plane: only it owns route
    generation and the fenced node-operation envelope. Other methods remain
    usable with a directly addressed node agent.
    """

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

    async def fork_sandbox(
        self,
        source_sandbox_id: str,
        sandbox: dict[str, Any],
        *,
        timeout_seconds: float = FORK_REQUEST_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        """Fork one sandbox through the public control plane."""
        payload = {"sandbox": dict(sandbox)}
        response = await self._request_json(
            "POST",
            self._fork_path(source_sandbox_id),
            payload=payload,
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
            raise GatewayError(error_message)
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
        response = await self._request_json(
            "POST",
            self._fork_path(source_sandbox_id),
            payload=payload,
            timeout_seconds=timeout_seconds,
        )
        error_message = _fork_success_error(
            response,
            source_sandbox_id=source_sandbox_id,
            requested_ids=tuple(str(item.get("id") or "") for item in requested),
            batch=True,
        )
        if error_message is not None:
            raise GatewayError(error_message)
        return response

    @staticmethod
    def _fork_path(source_sandbox_id: str) -> str:
        return f"/v1/sandboxes/{quote(source_sandbox_id, safe='')}/forks"

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
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._request_json_sync,
            method,
            path,
            payload,
            timeout_seconds,
        )

    def _request_json_sync(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        timeout_seconds: float | None,
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
                timeout=(
                    self.timeout_seconds
                    if timeout_seconds is None
                    else timeout_seconds
                ),
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
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
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
