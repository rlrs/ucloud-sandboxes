"""Transfer authorized, projection-free RPCs to asynchronous response I/O.

The gateway parser, authentication and authoritative route resolution finish
before handoff. No route is reinterpreted here and no request is replayed. The HTTP
server transfers the original socket only after its handler has closed its
reader/writer wrappers; the I/O loop then owns shutdown and close.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future, wait
from http import HTTPStatus
import socket
from threading import Lock
from urllib.error import URLError

from .node_http_async import node_http_pool


class AsyncGatewayResponses:
    def __init__(
        self,
        *,
        response_policy,
        response_limit,
        timeout,
        connect_timeout,
        pool=node_http_pool,
    ):
        self.pool = pool
        self.response_policy = response_policy
        self.response_limit = response_limit
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self._guard = Lock()
        self._pending = {}
        self._closed = False

    def start(self, client_socket, *, url, headers, trace_headers, telemetry,
              method="GET", body=None, event_poll=True, release=None):
        # Takes ownership even when submission fails; the caller must not close.
        owned = client_socket
        completion = Future()
        # This is a cleanup receipt, not a cancellable work future. Only actual
        # socket/task cleanup may finish it and return an owned upload lease.
        completion.set_running_or_notify_cancel()
        if release is not None:
            completion.add_done_callback(lambda _done: release())
        try:
            owned.setblocking(False)
            with self._guard:
                if self._closed:
                    raise RuntimeError("gateway response owner is closed")
                self._pending[completion] = None
            # Do not retain the response registry lock while waking another
            # thread. Close may race this handoff: the loop callback observes
            # _closed and completes the already-registered ownership receipt.
            self.pool.call_soon(
                lambda: self._start_on_loop(
                    completion,
                    owned,
                    url=url,
                    headers=headers,
                    trace_headers=trace_headers,
                    telemetry=telemetry,
                    method=method,
                    body=body,
                    event_poll=event_poll,
                )
            )
            return completion
        except BaseException:
            with self._guard:
                self._pending.pop(completion, None)
            owned.close()  # Ownership was never transferred to the loop.
            # close() may already be waiting on this receipt after registration
            # but before the failed handoff. Socket cleanup fulfils it too.
            completion.set_result(None)
            raise

    def _start_on_loop(self, completion, owned, **kwargs):
        with self._guard:
            closed = self._closed
            if closed:
                self._pending.pop(completion, None)
            else:
                task = asyncio.create_task(self._respond(owned, **kwargs))
                self._pending[completion] = task
        if closed:
            owned.close()
            # Future callbacks may reenter close(); never invoke under guard.
            completion.set_result(None)
            return

        def completed(task):
            # Task completion implies sock_sendall has unregistered its writer.
            # Close only here on the owner loop, never on a cancellation caller.
            owned.close()
            with self._guard:
                self._pending.pop(completion, None)
            if not task.cancelled():
                task.exception()
            if not completion.done():
                completion.set_result(None)

        task.add_done_callback(completed)

    async def _respond(self, owned, *, url, headers, trace_headers, telemetry,
                       method, body, event_poll):
        try:
            operation = "gateway.exec_events.response" if event_poll else "gateway.file_upload.response"
            with telemetry.span(
                operation,
                metric_operation=operation,
                parent_context=telemetry.extracted_context(trace_headers),
            ) as span:
                try:
                    response = await self.pool.request_async(
                        method,
                        url,
                        headers=headers,
                        body=body,
                        timeout=self.timeout,
                        connect_timeout=self.connect_timeout,
                        response_limit=self.response_limit,
                        event_poll=event_poll,
                    )
                    result = self.response_policy(response, None)
                except URLError as exc:
                    result = self.response_policy(None, exc.reason)
                status, response_headers, body = result
                if int(status) >= 500:
                    span.status = "error"
                span.set_attribute("http.response.status_code", int(status))
                span.set_attribute("http.response.body.size", len(body))
                data = encode_response(status, response_headers, body, trace_headers)
                # A slow/disconnected reader owns no request thread. The deadline
                # also bounds retained response memory and its socket lifetime.
                await asyncio.wait_for(
                    asyncio.get_running_loop().sock_sendall(owned, data), self.timeout
                )
                try:
                    owned.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
        except (ConnectionError, OSError, asyncio.TimeoutError):
            pass  # Peer/transport ended; there is no safe alternate response.

    def _cancel_on_loop(self):
        with self._guard:
            tasks = tuple(self._pending.values())
        for task in tasks:
            if task is not None:
                task.cancel()

    def close(self):
        with self._guard:
            self._closed = True
            pending = tuple(self._pending)
        if pending:
            try:
                self.pool.call_soon(self._cancel_on_loop)
            except URLError:
                # Pool shutdown itself cancels/drains all tasks on its loop.
                pass
            _done, unfinished = wait(pending, timeout=5)
            if unfinished:
                raise RuntimeError("gateway response cleanup did not finish")


def encode_response(status, headers, body, trace_headers):
    try:
        reason = HTTPStatus(status).phrase
    except ValueError:
        reason = "Response"
    lines = [f"HTTP/1.1 {int(status)} {reason}\r\n"]
    retained = {
        key: value
        for key, value in headers.items()
        if key.lower() not in {"connection", "transfer-encoding", "content-length"}
    }
    retained.update(trace_headers)
    for key, value in retained.items():
        if any(ch in str(key) or ch in str(value) for ch in ("\r", "\n")):
            raise ValueError("invalid upstream response header")
        lines.append(f"{key}: {value}\r\n")
    lines.extend((f"Content-Length: {len(body)}\r\n", "Connection: close\r\n", "\r\n"))
    return "".join(lines).encode("latin-1") + body
