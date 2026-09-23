"""Buffered worker RPCs on one asynchronous I/O loop.

Public HTTP handlers remain synchronous. Only framed, bounded RPC responses use
this transport; file streams retain their streaming transport. No RPC is replayed
by this adapter, and authenticated redirects are always returned to the caller.
"""
from __future__ import annotations

import asyncio
import atexit
from concurrent.futures import CancelledError, Future
from io import BytesIO
import os
from threading import Lock, Thread
from typing import Mapping
from urllib.error import URLError

import aiohttp
from urllib3.exceptions import EmptyPoolError


class BufferedNodeResponse(BytesIO):
    def __init__(self, status: int, headers: Mapping[str, str], body: bytes):
        super().__init__(body)
        self.status = status
        self.headers = headers


class AsyncNodeHttpPool:
    def __init__(self, *, control_connections: int = 128, poll_connections: int = 256):
        self._limits = (control_connections, poll_connections)
        self._guard = Lock()
        self._pid = os.getpid()
        self._loop = None
        self._thread = None
        self._clients = ()
        self._closed = False
        self._pending = set()

    def _start(self):
        # Publication is guarded so simultaneous first requests share one loop.
        with self._guard:
            if self._closed or self._pid != os.getpid():
                raise URLError('worker RPC transport is closed or inherited across fork')
            if self._loop is None:
                ready = Future()
                loop = asyncio.new_event_loop()
                thread = Thread(target=self._run, args=(loop, ready),
                                name='node-http-io', daemon=True)
                thread.start()
                ready.result()
                self._loop, self._thread = loop, thread
            return self._loop

    def _run(self, loop, ready):
        asyncio.set_event_loop(loop)
        async def initialize():
            clients = []
            try:
                for limit in self._limits:
                    trace = aiohttp.TraceConfig()
                    async def sent(_session, context, _params):
                        state = context.trace_request_ctx
                        if state["sent"]:
                            raise OSError("worker RPC reconnect replay refused")
                        state["sent"] = True
                    async def queued(_session, context, _params):
                        context.trace_request_ctx["queued"] = True
                    async def acquired(_session, context, _params):
                        context.trace_request_ctx["queued"] = False
                    trace.on_request_headers_sent.append(sent)
                    trace.on_connection_queued_start.append(queued)
                    trace.on_connection_queued_end.append(acquired)
                    clients.append(aiohttp.ClientSession(
                        connector=aiohttp.TCPConnector(limit=0, limit_per_host=limit,
                                                       use_dns_cache=False),
                        auto_decompress=False,
                        skip_auto_headers={'Accept-Encoding', 'Content-Type'},
                        trust_env=False, trace_configs=[trace],
                    ))
                self._clients = tuple(clients)
            except BaseException:
                for client in clients:
                    await client.close()
                raise
        try:
            loop.run_until_complete(initialize())
            ready.set_result(None)
            loop.run_forever()
        except BaseException as exc:
            if not ready.done():
                ready.set_exception(exc)
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(self._close_clients())
            loop.close()

    async def _close_clients(self):
        for client in self._clients:
            await client.close()

    async def _request(self, method, url, headers, body, timeout, connect_timeout,
                       response_limit, event_poll):
        client = self._clients[int(event_poll)]
        # The public tracing hook fences any library reconnect retry before
        # headers are sent twice, including nominally idempotent PUT/DELETE.
        # An ambiguous failure stays ambiguous to the gateway.
        state = {"sent": False, "queued": False}
        try:
            async with client.request(
                method, url, headers=headers, data=body, allow_redirects=False,
                trace_request_ctx=state,
                timeout=aiohttp.ClientTimeout(total=None, connect=connect_timeout,
                                               sock_connect=connect_timeout, sock_read=timeout),
            ) as response:
                chunks = []
                size = 0
                async for chunk in response.content.iter_chunked(64 * 1024):
                    # Retain one sentinel byte beyond the existing gateway bound so
                    # its normal oversized-response handler preserves the contract.
                    chunk = chunk[:response_limit + 1 - size]
                    chunks.append(chunk)
                    size += len(chunk)
                    if size > response_limit:
                        break
                return BufferedNodeResponse(response.status, response.headers.copy(), b''.join(chunks))
        except (asyncio.TimeoutError, TimeoutError) as exc:
            if state["queued"] and not state["sent"]:
                raise EmptyPoolError(None, "worker RPC connection admission timed out before dispatch") from exc
            raise

    def request(self, method: str, url: str, *, headers: Mapping[str, str],
                body: bytes | None, timeout: float, connect_timeout: float,
                response_limit: int, event_poll: bool = False) -> BufferedNodeResponse:
        loop = self._start()
        coroutine = self._request(method, url, headers, body, timeout, connect_timeout,
                                  response_limit, event_poll)
        try:
            with self._guard:
                if self._closed:
                    raise URLError("worker RPC transport is closed")
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
                self._pending.add(future)
        except BaseException:
            coroutine.close()
            raise
        try:
            return future.result()
        except (asyncio.TimeoutError, TimeoutError) as exc:
            raise URLError(TimeoutError(str(exc))) from exc
        except aiohttp.ClientConnectorError as exc:
            raise URLError(exc.os_error) from exc
        except (aiohttp.ClientError, OSError, CancelledError, EmptyPoolError) as exc:
            raise URLError(exc) from exc
        finally:
            with self._guard:
                self._pending.discard(future)

    def close(self):
        with self._guard:
            if self._closed:
                return
            self._closed = True
            loop, thread = self._loop, self._thread
            pending = tuple(self._pending)
        for future in pending:
            future.cancel()
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5)


node_http_pool = AsyncNodeHttpPool()
atexit.register(node_http_pool.close)
