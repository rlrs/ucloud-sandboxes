from __future__ import annotations

import asyncio
import base64
from collections import deque
from contextlib import asynccontextmanager
import heapq
from pathlib import Path
import sqlite3
import tempfile
from threading import Event, Timer
import time
from typing import Any, AsyncIterator
import unittest

from aiohttp import ClientSession, web

from ucloud_sandboxes.deployment import package_version
from ucloud_sandboxes.model_relay import (
    ModelRelayState,
    RelayWorkerResponse,
    create_model_relay_app,
)


class RelayHarness:
    def __init__(
        self,
        base_url: str,
        client: ClientSession,
    ) -> None:
        self.base_url = base_url
        self.client = client

    async def request(
        self,
        method: str,
        path: str,
        *,
        expected: int | None = None,
        **kwargs: Any,
    ) -> tuple[int, Any]:
        async with self.client.request(
            method,
            self.base_url + path,
            **kwargs,
        ) as response:
            try:
                payload = await response.json(content_type=None)
            except ValueError:
                payload = await response.text()
            if expected is not None and response.status != expected:
                raise AssertionError(
                    f"{method} {path} returned {response.status}, expected "
                    f"{expected}: {payload!r}"
                )
            return response.status, payload

    async def request_bytes(
        self,
        method: str,
        path: str,
        *,
        expected: int | None = None,
        **kwargs: Any,
    ) -> tuple[int, bytes, dict[str, str]]:
        async with self.client.request(
            method,
            self.base_url + path,
            **kwargs,
        ) as response:
            payload = await response.read()
            if expected is not None and response.status != expected:
                raise AssertionError(
                    f"{method} {path} returned {response.status}, expected "
                    f"{expected}: {payload!r}"
                )
            return response.status, payload, dict(response.headers)

    async def register(
        self,
        rollout_id: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> str:
        _status, payload = await self.request(
            "POST",
            "/v1/relay/rollouts",
            expected=201,
            headers=headers,
            json={"rollout_id": rollout_id},
        )
        return str(payload["rollout"]["registration_token"])

    async def poll(
        self,
        rollout_id: str,
        registration_token: str,
        **params: Any,
    ) -> dict[str, Any]:
        _status, payload = await self.request(
            "GET",
            "/worker/poll",
            expected=200,
            params={
                "rollout_id": rollout_id,
                "registration_token": registration_token,
                **params,
            },
        )
        return payload

    async def respond(
        self,
        request: dict[str, Any],
        registration_token: str,
        body: object,
        *,
        expected: int = 200,
    ) -> dict[str, Any] | str:
        _status, payload = await self.request(
            "POST",
            "/worker/respond",
            expected=expected,
            json={
                "request_id": request["request_id"],
                "registration_token": registration_token,
                "lease_id": request["lease_id"],
                "response": body,
            },
        )
        return payload

    async def respond_bytes(
        self,
        request: dict[str, Any],
        registration_token: str,
        body: bytes,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        auth_headers: dict[str, str] | None = None,
        expected: int = 200,
    ) -> dict[str, Any] | str:
        _status, payload = await self.request(
            "POST",
            "/worker/respond",
            expected=expected,
            headers=auth_headers,
            json={
                "request_id": request["request_id"],
                "registration_token": registration_token,
                "lease_id": request["lease_id"],
                "body_base64": base64.b64encode(body).decode("ascii"),
                "status": status,
                "headers": headers or {},
            },
        )
        return payload

    async def stats(self) -> dict[str, Any]:
        _status, payload = await self.request(
            "GET",
            "/v1/relay/stats",
            expected=200,
        )
        return payload

    async def model_call(
        self,
        rollout_id: str,
        *,
        path: str | None = None,
        headers: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
    ) -> tuple[int, Any]:
        return await self.request(
            "POST",
            path or f"/rollouts/{rollout_id}/v1/chat/completions",
            headers=headers,
            json=body or {"model": "m", "messages": []},
        )


@asynccontextmanager
async def relay_app(**kwargs: Any) -> AsyncIterator[RelayHarness]:
    runner = web.AppRunner(create_model_relay_app(**kwargs))
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    sockets = site._server.sockets if site._server else []
    base_url = f"http://127.0.0.1:{sockets[0].getsockname()[1]}"
    try:
        async with ClientSession() as client:
            yield RelayHarness(base_url, client)
    finally:
        await runner.cleanup()


async def enqueue_and_poll(
    state: ModelRelayState,
    rollout_id: str,
    registration_token: str,
    *,
    lease_seconds: float = 30,
    worker_id: str | None = None,
):
    request = await state.enqueue(
        rollout_id=rollout_id,
        endpoint="/v1/responses",
        body={"model": "m"},
        headers={},
    )
    delivery = (
        await state.poll(
            rollout_id=rollout_id,
            registration_token=registration_token,
            timeout_seconds=0,
            lease_seconds=lease_seconds,
            worker_id=worker_id,
        )
    )[0]
    return request, delivery


class ModelRelayTests(unittest.IsolatedAsyncioTestCase):
    async def test_registration_metadata_rejects_aliases_and_coercion(self) -> None:
        state = ModelRelayState()
        for metadata in (
            {"sandboxId": "sandbox-1", "sandbox_generation": 1},
            {"sandbox_id": "sandbox-1", "sandboxGeneration": 1},
            {"sandbox_id": "sandbox-1", "sandbox_generation": "1"},
        ):
            with self.subTest(metadata=metadata), self.assertRaises(
                web.HTTPBadRequest
            ):
                await state.register_rollout("strict-metadata", metadata)

    async def test_completed_responses_obey_byte_budget_and_drop_request_bodies(
        self,
    ) -> None:
        state = ModelRelayState(
            max_completed_requests=10,
            max_completed_bytes=1200,
        )
        await state.register_rollout("completed-bytes")
        completed_requests = []
        for index in range(3):
            request = await state.enqueue(
                rollout_id="completed-bytes",
                endpoint="/v1/responses",
                body={"request": "y" * 400, "index": index},
                headers={"Content-Type": "application/json"},
            )
            completed_requests.append(request)
            await state.cancel_request(
                request_id=request.request_id,
                response=RelayWorkerResponse(499, b"x" * 700),
            )

        stats = await state.stats()

        self.assertEqual(stats["completed_retained"], 1)
        self.assertLessEqual(stats["completed_bytes"], 1200)
        self.assertTrue(
            all(
                request.body is None
                and request.body_bytes == b""
                and request.headers == {}
                and request.payload_bytes == 0
                for request in completed_requests
            )
        )

    async def test_restart_prunes_completed_byte_budget_before_restore(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "relay.sqlite3"
            state = ModelRelayState(
                state_path=state_path,
                max_completed_requests=10,
                max_completed_bytes=10_000,
            )
            await state.register_rollout("restore-budget")
            for index in range(3):
                request = await state.enqueue(
                    rollout_id="restore-budget",
                    endpoint="/v1/responses",
                    body={"index": index},
                    headers={},
                )
                await state.cancel_request(
                    request_id=request.request_id,
                    response=RelayWorkerResponse(499, b"x" * 700),
                )
            await state.aclose()

            restored = ModelRelayState(
                state_path=state_path,
                max_completed_requests=10,
                max_completed_bytes=1200,
            )
            stats = await restored.stats()
            await restored.aclose()
            with sqlite3.connect(state_path) as connection:
                completed_rows = connection.execute(
                    "SELECT count(*) FROM relay_requests WHERE state = 'completed'"
                ).fetchone()[0]

        self.assertEqual(stats["completed_retained"], 1)
        self.assertLessEqual(stats["completed_bytes"], 1200)
        self.assertEqual(completed_rows, 1)

    async def test_sqlite_writes_do_not_block_the_event_loop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = ModelRelayState(
                state_path=Path(directory) / "relay.sqlite3",
            )
            await state.register_rollout("nonblocking-store")
            store = state._store  # noqa: SLF001
            assert store is not None
            original_save = store.save_request
            started = Event()
            release = Event()

            def blocking_save(request) -> None:
                started.set()
                release.wait()
                original_save(request)

            store.save_request = blocking_save  # type: ignore[method-assign]
            safety_release = Timer(0.5, release.set)
            safety_release.start()
            started_at = time.monotonic()
            ticked_at: float | None = None

            async def ticker() -> None:
                nonlocal ticked_at
                await asyncio.sleep(0.05)
                ticked_at = time.monotonic()

            ticker_task = asyncio.create_task(ticker())
            enqueue_task = asyncio.create_task(
                state.enqueue(
                    rollout_id="nonblocking-store",
                    endpoint="/v1/responses",
                    body={"model": "m"},
                    headers={},
                )
            )
            await asyncio.to_thread(started.wait)
            await ticker_task
            release.set()
            await enqueue_task
            safety_release.cancel()
            await state.aclose()

        assert ticked_at is not None
        self.assertLess(ticked_at - started_at, 0.2)

    async def test_enqueue_save_failure_does_not_publish_a_ghost_request(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = ModelRelayState(state_path=Path(directory) / "relay.sqlite3")
            await state.register_rollout("enqueue-failure")
            store = state._store  # noqa: SLF001
            assert store is not None
            original_save = store.save_request

            def fail_save(_request) -> None:
                raise sqlite3.OperationalError("injected enqueue failure")

            store.save_request = fail_save  # type: ignore[method-assign]
            with self.assertRaisesRegex(sqlite3.OperationalError, "injected"):
                await state.enqueue(
                    rollout_id="enqueue-failure",
                    endpoint="/v1/responses",
                    body={"payload": True},
                    headers={},
                    idempotency_key="same-request",
                )

            self.assertEqual(state._requests, {})  # noqa: SLF001
            self.assertEqual(len(state._pending["enqueue-failure"]), 0)  # noqa: SLF001
            self.assertEqual(state._idempotency, {})  # noqa: SLF001
            self.assertEqual(state._inflight_bytes, 0)  # noqa: SLF001
            with store._lock:  # noqa: SLF001
                row_count = store._connection.execute(  # noqa: SLF001
                    "SELECT count(*) FROM relay_requests"
                ).fetchone()[0]
            self.assertEqual(row_count, 0)

            store.save_request = original_save  # type: ignore[method-assign]
            request = await state.enqueue(
                rollout_id="enqueue-failure",
                endpoint="/v1/responses",
                body={"payload": True},
                headers={},
                idempotency_key="same-request",
            )
            self.assertIs(state._requests[request.request_id], request)  # noqa: SLF001
            await state.aclose()

    async def test_registration_save_failure_does_not_publish_registration(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = ModelRelayState(state_path=Path(directory) / "relay.sqlite3")
            await state.stats()
            store = state._store  # noqa: SLF001
            assert store is not None

            def fail_save(_record) -> None:
                raise sqlite3.OperationalError("injected registration failure")

            store.save_rollout = fail_save  # type: ignore[method-assign]
            with self.assertRaisesRegex(sqlite3.OperationalError, "injected"):
                await state.register_rollout("registration-failure")
            self.assertNotIn("registration-failure", state._rollouts)  # noqa: SLF001
            self.assertNotIn("registration-failure", state._pending)  # noqa: SLF001
            await state.aclose()

    async def test_enqueue_cancellation_after_commit_publishes_memory_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = ModelRelayState(state_path=Path(directory) / "relay.sqlite3")
            await state.register_rollout("enqueue-cancel")
            store = state._store  # noqa: SLF001
            assert store is not None
            original_save = store.save_request
            committed = Event()
            release = Event()

            def commit_then_block(request) -> None:
                original_save(request)
                committed.set()
                release.wait(1)

            store.save_request = commit_then_block  # type: ignore[method-assign]
            enqueue_task = asyncio.create_task(
                state.enqueue(
                    rollout_id="enqueue-cancel",
                    endpoint="/v1/responses",
                    body={"payload": True},
                    headers={},
                    idempotency_key="cancel-request",
                )
            )
            self.assertTrue(await asyncio.to_thread(committed.wait, 1))
            enqueue_task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await enqueue_task

            self.assertEqual(len(state._requests), 1)  # noqa: SLF001
            request = next(iter(state._requests.values()))  # noqa: SLF001
            self.assertEqual(
                state._idempotency[
                    (
                        request.rollout_id,
                        request.registration_token,
                        "cancel-request",
                    )
                ],
                request.request_id,
            )
            self.assertEqual(len(state._pending["enqueue-cancel"]), 1)  # noqa: SLF001
            with store._lock:  # noqa: SLF001
                row_state = store._connection.execute(  # noqa: SLF001
                    "SELECT state FROM relay_requests WHERE request_id = ?",
                    (request.request_id,),
                ).fetchone()[0]
            self.assertEqual(row_state, "pending")
            await state.aclose()

    async def test_terminal_commit_failure_never_publishes_in_memory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = ModelRelayState(state_path=Path(directory) / "relay.sqlite3")
            token = str(
                (await state.register_rollout("commit-failure"))["registration_token"]
            )
            request, delivery = await enqueue_and_poll(
                state,
                "commit-failure",
                token,
            )
            store = state._store  # noqa: SLF001
            assert store is not None
            original_commit = store.commit_request_batch

            def fail_commit(snapshots, evicted) -> None:
                del evicted
                snapshot = snapshots[0]
                self.assertEqual(snapshot.state, "completed")
                self.assertEqual(request.state, "leased")
                self.assertFalse(request.future.done())
                raise sqlite3.OperationalError("injected commit failure")

            store.commit_request_batch = fail_commit  # type: ignore[method-assign]
            with self.assertRaisesRegex(sqlite3.OperationalError, "injected"):
                await state.respond(
                    request_id=delivery.request_id,
                    registration_token=token,
                    lease_id=delivery.lease_id,
                    response=RelayWorkerResponse(200, {"ok": True}),
                )

            self.assertIs(state._requests[request.request_id], request)  # noqa: SLF001
            self.assertNotIn(request.request_id, state._completed)  # noqa: SLF001
            self.assertEqual(request.state, "leased")
            self.assertFalse(request.future.done())
            with store._lock:  # noqa: SLF001
                row_state = store._connection.execute(  # noqa: SLF001
                    "SELECT state FROM relay_requests WHERE request_id = ?",
                    (request.request_id,),
                ).fetchone()[0]
            self.assertEqual(row_state, "leased")

            store.commit_request_batch = original_commit  # type: ignore[method-assign]
            await state.respond(
                request_id=delivery.request_id,
                registration_token=token,
                lease_id=delivery.lease_id,
                response=RelayWorkerResponse(200, {"ok": True}),
            )
            self.assertTrue(request.future.done())
            await state.aclose()

    async def test_cancellation_after_terminal_commit_restores_memory_invariants(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = ModelRelayState(state_path=Path(directory) / "relay.sqlite3")
            token = str(
                (await state.register_rollout("commit-cancel"))["registration_token"]
            )
            request, delivery = await enqueue_and_poll(state, "commit-cancel", token)
            store = state._store  # noqa: SLF001
            assert store is not None
            original_commit = store.commit_request_batch
            committed = Event()
            release = Event()

            def commit_then_block(snapshot, evicted) -> None:
                original_commit(snapshot, evicted)
                committed.set()
                release.wait(1)

            store.commit_request_batch = commit_then_block  # type: ignore[method-assign]
            response_task = asyncio.create_task(
                state.respond(
                    request_id=delivery.request_id,
                    registration_token=token,
                    lease_id=delivery.lease_id,
                    response=RelayWorkerResponse(200, {"ok": True}),
                )
            )
            self.assertTrue(await asyncio.to_thread(committed.wait, 1))
            response_task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await response_task

            self.assertNotIn(request.request_id, state._requests)  # noqa: SLF001
            self.assertIs(state._completed[request.request_id], request)  # noqa: SLF001
            self.assertTrue(request.future.done())
            self.assertEqual(state._counters["completed"], 1)  # noqa: SLF001
            with store._lock:  # noqa: SLF001
                row_state = store._connection.execute(  # noqa: SLF001
                    "SELECT state FROM relay_requests WHERE request_id = ?",
                    (request.request_id,),
                ).fetchone()[0]
            self.assertEqual(row_state, "completed")
            await state.aclose()

    async def test_deferred_response_pin_survives_restart_until_durable_release(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "relay.sqlite3"
            state = ModelRelayState(state_path=state_path, max_completed_requests=1)
            token = str(
                (await state.register_rollout("durable-pin"))["registration_token"]
            )
            request, delivery = await enqueue_and_poll(state, "durable-pin", token)
            await state.respond(
                request_id=delivery.request_id,
                registration_token=token,
                lease_id=delivery.lease_id,
                response=RelayWorkerResponse(200, {"ok": True}),
                defer_delivery=True,
            )
            self.assertTrue(request.delivery_pending)
            self.assertFalse(request.future.done())
            await state.aclose()

            restored = ModelRelayState(
                state_path=state_path,
                max_completed_requests=1,
            )
            await restored.stats()
            recovered = restored._completed[request.request_id]  # noqa: SLF001
            self.assertTrue(recovered.delivery_pending)
            self.assertFalse(recovered.future.done())
            await restored.release_completed_response(request.request_id)
            self.assertFalse(recovered.delivery_pending)
            self.assertTrue(recovered.future.done())
            await restored.aclose()

            with sqlite3.connect(state_path) as connection:
                delivery_pending = connection.execute(
                    "SELECT delivery_pending FROM relay_requests WHERE request_id = ?",
                    (request.request_id,),
                ).fetchone()[0]
            self.assertEqual(delivery_pending, 0)

    async def test_completed_capacity_never_evicts_deferred_pins(self) -> None:
        state = ModelRelayState(max_completed_requests=1)
        token = str((await state.register_rollout("pin-capacity"))["registration_token"])
        first, first_delivery = await enqueue_and_poll(state, "pin-capacity", token)
        await state.respond(
            request_id=first_delivery.request_id,
            registration_token=token,
            lease_id=first_delivery.lease_id,
            response=RelayWorkerResponse(200, {"first": True}),
            defer_delivery=True,
        )
        second, second_delivery = await enqueue_and_poll(state, "pin-capacity", token)
        with self.assertRaises(web.HTTPServiceUnavailable):
            await state.respond(
                request_id=second_delivery.request_id,
                registration_token=token,
                lease_id=second_delivery.lease_id,
                response=RelayWorkerResponse(200, {"second": True}),
            )
        self.assertIs(state._completed[first.request_id], first)  # noqa: SLF001
        self.assertIs(state._requests[second.request_id], second)  # noqa: SLF001
        self.assertFalse(first.future.done())

        await state.release_completed_response(first.request_id)
        await state.respond(
            request_id=second_delivery.request_id,
            registration_token=token,
            lease_id=second_delivery.lease_id,
            response=RelayWorkerResponse(200, {"second": True}),
        )
        self.assertNotIn(first.request_id, state._completed)  # noqa: SLF001
        self.assertTrue(second.future.done())

    async def test_poll_wakeups_are_scoped_to_the_target_rollout(self) -> None:
        state = ModelRelayState()
        token_a = str((await state.register_rollout("wake-a"))["registration_token"])
        token_b = str((await state.register_rollout("wake-b"))["registration_token"])
        waits = 0
        original_wait = state._wait_for_rollout_wakeup  # noqa: SLF001

        async def counted_wait(wakeup, timeout_seconds) -> None:
            nonlocal waits
            waits += 1
            await original_wait(wakeup, timeout_seconds)

        state._wait_for_rollout_wakeup = counted_wait  # type: ignore[method-assign]  # noqa: SLF001
        poll_b = asyncio.create_task(
            state.poll(
                rollout_id="wake-b",
                registration_token=token_b,
                timeout_seconds=1,
            )
        )
        while waits == 0:
            await asyncio.sleep(0)
        await state.enqueue(
            rollout_id="wake-a",
            endpoint="/v1/responses",
            body={},
            headers={},
        )
        await asyncio.sleep(0.02)
        self.assertFalse(poll_b.done())
        self.assertEqual(waits, 1)

        request_b = await state.enqueue(
            rollout_id="wake-b",
            endpoint="/v1/responses",
            body={},
            headers={},
        )
        deliveries = await asyncio.wait_for(poll_b, 1)
        self.assertEqual(deliveries[0].request_id, request_b.request_id)
        await state.cancel_request(
            request_id=deliveries[0].request_id,
            response=RelayWorkerResponse(499, {}),
        )
        await state.cancel_request(
            request_id=next(iter(state._requests)),  # noqa: SLF001
            response=RelayWorkerResponse(499, {}),
        )
        del token_a

    async def test_shortened_lease_renewal_wakes_waiting_rollout_poller(self) -> None:
        state = ModelRelayState()
        token = str((await state.register_rollout("short-renew"))["registration_token"])
        request, leased = await enqueue_and_poll(
            state,
            "short-renew",
            token,
            lease_seconds=5,
        )
        waiting_poll = asyncio.create_task(
            state.poll(
                rollout_id="short-renew",
                registration_token=token,
                timeout_seconds=0.8,
                lease_seconds=1,
            )
        )
        deadline = time.monotonic() + 1
        while (
            "short-renew" not in state._rollout_wakeups  # noqa: SLF001
            and time.monotonic() < deadline
        ):
            await asyncio.sleep(0)

        await state.renew_lease(
            request_id=leased.request_id,
            registration_token=token,
            lease_id=leased.lease_id or "",
            lease_seconds=0.02,
        )
        retried = (await asyncio.wait_for(waiting_poll, 0.3))[0]
        self.assertEqual(retried.request_id, request.request_id)
        self.assertEqual(retried.delivery_count, 2)
        await state.cancel_request(
            request_id=request.request_id,
            response=RelayWorkerResponse(499, {}),
        )

    async def test_expiry_heaps_stay_bounded_after_completion_and_renewal(self) -> None:
        state = ModelRelayState(max_completed_requests=512)
        token = str((await state.register_rollout("heap-bounds"))["registration_token"])
        for _index in range(300):
            request = await state.enqueue(
                rollout_id="heap-bounds",
                endpoint="/v1/responses",
                body={},
                headers={},
            )
            await state.cancel_request(
                request_id=request.request_id,
                response=RelayWorkerResponse(499, {}),
            )
        self.assertLessEqual(len(state._request_expiry_heap), 256)  # noqa: SLF001

        _request, delivery = await enqueue_and_poll(state, "heap-bounds", token)
        for _index in range(200):
            delivery = await state.renew_lease(
                request_id=delivery.request_id,
                registration_token=token,
                lease_id=delivery.lease_id or "",
                lease_seconds=30,
            )
        self.assertLessEqual(
            len(state._lease_expiry_heaps["heap-bounds"]),  # noqa: SLF001
            66,
        )

    async def test_sqlite_poll_batch_uses_one_explicit_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = ModelRelayState(state_path=Path(directory) / "relay.sqlite3")
            token = str((await state.register_rollout("batch-tx"))["registration_token"])
            for _index in range(3):
                await state.enqueue(
                    rollout_id="batch-tx",
                    endpoint="/v1/responses",
                    body={},
                    headers={},
                )
            store = state._store  # noqa: SLF001
            assert store is not None
            statements: list[str] = []
            with store._lock:  # noqa: SLF001
                store._connection.set_trace_callback(statements.append)  # noqa: SLF001
            await state.poll(
                rollout_id="batch-tx",
                registration_token=token,
                timeout_seconds=0,
                limit=3,
            )
            with store._lock:  # noqa: SLF001
                store._connection.set_trace_callback(None)  # noqa: SLF001
            self.assertEqual(
                sum(statement.startswith("BEGIN IMMEDIATE") for statement in statements),
                1,
            )
            self.assertEqual(sum(statement == "COMMIT" for statement in statements), 1)
            await state.aclose()

    async def test_many_timeouts_use_one_terminal_transaction_and_queue_scan(
        self,
    ) -> None:
        class CountingDeque(deque):
            iterations = 0

            def __iter__(self):  # type: ignore[no-untyped-def]
                self.iterations += 1
                return super().__iter__()

        with tempfile.TemporaryDirectory() as directory:
            state = ModelRelayState(
                state_path=Path(directory) / "relay.sqlite3",
                max_completed_requests=256,
            )
            await state.register_rollout("bulk-timeout")
            requests = [
                await state.enqueue(
                    rollout_id="bulk-timeout",
                    endpoint="/v1/responses",
                    body={"index": index},
                    headers={},
                )
                for index in range(128)
            ]
            expired_at = time.time() - 1
            state._request_expiry_heap.clear()  # noqa: SLF001
            for request in requests:
                request.expires_at = expired_at
                state._request_expiry_heap.append(  # noqa: SLF001
                    (expired_at, request.request_id)
                )
            heapq.heapify(state._request_expiry_heap)  # noqa: SLF001
            queue = CountingDeque(state._pending["bulk-timeout"])  # noqa: SLF001
            state._pending["bulk-timeout"] = queue  # noqa: SLF001
            store = state._store  # noqa: SLF001
            assert store is not None
            store.save_requests(tuple(requests))
            statements: list[str] = []
            with store._lock:  # noqa: SLF001
                store._connection.set_trace_callback(statements.append)  # noqa: SLF001

            stats = await state.stats()

            with store._lock:  # noqa: SLF001
                store._connection.set_trace_callback(None)  # noqa: SLF001
            self.assertEqual(stats["inflight"], 0)
            self.assertEqual(stats["counters"]["timed_out"], 128)
            self.assertEqual(queue.iterations, 1)
            self.assertEqual(
                sum(statement.startswith("BEGIN IMMEDIATE") for statement in statements),
                1,
            )
            self.assertEqual(sum(statement == "COMMIT" for statement in statements), 1)
            self.assertTrue(all(request.future.done() for request in requests))
            await state.aclose()

    async def test_many_unregister_cancellations_use_one_terminal_transaction(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = ModelRelayState(
                state_path=Path(directory) / "relay.sqlite3",
                max_completed_requests=256,
            )
            token = str(
                (await state.register_rollout("bulk-unregister"))["registration_token"]
            )
            requests = [
                await state.enqueue(
                    rollout_id="bulk-unregister",
                    endpoint="/v1/responses",
                    body={"index": index},
                    headers={},
                )
                for index in range(128)
            ]
            store = state._store  # noqa: SLF001
            assert store is not None
            statements: list[str] = []
            with store._lock:  # noqa: SLF001
                store._connection.set_trace_callback(statements.append)  # noqa: SLF001

            self.assertTrue(
                await state.unregister_rollout(
                    "bulk-unregister",
                    registration_token=token,
                )
            )

            with store._lock:  # noqa: SLF001
                store._connection.set_trace_callback(None)  # noqa: SLF001
            self.assertEqual(state._requests, {})  # noqa: SLF001
            self.assertEqual(state._counters["unregister_canceled"], 128)  # noqa: SLF001
            self.assertEqual(
                sum(statement.startswith("BEGIN IMMEDIATE") for statement in statements),
                1,
            )
            self.assertEqual(sum(statement == "COMMIT" for statement in statements), 1)
            self.assertTrue(all(request.future.done() for request in requests))
            await state.aclose()

    async def test_startup_parse_failure_is_atomic_and_retry_does_not_duplicate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "relay.sqlite3"
            state = ModelRelayState(state_path=state_path)
            await state.register_rollout("atomic-restore")
            for index in range(2):
                await state.enqueue(
                    rollout_id="atomic-restore",
                    endpoint="/v1/responses",
                    body={"index": index},
                    headers={},
                )
            await state.aclose()

            restored = ModelRelayState(state_path=state_path)
            store = restored._store  # noqa: SLF001
            assert store is not None
            original_load = store.load_requests
            rows = original_load()
            malformed = dict(rows[1])
            malformed["delivery_pending"] = "invalid"
            store.load_requests = lambda: [rows[0], malformed]  # type: ignore[method-assign]

            with self.assertRaisesRegex(ValueError, "delivery_pending"):
                await restored.stats()
            self.assertFalse(restored._loaded)  # noqa: SLF001
            self.assertEqual(restored._rollouts, {})  # noqa: SLF001
            self.assertEqual(restored._requests, {})  # noqa: SLF001
            self.assertEqual(restored._pending, {})  # noqa: SLF001
            self.assertEqual(restored._inflight_bytes, 0)  # noqa: SLF001

            store.load_requests = original_load  # type: ignore[method-assign]
            stats = await restored.stats()
            self.assertEqual(stats["inflight"], 2)
            self.assertEqual(stats["pending"]["atomic-restore"], 2)
            self.assertEqual(stats["counters"]["restored_requests"], 2)
            self.assertEqual(
                len({request.request_id for request in restored._requests.values()}),  # noqa: SLF001
                2,
            )
            await restored.aclose()

    async def test_startup_recovery_commit_failure_keeps_live_state_empty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "relay.sqlite3"
            state = ModelRelayState(state_path=state_path)
            await state.register_rollout("recovery-commit")
            await state.enqueue(
                rollout_id="recovery-commit",
                endpoint="/v1/responses",
                body={},
                headers={},
                idempotency_key="reattach-after-restart",
                defer_idempotency_until_disconnect=True,
            )
            await state.aclose()

            restored = ModelRelayState(state_path=state_path)
            store = restored._store  # noqa: SLF001
            assert store is not None
            original_commit = store.commit_request_batch

            def fail_commit(_requests, _deleted) -> None:
                raise sqlite3.OperationalError("injected recovery commit failure")

            store.commit_request_batch = fail_commit  # type: ignore[method-assign]
            with self.assertRaisesRegex(sqlite3.OperationalError, "injected"):
                await restored.stats()
            self.assertFalse(restored._loaded)  # noqa: SLF001
            self.assertEqual(restored._requests, {})  # noqa: SLF001
            self.assertEqual(restored._idempotency, {})  # noqa: SLF001

            store.commit_request_batch = original_commit  # type: ignore[method-assign]
            stats = await restored.stats()
            self.assertEqual(stats["inflight"], 1)
            self.assertEqual(len(restored._idempotency), 1)  # noqa: SLF001
            await restored.aclose()

    async def test_startup_stranded_rows_use_one_recovery_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "relay.sqlite3"
            state = ModelRelayState(state_path=state_path)
            await state.register_rollout("stranded")
            for index in range(64):
                await state.enqueue(
                    rollout_id="stranded",
                    endpoint="/v1/responses",
                    body={"index": index},
                    headers={},
                )
            await state.aclose()
            with sqlite3.connect(state_path) as connection:
                connection.execute(
                    "DELETE FROM relay_rollouts WHERE rollout_id = ?",
                    ("stranded",),
                )

            restored = ModelRelayState(
                state_path=state_path,
                max_completed_requests=128,
            )
            store = restored._store  # noqa: SLF001
            assert store is not None
            statements: list[str] = []
            with store._lock:  # noqa: SLF001
                store._connection.set_trace_callback(statements.append)  # noqa: SLF001

            stats = await restored.stats()

            with store._lock:  # noqa: SLF001
                store._connection.set_trace_callback(None)  # noqa: SLF001
            self.assertEqual(stats["inflight"], 0)
            self.assertEqual(stats["completed_retained"], 64)
            self.assertEqual(
                sum(statement.startswith("BEGIN IMMEDIATE") for statement in statements),
                1,
            )
            self.assertEqual(sum(statement == "COMMIT" for statement in statements), 1)
            await restored.aclose()

    async def test_restore_does_not_rewrite_unchanged_request_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "relay.sqlite3"
            state = ModelRelayState(state_path=state_path)
            await state.register_rollout("restore-read-only")
            completed = await state.enqueue(
                rollout_id="restore-read-only",
                endpoint="/v1/responses",
                body={},
                headers={},
            )
            await state.cancel_request(
                request_id=completed.request_id,
                response=RelayWorkerResponse(499, {}),
            )
            await state.enqueue(
                rollout_id="restore-read-only",
                endpoint="/v1/responses",
                body={},
                headers={},
            )
            await state.aclose()

            restored = ModelRelayState(state_path=state_path)
            store = restored._store  # noqa: SLF001
            assert store is not None

            def unexpected_write(*_args, **_kwargs) -> None:
                raise AssertionError("restore rewrote an unchanged request row")

            store.save_request = unexpected_write  # type: ignore[method-assign]
            store.commit_request_batch = unexpected_write  # type: ignore[method-assign]
            stats = await restored.stats()
            self.assertEqual(stats["inflight"], 1)
            self.assertEqual(stats["completed_retained"], 1)
            await restored.aclose()

    async def test_transient_worker_disconnect_requeues_without_failing_caller(
        self,
    ) -> None:
        async with relay_app(worker_poll_timeout_seconds=1) as relay:
            token = await relay.register("transient-worker")
            caller = asyncio.create_task(relay.model_call("transient-worker"))
            first = (await relay.poll("transient-worker", token))["request"]

            _status, retry = await relay.request(
                "POST",
                "/worker/error",
                expected=200,
                json={
                    "request_id": first["request_id"],
                    "registration_token": token,
                    "lease_id": first["lease_id"],
                    "status": 502,
                    "error": "Server disconnected",
                },
            )
            second = (await relay.poll("transient-worker", token))["request"]
            await relay.respond(second, token, {"ok": True})
            caller_status, caller_body = await caller
            stats = await relay.stats()

        self.assertTrue(retry["retried"])
        self.assertEqual(second["request_id"], first["request_id"])
        self.assertEqual(second["delivery_count"], 2)
        self.assertEqual((caller_status, caller_body), (200, {"ok": True}))
        self.assertEqual(stats["counters"]["worker_retries"], 1)
        self.assertEqual(stats["counters"]["worker_errors"], 0)

    async def test_disconnected_call_reattaches_without_duplicate_work(self) -> None:
        state = ModelRelayState()
        token = str((await state.register_rollout("reattach"))["registration_token"])
        original = await state.enqueue(
            rollout_id="reattach",
            endpoint="/v1/chat/completions",
            method="POST",
            body={"model": "m"},
            body_bytes=b'{"model":"m"}',
            headers={},
            idempotency_key="auto/request-fingerprint",
            defer_idempotency_until_disconnect=True,
        )
        await state.mark_caller_detached(original.request_id)
        retried = await state.enqueue(
            rollout_id="reattach",
            endpoint="/v1/chat/completions",
            method="POST",
            body={"model": "m"},
            body_bytes=b'{"model":"m"}',
            headers={},
            idempotency_key="auto/request-fingerprint",
            defer_idempotency_until_disconnect=True,
        )
        delivery = (
            await state.poll(
                rollout_id="reattach",
                registration_token=token,
                timeout_seconds=0,
            )
        )[0]
        await state.respond(
            request_id=delivery.request_id,
            registration_token=token,
            lease_id=delivery.lease_id,
            response=RelayWorkerResponse(200, {"ok": True}),
        )

        self.assertIs(retried, original)
        self.assertEqual(
            (await state.wait_for_response(retried, timeout_seconds=1)).body,
            {"ok": True},
        )
        stats = await state.stats()
        self.assertEqual(stats["counters"]["detached_callers"], 1)
        self.assertEqual(stats["counters"]["reattached"], 1)
        self.assertEqual(stats["counters"]["enqueued"], 1)

    async def test_sqlite_journal_restores_exact_completed_response(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "relay.sqlite3"
            state = ModelRelayState(state_path=state_path)
            token = str(
                (
                    await state.register_rollout(
                        "durable",
                        {"sandbox_id": "sandbox-7", "sandbox_generation": 3},
                    )
                )["registration_token"]
            )
            request = await state.enqueue(
                rollout_id="durable",
                endpoint="/v1/responses",
                method="POST",
                body={"model": "m"},
                body_bytes=b'{"model":"m"}',
                headers={"Content-Type": "application/json"},
                idempotency_key="request-7",
            )
            delivery = (
                await state.poll(
                    rollout_id="durable",
                    registration_token=token,
                    timeout_seconds=0,
                )
            )[0]
            await state.respond(
                request_id=delivery.request_id,
                registration_token=token,
                lease_id=delivery.lease_id,
                response=RelayWorkerResponse(
                    206,
                    b"exact-response-bytes",
                    {"Content-Type": "text/event-stream"},
                ),
            )
            state.close()

            restored = ModelRelayState(state_path=state_path)
            replay = await restored.enqueue(
                rollout_id="durable",
                endpoint="/v1/responses",
                method="POST",
                body={"model": "m"},
                body_bytes=b'{"model":"m"}',
                headers={"Content-Type": "application/json"},
                idempotency_key="request-7",
            )
            response = await restored.wait_for_response(replay, timeout_seconds=1)
            restored.close()

        self.assertEqual(replay.request_id, request.request_id)
        self.assertEqual(replay.sandbox_id, "sandbox-7")
        self.assertEqual(replay.sandbox_generation, 3)
        self.assertEqual(
            (response.status, response.body, response.headers),
            (
                206,
                b"exact-response-bytes",
                {"Content-Type": "text/event-stream"},
            ),
        )

    async def test_relay_restart_makes_inflight_request_reattachable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "relay.sqlite3"
            state = ModelRelayState(state_path=state_path)
            await state.register_rollout("restart-inflight")
            request = await state.enqueue(
                rollout_id="restart-inflight",
                endpoint="/v1/chat/completions",
                method="POST",
                body={"model": "m"},
                body_bytes=b'{"model":"m"}',
                headers={},
                idempotency_key="auto/restart-fingerprint",
                defer_idempotency_until_disconnect=True,
            )
            state.close()

            restored = ModelRelayState(state_path=state_path)
            replay = await restored.enqueue(
                rollout_id="restart-inflight",
                endpoint="/v1/chat/completions",
                method="POST",
                body={"model": "m"},
                body_bytes=b'{"model":"m"}',
                headers={},
                idempotency_key="auto/restart-fingerprint",
                defer_idempotency_until_disconnect=True,
            )
            stats = await restored.stats()
            restored.close()

        self.assertEqual(replay.request_id, request.request_id)
        self.assertEqual(stats["counters"]["reattached"], 1)
        self.assertEqual(stats["inflight"], 1)

    async def test_lifecycle_notifications_use_registered_sandbox_generation(
        self,
    ) -> None:
        accepted: list[tuple[str, int | None]] = []
        completed: list[tuple[str, int | None]] = []

        async def notify_accepted(request) -> str:
            accepted.append((str(request.sandbox_id), request.sandbox_generation))
            return "placement-before-migration"

        async def notify_result(request) -> str:
            completed.append((str(request.sandbox_id), request.sandbox_generation))
            return "placement-after-migration"

        async with relay_app(
            request_timeout_seconds=5,
            accepted_notifier=notify_accepted,
            result_notifier=notify_result,
        ) as relay:
            _status, registration = await relay.request(
                "POST",
                "/v1/relay/rollouts",
                expected=201,
                json={
                    "rollout_id": "lifecycle",
                    "metadata": {
                        "sandbox_id": "sandbox-9",
                        "sandbox_generation": 4,
                    },
                },
            )
            token = registration["rollout"]["registration_token"]
            caller = asyncio.create_task(
                relay.request_bytes(
                    "POST",
                    "/tunnels/lifecycle/v1/chat/completions",
                    json={"model": "m"},
                    headers={
                        "Traceparent": "00-before",
                        "X-Stainless-Retry-Count": "0",
                        "X-Forwarded-For": "100.64.0.10",
                        "X-Forwarded-Host": "relay-before.example",
                        "X-Real-IP": "100.64.0.10",
                        "job-id": "source-node",
                    },
                )
            )
            delivery = (await relay.poll("lifecycle", token))["request"]
            forwarded = {
                key.lower(): value
                for key, value in delivery["headers"].items()
            }
            await relay.respond_bytes(
                delivery,
                token,
                b'{"ok":true}',
                headers={"Content-Type": "application/json"},
            )
            await caller
            replay_status, replay_body, _replay_headers = await relay.request_bytes(
                "POST",
                "/tunnels/lifecycle/v1/chat/completions",
                json={"model": "m"},
                headers={
                    "Traceparent": "00-after",
                    "X-Stainless-Retry-Count": "1",
                    "X-Forwarded-For": "100.64.0.11",
                    "X-Forwarded-Host": "relay-after.example",
                    "X-Real-IP": "100.64.0.11",
                    "job-id": "destination-node",
                },
            )
            stats = await relay.stats()

        self.assertEqual(accepted, [("sandbox-9", 4)])
        self.assertEqual(completed, [("sandbox-9", 4)])
        self.assertEqual((replay_status, replay_body), (200, b'{"ok":true}'))
        self.assertEqual(stats["counters"]["accepted_notifications"], 1)
        self.assertEqual(stats["counters"]["wake_notifications"], 1)
        self.assertEqual(stats["counters"]["transport_resets"], 1)
        self.assertEqual(stats["counters"]["reattached"], 1)
        self.assertEqual(stats["counters"]["enqueued"], 1)
        self.assertNotIn("x-forwarded-for", forwarded)
        self.assertNotIn("x-forwarded-host", forwarded)
        self.assertNotIn("x-real-ip", forwarded)
        self.assertNotIn("job-id", forwarded)

    async def test_lifecycle_response_delivery_waits_for_explicit_release(
        self,
    ) -> None:
        state = ModelRelayState()
        token = str(
            (
                await state.register_rollout(
                    "deferred-delivery",
                    metadata={
                        "sandbox_id": "sandbox-1",
                        "sandbox_generation": 1,
                    },
                )
            )["registration_token"]
        )
        relay_request, delivery = await enqueue_and_poll(
            state,
            "deferred-delivery",
            token,
        )
        await state.respond(
            request_id=delivery.request_id,
            registration_token=token,
            lease_id=delivery.lease_id,
            response=RelayWorkerResponse(200, {"ok": True}),
            defer_delivery=True,
        )

        with self.assertRaises(asyncio.TimeoutError):
            await state.wait_for_response(relay_request, timeout_seconds=0.001)

        await state.release_completed_response(relay_request.request_id)
        response = await state.wait_for_response(
            relay_request,
            timeout_seconds=1,
        )
        self.assertEqual(response.body, {"ok": True})

    async def test_reregister_fences_every_operation_from_prior_incarnation(
        self,
    ) -> None:
        state = ModelRelayState()
        first_token = str(
            (await state.register_rollout("rollout-aba"))["registration_token"]
        )
        first_request, first_delivery = await enqueue_and_poll(
            state,
            "rollout-aba",
            first_token,
            worker_id="old-worker",
        )
        second_registration = await state.register_rollout("rollout-aba")
        second_token = str(second_registration["registration_token"])
        self.assertNotEqual(first_token, second_token)
        self.assertEqual(
            (await state.wait_for_response(first_request, timeout_seconds=1)).status,
            410,
        )

        stale_operations = {
            "unregister": lambda: state.unregister_rollout(
                "rollout-aba", registration_token=first_token
            ),
            "poll": lambda: state.poll(
                rollout_id="rollout-aba",
                registration_token=first_token,
                timeout_seconds=0,
            ),
            "heartbeat": lambda: state.record_worker_heartbeat(
                rollout_id="rollout-aba",
                registration_token=first_token,
                worker_id="old-worker",
            ),
            "renew": lambda: state.renew_lease(
                request_id=first_delivery.request_id,
                registration_token=first_token,
                lease_id=str(first_delivery.lease_id),
                lease_seconds=30,
            ),
            "respond": lambda: state.respond(
                request_id=first_delivery.request_id,
                registration_token=first_token,
                lease_id=first_delivery.lease_id,
                response=RelayWorkerResponse(200, {"old": True}),
            ),
        }
        for name, operation in stale_operations.items():
            with self.subTest(operation=name), self.assertRaises(web.HTTPConflict):
                await operation()

        request, delivery = await enqueue_and_poll(
            state,
            "rollout-aba",
            second_token,
        )
        await state.respond(
            request_id=delivery.request_id,
            registration_token=second_token,
            lease_id=delivery.lease_id,
            response=RelayWorkerResponse(200, {"new": True}),
        )
        observed = await state.wait_for_response(request, timeout_seconds=1)
        self.assertEqual((observed.status, observed.body), (200, {"new": True}))
        self.assertEqual(
            (await state.list_rollouts())[0]["registration_token"],
            second_token,
        )

    async def test_worker_routes_require_registration_token(self) -> None:
        async with relay_app(worker_poll_timeout_seconds=0) as relay:
            await relay.register("token-required")
            statuses = []
            for method, path, kwargs in (
                ("GET", "/worker/poll", {"params": {"rollout_id": "token-required"}}),
                (
                    "DELETE",
                    "/v1/relay/rollouts/token-required",
                    {"json": {}},
                ),
            ):
                status, _payload = await relay.request(method, path, **kwargs)
                statuses.append(status)
        self.assertEqual(statuses, [400, 400])

    async def test_diagnostics_and_completed_payloads_expire_without_reregistration(
        self,
    ) -> None:
        state = ModelRelayState(
            completed_request_retention_seconds=0.005,
            worker_retention_seconds=0.005,
        )
        token = str((await state.register_rollout("retention"))["registration_token"])
        await state.record_worker_heartbeat(
            rollout_id="retention",
            registration_token=token,
            worker_id="stale-worker",
            metadata={"large": "diagnostic"},
        )
        request, delivery = await enqueue_and_poll(state, "retention", token)
        await state.respond(
            request_id=delivery.request_id,
            registration_token=token,
            lease_id=delivery.lease_id,
            response=RelayWorkerResponse(200, {"large": "response-payload"}),
        )
        await state.wait_for_response(request, timeout_seconds=1)
        await asyncio.sleep(0.02)
        stats = await state.stats()
        self.assertEqual((stats["completed_retained"], stats["workers"]), (0, []))

    async def test_respond_rechecks_lease_expiry_before_accepting_result(self) -> None:
        state = ModelRelayState()
        token = str(
            (await state.register_rollout("expiry-check"))["registration_token"]
        )
        request, leased = await enqueue_and_poll(state, "expiry-check", token)
        leased.lease_expires_at = time.time() - 1
        with self.assertRaises(web.HTTPConflict):
            await state.respond(
                request_id=request.request_id,
                registration_token=token,
                lease_id=leased.lease_id,
                response=RelayWorkerResponse(200, {"stale": True}),
            )
        stats = await state.stats()
        retried = (
            await state.poll(
                rollout_id="expiry-check",
                registration_token=token,
                timeout_seconds=0,
            )
        )[0]
        await state.cancel_request(
            request_id=request.request_id,
            response=RelayWorkerResponse(499, {}),
        )
        self.assertEqual(stats["pending"]["expiry-check"], 1)
        self.assertEqual(stats["counters"]["lease_expired"], 1)
        self.assertEqual(retried.delivery_count, 2)

    async def test_admission_limits_release_capacity_after_cancellation(self) -> None:
        state = ModelRelayState(
            max_inflight_requests=1,
            max_inflight_requests_per_rollout=1,
            max_inflight_bytes=1024,
        )
        await state.register_rollout("bounded")
        first = await state.enqueue(
            rollout_id="bounded", endpoint="/v1/responses", body={}, headers={}
        )
        with self.assertRaises(web.HTTPTooManyRequests):
            await state.enqueue(
                rollout_id="bounded", endpoint="/v1/responses", body={}, headers={}
            )
        rejected = await state.stats()
        await state.cancel_request(
            request_id=first.request_id,
            response=RelayWorkerResponse(499, {}),
        )
        second = await state.enqueue(
            rollout_id="bounded", endpoint="/v1/responses", body={}, headers={}
        )
        await state.cancel_request(
            request_id=second.request_id,
            response=RelayWorkerResponse(499, {}),
        )
        released = await state.stats()
        self.assertEqual(rejected["inflight"], 1)
        self.assertEqual(rejected["counters"]["admission_rejected"], 1)
        self.assertEqual((released["inflight"], released["inflight_bytes"]), (0, 0))

    async def test_absolute_request_expiry_releases_admission(self) -> None:
        state = ModelRelayState(request_timeout_seconds=0.005, max_inflight_requests=1)
        await state.register_rollout("expiry-admission")
        expired = await state.enqueue(
            rollout_id="expiry-admission", endpoint="/v1/responses", body={}, headers={}
        )
        await asyncio.sleep(0.02)
        replacement = await state.enqueue(
            rollout_id="expiry-admission", endpoint="/v1/responses", body={}, headers={}
        )
        response = await state.wait_for_response(expired, timeout_seconds=1)
        stats = await state.stats()
        await state.cancel_request(
            request_id=replacement.request_id,
            response=RelayWorkerResponse(499, {}),
        )
        self.assertEqual(response.status, 504)
        self.assertEqual((stats["inflight"], stats["counters"]["timed_out"]), (1, 1))

    async def test_completed_tombstones_and_worker_diagnostics_have_hard_caps(
        self,
    ) -> None:
        state = ModelRelayState(max_completed_requests=2, max_workers=2)
        token = str((await state.register_rollout("hard-caps"))["registration_token"])
        for index in range(3):
            await state.record_worker_heartbeat(
                rollout_id="hard-caps",
                registration_token=token,
                worker_id=f"worker-{index}",
            )
            request = await state.enqueue(
                rollout_id="hard-caps",
                endpoint="/v1/responses",
                body={"index": index},
                headers={},
            )
            await state.cancel_request(
                request_id=request.request_id,
                response=RelayWorkerResponse(499, {}),
            )
        stats = await state.stats()
        self.assertEqual(stats["completed_retained"], 2)
        self.assertEqual(
            {worker["worker_id"] for worker in stats["workers"]},
            {"worker-1", "worker-2"},
        )

    async def test_healthz_reports_service_version(self) -> None:
        async with relay_app() as relay:
            _status, payload = await relay.request("GET", "/healthz", expected=200)
        self.assertEqual(
            payload,
            {"ok": True, "service": "model-relay", "version": package_version()},
        )

    async def test_openai_chat_request_round_trips_through_worker_poll(self) -> None:
        async with relay_app(
            sandbox_bearer_token="sandbox-token",
            worker_bearer_token="worker-token",
            request_timeout_seconds=5,
            worker_poll_timeout_seconds=1,
        ) as relay:
            worker_headers = {"Authorization": "Bearer worker-token"}
            token = await relay.register("rollout-1", headers=worker_headers)
            sandbox_task = asyncio.create_task(
                relay.model_call(
                    "rollout-1",
                    headers={
                        "Authorization": "Bearer sandbox-token",
                        "Proxy-Authorization": "Bearer proxy-secret",
                        "X-UCloud-Sandbox-Token": "public-secret",
                        "X-Request-Metadata": "safe",
                    },
                    body={"model": "local-model", "messages": [{"content": "ping"}]},
                )
            )
            _status, polled = await relay.request(
                "GET",
                "/worker/poll",
                expected=200,
                headers=worker_headers,
                params={"rollout_id": "rollout-1", "registration_token": token},
            )
            request = polled["request"]
            await relay.request(
                "POST",
                "/worker/respond",
                expected=200,
                headers=worker_headers,
                json={
                    "request_id": request["request_id"],
                    "registration_token": token,
                    "lease_id": request["lease_id"],
                    "response": {"choices": [{"message": {"content": "pong"}}]},
                },
            )
            status, body = await sandbox_task

        forwarded = {key.lower(): value for key, value in request["headers"].items()}
        self.assertEqual(status, 200)
        self.assertEqual(request["rollout_id"], "rollout-1")
        self.assertEqual(polled["requests"][0]["request_id"], request["request_id"])
        self.assertIsInstance(request["lease_id"], str)
        self.assertEqual(request["endpoint"], "/v1/chat/completions")
        self.assertEqual(request["body"]["model"], "local-model")
        self.assertNotIn("authorization", forwarded)
        self.assertNotIn("proxy-authorization", forwarded)
        self.assertNotIn("x-ucloud-sandbox-token", forwarded)
        self.assertEqual(forwarded["x-request-metadata"], "safe")
        self.assertEqual(body["choices"][0]["message"]["content"], "pong")

    async def test_general_tunnel_preserves_http_bytes_path_query_and_headers(
        self,
    ) -> None:
        async with relay_app(
            sandbox_bearer_token="sandbox-token",
            worker_bearer_token="worker-token",
            request_timeout_seconds=5,
            worker_poll_timeout_seconds=1,
        ) as relay:
            worker_headers = {"Authorization": "Bearer worker-token"}
            _status, registered = await relay.request(
                "POST",
                "/v1/relay/rollouts",
                expected=201,
                headers=worker_headers,
                json={"rollout_id": "tunnel-1", "metadata": {"kind": "http"}},
            )
            token = registered["rollout"]["registration_token"]
            request_body = b"\x00\xffbinary-request"
            client_task = asyncio.create_task(
                relay.request_bytes(
                    "PUT",
                    f"/tunnels/tunnel-1/_relay/{token}/"
                    "api/a%2Fb%20c?x=1&x=2&literal=one+two",
                    headers={
                        "Authorization": "Bearer upstream-secret",
                        "Content-Type": "application/octet-stream",
                        "X-Custom": "safe",
                        "Forwarded": "for=100.64.0.10",
                        "X-Forwarded-For": "100.64.0.10",
                        "X-Real-IP": "100.64.0.10",
                        "job-id": "provider-job",
                    },
                    data=request_body,
                )
            )
            _status, polled = await relay.request(
                "GET",
                "/worker/poll",
                expected=200,
                headers=worker_headers,
                params={
                    "rollout_id": "tunnel-1",
                    "registration_token": token,
                },
            )
            request = polled["request"]
            await relay.respond_bytes(
                request,
                token,
                b"\xffbinary-response",
                status=207,
                auth_headers=worker_headers,
                headers={
                    "Content-Type": "application/vnd.ucloud.test",
                    "X-Upstream": "worker",
                    "Connection": "close",
                },
            )
            response_status, response_body, response_headers = await client_task

        forwarded = {key.lower(): value for key, value in request["headers"].items()}
        response_headers = {
            key.lower(): value for key, value in response_headers.items()
        }
        self.assertEqual(request["rollout_id"], "tunnel-1")
        self.assertEqual(request["method"], "PUT")
        self.assertEqual(
            request["endpoint"],
            "/api/a%2Fb%20c?x=1&x=2&literal=one+two",
        )
        self.assertEqual(base64.b64decode(request["body_base64"]), request_body)
        self.assertEqual(request["body_size"], len(request_body))
        self.assertIsNone(request["body"])
        self.assertEqual(forwarded["authorization"], "Bearer upstream-secret")
        self.assertEqual(forwarded["content-type"], "application/octet-stream")
        self.assertEqual(forwarded["x-custom"], "safe")
        self.assertNotIn("x-ucloud-relay-token", forwarded)
        self.assertNotIn("forwarded", forwarded)
        self.assertNotIn("x-forwarded-for", forwarded)
        self.assertNotIn("x-real-ip", forwarded)
        self.assertNotIn("job-id", forwarded)
        self.assertEqual(
            (response_status, response_body), (207, b"\xffbinary-response")
        )
        self.assertEqual(
            response_headers["content-type"],
            "application/vnd.ucloud.test",
        )
        self.assertEqual(response_headers["x-upstream"], "worker")
        self.assertNotEqual(response_headers.get("connection"), "close")

    async def test_general_tunnel_exposes_json_and_rejects_invalid_base64_response(
        self,
    ) -> None:
        async with relay_app(request_timeout_seconds=5) as relay:
            token = await relay.register("json-tunnel")
            client_task = asyncio.create_task(
                relay.request_bytes(
                    "POST",
                    "/tunnels/json-tunnel/echo",
                    json={"hello": "world"},
                )
            )
            request = (await relay.poll("json-tunnel", token))["request"]
            invalid_status, _payload = await relay.request(
                "POST",
                "/worker/respond",
                json={
                    "request_id": request["request_id"],
                    "registration_token": token,
                    "lease_id": request["lease_id"],
                    "body_base64": "not base64!",
                },
            )
            await relay.respond_bytes(
                request,
                token,
                b'{"echo":true}',
                headers={"Content-Type": "application/json"},
            )
            status, body, _headers = await client_task

        self.assertEqual(request["body"], {"hello": "world"}, repr(request))
        self.assertEqual(invalid_status, 400)
        self.assertEqual((status, body), (200, b'{"echo":true}'))

    async def test_auth_is_enforced_when_configured(self) -> None:
        async with relay_app(sandbox_bearer_token="sandbox-token") as relay:
            await relay.register("rollout-1")
            status, _payload = await relay.model_call(
                "rollout-1",
            )
        self.assertEqual(status, 401)

    async def test_empty_worker_poll_returns_null_request(self) -> None:
        async with relay_app(worker_poll_timeout_seconds=0) as relay:
            token = await relay.register("rollout-empty")
            _status, body = await relay.request(
                "GET",
                "/worker/poll",
                expected=200,
                params={
                    "rollout_id": "rollout-empty",
                    "registration_token": token,
                    "timeout_seconds": "0",
                },
            )
        self.assertEqual(body, {"request": None, "requests": []})

    async def test_worker_can_poll_batches_and_respond_idempotently(self) -> None:
        async with relay_app(request_timeout_seconds=5) as relay:
            token = await relay.register("rollout-batch")
            tasks = [
                asyncio.create_task(
                    relay.model_call(
                        "rollout-batch",
                        body={"model": "m", "messages": [{"content": str(index)}]},
                    )
                )
                for index in range(3)
            ]
            for _ in range(100):
                if (await relay.stats())["pending"].get("rollout-batch") == 3:
                    break
                await asyncio.sleep(0.01)

            first = (
                await relay.poll(
                    "rollout-batch", token, limit="2", worker_id="worker-a"
                )
            )["requests"]
            self.assertEqual(len(first), 2)
            for request in first:
                await relay.respond(
                    request,
                    token,
                    {"index": request["body"]["messages"][0]["content"]},
                )
            duplicate = await relay.respond(first[0], token, {"ignored": True})
            last = (await relay.poll("rollout-batch", token, limit="2"))["request"]
            await relay.respond(
                last,
                token,
                {"index": last["body"]["messages"][0]["content"]},
            )
            results = await asyncio.gather(*tasks)
            stats = await relay.stats()

        self.assertEqual(len({request["request_id"] for request in first}), 2)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual([status for status, _body in results], [200, 200, 200])
        self.assertEqual(stats["counters"]["completed"], 3)
        self.assertEqual(stats["counters"]["duplicate_responses"], 1)
        self.assertEqual(stats["workers"][0]["worker_id"], "worker-a")

    async def test_expired_lease_is_retried_and_stale_response_rejected(self) -> None:
        async with relay_app(
            request_timeout_seconds=5, worker_lease_seconds=0.01
        ) as relay:
            token = await relay.register("rollout-retry")
            task = asyncio.create_task(relay.model_call("rollout-retry"))
            first = (
                await relay.poll(
                    "rollout-retry",
                    token,
                    worker_id="slow-worker",
                    lease_seconds="0.01",
                )
            )["request"]
            await asyncio.sleep(0.03)
            second = (
                await relay.poll(
                    "rollout-retry",
                    token,
                    worker_id="fast-worker",
                    lease_seconds="1",
                )
            )["request"]
            await relay.respond(first, token, {"stale": True}, expected=409)
            await relay.respond(second, token, {"ok": True})
            result, stats = await task, await relay.stats()

        self.assertEqual(first["request_id"], second["request_id"])
        self.assertNotEqual(first["lease_id"], second["lease_id"])
        self.assertEqual(second["delivery_count"], 2)
        self.assertEqual(result, (200, {"ok": True}))
        self.assertEqual(stats["counters"]["lease_expired"], 1)

    async def test_worker_can_renew_lease_for_long_inference(self) -> None:
        async with relay_app(
            request_timeout_seconds=5, worker_lease_seconds=0.01
        ) as relay:
            token = await relay.register("rollout-renew")
            task = asyncio.create_task(relay.model_call("rollout-renew"))
            leased = (
                await relay.poll(
                    "rollout-renew",
                    token,
                    worker_id="worker-renew",
                    lease_seconds="0.05",
                )
            )["request"]
            await asyncio.sleep(0.02)
            _status, renewed_payload = await relay.request(
                "POST",
                "/worker/renew",
                expected=200,
                json={
                    "request_id": leased["request_id"],
                    "registration_token": token,
                    "lease_id": leased["lease_id"],
                    "worker_id": "worker-renew",
                    "lease_seconds": 1,
                },
            )
            await asyncio.sleep(0.04)
            await relay.respond(leased, token, {"renewed": True})
            result, stats = await task, await relay.stats()

        renewed = renewed_payload["request"]
        self.assertGreater(renewed["lease_expires_at"], renewed["delivered_at"])
        self.assertEqual(result, (200, {"renewed": True}))
        self.assertEqual(stats["counters"]["lease_renewed"], 1)
        self.assertEqual(stats["counters"]["lease_expired"], 0)

    async def test_expired_lease_cannot_be_renewed(self) -> None:
        async with relay_app(
            request_timeout_seconds=5, worker_lease_seconds=0.01
        ) as relay:
            token = await relay.register("rollout-expired-renew")
            task = asyncio.create_task(relay.model_call("rollout-expired-renew"))
            leased = (
                await relay.poll("rollout-expired-renew", token, lease_seconds="0.01")
            )["request"]
            await asyncio.sleep(0.03)
            status, _payload = await relay.request(
                "POST",
                "/worker/renew",
                json={
                    "request_id": leased["request_id"],
                    "registration_token": token,
                    "lease_id": leased["lease_id"],
                    "lease_seconds": 1,
                },
            )
            retried = (
                await relay.poll("rollout-expired-renew", token, lease_seconds="1")
            )["request"]
            await relay.respond(retried, token, {"ok": True})
            result = await task

        self.assertEqual(status, 409)
        self.assertEqual(result, (200, {"ok": True}))

    async def test_worker_heartbeat_updates_stats(self) -> None:
        async with relay_app() as relay:
            token = await relay.register("rollout-heartbeat")
            await relay.request(
                "POST",
                "/worker/heartbeat",
                expected=200,
                json={
                    "rollout_id": "rollout-heartbeat",
                    "registration_token": token,
                    "worker_id": "worker-heartbeat",
                    "metadata": {"host": "lumi"},
                },
            )
            stats = await relay.stats()

        self.assertEqual(stats["workers"][0]["worker_id"], "worker-heartbeat")
        self.assertEqual(stats["workers"][0]["metadata"], {"host": "lumi"})


if __name__ == "__main__":
    unittest.main()
