from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from threading import Event
import time
from typing import Any, AsyncIterator
import unittest
from unittest.mock import patch

from aiohttp import ClientSession, web

from ucloud_sandboxes.model_relay import (
    ACCEPTED_NOTIFIER_KEY,
    AGENT_LIFECYCLE_METADATA_KEY,
    MANAGED_AGENT_LIFECYCLE,
    ModelRelayState,
    RESULT_NOTIFIER_KEY,
    RelayRespondResult,
    RelaySqliteStore,
    RelayWorkerResponse,
    STATE_KEY,
    _notify_accepted,
    _notify_result,
    create_model_relay_app,
)


def _agent_metadata(sandbox_id: str, generation: int) -> dict[str, object]:
    return {
        AGENT_LIFECYCLE_METADATA_KEY: MANAGED_AGENT_LIFECYCLE,
        "sandbox_id": sandbox_id,
        "sandbox_generation": generation,
    }


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
                "body": {"encoding": "json", "value": body},
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
                "body": {
                    "encoding": "base64",
                    "value": base64.b64encode(body).decode("ascii"),
                },
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
    async def test_maintenance_requeues_expired_lease_without_api_traffic(
        self,
    ) -> None:
        state = ModelRelayState(request_timeout_seconds=5)
        registration = await state.register_rollout("maintenance")
        token = str(registration["registration_token"])
        request, delivery = await enqueue_and_poll(
            state,
            "maintenance",
            token,
            lease_seconds=0.01,
            worker_id="lost-worker",
        )

        await asyncio.sleep(0.03)
        await state.maintain()

        self.assertEqual(request.state, "pending")
        self.assertIsNone(request.lease_id)
        replacement = (
            await state.poll(
                rollout_id="maintenance",
                registration_token=token,
                timeout_seconds=0,
                lease_seconds=1,
                worker_id="replacement-worker",
            )
        )[0]
        self.assertEqual(replacement.request_id, delivery.request_id)
        self.assertEqual(replacement.delivery_count, 2)
        self.assertEqual((await state.stats())["counters"]["lease_expired"], 1)
        await state.aclose()

    async def test_accepted_notification_survives_caller_cancellation(self) -> None:
        state = ModelRelayState()
        await state.register_rollout(
            "park-cancellation",
            metadata=_agent_metadata("sandbox-1", 1),
        )
        relay_request = await state.enqueue(
            rollout_id="park-cancellation",
            endpoint="/v1/chat/completions",
            body={"model": "m"},
            headers={},
            idempotency_key="request-1",
        )
        notification_started = asyncio.Event()
        release_notification = asyncio.Event()

        async def notify_accepted(_request) -> str:
            notification_started.set()
            await release_notification.wait()
            return "transport-before-park"

        class FakeRequest:
            app = {
                STATE_KEY: state,
                ACCEPTED_NOTIFIER_KEY: notify_accepted,
            }

        task = asyncio.create_task(_notify_accepted(FakeRequest(), relay_request))  # type: ignore[arg-type]
        await notification_started.wait()
        task.cancel()
        release_notification.set()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertIsNotNone(relay_request.accepted_notified_at)
        self.assertEqual(
            relay_request.parked_transport_epoch,
            "transport-before-park",
        )
        self.assertEqual((await state.stats())["counters"]["accepted_notifications"], 1)
        await state.aclose()

    async def test_result_wake_joins_in_progress_park_notification(self) -> None:
        state = ModelRelayState()
        token = str(
            (
                await state.register_rollout(
                    "ordered-lifecycle",
                    _agent_metadata("sandbox-1", 1),
                )
            )["registration_token"]
        )
        relay_request = await state.enqueue(
            rollout_id="ordered-lifecycle",
            endpoint="/v1/responses",
            body={"model": "m"},
            headers={},
        )
        delivery = (
            await state.poll(
                rollout_id="ordered-lifecycle",
                registration_token=token,
                timeout_seconds=0,
                lease_seconds=30,
            )
        )[0]
        park_started = asyncio.Event()
        release_park = asyncio.Event()
        wake_started = asyncio.Event()
        order: list[str] = []

        async def park(_request) -> str:
            order.append("park-start")
            park_started.set()
            await release_park.wait()
            order.append("park-finish")
            return "before"

        async def wake(_request) -> str:
            order.append("wake")
            wake_started.set()
            return "after"

        class FakeRequest:
            app = {
                STATE_KEY: state,
                ACCEPTED_NOTIFIER_KEY: park,
                RESULT_NOTIFIER_KEY: wake,
            }

        park_task = asyncio.create_task(
            _notify_accepted(FakeRequest(), relay_request)  # type: ignore[arg-type]
        )
        await park_started.wait()
        response = await state.respond(
            request_id=delivery.request_id,
            registration_token=token,
            lease_id=delivery.lease_id,
            response=RelayWorkerResponse(200, {"ok": True}),
            defer_delivery=True,
        )
        wake_task = asyncio.create_task(
            _notify_result(  # type: ignore[arg-type]
                FakeRequest(),
                RelayRespondResult(response.request),
            )
        )
        await asyncio.sleep(0)
        self.assertFalse(wake_started.is_set())

        release_park.set()
        await asyncio.gather(park_task, wake_task)

        self.assertEqual(order, ["park-start", "park-finish", "wake"])
        self.assertIsNotNone(relay_request.accepted_notified_at)
        self.assertIsNotNone(relay_request.wake_notified_at)
        await state.aclose()

    async def test_completed_result_cannot_be_followed_by_a_late_park(self) -> None:
        state = ModelRelayState()
        token = str(
            (
                await state.register_rollout(
                    "late-park",
                    _agent_metadata("sandbox-1", 1),
                )
            )["registration_token"]
        )
        relay_request = await state.enqueue(
            rollout_id="late-park",
            endpoint="/v1/responses",
            body={"model": "m"},
            headers={},
        )
        delivery = (
            await state.poll(
                rollout_id="late-park",
                registration_token=token,
                timeout_seconds=0,
                lease_seconds=30,
            )
        )[0]
        await state.respond(
            request_id=delivery.request_id,
            registration_token=token,
            lease_id=delivery.lease_id,
            response=RelayWorkerResponse(200, {"ok": True}),
        )
        park_calls = 0

        async def park(_request) -> str:
            nonlocal park_calls
            park_calls += 1
            return "too-late"

        class FakeRequest:
            app = {
                STATE_KEY: state,
                ACCEPTED_NOTIFIER_KEY: park,
            }

        await _notify_accepted(FakeRequest(), relay_request)  # type: ignore[arg-type]

        self.assertEqual(park_calls, 0)
        await state.aclose()

    async def test_registration_metadata_rejects_aliases_and_coercion(self) -> None:
        state = ModelRelayState()
        for metadata in (
            {"sandboxId": "sandbox-1", "sandbox_generation": 1},
            {"sandbox_id": "sandbox-1", "sandboxGeneration": 1},
            {"sandbox_id": "sandbox-1", "sandbox_generation": "1"},
        ):
            with self.subTest(metadata=metadata), self.assertRaises(web.HTTPBadRequest):
                await state.register_rollout("strict-metadata", metadata)

        with self.assertRaises(web.HTTPBadRequest) as raised:
            await state.register_rollout(
                "missing-agent-contract",
                {"sandbox_id": "sandbox-1", "sandbox_generation": 1},
            )
        self.assertIn("register_agent_rollout", raised.exception.text)

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

        self.assertEqual(stats["completed_retained"], 1)
        self.assertLessEqual(stats["completed_bytes"], 1200)

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
                self.assertEqual(snapshots[0].state, "completed")
                raise sqlite3.OperationalError("injected commit failure")

            store.commit_request_batch = fail_commit  # type: ignore[method-assign]
            with self.assertRaisesRegex(sqlite3.OperationalError, "injected"):
                await state.respond(
                    request_id=delivery.request_id,
                    registration_token=token,
                    lease_id=delivery.lease_id,
                    response=RelayWorkerResponse(200, {"ok": True}),
                )

            failed = await state.stats()
            self.assertEqual(failed["leased"]["commit-failure"], 1)
            self.assertEqual(failed["counters"]["completed"], 0)

            store.commit_request_batch = original_commit  # type: ignore[method-assign]
            await state.respond(
                request_id=delivery.request_id,
                registration_token=token,
                lease_id=delivery.lease_id,
                response=RelayWorkerResponse(200, {"ok": True}),
            )
            response = await state.wait_for_response(request, timeout_seconds=1)
            self.assertEqual((response.status, response.body), (200, {"ok": True}))
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

            response = await state.wait_for_response(request, timeout_seconds=1)
            stats = await state.stats()
            self.assertEqual((response.status, response.body), (200, {"ok": True}))
            self.assertEqual(stats["inflight"], 0)
            self.assertEqual(stats["counters"]["completed"], 1)
            await state.aclose()

    async def test_deferred_response_survives_restart_until_release(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "relay.sqlite3"
            state = ModelRelayState(state_path=state_path, max_completed_requests=1)
            token = str(
                (await state.register_rollout("durable-pin"))["registration_token"]
            )
            request = await state.enqueue(
                rollout_id="durable-pin",
                endpoint="/v1/responses",
                body={"model": "m"},
                headers={},
                idempotency_key="durable-request",
            )
            delivery = (
                await state.poll(
                    rollout_id="durable-pin",
                    registration_token=token,
                    timeout_seconds=0,
                )
            )[0]
            await state.respond(
                request_id=request.request_id,
                registration_token=token,
                lease_id=delivery.lease_id,
                response=RelayWorkerResponse(200, {"ok": True}),
                defer_delivery=True,
            )
            await state.aclose()

            restored = ModelRelayState(
                state_path=state_path,
                max_completed_requests=1,
            )
            recovered = await restored.enqueue(
                rollout_id="durable-pin",
                endpoint="/v1/responses",
                body={"model": "m"},
                headers={},
                idempotency_key="durable-request",
            )
            self.assertEqual(recovered.request_id, request.request_id)
            self.assertTrue(recovered.delivery_pending)
            await restored.release_completed_response(recovered.request_id)
            response = await restored.wait_for_response(
                recovered,
                timeout_seconds=1,
            )
            self.assertEqual((response.status, response.body), (200, {"ok": True}))
            await restored.aclose()

    async def test_abandoned_pins_release_capacity_durably(self) -> None:
        for action in ("unregister", "replace", "expire"):
            with (
                self.subTest(action=action),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "relay.sqlite3"
                state = ModelRelayState(state_path=path, max_completed_requests=1)
                token = (await state.register_rollout("abandoned"))[
                    "registration_token"
                ]
                first, delivery = await enqueue_and_poll(state, "abandoned", token)
                await state.respond(
                    request_id=first.request_id,
                    registration_token=token,
                    lease_id=delivery.lease_id,
                    response=RelayWorkerResponse(200, {}),
                    defer_delivery=True,
                )
                if action == "unregister":
                    await state.unregister_rollout(
                        "abandoned", registration_token=token
                    )
                elif action == "replace":
                    await state.register_rollout("abandoned")
                else:
                    with patch(
                        "ucloud_sandboxes.model_relay.time.time",
                        return_value=time.time() + 7200,
                    ):
                        await state.maintain()
                result = await state.wait_for_response(first, timeout_seconds=1)
                self.assertEqual(result.status, 504 if action == "expire" else 410)
                self.assertEqual((await state.stats())["completed_retained"], 0)
                await state.aclose()
                restored = ModelRelayState(state_path=path, max_completed_requests=1)
                self.assertEqual((await restored.stats())["completed_retained"], 0)
                token = (await restored.register_rollout("next"))["registration_token"]
                second, delivery = await enqueue_and_poll(restored, "next", token)
                await restored.respond(
                    request_id=second.request_id,
                    registration_token=token,
                    lease_id=delivery.lease_id,
                    response=RelayWorkerResponse(200, {}),
                )
                self.assertEqual(
                    (
                        await restored.wait_for_response(second, timeout_seconds=1)
                    ).status,
                    200,
                )
                await restored.aclose()

    async def test_restart_resolves_expired_or_orphaned_pins(self) -> None:
        for orphan in (False, True):
            with (
                self.subTest(orphan=orphan),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "relay.sqlite3"
                state = ModelRelayState(state_path=path)
                token = (await state.register_rollout("pin"))["registration_token"]
                first, delivery = await enqueue_and_poll(state, "pin", token)
                await state.respond(
                    request_id=first.request_id,
                    registration_token=token,
                    lease_id=delivery.lease_id,
                    response=RelayWorkerResponse(200, {}),
                    defer_delivery=True,
                )
                if orphan:
                    state._store.delete_rollout("pin")
                await state.aclose()
                restored = ModelRelayState(state_path=path)
                with patch(
                    "ucloud_sandboxes.model_relay.time.time",
                    return_value=time.time() + 7200,
                ):
                    await restored.maintain()
                    recovered = restored._completed[first.request_id]
                    self.assertFalse(recovered.delivery_pending)
                    self.assertEqual(
                        recovered.completed_response.status, 410 if orphan else 504
                    )
                    self.assertTrue(recovered.future.done())
                await restored.aclose()

    async def test_expired_pin_cleanup_retries_failed_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = ModelRelayState(state_path=Path(directory) / "relay.sqlite3")
            token = (await state.register_rollout("pin"))["registration_token"]
            first, delivery = await enqueue_and_poll(state, "pin", token)
            await state.respond(
                request_id=first.request_id,
                registration_token=token,
                lease_id=delivery.lease_id,
                response=RelayWorkerResponse(200, {}),
                defer_delivery=True,
            )
            with patch(
                "ucloud_sandboxes.model_relay.time.time",
                return_value=time.time() + 7200,
            ):
                with patch.object(
                    state._store, "delete_requests", side_effect=OSError("disk")
                ):
                    with self.assertRaises(OSError):
                        await state.maintain()
                self.assertFalse(first.future.done())
                await state.maintain()
            self.assertEqual(
                (await state.wait_for_response(first, timeout_seconds=1)).status, 504
            )
            await state.aclose()

    async def test_completion_does_not_scan_retained_history(self) -> None:
        class NoScanDict(dict):
            def items(self):
                raise AssertionError("completion scanned history")

            def values(self):
                raise AssertionError("completion scanned history")

            def __iter__(self):
                raise AssertionError("completion scanned history")

        state = ModelRelayState(max_completed_requests=8)
        token = (await state.register_rollout("indexed"))["registration_token"]
        for _ in range(8):
            first, delivery = await enqueue_and_poll(state, "indexed", token)
            await state.respond(
                request_id=first.request_id,
                registration_token=token,
                lease_id=delivery.lease_id,
                response=RelayWorkerResponse(200, {}),
            )
        state._completed = NoScanDict(state._completed)
        second, delivery = await enqueue_and_poll(state, "indexed", token)
        await state.respond(
            request_id=second.request_id,
            registration_token=token,
            lease_id=delivery.lease_id,
            response=RelayWorkerResponse(200, {}),
        )
        self.assertEqual(len(state._completed), 8)
        self.assertEqual(
            (await state.wait_for_response(second, timeout_seconds=1)).status, 200
        )

    async def test_completed_capacity_never_evicts_deferred_pins(self) -> None:
        state = ModelRelayState(max_completed_requests=1)
        token = str(
            (await state.register_rollout("pin-capacity"))["registration_token"]
        )
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
        constrained = await state.stats()
        self.assertEqual(constrained["completed_retained"], 1)
        self.assertEqual(constrained["leased"]["pin-capacity"], 1)

        await state.release_completed_response(first.request_id)
        first_response = await state.wait_for_response(first, timeout_seconds=1)
        await state.respond(
            request_id=second_delivery.request_id,
            registration_token=token,
            lease_id=second_delivery.lease_id,
            response=RelayWorkerResponse(200, {"second": True}),
        )
        second_response = await state.wait_for_response(second, timeout_seconds=1)
        self.assertEqual(first_response.body, {"first": True})
        self.assertEqual(second_response.body, {"second": True})

    async def test_poll_wakeups_are_scoped_to_the_target_rollout(self) -> None:
        state = ModelRelayState()
        await state.register_rollout("wake-a")
        token_b = str((await state.register_rollout("wake-b"))["registration_token"])
        poll_b = asyncio.create_task(
            state.poll(
                rollout_id="wake-b",
                registration_token=token_b,
                timeout_seconds=1,
            )
        )
        await asyncio.sleep(0.01)
        request_a = await state.enqueue(
            rollout_id="wake-a",
            endpoint="/v1/responses",
            body={},
            headers={},
        )
        await asyncio.sleep(0.02)
        self.assertFalse(poll_b.done())

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
            request_id=request_a.request_id,
            response=RelayWorkerResponse(499, {}),
        )

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
        await asyncio.sleep(0.01)

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

            missing_field = dict(rows[1])
            missing_field.pop("request_digest")
            store.load_requests = lambda: [rows[0], missing_field]  # type: ignore[method-assign]
            with self.assertRaisesRegex(KeyError, "request_digest"):
                await restored.stats()

            store.load_requests = original_load  # type: ignore[method-assign]
            stats = await restored.stats()
            self.assertEqual(stats["inflight"], 2)
            self.assertEqual(stats["pending"]["atomic-restore"], 2)
            await restored.aclose()

    async def test_disconnected_call_reattaches_without_duplicate_work(self) -> None:
        state = ModelRelayState()
        token = str((await state.register_rollout("reattach"))["registration_token"])
        original = await state.enqueue(
            rollout_id="reattach",
            endpoint="/v1/chat/completions",
            method="POST",
            body={"model": "m"},
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
                        _agent_metadata("sandbox-7", 3),
                    )
                )["registration_token"]
            )
            request = await state.enqueue(
                rollout_id="durable",
                endpoint="/v1/responses",
                method="POST",
                body={"model": "m"},
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
            await state.aclose()

            restored = ModelRelayState(state_path=state_path)
            replay = await restored.enqueue(
                rollout_id="durable",
                endpoint="/v1/responses",
                method="POST",
                body={"model": "m"},
                headers={"Content-Type": "application/json"},
                idempotency_key="request-7",
            )
            response = await restored.wait_for_response(replay, timeout_seconds=1)
            await restored.aclose()

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

    async def test_restart_upgrades_legacy_sandbox_binding_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "relay.sqlite3"
            store = RelaySqliteStore(state_path)
            store.save_rollout(
                {
                    "rollout_id": "legacy-agent",
                    "registration_token": "a" * 32,
                    "metadata": {
                        "sandbox_id": "sandbox-legacy",
                        "sandbox_generation": 4,
                    },
                    "registered_at": 1.0,
                }
            )
            store.close()

            state = ModelRelayState(state_path=state_path)
            records = await state.list_rollouts()
            await state.aclose()

            restored = RelaySqliteStore(state_path)
            persisted = restored.load_rollouts()
            restored.close()

        expected = _agent_metadata("sandbox-legacy", 4)
        self.assertEqual(records[0]["metadata"], expected)
        self.assertEqual(persisted[0]["metadata"], expected)

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
                        AGENT_LIFECYCLE_METADATA_KEY: MANAGED_AGENT_LIFECYCLE,
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
            delivery = (await relay.poll("lifecycle", token))["requests"][0]
            forwarded = {
                key.lower(): value for key, value in delivery["headers"].items()
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
                        AGENT_LIFECYCLE_METADATA_KEY: MANAGED_AGENT_LIFECYCLE,
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
            max_inflight_requests=2,
            max_inflight_requests_per_rollout=2,
            max_inflight_bytes=1024,
        )
        token = str((await state.register_rollout("bounded"))["registration_token"])
        first = await state.enqueue(
            rollout_id="bounded", endpoint="/v1/responses", body={}, headers={}
        )
        await state.poll(
            rollout_id="bounded",
            registration_token=token,
            timeout_seconds=0,
        )
        second = await state.enqueue(
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
        await state.cancel_request(
            request_id=second.request_id,
            response=RelayWorkerResponse(499, {}),
        )
        replacement = await state.enqueue(
            rollout_id="bounded", endpoint="/v1/responses", body={}, headers={}
        )
        await state.cancel_request(
            request_id=replacement.request_id,
            response=RelayWorkerResponse(499, {}),
        )
        released = await state.stats()
        self.assertEqual(rejected["inflight"], 2)
        self.assertEqual(
            (rejected["pending"]["bounded"], rejected["leased"]["bounded"]), (1, 1)
        )
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
            request = polled["requests"][0]
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
        self.assertEqual(request["body"]["encoding"], "base64")
        self.assertEqual(base64.b64decode(request["body"]["value"]), request_body)
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
            request = (await relay.poll("json-tunnel", token))["requests"][0]
            invalid_status, _payload = await relay.request(
                "POST",
                "/worker/respond",
                json={
                    "request_id": request["request_id"],
                    "registration_token": token,
                    "lease_id": request["lease_id"],
                    "body": {"encoding": "base64", "value": "not base64!"},
                },
            )
            await relay.respond_bytes(
                request,
                token,
                b'{"echo":true}',
                headers={"Content-Type": "application/json"},
            )
            status, body, _headers = await client_task

        self.assertEqual(request["body"]["encoding"], "base64")
        self.assertEqual(
            json.loads(base64.b64decode(request["body"]["value"])),
            {"hello": "world"},
            repr(request),
        )
        self.assertEqual(invalid_status, 400)
        self.assertEqual((status, body), (200, b'{"echo":true}'))

    async def test_auth_is_enforced_when_configured(self) -> None:
        async with relay_app(sandbox_bearer_token="sandbox-token") as relay:
            await relay.register("rollout-1")
            status, _payload = await relay.model_call(
                "rollout-1",
            )
        self.assertEqual(status, 401)

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
                    {"index": request["body"]["value"]["messages"][0]["content"]},
                )
            duplicate = await relay.respond(first[0], token, {"ignored": True})
            last = (await relay.poll("rollout-batch", token, limit="2"))["requests"][0]
            await relay.respond(
                last,
                token,
                {"index": last["body"]["value"]["messages"][0]["content"]},
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
            )["requests"][0]
            await asyncio.sleep(0.03)
            renew_status, _payload = await relay.request(
                "POST",
                "/worker/renew",
                json={
                    "request_id": first["request_id"],
                    "registration_token": token,
                    "lease_id": first["lease_id"],
                    "lease_seconds": 1,
                },
            )
            second = (
                await relay.poll(
                    "rollout-retry",
                    token,
                    worker_id="fast-worker",
                    lease_seconds="1",
                )
            )["requests"][0]
            await relay.respond(first, token, {"stale": True}, expected=409)
            await relay.respond(second, token, {"ok": True})
            result, stats = await task, await relay.stats()

        self.assertEqual(first["request_id"], second["request_id"])
        self.assertNotEqual(first["lease_id"], second["lease_id"])
        self.assertEqual(renew_status, 409)
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
            )["requests"][0]
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

    async def test_sdk_worker_contract_preserves_request_identity(self) -> None:
        sdk_src = Path(__file__).resolve().parents[1] / "ucloud-sandboxes-sdk" / "src"
        if not sdk_src.is_dir():
            self.skipTest(f"SDK source directory is missing: {sdk_src}")
        sys.path.insert(0, str(sdk_src))
        try:
            from ucloud_sandboxes_sdk import AsyncRelayWorkerClient
        finally:
            sys.path.remove(str(sdk_src))

        rollout_id = "sdk-contract-rollout"
        worker_id = "sdk-contract-worker"
        worker_token = "sdk-contract-token"
        async with relay_app(
            worker_bearer_token=worker_token,
            request_timeout_seconds=5,
        ) as relay:
            async with AsyncRelayWorkerClient(
                relay.base_url,
                worker_token=worker_token,
            ) as client:
                sandbox = type(
                    "Sandbox",
                    (),
                    {
                        "id": "sdk-contract-sandbox",
                        "record": {
                            "generation": 7,
                            "spec": {
                                "parkable": True,
                                "managed_process": True,
                            },
                        },
                    },
                )()
                registration = await client.register_agent_rollout(
                    rollout_id,
                    sandbox,
                )
                registration_token = registration["rollout"]["registration_token"]
                model_call = asyncio.create_task(
                    relay.model_call(
                        rollout_id,
                        body={"model": "contract-model", "messages": []},
                    )
                )
                try:
                    polled = await client.poll(
                        rollout_id,
                        worker_id=worker_id,
                        timeout_seconds=1,
                        lease_seconds=1,
                    )
                    self.assertEqual(len(polled.requests), 1)
                    request = polled.requests[0]
                    self.assertEqual(request.rollout_id, rollout_id)
                    self.assertEqual(request.registration_token, registration_token)
                    self.assertTrue(request.request_id)
                    self.assertTrue(request.lease_id)
                    self.assertEqual(request.leased_by, worker_id)
                    self.assertEqual(request.sandbox_id, "sdk-contract-sandbox")
                    self.assertEqual(request.sandbox_generation, 7)

                    renewed = await client.renew_request(
                        request,
                        worker_id=worker_id,
                        lease_seconds=2,
                    )
                    self.assertEqual(renewed.request_id, request.request_id)
                    self.assertEqual(renewed.rollout_id, request.rollout_id)
                    self.assertEqual(
                        renewed.registration_token,
                        request.registration_token,
                    )
                    self.assertEqual(renewed.lease_id, request.lease_id)
                    self.assertEqual(renewed.sandbox_id, request.sandbox_id)
                    self.assertEqual(
                        renewed.sandbox_generation,
                        request.sandbox_generation,
                    )
                    self.assertGreater(
                        renewed.lease_expires_at or 0,
                        request.lease_expires_at or 0,
                    )

                    responded = await client.respond_to(
                        renewed,
                        {"contract": "ok"},
                        status=201,
                    )
                    self.assertEqual(responded["request_id"], request.request_id)
                    self.assertFalse(responded["duplicate"])
                    self.assertEqual(await model_call, (201, {"contract": "ok"}))
                finally:
                    if not model_call.done():
                        model_call.cancel()
                        try:
                            await model_call
                        except asyncio.CancelledError:
                            pass


if __name__ == "__main__":
    unittest.main()
