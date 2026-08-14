from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Awaitable, TypeVar
from unittest.mock import patch

from aiohttp import web
from hypothesis import settings
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from ucloud_sandboxes import model_relay as model_relay_module
from ucloud_sandboxes.model_relay import ModelRelayState, RelayWorkerResponse


_Result = TypeVar("_Result")


class _Clock:
    now = 1_000_000.0

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now


@dataclass
class _RequestModel:
    request_id: str
    registration_token: str
    state: str
    payload_bytes: int
    lease_id: str | None = None


class ModelRelayStateMachine(RuleBasedStateMachine):
    rollout_id = "stateful-rollout"
    idempotency_key = "stateful-request"

    def __init__(self) -> None:
        super().__init__()
        self._temporary_directory = TemporaryDirectory()
        self._state_path = Path(self._temporary_directory.name) / "model-relay.sqlite3"
        self._clock = _Clock()
        self._clock_patch = patch.object(model_relay_module, "time", self._clock)
        self._clock_patch.start()
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self.state = self._new_state()
        registration = self._run(self.state.register_rollout(self.rollout_id))
        self.registration_token = str(registration["registration_token"])
        self.current: _RequestModel | None = None
        self.requests: dict[str, _RequestModel] = {}
        self.stale_registration_token: str | None = None
        self.stale_lease: tuple[str, str, str] | None = None
        initial_stats = self._run(self.state.stats())
        self.expected_counters = {name: 0 for name in initial_stats["counters"]}
        assert initial_stats["counters"] == self.expected_counters

    def _new_state(self) -> ModelRelayState:
        return ModelRelayState(
            state_path=self._state_path,
            request_timeout_seconds=10_000,
            completed_request_retention_seconds=10_000,
            max_inflight_requests=64,
            max_inflight_requests_per_rollout=64,
            max_inflight_bytes=1024 * 1024,
            max_completed_requests=64,
            max_completed_bytes=1024 * 1024,
        )

    def _run(self, awaitable: Awaitable[_Result]) -> _Result:
        return self._loop.run_until_complete(awaitable)

    def _increment(self, counter: str, amount: int = 1) -> None:
        self.expected_counters[counter] += amount

    def _complete_current(self) -> None:
        assert self.current is not None
        self.current.state = "completed"
        self.current.lease_id = None

    def _assert_conflict(self, awaitable: Awaitable[Any]) -> None:
        try:
            self._run(awaitable)
        except web.HTTPConflict:
            return
        raise AssertionError("stale relay authority unexpectedly succeeded")

    def teardown(self) -> None:
        try:
            self._run(self.state.aclose())
            self._loop.run_until_complete(self._loop.shutdown_default_executor())
        finally:
            self._loop.close()
            asyncio.set_event_loop(None)
            self._clock_patch.stop()
            self._temporary_directory.cleanup()

    @rule()
    def enqueue_or_replay(self) -> None:
        request = self._run(
            self.state.enqueue(
                rollout_id=self.rollout_id,
                endpoint="/v1/responses",
                body={"model": "stateful"},
                headers={},
                idempotency_key=self.idempotency_key,
            )
        )
        if self.current is not None:
            assert request.request_id == self.current.request_id
            self._increment("reattached")
            return
        self.current = _RequestModel(
            request_id=request.request_id,
            registration_token=self.registration_token,
            state="pending",
            payload_bytes=request.payload_bytes,
        )
        self.requests[request.request_id] = self.current
        self._increment("enqueued")

    @rule()
    def poll(self) -> None:
        deliveries = self._run(
            self.state.poll(
                rollout_id=self.rollout_id,
                registration_token=self.registration_token,
                timeout_seconds=0,
                lease_seconds=5,
            )
        )
        self._increment("polls")
        if self.current is None or self.current.state != "pending":
            assert deliveries == []
            self._increment("empty_polls")
            return
        assert len(deliveries) == 1
        delivery = deliveries[0]
        assert delivery.request_id == self.current.request_id
        assert delivery.lease_id is not None
        self.current.state = "leased"
        self.current.lease_id = delivery.lease_id
        self._increment("delivered")

    @rule()
    @precondition(
        lambda self: self.current is not None and self.current.state == "leased"
    )
    def renew(self) -> None:
        assert self.current is not None and self.current.lease_id is not None
        renewed = self._run(
            self.state.renew_lease(
                request_id=self.current.request_id,
                registration_token=self.registration_token,
                lease_id=self.current.lease_id,
                lease_seconds=5,
            )
        )
        assert renewed.lease_id == self.current.lease_id
        self._increment("lease_renewed")

    @rule()
    @precondition(
        lambda self: self.current is not None and self.current.state == "leased"
    )
    def respond(self) -> None:
        assert self.current is not None and self.current.lease_id is not None
        result = self._run(
            self.state.respond(
                request_id=self.current.request_id,
                registration_token=self.registration_token,
                lease_id=self.current.lease_id,
                response=RelayWorkerResponse(200, {"ok": True}),
            )
        )
        assert not result.duplicate
        self._complete_current()
        self._increment("completed")

    @rule()
    @precondition(
        lambda self: self.current is not None
        and self.current.state in {"pending", "leased"}
    )
    def cancel(self) -> None:
        assert self.current is not None
        result = self._run(
            self.state.cancel_request(
                request_id=self.current.request_id,
                response=RelayWorkerResponse(499, {"canceled": True}),
            )
        )
        assert result is not None
        self._complete_current()
        self._increment("canceled")

    @rule()
    @precondition(
        lambda self: self.current is not None and self.current.state == "leased"
    )
    def expire_lease(self) -> None:
        assert self.current is not None and self.current.lease_id is not None
        self.stale_lease = (
            self.current.request_id,
            self.current.registration_token,
            self.current.lease_id,
        )
        self._clock.now += 6
        self._run(self.state.stats())
        self.current.state = "pending"
        self.current.lease_id = None
        self._increment("lease_expired")

    @rule()
    def replace_registration(self) -> None:
        previous_token = self.registration_token
        active = self.current is not None and self.current.state != "completed"
        registration = self._run(self.state.register_rollout(self.rollout_id))
        self.registration_token = str(registration["registration_token"])
        assert self.registration_token != previous_token
        self.stale_registration_token = previous_token
        if active:
            self._complete_current()
            self._increment("unregister_canceled")
        self.current = None

    @rule()
    def unregister_and_register_new_incarnation(self) -> None:
        previous_token = self.registration_token
        active = self.current is not None and self.current.state != "completed"

        assert self._run(
            self.state.unregister_rollout(
                self.rollout_id,
                registration_token=previous_token,
            )
        )
        self.stale_registration_token = previous_token
        if active:
            self._complete_current()
            self._increment("unregister_canceled")
        self.current = None

        registration = self._run(self.state.register_rollout(self.rollout_id))
        self.registration_token = str(registration["registration_token"])
        assert self.registration_token != previous_token

    @rule()
    def restart(self) -> None:
        active_count = int(
            self.current is not None and self.current.state != "completed"
        )
        self._run(self.state.aclose())
        self.state = self._new_state()
        self.expected_counters = {counter: 0 for counter in self.expected_counters}
        self.expected_counters["restored_requests"] = active_count
        self._run(self.state.stats())

    @rule()
    @precondition(lambda self: self.stale_registration_token is not None)
    def stale_registration_cannot_mutate(self) -> None:
        assert self.stale_registration_token is not None
        if self.current is not None and self.current.state == "leased":
            assert self.current.lease_id is not None
            self._assert_conflict(
                self.state.renew_lease(
                    request_id=self.current.request_id,
                    registration_token=self.stale_registration_token,
                    lease_id=self.current.lease_id,
                    lease_seconds=5,
                )
            )
            return
        self._assert_conflict(
            self.state.poll(
                rollout_id=self.rollout_id,
                registration_token=self.stale_registration_token,
                timeout_seconds=0,
            )
        )

    @rule()
    @precondition(
        lambda self: self.stale_lease is not None
        and self.current is not None
        and self.current.request_id == self.stale_lease[0]
        and self.current.state in {"pending", "leased"}
        and self.current.lease_id != self.stale_lease[2]
    )
    def stale_lease_cannot_mutate(self) -> None:
        assert self.stale_lease is not None
        request_id, registration_token, lease_id = self.stale_lease
        self._assert_conflict(
            self.state.respond(
                request_id=request_id,
                registration_token=registration_token,
                lease_id=lease_id,
                response=RelayWorkerResponse(200, {"stale": True}),
            )
        )

    @rule()
    @precondition(
        lambda self: self.current is not None and self.current.state == "completed"
    )
    def response_replay_is_idempotent(self) -> None:
        assert self.current is not None
        result = self._run(
            self.state.respond(
                request_id=self.current.request_id,
                registration_token=self.registration_token,
                lease_id="stale-response-lease",
                response=RelayWorkerResponse(500, {"ignored": True}),
            )
        )
        assert result.duplicate
        self._increment("duplicate_responses")

    @invariant()
    def state_and_stats_match_model(self) -> None:
        stats = self._run(self.state.stats())
        pending_count = sum(
            request.state == "pending" for request in self.requests.values()
        )
        leased_count = sum(
            request.state == "leased" for request in self.requests.values()
        )
        completed_count = sum(
            request.state == "completed" for request in self.requests.values()
        )
        assert stats["rollouts"] == 1
        assert stats["pending"].get(self.rollout_id, 0) == pending_count
        assert stats["leased"].get(self.rollout_id, 0) == leased_count
        assert stats["inflight"] == pending_count + leased_count
        assert stats["inflight_bytes"] == sum(
            request.payload_bytes
            for request in self.requests.values()
            if request.state in {"pending", "leased"}
        )
        assert stats["completed_retained"] == completed_count
        assert stats["counters"] == self.expected_counters


TestModelRelayStateMachine = ModelRelayStateMachine.TestCase
TestModelRelayStateMachine.settings = settings(
    max_examples=20,
    stateful_step_count=18,
    deadline=None,
    derandomize=True,
)
