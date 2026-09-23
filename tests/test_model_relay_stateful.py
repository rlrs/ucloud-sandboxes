"""Generated relay authority traces against the sole live PostgreSQL backend."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
from typing import Awaitable, TypeVar
import unittest

from aiohttp import web
from hypothesis import settings
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from tests.postgres_fixture import postgres_database
from ucloud_sandboxes.model_relay import RelayWorkerResponse

_Result = TypeVar("_Result")


@dataclass
class _RequestModel:
    request_id: str
    registration_token: str
    state: str
    payload_bytes: int
    lease_id: str | None = None
    response: RelayWorkerResponse | None = None


class ModelRelayStateMachine(RuleBasedStateMachine):
    rollout_id = "stateful-rollout"
    idempotency_key = "stateful-request"

    def __init__(self):
        super().__init__()
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._database_context = postgres_database()
        database = self._run(self._database_context.__aenter__())
        self._database_identity = database.pool.conninfo, database.deployment_id, database.schema
        self.state = self._new_state(database)
        registration = self._run(self.state.register_rollout(self.rollout_id))
        self.registration_token = registration["registration_token"]
        self.current: _RequestModel | None = None
        self.requests: dict[str, _RequestModel] = {}
        self.stale_registration_token: str | None = None
        self.stale_lease: tuple[str, str, str] | None = None

    def _new_state(self, database=None):
        from ucloud_sandboxes.shared_control.database import PostgresDatabase
        from ucloud_sandboxes.shared_control.relay import PostgresRelayState
        if database is None:
            dsn, deployment, schema = self._database_identity
            database = PostgresDatabase(dsn, deployment, schema=schema, max_connections=4)
        state = PostgresRelayState(database, request_timeout_seconds=10_000,
                                  completed_request_retention_seconds=10_000)
        self._run(state.open())
        return state

    def _run(self, awaitable: Awaitable[_Result]) -> _Result:
        return self._loop.run_until_complete(awaitable)

    def _assert_conflict(self, awaitable):
        try:
            self._run(awaitable)
        except web.HTTPConflict:
            return
        raise AssertionError("stale or changed relay authority unexpectedly succeeded")

    def teardown(self):
        try:
            self._run(self.state.aclose())
            self._run(self._database_context.__aexit__(None, None, None))
            self._run(self._loop.shutdown_default_executor())
        finally:
            self._loop.close()
            asyncio.set_event_loop(None)

    @rule()
    def enqueue_or_replay(self):
        request = self._run(self.state.enqueue(
            rollout_id=self.rollout_id, endpoint="/v1/responses",
            body={"model": "stateful"}, headers={}, idempotency_key=self.idempotency_key))
        if self.current is not None:
            assert request.request_id == self.current.request_id
            return
        self.current = _RequestModel(request.request_id, self.registration_token,
                                     "pending", request.payload_bytes)
        self.requests[request.request_id] = self.current

    @rule()
    @precondition(lambda self: self.current is not None)
    def changed_idempotent_request_is_rejected(self):
        self._assert_conflict(self.state.enqueue(
            rollout_id=self.rollout_id, endpoint="/v1/responses",
            body={"model": "different"}, headers={}, idempotency_key=self.idempotency_key))

    @rule()
    def poll(self):
        deliveries = self._run(self.state.poll(
            rollout_id=self.rollout_id, registration_token=self.registration_token,
            timeout_seconds=0, lease_seconds=600))
        if self.current is None or self.current.state not in {"pending", "expired_lease"}:
            assert deliveries == []
            return
        assert len(deliveries) == 1
        delivery = deliveries[0]
        assert delivery.request_id == self.current.request_id and delivery.lease_id
        if self.current.state == "expired_lease":
            assert delivery.lease_id != self.current.lease_id
        self.current.state, self.current.lease_id = "leased", delivery.lease_id

    @rule()
    @precondition(lambda self: self.current is not None and self.current.state == "leased")
    def renew(self):
        renewed = self._run(self.state.renew_lease(
            request_id=self.current.request_id, registration_token=self.registration_token,
            lease_id=self.current.lease_id, lease_seconds=600))
        assert renewed.lease_id == self.current.lease_id

    @rule()
    @precondition(lambda self: self.current is not None and self.current.state == "leased")
    def respond(self):
        response = RelayWorkerResponse(200, {"ok": True})
        result = self._run(self.state.respond(
            request_id=self.current.request_id, registration_token=self.registration_token,
            lease_id=self.current.lease_id, response=response))
        assert not result.duplicate
        self.current.state, self.current.response = "completed", response

    @rule()
    @precondition(lambda self: self.current is not None and self.current.state != "completed")
    def cancel(self):
        response = RelayWorkerResponse(499, {"canceled": True})
        result = self._run(self.state.cancel_request(request_id=self.current.request_id,
                                                   response=response))
        assert result == response
        self.current.state = "completed"
        # Cancellation is a terminal caller outcome, not a successful worker
        # receipt. Replay rules below apply only to results the worker committed.
        self.current.response = None

    @rule()
    @precondition(lambda self: self.current is not None and self.current.state == "leased")
    def expire_lease(self):
        self.stale_lease = (self.current.request_id, self.registration_token, self.current.lease_id)
        async def expire():
            async with self.state.store.transaction("stateful_expire_lease") as conn:
                await conn.execute(
                    "UPDATE relay_requests SET lease_expires_at=extract(epoch FROM clock_timestamp())-1 WHERE deployment_id=%s AND request_id=%s",
                    (self.state.deployment, self.current.request_id))
        self._run(expire())
        # PostgreSQL atomically replaces expired leases at the next claim. The
        # durable state remains leased until then; do not patch a Python clock.
        self.current.state = "expired_lease"

    def _retire_current(self):
        if self.current is not None:
            self.current.state = "completed"
        self.current = None

    @rule()
    def replace_registration(self):
        previous = self.registration_token
        registration = self._run(self.state.register_rollout(self.rollout_id))
        self.registration_token = registration["registration_token"]
        assert self.registration_token != previous
        self.stale_registration_token = previous
        self._retire_current()

    @rule()
    def unregister_and_register_new_incarnation(self):
        previous = self.registration_token
        assert self._run(self.state.unregister_rollout(self.rollout_id,
                                                       registration_token=previous))
        self.stale_registration_token = previous
        self._retire_current()
        registration = self._run(self.state.register_rollout(self.rollout_id))
        self.registration_token = registration["registration_token"]
        assert self.registration_token != previous

    @rule()
    def restart(self):
        self._run(self.state.aclose())
        self.state = self._new_state()

    @rule()
    @precondition(lambda self: self.stale_registration_token is not None)
    def stale_registration_cannot_mutate(self):
        self._assert_conflict(self.state.poll(
            rollout_id=self.rollout_id, registration_token=self.stale_registration_token,
            timeout_seconds=0))
        self._assert_conflict(self.state.unregister_rollout(
            self.rollout_id, registration_token=self.stale_registration_token))

    @rule()
    @precondition(lambda self: self.stale_lease is not None and self.current is not None
                  and self.current.request_id == self.stale_lease[0]
                  and self.current.state != "completed")
    def stale_lease_cannot_mutate(self):
        request, registration, lease = self.stale_lease
        self._assert_conflict(self.state.respond(
            request_id=request, registration_token=registration, lease_id=lease,
            response=RelayWorkerResponse(200, {"stale": True})))
        self._assert_conflict(self.state.renew_lease(
            request_id=request, registration_token=registration, lease_id=lease,
            lease_seconds=600))

    @rule()
    @precondition(lambda self: self.current is not None and self.current.response is not None)
    def identical_response_replay_is_idempotent(self):
        result = self._run(self.state.respond(
            request_id=self.current.request_id, registration_token=self.registration_token,
            lease_id=self.current.lease_id, response=self.current.response))
        assert result.duplicate

    @rule()
    @precondition(lambda self: self.current is not None and self.current.response is not None)
    def changed_response_or_lease_is_rejected(self):
        for response, lease in ((RelayWorkerResponse(500, {"changed": True}), self.current.lease_id),
                                (self.current.response, "stale-response-lease")):
            self._assert_conflict(self.state.respond(
                request_id=self.current.request_id, registration_token=self.registration_token,
                lease_id=lease, response=response))

    @invariant()
    def durable_state_and_stats_match_model(self):
        stats = self._run(self.state.stats())
        pending = sum(request.state == "pending" for request in self.requests.values())
        leased = sum(request.state in {"leased", "expired_lease"} for request in self.requests.values())
        completed = sum(request.state == "completed" for request in self.requests.values())
        assert stats["backend"] == "postgres"
        assert stats["rollouts"] == 1
        assert stats["pending"].get(self.rollout_id, 0) == pending
        assert stats["leased"].get(self.rollout_id, 0) == leased
        assert stats["inflight"] == pending + leased
        assert stats["inflight_bytes"] == sum(request.payload_bytes for request in self.requests.values()
                                               if request.state != "completed")
        assert stats["completed_retained"] == completed
        assert stats["reserved_storage_bytes"] >= 0


TestModelRelayStateMachine = unittest.skipUnless(
    os.environ.get("UCLOUD_TEST_POSTGRES_DSN"), "requires real PostgreSQL"
)(ModelRelayStateMachine.TestCase)
TestModelRelayStateMachine.settings = settings(
    max_examples=20, stateful_step_count=18, deadline=None, derandomize=True,
)
