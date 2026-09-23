"""Run against real PostgreSQL on Linux; HTTP tests use the production app."""

import asyncio
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from ucloud_sandboxes import model_relay as api

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")
if DSN:
    import psycopg
    from psycopg import sql
    from ucloud_sandboxes.shared_control.postgres import PostgresControlStore
    from ucloud_sandboxes.shared_control.relay import (
        PostgresRelayState,
        RESPONSE_RESERVATION,
    )


@unittest.skipUnless(DSN, "requires real PostgreSQL")
class PostgresRelayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.schema = "ucloud_shared_relay_" + uuid4().hex
        self.states = []
        self.clients = []
        admin = PostgresControlStore(DSN, "test", schema=self.schema)
        await admin.open()
        await admin.migrate()
        await admin.close()
        self.state = await self.new_state()
        self.reg = await self.state.register_rollout("agent")
        self.token = self.reg["registration_token"]

    async def new_state(self, **kwargs):
        store = PostgresControlStore(
            DSN,
            getattr(self, "deployment", "test"),
            schema=self.schema,
            max_connections=4,
        )
        state = PostgresRelayState(store, **kwargs)
        await state.open()
        self.states.append(state)
        return state

    async def asyncTearDown(self):
        for client in self.clients:
            await client.close()
        for state in self.states:
            await state.aclose()
        async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
            await conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
            )

    async def enqueue(self, state=None, **kwargs):
        return await (state or self.state).enqueue(
            rollout_id="agent",
            endpoint="/v1/responses",
            body={"hello": 1},
            headers={},
            **kwargs,
        )

    async def poll(self, state=None, **kwargs):
        return await (state or self.state).poll(
            rollout_id="agent",
            registration_token=self.token,
            timeout_seconds=0.1,
            **kwargs,
        )

    async def respond(self, req, state=None, **kwargs):
        return await (state or self.state).respond(
            request_id=req.request_id,
            registration_token=self.token,
            lease_id=req.lease_id,
            response=api.RelayWorkerResponse(200, b"answer"),
            **kwargs,
        )

    async def test_cross_instance_enqueue_lease_result_and_reattach(self):
        peer = await self.new_state()
        request = await self.enqueue(idempotency_key="stable")
        (leased,) = await self.poll(peer)
        waiter = asyncio.create_task(
            self.state.wait_for_response(request, timeout_seconds=3)
        )
        await self.respond(leased, peer)
        self.assertEqual((await waiter).body, b"answer")
        duplicate = await self.enqueue(peer, idempotency_key="stable")
        self.assertEqual(duplicate.request_id, request.request_id)
        self.assertTrue((await self.respond(leased)).duplicate)

    async def test_parallel_claimers_and_retries(self):
        peer = await self.new_state()
        requests = await asyncio.gather(
            *(self.enqueue(idempotency_key=str(i)) for i in range(64))
        )
        first, second = await asyncio.gather(
            self.poll(limit=64), self.poll(peer, limit=64)
        )
        self.assertEqual(
            {r.request_id for r in first + second}, {r.request_id for r in requests}
        )
        self.assertEqual(len(first + second), 64)
        req = (first + second)[0]
        await self.state.retry_worker_failure(
            request_id=req.request_id,
            registration_token=self.token,
            lease_id=req.lease_id,
        )
        (again,) = await self.poll(peer)
        self.assertNotEqual(req.lease_id, again.lease_id)
        with self.assertRaises(web.HTTPConflict):
            await self.respond(req)

    async def test_poll_commits_worker_heartbeat_and_batch_leases_together(self):
        requests = [await self.enqueue(idempotency_key=str(i)) for i in range(8)]
        samples = []
        self.state.store.observe = samples.append
        leased = await self.poll(limit=8, worker_id="worker")
        self.assertEqual([r.request_id for r in leased], [r.request_id for r in requests])
        self.assertEqual(len({r.lease_id for r in leased}), 8)
        self.assertTrue(all(len(r.lease_id) == 32 and r.delivery_count == 1 for r in leased))
        self.assertTrue(all(r.body == api._encoded_body({"hello": 1}) and r.leased_by == "worker" for r in leased))
        operations = [s.operation for s in samples]
        self.assertEqual(operations.count("relay_claim_inference"), 1)
        self.assertNotIn("relay_worker", operations)
        async with self.state.store.transaction("test_heartbeat") as conn:
            row = await (await conn.execute(
                "SELECT * FROM relay_workers WHERE rollout_id='agent' AND worker_id='worker'",
            )).fetchone()
        self.assertEqual(row["registration_token"], self.token)
        self.assertLessEqual(row["last_seen_at"], leased[0].delivered_at)

    async def test_poll_hydration_failure_rolls_back_heartbeat_and_claims(self):
        request = await self.enqueue()
        with patch.object(self.state, "_loaded_request", side_effect=ValueError("bad payload")):
            with self.assertRaisesRegex(ValueError, "bad payload"):
                await self.poll(worker_id="failed-worker")
        async with self.state.store.transaction("test_rollback") as conn:
            count = await (await conn.execute(
                "SELECT count(*) AS n FROM relay_workers WHERE worker_id='failed-worker'",
            )).fetchone()
        self.assertEqual(count["n"], 0)
        (leased,) = await self.poll()
        self.assertEqual(leased.request_id, request.request_id)
        self.assertEqual(leased.delivery_count, 1)

    async def test_poll_with_stale_registration_does_not_write_worker_heartbeat(self):
        await self.enqueue()
        await self.state.register_rollout("agent")
        with self.assertRaises(web.HTTPConflict):
            await self.poll(worker_id="stale-worker")
        async with self.state.store.transaction("test_no_stale_heartbeat") as conn:
            count = await (await conn.execute(
                "SELECT count(*) AS n FROM relay_workers WHERE worker_id='stale-worker'",
            )).fetchone()
        self.assertEqual(count["n"], 0)

    async def test_registration_replacement_fences_old_leases_and_cancels_waiter(self):
        req = await self.enqueue()
        (leased,) = await self.poll()
        peer = await self.new_state()
        await peer.register_rollout("agent")
        with self.assertRaises(web.HTTPConflict):
            await self.respond(leased)
        self.assertEqual(
            (await self.state.wait_for_response(req, timeout_seconds=1)).status, 410
        )

    async def test_conflicting_result_is_not_silently_accepted(self):
        await self.enqueue()
        (req,) = await self.poll()
        await self.respond(req)
        with self.assertRaises(web.HTTPConflict):
            await self.state.respond(
                request_id=req.request_id,
                registration_token=self.token,
                lease_id=req.lease_id,
                response=api.RelayWorkerResponse(200, b"different"),
            )

    async def test_expired_inference_lease_requeues_without_losing_request(self):
        await self.enqueue()
        (req,) = await self.poll(lease_seconds=0.001)
        await asyncio.sleep(0.01)
        with self.assertRaises(web.HTTPConflict):
            await self.respond(req)
        (again,) = await self.poll()
        self.assertEqual(req.request_id, again.request_id)
        self.assertNotEqual(req.lease_id, again.lease_id)
        await self.respond(again)

    async def test_implicit_calls_remain_distinct_until_disconnect(self):
        first = await self.enqueue(
            idempotency_key="implicit", defer_idempotency_until_disconnect=True
        )
        second = await self.enqueue(
            idempotency_key="implicit", defer_idempotency_until_disconnect=True
        )
        self.assertNotEqual(first.request_id, second.request_id)
        await self.state.mark_caller_detached(first.request_id)
        replay = await self.enqueue(
            idempotency_key="implicit", defer_idempotency_until_disconnect=True
        )
        self.assertEqual(first.request_id, replay.request_id)
        await self.state.mark_caller_detached(second.request_id)

    async def test_storage_budget_is_admission_only_and_completed_result_survives(self):
        state = await self.new_state(storage_budget_bytes=RESPONSE_RESERVATION + 1000)
        await self.enqueue(state)
        with self.assertRaises(web.HTTPTooManyRequests):
            await self.enqueue(state)
        (req,) = await self.poll(state)
        await self.respond(req, state)
        self.assertEqual(
            (await state.wait_for_response(req, timeout_seconds=1)).body, b"answer"
        )

    async def bound_state(self, **kwargs):
        self.deployment = "bound"
        state = await self.new_state(**kwargs)
        self.reg = await state.register_rollout(
            "agent",
            {
                "sandbox_id": "s1",
                "sandbox_generation": 1,
                api.AGENT_LIFECYCLE_METADATA_KEY: api.MANAGED_AGENT_LIFECYCLE,
            },
        )
        self.token = self.reg["registration_token"]
        return state

    async def test_terminal_history_does_not_amplify_database_work(self):
        async def hold_wake(_request):
            await asyncio.Event().wait()
        state = await self.bound_state(result_notifier=hold_wake)
        request = await self.enqueue(state)
        self.assertEqual(await state.pending_caller_incarnations(), {("s1", 1)})
        history = {(f"old-{i}", 1): "node_lost" for i in range(10000)}
        # A previous incarnation must not cancel the current caller.
        history[("s1", 0)] = "node_lost"
        samples = []
        state.store.observe = lambda sample: (
            samples.append(sample) if sample.operation.startswith("relay_terminal_") else None
        )
        await state.reconcile_unavailable_callers(history)
        self.assertLessEqual(len(samples), 2)
        (leased,) = await self.poll(state)
        await self.respond(leased, state, defer_delivery=True)
        self.assertEqual(await state.pending_caller_incarnations(), {("s1", 1)})
        samples.clear()
        history[("s1", 1)] = "node_lost"
        await state.reconcile_unavailable_callers(history)
        self.assertLessEqual(len(samples), 3)
        self.assertEqual(
            (await state.wait_for_response(request, timeout_seconds=1)).body,
            b"answer",
        )
        self.assertEqual((await state.stats())["delivery_pending"], 0)
        self.assertEqual(await state.pending_caller_incarnations(), set())
        samples.clear()
        await state.reconcile_unavailable_callers(history)
        self.assertLessEqual(len(samples), 2)

    async def test_committed_result_releases_queued_park_even_without_notifications(self):
        entered, skipped, woke = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def park(request):
            entered.set()
            await request.response_committed.wait()
            skipped.set()

        async def wake(request):
            woke.set()
            return "epoch"

        state = await self.bound_state(
            accepted_notifier=park, result_notifier=wake, lifecycle_concurrency=1,
        )
        request = await self.enqueue(state)
        await asyncio.wait_for(entered.wait(), 2)
        (leased,) = await self.poll(state)
        # Notifications are hints. The bounded durable read must also wake the
        # queued park after a lost hint.
        async def no_notify(_conn, *_keys):
            pass

        state._notify = no_notify
        result = await self.respond(leased, state, defer_delivery=True)
        await asyncio.wait_for(skipped.wait(), 2)
        await asyncio.wait_for(woke.wait(), 2)
        await state.wait_for_delivery(result.request, timeout_seconds=2)
        self.assertEqual((await state.wait_for_response(request, timeout_seconds=2)).body, b"answer")

    async def test_wake_progresses_while_park_dispatch_is_full(self):
        entered, release, woke = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def park(request):
            entered.set()
            await release.wait()
            return "old-epoch"

        async def wake(request):
            woke.set()
            return "new-epoch"

        state = await self.bound_state(
            accepted_notifier=park, result_notifier=wake, lifecycle_concurrency=1,
        )
        await self.enqueue(state)
        await asyncio.wait_for(entered.wait(), 2)
        (leased,) = await self.poll(state)
        try:
            result = await self.respond(leased, state, defer_delivery=True)
            await asyncio.wait_for(woke.wait(), 2)
            await state.wait_for_delivery(result.request, timeout_seconds=2)
            self.assertFalse(release.is_set())
        finally:
            release.set()

    async def test_deferred_park_releases_durable_claim_without_losing_intent(self):
        entered = asyncio.Event()

        async def park(request):
            entered.set()
            raise api.RelayLifecycleDeferred(30, transport_epoch="original")

        async def wake(request):
            return "migrated"

        state = await self.bound_state(accepted_notifier=park, result_notifier=wake)
        request = await self.enqueue(state, idempotency_key="local-park-migration",
                                     defer_idempotency_until_disconnect=True)
        await asyncio.wait_for(entered.wait(), 2)
        for _ in range(100):
            async with state.store.transaction("test_deferred") as conn:
                row = await (await conn.execute(
                    "SELECT claim_token,done,last_error,next_attempt_at>clock_timestamp() AS deferred FROM relay_lifecycle WHERE request_id=%s",
                    (request.request_id,),
                )).fetchone()
            if row["claim_token"] is None:
                break
            await asyncio.sleep(.01)
        self.assertIsNone(row["claim_token"])
        self.assertFalse(row["done"])
        self.assertIsNone(row["last_error"])
        self.assertTrue(row["deferred"])
        async with state.store.transaction("test_deferred_transport") as conn:
            observed = await (await conn.execute(
                "SELECT parked_transport_epoch FROM relay_requests WHERE request_id=%s",
                (request.request_id,),
            )).fetchone()
        self.assertEqual(observed["parked_transport_epoch"], "original")

        (leased,) = await self.poll(state)
        await self.respond(leased, state, defer_delivery=True)
        self.assertEqual((await state.wait_for_response(request, timeout_seconds=2)).body, b"answer")
        async with state.store.transaction("test_migrated_deferred_park") as conn:
            observed = await (await conn.execute(
                "SELECT reattachable FROM relay_requests WHERE request_id=%s",
                (request.request_id,),
            )).fetchone()
        self.assertTrue(observed["reattachable"])

    async def test_result_and_wake_commit_atomically_and_retry_without_client(self):
        attempts = 0
        parked = asyncio.Event()

        async def park(req):
            parked.set()
            return "epoch"

        async def wake(req):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise OSError("temporary overload")
            return "epoch"

        state = await self.bound_state(accepted_notifier=park, result_notifier=wake)
        req = await self.enqueue(state)
        await asyncio.wait_for(parked.wait(), 3)
        (leased,) = await self.poll(state)
        await self.respond(leased, state, defer_delivery=True)
        self.assertEqual(
            (await state.wait_for_response(req, timeout_seconds=5)).body, b"answer"
        )
        self.assertEqual(attempts, 3)
        self.assertEqual((await state.stats())["delivery_pending"], 0)

    async def test_commit_failure_rolls_back_both_result_and_intent(self):
        async def wake(req):
            return None

        state = await self.bound_state(result_notifier=wake)
        await self.enqueue(state)
        (req,) = await self.poll(state)
        async with state.store.transaction("inject") as conn:
            await conn.execute(
                "CREATE FUNCTION fail_wake() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$"
            )
            await conn.execute(
                "CREATE TRIGGER fail_wake BEFORE INSERT ON relay_lifecycle FOR EACH ROW EXECUTE FUNCTION fail_wake()"
            )
        with self.assertRaises(psycopg.errors.RaiseException):
            await self.respond(req, state, defer_delivery=True)
        async with state.store.transaction("inspect") as conn:
            self.assertEqual(
                (
                    await (
                        await conn.execute("SELECT count(*) AS n FROM relay_results")
                    ).fetchone()
                )["n"],
                0,
            )
        self.assertEqual((await state.stats())["inflight"], 1)

    async def test_process_replacement_recovers_uncertain_wake_same_identity(self):
        started = asyncio.Event()
        seen = []

        async def uncertain(req):
            seen.append(req.request_id)
            started.set()
            await asyncio.Event().wait()

        state = await self.bound_state(
            result_notifier=uncertain, lifecycle_lease_seconds=0.15
        )
        req = await self.enqueue(state)
        (leased,) = await self.poll(state)
        await self.respond(leased, state, defer_delivery=True)
        await asyncio.wait_for(started.wait(), 2)
        await state.aclose()
        self.states.remove(state)

        async def replay(req):
            seen.append(req.request_id)
            return None

        peer = await self.new_state(
            result_notifier=replay, lifecycle_lease_seconds=0.15
        )
        result = await peer.wait_for_response(req, timeout_seconds=3)
        self.assertEqual(result.body, b"answer")
        # The deliberately short lease can expire again on a busy host. Dispatch
        # is at least once: retries must preserve identity, not promise exactly
        # two network attempts under every scheduler/fsync delay.
        self.assertGreaterEqual(len(seen), 2)
        self.assertEqual(set(seen), {req.request_id})
        self.assertEqual((await peer.stats())["delivery_pending"], 0)

    async def test_no_pool_connection_held_during_wake(self):
        entered = asyncio.Event()
        release = asyncio.Event()

        async def wake(req):
            entered.set()
            await release.wait()

        state = await self.bound_state(result_notifier=wake)
        req = await self.enqueue(state)
        (leased,) = await self.poll(state)
        await self.respond(leased, state, defer_delivery=True)
        await asyncio.wait_for(entered.wait(), 2)
        held = []
        try:
            for _ in range(4):
                held.append(await asyncio.wait_for(state.store.pool.getconn(), 1))
        finally:
            for conn in held:
                await state.store.pool.putconn(conn)
            release.set()
        await state.wait_for_response(req, timeout_seconds=2)

    async def test_does_not_park_after_result_committed_before_dispatch(self):
        park_calls = []

        async def park(req):
            park_calls.append(req.request_id)

        async def wake(req):
            return None

        state = await self.bound_state(accepted_notifier=park, result_notifier=wake)
        state._tasks[1].cancel()
        await asyncio.gather(state._tasks[1], return_exceptions=True)
        req = await self.enqueue(state)
        (leased,) = await self.poll(state)
        await self.respond(leased, state, defer_delivery=True)
        state._tasks[1] = asyncio.create_task(state._dispatch_loop())
        await state.wait_for_response(req, timeout_seconds=3)
        self.assertEqual(park_calls, [])

    async def test_two_http_servers_share_authority(self):
        for _ in range(2):
            store = PostgresControlStore(
                DSN, "http", schema=self.schema, max_connections=4
            )
            app = api.create_model_relay_app(
                postgres_store=store,
                request_timeout_seconds=5,
                sandbox_bearer_token="sandbox",
                worker_bearer_token="worker",
            )
            client = TestClient(TestServer(app))
            await client.start_server()
            self.clients.append(client)
        one, two = self.clients
        async with one.post(
            "/v1/relay/rollouts",
            json={"rollout_id": "http"},
            headers={"Authorization": "Bearer worker"},
        ) as response:
            self.assertEqual(response.status, 201, await response.text())
            record = await response.json()
        token = record["rollout"]["registration_token"]
        call = asyncio.create_task(
            one.post(
                "/tunnels/http/hello",
                data=b"request",
                headers={
                    "Authorization": "Bearer sandbox",
                    api.RELAY_REQUEST_ID_HEADER: "http-key",
                },
            )
        )
        async with two.get(
            "/worker/poll",
            params={
                "rollout_id": "http",
                "registration_token": token,
                "timeout_seconds": 3,
            },
            headers={"Authorization": "Bearer worker"},
        ) as response:
            self.assertEqual(response.status, 200, await response.text())
            (request,) = (await response.json())["requests"]
        async with two.post(
            "/worker/respond",
            json={
                "request_id": request["request_id"],
                "registration_token": token,
                "lease_id": request["lease_id"],
                "status": 201,
                "body": api._encoded_body(b"http-answer"),
            },
            headers={"Authorization": "Bearer worker"},
        ) as response:
            self.assertEqual(response.status, 200, await response.text())
        response = await call
        self.assertEqual(response.status, 201)
        self.assertEqual(await response.read(), b"http-answer")
        async with two.get(
            "/v1/relay/stats", headers={"Authorization": "Bearer worker"}
        ) as response:
            self.assertEqual(response.status, 200, await response.text())

    async def test_idle_cutover_preserves_tokens_and_replay_and_fences_legacy_writer(
        self,
    ):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from ucloud_sandboxes.shared_control.migration import (
            import_idle_relay,
            assert_relay_cutover,
        )

        with TemporaryDirectory() as raw:
            path = (Path(raw) / "relay.sqlite").resolve()
            old = api.ModelRelayState(state_path=path)
            reg = await old.register_rollout("imported")
            req = await old.enqueue(
                rollout_id="imported",
                endpoint="/v1/responses",
                body={"q": 1},
                headers={},
                idempotency_key="replay",
            )
            (leased,) = await old.poll(
                rollout_id="imported",
                registration_token=reg["registration_token"],
                timeout_seconds=0,
            )
            await old.respond(
                request_id=req.request_id,
                registration_token=reg["registration_token"],
                lease_id=leased.lease_id,
                response=api.RelayWorkerResponse(200, b"sampled-once"),
            )
            store = PostgresControlStore(DSN, "import", schema=self.schema)
            await store.open()
            try:
                summary = await import_idle_relay(store, path)
                self.assertEqual(summary["responses"], 1)
                self.assertEqual(summary, await import_idle_relay(store, path))
                assert_relay_cutover(path, "import", self.schema)
                with self.assertRaisesRegex(ValueError, "fenced"):
                    await old.register_rollout("must-not-write")
                with self.assertRaisesRegex(ValueError, "fenced"):
                    api.RelaySqliteStore(path)
            finally:
                await old.aclose()
                await store.close()
            state = PostgresRelayState(
                PostgresControlStore(DSN, "import", schema=self.schema)
            )
            await state.open()
            self.states.append(state)
            await state.require_current_registration(
                "imported", reg["registration_token"]
            )
            replay = await state.enqueue(
                rollout_id="imported",
                endpoint="/v1/responses",
                body={"q": 1},
                headers={},
                idempotency_key="replay",
            )
            self.assertEqual(replay.request_id, req.request_id)
            self.assertEqual(
                (await state.wait_for_response(replay, timeout_seconds=1)).body,
                b"sampled-once",
            )

    async def test_import_refuses_active_requests_without_changing_source(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from ucloud_sandboxes.shared_control.migration import import_idle_relay

        with TemporaryDirectory() as raw:
            path = (Path(raw) / "relay.sqlite").resolve()
            old = api.ModelRelayState(state_path=path)
            await old.register_rollout("busy")
            await old.enqueue(
                rollout_id="busy", endpoint="/v1/responses", body={}, headers={}
            )
            with self.assertRaisesRegex(ValueError, "idle"):
                await import_idle_relay(self.state.store, path)
            await old.register_rollout("still-works")
            await old.aclose()

    async def test_gc_reclaims_reserved_space_without_losing_retained_response(self):
        state = await self.new_state(
            storage_budget_bytes=RESPONSE_RESERVATION + 1000,
            completed_request_retention_seconds=0.01,
        )
        await self.enqueue(state)
        (req,) = await self.poll(state)
        await self.respond(req, state)
        await state.maintain()
        self.assertLess((await state.stats())["reserved_storage_bytes"], 100000)
        await asyncio.sleep(0.02)
        await state.maintain()
        self.assertEqual((await state.stats())["reserved_storage_bytes"], 0)

    async def test_wake_does_not_wait_for_park_ack_and_late_transport_proof_reattaches(
        self,
    ):
        entered = asyncio.Event()
        finish = asyncio.Event()

        async def park(request):
            entered.set()
            await finish.wait()
            return "old-transport"

        async def wake(request):
            return "new-transport"

        state = await self.bound_state(accepted_notifier=park, result_notifier=wake)
        req = await self.enqueue(
            state, idempotency_key="implicit", defer_idempotency_until_disconnect=True
        )
        await asyncio.wait_for(entered.wait(), 2)
        (leased,) = await self.poll(state)
        await self.respond(leased, state, defer_delivery=True)
        self.assertEqual(
            (await state.wait_for_response(req, timeout_seconds=2)).body, b"answer"
        )
        finish.set()
        for _ in range(100):
            async with state.store.transaction("test_park_completion") as conn:
                row = await (
                    await conn.execute(
                        "SELECT reattachable FROM relay_requests WHERE request_id=%s",
                        (req.request_id,),
                    )
                ).fetchone()
            if row["reattachable"]:
                break
            await asyncio.sleep(0.01)
        self.assertTrue(row["reattachable"])
        self.assertEqual(
            (
                await self.enqueue(
                    state,
                    idempotency_key="implicit",
                    defer_idempotency_until_disconnect=True,
                )
            ).request_id,
            req.request_id,
        )

    async def test_lost_notifications_still_deliver_committed_response(self):
        peer = await self.new_state()
        # Both listener and publisher can disappear without losing queue state.
        for state in (self.state, peer):
            state._tasks[0].cancel()
            state._tasks[-1].cancel()
            await asyncio.gather(
                state._tasks[0], state._tasks[-1], return_exceptions=True
            )
        req = await self.enqueue()
        (leased,) = await self.poll(peer)
        waiting = asyncio.create_task(
            self.state.wait_for_response(req, timeout_seconds=2)
        )
        await asyncio.sleep(0.02)
        await self.respond(leased, peer)
        self.assertEqual((await waiting).body, b"answer")

    async def test_timeout_wakes_parked_caller_to_deliver_terminal_error(self):
        wake = asyncio.Event()

        async def notifier(request):
            wake.set()

        state = await self.bound_state(
            result_notifier=notifier, request_timeout_seconds=0.01
        )
        req = await self.enqueue(state)
        await asyncio.sleep(0.02)
        await state.maintain()
        await asyncio.wait_for(wake.wait(), 2)
        self.assertEqual(
            (await state.wait_for_response(req, timeout_seconds=2)).status, 504
        )

    async def test_mismatched_peer_lifecycle_configuration_is_rejected(self):
        async def wake(request):
            return None

        peer = PostgresRelayState(
            PostgresControlStore(DSN, "test", schema=self.schema), result_notifier=wake
        )
        with self.assertRaisesRegex(ValueError, "disagree"):
            await peer.open()

    async def test_source_fence_cannot_activate_an_empty_other_database(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from ucloud_sandboxes.shared_control.migration import import_idle_relay

        with TemporaryDirectory() as raw:
            path = (Path(raw) / "relay.sqlite").resolve()
            old = api.ModelRelayState(state_path=path)
            await old.register_rollout("old")
            await old.aclose()
            imported = PostgresControlStore(DSN, "import", schema=self.schema)
            await imported.open()
            try:
                await import_idle_relay(imported, path)
                async with imported.transaction("simulate_wrong_empty_target") as conn:
                    await conn.execute(
                        "DELETE FROM relay_imports WHERE deployment_id='import'"
                    )
                    await conn.execute(
                        "DELETE FROM relay_rollouts WHERE deployment_id='import'"
                    )
                with self.assertRaisesRegex(ValueError, "resurrect"):
                    await import_idle_relay(imported, path)
            finally:
                await imported.close()

    async def test_reattach_reads_one_snapshot_while_peer_commits_result(self):
        peer = await self.new_state()
        original = await self.enqueue(idempotency_key="interleaved")
        (leased,) = await self.poll()
        test = self
        async with self.state.store.transaction("interleaved_reader") as conn:

            class InterleavedConnection:
                first = True

                async def execute(self, query, params):
                    cursor = await conn.execute(query, params)
                    if self.first:
                        self.first = False
                        # Real peer transaction commits after the first SELECT
                        # snapshot, before the reader consumes its result.
                        await test.respond(leased, peer)
                    return cursor

            reattached = await self.state._load(
                InterleavedConnection(), original.request_id
            )
        self.assertEqual(reattached.request_id, original.request_id)
        self.assertEqual(
            (await self.state.wait_for_response(reattached, timeout_seconds=2)).body,
            b"answer",
        )

    async def test_worker_delivery_wait_does_not_read_response_body(self):
        release = asyncio.Event()

        async def wake(request):
            await release.wait()
            return "epoch"

        state = await self.bound_state(result_notifier=wake)
        req = await self.enqueue(state)
        (leased,) = await self.poll(state)
        result = await self.respond(leased, state, defer_delivery=True)
        samples = []
        state.store.observe = samples.append
        waiter = asyncio.create_task(
            state.wait_for_delivery(result.request, timeout_seconds=3)
        )
        await asyncio.sleep(0.02)
        self.assertFalse(waiter.done())
        release.set()
        await waiter
        self.assertNotIn("relay_delivery_bodies", [s.operation for s in samples])
        self.assertEqual(
            (await state.wait_for_response(req, timeout_seconds=3)).body, b"answer"
        )

    async def test_http_delivery_timeout_preserves_committed_result_for_retry(self):
        release = asyncio.Event()

        async def wake(request):
            await release.wait()
            return "epoch"

        app = api.create_model_relay_app(
            postgres_store=PostgresControlStore(
                DSN, "http-timeout", schema=self.schema
            ),
            request_timeout_seconds=1,
            result_notifier=wake,
            worker_bearer_token="worker",
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        self.clients.append(client)
        state = app[api.STATE_KEY]
        reg = await state.register_rollout(
            "agent",
            {
                "sandbox_id": "s1",
                "sandbox_generation": 1,
                api.AGENT_LIFECYCLE_METADATA_KEY: api.MANAGED_AGENT_LIFECYCLE,
            },
        )
        request = await self.enqueue(state)
        (leased,) = await state.poll(
            rollout_id="agent",
            registration_token=reg["registration_token"],
            timeout_seconds=0,
        )
        payload = {
            "request_id": request.request_id,
            "registration_token": reg["registration_token"],
            "lease_id": leased.lease_id,
            "body": api._encoded_body(b"accepted-once"),
        }
        async with client.post(
            "/worker/respond", json=payload, headers={"Authorization": "Bearer worker"}
        ) as response:
            self.assertEqual(response.status, 504)
            self.assertTrue((await response.json())["committed"])
        self.assertEqual((await state.stats())["delivery_pending"], 1)
        release.set()
        async with client.post(
            "/worker/respond", json=payload, headers={"Authorization": "Bearer worker"}
        ) as response:
            self.assertEqual(response.status, 200)
            self.assertTrue((await response.json())["duplicate"])
        self.assertEqual(
            (await state.wait_for_response(request, timeout_seconds=2)).body,
            b"accepted-once",
        )

    async def test_qualification_subapp_keeps_live_registrations_separate(self):
        from dataclasses import replace
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from scripts.serve_relay_qualification import combined_app
        from ucloud_sandboxes.config import DeploymentConfig

        with TemporaryDirectory() as directory:
            config = replace(DeploymentConfig.default(), data_root=directory)
            for path in (
                config.gateway_token_file(),
                config.relay_sandbox_token_file(),
                config.relay_worker_token_file(),
            ):
                path.write_text("test")
            store = PostgresControlStore(DSN, "qualification", schema=self.schema)
            client = TestClient(
                TestServer(combined_app(config, store, "/qualification-test"))
            )
            await client.start_server()
            try:
                headers = {"Authorization": "Bearer test"}
                async with client.post(
                    "/v1/relay/rollouts",
                    json={"rollout_id": "existing"},
                    headers=headers,
                ) as response:
                    self.assertEqual(response.status, 201, await response.text())
                async with client.post(
                    "/qualification-test/v1/relay/rollouts",
                    json={"rollout_id": "existing"},
                    headers=headers,
                ) as response:
                    self.assertNotEqual(response.status, 201)
                async with client.post(
                    "/qualification-test/v1/relay/rollouts",
                    json={
                        "rollout_id": "relay-load-test",
                        "metadata": {
                            "sandbox_id": "relay-load-test",
                            "sandbox_generation": 1,
                            api.AGENT_LIFECYCLE_METADATA_KEY: api.MANAGED_AGENT_LIFECYCLE,
                        },
                    },
                    headers=headers,
                ) as response:
                    self.assertEqual(response.status, 201, await response.text())
                for prefix, expected in [
                    ("", ["existing"]),
                    ("/qualification-test", ["relay-load-test"]),
                ]:
                    async with client.get(
                        prefix + "/v1/relay/rollouts", headers=headers
                    ) as response:
                        rows = (await response.json())["rollouts"]
                        self.assertEqual([r["rollout_id"] for r in rows], expected)
                self.assertTrue(Path(config.relay_state_file()).exists())
            finally:
                await client.close()
