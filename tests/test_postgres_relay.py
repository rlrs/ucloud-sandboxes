"""Run against real PostgreSQL on Linux; HTTP tests use the production app."""

from contextlib import asynccontextmanager, closing
import asyncio
import importlib.util
import os
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from ucloud_sandboxes import model_relay as api

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")
if DSN:
    import psycopg
    from psycopg import sql
    from psycopg_pool import PoolTimeout
    from ucloud_sandboxes.shared_control.database import PostgresDatabase
    from ucloud_sandboxes.shared_control.model import DatabaseAdmissionUnavailable
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
        admin = PostgresDatabase(DSN, "test", schema=self.schema)
        await admin.open()
        await admin.migrate()
        await admin.close()
        self.state = await self.new_state()
        self.reg = await self.state.register_rollout("agent")
        self.token = self.reg["registration_token"]

    async def new_state(self, **kwargs):
        store = PostgresDatabase(
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
            timeout_seconds=kwargs.pop("timeout_seconds", 0.1),
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

    async def test_lifecycle_dispatch_uses_configured_exporter(self):
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
        from ucloud_sandboxes.telemetry import Telemetry
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        telemetry = Telemetry.disabled("relay-test")
        telemetry.tracer = provider.get_tracer("relay-test")
        async def wake(_request):
            with telemetry.span("test.worker.wake"):
                return "epoch"
        try:
            state = await self.bound_state(result_notifier=wake, telemetry=telemetry)
            request = await self.enqueue(state)
            (leased,) = await self.poll(state)
            await self.respond(leased, state, defer_delivery=True)
            await state.wait_for_response(request, timeout_seconds=2)
            spans = exporter.get_finished_spans()
            dispatch = next(s for s in spans if s.name == "relay.lifecycle.dispatch"
                            and s.attributes["relay.lifecycle.action"] == "wake")
            child = next(s for s in spans if s.name == "test.worker.wake")
            transport = next(s for s in spans if s.name == "relay.lifecycle.wake.http")
            self.assertEqual(transport.parent.span_id, dispatch.context.span_id)
            self.assertEqual(child.parent.span_id, transport.context.span_id)
            self.assertIn("relay.lifecycle.response_age_seconds", dispatch.attributes)
            self.assertIn("relay.lifecycle.due_wait_seconds", dispatch.attributes)
        finally:
            provider.shutdown()

    async def test_registration_precheck_is_read_only_without_transaction_id(self):
        store = self.state.store
        original = store.transaction

        @asynccontextmanager
        async def read_only(operation):
            async with original(operation) as conn:
                await conn.execute("SET TRANSACTION READ ONLY")
                yield conn
                row = await (await conn.execute(
                    "SELECT pg_current_xact_id_if_assigned() AS xid"
                )).fetchone()
                self.assertIsNone(row["xid"])

        with patch.object(store, "transaction", read_only):
            await self.state.require_current_registration("agent", self.token)
            with self.assertRaises(web.HTTPUnauthorized):
                await self.state.require_current_registration("agent", "0" * 32)

    async def test_tunnel_precheck_cannot_cross_registration_replacement(self):
        client, state = await self.completion_client(None)
        old = await state.register_rollout("replaced")
        original = state.require_current_registration

        async def replace_after_precheck(rollout_id, token):
            await original(rollout_id, token)
            await state.register_rollout(rollout_id)

        with patch.object(state, "require_current_registration", replace_after_precheck):
            response = await client.post(
                f"/tunnels/replaced/_relay/{old['registration_token']}/v1/responses",
                json={"question": "must not enter replacement registration"},
            )
        self.assertEqual(response.status, 409, await response.text())
        self.assertEqual((await state.stats())["inflight"], 0)

    async def seed_renewal_claims(self, count):
        async with self.state.store.transaction("seed_claims") as conn:
            await conn.execute(
                """INSERT INTO relay_requests
                (deployment_id,request_id,rollout_id,registration_token,endpoint,method,
                created_at,expires_at,payload_bytes,reserved_bytes,state,request_digest,
                reattachable,delivery_pending)
                SELECT %s,'renew-'||i,'agent',%s,'/test','POST',0,1e12,0,0,'completed','digest',false,true
                FROM generate_series(1,%s) i""",
                (self.state.deployment, self.token, count),
            )
            rows = await (await conn.execute(
                """INSERT INTO relay_lifecycle
                (deployment_id,request_id,action,claim_token,claim_until,attempts)
                SELECT deployment_id,request_id,'wake',gen_random_uuid(),clock_timestamp()+interval '30 seconds',1
                FROM relay_requests WHERE request_id LIKE 'renew-%%'
                RETURNING request_id,action,claim_token,claim_until""",
            )).fetchall()
        self.state._active_claims.update({(r["request_id"], r["action"]): r["claim_token"] for r in rows})
        return rows

    async def test_stats_separate_due_deferred_and_claimed_work(self):
        await self.seed_renewal_claims(4)
        async with self.state.store.transaction("arrange_queue_states") as conn:
            await conn.execute("""UPDATE relay_lifecycle SET claim_token=NULL,claim_until=NULL,
                next_attempt_at=clock_timestamp()-interval '10 seconds', last_error='TimeoutError'
                WHERE request_id='renew-1'""")
            await conn.execute("""UPDATE relay_lifecycle SET claim_token=NULL,claim_until=NULL,
                next_attempt_at=clock_timestamp()+interval '30 seconds'
                WHERE request_id='renew-2'""")
            await conn.execute("""UPDATE relay_lifecycle SET claim_until=clock_timestamp()-interval '1 second',
                next_attempt_at=clock_timestamp()-interval '5 seconds'
                WHERE request_id='renew-3'""")
        lifecycle = (await self.state.stats())["lifecycle"]
        self.assertEqual(len(lifecycle), 1)
        row = lifecycle[0]
        self.assertEqual((row['n'], row['due'], row['deferred'], row['claimed']), (4, 2, 1, 1))
        self.assertEqual(row['retrying_after_error'], 1)
        self.assertGreaterEqual(row['oldest_due_seconds'], 10)
        self.assertLess(row['oldest_due_seconds'], 12)

    async def test_512_lifecycle_renewals_share_one_commit_and_fence_lost_owners(self):
        rows = await self.seed_renewal_claims(512)
        lost, done = rows[:2]
        replacement = uuid4()
        async with self.state.store.transaction("peer_ownership") as conn:
            await conn.execute("UPDATE relay_lifecycle SET claim_token=%s WHERE request_id=%s",
                               (replacement, lost["request_id"]))
            await conn.execute("UPDATE relay_lifecycle SET done=true WHERE request_id=%s",
                               (done["request_id"],))
        samples = []
        self.state.store.observe = samples.append
        await self.state._renew_lifecycle_claims()
        self.assertEqual(len([s for s in samples if s.operation == "relay_renew_lifecycle"]), 1)
        self.assertEqual(len(self.state._active_claims), 510)
        async with self.state.store.transaction("inspect_claims") as conn:
            current = await (await conn.execute("SELECT * FROM relay_lifecycle ORDER BY request_id")).fetchall()
            requests = await (await conn.execute("SELECT bool_and(delivery_pending) AS pending FROM relay_requests")).fetchone()
        self.assertTrue(requests["pending"])
        before = {r["request_id"]: r for r in rows}
        for row in current:
            original = before[row["request_id"]]
            self.assertEqual(row["attempts"], 1)
            if row["request_id"] in {lost["request_id"], done["request_id"]}:
                self.assertEqual(row["claim_until"], original["claim_until"])
            else:
                self.assertGreater(row["claim_until"], original["claim_until"])
                self.assertEqual(row["claim_token"], original["claim_token"])
        self.assertEqual(next(r for r in current if r["request_id"] == lost["request_id"])["claim_token"], replacement)

    async def test_batch_renewal_cancellation_and_uncertain_commit_never_complete_work(self):
        rows = await self.seed_renewal_claims(4)
        # Lifecycle renewal runs on the dedicated lifecycle/delivery pool.
        store = self.state._delivery_store
        original = store.transaction
        for cancel_before_commit in (True, False):
            @asynccontextmanager
            async def uncertain(operation):
                async with original(operation) as conn:
                    yield conn
                    if cancel_before_commit:
                        raise asyncio.CancelledError
                raise RuntimeError("commit acknowledgement lost")

            with patch.object(store, "transaction", uncertain):
                with self.assertRaises(asyncio.CancelledError if cancel_before_commit else RuntimeError):
                    await self.state._renew_lifecycle_claims()
            self.assertEqual(len(self.state._active_claims), 4)
            async with original("inspect") as conn:
                current = await (await conn.execute("SELECT * FROM relay_lifecycle ORDER BY request_id")).fetchall()
            before = {r["request_id"]: r for r in rows}
            for row in current:
                self.assertFalse(row["done"])
                self.assertEqual(row["attempts"], 1)
                self.assertEqual(row["claim_token"], before[row["request_id"]]["claim_token"])
                if cancel_before_commit:
                    self.assertEqual(row["claim_until"], before[row["request_id"]]["claim_until"])
        await self.state._renew_lifecycle_claims()

    async def test_batch_loop_keeps_live_wake_owned_until_shutdown(self):
        entered, release = asyncio.Event(), asyncio.Event()
        seen = []

        async def wake(req):
            seen.append(req.request_id)
            entered.set()
            await release.wait()

        state = await self.bound_state(result_notifier=wake, lifecycle_lease_seconds=0.6)
        request = await self.enqueue(state)
        (leased,) = await self.poll(state)
        await self.respond(leased, state, defer_delivery=True)
        await asyncio.wait_for(entered.wait(), 2)
        peer = await self.new_state(result_notifier=wake, lifecycle_lease_seconds=0.6)
        await asyncio.sleep(1.5)
        self.assertEqual(seen, [request.request_id])
        self.assertEqual((await peer.stats())["delivery_pending"], 1)
        release.set()
        self.assertEqual((await state.wait_for_response(request, timeout_seconds=2)).body, b"answer")
        self.assertFalse(state._active_claims)

    async def test_terminal_node_loss_retires_pending_and_leased_callers(self):
        state = await self.bound_state()
        first = await self.enqueue(state)
        (leased,) = await self.poll(state)
        second = await self.enqueue(state)
        await state.reconcile_unavailable_callers({("s1", 1): "node_lost"})
        for request in (first, second):
            response = await state.wait_for_response(request, timeout_seconds=1)
            self.assertEqual(response.status, 410)
        with self.assertRaises(web.HTTPConflict):
            await self.respond(leased, state)
        self.assertEqual(await state.pending_caller_incarnations(), set())

    async def test_registration_metadata_rejects_aliases_and_generation_coercion(self):
        invalid = [{"sandboxId": "s", "sandboxGeneration": 1},
                   {"sandbox_id": "s"}, {"sandbox_generation": 1}]
        invalid += [{"sandbox_id": "s", "sandbox_generation": value,
                     api.AGENT_LIFECYCLE_METADATA_KEY: api.MANAGED_AGENT_LIFECYCLE}
                    for value in (True, 1.0, "1", 0, -1)]
        for metadata in invalid:
            with self.subTest(metadata=metadata), self.assertRaises(web.HTTPBadRequest):
                await self.state.register_rollout("invalid", metadata)
        self.assertEqual([row["rollout_id"] for row in await self.state.list_rollouts()], ["agent"])

    async def test_pending_delivery_cannot_expire_from_retention_gc(self):
        wake = asyncio.Event()
        async def hold_wake(_request):
            await wake.wait()
        state = await self.bound_state(result_notifier=hold_wake,
                                       completed_request_retention_seconds=0.001)
        request = await self.enqueue(state)
        (leased,) = await self.poll(state)
        await self.respond(leased, state, defer_delivery=True)
        async with state.store.transaction("test_age_completed") as conn:
            await conn.execute("UPDATE relay_requests SET completed_at=completed_at-3600 WHERE deployment_id=%s AND request_id=%s", (state.deployment, request.request_id))
        await state.maintain()
        self.assertEqual((await state.stats())["delivery_pending"], 1)
        peer = await self.new_state(result_notifier=hold_wake,
                                    completed_request_retention_seconds=0.001)
        await peer.release_completed_response(request.request_id)
        self.assertEqual((await peer.wait_for_response(request, timeout_seconds=1)).body, b"answer")
        await peer.maintain()
        # Explicit delivery release cannot delete an unfinished lifecycle obligation.
        self.assertGreater((await peer.stats())["reserved_storage_bytes"], 0)
        wake.set()
        async def reclaimed():
            while (await peer.stats())["reserved_storage_bytes"]:
                await peer.maintain()
                await asyncio.sleep(0.01)
        await asyncio.wait_for(reclaimed(), 2)

    async def test_live_migration_creates_only_relay_tables_and_status_is_read_only(self):
        from pathlib import Path
        from types import SimpleNamespace
        from ucloud_sandboxes.shared_control.__main__ import run

        async with self.state.store.transaction("test_live_tables") as conn:
            rows = await (await conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname=%s", (self.schema,),
            )).fetchall()
        self.assertTrue(rows)
        self.assertTrue(all(row["tablename"].startswith("relay_") for row in rows))
        args = SimpleNamespace(
            command="status", dsn_file=Path("/unused-test-dsn"),
            deployment_id="not-running", schema=self.schema,
        )
        with patch("ucloud_sandboxes.shared_control.credentials.read_private_dsn", return_value=DSN):
            status = await run(args)
        self.assertEqual(status["authority"], "relay")
        self.assertEqual(status["inflight"], 0)
        self.assertEqual(status["reserved_storage_bytes"], 0)
        async with self.state.store.transaction("test_status_no_write") as conn:
            row = await (await conn.execute(
                "SELECT count(*) AS n FROM relay_runtime_config WHERE deployment_id='not-running'",
            )).fetchone()
        self.assertEqual(row["n"], 0)

    async def test_live_migration_preserves_unrelated_qualification_tables(self):
        async with self.state.store.transaction("test_legacy_qualification") as conn:
            await conn.execute(
                "CREATE TABLE schema_version (version integer); "
                "INSERT INTO schema_version VALUES (99); "
                "CREATE TABLE nodes (identity text); INSERT INTO nodes VALUES ('keep')"
            )
        database = PostgresDatabase(DSN, "test", schema=self.schema)
        await database.open()  # Qualification versions do not govern live relay.
        try:
            await database.migrate()
            async with database.transaction("test_preserved") as conn:
                row = await (await conn.execute("SELECT identity FROM nodes")).fetchone()
            self.assertEqual(row["identity"], "keep")
        finally:
            await database.close()

    async def test_ready_wait_does_not_swallow_concurrent_cancellation(self):
        from ucloud_sandboxes.shared_control.relay import _wait_cancellable

        future = asyncio.get_running_loop().create_future()
        waiter = asyncio.create_task(_wait_cancellable(future, 5))
        await asyncio.sleep(0)  # Enter the wait before both events become ready.
        future.set_result("ready")
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter

    async def test_shutdown_does_not_restart_after_dependency_consumes_cancellation(self):
        from contextlib import asynccontextmanager

        entered = asyncio.Event()
        state = self.state
        transaction = state.store.transaction

        class Connection:
            async def execute(self, *args):
                return None

        @asynccontextmanager
        async def consumes_cancellation(operation):
            if operation != "relay_notify":
                async with transaction(operation) as conn:
                    yield conn
                return
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                pass  # Reproduce a dependency's ready/timeout cancellation race.
            yield Connection()

        with patch.object(state.store, "transaction", consumes_cancellation):
            state._notify_event.set()
            await asyncio.wait_for(entered.wait(), 2)
            await asyncio.wait_for(state.aclose(), 2)
        self.states.remove(state)
        self.assertTrue(all(task.done() for task in state._tasks))

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

    async def test_fresh_empty_polls_are_read_only_without_transaction_id(self):
        await self.state.record_worker_heartbeat(rollout_id='agent', registration_token=self.token,
                                                worker_id='idle')
        original = self.state.store.transaction

        @asynccontextmanager
        async def read_only(operation):
            async with original(operation) as conn:
                await conn.execute('SET TRANSACTION READ ONLY')
                yield conn
                row = await (await conn.execute('SELECT pg_current_xact_id_if_assigned() AS xid')).fetchone()
                self.assertIsNone(row['xid'])

        with patch.object(self.state.store, 'transaction', read_only):
            for worker_id in ('idle', None):
                self.assertEqual(await self.poll(timeout_seconds=0, worker_id=worker_id), [])
            with self.assertRaises(web.HTTPConflict):
                await self.state.poll(rollout_id='agent', registration_token='0'*32,
                                      timeout_seconds=0, worker_id='idle')

    async def test_read_only_poll_observation_cannot_cross_registration_replacement(self):
        await self.enqueue()
        original = self.state._poll_observation
        replaced = False

        async def observe(*args):
            nonlocal replaced
            result = await original(*args)
            if not replaced:
                replaced = True
                await self.state.register_rollout('agent')
            return result

        with patch.object(self.state, '_poll_observation', side_effect=observe):
            with self.assertRaises(web.HTTPConflict):
                await self.poll(timeout_seconds=0, worker_id='stale')
        async with self.state.store.transaction('test_no_stale_claim') as conn:
            row = await (await conn.execute(
                "SELECT count(*) AS n FROM relay_requests WHERE state='leased'"
            )).fetchone()
            workers = await (await conn.execute('SELECT count(*) AS n FROM relay_workers')).fetchone()
        self.assertEqual(row['n'], 0)
        self.assertEqual(workers['n'], 0)

    async def test_peer_due_heartbeats_coalesce_before_any_row_lock(self):
        peer = await self.new_state()
        ready, seen = asyncio.Event(), set()
        originals = {state: state._poll_observation for state in (self.state, peer)}

        def observer(state):
            async def observe(*args):
                result = await originals[state](*args)
                task = asyncio.current_task()
                if task not in seen:
                    seen.add(task)
                    if len(seen) == 8:
                        ready.set()
                    await ready.wait()
                return result
            return observe

        with (patch.object(self.state, '_poll_observation', side_effect=observer(self.state)),
              patch.object(peer, '_poll_observation', side_effect=observer(peer)),
              patch.object(self.state, '_write_worker_heartbeat', wraps=self.state._write_worker_heartbeat) as first,
              patch.object(peer, '_write_worker_heartbeat', wraps=peer._write_worker_heartbeat) as second):
            results = await asyncio.wait_for(asyncio.gather(*(
                self.poll(self.state if i%2 else peer, worker_id='shared', timeout_seconds=0)
                for i in range(8))), 3)
        self.assertEqual(results, [[]]*8)
        self.assertEqual(first.call_count + second.call_count, 1)

    async def test_poll_heartbeat_refresh_uses_existing_retention_and_lease_lifetimes(self):
        for retention, lease in ((3600, 0.15), (0.15, 600)):
            with self.subTest(retention=retention, lease=lease):
                self.state.worker_retention = retention
                await self.poll(worker_id='idle', timeout_seconds=0, lease_seconds=lease)
                async with self.state.store.transaction('test_time') as conn:
                    before = await (await conn.execute("SELECT last_seen_at FROM relay_workers WHERE worker_id='idle'")).fetchone()
                # Outstanding long polls refresh even without notifications.
                await self.poll(worker_id='idle', timeout_seconds=0.18, lease_seconds=lease)
                async with self.state.store.transaction('test_time') as conn:
                    after = await (await conn.execute("SELECT last_seen_at FROM relay_workers WHERE worker_id='idle'")).fetchone()
                self.assertGreater(after['last_seen_at'] - before['last_seen_at'], 0.1)
                # An explicit heartbeat is never coalesced or metadata-delayed.
                explicit = await self.state.record_worker_heartbeat(rollout_id='agent',
                    registration_token=self.token, worker_id='idle', metadata={'ready': True})
                self.assertGreater(explicit['last_seen_at'], after['last_seen_at'])
                self.assertEqual(explicit['metadata'], {'ready': True})

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

    async def test_wake_release_and_delivery_progress_while_shared_pool_is_saturated(self):
        saturated = asyncio.Event()

        async def wake(_request):
            # Complete only once polls/enqueues hold every shared connection.
            await saturated.wait()

        state = await self.bound_state(result_notifier=wake)
        request = await self.enqueue(state)
        (leased,) = await self.poll(state)
        waiter = asyncio.create_task(state.wait_for_response(request, timeout_seconds=5))
        await self.respond(leased, state, defer_delivery=True)
        release, entered = asyncio.Event(), []

        async def occupy():
            async with state.store.transaction("occupy_shared_pool"):
                entered.append(True)
                await release.wait()

        occupants = [asyncio.create_task(occupy()) for _ in range(state.store.pool.max_size)]
        try:
            async with asyncio.timeout(2):
                while len(entered) < state.store.pool.max_size:
                    await asyncio.sleep(0.005)
            saturated.set()
            response = await asyncio.wait_for(asyncio.shield(waiter), 3)
            self.assertEqual(response.body, b"answer")
        finally:
            release.set()
            await asyncio.gather(*occupants, return_exceptions=True)
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)

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
        await self.respond(leased, state, defer_delivery=True)
        await asyncio.wait_for(skipped.wait(), 2)
        await asyncio.wait_for(woke.wait(), 2)
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
            await state.wait_for_response(result.request, timeout_seconds=2)
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
        async with state.store.transaction("inspect_retired_deferred_park") as conn:
            retired = await (await conn.execute(
                "SELECT done,claim_token FROM relay_lifecycle WHERE request_id=%s AND action='park'",
                (request.request_id,),
            )).fetchone()
        self.assertTrue(retired["done"])
        self.assertIsNone(retired["claim_token"])
        self.assertEqual((await state.wait_for_response(request, timeout_seconds=2)).body, b"answer")
        async with state.store.transaction("test_migrated_deferred_park") as conn:
            observed = await (await conn.execute(
                "SELECT reattachable FROM relay_requests WHERE request_id=%s",
                (request.request_id,),
            )).fetchone()
        self.assertTrue(observed["reattachable"])

    @unittest.skipUnless(importlib.util.find_spec("ucloud_sandboxes_sdk"), "requires coordinated SDK")
    async def test_sdk_renewal_preserves_original_transport_through_deferred_park(self):
        from ucloud_sandboxes_sdk.relay import RelayRequest, _renewed_request

        entered = asyncio.Queue()
        outcomes = asyncio.Queue()

        async def park(request):
            await entered.put(request.request_id)
            deferred, epoch = await outcomes.get()
            if deferred:
                raise api.RelayLifecycleDeferred(30, transport_epoch=epoch)
            return epoch

        async def wake(request):
            return "migrated"

        state = await self.bound_state(accepted_notifier=park, result_notifier=wake)
        original = await self.enqueue(
            state, idempotency_key="deferred-sdk-renew",
            defer_idempotency_until_disconnect=True,
        )
        await asyncio.wait_for(entered.get(), 2)
        (leased,) = await self.poll(state, lease_seconds=60)
        sdk_request = RelayRequest.from_payload(leased.envelope())
        self.assertIsNone(sdk_request.accepted_notified_at)
        self.assertIsNone(sdk_request.parked_transport_epoch)

        async def park_receipt(*, done):
            for _ in range(200):
                async with state.store.transaction("test_park_receipt") as conn:
                    row = await (await conn.execute(
                        "SELECT r.accepted_notified_at,r.parked_transport_epoch,l.done,l.claim_token "
                        "FROM relay_requests r JOIN relay_lifecycle l USING(deployment_id,request_id) "
                        "WHERE r.request_id=%s AND l.action='park'",
                        (original.request_id,),
                    )).fetchone()
                if row["claim_token"] is None and row["done"] == done:
                    return row
                await asyncio.sleep(.01)
            self.fail("park decision did not commit")

        async def renew(previous):
            renewed = await state.renew_lease(
                request_id=leased.request_id, registration_token=self.token,
                lease_id=leased.lease_id, lease_seconds=60,
            )
            # This is the actual SDK parser and strict transition checker. Do
            # not weaken SDK validation to accommodate a torn server receipt.
            return _renewed_request({"request": renewed.envelope()}, previous)

        first_receipt = None
        for attempt, (deferred, epoch) in enumerate((
            (True, "original"), (True, "migrated"), (False, "migrated"),
        )):
            if attempt:
                async with state.store.transaction("test_retry_park_now") as conn:
                    await conn.execute(
                        "UPDATE relay_lifecycle SET next_attempt_at=clock_timestamp() "
                        "WHERE request_id=%s AND action='park'",
                        (original.request_id,),
                    )
                state._signal("l")
                await asyncio.wait_for(entered.get(), 2)
                sdk_request = await renew(sdk_request)
            await outcomes.put((deferred, epoch))
            receipt = await park_receipt(done=not deferred)
            sdk_request = await renew(sdk_request)
            self.assertEqual(sdk_request.parked_transport_epoch, "original")
            self.assertIsNotNone(sdk_request.accepted_notified_at)
            pair = (receipt["accepted_notified_at"], receipt["parked_transport_epoch"])
            if first_receipt is None:
                first_receipt = pair
            self.assertEqual(pair, first_receipt)

        await self.respond(leased, state, defer_delivery=True)
        self.assertEqual(
            (await state.wait_for_response(original, timeout_seconds=2)).body,
            b"answer",
        )
        # The successful park ran on the migrated transport, but the caller's
        # original connection still belongs to the first deferred receipt.
        # Reattachment must compare wake against that original epoch.
        reattached = await self.enqueue(
            state, idempotency_key="deferred-sdk-renew",
            defer_idempotency_until_disconnect=True,
        )
        self.assertEqual(reattached.request_id, original.request_id)
        self.assertTrue(reattached.reattachable)

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
        # Callback entry precedes its durable lifecycle completion. Allow the
        # normal poll to wait for that commit rather than racing it at 100 ms.
        (leased,) = await self.poll(state, timeout_seconds=2)
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
        release = asyncio.Event()
        claim = state._claim_lifecycle

        async def delayed_claim(*args, **kwargs):
            await release.wait()
            return await claim(*args, **kwargs)

        # Hold dispatch admission instead of canceling a task at an arbitrary
        # pool/transaction boundary. The test controls the lifecycle ordering;
        # separate shutdown tests cover cancellation and restart recovery.
        with patch.object(state, "_claim_lifecycle", delayed_claim):
            req = await self.enqueue(state)
            (leased,) = await self.poll(state)
            await self.respond(leased, state, defer_delivery=True)
            async with state.store.transaction("inspect_obsolete_park") as conn:
                park = await (await conn.execute(
                    "SELECT done,attempts FROM relay_lifecycle WHERE request_id=%s AND action='park'",
                    (req.request_id,),
                )).fetchone()
            self.assertTrue(park["done"])
            self.assertEqual(park["attempts"], 0)
            release.set()
            await state.wait_for_response(req, timeout_seconds=3)
        self.assertEqual(park_calls, [])

    async def test_two_http_servers_share_authority(self):
        for _ in range(2):
            store = PostgresDatabase(
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
            from tests.legacy_relay_fixture import write_legacy_journal
            old = await self.new_state()
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
            result = await old.respond(
                request_id=req.request_id,
                registration_token=reg["registration_token"],
                lease_id=leased.lease_id,
                response=api.RelayWorkerResponse(200, b"sampled-once"),
            )
            write_legacy_journal(path, rollouts=[reg], requests=[result.request])
            store = PostgresDatabase(DSN, "import", schema=self.schema)
            await store.open()
            try:
                summary = await import_idle_relay(store, path)
                self.assertEqual(summary["responses"], 1)
                self.assertEqual(summary, await import_idle_relay(store, path))
                assert_relay_cutover(path, "import", self.schema)
                import sqlite3
                from ucloud_sandboxes.shared_control.legacy_relay import SQLITE_RELAY_VERSION
                with closing(sqlite3.connect(path)) as source:
                    self.assertEqual(int(source.execute("SELECT value FROM relay_meta WHERE key='version'").fetchone()[0]), SQLITE_RELAY_VERSION + 1)
                with self.assertRaisesRegex(ValueError, "0.5.114 requires PostgreSQL"):
                    api.create_model_relay_app(state_path=path)
            finally:
                await old.aclose()
                await store.close()
            state = PostgresRelayState(
                PostgresDatabase(DSN, "import", schema=self.schema)
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
            from tests.legacy_relay_fixture import write_legacy_journal
            old = await self.new_state()
            reg = await old.register_rollout("busy")
            req = await old.enqueue(
                rollout_id="busy", endpoint="/v1/responses", body={}, headers={}
            )
            write_legacy_journal(path, rollouts=[reg], requests=[req])
            before = path.read_bytes()
            with self.assertRaisesRegex(ValueError, "idle"):
                await import_idle_relay(self.state.store, path)
            self.assertEqual(path.read_bytes(), before)
            await old.aclose()

    async def test_import_recovers_after_source_fence_before_target_activation(self):
        from contextlib import asynccontextmanager
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from tests.legacy_relay_fixture import write_legacy_journal
        from ucloud_sandboxes.shared_control.migration import import_idle_relay, assert_relay_cutover
        with TemporaryDirectory() as raw:
            path = Path(raw) / 'old.sqlite'
            registration = await self.state.register_rollout('offline-import')
            write_legacy_journal(path, rollouts=[registration])
            store = PostgresDatabase(DSN, 'interrupted-import', schema=self.schema)
            await store.open()
            original = store.transaction
            @asynccontextmanager
            async def interrupted(operation, **kwargs):
                if operation == 'relay_activate_import':
                    raise RuntimeError('crash after source fence')
                async with original(operation, **kwargs) as connection:
                    yield connection
            try:
                with patch.object(store, 'transaction', interrupted):
                    with self.assertRaisesRegex(RuntimeError, 'after source fence'):
                        await import_idle_relay(store, path)
                identity = assert_relay_cutover(path, store.deployment_id, store.schema)
                self.assertTrue(identity['source_digest'])
                async with store.transaction('inspect_unactivated') as connection:
                    row = await (await connection.execute(
                        'SELECT active FROM relay_imports WHERE deployment_id=%s', (store.deployment_id,),
                    )).fetchone()
                self.assertFalse(row['active'])
                self.assertTrue((await import_idle_relay(store, path))['active'])
            finally:
                await store.close()

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

    async def test_delivery_hints_read_only_changed_rows_at_512_waiters(self):
        requests = [await self.enqueue() for _ in range(512)]
        counts = []
        original = psycopg.AsyncConnection.execute

        async def observed(conn, query, *args, **kwargs):
            cursor = await original(conn, query, *args, **kwargs)
            if isinstance(query, str) and query.startswith(
                "SELECT request_id,state,delivery_pending,completed_bytes,completed_at"
            ):
                counts.append(cursor.rowcount)
            return cursor

        async def until(predicate):
            while not predicate():
                await asyncio.sleep(.001)

        waiting = []
        with patch.object(psycopg.AsyncConnection, "execute", observed):
            try:
                waiting = [asyncio.create_task(self.state.wait_for_response(
                    request, timeout_seconds=10
                )) for request in requests]
                await asyncio.wait_for(until(lambda: 512 in counts), 5)
                counts.clear()
                for request in requests[:32]:
                    before = len(counts)
                    self.state._signal("r:" + request.request_id)
                    await asyncio.wait_for(until(lambda: len(counts) > before), 2)
                # The previous all-waiter scan returned 512 rows on every hint
                # (16,384 rows for these 32 hints). Periodic reconciliation is
                # allowed; each ordinary hint must read only its own row.
                self.assertGreaterEqual(counts.count(1), 28, counts)
                self.assertTrue(all(count in (1, 512) for count in counts), counts)
            finally:
                for task in waiting:
                    task.cancel()
                await asyncio.gather(*waiting, return_exceptions=True)
        self.assertFalse(self.state._response_waiters)

    async def test_delivery_reconciles_lost_hint_during_unrelated_hint_stream(self):
        peer = await self.new_state()
        for state in (self.state, peer):
            for task in (state._tasks[0], state._tasks[-1]):
                task.cancel()
            await asyncio.gather(state._tasks[0], state._tasks[-1], return_exceptions=True)
        request = await self.enqueue()
        (leased,) = await self.poll(peer)
        other = await self.enqueue()
        waiting = [asyncio.create_task(self.state.wait_for_response(
            req, timeout_seconds=3)) for req in (request, other)]
        started = asyncio.Event()

        def observed(sample):
            if sample.operation == "relay_delivery_status":
                started.set()

        self.state.store.observe = observed

        async def unrelated_hints():
            while True:
                self.state._signal("r:" + other.request_id)
                await asyncio.sleep(.01)

        spam = None
        try:
            await asyncio.wait_for(started.wait(), 1)
            # Only the peer observes this completion. Continuing hints for the
            # other request must not postpone full durable reconciliation.
            spam = asyncio.create_task(unrelated_hints())
            await self.respond(leased, peer)
            self.assertEqual((await asyncio.wait_for(waiting[0], 1.5)).body, b"answer")
        finally:
            tasks = waiting + ([spam] if spam is not None else [])
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_late_delivery_waiter_recovers_failed_hint_read_and_cancelled_peer(self):
        from contextlib import asynccontextmanager

        peer = await self.new_state()
        # Completion precedes socket attachment; neither process receives a
        # notification, so only the local attach hint and durable fallback exist.
        for state in (self.state, peer):
            for task in (state._tasks[0], state._tasks[-1]):
                task.cancel()
            await asyncio.gather(state._tasks[0], state._tasks[-1], return_exceptions=True)
        request = await self.enqueue()
        (leased,) = await self.poll(peer)
        await self.respond(leased, peer)
        cancelled_request = await self.enqueue()
        cancelled = asyncio.create_task(self.state.wait_for_response(
            cancelled_request, timeout_seconds=3,
        ))
        await asyncio.sleep(0)
        cancelled.cancel()
        await asyncio.gather(cancelled, return_exceptions=True)
        self.assertNotIn(cancelled_request.request_id, self.state._response_waiters)
        # Delivery reads run on the dedicated lifecycle/delivery pool.
        transaction = self.state._delivery_store.transaction
        failed = False

        @asynccontextmanager
        async def fail_once(operation):
            nonlocal failed
            if operation == "relay_delivery_status" and not failed:
                failed = True
                raise OSError("qualification lost status read")
            async with transaction(operation) as conn:
                yield conn

        with patch.object(self.state._delivery_store, "transaction", fail_once):
            with self.assertLogs("ucloud_sandboxes.shared_control.relay", "WARNING"):
                response = await self.state.wait_for_response(request, timeout_seconds=2)
        self.assertTrue(failed)
        self.assertEqual(response.body, b"answer")
        self.assertFalse(self.state._response_waiters)
        self.assertFalse(self.state._dirty_deliveries)

    async def test_idle_pollers_share_readiness_queries_and_cancel_cleanly(self):
        from collections import Counter
        operations = Counter()
        self.state.store.observe = lambda sample: operations.update([sample.operation])
        registrations = [await self.state.register_rollout(f"idle-{i}") for i in range(32)]
        # Isolate periodic fallback from delayed registration NOTIFY hints.
        for task in (self.state._tasks[0], self.state._tasks[-1]):
            task.cancel()
        await asyncio.gather(self.state._tasks[0], self.state._tasks[-1], return_exceptions=True)
        waiting = [asyncio.create_task(self.state.poll(
            rollout_id=reg["rollout_id"], registration_token=reg["registration_token"],
            timeout_seconds=10,
        )) for reg in registrations]
        try:
            async def started():
                while operations["relay_claim_inference"] < 32:
                    await asyncio.sleep(.01)
            await asyncio.wait_for(started(), 5)
            observations = operations["relay_claim_inference"]
            await asyncio.sleep(1.1)
            self.assertEqual(operations["relay_claim_inference"], observations)
            self.assertGreaterEqual(operations["relay_poll_readiness"], 1)
            self.assertTrue(all(not task.done() for task in waiting))
        finally:
            for task in waiting:
                task.cancel()
            await asyncio.gather(*waiting, return_exceptions=True)
        self.assertEqual(self.state._poll_waiters, {})

    async def test_batched_poll_fallback_recovers_enqueue_and_expired_lease_without_hints(self):
        peer = await self.new_state()
        for task in (self.state._tasks[0], self.state._tasks[-1]):
            task.cancel()
        await asyncio.gather(self.state._tasks[0], self.state._tasks[-1], return_exceptions=True)
        waiting = asyncio.create_task(self.poll(timeout_seconds=3))
        try:
            await asyncio.sleep(.05)
            request = await self.enqueue(peer)
            (claimed,) = await waiting
            self.assertEqual(claimed.request_id, request.request_id)
            await peer.retry_worker_failure(request_id=claimed.request_id,
                               registration_token=self.token, lease_id=claimed.lease_id)
            (leased,) = await self.poll(peer, lease_seconds=.05)
            (again,) = await self.poll(timeout_seconds=3)
            self.assertEqual(again.request_id, leased.request_id)
            self.assertNotEqual(again.lease_id, leased.lease_id)
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)

    async def test_batched_poll_fallback_observes_registration_revocation(self):
        peer = await self.new_state()
        for task in (self.state._tasks[0], self.state._tasks[-1]):
            task.cancel()
        await asyncio.gather(self.state._tasks[0], self.state._tasks[-1], return_exceptions=True)
        waiting = asyncio.create_task(self.poll(timeout_seconds=3))
        try:
            await asyncio.sleep(.05)
            await peer.unregister_rollout("agent", registration_token=self.token)
            with self.assertRaises(web.HTTPNotFound):
                await asyncio.wait_for(waiting, 2)
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)

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
            PostgresDatabase(DSN, "test", schema=self.schema), result_notifier=wake
        )
        with self.assertRaisesRegex(ValueError, "disagree"):
            await peer.open()

    async def test_source_fence_cannot_activate_an_empty_other_database(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from ucloud_sandboxes.shared_control.migration import import_idle_relay

        with TemporaryDirectory() as raw:
            path = (Path(raw) / "relay.sqlite").resolve()
            from tests.legacy_relay_fixture import write_legacy_journal
            reg = await self.state.register_rollout("old")
            write_legacy_journal(path, rollouts=[reg])
            imported = PostgresDatabase(DSN, "import", schema=self.schema)
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

    async def completion_client(self, wake, *, deployment="http-acceptance"):
        app = api.create_model_relay_app(
            postgres_store=PostgresDatabase(DSN, deployment, schema=self.schema),
            request_timeout_seconds=1,
            result_notifier=wake,
            worker_bearer_token="worker",
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        self.clients.append(client)
        return client, app[api.STATE_KEY]

    async def test_pool_exhaustion_is_predispatch_503_and_retry_commits_once(self):
        store = PostgresDatabase(
            DSN, "pool-admission", schema=self.schema,
            max_connections=1,
        )
        app = api.create_model_relay_app(
            postgres_store=store, worker_bearer_token="worker",
            maintenance_interval_seconds=60,
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        self.clients.append(client)
        state = app[api.STATE_KEY]
        # Exercise short acquisition failure after initialization, not a race
        # between a cold PostgreSQL connection and a 100 ms startup deadline.
        store.pool.timeout = 0.1
        request, payload = await self.completion_request(state)
        samples = []
        store.observe = samples.append
        async with store.pool.connection():
            with self.assertRaises(DatabaseAdmissionUnavailable):
                async with store.transaction("must_not_enter"):
                    self.fail("pool exhaustion entered transaction body")
            async with client.get(
                "/worker/poll", params={"rollout_id": "agent",
                                       "registration_token": payload["registration_token"],
                                       "timeout_seconds": "0"},
                headers={"Authorization": "Bearer worker"},
            ) as response:
                self.assertEqual(response.status, 503, await response.text())
                self.assertTrue((await response.json())["retryable"])
            async with client.post(
                "/worker/respond", json=payload,
                headers={"Authorization": "Bearer worker"},
            ) as response:
                self.assertEqual(response.status, 503, await response.text())
                self.assertEqual((await response.json())["error_code"], "relay_database_busy")
                self.assertEqual(response.headers["Retry-After"], "1")
            async with client.post(
                "/worker/renew", json=payload,
                headers={"Authorization": "Bearer worker"},
            ) as response:
                self.assertEqual(response.status, 503, await response.text())
                self.assertEqual(response.headers["X-UCloud-Retryable"], "true")
            async with client.delete(
                "/v1/relay/rollouts/agent",
                json={"registration_token": payload["registration_token"]},
                headers={"Authorization": "Bearer worker"},
            ) as response:
                self.assertEqual(response.status, 503, await response.text())
                self.assertEqual((await response.json())["error_code"], "relay_database_busy")
        failed = [sample for sample in samples if not sample.succeeded]
        self.assertTrue(failed)
        self.assertTrue(all(sample.transaction_seconds == 0 for sample in failed))
        async with client.post(
            "/worker/renew", json=payload,
            headers={"Authorization": "Bearer worker"},
        ) as response:
            self.assertEqual(response.status, 200, await response.text())
            self.assertEqual((await response.json())["request"]["lease_id"], payload["lease_id"])
        receipt = await self.post_completion(client, payload)
        self.assertTrue(receipt["committed"])
        self.assertFalse(receipt["duplicate"])
        duplicate = await self.post_completion(client, payload)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual((await state.wait_for_response(request, timeout_seconds=2)).body,
                         b"accepted-once")
        for existed in (True, False):
            async with client.delete(
                "/v1/relay/rollouts/agent",
                json={"registration_token": payload["registration_token"]},
                headers={"Authorization": "Bearer worker"},
            ) as response:
                self.assertEqual(response.status, 200, await response.text())
                self.assertEqual((await response.json())["existed"], existed)

    async def test_later_model_transaction_admission_does_not_claim_safe_http_retry(self):
        client, state = await self.completion_client(None)
        await state.register_rollout("model-caller")
        with patch.object(state, "wait_for_response", AsyncMock(side_effect=asyncio.TimeoutError)), \
             patch.object(state, "cancel_request", AsyncMock(side_effect=DatabaseAdmissionUnavailable)):
            async with client.post("/rollouts/model-caller/v1/responses", json={"model": "test"}) as response:
                self.assertEqual(response.status, 500)
                self.assertNotIn("X-UCloud-Retryable", response.headers)
                self.assertNotIn("relay_database_busy", await response.text())
        # The enqueue already committed; retrying this entire model call cannot
        # be justified by the later cancellation transaction failing admission.
        self.assertEqual((await state.stats())["pending"], {"model-caller": 1})

    async def test_acquired_transaction_timeout_is_never_admission_failure(self):
        store = self.state.store
        original = store.pool.connection
        for phase in ("BEGIN", "body", "COMMIT"):
            with self.subTest(phase=phase):
                @asynccontextmanager
                async def connection():
                    async with original() as actual:
                        class Connection:
                            async def execute(self, statement, *args):
                                result = await actual.execute(statement, *args)
                                if statement == phase:
                                    # COMMIT deliberately succeeds before its
                                    # acknowledgement is lost: never safe to
                                    # classify as a transaction not yet started.
                                    raise PoolTimeout("injected after acquisition")
                                return result

                            async def close(self):
                                await actual.close()
                        yield Connection()
                with patch.object(store.pool, "connection", connection):
                    with self.assertRaises(PoolTimeout):
                        async with store.transaction("ambiguous"):
                            if phase == "body":
                                raise PoolTimeout("injected in body")

    async def test_postgres_http_startup_never_constructs_legacy_authority(self):
        # Exercise actual startup, maintenance and a request against PostgreSQL,
        # not just which object remains in app state after construction.
        self.assertFalse(hasattr(api, "ModelRelayState"))
        self.assertFalse(hasattr(api, "RelaySqliteStore"))
        with patch("sqlite3.connect", side_effect=AssertionError(
                "PostgreSQL startup opened a SQLite connection")):
            client, state = await self.completion_client(None)
            self.assertIsInstance(state, PostgresRelayState)
            registration = await state.register_rollout("postgres-only")
            response = await client.get("/v1/relay/stats", headers={
                "Authorization": "Bearer worker",
            })
            self.assertEqual(response.status, 200, await response.text())
            self.assertTrue(registration["registration_token"])

    async def test_resource_phase_sequence_expiry_and_registration_fences(self):
        update = {"sequence": 2, "phase": "model_wait", "ttl_seconds": 30,
                  "expected_remaining_wait_seconds": 20}
        first = await self.state.update_resource_phase(
            "agent", registration_token=self.token, update=update)
        repeated = await self.state.update_resource_phase(
            "agent", registration_token=self.token, update=update)
        self.assertEqual(first["current"]["expires_at"], repeated["current"]["expires_at"])
        older = await self.state.update_resource_phase(
            "agent", registration_token=self.token, update={**update, "sequence": 1})
        self.assertFalse(older["accepted"])
        with self.assertRaises(web.HTTPConflict):
            await self.state.update_resource_phase(
                "agent", registration_token=self.token, update={**update, "ttl_seconds": 31})
        async with self.state.store.transaction("test_expire_phase") as conn:
            await conn.execute(
                "UPDATE relay_rollouts SET metadata=jsonb_set(metadata, '{_ucloud_resource_phase,expires_at}', '0') WHERE deployment_id=%s AND rollout_id='agent'",
                (self.state.deployment,),
            )
        expired = await self.state.update_resource_phase(
            "agent", registration_token=self.token, update=update)
        self.assertIsNone(expired["current"])
        await self.state.register_rollout("agent")
        with self.assertRaises(web.HTTPConflict):
            await self.state.update_resource_phase(
                "agent", registration_token=self.token, update={**update, "sequence": 3})

    async def test_resource_phase_http_auth_validation_and_revocation(self):
        client, state = await self.completion_client(None)
        registration = await state.register_rollout("phase-http")
        token = registration["registration_token"]
        endpoint = "/v1/relay/rollouts/phase-http/resource-phase"
        payload = {"registration_token": token,
                   "update": {"sequence": 1, "phase": "rollout_complete"}}
        response = await client.post(endpoint, json=payload)
        self.assertEqual(response.status, 401)
        response = await client.post(endpoint, json=payload,
                                     headers={"Authorization": "Bearer worker"})
        self.assertEqual(response.status, 200, await response.text())
        # A completion hint neither revokes the registration nor deletes work.
        self.assertEqual((await state.list_rollouts())[0]["rollout_id"], "phase-http")
        await state.unregister_rollout("phase-http", registration_token=token)
        response = await client.post(endpoint, json=payload,
                                     headers={"Authorization": "Bearer worker"})
        self.assertEqual(response.status, 404)

    async def test_resource_phase_cancelled_transaction_leaves_no_partial_hint(self):
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.state._now
        async def blocked_now(conn):
            entered.set()
            await release.wait()
            return await original(conn)
        with patch.object(self.state, "_now", side_effect=blocked_now):
            task = asyncio.create_task(self.state.update_resource_phase(
                "agent", registration_token=self.token,
                update={"sequence": 1, "phase": "training_pause"}))
            await asyncio.wait_for(entered.wait(), 3)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        record = (await self.state.list_rollouts())[0]
        self.assertNotIn("_ucloud_resource_phase", record["metadata"])
        result = await self.state.update_resource_phase(
            "agent", registration_token=self.token,
            update={"sequence": 1, "phase": "training_resume"})
        self.assertTrue(result["accepted"])

    async def test_resource_phase_is_advice_on_existing_park_not_new_lifecycle_work(self):
        from ucloud_sandboxes.relay_phase import transport_phase
        arrived = asyncio.Event()
        hints = []
        async def park(request):
            hints.append(transport_phase(request.resource_phase))
            arrived.set()
        state = await self.bound_state(accepted_notifier=park)
        registration = (await state.list_rollouts())[0]
        await state.update_resource_phase(
            "agent", registration_token=registration["registration_token"],
            update={"sequence": 1, "phase": "model_wait", "ttl_seconds": 30,
                    "expected_remaining_wait_seconds": 20})
        self.assertEqual((await state.stats())["lifecycle"], [])
        request = await self.enqueue(state)
        await asyncio.wait_for(arrived.wait(), 3)
        self.assertEqual(hints[0]["phase"], "model_wait")
        self.assertGreater(hints[0]["expected_remaining_wait_seconds"], 15)
        self.assertNotIn(request.registration_token, str(hints))
        self.assertEqual(request.state, "pending")

    async def test_registration_cannot_seed_forged_resource_phase(self):
        with self.assertRaises(web.HTTPBadRequest):
            await self.state.register_rollout("forged", {
                "_ucloud_resource_phase": {"sequence": 999, "phase": "model_wait"},
            })

    @unittest.skipUnless(importlib.util.find_spec("ucloud_sandboxes_sdk"), "requires coordinated SDK")
    async def test_resource_phase_real_sync_and_async_sdk_round_trip(self):
        from ucloud_sandboxes_sdk import AsyncRelayWorkerClient, RelayWorkerClient
        http, state = await self.completion_client(None)
        url = str(http.make_url("/")).rstrip("/")
        async with AsyncRelayWorkerClient(url, worker_token="worker") as client:
            await client.register_rollout("sdk-phase")
            receipt = await client.update_resource_phase(
                "sdk-phase", sequence=1, phase="model_wait", expected_remaining_wait_seconds=15)
            self.assertTrue(receipt["accepted"])
            self.assertEqual(receipt["current"]["phase"], "model_wait")
            sync = RelayWorkerClient(url, worker_token="worker")
            registration = (await state.list_rollouts())[0]
            receipt = await asyncio.to_thread(sync.update_resource_phase,
                "sdk-phase", sequence=2, phase="tool",
                registration_token=registration["registration_token"])
            self.assertTrue(receipt["accepted"])
            self.assertEqual(receipt["current"]["phase"], "tool")

    async def completion_request(self, state):
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
        return request, {
            "request_id": request.request_id,
            "registration_token": reg["registration_token"],
            "lease_id": leased.lease_id,
            "body": api._encoded_body(b"accepted-once"),
        }

    async def post_completion(self, client, payload, *, path="/worker/respond"):
        async with client.post(
            path, json=payload, headers={"Authorization": "Bearer worker"}
        ) as response:
            self.assertEqual(response.status, 200, await response.text())
            return await response.json()

    async def test_http_acceptance_completes_while_wake_is_blocked(self):
        release, entered = asyncio.Event(), asyncio.Event()

        async def wake(request):
            entered.set()
            await release.wait()
            return "epoch"

        client, state = await self.completion_client(wake)
        request, payload = await self.completion_request(state)
        samples = []
        state.store.observe = samples.append
        receipt = await asyncio.wait_for(self.post_completion(client, payload), 2)
        self.assertEqual(receipt, {
            "ok": True, "request_id": request.request_id, "duplicate": False,
            "committed": True, "delivery_status": "pending",
        })
        await asyncio.wait_for(entered.wait(), 2)
        self.assertFalse(release.is_set())
        self.assertEqual((await state.stats())["delivery_pending"], 1)
        self.assertNotIn("relay_delivery_bodies", [s.operation for s in samples])
        # An identical retry acknowledges the same result without waiting or
        # sampling again; a different result cannot replace accepted bytes.
        duplicate = await asyncio.wait_for(self.post_completion(client, payload), 2)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(duplicate["delivery_status"], "pending")
        async with client.post(
            "/worker/respond", json={**payload, "body": api._encoded_body(b"changed")},
            headers={"Authorization": "Bearer worker"},
        ) as response:
            self.assertEqual(response.status, 409)
        release.set()
        self.assertEqual(
            (await state.wait_for_response(request, timeout_seconds=2)).body,
            b"accepted-once",
        )
        receipt = await self.post_completion(client, payload)
        self.assertEqual(receipt["delivery_status"], "released")

    async def test_http_sibling_responses_keep_independent_delivery_obligations(self):
        release = asyncio.Event()
        woke = []

        async def wake(request):
            await release.wait()
            woke.append((request.request_id, request.sandbox_id, request.sandbox_generation))
            return "epoch"

        client, state = await self.completion_client(wake)
        first, payload = await self.completion_request(state)
        requests = [first] + [await self.enqueue(state) for _ in range(3)]
        leased = await state.poll(
            rollout_id="agent", registration_token=payload["registration_token"],
            timeout_seconds=0, limit=3,
        )
        payloads = [payload] + [
            {**payload, "request_id": request.request_id, "lease_id": request.lease_id}
            for request in leased
        ]
        receipts = await asyncio.wait_for(asyncio.gather(*(
            self.post_completion(client, item) for item in payloads
        )), 2)
        self.assertEqual(len(receipts), 4)
        self.assertTrue(all(r["delivery_status"] == "pending" for r in receipts))
        self.assertEqual((await state.stats())["delivery_pending"], 4)
        release.set()
        responses = await asyncio.gather(*(
            state.wait_for_response(request, timeout_seconds=2) for request in requests
        ))
        self.assertTrue(all(response.body == b"accepted-once" for response in responses))
        self.assertEqual(set(woke), {(request.request_id, "s1", 1) for request in requests})

    async def test_http_lost_ack_and_relay_restart_preserve_delivery(self):
        blocked = asyncio.Event()

        async def wake(request):
            await blocked.wait()
            return "epoch"

        client, state = await self.completion_client(wake)
        request, payload = await self.completion_request(state)
        committed = asyncio.Event()
        original = api._worker_completion_response

        async def lose_ack(http_request, result):
            # Simulate connection loss in the precise commit-before-ACK window.
            committed.set()
            http_request.transport.close()
            return await original(http_request, result)

        with patch.object(api, "_worker_completion_response", side_effect=lose_ack):
            from aiohttp import ClientConnectionError
            with self.assertRaises(ClientConnectionError):
                await self.post_completion(client, payload)
        await asyncio.wait_for(committed.wait(), 2)
        self.assertEqual((await state.stats())["delivery_pending"], 1)
        await client.close()
        self.clients.remove(client)
        woke = asyncio.Event()

        async def recovered_wake(request):
            woke.set()
            return "epoch"

        peer_client, peer = await self.completion_client(recovered_wake)
        # Restart leaves any in-flight lease fenced until expiry. Expire only
        # this test's lease instead of making the regression sleep 30 seconds.
        async with peer.store.transaction("test_expire_dead_dispatcher") as conn:
            await conn.execute(
                "UPDATE relay_lifecycle SET claim_until=clock_timestamp() "
                "WHERE deployment_id=%s AND request_id=%s AND NOT done",
                (peer.deployment, request.request_id),
            )
        receipt = await self.post_completion(peer_client, payload)
        self.assertTrue(receipt["committed"])
        self.assertTrue(receipt["duplicate"])
        await asyncio.wait_for(woke.wait(), 2)
        self.assertEqual(
            (await peer.wait_for_response(request, timeout_seconds=2)).body,
            b"accepted-once",
        )
        leased = await peer.poll(
            rollout_id="agent", registration_token=payload["registration_token"],
            timeout_seconds=0,
        )
        self.assertEqual(leased, [])

    async def test_http_terminal_worker_error_uses_same_acceptance_contract(self):
        release = asyncio.Event()

        async def wake(request):
            await release.wait()
            return "epoch"

        client, state = await self.completion_client(wake)
        request, payload = await self.completion_request(state)
        payload.pop("body")
        payload.update(error="terminal failure", status=422, retryable=False)
        receipt = await asyncio.wait_for(
            self.post_completion(client, payload, path="/worker/error"), 2,
        )
        self.assertTrue(receipt["committed"])
        self.assertEqual(receipt["delivery_status"], "pending")
        release.set()
        response = await state.wait_for_response(request, timeout_seconds=2)
        self.assertEqual(response.status, 422)

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
            store = PostgresDatabase(DSN, "qualification", schema=self.schema)
            client = TestClient(
                TestServer(combined_app(config, store, "/qualification-test",
                        live_store=PostgresDatabase(DSN, "live", schema=self.schema)))
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
                self.assertFalse(Path(config.relay_state_file()).exists())
            finally:
                await client.close()
