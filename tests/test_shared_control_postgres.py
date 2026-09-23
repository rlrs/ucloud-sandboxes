"""Real PostgreSQL contract. CI supplies UCLOUD_TEST_POSTGRES_DSN; no SQL mocks."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import os
import sys
import unittest
from uuid import uuid4

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")
if DSN:
    import psycopg
    from psycopg import sql
    from ucloud_sandboxes.shared_control import fixtures
    from ucloud_sandboxes.shared_control.dispatcher import WakeDispatcher
    from ucloud_sandboxes.shared_control.model import StateConflict, WakeProof
    from ucloud_sandboxes.shared_control.qualification import QualificationControlStore


@unittest.skipUnless(DSN, "set UCLOUD_TEST_POSTGRES_DSN for the real PostgreSQL contract")
class SharedControlPostgresTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.schema = "ucloud_shared_test_" + uuid4().hex
        self.stores = []
        self.samples = []
        self.store = await self.new_store()
        await self.store.migrate()
        await fixtures.node(self.store, "node-1")
        await self.seed("sandbox-1", "request-1")

    async def new_store(self, deployment="test", max_connections=8):
        store = QualificationControlStore(DSN, deployment, schema=self.schema, max_connections=max_connections, observe=self.samples.append)
        await store.open()
        self.stores.append(store)
        return store

    async def asyncTearDown(self):
        for store in self.stores:
            await store.close()
        async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
            await conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    async def seed(self, sandbox, request, node="node-1", **kwargs):
        await fixtures.sandbox(self.store, sandbox, node, **kwargs)
        await fixtures.request(self.store, request, sandbox)

    async def accept(self, request="request-1", body=b"model answer", store=None, **kwargs):
        return await (store or self.store).accept_result(
            request, registration_id=kwargs.get("registration_id", "registration-1"),
            lease_id=kwargs.get("lease_id", "lease-1"), body=body,
        )

    async def expire_claims(self):
        async with self.store.transaction("test_expire") as conn:
            await conn.execute("UPDATE wake_operations SET claim_until=clock_timestamp()-interval '1 second', next_attempt_at=clock_timestamp()")

    @staticmethod
    def proof(op):
        return WakeProof(op.operation_id, op.generation, op.node_epoch, op.lifecycle_sequence, 1)

    async def test_result_and_wake_survive_store_reopen_and_replay(self):
        accepted = await self.accept()
        self.assertIsNotNone(accepted.operation_id)
        self.assertIsNone(await self.store.read_result("request-1", registration_id="registration-1"))
        reopened = await self.new_store()
        replay = await self.accept(store=reopened)
        self.assertTrue(replay.duplicate)
        self.assertEqual(accepted.operation_id, replay.operation_id)
        op, = await reopened.claim_due()
        self.assertTrue(await reopened.prepare_dispatch(op))
        self.assertTrue(await reopened.complete(op, self.proof(op)))
        self.assertEqual(await reopened.read_result("request-1", registration_id="registration-1"), b"model answer")
        self.assertFalse(await reopened.complete(op, self.proof(op)))
        snapshot = await reopened.snapshot()
        self.assertEqual(snapshot["operations"], {"succeeded": 1})
        self.assertEqual(snapshot["nodes"][0]["reserved_restore_mb"], 0)

    async def test_concurrent_duplicate_results_have_one_effect(self):
        peer = await self.new_store()
        results = await asyncio.gather(*(self.accept(store=peer if i % 2 else self.store) for i in range(32)))
        self.assertEqual(sum(not r.duplicate for r in results), 1)
        self.assertEqual(len({r.operation_id for r in results}), 1)
        self.assertEqual((await self.store.snapshot())["responses"], 1)

    async def test_conflicting_response_never_overwrites_first_commit(self):
        results = await asyncio.gather(self.accept(body=b"one"), self.accept(body=b"two"), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, StateConflict) for r in results), 1)
        async with self.store.transaction("test_read") as conn:
            row = await (await conn.execute("SELECT body FROM model_responses")).fetchone()
            self.assertIn(bytes(row["body"]), (b"one", b"two"))
        self.assertEqual((await self.store.snapshot())["operations"], {"queued": 1})

    async def test_response_metadata_is_durable_and_part_of_replay_identity(self):
        await self.store.accept_result("request-1", registration_id="registration-1", lease_id="lease-1",
                                       body=b"result", status=201, headers={"Content-Type": "application/json"})
        with self.assertRaises(StateConflict):
            await self.accept(body=b"result")
        op, = await self.store.claim_due()
        await self.store.prepare_dispatch(op)
        await self.store.complete(op, self.proof(op))
        response = await self.store.read_response("request-1", registration_id="registration-1")
        self.assertEqual((response.status, response.headers, response.body), (201, {"Content-Type": "application/json"}, b"result"))

    async def test_two_results_for_one_sandbox_coalesce_wake(self):
        await fixtures.request(self.store, "request-2", "sandbox-1")
        first, second = await asyncio.gather(self.accept(), self.accept("request-2"))
        self.assertEqual(first.operation_id, second.operation_id)
        self.assertEqual((await self.store.snapshot())["responses"], 2)

    async def test_wrong_registration_or_lease_is_not_an_idempotent_replay(self):
        await self.accept()
        for kwargs in ({"lease_id": "other"}, {"registration_id": "other"}):
            with self.assertRaises(StateConflict):
                await self.accept(**kwargs)
        with self.assertRaises(StateConflict):
            await self.store.read_result("request-1", registration_id="other")

    async def test_expired_lease_rejects_new_result_but_allows_committed_replay(self):
        await self.accept()
        await fixtures.request(self.store, "request-2", "sandbox-1")
        async with self.store.transaction("test_expire") as conn:
            await conn.execute("UPDATE model_requests SET lease_until=clock_timestamp()-interval '1 second'")
        self.assertTrue((await self.accept()).duplicate)
        with self.assertRaises(StateConflict):
            await self.accept("request-2")

    async def test_commit_failure_rolls_back_body_operation_and_sequence(self):
        async with self.store.transaction("test_trigger") as conn:
            await conn.execute("""CREATE FUNCTION reject_response() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'injected write failure'; END; $$;
                CREATE TRIGGER reject_response BEFORE INSERT ON model_responses
                FOR EACH ROW EXECUTE FUNCTION reject_response();""")
        with self.assertRaises(psycopg.errors.RaiseException):
            await self.accept()
        snapshot = await self.store.snapshot()
        self.assertEqual(snapshot["operations"], {})
        self.assertEqual(snapshot["responses"], 0)
        async with self.store.transaction("test_read") as conn:
            row = await (await conn.execute("SELECT lifecycle_sequence FROM sandboxes")).fetchone()
            self.assertEqual(row["lifecycle_sequence"], 0)

    async def test_capacity_pressure_queues_without_losing_committed_result(self):
        await fixtures.node(self.store, "small", budget_mb=128)
        await self.seed("small-1", "small-r1", "small")
        await self.seed("small-2", "small-r2", "small")
        await asyncio.gather(self.accept("small-r1"), self.accept("small-r2"))
        first, second = await self.store.claim_due()
        self.assertTrue(await self.store.prepare_dispatch(first))
        self.assertFalse(await self.store.prepare_dispatch(second))
        snapshot = await self.store.snapshot()
        self.assertEqual(snapshot["operations"], {"queued": 1, "dispatching": 1})
        self.assertEqual(snapshot["responses"], 2)
        self.assertTrue(await self.store.complete(first, self.proof(first)))
        async with self.store.transaction("test_due") as conn:
            await conn.execute("UPDATE wake_operations SET next_attempt_at=clock_timestamp()")
        retry, = await self.store.claim_due()
        self.assertEqual(retry.operation_id, second.operation_id)
        self.assertTrue(await self.store.prepare_dispatch(retry))
        self.assertTrue(await self.store.complete(retry, self.proof(retry)))

    async def test_claim_expiry_retries_same_operation_and_keeps_reservation(self):
        await self.accept()
        old, = await self.store.claim_due()
        await self.store.prepare_dispatch(old)
        await self.expire_claims()
        peer = await self.new_store()
        new, = await peer.claim_due()
        self.assertEqual(old.operation_id, new.operation_id)
        self.assertNotEqual(old.claim_token, new.claim_token)
        self.assertTrue(await peer.prepare_dispatch(new))
        self.assertEqual((await peer.snapshot())["nodes"][0]["reserved_restore_mb"], 128)
        self.assertFalse(await self.store.complete(old, self.proof(old)))
        self.assertTrue(await peer.complete(new, self.proof(new)))

    async def test_wrong_or_stale_worker_proof_keeps_reservation(self):
        await self.accept()
        op, = await self.store.claim_due()
        await self.store.prepare_dispatch(op)
        for proof in (replace(self.proof(op), node_epoch="old-boot"), replace(self.proof(op), activity_epoch=0),
                      replace(self.proof(op), lifecycle_sequence=2), replace(self.proof(op), generation=2)):
            with self.assertRaises(StateConflict):
                await self.store.complete(op, proof)
        self.assertEqual((await self.store.snapshot())["nodes"][0]["reserved_restore_mb"], 128)

    async def test_two_consumers_claim_disjoint_work(self):
        await asyncio.gather(*(self.seed(f"s{i}", f"r{i}") for i in range(20)))
        await asyncio.gather(*(self.accept(f"r{i}") for i in range(20)))
        peer = await self.new_store()
        batches = await asyncio.gather(self.store.claim_due(limit=20), peer.claim_due(limit=20))
        ids = [op.operation_id for batch in batches for op in batch]
        self.assertEqual(len(ids), 20)
        self.assertEqual(len(set(ids)), 20)

    async def test_independent_nodes_do_not_share_a_placement_mutex(self):
        await fixtures.node(self.store, "node-2")
        await self.seed("sandbox-2", "request-2", "node-2")
        async with self.store.transaction("hold_node") as conn:
            await conn.execute("SELECT * FROM nodes WHERE node_id='node-1' FOR UPDATE")
            result = await asyncio.wait_for(self.accept("request-2"), 2)
            self.assertFalse(result.duplicate)

    async def test_result_acceptance_does_not_wait_for_node_capacity_lock(self):
        async with self.store.transaction("hold_node") as conn:
            await conn.execute("SELECT * FROM nodes WHERE node_id='node-1' FOR UPDATE")
            result = await asyncio.wait_for(self.accept(), 2)
            self.assertIsNotNone(result.operation_id)

    async def test_deployment_isolation_in_one_schema(self):
        other = await self.new_store("other")
        with self.assertRaises(StateConflict):
            await self.accept(store=other)
        await fixtures.node(other, "node-1")
        await fixtures.sandbox(other, "sandbox-1", "node-1")
        await fixtures.request(other, "request-1", "sandbox-1")
        await self.accept(store=other)
        self.assertEqual((await self.store.snapshot())["responses"], 0)
        self.assertEqual((await other.snapshot())["responses"], 1)

    async def test_cancelled_transaction_rolls_back_and_returns_connection(self):
        entered = asyncio.Event()
        async def interrupted():
            async with self.store.transaction("cancel") as conn:
                await conn.execute("UPDATE nodes SET reserved_restore_mb=10")
                entered.set()
                await asyncio.Event().wait()
        task = asyncio.create_task(interrupted())
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual((await self.store.snapshot())["nodes"][0]["reserved_restore_mb"], 0)
        self.assertFalse(next(s for s in self.samples if s.operation == "cancel").succeeded)

    async def test_migration_is_idempotent_and_newer_version_is_rejected(self):
        await asyncio.gather(self.store.migrate(), self.store.migrate())
        async with self.store.transaction("future_schema") as conn:
            row = await (await conn.execute(
                "SELECT to_regclass(%s) AS name", (f"{self.schema}.relay_schema_version",),
            )).fetchone()
            self.assertIsNone(row["name"])
            await conn.execute("UPDATE schema_version SET version=2")
        with self.assertRaises(ValueError):
            await self.new_store()

    async def test_dispatcher_does_not_hold_connection_during_rpc(self):
        await self.accept()
        peer = await self.new_store(max_connections=1)
        async def wake(op):
            snapshot = await asyncio.wait_for(peer.snapshot(), 2)
            self.assertEqual(snapshot["operations"], {"dispatching": 1})
            return self.proof(op)
        dispatcher = WakeDispatcher(peer, wake)
        self.assertEqual(await dispatcher.run_once(), 1)
        self.assertEqual(dispatcher.completed, 1)

    async def test_rpc_timeout_keeps_reservation_and_schedules_retry(self):
        await self.accept()
        async def slow_worker(op):
            await asyncio.sleep(60)
        dispatcher = WakeDispatcher(self.store, slow_worker, claim_seconds=1, rpc_seconds=.01)
        await dispatcher.run_once()
        self.assertEqual(dispatcher.retried, 1)
        snapshot = await self.store.snapshot()
        self.assertEqual(snapshot["operations"], {"dispatching": 1})
        self.assertEqual(snapshot["nodes"][0]["reserved_restore_mb"], 128)

    async def test_lost_acknowledgment_retries_without_new_worker_effect(self):
        await self.accept()
        effects = {}
        async def wake(op):
            if op.operation_id not in effects:
                effects[op.operation_id] = self.proof(op)
                raise OSError("lost acknowledgment after successful restore")
            return effects[op.operation_id]
        dispatcher = WakeDispatcher(self.store, wake, retry_seconds=.001)
        await dispatcher.run_once()
        self.assertEqual(dispatcher.retried, 1)
        async with self.store.transaction("test_due") as conn:
            await conn.execute("UPDATE wake_operations SET next_attempt_at=clock_timestamp()")
        await dispatcher.run_once()
        self.assertEqual(len(effects), 1)
        self.assertEqual(dispatcher.completed, 1)

    async def test_dispatch_refills_while_an_earlier_rpc_is_still_running(self):
        await self.seed("sandbox-2", "request-2")
        await self.seed("sandbox-3", "request-3")
        for request in ("request-1", "request-2", "request-3"):
            await self.accept(request)
        blocked = asyncio.Event()
        third_started = asyncio.Event()
        stop = asyncio.Event()
        calls = []
        async def wake(op):
            calls.append(op.operation_id)
            if len(calls) == 1:
                await blocked.wait()
            if len(calls) == 3:
                third_started.set()
            return self.proof(op)
        task = asyncio.create_task(WakeDispatcher(self.store, wake, concurrency=2).run(stop))
        try:
            await asyncio.wait_for(third_started.wait(), 2)
            self.assertFalse(blocked.is_set())
        finally:
            blocked.set()
            stop.set()
            await task

    async def test_dispatcher_crash_retains_work_for_other_process(self):
        await self.accept()
        applied = asyncio.Event()
        effects = {}
        async def interrupted_wake(op):
            effects[op.operation_id] = self.proof(op)
            applied.set()
            await asyncio.Event().wait()
        task = asyncio.create_task(WakeDispatcher(self.store, interrupted_wake).run_once())
        await applied.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await self.expire_claims()
        async def recover(op):
            return effects[op.operation_id]
        peer = await self.new_store()
        dispatcher = WakeDispatcher(peer, recover)
        await dispatcher.run_once()
        self.assertEqual(dispatcher.completed, 1)
        self.assertEqual((await peer.snapshot())["nodes"][0]["reserved_restore_mb"], 0)

    async def test_abrupt_process_exit_preserves_claim_and_reservation(self):
        await self.accept()
        code = """
import asyncio,os
from ucloud_sandboxes.shared_control.qualification import QualificationControlStore
async def main():
    store=QualificationControlStore(os.environ['UCLOUD_TEST_POSTGRES_DSN'],'test',schema=os.environ['UCLOUD_TEST_SCHEMA'])
    await store.open()
    op,=await store.claim_due()
    assert await store.prepare_dispatch(op)
    os._exit(23)
asyncio.run(main())
"""
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-c", code, env={**os.environ, "UCLOUD_TEST_SCHEMA": self.schema},
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        _stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        self.assertEqual(process.returncode, 23, stderr.decode())
        self.assertEqual((await self.store.snapshot())["nodes"][0]["reserved_restore_mb"], 128)
        await self.expire_claims()
        op, = await self.store.claim_due()
        self.assertTrue(await self.store.prepare_dispatch(op))
        self.assertTrue(await self.store.complete(op, self.proof(op)))

    async def test_result_size_and_configuration_validation(self):
        with self.assertRaises(ValueError):
            QualificationControlStore(DSN, "test", schema="public")
        for value in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                await self.store.claim_due(lease_seconds=value)
        with self.assertRaises(ValueError):
            await self.accept(body=b"x" * (32 * 1024 * 1024 + 1))


if __name__ == "__main__":
    unittest.main()
