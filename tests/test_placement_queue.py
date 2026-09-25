"""Durable placement recovery contracts against the canonical PostgreSQL authority."""

import asyncio
from datetime import timedelta
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock
from uuid import uuid4

from ucloud_sandboxes.models import ResourceQuantity, utc_now
from ucloud_sandboxes.routing import SandboxRouteAllocation
from ucloud_sandboxes.sandbox import SandboxSpec, sandbox_spec_fingerprint
from ucloud_sandboxes.shared_control.placement_queue import (
    PlacementQueue,
    PlacementQueueWorker,
)
from ucloud_sandboxes.shared_control.routing_repository import (
    PlacementCommandRejected,
    PostgresRoutingStore,
)

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")


@unittest.skipUnless(DSN, "requires isolated PostgreSQL")
class PlacementQueueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.schema = "ucloud_routing_queue_" + uuid4().hex
        self.routing = PostgresRoutingStore(
            Path(self.temp.name) / "routes", dsn=DSN, schema=self.schema
        )
        self.routing.migrate()
        self.queue = PlacementQueue(DSN, "queue-test", schema=self.schema)
        await self.queue.open()
        self.spec = SandboxSpec(id="queued-sandbox", image="busybox", memory_mb=512)
        self.body = json.dumps(self.spec.to_dict()).encode()
        self.path = "/v1/sandboxes"

    async def asyncTearDown(self):
        import psycopg
        from psycopg import sql

        await self.queue.close()
        self.routing.close()
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
            )
        self.temp.cleanup()

    async def submit(self):
        return (
            await self.queue.submit("create", self.spec.id, self.path, {}, self.body)
        ).command_id

    async def claimed(self):
        await self.submit()
        return (await self.queue.claim("create", 1))[0]

    def execution(self, command, *, path=None, body=None):
        return self.routing.command_execution(
            str(command["command_id"]),
            str(command["claim_token"]),
            self.path if path is None else path,
            self.body if body is None else body,
        )

    def allocate(self, sandbox_id=None):
        return self.routing.allocate_sandbox_create_with_pending(
            SandboxRouteAllocation(
                sandbox_id=sandbox_id or self.spec.id,
                node_id="worker",
                job_id="job",
                node_url="http://worker:8090",
                resources=self.spec.requested_resources(),
                spec={**self.spec.to_dict(), "id": sandbox_id or self.spec.id},
            ),
            spec_hash=sandbox_spec_fingerprint(self.spec),
        )[0]

    async def row(self, command):
        async with self.queue.transaction("test_read") as conn:
            return await (
                await conn.execute(
                    "SELECT * FROM gateway_commands WHERE command_id=%s",
                    (command["command_id"],),
                )
            ).fetchone()

    async def expire_claim(self, command):
        async with self.queue.transaction("test_expire") as conn:
            await conn.execute(
                "UPDATE gateway_commands SET claim_until=clock_timestamp()-interval '1 second' WHERE command_id=%s",
                (command["command_id"],),
            )

    async def pending(self):
        async with self.queue.transaction("test_pending") as conn:
            return await (
                await conn.execute(
                    "SELECT sandbox_id,operation_id,failure_reason,spec_hash FROM pending"
                )
            ).fetchall()

    async def test_single_statement_create_enqueue_records_coalesced_demand(self):
        samples = []
        self.queue.observe = samples.append
        first = await self.submit()
        # A concurrent duplicate joins the live command and its demand row.
        self.assertEqual(await self.submit(), first)
        self.assertEqual(
            await self.pending(),
            [{
                "sandbox_id": self.spec.id,
                "operation_id": str(first),
                "failure_reason": "queued_create",
                "spec_hash": sandbox_spec_fingerprint(self.spec),
            }],
        )
        enqueues = [s for s in samples if s.operation == "placement_enqueue"]
        self.assertEqual(len(enqueues), 2)
        self.assertTrue(all(s.succeeded and s.commit_seconds == 0 for s in enqueues))

    async def test_enqueue_does_not_record_demand_for_existing_route_or_wake(self):
        self.allocate()
        await self.submit()
        await self.queue.submit(
            "wake", "other", "/v1/sandboxes/other/wake", {},
            json.dumps({"generation": 1, "operation_id": "w"}).encode(),
        )
        self.assertEqual(
            [row["failure_reason"] for row in await self.pending()
             if row["failure_reason"] == "queued_create"],
            [],
        )

    async def test_reclaimed_command_fences_old_completion_renewal_and_execution(self):
        first = await self.claimed()
        await self.expire_claim(first)
        second = (await self.queue.claim("create", 1))[0]
        self.assertNotEqual(first["claim_token"], second["claim_token"])
        self.assertEqual(second["attempts"], 2)
        self.assertFalse(await self.queue.renew(first))
        self.assertFalse(await self.queue.complete(first, 201, {}, b"stale"))
        await self.queue.defer(first, delay=0)
        self.assertEqual((await self.row(second))["state"], "running")
        with self.assertRaises(PlacementCommandRejected):
            with self.execution(first):
                self.allocate()
        self.assertTrue(await self.queue.complete(second, 201, {}, b"current"))
        result = (await self.queue.results([second["command_id"]]))[0]
        self.assertEqual(bytes(result["result_body"]), b"current")

    async def test_allocation_and_generation_binding_commit_together(self):
        command = await self.claimed()
        self.assertEqual(
            self.routing.get_pending(self.spec.id).operation_id,
            str(command["command_id"]),
        )
        with self.execution(command):
            route = self.allocate()
        self.assertEqual((await self.row(command))["generation"], route.generation)
        self.assertIsNone(self.routing.get_pending(self.spec.id))
        # Lost replies repeat the same incarnation and durable worker operation.
        with self.execution(command):
            repeated = self.allocate()
        self.assertEqual(repeated.generation, route.generation)
        self.assertEqual(repeated.create_operation_id, route.create_operation_id)

    async def test_proven_create_rejection_can_reallocate_but_external_delete_cancels(
        self,
    ):
        command = await self.claimed()
        with self.execution(command):
            first = self.allocate()
            removed = self.routing.delete_sandbox_if_current(
                first.sandbox_id,
                generation=first.generation,
                create_operation_id=first.create_operation_id,
            )
            self.assertIsNotNone(removed)
            self.assertIsNone((await self.row(command))["generation"])
            second = self.allocate()
        self.assertEqual(second.generation, first.generation + 1)
        self.routing.cancel_create_commands(second.sandbox_id)
        with self.assertRaises(PlacementCommandRejected):
            with self.execution(command):
                self.allocate()
        self.assertEqual((await self.row(command))["result_status"], 410)

    async def test_failed_allocation_rolls_back_route_and_command_binding(self):
        command = await self.claimed()
        with self.execution(command):
            with self.assertRaisesRegex(RuntimeError, "rollback"):

                def fail_after_allocation():
                    self.allocate()
                    raise RuntimeError("rollback")

                self.routing.run_placement(fail_after_allocation)
        self.assertIsNone(self.routing.get_sandbox(self.spec.id))
        self.assertIsNone((await self.row(command))["generation"])
        self.assertIsNotNone(self.routing.get_pending(self.spec.id))

    async def test_replay_after_delete_cannot_recreate_sandbox(self):
        command = await self.claimed()
        with self.execution(command):
            self.allocate()
        self.routing.delete_sandbox(self.spec.id)
        with self.assertRaisesRegex(PlacementCommandRejected, "incarnation"):
            with self.execution(command):
                self.allocate()
        self.assertIsNone(self.routing.get_sandbox(self.spec.id))

    async def test_replay_cannot_target_new_incarnation_with_same_id(self):
        command = await self.claimed()
        with self.execution(command):
            old = self.allocate()
        self.routing.delete_sandbox(self.spec.id)
        current = self.allocate()
        self.assertGreater(current.generation, old.generation)
        with self.assertRaisesRegex(PlacementCommandRejected, "incarnation"):
            with self.execution(command):
                self.allocate()
        self.assertEqual(
            self.routing.get_sandbox(self.spec.id).generation, current.generation
        )

    async def test_mismatched_body_path_or_sandbox_cannot_allocate(self):
        command = await self.claimed()
        for args in ({"body": b"{}"}, {"path": "/v1/sandboxes/other"}):
            with self.subTest(args=args), self.assertRaises(PlacementCommandRejected):
                with self.execution(command, **args):
                    self.allocate()
        with self.execution(command):
            with self.assertRaises(PlacementCommandRejected):
                self.allocate("different-sandbox")
        self.assertIsNone(self.routing.get_sandbox("different-sandbox"))
        self.assertIsNone((await self.row(command))["generation"])

    async def test_terminal_result_cleans_only_its_own_pending_demand(self):
        command = await self.claimed()
        self.assertTrue(await self.queue.complete(command, 422, {}, b"invalid"))
        self.assertIsNone(self.routing.get_pending(self.spec.id))
        command = await self.claimed()
        self.routing.upsert_pending(
            self.spec.id,
            ResourceQuantity(memory_mb=1024),
            operation_id="new-demand",
            failure_reason="memory_pressure",
        )
        self.assertTrue(await self.queue.complete(command, 504, {}, b"timeout"))
        self.assertEqual(
            self.routing.get_pending(self.spec.id).operation_id, "new-demand"
        )

    async def test_expired_unbound_command_cannot_allocate(self):
        command = await self.claimed()
        async with self.queue.transaction("test_deadline") as conn:
            await conn.execute(
                "UPDATE gateway_commands SET deadline=clock_timestamp()-interval '1 second' WHERE command_id=%s",
                (command["command_id"],),
            )
        with self.assertRaisesRegex(PlacementCommandRejected, "expired"):
            with self.execution(command):
                self.allocate()
        self.assertIsNone(self.routing.get_sandbox(self.spec.id))
        command = await self.row(command)
        session = AsyncMock()
        worker = PlacementQueueWorker(self.queue, origin="http://unused", token="test")
        await worker.execute(session, command)
        session.post.assert_not_called()
        result = (await self.queue.results([command["command_id"]]))[0]
        self.assertEqual(result["result_status"], 504)
        self.assertIsNone(self.routing.get_pending(self.spec.id))

    async def test_expired_bound_command_can_reconcile_existing_incarnation(self):
        command = await self.claimed()
        with self.execution(command):
            route = self.allocate()
        async with self.queue.transaction("test_deadline") as conn:
            await conn.execute(
                "UPDATE gateway_commands SET deadline=%s WHERE command_id=%s",
                (utc_now() - timedelta(seconds=1), command["command_id"]),
            )
        with self.execution(command):
            self.assertEqual(self.allocate().generation, route.generation)

    async def test_claims_are_disjoint_and_wakes_do_not_wait_for_create_claims(self):
        ids = [
            await self.queue.submit(
                "create",
                f"unique-{i}",
                self.path,
                {},
                json.dumps({**self.spec.to_dict(), "id": f"unique-{i}"}).encode(),
            )
            for i in range(12)
        ]
        first, second = await asyncio.gather(
            self.queue.claim("create", 6), self.queue.claim("create", 6)
        )
        claimed = [row["command_id"] for row in first + second]
        self.assertEqual(set(claimed), {item.command_id for item in ids})
        self.assertEqual(len(claimed), len(set(claimed)))
        wake_id = await self.queue.submit(
            "wake",
            self.spec.id,
            "/v1/sandboxes/queued-sandbox/wake",
            {},
            b'{"generation":1,"operation_id":"wake-test"}',
        )
        wake = await self.queue.claim("wake", 1)
        self.assertEqual(wake[0]["command_id"], wake_id.command_id)
        self.assertEqual(await self.queue.claim("create", 1), [])

    async def test_reclaim_between_request_validation_and_allocation_is_fenced(self):
        command = await self.claimed()
        with self.execution(command):
            await self.expire_claim(command)
            successor = (await self.queue.claim("create", 1))[0]
            with self.assertRaises(PlacementCommandRejected):
                self.allocate()
        self.assertIsNone(self.routing.get_sandbox(self.spec.id))
        with self.execution(successor):
            self.allocate()

    async def test_existing_route_is_bound_before_idempotent_create_reply(self):
        original = self.allocate()
        command = await self.claimed()
        with self.execution(command):
            # The HTTP fast path can return an existing route without allocation.
            self.assertEqual(
                self.routing.get_sandbox(self.spec.id).generation, original.generation
            )
        self.assertEqual((await self.row(command))["generation"], original.generation)
        self.routing.delete_sandbox(self.spec.id)
        with self.assertRaises(PlacementCommandRejected):
            with self.execution(command):
                pass

    async def test_worker_dispatches_wake_while_create_budget_is_full(self):
        await self.submit()
        create_started = asyncio.Event()
        wake_started = asyncio.Event()
        release_create = asyncio.Event()
        stop = asyncio.Event()
        observed = []

        class BlockedCreateWorker(PlacementQueueWorker):
            async def execute(inner_self, session, command):
                observed.append(command["kind"])
                if command["kind"] == "create":
                    create_started.set()
                    await release_create.wait()
                else:
                    wake_started.set()
                await inner_self.store.complete(command, 200, {}, b"{}")

        # The worker owns its connection-pool lifetime, as a separate process does.
        worker_queue = PlacementQueue(DSN, "queue-test", schema=self.schema)
        worker = BlockedCreateWorker(
            worker_queue,
            origin="http://unused",
            token="test",
            create_concurrency=1,
            wake_concurrency=1,
        )
        task = asyncio.create_task(worker.run(stop))
        try:
            await asyncio.wait_for(create_started.wait(), 3)
            await self.queue.submit(
                "create",
                "second-create",
                self.path,
                {},
                json.dumps({**self.spec.to_dict(), "id": "second-create"}).encode(),
            )
            await self.queue.submit(
                "wake",
                self.spec.id,
                "/v1/sandboxes/queued-sandbox/wake",
                {},
                b'{"generation":1,"operation_id":"wake-independent"}',
            )
            await asyncio.wait_for(wake_started.wait(), 3)
            self.assertFalse(release_create.is_set())
            self.assertEqual(observed, ["create", "wake"])
        finally:
            stop.set()
            release_create.set()
            await asyncio.wait_for(task, 3)
