"""Completed replies retain database access while submission admission is full."""

import asyncio
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

from ucloud_sandboxes.shared_control.database import postgres_transaction_observer
from ucloud_sandboxes.shared_control.model import TransactionSample
from ucloud_sandboxes.shared_control.placement_queue import (
    PlacementQueue,
    PlacementQueueClient,
)
from ucloud_sandboxes.shared_control.routing_repository import PostgresRoutingStore

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")


class CompletionReaderLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_open_failure_closes_both_pools(self):
        writer, reader = Mock(), Mock()
        writer.open, writer.close = AsyncMock(), AsyncMock()
        reader.open, reader.close = (
            AsyncMock(side_effect=OSError("unavailable")),
            AsyncMock(),
        )
        client = PlacementQueueClient(writer, results_store=reader)
        with self.assertRaises(OSError):
            await client.open()
        writer.close.assert_awaited_once()
        reader.close.assert_awaited_once()
        self.assertFalse(client._opened)

    async def test_shutdown_is_terminal_even_after_failed_startup(self):
        writer, reader = Mock(), Mock()
        writer.open, writer.close = AsyncMock(), AsyncMock()
        reader.open, reader.close = (
            AsyncMock(side_effect=OSError("unavailable")),
            AsyncMock(),
        )
        client = PlacementQueueClient(writer, results_store=reader)
        with self.assertRaises(OSError):
            await client.open()
        await client.close()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            await client.open()
        writer.fresh.assert_not_called()
        reader.fresh.assert_not_called()

    async def test_cancelled_open_closes_both_pools(self):
        writer, reader = Mock(), Mock()
        writer.open, writer.close = AsyncMock(), AsyncMock()
        started = asyncio.Event()

        async def opening():
            started.set()
            await asyncio.Event().wait()

        reader.open, reader.close = opening, AsyncMock()
        client = PlacementQueueClient(writer, results_store=reader)
        task = asyncio.create_task(client.open())
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        writer.close.assert_awaited_once()
        reader.close.assert_awaited_once()

    async def test_close_failure_still_closes_other_pool(self):
        writer, reader = Mock(), Mock()
        writer.open, writer.close = AsyncMock(), AsyncMock()
        reader.open, reader.close = (
            AsyncMock(),
            AsyncMock(side_effect=OSError("close failed")),
        )
        client = PlacementQueueClient(writer, results_store=reader)
        await client.open()
        with self.assertRaises(OSError):
            await client.close()
        writer.close.assert_awaited_once()
        self.assertFalse(client._opened)

    async def test_submit_returning_after_shutdown_cannot_resurrect_waiter(self):
        writer, reader = Mock(), Mock()
        writer.open, writer.close = AsyncMock(), AsyncMock()
        reader.open, reader.close = AsyncMock(), AsyncMock()
        submitted, release = asyncio.Event(), asyncio.Event()
        accepted = Mock()
        accepted.command_id = uuid4()

        async def delayed_submit(*args):
            submitted.set()
            await release.wait()
            return accepted

        writer.submit = delayed_submit
        client = PlacementQueueClient(writer, results_store=reader)
        task = asyncio.create_task(client.response("wake", "s", "/wake", {}, b"{}"))
        await submitted.wait()
        await client.close()
        release.set()
        response = await asyncio.wait_for(task, 1)
        self.assertEqual(response[0], 503)
        payload = json.loads(response[2])
        self.assertEqual(payload["error_code"], "placement_outcome_unknown")
        self.assertTrue(payload["retryable"])
        self.assertEqual(client.waiters, {})
        self.assertIsNone(client._poller)
        writer.close.assert_awaited_once()
        reader.close.assert_awaited_once()
        reader.results.assert_not_called()

    def test_shared_observer_reports_existing_phase_metric(self):
        telemetry = Mock()
        callback = postgres_transaction_observer(telemetry)
        callback(TransactionSample("placement_results", 0.1, 0.2, 0.3, True))
        records = telemetry.meter.create_histogram.return_value.record.call_args_list
        self.assertEqual(
            [r.args[1]["phase"] for r in records],
            ["pool_wait", "transaction", "commit", "lock_query"],
        )
        self.assertTrue(
            all(r.args[1]["operation"] == "placement_results" for r in records)
        )
        self.assertEqual(
            telemetry.meter.create_histogram.call_args.args[0],
            "ucloud.platform.postgres.duration",
        )


@unittest.skipUnless(DSN, "requires isolated PostgreSQL")
class CompletionReaderPostgresTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.schema = "ucloud_routing_reads_" + uuid4().hex
        self.routing = PostgresRoutingStore(
            Path(self.temp.name) / "routes", dsn=DSN, schema=self.schema
        )
        self.routing.migrate()
        self.samples = []
        writer = PlacementQueue(
            DSN,
            "reads-test",
            schema=self.schema,
            max_connections=1,
            observe=self.samples.append,
        )
        self.client = PlacementQueueClient(writer)
        await self.client.open()
        self.worker = PlacementQueue(DSN, "reads-test", schema=self.schema)
        await self.worker.open()
        self.tasks = []

    async def asyncTearDown(self):
        import psycopg
        from psycopg import sql

        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.client.close()
        await self.worker.close()
        self.routing.close()
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
            )
        self.temp.cleanup()

    async def request(self, sid):
        task = asyncio.create_task(
            self.client.response(
                "wake",
                sid,
                f"/v1/sandboxes/{sid}/wake",
                {},
                json.dumps({"generation": 1, "operation_id": "wake-" + sid}).encode(),
            )
        )
        self.tasks.append(task)
        return task

    async def wait_until(self, predicate):
        async with asyncio.timeout(2):
            while not predicate():
                await asyncio.sleep(0.005)

    async def test_partial_open_failure_recreates_pools_and_next_request_completes(
        self,
    ):
        writer = PlacementQueue(
            DSN, "reads-test", schema=self.schema, max_connections=2
        )
        reader = writer.completion_reader()
        original_open = reader.open

        async def fail_after_open():
            await original_open()
            raise OSError("transient startup failure")

        reader.open = fail_after_open
        client = PlacementQueueClient(writer, results_store=reader)
        try:
            failed = await client.response(
                "wake",
                "startup",
                "/v1/sandboxes/startup/wake",
                {},
                b'{"generation":1,"operation_id":"startup"}',
            )
            self.assertEqual(failed[0], 503)
            self.assertTrue(writer.pool.closed)
            self.assertTrue(reader.pool.closed)
            retry = asyncio.create_task(
                client.response(
                    "wake",
                    "startup",
                    "/v1/sandboxes/startup/wake",
                    {},
                    b'{"generation":1,"operation_id":"startup"}',
                )
            )
            self.tasks.append(retry)
            await self.wait_until(lambda: bool(client.waiters))
            self.assertIsNot(client.store, writer)
            self.assertIsNot(client.results_store, reader)
            self.assertEqual(client.store.pool.max_size, 2)
            self.assertEqual(client.results_store.pool.max_size, 1)
            command = (await self.worker.claim("wake", 1))[0]
            await self.worker.complete(command, 200, {}, b"recovered startup")
            self.assertEqual(
                (await asyncio.wait_for(retry, 2))[2], b"recovered startup"
            )
        finally:
            await client.close()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            await client.open()

    async def test_completed_reply_bypasses_saturated_submission_pool(self):
        completed = await self.request("first")
        await self.wait_until(lambda: len(self.client.waiters) == 1)
        command = (await self.worker.claim("wake", 1))[0]
        async with self.client.store.pool.connection():
            waiting = await self.request("second")
            await self.wait_until(
                lambda: (
                    self.client.store.pool.get_stats().get("requests_waiting", 0) > 0
                )
            )
            await self.worker.complete(
                command, 200, {"Content-Type": "application/json"}, b'{"ok":true}'
            )
            response = await asyncio.wait_for(asyncio.shield(completed), 1)
            self.assertEqual(
                response, (200, {"Content-Type": "application/json"}, b'{"ok":true}')
            )
            self.assertFalse(waiting.done())
        self.assertIsNot(self.client.store.pool, self.client.results_store.pool)
        self.assertEqual(self.client.results_store.pool.max_size, 1)
        self.assertTrue(
            {"placement_enqueue", "placement_results"}
            <= {s.operation for s in self.samples}
        )
        self.assertTrue(
            all(
                s.pool_wait_seconds >= 0
                and s.transaction_seconds >= 0
                and s.commit_seconds >= 0
                for s in self.samples
            )
        )

    async def test_transient_result_read_failure_preserves_waiter_and_recovers(self):
        original = self.client.results_store.results
        failed = asyncio.Event()
        allow_recovery = asyncio.Event()

        async def interrupted(ids):
            if not failed.is_set():
                failed.set()
                raise OSError("temporary read outage")
            await allow_recovery.wait()
            return await original(ids)

        self.client.results_store.results = interrupted
        task = await self.request("recover")
        await asyncio.wait_for(failed.wait(), 2)
        command = (await self.worker.claim("wake", 1))[0]
        await self.worker.complete(command, 200, {}, b"recovered")
        self.assertFalse(task.done())
        self.assertEqual(len(self.client.waiters), 1)
        allow_recovery.set()
        self.assertEqual(
            (await asyncio.wait_for(asyncio.shield(task), 2))[2], b"recovered"
        )
