"""Real PostgreSQL contract for the shared pool, transactions and migration.

CI supplies UCLOUD_TEST_POSTGRES_DSN; no SQL mocks.
"""
from __future__ import annotations

import asyncio
import os
import unittest
from uuid import uuid4

TEST_TIER = "contract"

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")
if DSN:
    import psycopg
    from psycopg import sql
    from ucloud_sandboxes.shared_control.database import PostgresDatabase


@unittest.skipUnless(DSN, "set UCLOUD_TEST_POSTGRES_DSN for the real PostgreSQL contract")
class SharedControlPostgresTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.schema = "ucloud_shared_test_" + uuid4().hex
        self.stores = []
        self.samples = []
        self.store = await self.new_store()
        await self.store.migrate()

    async def new_store(self, max_connections=8):
        store = PostgresDatabase(DSN, "test", schema=self.schema, max_connections=max_connections,
                                 observe=self.samples.append)
        await store.open()
        self.stores.append(store)
        return store

    async def asyncTearDown(self):
        for store in self.stores:
            await store.close()
        async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
            await conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    async def quota_rows(self, store):
        async with store.transaction("test_read") as conn:
            row = await (await conn.execute("SELECT count(*) AS n FROM relay_quota")).fetchone()
        return row["n"]

    async def test_cancelled_transaction_rolls_back_and_returns_connection(self):
        store = await self.new_store(max_connections=1)
        entered = asyncio.Event()

        async def interrupted():
            async with store.transaction("cancel") as conn:
                await conn.execute("INSERT INTO relay_quota (deployment_id) VALUES ('cancelled')")
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(interrupted())
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        # The single pooled connection must be reusable after the rollback.
        self.assertEqual(await asyncio.wait_for(self.quota_rows(store), 5), 0)
        self.assertFalse(next(s for s in self.samples if s.operation == "cancel").succeeded)

    async def test_migration_is_idempotent_and_newer_version_is_rejected(self):
        await asyncio.gather(self.store.migrate(), self.store.migrate())
        async with self.store.transaction("future_schema") as conn:
            await conn.execute("UPDATE relay_schema_version SET version=2")
        with self.assertRaises(ValueError):
            await self.new_store()

    async def test_configuration_validation(self):
        for kwargs in ({"schema": "public"}, {"max_connections": 0}, {"timeout_seconds": float("nan")}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                PostgresDatabase(DSN, "test", **{"schema": self.schema, **kwargs})
        with self.assertRaises(ValueError):
            PostgresDatabase(DSN, " ", schema=self.schema)


if __name__ == "__main__":
    unittest.main()
