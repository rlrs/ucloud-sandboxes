"""One real PostgreSQL fixture for relay HTTP and stateful contract tests."""

from contextlib import asynccontextmanager
import os
from unittest import SkipTest
from uuid import uuid4


@asynccontextmanager
async def postgres_database():
    dsn = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")
    if not dsn:
        raise SkipTest("requires real PostgreSQL (UCLOUD_TEST_POSTGRES_DSN)")
    import psycopg
    from psycopg import sql
    from ucloud_sandboxes.shared_control.database import PostgresDatabase

    schema = "ucloud_shared_contract_" + uuid4().hex
    database = PostgresDatabase(dsn, "contract", schema=schema, max_connections=4)
    await database.open()
    await database.migrate()
    await database.close()
    try:
        yield PostgresDatabase(dsn, "contract", schema=schema, max_connections=4)
    finally:
        async with await psycopg.AsyncConnection.connect(
            dsn, autocommit=True
        ) as connection:
            await connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
