"""Live relay database facilities; no sandbox scheduling or runtime authority."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from importlib.resources import files
import logging
import re
import time
from typing import Callable

from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from .model import DatabaseAdmissionUnavailable, TransactionSample, positive_seconds

LOGGER = logging.getLogger(__name__)


class PostgresDatabase:
    """Bounded connections, observed transactions, and explicit relay migration.

    The qualification store reuses these facilities with its own schema; the
    live service never constructs that scheduling prototype.
    """

    version_table = "relay_schema_version"
    schema_file = "relay_schema.sql"
    schema_version = 1

    def __init__(
        self, dsn: str, deployment_id: str, *, schema: str = "ucloud_shared",
        max_connections: int = 16, timeout_seconds: float = 10,
        observe: Callable[[TransactionSample], None] | None = None,
    ) -> None:
        if not re.fullmatch(r"ucloud_shared(?:_[a-z0-9_]+)?", schema) or len(schema) > 63:
            raise ValueError("invalid shared-control schema")
        if not deployment_id.strip() or max_connections < 1:
            raise ValueError("deployment identity and positive connection budget required")
        self.deployment_id = deployment_id
        self.schema = schema
        self.timeout = positive_seconds(timeout_seconds)
        self.observe = observe
        self._lock_times: ContextVar[list[float] | None] = ContextVar("shared_control_lock_times", default=None)
        self._commit_callbacks: ContextVar[list | None] = ContextVar("shared_control_commit_callbacks", default=None)
        self.pool = AsyncConnectionPool(
            dsn, open=False, min_size=1, max_size=max_connections,
            timeout=self.timeout, kwargs={"autocommit": True, "row_factory": dict_row},
            configure=self._configure,
        )

    async def _configure(self, conn):
        await conn.execute(sql.SQL("SET search_path TO {}, pg_catalog").format(sql.Identifier(self.schema)))
        # Ownership/result acknowledgments must survive a database process crash.
        # A synchronous standby is a separate deployment requirement.
        await conn.execute("SET synchronous_commit = on")
        await conn.execute("SELECT set_config('statement_timeout', %s, false)", (str(max(1, int(self.timeout * 1000))),))

    async def open(self) -> None:
        await self.pool.open(wait=True, timeout=self.timeout)
        try:
            async with self.pool.connection() as conn:
                row = await (await conn.execute(
                    "SELECT to_regclass(%s) AS name",
                    (f"{self.schema}.{self.version_table}",),
                )).fetchone()
                if row["name"] is not None:
                    await self._check_version(conn)
        except BaseException:
            await self.pool.close()
            raise

    async def close(self) -> None:
        await self.pool.close()

    async def _check_version(self, conn):
        row = await (await conn.execute(sql.SQL(
            "SELECT version FROM {} WHERE singleton"
        ).format(sql.Identifier(self.version_table)))).fetchone()
        if row is None or row["version"] != self.schema_version:
            raise ValueError(f"unsupported {self.version_table} version")

    async def migrate(self) -> None:
        """Initialize only this domain's tables; runtime startup never runs DDL."""
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (self.schema,))
            await conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema)))
            row = await (await conn.execute(
                "SELECT to_regclass(%s) AS name",
                (f"{self.schema}.{self.version_table}",),
            )).fetchone()
            if row["name"] is None:
                await conn.execute(files(__package__).joinpath(self.schema_file).read_text())
            await self._check_version(conn)

    @asynccontextmanager
    async def transaction(self, operation: str):
        """One short transaction per acquired connection; cancellation rolls back."""
        started = time.monotonic()
        acquired = body_done = ended = None
        succeeded = False
        lock_times: list[float] = []
        token = self._lock_times.set(lock_times)
        callbacks = []
        callback_token = self._commit_callbacks.set(callbacks)
        try:
            async with self.pool.connection() as conn:
                acquired = time.monotonic()
                # Explicit BEGIN also covers cancellation *during* transaction
                # entry. A context manager whose __aenter__ is interrupted can
                # leave psycopg's savepoint stack active without an __aexit__.
                try:
                    await conn.execute("BEGIN")
                    yield conn
                    body_done = time.monotonic()
                    await conn.execute("COMMIT")
                except BaseException:
                    cleanup = asyncio.create_task(conn.execute("ROLLBACK"))
                    while not cleanup.done():
                        try:
                            await asyncio.shield(cleanup)
                        except asyncio.CancelledError:
                            continue
                        except Exception:
                            break
                    try:
                        cleanup.result()
                    except BaseException:
                        await conn.close()
                    raise
                ended = time.monotonic()
                succeeded = True
                for callback in callbacks:
                    try:
                        callback()
                    except Exception:
                        LOGGER.warning("shared-control post-commit hint failed")
        except PoolTimeout as exc:
            # Only acquisition is known not to have executed database work.
            # A body/commit failure (even the same exception type) must retain
            # its original semantics: its outcome can be ambiguous.
            if acquired is None:
                raise DatabaseAdmissionUnavailable(
                    "relay database admission is temporarily unavailable"
                ) from exc
            raise
        finally:
            self._lock_times.reset(token)
            self._commit_callbacks.reset(callback_token)
            if self.observe is not None:
                now = time.monotonic()
                sample = TransactionSample(
                    operation, (acquired or now) - started,
                    (body_done or ended or now) - (acquired or now),
                    (ended or now) - body_done if body_done is not None else 0,
                    succeeded,
                    sum(lock_times),
                )
                try:
                    self.observe(sample)
                except Exception:
                    LOGGER.exception("shared-control metrics callback failed")

    def after_commit(self, callback):
        callbacks = self._commit_callbacks.get()
        if callbacks is None:
            raise RuntimeError("post-commit callback requires a transaction")
        callbacks.append(callback)

    async def _lock_query(self, conn, query, params):
        started = time.monotonic()
        try:
            return await conn.execute(query, params)
        finally:
            times = self._lock_times.get()
            if times is not None:
                # Includes query execution and round-trip, not only server lock
                # wait. Correlate with pg_stat_activity for that distinction.
                times.append(time.monotonic() - started)
