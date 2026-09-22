from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio
from contextvars import ContextVar
import hashlib
from importlib.resources import files
import json
import logging
import re
import time
from typing import Callable
from uuid import UUID, uuid4

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .model import (
    AcceptedResult, StateConflict, StoredResponse, TransactionSample, WakeOperation, WakeProof,
    positive_seconds,
)

LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = 1
MAX_RESPONSE_BYTES = 32 * 1024 * 1024


class PostgresControlStore:
    """Shared pool/schema primitives plus the scheduling qualification contract.

    The live relay uses the pool and transaction API, with its own domain tables.
    Fixture-based sandbox scheduling methods remain qualification-only; this is
    not a RoutingStore replacement. Worker RPCs never occur in transactions.
    """

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
                row = await (await conn.execute("SELECT to_regclass(%s) AS name", (f"{self.schema}.schema_version",))).fetchone()
                if row["name"] is not None:
                    await self._check_version(conn)
        except BaseException:
            await self.pool.close()
            raise

    async def close(self) -> None:
        await self.pool.close()

    @staticmethod
    async def _check_version(conn):
        row = await (await conn.execute("SELECT version FROM schema_version WHERE singleton")).fetchone()
        if row is None or row["version"] != SCHEMA_VERSION:
            raise ValueError("unsupported shared-control schema version")

    async def migrate(self) -> None:
        """Explicit, transactional, serialized initialization; never startup DDL."""
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (self.schema,))
            await conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema)))
            row = await (await conn.execute("SELECT to_regclass(%s) AS name", (f"{self.schema}.schema_version",))).fetchone()
            if row["name"] is None:
                await conn.execute(files(__package__).joinpath("schema.sql").read_text())
            await self._check_version(conn)
            relay = await (await conn.execute("SELECT to_regclass(%s) AS name", (f"{self.schema}.relay_schema_version",))).fetchone()
            if relay["name"] is None:
                await conn.execute(files(__package__).joinpath("relay_schema.sql").read_text())
            row = await (await conn.execute("SELECT version FROM relay_schema_version WHERE singleton")).fetchone()
            if row is None or row["version"] != 1:
                raise ValueError("unsupported relay schema version")

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

    async def _lock_owner(self, conn, sandbox_id: str, node_id: str):
        node = await (await self._lock_query(conn,
            "SELECT * FROM nodes WHERE deployment_id=%s AND node_id=%s FOR UPDATE",
            (self.deployment_id, node_id),
        )).fetchone()
        sandbox = await (await self._lock_query(conn,
            "SELECT * FROM sandboxes WHERE deployment_id=%s AND sandbox_id=%s FOR UPDATE",
            (self.deployment_id, sandbox_id),
        )).fetchone()
        if node is None or sandbox is None or sandbox["node_id"] != node_id or sandbox["node_epoch"] != node["node_epoch"]:
            raise StateConflict("sandbox owner changed or is unavailable")
        return node, sandbox

    async def accept_result(
        self, request_id: str, *, registration_id: str, lease_id: str, body: bytes,
        status: int = 200, headers: dict[str, str] | None = None,
    ) -> AcceptedResult:
        if not isinstance(body, bytes) or len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("response must be bytes within the relay body limit")
        headers = dict(headers or {})
        if isinstance(status, bool) or not isinstance(status, int) or not 100 <= status <= 599:
            raise ValueError("invalid response status")
        for key, value in headers.items():
            if (not isinstance(key, str) or not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", key)
                    or not isinstance(value, str) or '\r' in value or '\n' in value):
                raise ValueError("invalid response header")
        envelope = json.dumps([status, headers], sort_keys=True, separators=(',', ':')).encode()
        if len(envelope) > 65536:
            raise ValueError("response headers exceed metadata limit")
        digest = hashlib.sha256(len(envelope).to_bytes(4, 'big') + envelope + body).hexdigest()
        async with self.transaction("accept_result") as conn:
            hint = await (await conn.execute(
                """SELECT r.sandbox_id, s.node_id FROM model_requests r JOIN sandboxes s
                   USING (deployment_id, sandbox_id) WHERE r.deployment_id=%s AND r.request_id=%s""",
                (self.deployment_id, request_id),
            )).fetchone()
            if hint is None:
                raise StateConflict("unknown model request")
            # Accepting a result creates intent but reserves no node resources.
            # Only the incarnation/request need serialization here. Taking the
            # node lock would serialize every agent's model response on its host.
            sandbox = await (await self._lock_query(conn,
                "SELECT * FROM sandboxes WHERE deployment_id=%s AND sandbox_id=%s FOR UPDATE",
                (self.deployment_id, hint["sandbox_id"]),
            )).fetchone()
            if sandbox is None:
                raise StateConflict("sandbox was removed")
            request = await (await self._lock_query(conn,
                """SELECT *, lease_until > clock_timestamp() AS lease_valid FROM model_requests
                   WHERE deployment_id=%s AND request_id=%s FOR UPDATE""",
                (self.deployment_id, request_id),
            )).fetchone()
            if (request["registration_id"], request["lease_id"], request["generation"]) != (
                registration_id, lease_id, sandbox["generation"],
            ):
                raise StateConflict("model request incarnation or lease mismatch")
            if request["response_hash"] is not None:
                if request["response_hash"] != digest:
                    raise StateConflict("committed model result differs")
                # An acknowledged result stays replayable after inference lease expiry.
                return AcceptedResult(request_id, request["operation_id"], digest, True)
            if not request["lease_valid"]:
                raise StateConflict("model inference lease expired")
            operation_id = await self._ensure_wake(conn, sandbox)
            await conn.execute(
                "INSERT INTO model_responses VALUES (%s, %s, %s, %s, %s)",
                (self.deployment_id, request_id, body, status, Jsonb(headers)),
            )
            await conn.execute(
                """UPDATE model_requests SET response_hash=%s, operation_id=%s,
                   result_committed_at=clock_timestamp() WHERE deployment_id=%s AND request_id=%s""",
                (digest, operation_id, self.deployment_id, request_id),
            )
            return AcceptedResult(request_id, operation_id, digest, False)

    async def _ensure_wake(self, conn, sandbox) -> UUID | None:
        if sandbox["state"] == "running":
            return None
        existing = await (await conn.execute(
            "SELECT operation_id FROM wake_operations WHERE deployment_id=%s AND sandbox_id=%s AND state!='succeeded'",
            (self.deployment_id, sandbox["sandbox_id"]),
        )).fetchone()
        if existing is not None:
            return existing["operation_id"]
        operation_id = uuid4()
        sequence = sandbox["lifecycle_sequence"] + 1
        await conn.execute(
            """INSERT INTO wake_operations (
                deployment_id, operation_id, sandbox_id, generation, create_operation_id,
                spec_hash, node_id, node_epoch, lifecycle_sequence, restore_mb, state
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'queued')""",
            (self.deployment_id, operation_id, sandbox["sandbox_id"], sandbox["generation"],
             sandbox["create_operation_id"], sandbox["spec_hash"], sandbox["node_id"],
             sandbox["node_epoch"], sequence, sandbox["restore_mb"]),
        )
        await conn.execute(
            "UPDATE sandboxes SET lifecycle_sequence=%s WHERE deployment_id=%s AND sandbox_id=%s",
            (sequence, self.deployment_id, sandbox["sandbox_id"]),
        )
        return operation_id

    async def claim_due(self, *, limit: int = 16, lease_seconds: float = 30) -> list[WakeOperation]:
        if limit < 1:
            raise ValueError("claim batch must be positive")
        positive_seconds(lease_seconds)
        async with self.transaction("claim_due") as conn:
            rows = await (await self._lock_query(conn,
                """WITH due AS (
                    SELECT deployment_id, operation_id FROM wake_operations
                    WHERE deployment_id=%s AND state!='succeeded'
                      AND next_attempt_at <= clock_timestamp()
                      AND (claim_until IS NULL OR claim_until <= clock_timestamp())
                    ORDER BY next_attempt_at, created_at, operation_id
                    FOR UPDATE SKIP LOCKED LIMIT %s
                ) UPDATE wake_operations w SET claim_token=%s,
                    claim_until=clock_timestamp() + %s * interval '1 second', attempts=attempts+1
                  FROM due WHERE w.deployment_id=due.deployment_id AND w.operation_id=due.operation_id
                  RETURNING w.*""",
                (self.deployment_id, limit, uuid4(), lease_seconds),
            )).fetchall()
            return [self._operation(row) for row in rows]

    @staticmethod
    def _operation(row) -> WakeOperation:
        return WakeOperation(**{name: row[name] for name in WakeOperation.__dataclass_fields__})

    async def _lock_operation(self, conn, operation: WakeOperation):
        node, sandbox = await self._lock_owner(conn, operation.sandbox_id, operation.node_id)
        row = await (await self._lock_query(conn,
            """SELECT *, claim_until > clock_timestamp() AS claim_valid FROM wake_operations
               WHERE deployment_id=%s AND operation_id=%s FOR UPDATE""",
            (self.deployment_id, operation.operation_id),
        )).fetchone()
        if row is None or row["claim_token"] != operation.claim_token or not row["claim_valid"] or row["state"] == "succeeded":
            return None
        for name in ("generation", "create_operation_id", "spec_hash", "node_id", "node_epoch", "lifecycle_sequence", "restore_mb"):
            if row[name] != getattr(operation, name) or sandbox[name] != row[name]:
                raise StateConflict("wake no longer matches its sandbox owner")
        return node, sandbox, row

    async def prepare_dispatch(self, operation: WakeOperation, *, retry_seconds: float = .1) -> bool:
        positive_seconds(retry_seconds)
        async with self.transaction("prepare_dispatch") as conn:
            locked = await self._lock_operation(conn, operation)
            if locked is None:
                return False
            node, _sandbox, row = locked
            if row["state"] == "dispatching":
                # An expired claim does not release an uncertain physical reservation.
                return True
            if node["reserved_restore_mb"] + operation.restore_mb > node["restore_budget_mb"]:
                await self._release_claim(conn, operation, retry_seconds, "restore_capacity")
                return False
            await conn.execute(
                "INSERT INTO restore_reservations VALUES (%s,%s,%s,%s)",
                (self.deployment_id, operation.operation_id, operation.node_id, operation.restore_mb),
            )
            await conn.execute(
                "UPDATE nodes SET reserved_restore_mb=reserved_restore_mb+%s WHERE deployment_id=%s AND node_id=%s",
                (operation.restore_mb, self.deployment_id, operation.node_id),
            )
            await conn.execute(
                "UPDATE wake_operations SET state='dispatching', last_reason=NULL WHERE deployment_id=%s AND operation_id=%s",
                (self.deployment_id, operation.operation_id),
            )
            await conn.execute(
                "UPDATE sandboxes SET state='waking' WHERE deployment_id=%s AND sandbox_id=%s",
                (self.deployment_id, operation.sandbox_id),
            )
            return True

    async def _release_claim(self, conn, operation, retry_seconds, reason):
        await conn.execute(
            """UPDATE wake_operations SET claim_token=NULL, claim_until=NULL,
               next_attempt_at=clock_timestamp() + %s * interval '1 second', last_reason=%s
               WHERE deployment_id=%s AND operation_id=%s AND claim_token=%s""",
            (retry_seconds, reason, self.deployment_id, operation.operation_id, operation.claim_token),
        )

    async def retry(self, operation: WakeOperation, *, delay_seconds: float = .1) -> bool:
        positive_seconds(delay_seconds)
        async with self.transaction("retry") as conn:
            if await self._lock_operation(conn, operation) is None:
                return False
            await self._release_claim(conn, operation, delay_seconds, "worker_outcome_uncertain")
            return True

    async def complete(self, operation: WakeOperation, proof: WakeProof) -> bool:
        if (proof.operation_id, proof.generation, proof.node_epoch, proof.lifecycle_sequence) != (
            operation.operation_id, operation.generation, operation.node_epoch, operation.lifecycle_sequence,
        ) or proof.activity_epoch < 0:
            raise StateConflict("wake acknowledgment identity mismatch")
        async with self.transaction("complete") as conn:
            locked = await self._lock_operation(conn, operation)
            if locked is None:
                return False
            _node, sandbox, row = locked
            if row["state"] != "dispatching" or proof.activity_epoch <= sandbox["activity_epoch"]:
                raise StateConflict("wake acknowledgment lacks newer activity proof")
            reservation = await (await conn.execute(
                "DELETE FROM restore_reservations WHERE deployment_id=%s AND operation_id=%s RETURNING amount_mb",
                (self.deployment_id, operation.operation_id),
            )).fetchone()
            if reservation is None:
                raise StateConflict("wake lost its physical reservation")
            await conn.execute(
                "UPDATE nodes SET reserved_restore_mb=reserved_restore_mb-%s WHERE deployment_id=%s AND node_id=%s",
                (reservation["amount_mb"], self.deployment_id, operation.node_id),
            )
            await conn.execute(
                """UPDATE sandboxes SET state='running', activity_epoch=%s
                   WHERE deployment_id=%s AND sandbox_id=%s""",
                (proof.activity_epoch, self.deployment_id, operation.sandbox_id),
            )
            await conn.execute(
                """UPDATE wake_operations SET state='succeeded', completed_at=clock_timestamp(),
                   claim_token=NULL, claim_until=NULL, last_reason=NULL
                   WHERE deployment_id=%s AND operation_id=%s""",
                (self.deployment_id, operation.operation_id),
            )
            return True

    async def read_response(self, request_id: str, *, registration_id: str) -> StoredResponse | None:
        async with self.transaction("read_result") as conn:
            row = await (await conn.execute(
                """SELECT r.registration_id, b.body, b.status, b.headers, o.state AS operation_state
                   FROM model_requests r LEFT JOIN model_responses b USING (deployment_id, request_id)
                   LEFT JOIN wake_operations o ON o.deployment_id=r.deployment_id AND o.operation_id=r.operation_id
                   WHERE r.deployment_id=%s AND r.request_id=%s""",
                (self.deployment_id, request_id),
            )).fetchone()
            if row is None or row["registration_id"] != registration_id:
                raise StateConflict("unknown request registration")
            if row["operation_state"] not in (None, "succeeded"):
                return None
            return StoredResponse(row["status"], row["headers"], bytes(row["body"])) if row["body"] is not None else None

    async def read_result(self, request_id: str, *, registration_id: str) -> bytes | None:
        response = await self.read_response(request_id, registration_id=registration_id)
        return response.body if response is not None else None

    async def snapshot(self) -> dict:
        """Bounded diagnostic counts; never use these to make placement decisions."""
        async with self.transaction("snapshot") as conn:
            nodes = await (await conn.execute(
                "SELECT node_id, reserved_restore_mb, restore_budget_mb FROM nodes WHERE deployment_id=%s ORDER BY node_id",
                (self.deployment_id,),
            )).fetchall()
            operations = await (await conn.execute(
                "SELECT state, count(*) AS count FROM wake_operations WHERE deployment_id=%s GROUP BY state",
                (self.deployment_id,),
            )).fetchall()
            responses = await (await conn.execute(
                "SELECT count(*) AS count FROM model_responses WHERE deployment_id=%s", (self.deployment_id,),
            )).fetchone()
            return {"nodes": nodes, "operations": {r["state"]: r["count"] for r in operations}, "responses": responses["count"]}
