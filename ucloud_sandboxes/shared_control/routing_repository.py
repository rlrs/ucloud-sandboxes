"""PostgreSQL persistence for the canonical routing domain operations.

All routing domains move together: lifecycle, migrations, generations, execs,
program observations, demand and snapshot liveness. Reusing the domain methods
avoids a second implementation of their safety rules. The persistence boundary
supplies serializable transactions and PostgreSQL bulk queries, never a process
lock or a SQLite writer queue.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import replace
from functools import wraps
from importlib.resources import files
import json
import os
from pathlib import Path
import random
import re
import sqlite3
import time

import psycopg
from psycopg import sql
from psycopg.errors import DeadlockDetected, SerializationFailure
from psycopg_pool import ConnectionPool

from ..routing import RoutingStore, PlacementCommandRejected
from ..models import utc_now
from uuid import UUID
from ..telemetry import Telemetry


class _Row(dict):
    """Named domain rows, also allowing the count/projection queries' offsets."""

    def __getitem__(self, key):
        if isinstance(key, int):
            return tuple(self.values())[key]
        return super().__getitem__(key)

    def __iter__(self):
        return iter(self.values())


def _row_factory(cursor):
    names = [column.name for column in cursor.description] if cursor.description else []
    return lambda values: _Row(zip(names, values))


class _Connection:
    """DB-API parameter-style boundary; SQL dialects are explicit methods below."""

    def __init__(self, connection, *, coherent_reader=False):
        self.connection = connection
        self.coherent_reader = coherent_reader

    def execute(self, query, parameters=()):
        if self.coherent_reader and query.strip().upper() == "BEGIN":
            # The PostgreSQL reader already owns its repeatable-read snapshot.
            return None
        # Domain queries use qmark parameters and contain no question-mark
        # literals/operators. Values always remain separate bound parameters.
        return self.connection.execute(query.replace("?", "%s"), parameters)

    def executemany(self, query, parameters):
        cursor = self.connection.cursor()
        cursor.executemany(query.replace("?", "%s"), parameters)
        return cursor


def _transactional(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        if self._current.get() is not None:
            return method(self, *args, **kwargs)
        # A transaction retry must not reuse an exhausted inventory iterator.
        args = tuple(tuple(x) if isinstance(x, Iterator) else x for x in args)
        kwargs = {
            k: tuple(v) if isinstance(v, Iterator) else v for k, v in kwargs.items()
        }
        deadline = time.monotonic() + self.timeout
        delay = 0.001
        while True:
            try:
                with self._transaction():
                    return method(self, *args, **kwargs)
            except (SerializationFailure, DeadlockDetected):
                self.serialization_retries += 1
                # These SQLSTATEs prove rollback. Connection/COMMIT failures do
                # not, and must propagate without replaying a mutation.
                if time.monotonic() >= deadline:
                    raise sqlite3.DatabaseError(
                        "routing transaction could not acquire a stable snapshot"
                    ) from None
                time.sleep(random.uniform(0, delay))
                delay = min(delay * 2, 0.05)
            except psycopg.Error as exc:
                # Preserve the current domain's availability exception contract.
                # Never expose DSNs or driver diagnostics to API clients.
                raise sqlite3.DatabaseError(
                    "PostgreSQL routing operation failed"
                ) from exc

    return call


class PostgresRoutingStore(RoutingStore):
    distributed = True
    _program_index_hint = ""
    _json_values_query = "SELECT jsonb_array_elements_text(?::jsonb)"
    _expired_signal_predicate = """
        CASE WHEN pg_input_is_valid(COALESCE(NULLIF(updated_at,''),created_at),'timestamptz')
        THEN COALESCE(NULLIF(updated_at,''),created_at)::timestamptz END <= ?::timestamptz
    """

    def __init__(
        self,
        path: Path,
        *,
        dsn: str,
        schema: str,
        timeout_seconds=30,
        max_connections=16,
    ):
        if (
            not re.fullmatch(r"ucloud_routing(?:_[a-z0-9_]+)?", schema)
            or len(schema) > 63
        ):
            raise ValueError("invalid PostgreSQL routing schema")
        self.path = Path(path)
        self.schema = schema
        self.timeout = timeout_seconds
        self.serialization_retries = 0
        self.telemetry = Telemetry.disabled("routing-authority")
        self._pid = os.getpid()
        self._authority_identity = None
        self._current = ContextVar("routing_connection", default=None)
        self._placement_snapshot = ContextVar("placement_snapshot", default=False)
        self._placement_worker = ContextVar("placement_worker", default=None)
        self._capacity_touched = ContextVar("capacity_touched", default=None)
        self._command = ContextVar("placement_command", default=None)
        self._lock = nullcontext()
        self._fleet_read_lock = nullcontext()
        self.pool = ConnectionPool(
            dsn,
            min_size=1,
            max_size=max_connections,
            timeout=timeout_seconds,
            kwargs={"autocommit": True, "row_factory": _row_factory},
            configure=self._configure,
            open=True,
        )
        self.pool.wait(timeout=timeout_seconds)

    def _configure(self, conn):
        conn.execute(
            sql.SQL("SET search_path TO {}, pg_catalog").format(
                sql.Identifier(self.schema)
            )
        )
        conn.execute("SET synchronous_commit=on")
        conn.execute(
            "SELECT set_config('statement_timeout',%s,false)",
            (str(int(self.timeout * 1000)),),
        )

    def close(self):
        self.pool.close()

    def bind_authority(self):
        info = self.path.stat()
        self._authority_identity = (info.st_dev, info.st_ino)

    def validate_authority(self):
        if os.getpid() != self._pid:
            raise sqlite3.DatabaseError("reopen PostgreSQL routing store after fork")
        if self._authority_identity is not None:
            info = self.path.stat()
            if (info.st_dev, info.st_ino) != self._authority_identity:
                raise sqlite3.DatabaseError(
                    "routing authority descriptor was replaced; restart required"
                )

    @_transactional
    def _placement_attempt(self, operation, outcomes):
        try:
            return True, operation()
        except outcomes as outcome:
            # A deferred admission may have recorded autoscaler demand. Commit
            # that domain result before returning it to the caller; exceptions
            # indicating broken invariants or database failures still roll back.
            return False, outcome

    def run_placement(self, operation, *, outcomes=(), worker_id=None):
        # Worker revision writes turn capacity predicates into exact write/write
        # conflicts. Snapshot isolation avoids SSI's fleet-wide index-page false
        # conflicts without admitting against mixed snapshots. Non-placement
        # domain operations retain SERIALIZABLE until separately qualified.
        token = self._placement_snapshot.set(True)
        worker_token = self._placement_worker.set(worker_id)
        try:
            succeeded, result = self._placement_attempt(operation, outcomes)
            if not succeeded:
                raise result
            return result
        finally:
            self._placement_snapshot.reset(token)
            self._placement_worker.reset(worker_token)

    def _capacity_fence(self, conn, *identities):
        touched = self._capacity_touched.get()
        if touched is None:
            raise RuntimeError("capacity mutation outside routing transaction")
        keys = set()
        for node_id, job_id, node_url in identities:
            keys.update(
                prefix + value
                for prefix, value in (
                    ("node:", node_id),
                    ("job:", job_id),
                    ("url:", node_url.rstrip("/")),
                )
                if value
            )
        for key in sorted(keys - touched):
            conn.execute(
                """INSERT INTO worker_capacity_revisions(identity,revision) VALUES (?,1)
                ON CONFLICT(identity) DO UPDATE SET revision=worker_capacity_revisions.revision+1""",
                (key,),
            )
            touched.add(key)

    def _fence_route(self, conn, route):
        self._capacity_fence(conn, (route.node_id, route.job_id, route.node_url))

    @staticmethod
    def _same_capacity_projection(previous, route):
        if previous is None:
            return False
        # Running receipts refresh one row's inventory proof. No aggregate
        # admission or cold-detach predicate consumes that activity epoch.
        # Parked proofs still fence because cold offload observes their epoch.
        activity_epoch = (
            route.activity_epoch
            if previous.state == route.state == "running"
            else previous.activity_epoch
        )
        return (
            replace(
                previous, updated_at=route.updated_at, activity_epoch=activity_epoch
            )
            == route
        )

    def _write_sandbox(self, conn, route):
        previous = self._get_sandbox_unlocked(conn, route.sandbox_id)
        identities = [(route.node_id, route.job_id, route.node_url)]
        if previous is not None:
            identities.append((previous.node_id, previous.job_id, previous.node_url))
        if not self._same_capacity_projection(previous, route):
            self._capacity_fence(conn, *identities)
        return super()._write_sandbox(conn, route)

    def _write_sandbox_lifecycle(self, conn, route):
        previous = self._get_sandbox_unlocked(conn, route.sandbox_id)
        # Persist freshness without serializing an unchanged running owner with
        # all other sandboxes. New domain fields remain fenced by default.
        if not self._same_capacity_projection(previous, route):
            self._fence_route(conn, route)
        return super()._write_sandbox_lifecycle(conn, route)

    def _write_sandbox_migration(self, conn, migration):
        self._capacity_fence(
            conn,
            (
                migration.source_node_id,
                migration.source_job_id,
                migration.source_node_url,
            ),
            (
                migration.destination_node_id,
                migration.destination_job_id,
                migration.destination_node_url,
            ),
        )
        return super()._write_sandbox_migration(conn, migration)

    def reserve_sandbox_wakes(self, requests):
        with self._transaction() as conn:
            self._capacity_fence(
                conn, *((r.node_id, r.job_id, r.node_url) for r, _ in requests)
            )
            return super().reserve_sandbox_wakes(requests)

    def reconcile_sandboxes_for_node(
        self, node_url, observations, *, node_id, job_id, **kwargs
    ):
        with self._transaction() as conn:
            self._capacity_fence(conn, (node_id, job_id, node_url))
            return super().reconcile_sandboxes_for_node(
                node_url, observations, node_id=node_id, job_id=job_id, **kwargs
            )

    def _delete_sandbox_unlocked(self, conn, route, **kwargs):
        existing = (
            route
            if hasattr(route, "job_id")
            else self._get_sandbox_unlocked(conn, route)
        )
        if existing is not None:
            self._fence_route(conn, existing)
        return super()._delete_sandbox_unlocked(conn, route, **kwargs)

    def upsert_program_request_transition_with_change(self, route, **kwargs):
        with self._transaction() as conn:
            previous = self.program_request_readonly(kwargs["request_id"].strip())
            kwargs["_connection"] = conn
            current, changed = super().upsert_program_request_transition_with_change(
                route, **kwargs
            )
            # Live admission reads only nonterminal program membership, in the
            # cold-detach predicate. Wake shadow plans are observational. State
            # progress, timestamps and retries within that membership cannot
            # invalidate another sandbox's capacity decision on this worker.
            was_active = previous is not None and previous.state != "terminal"
            is_active = current.state != "terminal"
            if was_active != is_active:
                # A program receipt may name the source owner after migration;
                # the generation check is canonical, so fence its current owner.
                owner = self._get_sandbox_unlocked(conn, route.sandbox_id)
                self._fence_route(conn, owner)
            return current, changed

    @contextmanager
    def command_execution(self, command_id, claim_token, path, body):
        try:
            identity = (UUID(command_id), UUID(claim_token))
        except (ValueError, TypeError, AttributeError):
            raise PlacementCommandRejected("invalid placement claim") from None
        token = self._command.set(identity)
        try:
            self._check_command_request(path, body)
            yield
        finally:
            self._command.reset(token)

    def _command_row(self, conn):
        identity = self._command.get()
        if identity is None:
            return None
        row = conn.execute(
            """SELECT * FROM gateway_commands WHERE command_id=?
            AND claim_token=? AND state='running' AND claim_until>clock_timestamp()
            FOR UPDATE""",
            identity,
        ).fetchone()
        if row is None:
            raise PlacementCommandRejected("placement claim expired or was replaced")
        route = self._get_sandbox_unlocked(conn, row["sandbox_id"])
        if row["generation"] is not None:
            if route is None or route.generation != row["generation"]:
                raise PlacementCommandRejected(
                    "placement command incarnation no longer exists"
                )
        elif row["deadline"] <= utc_now():
            raise PlacementCommandRejected(
                "placement command expired before allocation"
            )
        return row

    @_transactional
    def _check_command_request(self, path, body):
        with self._transaction() as conn:
            row = self._command_row(conn)
            if row["path"] != path or bytes(row["body"]) != body:
                raise PlacementCommandRejected(
                    "placement request differs from durable command"
                )
            route = self._get_sandbox_unlocked(conn, row["sandbox_id"])
            if (
                row["kind"] == "create"
                and route is not None
                and row["generation"] is None
            ):
                conn.execute(
                    "UPDATE gateway_commands SET generation=? WHERE command_id=?",
                    (route.generation, row["command_id"]),
                )

    def delete_sandbox_if_current(self, sandbox_id, **kwargs):
        with self._transaction() as conn:
            command = (
                self._command_row(conn) if self._command.get() is not None else None
            )
            removed = super().delete_sandbox_if_current(sandbox_id, **kwargs)
            if removed is not None and command is not None:
                if command["kind"] != "create" or command["sandbox_id"] != sandbox_id:
                    raise PlacementCommandRejected(
                        "placement command cannot release this route"
                    )
                # Canonical create releases a provisional route only after a
                # proven rejection/absence. This command may then re-place it;
                # an external DELETE has no command context and cannot grant
                # that permission. Release and permission commit together.
                conn.execute(
                    "UPDATE gateway_commands SET generation=NULL WHERE command_id=?",
                    (command["command_id"],),
                )
            return removed

    @_transactional
    def cancel_create_commands(self, sandbox_id):
        with self._transaction() as conn:
            rows = conn.execute(
                """UPDATE gateway_commands SET state='done',claim_token=NULL,claim_until=NULL,
                result_status=410,result_headers=?::jsonb,result_body=?,completed_at=clock_timestamp()
                WHERE sandbox_id=? AND kind='create' AND state!='done' RETURNING command_id""",
                (
                    json.dumps({"Content-Type": "application/json"}),
                    b'{"error":"sandbox creation was cancelled by deletion","error_code":"sandbox_create_cancelled","retryable":false}',
                    sandbox_id,
                ),
            ).fetchall()
            if rows:
                conn.execute(
                    "DELETE FROM pending WHERE sandbox_id=? AND operation_id=ANY(?) AND failure_reason='queued_create'",
                    (sandbox_id, [str(row["command_id"]) for row in rows]),
                )

    def allocate_sandbox_create_with_pending(self, allocation, **kwargs):
        with self._transaction() as conn:
            command = self._command_row(conn)
            result = super().allocate_sandbox_create_with_pending(allocation, **kwargs)
            if command is not None:
                if (
                    command["kind"] != "create"
                    or command["sandbox_id"] != allocation.sandbox_id
                ):
                    raise PlacementCommandRejected(
                        "placement command allocation differs"
                    )
                conn.execute(
                    "UPDATE gateway_commands SET generation=? WHERE command_id=?",
                    (result[0].generation, command["command_id"]),
                )
            return result

    def migrate(self):
        """Explicit offline initialization; constructing the store never runs DDL."""
        with self.pool.connection() as conn, conn.transaction():
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (self.schema,)
            )
            conn.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(self.schema)
                )
            )
            if (
                conn.execute(
                    "SELECT to_regclass(%s)", (self.schema + ".routing_schema_version",)
                ).fetchone()[0]
                is None
            ):
                conn.execute(
                    files(__package__).joinpath("routing_schema.sql").read_text()
                )
        self.check_schema()

    def check_schema(self):
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT version FROM routing_schema_version WHERE singleton"
            ).fetchone()
            if row is None or row["version"] != 1:
                raise ValueError("unsupported PostgreSQL routing schema")

    @contextmanager
    def _connect(self):
        current = self._current.get()
        if current is not None:
            yield current
            return
        self.validate_authority()
        try:
            with self.pool.connection() as conn, conn.transaction():
                # SQLite readers see one snapshot across explicit BEGIN and
                # their subsequent queries. Preserve that contract for GC's
                # completeness check and all other multi-query domain reads.
                conn.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                yield _Connection(conn, coherent_reader=True)
        except psycopg.Error as exc:
            raise sqlite3.DatabaseError("PostgreSQL routing read failed") from exc

    @contextmanager
    def _worker_turn(self, conn, span):
        worker = self._placement_worker.get()
        if worker is None:
            yield
            return
        # Optional contention suppression, not the correctness fence. Take the
        # turn BEFORE opening the snapshot, and release it before returning the
        # pooled connection. Unrelated workers never share this lock.
        key = self.schema + "/placement/" + worker
        started = time.monotonic()
        try:
            conn.execute("SELECT pg_advisory_lock(hashtextextended(%s,0))", (key,))
            span.set_attribute(
                "routing.worker_wait_seconds", time.monotonic() - started
            )
            yield
        finally:
            try:
                conn.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s,0))", (key,)
                )
            except BaseException:
                conn.close()  # Never return a session carrying a lock to the pool.
                raise

    @contextmanager
    def _transaction(self):
        current = self._current.get()
        if current is not None:
            yield current
            return
        self.validate_authority()
        with self.telemetry.span(
            "routing.transaction", attributes={"routing.backend": "postgres"}
        ) as span:
            started = time.monotonic()
            acquired = body_done = committed = None
            try:
                with self.pool.connection() as conn:
                    acquired = time.monotonic()
                    with self._worker_turn(conn, span), conn.transaction():
                        conn.execute(
                            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                            if self._placement_snapshot.get()
                            else "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
                        )
                        adapted = _Connection(conn)
                        token = self._current.set(adapted)
                        capacity_token = self._capacity_touched.set(set())
                        try:
                            yield adapted
                            self.validate_authority()
                            body_done = time.monotonic()
                        finally:
                            self._current.reset(token)
                            self._capacity_touched.reset(capacity_token)
                    committed = time.monotonic()
            finally:
                ended = time.monotonic()
                span.set_attribute(
                    "routing.pool_wait_seconds", (acquired or ended) - started
                )
                span.set_attribute(
                    "routing.transaction_seconds",
                    (body_done or ended) - (acquired or ended),
                )
                span.set_attribute(
                    "routing.commit_seconds",
                    (committed or ended) - body_done if body_done else 0,
                )

    def _sandbox_route_rows_readonly(self, *, background=False, node_identity=None):
        where = (
            "WHERE node_id=? OR job_id=? OR node_url IN (?,?)" if node_identity else ""
        )
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM sandboxes " + where + " ORDER BY sandbox_id",
                node_identity or (),
            ).fetchall()

    def _write_storage_dependencies(self, conn, dependencies):
        conn.execute(
            """INSERT INTO sandbox_storage_dependencies
            SELECT value->>0,(value->>1)::bigint,value->>2
            FROM jsonb_array_elements(?::jsonb)
            ON CONFLICT(sandbox_id) DO UPDATE SET generation=excluded.generation,
                storage_snapshot_json=excluded.storage_snapshot_json
            WHERE excluded.storage_snapshot_json!='{}' """,
            (json.dumps(dependencies),),
        )


# These methods own complete database-only operations. Nested calls participate
# in the caller's transaction; serialization retries never repeat external I/O.
TRANSACTIONAL_METHODS = (
    "load",
    "upsert_managed_process",
    "upsert_program_request_transition_with_change",
    "set_sandbox_state_if_current",
    "confirm_sandbox_wake",
    "confirm_sandbox_observation",
    "reserve_sandbox_wake",
    "reserve_sandbox_wakes",
    "begin_sandbox_detach",
    "complete_sandbox_detach",
    "upsert_sandbox",
    "allocate_sandbox_create_with_pending",
    "prepare_sandbox_delete",
    "begin_sandbox_migration",
    "terminalize_orphaned_sandbox_migrations",
    "advance_sandbox_migration",
    "complete_sandbox_migration",
    "route_sandbox_migration",
    "delete_sandbox_if_current",
    "reconcile_sandboxes_for_node",
    "delete_sandbox",
    "delete_sandboxes_for_jobs",
    "delete_sandboxes_for_jobs_with_error",
    "delete_stale_sandboxes",
    "upsert_exec",
    "delete_exec",
    "upsert_pending",
    "upsert_pending_with_demand",
    "clear_pending",
    "consume_pending_demand",
    "upsert_pending_image_build",
    "clear_pending_image_build",
    "consume_pending_image_builds",
    "upsert_image_warmup",
    "mark_image_warmup_node",
    "delete_image_warmup",
    "upsert_prepared_capacity",
    "delete_prepared_capacity",
    "upsert_prepared_builder",
    "delete_prepared_builder",
    "consume_prepared_builders",
    "pending_sandboxes",
    "image_warmups",
    "prepared_capacity",
    "prepared_builders",
    "prepared_builder_count",
    "pending_image_build_count",
    "pending_demand",
)
for _name in TRANSACTIONAL_METHODS:
    setattr(
        PostgresRoutingStore,
        _name,
        _transactional(getattr(PostgresRoutingStore, _name)),
    )
