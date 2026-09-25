"""PostgreSQL authority for the deployed relay HTTP API.

Only waiters live in process memory. Registration, inference leases, retry identity,
responses and lifecycle work are durable. The gateway remains the sole sandbox
owner authority during the relay cutover; this module never imports fixture owners.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
import hashlib
import hmac
import json
import logging
import time
from uuid import uuid4

from aiohttp import web
from opentelemetry import trace
from psycopg import AsyncConnection, sql
from psycopg.types.json import Jsonb

from .. import model_relay as api
from ..telemetry import Telemetry
from ..relay_phase import METADATA_KEY as PHASE_METADATA_KEY, current_phase, phase_update
from .database import PostgresDatabase

LOGGER = logging.getLogger(__name__)
# Reserve response space BEFORE accepting work. This is a durable-storage safety
# budget, not an execution/concurrency limit. Completion cannot exhaust admission.
RESPONSE_RESERVATION = api.MAX_WORKER_RESPONSE_BYTES + 65536
DEFAULT_STORAGE_BUDGET = 64 * 1024**3


async def _wait_cancellable(awaitable, timeout):
    """Bound a wait without losing cancellation when its result becomes ready.

    Python 3.10's wait_for can consume task cancellation if the inner future
    finishes at the same time. A dispatcher then survives shutdown indefinitely.
    wait() keeps cancellation with this task; we always retire its child wait.
    """
    future = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({future}, timeout=timeout)
        if not done:
            raise asyncio.TimeoutError
        return future.result()
    finally:
        future.cancel()
        await asyncio.gather(future, return_exceptions=True)


class PostgresRelayState:
    durable_lifecycle = True

    def __init__(
        self,
        store: PostgresDatabase,
        *,
        request_timeout_seconds=7200,
        completed_request_retention_seconds=3600,
        worker_retention_seconds=3600,
        storage_budget_bytes=DEFAULT_STORAGE_BUDGET,
        lifecycle_concurrency=256,
        lifecycle_lease_seconds=30,
        accepted_notifier=None,
        result_notifier=None,
        telemetry=None,
        lifecycle_connections=4,
    ):
        if (
            storage_budget_bytes < RESPONSE_RESERVATION
            or lifecycle_concurrency < 1
            or lifecycle_lease_seconds <= 0
            or lifecycle_connections < 0
        ):
            raise ValueError("invalid relay storage or dispatch budget")
        self.store = store
        # Wake/park dispatch and response delivery release waiting guests.
        # Worker polls and enqueues can saturate the shared pool; these short
        # transactions get their own connections to the same authority.
        self._lifecycle_connections = lifecycle_connections
        self._lifecycle_store = None
        self.telemetry = telemetry or Telemetry.disabled("model-relay")
        self.deployment = store.deployment_id
        self.request_timeout = request_timeout_seconds
        self.retention = completed_request_retention_seconds
        self.worker_retention = worker_retention_seconds
        self.storage_budget = storage_budget_bytes
        self.concurrency = lifecycle_concurrency
        self._active_parks = {}
        self.claim_seconds = lifecycle_lease_seconds
        self.notifiers = {"park": accepted_notifier, "wake": result_notifier}
        self.channel = (
            "relay_"
            + hashlib.sha256(
                (store.schema + ":" + self.deployment).encode()
            ).hexdigest()[:48]
        )
        self._waiters: dict[str, set[asyncio.Event]] = {}
        self._tasks: list[asyncio.Task] = []
        self._closing = False
        self._active: set[asyncio.Task] = set()
        self._active_by_action = {action: set() for action in self.notifiers}
        self._active_claims: dict[tuple[str, str], object] = {}
        self._response_waiters: dict[str, set[asyncio.Future]] = {}
        self._delivery_event = asyncio.Event()
        self._dirty_deliveries: set[str] = set()
        self._notify_event = asyncio.Event()
        self._pending_notifications: set[str] = set()
        self._poll_waiters: dict[asyncio.Event, tuple[str, str]] = {}

    @property
    def _delivery_store(self):
        return self._lifecycle_store or self.store

    async def open(self):
        await self.store.open()
        try:
            async with self.store.transaction("relay_open") as conn:
                row = await (
                    await conn.execute(
                        "SELECT version FROM relay_schema_version WHERE singleton"
                    )
                ).fetchone()
                if row is None or row["version"] != 1:
                    raise ValueError(
                        "unsupported relay schema; run explicit migration first"
                    )
                cutover = await (
                    await conn.execute(
                        "SELECT active,source_digest FROM relay_imports WHERE deployment_id=%s",
                        (self.deployment,),
                    )
                ).fetchone()
                expected = getattr(self.store, "expected_relay_import_digest", None)
                if expected is not None and (
                    cutover is None or cutover["source_digest"] != expected
                ):
                    raise ValueError("database does not match the source relay cutover")
                if cutover is not None and not cutover["active"]:
                    raise ValueError(
                        "relay import is not activated; rerun idle cutover"
                    )
                await conn.execute(
                    "INSERT INTO relay_quota(deployment_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    (self.deployment,),
                )
                mode = (
                    self.notifiers["park"] is not None,
                    self.notifiers["wake"] is not None,
                )
                await conn.execute(
                    "INSERT INTO relay_runtime_config VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                    (self.deployment, *mode),
                )
                current = await (
                    await conn.execute(
                        "SELECT park_enabled,wake_enabled FROM relay_runtime_config WHERE deployment_id=%s",
                        (self.deployment,),
                    )
                ).fetchone()
                if (current["park_enabled"], current["wake_enabled"]) != mode:
                    raise ValueError(
                        "relay processes disagree about lifecycle configuration"
                    )
            fresh = getattr(self.store, "fresh", None)
            if self._lifecycle_connections and fresh is not None:
                lifecycle_store = fresh(max_connections=self._lifecycle_connections)

                def observe(sample):
                    # One metric stream: follow the shared store's observer.
                    if self.store.observe is not None:
                        self.store.observe(sample)

                lifecycle_store.observe = observe
                await lifecycle_store.open()
                self._lifecycle_store = lifecycle_store
            self._tasks = [asyncio.create_task(self._listen())]
            if any(self.notifiers.values()):
                self._tasks.append(asyncio.create_task(self._dispatch_loop()))
                self._tasks.append(asyncio.create_task(self._renew_lifecycle_loop()))
            self._tasks.append(asyncio.create_task(self._deliver_loop()))
            self._tasks.append(asyncio.create_task(self._reconcile_poll_waiters()))
            self._tasks.append(asyncio.create_task(self._notify_loop()))
        except BaseException:
            if self._lifecycle_store is not None:
                await self._lifecycle_store.close()
                self._lifecycle_store = None
            await self.store.close()
            raise

    async def aclose(self):
        # Also record shutdown outside cancellation: a dependency's timed wait
        # may consume cancellation racing readiness on Python 3.10. In that
        # case the background loop must not enter another indefinite wait.
        self._closing = True
        tasks = set(self._tasks) | self._active
        pending = tasks
        while pending:
            for task in pending:
                task.cancel()
            # Retrying cancellation is needed when a dependency consumes the
            # first signal while entering another wait. asyncio.wait itself
            # does not consume cancellation; transaction rollback is shielded.
            _, pending = await asyncio.wait(pending, timeout=0.1)
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            if self._lifecycle_store is not None:
                await self._lifecycle_store.close()
        finally:
            await self.store.close()

    def _signal(self, key):
        if key.startswith("r:") and (
            key[2:] in self._response_waiters
            or key[2:] in self._active_parks
        ):
            self._dirty_deliveries.add(key[2:])
            self._delivery_event.set()
        for event in tuple(self._waiters.get(key, ())):
            event.set()

    @asynccontextmanager
    async def _watch(self, key):
        event = asyncio.Event()
        self._waiters.setdefault(key, set()).add(event)
        try:
            yield event
        finally:
            self._waiters[key].discard(event)
            if not self._waiters[key]:
                del self._waiters[key]

    async def _notify(self, conn, *keys):
        # NOTIFY is a best-effort hint, never part of the durable write commit.
        # Its shared queue lock can otherwise serialize concurrent WAL flushes.
        def committed():
            for key in keys:
                self._signal(key)
            self._pending_notifications.update(keys)
            self._notify_event.set()

        lifecycle = self._lifecycle_store
        store = lifecycle if lifecycle is not None and lifecycle.in_transaction() else self.store
        store.after_commit(committed)

    async def _notify_loop(self):
        while not self._closing:
            await self._notify_event.wait()
            self._notify_event.clear()
            await asyncio.sleep(0.002)
            keys = list(self._pending_notifications)
            self._pending_notifications.difference_update(keys)
            try:
                async with self.store.transaction("relay_notify") as conn:
                    await conn.execute(
                        "SELECT pg_notify(%s,key) FROM unnest(%s::text[]) AS key",
                        (self.channel, keys),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                # Bounded periodic durable reads recover lost hints, including
                # a process crash between the data commit and notification.
                LOGGER.warning(
                    "relay notification failed; durable readers will reconcile"
                )

    async def _listen(self):
        while not self._closing:
            try:
                # Dedicated LISTEN connection, never consumes the transaction pool.
                async with await AsyncConnection.connect(
                    self.store.pool.conninfo, autocommit=True
                ) as conn:
                    await conn.execute(
                        sql.SQL("LISTEN {}").format(sql.Identifier(self.channel))
                    )
                    for key in tuple(self._waiters):
                        self._signal(key)
                    async for notification in conn.notifies():
                        if self._closing:
                            return
                        self._signal(notification.payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Notifications are hints. Durable queries + bounded fallback
                # recover missed events and reconnects, without resetting work.
                LOGGER.warning("relay notification connection lost; reconnecting")
                await asyncio.sleep(0.5)

    async def _reconcile_poll_waiters(self):
        """Recover lost queue hints with one read, not a transaction per socket.

        This only wakes existing pollers. The normal claim transaction remains
        the sole authority for registration, lease expiry and work assignment.
        """
        while not self._closing:
            await asyncio.sleep(0.5)
            waiting = tuple(self._poll_waiters.items())
            if not waiting:
                continue
            rollouts = list({registration[0] for _, registration in waiting})
            try:
                async with self.store.transaction("relay_poll_readiness") as conn:
                    rows = await (await conn.execute(
                        """SELECT wanted.rollout_id,g.registration_token,g.enabled,
                        EXISTS (
                          SELECT 1 FROM relay_requests r WHERE r.deployment_id=%s
                          AND r.rollout_id=wanted.rollout_id
                          AND r.registration_token=g.registration_token
                          AND r.expires_at>extract(epoch FROM clock_timestamp())
                          AND (r.state='pending' OR (r.state='leased'
                            AND r.lease_expires_at<=extract(epoch FROM clock_timestamp())))
                        ) AS ready
                        FROM unnest(%s::text[]) AS wanted(rollout_id)
                        LEFT JOIN relay_rollouts g ON g.deployment_id=%s
                          AND g.rollout_id=wanted.rollout_id""",
                        (self.deployment, rollouts, self.deployment),
                    )).fetchall()
                by_rollout = {row["rollout_id"]: row for row in rows}
                for event, (rollout, token) in waiting:
                    row = by_rollout[rollout]
                    if (row["ready"] or not row["enabled"]
                            or row["registration_token"] != token):
                        event.set()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Poll deadlines still perform an authoritative final check;
                # another batch recovers temporary read/connection failures.
                LOGGER.warning("relay queue readiness failed; durable work retained")

    @asynccontextmanager
    async def _watch_poll(self, rollout, token):
        async with self._watch("q:" + rollout) as event:
            self._poll_waiters[event] = (rollout, token)
            try:
                yield event
            finally:
                self._poll_waiters.pop(event, None)

    @staticmethod
    async def _now(conn):
        return float(
            (
                await (
                    await conn.execute(
                        "SELECT extract(epoch FROM clock_timestamp()) AS now"
                    )
                ).fetchone()
            )["now"]
        )

    async def _registration(self, conn, rollout_id, token=None, *, lock="SHARE"):
        if lock not in {None, "SHARE", "UPDATE"}:
            raise ValueError("invalid registration lock")
        suffix = "" if lock is None else f" FOR {lock}"
        record = await (
            await conn.execute(
                f"SELECT * FROM relay_rollouts WHERE deployment_id=%s AND rollout_id=%s{suffix}",
                (self.deployment, rollout_id),
            )
        ).fetchone()
        self._validate_registration(record, token)
        return record

    @staticmethod
    def _validate_registration(record, token):
        if record is None or not record["enabled"]:
            raise web.HTTPNotFound(text="rollout is not registered")
        if token is not None and not hmac.compare_digest(
            record["registration_token"], token
        ):
            raise web.HTTPConflict(text="rollout registration is no longer current")

    async def _poll_observation(self, conn, rollout_id, token, worker_id, heartbeat_seconds):
        """Read-only readiness/authentication, never a grant of inference work."""
        row = await (await conn.execute(
            """WITH clock AS (SELECT extract(epoch FROM clock_timestamp()) AS now)
            SELECT g.registration_token,g.enabled,
              EXISTS (SELECT 1 FROM relay_requests r
                WHERE r.deployment_id=g.deployment_id AND r.rollout_id=g.rollout_id
                AND r.registration_token=g.registration_token AND r.expires_at>clock.now
                AND (r.state='pending' OR (r.state='leased' AND r.lease_expires_at<=clock.now))) AS ready,
              CASE WHEN %s::text IS NULL THEN NULL
                ELSE greatest(0,coalesce(w.last_seen_at+%s-clock.now,0)) END AS heartbeat_wait
            FROM relay_rollouts g CROSS JOIN clock
            LEFT JOIN relay_workers w ON w.deployment_id=g.deployment_id AND w.rollout_id=g.rollout_id
              AND w.registration_token=g.registration_token AND w.worker_id=%s
            WHERE g.deployment_id=%s AND g.rollout_id=%s""",
            (worker_id, heartbeat_seconds, worker_id, self.deployment, rollout_id),
        )).fetchone()
        self._validate_registration(row, token)
        return row["ready"], (None if row["heartbeat_wait"] is None else float(row["heartbeat_wait"]))

    @staticmethod
    def _registration_record(row):
        return {
            key: row[key]
            for key in ("rollout_id", "registration_token", "metadata", "registered_at")
        }

    async def register_rollout(self, rollout_id, metadata=None):
        api.validate_rollout_id(rollout_id)
        api._validate_registration_metadata(metadata)
        async with self.store.transaction("relay_register") as conn:
            # Serialize only this registration, including its initially absent row.
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (self.channel + ":" + rollout_id,),
            )
            previous = await (
                await conn.execute(
                    "SELECT * FROM relay_rollouts WHERE deployment_id=%s AND rollout_id=%s FOR UPDATE",
                    (self.deployment, rollout_id),
                )
            ).fetchone()
            now = await self._now(conn)
            if previous is not None:
                await self._retire_registration(
                    conn, rollout_id, previous["registration_token"], now
                )
            row = await (
                await conn.execute(
                    """INSERT INTO relay_rollouts VALUES (%s,%s,%s,%s,%s,true)
                ON CONFLICT(deployment_id,rollout_id) DO UPDATE SET registration_token=excluded.registration_token,
                metadata=excluded.metadata, registered_at=excluded.registered_at, enabled=true RETURNING *""",
                    (
                        self.deployment,
                        rollout_id,
                        uuid4().hex,
                        Jsonb(metadata or {}),
                        now,
                    ),
                )
            ).fetchone()
            await self._notify(conn, "q:" + rollout_id)
            return self._registration_record(row)

    async def unregister_rollout(self, rollout_id, *, registration_token):
        api.validate_rollout_id(rollout_id)
        api.validate_registration_token(registration_token)
        async with self.store.transaction("relay_unregister") as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM relay_rollouts WHERE deployment_id=%s AND rollout_id=%s FOR UPDATE",
                    (self.deployment, rollout_id),
                )
            ).fetchone()
            if row is None or not row["enabled"]:
                return False
            if not hmac.compare_digest(row["registration_token"], registration_token):
                raise web.HTTPConflict(text="rollout registration is no longer current")
            await self._retire_registration(
                conn, rollout_id, registration_token, await self._now(conn)
            )
            await conn.execute(
                "UPDATE relay_rollouts SET enabled=false WHERE deployment_id=%s AND rollout_id=%s",
                (self.deployment, rollout_id),
            )
            await self._notify(conn, "q:" + rollout_id)
            return True

    async def update_resource_phase(self, rollout_id, *, registration_token, update):
        api.validate_rollout_id(rollout_id)
        api.validate_registration_token(registration_token)
        try:
            update = phase_update(update)
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        async with self.store.transaction("relay_resource_phase") as conn:
            # The immutable registration token also fences its sandbox binding.
            # Re-registration/revocation serialize on this same authoritative row.
            row = await self._registration(conn, rollout_id, registration_token, lock="UPDATE")
            now = await self._now(conn)
            metadata = dict(row["metadata"])
            previous = metadata.get(PHASE_METADATA_KEY)
            if previous is not None and previous["sequence"] >= update["sequence"]:
                duplicate = previous["sequence"] == update["sequence"]
                original = {key: previous[key] for key in update if key in previous}
                original_keys = set(previous) - {"observed_at", "expires_at"}
                if duplicate and (original != update or original_keys != set(update)):
                    raise web.HTTPConflict(text="resource phase sequence already has another payload")
                return {"accepted": duplicate, "current": current_phase(metadata, now=now)}
            metadata[PHASE_METADATA_KEY] = {
                **update, "observed_at": now, "expires_at": now + update["ttl_seconds"],
            }
            await conn.execute(
                "UPDATE relay_rollouts SET metadata=%s WHERE deployment_id=%s AND rollout_id=%s",
                (Jsonb(metadata), self.deployment, rollout_id),
            )
            return {"accepted": True, "current": current_phase(metadata, now=now)}

    async def _retire_registration(self, conn, rollout, token, now):
        rows = await (
            await conn.execute(
                "SELECT * FROM relay_requests WHERE deployment_id=%s AND rollout_id=%s AND registration_token=%s AND (state!='completed' OR delivery_pending) ORDER BY request_id FOR UPDATE",
                (self.deployment, rollout, token),
            )
        ).fetchall()
        for row in rows:
            if row["state"] != "completed":
                await self._complete(
                    conn,
                    row,
                    api.RelayWorkerResponse(
                        410,
                        api._openai_error("rollout unregistered", "relay_unregistered"),
                    ),
                    now,
                    defer=False,
                )
            else:
                await self._release(conn, row["request_id"])
            await conn.execute(
                "UPDATE relay_lifecycle SET done=true, claim_token=NULL, claim_until=NULL WHERE deployment_id=%s AND request_id=%s",
                (self.deployment, row["request_id"]),
            )
        await conn.execute(
            "DELETE FROM relay_workers WHERE deployment_id=%s AND rollout_id=%s",
            (self.deployment, rollout),
        )

    async def require_current_registration(self, rollout_id, registration_token):
        api.validate_rollout_id(rollout_id)
        if not api.REGISTRATION_TOKEN_RE.fullmatch(registration_token):
            raise web.HTTPUnauthorized(text="invalid rollout registration token")
        try:
            async with self.store.transaction("relay_authorize") as conn:
                # This precheck protects body admission, not subsequent work.
                # Tuple locks here force WAL/COMMIT I/O but expire before the
                # HTTP body is read. Enqueue checks the token under its lock.
                await self._registration(conn, rollout_id, registration_token, lock=None)
        except (web.HTTPNotFound, web.HTTPConflict) as exc:
            raise web.HTTPUnauthorized(
                text="invalid rollout registration token"
            ) from exc

    async def list_rollouts(self):
        async with self.store.transaction("relay_rollouts") as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM relay_rollouts WHERE deployment_id=%s AND enabled ORDER BY rollout_id",
                    (self.deployment,),
                )
            ).fetchall()
            return [self._registration_record(row) for row in rows]

    async def record_worker_heartbeat(
        self, *, rollout_id, registration_token, worker_id, metadata=None
    ):
        api.validate_rollout_id(rollout_id)
        api.validate_registration_token(registration_token)
        api.validate_worker_id(worker_id)
        async with self.store.transaction("relay_worker") as conn:
            await self._registration(conn, rollout_id, registration_token)
            now = await self._now(conn)
            await self._write_worker_heartbeat(
                conn, rollout_id, registration_token, worker_id, now, metadata,
            )
            return dict(
                rollout_id=rollout_id,
                worker_id=worker_id,
                last_seen_at=now,
                metadata=metadata or {},
            )

    async def _write_worker_heartbeat(self, conn, rollout_id, token, worker_id, now, metadata=None):
        # Caller owns the registration SHARE lock for this transaction.
        await conn.execute(
            """INSERT INTO relay_workers VALUES (%s,%s,%s,%s,
            COALESCE(%s::double precision,extract(epoch FROM clock_timestamp())),%s)
            ON CONFLICT(deployment_id,rollout_id,worker_id) DO UPDATE SET
            registration_token=excluded.registration_token,last_seen_at=excluded.last_seen_at,
            metadata=excluded.metadata""",
            (self.deployment, rollout_id, token, worker_id, now, Jsonb(metadata or {})),
        )

    async def enqueue(
        self,
        *,
        rollout_id,
        endpoint,
        body,
        headers,
        method="POST",
        idempotency_key=None,
        defer_idempotency_until_disconnect=False,
        expected_registration_token=None,
    ):
        api.validate_rollout_id(rollout_id)
        method = method.upper()
        if method not in api.TUNNEL_HTTP_METHODS:
            raise web.HTTPMethodNotAllowed(method, sorted(api.TUNNEL_HTTP_METHODS))
        if not endpoint.startswith("/") or endpoint.startswith("//"):
            raise web.HTTPBadRequest(text="relay endpoint must be an absolute path")
        encoded = api._encoded_body(body)
        raw = api._encoded_body_bytes(encoded)
        if len(raw) > api.MAX_RELAY_BODY_BYTES:
            raise web.HTTPRequestEntityTooLarge(
                max_size=api.MAX_RELAY_BODY_BYTES, actual_size=len(raw)
            )
        if idempotency_key is not None:
            api.validate_idempotency_key(idempotency_key)
        identity_headers = {
            k: v
            for k, v in headers.items()
            if k.lower()
            not in {
                "baggage",
                "traceparent",
                "tracestate",
                "x-correlation-id",
                "x-request-id",
                "x-stainless-read-timeout",
                "x-stainless-retry-count",
            }
        }
        metadata = json.dumps(
            dict(endpoint=endpoint, method=method, headers=identity_headers),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
        digest = hashlib.sha256(
            b"\0".join(
                (rollout_id.encode(), endpoint.encode(), method.encode(), metadata, raw)
            )
        ).hexdigest()
        size = len(raw) + len(metadata)
        async with self.store.transaction("relay_enqueue") as conn:
            reg = await self._registration(conn, rollout_id, expected_registration_token)
            token = reg["registration_token"]
            # Serialize only equal idempotency keys; unrelated requests progress.
            if idempotency_key is not None:
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (self.channel + ":" + token + ":" + idempotency_key,),
                )
                existing = await (
                    await conn.execute(
                        "SELECT request_id,request_digest FROM relay_requests WHERE deployment_id=%s AND rollout_id=%s AND registration_token=%s AND idempotency_key=%s AND reattachable",
                        (self.deployment, rollout_id, token, idempotency_key),
                    )
                ).fetchone()
                if existing:
                    if existing["request_digest"] != digest:
                        raise web.HTTPConflict(
                            text="idempotency key was already used for a different relay request"
                        )
                    return await self._load(conn, existing["request_id"])
            reserved = size + RESPONSE_RESERVATION
            now = await self._now(conn)
            request_id = uuid4().hex
            sandbox_id = api._registration_sandbox_id(reg)
            await conn.execute(
                """INSERT INTO relay_requests(deployment_id,request_id,rollout_id,registration_token,endpoint,method,
                created_at,expires_at,payload_bytes,reserved_bytes,state,idempotency_key,request_digest,reattachable,sandbox_id,sandbox_generation)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',%s,%s,%s,%s,%s)""",
                (
                    self.deployment,
                    request_id,
                    rollout_id,
                    token,
                    endpoint,
                    method,
                    now,
                    now + self.request_timeout,
                    size,
                    reserved,
                    idempotency_key,
                    digest,
                    idempotency_key is not None
                    and not defer_idempotency_until_disconnect,
                    sandbox_id,
                    api._registration_sandbox_generation(reg),
                ),
            )
            await conn.execute(
                "INSERT INTO relay_payloads VALUES (%s,%s,%s,%s,%s)",
                (self.deployment, request_id, raw, encoded["encoding"], Jsonb(headers)),
            )
            if sandbox_id and self.notifiers["park"]:
                await conn.execute(
                    "INSERT INTO relay_lifecycle(deployment_id,request_id,action) VALUES (%s,%s,'park')",
                    (self.deployment, request_id),
                )
            await self._notify(conn, "q:" + rollout_id, "l")
            request = await self._load(conn, request_id)
            # Take the byte-accounting lock LAST; never serialize payload writes
            # and lifecycle setup behind a deployment-wide quota row.
            admitted = await (
                await conn.execute(
                    "UPDATE relay_quota SET reserved_bytes=reserved_bytes+%s WHERE deployment_id=%s AND reserved_bytes+%s<=%s RETURNING deployment_id",
                    (reserved, self.deployment, reserved, self.storage_budget),
                )
            ).fetchone()
            if admitted is None:
                raise web.HTTPTooManyRequests(
                    text="relay durable storage budget is exhausted",
                    headers={"Retry-After": "1"},
                )
            return request

    async def _load(self, conn, request_id, *, bodies=True):
        # One READ COMMITTED statement supplies metadata and its matching body.
        # Separate SELECTs can observe a leased request, then a peer's result
        # commit/deletion of the request body, producing a torn reattach read.
        query = (
            """SELECT r.*,p.body AS input_body,p.encoding AS input_encoding,p.headers AS input_headers,
            b.body AS result_body,b.encoding AS result_encoding,b.status AS result_status,b.headers AS result_headers
            FROM relay_requests r LEFT JOIN relay_payloads p USING(deployment_id,request_id)
            LEFT JOIN relay_results b USING(deployment_id,request_id)
            WHERE r.deployment_id=%s AND r.request_id=%s"""
            if bodies
            else (
                "SELECT * FROM relay_requests WHERE deployment_id=%s AND request_id=%s"
            )
        )
        row = await (
            await conn.execute(query, (self.deployment, request_id))
        ).fetchone()
        if row is None:
            raise web.HTTPNotFound(text="request not found")
        return self._loaded_request(row, bodies=bodies)

    def _loaded_request(self, row, *, bodies=True):
        values = dict(row)
        values.update(body=None, headers={}, completed_response=None)
        if bodies and row["state"] != "completed":
            body = bytes(row["input_body"])
            values.update(
                body=api._encoded_body(
                    body if row["input_encoding"] == "base64" else json.loads(body)
                ),
                headers=row["input_headers"],
            )
        elif bodies:
            body = bytes(row["result_body"])
            values["completed_response"] = api.RelayWorkerResponse(
                row["result_status"],
                body if row["result_encoding"] == "base64" else json.loads(body),
                row["result_headers"],
            )
        return self._request_value(values)

    @staticmethod
    def _request_value(values, *, response=None):
        values = {
            key: value
            for key, value in values.items()
            if key in api.RelayRequest.__dataclass_fields__
        }
        values.setdefault("body", None)
        values.setdefault("headers", {})
        if "future" not in values:
            values["future"] = asyncio.get_running_loop().create_future()
        if response is not None:
            values["completed_response"] = response
        request = api.RelayRequest(**values)
        request.durable_lifecycle = True
        if request.state == "completed":
            request.response_committed.set()
            if not request.delivery_pending and request.completed_response is not None:
                request.future.set_result(request.completed_response)
        return request

    async def _request_lock(self, conn, request_id, token=None):
        if token is not None:
            registration = await (
                await conn.execute(
                    """SELECT g.registration_token,g.enabled FROM relay_rollouts g
                JOIN relay_requests r ON (r.deployment_id,r.rollout_id)=(g.deployment_id,g.rollout_id)
                WHERE r.deployment_id=%s AND r.request_id=%s FOR SHARE OF g""",
                    (self.deployment, request_id),
                )
            ).fetchone()
            if registration is None:
                raise web.HTTPNotFound(text="request not found")
            if not registration["enabled"] or not hmac.compare_digest(
                registration["registration_token"], token
            ):
                raise web.HTTPConflict(text="rollout registration is no longer current")
        row = await (
            await conn.execute(
                "SELECT *,extract(epoch FROM clock_timestamp()) AS db_now FROM relay_requests WHERE deployment_id=%s AND request_id=%s FOR UPDATE",
                (self.deployment, request_id),
            )
        ).fetchone()
        if row is None:
            raise web.HTTPNotFound(text="request not found")
        if token is not None and row["registration_token"] != token:
            raise web.HTTPConflict(text="request registration is no longer current")
        return row

    async def mark_caller_detached(self, request_id):
        async with self.store.transaction("relay_detach") as conn:
            row = await self._request_lock(conn, request_id)
            await self._reattach(conn, row)

    async def _reattach(self, conn, row):
        if row["idempotency_key"] is not None:
            # Only one implicit retry identity is claimable. A simultaneous
            # identical logical call may already own it; keep both results.
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (
                    self.channel
                    + ":"
                    + row["registration_token"]
                    + ":"
                    + row["idempotency_key"],
                ),
            )
            await conn.execute(
                """UPDATE relay_requests SET reattachable=true WHERE deployment_id=%s AND request_id=%s
                AND NOT EXISTS(SELECT 1 FROM relay_requests x WHERE x.deployment_id=%s AND x.rollout_id=%s
                AND x.registration_token=%s AND x.idempotency_key=%s AND x.reattachable AND x.request_id!=%s)""",
                (
                    self.deployment,
                    row["request_id"],
                    self.deployment,
                    row["rollout_id"],
                    row["registration_token"],
                    row["idempotency_key"],
                    row["request_id"],
                ),
            )

    mark_transport_reset = mark_caller_detached

    async def poll(
        self,
        *,
        rollout_id,
        registration_token,
        timeout_seconds,
        limit=1,
        lease_seconds=api.DEFAULT_WORKER_LEASE_SECONDS,
        worker_id=None,
    ):
        api.validate_rollout_id(rollout_id)
        api.validate_registration_token(registration_token)
        if worker_id is not None:
            api.validate_worker_id(worker_id)
        # Worker rows are observational retention, not execution leases. Persist
        # their presence at least three times within either existing lifetime;
        # explicit heartbeat calls remain immediate. No local auth/freshness cache.
        heartbeat_seconds = max(0, min(self.worker_retention, lease_seconds)) / 3
        deadline = time.monotonic() + max(0, timeout_seconds)
        async with self._watch_poll(rollout_id, registration_token) as event:
            while True:
                event.clear()
                async with self.store.transaction("relay_claim_inference") as conn:
                    ready, heartbeat_wait = await self._poll_observation(
                        conn, rollout_id, registration_token, worker_id, heartbeat_seconds)
                    result = []
                    if ready or heartbeat_wait == 0:
                        if heartbeat_wait == 0:
                            # Peers racing a due heartbeat recheck after one
                            # transaction-scoped gate. A fresh/empty loser never
                            # takes a row lock or issues a no-op UPSERT/WAL write.
                            await conn.execute(
                                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                                (self.channel + ":worker:" + rollout_id + ":" + worker_id,),
                            )
                            ready, heartbeat_wait = await self._poll_observation(
                                conn, rollout_id, registration_token, worker_id, heartbeat_seconds)
                        if ready or heartbeat_wait == 0:
                            # The read-only observation cannot authorize a claim:
                            # registration replacement is fenced atomically here.
                            await self._registration(conn, rollout_id, registration_token)
                            if heartbeat_wait == 0:
                                await self._write_worker_heartbeat(
                                    conn, rollout_id, registration_token, worker_id, None)
                                heartbeat_wait = heartbeat_seconds
                            # A concurrent heartbeat may hold the worker row. Start the
                            # inference lease only after acquiring that row's lock.
                            now = await self._now(conn)
                            # Lock candidates, assign distinct leases and hydrate their
                            # immutable input bodies in one statement. The row locks
                            # remain held through commit, including for peer claimers.
                            rows = await (await conn.execute(
                                """WITH candidates AS (
                                SELECT deployment_id,request_id FROM relay_requests
                                WHERE deployment_id=%s AND rollout_id=%s AND registration_token=%s
                                AND expires_at>%s AND (state='pending' OR (state='leased' AND lease_expires_at<=%s))
                                ORDER BY created_at,request_id LIMIT %s FOR UPDATE SKIP LOCKED
                                ), claimed AS (
                                UPDATE relay_requests r SET state='leased',
                                lease_id=replace(gen_random_uuid()::text,'-',''),lease_expires_at=%s,leased_by=%s,
                                delivered_at=%s,first_delivered_at=coalesce(r.first_delivered_at,%s),
                                delivery_count=r.delivery_count+1 FROM candidates c
                                WHERE (r.deployment_id,r.request_id)=(c.deployment_id,c.request_id) RETURNING r.*)
                                SELECT claimed.*,p.body AS input_body,p.encoding AS input_encoding,p.headers AS input_headers
                                FROM claimed LEFT JOIN relay_payloads p USING(deployment_id,request_id)
                                ORDER BY claimed.created_at,claimed.request_id""",
                                (self.deployment, rollout_id, registration_token, now, now,
                                 max(1, min(256, limit)), now + max(0.001, lease_seconds),
                                 worker_id, now, now),
                            )).fetchall()
                            result = [self._loaded_request(row) for row in rows]
                if result or time.monotonic() >= deadline:
                    return result
                wait = deadline - time.monotonic()
                if heartbeat_wait is not None:
                    wait = min(wait, heartbeat_wait)
                with suppress(asyncio.TimeoutError):
                    await _wait_cancellable(event.wait(), max(0.001, wait))

    @staticmethod
    def _valid_lease(row, lease_id, now):
        if row["state"] != "leased" or not lease_id or row["lease_id"] != lease_id:
            raise web.HTTPConflict(text="request lease is no longer active")
        if row["lease_expires_at"] <= now or row["expires_at"] <= now:
            raise web.HTTPConflict(text="request lease has expired")

    async def renew_lease(
        self, *, request_id, registration_token, lease_id, lease_seconds, worker_id=None
    ):
        api.validate_registration_token(registration_token)
        if worker_id is not None:
            api.validate_worker_id(worker_id)
        async with self.store.transaction("relay_renew") as conn:
            row = await self._request_lock(conn, request_id, registration_token)
            if row["state"] == "completed":
                raise web.HTTPGone(text="request is already completed")
            now = float(row["db_now"])
            self._valid_lease(row, lease_id, now)
            await conn.execute(
                "UPDATE relay_requests SET lease_expires_at=%s,leased_by=coalesce(%s,leased_by) WHERE deployment_id=%s AND request_id=%s",
                (
                    now + max(0.001, lease_seconds),
                    worker_id,
                    self.deployment,
                    request_id,
                ),
            )
            return await self._load(conn, request_id)

    @staticmethod
    def _response_parts(response):
        encoded = api._encoded_body(response.body)
        raw = api._encoded_body_bytes(encoded)
        metadata = json.dumps(
            [encoded["encoding"], response.status, response.headers],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        if len(raw) > api.MAX_WORKER_RESPONSE_BYTES or len(metadata) > 65536:
            raise web.HTTPRequestEntityTooLarge(
                max_size=RESPONSE_RESERVATION, actual_size=len(raw) + len(metadata)
            )
        digest = hashlib.sha256(
            len(metadata).to_bytes(4, "big") + metadata + raw
        ).hexdigest()
        return raw, encoded["encoding"], digest

    async def respond(
        self,
        *,
        request_id,
        registration_token,
        response,
        lease_id,
        error=False,
        defer_delivery=False,
    ):
        api.validate_registration_token(registration_token)
        parts = self._response_parts(response)
        async with self.store.transaction("relay_commit_result") as conn:
            row = await self._request_lock(conn, request_id, registration_token)
            if row["state"] == "completed":
                previous = await (
                    await conn.execute(
                        "SELECT digest FROM relay_results WHERE deployment_id=%s AND request_id=%s",
                        (self.deployment, request_id),
                    )
                ).fetchone()
                if previous["digest"] != parts[2] or row["lease_id"] != lease_id:
                    raise web.HTTPConflict(text="committed response or lease differs")
                return api.RelayRespondResult(
                    self._request_value(row, response=response), duplicate=True
                )
            now = float(row["db_now"])
            self._valid_lease(row, lease_id, now)
            row = await self._complete(
                conn, row, response, now, defer=defer_delivery, parts=parts
            )
            return api.RelayRespondResult(self._request_value(row, response=response))

    async def _complete(self, conn, row, response, now, *, defer, parts=None):
        raw, encoding, digest = parts or self._response_parts(response)
        pending = bool(defer and row["sandbox_id"] and self.notifiers["wake"])
        # One round trip after validation/locking. Bodies never travel back from
        # PostgreSQL merely to construct the worker's acknowledgment object.
        completed = await (
            await conn.execute(
                """WITH result AS (
            INSERT INTO relay_results VALUES (%s,%s,%s,%s,%s,%s,%s)), changed AS (
            UPDATE relay_requests SET state='completed',completed_at=%s,completed_bytes=%s,delivery_pending=%s,
            payload_bytes=0 WHERE deployment_id=%s AND request_id=%s RETURNING *), payload AS (
            DELETE FROM relay_payloads WHERE deployment_id=%s AND request_id=%s), wake AS (
            INSERT INTO relay_lifecycle(deployment_id,request_id,action)
            SELECT deployment_id,request_id,'wake' FROM changed WHERE delivery_pending ON CONFLICT DO NOTHING),
            obsolete_parks AS (
            UPDATE relay_lifecycle l SET done=true
            FROM changed r WHERE (l.deployment_id,l.request_id)=(r.deployment_id,r.request_id)
            AND l.action='park' AND NOT l.done AND l.claim_token IS NULL)
            SELECT changed.* FROM changed""",
                (
                    self.deployment,
                    row["request_id"],
                    raw,
                    encoding,
                    response.status,
                    Jsonb(response.headers),
                    digest,
                    now,
                    api._relay_response_retained_bytes(response),
                    pending,
                    self.deployment,
                    row["request_id"],
                    self.deployment,
                    row["request_id"],
                ),
            )
        ).fetchone()
        await self._notify(conn, "r:" + row["request_id"], "l")
        return completed

    async def retry_worker_failure(self, *, request_id, registration_token, lease_id):
        api.validate_registration_token(registration_token)
        async with self.store.transaction("relay_retry_inference") as conn:
            row = await self._request_lock(conn, request_id, registration_token)
            if row["state"] == "completed":
                return None
            self._valid_lease(row, lease_id, float(row["db_now"]))
            if row["delivery_count"] >= api.MAX_TRANSIENT_WORKER_DELIVERIES:
                return None
            await conn.execute(
                "UPDATE relay_requests SET state='pending',lease_id=NULL,lease_expires_at=NULL,leased_by=NULL,delivered_at=NULL WHERE deployment_id=%s AND request_id=%s",
                (self.deployment, request_id),
            )
            await self._notify(conn, "q:" + row["rollout_id"])
            return await self._load(conn, request_id)

    async def _release(self, conn, request_id):
        await conn.execute(
            "UPDATE relay_requests SET delivery_pending=false,delivery_released_at=extract(epoch FROM clock_timestamp()) WHERE deployment_id=%s AND request_id=%s",
            (self.deployment, request_id),
        )
        await self._notify(conn, "r:" + request_id)

    async def release_completed_response(self, request_id):
        async with self.store.transaction("relay_release") as conn:
            row = await self._request_lock(conn, request_id)
            if row["state"] != "completed":
                raise web.HTTPConflict(text="response is not committed")
            await self._release(conn, request_id)

    async def cancel_request(self, *, request_id, response, reason="canceled"):
        async with self.store.transaction("relay_cancel") as conn:
            row = await self._request_lock(conn, request_id)
            if row["state"] == "completed":
                # Never replace an accepted sample with a timeout response.
                return (await self._load(conn, request_id)).completed_response
            await self._complete(conn, row, response, await self._now(conn), defer=True)
            return response

    async def wait_for_response(self, request, *, timeout_seconds):
        if request.future.done():
            return request.future.result()
        request_id = request.request_id
        waiters = self._response_waiters
        future = asyncio.get_running_loop().create_future()
        waiters.setdefault(request_id, set()).add(future)
        self._signal("r:" + request_id)
        try:
            return await _wait_cancellable(future, timeout_seconds)
        finally:
            waiting = waiters[request_id]
            waiting.discard(future)
            if not waiting:
                del waiters[request_id]

    async def _deliver_loop(self):
        """Batch socket waiters' reads; hundreds of sockets are not DB pollers.

        This cache owns no work. A restart loses only socket futures, and any
        authenticated reattach reconstructs delivery from the committed rows.
        """
        next_reconcile = time.monotonic() + 0.5
        while not self._closing:
            with suppress(asyncio.TimeoutError):
                await _wait_cancellable(
                    self._delivery_event.wait(), max(0, next_reconcile - time.monotonic())
                )
            self._delivery_event.clear()
            await asyncio.sleep(
                0.002
            )  # Coalesce a notification burst, not work admission.
            if time.monotonic() >= next_reconcile:
                # A fixed schedule recovers missed hints even during a stream
                # of unrelated notifications. Hints are never authority.
                ids = list(self._response_waiters.keys() | self._active_parks.keys())
                next_reconcile = time.monotonic() + 0.5
            else:
                ids = [request_id for request_id in self._dirty_deliveries
                       if request_id in self._response_waiters or request_id in self._active_parks]
            self._dirty_deliveries.clear()
            if not ids:
                continue
            try:
                async with self._delivery_store.transaction("relay_delivery_status") as conn:
                    rows = await (
                        await conn.execute(
                            "SELECT request_id,state,delivery_pending,completed_bytes,completed_at FROM relay_requests WHERE deployment_id=%s AND request_id=ANY(%s)",
                            (self.deployment, ids),
                        )
                    ).fetchall()
                # A park notifier may still be queued behind checkpoint work.
                # Tell it when the durable result supersedes that work, just
                # for the same durable request identity. This
                # is only an optimization; worker wake fences remain authority.
                for row in rows:
                    park = self._active_parks.get(row["request_id"])
                    if park is not None and row["state"] == "completed":
                        park.completed_at = row["completed_at"]
                        park.response_committed.set()
                missing = set(ids) - {r["request_id"] for r in rows}
                for request_id in missing:
                    for future in tuple(self._response_waiters.get(request_id, ())):
                        if not future.done():
                            future.set_exception(
                                web.HTTPGone(text="relay result retention expired")
                            )
                ready = [
                    r
                    for r in rows
                    if r["state"] == "completed"
                    and not r["delivery_pending"]
                    and r["request_id"] in self._response_waiters
                ]
                while ready:
                    batch, size = [], 0
                    while ready and (
                        not batch or size + ready[-1]["completed_bytes"] <= 8 * 1024**2
                    ):
                        row = ready.pop()
                        batch.append(row["request_id"])
                        size += row["completed_bytes"]
                    async with self._delivery_store.transaction("relay_delivery_bodies") as conn:
                        results = await (
                            await conn.execute(
                                "SELECT request_id,body,encoding,status,headers FROM relay_results WHERE deployment_id=%s AND request_id=ANY(%s)",
                                (self.deployment, batch),
                            )
                        ).fetchall()
                    for result in results:
                        body = bytes(result["body"])
                        response = api.RelayWorkerResponse(
                            result["status"],
                            body
                            if result["encoding"] == "base64"
                            else json.loads(body),
                            result["headers"],
                        )
                        for future in tuple(
                            self._response_waiters.get(result["request_id"], ())
                        ):
                            if not future.done():
                                future.set_result(response)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.warning("relay delivery read failed; durable response retained")

    async def maintain(self):
        async with self.store.transaction("relay_maintenance") as conn:
            now = await self._now(conn)
            rows = await (
                await conn.execute(
                    "SELECT * FROM relay_requests WHERE deployment_id=%s AND state!='completed' AND expires_at<=%s ORDER BY request_id LIMIT 128 FOR UPDATE SKIP LOCKED",
                    (self.deployment, now),
                )
            ).fetchall()
            for row in rows:
                await self._complete(
                    conn,
                    row,
                    api.RelayWorkerResponse(
                        504, api._openai_error("relay request expired", "relay_timeout")
                    ),
                    now,
                    defer=True,
                )
            # Reclaim unused response reservations in batches, off the result
            # commit path. Retain the actual response bytes until retention ends.
            compact = await (
                await conn.execute(
                    """SELECT request_id,reserved_bytes,completed_bytes
                FROM relay_requests WHERE deployment_id=%s AND state='completed'
                AND reserved_bytes>completed_bytes+65536 ORDER BY request_id LIMIT 256
                FOR UPDATE SKIP LOCKED""",
                    (self.deployment,),
                )
            ).fetchall()
            reclaimed = sum(
                r["reserved_bytes"] - r["completed_bytes"] - 65536 for r in compact
            )
            if compact:
                await conn.execute(
                    "UPDATE relay_requests SET reserved_bytes=completed_bytes+65536 WHERE deployment_id=%s AND request_id=ANY(%s)",
                    (self.deployment, [r["request_id"] for r in compact]),
                )
            # Delivered retention starts only once delivery is released, below.
            expired = await (
                await conn.execute(
                    """SELECT request_id,reserved_bytes FROM relay_requests r WHERE deployment_id=%s
                AND state='completed' AND NOT delivery_pending AND greatest(completed_at,coalesce(delivery_released_at,completed_at))<%s
                AND NOT EXISTS(SELECT 1 FROM relay_lifecycle l WHERE l.deployment_id=r.deployment_id AND l.request_id=r.request_id AND NOT l.done)
                ORDER BY request_id LIMIT 128 FOR UPDATE SKIP LOCKED""",
                    (self.deployment, now - self.retention),
                )
            ).fetchall()
            if expired:
                await conn.execute(
                    "DELETE FROM relay_requests WHERE deployment_id=%s AND request_id=ANY(%s)",
                    (self.deployment, [r["request_id"] for r in expired]),
                )
            reclaimed += sum(r["reserved_bytes"] for r in expired)
            if reclaimed:
                await conn.execute(
                    "UPDATE relay_quota SET reserved_bytes=reserved_bytes-%s WHERE deployment_id=%s",
                    (reclaimed, self.deployment),
                )
            await conn.execute(
                "DELETE FROM relay_workers WHERE deployment_id=%s AND last_seen_at<%s",
                (self.deployment, now - self.worker_retention),
            )

    async def pending_caller_incarnations(self):
        async with self.store.transaction("relay_pending_callers") as conn:
            rows = await (await conn.execute(
                "SELECT DISTINCT sandbox_id,sandbox_generation FROM relay_requests "
                "WHERE deployment_id=%s AND sandbox_id IS NOT NULL "
                "AND sandbox_generation IS NOT NULL AND (state!='completed' OR delivery_pending)",
                (self.deployment,),
            )).fetchall()
        return {(r['sandbox_id'], r['sandbox_generation']) for r in rows}

    async def reconcile_unavailable_callers(self, terminal):
        if not terminal:
            return
        # Terminal history grows with the lifetime of the fleet. Restrict SQL
        # work to callers that still have outstanding work in this deployment,
        # rather than opening a transaction for every historical sandbox loss.
        async with self.store.transaction("relay_terminal_candidates") as conn:
            callers = await (
                await conn.execute(
                    "SELECT DISTINCT sandbox_id,sandbox_generation FROM relay_requests WHERE deployment_id=%s AND (state!='completed' OR delivery_pending)",
                    (self.deployment,),
                )
            ).fetchall()
        for caller in callers:
            sandbox, generation = caller["sandbox_id"], caller["sandbox_generation"]
            reason = terminal.get((sandbox, generation))
            if reason is None:
                continue
            # Recheck under the row lock: result delivery may have completed
            # since the candidate read. New requests are handled next pass.
            async with self.store.transaction("relay_terminal_caller") as conn:
                rows = await (
                    await conn.execute(
                        "SELECT * FROM relay_requests WHERE deployment_id=%s AND sandbox_id=%s AND sandbox_generation=%s AND (state!='completed' OR delivery_pending) ORDER BY request_id FOR UPDATE",
                        (self.deployment, sandbox, generation),
                    )
                ).fetchall()
                for row in rows:
                    if row["state"] == "completed":
                        await self._release(conn, row["request_id"])
                    else:
                        await self._complete(
                            conn,
                            row,
                            api.RelayWorkerResponse(
                                410,
                                api._openai_error("sandbox caller unavailable", reason),
                            ),
                            await self._now(conn),
                            defer=False,
                        )
                    await conn.execute(
                        "UPDATE relay_lifecycle SET done=true,claim_token=NULL,claim_until=NULL WHERE deployment_id=%s AND request_id=%s",
                        (self.deployment, row["request_id"]),
                    )

    async def stats(self):
        async with self.store.transaction("relay_stats") as conn:
            rows = await (
                await conn.execute(
                    "SELECT state,rollout_id,count(*) AS n,coalesce(sum(payload_bytes),0) AS bytes FROM relay_requests WHERE deployment_id=%s GROUP BY state,rollout_id",
                    (self.deployment,),
                )
            ).fetchall()
            summary = await (
                await conn.execute(
                    """SELECT count(*) FILTER(WHERE delivery_pending) AS delivery_pending,
                coalesce(max(extract(epoch FROM clock_timestamp())-completed_at) FILTER(WHERE delivery_pending),0) AS oldest_delivery_pending_seconds,
                coalesce(sum(completed_bytes),0) AS completed_bytes FROM relay_requests WHERE deployment_id=%s""",
                    (self.deployment,),
                )
            ).fetchone()
            workers = await (
                await conn.execute(
                    "SELECT rollout_id,worker_id,last_seen_at,metadata FROM relay_workers WHERE deployment_id=%s",
                    (self.deployment,),
                )
            ).fetchall()
            quota = await (
                await conn.execute(
                    "SELECT reserved_bytes FROM relay_quota WHERE deployment_id=%s",
                    (self.deployment,),
                )
            ).fetchone()
            nrollouts = (
                await (
                    await conn.execute(
                        "SELECT count(*) AS n FROM relay_rollouts WHERE deployment_id=%s AND enabled",
                        (self.deployment,),
                    )
                ).fetchone()
            )["n"]
            lifecycle = await (
                await conn.execute(
                    """SELECT action,count(*) AS n,max(attempts) AS max_attempts,
                    count(*) FILTER(WHERE claim_until > statement_timestamp()) AS claimed,
                    count(*) FILTER(WHERE (claim_until IS NULL OR claim_until <= statement_timestamp())
                        AND next_attempt_at <= statement_timestamp()) AS due,
                    count(*) FILTER(WHERE (claim_until IS NULL OR claim_until <= statement_timestamp())
                        AND next_attempt_at > statement_timestamp()) AS deferred,
                    coalesce(max(extract(epoch FROM statement_timestamp()-next_attempt_at))
                        FILTER(WHERE (claim_until IS NULL OR claim_until <= statement_timestamp())
                        AND next_attempt_at <= statement_timestamp()),0)::double precision AS oldest_due_seconds,
                    count(*) FILTER(WHERE last_error IS NOT NULL) AS retrying_after_error
                    FROM relay_lifecycle WHERE deployment_id=%s AND NOT done GROUP BY action""",
                    (self.deployment,),
                )
            ).fetchall()
        return {
            **{
                k: float(v) if k.endswith("seconds") else int(v)
                for k, v in summary.items()
            },
            "backend": "postgres",
            "rollouts": nrollouts,
            "pending": {
                r["rollout_id"]: r["n"] for r in rows if r["state"] == "pending"
            },
            "leased": {r["rollout_id"]: r["n"] for r in rows if r["state"] == "leased"},
            "inflight": sum(r["n"] for r in rows if r["state"] != "completed"),
            "inflight_bytes": int(sum(r["bytes"] for r in rows)),
            "completed_retained": sum(
                r["n"] for r in rows if r["state"] == "completed"
            ),
            "workers": workers,
            "lifecycle": lifecycle,
            "reserved_storage_bytes": quota["reserved_bytes"] if quota else 0,
            "database_pool": self.store.pool.get_stats(),
            "lifecycle_database_pool": (
                self._lifecycle_store.pool.get_stats()
                if self._lifecycle_store is not None else None
            ),
            "limits": {"storage_budget_bytes": self.storage_budget},
            "counters": {},
            "timers": {},
            "averages": {},
        }

    async def _claim_lifecycle(self, limit, *, action=None):
        async with self._delivery_store.transaction("relay_claim_lifecycle") as conn:
            # Requests are always locked before operations by mutation paths.
            # Claim only operation rows here and COMMIT before touching requests.
            rows = await (
                await conn.execute(
                    """WITH due AS (
                SELECT l.deployment_id,l.request_id,l.action FROM relay_lifecycle l
                WHERE l.deployment_id=%s AND l.action=ANY(%s) AND NOT l.done AND l.next_attempt_at<=clock_timestamp()
                AND (l.claim_until IS NULL OR l.claim_until<=clock_timestamp())
                ORDER BY l.next_attempt_at,l.request_id LIMIT %s FOR UPDATE OF l SKIP LOCKED)
                UPDATE relay_lifecycle l SET claim_token=gen_random_uuid(),claim_until=clock_timestamp()+%s*interval '1 second',attempts=l.attempts+1
                FROM due JOIN relay_requests r ON (r.deployment_id,r.request_id)=(due.deployment_id,due.request_id)
                WHERE (l.deployment_id,l.request_id,l.action)=(due.deployment_id,due.request_id,due.action)
                RETURNING l.*,extract(epoch FROM clock_timestamp())::double precision AS claimed_at,to_jsonb(r) AS request_record,
                CASE WHEN l.action='park' THEN (SELECT g.metadata->'_ucloud_resource_phase' FROM relay_rollouts g
                 WHERE g.deployment_id=r.deployment_id AND g.rollout_id=r.rollout_id
                 AND g.registration_token=r.registration_token AND g.enabled)
                ELSE NULL END AS phase_observation""",
                    (
                        self.deployment,
                        [k for k, v in self.notifiers.items() if v is not None and (action is None or k == action)],
                        limit,
                        self.claim_seconds,
                    ),
                )
            ).fetchall()
            return rows

    async def _dispatch_loop(self):
        async with self._watch("l") as event:
            while not self._closing:
                event.clear()
                try:
                    claimed = False
                    # Waiting parks cannot consume wake dispatch admission.
                    # Each class retains durable overflow rather than rejecting work.
                    for action in ("wake", "park"):
                        available = self.concurrency - len(self._active_by_action[action])
                        if available <= 0 or self.notifiers[action] is None:
                            continue
                        rows = await self._claim_lifecycle(available, action=action)
                        if self._closing:
                            return  # Any claimed work recovers after lease expiry.
                        for row in rows:
                            task = asyncio.create_task(self._dispatch(row))
                            self._active.add(task)
                            self._active_by_action[action].add(task)
                            task.add_done_callback(self._dispatch_done)
                        claimed |= bool(rows)
                    if claimed:
                        continue
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.warning(
                        "relay lifecycle claim failed; durable work retained"
                    )
                with suppress(asyncio.TimeoutError):
                    await _wait_cancellable(event.wait(), 0.25)

    def _dispatch_done(self, task):
        self._active.discard(task)
        for active in self._active_by_action.values():
            active.discard(task)
        self._signal("l")
        if not task.cancelled() and task.exception() is not None:
            LOGGER.warning(
                "relay lifecycle dispatch interrupted; durable claim will recover (%s)",
                type(task.exception()).__name__,
            )

    async def _renew_lifecycle_claims(self):
        # One physical commit renews all this process's in-flight claims. The
        # snapshot is only a hint: each exact token is checked by PostgreSQL.
        owned = tuple(self._active_claims.items())
        if not owned:
            return
        async with self._delivery_store.transaction("relay_renew_lifecycle") as conn:
            rows = await (await conn.execute(
                """WITH owned AS (
                    SELECT * FROM unnest(%s::text[],%s::text[],%s::uuid[])
                    AS x(request_id,action,claim_token)
                ), eligible AS (
                    SELECT l.deployment_id,l.request_id,l.action
                    FROM relay_lifecycle l JOIN owned x
                    ON (l.request_id,l.action,l.claim_token)=(x.request_id,x.action,x.claim_token)
                    WHERE l.deployment_id=%s AND NOT l.done
                    ORDER BY l.request_id,l.action FOR UPDATE OF l
                ) UPDATE relay_lifecycle l
                SET claim_until=clock_timestamp()+%s*interval '1 second'
                FROM eligible e
                WHERE (l.deployment_id,l.request_id,l.action)=(e.deployment_id,e.request_id,e.action)
                RETURNING l.request_id,l.action""",
                ([key[0] for key, _ in owned], [key[1] for key, _ in owned],
                 [token for _, token in owned], self.deployment, self.claim_seconds),
            )).fetchall()
        renewed = {(row["request_id"], row["action"]) for row in rows}
        for key, token in owned:
            if key not in renewed and self._active_claims.get(key) == token:
                self._active_claims.pop(key)

    async def _renew_lifecycle_loop(self):
        while not self._closing:
            await asyncio.sleep(self.claim_seconds / 3)
            try:
                await self._renew_lifecycle_claims()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Loss of a lease is recoverable by the existing durable claim
                # loop. Never turn an uncertain renewal into lifecycle success.
                LOGGER.warning("relay lifecycle renewal failed; durable claims remain fenced")

    async def _dispatch(self, work):
        # Separate time waiting for an eligible claim from the HTTP attempt and
        # durable completion. This span also parents the gateway/worker trace.
        dispatched = time.time()
        claimed = float(work.get("claimed_at", dispatched))
        attributes = {
            "relay.lifecycle.dispatch_lag_seconds": max(0.0, dispatched - claimed),
            "relay.request.id": work["request_id"],
            "relay.lifecycle.action": work["action"],
            "relay.lifecycle.attempt": work["attempts"],
            "relay.lifecycle.due_wait_seconds": max(
                0.0, claimed - work["next_attempt_at"].timestamp(),
            ),
        }
        completed = work["request_record"].get("completed_at")
        if completed is not None:
            attributes["relay.lifecycle.response_age_seconds"] = max(0.0, dispatched - float(completed))
        with self.telemetry.span(
            "relay.lifecycle.dispatch", attributes=attributes,
        ):
            await self._dispatch_claimed(work)

    async def _dispatch_claimed(self, work):
        request_id, action = work["request_id"], work["action"]
        # The claim hydrates only small request columns, never model bodies.
        request = self._request_value(work["request_record"])
        if action == "park":
            request.resource_phase = current_phase(
                {PHASE_METADATA_KEY: work.get("phase_observation")}, now=time.time())
            if request.resource_phase is not None:
                request.resource_phase["registration_incarnation"] = hashlib.sha256(
                    request.registration_token.encode()
                ).hexdigest()
        if request.state == "completed":
            request.expires_at = (
                None  # Accepted result delivery outlives inference timeout.
            )
        claim_key = (request_id, action)
        self._active_claims[claim_key] = work["claim_token"]
        if action == "park":
            self._active_parks[request_id] = request
            self._signal("r:" + request_id)
        epoch, unavailable, failure = None, False, None
        deferred = None
        try:
            # A committed result makes a queued park obsolete. Never replay it.
            if not (action == "park" and request.state == "completed"):
                notifier = self.notifiers[action]
                if notifier is None:
                    raise RuntimeError("lifecycle notifier missing")
                # Keep transport acknowledgment separate from the subsequent
                # durable completion transaction (measured by PostgresDatabase).
                # This also records HTTP duration when traces are not sampled.
                with self.telemetry.span(f"relay.lifecycle.{action}.http"):
                    epoch = await notifier(request)
        except api.RelayLifecycleDeferred as exc:
            trace.get_current_span().set_attribute("relay.lifecycle.retry_delay_seconds", exc.seconds)
            deferred = exc.seconds
            epoch = exc.transport_epoch
        except api.RelayCallerUnavailable:
            unavailable = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failure = type(exc).__name__  # No credentials/URLs in durable diagnostics.
        finally:
            if action == "park":
                self._active_parks.pop(request_id, None)
            if self._active_claims.get(claim_key) == work["claim_token"]:
                self._active_claims.pop(claim_key)
        async with self._delivery_store.transaction("relay_dispatch_complete") as conn:
            row = await self._request_lock(conn, request_id)
            current = await (
                await conn.execute(
                    "SELECT claim_token,done FROM relay_lifecycle WHERE deployment_id=%s AND request_id=%s AND action=%s FOR UPDATE",
                    (self.deployment, request_id, action),
                )
            ).fetchone()
            if (
                current is None
                or current["done"]
                or current["claim_token"] != work["claim_token"]
            ):
                return
            if failure or deferred is not None:
                if action == 'park' and epoch is not None:
                    # A retained sandbox can park locally as pressure changes.
                    # The worker accepted ownership of the local park decision.
                    # Publish that receipt with its original transport as one
                    # monotonic pair; SDK lease renewal observes this state.
                    # This is not a claim that checkpointing has completed.
                    await conn.execute(
                        "UPDATE relay_requests SET accepted_notified_at=COALESCE(accepted_notified_at,%s),parked_transport_epoch=COALESCE(parked_transport_epoch,%s) WHERE deployment_id=%s AND request_id=%s",
                        (await self._now(conn), epoch, self.deployment, request_id),
                    )
                await conn.execute(
                    "UPDATE relay_lifecycle SET claim_token=NULL,claim_until=NULL,last_error=%s,next_attempt_at=clock_timestamp()+%s*interval '1 second' WHERE deployment_id=%s AND request_id=%s AND action=%s",
                    (
                        failure,
                        deferred if deferred is not None else min(5, 0.05 * 2 ** min(work["attempts"], 7)),
                        self.deployment,
                        request_id,
                        action,
                    ),
                )
                return
            now = await self._now(conn)
            if action == "wake":
                # Response remains available even if its caller definitively died.
                await conn.execute(
                    "UPDATE relay_requests SET wake_notified_at=%s,wake_transport_epoch=%s,delivery_pending=false,delivery_released_at=%s WHERE deployment_id=%s AND request_id=%s",
                    (
                        None if unavailable else now,
                        epoch,
                        now,
                        self.deployment,
                        request_id,
                    ),
                )
                if (
                    epoch is not None
                    and row["parked_transport_epoch"] is not None
                    and epoch != row["parked_transport_epoch"]
                ):
                    await self._reattach(conn, row)
                await self._notify(conn, "r:" + request_id)
            else:
                await conn.execute(
                    "UPDATE relay_requests SET accepted_notified_at=COALESCE(accepted_notified_at,%s),parked_transport_epoch=COALESCE(parked_transport_epoch,%s) WHERE deployment_id=%s AND request_id=%s",
                    (now, epoch, self.deployment, request_id),
                )
                original_epoch = row["parked_transport_epoch"] or epoch
                if (
                    original_epoch is not None
                    and row["wake_transport_epoch"] is not None
                    and original_epoch != row["wake_transport_epoch"]
                ):
                    await self._reattach(conn, row)
            await conn.execute(
                "UPDATE relay_lifecycle SET done=true,claim_token=NULL,claim_until=NULL,last_error=%s WHERE deployment_id=%s AND request_id=%s AND action=%s",
                (
                    "caller_unavailable" if unavailable else None,
                    self.deployment,
                    request_id,
                    action,
                ),
            )
            await self._notify(conn, "l")
