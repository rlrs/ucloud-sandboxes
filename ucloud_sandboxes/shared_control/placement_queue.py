"""Durable, replay-safe lifecycle commands; HTTP sockets are not the work queue.

The placement role executes the existing gateway lifecycle domain. Create
allocation binds the command to its generation in the *same* routing transaction;
replaying a lost HTTP reply can never recreate a deliberately deleted sandbox.
Wake commands already carry the worker's durable generation/operation fence.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from datetime import datetime, timedelta
from hashlib import sha256
import json
import logging
import threading
from uuid import UUID, uuid4
from typing import NamedTuple

import aiohttp
from psycopg import AsyncConnection, sql
from psycopg.types.json import Jsonb

from ..models import utc_now
from ..sandbox import SandboxSpec, sandbox_spec_fingerprint
from .database import PostgresDatabase
from .routing_repository import ROUTING_ADDITIVE_DDL

LOGGER = logging.getLogger(__name__)
COMMAND_HEADER = "X-UCloud-Placement-Command"
CLAIM_HEADER = "X-UCloud-Placement-Claim"


_ENQUEUE_SQL = """INSERT INTO gateway_commands(command_id,kind,sandbox_id,path,headers,body,deadline,command_key)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(command_key) WHERE state!='done'
    DO UPDATE SET command_key=EXCLUDED.command_key
    RETURNING command_id,deadline"""


class PlacementHints:
    """LISTEN/NOTIFY wakeups for queue and result readers; polling stays authoritative.

    NOTIFY runs after commit on its own coalesced statement, as in the relay:
    inside a transaction its queue lock serializes concurrent commits. A lost
    hint costs one fallback interval, never a command or a result.
    """

    def __init__(self, store, *, wake_on, fallback_seconds):
        self.store = store
        self.channel = f"placement_{store.schema}"[:63]
        self.wake_on = frozenset(wake_on)
        self.fallback = fallback_seconds
        self._wake = asyncio.Event()
        self._send = asyncio.Event()
        self._pending: set[str] = set()
        self._tasks: tuple[asyncio.Task, ...] = ()

    def start(self):
        if not self._tasks:
            self._tasks = (
                asyncio.create_task(self._listen()),
                asyncio.create_task(self._notify_loop()),
            )

    def notify(self, key):
        self._pending.add(key)
        self._send.set()

    def poke(self):
        """Wake this process's waiter, e.g. when local capacity frees up."""
        self._wake.set()

    async def wait(self):
        """Return on a relevant hint or after the fallback interval."""
        try:
            await asyncio.wait_for(self._wake.wait(), self.fallback)
        except asyncio.TimeoutError:
            pass
        # No await since the wait returned: a hint arriving during the
        # caller's next read sets the event again instead of being lost.
        self._wake.clear()

    async def close(self):
        tasks, self._tasks = self._tasks, ()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _notify_loop(self):
        while True:
            await self._send.wait()
            self._send.clear()
            await asyncio.sleep(0.002)  # coalesce a burst into one statement
            keys = sorted(self._pending)
            self._pending.clear()
            try:
                async with self.store.statement("placement_notify") as conn:
                    await conn.execute(
                        "SELECT pg_notify(%s,key) FROM unnest(%s::text[]) AS key",
                        (self.channel, keys),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.warning("placement hint failed; fallback polling continues")

    async def _listen(self):
        while True:
            try:
                # A dedicated connection; LISTEN never holds a pooled one.
                async with await AsyncConnection.connect(
                    self.store.pool.conninfo, autocommit=True
                ) as conn:
                    await conn.execute(
                        sql.SQL("LISTEN {}").format(sql.Identifier(self.channel))
                    )
                    self._wake.set()  # recover anything sent while disconnected
                    async for notification in conn.notifies():
                        if notification.payload in self.wake_on:
                            self._wake.set()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.warning("placement hint connection lost; reconnecting")
                await asyncio.sleep(0.5)


class PlacementSubmission(NamedTuple):
    command_id: UUID
    deadline: datetime


class PlacementQueue(PostgresDatabase):
    schema_prefix = "ucloud_routing"
    schema_file = "routing_schema.sql"
    version_table = "routing_schema_version"
    additive_ddl = ROUTING_ADDITIVE_DDL

    def completion_reader(self):
        """Give batched completion reads one connection outside enqueue admission."""
        return self.fresh(max_connections=1)

    async def submit(
        self, kind, sandbox_id, path, headers, body, *, timeout_seconds=600
    ):
        command_id = uuid4()
        if (
            kind not in ("create", "wake")
            or not sandbox_id
            or not 0 < timeout_seconds <= 7200
        ):
            raise ValueError("invalid placement command")
        if len(body) > 1024 * 1024:
            raise ValueError("placement command exceeds the JSON request budget")
        # The same pending-demand row used by autoscaling represents queued
        # creates. Never hide a large pre-placement queue from scale-up.
        payload = json.loads(body)
        reference_kind = next(
            (
                value
                for key, value in headers.items()
                if key.lower() == "x-ucloud-image-reference-kind"
            ),
            "",
        )
        command_key = sha256(
            json.dumps(
                [kind, sandbox_id, payload, reference_kind],
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        ).hexdigest()
        spec = SandboxSpec.from_dict(payload) if kind == "create" else None
        if spec is not None:
            spec.validate()
            if spec.id != sandbox_id:
                raise ValueError("placement command sandbox identity differs")
        command = (
            command_id,
            kind,
            sandbox_id,
            path,
            Jsonb(headers),
            body,
            utc_now() + timedelta(seconds=timeout_seconds),
            command_key,
        )
        # One autocommit statement per submission: under a burst every extra
        # BEGIN/COMMIT round trip holds a pooled connection for another loop turn.
        async with self.statement("placement_enqueue") as conn:
            if spec is None:
                row = await (await conn.execute(_ENQUEUE_SQL, command)).fetchone()
            else:
                now = utc_now().isoformat()
                # A coalesced submission records demand under the command it
                # joined, exactly as the separate statements previously did.
                row = await (
                    await conn.execute(
                        "WITH command AS (" + _ENQUEUE_SQL + """), demand AS (
                        INSERT INTO pending(sandbox_id,resources_json,created_at,updated_at,
                        attempts,generation,operation_id,spec_hash,failure_reason)
                        SELECT %s,%s,%s,%s,1,0,command.command_id::text,%s,'queued_create'
                        FROM command WHERE NOT EXISTS(SELECT 1 FROM sandboxes WHERE sandbox_id=%s)
                        ON CONFLICT(sandbox_id) DO NOTHING)
                        SELECT command_id,deadline FROM command""",
                        (
                            *command,
                            sandbox_id,
                            json.dumps(spec.requested_resources().to_dict()),
                            now,
                            now,
                            sandbox_spec_fingerprint(spec),
                            sandbox_id,
                        ),
                    )
                ).fetchone()
        return PlacementSubmission(row["command_id"], row["deadline"])

    async def results(self, ids):
        if not ids:
            return []
        async with self.statement("placement_results") as conn:
            return await (
                await conn.execute(
                    """SELECT command_id,result_status,result_headers,result_body
                FROM gateway_commands WHERE command_id=ANY(%s) AND state='done' """,
                    (list(ids),),
                )
            ).fetchall()

    async def claim(self, kind, limit, *, lease_seconds=30):
        if limit <= 0:
            return []
        async with self.transaction("placement_claim_commands") as conn:
            return await (
                await conn.execute(
                    """WITH due AS (
                SELECT command_id FROM gateway_commands WHERE kind=%s AND state!='done'
                AND next_attempt_at<=clock_timestamp()
                AND (claim_until IS NULL OR claim_until<=clock_timestamp())
                ORDER BY created_at,command_id LIMIT %s FOR UPDATE SKIP LOCKED)
                UPDATE gateway_commands c SET state='running',claim_token=gen_random_uuid(),
                    claim_until=clock_timestamp()+%s*interval '1 second',attempts=c.attempts+1
                FROM due WHERE c.command_id=due.command_id RETURNING c.*""",
                    (kind, limit, lease_seconds),
                )
            ).fetchall()

    async def renew(self, command, *, lease_seconds=30):
        async with self.transaction("placement_renew_command") as conn:
            row = await (
                await conn.execute(
                    """UPDATE gateway_commands
                SET claim_until=clock_timestamp()+%s*interval '1 second'
                WHERE command_id=%s AND claim_token=%s AND state='running' RETURNING command_id""",
                    (lease_seconds, command["command_id"], command["claim_token"]),
                )
            ).fetchone()
            return row is not None

    async def defer(self, command, *, delay=1):
        async with self.transaction("placement_defer_command") as conn:
            await conn.execute(
                """UPDATE gateway_commands SET state='queued',claim_token=NULL,claim_until=NULL,
                next_attempt_at=clock_timestamp()+%s*interval '1 second'
                WHERE command_id=%s AND claim_token=%s AND state='running' """,
                (delay, command["command_id"], command["claim_token"]),
            )

    async def complete(self, command, status, headers, body):
        if len(body) > 16 * 1024 * 1024:
            raise ValueError("placement result exceeds metadata response budget")
        async with self.transaction("placement_complete_command") as conn:
            row = await (
                await conn.execute(
                    """UPDATE gateway_commands SET state='done',claim_token=NULL,claim_until=NULL,
                result_status=%s,result_headers=%s,result_body=%s,completed_at=clock_timestamp()
                WHERE command_id=%s AND claim_token=%s AND state='running' RETURNING sandbox_id""",
                    (
                        status,
                        Jsonb(headers),
                        body,
                        command["command_id"],
                        command["claim_token"],
                    ),
                )
            ).fetchone()
            if row is None:
                return False
            await conn.execute(
                "DELETE FROM pending WHERE sandbox_id=%s AND operation_id=%s AND failure_reason='queued_create'",
                (row["sandbox_id"], str(command["command_id"])),
            )
            return True

    async def prune(self, *, batch=1000, max_batches=20):
        """Delete expired results in short batches until caught up.

        One batch a minute capped retention at ~17 commands/s; beyond that the
        table (bodies up to 1 MiB, results up to 16 MiB) grew without bound.
        """
        deleted = 0
        for _ in range(max_batches):
            async with self.transaction("placement_prune_commands") as conn:
                cursor = await conn.execute("""DELETE FROM gateway_commands WHERE command_id IN (
                    SELECT command_id FROM gateway_commands WHERE state='done'
                    AND completed_at<clock_timestamp()-interval '1 hour' LIMIT %s)""",
                    (batch,))
            deleted += cursor.rowcount
            if cursor.rowcount < batch:
                break
        return deleted


class PlacementQueueClient:
    """Batch result polling for every detached HTTP waiter in this process.

    One bounded fallback read serves all outstanding requests; 512 sockets do
    not become 512 independent polling loops. The durable rows own the work.
    """

    def __init__(self, store, *, results_store=None):
        self.store = store
        self.results_store = (
            store.completion_reader() if results_store is None else results_store
        )
        self.waiters = {}
        self._ready = asyncio.Lock()
        self._opened = False
        self._closed = False
        self._replace_failed_pools = False
        self._poller = None
        self._hints = None

    async def open(self):
        async with self._ready:
            if self._closed:
                raise RuntimeError("placement response owner is closed")
            if not self._opened:
                if self._replace_failed_pools:
                    # psycopg pools cannot reopen after close. Nothing was
                    # accepted before both initial pools opened, so replace
                    # only these failed startup resources, never live work.
                    self.store = self.store.fresh()
                    self.results_store = self.results_store.fresh()
                    self._replace_failed_pools = False
                try:
                    await self.store.open()
                    await self.results_store.open()
                    if self._closed:
                        raise RuntimeError("placement response owner is closed")
                except BaseException:
                    self._replace_failed_pools = True
                    await asyncio.gather(
                        self.results_store.close(),
                        self.store.close(),
                        return_exceptions=True,
                    )
                    raise
                self._hints = PlacementHints(
                    self.store, wake_on={"done"}, fallback_seconds=0.25
                )
                self._hints.start()
                self._opened = True

    async def response(self, kind, sandbox_id, path, headers, body):
        try:
            await self.open()
            submission = await self.store.submit(kind, sandbox_id, path, headers, body)
            self._hints.notify(kind)
        except Exception:
            return (
                503,
                {"Content-Type": "application/json"},
                b'{"error":"placement submission unavailable; retry the same sandbox identity","retryable":true}',
            )
        if self._closed:
            # Shutdown may finish while submit is committing. The durable
            # command still owns the work; never resurrect an HTTP waiter or
            # poller against closed pools, or imply that nothing was accepted.
            return (
                503,
                {"Content-Type": "application/json"},
                b'{"error":"placement completion is unknown after shutdown; retry the same sandbox identity","error_code":"placement_outcome_unknown","retryable":true}',
            )
        future = asyncio.get_running_loop().create_future()
        command_id = submission.command_id
        self.waiters.setdefault(command_id, set()).add(future)
        if self._poller is None or self._poller.done():
            self._poller = asyncio.create_task(self._poll())
        try:
            return await asyncio.wait_for(
                future,
                timeout=max(
                    0.001, (submission.deadline - utc_now()).total_seconds() + 60
                ),
            )
        except TimeoutError:
            return (
                504,
                {"Content-Type": "application/json"},
                b'{"error":"placement completion is not yet known; retry the same sandbox identity","error_code":"placement_wait_timeout","retryable":true}',
            )
        finally:
            waiters = self.waiters.get(command_id)
            if waiters is not None:
                waiters.discard(future)
                if not waiters:
                    self.waiters.pop(command_id, None)

    async def _poll(self):
        while self.waiters:
            try:
                results = await self.results_store.results(self.waiters)
                for row in results:
                    for future in self.waiters.get(row["command_id"], ()):
                        if not future.done():
                            future.set_result(
                                (
                                    row["result_status"],
                                    row["result_headers"],
                                    bytes(row["result_body"]),
                                )
                            )
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.warning(
                    "placement result read unavailable; durable work retained"
                )
            await self._hints.wait()

    async def close(self):
        self._closed = True
        # Wait for startup ownership to settle before closing its pools.
        async with self._ready:
            if self._poller is not None:
                self._poller.cancel()
                await asyncio.gather(self._poller, return_exceptions=True)
            for waiters in self.waiters.values():
                for future in waiters:
                    future.cancel()
            self.waiters.clear()
            if self._hints is not None:
                await self._hints.close()
            if self._opened:
                try:
                    await self.results_store.close()
                finally:
                    try:
                        await self.store.close()
                    finally:
                        self._opened = False


class IsolatedPlacementResponses:
    """Run a PlacementQueueClient on its own event loop thread.

    The gateway's shared worker-RPC loop also carries every asynchronous proxy
    response and event long-poll. Enqueue pool waits and database round trips
    queued behind that traffic inflated submission latency far beyond the
    database's own commit time. Callers on any loop await the same coroutine
    contract; cancellation of a caller cancels its task on this loop.
    """

    def __init__(self, client, *, name="placement-queue-io", on_loop_started=None):
        self.client = client
        self._name = name
        self._on_loop_started = on_loop_started
        self._guard = threading.Lock()
        self._loop = None
        self._thread = None
        self._closed = False

    def _running_loop(self):
        with self._guard:
            if self._closed:
                raise RuntimeError("placement response owner is closed")
            if self._loop is None:
                ready = concurrent.futures.Future()

                def run(loop):
                    asyncio.set_event_loop(loop)
                    loop.call_soon(ready.set_result, None)
                    loop.run_forever()

                loop = asyncio.new_event_loop()
                thread = threading.Thread(
                    target=run, args=(loop,), name=self._name, daemon=True
                )
                thread.start()
                ready.result()
                self._loop, self._thread = loop, thread
                if self._on_loop_started is not None:
                    try:
                        self._on_loop_started(loop)
                    except Exception:
                        LOGGER.warning("placement loop observer unavailable")
            return self._loop

    async def open(self):
        loop = self._running_loop()
        await asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(self.client.open(), loop)
        )

    async def response(self, kind, sandbox_id, path, headers, body):
        try:
            loop = self._running_loop()
        except RuntimeError:
            # Nothing was submitted: shutdown precedes any durable command.
            return (
                503,
                {"Content-Type": "application/json"},
                b'{"error":"placement submission unavailable; retry the same sandbox identity","retryable":true}',
            )
        return await asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(
                self.client.response(kind, sandbox_id, path, headers, body), loop
            )
        )

    async def close(self):
        with self._guard:
            self._closed = True
            loop, thread = self._loop, self._thread
        if loop is None:
            await self.client.close()
            return
        async def shutdown():
            try:
                await self.client.close()
            finally:
                # Background probes (loop lag) end with their loop.
                current = asyncio.current_task()
                pending = [t for t in asyncio.all_tasks() if t is not current]
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        try:
            await asyncio.wrap_future(
                asyncio.run_coroutine_threadsafe(shutdown(), loop)
            )
        finally:
            loop.call_soon_threadsafe(loop.stop)
            await asyncio.get_running_loop().run_in_executor(None, thread.join, 10)
            if not thread.is_alive():
                loop.close()


class PlacementQueueWorker:
    def __init__(
        self,
        store,
        *,
        origin,
        token,
        create_concurrency=32,
        wake_concurrency=64,
        lease_seconds=30,
    ):
        self.store, self.origin, self.token = store, origin.rstrip("/"), token
        self.budgets = {"create": create_concurrency, "wake": wake_concurrency}
        self.lease = lease_seconds
        self.hints = None

    async def _complete(self, command, status, headers, body):
        completed = await self.store.complete(command, status, headers, body)
        if self.hints is not None:
            self.hints.notify("done")
        return completed

    async def execute(self, session, command):
        async def renew():
            while True:
                await asyncio.sleep(self.lease / 3)
                if not await self.store.renew(command, lease_seconds=self.lease):
                    raise RuntimeError("placement command lease was replaced")

        async def request():
            async with session.post(
                self.origin + command["path"],
                data=command["body"],
                headers={
                    **command["headers"],
                    "Authorization": "Bearer " + self.token,
                    COMMAND_HEADER: str(command["command_id"]),
                    CLAIM_HEADER: str(command["claim_token"]),
                    "Content-Type": "application/json",
                },
                allow_redirects=False,
            ) as response:
                body = bytearray()
                async for part in response.content.iter_chunked(65536):
                    body.extend(part)
                    if len(body) > 16 * 1024 * 1024:
                        raise RuntimeError("placement result exceeds metadata budget")
                return response.status, dict(response.headers), bytes(body)

        if command["deadline"] <= utc_now() and command["generation"] is None:
            await self._complete(
                command,
                504,
                {"Content-Type": "application/json"},
                b'{"error":"placement deadline expired; allocation not authorized","error_code":"placement_deadline_expired"}',
            )
            return
        rpc = asyncio.create_task(request())
        lease = asyncio.create_task(renew())
        try:
            done, _ = await asyncio.wait(
                {rpc, lease}, return_when=asyncio.FIRST_COMPLETED
            )
            if lease in done:
                lease.result()
                return
            status, headers, body = rpc.result()
            if status in (408, 425, 429) or status >= 500:
                if command["deadline"] > utc_now():
                    await self.store.defer(
                        command, delay=min(2, 0.05 * 2 ** min(command["attempts"], 5))
                    )
                    return
            await self._complete(command, status, headers, body)
        except (aiohttp.ClientError, OSError, TimeoutError):
            # Replay only these generation/operation-fenced lifecycle commands.
            # Exec, upload, model inference and arbitrary mutations never enter.
            if command["deadline"] <= utc_now():
                await self._complete(
                    command,
                    504,
                    {"Content-Type": "application/json"},
                    b'{"error":"placement outcome is unknown; retry the same sandbox identity","retryable":true}',
                )
            else:
                await self.store.defer(command)
        finally:
            rpc.cancel()
            lease.cancel()
            await asyncio.gather(rpc, lease, return_exceptions=True)
            if self.hints is not None:
                self.hints.poke()  # a budget slot is free for queued work

    async def run(self, stop):
        await self.store.open()
        # Submissions wake the claim loop at once; the fallback only bounds
        # recovery of lost hints and deferred retries.
        self.hints = PlacementHints(
            self.store, wake_on=set(self.budgets), fallback_seconds=0.1
        )
        self.hints.start()
        tasks = {kind: set() for kind in self.budgets}
        next_prune = 0
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=660, connect=10),
                connector=aiohttp.TCPConnector(limit=0),
                auto_decompress=False,
            ) as session:
                while not stop.is_set():
                    changed = False
                    for kind, active in tasks.items():
                        for task in tuple(active):
                            if task.done():
                                active.remove(task)
                                if not task.cancelled() and task.exception():
                                    LOGGER.warning(
                                        "placement command retained for recovery"
                                    )
                        try:
                            commands = await self.store.claim(
                                kind,
                                self.budgets[kind] - len(active),
                                lease_seconds=self.lease,
                            )
                        except Exception:
                            LOGGER.warning(
                                "placement claim unavailable; retrying durable queue"
                            )
                            continue
                        active.update(
                            asyncio.create_task(self.execute(session, c))
                            for c in commands
                        )
                        changed |= bool(commands)
                    now = asyncio.get_running_loop().time()
                    if now >= next_prune:
                        try:
                            await self.store.prune()
                        except Exception:
                            LOGGER.warning("placement queue retention deferred")
                        next_prune = now + 60
                    if not changed:
                        await self.hints.wait()
        finally:
            pending = set().union(*tasks.values())
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            await self.hints.close()
            await self.store.close()
