"""ABBA empty-poll comparison with the exact rc19 poll method retained below.

Uses a disposable real PostgreSQL schema; never point at production. Worker
registrations are seeded before timing and remain within heartbeat freshness.
"""
import asyncio
from contextlib import suppress
import json
import os
import time
from uuid import uuid4
from psycopg import AsyncConnection, sql
from ucloud_sandboxes import model_relay as api
from ucloud_sandboxes.shared_control.database import PostgresDatabase
from ucloud_sandboxes.shared_control.relay import PostgresRelayState, _wait_cancellable

async def baseline_poll(self, *, rollout_id, registration_token, timeout_seconds, limit=1, lease_seconds=api.DEFAULT_WORKER_LEASE_SECONDS, worker_id=None):
    api.validate_rollout_id(rollout_id)
    api.validate_registration_token(registration_token)
    if worker_id is not None:
        api.validate_worker_id(worker_id)
    heartbeat_pending = worker_id is not None
    deadline = time.monotonic() + max(0, timeout_seconds)
    async with self._watch_poll(rollout_id, registration_token) as event:
        while True:
            event.clear()
            async with self.store.transaction('relay_claim_inference') as conn:
                await self._registration(conn, rollout_id, registration_token)
                if heartbeat_pending:
                    await self._write_worker_heartbeat(conn, rollout_id, registration_token, worker_id, None)
                    heartbeat_pending = False
                now = await self._now(conn)
                rows = await (await conn.execute("WITH candidates AS (\n                        SELECT deployment_id,request_id FROM relay_requests\n                        WHERE deployment_id=%s AND rollout_id=%s AND registration_token=%s\n                        AND expires_at>%s AND (state='pending' OR (state='leased' AND lease_expires_at<=%s))\n                        ORDER BY created_at,request_id LIMIT %s FOR UPDATE SKIP LOCKED\n                        ), claimed AS (\n                        UPDATE relay_requests r SET state='leased',\n                        lease_id=replace(gen_random_uuid()::text,'-',''),lease_expires_at=%s,leased_by=%s,\n                        delivered_at=%s,first_delivered_at=coalesce(r.first_delivered_at,%s),\n                        delivery_count=r.delivery_count+1 FROM candidates c\n                        WHERE (r.deployment_id,r.request_id)=(c.deployment_id,c.request_id) RETURNING r.*)\n                        SELECT claimed.*,p.body AS input_body,p.encoding AS input_encoding,p.headers AS input_headers\n                        FROM claimed LEFT JOIN relay_payloads p USING(deployment_id,request_id)\n                        ORDER BY claimed.created_at,claimed.request_id", (self.deployment, rollout_id, registration_token, now, now, max(1, min(256, limit)), now + max(0.001, lease_seconds), worker_id, now, now))).fetchall()
                result = [self._loaded_request(row) for row in rows]
            if result or time.monotonic() >= deadline:
                return result
            with suppress(asyncio.TimeoutError):
                await _wait_cancellable(event.wait(), max(0.001, deadline - time.monotonic()))

async def main():
    schema = 'ucloud_shared_idle_' + uuid4().hex
    store = PostgresDatabase(os.environ['UCLOUD_TEST_POSTGRES_DSN'], 'idle-bench', schema=schema)
    await store.open()
    await store.migrate()
    state = PostgresRelayState(store)
    async with store.transaction('seed') as conn:
        await conn.execute('INSERT INTO relay_quota(deployment_id) VALUES (%s)', (state.deployment,))
    registrations = []
    for i in range(512):
        reg = await state.register_rollout('idle-' + str(i))
        await state.record_worker_heartbeat(rollout_id=reg['rollout_id'],
            registration_token=reg['registration_token'], worker_id='worker')
        registrations.append(reg)
    # No background maintenance/notifications run: isolate the poll work itself.
    async with await AsyncConnection.connect(os.environ['UCLOUD_TEST_POSTGRES_DSN'], autocommit=True) as observer:
        async def lsn():
            return (await (await observer.execute('SELECT pg_current_wal_insert_lsn()')).fetchone())[0]
        try:
            for kind in ('idle', 'ready'):
                for variant in ('baseline', 'candidate', 'candidate', 'baseline'):
                    if kind == 'ready':
                        for reg in registrations:
                            await state.enqueue(rollout_id=reg['rollout_id'], endpoint='/v1/responses',
                                body={'hello': 1}, headers={})
                    samples = []
                    store.observe = samples.append
                    before = await lsn()
                    start, cpu = time.perf_counter(), time.process_time()
                    semaphore = asyncio.Semaphore(128)
                    async def poll(reg):
                        async with semaphore:
                            method = baseline_poll if variant == 'baseline' else PostgresRelayState.poll
                            result = await method(state, rollout_id=reg['rollout_id'],
                                registration_token=reg['registration_token'], worker_id='worker', timeout_seconds=0)
                            assert (len(result) == 1 and result[0].body == api._encoded_body({'hello': 1})) if kind == 'ready' else result == []
                    await asyncio.gather(*(poll(reg) for reg in registrations))
                    elapsed, cpu_seconds = time.perf_counter()-start, time.process_time()-cpu
                    after = await lsn()
                    wal = (await (await observer.execute('SELECT pg_wal_lsn_diff(%s,%s)', (after,before))).fetchone())[0]
                    print(json.dumps(dict(kind=kind, variant=variant, polls=512, concurrency=128,
                        seconds=elapsed, python_cpu_seconds=cpu_seconds, transactions=len(samples),
                        poll_transactions=sum(s.operation=='relay_claim_inference' for s in samples),
                        wal_bytes=int(wal), pool_wait_seconds=sum(s.pool_wait_seconds for s in samples),
                        commit_seconds=sum(s.commit_seconds for s in samples))), flush=True)
                    if kind == 'ready':
                        async with store.transaction('reset_fixture') as conn:
                            await conn.execute('TRUNCATE relay_requests CASCADE')
                            await conn.execute('UPDATE relay_quota SET reserved_bytes=0')
        finally:
            await store.close()
            await observer.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))

asyncio.run(main())
