"""Compare durable relay dispatch against an explicit prior source module.

Use UCLOUD_TEST_POSTGRES_DSN for a disposable local PostgreSQL instance only.
Each case owns and removes a unique schema. Driver CPU is included in timings;
this is not an HTTP, real-sandbox, or whole-gateway capacity qualification.
"""
import argparse
import asyncio
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
from uuid import uuid4

import psycopg
from psycopg import sql
from ucloud_sandboxes import model_relay as api
from ucloud_sandboxes.shared_control.database import PostgresDatabase
from ucloud_sandboxes.shared_control.relay import PostgresRelayState

DSN = os.environ['UCLOUD_TEST_POSTGRES_DSN']


async def run(label, cls):
    schema = 'ucloud_shared_scan_' + uuid4().hex
    operations, claims = Counter(), Counter()
    store = PostgresDatabase(DSN, 'scan-benchmark', schema=schema, max_connections=16,
                             observe=lambda sample: operations.update([sample.operation]))
    state = None
    tasks = []
    try:
        await store.open()
        await store.migrate()
        async def park(request):
            raise api.RelayLifecycleDeferred(30, transport_epoch='local')
        async def wake(request):
            return 'local'
        state = cls(store, accepted_notifier=park, result_notifier=wake)
        await state.open()
        registrations = []
        for i in range(64):
            registrations.append(await state.register_rollout('agent-' + str(i), {
                'sandbox_id': 'sandbox-' + str(i), 'sandbox_generation': 1,
                api.AGENT_LIFECYCLE_METADATA_KEY: api.MANAGED_AGENT_LIFECYCLE,
            }))
        observer = await state.register_rollout('observer')
        original_claim = state._claim_lifecycle
        async def claim(*args, **kwargs):
            action = kwargs.get('action', 'all')
            claims[action + '_calls'] += 1
            rows = await original_claim(*args, **kwargs)
            claims[action + '_rows'] += len(rows)
            claims[action + '_empty'] += not bool(rows)
            return rows
        state._claim_lifecycle = claim
        await asyncio.sleep(.3)
        operations.clear()
        claims.clear()
        latencies = []
        async def cycle(i):
            registration = registrations[i % len(registrations)]
            request = await state.enqueue(rollout_id=registration['rollout_id'],
                endpoint='/responses', body=b'x' * 32768, headers={})
            waiting = asyncio.create_task(state.wait_for_response(request, timeout_seconds=10))
            (leased,) = await state.poll(rollout_id=registration['rollout_id'],
                registration_token=registration['registration_token'], timeout_seconds=1,
                lease_seconds=120, worker_id='benchmark')
            await asyncio.sleep(1)
            started = time.monotonic()
            await state.respond(request_id=leased.request_id,
                registration_token=registration['registration_token'], lease_id=leased.lease_id,
                response=api.RelayWorkerResponse(200, b'y' * 32768), defer_delivery=True)
            assert (await waiting).body == b'y' * 32768
            latencies.append(time.monotonic() - started)
            receipt = await state.enqueue(rollout_id='observer', endpoint='/continued',
                body=b'receipt', headers={})
            (observed,) = await state.poll(rollout_id='observer',
                registration_token=observer['registration_token'], timeout_seconds=1,
                lease_seconds=120)
            await state.respond(request_id=observed.request_id,
                registration_token=observer['registration_token'], lease_id=observed.lease_id,
                response=api.RelayWorkerResponse(200, b'ok'), defer_delivery=True)
            assert (await state.wait_for_response(receipt, timeout_seconds=10)).body == b'ok'
        started, cpu = time.monotonic(), time.process_time()
        for i in range(200):
            await asyncio.sleep(max(0, started + i / 25 - time.monotonic()))
            tasks.append(asyncio.create_task(cycle(i)))
        await asyncio.gather(*tasks)
        await asyncio.sleep(.1)
        result = dict(label=label, cycles=200, requested_cycles_per_second=25,
            elapsed_seconds=time.monotonic()-started,
            python_process_cpu_seconds=time.process_time()-cpu,
            operations=dict(operations), claims=dict(claims),
            response_to_delivery_p95_seconds=sorted(latencies)[int(len(latencies)*.95)],
            delivered_responses=len(latencies), errors=0)
        print(json.dumps(result), flush=True)
        return result
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if state is not None:
            await state.aclose()
        else:
            await store.close()
        async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
            await conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location(
        'ucloud_sandboxes.shared_control.relay_baseline', args.baseline)
    baseline = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = baseline
    spec.loader.exec_module(baseline)
    results = []
    for label, cls in [('before-1', baseline.PostgresRelayState), ('after-1', PostgresRelayState),
                       ('after-2', PostgresRelayState), ('before-2', baseline.PostgresRelayState)]:
        results.append(await run(label, cls))
    report = dict(scope='Disposable local PostgreSQL17, direct state API; includes driver CPU; no real sandbox or HTTP latency claim.',
        cases=results, source_sha256={name: hashlib.sha256(Path(path).read_bytes()).hexdigest() for name,path in {
            'before': args.baseline,
            'after': 'ucloud_sandboxes/shared_control/relay.py'}.items()})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')

if __name__ == '__main__':
    asyncio.run(main())
