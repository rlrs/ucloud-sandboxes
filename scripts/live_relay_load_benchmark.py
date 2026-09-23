#!/usr/bin/env python3
"""Repeatable managed-agent load through the real SDK, relay, park and wake path.

No model API is called. Run on a separate driver host when measuring gateway CPU.
A unique prefix fences all cleanup; the run refuses an already occupied fleet.
"""
from __future__ import annotations

import argparse
import asyncio
import aiohttp
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import random
import re
import statistics
import sys
import time
from uuid import uuid4

SDK_SRC = Path(__file__).resolve().parents[1] / "ucloud-sandboxes-sdk" / "src"
if SDK_SRC.is_dir():
    sys.path.insert(0, str(SDK_SRC))

from ucloud_sandboxes_sdk import (  # noqa: E402
    AsyncRelayWorkerClient, AsyncSandboxClient, Image, SandboxSpec, SandboxSecuritySpec, http_tunnel_url,
)
import ucloud_sandboxes_sdk as sdk  # noqa: E402
import ucloud_sandboxes_sdk.client as sdk_client  # noqa: E402

# Incompressible resident memory and rotating dirty pages prevent an empty-agent
# benchmark from hiding checkpoint/restore costs. Each round changes files and
# does a configurable amount of CPU work before issuing a model-shaped request.
AGENT = r'''
import hashlib,json,os,subprocess,sys,time,urllib.request,uuid
from pathlib import Path
config=json.loads(os.environ['BENCH_CONFIG']);root=Path(os.environ.get('BENCH_ROOT','/workspace/relay-bench'));root.mkdir(exist_ok=True)
resident=bytearray(config['resident_mb']*1024*1024)
for offset in range(0,len(resident),1024*1024):
 resident[offset:offset+1024*1024]=os.urandom(min(1024*1024,len(resident)-offset))
nonce=uuid.uuid4().hex
for cycle in range(config['cycles']):
 gate=root/('go-'+str(cycle))
 while not gate.exists():time.sleep(.01)
 dirty=config['dirty_mb']*1024*1024
 for page in range(dirty//4096):
  offset=((cycle*dirty//4096+page)%(len(resident)//4096))*4096
  resident[offset]=(resident[offset]+1)%256
 memory_digest=hashlib.sha256(resident).hexdigest()
 data=bytes(resident[:min(len(resident),1024*1024)])
 for i in range(config['files']):
  (root/('file-'+str(i))).write_bytes(data[:config['file_kib']*1024])
 until=time.process_time()+config['cpu_ms']/1000
 while time.process_time()<until:hashlib.sha256(data).digest()
 body=json.dumps({'cycle':cycle,'nonce':nonce,'digest':memory_digest,'padding':'x'*config['payload_kib']*1024}).encode()
 stable=uuid.uuid4().hex
 for attempt in range(120):
  req=urllib.request.Request(os.environ['RELAY_URL'],data=body,headers={'Content-Type':'application/json','X-UCloud-Relay-Request-Id':stable},method='POST')
  try:
   with urllib.request.urlopen(req,timeout=180) as response:reply=json.load(response)
   break
  except OSError:
   if attempt==119:raise
   time.sleep(min(1,.05*(attempt+1)))
 received=time.monotonic()
 assert reply['cycle']==cycle and reply['nonce']==nonce and reply['digest']==memory_digest
 tool=subprocess.check_output([sys.executable,'-c','print(6*7)'],text=True).strip();assert tool=='42'
 tool_finished=time.monotonic()
 usable={'cycle':cycle,'nonce':nonce,'tool':tool,'pid':os.getpid()}
 tmp=root/'usable.tmp';tmp.write_text(json.dumps(usable));os.replace(tmp,root/('usable-'+str(cycle)+'.json'))
 assert hashlib.sha256(resident).hexdigest()==memory_digest,'resident memory changed during park'
 assert (root/'file-0').read_bytes()==data[:config['file_kib']*1024],'filesystem changed during park'
 verified=time.monotonic()
 result={'cycle':cycle,'nonce':nonce,'digest':memory_digest,'tool':tool,'pid':os.getpid(),'transport_retries':attempt,'verification_seconds':verified-tool_finished,'tool_seconds':tool_finished-received}
 tmp=root/'result.tmp';tmp.write_text(json.dumps(result));os.replace(tmp,root/('result-'+str(cycle)+'.json'))
while True:time.sleep(3600)
'''

PROBE = r'''
import json,sys,time
from pathlib import Path
stage=sys.argv[2] if len(sys.argv)>2 else 'result'
path=Path('/workspace/relay-bench/'+stage+'-'+sys.argv[1]+'.json');deadline=time.monotonic()+120
while not path.exists():
 if time.monotonic()>deadline:raise TimeoutError('managed agent did not acknowledge response')
 time.sleep(.01)
print(path.read_text())
'''


def summary(values):
    ordered = sorted(values)
    def percentile(q):
        return ordered[max(0, math.ceil(len(ordered) * q) - 1)] if ordered else None
    return {"count": len(ordered), "p50": statistics.median(ordered) if ordered else None,
            "p95": percentile(.95), "p99": percentile(.99), "max": max(ordered, default=None)}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-url", required=True)
    parser.add_argument("--relay-url", required=True)
    parser.add_argument("--sandbox-token-file", type=Path, required=True)
    parser.add_argument("--gateway-token-file", type=Path,
                        help="Gateway control credential; required for explicit forced parking")
    parser.add_argument("--relay-worker-token-file", type=Path, required=True)
    parser.add_argument("--image", required=True, help="Immutable registry image containing Python")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sandboxes", type=int, default=256)
    parser.add_argument("--cycles", type=int, default=8)
    parser.add_argument("--startup-mode", choices=("barrier", "rolling"), default="barrier",
                        help="rolling overlaps creation with active model/park/wake traffic")
    parser.add_argument("--resident-mb", type=int, default=128)
    parser.add_argument("--dirty-mb", type=int, default=16)
    parser.add_argument("--files", type=int, default=64)
    parser.add_argument("--file-kib", type=int, default=64)
    parser.add_argument("--payload-kib", type=int, default=32)
    parser.add_argument("--cpu-ms", type=float, default=100)
    parser.add_argument("--model-seconds", type=float, default=10)
    parser.add_argument("--model-jitter", type=float, default=5)
    parser.add_argument("--parking-mode", choices=("natural", "forced"), default="natural",
                        help="natural submits when the model is ready; forced explicitly parks during the model wait")
    parser.add_argument("--cpus", type=float, default=1)
    parser.add_argument("--memory-mb", type=int, default=1024)
    parser.add_argument("--disk-mb", type=int, default=4096)
    parser.add_argument("--create-concurrency", type=int, default=32)
    parser.add_argument("--fleet-pollers", type=int, default=1,
                        help="Concurrent fleet inventory pollers; exercises listing/lifecycle contention")
    parser.add_argument("--warmup-cycles", type=int, default=1)
    parser.add_argument("--deadline-seconds", type=float, default=1800)
    parser.add_argument("--wake-p95-seconds", type=float, default=1)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--start-signal-file", help="optional absent file to create after all_agents_ready; pauses before model traffic")
    args = parser.parse_args(argv)
    if args.parking_mode == 'forced' and args.gateway_token_file is None:
        parser.error('--parking-mode forced requires --gateway-token-file')
    for name in ("sandboxes", "cycles", "resident_mb", "dirty_mb", "files", "file_kib",
                 "payload_kib", "memory_mb", "disk_mb", "create_concurrency", "fleet_pollers"):
        if getattr(args, name) <= 0:
            parser.error(name + " must be positive")
    for name in ("cpu_ms", "model_seconds", "model_jitter", "cpus", "deadline_seconds", "wake_p95_seconds"):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 0 or (name in ("cpus", "deadline_seconds", "wake_p95_seconds") and value == 0):
            parser.error(name + " has an invalid value")
    if args.start_signal_file and args.startup_mode != "barrier":
        parser.error("start_signal_file requires barrier startup")
    if args.dirty_mb > args.resident_mb or args.resident_mb >= args.memory_mb:
        parser.error("require dirty_mb <= resident_mb < memory_mb")
    if args.file_kib > 1024 or not 0 <= args.warmup_cycles < args.cycles:
        parser.error("file_kib must be <=1024 and warmup_cycles must be less than cycles")
    return args


def safe_error(exc):
    # A transport exception can include its authenticated tunnel URL.
    return re.sub(r"/_relay/[^/\s'\"]+", "/_relay/REDACTED", f"{type(exc).__name__}: {exc}")[:1200]


async def retry_control(operation, *, deadline, on_retry, active_delete=False):
    """Retry transport failures on polling/owned cleanup, never replay exec."""
    attempt = 0
    while True:
        try:
            return await asyncio.wait_for(operation(), max(.001, deadline - time.monotonic()))
        except Exception as exc:
            status = getattr(exc, 'status_code', None)
            transient = status in {429, 502, 503, 504} or isinstance(exc, (aiohttp.ClientError, TimeoutError))
            transient |= active_delete and status == 409 and 'active exec/file activity' in str(exc)
            if not transient or time.monotonic() >= deadline:
                raise
            attempt += 1
            on_retry(attempt, status)
            await asyncio.sleep(min(2, .1 * attempt, max(0, deadline - time.monotonic())))


async def with_lease_renewal(awaitable, relay, request, *, interval=40, lease_seconds=120):
    """Keep the model worker's claim alive while the test deliberately waits."""
    async def renew():
        current = request
        while True:
            await asyncio.sleep(interval)
            current = await relay.renew_request(current, lease_seconds=lease_seconds)
    operation = asyncio.ensure_future(awaitable)
    keeper = asyncio.create_task(renew())
    try:
        done, _ = await asyncio.wait((operation, keeper), return_when=asyncio.FIRST_COMPLETED)
        if keeper in done:
            keeper.result()  # A lost lease must fail the scenario before commit.
        return await operation
    finally:
        operation.cancel()
        keeper.cancel()
        await asyncio.gather(operation, keeper, return_exceptions=True)


async def response_window(*, claimed_at, model_seconds, mode, sandbox_id, inventory, inventory_task, park=None):
    # Synthetic readiness is scheduled independently of event-loop lag or
    # parking. Those waits must count in the product latency measurement.
    ready_at = claimed_at + model_seconds
    parking = asyncio.create_task(park()) if mode == 'forced' and park is not None else None
    def observed():
        return (inventory.get(sandbox_id, (None, 0))[0] == 'parked'
                and inventory[sandbox_id][1] >= claimed_at)
    try:
        await asyncio.sleep(max(0, ready_at - time.monotonic()))
        if parking is not None:
            await parking
        if mode == 'forced':
            while not observed():
                if inventory_task.done():
                    inventory_task.result()
                if time.monotonic() - claimed_at > 180:
                    raise TimeoutError('sandbox did not reach parked state')
                await asyncio.sleep(.1)
        return ready_at, time.monotonic() - claimed_at if observed() else None
    finally:
        if parking is not None:
            parking.cancel()
            await asyncio.gather(parking, return_exceptions=True)


def meets_wake_slo(result, target_seconds):
    if not result['correct']:
        return False
    measured = result['response_ready_to_usable_exec_seconds']
    if not measured['count'] or measured['p95'] >= target_seconds:
        return False
    phases = result['phase_latency_seconds']
    if result['configuration']['startup_mode'] == 'rolling' and not phases['during_provisioning']['count']:
        return False
    # Do not hide cold/overlapping delays by excluding them as warmup cycles.
    return all(not phase['count'] or phase['p95'] < target_seconds for phase in phases.values())


async def run(args):
    if args.start_signal_file and Path(args.start_signal_file).exists():
        raise ValueError("start signal file already exists; use a fresh path")
    prefix = "relay-load-" + uuid4().hex[:12]
    token = args.sandbox_token_file.read_text().strip()
    lifecycle_token = (args.gateway_token_file.read_text().strip()
                       if args.parking_mode == 'forced' else token)
    worker_token = args.relay_worker_token_file.read_text().strip()
    config = {k: v for k, v in vars(args).items() if not k.endswith("token_file") and k != "output"}
    result = {"run_id": prefix, "started_at": datetime.now(timezone.utc).isoformat(),
              "configuration": config, "cycles": [], "errors": [], "cleanup_errors": [],
              "health": [], "fleet_polls": [], "placements": {}, "control_retries": [],
              "driver_python": sys.version.split()[0],
              "sdk_version": sdk.__version__,
              "sdk_client_sha256": hashlib.sha256(Path(sdk_client.__file__).read_bytes()).hexdigest(),
              "latency_definition": "response ready through guest tool and external exec confirmation; full memory/file integrity is a separate mandatory gate",
              "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    last_persist = 0.0
    def persist():
        temp = args.output.with_suffix(args.output.suffix + ".tmp")
        temp.write_text(json.dumps(result, indent=2) + "\n")
        temp.replace(args.output)
    def event(name, **fields):
        nonlocal last_persist
        print(json.dumps({"event": name, "run_id": prefix, "at": datetime.now(timezone.utc).isoformat(), **fields}), flush=True)
        # Rewriting all earlier samples after every event makes the driver do
        # quadratic JSON work on its event loop and inflates loaded latency.
        # Keep the event log complete and checkpoint the report once a second.
        now = time.monotonic()
        if now - last_persist >= 1 or name == 'benchmark_finished':
            persist()
            last_persist = now
    create_slots = asyncio.Semaphore(args.create_concurrency)
    handles = {}
    registrations = set()
    clients = []
    jobs = {}
    tasks = []
    ready = asyncio.Event()
    inventory = {}
    async with (
        aiohttp.ClientSession(
            headers={'Authorization': 'Bearer ' + lifecycle_token},
            timeout=aiohttp.ClientTimeout(total=180),
        ) as lifecycle,
        AsyncSandboxClient(args.gateway_url, api_token=token, timeout_seconds=180) as operator,
        AsyncRelayWorkerClient(args.relay_url, worker_token=worker_token,
                               timeout_seconds=180, forward_timeout_seconds=180) as relay,
    ):
        if await operator.list_sandboxes():
            raise RuntimeError("deployment is occupied; benchmark requires an idle fleet")
        result['deployed_health'] = await operator.health()
        async def health_probe():
            while True:
                started = time.monotonic()
                try:
                    await asyncio.wait_for(operator.health(), 5)
                    result['health'].append({'seconds': time.monotonic() - started, 'ok': True})
                except Exception as exc:
                    result['health'].append({'seconds': time.monotonic() - started, 'ok': False, 'error': safe_error(exc)})
                await asyncio.sleep(2)
        monitor = asyncio.create_task(health_probe())
        async def inventory_probe():
            while True:
                started = time.monotonic()
                try:
                    records = await operator.list_sandboxes()
                    result['fleet_polls'].append({'seconds': time.monotonic() - started, 'ok': True})
                except Exception as exc:
                    result['fleet_polls'].append({'seconds': time.monotonic() - started, 'ok': False, 'error': safe_error(exc)})
                    await asyncio.sleep(1)
                    continue
                observed = time.monotonic()
                for record in records:
                    sid = record.get('spec', {}).get('id')
                    state = record.get('state') or record.get('status', {}).get('state')
                    inventory[sid] = (state, observed)
                    if sid and sid.startswith(prefix + '-'):
                        result['placements'][sid] = record.get('node', {}).get('job_id')
                await asyncio.sleep(1)
        inventory_tasks = [asyncio.create_task(inventory_probe()) for _ in range(args.fleet_pollers)]
        async def scenario(index):
            sid = f"{prefix}-{index:04d}"
            rng = random.Random(args.seed + index)
            client = AsyncSandboxClient(args.gateway_url, api_token=token, timeout_seconds=180)
            clients.append(client)
            stage, cycle = 'create', None
            try:
                async with create_slots:
                    # Track IDs before the call so timed-out successful creates
                    # are also included in exact-ID cleanup.
                    handles[sid] = None
                    handle = await client.create_sandbox(SandboxSpec(
                        id=sid, image=Image.from_registry(args.image), cpus=args.cpus,
                        memory_mb=args.memory_mb, disk_mb=args.disk_mb, parkable=True,
                        managed_process=True, ttl_seconds=math.ceil(args.deadline_seconds + 300),
                        security=SandboxSecuritySpec(user='0:0'),
                    ), request_timeout_seconds=args.deadline_seconds)
                    handles[sid] = handle
                    event('sandbox_created', sandbox_id=sid)
                    registration = await relay.register_agent_rollout(sid, handle, metadata={'benchmark': prefix})
                    registrations.add(sid)
                    event('relay_registered', sandbox_id=sid)
                    tunnel = http_tunnel_url(args.relay_url, sid, 'chat/completions',
                                            registration_token=registration['rollout']['registration_token'])
                    await handle.upload_file('/workspace/relay-load.py', AGENT)
                    event('agent_uploaded', sandbox_id=sid)
                    job = await handle.start_agent(['python', '/workspace/relay-load.py'],
                                                  env={'BENCH_CONFIG': json.dumps(config), 'RELAY_URL': tunnel})
                    jobs[sid] = job
                event('agent_started', sandbox_id=sid, job_id=job.job_id)
                if len(jobs) == args.sandboxes:
                    event('all_agents_ready')
                    if args.start_signal_file:
                        while not Path(args.start_signal_file).exists():
                            await asyncio.sleep(.2)
                    ready.set()
                if args.startup_mode == "barrier":
                    await ready.wait()
                identity = None
                for cycle in range(args.cycles):
                    stage = 'cycle_gate_upload'
                    await handle.upload_file('/workspace/relay-bench/go-' + str(cycle), b'go')
                    deadline = time.monotonic() + 180
                    next_job_check = time.monotonic() + 5
                    while True:
                        stage = 'relay_claim'
                        def poll_retry(attempt, status):
                            result['control_retries'].append(dict(operation='poll', sandbox_id=sid, cycle=cycle, attempt=attempt, status=status))
                        # A lost poll response may already own a lease. Its
                        # expiry must fit inside this polling recovery window.
                        polled = await retry_control(
                            lambda: relay.poll(sid, worker_id=prefix, timeout_seconds=1, lease_seconds=120, limit=1),
                            deadline=deadline, on_retry=poll_retry,
                        )
                        if polled.requests:
                            request = polled.requests[0]
                            break
                        if time.monotonic() >= next_job_check:
                            stage = 'agent_status'
                            record = await job.refresh()
                            if record.terminal:
                                logs = await job.logs('stderr', limit=4096)
                                raise RuntimeError('managed agent exited: ' + logs.data.decode(errors='replace'))
                            next_job_check = time.monotonic() + 5
                        if time.monotonic() > deadline:
                            raise TimeoutError('agent did not issue its model request')
                    payload = json.loads(request.body_bytes)
                    claimed_at = time.monotonic()
                    if payload['cycle'] != cycle or (identity is not None and payload['nonce'] != identity):
                        raise RuntimeError('cycle or process identity mismatch')
                    identity = payload['nonce']
                    delay = args.model_seconds + rng.uniform(0, args.model_jitter)
                    during_provisioning = len(jobs) < args.sandboxes
                    async def park_for_qualification():
                        # An explicit user park bypasses adaptive warm retention.
                        # It starts during the model wait; an overrun still counts
                        # from the independently scheduled response-ready time.
                        async def attempt():
                            async with lifecycle.post(
                                args.gateway_url.rstrip('/') + '/v1/sandboxes/' + sid + '/park',
                                json={'operation_id': f'benchmark-park:{prefix}:{cycle}'},
                            ) as response:
                                body = await response.text()
                                if response.status != 200:
                                    error = RuntimeError(f'explicit park HTTP {response.status}: {body[:500]}')
                                    error.status_code = response.status
                                    raise error
                        await retry_control(
                            attempt, deadline=time.monotonic() + 180,
                            on_retry=lambda a, s: result['control_retries'].append(dict(
                                operation='park', sandbox_id=sid, cycle=cycle, attempt=a, status=s,
                            )),
                        )
                    stage = 'model_wait_and_park'
                    model_ready, parked_after = await with_lease_renewal(response_window(
                        claimed_at=claimed_at, model_seconds=delay, mode=args.parking_mode,
                        sandbox_id=sid, inventory=inventory, inventory_task=inventory_tasks[0],
                        park=park_for_qualification,
                    ), relay, request)
                    # Start before SDK connection admission: SDK queuing, relay
                    # locks, scheduling, storage, restore and retries all count.
                    started = time.monotonic()
                    started_unix = time.time()
                    stage = 'commit_and_wake'
                    await relay.commit_response_bytes_to(request, request.body_bytes,
                                                         headers={'Content-Type': 'application/json'},
                                                         attempts=120, retry_delay_seconds=.1)
                    committed = time.monotonic()
                    # Same SDK start/wait path as handle.exec(), split only to
                    # distinguish dispatch delay from guest execution/event reads.
                    stage = 'usable_exec_start'
                    probe_handle = await handle.start_exec(['python', '-c', PROBE, str(cycle), 'usable'])
                    probe_dispatched = time.monotonic()
                    stage = 'usable_exec_wait'
                    probe = await probe_handle.wait(timeout_seconds=150)
                    finished = time.monotonic()
                    if not probe.success:
                        raise RuntimeError('usable-exec probe failed: ' + probe.stderr[:500])
                    usable = json.loads(probe.stdout)
                    if usable['nonce'] != identity or usable['cycle'] != cycle or usable['tool'] != '42':
                        raise RuntimeError('restored agent did not execute its first tool')
                    # Integrity remains a mandatory gate, but scanning the entire
                    # working set is application work after the first usable tool.
                    stage = 'integrity_exec'
                    probe = await handle.exec(['python', '-c', PROBE, str(cycle), 'result'], timeout_seconds=150)
                    verified = time.monotonic()
                    if not probe.success:
                        raise RuntimeError('integrity probe failed: ' + probe.stderr[:500])
                    ack = json.loads(probe.stdout)
                    if ack['nonce'] != identity or ack['cycle'] != cycle or ack['digest'] != payload['digest'] or ack['tool'] != '42':
                        raise RuntimeError('restored agent failed integrity check')
                    result['cycles'].append({'sandbox_id': sid, 'cycle': cycle,
                                             'request_id': request.request_id, 'model_seconds': delay,
                                             'response_ready_unix': started_unix + model_ready - started,
                                             'wake_completed_unix': started_unix + committed - started,
                                             'exec_completed_unix': started_unix + finished - started,
                                             'commit_and_wake_seconds': committed - started,
                                             'usable_exec_seconds': finished - started,
                                             'full_integrity_seconds': verified - model_ready,
                                             'response_ready_to_usable_exec_seconds': finished - model_ready,
                                             'response_ready_to_submit_seconds': started - model_ready,
                                             'response_ready_to_wake_seconds': committed - model_ready,
                                             'post_wake_exec_seconds': finished - committed,
                                             'post_wake_exec_start_seconds': probe_dispatched - committed,
                                             'post_wake_exec_wait_seconds': finished - probe_dispatched,
                                             'guest_transport_retries': ack['transport_retries'],
                                             'guest_verification_seconds': ack['verification_seconds'],
                                             'guest_tool_seconds': ack['tool_seconds'],
                                             'pid': ack['pid'], 'delivery_count': request.delivery_count})
                    result['cycles'][-1]['park_observed_after_seconds'] = parked_after
                    result['cycles'][-1]['during_provisioning'] = during_provisioning
                    result['cycles'][-1]['agents_started'] = len(jobs)
                    event('cycle_completed', completed=len(result['cycles']), sandbox_id=sid, cycle=cycle,
                          wake_seconds=committed-started, usable_seconds=finished-started)
            except Exception as exc:
                result['errors'].append({'sandbox_id': sid, 'stage': stage, 'cycle': cycle, 'error': safe_error(exc)})
                event('scenario_failed', sandbox_id=sid, stage=stage, cycle=cycle, error=safe_error(exc))
                raise
        try:
            await operator.prepare_capacity(count=args.sandboxes, cpus=args.cpus, memory_mb=args.memory_mb,
                                            disk_mb=args.disk_mb, image=Image.from_registry(args.image),
                                            parkable=True, ttl_seconds=math.ceil(args.deadline_seconds+300), prepare_id=prefix)
            event('capacity_requested', count=args.sandboxes)
            tasks = [asyncio.create_task(scenario(i)) for i in range(args.sandboxes)]
            await asyncio.wait_for(asyncio.gather(*tasks), args.deadline_seconds)
        except Exception as exc:
            result['failure'] = safe_error(exc)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            monitor.cancel()
            for task in inventory_tasks:
                task.cancel()
            await asyncio.gather(monitor, *inventory_tasks, return_exceptions=True)
            cleanup_slots = asyncio.Semaphore(16)
            async def cleanup(sid):
                async with cleanup_slots:
                    try:
                        await retry_control(
                            lambda: operator.delete_sandbox(sid),
                            deadline=time.monotonic() + 180, active_delete=True,
                            on_retry=lambda attempt, status: result['control_retries'].append(
                                dict(operation='delete', sandbox_id=sid, attempt=attempt, status=status)),
                        )
                    except Exception as exc:
                        result['cleanup_errors'].append({'sandbox_id': sid, 'error': safe_error(exc)})
                    if sid in registrations:
                        try:
                            await asyncio.wait_for(relay.unregister_rollout(sid), 30)
                        except Exception as exc:
                            result['cleanup_errors'].append({'rollout_id': sid, 'error': safe_error(exc)})
            await asyncio.gather(*(cleanup(sid) for sid in handles))
            try:
                await operator.delete_prepared_capacity(prefix)
            except Exception as exc:
                result['cleanup_errors'].append({'reservation': prefix, 'error': safe_error(exc)})
            for client in clients:
                await client.close()
        measured = [row for row in result['cycles'] if row['cycle'] >= args.warmup_cycles]
        result['commit_and_wake_seconds'] = summary([r['commit_and_wake_seconds'] for r in measured])
        result['usable_exec_seconds'] = summary([r['usable_exec_seconds'] for r in measured])
        result['response_ready_to_usable_exec_seconds'] = summary([r['response_ready_to_usable_exec_seconds'] for r in measured])
        for field in ('response_ready_to_wake_seconds', 'post_wake_exec_seconds', 'post_wake_exec_start_seconds', 'post_wake_exec_wait_seconds', 'full_integrity_seconds',
                      'guest_verification_seconds', 'guest_tool_seconds', 'guest_transport_retries'):
            result[field] = summary([r[field] for r in measured])
        result['measured_park_observed_cycles'] = sum(r['park_observed_after_seconds'] is not None for r in measured)
        result['phase_latency_seconds'] = {
            name: summary([r['response_ready_to_usable_exec_seconds'] for r in result['cycles']
                           if r['during_provisioning'] == provisioning])
            for name, provisioning in [('during_provisioning', True), ('after_provisioning', False)]
        }
        result['completed_cycles'] = len(result['cycles'])
        result['correct'] = (not result.get('failure') and not result['errors'] and not result['cleanup_errors']
                             and len(result['cycles']) == args.sandboxes * args.cycles
                             and all(h['ok'] for h in result['health'])
                             and all(p['ok'] for p in result['fleet_polls']))
        result['slo_passed'] = meets_wake_slo(result, args.wake_p95_seconds)
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        event('benchmark_finished', correct=result['correct'], slo_passed=result['slo_passed'],
              commit_and_wake_seconds=result['commit_and_wake_seconds'], usable_exec_seconds=result['usable_exec_seconds'],
              response_ready_to_usable_exec_seconds=result['response_ready_to_usable_exec_seconds'])
    return result


def main():
    args = parse_args()
    # Scope descriptor headroom to this load-generator process, not the host.
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(max(soft, 8192), hard), hard))
    try:
        result = asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    return 0 if result['slo_passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
