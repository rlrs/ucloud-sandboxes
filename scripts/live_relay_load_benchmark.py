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
import hashlib,json,os,sqlite3,subprocess,sys,time,urllib.request,uuid
from pathlib import Path
config=json.loads(os.environ['BENCH_CONFIG']);root=Path(os.environ.get('BENCH_ROOT','/workspace/relay-bench'));root.mkdir(exist_ok=True)
resident=bytearray(config['resident_mb']*1024*1024)
for offset in range(0,len(resident),1024*1024):
 resident[offset:offset+1024*1024]=os.urandom(min(1024*1024,len(resident)-offset))
nonce=uuid.uuid4().hex
transactions=config.get('sqlite_transactions',0)
def transaction_bytes(cycle,transaction):
 return hashlib.shake_256(f'{nonce}:{cycle}:{transaction}'.encode()).digest(config['sqlite_payload_bytes'])
if transactions:
 database=root/'repository.sqlite'
 writer=sqlite3.connect(database)
 assert writer.execute('PRAGMA journal_mode=WAL').fetchone()[0]=='wal'
 writer.execute('PRAGMA synchronous=FULL');writer.execute('PRAGMA wal_autocheckpoint=0')
 writer.execute('CREATE TABLE changes(cycle INTEGER, transaction_id INTEGER, payload BLOB NOT NULL, PRIMARY KEY(cycle,transaction_id))');writer.commit()
 writer.execute('PRAGMA wal_checkpoint(TRUNCATE)')
 # Keep an old reader snapshot and both connections live across every wait.
 # This pins WAL history instead of silently checkpointing it away on close.
 reader=sqlite3.connect(database);reader.execute('BEGIN')
 assert reader.execute('SELECT count(*) FROM changes').fetchone()[0]==0
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
 if transactions:
  for transaction in range(transactions):
   with writer:
    writer.execute('INSERT INTO changes VALUES(?,?,?)',(cycle,transaction,transaction_bytes(cycle,transaction)))
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
 sqlite_rows=0
 if transactions:
  sqlite_rows=writer.execute('SELECT count(*) FROM changes').fetchone()[0]
  assert sqlite_rows==(cycle+1)*transactions,'committed SQLite rows lost during park'
  assert reader.execute('SELECT count(*) FROM changes').fetchone()[0]==0,'SQLite reader snapshot changed during park'
 tool_finished=time.monotonic()
 usable={'cycle':cycle,'nonce':nonce,'tool':tool,'pid':os.getpid(),'digest':memory_digest}
 tmp=root/'usable.tmp';tmp.write_text(json.dumps(usable));os.replace(tmp,root/('usable-'+str(cycle)+'.json'))
 # This separate, unbound observation tunnel has no lifecycle authority. The
 # driver must see this continuation before any file/exec probe can cause a wake.
 receipt_id=uuid.uuid4().hex
 for receipt_attempt in range(120):
  receipt=urllib.request.Request(os.environ['OBSERVER_URL'],data=json.dumps(usable).encode(),headers={'Content-Type':'application/json','X-UCloud-Relay-Request-Id':receipt_id},method='POST')
  try:
   with urllib.request.urlopen(receipt,timeout=180) as response:json.load(response)
   break
  except OSError:
   if receipt_attempt==119:raise
   time.sleep(min(1,.05*(receipt_attempt+1)))
 verification_started=time.monotonic()
 assert hashlib.sha256(resident).hexdigest()==memory_digest,'resident memory changed during park'
 assert (root/'file-0').read_bytes()==data[:config['file_kib']*1024],'filesystem changed during park'
 sqlite_digest=None
 if transactions:
  assert writer.execute('PRAGMA integrity_check').fetchall()==[('ok',)],'SQLite integrity check failed'
  digest=hashlib.sha256()
  # A fresh connection must recover the same committed WAL as the live one.
  verifier=sqlite3.connect(database)
  try:
   rows=0
   for saved_cycle,transaction,value in verifier.execute('SELECT cycle,transaction_id,payload FROM changes ORDER BY cycle,transaction_id'):
    assert value==transaction_bytes(saved_cycle,transaction),'SQLite payload changed during park'
    digest.update(value);rows+=1
   assert rows==sqlite_rows,'SQLite reopen lost committed rows'
  finally:verifier.close()
  sqlite_digest=digest.hexdigest()
 verified=time.monotonic()
 result={'cycle':cycle,'nonce':nonce,'digest':memory_digest,'tool':tool,'pid':os.getpid(),'transport_retries':attempt,'verification_seconds':verified-verification_started,'tool_seconds':tool_finished-received,'sqlite_rows':sqlite_rows,'sqlite_digest':sqlite_digest}
 tmp=root/'result.tmp';tmp.write_text(json.dumps(result));os.replace(tmp,root/('result-'+str(cycle)+'.json'))
# The primary finishes after its final integrity proof. Retaining completed
# heaps forever would prevent an overcommitted fleet from finishing later jobs.
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


def uploaded_tool_probe(kib):
    """A file-backed tool with a checksum gate, including its upload in wake time."""
    source = ("import hashlib,sys\n"
              "assert hashlib.sha256(open(__file__,'rb').read()).hexdigest()==sys.argv[3], 'tool upload corrupted'\n"
              + PROBE + "\n#").encode()
    return source + b'x' * max(0, kib * 1024 - len(source) - 1) + b'\n'


def summary(values):
    ordered = sorted(values)
    def percentile(q):
        return ordered[max(0, math.ceil(len(ordered) * q) - 1)] if ordered else None
    return {"count": len(ordered), "p50": statistics.median(ordered) if ordered else None,
            "p95": percentile(.95), "p99": percentile(.99), "max": max(ordered, default=None)}


def resource_sample(nodes, now):
    """Keep fresh resource evidence, never credentials, URLs or workload specs."""
    rows = []
    for node in nodes:
        raw = node.get('runtime_metrics')
        if not isinstance(raw, dict):
            continue
        try:
            collected = datetime.fromisoformat(raw['collected_at'])
            age = (now - collected).total_seconds()
        except (KeyError, ValueError, TypeError):
            continue
        if not 0 <= age <= 30:
            continue
        rows.append({
            'job_id': node.get('job_id'), 'node_epoch': node.get('node_epoch'),
            'agent_version': node.get('agent_version'),
            'total_resources': node.get('total_resources'),
            'active_sandboxes': node.get('active_sandboxes'),
            'runtime_metrics': raw,
        })
    return {'at': now.isoformat(), 'nodes': rows}


class FleetHealthQualification:
    """Qualify only workers used by this run from the gateway heartbeat cache."""

    def __init__(self, *, enabled):
        self.enabled = enabled
        self.placements = {}
        self.first_seen = {}
        self.checks = []
        self.failures = []

    def placed(self, sandbox_id, job_id, *, monotonic_now):
        if not job_id:
            return
        job_id = str(job_id)
        self.placements[sandbox_id] = job_id
        self.first_seen.setdefault(job_id, monotonic_now)

    def observe(self, nodes, *, now, monotonic_now, probe_error=None):
        if not self.enabled:
            return
        used = set(self.placements.values())
        required = sorted(job for job in used
                          if monotonic_now - self.first_seen[job] >= 30)
        check = {'at': now.isoformat(), 'required_jobs': required,
                 'grace_jobs': sorted(used - set(required)), 'healthy_jobs': [],
                 'failures': []}
        by_job = {str(node.get('job_id')): node for node in (nodes or [])}
        for job in required:
            node = by_job.get(job)
            if probe_error is not None:
                kind = 'resource_probe_failed'
            elif node is None:
                kind = 'missing_node'
            else:
                kind = None
                metrics = node.get('runtime_metrics')
                for label, timestamp in (
                    ('heartbeat', node.get('updated_at')),
                    ('resource_metrics', metrics.get('collected_at') if isinstance(metrics, dict) else None),
                ):
                    try:
                        age = (now - datetime.fromisoformat(timestamp)).total_seconds()
                    except (TypeError, ValueError):
                        kind = 'missing_or_invalid_' + label
                        break
                    if not 0 <= age <= 30:
                        kind = 'stale_' + label
                        break
            if kind:
                failure = {'at': now.isoformat(), 'job_id': job, 'kind': kind}
                check['failures'].append(failure)
                self.failures.append(failure)
            else:
                check['healthy_jobs'].append(job)
        self.checks.append(check)

    def summary(self):
        covered = {job for check in self.checks for job in check['healthy_jobs']}
        used = set(self.placements.values())
        passed = (not self.failures and bool(used) and used <= covered) if self.enabled else None
        return {'enabled': self.enabled, 'passed': passed,
                'status': ('unknown' if not self.enabled else
                           'failed' if self.failures else
                           'passed' if passed else 'insufficient_observation'),
                'used_jobs': sorted(used), 'observed_healthy_jobs': sorted(covered),
                'failure_count': len(self.failures),
                'placement_grace_seconds': 30, 'freshness_seconds': 30}


def resource_summary(samples):
    """Integrate sampled host costs; gaps/resets are unknown, not free work.

    This is diagnostic evidence, not exact per-sandbox billing. Device rates
    cover guest-visible leaf devices. Host memory includes other worker duties.
    Repeated heartbeat observations never extend an old measurement's coverage.
    """
    previous = {}
    intervals = []
    for sample in samples:
        for node in sample['nodes']:
            metric = node['runtime_metrics']
            identity = (node['job_id'], node['node_epoch'])
            old = previous.get(identity)
            at = datetime.fromisoformat(metric['collected_at']).timestamp()
            if old is not None and at <= old[0]:
                continue
            previous[identity] = (at, metric)
            if old is None or not 0 < at - old[0] <= 30:
                continue
            seconds = at - old[0]
            cpu = metric.get('cpu_vcpu')
            memory = metric.get('memory_working_set_mb')
            row = {'worker_seconds': seconds}
            if isinstance(cpu, (int, float)) and cpu >= 0:
                row['cpu_seconds'] = seconds * cpu
            if isinstance(memory, (int, float)) and memory >= 0:
                row['host_memory_mib_seconds'] = seconds * memory
            # Counter deltas retain short CPU bursts between heartbeats. The
            # point-sampled estimate above remains separately named for old nodes.
            evidence = metric.get('resource_evidence') or {}
            before_evidence = old[1].get('resource_evidence') or {}
            for field in ('host_cpu_usage_usec', 'host_cpu_steal_usec'):
                current, before = evidence.get(field), before_evidence.get(field)
                if type(current) is int and type(before) is int and current >= before >= 0:
                    row[field] = (current - before) / 1_000_000
            devices = (metric.get('resource_evidence') or {}).get('devices') or []
            old_devices = (old[1].get('resource_evidence') or {}).get('devices') or []
            old_by_id = {device['identity']: device for device in old_devices}
            # Cumulative counters give exact interval bytes. Do not extrapolate
            # a one-second rate across the five-second heartbeat sampling gap.
            if devices and {device['identity'] for device in devices} == set(old_by_id):
                for field in ('read_bytes', 'write_bytes'):
                    deltas = []
                    for device in devices:
                        current = device.get(field)
                        before = old_by_id[device['identity']].get(field)
                        if type(current) is not int or type(before) is not int or current < before:
                            break
                        deltas.append(current - before)
                    else:
                        row['device_' + field] = sum(deltas)
            intervals.append(row)
    observed = sum(row['worker_seconds'] for row in intervals)
    return {
        'sample_count': len(samples), 'observed_worker_seconds': observed,
        'sampled_cpu_seconds': sum(row['cpu_seconds'] for row in intervals
                                   if 'cpu_seconds' in row),
        'cpu_coverage_worker_seconds': sum(row['worker_seconds'] for row in intervals
                                           if 'cpu_seconds' in row),
        **{field.removesuffix('_usec') + '_seconds': sum(row[field] for row in intervals if field in row)
           for field in ('host_cpu_usage_usec', 'host_cpu_steal_usec')},
        **{field.removesuffix('_usec') + '_coverage_worker_seconds': sum(row['worker_seconds'] for row in intervals if field in row)
           for field in ('host_cpu_usage_usec', 'host_cpu_steal_usec')},
        'sampled_host_memory_mib_seconds': sum(row['host_memory_mib_seconds'] for row in intervals
                                               if 'host_memory_mib_seconds' in row),
        'memory_coverage_worker_seconds': sum(row['worker_seconds'] for row in intervals
                                              if 'host_memory_mib_seconds' in row),
        **{field: sum(row[field] for row in intervals if field in row)
           for field in ('device_read_bytes', 'device_write_bytes')},
        **{field + '_coverage_worker_seconds': sum(row['worker_seconds'] for row in intervals if field in row)
           for field in ('device_read_bytes', 'device_write_bytes')},
        'scope': 'sampled whole worker costs; excludes missing/stale intervals; not per-sandbox attribution',
    }


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
    parser.add_argument("--sqlite-transactions", type=int, default=0,
                        help="FULL-synchronous SQLite WAL commits per cycle; 0 disables the repository profile")
    parser.add_argument("--sqlite-payload-bytes", type=int, default=4096,
                        help="Deterministic incompressible bytes committed in each SQLite transaction")
    parser.add_argument("--tool-upload-kib", type=int, default=64,
                        help="Upload and checksum a tool script after each wake; 0 selects the legacy inline probe")
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
    parser.add_argument("--sandbox-request-timeout-seconds", type=float, default=180,
                        help="Per-sandbox SDK request budget; the overall deadline still bounds the run")
    parser.add_argument("--continuation-timeout-seconds", type=float, default=180,
                        help="Guest continuation observation budget; increasing it qualifies eventual correctness, not the original latency deadline")
    parser.add_argument("--useful-action-p95-seconds", type=float, default=1)
    parser.add_argument("--continuation-p95-seconds", type=float, default=.8)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--start-signal-file", help="optional absent file to create after all_agents_ready; pauses before model traffic")
    args = parser.parse_args(argv)
    if args.parking_mode == 'forced' and args.gateway_token_file is None:
        parser.error('--parking-mode forced requires --gateway-token-file')
    for name in ("sandboxes", "cycles", "resident_mb", "dirty_mb", "files", "file_kib",
                 "payload_kib", "memory_mb", "disk_mb", "create_concurrency", "fleet_pollers"):
        if getattr(args, name) <= 0:
            parser.error(name + " must be positive")
    for name in ("cpu_ms", "model_seconds", "model_jitter", "cpus", "deadline_seconds", "sandbox_request_timeout_seconds", "continuation_timeout_seconds", "useful_action_p95_seconds", "continuation_p95_seconds"):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 0 or (name in ("cpus", "deadline_seconds", "sandbox_request_timeout_seconds", "continuation_timeout_seconds", "useful_action_p95_seconds", "continuation_p95_seconds") and value == 0):
            parser.error(name + " has an invalid value")
    if not 0 <= args.sqlite_transactions <= 10000:
        parser.error("sqlite_transactions must be between 0 and 10000")
    if not 1 <= args.sqlite_payload_bytes <= 1024 * 1024:
        parser.error("sqlite_payload_bytes must be between 1 and 1048576")
    if not 0 <= args.tool_upload_kib <= 16384:
        parser.error("tool_upload_kib must be between 0 and 16384")
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


async def probe_response_diagnostic(response, *, redact=()):
    """Retain bounded failure evidence without retrying or relaxing a health gate."""
    evidence = {'status': response.status, 'headers': {
        name: response.headers[name][:256]
        for name in ('Content-Type', 'Server', 'Retry-After', 'X-Trace-Id',
                     'X-UCloud-Sandbox-Retryable', 'Via') if name in response.headers
    }}
    body = bytearray()

    async def read_preview():
        while len(body) < 4097:
            chunk = await response.content.read(4097 - len(body))
            if not chunk:
                break
            body.extend(chunk)

    try:
        await asyncio.wait_for(read_preview(), 0.25)
    except Exception as exc:
        evidence['body_error'] = safe_error(exc)
    preview = bytes(body[:4096]).decode('utf-8', errors='replace')
    for secret in redact:
        if secret:
            preview = preview.replace(secret, 'REDACTED')
    preview = re.sub(r"/_relay/[^/\s'\"]+", '/_relay/REDACTED', preview)
    evidence['body_preview'] = preview
    evidence['body_truncated'] = len(body) > 4096 or 'body_error' in evidence
    try:
        payload = json.loads(preview)
    except (ValueError, TypeError):
        payload = None
    if isinstance(payload, dict):
        if isinstance(payload.get('error_code'), str):
            evidence['error_code'] = payload['error_code'][:128]
        if type(payload.get('retryable')) is bool:
            evidence['retryable'] = payload['retryable']
    return evidence


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


async def finish_primary(job, *, deadline, on_retry):
    """Require authoritative normal exit, retiring the worker's growth forecast."""
    while True:
        record = await retry_control(job.refresh, deadline=deadline, on_retry=on_retry)
        if record.job_id != job.job_id or record.state not in {
            'starting', 'running', 'exited', 'signaled', 'failed',
        }:
            raise RuntimeError('managed primary returned an invalid completion record')
        if record.terminal:
            if (record.state != 'exited' or type(record.exit_code) is not int
                    or record.exit_code != 0 or type(record.signal) is not int
                    or record.signal != 0):
                raise RuntimeError(
                    f'managed primary did not exit normally: state={record.state}, '
                    f'exit_code={record.exit_code}, signal={record.signal}'
                )
            return record
        if record.state not in {'starting', 'running'}:
            raise RuntimeError('managed primary returned inconsistent terminal state')
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('managed primary did not exit after its final integrity proof')
        await asyncio.sleep(min(.1, remaining))


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


def meets_useful_action_slo(result, target_seconds, continuation_target_seconds=.8):
    if not result['correct']:
        return False
    fleet_health = result.get('fleet_health', {})
    if fleet_health.get('enabled') and fleet_health.get('passed') is not True:
        return False
    measured = result['response_ready_to_usable_exec_seconds']
    if not measured['count'] or measured['p95'] >= target_seconds:
        return False
    continuation = result['response_ready_to_guest_continuation_seconds']
    if not continuation['count'] or continuation['p95'] >= continuation_target_seconds:
        return False
    phases = result['phase_latency_seconds']
    if result['configuration']['startup_mode'] == 'rolling' and not phases['during_provisioning']['count']:
        return False
    # Do not hide cold/overlapping delays by excluding them as warmup cycles.
    return all(not phase['count'] or phase['p95'] < limit
               for key, limit in (('phase_latency_seconds', target_seconds),
                                  ('phase_continuation_seconds', continuation_target_seconds))
               for phase in result[key].values())


class ContinuationObserver:
    """Observe guest-originated evidence without touching the sandbox API.

    The timestamp includes observation-tunnel transit/polling and is therefore
    an upper bound on guest continuation, not a pure runtime-restore duration.
    """

    def __init__(self, relay, rollout_id):
        self.relay, self.rollout_id = relay, rollout_id
        self.expected = {}

    def expect(self, payload):
        key = (payload['nonce'], payload['cycle'])
        if key in self.expected:
            raise ValueError('duplicate continuation expectation')
        future = asyncio.get_running_loop().create_future()
        self.expected[key] = (payload['digest'], future)
        return future

    async def run(self):
        while True:
            polled = await retry_control(
                lambda: self.relay.poll(self.rollout_id, timeout_seconds=1,
                                       limit=64, lease_seconds=120),
                deadline=time.monotonic() + 180, on_retry=lambda *_: None,
            )
            observed = time.monotonic()
            for request in polled.requests:
                payload = json.loads(request.body_bytes)
                expected = self.expected.get((payload.get('nonce'), payload.get('cycle')))
                if expected is None or payload.get('digest') != expected[0] or payload.get('tool') != '42':
                    raise RuntimeError('unexpected guest continuation receipt')
                future = expected[1]
                if not future.done():
                    future.set_result(observed)
            # All returned receipts were observed together. Do not add earlier
            # ACK round trips to later timestamps in this same batch. Polling
            # admits at most 64 receipts, bounding this concurrent ACK group.
            await asyncio.gather(*(self.relay.commit_response_bytes_to(
                request, b'{}', headers={'Content-Type': 'application/json'},
            ) for request in polled.requests))

    async def wait(self, future, task, timeout_seconds=180):
        done, _ = await asyncio.wait((future, task), timeout=timeout_seconds,
                                     return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            task.result()
            raise RuntimeError('continuation observer stopped')
        if future not in done:
            raise TimeoutError('guest did not continue after model response')
        return future.result()


async def run(args):
    if args.start_signal_file and Path(args.start_signal_file).exists():
        raise ValueError("start signal file already exists; use a fresh path")
    prefix = "relay-load-" + uuid4().hex[:12]
    token = args.sandbox_token_file.read_text().strip()
    lifecycle_token = (args.gateway_token_file.read_text().strip()
                       if args.parking_mode == 'forced' else token)
    worker_token = args.relay_worker_token_file.read_text().strip()
    config = {k: v for k, v in vars(args).items() if not k.endswith("token_file") and k != "output"}
    fleet_health = FleetHealthQualification(enabled=args.gateway_token_file is not None)
    result = {"report_version": 3, "run_id": prefix, "started_at": datetime.now(timezone.utc).isoformat(),
              "configuration": config, "cycles": [], "completed_scenarios": [], "errors": [], "cleanup_errors": [],
              "health": [], "fleet_polls": [], "placements": {}, "control_retries": [],
              "resource_samples": [], "resource_errors": [],
              "fleet_health_checks": fleet_health.checks,
              "fleet_health_failures": fleet_health.failures,
              "driver_python": sys.version.split()[0],
              "sdk_version": sdk.__version__,
              "sdk_client_sha256": hashlib.sha256(Path(sdk_client.__file__).read_bytes()).hexdigest(),
              "latency_definition": "model ready through guest-originated continuation receipt, then uploaded tool confirmation; receipt includes observation tunnel overhead; full integrity is a separate mandatory gate",
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
        observer_id = prefix + '-observer'
        observer_registration = await relay.register_rollout(observer_id, metadata={'benchmark': prefix, 'purpose': 'continuation-observation'})
        observer_url = http_tunnel_url(args.relay_url, observer_id, 'continued',
                                      registration_token=observer_registration['rollout']['registration_token'])
        observer = ContinuationObserver(relay, observer_id)
        observer_task = asyncio.create_task(observer.run())
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
        async def sample_resources():
            admin_token = args.gateway_token_file.read_text().strip()
            headers = {'Authorization': 'Bearer ' + admin_token}
            diagnostic = {}
            try:
                async with lifecycle.get(
                    args.gateway_url.rstrip('/') + '/v1/nodes', headers=headers,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as response:
                    if response.status >= 400:
                        diagnostic = await probe_response_diagnostic(response, redact=(admin_token,))
                    response.raise_for_status()
                    payload = await response.json()
                now = datetime.now(timezone.utc)
                result['resource_samples'].append(resource_sample(payload['nodes'], now))
                fleet_health.observe(payload['nodes'], now=now, monotonic_now=time.monotonic())
            except Exception as exc:
                now = datetime.now(timezone.utc)
                error = safe_error(exc)
                result['resource_errors'].append({'at': now.isoformat(), 'error': error, **diagnostic})
                fleet_health.observe(None, now=now, monotonic_now=time.monotonic(), probe_error=error)
            result['fleet_health'] = fleet_health.summary()

        async def resource_probe():
            # Use the existing heartbeat cache, not a worker fanout or profiler.
            # Admin credentials are optional for ordinary benchmark clients.
            if args.gateway_token_file is None:
                return
            while True:
                await sample_resources()
                await asyncio.sleep(5)
        resource_monitor = asyncio.create_task(resource_probe())
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
                        fleet_health.placed(sid, result['placements'][sid], monotonic_now=observed)
                await asyncio.sleep(1)
        inventory_tasks = [asyncio.create_task(inventory_probe()) for _ in range(args.fleet_pollers)]
        async def scenario(index):
            sid = f"{prefix}-{index:04d}"
            rng = random.Random(args.seed + index)
            client = AsyncSandboxClient(
                args.gateway_url, api_token=token,
                timeout_seconds=min(args.sandbox_request_timeout_seconds, args.deadline_seconds),
            )
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
                    stage = 'relay_register'
                    registration = await relay.register_agent_rollout(sid, handle, metadata={'benchmark': prefix})
                    registrations.add(sid)
                    event('relay_registered', sandbox_id=sid)
                    tunnel = http_tunnel_url(args.relay_url, sid, 'chat/completions',
                                            registration_token=registration['rollout']['registration_token'])
                    stage = 'agent_upload'
                    await handle.upload_file('/workspace/relay-load.py', AGENT)
                    event('agent_uploaded', sandbox_id=sid)
                    stage = 'agent_start'
                    job = await handle.start_agent(['python', '/workspace/relay-load.py'],
                                                  env={'BENCH_CONFIG': json.dumps(config), 'RELAY_URL': tunnel, 'OBSERVER_URL': observer_url})
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
                            record = await retry_control(
                                job.refresh, deadline=deadline,
                                on_retry=lambda attempt, status: result['control_retries'].append(
                                    dict(operation='agent_status', sandbox_id=sid,
                                         attempt=attempt, status=status)),
                            )
                            if record.terminal:
                                logs = await job.logs('stderr', limit=4096)
                                raise RuntimeError(
                                    f'managed agent {job.job_id} terminated '
                                    f'(state={record.state}, exit_code={record.exit_code}, '
                                    f'signal={record.signal}): '
                                    + logs.data.decode(errors='replace')
                                )
                            next_job_check = time.monotonic() + 5
                        if time.monotonic() > deadline:
                            raise TimeoutError('agent did not issue its model request')
                    payload = json.loads(request.body_bytes)
                    claimed_at = time.monotonic()
                    if payload['cycle'] != cycle or (identity is not None and payload['nonce'] != identity):
                        raise RuntimeError('cycle or process identity mismatch')
                    identity = payload['nonce']
                    continuation = observer.expect(payload)
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
                    stage = 'response_commit'
                    await relay.commit_response_bytes_to(request, request.body_bytes,
                                                         headers={'Content-Type': 'application/json'},
                                                         attempts=120, retry_delay_seconds=.1)
                    committed = time.monotonic()
                    stage = 'guest_continuation'
                    continued = await observer.wait(
                        continuation, observer_task,
                        timeout_seconds=args.continuation_timeout_seconds,
                    )
                    # Same SDK start/wait path as handle.exec(), split only to
                    # distinguish dispatch delay from guest execution/event reads.
                    probe_command = ['python', '-c', PROBE, str(cycle), 'usable']
                    if args.tool_upload_kib:
                        stage = 'usable_tool_upload'
                        tool_body = uploaded_tool_probe(args.tool_upload_kib)
                        await handle.upload_file('/workspace/relay-tool.py', tool_body)
                        probe_command = ['python', '/workspace/relay-tool.py', str(cycle),
                                         'usable', hashlib.sha256(tool_body).hexdigest()]
                    uploaded = time.monotonic()
                    stage = 'usable_exec_start'
                    probe_handle = await handle.start_exec(probe_command)
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
                    if ack['sqlite_rows'] != (cycle + 1) * args.sqlite_transactions:
                        raise RuntimeError('restored SQLite transaction count mismatch')
                    if args.sqlite_transactions and not re.fullmatch('[0-9a-f]{64}', ack['sqlite_digest'] or ''):
                        raise RuntimeError('restored SQLite content proof is missing')
                    result['cycles'].append({'sandbox_id': sid, 'cycle': cycle,
                                             'request_id': request.request_id, 'model_seconds': delay,
                                             'response_ready_unix': started_unix + model_ready - started,
                                             'response_committed_unix': started_unix + committed - started,
                                             'guest_continuation_observed_unix': started_unix + continued - started,
                                             'exec_completed_unix': started_unix + finished - started,
                                             'response_commit_seconds': committed - started,
                                             'usable_exec_seconds': finished - started,
                                             'full_integrity_seconds': verified - model_ready,
                                             'response_ready_to_usable_exec_seconds': finished - model_ready,
                                             'response_ready_to_submit_seconds': started - model_ready,
                                             'response_ready_to_commit_seconds': committed - model_ready,
                                             'response_ready_to_guest_continuation_seconds': continued - model_ready,
                                             'post_continuation_exec_seconds': finished - continued,
                                             'post_continuation_tool_upload_seconds': uploaded - continued,
                                             'post_continuation_exec_start_seconds': probe_dispatched - uploaded,
                                             'post_continuation_exec_wait_seconds': finished - probe_dispatched,
                                             'guest_transport_retries': ack['transport_retries'],
                                             'guest_verification_seconds': ack['verification_seconds'],
                                             'guest_tool_seconds': ack['tool_seconds'],
                                             'sqlite_rows': ack['sqlite_rows'],
                                             'sqlite_digest': ack['sqlite_digest'],
                                             'pid': ack['pid'], 'delivery_count': request.delivery_count})
                    result['cycles'][-1]['park_observed_after_seconds'] = parked_after
                    result['cycles'][-1]['during_provisioning'] = during_provisioning
                    result['cycles'][-1]['agents_started'] = len(jobs)
                    event('cycle_completed', completed=len(result['cycles']), sandbox_id=sid, cycle=cycle,
                          commit_seconds=committed-started, continuation_seconds=continued-model_ready, usable_seconds=finished-started)
                stage = 'agent_finish'
                terminal = await finish_primary(
                    job, deadline=time.monotonic() + args.sandbox_request_timeout_seconds,
                    on_retry=lambda attempt, status: result['control_retries'].append(
                        dict(operation='agent_finish', sandbox_id=sid, attempt=attempt, status=status)),
                )
                result['completed_scenarios'].append({
                    'sandbox_id': sid, 'job_id': terminal.job_id,
                    'state': terminal.state, 'exit_code': terminal.exit_code,
                })
                event('scenario_completed', sandbox_id=sid, job_id=terminal.job_id)
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
            resource_monitor.cancel()
            for task in inventory_tasks:
                task.cancel()
            await asyncio.gather(monitor, resource_monitor, *inventory_tasks, return_exceptions=True)
            # Close the final sampling gap before deleting this run's sandboxes.
            # This occurs after all latency measurements, never in their path.
            if args.gateway_token_file is not None:
                await sample_resources()
            observer_task.cancel()
            await asyncio.gather(observer_task, return_exceptions=True)
            try:
                await asyncio.wait_for(relay.unregister_rollout(observer_id), 30)
            except Exception as exc:
                result['cleanup_errors'].append({'rollout_id': observer_id, 'error': safe_error(exc)})
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
        result['response_commit_seconds'] = summary([r['response_commit_seconds'] for r in measured])
        result['usable_exec_seconds'] = summary([r['usable_exec_seconds'] for r in measured])
        result['response_ready_to_usable_exec_seconds'] = summary([r['response_ready_to_usable_exec_seconds'] for r in measured])
        for field in ('response_ready_to_commit_seconds', 'response_ready_to_guest_continuation_seconds', 'post_continuation_exec_seconds', 'post_continuation_exec_start_seconds', 'post_continuation_exec_wait_seconds', 'full_integrity_seconds',
                      'guest_verification_seconds', 'guest_tool_seconds', 'guest_transport_retries'):
            result[field] = summary([r[field] for r in measured])
        result['measured_park_observed_cycles'] = sum(r['park_observed_after_seconds'] is not None for r in measured)
        result['phase_latency_seconds'] = {
            name: summary([r['response_ready_to_usable_exec_seconds'] for r in result['cycles']
                           if r['during_provisioning'] == provisioning])
            for name, provisioning in [('during_provisioning', True), ('after_provisioning', False)]
        }
        result['phase_continuation_seconds'] = {
            name: summary([r['response_ready_to_guest_continuation_seconds'] for r in result['cycles']
                           if r['during_provisioning'] == provisioning])
            for name, provisioning in [('during_provisioning', True), ('after_provisioning', False)]
        }
        result['completed_cycles'] = len(result['cycles'])
        result['resource_summary'] = resource_summary(result['resource_samples'])
        result['fleet_health'] = fleet_health.summary()
        result['correct'] = (not result.get('failure') and not result['errors'] and not result['cleanup_errors']
                             and len(result['cycles']) == args.sandboxes * args.cycles
                             and len(result['completed_scenarios']) == args.sandboxes
                             and all(h['ok'] for h in result['health'])
                             and all(p['ok'] for p in result['fleet_polls']))
        result['slo_passed'] = meets_useful_action_slo(result, args.useful_action_p95_seconds, args.continuation_p95_seconds)
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        event('benchmark_finished', correct=result['correct'], slo_passed=result['slo_passed'],
              response_commit_seconds=result['response_commit_seconds'], usable_exec_seconds=result['usable_exec_seconds'],
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
