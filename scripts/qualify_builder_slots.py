#!/usr/bin/env python3
"""Owned, bounded 48-build admission qualification; no deployment or pruning.

Prepare validates/copies frozen synthetic fixtures without network access. Run
uses explicit SDK 0.4.34 and gateway-local credentials. One arrival timestamp
covers all local preparation, upload, admission and completion waiting. A missed
client deadline remains a failure even when owned server work later drains.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import signal
import sys
import time
from urllib.parse import urlsplit
from uuid import uuid4


RECIPES = ('python-agent', 'typescript-tools', 'typescript-multistage')
VARIANTS = tuple('app-change-' + str(i) for i in range(5, 21))
SDK_SHA256 = '520b15d66c828193179e86a1521d2871c1e08bfef2227d0bc7a1036445fd8957'
HTTP_RECORD = ContextVar('builder_qualification_http', default=None)
TERMINAL = {'succeeded', 'failed'}


def stamp():
    return datetime.now(timezone.utc).isoformat()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def require(value, message):
    if not value:
        raise ValueError(message)


def context_inventory(root):
    digest, count, size = hashlib.sha256(), 0, 0
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root)
        require(not path.is_symlink(), 'Fixture symlinks are unsupported')
        if not path.is_file() or relative.as_posix() == 'fixture.json' or any(
                part in {'.git', 'node_modules', '__pycache__'} for part in relative.parts):
            continue
        require(path.stat().st_size <= 32 * 1024**2, 'Unexpected large fixture file')
        body = path.read_bytes()
        count, size = count + 1, size + len(body)
        require(count <= 8000 and size <= 32 * 1024**2, 'Fixture exceeds owned workload bounds')
        digest.update(relative.as_posix().encode() + b'\0' + hashlib.sha256(body).digest())
    return dict(context_sha256=digest.hexdigest(), context_files=count, context_bytes=size)


def cold_dockerfile(source):
    require('UCLOUD_QUAL_NONCE' not in source, 'Fixture already contains a qualification nonce')
    lines = source.splitlines(keepends=True)
    index = next((i for i, line in enumerate(lines) if line.startswith('RUN ')), None)
    require(index is not None and any(line.startswith('FROM ') for line in lines[:index]),
            'Expected a dependency RUN after FROM')
    lines[index:index] = [
        'ARG UCLOUD_QUAL_NONCE\n',
        'RUN test -n "$UCLOUD_QUAL_NONCE" && printf "%s\\n" "$UCLOUD_QUAL_NONCE" > /ucloud-qualification-cache-key\n',
    ]
    return ''.join(lines)


def prepare_fixtures(args):
    require(sha(args.inventory) == args.inventory_sha256, 'Frozen inventory SHA differs')
    frozen = {(v['recipe'], v['variant']): v for v in json.loads(args.inventory.read_text())}
    expected = {(r, v) for r in RECIPES for v in VARIANTS}
    require(expected <= frozen.keys(), 'Missing frozen recipe/variant pairs')
    require(re.fullmatch(r'[a-z0-9-]{1,40}', args.arm), 'Invalid arm identity')
    require(not args.output.exists(), 'Output must be new')
    verified = []
    for variant in VARIANTS:
        for recipe in RECIPES:
            source = args.source_root / 'contexts' / recipe / variant
            manifest = json.loads((source / 'fixture.json').read_text())
            require(manifest['recipe'] == recipe and manifest['variant'] == variant,
                    'Unexpected fixture identity')
            inventory = context_inventory(source)
            require(all(inventory[k] == frozen[(recipe, variant)][k] == manifest[k] for k in inventory),
                    'Frozen fixture bytes differ')
            require(manifest.get('bases_digest_pinned') is True, 'Base images must be digest pinned')
            verified.append((source, manifest))
    args.output.mkdir(mode=0o700, parents=True)
    cases = []
    for index, (source, manifest) in enumerate(verified):
        destination = args.output / 'contexts' / manifest['recipe'] / manifest['variant']
        shutil.copytree(source, destination)
        build_args = {}
        if args.mode == 'cold':
            dockerfile = destination / 'Dockerfile'
            dockerfile.write_text(cold_dockerfile(dockerfile.read_text()))
            # Distinct per case as well as per arm: all 48 dependency graphs
            # must execute, rather than sharing only three nonce dependencies.
            build_args['UCLOUD_QUAL_NONCE'] = hashlib.sha256(
                (args.arm + ':' + str(index)).encode()).hexdigest()[:32]
        actual = {**manifest, **context_inventory(destination), 'context_path': str(destination.resolve())}
        write_json(destination / 'fixture.json', actual)
        cases.append(dict(index=index, recipe=manifest['recipe'], variant=manifest['variant'],
                          context_path=str(destination.resolve()), build_args=build_args,
                          context_sha256=actual['context_sha256'], fixture=actual))
    receipt = dict(schema=1, arm=args.arm, mode=args.mode, prepared_at=stamp(),
                   source_inventory_sha256=args.inventory_sha256, cases=cases,
                   semantics='Cold mode uses distinct equal-length ARG markers before dependency installation; bases and package sources may remain cached. No cache is deleted.')
    write_json(args.output / 'cases.json', receipt)
    return dict(cases=48, mode=args.mode, path=str(args.output / 'cases.json'), sha256=sha(args.output / 'cases.json'))


def error_metadata(error):
    result = {'type': type(error).__name__}
    status = getattr(error, 'status_code', None)
    if isinstance(status, int):
        result['status'] = status
    body = getattr(error, 'body', None)
    if isinstance(body, dict) and re.fullmatch(r'[a-z0-9_]{1,100}', str(body.get('error_code', ''))):
        result['code'] = body['error_code']
    return result


def category(path):
    return ('context' if '/image-contexts/' in path else
            'submit' if path.endswith('/v1/images/build') else
            'poll' if '/images/builds/' in path else 'other')


def http_trace(aiohttp):
    trace = aiohttp.TraceConfig()

    async def started(_session, context, params):
        record = HTTP_RECORD.get()
        context.event = None
        if record is not None:
            context.event = dict(method=params.method, category=category(params.url.path), status=None)
            context.started = time.monotonic()
            context.event['start_offset_seconds'] = context.started - record['_arrival']
            if context.event['category'] == 'context' and params.method == 'GET':
                record.setdefault('context_preparation_seconds', context.started - record['_arrival'])
            record['http'].append(context.event)

    async def finished(_session, context, params):
        if context.event is not None:
            context.event.update(status=params.response.status,
                                 headers_seconds=time.monotonic() - context.started)

    async def failed(_session, context, params):
        if context.event is not None:
            context.event.update(error_type=type(params.exception).__name__,
                                 headers_seconds=time.monotonic() - context.started)

    trace.on_request_start.append(started)
    trace.on_request_end.append(finished)
    trace.on_request_exception.append(failed)
    return trace


def sanitize_build(build):
    return {key: build.get(key) for key in (
        'build_id', 'image_id', 'status', 'created_at', 'started_at', 'queued_at',
        'execution_started_at', 'finished_at', 'timings', 'image', 'node', 'location')}


def progress_evidence(build, evidence):
    # Keep only numeric vertex IDs, enums and timings. Never persist stdout,
    # command strings, URLs or package output, even for these owned fixtures.
    for line in build.get('log_tail', '').splitlines():
        match = re.match(r'^#(\d+) (.*)$', line)
        if not match:
            continue
        identity, text = match.groups()
        vertex = evidence.setdefault(identity, {})
        if '] RUN ' in text:
            vertex['operation'] = ('dependency' if 'pip install' in text or 'npm ci' in text else 'run')
        if text == 'CACHED':
            vertex['cached'] = True
        done = re.fullmatch(r'DONE ([0-9.]+)s', text)
        if done:
            vertex['done_seconds'] = float(done[1])
        if re.match(r'^\d+(?:\.\d+)? ', text):
            vertex['command_output_seen'] = True


async def run_case(case, client, image_factory, *, prefix, arrival, deadline, drain_deadline,
                   poll_seconds=1):
    identity = prefix + '-' + str(case['index']).zfill(3)
    record = {key: case[key] for key in ('index', 'recipe', 'variant', 'context_sha256')}
    record.update(image_id=identity, started_at=stamp(), start_offset_seconds=time.monotonic() - arrival,
                  http=[], deadline_missed=False, progress={}, _arrival=arrival)
    token = HTTP_RECORD.set(record)
    started = time.monotonic()
    stage = 'submission'

    def observe(value):
        progress_evidence(value, record['progress'])

    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('common arrival deadline expired')
        image = image_factory(name=identity, context_path=case['context_path'], build_args=case['build_args'])
        submitted = await client.submit_image_build(image, timeout_seconds=remaining)
        record['build_id'] = submitted['build_id']
        record['submission_seconds'] = time.monotonic() - arrival
        stage = 'completion'
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('common arrival deadline expired')
        completed = await client.wait_for_image_build(submitted['build_id'], timeout_seconds=remaining,
                                                      poll_interval_seconds=poll_seconds, on_status=observe)
        observe(completed)
        record['build'] = sanitize_build(completed)
        record['polling_seconds'] = time.monotonic() - arrival - record['submission_seconds']
    except Exception as error:
        record['error'] = {'stage': stage, **error_metadata(error)}
        record['deadline_missed'] = isinstance(error, (TimeoutError, asyncio.TimeoutError)) or time.monotonic() >= deadline
    finally:
        finished = time.monotonic()
        record.update(client_wall_seconds=finished - arrival, own_task_seconds=finished - started,
                      finish_offset_seconds=finished - arrival, finished_at=stamp())
        record['deadline_missed'] |= finished > deadline
        record.pop('_arrival')
        HTTP_RECORD.reset(token)
    # Submission may time out after the server accepted it. Reconcile by this
    # run's unique image identity; never classify a single 404 as proof of no work.
    if record.get('build', {}).get('status') not in TERMINAL:
        posted = any(v['category'] == 'submit' for v in record['http'])
        if not record.get('build_id') and not posted and stage == 'submission':
            record['drain'] = {'state': 'never_posted'}
        else:
            record['drain'] = await drain_case(client, record.get('build_id', identity), drain_deadline,
                                                poll_seconds=poll_seconds, observe=observe)
            if 'build' in record['drain']:
                record['build'] = record['drain']['build']
                record.setdefault('build_id', record['build']['build_id'])
    return record


async def drain_case(client, identity, deadline, *, poll_seconds, observe):
    retries = Counter()
    while time.monotonic() < deadline:
        try:
            build = await client.get_image_build(identity, timeout_seconds=min(10, max(.001, deadline - time.monotonic())))
            observe(build)
            if build['status'] in TERMINAL:
                return dict(state='terminal_after_client_failure', at=stamp(), build=sanitize_build(build), errors=dict(retries))
        except Exception as error:
            detail = error_metadata(error)
            retries[str(detail.get('status', detail['type']))] += 1
        await asyncio.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))
    return dict(state='unresolved', at=stamp(), errors=dict(retries))


def distribution(values):
    ordered = sorted(values)
    return dict(count=len(ordered), min=min(ordered, default=None),
                p50=ordered[math.ceil(len(ordered) * .5) - 1] if ordered else None,
                p95=ordered[math.ceil(len(ordered) * .95) - 1] if ordered else None,
                max=max(ordered, default=None))


def summarize(records, wall_seconds):
    succeeded = sum(v.get('build', {}).get('status') == 'succeeded' for v in records)
    deadline_misses = sum(v['deadline_missed'] for v in records)
    client_errors = sum('error' in v for v in records)
    unresolved = sum(v.get('drain', {}).get('state') == 'unresolved' for v in records)
    return dict(submitted_cases=len(records), succeeded=succeeded, failed_or_incomplete=len(records) - succeeded,
                client_errors=client_errors, deadline_misses=deadline_misses, unresolved=unresolved,
                batch_wall_seconds=wall_seconds,
                passed=len(records) == 48 and succeeded == 48 and not (deadline_misses or client_errors or unresolved),
                measurements={name: distribution([v[name] for v in records if name in v])
                              for name in ('client_wall_seconds', 'context_preparation_seconds', 'submission_seconds', 'polling_seconds')},
                http_statuses=dict(Counter(str(event['status']) for v in records for event in v['http'])),
                records=sorted(records, key=lambda v: v['index']))


def load_sdk(path):
    require(sha(path) == SDK_SHA256, 'SDK wheel differs from tested 0.4.34')
    require('ucloud_sandboxes_sdk' not in sys.modules, 'Load explicit SDK before other SDK imports')
    sys.path.insert(0, str(path.resolve()))
    import ucloud_sandboxes_sdk as sdk
    require(sdk.__version__ == '0.4.34', 'Unexpected SDK version')
    return sdk


def verify_builder_receipts(paths, expected_nodes, declared_slots, expected_wheel_sha256=None):
    if declared_slots == 4:
        require(not paths, 'Baseline must not claim candidate upgrade receipts')
        return []
    require(len(paths) == 4, 'Six-slot policy requires four verified owned runtime receipts')
    result = []
    for path in paths:
        require(path.stat().st_size <= 1024**2, 'Oversized builder receipt')
        value = json.loads(path.read_text())
        after = value.get('after', {})
        if value.get('kind') == 'installed_runtime':
            require(expected_wheel_sha256 is not None, 'Fresh runtime inspection requires an explicit wheel pin')
            inspector = Path(__file__).with_name('inspect_owned_builder.py')
            require(value.get('service_changed') is False
                    and value.get('inspector_sha256') == sha(inspector)
                    and type(value.get('installed_files_match_wheel')) is int
                    and value['installed_files_match_wheel'] > 0
                    and value.get('runtime_file_set_matches') is True
                    and value.get('import_origins_match') is True
                    and after.get('active_builds') == 0 and after.get('admission_open') is True
                    and after.get('draining') is False, 'Installed runtime inspection was not verified')
        else:
            require(value.get('service_changed') is True, 'Candidate builder upgrade was not verified')
        if expected_wheel_sha256 is not None:
            require(re.fullmatch(r'[0-9a-f]{64}', expected_wheel_sha256)
                    and value.get('wheel_sha256') == expected_wheel_sha256, 'Builder wheel identity differs')
        require(value.get('complete') is True and after.get('finishing_capacity') == 2 and after.get('node_epoch'),
                'Builder policy or incarnation was not verified')
        result.append(dict(job_id=str(after['job_id']), node_epoch=after['node_epoch'],
                           finishing_capacity=2, receipt_sha256=sha(path),
                           verification=value.get('kind', 'owned_upgrade'),
                           wheel_sha256=value.get('wheel_sha256')))
    require({v['job_id'] for v in result} == set(expected_nodes), 'Candidate receipts name different builders')
    return result


async def hold_builders(args, sdk):
    from ucloud_sandboxes.config import DeploymentConfig
    config = DeploymentConfig.from_dict(json.loads(args.deployment_config.read_text()))
    token = config.sandbox_api_token_file().read_text().strip()
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stopping.set)
    loop.add_signal_handler(signal.SIGINT, stopping.set)
    deadline = time.monotonic() + args.duration
    async with sdk.AsyncSandboxClient(args.gateway_url, api_token=token, timeout_seconds=30) as client:
        require(not await client.list_sandboxes(), 'Hold must start on an idle sandbox fleet')
        require(not any(v['status'] not in TERMINAL for v in await client.list_image_builds()),
                'Hold must start without existing active builds')
        with (args.output / 'hold.jsonl').open('x') as stream:
            try:
                while not stopping.is_set() and time.monotonic() < deadline:
                    row = dict(at=stamp(), reservation=args.reservation, count=4)
                    try:
                        response = await client.prepare_builder(count=4, ttl_seconds=180, prepare_id=args.reservation)
                        row.update(ok=True, expires_at=response['prepare']['expires_at'])
                    except Exception as error:
                        row.update(ok=False, error=error_metadata(error))
                    stream.write(json.dumps(row) + '\n')
                    stream.flush()
                    print(json.dumps(row), flush=True)
                    try:
                        await asyncio.wait_for(stopping.wait(), min(30, max(.001, deadline - time.monotonic())))
                    except asyncio.TimeoutError:
                        pass
            finally:
                try:
                    await client.delete_prepared_builder(args.reservation)
                    cleanup = dict(at=stamp(), reservation=args.reservation, released=True)
                except Exception as error:
                    cleanup = dict(at=stamp(), reservation=args.reservation, released=False, error=error_metadata(error))
                write_json(args.output / 'hold-release.json', cleanup)
    return dict(completed=True, reservation=args.reservation)


async def measure(args, sdk, cases):
    import aiohttp
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.control_state import ControlStateStore
    config = DeploymentConfig.from_dict(json.loads(args.deployment_config.read_text()))
    token = config.sandbox_api_token_file().read_text().strip()
    node_token = config.node_control_token_file().read_text().strip()

    def fleet():
        return [{k: getattr(value, k) for k in ('job_id', 'node_url', 'active_image_builds', 'active_sandboxes',
                 'physical_disk_free_mb', 'capabilities')} | {'updated_at': str(value.updated_at)}
                for value in ControlStateStore(config.control_state_file()).load_heartbeats().values()]

    async with aiohttp.ClientSession(trace_configs=[http_trace(aiohttp)]) as session:
        async def live_builders(nodes):
            async def one(node):
                async with session.get(node['node_url'].rstrip('/') + '/v1/heartbeat',
                    headers={'Authorization': 'Bearer ' + node_token}, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    response.raise_for_status()
                    value = (await response.json())['heartbeat']
                return {k: value.get(k) for k in ('job_id', 'node_epoch', 'updated_at', 'active_image_builds', 'labels')}
            return await asyncio.gather(*(one(node) for node in nodes))

        async with sdk.AsyncSandboxClient(args.gateway_url, api_token=token, timeout_seconds=60, session=session) as client:
            require(not await client.list_sandboxes(), 'Qualification requires no existing sandboxes')
            require(not any(v['status'] not in TERMINAL for v in await client.list_image_builds()),
                    'Qualification requires no existing active builds')
            nodes = [v for v in fleet() if 'image-build' in v['capabilities']]
            require({str(v['job_id']) for v in nodes} == set(args.expected_node), 'Unexpected fleet identity')
            require(all('image-build' in v['capabilities'] and v['active_image_builds'] == 0 and
                        v['physical_disk_free_mb'] > 20 * 1024 and
                        0 <= (datetime.now(timezone.utc) - datetime.fromisoformat(v['updated_at'])).total_seconds() < 30
                        for v in nodes), 'Builder readiness, disk or freshness gate failed')
            write_json(args.output / 'before.json', dict(at=stamp(), fleet=nodes))
            before_live = await live_builders(nodes)
            require(all(v['active_image_builds'] == 0 for v in before_live), 'Live builder admission is occupied')
            # The hint is dynamic: idle candidate nodes expose four available
            # preparing/solving slots. Two finishing positions are verified by
            # their owned upgrade receipts, not by expecting an idle hint of6.
            require(all((v['labels'] or {}).get('ucloud.image-build-admission-capacity', '4') == '4'
                        for v in before_live), 'Live idle builder admission differs from four solving slots')
            if args.builder_receipts:
                require({(str(v['job_id']), v['node_epoch']) for v in before_live} ==
                        {(v['job_id'], v['node_epoch']) for v in args.builder_receipts},
                        'Live builder incarnation differs from upgrade receipts')
            write_json(args.output / 'live-before.json', dict(at=stamp(), builders=before_live))
            arrival = time.monotonic()
            started_at = stamp()
            deadline = arrival + args.deadline_seconds
            lag, records, activity, activity_errors = [], [], [], []

            async def monitor():
                while True:
                    due = time.monotonic() + .05
                    await asyncio.sleep(.05)
                    lag.append(max(0, time.monotonic() - due))

            async def monitor_activity():
                while True:
                    try:
                        activity.append(dict(at=stamp(), builders=await live_builders(nodes)))
                    except Exception as error:
                        activity_errors.append(dict(at=stamp(), error=error_metadata(error)))
                    await asyncio.sleep(2)

            monitor_task = asyncio.create_task(monitor())
            activity_task = asyncio.create_task(monitor_activity())
            tasks = [asyncio.create_task(run_case(case, client, sdk.Image.from_dockerfile,
                     prefix=args.image_prefix, arrival=arrival, deadline=deadline,
                     drain_deadline=deadline + args.drain_seconds)) for case in cases]
            try:
                with (args.output / 'completed.jsonl').open('x') as stream:
                    for future in asyncio.as_completed(tasks):
                        record = await future
                        records.append(record)
                        stream.write(json.dumps(record) + '\n')
                        stream.flush()
                        write_json(args.output / (record['image_id'] + '.json'), record)
                        print(json.dumps(dict(event='case_finished', at=stamp(), index=record['index'],
                            status=record.get('build', {}).get('status'), deadline_missed=record['deadline_missed'],
                            client_wall_seconds=record['client_wall_seconds'], completed=len(records))), flush=True)
            finally:
                monitor_task.cancel()
                activity_task.cancel()
                await asyncio.gather(monitor_task, activity_task, return_exceptions=True)
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            result = summarize(records, time.monotonic() - arrival)
            result.update(phase=args.run_id, started_at=started_at, finished_at=stamp(),
                          deadline_seconds=args.deadline_seconds, drain_seconds=args.drain_seconds,
                          deadline_origin='one wave arrival before any task or context preparation',
                          sdk_version=sdk.__version__, sdk_sha256=SDK_SHA256,
                          harness_sha256=sha(__file__), mode=args.mode,
                          declared_slots=args.declared_slots, cases_sha256=sha(args.cases),
                          builder_upgrade_receipts=args.builder_receipts,
                          fleet_after=fleet(), driver_event_loop_lag_seconds=distribution(lag),
                          live_builder_activity=activity, activity_sampling_errors=activity_errors,
                          activity_note='Heartbeat active_image_builds can include in-flight HTTP operation fences; it is activity, not an exact owned-record slot count.',
                          gateway_loopback_client=True)
            owners = {str((v.get('build', {}).get('node') or {}).get('job_id')) for v in records}
            result['owner_gate'] = owners <= set(args.expected_node) and 'None' not in owners
            result['owned_builds_terminal'] = all(v.get('build', {}).get('status') in TERMINAL for v in records)
            result['active_builds_after'] = sum(v['status'] not in TERMINAL for v in await client.list_image_builds())
            cleanup_due = time.monotonic() + 180
            while True:
                result['live_builders_after'] = await live_builders(nodes)
                result['live_admission_drained'] = all(v['active_image_builds'] == 0 for v in result['live_builders_after'])
                if result['live_admission_drained'] or time.monotonic() >= cleanup_due:
                    break
                await asyncio.sleep(1)
            result['cold_dependency_evidence'] = [dict(index=v['index'],
                executed=sum(e.get('operation') == 'dependency' and not e.get('cached') and e.get('command_output_seen', False)
                             for e in v['progress'].values())) for v in records]
            result['cold_execution_gate'] = (args.mode != 'cold' or all(v['executed'] > 0 for v in result['cold_dependency_evidence']))
            result['passed'] &= (result['owner_gate'] and result['cold_execution_gate']
                                 and result['active_builds_after'] == 0 and result['live_admission_drained']
                                 and not activity_errors)
            write_json(args.output / 'summary.json', result)
            return {k: v for k, v in result.items() if k not in {'records', 'fleet_after', 'cold_dependency_evidence', 'live_builder_activity'}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    prepare = sub.add_parser('prepare')
    prepare.add_argument('--source-root', type=Path, required=True)
    prepare.add_argument('--inventory', type=Path, required=True)
    prepare.add_argument('--inventory-sha256', required=True)
    prepare.add_argument('--output', type=Path, required=True)
    prepare.add_argument('--arm', required=True)
    prepare.add_argument('--mode', choices=('warm', 'cold'), required=True)
    run = sub.add_parser('run')
    run.add_argument('--cases', type=Path, required=True)
    run.add_argument('--cases-sha256', required=True)
    run.add_argument('--sdk-wheel', type=Path, required=True)
    run.add_argument('--deployment-config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    run.add_argument('--gateway-url', default='https://77.42.92.27')
    run.add_argument('--output', type=Path, required=True)
    run.add_argument('--run-id', required=True)
    run.add_argument('--expected-node', action='append', required=True)
    run.add_argument('--declared-slots', type=int, choices=(4, 6), required=True,
                     help='Declared configuration; independently capture actual per-node admission configuration')
    run.add_argument('--deadline-seconds', type=float, required=True)
    run.add_argument('--drain-seconds', type=float, default=1900)
    run.add_argument('--builder-receipt', type=Path, action='append', default=[],
                     help='Four verified installed-runtime or owned-upgrade receipts for the6-total policy')
    run.add_argument('--builder-wheel-sha256',
                     help='Pin every builder receipt to this wheel; required for installed-runtime inspection')
    hold = sub.add_parser('hold')
    hold.add_argument('--sdk-wheel', type=Path, required=True)
    hold.add_argument('--deployment-config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    hold.add_argument('--gateway-url', default='https://77.42.92.27')
    hold.add_argument('--output', type=Path, required=True)
    hold.add_argument('--reservation', required=True)
    hold.add_argument('--duration', type=float, default=5400)
    args = parser.parse_args(argv)
    if args.command == 'prepare':
        result = prepare_fixtures(args)
    elif args.command == 'hold':
        require(re.fullmatch(r'builder-slot-qualification-[a-z0-9]{12}', args.reservation), 'Unexpected owned reservation')
        require(0 < args.duration <= 7200, 'Hold must be bounded to two hours')
        require(not args.output.exists(), 'Output must be new')
        args.output.mkdir(mode=0o700, parents=True)
        result = asyncio.run(hold_builders(args, load_sdk(args.sdk_wheel)))
    else:
        require(re.fullmatch(r'slotq-[a-z0-9-]{1,35}', args.run_id), 'Expected unique owned slotq- run ID')
        require(len(set(args.expected_node)) == 4 and all(re.fullmatch(r'[0-9]+', v) for v in args.expected_node),
                'Expected four numeric builder IDs')
        require(0 < args.deadline_seconds <= 1800 and 0 < args.drain_seconds <= 2100, 'Invalid finite time bounds')
        parsed = urlsplit(args.gateway_url)
        require(parsed.scheme == 'https' and parsed.hostname and not parsed.username and not parsed.password
                and not parsed.query and not parsed.fragment, 'Use credential-free HTTPS gateway URL')
        require(sha(args.cases) == args.cases_sha256, 'Case inventory SHA differs')
        manifest = json.loads(args.cases.read_text())
        args.mode = manifest['mode']
        cases = manifest['cases']
        require(args.mode in {'warm', 'cold'} and len(cases) == 48 and
                {v['index'] for v in cases} == set(range(48)), 'Expected48 cases')
        for case in cases:
            require(context_inventory(Path(case['context_path']))['context_sha256'] == case['context_sha256'],
                    'Prepared context changed')
        require(not args.output.exists(), 'Output must be new')
        args.builder_receipts = verify_builder_receipts(args.builder_receipt, args.expected_node,
                                                       args.declared_slots, args.builder_wheel_sha256)
        args.output.mkdir(mode=0o700, parents=True)
        sdk = load_sdk(args.sdk_wheel)
        args.image_prefix = args.run_id + '-' + uuid4().hex[:8]
        write_json(args.output / 'launch.json', dict(run_id=args.run_id, at=stamp(), cases_sha256=args.cases_sha256,
                   owned_image_ids=[args.image_prefix + '-' + str(case['index']).zfill(3) for case in cases],
                   declared_slots=args.declared_slots, expected_nodes=args.expected_node, sdk_sha256=SDK_SHA256,
                   deadline_seconds=args.deadline_seconds, drain_seconds=args.drain_seconds,
                   note='No service configuration changes; no automatic image or cache deletion. Interruptions require reconciling every owned image ID.'))
        result = asyncio.run(measure(args, sdk, cases))
    print(json.dumps(result, indent=2))
    return 0 if result.get('passed', True) else 1


if __name__ == '__main__':
    raise SystemExit(main())
