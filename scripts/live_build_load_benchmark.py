#!/usr/bin/env python3
"""Bounded production build qualification; credentials remain on the gateway.

Run with the deployed gateway Python. Every build has a unique image ID, so
completed-image API shortcuts cannot substitute for measured BuildKit work.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import threading
import time
from urllib import error, parse, request


RECIPES = ('python-agent', 'typescript-tools', 'typescript-multistage')
PREPARE = 'realistic-build-load-20260929'
SDK_WHEEL = '/work/ucloud-sandboxes/sdk-status-0.4.33-20260928/ucloud_sandboxes_sdk-0.4.33-py3-none-any.whl'
CONTEXT = threading.local()


def stamp():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def clients():
    sys.path.insert(0, SDK_WHEEL)
    import ucloud_sandboxes_sdk.client as sdk
    from ucloud_sandboxes.config import DeploymentConfig
    config = DeploymentConfig.from_dict(json.loads(Path('/etc/ucloud-sandboxes/deployment.json').read_text()))
    token = config.sandbox_api_token_file().read_text().strip()
    return sdk, config, lambda: sdk.SandboxClient('https://77.42.92.27', api_token=token, timeout_seconds=120)


def fleet(config):
    from ucloud_sandboxes.control_state import ControlStateStore
    return [{'job_id': h.job_id, 'node_url': h.node_url,
             'active_image_builds': h.active_image_builds,
             'active_sandboxes': h.active_sandboxes,
             'physical_disk_free_mb': h.physical_disk_free_mb,
             'updated_at': str(h.updated_at), 'capabilities': h.capabilities}
            for h in ControlStateStore(config.control_state_file()).load_heartbeats().values()]


def pin_bases(root):
    destination = root / 'base-pins.json'
    if destination.exists():
        raise RuntimeError('Base pins already exist; reuse them for the entire comparison')
    result = {'captured_at': stamp(), 'images': {}}
    for image in ('python:3.12-bookworm', 'node:22-bookworm', 'node:22-bookworm-slim'):
        repository, tag = image.split(':')
        repository = 'library/' + repository
        query = parse.urlencode({'service': 'registry.docker.io', 'scope': 'repository:' + repository + ':pull'})
        with request.urlopen('https://auth.docker.io/token?' + query, timeout=30) as response:
            token = json.load(response)['token']
        req = request.Request('https://registry-1.docker.io/v2/' + repository + '/manifests/' + tag,
            headers={'Authorization': 'Bearer ' + token, 'Accept': ', '.join((
                'application/vnd.oci.image.index.v1+json', 'application/vnd.docker.distribution.manifest.list.v2+json',
                'application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json'))})
        with request.urlopen(req, timeout=30) as response:
            raw = response.read(4 * 1024**2)
            digest = 'sha256:' + hashlib.sha256(raw).hexdigest()
            assert response.headers['Docker-Content-Digest'] == digest
        result['images'][image] = image + '@' + digest
    write_json(destination, result)
    print(json.dumps(result), flush=True)


def instrument_http(sdk):
    original = sdk.open_no_redirect

    def measured(req, *args, **kwargs):
        start = time.monotonic()
        status = None
        try:
            response = original(req, *args, **kwargs)
            status = response.status
            return response
        except error.HTTPError as exc:
            status = exc.code
            raise
        finally:
            events = getattr(CONTEXT, 'http', None)
            if events is not None:
                path = parse.urlsplit(req.full_url).path
                category = ('context' if '/image-contexts/' in path else
                            'submit' if path == '/v1/images/build' else
                            'poll' if '/images/builds/' in path else 'other')
                events.append({'method': req.method, 'category': category, 'status': status,
                               'headers_seconds': time.monotonic() - start})
    sdk.open_no_redirect = measured


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def distribution(values):
    return {'count': len(values), 'min': min(values) if values else None,
            'p50': percentile(values, .5), 'p95': percentile(values, .95),
            'max': max(values) if values else None}


def summarize(records, wall_seconds):
    completed = [r for r in records if r.get('build', {}).get('status') == 'succeeded']
    measurements = {}
    for name in ('client_wall_seconds', 'submission_seconds', 'polling_seconds'):
        measurements[name] = distribution([r[name] for r in records if name in r])
    for name in ('preparation_ms', 'queue_wait_ms', 'total_ms', 'end_to_end_ms'):
        measurements[name] = distribution([r['build']['timings'][name] for r in completed
                                           if name in r['build'].get('timings', {})])
    phases = ('docker_build_and_push_ms', 'immutable_environment_ms', 'cleanup_ms')
    for name in phases:
        measurements[name] = distribution([r['build']['timings']['phases'][name] for r in completed
                                           if name in r['build'].get('timings', {}).get('phases', {})])
    return {'submitted_cases': len(records), 'succeeded': len(completed),
            'failed_or_incomplete': len(records) - len(completed),
            'batch_wall_seconds': wall_seconds,
            'successful_builds_per_minute': len(completed) * 60 / wall_seconds,
            'measurements': measurements,
            'builders': dict(Counter(str((r.get('build', {}).get('node') or {}).get('job_id', 'unknown')) for r in completed)),
            'http_statuses': dict(Counter(str(e['status']) for r in records for e in r.get('http', []))),
            'environment_totals': {k: sum(r['build'].get('timings', {}).get('environment', {}).get(k, 0) for r in completed)
                for k in ('groups_reused', 'groups_built', 'erofs_bytes_built', 'docker_pull_skipped')},
            'records': records}


def build_phase(args, root, sdk, config, factory):
    if not (1 <= args.concurrency <= 48 and 1 <= args.count <= 96 and 0 < args.timeout <= 1800):
        raise ValueError('Supported bounds: concurrency1..48, count1..96, timeout<=1800')
    if not re.fullmatch(r'[a-z0-9-]{1,40}', args.phase):
        raise ValueError('Invalid phase')
    output = root / args.phase
    output.mkdir()  # Never silently overwrite evidence or reuse image IDs.
    client = factory()
    assert not client.list_sandboxes(), 'Run build-only phases without unrelated sandbox work'
    assert not any(b['status'] not in {'succeeded', 'failed'} for b in client.list_image_builds()), 'Existing builds must finish first'
    builders = [h for h in fleet(config) if 'image-build' in h['capabilities']]
    assert len(builders) == args.expected_builders, 'Wait for the expected builder pool before measuring'
    assert all((datetime.now(timezone.utc) - datetime.fromisoformat(h['updated_at'])).total_seconds() < 90
               for h in builders), 'Builder heartbeats must be fresh'
    assert all(h['physical_disk_free_mb'] > 20 * 1024 for h in builders), 'Insufficient builder disk headroom'
    for recipe in RECIPES:
        for variant in args.variants:
            context = root / 'contexts' / recipe / variant
            manifest = json.loads((context / 'fixture.json').read_text())
            assert manifest['recipe'] == recipe and manifest['variant'] == variant
            assert (context / 'Dockerfile').is_file()
    instrument_http(sdk)
    write_json(output / 'before.json', {'at': stamp(), 'fleet': fleet(config)})
    start = time.monotonic()
    gate = threading.Barrier(min(args.count, args.concurrency))
    records = []

    def run(index):
        recipe = RECIPES[index % len(RECIPES)]
        variant = args.variants[(index // len(RECIPES)) % len(args.variants)]
        context = root / 'contexts' / recipe / variant
        manifest = json.loads((context / 'fixture.json').read_text())
        name = 'bl20260929-' + args.phase + '-' + str(index).zfill(3)
        if index < args.concurrency:
            gate.wait(timeout=120)
        before = time.monotonic()
        CONTEXT.http = []
        receipt = {'index': index, 'recipe': recipe, 'variant': variant, 'image_id': name,
                   'context_sha256': manifest.get('context_sha256'), 'started_at': stamp(),
                   'start_offset_seconds': before - start}
        current = factory()
        steps, cached = {}, set()

        def observe(value):
            for line in value.get('log_tail', '').splitlines():
                match = re.match(r'^(#\d+) (.*)$', line)
                if match:
                    if match[2].startswith('['):
                        steps[match[1]] = match[2]
                    elif match[2] == 'CACHED':
                        cached.add(match[1])
        try:
            image = sdk.Image.from_dockerfile(name=name, context_path=context)
            submitted = current.submit_image_build(image, timeout_seconds=args.timeout)
            receipt['submission_seconds'] = time.monotonic() - before
            receipt['build_id'] = submitted['build_id']
            done = current.wait_for_image_build(submitted['build_id'], timeout_seconds=max(1, args.timeout - (time.monotonic() - before)),
                                               poll_interval_seconds=1, on_status=observe)
            receipt['polling_seconds'] = time.monotonic() - before - receipt['submission_seconds']
            receipt['build'] = {k: done.get(k) for k in ('build_id', 'image_id', 'status', 'created_at', 'started_at',
                'queued_at', 'execution_started_at', 'finished_at', 'timings', 'image', 'node', 'location')}
            log = done.get('log_tail', '')
            (output / (name + '.build.log')).write_text(log)
            receipt['cached_steps_in_retained_tail'] = sum(bool(re.match(r'^#\d+ CACHED$', line)) for line in log.splitlines())
            receipt['cached_steps_observed'] = [{'id': key, 'description': steps.get(key)} for key in sorted(cached)]
            if done['status'] != 'succeeded':
                receipt['error'] = done.get('error')
        except Exception as exc:
            receipt['exception_type'] = type(exc).__name__
            receipt['error'] = str(exc)[:1000]
        finally:
            receipt.update(client_wall_seconds=time.monotonic() - before,
                           finish_offset_seconds=time.monotonic() - start, finished_at=stamp(), http=CONTEXT.http)
            del CONTEXT.http
        write_json(output / (name + '.json'), receipt)
        return receipt

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor, (output / 'completed.jsonl').open('x') as stream:
        futures = [executor.submit(run, index) for index in range(args.count)]
        for future in as_completed(futures):
            result = future.result()
            records.append(result)
            stream.write(json.dumps(result) + '\n')
            stream.flush()
            print(json.dumps({'at': stamp(), 'phase': args.phase, 'completed': len(records),
                              'index': result['index'], 'status': result.get('build', {}).get('status', 'error'),
                              'wall_seconds': result['client_wall_seconds']}), flush=True)
    wall_seconds = time.monotonic() - start
    # Final cleanup updates can follow the first terminal response. This read
    # is outside the measured batch and also observes durable history updates.
    for record in records:
        if record.get('build_id'):
            try:
                latest = client.get_image_build(record['build_id'], timeout_seconds=30)
                if record.get('build'):
                    record['build'].update({k: latest.get(k) for k in record['build']})
                write_json(output / (record['image_id'] + '.json'), record)
            except Exception as exc:
                record['refresh_error_type'] = type(exc).__name__
    ids = [r['build_id'] for r in records if r.get('build_id')]
    assert len(ids) == len(set(ids)), 'Independent test cases must not join the same build'
    result = summarize(sorted(records, key=lambda r: r['index']), wall_seconds)
    result.update(phase=args.phase, finished_at=stamp(), fleet_after=fleet(config), concurrency=args.concurrency,
                  recipes=list(RECIPES), variants=args.variants, gateway_loopback_client=True)
    # Read persisted summaries without opening a competing writer.
    with sqlite3.connect(config.metrics_path().with_name('build-history.sqlite').as_uri() + '?mode=ro', uri=True) as db:
        result['durable_history_records'] = sum(db.execute('SELECT 1 FROM terminal_builds WHERE build_id=?',
            (r.get('build_id', ''),)).fetchone() is not None for r in records)
    write_json(output / 'summary.json', result)
    print(json.dumps({k:v for k,v in result.items() if k != 'records'}), flush=True)
    if result['failed_or_incomplete']:
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('/work/ucloud-sandboxes/build-load-20260929'))
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('pin-bases')
    prepare = commands.add_parser('prepare')
    prepare.add_argument('--count', type=int, default=4)
    commands.add_parser('release')
    commands.add_parser('state')
    build = commands.add_parser('build')
    build.add_argument('--phase', required=True)
    build.add_argument('--count', type=int, required=True)
    build.add_argument('--concurrency', type=int, required=True)
    build.add_argument('--variants', nargs='+', default=['base'])
    build.add_argument('--timeout', type=float, default=1200)
    build.add_argument('--expected-builders', type=int, default=4)
    args = parser.parse_args()
    args.root.mkdir(exist_ok=True, parents=True)
    if args.command == 'pin-bases':
        pin_bases(args.root)
        return
    sdk, config, factory = clients()
    client = factory()
    if args.command == 'prepare':
        if not 1 <= args.count <= config.builder.max_nodes:
            raise ValueError('Prepare count exceeds configured production builder limit')
        client.prepare_builder(count=args.count, ttl_seconds=3600, prepare_id=PREPARE)
        print(json.dumps({'prepared': args.count, 'at': stamp()}))
    elif args.command == 'release':
        client.delete_prepared_builder(PREPARE)
        print(json.dumps({'released': True, 'at': stamp()}))
    elif args.command == 'state':
        print(json.dumps({'at': stamp(), 'fleet': fleet(config), 'prepared': client.list_prepared_builders(),
                          'sandbox_count': len(client.list_sandboxes()),
                          'active_builds': sum(b['status'] not in {'succeeded', 'failed'} for b in client.list_image_builds())}))
    else:
        build_phase(args, args.root, sdk, config, factory)


if __name__ == '__main__':
    main()
