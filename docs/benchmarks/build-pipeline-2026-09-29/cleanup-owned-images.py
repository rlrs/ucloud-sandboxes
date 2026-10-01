#!/usr/bin/env python3
"""Plan/apply exact owned qualification-image removals; never collect blobs.

Requires frozen ledger and smoke receipt hashes. A plan is read-only except its
new output receipt; --apply additionally uses the ordinary registry lease fence
and maintenance lock. No cache, environment, component or snapshot repository
is eligible. An unknown alias protects its entire manifest.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
from urllib.request import Request, urlopen
from uuid import UUID

PHASES = {'slotq-a-condition', 'slotq-a-warm', 'slotq-a-cold', 'slotq-b-warm', 'slotq-b-cold'}
RECIPES = {'python-agent', 'typescript-tools', 'typescript-multistage'}


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    require(path.is_file() and path.stat().st_size <= 32 * 1024 * 1024, 'Input missing/too large')
    return json.loads(path.read_text())


def save(path, value):
    with path.open('x') as f:
        path.chmod(0o600)
        json.dump(value, f, indent=2)
        f.write('\n')


def load_owned(ledger, root, worker_url):
    from ucloud_sandboxes.control_plane import _managed_registry_build_tag
    from ucloud_sandboxes.managed_registry import registry_repository_tag_from_image_ref
    rows = ledger['rows']
    require(ledger['count'] == len(rows) == 240, 'Expected exactly240 owned images')
    require(set(ledger['sources']) == PHASES, 'Expected exactlyfive source phases')
    require(len({r['image_id'] for r in rows}) == 240 and len({r['build_id'] for r in rows}) == 240,
            'Duplicate owned build/image identity')
    require(len({r['repository'] for r in rows}) == 240, 'Expected one unique repository per image')
    for phase in sorted(PHASES):
        source = ledger['sources'][phase]
        summary_path = root / phase / 'summary.json'
        launch_path = root / phase / 'launch.json'
        require(Path(source['summary_path']) == summary_path and Path(source['launch_path']) == launch_path,
                'Source path differs from qualification root')
        require(sha(summary_path) == source['summary_sha256'] and sha(launch_path) == source['launch_sha256'],
                'Source receipt hash differs')
        summary, launch = read(summary_path), read(launch_path)
        expected = summary['records']
        actual = [r for r in rows if r['phase'] == phase]
        require(summary['phase'] == phase and summary['passed'] is True and len(expected) == len(actual) == 48,
                'Phase incomplete')
        require({r['index'] for r in actual} == set(range(48)), 'Phase index set incomplete')
        require({r['image_id'] for r in actual} == set(launch['owned_image_ids']), 'Not all images declared before launch')
        by_id = {r['image_id']: r for r in expected}
        for row in actual:
            require(re.fullmatch(re.escape(phase) + r'-[0-9a-f]{8}-[0-9]{3}', row['image_id']), 'Unexpected image name')
            require(str(UUID(row['build_id'])) == row['build_id'], 'Malformed build UUID')
            previous = by_id[row['image_id']]
            build = previous['build']
            image = build['image']
            require(build['status'] == 'succeeded' and build['build_id'] == row['build_id']
                    and image['id'] == row['image_id'] and image['pushed'] is True, 'Build/image identity mismatch')
            require(all(previous[key] == row[key] for key in ('recipe', 'variant', 'index')), 'Case binding differs')
            require(row['image_ref'] == image['tag'] == _managed_registry_build_tag(row['image_id'], worker_url),
                    'Not the exact gateway-managed owned image reference')
            require(registry_repository_tag_from_image_ref(row['image_ref']) == (row['repository'], row['tag']),
                    'Registry coordinates differ')
            require(row['tag'] == 'latest' and row['repository'].startswith('ucloud-managed/'), 'Unexpected registry scope')
            require(row['manifest_digest'] == image['manifest_digest']
                    and re.fullmatch(r'sha256:[0-9a-f]{64}', row['manifest_digest']), 'Manifest identity differs')
    return rows


def verify_smokes(smokes, rows):
    if isinstance(smokes, dict):
        require(smokes.get('passed') is True, 'Smoke receipt did not pass')
        smokes = smokes.get('results', smokes.get('records', smokes.get('smokes')))
    require(isinstance(smokes, list) and len(smokes) == 3, 'Expected three smoke records')
    by_id = {r['image_id']: r for r in rows if r['phase'] in {'slotq-b-warm', 'slotq-b-cold'}}
    require({r.get('recipe') for r in smokes} == RECIPES, 'Smoke recipes incomplete')
    require(len({r.get('sandbox_id') for r in smokes}) == 3
            and len({r.get('image_id') for r in smokes}) == 3, 'Duplicate smoke identity')
    for row in smokes:
        require(row.get('verified') is True and row.get('deleted') is True
                and type(row.get('exit_code')) is int and row['exit_code'] == 0
                and row.get('image_id') in by_id and row['recipe'] == by_id[row['image_id']]['recipe'],
                'Smoke did not verify and delete an owned candidate image sandbox')
        require(isinstance(row['sandbox_id'], str) and row['sandbox_id'].startswith('slotq-'), 'Unexpected smoke sandbox')


def get_json(url, token):
    with urlopen(Request(url, headers={'Authorization': 'Bearer ' + token}), timeout=10) as response:
        body = response.read(16 * 1024 * 1024 + 1)
    require(len(body) <= 16 * 1024 * 1024, 'API response too large')
    return json.loads(body)


def idle_guard(config):
    token = config.sandbox_api_token_file().read_text().strip()
    base = 'http://127.0.0.1:' + str(config.gateway_port)
    require(get_json(base + '/v1/sandboxes?view=status', token)['sandboxes'] == [], 'Sandboxes still exist')
    builds = get_json(base + '/v1/images/builds', token)['builds']
    require(all(row['status'] in {'succeeded', 'failed'} for row in builds), 'Builds still active')
    # The aggregate build endpoint tolerates unavailable nodes. Check every
    # current heartbeat directly instead of treating a partial response as idle.
    with sqlite3.connect(config.control_state_file().resolve().as_uri() + '?mode=ro', uri=True) as db:
        nodes = [json.loads(row[0]) for row in db.execute("SELECT payload FROM control_records WHERE namespace='heartbeat'")]
    node_token = config.node_control_token_file().read_text().strip()
    for node in nodes:
        live = get_json(node['node_url'].rstrip('/') + '/v1/heartbeat', node_token)['heartbeat']
        require((live['job_id'], live['node_epoch']) == (node['job_id'], node['node_epoch']), 'Node incarnation changed')
        require(live['active_image_builds'] == 0 and live['active_sandboxes'] == 0, 'Node still active')
    return {'sandbox_count': 0, 'active_builds': 0, 'live_nodes_checked': len(nodes)}


def inventory(client, row):
    from ucloud_sandboxes.managed_registry import RegistryRequestError, digest_protection_tag
    repository, digest = row['repository'], row['manifest_digest']
    try:
        tags = client.tags(repository)
    except RegistryRequestError as error:
        if error.status_code != 404:
            raise
        tags = []
    require(len(tags) <= 8, 'Unexpected number of aliases in owned repository')
    expected = {'latest', digest_protection_tag(digest)}
    aliases = {}
    for tag in tags:
        found = client.manifest_digest(repository, tag)
        if found == digest:
            require(tag in expected, 'Unknown alias protects this owned manifest')
            aliases[tag] = found
        elif tag == 'latest':
            raise ValueError('Owned latest tag changed')
    if 'latest' not in aliases:
        try:
            client.manifest_digest(repository, digest)
        except RegistryRequestError as error:
            if error.status_code == 404:
                return {'state': 'absent', 'aliases': [], 'unrelated_aliases': len(tags)}
            raise
        raise ValueError('Owned manifest exists without its declared latest tag')
    return {'state': 'present', 'aliases': sorted(aliases), 'unrelated_aliases': len(tags)-len(aliases)}


def image_record_guard(images, conn, row):
    from ucloud_sandboxes.managed_registry import registry_repository_tag_from_image_ref
    records = images._load(conn)
    for record in records.values():
        coordinates = registry_repository_tag_from_image_ref(record.tag)
        if coordinates is not None and coordinates[0] == row['repository']:
            require(record.id == row['image_id'], 'Unrelated image record protects owned repository')
    own = records.get(row['image_id'])
    if own is not None:
        require(own.tag == row['image_ref'] and own.manifest_digest == row['manifest_digest'],
                'Owned image record was replaced')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ledger', type=Path, required=True)
    parser.add_argument('--ledger-sha256', required=True)
    parser.add_argument('--smokes', type=Path, required=True)
    parser.add_argument('--smokes-sha256', required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    require(not args.output.exists() and not args.output.with_suffix('.jsonl').exists(), 'Output must be new')
    require(sha(args.ledger) == args.ledger_sha256 and sha(args.smokes) == args.smokes_sha256, 'Input hash differs')
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.managed_registry import RegistryClient, RegistryUsageStore, registry_maintenance_lock
    from ucloud_sandboxes.systemd import REGISTRY_MAINTENANCE_LOCK
    from ucloud_sandboxes.images import ImageStore
    config = DeploymentConfig.from_dict(read(args.config))
    rows = load_owned(read(args.ledger), args.root, config.registry_worker_url)
    verify_smokes(read(args.smokes), rows)
    client = RegistryClient(config.registry_url, timeout_seconds=5)
    result = {'started_at':datetime.now(timezone.utc).isoformat(), 'apply':args.apply, 'complete':False,
              'ledger_sha256':args.ledger_sha256, 'smokes_sha256':args.smokes_sha256,
              'image_count':len(rows), 'items':[],
              'scope':'Exact owned managed manifests only. Shared cache/components/environments, unrelated aliases and all blobs are preserved.'}
    deadline = time.monotonic()+300
    try:
        result['idle_before'] = idle_guard(config)
        if not args.apply:
            for row in rows:
                require(time.monotonic() < deadline, 'Bounded cleanup deadline expired')
                result['items'].append({'image_id':row['image_id'], 'repository':row['repository'],
                    'manifest_digest':row['manifest_digest'], **inventory(client, row)})
        else:
            usage = RegistryUsageStore(config.registry_usage_file())
            images = ImageStore(config.image_file())
            with registry_maintenance_lock(REGISTRY_MAINTENANCE_LOCK, timeout_seconds=10):
                with args.output.with_suffix('.jsonl').open('x') as progress:
                    args.output.with_suffix('.jsonl').chmod(0o600)
                    for index, row in enumerate(rows):
                        require(time.monotonic() < deadline, 'Bounded cleanup deadline expired')
                        if index % 32 == 0:
                            idle_guard(config)
                        with usage.lease_fence() as snapshot, images._transaction(write=True, timeout_seconds=5) as image_db:
                            image_record_guard(images, image_db, row)
                            require(not any(lease.repository == row['repository'] for lease in snapshot.leases.values()),
                                    'Owned image has an active lease')
                            item = {'image_id':row['image_id'], 'repository':row['repository'],
                                    'manifest_digest':row['manifest_digest'], **inventory(client, row)}
                            if item['state'] == 'present':
                                client.delete_manifest(row['repository'], row['manifest_digest'])
                                item['deleted'] = True
                            else:
                                item['already_absent'] = True
                            require(inventory(client, row)['state'] == 'absent', 'Manifest remained after deletion')
                            image_db.execute('DELETE FROM image_state_v1_images WHERE record_id = ?', (row['image_id'],))
                        result['items'].append(item)
                        progress.write(json.dumps(item)+'\n')
                        progress.flush()
        result['idle_after'] = idle_guard(config)
        result['complete'] = len(result['items']) == 240
    except Exception as error:
        result['error_type'] = type(error).__name__
        raise
    finally:
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        save(args.output, result)
    print(json.dumps({'complete':result['complete'], 'apply':args.apply, 'images':len(result['items'])}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'complete':False, 'error_type':type(error).__name__}))
        raise SystemExit(1) from None
