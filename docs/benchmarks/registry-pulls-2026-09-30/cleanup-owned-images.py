#!/usr/bin/env python3
"""Plan/apply exact owned qualification-image removals; never collect blobs.

Requires frozen ledger and smoke receipt hashes. A plan is read-only except its
new output receipt; --apply additionally uses the registry lease writer fence
and maintenance lock. No cache, environment, component or snapshot repository
is eligible. An unknown alias protects its entire manifest.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
from urllib.request import Request, urlopen
from uuid import UUID

PHASES = {'slotq-pulls-a-cold', 'slotq-pulls-b-cold', 'slotq-pulls-a2-cold'}
SOURCES = PHASES | {'python-source'}
COUNT = 145
ROOT = Path('/work/ucloud-sandboxes/registry-pull-qualification-20260930')
CANARY_ID = 'registry-pull-canary-d81f2c41758a-045'
VERIFIER_SHA = 'ec1f2dd97907eaa05ea8b71d9940e84345e9c526b29273347b77d54ecc05f22a'
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


def source_receipts(source, root, phase):
    summary_path, launch_path = root / phase / 'summary.json', root / phase / 'launch.json'
    require(Path(source['summary_path']) == summary_path and Path(source['launch_path']) == launch_path,
            'Source path differs from qualification root')
    require(sha(summary_path) == source['summary_sha256'] and sha(launch_path) == source['launch_sha256'],
            'Source receipt hash differs')
    return read(summary_path), read(launch_path)


def load_owned(ledger, root, worker_url):
    from ucloud_sandboxes.control_plane import _managed_registry_build_tag
    from ucloud_sandboxes.managed_registry import registry_repository_tag_from_image_ref
    rows = ledger['rows']
    require(ledger['count'] == len(rows) == COUNT, 'Expected exactly 145 owned images')
    require(set(ledger['sources']) == SOURCES, 'Expected exactly three waves and one source canary')
    for key in ('image_id', 'build_id', 'repository'):
        require(len({r[key] for r in rows}) == COUNT, 'Duplicate owned build/image/repository identity')
    bindings = {}
    for phase in sorted(PHASES):
        summary, launch = source_receipts(ledger['sources'][phase], root, phase)
        expected = summary['records']
        actual = [r for r in rows if r['phase'] == phase]
        require(summary['phase'] == phase and summary['passed'] is True and len(expected) == len(actual) == 48,
                'Phase incomplete')
        require({r['index'] for r in actual} == set(range(48)), 'Phase index set incomplete')
        require({r['image_id'] for r in actual} == set(launch['owned_image_ids']),
                'Not all images declared before launch')
        require(len({r['image_id'] for r in expected}) == 48, 'Duplicate source image record')
        bindings.update({r['image_id']: r for r in expected})
        for row in actual:
            require(re.fullmatch(re.escape(phase) + r'-[0-9a-f]{8}-[0-9]{3}', row['image_id']),
                    'Unexpected image name')
    source = ledger['sources']['python-source']
    summary, launch = source_receipts(source, root, 'python-source')
    verification_path = root / 'python-source' / 'source-verification.json'
    require(Path(source['verification_path']) == verification_path
            and sha(verification_path) == source['verification_sha256'], 'Source verification hash differs')
    verification = read(verification_path)
    actual = [r for r in rows if r['phase'] == 'python-source']
    require(len(actual) == 1 and actual[0]['image_id'] == CANARY_ID == launch['image_id'],
            'Unexpected canary image identity')
    record = summary['record']
    require(record['image_id'] == CANARY_ID and not record.get('error') and record['deadline_missed'] is False,
            'Canary build did not finish cleanly')
    # Preserve the original failed diagnostic: only its expected image-name
    # label difference is accepted by the separately pinned read-only verifier.
    require(verification['complete'] is True and verification['helper_sha256'] == VERIFIER_SHA
            and verification['source_receipt_sha256'] == source['summary_sha256']
            and verification['source_launch_sha256'] == source['launch_sha256']
            and verification['source_rootfs_equal'] is True
            and verification['compressed_descriptors_equal'] is True
            and verification['runtime_config_equal_except_owned_name'] is True
            and verification['registry_mutations'] == 0,
            'Canary source equivalence was not independently verified')
    require((verification['image_id'], verification['repository'], verification['manifest_digest']) ==
            tuple(actual[0][key] for key in ('image_id', 'repository', 'manifest_digest')),
            'Verified canary binding differs')
    require((record['index'], record['recipe'], record['variant']) == (45, 'python-agent', 'app-change-20'),
            'Unexpected canary fixture')
    bindings[CANARY_ID] = record
    require(set(bindings) == {r['image_id'] for r in rows}, 'Source image set differs')
    for row in rows:
        require(str(UUID(row['build_id'])) == row['build_id'], 'Malformed build UUID')
        previous = bindings[row['image_id']]
        build, image = previous['build'], previous['build']['image']
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


def build_ledger(root, worker_url):
    """Derive a reviewable ledger; every row is validated again before use."""
    from ucloud_sandboxes.managed_registry import registry_repository_tag_from_image_ref
    ledger = {'schema': 1, 'count': COUNT, 'rows': [], 'sources': {}}
    for phase in sorted(SOURCES):
        summary_path, launch_path = root / phase / 'summary.json', root / phase / 'launch.json'
        source = dict(summary_path=str(summary_path), summary_sha256=sha(summary_path),
                      launch_path=str(launch_path), launch_sha256=sha(launch_path))
        summary = read(summary_path)
        if phase == 'python-source':
            verification = root / phase / 'source-verification.json'
            source.update(verification_path=str(verification), verification_sha256=sha(verification))
            records = [summary['record']]
        else:
            records = summary['records']
        ledger['sources'][phase] = source
        for record in records:
            build, image = record['build'], record['build']['image']
            repository, tag = registry_repository_tag_from_image_ref(image['tag'])
            ledger['rows'].append(dict(phase=phase, index=record['index'], recipe=record['recipe'],
                variant=record['variant'], image_id=record['image_id'], build_id=build['build_id'],
                image_ref=image['tag'], repository=repository, tag=tag, manifest_digest=image['manifest_digest']))
    load_owned(ledger, root, worker_url)
    return ledger


def verify_smokes(smokes, rows):
    if isinstance(smokes, dict):
        require(smokes.get('passed') is True, 'Smoke receipt did not pass')
        smokes = smokes.get('results', smokes.get('records', smokes.get('smokes')))
    require(isinstance(smokes, list) and len(smokes) == 3, 'Expected three smoke records')
    by_id = {r['image_id']: r for r in rows if r['phase'] in {'slotq-pulls-b-cold'}}
    require({r.get('recipe') for r in smokes} == RECIPES, 'Smoke recipes incomplete')
    require(len({r.get('sandbox_id') for r in smokes}) == 3
            and len({r.get('image_id') for r in smokes}) == 3, 'Duplicate smoke identity')
    for row in smokes:
        require(row.get('verified') is True and row.get('deleted') is True
                and type(row.get('exit_code')) is int and row['exit_code'] == 0
                and row.get('image_id') in by_id and row['recipe'] == by_id[row['image_id']]['recipe'],
                'Smoke did not verify and delete an owned candidate image sandbox')
        require(isinstance(row['sandbox_id'], str) and re.fullmatch(r'registry-pull-smoke-[0-9a-f]{12}-[0-9]+', row['sandbox_id']), 'Unexpected smoke sandbox')


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


@contextmanager
def database(path, *, write=False):
    # No store constructor: even planning must not initialize/change DB files.
    conn = sqlite3.connect(path.resolve().as_uri() + ('?mode=rw' if write else '?mode=ro'),
                           uri=True, timeout=5, isolation_level=None)
    try:
        conn.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def active_leases(conn, row):
    from ucloud_sandboxes.managed_registry import RegistryUsageStore
    values = conn.execute('SELECT repository,tag,owner,acquired_at,renewed_at,expires_at,digest '
                          'FROM registry_leases WHERE repository=?', (row['repository'],))
    now = datetime.now(timezone.utc)
    leases = [RegistryUsageStore._lease_from_row(tuple(value)) for value in values]
    return [lease for lease in leases if lease.is_active(now)]


def image_record_guard(conn, row):
    from ucloud_sandboxes.images import ImageRecord
    from ucloud_sandboxes.managed_registry import registry_repository_tag_from_image_ref, image_ref_with_manifest_digest
    own = None
    for record_id, payload in conn.execute('SELECT record_id,record_json FROM image_state_v1_images'):
        record = ImageRecord.from_dict(json.loads(payload))
        require(record.id == record_id, 'Image database record identity differs')
        coordinates = registry_repository_tag_from_image_ref(record.tag)
        if coordinates is not None and coordinates[0] == row['repository']:
            require(record.id == row['image_id'], 'Unrelated image record protects owned repository')
        if record.id == row['image_id']:
            own = record
    if own is not None:
        refs = {row['image_ref'], image_ref_with_manifest_digest(row['image_ref'], row['manifest_digest'])}
        require(own.tag in refs and own.manifest_digest == row['manifest_digest'],
                'Owned image record was replaced')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ledger', type=Path)
    parser.add_argument('--write-ledger', action='store_true')
    parser.add_argument('--ledger-sha256')
    parser.add_argument('--smokes', type=Path)
    parser.add_argument('--smokes-sha256')
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    require(not args.output.exists() and not args.output.with_suffix('.jsonl').exists(), 'Output must be new')
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.managed_registry import RegistryClient, registry_maintenance_lock
    from ucloud_sandboxes.systemd import REGISTRY_MAINTENANCE_LOCK
    config = DeploymentConfig.from_dict(read(args.config))
    require(args.root == ROOT, 'Unexpected qualification root')
    if args.write_ledger:
        require(not args.apply and not args.ledger and not args.smokes, 'Ledger generation is a separate read-only action')
        save(args.output, build_ledger(args.root, config.registry_worker_url))
        print(json.dumps({'ledger_created': True, 'count': COUNT, 'sha256': sha(args.output)}))
        return
    require(args.ledger and args.smokes and sha(args.ledger) == args.ledger_sha256
            and sha(args.smokes) == args.smokes_sha256, 'Input hash differs')
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
                with database(config.registry_usage_file()) as lease_db, database(config.image_file()) as image_db:
                    image_record_guard(image_db, row)
                    leases = active_leases(lease_db, row)
                    result['items'].append({'image_id':row['image_id'], 'repository':row['repository'],
                        'manifest_digest':row['manifest_digest'], **inventory(client, row),
                        'active_lease_count':len(leases), 'eligible':not leases})
        else:
            with registry_maintenance_lock(REGISTRY_MAINTENANCE_LOCK, timeout_seconds=10):
                with args.output.with_suffix('.jsonl').open('x') as progress:
                    args.output.with_suffix('.jsonl').chmod(0o600)
                    for index, row in enumerate(rows):
                        require(time.monotonic() < deadline, 'Bounded cleanup deadline expired')
                        if index % 32 == 0:
                            idle_guard(config)
                        # Same writer-lock exclusion as lease_fence, without its
                        # unrelated expired-lease deletion. Only image rows below mutate.
                        with database(config.registry_usage_file(), write=True) as lease_db, \
                                database(config.image_file(), write=True) as image_db:
                            image_record_guard(image_db, row)
                            require(not active_leases(lease_db, row), 'Owned image has an active lease')
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
        result['complete'] = len(result['items']) == COUNT
        result['eligible'] = all(item.get('eligible', True) for item in result['items'])
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
