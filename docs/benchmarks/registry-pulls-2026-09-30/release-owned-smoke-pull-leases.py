#!/usr/bin/env python3
"""Plan/release only three exact pull leases from retired owned smoke workers.

All immutable image bindings, full worker incarnations, owner hashes and lease
fields must match the successful/deleted smoke evidence. No environment,
component, unrelated or general expired lease cleanup is performed. Registry
manifests and blobs are untouched; the separate cleanup helper owns deletion.
"""
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import runpy
import sqlite3

ROOT = Path('/work/ucloud-sandboxes/registry-pull-qualification-20260930')
CLEANUP_SHA = '6e4dd31b0528a66ce20801118012abc466ddf6fda3bfde3d3c5df06ac58cbd3d'
SMOKE_HELPER_SHA = '1a5f47ab23892e42d81af894a1a687aabf4c99b79e71c90674e21218b912d5a2'
FIELDS = ('repository', 'tag', 'owner', 'acquired_at', 'renewed_at', 'expires_at', 'digest')
CREATED_FIELDS = ('sandbox_id', 'image_id', 'recipe', 'variant', 'published_image_tag',
                  'published_manifest_digest', 'worker', 'resolved_image_ref',
                  'create_image_pull_lease_owner', 'create_image_pull_lease_owner_verified',
                  'create_image_pull_lease')


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(result.tzinfo is not None, 'Expected a timezone-aware evidence timestamp')
    return result.astimezone(timezone.utc)


def expected_lease(case, row):
    from ucloud_sandboxes.control_plane import _registry_operation_lease_owner
    from ucloud_sandboxes.managed_registry import RegistryUsageStore, image_ref_with_manifest_digest
    require(case['image_id'] == row['image_id'] and case['recipe'] == row['recipe']
            and case['variant'] == row['variant'], 'Smoke image binding differs')
    require(case['published_image_tag'] == row['image_ref']
            and case['published_manifest_digest'] == row['manifest_digest'], 'Published smoke image changed')
    resolved = image_ref_with_manifest_digest(row['image_ref'], row['manifest_digest'])
    require(case['resolved_image_ref'] == resolved, 'Resolved smoke repository/digest changed')
    worker = case['worker']
    require(all(isinstance(worker.get(key), str) and worker[key]
                for key in ('job_id', 'node_id', 'node_epoch', 'node_url')), 'Incomplete worker incarnation')
    require(worker['job_id'].isdigit(), 'Unexpected worker job identity')
    owner = _registry_operation_lease_owner('create-image-pull',
        (worker['job_id'], worker['node_epoch'], worker['node_url'], resolved))
    require(case['create_image_pull_lease_owner_verified'] is True
            and case['create_image_pull_lease_owner'] == owner, 'Computed pull owner differs')
    captured = case['create_image_pull_lease']
    require(isinstance(captured, dict) and set(captured) == set(FIELDS), 'Captured lease schema differs')
    lease = RegistryUsageStore._lease_from_row(tuple(captured[key] for key in FIELDS))
    require((lease.repository, lease.tag, lease.owner, lease.digest) ==
            (row['repository'], row['tag'], owner, row['manifest_digest']), 'Captured lease image/owner differs')
    require(lease.expires_at and timestamp(lease.acquired_at) <= timestamp(lease.renewed_at)
            <= timestamp(case['finished_at']) and timestamp(lease.renewed_at) < timestamp(lease.expires_at),
            'Captured lease times do not bind a finished finite pull')
    return asdict(lease)


def verify_provider(provider, cases):
    require(provider['all_absent'] is True and provider['provider'] == 'hetzner'
            and provider['mutations'] == 0, 'Expected read-only provider absence evidence')
    nodes = provider['nodes']
    require(len({row['id'] for row in nodes}) == len(nodes), 'Duplicate provider node evidence')
    by_id = {row['id']: row for row in nodes}
    worker_ids = {case['worker']['job_id'] for case in cases}
    require(worker_ids <= set(by_id), 'Smoke worker retirement not proved')
    for identity in worker_ids:
        row = by_id[identity]
        require(row['absent'] is True and type(row['http_status']) is int and row['http_status'] == 404,
                'Owned worker still exists or absence is ambiguous')
    require(timestamp(provider['verified_at']) >= max(timestamp(case['finished_at']) for case in cases),
            'Provider evidence predates smoke deletion')
    return sorted(worker_ids)


def current_selection(path, expected):
    """Read only; do not initialize a missing DB or prune expired rows."""
    from ucloud_sandboxes.managed_registry import RegistryUsageStore
    conn = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)
    try:
        conn.execute('BEGIN')
        selected = []
        for image_id, wanted in expected.items():
            values = conn.execute('SELECT ' + ','.join(FIELDS) + ' FROM registry_leases WHERE repository=?',
                                  (wanted['repository'],)).fetchall()
            require(len(values) <= 1, 'Additional lease protects owned image')
            if not values:
                selected.append(dict(image_id=image_id, expected=wanted, state='already_absent'))
                continue
            lease = RegistryUsageStore._lease_from_row(tuple(values[0]))
            require(asdict(lease) == wanted, 'Exact owned pull lease changed')
            selected.append(dict(image_id=image_id, expected=wanted,
                                 state='active' if lease.is_active(datetime.now(timezone.utc)) else 'expired'))
        return selected
    finally:
        conn.close()


def release_selected(usage, selected, result):
    # Host registry coordination is held by the caller, matching gateway lease
    # acquisition. Re-read every tuple before the first release; release_lease
    # owns its SQLite transaction, so never nest it inside another writer fence.
    expected = {row['image_id']: row['expected'] for row in selected}
    selected = current_selection(usage.path, expected)
    for row in selected:
        entry = {**row, 'released': False}
        if row['state'] != 'already_absent':
            lease = row['expected']
            # The API returns False for an expired row even when it deleted it.
            # Exact identity was validated above; absence below is the proof.
            usage.release_lease(lease['repository'], lease['tag'], lease['owner'])
            require(current_selection(usage.path, {row['image_id']: lease})[0]['state'] == 'already_absent',
                    'Exact owned lease remained after release')
            entry['released'] = True
        result.append(entry)
    require(all(row['state'] == 'already_absent' for row in current_selection(usage.path, expected)),
            'An owned image still has a lease')


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ledger-sha256', required=True)
    parser.add_argument('--smokes-sha256', required=True)
    parser.add_argument('--provider-sha256', required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--plan', type=Path)
    parser.add_argument('--plan-sha256')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), 'Output must be new')
    pins = {'cleanup-owned-images.py': CLEANUP_SHA, 'owned-images.json': args.ledger_sha256,
            'image-smokes/summary.json': args.smokes_sha256, 'provider-absence.json': args.provider_sha256}
    for name, digest in pins.items():
        require(sha(ROOT / name) == digest, 'Pinned receipt/helper changed')
    helper = runpy.run_path(str(ROOT / 'cleanup-owned-images.py'))
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.control_plane import _registry_lease_coordination
    from ucloud_sandboxes.host_locks import HOST_LOCKS
    from ucloud_sandboxes.managed_registry import RegistryUsageStore, registry_maintenance_lock
    from ucloud_sandboxes.systemd import REGISTRY_MAINTENANCE_LOCK
    config = DeploymentConfig.from_dict(helper['read'](Path('/etc/ucloud-sandboxes/deployment.json')))
    rows = helper['load_owned'](helper['read'](ROOT / 'owned-images.json'), ROOT, config.registry_worker_url)
    smokes = helper['read'](ROOT / 'image-smokes/summary.json')
    helper['verify_smokes'](smokes, rows)
    cases = smokes['results']
    launch = helper['read'](ROOT / 'image-smokes/launch.json')
    require(launch['helper_sha256'] == SMOKE_HELPER_SHA
            and set(launch['sandbox_ids']) == {case['sandbox_id'] for case in cases}
            and set(launch['image_ids']) == {case['image_id'] for case in cases}, 'Smoke launch identity changed')
    captured_hashes = {'launch.json': sha(ROOT / 'image-smokes/launch.json')}
    by_id = {row['image_id']: row for row in rows}
    expected = {}
    for case in cases:
        created = ROOT / 'image-smokes' / (case['sandbox_id'] + '-created.json')
        capture = helper['read'](created)
        require(all(capture[key] == case[key] for key in CREATED_FIELDS), 'Created worker/lease evidence changed')
        captured_hashes[created.name] = sha(created)
        expected[case['image_id']] = expected_lease(case, by_id[case['image_id']])
    require(len(expected) == 3 and len({v['owner'] for v in expected.values()}) == 3,
            'Expected exactly three distinct owned pull leases')
    worker_ids = verify_provider(helper['read'](ROOT / 'provider-absence.json'), cases)
    if args.apply:
        require(args.plan and args.plan_sha256 and sha(args.plan) == args.plan_sha256, 'Exact reviewed plan required')
        plan = helper['read'](args.plan)
        require(plan['complete'] is True and plan['apply'] is False and plan['pins'] == pins
                and plan['captured_receipt_sha256'] == captured_hashes
                and {row['image_id']: row['expected'] for row in plan['leases']} == expected,
                'Reviewed plan binding differs')
    else:
        require(not args.plan and not args.plan_sha256, 'Plan input is only used when applying')
    result = dict(apply=args.apply, complete=False, started_at=datetime.now(timezone.utc).isoformat(),
                  pins=pins, captured_receipt_sha256=captured_hashes, worker_ids=worker_ids,
                  owner_hash_recomputed=True, leases=[],
                  scope='Only three exact create-image-pull lease tuples; dependency/unrelated leases and registry content untouched.')
    try:
        HOST_LOCKS.configure(config.control_state_file().parent / 'gateway-locks')
        with registry_maintenance_lock(REGISTRY_MAINTENANCE_LOCK, timeout_seconds=10), _registry_lease_coordination():
            result['idle'] = helper['idle_guard'](config)
            require(result['idle']['live_nodes_checked'] == 0, 'Fleet must be fully retired')
            selected = current_selection(config.registry_usage_file(), expected)
            if args.apply:
                # Existing DB was just opened mode=ro; only apply constructs the
                # ordinary store for its exact-key release API. No snapshot/GC.
                usage = RegistryUsageStore(config.registry_usage_file())
                release_selected(usage, selected, result['leases'])
            else:
                result['leases'] = [{**row, 'released': False} for row in selected]
            result['complete'] = True
    except Exception as error:
        result['error_type'] = type(error).__name__
        raise
    finally:
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        helper['save'](args.output, result)
    print(json.dumps({'complete': True, 'apply': args.apply, 'leases': len(result['leases'])}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'complete': False, 'error_type': type(error).__name__}))
        raise SystemExit(1) from None
