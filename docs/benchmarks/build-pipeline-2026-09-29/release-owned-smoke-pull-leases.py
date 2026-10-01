#!/usr/bin/env python3
"""Release exactly three finished, retired qualification-worker pull leases.

The original worker epoch was not retained, so the owner hash is NOT claimed
as independently recomputed. This one-off receipt binds the exact persisted
owner/repository/digest/timestamps to the three successful deleted smoke cases,
retained sandbox-scheduled metrics and provider-confirmed worker retirement.
It does not release environment/component leases or delete registry manifests.
The original reviewed cleanup helper must perform any subsequent deletion.
"""
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import runpy
import sqlite3

ROOT = Path('/work/ucloud-sandboxes/builder-slot-qualification-20260929')
PINS = {
    'cleanup-owned-images.py': '77ab34434def2b45cf311ff9f1fe1f213f32643dfb10d9dcb0a731b991274c7a',
    'owned-images.json': '77ad2a45ae495389b80b832ae74136a93f1bef01d04c401778fc0187cdf34780',
    'image-smokes/summary.json': '2c756e473e54cbbb43211ad9fd9ca7f713be2e07da3d149ebc9707a5abb6fcee',
    'provider-absence.json': '297d7427d83b18106ed940425237aeac5776c89e67398ee12371dc07a66b6c4b',
}
EXPECTED = {
    'slotq-b-cold-71e13d91-045': ('2ccf655d9fd9109b100e1a97ba87b1afe6aee59cf0e253c3540dbb5ff2096b9b', '635016'),
    'slotq-b-cold-71e13d91-046': ('cd7b5b9e65cffc07d6f738f2be6818a0d50bc131eecc13f27d22cc63b8c04f49', '674732'),
    'slotq-b-cold-71e13d91-047': ('f0c5c21d283b3f51d9a31c5ec6956c13d953fc2dea4a6907d36b735390e08da8', '649018'),
}


def require(value, message):
    if not value:
        raise ValueError(message)


def validate_lease(lease, row):
    suffix, micros = EXPECTED[row['image_id']]
    acquired = '2026-09-29T21:18:56.' + micros + '+00:00'
    expected = {'repository': row['repository'], 'tag': 'latest',
                'digest': row['manifest_digest'], 'owner': 'create-image-pull:v1:' + suffix,
                'acquired_at': acquired, 'renewed_at': acquired,
                'expires_at': '2026-09-29T22:18:56.' + micros + '+00:00'}
    require(asdict(lease) == expected, 'Exact owned pull lease changed')
    return expected


def exact_leases(path, repositories):
    from ucloud_sandboxes.managed_registry import RegistryImageLease
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=5) as db:
        return [RegistryImageLease(*values)
                for repository in repositories
                for values in db.execute('SELECT repository,tag,owner,acquired_at,renewed_at,expires_at,digest '
                                         'FROM registry_leases WHERE repository=?', (repository,))]


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), 'Output must be new')
    for name, expected in PINS.items():
        require(hashlib.sha256((ROOT/name).read_bytes()).hexdigest() == expected, 'Pinned receipt/helper changed')
    helper = runpy.run_path(str(ROOT/'cleanup-owned-images.py'))
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.control_plane import _registry_lease_coordination
    from ucloud_sandboxes.host_locks import HOST_LOCKS
    from ucloud_sandboxes.managed_registry import RegistryUsageStore, registry_maintenance_lock
    from ucloud_sandboxes.systemd import REGISTRY_MAINTENANCE_LOCK
    config = DeploymentConfig.from_dict(json.loads(Path('/etc/ucloud-sandboxes/deployment.json').read_text()))
    HOST_LOCKS.configure(config.control_state_file().parent/'gateway-locks')
    ledger = json.loads((ROOT/'owned-images.json').read_text())
    rows = helper['load_owned'](ledger, ROOT, config.registry_worker_url)
    smoke = json.loads((ROOT/'image-smokes/summary.json').read_text())
    helper['verify_smokes'](smoke, rows)
    require({r['image_id'] for r in smoke['results']} == set(EXPECTED), 'Smoke identities changed')
    provider = json.loads((ROOT/'provider-absence.json').read_text())
    require(provider['all_absent'] is True and any(r['id']=='168011780' and r['http_status']==404
            and r['absent'] is True for r in provider['nodes']), 'Owned worker retirement not proved')
    # Only exact owned sandbox scheduling metadata is read. No customer payload.
    scheduled = []
    with sqlite3.connect(config.metrics_path().resolve().as_uri()+'?mode=ro', uri=True) as db:
        for case in smoke['results']:
            events = db.execute("SELECT timestamp,data_json FROM metric_events WHERE kind='sandbox_scheduled' "
                "AND timestamp>='2026-09-29T21:18:56' AND timestamp<'2026-09-29T21:19:00' "
                "AND json_extract(data_json,'$.sandbox_id')=?", (case['sandbox_id'],)).fetchall()
            require(len(events)==1, 'Exact smoke scheduling evidence missing')
            at, payload = events[0]
            event = json.loads(payload)
            require(event['job_id']=='168011780' and event['node_id']=='10.42.0.7', 'Smoke worker identity differs')
            scheduled.append({'sandbox_id':case['sandbox_id'],'image_id':case['image_id'],
                              'at':at,'job_id':event['job_id'],'node_id':event['node_id']})
    result = {'apply':args.apply,'complete':False,'started_at':datetime.now(timezone.utc).isoformat(),
              'scheduled':scheduled,'pins':PINS,'leases':[],
              'owner_hash_recomputed':False,
              'scope':'Only three exact create-image-pull lease tuples; shared dependency leases untouched.'}
    usage = RegistryUsageStore(config.registry_usage_file()) if args.apply else None
    try:
        with registry_maintenance_lock(REGISTRY_MAINTENANCE_LOCK, timeout_seconds=10), _registry_lease_coordination():
            result['idle'] = helper['idle_guard'](config)
            require(result['idle']['live_nodes_checked']==0, 'Qualification fleet must be fully retired')
            exact_repositories = {row['repository'] for row in rows if row['image_id'] in EXPECTED}
            current = exact_leases(config.registry_usage_file(), exact_repositories)
            selected = []
            for row in rows:
                if row['image_id'] not in EXPECTED:
                    continue
                leases = [lease for lease in current if lease.repository==row['repository']]
                require(len(leases)==1, 'Unexpected lease set for exact owned image')
                selected.append((row, leases[0], validate_lease(leases[0], row)))
            require(len(selected)==3, 'Expected exactly three owned pull leases')
            for row, lease, expected in selected:
                entry = {'image_id':row['image_id'], **expected, 'released':False}
                if args.apply:
                    # This existing API owns its SQLite writer transaction;
                    # nesting it inside lease_fence would deadlock a connection.
                    # Host registry coordination remains held across validation
                    # and release, matching the lease acquisition path.
                    require(usage.release_lease(lease.repository, lease.tag, lease.owner), 'Lease was not active')
                    entry['released'] = True
                result['leases'].append(entry)
            if args.apply:
                require(not exact_leases(config.registry_usage_file(), exact_repositories), 'Owned image lease remained')
            result['complete'] = True
    except Exception as error:
        result['error_type'] = type(error).__name__
        raise
    finally:
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        helper['save'](args.output,result)
    print(json.dumps({'complete':True,'apply':args.apply,'leases':len(result['leases'])}))


if __name__=='__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'complete':False,'error_type':type(error).__name__}))
        raise SystemExit(1) from None
