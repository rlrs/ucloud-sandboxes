#!/usr/bin/env python3
"""Read-only final audit; execute on the gateway with its Hetzner environment."""
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

ROOT = Path('/work/ucloud-sandboxes/build-load-20260929')
sys.path.insert(0, str(ROOT))
import live_build_load_benchmark as bench  # noqa: E402


def main():
    from ucloud_sandboxes.providers.hetzner.composition import provider_from_configuration

    _, config, factory = bench.clients()
    client = factory()
    results = [json.loads(path.read_text()) for path in ROOT.glob('*/summary.json')]
    records = [record for result in results for record in result['records']]
    known_nodes = {'167929368', '167931118'}
    for result in results:
        known_nodes.update(str(node['job_id']) for node in result['fleet_after'])
        known_nodes.update(str(record['build']['node']['job_id']) for record in result['records'])
    instances = provider_from_configuration(config.provider).list_instances()
    live_ids = {str(instance.id) for instance in instances}
    builds = []
    # Terminal history is an operator database, not a public SDK status fallback.
    history_path = config.metrics_path().with_name('build-history.sqlite')
    with sqlite3.connect(history_path.as_uri() + '?mode=ro', uri=True) as db:
        for record in records:
            row = db.execute('SELECT summary_json FROM terminal_builds WHERE build_id=?',
                             (record['build_id'],)).fetchone()
            observed = json.loads(row[0]) if row else {}
            builds.append({'build_id': record['build_id'], 'image_id': record['image_id'],
                           'status': observed.get('status'),
                           'identity_matches': observed.get('image_id') == record['image_id']
                               and observed.get('build_id') == record['build_id']})
    controller = Path('/work/ucloud-sandboxes/buildkit-cache-optimization-20260929-r3/deployment-controller.py')
    spec = importlib.util.spec_from_file_location('deployed_health_audit', controller)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    service = subprocess.run(['systemctl', 'is-active', '--quiet', 'ucloud-build-load-monitor'], check=False)
    result = {
        'captured_at': bench.stamp(), 'health': module.health(),
        'fleet': bench.fleet(config), 'sandbox_count': len(client.list_sandboxes()),
        'active_builds': sum(b['status'] not in {'succeeded', 'failed'} for b in client.list_image_builds()),
        'prepared': client.list_prepared_builders(),
        'known_test_node_ids': sorted(known_nodes),
        'test_nodes_remaining_at_provider': sorted(known_nodes & live_ids),
        'all_test_nodes_retired': not (known_nodes & live_ids),
        'provider_instances': [{'id': str(i.id), 'name': i.name, 'state': i.state, 'product_id': i.product_id} for i in instances],
        'gateway_sampler_active': service.returncode == 0,
        'history_source': 'gateway build-history.sqlite, read-only exact-UUID lookup',
        'successful_history_count': sum(b['status'] == 'succeeded' and b['identity_matches'] for b in builds),
        'history_count': len(builds), 'history': builds,
        'retained_artifacts': ['138 managed test image aliases', 'frozen fixture contexts and dependency locks',
                               'build receipts and gateway-local raw progress logs'],
    }
    bench.write_json(ROOT / 'final-state.json', result)
    print(json.dumps({key: value for key, value in result.items() if key != 'history'}))


if __name__ == '__main__':
    main()
