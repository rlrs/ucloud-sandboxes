#!/usr/bin/env python3
"""Read-only final audit on the gateway using its existing Hetzner environment.

The sole write is a new local audit receipt. No provider, reservation, service,
sandbox, image, registry, or database mutations are performed. Credentials are
read by the deployed SDK/provider only and are never included in the output.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
from uuid import UUID

ROOT = Path('/work/ucloud-sandboxes/registry-io-load-20260929')
CONTROLLER = Path('/work/ucloud-sandboxes/registry-io-20260929-r1/deployment-controller.py')
BENCH_ROOT = Path('/work/ucloud-sandboxes/build-load-20260929')
RECIPES = {'python-agent', 'typescript-tools', 'typescript-multistage'}


def known_node(value):
    value = str(value)
    if not re.fullmatch(r'[1-9][0-9]{0,19}', value):
        raise ValueError('Expected a numeric Hetzner test-node ID')
    return value


def node_ids_from_fleet(fleet):
    return {known_node(item['job_id']) for item in fleet if item.get('job_id')}


def input_records(root, additional_nodes):
    phase_path = root / 'io-repeat/summary.json'
    phase = json.loads(phase_path.read_text())
    if phase.get('phase') != 'io-repeat':
        raise ValueError('Expected the io-repeat phase receipt')
    records = phase['records']
    if len(records) != 48:
        raise ValueError('Expected exactly 48 measured build records')
    identities = [(item['build_id'], item['image_id']) for item in records]
    if len({build for build, _ in identities}) != 48 or len({image for _, image in identities}) != 48:
        raise ValueError('Measured build/image IDs must each be distinct')
    for build, image in identities:
        if str(UUID(build)) != build or not image.startswith('bl20260929-io-repeat-'):
            raise ValueError('Unexpected measured build/image identity')
    nodes = {known_node(value) for value in additional_nodes}
    for key in ('fleet_before', 'fleet_after'):
        nodes.update(node_ids_from_fleet(phase.get(key, [])))
    nodes.update(node_ids_from_fleet([item.get('build', {}).get('node') or {} for item in records]))
    before_path = root / 'io-repeat/before.json'
    if before_path.exists():
        before = json.loads(before_path.read_text())
        nodes.update(node_ids_from_fleet(before.get('fleet', [])))
    application = json.loads((root / 'smoke-io-repeat.json').read_text())
    return phase, records, nodes, application, before_path.exists()


def exact_history(path, records):
    results = []
    # This is an operator-only audit, not a public SDK history fallback.
    with sqlite3.connect(path.absolute().as_uri() + '?mode=ro', uri=True) as db:
        for record in records:
            row = db.execute('SELECT summary_json FROM terminal_builds WHERE build_id=?',
                             (record['build_id'],)).fetchone()
            observed = json.loads(row[0]) if row else {}
            results.append({'build_id': record['build_id'], 'image_id': record['image_id'],
                            'status': observed.get('status'),
                            'identity_matches': observed.get('image_id') == record['image_id']
                                and observed.get('build_id') == record['build_id']})
    return results


def sampler_state(unit):
    if not re.fullmatch(r'[A-Za-z0-9_.@-]{1,150}', unit):
        raise ValueError('Invalid sampler unit')
    process = subprocess.run(['systemctl', 'show', unit, '--property=LoadState', '--property=ActiveState'],
                             capture_output=True, text=True, check=False, timeout=15)
    state = dict(line.split('=', 1) for line in process.stdout.splitlines() if '=' in line)
    return {'unit': unit, 'command_ok': process.returncode == 0,
            'load_state': state.get('LoadState'), 'active_state': state.get('ActiveState'),
            'inactive': process.returncode in {0, 1} and state.get('LoadState') in {'loaded', 'not-found'}
                and state.get('ActiveState') in {'inactive', 'failed'}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--known-node', nargs='+', action='extend', default=[],
                        help='Additional owned canary/worker/provider IDs, beyond receipt fleets')
    parser.add_argument('--sampler-unit', action='append',
                        help='Actual temporary gateway sampler unit; repeat if multiple were used')
    parser.add_argument('--output', type=Path, default=ROOT / 'final-state.json')
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('Audit output already exists; choose a new --output to preserve the earlier observation')
    phase, records, known_nodes, application, before_present = input_records(ROOT, args.known_node)
    sys.path.insert(0, str(BENCH_ROOT))
    import live_build_load_benchmark as bench
    from ucloud_sandboxes.providers.hetzner.composition import provider_from_configuration

    _, config, factory = bench.clients()
    client = factory()
    instances = provider_from_configuration(config.provider).list_instances()
    live_ids = {str(instance.id) for instance in instances}
    history = exact_history(config.metrics_path().with_name('build-history.sqlite'), records)
    spec = importlib.util.spec_from_file_location('registry_io_deployed_health_audit', CONTROLLER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    samplers = [sampler_state(unit) for unit in (args.sampler_unit or ['ucloud-build-load-monitor'])]
    prepared = client.list_prepared_builders()
    capacity = client.list_prepared_capacity()
    demand = prepared.get('demand')
    demand_known = isinstance(demand, dict) and all(key in demand for key in
        ('pending_count', 'suppressed_pending_count', 'pending_image_builds', 'prepared_builder_count', 'pending', 'prepared'))
    pending_absent = (demand_known and prepared.get('prepared_builders') == [] and capacity.get('prepared') == []
        and all(demand[key] == 0 for key in ('pending_count', 'suppressed_pending_count',
                                           'pending_image_builds', 'prepared_builder_count'))
        and demand['pending'] == [] and demand['prepared'] == [])
    phase_success = (phase.get('succeeded') == 48 and phase.get('failed_or_incomplete') == 0
                     and all(item.get('build', {}).get('status') == 'succeeded' for item in records))
    smoke_ids = {item.get('sandbox_id') for item in application}
    smoke_ok = (len(application) == 3 and len(smoke_ids) == 3
                and {item.get('recipe') for item in application} == RECIPES
                and all(item.get('verified') is True and item.get('deleted') is True
                        and item.get('exit_code') == 0 for item in application)
                and all(item.get('image_id') in {row['image_id'] for row in records} for item in application))
    result = {
        'captured_at': bench.stamp(), 'health': module.health(), 'fleet': bench.fleet(config),
        'sandbox_count': len(client.list_sandboxes()),
        'active_builds': sum(item['status'] not in {'succeeded', 'failed'} for item in client.list_image_builds()),
        'prepared': prepared, 'prepared_capacity': capacity, 'pending_reservations_absent': pending_absent,
        'known_test_node_ids': sorted(known_nodes), 'before_receipt_present': before_present,
        'additional_known_node_ids': sorted({known_node(value) for value in args.known_node}),
        'test_nodes_remaining_at_provider': sorted(known_nodes & live_ids),
        'all_test_nodes_retired': bool(known_nodes) and not (known_nodes & live_ids),
        'provider_instances': [{'id': str(item.id), 'name': item.name, 'state': item.state,
                               'product_id': item.product_id} for item in instances],
        'samplers': samplers, 'gateway_sampler_active': any(not item['inactive'] for item in samplers),
        'measured_build_count': len(records), 'measured_phase_succeeded': phase_success,
        'application_sandboxes_verified_and_deleted': sum(
            item.get('verified') is True and item.get('deleted') is True for item in application),
        'application_smoke_gate_passed': smoke_ok,
        'history_source': 'gateway build-history.sqlite, read-only exact-UUID lookup',
        'successful_history_count': sum(item['status'] == 'succeeded' and item['identity_matches'] for item in history),
        'history_count': len(history), 'history': history,
        'retained_artifacts': ['48 measured managed test image aliases', 'owned mount/fallback canary references',
                               'frozen fixture contexts and dependency locks', 'qualification receipts and local raw progress logs'],
    }
    result['audit_complete'] = bool(phase_success and smoke_ok and result['successful_history_count'] == 48
        and result['sandbox_count'] == 0 and result['active_builds'] == 0 and not result['fleet']
        and pending_absent and result['all_test_nodes_retired'] and not result['gateway_sampler_active'])
    bench.write_json(args.output, result)
    print(json.dumps({key: value for key, value in result.items() if key != 'history'}))
    if not result['audit_complete']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
