#!/usr/bin/env python3
"""Read-only single-import phase production audit; only create a new local receipt.

Run using the gateway venv and its existing SDK/provider configuration. No
provider, reservation, service, registry, sandbox or database writes occur.
Credential values and workload bodies/logs are never included in the receipt.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import sqlite3
import subprocess
import sys
import tempfile
from uuid import NAMESPACE_URL, UUID, uuid5
import zipfile


ROOT = Path('/work/ucloud-sandboxes/build-cache-single-import-load-20260929')
RELEASE = Path('/work/ucloud-sandboxes/build-cache-single-import-20260929-r1')
BENCH_ROOT = Path('/work/ucloud-sandboxes/build-load-20260929')
RECIPES = {'python-agent', 'typescript-tools', 'typescript-multistage'}
PHASES = ('single-import-repeat',)
VARIANTS = {'app-change-' + str(index) for index in range(5, 21)}
SAMPLERS = ('ucloud-single-import-load-monitor', 'ucloud-build-load-client-single-import-repeat')
WHEEL_NAME = 'ucloud_sandboxes-0.7.0-py3-none-any.whl'
HARNESS_SHA256 = 'd0b2754af69c69d9a303fd15117f60fad044ca0e559b3a1cfbe934f49682c6ec'
INVENTORY_SHA256 = '45efcdbd714d7fbc6748b26f7fcdc29b4b18d1f4117baac0feb8af24e32505a7'
BUILDKIT_CONFIG_SHA256 = 'ca46c1e0f19982ff9ef00df1b096674023b2d2a6603074abbcad7a8700c9de8d'
SDK_SHA256 = 'd15b65fbb5e1570fde69cb9d571789a9b61d4418682efc17732bdc9c2ca8414c'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def read_json(path):
    require(path.stat().st_size <= 16 * 1024 * 1024, 'Receipt exceeds size bound')
    return json.loads(path.read_text())


def known_node(value):
    value = str(value)
    require(bool(re.fullmatch(r'[1-9][0-9]{0,19}', value)), 'Expected numeric test-node ID')
    return value


def node_ids(fleet):
    return {known_node(row['job_id']) for row in fleet if row.get('job_id')}


def phase_inputs(root, name):
    summary_path = root / name / 'summary.json'
    smoke_path = root / ('smoke-' + name + '.json')
    before_path = root / name / 'before.json'
    require(name in PHASES, 'Unexpected qualification phase')
    smoke_required = True
    summary = read_json(summary_path)
    smoke = read_json(smoke_path) if smoke_required else []
    require(summary.get('phase') == name, 'Unexpected phase')
    records = summary['records']
    require(isinstance(records, list) and len(records) == 48, 'Expected exactly 48 records')
    identities = [(record['build_id'], record['image_id']) for record in records]
    require(len({build for build, _ in identities}) == 48, 'Duplicate build identity')
    require(len({image for _, image in identities}) == 48, 'Duplicate image identity')
    cases = {(record['recipe'], record['variant']) for record in records}
    require(cases == {(recipe, variant) for recipe in RECIPES for variant in VARIANTS},
            'Expected all 48 frozen recipe/variant pairs')
    require(all(isinstance(record.get('context_sha256'), str)
                and re.fullmatch(r'[0-9a-f]{64}', record['context_sha256']) for record in records),
            'Expected context SHA256 for every build')
    for build, image in identities:
        require(str(UUID(build)) == build, 'Malformed build UUID')
        require(bool(re.fullmatch('bl20260929-' + re.escape(name) + r'-[0-9]{3}', image)),
                'Unexpected benchmark image identity')
    build_ok = (summary.get('succeeded') == 48 and summary.get('failed_or_incomplete') == 0
                and all(record.get('build', {}).get('status') == 'succeeded' for record in records))
    require(isinstance(smoke, list), 'Expected smoke list')
    sandbox_ids = [row.get('sandbox_id') for row in smoke]
    image_recipes = {row['image_id']: row['recipe'] for row in records}
    smoke_ok = (not smoke_required) or (len(smoke) == 3 and len(set(sandbox_ids)) == 3
                and len({row.get('image_id') for row in smoke}) == 3
                and all(isinstance(value, str) and value.startswith('bl-smoke-' + name + '-')
                        for value in sandbox_ids)
                and {row.get('recipe') for row in smoke} == RECIPES
                and all(row.get('verified') is True and row.get('deleted') is True
                        and type(row.get('exit_code')) is int and row['exit_code'] == 0
                        and row.get('image_id') in image_recipes
                        and image_recipes[row['image_id']] == row.get('recipe') for row in smoke))
    nodes = node_ids([record.get('build', {}).get('node') or {} for record in records])
    for key in ('fleet_before', 'fleet_after'):
        nodes.update(node_ids(summary.get(key, [])))
    require(before_path.is_file(), 'Before-fleet receipt is required')
    before_nodes = node_ids(read_json(before_path).get('fleet', []))
    require(len(before_nodes) == 4, 'Expected four initial builders per phase')
    owners = node_ids([record.get('build', {}).get('node') or {} for record in records])
    require(owners == before_nodes, 'Build owners must be the four initial builders')
    nodes.update(before_nodes)
    evidence = {'phase': name, 'records': len(records), 'phase_succeeded': build_ok,
                'application_smoke_required': smoke_required, 'application_smoke_gate_passed': smoke_ok,
                'initial_builder_ids': sorted(before_nodes),
                'smoke_sandbox_ids': sandbox_ids,
                'successful_deleted_smokes': sum(row.get('verified') is True and row.get('deleted') is True
                                                and row.get('exit_code') == 0 for row in smoke),
                'known_node_ids': sorted(nodes),
                'input_sha256': {str(path): sha(path) for path in
                    [summary_path, before_path] + ([smoke_path] if smoke_required else [])}}
    return records, nodes, evidence


def combine_inputs(loaded):
    require(len(loaded) == 1 and loaded[0][2]['phase'] == PHASES[0], 'Expected one single-import-repeat phase')
    records, nodes, evidence = loaded[0]
    require(len(records) == 48 and len({row['build_id'] for row in records}) == 48,
            'Expected 48 distinct build UUIDs')
    return records, nodes, [evidence]


def qualification_inputs(root, records, expected_wheel):
    path = root / (PHASES[0] + '.qualification.json')
    item = read_json(path)
    require(item.get('phase') == PHASES[0] and item.get('ran') is True
            and type(item.get('returncode')) is int and item['returncode'] == 0,
            'Qualification wrapper did not complete successfully')
    require(item.get('count') == 48 and item.get('concurrency') == 48
            and item.get('expected_builders') == 4, 'Qualification workload changed')
    require(item.get('inventory_sha256') == INVENTORY_SHA256
            and item.get('harness_sha256') == HARNESS_SHA256
            and item.get('declared_artifact_sha256') == expected_wheel, 'Qualification identity changed')
    verified = item.get('verified_contexts')
    require(isinstance(verified, list) and len(verified) == 48, 'Frozen context coverage missing')
    observed = {(row['recipe'], row['variant']): row['context_sha256'] for row in verified}
    expected = {(row['recipe'], row['variant']): row['context_sha256'] for row in records}
    require(observed == expected, 'Measured contexts differ from frozen qualification')
    launch_path = root / 'run-launch.json'
    launch = read_json(launch_path)
    require(launch.get('ran') is True and type(launch.get('returncode')) is int and launch['returncode'] == 0
            and launch.get('unit') == 'ucloud-build-load-client-single-import-repeat'
            and launch.get('candidate_wheel_sha256') == expected_wheel
            and launch.get('sdk_wheel_sha256') == SDK_SHA256
            and launch.get('harness_sha256') == HARNESS_SHA256,
            'Classified SDK driver launch evidence missing or changed')
    return {'receipt_sha256': sha(path), 'frozen_contexts_verified': 48,
            'inventory_sha256': INVENTORY_SHA256, 'launch_receipt_sha256': sha(launch_path),
            'driver_service_unit': launch['unit']}


def builder_receipts(root, pool, wheel):
    result = []
    names = ('images.py', 'build_cache.py', 'environment_prepare.py', 'environment_builder.py')
    with zipfile.ZipFile(wheel) as archive:
        expected = {name: hashlib.sha256(archive.read('ucloud_sandboxes/' + name)).hexdigest() for name in names}
    for node in sorted(pool):
        path = root / ('builder-' + known_node(node) + '-initial.json')
        item = read_json(path)
        require(item.get('node_active') is True and item.get('buildkit_cache_empty') is True,
                'Initial builder readiness/cache evidence missing')
        require(all(item.get('source_hashes', {}).get(name) == digest for name, digest in expected.items()),
                'Initial builder source differs from candidate wheel')
        require(item.get('buildkit_config_sha256') == BUILDKIT_CONFIG_SHA256
                and ' v0.33.0 ' in item.get('buildkit_version', ''), 'Builder configuration/version changed')
        result.append({'node_id': node, 'receipt_sha256': sha(path), 'source_hashes': expected,
                       'initial_buildkit_cache_empty': True, 'configuration_matches': True})
    return result


def exact_history(path, records):
    results = []
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


def fleet_count(path):
    # Avoid ControlStateStore construction: it initializes/chmods/migrates the
    # state file. This gate needs only the existing heartbeat row count.
    with sqlite3.connect(path.absolute().as_uri() + '?mode=ro', uri=True) as db:
        return db.execute("SELECT COUNT(*) FROM control_records WHERE namespace='heartbeat'").fetchone()[0]


def sampler_state(unit):
    require(bool(re.fullmatch(r'[A-Za-z0-9_.@-]{1,150}', unit)), 'Invalid sampler unit')
    process = subprocess.run(['systemctl', 'show', unit, '--property=LoadState', '--property=ActiveState'],
                             capture_output=True, text=True, check=False, timeout=15)
    state = dict(line.split('=', 1) for line in process.stdout.splitlines() if '=' in line)
    return {'unit': unit, 'load_state': state.get('LoadState'), 'active_state': state.get('ActiveState'),
            'inactive': process.returncode in {0, 1} and state.get('LoadState') in {'loaded', 'not-found'}
                and state.get('ActiveState') in {'inactive', 'failed'}}


def wheel_files(wheel, package_root):
    mismatches, count = [], 0
    with zipfile.ZipFile(wheel) as archive:
        names = [name for name in archive.namelist() if name.startswith('ucloud_sandboxes/')
                 and not name.endswith('/')]
        require(names and len(names) == len(set(names)), 'Empty or duplicate wheel inventory')
        for name in names:
            relative = PurePosixPath(name).relative_to('ucloud_sandboxes')
            require('..' not in relative.parts and not relative.is_absolute(), 'Invalid packaged path')
            target = package_root.joinpath(*relative.parts)
            if not target.is_file() or sha(target) != hashlib.sha256(archive.read(name)).hexdigest():
                mismatches.append(name)
            count += 1
    return {'packaged_files_compared': count, 'mismatched_files': mismatches, 'all_packaged_files_match': not mismatches}


def source_identity(expected):
    require(bool(re.fullmatch(r'[0-9a-f]{64}', expected)), 'Expected exact SHA256')
    staged = read_json(RELEASE / 'staging-receipt.json')
    deployed = read_json(RELEASE / 'deployment-receipt.json')
    require(staged['wheel_sha256'] == deployed['wheel_sha256'] == expected, 'Release receipt hash differs')
    require(deployed['staging_receipt_sha256'] == sha(RELEASE / 'staging-receipt.json'), 'Staging receipt changed')
    require(sha(RELEASE / WHEEL_NAME) == expected, 'Wheel digest mismatch')
    controller = RELEASE / 'deployment-controller.py'
    require(sha(controller) == staged['controller_sha256'], 'Controller changed after staging')
    bundles = {}
    for role in ('builder', 'sandbox'):
        bundles[role] = sha(RELEASE / (role + '-node-package.tar.gz'))
        require(bundles[role] == staged['bundles'][role]['sha256'], 'Candidate bundle changed')
    import ucloud_sandboxes
    inventory = wheel_files(RELEASE / WHEEL_NAME, Path(ucloud_sandboxes.__file__).parent)
    require(inventory['all_packaged_files_match'], 'Installed package differs from candidate wheel')
    spec = importlib.util.spec_from_file_location('build_cache_single_import_health_audit', controller)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    require(module.ROOT == RELEASE, 'Controller targets another release')
    raw, _ = module.read_config()
    require(Path(raw['node_package_root']) == RELEASE, 'Future-node bundle root differs')
    return {'wheel_sha256': expected, 'controller_sha256': sha(controller), 'bundle_sha256': bundles,
            'configured_node_package_root': str(RELEASE), **inventory}, module


def pending_absent(prepared, capacity):
    demand = prepared.get('demand')
    required = ('pending_count', 'suppressed_pending_count', 'pending_image_builds', 'prepared_builder_count')
    return (isinstance(demand, dict) and all(type(demand.get(key)) is int and demand[key] == 0 for key in required)
            and prepared.get('prepared_builders') == [] and capacity.get('prepared') == []
            and demand.get('pending') == [] and demand.get('prepared') == [])


def complete(result):
    return bool(all(phase['phase_succeeded'] and phase['application_smoke_gate_passed'] for phase in result['phases'])
        and result['successful_history_count'] == 48 and result['history_count'] == 48
        and result['sandbox_count'] == 0 and result['active_builds'] == 0 and result['fleet_count'] == 0
        and result['pending_reservations_absent'] and result['all_test_nodes_retired']
        and not result['gateway_sampler_active'] and result['source_identity']['all_packaged_files_match'])


def audit(args):
    require(not args.output.exists(), 'Choose a new output to preserve earlier receipts')
    records, nodes, phases = combine_inputs([phase_inputs(ROOT, phase) for phase in PHASES])
    nodes.update(known_node(value) for value in args.known_node)
    source, controller = source_identity(args.wheel_sha256)
    qualification = qualification_inputs(ROOT, records, args.wheel_sha256)
    initial_builders = builder_receipts(ROOT, phases[0]['initial_builder_ids'], RELEASE / WHEEL_NAME)
    require(sha(BENCH_ROOT / 'live_build_load_benchmark.py') == HARNESS_SHA256, 'Frozen harness changed')
    sys.path.insert(0, str(BENCH_ROOT))
    import live_build_load_benchmark as bench
    require(sha(Path(bench.SDK_WHEEL)) == SDK_SHA256, 'Frozen SDK 0.4.33 wheel changed')
    from ucloud_sandboxes.providers.hetzner.composition import provider_from_configuration
    _, config, factory = bench.clients()
    client = factory()
    instances = provider_from_configuration(config.provider).list_instances()
    live_ids = {str(instance.id) for instance in instances}
    history = exact_history(config.metrics_path().with_name('build-history.sqlite'), records)
    units = sorted(set(SAMPLERS) | set(args.sampler_unit))
    samplers = [sampler_state(unit) for unit in units]
    health = controller.health()  # Both authenticated loopback and public HTTPS /healthz.
    idle = controller.idle_guard()  # Also requires zero outstanding relay/lifecycle work.
    result = {'captured_at': bench.stamp(), 'source_identity': source, 'health': health, 'idle_guard': idle,
        'phases': phases, 'measured_build_count': len(records),
        'qualification': qualification, 'initial_builders': initial_builders,
        'harness_sha256': HARNESS_SHA256, 'sdk_wheel_sha256': SDK_SHA256,
        'sandbox_count': len(client.list_sandboxes()),
        'active_builds': sum(row['status'] not in {'succeeded', 'failed'} for row in client.list_image_builds()),
        'fleet_count': fleet_count(config.control_state_file()),
        'pending_reservations_absent': pending_absent(client.list_prepared_builders(), client.list_prepared_capacity()),
        'known_test_node_ids': sorted(nodes), 'test_nodes_remaining_at_provider': sorted(nodes & live_ids),
        'all_test_nodes_retired': bool(nodes) and not (nodes & live_ids),
        'provider_instances': [{'id': str(row.id), 'state': row.state, 'product_id': row.product_id} for row in instances],
        'samplers': samplers, 'gateway_sampler_active': any(not row['inactive'] for row in samplers),
        'history_source': 'gateway build-history.sqlite, read-only exact-UUID lookups for the measured phase',
        'successful_history_count': sum(row['status'] == 'succeeded' and row['identity_matches'] for row in history),
        'history_count': len(history), 'history': history,
        'retained_artifacts': ['48 measured managed test image aliases', 'local synthetic benchmark receipts',
                               'frozen fixture contexts and dependency locks', 'qualification receipts and local raw progress logs'],
        'limitations': ['Historical cache/placement chronology limits causal attribution of a single fresh-pool comparison.',
                        'This final audit does not prove cache hits or semantic invalidation; retain separate cache-witness and canary evidence.',
                        'This final idle audit does not establish health during the measured burst or agent-sandbox capacity.']}
    result['audit_complete'] = complete(result)
    # Exclusive creation preserves prior observations and cannot overwrite an input.
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write('\n')
    print(json.dumps({key: result[key] for key in ('captured_at', 'audit_complete', 'measured_build_count',
        'successful_history_count', 'test_nodes_remaining_at_provider', 'pending_reservations_absent', 'gateway_sampler_active')}))
    return 0 if result['audit_complete'] else 1


def self_test():
    with tempfile.TemporaryDirectory(prefix='single-import-audit-selftest-') as temporary:
        root = Path(temporary)
        loaded = []
        for phase_index, phase in enumerate(PHASES):
            pool = [str(167990000 + phase_index * 4 + index) for index in range(4)]
            (root / phase).mkdir()
            records = [{'build_id': str(uuid5(NAMESPACE_URL, phase + str(index))),
                        'image_id': f'bl20260929-{phase}-{index:03}', 'recipe': sorted(RECIPES)[index % 3],
                        'variant': 'app-change-' + str(5 + index // 3),
                        'context_sha256': hashlib.sha256(str(index).encode()).hexdigest(),
                        'build': {'status': 'succeeded',
                        'node': {'job_id': pool[index % 4]}}} for index in range(48)]
            summary = {'phase': phase, 'records': records, 'succeeded': 48, 'failed_or_incomplete': 0}
            smoke = [{'recipe': recipe, 'sandbox_id': f'bl-smoke-{phase}-{index}',
                      'image_id': records[index]['image_id'], 'verified': True, 'deleted': True, 'exit_code': 0}
                     for index, recipe in enumerate(sorted(RECIPES))]
            (root / phase / 'summary.json').write_text(json.dumps(summary))
            (root / phase / 'before.json').write_text(json.dumps({'fleet': [{'job_id': node} for node in pool]}))
            smoke_path = root / ('smoke-' + phase + '.json')
            if phase == 'single-import-repeat':
                smoke_path.write_text(json.dumps(smoke))
            loaded.append(phase_inputs(root, phase))
            smoke[0]['deleted'] = False
            smoke_path.write_text(json.dumps(smoke))
            assert not phase_inputs(root, phase)[2]['application_smoke_gate_passed']
            smoke[0]['deleted'] = True
            original = smoke[0]['image_id']
            smoke[0]['image_id'] = records[1]['image_id']
            smoke_path.write_text(json.dumps(smoke))
            assert not phase_inputs(root, phase)[2]['application_smoke_gate_passed']
            smoke[0]['image_id'] = original
            smoke[0]['recipe'], smoke[1]['recipe'] = smoke[1]['recipe'], smoke[0]['recipe']
            smoke_path.write_text(json.dumps(smoke))
            assert not phase_inputs(root, phase)[2]['application_smoke_gate_passed']
            smoke[0]['recipe'], smoke[1]['recipe'] = smoke[1]['recipe'], smoke[0]['recipe']
            smoke[0]['image_id'] = 'bl20260929-foreign-000'
            smoke_path.write_text(json.dumps(smoke))
            assert not phase_inputs(root, phase)[2]['application_smoke_gate_passed']
            summary['records'][1]['build_id'] = summary['records'][0]['build_id']
            (root / phase / 'summary.json').write_text(json.dumps(summary))
            try:
                phase_inputs(root, phase)
            except ValueError:
                pass
            else:
                raise AssertionError('duplicate UUID accepted')
        records, nodes, phases = combine_inputs(loaded)
        assert len(records) == 48 and nodes == {str(167990000 + index) for index in range(4)}
        assert phases[0]['successful_deleted_smokes'] == 3
        try:
            combine_inputs([loaded[0], loaded[0]])
        except ValueError:
            pass
        else:
            raise AssertionError('Multiple measured phases accepted')
        qualification = {'phase': PHASES[0], 'ran': True, 'returncode': 0, 'count': 48,
            'concurrency': 48, 'expected_builders': 4, 'inventory_sha256': INVENTORY_SHA256,
            'harness_sha256': HARNESS_SHA256, 'declared_artifact_sha256': 'a' * 64,
            'verified_contexts': [{key: row[key] for key in ('recipe', 'variant', 'context_sha256')} for row in records]}
        (root / 'run-launch.json').write_text(json.dumps({'ran': True, 'returncode': 0,
            'unit': 'ucloud-build-load-client-single-import-repeat', 'candidate_wheel_sha256': 'a' * 64,
            'sdk_wheel_sha256': SDK_SHA256, 'harness_sha256': HARNESS_SHA256}))
        qual_path = root / (PHASES[0] + '.qualification.json')
        qual_path.write_text(json.dumps(qualification))
        assert qualification_inputs(root, records, 'a' * 64)['frozen_contexts_verified'] == 48
        qualification['verified_contexts'][0]['context_sha256'] = 'b' * 64
        qual_path.write_text(json.dumps(qualification))
        try:
            qualification_inputs(root, records, 'a' * 64)
        except ValueError:
            pass
        else:
            raise AssertionError('Changed fixture digest accepted')
        db_path = root / 'history.sqlite'
        with sqlite3.connect(db_path) as db:
            db.execute('CREATE TABLE terminal_builds(build_id TEXT PRIMARY KEY, summary_json TEXT)')
            for row in records:
                db.execute('INSERT INTO terminal_builds VALUES (?,?)', (row['build_id'], json.dumps({
                    'build_id': row['build_id'], 'image_id': row['image_id'], 'status': 'succeeded'})))
        history = exact_history(db_path, records)
        assert len(history) == 48 and all(row['identity_matches'] and row['status'] == 'succeeded' for row in history)
        assert not exact_history(db_path, [{**records[0], 'image_id': 'wrong'}])[0]['identity_matches']
        assert exact_history(db_path, [{'build_id': str(uuid5(NAMESPACE_URL, 'absent')), 'image_id': 'missing'}])[0]['status'] is None
        state_path = root / 'control.sqlite'
        with sqlite3.connect(state_path) as db:
            db.execute('CREATE TABLE control_records(namespace TEXT, record_id TEXT)')
            db.executemany('INSERT INTO control_records VALUES (?,?)', [('bootstrap', '1'), ('heartbeat', '2')])
        before = (sha(state_path), state_path.stat().st_mode)
        assert fleet_count(state_path) == 1
        assert (sha(state_path), state_path.stat().st_mode) == before
        missing_state = root / 'absent.sqlite'
        try:
            fleet_count(missing_state)
        except sqlite3.OperationalError:
            assert not missing_state.exists()
        else:
            raise AssertionError('absent state file created')
        fixture = {'phases': phases, 'successful_history_count': 48, 'history_count': 48, 'sandbox_count': 0,
                   'active_builds': 0, 'fleet_count': 0, 'pending_reservations_absent': True,
                   'all_test_nodes_retired': True, 'gateway_sampler_active': False,
                   'source_identity': {'all_packaged_files_match': True}}
        assert complete(fixture)
        for field, value in (('all_test_nodes_retired', False), ('fleet_count', 4), ('gateway_sampler_active', True),
                             ('pending_reservations_absent', False), ('successful_history_count', 47)):
            assert not complete({**fixture, field: value})
        assert not pending_absent({}, {})
        demand = {key: 0 for key in ('pending_count', 'suppressed_pending_count', 'pending_image_builds', 'prepared_builder_count')}
        demand.update(pending=[], prepared=[])
        assert pending_absent({'demand': demand, 'prepared_builders': []}, {'prepared': []})
        demand['prepared_builder_count'] = 4
        assert not pending_absent({'demand': demand, 'prepared_builders': []}, {'prepared': []})
        package = root / 'package'
        package.mkdir()
        (package / '__init__.py').write_bytes(b'pass\n')
        wheel = root / 'fixture.whl'
        with zipfile.ZipFile(wheel, 'w') as archive:
            archive.writestr('ucloud_sandboxes/__init__.py', b'pass\n')
        assert wheel_files(wheel, package)['all_packaged_files_match']
        (package / '__init__.py').write_bytes(b'# changed\n')
        assert not wheel_files(wheel, package)['all_packaged_files_match']
        modules = ('images.py', 'build_cache.py', 'environment_prepare.py', 'environment_builder.py')
        with zipfile.ZipFile(wheel, 'w') as archive:
            for name in modules:
                archive.writestr('ucloud_sandboxes/' + name, name.encode())
        initial = {'node_active': True, 'buildkit_cache_empty': True,
            'source_hashes': {name: hashlib.sha256(name.encode()).hexdigest() for name in modules},
            'buildkit_config_sha256': BUILDKIT_CONFIG_SHA256,
            'buildkit_version': 'buildkitd github.com/moby/buildkit v0.33.0 test'}
        initial_path = root / 'builder-167990000-initial.json'
        initial_path.write_text(json.dumps(initial))
        assert len(builder_receipts(root, ['167990000'], wheel)) == 1
        initial['buildkit_cache_empty'] = False
        initial_path.write_text(json.dumps(initial))
        try:
            builder_receipts(root, ['167990000'], wheel)
        except ValueError:
            pass
        else:
            raise AssertionError('Nonempty initial builder cache accepted')
    print(json.dumps({'self_test': 'passed', 'network_calls': 0, 'production_calls': 0}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wheel-sha256', help='Exact single-import candidate wheel digest; mandatory for a live audit')
    parser.add_argument('--known-node', nargs='+', action='extend', default=[])
    parser.add_argument('--sampler-unit', action='append', default=[], help='Additional sampler; single-import sampler and SDK driver units are always checked')
    parser.add_argument('--output', type=Path, default=ROOT / 'final-state.json')
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if args.wheel_sha256 is None:
        parser.error('--wheel-sha256 is required for the live audit')
    return audit(args)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        # Keep unexpected SDK/provider/authentication failures out of exported logs.
        print(json.dumps({'audit_complete': False, 'error_type': type(exc).__name__}), file=sys.stderr)
        raise SystemExit(2) from None
