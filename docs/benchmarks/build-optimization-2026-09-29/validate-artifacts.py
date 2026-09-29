#!/usr/bin/env python3
"""Validate retained qualification evidence locally; no production access."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]


def read(name):
    return json.loads((ROOT / name).read_text())


def main():
    deployment = read('deployment-receipt.json')
    assert deployment['health']['gateway_https'] and deployment['health']['relay_https']
    assert deployment['configuration_changed_keys'] == ['node_package_root']
    assert deployment['dependencies_unchanged'] and deployment['native_bundle_files_unchanged']
    identities = set()
    for phase, comparison in [('opt-repeat', 'repeat-comparison.json'), ('opt-fresh', 'fresh-comparison.json')]:
        summary = read(phase + '/summary.json')
        assert summary['succeeded'] == summary['submitted_cases'] == summary['durable_history_records'] == 48
        assert summary['failed_or_incomplete'] == 0
        assert all(value is True for value in read(comparison)['candidate_evidence_gates'].values())
        for record in summary['records']:
            assert record['build_id'] not in identities
            identities.add(record['build_id'])
            metrics = record['build']['timings']['environment']
            assert metrics['selective_materializations'] == metrics['docker_pull_skipped'] == 1
    for name in ['python-abba', 'node-tools-abba', 'node-runtime-abba']:
        result = read(name + '.json')
        assert result['equivalent'] and result['registry_writes'] == 0 and len(result['trials']) == 4
    for name in ['links', 'whiteout-opaque', 'missing-parent']:
        assert read(name + '-comparison.json')['equivalent']
    semantic = read('semantic-runtime.json')
    assert semantic['complete'] and len(semantic['cases']) == 3
    assert all(case['verified'] and case['cleanup']['deleted'] for case in semantic['cases'])
    apps = read('smoke-opt-fresh.json')
    assert len(apps) == 3 and all(case['verified'] and case['deleted'] for case in apps)
    final = read('final-state.json')
    assert final['all_test_nodes_retired'] and not final['test_nodes_remaining_at_provider']
    assert not final['sandbox_count'] and not final['active_builds'] and not final['gateway_sampler_active']
    assert final['successful_history_count'] == final['history_count'] == 100
    assert final['semantic_runtime_complete']
    cache = read('image-inventory-final.json')['cache_prune_dry_run']
    assert cache['inventoried_tags'] == cache['retained_entries'] == 64
    assert cache['retained_bytes'] <= cache['max_bytes']
    preparation = read('fresh-preparation-receipt.json')
    assert preparation['fixture_count'] == 48 and preparation['original_inputs_unchanged']
    assert preparation['manifest_sha256'] == hashlib.sha256((ROOT / 'fresh-fixture-manifests.json').read_bytes()).hexdigest()
    wheel = Path('/tmp/ucloud-build-optimization-20260929/ucloud_sandboxes-0.7.0-py3-none-any.whl')
    assert hashlib.sha256(wheel.read_bytes()).hexdigest() == deployment['wheel_sha256']
    runtime = {}
    with zipfile.ZipFile(wheel) as archive:
        for name in ['oci_layer_materialize', 'environment_builder', 'build_history', 'control_plane', 'node_agent']:
            path = REPO / 'ucloud_sandboxes' / (name + '.py')
            payload = path.read_bytes()
            assert payload == archive.read('ucloud_sandboxes/' + name + '.py')
            runtime[name] = hashlib.sha256(payload).hexdigest()
    checks = {'successful_measured_builds': len(identities), 'successful_durable_history': 100,
              'live_exact_real_image_comparisons': 3, 'live_exact_semantic_comparisons': 3,
              'verified_deleted_application_sandboxes': 3, 'verified_deleted_semantic_sandboxes': 3,
              'remaining_test_vms': 0, 'cache_entries': 64, 'deployed_runtime_sources_match_wheel': True}
    fingerprints = {str(path.relative_to(REPO)): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in sorted(ROOT.rglob('*')) if path.is_file()
                    and '__pycache__' not in path.parts and path.name != 'validation.json'}
    output = {'captured_at': datetime.now(timezone.utc).isoformat(), 'checks': checks,
              'runtime_source_sha256': runtime, 'artifact_sha256': fingerprints}
    (ROOT / 'validation.json').write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps(checks))


if __name__ == '__main__':
    main()
