#!/usr/bin/env python3
"""Execute three frozen app assertions; require selective Python publication."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
from pathlib import Path
import time
from uuid import uuid4


OLD = Path('/work/ucloud-sandboxes/builder-slot-qualification-20260929')
HARNESS_SHA = '01b3679ac88ad767d9275fe7ea9d9b9d93834e4887e2bd13d5e2259c5c044531'
SDK_WHEEL = Path('/work/ucloud-sandboxes/build-reliability-20260929-r2/ucloud_sandboxes_sdk-0.4.34-py3-none-any.whl')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--summary', type=Path, required=True)
    parser.add_argument('--cases', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    import hashlib
    path = OLD / 'qualify_builder_slots.py'
    if hashlib.sha256(path.read_bytes()).hexdigest() != HARNESS_SHA:
        raise ValueError('Frozen qualification helper changed')
    spec = importlib.util.spec_from_file_location('qualify_builder_slots', path)
    q = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(q)
    q.require(not args.output.exists(), 'Do not overwrite smoke evidence')
    summary, cases = json.loads(args.summary.read_text()), json.loads(args.cases.read_text())
    q.require(summary['passed'] and summary['cases_sha256'] == q.sha(args.cases),
              'Require passed matching candidate qualification')
    q.require(summary['mode'] == 'cold' and summary['declared_slots'] == 6,
              'Expected candidate cold wave')
    selected = [r for r in summary['records'] if r['recipe'] in q.RECIPES
                and r['variant'] == 'app-change-20']
    q.require(len(selected) == 3 and {r['recipe'] for r in selected} == set(q.RECIPES),
              'Require exactly three frozen recipes')
    fixtures = {c['index']: c for c in cases['cases']}
    for record in selected:
        case = fixtures[record['index']]
        q.require(case['recipe'] == record['recipe'] and case['variant'] == record['variant']
                  and case['context_sha256'] == record['context_sha256'], 'Fixture binding changed')
        q.require(record['build']['status'] == 'succeeded'
                  and record['build']['image']['id'] == record['image_id'], 'Published image binding changed')
    python_record = next(r for r in selected if r['recipe'] == 'python-agent')
    metrics = python_record['build']['timings']['environment']
    q.require(metrics.get('selective_materializations', 0) > 0
              and metrics.get('selective_subprocess_ms', 0) > 0
              and not metrics.get('selective_fallbacks', 0), 'Require actual candidate selective publication')
    sdk = q.load_sdk(SDK_WHEEL)
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.control_state import ControlStateStore
    config = DeploymentConfig.from_dict(json.loads(Path('/etc/ucloud-sandboxes/deployment.json').read_text()))
    token = config.sandbox_api_token_file().read_text().strip()
    node_token = config.node_control_token_file().read_text().strip()
    def make_client():
        return sdk.SandboxClient('https://77.42.92.27', api_token=token, timeout_seconds=120)
    client = make_client()
    q.require(not client.list_sandboxes(), 'Require idle sandbox fleet')
    q.require(not any(r['status'] not in q.TERMINAL for r in client.list_image_builds()),
              'Require completed image builds')
    prefix = 'registry-pull-smoke-' + uuid4().hex[:12]
    owned = [prefix + '-' + str(r['index']) for r in selected]
    args.output.mkdir(mode=0o700, parents=True)
    q.write_json(args.output / 'launch.json', dict(at=q.stamp(), sandbox_ids=owned,
        image_ids=[r['image_id'] for r in selected], summary_sha256=q.sha(args.summary),
        cases_sha256=q.sha(args.cases), sdk_sha256=q.SDK_SHA256, helper_sha256=q.sha(__file__)))

    def run(pair):
        from urllib.request import Request, urlopen
        import sqlite3
        from ucloud_sandboxes.control_plane import _registry_operation_lease_owner
        from ucloud_sandboxes.managed_registry import image_ref_with_manifest_digest, registry_repository_tag_from_image_ref
        record, identity = pair
        case, api = fixtures[record['index']], make_client()
        result = dict(started_at=q.stamp(), sandbox_id=identity, image_id=record['image_id'],
            recipe=record['recipe'], variant=record['variant'],
            published_image_tag=record['build']['image']['tag'],
            published_manifest_digest=record['build']['image']['manifest_digest'])
        try:
            started = time.monotonic()
            api.create_sandbox(sdk.SandboxSpec(id=identity, image=sdk.Image.from_name(record['image_id']),
                command=['sleep', '600'], cpus=2, memory_mb=2048, disk_mb=2048, ttl_seconds=600,
                labels={'qualification': 'registry-pulls-20260930'}), request_timeout_seconds=600)
            result['create_seconds'] = time.monotonic() - started
            current = api.get_sandbox(identity)
            q.require(current and current['spec']['id'] == identity, 'Owned sandbox is not visible')
            node = current['node']
            request = Request(node['node_url'].rstrip('/') + '/v1/heartbeat',
                              headers={'Authorization': 'Bearer ' + node_token})
            with urlopen(request, timeout=10) as response:
                body = response.read(1024 * 1024 + 1)
            q.require(len(body) <= 1024 * 1024, 'Oversized heartbeat')
            heartbeat = json.loads(body)['heartbeat']
            q.require(heartbeat['job_id'] == node['job_id'] and heartbeat['node_epoch'],
                      'Worker identity changed')
            result['worker'] = {k: heartbeat.get(k) for k in ('job_id', 'node_id', 'node_epoch', 'node_url')}
            q.require(result['worker']['node_url'] == node['node_url'], 'Worker endpoint changed')
            resolved = current['spec']['image']
            expected_ref = image_ref_with_manifest_digest(result['published_image_tag'], result['published_manifest_digest'])
            q.require(resolved == expected_ref, 'Resolved guest repository or manifest differs')
            result['resolved_image_ref'] = resolved
            key = (heartbeat['job_id'], heartbeat['node_epoch'], node['node_url'], resolved)
            result['create_image_pull_lease_owner'] = _registry_operation_lease_owner('create-image-pull', key)
            repository, tag = registry_repository_tag_from_image_ref(result['published_image_tag'])
            columns = ('repository', 'tag', 'owner', 'acquired_at', 'renewed_at', 'expires_at', 'digest')
            with sqlite3.connect(config.registry_usage_file().resolve().as_uri() + '?mode=ro', uri=True, timeout=5) as db:
                lease = db.execute('SELECT ' + ','.join(columns) + ' FROM registry_leases WHERE repository=? AND tag=? AND owner=?',
                    (repository, tag, result['create_image_pull_lease_owner'])).fetchone()
            q.require(lease is not None and lease[-1] == result['published_manifest_digest'],
                      'Exact owned create-pull lease was not verified')
            result['create_image_pull_lease'] = dict(zip(columns, lease))
            result['create_image_pull_lease_owner_verified'] = True
            q.write_json(args.output / (identity + '-created.json'), result)
            executed = api.exec(identity, case['fixture']['smoke_command'], timeout_seconds=120)
            q.require(executed.exit_code == 0, 'Guest assertion command failed')
            output, expected = json.loads(executed.stdout), case['fixture']['smoke_expected_json']
            q.require(all(output.get(k) == v for k, v in expected.items()), 'Frozen guest assertion failed')
            result.update(exit_code=0, verified=True, output={k: output[k] for k in expected})
        except Exception as error:
            result['error'] = q.error_metadata(error)
        finally:
            # Creation may time out after acceptance: always reconcile this owned ID.
            try:
                api.delete_sandbox(identity)
                result['deleted'] = api.get_sandbox_status(identity) is None
            except Exception as error:
                result['cleanup_error'] = q.error_metadata(error)
            result['finished_at'] = q.stamp()
            q.write_json(args.output / (identity + '.json'), result)
        return result

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(run, zip(selected, owned)))
    result = dict(passed=all(r.get('verified') and r.get('deleted') for r in results),
        started_at=min(r['started_at'] for r in results), finished_at=q.stamp(), results=results,
        fleet_after=[{k: getattr(h, k) for k in
            ('job_id', 'node_id', 'node_epoch', 'node_url', 'capabilities', 'active_sandboxes', 'active_image_builds')}
            for h in ControlStateStore(config.control_state_file()).load_heartbeats().values()])
    q.write_json(args.output / 'summary.json', result)
    print(json.dumps(result), flush=True)
    if not result['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
