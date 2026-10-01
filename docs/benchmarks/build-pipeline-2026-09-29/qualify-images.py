#!/usr/bin/env python3
"""Run three owned synthetic image assertions with the pinned qualification SDK."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import time
from uuid import uuid4
import qualify_builder_slots as q


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--summary', type=Path, required=True)
    parser.add_argument('--cases', type=Path, required=True)
    parser.add_argument('--sdk-wheel', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    summary, cases = json.loads(args.summary.read_text()), json.loads(args.cases.read_text())
    q.require(summary['passed'] and summary['cases_sha256'] == q.sha(args.cases), 'Require passed matching candidate evidence')
    q.require(summary['declared_slots'] == 6 and summary['mode'] == 'cold', 'Expected candidate cold images')
    q.require(not args.output.exists(), 'Refuse to overwrite evidence')
    selected = [next(r for r in summary['records'] if r['recipe'] == recipe and r['variant'] == 'app-change-20') for recipe in q.RECIPES]
    fixtures = {v['index']: v['fixture'] for v in cases['cases']}
    sdk = q.load_sdk(args.sdk_wheel)
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.control_state import ControlStateStore
    config = DeploymentConfig.from_dict(json.loads(Path('/etc/ucloud-sandboxes/deployment.json').read_text()))
    token = config.sandbox_api_token_file().read_text().strip()
    def client():
        return sdk.SandboxClient('https://77.42.92.27', api_token=token, timeout_seconds=120)
    q.require(not client().list_sandboxes(), 'Require idle sandbox fleet')
    q.require(not any(v['status'] not in q.TERMINAL for v in client().list_image_builds()), 'Require completed builds')
    prefix = 'slotq-smoke-' + uuid4().hex[:12]
    args.output.mkdir(mode=0o700, parents=True)
    owned = [prefix + '-' + str(r['index']) for r in selected]
    q.write_json(args.output / 'launch.json', dict(at=q.stamp(), sandbox_ids=owned, summary_sha256=q.sha(args.summary), cases_sha256=q.sha(args.cases), sdk_sha256=q.SDK_SHA256, helper_sha256=q.sha(__file__)))
    def run(pair):
        record, identity = pair
        api, fixture = client(), fixtures[record['index']]
        result = dict(sandbox_id=identity, image_id=record['image_id'], recipe=record['recipe'], variant=record['variant'], started_at=q.stamp())
        try:
            started = time.monotonic()
            api.create_sandbox(sdk.SandboxSpec(id=identity, image=sdk.Image.from_name(record['image_id']), command=['sleep','600'], cpus=2, memory_mb=2048, disk_mb=2048, ttl_seconds=600, labels={'qualification':'builder-slot-20260929'}), request_timeout_seconds=600)
            result['create_seconds'] = time.monotonic() - started
            executed = api.exec(identity, fixture['smoke_command'], timeout_seconds=120)
            q.require(executed.exit_code == 0, 'Guest assertion command failed')
            output = json.loads(executed.stdout)
            expected = fixture['smoke_expected_json']
            q.require(all(output.get(k) == v for k,v in expected.items()), 'Guest output differs from frozen fixture')
            result.update(exit_code=0, output={k:output[k] for k in expected}, verified=True)
        except Exception as error:
            result['error'] = q.error_metadata(error)
        finally:
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
    fleet = [{k:getattr(h,k) for k in ('job_id','capabilities','active_sandboxes','active_image_builds')} for h in ControlStateStore(config.control_state_file()).load_heartbeats().values()]
    result = dict(started_at=min(r['started_at'] for r in results), finished_at=q.stamp(), passed=all(r.get('verified') and r.get('deleted') for r in results), results=results, fleet_after=fleet)
    q.write_json(args.output / 'summary.json', result)
    print(json.dumps(result), flush=True)
    if not result['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
