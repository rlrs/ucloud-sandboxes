#!/usr/bin/env python3
"""Republish one frozen owned Python fixture, retaining its exact OCI inputs.

Run on the gateway after an explicit idle/pool gate. This submits one new owned
image build. It does not change deployments, prune caches, or create sandboxes.
An accepted request drains to a terminal record even after its client deadline.
"""
import argparse
import asyncio
import hashlib
import importlib.util
import json
from pathlib import Path
import time
from uuid import uuid4


OLD = Path('/work/ucloud-sandboxes/builder-slot-qualification-20260929')
HARNESS_SHA = '01b3679ac88ad767d9275fe7ea9d9b9d93834e4887e2bd13d5e2259c5c044531'
CASES_SHA = 'ff3e4555fc44dbbaf131f9ed610f03b8400ee3d4a1c1ddecf77d3a7c5d7d4704'
SOURCE_MANIFEST = 'sha256:85b141c8f4642fe36d4cabe46e18a73494ce9c9b5224412b2ecc7c26d493875d'
SOURCE_CONFIG = 'sha256:e3eabd6dfdb2c0bbc2c8c6119040d630e60a01cd5d4017b2cbec02ddd4e47777'
SDK_WHEEL = Path('/work/ucloud-sandboxes/build-reliability-20260929-r2/ucloud_sandboxes_sdk-0.4.34-py3-none-any.whl')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def owned_document(digest):
    if digest not in {SOURCE_MANIFEST, SOURCE_CONFIG}:
        raise ValueError('Only the two pinned owned metadata blobs are readable')
    name = digest.removeprefix('sha256:')
    path = Path('/mnt/ucloud-registry/docker-registry/docker/registry/v2/blobs/sha256') / name[:2] / name / 'data'
    if path.stat().st_size > 4 * 1024**2:
        raise ValueError('Oversized owned metadata')
    body = path.read_bytes()
    if 'sha256:' + hashlib.sha256(body).hexdigest() != digest:
        raise ValueError('Owned metadata identity changed')
    return json.loads(body)


async def run(args, q, sdk, case, config, prefix):
    import aiohttp
    token = config.sandbox_api_token_file().read_text().strip()
    async with aiohttp.ClientSession(trace_configs=[q.http_trace(aiohttp)]) as session:
        async with sdk.AsyncSandboxClient('https://77.42.92.27', api_token=token,
                                         timeout_seconds=60, session=session) as client:
            q.require(not await client.list_sandboxes(), 'Sandbox fleet must be idle')
            q.require(not any(v['status'] not in q.TERMINAL for v in await client.list_image_builds()),
                      'No unrelated builds may be active')
            arrival = time.monotonic()
            return await q.run_case(case, client, sdk.Image.from_dockerfile,
                prefix=prefix, arrival=arrival, deadline=arrival + 600,
                drain_deadline=arrival + 2500)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--expected-node', action='append', required=True)
    args = parser.parse_args()
    if args.output.exists() or len(args.expected_node) != len(set(args.expected_node)):
        parser.error('Output must be new and expected nodes unique')
    path = OLD / 'qualify_builder_slots.py'
    if sha(path) != HARNESS_SHA:
        raise ValueError('Frozen SDK qualification module changed')
    spec = importlib.util.spec_from_file_location('qualify_builder_slots', path)
    q = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(q)
    cases = OLD / 'cold-b-fixtures/cases.json'
    q.require(sha(cases) == CASES_SHA, 'Frozen fixture metadata changed')
    case = json.loads(cases.read_text())['cases'][45]
    q.require(case['index'] == 45 and case['recipe'] == 'python-agent'
              and case['variant'] == 'app-change-20', 'Unexpected source fixture')
    q.require(q.context_inventory(Path(case['context_path']))['context_sha256'] == case['context_sha256'],
              'Frozen source files changed')
    source_manifest, source_config = owned_document(SOURCE_MANIFEST), owned_document(SOURCE_CONFIG)
    q.require(source_manifest['config']['digest'] == SOURCE_CONFIG, 'Source metadata binding changed')
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.control_state import ControlStateStore
    from ucloud_sandboxes.managed_registry import RegistryClient, registry_repository_tag_from_image_ref
    config = DeploymentConfig.from_dict(json.loads(Path('/etc/ucloud-sandboxes/deployment.json').read_text()))
    builders = [h for h in ControlStateStore(config.control_state_file()).load_heartbeats().values()
                if 'image-build' in h.capabilities]
    q.require({h.job_id for h in builders} == set(args.expected_node)
              and all(h.active_image_builds == 0 and h.active_sandboxes == 0 for h in builders),
              'Expected owned builder pool must be idle')
    sdk = q.load_sdk(SDK_WHEEL)
    prefix = 'registry-pull-canary-' + uuid4().hex[:12]
    args.output.mkdir(mode=0o700, parents=True)
    q.write_json(args.output / 'launch.json', dict(at=q.stamp(), image_id=prefix + '-045',
        expected_nodes=args.expected_node, cases_sha256=CASES_SHA, source_manifest=SOURCE_MANIFEST,
        source_config=SOURCE_CONFIG, sdk_sha256=q.SDK_SHA256, helper_sha256=sha(Path(__file__))))
    result = {'complete': False, 'source_manifest': SOURCE_MANIFEST, 'source_config': SOURCE_CONFIG}
    try:
        record = asyncio.run(run(args, q, sdk, case, config, prefix))
        result['record'] = record
        q.require(record.get('build', {}).get('status') == 'succeeded' and not record.get('error')
                  and not record['deadline_missed'], 'Owned canary did not complete within its deadline')
        q.require(record['build']['node']['job_id'] in set(args.expected_node), 'Unexpected canary owner')
        image = record['build']['image']
        repository, _ = registry_repository_tag_from_image_ref(image['tag'])
        registry = RegistryClient(config.registry_url)
        manifest, _ = registry.manifest_document(repository, image['manifest_digest'])
        descriptor = manifest['config']
        q.require(0 < descriptor['size'] <= 4 * 1024**2, 'Unexpected canary config size')
        raw = registry.blob_bytes(repository, descriptor['digest'], max_bytes=descriptor['size'])
        q.require(len(raw) == descriptor['size'] and 'sha256:' + hashlib.sha256(raw).hexdigest() == descriptor['digest'],
                  'Published canary config identity mismatch')
        published = json.loads(raw)
        q.require(published['rootfs']['diff_ids'] == source_config['rootfs']['diff_ids'],
                  'Republished source layer identities changed; investigate before byte qualification')
        q.require(published.get('config') == source_config.get('config'),
                  'Republished runtime image configuration changed')
        fields = ('digest', 'size', 'mediaType')
        q.require([[layer[k] for k in fields] for layer in manifest['layers']]
                  == [[layer[k] for k in fields] for layer in source_manifest['layers']],
                  'Republished compressed layer descriptors changed')
        result.update(complete=True, repository=repository, manifest_digest=image['manifest_digest'],
                      image_id=image['id'], diff_ids_equal=True, compressed_descriptors_equal=True,
                      runtime_config_equal=True, selected_groups=[3, 4, 5])
    except Exception as error:
        result['error'] = q.error_metadata(error)
    finally:
        result['finished_at'] = q.stamp()
        q.write_json(args.output / 'summary.json', result)
    print(json.dumps({k:v for k,v in result.items() if k != 'record'}), flush=True)
    if not result['complete']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
