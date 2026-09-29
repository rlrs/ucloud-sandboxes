"""Controlled BuildKit cache qualification on an otherwise idle canary builder.

Uses independent empty BuildKit stores for replacement cases. Credentials and
the environment signing key stay on this builder. No production service restart.
"""
# ruff: noqa: E402 -- inspect the staged wheel, never the installed service code.
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

ROOT = Path('/work/ucloud-sandboxes/buildkit-cache-qualification-20260929')
WHEEL = ROOT / 'ucloud_sandboxes-0.7.0-py3-none-any.whl'
sys.path.insert(0, str(WHEEL))
from ucloud_sandboxes.build_cache import RegistryBuildCache
from ucloud_sandboxes.environment_config import environment_publisher_from_args
from ucloud_sandboxes.images import DockerImageRuntime, ImageBuildSpec, ImageManager, ImageStore
from ucloud_sandboxes.managed_registry import RegistryClient
from ucloud_sandboxes.vm_init import PINNED_BUILDKIT_IMAGE

REGISTRY = '10.42.0.2:5000'
CACHE_REF = REGISTRY + '/ucloud-build-cache:shared'


def docker(*args):
    return subprocess.check_output(['docker', *args], text=True, stderr=subprocess.STDOUT).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('case', choices=['populate', 'replacement-no-cache', 'replacement-cache', 'provisioned-cache'])
    args = parser.parse_args()
    context = ROOT / 'context'
    context.mkdir(parents=True, exist_ok=True)
    payload = hashlib.shake_256(b'ucloud-buildkit-cache-qualification-20260929-v1').digest(32 * 1024**2)
    (context / 'payload').write_bytes(payload)
    (context / 'Dockerfile').write_text('FROM busybox:1.37.0\nCOPY payload /payload\nRUN for i in $(seq 1 32); do sha256sum /payload; done > /proof\nCMD ["sh"]\n')
    builder = 'ucloud-shared-cache'
    owned = args.case.startswith('replacement-')
    if owned:
        builder = 'ucloud-cache-' + args.case + '-20260929'
        docker('buildx', 'create', '--name', builder, '--driver', 'docker-container',
               '--driver-opt', 'image=' + PINNED_BUILDKIT_IMAGE, '--driver-opt', 'network=host',
               '--buildkitd-config', '/etc/ucloud-sandboxes/buildkit/buildkitd.toml')
        docker('buildx', 'inspect', builder, '--bootstrap')
    receipt = {'case': args.case, 'builder': builder, 'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
               'wheel_sha256': hashlib.sha256(WHEEL.read_bytes()).hexdigest(),
               'payload_sha256': hashlib.sha256(payload).hexdigest(), 'payload_bytes': len(payload),
               'buildkit_image': PINNED_BUILDKIT_IMAGE}
    try:
        receipt['initial_usage'] = docker('buildx', 'du', '--builder', builder)
        if owned:
            assert '0B' in receipt['initial_usage'], receipt['initial_usage']
        cache_enabled = args.case != 'replacement-no-cache'
        runtime = DockerImageRuntime(buildx_direct_push=True, buildx_builder=builder,
            buildx_cache_ref=CACHE_REF if cache_enabled else '',
            buildx_cache_registry_url='http://' + REGISTRY)
        state = ROOT / args.case
        state.mkdir(exist_ok=True)
        publisher = environment_publisher_from_args(SimpleNamespace(
            image_file=state / 'images.sqlite', docker_binary='docker',
            environment_registry_url='http://' + REGISTRY, environment_registry_repository='environments',
            environment_trusted_keys=Path('/etc/ucloud-sandboxes/environment/producers.json'),
            environment_signing_key=Path('/etc/ucloud-sandboxes/environment/producer.pem'),
            environment_allow_path=['*'],
        ))
        manager = ImageManager(ImageStore(state / 'images.sqlite'), runtime, environment_publisher=publisher)
        name = 'buildkit-cache-' + args.case + '-20260929'
        spec = ImageBuildSpec(id=name, tag=REGISTRY + '/ucloud-managed/' + name + ':latest', context_path=str(context))
        before = time.monotonic()
        identity = 'archive:sha256:' + hashlib.sha256(payload + (context / 'Dockerfile').read_bytes()).hexdigest()
        build, _accepted = manager.start_build(spec, push=True, context_identity=identity,
            materialize_context=lambda: SimpleNamespace(path=context, context_identity=identity, cleanup=lambda: None))
        done = manager.wait_for_build(build.build_id, timeout_seconds=600)
        receipt['wall_seconds'] = time.monotonic() - before
        (ROOT / (args.case + '-build.log')).write_text(done.log_tail)
        assert done.status == 'succeeded', done.error
        receipt['status'] = done.status
        receipt['build_id'] = done.build_id
        receipt['image_id'] = name
        receipt['timings'] = done.timings
        receipt['cached_step_lines'] = [line for line in done.log_tail.splitlines() if 'CACHED' in line]
        receipt['command'] = list(done.command)
        receipt['final_usage'] = docker('buildx', 'du', '--builder', builder)
        client = RegistryClient('http://' + REGISTRY)
        document, _headers = client.manifest_document('ucloud-managed/' + name, 'latest')
        assert document.get('mediaType') in {'application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json'}
        assert document.get('annotations', {}).get('org.ucloud.immutable-environment.v1')
        receipt['published_image_manifest'] = done.image.get('manifest_digest')
        receipt['environment_attached'] = True
        receipt['cache'] = RegistryBuildCache(CACHE_REF, registry_url='http://' + REGISTRY).prune()
        if args.case in {'replacement-cache', 'provisioned-cache'}:
            assert receipt['cached_step_lines'], 'Expected real BuildKit cache hits'
        (ROOT / (args.case + '-receipt.json')).write_text(json.dumps(receipt, indent=2) + '\n')
        print(json.dumps(receipt, indent=2))
    finally:
        if owned:
            docker('buildx', 'rm', builder)


if __name__ == '__main__':
    main()
