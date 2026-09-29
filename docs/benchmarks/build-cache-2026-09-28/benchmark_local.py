"""Isolated Docker/HTTP-registry cache comparison; run as root on overlay2 Linux.

Uses cached registry:2, creates/removes only its own container and scratch image.
The baseline disables only preflight reuse, reproducing the former pull-first
path. The EROFS format is unchanged between cases. No production service is used.
"""
import json
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import time
from unittest.mock import patch
from urllib.request import urlopen

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from ucloud_sandboxes.environment_artifact import EnvironmentArtifactRegistry, content_digest
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder, publication_metrics
from ucloud_sandboxes.image_rootfs import DockerOverlay2RootfsStore
from ucloud_sandboxes.managed_registry import RegistryClient


def docker(*args):
    return subprocess.check_output(['docker', *args], text=True, stderr=subprocess.PIPE).strip()


def main():
    name = f'ucloud-build-cache-test-{os.getpid()}'
    ref = None
    with TemporaryDirectory(prefix='ucloud-build-cache-') as raw:
        root = Path(raw)
        try:
            docker('run', '-d', '--rm', '--name', name, '-p', '127.0.0.1::5000', 'registry:2')
            address = docker('port', name, '5000/tcp')
            for attempt in range(50):
                try:
                    with urlopen('http://' + address + '/v2/', timeout=1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError('fixture registry did not become ready')
            ref = address + '/ucloud-build-cache-fixture:base'
            (root / 'payload').write_bytes(os.urandom(64 * 1024 * 1024))
            (root / 'Dockerfile').write_text('FROM scratch\nCOPY payload /payload\nCMD ["/fixture"]\n')
            docker('build', '-q', '-t', ref, str(root))
            docker('push', ref)
            key = Ed25519PrivateKey.generate()
            public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
            registry = EnvironmentArtifactRegistry(RegistryClient('http://' + address),
                                                   'environments', {content_digest(public): public})
            results = {}
            for index, case in enumerate(['populate', 'baseline_cold_docker', 'optimized_cold_docker']):
                if index:
                    docker('image', 'rm', '-f', ref)
                builder = FreshEnvironmentBuilder(DockerOverlay2RootfsStore(root / case / 'images'),
                                                  registry, key, root / case / 'scratch')
                # Host tool is 1.4 (no -V support); pin its real known format.
                builder._layer_format = {'layout': 1, 'mkfs': 'mkfs.erofs 1.4', 'compression': 'lz4',
                                         'excludes': ['dev', 'proc', 'run', 'sys']}
                with publication_metrics() as metrics, \
                     patch.object(builder, '_reuse_image_layers', wraps=builder._reuse_image_layers) as preflight, \
                     patch.object(builder.image_store, '_checked', wraps=builder.image_store._checked) as commands:
                    if case == 'baseline_cold_docker':
                        preflight.side_effect = lambda *args, **kwargs: None
                    started = time.monotonic()
                    digest = builder.publish_image(ref, allowlist=('*',))
                    elapsed = time.monotonic() - started
                results[case] = {'seconds': elapsed, 'metrics': dict(metrics), 'manifest': digest,
                                 'docker_commands': [list(call.args) for call in commands.call_args_list]}
            assert len({v['manifest'] for v in results.values()}) == 1
            assert results['optimized_cold_docker']['docker_commands'] == []
            assert results['baseline_cold_docker']['metrics']['groups_reused'] == 1
            results['scope'] = ('64 MiB incompressible single-layer image; real Docker overlay2, local HTTP registry, '
                                'mkfs.erofs 1.4; cached components with image absent locally; not production throughput')
            print(json.dumps(results, indent=2))
        finally:
            if ref:
                subprocess.run(['docker', 'image', 'rm', '-f', ref], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(['docker', 'rm', '-f', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == '__main__':
    main()
