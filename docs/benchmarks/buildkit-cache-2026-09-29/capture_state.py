"""Read-only qualification state; execute on the gateway, retaining credentials there."""
import importlib.util
import json
from pathlib import Path
import sqlite3
import time

from ucloud_sandboxes.cli import run_registry_prune
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.managed_registry import RegistryClient

ROOT = Path('/work/ucloud-sandboxes/buildkit-cache-optimization-20260929-r3')


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    controller = load('controller', ROOT / 'deployment-controller.py')
    qualification = load('qualification', ROOT / 'qualify_gateway.py')
    _, config = controller.read_config()
    client = qualification.SandboxClient('https://77.42.92.27',
        api_token=config.sandbox_api_token_file().read_text().strip())
    builds = client.list_image_builds()
    states = ControlStateStore(config.control_state_file()).load_heartbeats()
    with sqlite3.connect(config.metrics_path().with_name('build-history.sqlite').as_uri() + '?mode=ro', uri=True) as db:
        build_id = json.loads((ROOT / 'public-build-receipt.json').read_text())['build']['build_id']
        row = db.execute('SELECT summary_json FROM terminal_builds WHERE build_id=?', (build_id,)).fetchone()
        assert row is not None, 'Terminal summary must survive builder retirement'
        history = json.loads(row[0])
    registry = RegistryClient(config.registry_url)
    cache_blobs, image_blobs = {}, {}
    cache_tags = registry.tags('ucloud-build-cache')
    for tag in cache_tags:
        document, _ = registry.manifest_document('ucloud-build-cache', tag)
        for blob in [document['config'], *document['layers']]:
            cache_blobs[blob['digest']] = blob['size']
    image_tags_scanned = 0
    # Controlled canaries use these explicit repositories; SDK-managed image
    # repositories are generated and are deliberately outside this comparison.
    for case in ('populate', 'replacement-no-cache', 'replacement-cache'):
        repository = 'ucloud-managed/buildkit-cache-' + case + '-20260929'
        for tag in registry.tags(repository):
            document, _ = registry.manifest_document(repository, tag)
            for blob in [document['config'], *document['layers']]:
                image_blobs[blob['digest']] = blob['size']
            image_tags_scanned += 1
    receipt = {
        'captured_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'health': controller.health(),
        'sandbox_count': len(client.list_sandboxes()),
        'prepared_builders': client.list_prepared_builders(),
        'active_builds': sum(b['status'] not in {'succeeded', 'failed'} for b in builds),
        'heartbeats': [{'job_id': h.job_id, 'updated_at': str(h.updated_at),
            'node_url': h.node_url, 'active_sandboxes': h.active_sandboxes,
            'active_image_builds': h.active_image_builds,
            'idle_since': str(h.idle_since)} for h in states.values()],
        'history_after_builder_retirement': '167925517' not in states,
        'durable_history': history,
        'cache_prune': run_registry_prune(config, execute=False, repository_prefix='ucloud-build-cache'),
        'blob_accounting': {
            'image_tags_scanned': image_tags_scanned,
            'cache_tags': len(cache_tags),
            'unique_cache_blob_bytes': sum(cache_blobs.values()),
            'cache_blob_bytes_also_in_fixture_images': sum(v for k, v in cache_blobs.items() if k in image_blobs),
            'cache_only_blob_bytes': sum(v for k, v in cache_blobs.items() if k not in image_blobs),
            'scope': 'Descriptor accounting; excludes manifests, filesystem metadata, uploads and pending GC.',
        },
    }
    print(json.dumps(receipt, indent=2))


if __name__ == '__main__':
    main()
