#!/usr/bin/env python3
"""Read measured image metadata and execute owned smoke sandboxes on the gateway."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import time

import live_build_load_benchmark as bench


def records(root, phase=None):
    result = []
    for path in sorted(root.glob('*/summary.json')):
        if phase is not None and path.parent.name != phase:
            continue
        for record in json.loads(path.read_text())['records']:
            if record.get('build', {}).get('status') == 'succeeded':
                result.append({'phase': path.parent.name, **record})
    return result


def inventory(root, config):
    from ucloud_sandboxes.build_cache import RegistryBuildCache
    from ucloud_sandboxes.environment_artifact import load_image_environment
    from ucloud_sandboxes.environment_config import environment_registry_from_deployment
    from ucloud_sandboxes.managed_registry import RegistryClient, registry_repository_tag_from_image_ref
    from ucloud_sandboxes.registry_disk import registry_disk_usage
    registry = RegistryClient(config.registry_url)
    environments = environment_registry_from_deployment(config)
    oci_blobs, erofs_blobs, cache_blobs = {}, {}, {}
    rows, source_memo, component_memo = [], {}, {}
    for record in records(root):
        image = record['build']['image']
        repository, _ = registry_repository_tag_from_image_ref(image['tag'])
        digest = image['manifest_digest']
        key = repository, digest
        if key not in source_memo:
            layers = registry.manifest_layers(repository, digest)
            eroot, environment = load_image_environment(environments, repository, digest)
            components = []
            for component_digest in environment.components:
                if component_digest not in component_memo:
                    component = environments.load(component_digest)
                    component_memo[component_digest] = {'digest': component.image_digest, 'bytes': component.image_size}
                components.append(component_memo[component_digest])
            source_memo[key] = {'oci_layers': [{'digest': layer.digest, 'bytes': layer.size} for layer in layers.layers],
                                'compressed_oci_bytes': layers.total_size, 'environment_root': eroot,
                                'erofs_components': components, 'erofs_bytes': sum(c['bytes'] for c in components)}
        item = source_memo[key]
        rows.append({'image_id': record['image_id'], 'phase': record['phase'], 'recipe': record['recipe'],
                     'variant': record['variant'], 'manifest_digest': digest, **item})
        oci_blobs.update({layer['digest']: layer['bytes'] for layer in item['oci_layers']})
        erofs_blobs.update({layer['digest']: layer['bytes'] for layer in item['erofs_components']})
    cache = RegistryBuildCache(config.builder.buildx_cache_ref, registry_url=config.registry_url,
        max_bytes=config.builder.buildx_cache_max_bytes, max_entries=config.builder.buildx_cache_max_entries,
        max_age_seconds=config.builder.buildx_cache_max_age_seconds)
    for tag in registry.tags(cache.repository):
        document, _ = registry.manifest_document(cache.repository, tag)
        cache_blobs.update({blob['digest']: blob['size'] for blob in [document['config'], *document['layers']]})
    result = {'captured_at': bench.stamp(), 'images': rows, 'image_count': len(rows),
        'unique_oci_layer_bytes': sum(oci_blobs.values()), 'summed_image_oci_layer_bytes': sum(row['compressed_oci_bytes'] for row in rows),
        'unique_erofs_bytes': sum(erofs_blobs.values()), 'summed_image_erofs_bytes': sum(row['erofs_bytes'] for row in rows),
        'shared_cache_blob_bytes': sum(cache_blobs.values()),
        'cache_blob_bytes_shared_with_test_images': sum(v for k,v in cache_blobs.items() if k in oci_blobs),
        'cache_blob_bytes_not_in_test_images': sum(v for k,v in cache_blobs.items() if k not in oci_blobs),
        'cache_prune_dry_run': cache.prune(), 'registry_disk': registry_disk_usage(config).to_dict(),
        'limits': ['Compressed OCI and signed EROFS image bytes are different accounting views; neither is a measured unpacked rootfs size.',
                   'Cache inventory includes pre-existing cache entries; no registry blobs are downloaded or deleted.',
                   'Metadata/filesystem overhead, uploads and pending garbage collection are excluded from descriptor totals.']}
    bench.write_json(root / 'image-inventory.json', result)
    print(json.dumps({k:v for k,v in result.items() if k != 'images'}))


def smoke(root, phase, sdk, factory):
    destination = root / ('smoke-' + phase + '.json')
    if destination.exists():
        raise ValueError('Do not overwrite smoke evidence')
    selected = {}
    for record in records(root, phase):
        selected.setdefault(record['recipe'], record)
    if len(selected) != 3:
        raise ValueError('Expected successful images for all three recipes')

    def run(record):
        manifest = json.loads((root / 'contexts' / record['recipe'] / record['variant'] / 'fixture.json').read_text())
        client = factory()
        name = 'bl-smoke-' + phase + '-' + str(record['index'])
        result = {'image_id': record['image_id'], 'sandbox_id': name, 'recipe': record['recipe'],
                  'variant': record['variant'], 'started_at': bench.stamp()}
        created = False
        started = time.monotonic()
        try:
            client.create_sandbox(sdk.SandboxSpec(id=name, image=sdk.Image.from_name(record['image_id']), command=['sleep', '600'],
                memory_mb=2048, cpus=2, disk_mb=2048, ttl_seconds=600,
                labels={'qualification': 'build-load-20260929'}), request_timeout_seconds=600)
            created = True
            result['create_seconds'] = time.monotonic() - started
            executed = client.exec(name, manifest['smoke_command'], timeout_seconds=120)
            result['exit_code'] = executed.exit_code
            result['output'] = json.loads(executed.stdout)
            assert executed.exit_code == 0
            assert all(result['output'].get(k) == v for k,v in manifest['smoke_expected_json'].items())
            result['verified'] = True
        except Exception as exc:
            result['error_type'], result['error'] = type(exc).__name__, str(exc)[:1000]
        finally:
            if created:
                client.delete_sandbox(name)
                result['deleted'] = True
            result['finished_at'] = bench.stamp()
        return result
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(run, selected.values()))
    bench.write_json(destination, results)
    print(json.dumps(results, indent=2))
    if not all(item.get('verified') and item.get('deleted') for item in results):
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('/work/ucloud-sandboxes/build-load-20260929'))
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('inventory')
    check = sub.add_parser('smoke')
    check.add_argument('--phase', required=True)
    args = parser.parse_args()
    sdk, config, factory = bench.clients()
    if args.command == 'inventory':
        inventory(args.root, config)
    else:
        smoke(args.root, args.phase, sdk, factory)


if __name__ == '__main__':
    main()
