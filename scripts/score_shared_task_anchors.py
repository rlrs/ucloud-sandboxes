#!/usr/bin/env python3
"""Compare a recorded flat source against retained anchor indexes without blobs.

Results estimate changed logical bytes, not compressed cost or readiness. No
images are built, published or aliased. Candidates still require full qualification.
"""
import argparse
import json
from pathlib import Path

from cache_source_receipts import validated
from prepare_shared_task_image import read_flat_index
from ucloud_sandboxes.oci_flat_delta import plan_flat_delta


def rank_indices(target, candidates):
    rows = []
    for identity, index in candidates:
        plan = plan_flat_delta(index, target)
        rows.append({**identity, 'changed_regular_file_bytes': plan.regular_file_bytes,
                     'changed_paths': len(plan.changed), 'removed_paths': len(plan.removed)})
    return sorted(rows, key=lambda r: (r['changed_regular_file_bytes'], r['changed_paths'], r['anchor_source']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path, help='preparation work directory with source-index.json.gz')
    parser.add_argument('--catalog', required=True, type=Path)
    parser.add_argument('--anchor-cache', required=True, type=Path, action='append')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    args = parser.parse_args()
    if args.output.exists():
        parser.error('use a new score output path')
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.managed_registry import RegistryClient, RegistryRequestError, registry_repository_tag_from_image_ref
    identity = json.loads((args.root / 'identity.json').read_text())
    resolved = validated(identity['source'], json.loads((args.root / 'resolved.json').read_text()))
    if resolved['layer_count'] != 1:
        raise ValueError('only original flat sources can be scored')
    target = read_flat_index(args.root / 'source-index.json.gz', resolved['layers'][0])
    catalog = json.loads(args.catalog.read_text())
    if catalog.get('schema') != 1:
        raise ValueError('unsupported anchor catalog')
    client = RegistryClient(DeploymentConfig.from_file(args.config).registry_url)
    def candidates():
        for source, row in sorted(catalog['images'].items()):
            if (row.get('status') != 'ready' or row.get('method')
                    or not source.startswith('aweaiteam/scaleswe:') or '@sha256:' not in row['reference']):
                continue
            repo, _ = registry_repository_tag_from_image_ref(row['reference'])
            selector = row['reference'].split('@')[1]
            try:
                manifest, _ = client.manifest_document(repo, selector)
            except RegistryRequestError as error:
                if error.status_code == 404:
                    continue
                raise
            layers = manifest.get('layers', [])
            if len(layers) != 1:
                continue
            for cache in args.anchor_cache:
                path = cache / (layers[0]['digest'].split(':')[1] + '.json.gz')
                if path.exists():
                    yield {'anchor_source': source, 'anchor': row['reference'],
                           'anchor_source_reference': row['source_reference']}, read_flat_index(path, layers[0])
                    break
    scored = rank_indices(target, candidates())
    report = {'schema': 1, 'source': identity['source'], 'pinned_source': resolved['reference'],
              'scored_anchors': len(scored), 'best_candidates': scored[:10],
              'scope': 'Changed logical bytes only; full preparation and qualification still required.'}
    with args.output.open('x') as output:
        json.dump(report, output, indent=2)
    print(json.dumps(report))


if __name__ == '__main__':
    main()
