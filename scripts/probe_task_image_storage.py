#!/usr/bin/env python3
"""Resolve bounded source inputs and measure retained versus missing OCI blobs.

Writes pinned preparation receipts, not images or aliases. Filesystem usage is
only an OCI lower bound: new EROFS/cache bytes require measured preparations.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from prepare_image_pool import SourceResolver, save
from ucloud_sandboxes.environment_artifact import require_digest


def blob_accounting(descriptors, retained_size, seen_missing):
    retained, missing = {}, {}
    for descriptor in descriptors:
        digest, size = descriptor['digest'], descriptor['size']
        require_digest(digest)
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise ValueError('invalid OCI blob size')
        prior = retained.get(digest, missing.get(digest))
        if prior is not None and prior != size:
            raise ValueError('conflicting OCI blob sizes')
        (retained if retained_size(digest) == size else missing)[digest] = size
    additional = {digest: size for digest, size in missing.items() if digest not in seen_missing}
    seen_missing.update(missing)
    return {'retained_oci_bytes': sum(retained.values()), 'missing_oci_bytes': sum(missing.values()),
            'additional_union_oci_bytes': sum(additional.values()), 'missing_blobs': len(missing)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--limit', type=int, default=70)
    parser.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    args = parser.parse_args()
    if args.limit < 1:
        parser.error('limit must be positive')
    from ucloud_sandboxes.config import DeploymentConfig
    config = DeploymentConfig.from_dict(json.loads(args.config.read_text()))
    plan = json.loads((args.root / 'plan.json').read_text())
    if plan.get('schema') != 1:
        raise ValueError('unsupported plan')
    blob_root = config.registry_data_dir() / 'docker/registry/v2/blobs/sha256'
    def retained_size(digest):
        key = digest.split(':')[1]
        try:
            return (blob_root / key[:2] / key / 'data').stat().st_size
        except FileNotFoundError:
            return None
    resolve = SourceResolver(config.control_state_file().parent / 'image-pool-locks')
    rows, seen = [], set()
    for item in plan['images'][:args.limit]:
        source = item['source']
        path = args.root / (hashlib.sha256(source.encode()).hexdigest() + '.json')
        receipt = json.loads(path.read_text()) if path.exists() else {'source': source}
        if receipt['source'] != source:
            raise ValueError('source receipt identity mismatch')
        try:
            if 'resolved' not in receipt:
                receipt['resolved'] = resolve(item.get('pinned_source', source))
                save(path, receipt)
            resolved = receipt['resolved']
            document = json.loads(resolved['manifest_json'])
            if 'sha256:' + hashlib.sha256(resolved['manifest_json'].encode()).hexdigest() != resolved['reference'].split('@')[1]:
                raise ValueError('source manifest identity mismatch')
            row = {**item, 'source_reference': resolved['reference'], 'status': 'resolved',
                   'layer_count': len(document['layers']),
                   **blob_accounting([document['config'], *document['layers']], retained_size, seen)}
        except Exception as error:
            row = {**item, 'status': 'unresolved', 'error': str(error)}
        rows.append(row)
        save(args.root / 'storage-probe.json', {'schema': 1, 'scope': 'OCI-only lower bound, not prepared coverage', 'images': rows})
        print(json.dumps(row), flush=True)
        if row['status'] == 'unresolved' and 'public registry cooldown' in row['error']:
            raise SystemExit(75)


if __name__ == '__main__':
    main()
