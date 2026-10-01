#!/usr/bin/env python3
"""Import trusted preparation receipts into the backend resolver catalog.

Run as the gateway service account. This does not prepare, delete, or mutate
registry blobs. Resolution acquires the usual registry lease before use.
"""
import argparse
import json
from pathlib import Path
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.prepared_images import PreparedImageCatalog, catalog_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    parser.add_argument('--catalog', type=Path, action='append', required=True)
    args = parser.parse_args()
    config = DeploymentConfig.from_file(args.config)
    store = PreparedImageCatalog(catalog_path(config.image_file()))
    counts = {'sources': 0, 'foundations': 0}
    for path in args.catalog:
        receipt = json.loads(path.read_text())
        if receipt.get('schema') != 1:
            raise ValueError('unsupported preparation catalog')
        for source, row in receipt.get('images', {}).items():
            counts['sources'] += store.register_source({**row, 'source': source})
        for row in receipt.get('foundations', {}).values():
            counts['foundations'] += store.register_foundation(row)
    print(json.dumps(counts))


if __name__ == '__main__':
    main()
