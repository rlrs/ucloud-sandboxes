#!/usr/bin/env python3
"""Verify an already-republished owned source; permit only its name-binding label."""
import argparse
import hashlib
import json
from pathlib import Path
import runpy


HELPER_SHA = 'd349f7b6db9a566c0eec091b46421caf0851587d32e9e4ef3afcec4300ec2702'
ORIGINAL_ID = 'slotq-b-cold-71e13d91-045'
IMAGE_ID_LABEL = 'ucloud-sandboxes.image-id'


def compare_runtime_config(original, published, new_id):
    """The actual filesystem/config contract permits no unrelated difference."""
    if not isinstance(new_id, str) or not new_id.startswith('registry-pull-canary-'):
        raise ValueError('Expected the owned canary image identity')
    if original.get('Labels', {}).get(IMAGE_ID_LABEL) != ORIGINAL_ID:
        raise ValueError('Original name binding differs')
    if published.get('Labels', {}).get(IMAGE_ID_LABEL) != new_id:
        raise ValueError('Published name binding differs')
    normalized = {**published, 'Labels': {**published['Labels'], IMAGE_ID_LABEL: ORIGINAL_ID}}
    if normalized != original:
        raise ValueError('Runtime configuration changed beyond the owned image name')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('Refuse to replace existing evidence')
    helper = args.source_root.parent / 'republish-python-canary.py'
    if hashlib.sha256(helper.read_bytes()).hexdigest() != HELPER_SHA:
        raise ValueError('Frozen republish helper changed')
    h = runpy.run_path(str(helper))
    source = h['owned_document'](h['SOURCE_MANIFEST'])
    original = h['owned_document'](h['SOURCE_CONFIG'])
    launch = json.loads((args.source_root / 'launch.json').read_text())
    previous = json.loads((args.source_root / 'summary.json').read_text())
    record = previous['record']
    image = record['build']['image']
    if (record['build']['status'] != 'succeeded' or record['deadline_missed'] or record.get('error')
            or image['id'] != launch['image_id'] or record['image_id'] != launch['image_id']
            or record['build']['node']['job_id'] not in launch['expected_nodes']
            or launch['helper_sha256'] != HELPER_SHA):
        raise ValueError('Owned build receipt failed validation')
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.managed_registry import RegistryClient, registry_repository_tag_from_image_ref
    config = DeploymentConfig.from_dict(json.loads(Path('/etc/ucloud-sandboxes/deployment.json').read_text()))
    registry = RegistryClient(config.registry_url)
    repository, _ = registry_repository_tag_from_image_ref(image['tag'])
    manifest, _ = registry.manifest_document(repository, image['manifest_digest'])
    descriptor = manifest['config']
    if not 0 < descriptor['size'] <= 4 * 1024**2:
        raise ValueError('Unexpected owned config size')
    body = registry.blob_bytes(repository, descriptor['digest'], max_bytes=descriptor['size'])
    if len(body) != descriptor['size'] or 'sha256:' + hashlib.sha256(body).hexdigest() != descriptor['digest']:
        raise ValueError('Published config authentication failed')
    published = json.loads(body)
    if original['rootfs'] != published['rootfs']:
        raise ValueError('Full source layer identity changed')
    fields = ('digest', 'size', 'mediaType')
    if [[v[k] for k in fields] for v in source['layers']] != [[v[k] for k in fields] for v in manifest['layers']]:
        raise ValueError('Compressed layer descriptors changed')
    compare_runtime_config(original['config'], published['config'], image['id'])
    result = dict(complete=True, image_id=image['id'], repository=repository,
        manifest_digest=image['manifest_digest'], original_manifest=h['SOURCE_MANIFEST'],
        original_config=h['SOURCE_CONFIG'], published_config=descriptor['digest'],
        source_rootfs_equal=True, compressed_descriptors_equal=True, layer_count=len(manifest['layers']),
        runtime_config_equal_except_owned_name=True,
        normalized_label=dict(key=IMAGE_ID_LABEL, original=ORIGINAL_ID, published=image['id']),
        selected_groups=[3, 4, 5], registry_mutations=0,
        source_receipt_sha256=hashlib.sha256((args.source_root / 'summary.json').read_bytes()).hexdigest(),
        source_launch_sha256=hashlib.sha256((args.source_root / 'launch.json').read_bytes()).hexdigest(),
        helper_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
