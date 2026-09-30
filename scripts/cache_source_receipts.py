#!/usr/bin/env python3
"""Archive authenticated public OCI metadata and hydrate pinned preparation plans.

Contains no blobs, build receipts, private references, or readiness claims.
Reusing digest-bound metadata avoids another public manifest pull on recovery.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from pathlib import Path

from prepare_image_pool import registry_parts, save
from ucloud_sandboxes.environment_artifact import require_digest


def validated(source, resolved):
    host, repository, digest = registry_parts(resolved['reference'])
    require_digest(digest)
    if registry_parts(source)[:2] != (host, repository):
        raise ValueError('source receipt repository mismatch')
    raw = resolved['manifest_json'].encode()
    config_raw = resolved['config_json'].encode()
    if max(len(raw), len(config_raw)) > 16 * 1024**2:
        raise ValueError('source metadata too large')
    if 'sha256:' + hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError('source manifest digest mismatch')
    manifest, config = json.loads(raw), json.loads(config_raw)
    descriptor = manifest['config']
    if ('sha256:' + hashlib.sha256(config_raw).hexdigest() != descriptor['digest']
            or len(config_raw) != descriptor['size']):
        raise ValueError('source config identity mismatch')
    layers, diff_ids = manifest['layers'], config['rootfs']['diff_ids']
    if config.get('os') != 'linux' or config.get('architecture') != 'amd64' or len(layers) != len(diff_ids):
        raise ValueError('source platform or layer chain mismatch')
    for layer, diff_id in zip(layers, diff_ids, strict=True):
        require_digest(layer['digest'])
        require_digest(diff_id)
        if type(layer['size']) is not int or layer['size'] < 0:
            raise ValueError('invalid source blob size')
    # Reconstruct derived fields instead of trusting old accounting/ONBUILD flags.
    return {'reference': host + '/' + repository + '@' + digest,
            'manifest_json': resolved['manifest_json'], 'config_json': resolved['config_json'],
            'layers': layers, 'diff_ids': diff_ids, 'layer_count': len(layers),
            'compressed_bytes': sum(row['size'] for row in layers),
            'onbuild': (config.get('config') or {}).get('OnBuild') or []}


def pack(roots, output):
    if output.exists():
        raise ValueError('refusing to replace an input snapshot')
    receipts = {}
    incomplete = 0
    for root in roots:
        for path in sorted(root.glob('*.json')):
            if len(path.stem) != 64 or any(c not in '0123456789abcdef' for c in path.stem):
                continue
            row = json.loads(path.read_text())
            if 'resolved' not in row or 'source' not in row:
                continue
            if not {'manifest_json', 'config_json'} <= row['resolved'].keys():
                incomplete += 1
                continue
            source = row['source']
            if hashlib.sha256(source.encode()).hexdigest() != path.stem:
                raise ValueError('source receipt filename mismatch')
            resolved = validated(source, row['resolved'])
            receipts[source, resolved['reference']] = {'source': source, 'resolved': resolved}
        for path in sorted((root / 'work').glob('*/resolved.json')):
            identity = json.loads((path.parent / 'identity.json').read_text())
            source = identity['source']
            resolved = validated(source, json.loads(path.read_text()))
            receipts[source, resolved['reference']] = {'source': source, 'resolved': resolved}
    payload = {'schema': 1, 'receipts': [receipts[key] for key in sorted(receipts)]}
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
    envelope = {'payload_sha256': hashlib.sha256(raw).hexdigest(), 'payload': payload}
    with output.open('xb') as stream:
        stream.write(gzip.compress(json.dumps(envelope, sort_keys=True, separators=(',', ':')).encode(), mtime=0))
    return {'receipts': len(receipts), 'incomplete_receipts_skipped': incomplete, 'bytes': output.stat().st_size,
            'sha256': hashlib.sha256(output.read_bytes()).hexdigest()}


def load(path):
    data = json.loads(gzip.decompress(path.read_bytes()))
    raw = json.dumps(data['payload'], sort_keys=True, separators=(',', ':')).encode()
    if data['payload']['schema'] != 1 or hashlib.sha256(raw).hexdigest() != data['payload_sha256']:
        raise ValueError('source metadata archive checksum mismatch')
    result = {}
    for row in data['payload']['receipts']:
        resolved = validated(row['source'], row['resolved'])
        result.setdefault(row['source'], {})[resolved['reference']] = resolved
    return result


def select(source, pin, candidates):
    if pin:
        return candidates.get(pin)
    if len(candidates) > 1:
        raise ValueError('mutable source has multiple saved digests; pin the plan explicitly: ' + source)
    return next(iter(candidates.values()), None)


def hydrate(bundle, root, layout):
    known = load(bundle)
    plan = json.loads((root / 'plan.json').read_text())
    if plan.get('schema') != 1:
        raise ValueError('unsupported preparation plan')
    writes = []
    for item in plan['images']:
        source = item['source']
        resolved = select(source, item.get('pinned_source'), known.get(source, {}))
        if resolved is None:
            continue
        key = hashlib.sha256(source.encode()).hexdigest()
        path = root / (key + '.json') if layout == 'normal' else root / 'work' / key / 'resolved.json'
        if path.exists():
            row = json.loads(path.read_text())
            if 'resolved' in row or layout == 'shared':
                old = validated(source, row['resolved'] if layout == 'normal' else row)
                if old != resolved:
                    raise ValueError('existing source resolution differs; refusing replacement')
                continue
            # Never overwrite another preparer's partially populated journal.
            raise ValueError('existing preparation receipt needs manual reconciliation')
        writes.append((path, {'source': source, 'resolved': resolved} if layout == 'normal' else resolved))
    # Validate the whole plan before writing. Hydrate only while its coordinator
    # is stopped; these are inputs, not a live cache replacement protocol.
    for path, value in writes:
        path.parent.mkdir(parents=True, exist_ok=True)
        save(path, value)
    return {'hydrated': len(writes), 'planned': len(plan['images'])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    archive = commands.add_parser('pack')
    archive.add_argument('--root', type=Path, action='append', required=True)
    archive.add_argument('--output', type=Path, required=True)
    restore = commands.add_parser('hydrate')
    restore.add_argument('--bundle', type=Path, required=True)
    restore.add_argument('--root', type=Path, required=True)
    restore.add_argument('--layout', choices=['normal', 'shared'], default='normal')
    args = parser.parse_args()
    print(json.dumps(pack(args.root, args.output) if args.command == 'pack'
                     else hydrate(args.bundle, args.root, args.layout)))


if __name__ == '__main__':
    main()
