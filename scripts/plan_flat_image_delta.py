#!/usr/bin/env python3
"""Prepare an experimental OCI delta from two digest-pinned, flat tar.gz layers.

This only writes a new local directory. It does not publish images, rewrite source
aliases, or qualify runtime equivalence. Inputs must be retained OCI layer blobs.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import gzip
import hashlib
import json
from pathlib import Path
import re
import tempfile

from ucloud_sandboxes.oci_flat_delta import index_flat_tar, plan_flat_delta, write_flat_delta


class CheckedReader:
    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.sha256()
        self.size = 0

    def read(self, size=-1):
        value = self.stream.read(size)
        self.digest.update(value)
        self.size += len(value)
        return value


@contextmanager
def verified_tar(path, expected):
    with path.open('rb') as raw:
        reader = CheckedReader(raw)
        with gzip.GzipFile(fileobj=reader, mode='rb') as stream:
            yield stream
            # Drain gzip, including its trailer and all concatenated members.
            while stream.read(1024 * 1024):
                pass
        while reader.read(1024 * 1024):
            pass
        if 'sha256:' + reader.digest.hexdigest() != expected:
            raise ValueError(f'compressed source digest mismatch: {path}')


class CheckedWriter:
    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.sha256()
        self.size = 0

    def write(self, value):
        self.digest.update(value)
        self.size += len(value)
        return self.stream.write(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-layer', required=True, type=Path)
    parser.add_argument('--base-digest', required=True)
    parser.add_argument('--target-layer', required=True, type=Path)
    parser.add_argument('--target-digest', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    for digest in (args.base_digest, args.target_digest):
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
            parser.error('both immutable compressed layer digests are required')
    if args.output.exists():
        parser.error('output must be a new directory')
    indices = []
    for path, digest in ((args.base_layer, args.base_digest), (args.target_layer, args.target_digest)):
        with verified_tar(path, digest) as stream:
            indices.append(index_flat_tar(stream))
    plan = plan_flat_delta(*indices)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.flat-delta-', dir=args.output.parent) as temporary:
        stage = Path(temporary)
        delta = stage / 'delta.tar.gz'
        with delta.open('wb') as raw:
            with gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0) as compressed:
                writer = CheckedWriter(compressed)
                with verified_tar(args.target_layer, args.target_digest) as stream:
                    write_flat_delta(stream, writer, indices[1], plan)
        with delta.open('rb') as stream:
            digest = hashlib.sha256()
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        report = {'schema': 1, 'experimental': True, 'runtime_equivalence_qualified': False,
                  'base_layer': args.base_digest, 'target_layer': args.target_digest,
                  'base_entries': len(indices[0]), 'target_entries': len(indices[1]),
                  'changed_entries': len(plan.changed), 'whiteouts': len(plan.removed),
                  'directory_headers': len(plan.directories), 'changed_regular_file_bytes': plan.regular_file_bytes,
                  'delta_digest': 'sha256:' + digest.hexdigest(), 'delta_compressed_bytes': delta.stat().st_size,
                  'delta_diff_id': 'sha256:' + writer.digest.hexdigest(), 'delta_tar_bytes': writer.size}
        (stage / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        # Exclusive directory creation prevents replacing any prior output.
        args.output.mkdir()
        delta.rename(args.output / delta.name)
        (stage / 'report.json').rename(args.output / 'report.json')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
