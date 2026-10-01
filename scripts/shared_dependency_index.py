#!/usr/bin/env python3
"""Find qualified project bases by exact file reuse, then verify full delta cost.

The read-only SQLite index is a selection accelerator, never a readiness proof.
Every selected image still goes through the source preparer's full qualification.
"""
from __future__ import annotations

import hashlib
import argparse
import json
from pathlib import Path
import sqlite3
import tarfile

from prepare_shared_task_image import encode, read_flat_index
from ucloud_sandboxes.oci_flat_delta import plan_flat_delta

MIN_FILE_BYTES = 16 * 1024


def qualified_candidates(pools):
    """Rebuild the accelerator from retained, fully qualified preparation pools."""
    from cache_source_receipts import validated
    candidates = {}
    for pool in pools:
        rows = json.loads((pool / 'catalog.json').read_text())['images']
        for source, row in sorted(rows.items()):
            proof = row.get('qualification', {})
            components = row.get('components', [])
            if (row.get('status') != 'ready' or row.get('method') != 'verified-flat-delta-v1'
                    or proof.get('equivalent') is not True or proof.get('mode') != 'full_source_scan'
                    or not 1 <= len(components) <= 8 or sum(c['bytes'] for c in components) > 2 * 1024**3):
                continue
            work = pool.resolve() / 'work' / hashlib.sha256(source.encode()).hexdigest()
            index = work / 'source-index.json.gz'
            if not index.exists():
                continue
            resolved = validated(source, json.loads((work / 'resolved.json').read_text()))
            if resolved['layer_count'] != 1 or resolved['reference'] != row['source_reference']:
                raise ValueError('qualified dependency source mismatch')
            candidates[source] = {'source': source, 'reference': row['reference'],
                'source_reference': resolved['reference'], 'index_path': str(index),
                'resolved_path': str(work / 'resolved.json'), 'layer': resolved['layers'][0],
                'erofs_bytes': sum(c['bytes'] for c in components)}
    return [candidates[source] for source in sorted(candidates)]


def file_signature(entry):
    identity = (entry.path, entry.kind.decode(), *entry.filesystem_identity()[1:])
    return hashlib.sha256(encode(identity)).digest()


def build_index(output, candidates, *, progress=None):
    """Candidates include authenticated source-layer descriptors and index paths."""
    if output.exists():
        raise ValueError('dependency index snapshots are immutable')
    features, accepted = {}, []
    for candidate in candidates:
        if len(accepted) >= 1024:
            raise ValueError('dependency anchor count exceeds bound')
        index = read_flat_index(Path(candidate['index_path']), candidate['layer'])
        anchor_id = len(accepted)
        accepted.append(candidate)
        for entry in index.values():
            if entry.kind == tarfile.REGTYPE and entry.size >= MIN_FILE_BYTES:
                signature = file_signature(entry)
                features[signature] = features.get(signature, 0) | (1 << anchor_id)
        if len(features) > 2_000_000:
            raise ValueError('dependency fingerprint count exceeds bound')
        if progress:
            progress(len(accepted), len(features))
    partial = output.with_suffix('.partial')
    if partial.exists():
        raise ValueError('incomplete dependency index must be reconciled first')
    with sqlite3.connect(partial) as db:
        db.execute('CREATE TABLE metadata (schema INTEGER, min_file_bytes INTEGER)')
        db.execute('INSERT INTO metadata VALUES (1, ?)', (MIN_FILE_BYTES,))
        db.execute('CREATE TABLE anchors (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)')
        db.executemany('INSERT INTO anchors VALUES (?, ?)', ((i, json.dumps(c)) for i, c in enumerate(accepted)))
        db.execute('CREATE TABLE files (signature BLOB PRIMARY KEY, anchors BLOB NOT NULL) WITHOUT ROWID')
        width = max(1, (len(accepted) + 7) // 8)
        db.executemany('INSERT INTO files VALUES (?, ?)', ((k, v.to_bytes(width, 'little')) for k, v in features.items()))
    partial.replace(output)
    return {'anchors': len(accepted), 'fingerprints': len(features), 'bytes': output.stat().st_size,
            'sha256': hashlib.sha256(output.read_bytes()).hexdigest()}


def rank_anchors(database, target, *, excluded_source=None, limit=8):
    if not 1 <= limit <= 16:
        raise ValueError('invalid shortlist bound')
    with sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True) as db:
        if db.execute('SELECT schema, min_file_bytes FROM metadata').fetchall() != [(1, MIN_FILE_BYTES)]:
            raise ValueError('unsupported dependency index')
        candidates = {i: json.loads(payload) for i, payload in db.execute('SELECT id,payload FROM anchors')}
        if len(candidates) > 1024 or set(candidates) != set(range(len(candidates))):
            raise ValueError('invalid anchor IDs')
        scores = {i: 0 for i in candidates}
        for entry in target.values():
            if entry.kind != tarfile.REGTYPE or entry.size < MIN_FILE_BYTES:
                continue
            row = db.execute('SELECT anchors FROM files WHERE signature=?', (file_signature(entry),)).fetchone()
            if row is None:
                continue
            mask = int.from_bytes(row[0], 'little')
            if mask.bit_length() > len(candidates):
                raise ValueError('unknown dependency anchor')
            while mask:
                bit = mask & -mask
                scores[bit.bit_length() - 1] += entry.size
                mask ^= bit
        shortlist = sorted((i for i in candidates if candidates[i]['source'] != excluded_source),
                           key=lambda i: (-scores[i], candidates[i]['erofs_bytes'], candidates[i]['source']))[:limit]
    result = []
    for i in shortlist:
        candidate = candidates[i]
        anchor = read_flat_index(Path(candidate['index_path']), candidate['layer'])
        # Full planning accounts for small files, metadata, deletion and hardlink
        # closure; approximate large-file scores never decide the final choice.
        plan = plan_flat_delta(anchor, target)
        result.append({**candidate, 'changed_regular_file_bytes': plan.regular_file_bytes,
                       'changed_paths': len(plan.changed), 'removed_paths': len(plan.removed)})
    return sorted(result, key=lambda r: (r['changed_regular_file_bytes'], r['erofs_bytes'], r['removed_paths'], r['source']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pool', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    candidates = qualified_candidates(args.pool)
    if not candidates:
        raise ValueError('no fully qualified dependency bases with recorded indexes')
    print(json.dumps(build_index(args.output, candidates)), flush=True)


if __name__ == '__main__':
    main()
