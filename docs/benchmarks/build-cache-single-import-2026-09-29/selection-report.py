#!/usr/bin/env python3
"""Read owned 48-build logs and snapshot cache tag/digest metadata with GET/HEAD.

No cache, service, database or provider mutation. Only a new local JSON receipt
is created. Raw logs, command text and credential values are never serialized.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
from uuid import UUID, NAMESPACE_URL, uuid5


ROOT = Path('/work/ucloud-sandboxes/build-cache-single-import-load-20260929')
PHASES = ('single-import-repeat',)
RECIPES = ('python-agent', 'typescript-tools', 'typescript-multistage')
TAG = re.compile(r'bc(?:(1)-([0-9a-f]{16})-([0-9]{10})-([0-9a-f]{32})|(2)-([0-9a-f]{16})-([0-9a-f]{32})-([0-9]{10})-([0-9a-f]{32}))')
DIGEST = re.compile(r'sha256:[0-9a-f]{64}')
SELECT = re.compile(r'^(?:\[stderr\] )?Shared build cache selection: (\{.*\})$')
EXPORT = re.compile(r'^#\d+ writing cache image manifest (sha256:[0-9a-f]{64})(?: \d+(?:\.\d+)?s)? done$')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read_bounded(path, limit):
    with path.open('rb') as stream:
        data = stream.read(limit + 1)
    require(len(data) <= limit, 'Input exceeds byte bound')
    return data


def tag_metadata(tag):
    match = TAG.fullmatch(tag)
    if match is None:
        return None
    if match[1]:
        return {'schema': 'bc1', 'recipe_hint': match[2], 'created_at_unix': int(match[3])}
    return {'schema': 'bc2', 'recipe_hint': match[6], 'affinity_hint': match[7],
            'created_at_unix': int(match[8])}


def parse_log(data, repository_ref):
    text = data.decode('utf-8', errors='replace')
    selections, exported, imported, malformed = [], set(), set(), 0
    import_pattern = re.compile(r'^#\d+ importing cache manifest from '
                                + re.escape(repository_ref) + r':([^\s]+)$')
    for line in text.splitlines():
        if match := SELECT.fullmatch(line):
            try:
                value = json.loads(match[1])
                require(set(value) == {'affinity_match', 'imports'}
                        and type(value['affinity_match']) is bool
                        and type(value['imports']) is int and 0 <= value['imports'] <= 8,
                        'Invalid selection observation')
                selections.append(value)
            except (ValueError, TypeError, KeyError):
                malformed += 1
        elif 'Shared build cache selection:' in line:
            malformed += 1
        if match := EXPORT.fullmatch(line):
            exported.add(match[1])
        if match := import_pattern.fullmatch(line):
            if tag_metadata(match[1]) is not None:
                imported.add(match[1])
    # Repeated identical rendering is harmless; disagreement remains explicit.
    unique = {(item['affinity_match'], item['imports']) for item in selections}
    selected = selections[0] if len(unique) == 1 and not malformed else None
    return {'selection': selected, 'selection_observations': len(selections),
            'selection_malformed_or_conflicting': malformed > 0 or len(unique) > 1,
            'imported_owned_tags': sorted(imported),
            'exported_manifest_digests': sorted(exported),
            'log_sha256': sha(data), 'log_bytes': len(data),
            'at_or_above_retained_tail_limit': len(text) >= 64 * 1024}


def phase_rows(root, phase, repository_ref):
    summary_data = read_bounded(root / phase / 'summary.json', 16 * 1024 * 1024)
    summary = json.loads(summary_data)
    records = summary['records']
    require(summary.get('phase') == phase and len(records) == 48, 'Expected owned 48-build phase')
    require({row['index'] for row in records} == set(range(48)), 'Expected unique case indices')
    require(len({row['build_id'] for row in records}) == 48, 'Duplicate build UUID')
    result = []
    for row in sorted(records, key=lambda value: value['index']):
        index = row['index']
        expected_image = f'bl20260929-{phase}-{index:03}'
        require(row['image_id'] == expected_image, 'Image is outside the exact owned phase')
        require(str(UUID(row['build_id'])) == row['build_id'], 'Expected canonical build UUID')
        require(row['recipe'] == RECIPES[index % 3]
                and row['variant'] == 'app-change-' + str(5 + index // 3), 'Unexpected frozen case')
        require(re.fullmatch(r'[0-9a-f]{64}', row['context_sha256']), 'Expected context SHA256')
        status = row.get('build', {}).get('status')
        require(status in {'succeeded', 'failed'}, 'Expected terminal build status')
        item = {key: row[key] for key in ('index', 'build_id', 'image_id', 'recipe', 'variant', 'context_sha256')}
        item['status'] = status
        path = root / phase / (expected_image + '.build.log')
        if path.is_file():
            item.update(log_present=True, **parse_log(read_bounded(path, 512 * 1024), repository_ref))
        else:
            item.update(log_present=False, selection=None, exported_manifest_digests=[], imported_owned_tags=[])
        result.append(item)
    return result, sha(summary_data)


def snapshot(cache, *, timeout_seconds):
    deadline = time.monotonic() + timeout_seconds
    start = datetime.now(timezone.utc).isoformat()
    tags = cache._tags(deadline=deadline)
    require(len(tags) <= 256, 'Cache snapshot exceeds 256-tag proof bound')
    entries, unknown = [], 0
    for tag in tags:
        metadata = tag_metadata(tag)
        if metadata is None:
            unknown += 1
            continue
        require(time.monotonic() < deadline, 'Cache snapshot deadline elapsed')
        digest = cache.client.manifest_digest(cache.repository, tag)
        require(isinstance(digest, str) and DIGEST.fullmatch(digest), 'Invalid cache manifest digest')
        entries.append({'tag': tag, 'manifest_digest': digest, **metadata})
    require(cache._tags(deadline=deadline) == tags, 'Cache tag set changed during snapshot')
    encoded = json.dumps(entries, sort_keys=True, separators=(',', ':')).encode()
    return {'started_at': start, 'finished_at': datetime.now(timezone.utc).isoformat(),
            'owned_tags': entries, 'unknown_tag_count': unknown,
            'tag_set_unchanged_at_end': True, 'inventory_sha256': sha(encoded),
            'atomic_registry_snapshot': False}


def enrich(rows, inventory):
    by_digest = {}
    for entry in inventory['owned_tags']:
        by_digest.setdefault(entry['manifest_digest'], []).append(entry)
    for row in rows:
        digests = row['exported_manifest_digests']
        aliases = [entry for digest in digests for entry in by_digest.get(digest, [])
                   if entry['schema'] == 'bc2']
        row['export_bc2_aliases'] = sorted(aliases, key=lambda item: item['tag'])
        row['export_tag_uniquely_resolved'] = len(digests) == 1 and len(aliases) == 1
        row['export_tag'] = aliases[0]['tag'] if row['export_tag_uniquely_resolved'] else None
    selections = [row['selection'] for row in rows if row['selection'] is not None]
    return {'records': len(rows), 'logs_present': sum(row['log_present'] for row in rows),
            'selection_coverage': len(selections),
            'affinity_match_true': sum(row['affinity_match'] for row in selections),
            'affinity_match_false': sum(not row['affinity_match'] for row in selections),
            'import_count_distribution': dict(sorted(Counter(str(row['imports']) for row in selections).items())),
            'single_export_digest_coverage': sum(len(row['exported_manifest_digests']) == 1 for row in rows),
            'unique_export_tag_coverage': sum(row['export_tag_uniquely_resolved'] for row in rows)}


def live(args):
    from ucloud_sandboxes.config import DeploymentConfig
    from ucloud_sandboxes.build_cache import RegistryBuildCache

    require(not args.output.exists(), 'Use a new receipt path')
    require(1 <= args.timeout_seconds <= 180, 'Timeout must be 1..180 seconds')
    config = DeploymentConfig.from_dict(json.loads(read_bounded(args.config, 1024 * 1024)))
    cache = RegistryBuildCache(config.builder.buildx_cache_ref, registry_url=config.registry_url)
    rows, summary_sha = phase_rows(args.root, args.phase, cache.repository_ref)
    inventory = snapshot(cache, timeout_seconds=args.timeout_seconds)
    counts = enrich(rows, inventory)
    receipt = {'schema_version': 1, 'phase': args.phase,
               'captured_at': datetime.now(timezone.utc).isoformat(),
               'summary_sha256': summary_sha, 'repository': cache.repository,
               'counts': counts, 'records': rows, 'cache_snapshot': inventory,
               'limitations': ['Affinity match is a selector observation, not proof a RUN was reused.',
                   'Export tags are joined by logged immutable manifest digest; multiple aliases remain ambiguous.',
                   'An absent tag can reflect optional export failure or pruning; logs can be truncated.',
                   'The bounded registry snapshot is sequential; tags are not a transactional cache inventory.',
                   'No raw logs, command text or credentials are included.']}
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(receipt, stream, indent=2, allow_nan=False)
        stream.write('\n')
    print(json.dumps({'phase': args.phase, **counts}))


def self_test():
    tag = 'bc2-' + 'a' * 16 + '-' + 'b' * 32 + '-1790680000-' + 'c' * 32
    digest = 'sha256:' + 'd' * 64
    log = ('[stderr] Shared build cache selection: {"affinity_match": true, "imports": 8}\n'
           '#9 importing cache manifest from registry:5000/ucloud-build-cache:' + tag + '\n'
           '#20 writing cache image manifest ' + digest + ' 0.1s done\n'
           '#2 0.1 discarded-customer-output\nAuthorization: Bearer SHOULD_NOT_LEAK\n')
    parsed = parse_log(log.encode(), 'registry:5000/ucloud-build-cache')
    require(parsed['selection'] == {'affinity_match': True, 'imports': 8}, 'Selection parsing')
    require(parsed['imported_owned_tags'] == [tag] and parsed['exported_manifest_digests'] == [digest], 'Cache parsing')
    require('SHOULD_NOT_LEAK' not in json.dumps(parsed) and 'customer' not in json.dumps(parsed), 'Unsafe serialization')
    require(tag_metadata('bc1-' + 'a' * 16 + '-1790680000-' + 'c' * 32)['schema'] == 'bc1', 'Legacy parsing')
    require(tag_metadata(tag.replace('bc2', 'bc3')) is None, 'Unknown ownership')
    conflict = log + 'Shared build cache selection: {"affinity_match": false, "imports": 8}\n'
    require(parse_log(conflict.encode(), 'registry:5000/ucloud-build-cache')['selection'] is None, 'Conflicting observations')
    rows = [{'log_present': True, **parsed}]
    inventory = {'owned_tags': [{'tag': tag, 'manifest_digest': digest, **tag_metadata(tag)}]}
    require(enrich(rows, inventory)['unique_export_tag_coverage'] == 1, 'Unique alias')
    inventory['owned_tags'].append({**inventory['owned_tags'][0], 'tag': tag[:-1] + 'e'})
    require(enrich(rows, inventory)['unique_export_tag_coverage'] == 0 and rows[0]['export_tag'] is None, 'Ambiguous alias')
    with tempfile.TemporaryDirectory() as temporary:
        root, phase = Path(temporary), PHASES[0]
        (root / phase).mkdir()
        records = [{'index': index, 'build_id': str(uuid5(NAMESPACE_URL, str(index))),
                    'image_id': f'bl20260929-{phase}-{index:03}', 'recipe': RECIPES[index % 3],
                    'variant': 'app-change-' + str(5 + index // 3), 'context_sha256': 'f' * 64,
                    'build': {'status': 'succeeded'}} for index in range(48)]
        (root / phase / 'summary.json').write_text(json.dumps({'phase': phase, 'records': records}))
        result, _ = phase_rows(root, phase, 'registry:5000/ucloud-build-cache')
        require(len(result) == 48 and all(not row['log_present'] for row in result), 'Missing coverage')
        records[0]['image_id'] = '../../unowned'
        (root / phase / 'summary.json').write_text(json.dumps({'phase': phase, 'records': records}))
        try:
            phase_rows(root, phase, 'registry:5000/ucloud-build-cache')
        except ValueError:
            pass
        else:
            raise AssertionError('Foreign image path accepted')
    print(json.dumps({'self_test': 'passed', 'network_calls': 0, 'production_calls': 0}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--phase', choices=PHASES)
    parser.add_argument('--config', type=Path, default=Path('/etc/ucloud-sandboxes/deployment.json'))
    parser.add_argument('--timeout-seconds', type=float, default=60)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.phase is None or args.output is None:
        parser.error('--phase and --output are required')
    live(args)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'complete': False, 'error_type': type(exc).__name__}), file=sys.stderr)
        raise SystemExit(1) from None
