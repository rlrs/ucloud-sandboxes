#!/usr/bin/env python3
"""Select missing project examples and dependency prefixes before task variants.

Offline planning only: project fanout is potential reuse, not task readiness.
Generate a fresh plan from live receipts and active assignments before admission.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path

from cache_source_receipts import load, select
from image_campaign import load_bundle
from prepare_image_foundations import validate_context
from report_image_readiness import bounded_base, project

GIB = 1024**3


def matching_ready(item, catalog):
    row = catalog['images'].get(item['source'], {})
    proof = row.get('qualification', {})
    faithful = (bool(row.get('components')) and (not row.get('method')
        or (row['method'] in {'verified-flat-delta-v1', 'verified-oci-delta-v1'}
            and proof.get('equivalent') is True and proof.get('mode') == 'full_source_scan')))
    return (row.get('status') == 'ready'
            and row.get('preparation', 'source') == item.get('preparation', 'source')
            and (not item.get('pinned_source') or item['pinned_source'] == row.get('source_reference'))
            and '@sha256:' in row.get('reference', '')
            and '@sha256:' in row.get('source_reference', '')
            and (item.get('preparation') == 'swesmith-v1' or faithful))


def source_bases(inventory, catalog, metadata, *, per_family=32, assigned=(),
                 externally_managed=('ScaleSWE',), maximum=2*GIB):
    groups = defaultdict(list)
    for item in inventory['images']:
        for use in item['uses']:
            if use['level'] == 'base_only':
                key = ('generic_base', item['source'])
            elif use['family'] == 'SWE-smith':
                # Distinct dependency environments are not interchangeable even
                # when their repository names match.
                key = ('environment', item['source'])
            else:
                name = project(item, use['family'])
                key = ('project', name) if name else ('source', item['source'])
            groups[(use['family'], *key)].append((item, use['task_rows']))
    assigned = set(assigned)
    pending, covered, held, deferred = defaultdict(list), defaultdict(int), defaultdict(int), []
    for (family, kind, name), entries in sorted(groups.items()):
        if any(matching_ready(item, catalog) for item, _ in entries):
            covered[family] += 1
            continue
        if family in externally_managed or any(item['source'] in assigned for item, _ in entries):
            held[family] += 1
            continue
        choices = []
        for item, _ in entries:
            receipt = select(item['source'], item.get('pinned_source'), metadata.get(item['source'], {}))
            if receipt and receipt['compressed_bytes'] > maximum:
                continue
            choices.append((item, receipt))
        if not choices:
            deferred.append({'family': family, 'kind': kind, 'group': name,
                             'reason': 'all known sources exceed compressed input bound'})
            continue
        # Prefer measured small sources over unknowns; never download all task
        # variants merely to choose one representative.
        item, receipt = min(choices, key=lambda pair: (
            pair[1] is None, pair[1]['compressed_bytes'] if pair[1] else maximum,
            not bool(pair[0].get('pinned_source')), pair[0]['source']))
        pin = item.get('pinned_source') or (receipt['reference'] if receipt else None)
        row = {**item, 'selection_family': family, 'base_kind': kind, 'base_group': name,
               'potential_task_rows': sum(n for _, n in entries),
               'source_compressed_bytes': receipt['compressed_bytes'] if receipt else None,
               'metadata_preflight_required': receipt is None,
               'pin_required_before_build': pin is None}
        if pin:
            row['pinned_source'] = pin
        pending[family].append(row)
    for rows in pending.values():
        rows.sort(key=lambda r: (-r['potential_task_rows'], r['source_compressed_bytes'] is None,
                                r['source_compressed_bytes'] or maximum, r['source']))
    # Round robin prevents large corpora from consuming every admission slot.
    images, seen = [], set()
    for index in range(per_family):
        for family in sorted(pending):
            if index < len(pending[family]):
                row = pending[family][index]
                if row['source'] not in seen:
                    images.append(row)
                    seen.add(row['source'])
    return {'schema': 1, 'images': images, 'summary': {
        'existing_groups': dict(covered), 'externally_assigned_groups': dict(held),
        'missing_admissible_groups': {f: len(rs) for f, rs in pending.items()},
        'selected_groups': {f: min(per_family, len(rs)) for f, rs in pending.items()},
        'deferred_groups': deferred},
        'scope': 'One example per missing project or exact environment. Fanout is potential, not readiness.'}


def missing_prefixes(bundle, snapshot, *, per_family=32, assigned=()):
    ready = {r['key'] for r in snapshot['rows'] if r.get('validated') is True}
    grouped, seen = defaultdict(list), {}
    for entry in bundle['foundations']:
        key = entry['item']['key']
        if key in seen and seen[key] != entry:
            raise ValueError('conflicting foundation inputs')
        if key in seen:
            continue
        seen[key] = entry
        if key not in ready and key not in assigned:
            grouped[entry['item'].get('family', 'tmax')].append(entry)
    for rows in grouped.values():
        rows.sort(key=lambda e: (-e['item']['tasks'], e['item']['key']))
    return {f: rows[:per_family] for f, rows in grouped.items()}, {
        f: {'missing_prefixes': len(rows), 'selected_prefixes': min(per_family, len(rows)),
            'selected_recipe_rows': sum(r['item']['tasks'] for r in rows[:per_family])}
        for f, rows in grouped.items()}


def shared_sources(sources, catalog, metadata, fallback_source):
    """Route measured flat sources through existing content-based sharing."""
    anchor = catalog['images'].get(fallback_source, {})
    if (not matching_ready({'source': fallback_source}, catalog)
            or anchor.get('method') or not bounded_base(anchor, 2*GIB)):
        raise ValueError('fallback must be a bounded faithful original base')
    shared, normal = [], []
    for item in sources['images']:
        receipt = select(item['source'], item.get('pinned_source'), metadata.get(item['source'], {}))
        if item.get('preparation', 'source') == 'source' and receipt and receipt['layer_count'] == 1:
            shared.append({**item, 'anchor': anchor['reference'], 'anchor_source': fallback_source,
                'anchor_source_reference': anchor['source_reference'], 'anchor_strategy': 'shared_fallback'})
        else:
            normal.append(item)
    return {**sources, 'images': normal}, {'schema': 1, 'images': shared,
        'scope': 'Use dependency index selection and full source qualification before publication.'}


def write_plans(output, sources, prefixes, provenance):
    output.mkdir(parents=True, exist_ok=False)
    source_root = output / 'sources'
    source_root.mkdir()
    (source_root / 'plan.json').write_text(json.dumps(sources, indent=2)+'\n')
    for family, entries in prefixes.items():
        if family not in {'tmax', 'tmax-inline', 'openswe', 'terminal-prefix'}:
            raise ValueError('invalid foundation family')
        root = output / family
        root.mkdir()
        revisions = {e['revision'] for e in entries}
        if len(revisions) != 1:
            raise ValueError('conflicting foundation revisions')
        for entry in entries:
            item = entry['item']
            # validate_context checks contents/hash; reject path components
            # before creating any directory from the input.
            expected = 'foundation-' + family + '-' + item['key'][:32]
            if family not in {'tmax', 'tmax-inline', 'openswe', 'terminal-prefix'} or item['image_id'] != expected:
                raise ValueError('invalid foundation identity')
            if len(item['key']) != 64 or any(c not in '0123456789abcdef' for c in item['key']):
                raise ValueError('invalid foundation key')
            context = root / expected
            context.mkdir()
            for name, content in entry['files'].items():
                if name not in {'Dockerfile', 'base_install.sh'}:
                    raise ValueError('invalid foundation input file')
                (context / name).write_text(content)
            validate_context(root, item)
        (root / 'plan.json').write_text(json.dumps({'schema': 1, 'revision': next(iter(revisions)),
            'foundations': [e['item'] for e in entries]}, indent=2)+'\n')
    (output / 'expansion.json').write_text(json.dumps(provenance, indent=2)+'\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['inventory', 'catalog', 'source_receipts', 'foundation_bundle', 'foundation_snapshot', 'output']:
        parser.add_argument('--'+name.replace('_', '-'), required=True, type=Path)
    parser.add_argument('--per-family', type=int, default=32)
    parser.add_argument('--fallback-anchor-source', help='route known flat sources through dependency sharing')
    parser.add_argument('--exclude-plan', type=Path, action='append', default=[])
    args = parser.parse_args()
    if args.per_family < 1:
        parser.error('per-family must be positive')
    assigned_sources, assigned_prefixes = set(), set()
    for path in args.exclude_plan:
        plan = json.loads(path.read_text())
        if plan.get('schema') != 1:
            raise ValueError('unsupported assignment plan')
        assigned_sources.update(r['source'] for r in plan.get('images', []))
        assigned_prefixes.update(r['key'] for r in plan.get('foundations', []))
    catalog, metadata = json.loads(args.catalog.read_text()), load(args.source_receipts)
    sources = source_bases(json.loads(args.inventory.read_text()), catalog,
        metadata, per_family=args.per_family, assigned=assigned_sources)
    prefixes, counts = missing_prefixes(load_bundle(args.foundation_bundle),
        json.loads(args.foundation_snapshot.read_text()), per_family=args.per_family, assigned=assigned_prefixes)
    provenance = {'schema': 1, 'status': 'offline_plan_not_submitted', 'sources': sources['summary'],
        'prefixes': counts, 'volume_ceiling_provider_units': 4000, 'free_floor_gib': 500,
        'maximum_source_compressed_gib': 2,
        'admission': 'Refresh live receipts and assignments; reallocate existing budgets; resolve missing pins before building. No per-task completion phase.',
        'input_sha256': {n: hashlib.sha256(getattr(args, n).read_bytes()).hexdigest()
            for n in ['inventory', 'catalog', 'source_receipts', 'foundation_bundle', 'foundation_snapshot']},
        'excluded_plan_sha256': [hashlib.sha256(p.read_bytes()).hexdigest() for p in args.exclude_plan]}
    shared = None
    if args.fallback_anchor_source:
        sources, shared = shared_sources(sources, catalog, metadata, args.fallback_anchor_source)
        provenance['shared_sources'] = len(shared['images'])
        provenance['fallback_anchor_source'] = args.fallback_anchor_source
    provenance['normal_sources'] = len(sources['images'])
    write_plans(args.output, sources, prefixes, provenance)
    if shared:
        root = args.output / 'shared-sources'
        root.mkdir()
        (root / 'plan.json').write_text(json.dumps(shared, indent=2)+'\n')
    print(json.dumps(provenance, indent=2))


if __name__ == '__main__':
    main()
