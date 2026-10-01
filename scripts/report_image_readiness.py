#!/usr/bin/env python3
"""Separate prepared task sources, project-base candidates, and setup prefixes.

Offline receipt accounting only. A same-project base is not a measured cheap
remaining build; split membership and live artifact availability are separate.
"""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re

from image_campaign import load_bundle


def project(item, family):
    source = item['source']
    if family == 'ScaleSWE':
        return re.sub(r'_pr\d+$', '', source) if re.search(r'_pr\d+$', source) else None
    if family in {'MultiSWE', 'R2E-Gym'}:
        return source.rsplit(':', 1)[0]
    if family in {'SWE-Lego', 'SWE-rebench v2'}:
        return item.get('repository')
    if family == 'SWE-smith':
        value = item.get('repository')
        return re.sub(r'\.[a-f0-9]{7,40}$', '', value) if value else None
    if family in {'SWE-bench Verified', 'SWE-bench Multilingual'}:
        match = re.search(r'\.x86_64\.(.+_1776_.+)-\d+(?::latest)?$', source)
        return match[1] if match else None
    return None


def bounded_base(row, maximum):
    if row.get('preparation', 'source') != 'source':
        return False
    components = row.get('components', [])
    if not 1 <= len(components) <= 8 or sum(c['bytes'] for c in components) > maximum:
        return False
    if not row.get('method'):
        return True
    proof = row.get('qualification', {})
    return (row['method'] == 'verified-flat-delta-v1' and proof.get('equivalent') is True
            and proof.get('mode') == 'full_source_scan')


def source_accounting(inventory, catalog, maximum=2 * 1024**3):
    groups = defaultdict(list)
    mismatches = []
    for item in inventory['images']:
        row = catalog['images'].get(item['source'])
        if row and row.get('status') == 'ready':
            if item.get('pinned_source') and item['pinned_source'] != row.get('source_reference'):
                mismatches.append(item['source'])
                row = None
            elif row.get('preparation', 'source') != item.get('preparation', 'source'):
                row = None
        else:
            row = None
        # A shared generic source can serve several families with different
        # fanouts. Never use the source's combined task_rows for one family.
        for use in item['uses']:
            groups[use['family']].append((item, use, row))
    result = {}
    for family, entries in sorted(groups.items()):
        task_entries = [(i, u, r) for i, u, r in entries if u['level'] != 'base_only']
        base_entries = [(i, u, r) for i, u, r in entries if u['level'] == 'base_only']
        projects = {project(i, family) for i, _, _ in task_entries} - {None}
        any_projects = {project(i, family) for i, _, r in task_entries if r} - {None}
        bounded_projects = {project(i, family) for i, _, r in task_entries
                            if r and bounded_base(r, maximum)} - {None}
        additional = [(i, u) for i, u, r in task_entries if not r and project(i, family) in bounded_projects]
        broader = [(i, u) for i, u, r in task_entries if not r and project(i, family) in any_projects]
        total = sum(u['task_rows'] for _, u, _ in task_entries)
        complete = sum(u['task_rows'] for _, u, r in task_entries if r)
        candidate = sum(u['task_rows'] for _, u in additional)
        result[family] = {
            'task_image_rows': total, 'task_image_rows_prepared': complete,
            'unique_task_images': len({i['source'] for i, _, _ in task_entries}),
            'unique_task_images_prepared': len({i['source'] for i, _, r in task_entries if r}),
            'projects': len(projects), 'projects_with_prepared_task': len(any_projects),
            'additional_rows_with_bounded_same_project_base': candidate,
            'additional_rows_with_any_same_project_example': sum(u['task_rows'] for _, u in broader),
            'rows_without_bounded_same_project_base': total - complete - candidate,
            'generic_base_rows': sum(u['task_rows'] for _, u, _ in base_entries),
            'generic_base_rows_prepared': sum(u['task_rows'] for _, u, r in base_entries if r),
            'generic_base_refs': len({i['source'] for i, _, _ in base_entries}),
            'generic_base_refs_prepared': len({i['source'] for i, _, r in base_entries if r}),
            'same_project_delta_cost_qualified': False,
        }
    return {'families': result, 'source_pin_mismatches': mismatches}


def foundation_accounting(bundle, rows):
    ready = {r['key'] for r in rows if r.get('validated') is True}
    groups = defaultdict(dict)
    for entry in bundle['foundations']:
        item = entry['item']
        groups[item.get('family', 'tmax')][item['key']] = item
    result = {}
    for family, items in groups.items():
        result[family] = {'prefixes': len(items), 'prefixes_prepared': len(set(items) & ready),
                         'planned_task_rows': sum(i['tasks'] for i in items.values()),
                         'task_rows_with_prepared_prefix': sum(i['tasks'] for k, i in items.items() if k in ready),
                         'remaining_recipe_cost_qualified': False}
    # Explicit TMax base installers and inline-only installers are disjoint:
    # plan_tmax_inline excludes every context containing base_install.sh.
    for family, parts, total in [('TMax', ['tmax', 'tmax-inline'], 14600),
                                 ('OpenSWE', ['openswe'], 36884),
                                 ('Terminal-Lego', ['terminal-prefix'], 13825)]:
        count = sum(result.get(p, {}).get('task_rows_with_prepared_prefix', 0) for p in parts)
        if count > total:
            raise ValueError('foundation counts exceed pinned corpus denominator')
        result[family] = {'pinned_corpus_task_rows': total, 'task_rows_with_prepared_prefix': count,
                         'task_rows_without_prepared_prefix': total-count,
                         'remaining_recipe_cost_qualified': False}
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ['inventory', 'catalog', 'foundation_bundle', 'foundation_snapshot', 'output']:
        p.add_argument('--'+name.replace('_', '-'), type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        raise ValueError('use a new snapshot output')
    inventory, catalog = json.loads(args.inventory.read_text()), json.loads(args.catalog.read_text())
    result = {'schema': 1, 'source_snapshot_utc': catalog.get('observed_at_utc'),
              'scope': 'Pinned upstream pools, not a verified training/heldout split or live availability check.',
              'same_project_base_policy': 'Faithful original or fully source-scanned flat delta; <=8 components, <=2 GiB EROFS. Candidate only, not a cheap-tail certificate.',
              **source_accounting(inventory, catalog),
              'foundations': foundation_accounting(load_bundle(args.foundation_bundle),
                  json.loads(args.foundation_snapshot.read_text())['rows']),
              'unmeasured': ['BIRD SQL / BIRD dev SQL', 'NeMo Calendar', 'NeMo Instruction', 'NeMo Pivot', 'NeMo Workplace'],
              'separate_service': {'BrowseComp-Plus': 'Shared BM25 corpus/index/tokenizer service; no per-question task image in pinned adapter; readiness not verified.'},
              'input_sha256': {name: hashlib.sha256(getattr(args, name).read_bytes()).hexdigest()
                              for name in ['inventory', 'catalog', 'foundation_bundle', 'foundation_snapshot']}}
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps({'output': str(args.output), 'families': len(result['families']),
                      'source_pin_mismatches': len(result['source_pin_mismatches'])}))


if __name__ == '__main__':
    main()
