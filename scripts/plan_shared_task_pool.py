#!/usr/bin/env python3
"""Plan source-qualified ScaleSWE sharing from retained anchors.

Private references are derived from the supplied current catalogs. Public
anchor/source identities remain in the plan for recovery and review. Planning
never counts a candidate as covered and never contacts an upstream registry.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path
import re


def plan_shared(inventory, catalogs, *, max_anchor_bytes=1024**3, preferred=(), seed_projects=12, seed_per_project=4,
                fallback_anchor_source=None, exclude_sources=(), allow_compact_anchors=False):
    ready = {}
    for catalog in catalogs:
        if catalog.get('schema') != 1:
            raise ValueError('unsupported source catalog')
        for source, row in catalog['images'].items():
            if row.get('status') == 'ready':
                if source in ready and ready[source]['reference'] != row['reference']:
                    raise ValueError('conflicting source versions')
                ready[source] = row
    def group(source):
        if not source.startswith('aweaiteam/scaleswe:') or not re.search(r'_pr\d+$', source):
            return None
        return re.sub(r'_pr\d+$', '', source)
    anchors, eligible, project_candidates = {}, {}, {}
    for source in sorted(ready):
        row = ready[source]
        name = group(source)
        if (name and row.get('preparation', 'source') == 'source' and not row.get('method')
                and row.get('components') and sum(c['bytes'] for c in row['components']) <= max_anchor_bytes
                and '@sha256:' in row['reference'] and '@sha256:' in row.get('source_reference', '')):
            anchors.setdefault(name, (source, row))
            eligible[source] = (source, row)
            project_candidates.setdefault(name, []).append((source, row))
    if allow_compact_anchors:
        for source in sorted(ready):
            row, name = ready[source], group(source)
            proof = row.get('qualification', {})
            if (name and row.get('method') == 'verified-flat-delta-v1' and proof.get('equivalent') is True
                    and proof.get('mode') == 'full_source_scan'
                    and row.get('components') and len(row['components']) <= 8
                    and sum(c['bytes'] for c in row['components']) <= max_anchor_bytes
                    and '@sha256:' in row['reference'] and '@sha256:' in row.get('source_reference', '')):
                anchors.setdefault(name, (source, row))
                project_candidates.setdefault(name, []).append((source, row))
    fallback = eligible.get(fallback_anchor_source)
    if fallback_anchor_source is not None and fallback is None:
        raise ValueError('fallback must be an eligible retained original anchor')
    excluded = set(exclude_sources)
    groups = defaultdict(list)
    for item in inventory['images']:
        name = group(item['source'])
        choice = anchors.get(name) or fallback
        if allow_compact_anchors and name in project_candidates:
            target_pr = int(item['source'].rsplit('_pr', 1)[1])
            choice = min(project_candidates[name], key=lambda candidate: (
                abs(int(candidate[0].rsplit('_pr', 1)[1]) - target_pr),
                bool(candidate[1].get('method')), candidate[0]))
        if name and choice and item['source'] not in ready and item['source'] not in excluded:
            source, anchor = choice
            groups[name].append({**item, 'anchor': anchor['reference'], 'anchor_source': source,
                                 'anchor_source_reference': anchor['source_reference'],
                                 'anchor_strategy': 'same_project' if name in anchors else 'shared_fallback'})
            if anchor.get('method'):
                groups[name][-1].update(anchor_filesystem_source=anchor['source_reference'],
                                        anchor_strategy='same_project_compact')
    for rows in groups.values():
        rows.sort(key=lambda row: row['source'])
    keys = sorted(groups, key=lambda key: (key not in preferred, -len(groups[key]), key))
    selected, seen = [], set()
    def add(row):
        if row['source'] not in seen:
            selected.append(row)
            seen.add(row['source'])
    for index in range(seed_per_project):
        for key in keys[:seed_projects]:
            if index < len(groups[key]):
                add(groups[key][index])
    for index in range(max((len(rows) for rows in groups.values()), default=0)):
        for key in keys:
            if index < len(groups[key]):
                add(groups[key][index])
    return {'schema': 1, 'scope': 'Candidates requiring full source qualification; not prepared coverage.',
            'anchor_projects': len({group(r['anchor_source']) for r in selected}),
            'anchor_images': len({r['anchor_source'] for r in selected}),
            'source_projects': len(groups), 'images': selected}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--catalog', required=True, action='append', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--preferred-project', action='append', default=[])
    parser.add_argument('--fallback-anchor-source', help='explicit original anchor for projects without a retained base')
    parser.add_argument('--exclude-plan', type=Path, action='append', default=[], help='avoid sources already assigned to another queue')
    parser.add_argument('--allow-compact-anchors', action='store_true', help='use fully source-qualified compact images within their project')
    args = parser.parse_args()
    excluded = set()
    for path in args.exclude_plan:
        plan = json.loads(path.read_text())
        if plan.get('schema') != 1:
            raise ValueError('unsupported exclusion plan')
        excluded.update(row['source'] for row in plan['images'])
    result = plan_shared(json.loads(args.inventory.read_text()), [json.loads(p.read_text()) for p in args.catalog],
                         preferred=args.preferred_project, fallback_anchor_source=args.fallback_anchor_source,
                         exclude_sources=excluded, allow_compact_anchors=args.allow_compact_anchors)
    with args.output.open('x') as output:
        output.write(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'candidates': len(result['images']), 'anchor_projects': result['anchor_projects']}))


if __name__ == '__main__':
    main()
