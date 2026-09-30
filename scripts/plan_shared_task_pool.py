#!/usr/bin/env python3
"""Plan source-qualified ScaleSWE sharing from retained, same-project anchors.

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


def plan_shared(inventory, catalogs, *, max_anchor_bytes=1024**3, preferred=(), seed_projects=12, seed_per_project=4):
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
    anchors = {}
    for source in sorted(ready):
        row = ready[source]
        name = group(source)
        if (name and row.get('preparation', 'source') == 'source' and not row.get('method')
                and row.get('components') and sum(c['bytes'] for c in row['components']) <= max_anchor_bytes
                and '@sha256:' in row['reference'] and '@sha256:' in row.get('source_reference', '')):
            anchors.setdefault(name, (source, row))
    groups = defaultdict(list)
    for item in inventory['images']:
        name = group(item['source'])
        if name in anchors and item['source'] not in ready:
            source, anchor = anchors[name]
            groups[name].append({**item, 'anchor': anchor['reference'], 'anchor_source': source,
                                 'anchor_source_reference': anchor['source_reference']})
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
            'anchor_projects': len(groups), 'images': selected}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--catalog', required=True, action='append', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--preferred-project', action='append', default=[])
    args = parser.parse_args()
    result = plan_shared(json.loads(args.inventory.read_text()), [json.loads(p.read_text()) for p in args.catalog],
                         preferred=args.preferred_project)
    with args.output.open('x') as output:
        output.write(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'candidates': len(result['images']), 'anchor_projects': result['anchor_projects']}))


if __name__ == '__main__':
    main()
