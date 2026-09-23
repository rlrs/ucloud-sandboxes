#!/usr/bin/env python3
"""Temporary read-only cgroup-v2 diagnostic; no runtime/scheduler mutations."""
import argparse
import datetime
import json
import pathlib
import re
import time

FIELDS = ('usage_usec', 'user_usec', 'system_usec', 'nr_periods', 'nr_throttled', 'throttled_usec')

def counters(path):
    out = {}
    for line in path.read_text().splitlines():
        k, v = line.split()
        if v.isdecimal():
            out[k] = int(v)
    return out

def sample(root):
    rows, errors = [], 0
    try:
        paths = list(root.iterdir())
    except OSError:
        return [], 1
    for path in paths:
        if not re.fullmatch(r'[a-f0-9]{64}', path.name):
            continue
        try:
            before = path.stat()
            raw = counters(path / 'cpu.stat')
            quota = (path / 'cpu.max').read_text().strip()
            populated = counters(path / 'cgroup.events').get('populated')
            after = path.stat()
            if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                errors += 1
                continue
            row = {'runtime': path.name, 'identity': f'{before.st_dev}:{before.st_ino}',
                   'cpu_max': quota, 'populated': populated}
            row.update({k: raw.get(k) for k in FIELDS})
            rows.append(row)
        except (OSError, ValueError):
            errors += 1
    return rows, errors

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--period', type=float, default=10)
    p.add_argument('--duration', type=float, default=1800)
    p.add_argument('--root', type=pathlib.Path, default=pathlib.Path('/sys/fs/cgroup/ucloud-sandboxes'))
    a = p.parse_args()
    previous, last, start = {}, None, time.monotonic()
    while True:
        now = time.monotonic()
        rows, errors = sample(a.root)
        sample_elapsed = time.monotonic() - now
        current = {r['identity']: r for r in rows}
        deltas, known, resets = {k: 0 for k in FIELDS}, {k: 0 for k in FIELDS}, 0
        for identity, row in current.items():
            before = previous.get(identity)
            if before is None:
                continue
            if any(row[k] is not None and before[k] is not None and row[k] < before[k] for k in FIELDS):
                resets += 1
                continue
            for k in FIELDS:
                if row[k] is not None and before[k] is not None:
                    deltas[k] += row[k] - before[k]
                    row.setdefault('delta', {})[k] = row[k] - before[k]
                    known[k] += 1
        elapsed = now - last if last is not None else None
        report = {
            'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'scope': str(a.root), 'elapsed_seconds': elapsed, 'sample_elapsed_seconds': sample_elapsed,
            'observed_cgroups': len(rows),
            'populated_cgroups': sum(r['populated'] == 1 for r in rows),
            'read_errors': errors,
            'new_identities': len(current.keys() - previous.keys()),
            'removed_identities': len(previous.keys() - current.keys()),
            'counter_resets': resets,
            'delta': {k: deltas[k] if known[k] else None for k in FIELDS},
            'delta_coverage': known, 'runtimes': sorted(rows, key=lambda r: (r.get('delta', {}).get('throttled_usec', 0), r.get('delta', {}).get('usage_usec', 0)), reverse=True)[:8],
        }
        if elapsed and known['usage_usec']:
            report['observed_cpu_equivalents'] = deltas['usage_usec'] / (elapsed * 1e6)
        print(json.dumps(report, separators=(',', ':')), flush=True)
        previous, last = current, now
        if now - start >= a.duration:
            break
        time.sleep(max(0, a.period - (time.monotonic() - now)))

if __name__ == '__main__':
    main()
