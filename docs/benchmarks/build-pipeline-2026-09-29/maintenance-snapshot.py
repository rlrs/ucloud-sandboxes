#!/usr/bin/env python3
"""Read-only, allowlisted registry-maintenance journal summary; run on gateway."""
import argparse
from datetime import datetime, timezone
import json
import subprocess

UNITS = tuple('ucloud-sandbox-registry-' + name + '.service'
              for name in ('prune', 'gc', 'pressure'))


def stamp(microseconds):
    return datetime.fromtimestamp(int(microseconds) / 1e6, timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--since', required=True)
    parser.add_argument('--until', required=True)
    args = parser.parse_args()
    command = ['journalctl', '--since', args.since, '--until', args.until,
               '--no-pager', '-o', 'json']
    for unit in UNITS:
        command += ['-u', unit]
    events, reports, buffers, counts = [], [], {}, {unit: 0 for unit in UNITS}
    with subprocess.Popen(command, stdout=subprocess.PIPE, text=True) as process:
        for line in process.stdout:
            row = json.loads(line)
            message = row.get('MESSAGE', '')
            if not isinstance(message, str):
                continue
            unit = row.get('_SYSTEMD_UNIT')
            at = stamp(row['__REALTIME_TIMESTAMP'])
            if unit not in UNITS:
                unit = next((name for name in UNITS if name in message), None)
                if unit is not None:
                    for prefix, kind in (('Starting ', 'start'), ('Finished ', 'finish'),
                                         ('Failed ', 'failed')):
                        if message.startswith(prefix):
                            events.append({'at': at, 'unit': unit, 'event': kind})
                continue
            counts[unit] += 1
            if 'PythonFinalizationError:' in message:
                events.append({'at': at, 'unit': unit, 'event': 'python_finalization_error'})
            if message == '{':
                buffers[unit] = [message]
                continue
            if unit in buffers:
                buffers[unit].append(message)
                if message != '}':
                    continue
                payload = '\n'.join(buffers.pop(unit))
            elif message.startswith('{'):
                payload = message
            else:
                continue
            try:
                value = json.loads(payload)
            except json.JSONDecodeError:
                events.append({'at': at, 'unit': unit, 'event': 'unparsed_structured_report'})
                continue
            projected = {'at': at, 'unit': unit}
            for key in ('action', 'gc_runs', 'deleted_manifest_count', 'active_lease_count'):
                if key in value:
                    projected[key] = value[key]
            for section, keys in {
                'build_cache': ('inventoried_tags', 'retained_entries', 'retained_bytes',
                                'deleted_manifests', 'max_entries', 'max_bytes'),
                'registry_disk_before': ('used_bytes', 'available_bytes', 'used_percent', 'state'),
                'maintenance_state': ('last_gc_at', 'last_gc_deleted_bytes', 'deleted_since_gc'),
            }.items():
                if isinstance(value.get(section), dict):
                    projected[section] = {key: value[section][key] for key in keys if key in value[section]}
            reports.append(projected)
        if process.wait() != 0:
            raise RuntimeError('journal query failed')
    print(json.dumps({'since': args.since, 'until': args.until, 'units': UNITS,
                      'journal_rows': counts, 'events': events, 'reports': reports,
                      'incomplete_structured_reports': sorted(buffers)}, indent=2))


if __name__ == '__main__':
    main()
