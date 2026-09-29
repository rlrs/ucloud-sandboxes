#!/usr/bin/env python3
"""Control only the owned four-builder single-import reservation; no secret output."""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path('/work/ucloud-sandboxes/build-cache-single-import-load-20260929')
BENCH_ROOT = Path('/work/ucloud-sandboxes/build-load-20260929')
RESERVATION = 'build-cache-single-import-20260929'
HARNESS_SHA256 = 'd0b2754af69c69d9a303fd15117f60fad044ca0e559b3a1cfbe934f49682c6ec'
SDK_SHA256 = 'd15b65fbb5e1570fde69cb9d571789a9b61d4418682efc17732bdc9c2ca8414c'


def fleet(path):
    with sqlite3.connect(path.absolute().as_uri() + '?mode=ro', uri=True) as db:
        values = [json.loads(row[0]) for row in db.execute(
            "SELECT payload FROM control_records WHERE namespace='heartbeat'")]
    keys = ('job_id', 'node_url', 'active_image_builds', 'active_sandboxes',
            'physical_disk_free_mb', 'updated_at', 'capabilities')
    return [{key: item.get(key) for key in keys} for item in values]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'hold', 'state', 'release'))
    parser.add_argument('--output', type=Path, help='New receipt path; use a new path for repeated holds')
    args = parser.parse_args()
    path = args.output or ROOT / ('pool-' + args.action + '.json')
    if args.action != 'state' and path.exists():
        raise ValueError('Choose a new output before any reservation mutation')
    if hashlib.sha256((BENCH_ROOT / 'live_build_load_benchmark.py').read_bytes()).hexdigest() != HARNESS_SHA256:
        raise ValueError('Frozen harness changed')
    sys.path.insert(0, str(BENCH_ROOT))
    import live_build_load_benchmark as bench
    if hashlib.sha256(Path(bench.SDK_WHEEL).read_bytes()).hexdigest() != SDK_SHA256:
        raise ValueError('Frozen SDK changed')
    _, config, factory = bench.clients()
    client = factory()
    state = {'at': bench.stamp(), 'fleet': fleet(config.control_state_file()),
             'prepared': client.list_prepared_builders(),
             'sandbox_count': len(client.list_sandboxes()),
             'active_builds': sum(b['status'] not in {'succeeded', 'failed'} for b in client.list_image_builds())}
    if args.action != 'state':
        if state['sandbox_count'] or state['active_builds']:
            raise ValueError('Owned reservation changes require idle work')
        if args.action == 'prepare' and state['fleet']:
            raise ValueError('Prepare requires a fresh, empty fleet')
        # Reserve the receipt name before making an API change. Failure retains
        # the attempted action without exposing provider/SDK error text.
        with path.open('x') as output:
            path.chmod(0o600)
            state.update(action=args.action, reservation=RESERVATION, attempted=True)
            json.dump(state, output, indent=2)
        try:
            state['result'] = (client.prepare_builder(count=4, ttl_seconds=3600, prepare_id=RESERVATION)
                               if args.action in {'prepare', 'hold'} else client.delete_prepared_builder(RESERVATION))
        except Exception as exc:
            state['error_type'] = type(exc).__name__
            path.write_text(json.dumps(state, indent=2) + '\n')
            raise
        path.write_text(json.dumps(state, indent=2) + '\n')
    print(json.dumps(state))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'complete': False, 'error_type': type(exc).__name__}), file=sys.stderr)
        raise SystemExit(1) from None
