#!/usr/bin/env python3
"""Bounded owned builder reservation control; no credential output."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, '/work/ucloud-sandboxes/build-load-20260929')
import live_build_load_benchmark as bench

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('action', choices=('prepare', 'hold', 'state', 'release'))
parser.add_argument('--phase', choices=('seed', 'repeat'), required=True)
args = parser.parse_args()
root = Path('/work/ucloud-sandboxes/build-cache-affinity-load-20260929')
_, config, factory = bench.clients()
client = factory()
state = {'at': bench.stamp(), 'fleet': bench.fleet(config),
         'prepared': client.list_prepared_builders(),
         'sandbox_count': len(client.list_sandboxes()),
         'active_builds': sum(b['status'] not in {'succeeded', 'failed'} for b in client.list_image_builds())}
reservation = 'build-cache-affinity-20260929-' + args.phase
if args.action != 'state':
    assert state['sandbox_count'] == state['active_builds'] == 0, 'Work must be idle'
    if args.action == 'prepare':
        assert not state['fleet'], 'Initial preparation requires a fresh pool'
    if args.action in {'prepare', 'hold'}:
        state['prepare'] = client.prepare_builder(count=4, ttl_seconds=3600, prepare_id=reservation)
    else:
        state['release'] = client.delete_prepared_builder(reservation)
    path = root / ('pool-' + args.phase + '-' + args.action + '.json')
    with path.open('x') as output:
        json.dump(state, output, indent=2)
        output.write('\n')
print(json.dumps(state))
