"""Run on the gateway only after explicit baseline-complete upgrade approval."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import time

NODES = {'168008406': '10.42.0.3', '168008407': '10.42.0.4',
         '168008410': '10.42.0.5', '168008411': '10.42.0.6'}
ROOT = Path('/work/ucloud-sandboxes/build-pipeline-20260929-r2')
RESULTS = Path('/work/ucloud-sandboxes/builder-slot-qualification-20260929/upgrades-r2')
REMOTE = Path('/work/slotq-pipeline-inputs-r2')
WHEEL = 'ucloud_sandboxes-0.7.0-py3-none-any.whl'
HASHES = {WHEEL: '405516727b99eed510a0bcfcee39203cb69f67a4b56ad7c68bc5d3eaa672e628',
          'upgrade_owned_builder.py': '160559f723d08e83b88759f1e4f8b9d9b4675db319315a598cd22e22a898591c',
          'sample_builder_admission.py': 'dc2a9617f278dd4fa09c010cf54aace4ee598c02c342646e0310d02d5873feb2'}


def ssh(node, argv, *, source=None, timeout=240):
    known = '/var/lib/ucloud-sandboxes/state/ssh-known-hosts/' + node
    command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
               '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + known,
               '-i', '/var/lib/ucloud-sandboxes/state/ssh/gateway-init',
               'root@' + NODES[node], shlex.join(argv)]
    response = subprocess.run(command, input=source, text=True, capture_output=True, timeout=timeout)
    if response.returncode:
        raise RuntimeError('Owned node command failed; inspect its sanitized receipt')
    return response.stdout


def save(name, value):
    path = RESULTS / name
    with path.open('x') as output:
        path.chmod(0o600)
        json.dump(value, output, indent=2)
        output.write('\n')


def upgrade(node):
    result = {'job_id': node, 'complete': False}
    try:
        for name, expected in HASHES.items():
            body = (ROOT / name).read_bytes()
            if hashlib.sha256(body).hexdigest() != expected:
                raise ValueError('Staged artifact checksum mismatch')
            source = ('import base64,hashlib,os\nfrom pathlib import Path\nos.umask(0o022)\n'
                      f'p=Path({str(REMOTE / name)!r});p.parent.mkdir(mode=0o755,exist_ok=True)\n'
                      f'b=base64.b64decode({base64.b64encode(body).decode()!r})\n'
                      f'assert hashlib.sha256(b).hexdigest()=={expected!r}\n'
                      'if p.exists():assert p.read_bytes()==b\n'
                      'else:p.write_bytes(b);p.chmod(0o644)\n')
            ssh(node, ['python3', '-'], source=source)
        before = json.loads(ssh(node, ['python3', str(REMOTE / 'upgrade_owned_builder.py'),
                                       '--expected-job-id', node]))
        if before['job_id'] != node or before['active_builds'] != 0 or before['draining']:
            raise ValueError('Node not ready for owned idle upgrade')
        save('before-' + node + '.json', before)
        after = json.loads(ssh(node, ['python3', str(REMOTE / 'upgrade_owned_builder.py'),
            '--expected-job-id', node, '--expected-node-epoch', before['node_epoch'],
            '--apply', '--wheel', str(REMOTE / WHEEL), '--wheel-sha256', HASHES[WHEEL],
            '--output-root', '/work/builder-pipeline-upgrade-20260929-r2']))
        if not (after['complete'] and after['service_changed']
                and after['after']['job_id'] == node
                and after['after']['node_epoch'] == before['node_epoch']
                and after['after']['finishing_capacity'] == 2):
            raise ValueError('Candidate upgrade receipt is invalid')
        save('upgrade-' + node + '.json', after)
        result.update(complete=True, receipt=str(RESULTS / ('upgrade-' + node + '.json')))
    except Exception as error:
        result['error_type'] = type(error).__name__
    return result


def probe(node):
    output = '/work/slotq-admission-r2.jsonl'
    ssh(node, ['systemd-run', '--unit=ucloud-slotq-admission', '--collect', '--quiet',
        '--property=RuntimeMaxSec=1850', '--property=Nice=10', '/usr/bin/python3',
        str(REMOTE / 'sample_builder_admission.py'), '--expected-job-id', node,
        '--output', output, '--duration', '1800', '--interval', '2'])
    time.sleep(0.2)
    ssh(node, ['systemctl', 'is-active', '--quiet', 'ucloud-slotq-admission'])
    return {'job_id': node, 'active': True, 'output': output,
            'sampler_sha256': HASHES['sample_builder_admission.py']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply-after-baseline-complete', action='store_true')
    args = parser.parse_args()
    if not args.apply_after_baseline_complete:
        raise ValueError('Explicit root approval is required before running this controller')
    for name, expected in HASHES.items():
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != expected:
            raise ValueError('Staged artifact checksum mismatch')
    RESULTS.mkdir(mode=0o700)
    with ThreadPoolExecutor(4) as executor:
        rows = list(executor.map(upgrade, NODES))
    save('pool-upgrade.json', rows)
    print(json.dumps(rows), flush=True)
    if not all(row['complete'] for row in rows):
        raise ValueError('At least one owned upgrade did not complete; qualification blocked')
    with ThreadPoolExecutor(4) as executor:
        samplers = list(executor.map(probe, NODES))
    save('admission-samplers.json', samplers)
    print(json.dumps(samplers), flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'complete': False, 'error_type': type(error).__name__}), flush=True)
        raise SystemExit(1) from None
