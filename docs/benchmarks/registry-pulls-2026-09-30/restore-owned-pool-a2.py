"""Return four exact owned builders to the pinned old runtime after candidate qualification.

Run on the gateway only after explicit root GO. The frozen node helper rechecks
identity, zero owned work and its drain token before stopping any service.
No sampler, capacity-policy, dependency or provider changes are made here.
"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

NODES = {'168016286': '10.42.0.3', '168016287': '10.42.0.4',
         '168016314': '10.42.0.5', '168016315': '10.42.0.6'}
ROOT = Path('/work/ucloud-sandboxes/registry-pull-qualification-20260930/baseline-runtime')
RESULTS = Path('/work/ucloud-sandboxes/registry-pull-qualification-20260930/upgrades-a2')
REMOTE = Path('/work/registry-pulls-baseline-a2-inputs')
WHEEL = 'ucloud_sandboxes-0.7.0-py3-none-any.whl'
HASHES = {WHEEL: '405516727b99eed510a0bcfcee39203cb69f67a4b56ad7c68bc5d3eaa672e628',
          'upgrade_owned_builder_a2.py': '52d10206df72996fc83c66f0e1480b4542adc143cec46a36c873baf516a718dc'}


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
        before = json.loads(ssh(node, ['python3', str(REMOTE / 'upgrade_owned_builder_a2.py'),
                                       '--expected-job-id', node]))
        if (before['job_id'] != node or before['active_builds'] != 0
                or before['draining'] or before['admission_open'] is not True):
            raise ValueError('Node not ready for owned idle upgrade')
        save('before-' + node + '.json', before)
        after = json.loads(ssh(node, ['python3', str(REMOTE / 'upgrade_owned_builder_a2.py'),
            '--expected-job-id', node, '--expected-node-epoch', before['node_epoch'],
            '--apply', '--wheel', str(REMOTE / WHEEL), '--wheel-sha256', HASHES[WHEEL],
            '--output-root', '/work/registry-pulls-baseline-a2-20260930']))
        if not (after['complete'] and after['service_changed']
                and after['wheel_sha256'] == HASHES[WHEEL]
                and after['after']['job_id'] == node
                and after['after']['node_epoch'] == before['node_epoch']
                and after['after']['finishing_capacity'] == 2):
            raise ValueError('Candidate upgrade receipt is invalid')
        save('upgrade-' + node + '.json', after)
        result.update(complete=True, receipt=str(RESULTS / ('upgrade-' + node + '.json')))
    except Exception as error:
        result['error_type'] = type(error).__name__
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply-after-candidate-qualification', action='store_true')
    args = parser.parse_args()
    if not args.apply_after_candidate_qualification:
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



if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'complete': False, 'error_type': type(error).__name__}), flush=True)
        raise SystemExit(1) from None
