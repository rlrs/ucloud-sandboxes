#!/usr/bin/env python3
"""Enable the qualified two-finisher policy; preserve an exact config rollback."""
import argparse
from copy import deepcopy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path('/work/ucloud-sandboxes/build-pipeline-20260929-r2')
WHEEL = '405516727b99eed510a0bcfcee39203cb69f67a4b56ad7c68bc5d3eaa672e628'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for mode in ('warm', 'cold'):
        parser.add_argument('--' + mode + '-summary', type=Path, required=True)
        parser.add_argument('--' + mode + '-sha256', required=True)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise ValueError('Run on the gateway as root')
    spec = importlib.util.spec_from_file_location('deploy', ROOT / 'deployment-controller.py')
    deploy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(deploy)
    receipts = {}
    for mode in ('warm', 'cold'):
        path = getattr(args, mode + '_summary')
        expected = getattr(args, mode + '_sha256')
        deploy.require(sha(path) == expected, 'Qualification receipt changed')
        result = json.loads(path.read_text())
        deploy.require(result['passed'] and result['mode'] == mode and result['declared_slots'] == 6
                       and result['submitted_cases'] == result['succeeded'] == 48
                       and result['deadline_misses'] == result['client_errors'] == 0
                       and result['live_admission_drained'], 'Qualification did not pass')
        receipts[mode] = {'summary_sha256': expected, 'phase': result['phase']}
    deployed = json.loads((ROOT / 'deployment-receipt.json').read_text())
    deploy.require(deployed['wheel_sha256'] == WHEEL, 'Runtime identity changed')
    deploy.idle_guard()
    raw, _ = deploy.read_config()
    deploy.require(raw['node_package_root'] == str(ROOT) and raw['builder']['max_finishing_builds'] == 0,
                   'Unexpected current build policy')
    backup = ROOT / 'pre-enable-deployment.json'
    deploy.require(not backup.exists(), 'Activation was already attempted')
    # replace_config copies this backup's mode. Preserve the live mode so
    # the unprivileged services can read both activation and rollback config.
    shutil.copy2(deploy.CONFIG, backup)
    candidate = deepcopy(raw)
    candidate['builder']['max_finishing_builds'] = 2
    try:
        deploy.replace_config(candidate, backup)
        subprocess.run(['systemctl', 'restart', 'ucloud-sandbox-autoscaler'], check=True, timeout=60)
        checks = deploy.wait_health_current_process()
    except BaseException:
        deploy.replace_config(raw, backup)
        subprocess.run(['systemctl', 'restart', 'ucloud-sandbox-autoscaler'], check=True, timeout=60)
        deploy.wait_health_current_process()
        raise
    result = {'enabled_at': deploy.stamp(), 'wheel_sha256': WHEEL,
              'max_preparing_solving': 4, 'max_finishing_builds': 2,
              'max_owned_builds': 6, 'buildkit_parallelism': 4,
              'qualification': receipts, 'health': checks}
    deploy.write_json(ROOT / 'activation-receipt.json', result)
    print(json.dumps(result))

if __name__ == '__main__':
    main()
