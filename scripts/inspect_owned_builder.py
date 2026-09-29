#!/usr/bin/env python3
"""Audit one explicit idle builder against a pinned wheel without changing it.

Run on the owned builder with the reviewed upgrade helper beside this file.
Credentials, process arguments and environment values never enter the receipt.
Only a new optional receipt file is written; no service/config/runtime changes.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess

UPGRADE_HELPER_SHA256 = '160559f723d08e83b88759f1e4f8b9d9b4675db319315a598cd22e22a898591c'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def audit_package(source, members):
    expected = {name: body for name, body in members.items() if name.startswith('ucloud_sandboxes/')}
    if not expected:
        raise ValueError('Wheel has no runtime package')
    package = source / 'ucloud_sandboxes'
    if package.is_symlink():
        raise ValueError('Runtime package is unexpectedly symlinked')
    actual = set()
    for path in package.rglob('*'):
        if '__pycache__' in path.parts or path.suffix == '.pyc':
            continue
        if path.is_symlink():
            raise ValueError('Runtime package contains an unexpected symlink')
        if path.is_file():
            actual.add(path.relative_to(source).as_posix())
    if actual != set(expected):
        raise ValueError('Installed runtime file set differs from wheel')
    for name, body in expected.items():
        if (source / name).read_bytes() != body:
            raise ValueError('Installed runtime bytes differ from wheel')
    return len(expected)


def verify_import_origin(state):
    cwd = Path(f"/proc/{state['pid']}/cwd").resolve(strict=True)
    # Match the running -m interpreter's first search entry and environment.
    # PathFinder resolves package/module locations without executing app code.
    program = ("import importlib.machinery,json,sys;"
               "p=importlib.machinery.PathFinder.find_spec('ucloud_sandboxes',sys.path);"
               "m=importlib.machinery.PathFinder.find_spec('ucloud_sandboxes.cli',p.submodule_search_locations);"
               "print(json.dumps([p.origin,m.origin]))")
    result = subprocess.run([state['argv'][0], '-c', program], env=state['env'], cwd=cwd,
                            capture_output=True, text=True, timeout=10)
    expected = [state['source'] / 'ucloud_sandboxes' / name for name in ('__init__.py', 'cli.py')]
    if result.returncode or [Path(value).resolve(strict=True) for value in json.loads(result.stdout)] != expected:
        raise ValueError('Builder import origin differs from audited runtime')
    return cwd


def inspect(args, helper):
    helper.require(sha(args.wheel) == args.wheel_sha256, 'Wheel digest differs')
    state = helper.discover(args.expected_job_id, args.expected_node_epoch)
    helper.require(helper.flag(state['argv'], '--max-finishing-image-builds') == '2',
                   'Expected qualified two-finisher policy')
    current = helper.heartbeat(state)
    helper.require(not current['draining'] and current['admission_open'] is True,
                   'Builder admission must be open and idle')
    helper.require(current['labels'].get(helper.LABEL) == '4', 'Expected four idle solve permits')
    cwd = verify_import_origin(state)
    count = audit_package(state['source'], helper.wheel_members(args.wheel))
    # A node epoch can survive a service restart; pin the same PID and runtime
    # path at both ends of the disk-byte audit rather than trusting epoch alone.
    after = helper.discover(args.expected_job_id, state['node_epoch'])
    helper.require(after['pid'] == state['pid'] and after['source'] == state['source']
                   and after['argv'] == state['argv'], 'Builder process changed during audit')
    helper.require(verify_import_origin(after) == cwd, 'Builder working directory changed during audit')
    return {'kind': 'installed_runtime', 'complete': True, 'service_changed': False,
            'wheel_sha256': args.wheel_sha256, 'inspector_sha256': sha(__file__),
            'upgrade_helper_sha256': UPGRADE_HELPER_SHA256,
            'installed_files_match_wheel': count, 'runtime_file_set_matches': True,
            'import_origins_match': True,
            'after': {'job_id': state['job_id'], 'node_epoch': state['node_epoch'],
                      'finishing_capacity': 2, 'active_builds': 0, 'admission_open': True,
                      'draining': False}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-job-id', required=True)
    parser.add_argument('--expected-node-epoch')
    parser.add_argument('--wheel', type=Path, required=True)
    parser.add_argument('--wheel-sha256', required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise ValueError('Run on the owned builder as root')
    if args.output is not None and args.output.exists():
        raise ValueError('Receipt output must be new')
    path = Path(__file__).with_name('upgrade_owned_builder.py')
    if sha(path) != UPGRADE_HELPER_SHA256:
        raise ValueError('Reviewed inspection helper changed')
    spec = importlib.util.spec_from_file_location('owned_builder_helpers', path)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    result = inspect(args, helper)
    payload = json.dumps(result, indent=2) + '\n'
    if args.output is not None:
        with args.output.open('x') as stream:
            args.output.chmod(0o600)
            stream.write(payload)
    print(payload, end='')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'complete': False, 'error_type': type(error).__name__}))
        raise SystemExit(1) from None
