#!/usr/bin/env python3
"""Plan, or explicitly launch, the frozen 48-build test in its classified cgroup."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess

RELEASE = Path('/work/ucloud-sandboxes/build-cache-single-import-20260929-r1')
ROOT = Path('/work/ucloud-sandboxes/build-cache-single-import-load-20260929')
BENCH_ROOT = Path('/work/ucloud-sandboxes/build-load-20260929')
PYTHON = '/work/ucloud-sandboxes/gateway-venv/bin/python'
PHASE = 'single-import-repeat'
UNIT = 'ucloud-build-load-client-single-import-repeat'
HARNESS_SHA256 = 'd0b2754af69c69d9a303fd15117f60fad044ca0e559b3a1cfbe934f49682c6ec'
SDK_SHA256 = 'd15b65fbb5e1570fde69cb9d571789a9b61d4418682efc17732bdc9c2ca8414c'
WRAPPER_SHA256 = 'e9c60880598396d18aa377cc02b5659d9f4d3610cf2ae65411b829361beb288b'
INVENTORY_SHA256 = '45efcdbd714d7fbc6748b26f7fcdc29b4b18d1f4117baac0feb8af24e32505a7'
SDK_WHEEL = Path('/work/ucloud-sandboxes/sdk-status-0.4.33-20260928/ucloud_sandboxes_sdk-0.4.33-py3-none-any.whl')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command(wheel, inventory):
    if re.fullmatch(r'[0-9a-f]{64}', wheel) is None:
        raise ValueError('Expected exact candidate wheel SHA256')
    return ['systemd-run', '--unit=' + UNIT, '--property=Type=exec', '--property=User=root',
            '--property=UMask=0077', '--property=RuntimeMaxSec=1600', '--wait', '--collect', '--pipe',
            PYTHON, str(RELEASE / 'qualify_build_optimization.py'),
            '--source-root', str(BENCH_ROOT), '--output-root', str(ROOT),
            '--fixture-manifests', str(inventory), '--inventory-sha256', INVENTORY_SHA256,
            '--harness', str(BENCH_ROOT / 'live_build_load_benchmark.py'), '--harness-sha256', HARNESS_SHA256,
            '--phase', PHASE, '--candidate', 'B2-selective-and-scheduling', '--artifact-sha256', wheel, '--run']


def prepare(wheel, inventory):
    wrapper = RELEASE / 'qualify_build_optimization.py'
    for path, expected in ((wrapper, WRAPPER_SHA256), (BENCH_ROOT / 'live_build_load_benchmark.py', HARNESS_SHA256),
                           (SDK_WHEEL, SDK_SHA256), (RELEASE / 'ucloud_sandboxes-0.7.0-py3-none-any.whl', wheel)):
        if sha(path) != expected:
            raise ValueError('A frozen qualification artifact changed')
    deployed = json.loads((RELEASE / 'deployment-receipt.json').read_text())
    if deployed['wheel_sha256'] != wheel or deployed['node_package_root'] != str(RELEASE):
        raise ValueError('Candidate deployment identity does not match')
    if (ROOT / PHASE).exists() or (ROOT / (PHASE + '.qualification.json')).exists():
        raise ValueError('Measured phase already exists; do not reuse image identities')
    spec = importlib.util.spec_from_file_location('frozen_qualification_wrapper', wrapper)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    verified = module.verify_contexts(BENCH_ROOT, inventory, INVENTORY_SHA256)
    return {'phase': PHASE, 'unit': UNIT, 'candidate_wheel_sha256': wheel,
            'verified_contexts': len(verified), 'harness_sha256': HARNESS_SHA256,
            'sdk_wheel_sha256': SDK_SHA256, 'wrapper_sha256': WRAPPER_SHA256,
            'systemd_argv': command(wheel, inventory), 'ran': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wheel-sha256')
    parser.add_argument('--fixture-manifests', type=Path, help='Explicit path to the frozen inventory copy')
    parser.add_argument('--run', action='store_true')
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        argv = command('a' * 64, Path('/tmp/frozen-fixture.json'))
        assert '--unit=ucloud-build-load-client-single-import-repeat' in argv
        assert '--property=RuntimeMaxSec=1600' in argv and '--wait' in argv
        assert argv[argv.index('--phase') + 1] == PHASE
        assert argv[argv.index('--output-root') + 1] == str(ROOT)
        assert argv[argv.index('--artifact-sha256') + 1] == 'a' * 64
        print(json.dumps({'self_test': 'passed', 'network_calls': 0, 'production_calls': 0}))
        return
    if args.wheel_sha256 is None or args.fixture_manifests is None:
        parser.error('--wheel-sha256 and --fixture-manifests are required')
    # Validate syntax before reading the candidate path.
    command(args.wheel_sha256, args.fixture_manifests)
    receipt = prepare(args.wheel_sha256, args.fixture_manifests)
    if not args.run:
        print(json.dumps(receipt, indent=2))
        return
    path = ROOT / 'run-launch.json'
    with path.open('x') as output:
        path.chmod(0o600)
        json.dump(receipt, output, indent=2)
    try:
        result = subprocess.run(receipt['systemd_argv'], timeout=1700, check=False)
        receipt.update(ran=True, returncode=result.returncode)
    except Exception as exc:
        receipt['error_type'] = type(exc).__name__
        raise
    finally:
        path.write_text(json.dumps(receipt, indent=2) + '\n')
    raise SystemExit(result.returncode)


if __name__ == '__main__':
    main()
