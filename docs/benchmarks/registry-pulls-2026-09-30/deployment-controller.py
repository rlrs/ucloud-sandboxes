#!/usr/bin/env python3
"""Stage and apply a pinned Python-only gateway release; no provider/DB/native changes.

Run with the existing gateway venv Python. Stage/check do not alter live config or
services. Apply requires the exact staging receipt digest and an idle deployment.
Source artifacts stay intact. Explicit rollback uses the current release backup.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import time
import urllib.request
import zipfile

ROOT = Path('/work/ucloud-sandboxes/registry-pulls-20260930-r1')
VENV = Path('/work/ucloud-sandboxes/gateway-venv')
CONFIG = Path('/etc/ucloud-sandboxes/deployment.json')
SERVICES = ['ucloud-sandbox-autoscaler', 'ucloud-sandbox-placement',
            'ucloud-sandbox-gateway', 'ucloud-sandbox-relay']
BUDGET = 64 * 1024**3
PUBLIC_BASE = 'https://77.42.92.27'
WHEEL_NAME = 'ucloud_sandboxes-0.7.0-py3-none-any.whl'


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()


def write_json(path, value):
    path.write_bytes(json_bytes(value))
    path.chmod(0o600)


def stamp():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def read_config():
    from ucloud_sandboxes.config import DeploymentConfig
    raw = json.loads(CONFIG.read_text())
    require(raw.get('relay_postgres', {}).get('storage_budget_bytes') == BUDGET,
            'Unexpected relay storage budget; preserve configuration explicitly')
    config = DeploymentConfig.from_dict(raw)
    require(config.builder.max_finishing_builds == 2, 'Preserve qualified two-finisher policy')
    require(config.builder.build_execution_timeout_seconds == 1800, 'Preserve execution deadline')
    require(config.builder.buildx_cache_max_entries == 512 and
            config.builder.buildx_cache_max_bytes == 32 * 1024**3, 'Preserve cache budget')
    return raw, config


def request_json(path, *, relay=False, public=False):
    _, config = read_config()
    if public:
        url = PUBLIC_BASE + ('/relay' if relay else '') + path
        headers = {}
    else:
        token_path = config.relay_worker_token_file() if relay else config.gateway_token_file()
        headers = {'Authorization': 'Bearer ' + token_path.read_text().strip()}
        port = config.relay_port if relay else config.gateway_port
        url = f'http://127.0.0.1:{port}' + path
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=15) as response:
        return json.load(response)


def idle_guard():
    require(not request_json('/v1/sandboxes')['sandboxes'], 'Fleet must be idle')
    builds = request_json('/v1/images/builds').get('builds', [])
    require(not any(b.get('status') not in {'succeeded', 'failed'} for b in builds), 'Builders must have no active or queued work')
    # Terminal image results can still own bounded publication cleanup.
    # Probe live builder activity before replacing any runtime or service.
    _, config = read_config()
    from ucloud_sandboxes.control_state import ControlStateStore
    heartbeats = list(ControlStateStore(config.control_state_file()).load_heartbeats().values())
    for heartbeat in heartbeats:
        require(heartbeat.node_url and 'image-build' in heartbeat.capabilities,
                'Only idle qualification builders may remain during this update')
        headers = {'Authorization': 'Bearer ' + config.node_control_token_file().read_text().strip()}
        req = urllib.request.Request(heartbeat.node_url.rstrip('/') + '/v1/heartbeat', headers=headers)
        with urllib.request.urlopen(req, timeout=15) as response:
            live = json.load(response)['heartbeat']
        require(live['job_id'] == heartbeat.job_id and live['active_image_builds'] == 0
                and live['active_sandboxes'] == 0, 'Live builder still owns work or cleanup')
    stats = request_json('/v1/relay/stats', relay=True)
    for name in ('inflight', 'delivery_pending', 'pending_count', 'leased_count'):
        require(stats.get(name, 0) == 0, 'Relay must have no outstanding work')
    lifecycle = stats.get('lifecycle')
    require(isinstance(lifecycle, list), 'Lifecycle stats unavailable')
    for item in lifecycle:
        require(item.get('claimed') == 0 and item.get('due') == 0,
                'Relay has claimed or due lifecycle work')
    return {'sandboxes': 0, 'relay_inflight': 0, 'relay_delivery_pending': 0,
            'lifecycle_claimed': 0, 'lifecycle_due': 0,
            'lifecycle_deferred': sum(item.get('deferred', 0) for item in lifecycle)}


def inventory(root, *, exclude_agent=False, skip=()):
    values = {}
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root).as_posix()
        if relative in skip or '__pycache__' in path.parts or path.suffix == '.pyc':
            continue
        if exclude_agent and any(part == 'ucloud_sandboxes' or
                                 (part.startswith('ucloud_sandboxes-') and part.endswith('.dist-info'))
                                 for part in path.relative_to(root).parts):
            continue
        if path.is_symlink():
            values[relative] = {'link': os.readlink(path)}
        elif path.is_file():
            values[relative] = {'sha256': sha(path), 'mode': stat.S_IMODE(path.stat().st_mode)}
    return values


def inventory_digest(value):
    return hashlib.sha256(json_bytes(value)).hexdigest()


def repacker(expected):
    path = ROOT / 'repack_node_bundle.py'
    require(sha(path) == expected, 'Repacker digest mismatch')
    spec = importlib.util.spec_from_file_location('qualified_repacker', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def verify_wheel(wheel, expected):
    require(sha(wheel) == expected, 'Wheel digest mismatch')
    # Dependency changes need a separate reviewed dependency upgrade, not --no-deps.
    from email.parser import BytesParser
    from importlib.metadata import distribution
    with zipfile.ZipFile(wheel) as archive:
        names = [n for n in archive.namelist() if n.endswith('.dist-info/METADATA')]
        require(len(names) == 1, 'Expected one wheel metadata file')
        metadata = BytesParser().parsebytes(archive.read(names[0]))
        old = distribution('ucloud-sandboxes')
        require(sorted(metadata.get_all('Requires-Dist', [])) == sorted(old.requires or []),
                'Wheel dependencies differ from installed package')
        require(metadata['Name'] == old.metadata['Name'] and metadata['Version'] == '0.7.0',
                'Unexpected wheel distribution/version')
        entry_name = names[0].replace('METADATA', 'entry_points.txt')
        require(archive.read(entry_name).decode() == old.read_text('entry_points.txt'),
                'Entry points differ from installed package')


def stage(args):
    require(not (ROOT / 'staging-receipt.json').exists(), 'Staging receipt already exists')
    raw, _ = read_config()
    source_root = Path(raw['node_package_root'])
    require(source_root != ROOT, 'Candidate is already the configured source')
    wheel = ROOT / WHEEL_NAME
    verify_wheel(wheel, args.wheel_sha256)
    repack = repacker(args.repacker_sha256)
    repack.inspect_wheel(wheel)
    require(shutil.disk_usage(ROOT).free > 10 * 1024**3, 'Insufficient staging disk space')
    receipt = {'started_at': stamp(), 'root': str(ROOT), 'source_root': str(source_root),
               'wheel_sha256': args.wheel_sha256, 'repacker_sha256': args.repacker_sha256,
               'controller_sha256': sha(Path(__file__)), 'config_sha256': sha(CONFIG),
               'native_changes': False, 'dependency_changes': False, 'bundles': {}}
    for role, source_digest in [('builder', args.builder_source_sha256),
                                ('sandbox', args.sandbox_source_sha256)]:
        source = source_root / f'{role}-node-package.tar.gz'
        output = ROOT / source.name
        require(not output.exists(), f'Staged {role} bundle already exists')
        require(sha(source) == source_digest, f'Qualified {role} source digest mismatch')
        with tempfile.TemporaryDirectory(prefix=f'stage-{role}-', dir=ROOT) as directory:
            work = Path(directory)
            bundle, agent = work / 'bundle', work / 'agent'
            bundle.mkdir()
            agent.mkdir()
            repack.extract_tar(source, bundle)
            manifest = json.loads((bundle / 'package-bundle.json').read_text())
            repack.validate_source_bundle(bundle, manifest)
            require(manifest['runtime']['role'] == role, 'Bundle role mismatch')
            original = deepcopy(manifest)
            agent_relative = manifest['runtime']['agent']['file']
            native = inventory(bundle, skip=('package-bundle.json', agent_relative))
            agent_archive = bundle / agent_relative
            repack.extract_tar(agent_archive, agent)
            dependencies = inventory(agent, exclude_agent=True)
            repack.replace_agent_package(agent, wheel)
            repack.validate_agent_runtime_dependencies(agent)
            require(dependencies == inventory(agent, exclude_agent=True), 'Dependency bytes changed')
            repack.build_agent_archive(agent, agent_archive)
            manifest['runtime']['agent'].update(sha256=sha(agent_archive), size=agent_archive.stat().st_size)
            expected = deepcopy(original)
            expected['runtime']['agent'] = manifest['runtime']['agent']
            require(manifest == expected, 'Unexpected runtime manifest changes')
            require(native == inventory(bundle, skip=('package-bundle.json', agent_relative)),
                    'Native/kernel/OS artifacts changed')
            repack.validate_source_bundle(bundle, manifest)
            repack.build_bundle(bundle, json_bytes(manifest), output)
            output.chmod(0o644)
            receipt['bundles'][role] = {'source_sha256': source_digest, 'sha256': sha(output),
                'bytes': output.stat().st_size, 'agent_sha256': manifest['runtime']['agent']['sha256'],
                'native_files_unchanged': True, 'dependency_files_unchanged': True,
                'native_inventory_sha256': inventory_digest(native),
                'dependency_inventory_sha256': inventory_digest(dependencies)}
    receipt['finished_at'] = stamp()
    write_json(ROOT / 'staging-receipt.json', receipt)
    print(json.dumps({'staged': True, 'receipt_sha256': sha(ROOT / 'staging-receipt.json'), **receipt}))


def validate_receipt(expected, *, live_config=True):
    path = ROOT / 'staging-receipt.json'
    require(sha(path) == expected, 'Staging receipt digest mismatch')
    receipt = json.loads(path.read_text())
    require(receipt['root'] == str(ROOT), 'Unexpected release root')
    require(receipt['controller_sha256'] == sha(Path(__file__)), 'Controller changed since staging')
    require(receipt['native_changes'] is False and receipt['dependency_changes'] is False,
            'Release changes native code or dependencies')
    verify_wheel(ROOT / WHEEL_NAME, receipt['wheel_sha256'])
    require(sha(ROOT / 'repack_node_bundle.py') == receipt['repacker_sha256'], 'Repacker changed')
    for role in ('builder', 'sandbox'):
        item = receipt['bundles'][role]
        require(item['native_files_unchanged'] and item['dependency_files_unchanged'], 'Unsafe bundle')
        require(sha(ROOT / f'{role}-node-package.tar.gz') == item['sha256'], 'Candidate bundle changed')
        require(sha(Path(receipt['source_root']) / f'{role}-node-package.tar.gz') == item['source_sha256'],
                'Source bundle changed since staging')
    if live_config:
        raw, _ = read_config()
        require(raw['node_package_root'] == receipt['source_root'], 'Configured source root changed')
        require(sha(CONFIG) == receipt['config_sha256'], 'Configuration changed since staging')
    return receipt


def service(action):
    subprocess.run(['systemctl', action, *(SERVICES if action == 'stop' else reversed(SERVICES))],
                   check=True, timeout=120)


def health():
    for unit in SERVICES:
        subprocess.run(['systemctl', 'is-active', '--quiet', unit], check=True, timeout=10)
    for relay in (False, True):
        require(request_json('/healthz', relay=relay).get('ok'), 'Local health failed')
        require(request_json('/healthz', relay=relay, public=True).get('ok'), 'Public HTTPS health failed')
    metrics = request_json('/v1/metrics')
    require(isinstance(metrics, dict) and bool(metrics), 'Gateway metrics unavailable')
    stats = request_json('/v1/relay/stats', relay=True)
    require(stats.get('ok', True) and stats['limits']['storage_budget_bytes'] == BUDGET,
            'Relay stats/budget check failed')
    pools = {}
    for name in ('database_pool', 'lifecycle_database_pool'):
        require(isinstance(stats.get(name), dict), 'Missing PostgreSQL pool metrics')
        pool = stats[name]
        require(not pool.get('requests_waiting', 0), 'Unexpected idle database pool wait')
        pools[name] = {k: v for k, v in pool.items() if isinstance(v, (int, float))}
    return {'gateway_https': True, 'relay_https': True, 'metrics_ok': True,
            'relay_storage_budget_bytes': BUDGET, 'database_pools': pools}


def wait_health():
    # Installation/rollback replaces this interpreter's imported package files.
    # Validate with the currently installed parser in a fresh interpreter.
    result = subprocess.run([str(VENV / 'bin/python'), str(Path(__file__)), 'health'],
                            capture_output=True, text=True, check=True, timeout=180)
    return json.loads(result.stdout)


def wait_health_current_process():
    for attempt in range(30):
        try:
            return health()
        except Exception:
            if attempt == 29:
                raise
            time.sleep(1)


def replace_config(raw, backup):
    # Preserve owner/mode, validate new-package config, then atomically publish it.
    candidate = CONFIG.with_name(CONFIG.name + '.build-pull-candidate')
    require(not candidate.exists(), 'Stale config candidate exists; inspect it')
    shutil.copy2(backup, candidate)
    original = CONFIG.stat()
    os.chown(candidate, original.st_uid, original.st_gid)
    candidate.write_bytes(json_bytes(raw))
    try:
        subprocess.run([str(VENV / 'bin/python'), '-c',
            'import json,sys; from ucloud_sandboxes.config import DeploymentConfig; '
            'DeploymentConfig.from_dict(json.load(open(sys.argv[1])))', str(candidate)], check=True, timeout=30)
        os.replace(candidate, CONFIG)
    finally:
        candidate.unlink(missing_ok=True)


def restore(rollback):
    require((rollback / 'gateway-venv').is_dir() and (rollback / 'deployment.json').is_file(),
            'Incomplete rollback artifacts')
    manifest = json.loads((rollback / 'backup-manifest.json').read_text())
    require(sha(rollback / 'deployment.json') == manifest['config_sha256'], 'Backup config changed')
    require(inventory_digest(inventory(rollback / 'gateway-venv')) == manifest['venv_inventory_sha256'],
            'Backup venv changed')
    service('stop')
    failed = ROOT / ('failed-venv-' + str(time.time_ns()))
    VENV.rename(failed)
    shutil.copytree(rollback / 'gateway-venv', VENV, symlinks=True)
    previous = CONFIG.with_name(CONFIG.name + '.build-pull-rollback')
    shutil.copy2(rollback / 'deployment.json', previous)
    meta = manifest['config_owner']
    os.chown(previous, meta['uid'], meta['gid'])
    os.chmod(previous, meta['mode'])
    os.replace(previous, CONFIG)
    service('start')
    return wait_health()


def apply(args):
    # pip creates package directories/files using the process umask. Services
    # run as an unprivileged user; an inherited test umask must not hide imports.
    os.umask(0o022)
    receipt = validate_receipt(args.receipt_sha256)
    idle_guard()
    raw, _ = read_config()
    old = deepcopy(raw)
    raw['node_package_root'] = str(ROOT)
    expected = deepcopy(old)
    expected['node_package_root'] = str(ROOT)
    require(raw == expected, 'Unexpected config change')
    rollback = ROOT / 'rollback'
    require(not rollback.exists(), 'Refusing to overwrite rollback artifacts')
    require(shutil.disk_usage(ROOT).free > 10 * 1024**3, 'Insufficient backup disk space')
    before = inventory(VENV, exclude_agent=True)
    rollback.mkdir(mode=0o700)
    shutil.copytree(VENV, rollback / 'gateway-venv', symlinks=True)
    shutil.copy2(CONFIG, rollback / 'deployment.json')
    info = CONFIG.stat()
    backup_manifest = {'config_sha256': sha(CONFIG), 'config_owner':
                      {'uid': info.st_uid, 'gid': info.st_gid, 'mode': stat.S_IMODE(info.st_mode)},
                      'venv_inventory_sha256': inventory_digest(inventory(rollback / 'gateway-venv')),
                      'created_at': stamp()}
    write_json(rollback / 'backup-manifest.json', backup_manifest)
    # These guards remain outside rollback: if new work arrived during backup,
    # no service or live-file mutation has begun and the deployment must abort.
    idle_guard()
    require(sha(CONFIG) == receipt['config_sha256'], 'Config changed during backup')
    try:
        service('stop')
        subprocess.run([str(VENV / 'bin/python'), '-m', 'pip', 'install', '--no-index',
                        '--no-deps', '--force-reinstall', str(ROOT / WHEEL_NAME)], check=True, timeout=180)
        require(before == inventory(VENV, exclude_agent=True), 'Installed dependency/native bytes changed')
        subprocess.run([str(VENV / 'bin/python'), '-m', 'pip', 'check'], check=True, timeout=60)
        subprocess.run(['runuser', '-u', 'ucloud', '--', str(VENV / 'bin/python'), '-I', '-c',
            'import ucloud_sandboxes.cli, ucloud_sandboxes.images, '
            'ucloud_sandboxes.image_rootfs, ucloud_sandboxes.environment_builder'],
            check=True, timeout=30, cwd='/')
        replace_config(raw, rollback / 'deployment.json')
        service('start')
        checks = wait_health()
    except BaseException as error:
        write_json(ROOT / 'apply-failure.json', {'at': stamp(), 'exception_type': type(error).__name__})
        restored = restore(rollback)
        write_json(ROOT / 'automatic-rollback-receipt.json', {'at': stamp(), 'health': restored})
        raise RuntimeError('Apply failed; original venv/config restored and health verified') from None
    deployed = {'deployed_at': stamp(), 'staging_receipt_sha256': args.receipt_sha256,
                'wheel_sha256': receipt['wheel_sha256'], 'node_package_root': str(ROOT),
                'rollback': str(rollback), 'configuration_changed_keys': ['node_package_root'],
                'dependencies_unchanged': True, 'native_bundle_files_unchanged': True, 'health': checks}
    write_json(ROOT / 'deployment-receipt.json', deployed)
    print(json.dumps(deployed))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    candidate = commands.add_parser('stage')
    for field in ('wheel', 'repacker', 'builder-source', 'sandbox-source'):
        candidate.add_argument('--' + field + '-sha256', required=True)
    for command in ('check', 'apply'):
        selected = commands.add_parser(command)
        selected.add_argument('--receipt-sha256', required=True)
    commands.add_parser('rollback')
    commands.add_parser('health')
    args = parser.parse_args()
    require(os.geteuid() == 0, 'Run on gateway as root with its existing venv Python')
    require(ROOT.is_dir() and not ROOT.is_symlink(), 'Stage artifacts in the fixed release directory')
    if args.command == 'stage':
        stage(args)
    elif args.command == 'check':
        receipt = validate_receipt(args.receipt_sha256)
        print(json.dumps({'validated': True, 'wheel_sha256': receipt['wheel_sha256'],
                          'idle': idle_guard(), 'health': health()}))
    elif args.command == 'apply':
        apply(args)
    elif args.command == 'health':
        print(json.dumps(wait_health_current_process()))
    else:
        idle_guard()
        checks = restore(ROOT / 'rollback')
        write_json(ROOT / 'manual-rollback-receipt.json', {'at': stamp(), 'health': checks})
        print(json.dumps({'rolled_back': True, 'health': checks}))


if __name__ == '__main__':
    main()
